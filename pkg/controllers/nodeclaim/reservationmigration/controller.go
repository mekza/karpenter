/*
Copyright The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package reservationmigration moves running on-demand nodes into compatible capacity reservations in place, without
// replacing them. A node is migrated only when every pod bound to it, and its NodePool, accept the reserved labels
// the node will carry afterwards. See designs/reserved-capacity-migration.md.
package reservationmigration

import (
	"context"
	"fmt"
	"sort"
	"sync/atomic"
	"time"

	"github.com/awslabs/operatorpkg/reconciler"
	"github.com/awslabs/operatorpkg/serrors"
	"github.com/awslabs/operatorpkg/singleton"
	"github.com/samber/lo"
	"go.uber.org/multierr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/util/retry"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/operator/injection"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	nodeclaimutils "sigs.k8s.io/karpenter/pkg/utils/nodeclaim"
)

const (
	// SettleDuration is the minimum time a node stays fenced before Karpenter commits to migrating it. It lets pod
	// bindings the scheduler decided before it observed the fence land, so the final compatibility check sees them.
	SettleDuration = 10 * time.Second
	// ReissueInterval is how often a committed migration is re-requested from the cloud provider while the provider
	// doesn't yet report the instance in the target reservation.
	ReissueInterval = time.Minute
	// ObservationTimeout is how long a committed migration may go without the cloud provider accepting it before it is
	// abandoned, and how long an accepted migration may go unobserved before Karpenter warns about it.
	ObservationTimeout = 5 * time.Minute
	// AbandonTimeout is how long an accepted migration may go unobserved before it is abandoned.
	AbandonTimeout = 30 * time.Minute
	// FailureBackoff is how long a NodeClaim is skipped after its migration was abandoned.
	FailureBackoff = 10 * time.Minute
	// MaxInFlight bounds how many nodes are fenced for migration at once.
	MaxInFlight = 10

	pollInterval   = 5 * time.Second
	resyncInterval = 30 * time.Second
)

// Controller moves running on-demand NodeClaims into compatible capacity reservations in place. Each migration runs
// through these phases:
//  1. Fence: the Node is tainted with karpenter.sh/reservation-migration:NoSchedule so no new pod binds against its
//     on-demand labels.
//  2. Commit: after SettleDuration, eligibility is re-checked against the pods now bound to the node. If it still
//     holds, the target reservation is recorded on the NodeClaim, and the migration is never abandoned while the cloud
//     provider might still move the instance.
//  3. Migrate: the cloud provider is asked to move the instance, until it reports the instance in the reservation.
//  4. Relabel: the NodeClaim and Node take the provider's reserved labels, and the fence is lifted.
//
// The fence taint and the commit annotation persist the phase, so a restarted controller resumes where it left off.
// In-flight migrations are always driven to completion, even after the feature gate is disabled.
type Controller struct {
	clock         clock.Clock
	kubeClient    client.Client
	cluster       *state.Cluster
	cloudProvider cloudprovider.CloudProvider
	recorder      events.Recorder

	// fencedAt tracks when each in-flight NodeClaim was first observed fenced. Process-local; a restart re-seeds it, which
	// only extends the settle window.
	fencedAt map[types.UID]time.Time
	// issuedAt tracks when the migration of each committed NodeClaim was last requested from the cloud provider, and
	// committedAt when it was first observed committed. Process-local; a restart re-issues the request.
	issuedAt    map[types.UID]time.Time
	committedAt map[types.UID]time.Time
	// acceptedAt tracks when the cloud provider first accepted the migration of each committed NodeClaim.
	// Process-local; after a restart, an accepted migration is recognized once the provider reports the instance moved.
	acceptedAt map[types.UID]time.Time
	// plannedFor tracks the reservation each fenced, uncommitted NodeClaim was planned into, so its capacity stays claimed
	// while it settles. Process-local; a restart forgets the claim until the node is re-planned.
	plannedFor map[types.UID]string
	// backoffUntil holds when NodeClaims whose migration was recently abandoned may be fenced again.
	backoffUntil map[types.UID]time.Time
	// notMigratable holds the UIDs of NodeClaims the cloud provider can never migrate.
	notMigratable sets.Set[types.UID]
	// notImplemented is set once the cloud provider reports that it can't migrate instances.
	notImplemented atomic.Bool
}

func NewController(clk clock.Clock, kubeClient client.Client, cluster *state.Cluster, cloudProvider cloudprovider.CloudProvider, recorder events.Recorder) *Controller {
	return &Controller{
		clock:         clk,
		kubeClient:    kubeClient,
		cluster:       cluster,
		cloudProvider: cloudProvider,
		recorder:      recorder,
		fencedAt:      map[types.UID]time.Time{},
		issuedAt:      map[types.UID]time.Time{},
		committedAt:   map[types.UID]time.Time{},
		acceptedAt:    map[types.UID]time.Time{},
		plannedFor:    map[types.UID]string{},
		backoffUntil:  map[types.UID]time.Time{},
		notMigratable: sets.New[types.UID](),
	}
}

func (c *Controller) Name() string {
	return "nodeclaim.reservationmigration"
}

func (c *Controller) Register(_ context.Context, m manager.Manager) error {
	return controllerruntime.NewControllerManagedBy(m).
		Named(c.Name()).
		WatchesRawSource(singleton.Source()).
		Complete(singleton.AsReconciler(c))
}

func (c *Controller) Reconcile(ctx context.Context) (reconciler.Result, error) {
	ctx = injection.WithControllerName(ctx, c.Name())
	// Eligibility depends on every pod and node being accounted for, and nominations come from cluster state.
	if !c.cluster.Synced(ctx) {
		return reconciler.Result{RequeueAfter: time.Second}, nil
	}
	nodes := lo.Filter(c.cluster.DeepCopyNodes(), func(n *state.StateNode, _ int) bool {
		return n.Managed() && n.Node != nil && nodeclaimutils.IsManaged(n.NodeClaim, c.cloudProvider)
	})
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].NodeClaim.Name < nodes[j].NodeClaim.Name })
	inFlight := lo.Filter(nodes, func(n *state.StateNode, _ int) bool { return n.ReservationMigrationInProgress() })
	c.forgetCompleted(inFlight)
	c.notMigratable = c.notMigratable.Intersection(sets.New(lo.Map(nodes, func(n *state.StateNode, _ int) types.UID { return n.NodeClaim.UID })...))

	pass := newPass(c)
	// Claim the capacity of every in-flight migration up front, so no node is planned into a slot another node holds.
	for _, n := range inFlight {
		if target := n.NodeClaim.Annotations[v1.ReservationMigrationTargetAnnotationKey]; target != "" {
			pass.claim(target)
		} else if planned, ok := c.plannedFor[n.NodeClaim.UID]; ok {
			pass.claim(planned)
		}
	}
	requeueAfter := resyncInterval
	// One node's failure doesn't hold up the others; their errors are returned together.
	var errs []error
	for _, n := range inFlight {
		after, err := c.reconcileInFlight(ctx, pass, n)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		requeueAfter = min(requeueAfter, after)
	}
	if len(errs) != 0 {
		return reconciler.Result{}, multierr.Combine(errs...)
	}
	if !c.enabled(ctx) {
		return reconciler.Result{RequeueAfter: requeueAfter}, nil
	}
	fenced, err := c.fenceCandidates(ctx, pass, nodes, MaxInFlight-len(inFlight))
	if err != nil {
		return reconciler.Result{}, err
	}
	if fenced {
		requeueAfter = min(requeueAfter, SettleDuration)
	}
	return reconciler.Result{RequeueAfter: requeueAfter}, nil
}

// enabled returns whether new migrations may start.
func (c *Controller) enabled(ctx context.Context) bool {
	gates := options.FromContext(ctx).FeatureGates
	return gates.ReservedCapacityMigration && gates.ReservedCapacity && !c.notImplemented.Load()
}

// forgetCompleted drops the process-local tracking of NodeClaims that are no longer migrating.
func (c *Controller) forgetCompleted(inFlight []*state.StateNode) {
	active := sets.New(lo.Map(inFlight, func(n *state.StateNode, _ int) types.UID { return n.NodeClaim.UID })...)
	for _, m := range []map[types.UID]time.Time{c.fencedAt, c.issuedAt, c.committedAt, c.acceptedAt} {
		for uid := range m {
			if !active.Has(uid) {
				delete(m, uid)
			}
		}
	}
	for uid := range c.plannedFor {
		if !active.Has(uid) {
			delete(c.plannedFor, uid)
		}
	}
	for uid, until := range c.backoffUntil {
		if !c.clock.Now().Before(until) {
			delete(c.backoffUntil, uid)
		}
	}
}

// fenceCandidates starts migrating up to limit eligible on-demand nodes by fencing them. It returns whether any node was
// fenced.
func (c *Controller) fenceCandidates(ctx context.Context, pass *pass, nodes []*state.StateNode, limit int) (bool, error) {
	fenced := false
	// One node's failure, e.g. its NodePool's instance types being unavailable, doesn't hold up the others.
	var errs []error
	for _, n := range nodes {
		if limit <= 0 {
			break
		}
		if !c.isCandidate(n) {
			continue
		}
		p, err := pass.plan(ctx, n)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if p == nil {
			continue
		}
		if err := c.setFence(ctx, n.Node.Name, true); err != nil {
			errs = append(errs, err)
			continue
		}
		c.fencedAt[n.NodeClaim.UID] = c.clock.Now()
		c.plannedFor[n.NodeClaim.UID] = p.offering.ReservationID()
		pass.claim(p.offering.ReservationID())
		log.FromContext(ctx).WithValues("NodeClaim", klog.KObj(n.NodeClaim), "Node", klog.KObj(n.Node), "reservation-id", p.offering.ReservationID()).
			V(1).Info("fenced node for migration into capacity reservation")
		fenced = true
		limit--
	}
	return fenced, multierr.Combine(errs...)
}

// isCandidate runs the checks for starting a migration that don't require any API calls.
func (c *Controller) isCandidate(n *state.StateNode) bool {
	if n.ReservationMigrationInProgress() {
		return false
	}
	if until, ok := c.backoffUntil[n.NodeClaim.UID]; ok && c.clock.Now().Before(until) {
		return false
	}
	if c.notMigratable.Has(n.NodeClaim.UID) {
		return false
	}
	return c.isEligibleNode(n)
}

// isEligibleNode returns whether a node may be migrated, ignoring the migration's own fence. Nodes that are deleting,
// disrupted, rebooting, nominated for pending pods, or opted out of disruption are left alone.
func (c *Controller) isEligibleNode(n *state.StateNode) bool {
	if !n.Initialized() || n.MarkedForDeletion() || n.Nominated(c.clock) || n.RebootInProgress() {
		return false
	}
	if !n.Node.DeletionTimestamp.IsZero() || !n.NodeClaim.DeletionTimestamp.IsZero() {
		return false
	}
	if n.NodeClaim.Status.ProviderID == "" || n.NodeClaim.Labels[v1.NodePoolLabelKey] == "" {
		return false
	}
	if n.NodeClaim.Labels[v1.CapacityTypeLabelKey] != v1.CapacityTypeOnDemand || n.Node.Labels[v1.CapacityTypeLabelKey] != v1.CapacityTypeOnDemand {
		return false
	}
	if n.Annotations()[v1.DoNotDisruptAnnotationKey] == "true" {
		return false
	}
	return !lo.ContainsBy(n.Node.Spec.Taints, func(t corev1.Taint) bool { return t.MatchTaint(&v1.DisruptedNoScheduleTaint) })
}

// reconcileInFlight drives a fenced or committed migration and returns when it should be reconciled next.
func (c *Controller) reconcileInFlight(ctx context.Context, pass *pass, n *state.StateNode) (time.Duration, error) {
	ctx = log.IntoContext(ctx, log.FromContext(ctx).WithValues("NodeClaim", klog.KObj(n.NodeClaim), "Node", klog.KObj(n.Node)))
	// A deleting node is owned by termination; there is nothing left to migrate and no fence worth lifting.
	if !n.Node.DeletionTimestamp.IsZero() || !n.NodeClaim.DeletionTimestamp.IsZero() {
		return resyncInterval, nil
	}
	if target := n.NodeClaim.Annotations[v1.ReservationMigrationTargetAnnotationKey]; target != "" {
		return c.reconcileCommitted(ctx, pass, n, target)
	}
	return c.reconcileFenced(ctx, pass, n)
}

// reconcileFenced commits a fenced migration once the settle window has elapsed and the node is still eligible, or
// lifts the fence otherwise.
func (c *Controller) reconcileFenced(ctx context.Context, pass *pass, n *state.StateNode) (time.Duration, error) {
	fencedAt, ok := c.fencedAt[n.NodeClaim.UID]
	if !ok {
		fencedAt = c.clock.Now()
		c.fencedAt[n.NodeClaim.UID] = fencedAt
	}
	if remaining := SettleDuration - c.clock.Since(fencedAt); remaining > 0 {
		return remaining, nil
	}
	// Re-plan from scratch against the pods bound now, releasing this node's own claim so it can be planned again.
	if planned, ok := c.plannedFor[n.NodeClaim.UID]; ok {
		pass.unclaim(planned)
		delete(c.plannedFor, n.NodeClaim.UID)
	}
	var p *plan
	if c.enabled(ctx) && c.isEligibleNode(n) {
		var err error
		if p, err = pass.plan(ctx, n); err != nil {
			return 0, err
		}
	}
	if p == nil {
		log.FromContext(ctx).V(1).Info("abandoning migration into capacity reservation, node is no longer eligible")
		return resyncInterval, c.abandon(ctx, n)
	}
	pass.claim(p.offering.ReservationID())
	if err := c.commit(ctx, n.NodeClaim, p.offering.ReservationID()); err != nil {
		return 0, err
	}
	log.FromContext(ctx).WithValues("reservation-id", p.offering.ReservationID()).Info("migrating node into capacity reservation")
	return c.reconcileCommitted(ctx, pass, n, p.offering.ReservationID())
}

// reconcileCommitted requests the migration from the cloud provider until it reports the instance in a reservation,
// then relabels the NodeClaim and Node and lifts the fence.
//
// Until the provider accepts the request, the migration is abandoned when the provider rejects it, the node stops being
// compatible with the reservation, or no attempt succeeds within ObservationTimeout. Once the provider accepts it, the
// instance may move at any point, so the node stays fenced and the request is re-issued until the provider reports the
// move. Only if it never does within AbandonTimeout is the migration abandoned, as the provider's view of the instance,
// which the node's labels already match, is then trusted.
//
//nolint:gocyclo
func (c *Controller) reconcileCommitted(ctx context.Context, pass *pass, n *state.StateNode, reservationID string) (time.Duration, error) {
	ctx = log.IntoContext(ctx, log.FromContext(ctx).WithValues("reservation-id", reservationID))
	uid := n.NodeClaim.UID
	if _, ok := c.committedAt[uid]; !ok {
		c.committedAt[uid] = c.clock.Now()
	}
	// Keep the node fenced, even if the taint was removed out of band, for as long as the migration is committed.
	if err := c.setFence(ctx, n.Node.Name, true); err != nil {
		return 0, err
	}
	done, err := c.completeIfMigrated(ctx, pass, n, reservationID)
	if err != nil || done {
		return resyncInterval, err
	}
	_, accepted := c.acceptedAt[uid]
	committedFor := c.clock.Since(c.committedAt[uid])
	if accepted && committedFor > AbandonTimeout {
		log.FromContext(ctx).Info("abandoning migration into capacity reservation, cloud provider never reported the instance in it")
		c.recorder.Publish(MigrationFailed(n.NodeClaim, reservationID, fmt.Sprintf("cloud provider didn't report the instance in the reservation within %s", AbandonTimeout)))
		return resyncInterval, c.abandon(ctx, n)
	}
	if !accepted && committedFor > ObservationTimeout {
		log.FromContext(ctx).Info("abandoning migration into capacity reservation, cloud provider didn't accept it")
		c.recorder.Publish(MigrationFailed(n.NodeClaim, reservationID, fmt.Sprintf("cloud provider didn't accept the migration within %s", ObservationTimeout)))
		return resyncInterval, c.abandon(ctx, n)
	}
	if issuedAt, ok := c.issuedAt[uid]; ok && c.clock.Since(issuedAt) < ReissueInterval {
		return pollInterval, nil
	}
	if accepted && committedFor > ObservationTimeout {
		c.recorder.Publish(MigrationNotObserved(n.NodeClaim, reservationID))
	}
	offering, err := pass.offering(ctx, n.NodeClaim, reservationID)
	if err != nil {
		return 0, err
	}
	if !accepted {
		// Nothing was accepted yet, so the migration can still be called off if it no longer holds. Check right before
		// the request, against the pods bound now, so a pod that bound after the commit isn't relabeled.
		reason, err := pass.incompatible(ctx, n, offering)
		if err != nil {
			return 0, err
		}
		if reason != "" {
			log.FromContext(ctx).Info(fmt.Sprintf("abandoning migration into capacity reservation, %s", reason))
			c.recorder.Publish(MigrationFailed(n.NodeClaim, reservationID, reason))
			return resyncInterval, c.abandon(ctx, n)
		}
	} else if offering == nil {
		// The reservation is no longer offered, so the request can't be re-issued, but the instance may still move.
		c.issuedAt[uid] = c.clock.Now()
		return pollInterval, nil
	}
	c.issuedAt[uid] = c.clock.Now()
	if err := c.cloudProvider.MigrateToReservation(ctx, n.NodeClaim, offering); err != nil {
		// The provider may have applied the change even though the call failed.
		if done, getErr := c.completeIfMigrated(ctx, pass, n, reservationID); getErr != nil || done {
			return resyncInterval, getErr
		}
		if accepted {
			log.FromContext(ctx).Error(err, "failed re-requesting migration into capacity reservation")
			return pollInterval, nil
		}
		switch {
		case cloudprovider.IsReservationMigrationNotImplementedError(err):
			c.notImplemented.Store(true)
			log.FromContext(ctx).Info("cloud provider doesn't support migrating instances into capacity reservations, disabling reservation migration")
		case cloudprovider.IsNodeClaimNotMigratableError(err):
			c.notMigratable.Insert(uid)
			log.FromContext(ctx).Info(fmt.Sprintf("node can't be migrated into a capacity reservation, %s", err))
		case cloudprovider.IsReservationMigrationRejectedError(err):
			log.FromContext(ctx).Info(fmt.Sprintf("cloud provider rejected migration into capacity reservation, %s", err))
		default:
			// It's unknown whether the request was applied, so it is retried rather than abandoned.
			log.FromContext(ctx).Error(err, "failed requesting migration into capacity reservation")
			return pollInterval, nil
		}
		c.recorder.Publish(MigrationFailed(n.NodeClaim, reservationID, err.Error()))
		return resyncInterval, c.abandon(ctx, n)
	}
	if !accepted {
		c.acceptedAt[uid] = c.clock.Now()
	}
	if done, err := c.completeIfMigrated(ctx, pass, n, reservationID); err != nil || done {
		return resyncInterval, err
	}
	return pollInterval, nil
}

// completeIfMigrated relabels the NodeClaim and Node and lifts the fence once the cloud provider reports the instance
// in a capacity reservation. It returns whether the migration is complete.
func (c *Controller) completeIfMigrated(ctx context.Context, pass *pass, n *state.StateNode, reservationID string) (bool, error) {
	cpNodeClaim, err := c.cloudProvider.Get(ctx, n.NodeClaim.Status.ProviderID)
	if err != nil {
		if cloudprovider.IsNodeClaimNotFoundError(err) {
			// The instance is gone, so the NodeClaim is about to be garbage collected. Lift the fence regardless.
			return true, c.abandon(ctx, n)
		}
		return false, serrors.Wrap(fmt.Errorf("getting nodeclaim from cloud provider, %w", err), "NodeClaim", klog.KObj(n.NodeClaim))
	}
	if cpNodeClaim.Labels[v1.CapacityTypeLabelKey] != v1.CapacityTypeReserved || cpNodeClaim.Labels[cloudprovider.ReservationIDLabel] == "" {
		return false, nil
	}
	// The cloud provider is the source of truth for the labels of the instance it now runs in a reservation, even where
	// they differ from the reservation that was planned, e.g. because an earlier, abandoned request landed after all.
	reserved := lo.SliceToMap(migratingLabelKeys(), func(key string) (string, *string) {
		value, ok := cpNodeClaim.Labels[key]
		return key, lo.Ternary(ok, &value, nil)
	})
	if reason, err := pass.incompatibleLabels(ctx, n, reserved); err != nil {
		return false, err
	} else if reason != "" || cpNodeClaim.Labels[cloudprovider.ReservationIDLabel] != reservationID {
		// Too late to call off: the labels must match where the instance runs.
		message := fmt.Sprintf("instance runs in capacity reservation %s, relabeling it even though %s", cpNodeClaim.Labels[cloudprovider.ReservationIDLabel],
			lo.Ternary(reason != "", reason, fmt.Sprintf("capacity reservation %s was planned", reservationID)))
		log.FromContext(ctx).Info(message)
		c.recorder.Publish(MigrationMismatched(n.NodeClaim, message))
	}
	if err := c.relabel(ctx, n, reserved); err != nil {
		return false, err
	}
	if err := c.uncommit(ctx, n.NodeClaim); err != nil {
		return false, err
	}
	if err := c.setFence(ctx, n.Node.Name, false); err != nil {
		return false, err
	}
	log.FromContext(ctx).Info("migrated node into capacity reservation")
	c.recorder.Publish(Migrated(n.NodeClaim, cpNodeClaim.Labels[cloudprovider.ReservationIDLabel]))
	return true, nil
}

// abandon lifts the fence and the commitment of a migration, and backs off the NodeClaim so it isn't fenced again right
// away.
func (c *Controller) abandon(ctx context.Context, n *state.StateNode) error {
	c.backoffUntil[n.NodeClaim.UID] = c.clock.Now().Add(FailureBackoff)
	if err := c.uncommit(ctx, n.NodeClaim); err != nil {
		return err
	}
	return c.setFence(ctx, n.Node.Name, false)
}

// relabel sets the NodeClaim and Node labels for migratingLabelKeys to the given values, removing the labels whose
// value is nil.
func (c *Controller) relabel(ctx context.Context, n *state.StateNode, values map[string]*string) error {
	nodeClaim := &v1.NodeClaim{}
	if err := c.kubeClient.Get(ctx, client.ObjectKeyFromObject(n.NodeClaim), nodeClaim); err != nil {
		return serrors.Wrap(fmt.Errorf("getting nodeclaim, %w", err), "NodeClaim", klog.KObj(n.NodeClaim))
	}
	if nodeClaim.UID != n.NodeClaim.UID {
		return nil
	}
	stored := nodeClaim.DeepCopy()
	nodeClaim.Labels = applyLabels(nodeClaim.Labels, values)
	if !equality.Semantic.DeepEqual(stored.Labels, nodeClaim.Labels) {
		if err := c.kubeClient.Patch(ctx, nodeClaim, client.MergeFrom(stored)); err != nil {
			return serrors.Wrap(fmt.Errorf("patching nodeclaim labels, %w", err), "NodeClaim", klog.KObj(nodeClaim))
		}
	}
	node := &corev1.Node{}
	if err := c.kubeClient.Get(ctx, client.ObjectKeyFromObject(n.Node), node); err != nil {
		return serrors.Wrap(fmt.Errorf("getting node, %w", err), "Node", klog.KObj(n.Node))
	}
	storedNode := node.DeepCopy()
	node.Labels = applyLabels(node.Labels, values)
	if !equality.Semantic.DeepEqual(storedNode.Labels, node.Labels) {
		if err := c.kubeClient.Patch(ctx, node, client.MergeFrom(storedNode)); err != nil {
			return serrors.Wrap(fmt.Errorf("patching node labels, %w", err), "Node", klog.KObj(node))
		}
	}
	return nil
}

// commit records the target reservation on the NodeClaim. From then on the migration is only abandoned once the
// instance is confirmed outside of the reservation.
func (c *Controller) commit(ctx context.Context, nc *v1.NodeClaim, reservationID string) error {
	return c.patchAnnotation(ctx, nc, &reservationID)
}

func (c *Controller) uncommit(ctx context.Context, nc *v1.NodeClaim) error {
	return c.patchAnnotation(ctx, nc, nil)
}

func (c *Controller) patchAnnotation(ctx context.Context, nc *v1.NodeClaim, reservationID *string) error {
	nodeClaim := &v1.NodeClaim{}
	if err := c.kubeClient.Get(ctx, client.ObjectKeyFromObject(nc), nodeClaim); err != nil {
		return client.IgnoreNotFound(serrors.Wrap(fmt.Errorf("getting nodeclaim, %w", err), "NodeClaim", klog.KObj(nc)))
	}
	if nodeClaim.UID != nc.UID {
		return nil
	}
	stored := nodeClaim.DeepCopy()
	if reservationID != nil {
		nodeClaim.Annotations = lo.Assign(nodeClaim.Annotations, map[string]string{v1.ReservationMigrationTargetAnnotationKey: *reservationID})
	} else {
		delete(nodeClaim.Annotations, v1.ReservationMigrationTargetAnnotationKey)
	}
	if equality.Semantic.DeepEqual(stored.Annotations, nodeClaim.Annotations) {
		return nil
	}
	if err := c.kubeClient.Patch(ctx, nodeClaim, client.MergeFrom(stored)); err != nil {
		return client.IgnoreNotFound(serrors.Wrap(fmt.Errorf("patching nodeclaim, %w", err), "NodeClaim", klog.KObj(nodeClaim)))
	}
	return nil
}

// setFence adds or removes the reservation migration taint on the Node.
func (c *Controller) setFence(ctx context.Context, nodeName string, fence bool) error {
	return retry.OnError(retry.DefaultBackoff, func(err error) bool { return client.IgnoreNotFound(err) != nil }, func() error {
		node := &corev1.Node{}
		if err := c.kubeClient.Get(ctx, types.NamespacedName{Name: nodeName}, node); err != nil {
			return client.IgnoreNotFound(err)
		}
		stored := node.DeepCopy()
		node.Spec.Taints = lo.Reject(node.Spec.Taints, func(t corev1.Taint, _ int) bool { return t.MatchTaint(&v1.ReservationMigrationNoScheduleTaint) })
		if fence {
			node.Spec.Taints = append(node.Spec.Taints, v1.ReservationMigrationNoScheduleTaint)
		}
		if equality.Semantic.DeepEqual(stored.Spec.Taints, node.Spec.Taints) {
			return nil
		}
		// Taints are a list, which a JSON merge patch replaces wholesale, so lock to avoid dropping concurrent changes.
		if err := c.kubeClient.Patch(ctx, node, client.MergeFromWithOptions(stored, client.MergeFromWithOptimisticLock{})); err != nil {
			return serrors.Wrap(fmt.Errorf("patching node taints, %w", err), "Node", klog.KObj(node))
		}
		return nil
	})
}

// pass caches the cluster-wide inputs of a single reconcile, and tracks reservation capacity claimed by in-flight
// migrations so the same free slot is never planned for two nodes.
type pass struct {
	c             *Controller
	instanceTypes map[string][]*cloudprovider.InstanceType
	daemonSets    []*appsv1.DaemonSet
	constraints   []topologyConstraint
	loaded        bool
	claimed       map[string]int
}

func newPass(c *Controller) *pass {
	return &pass{c: c, instanceTypes: map[string][]*cloudprovider.InstanceType{}, claimed: map[string]int{}}
}

func (p *pass) claim(reservationID string) {
	p.claimed[reservationID]++
}

func (p *pass) unclaim(reservationID string) {
	if p.claimed[reservationID] > 0 {
		p.claimed[reservationID]--
	}
}

// load lists the cluster-wide inputs needed to evaluate a migration.
func (p *pass) load(ctx context.Context) error {
	if p.loaded {
		return nil
	}
	dsList := &appsv1.DaemonSetList{}
	if err := p.c.kubeClient.List(ctx, dsList); err != nil {
		return serrors.Wrap(fmt.Errorf("listing daemonsets, %w", err))
	}
	p.daemonSets = lo.ToSlicePtr(dsList.Items)
	podList := &corev1.PodList{}
	if err := p.c.kubeClient.List(ctx, podList); err != nil {
		return serrors.Wrap(fmt.Errorf("listing pods, %w", err))
	}
	p.constraints = topologyConstraints(lo.ToSlicePtr(podList.Items), migratingLabelKeys())
	p.loaded = true
	return nil
}

func (p *pass) getInstanceTypes(ctx context.Context, nodePool *v1.NodePool) ([]*cloudprovider.InstanceType, error) {
	if its, ok := p.instanceTypes[nodePool.Name]; ok {
		return its, nil
	}
	its, err := p.c.cloudProvider.GetInstanceTypes(ctx, nodePool)
	if err != nil {
		return nil, serrors.Wrap(fmt.Errorf("getting instance types, %w", err), "NodePool", klog.KObj(nodePool))
	}
	p.instanceTypes[nodePool.Name] = its
	return its, nil
}

// reservedOfferings returns the reserved offerings of the NodeClaim's instance type, or nil if its NodePool or instance
// type can't be resolved.
func (p *pass) reservedOfferings(ctx context.Context, nodeClaim *v1.NodeClaim) (*v1.NodePool, cloudprovider.Offerings, error) {
	nodePool := &v1.NodePool{}
	if err := p.c.kubeClient.Get(ctx, types.NamespacedName{Name: nodeClaim.Labels[v1.NodePoolLabelKey]}, nodePool); err != nil {
		return nil, nil, client.IgnoreNotFound(serrors.Wrap(fmt.Errorf("getting nodepool, %w", err), "NodePool", klog.KRef("", nodeClaim.Labels[v1.NodePoolLabelKey])))
	}
	its, err := p.getInstanceTypes(ctx, nodePool)
	if err != nil {
		return nil, nil, err
	}
	it, ok := lo.Find(its, func(it *cloudprovider.InstanceType) bool {
		return it.Name == nodeClaim.Labels[corev1.LabelInstanceTypeStable]
	})
	if !ok {
		return nodePool, nil, nil
	}
	return nodePool, lo.Filter(it.Offerings, func(o *cloudprovider.Offering, _ int) bool {
		return o.CapacityType() == v1.CapacityTypeReserved && o.ReservationID() != ""
	}), nil
}

// offering returns the reserved offering for the reservation, regardless of its availability, or nil if it isn't offered.
func (p *pass) offering(ctx context.Context, nodeClaim *v1.NodeClaim, reservationID string) (*cloudprovider.Offering, error) {
	_, offerings, err := p.reservedOfferings(ctx, nodeClaim)
	if err != nil {
		return nil, err
	}
	o, _ := lo.Find(offerings, func(o *cloudprovider.Offering) bool { return o.ReservationID() == reservationID })
	return o, nil
}

// incompatible returns why the node can no longer migrate into the offering's reservation, or "" if it still can. Unlike
// plan, it doesn't require the reservation to have free capacity, since the node already holds its slot.
func (p *pass) incompatible(ctx context.Context, n *state.StateNode, offering *cloudprovider.Offering) (string, error) {
	if offering == nil {
		return "capacity reservation is no longer offered", nil
	}
	values, ok := reservedLabels(n.NodeClaim.Labels, offering)
	if !ok {
		return "capacity reservation no longer matches the node", nil
	}
	return p.incompatibleLabels(ctx, n, values)
}

// incompatibleLabels returns why relabeling the node with the migrating label values would break a scheduling decision,
// or "" if it wouldn't.
func (p *pass) incompatibleLabels(ctx context.Context, n *state.StateNode, values map[string]*string) (string, error) {
	nodePool := &v1.NodePool{}
	if err := p.c.kubeClient.Get(ctx, types.NamespacedName{Name: n.NodeClaim.Labels[v1.NodePoolLabelKey]}, nodePool); err != nil {
		if errors.IsNotFound(err) {
			return fmt.Sprintf("nodepool %q doesn't exist", n.NodeClaim.Labels[v1.NodePoolLabelKey]), nil
		}
		return "", serrors.Wrap(fmt.Errorf("getting nodepool, %w", err), "NodePool", klog.KRef("", n.NodeClaim.Labels[v1.NodePoolLabelKey]))
	}
	if err := p.load(ctx); err != nil {
		return "", err
	}
	pods, err := n.Pods(ctx, p.c.kubeClient)
	if err != nil {
		return "", err
	}
	return incompatibility(nodePool, n.NodeClaim, n.Node, values, pods, p.daemonSets, p.constraints), nil
}

type plan struct {
	offering *cloudprovider.Offering
}

// plan returns the reservation the node should migrate into, or nil if it shouldn't migrate.
func (p *pass) plan(ctx context.Context, n *state.StateNode) (*plan, error) {
	nodePool, offerings, err := p.reservedOfferings(ctx, n.NodeClaim)
	if err != nil || nodePool == nil {
		return nil, err
	}
	offerings = lo.Filter(offerings, func(o *cloudprovider.Offering, _ int) bool {
		return o.Available && o.Compatible && o.Migratable && o.ReservationCapacity-p.claimed[o.ReservationID()] > 0
	})
	if len(offerings) == 0 {
		return nil, nil
	}
	// Prefer the cheapest reservation, breaking ties deterministically.
	sort.SliceStable(offerings, func(i, j int) bool {
		if offerings[i].Price != offerings[j].Price {
			return offerings[i].Price < offerings[j].Price
		}
		return offerings[i].ReservationID() < offerings[j].ReservationID()
	})
	if err := p.load(ctx); err != nil {
		return nil, err
	}
	pods, err := n.Pods(ctx, p.c.kubeClient)
	if err != nil {
		return nil, err
	}
	for _, o := range offerings {
		values, ok := reservedLabels(n.NodeClaim.Labels, o)
		if !ok {
			continue
		}
		if reason := incompatibility(nodePool, n.NodeClaim, n.Node, values, pods, p.daemonSets, p.constraints); reason != "" {
			log.FromContext(ctx).WithValues("NodeClaim", klog.KObj(n.NodeClaim), "reservation-id", o.ReservationID()).V(1).Info(fmt.Sprintf("not migrating node into capacity reservation, %s", reason))
			continue
		}
		return &plan{offering: o}, nil
	}
	return nil, nil
}
