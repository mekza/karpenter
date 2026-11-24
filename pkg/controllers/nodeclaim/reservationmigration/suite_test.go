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

package reservationmigration_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/awslabs/operatorpkg/singleton"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	crreconcile "sigs.k8s.io/controller-runtime/pkg/reconcile"

	"sigs.k8s.io/karpenter/pkg/apis"
	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/controllers/nodeclaim/reservationmigration"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/controllers/state/informer"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/scheduling"
	"sigs.k8s.io/karpenter/pkg/state/cost"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
	"sigs.k8s.io/karpenter/pkg/test/v1alpha1"
	. "sigs.k8s.io/karpenter/pkg/utils/testing"
)

const (
	zone          = "test-zone-1"
	reservationID = "r-1"
)

var (
	ctx                      context.Context
	env                      *test.Environment
	cloudProvider            *fake.CloudProvider
	cluster                  *state.Cluster
	nodeStateController      *informer.NodeController
	nodeClaimStateController *informer.NodeClaimController
	recorder                 *test.EventRecorder
	controller               *reservationmigration.Controller
)

func TestAPIs(t *testing.T) {
	ctx = TestContextWithLogger(t)
	RegisterFailHandler(Fail)
	RunSpecs(t, "ReservationMigration")
}

var _ = BeforeSuite(func() {
	env = test.NewEnvironment(
		test.WithCRDs(apis.CRDs...),
		test.WithCRDs(v1alpha1.CRDs...),
		test.WithFieldIndexers(test.NodeClaimProviderIDFieldIndexer(ctx), test.NodeProviderIDFieldIndexer(ctx)),
	)
	cloudProvider = fake.NewCloudProvider()
	recorder = test.NewEventRecorder()
})

var _ = AfterSuite(func() {
	Expect(env.Stop()).To(Succeed(), "Failed to stop environment")
})

func migrationOptions(enabled bool) context.Context {
	return options.ToContext(ctx, test.Options(test.OptionsFields{FeatureGates: test.FeatureGates{
		ReservedCapacity:          new(true),
		ReservedCapacityMigration: new(enabled),
	}}))
}

// reservedOffering returns a reserved offering in zone for the reservation.
func reservedOffering(id string, capacity int, migratable bool) *cloudprovider.Offering {
	return &cloudprovider.Offering{
		Requirements: scheduling.NewLabelRequirements(map[string]string{
			v1.CapacityTypeLabelKey:     v1.CapacityTypeReserved,
			corev1.LabelTopologyZone:    zone,
			v1alpha1.LabelReservationID: id,
		}),
		Price:               0.000001,
		Available:           true,
		Compatible:          true,
		ReservationCapacity: capacity,
		Migratable:          migratable,
	}
}

var _ = Describe("ReservationMigration", func() {
	var instanceType *cloudprovider.InstanceType
	var nodePool *v1.NodePool
	var nodeClaim *v1.NodeClaim
	var node *corev1.Node

	// newNodeClaimAndNode returns an initialized on-demand NodeClaim and Node of instanceType in zone, known to the cloud
	// provider.
	newNodeClaimAndNode := func() (*v1.NodeClaim, *corev1.Node) {
		labels := map[string]string{
			v1.NodePoolLabelKey:            nodePool.Name,
			corev1.LabelInstanceTypeStable: instanceType.Name,
			corev1.LabelTopologyZone:       zone,
			v1.CapacityTypeLabelKey:        v1.CapacityTypeOnDemand,
		}
		nc, n := test.NodeClaimAndNode(v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Labels: labels}})
		n.Labels = lo.Assign(n.Labels, labels)
		cloudProvider.CreatedNodeClaims[nc.Status.ProviderID] = nc.DeepCopy()
		return nc, n
	}
	// expectStateUpdated syncs cluster state with the current Nodes and NodeClaims.
	expectStateUpdated := func(nodes []*corev1.Node, nodeClaims []*v1.NodeClaim) {
		GinkgoHelper()
		for _, n := range nodes {
			ExpectReconcileSucceeded(ctx, nodeStateController, client.ObjectKeyFromObject(n))
		}
		for _, nc := range nodeClaims {
			ExpectReconcileSucceeded(ctx, nodeClaimStateController, client.ObjectKeyFromObject(nc))
		}
	}
	// reconcile syncs cluster state and runs the controller once.
	reconcile := func(nodes []*corev1.Node, nodeClaims []*v1.NodeClaim) {
		GinkgoHelper()
		expectStateUpdated(nodes, nodeClaims)
		ExpectSingletonReconciled(ctx, controller)
	}
	expectFenced := func(n *corev1.Node, fenced bool) {
		GinkgoHelper()
		n = ExpectExists(ctx, env.Client, n)
		Expect(lo.ContainsBy(n.Spec.Taints, func(t corev1.Taint) bool { return t.MatchTaint(&v1.ReservationMigrationNoScheduleTaint) })).To(Equal(fenced))
	}
	expectOnDemand := func(nc *v1.NodeClaim, n *corev1.Node) {
		GinkgoHelper()
		nc = ExpectExists(ctx, env.Client, nc)
		n = ExpectExists(ctx, env.Client, n)
		Expect(nc.Labels).To(HaveKeyWithValue(v1.CapacityTypeLabelKey, v1.CapacityTypeOnDemand))
		Expect(nc.Labels).ToNot(HaveKey(v1alpha1.LabelReservationID))
		Expect(n.Labels).To(HaveKeyWithValue(v1.CapacityTypeLabelKey, v1.CapacityTypeOnDemand))
		Expect(n.Labels).ToNot(HaveKey(v1alpha1.LabelReservationID))
		Expect(nc.Annotations).ToNot(HaveKey(v1.ReservationMigrationTargetAnnotationKey))
	}
	expectReserved := func(nc *v1.NodeClaim, n *corev1.Node, id string) {
		GinkgoHelper()
		nc = ExpectExists(ctx, env.Client, nc)
		n = ExpectExists(ctx, env.Client, n)
		Expect(nc.Labels).To(HaveKeyWithValue(v1.CapacityTypeLabelKey, v1.CapacityTypeReserved))
		Expect(nc.Labels).To(HaveKeyWithValue(v1alpha1.LabelReservationID, id))
		Expect(n.Labels).To(HaveKeyWithValue(v1.CapacityTypeLabelKey, v1.CapacityTypeReserved))
		Expect(n.Labels).To(HaveKeyWithValue(v1alpha1.LabelReservationID, id))
		Expect(nc.Annotations).ToNot(HaveKey(v1.ReservationMigrationTargetAnnotationKey))
		Expect(lo.ContainsBy(n.Spec.Taints, func(t corev1.Taint) bool { return t.MatchTaint(&v1.ReservationMigrationNoScheduleTaint) })).To(BeFalse())
	}
	// settle steps the clock past the settle window of a fenced node.
	settle := func() {
		env.Clock.Step(reservationmigration.SettleDuration)
	}

	BeforeEach(func() {
		ctx = migrationOptions(true)
		cloudProvider.Reset()
		recorder.Reset()
		cluster = state.NewCluster(env.Clock, env.Client, cloudProvider)
		clusterCost := cost.NewClusterCost(ctx, cloudProvider, env.Client)
		nodeStateController = informer.NewNodeController(env.Client, cluster)
		nodeClaimStateController = informer.NewNodeClaimController(env.Client, cloudProvider, cluster, clusterCost)
		controller = reservationmigration.NewController(env.Clock, env.Client, cluster, cloudProvider, recorder)

		instanceType = fake.NewInstanceType("reservable-instance-type")
		instanceType.Offerings = append(instanceType.Offerings, reservedOffering(reservationID, 2, true))
		cloudProvider.InstanceTypes = []*cloudprovider.InstanceType{instanceType}

		nodePool = test.NodePool(v1.NodePool{Spec: v1.NodePoolSpec{Template: v1.NodeClaimTemplate{Spec: v1.NodeClaimTemplateSpec{
			Requirements: []v1.NodeSelectorRequirementWithMinValues{{
				Key: v1.CapacityTypeLabelKey, Operator: corev1.NodeSelectorOpIn, Values: []string{v1.CapacityTypeOnDemand, v1.CapacityTypeReserved},
			}},
		}}}})
		nodeClaim, node = newNodeClaimAndNode()
		ExpectApplied(ctx, env.Client, nodePool, nodeClaim, node)
		ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController, []*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})
		nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
		node = ExpectExists(ctx, env.Client, node)
	})
	AfterEach(func() {
		ExpectCleanedUp(ctx, env.Client)
	})

	It("should migrate a compatible on-demand node into the reservation in place", func() {
		pod := test.Pod()
		ExpectApplied(ctx, env.Client, pod)
		ExpectManualBinding(ctx, env.Client, pod, node)

		reconcile([]*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})
		expectFenced(node, true)
		Expect(cloudProvider.MigrateToReservationCalls).To(BeEmpty())

		settle()
		reconcile([]*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})
		Expect(cloudProvider.MigrateToReservationCalls).To(HaveLen(1))
		Expect(cloudProvider.MigrateToReservationCalls[0].Offering.ReservationID()).To(Equal(reservationID))
		Expect(cloudProvider.MigrateToReservationCalls[0].NodeClaim.Name).To(Equal(nodeClaim.Name))
		expectReserved(nodeClaim, node, reservationID)
		ExpectExists(ctx, env.Client, pod)
		Expect(recorder.Calls(events.ReservationMigrated)).To(Equal(1))
	})
	It("should keep the fence and commitment until the cloud provider reports the instance in the reservation", func() {
		cloudProvider.SkipMigrationRelabel = true
		reconcile([]*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})
		settle()
		reconcile([]*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})
		Expect(cloudProvider.MigrateToReservationCalls).To(HaveLen(1))
		expectFenced(node, true)
		Expect(ExpectExists(ctx, env.Client, nodeClaim).Annotations).To(HaveKeyWithValue(v1.ReservationMigrationTargetAnnotationKey, reservationID))
		Expect(ExpectExists(ctx, env.Client, nodeClaim).Labels).To(HaveKeyWithValue(v1.CapacityTypeLabelKey, v1.CapacityTypeOnDemand))

		// Polling before the reissue interval doesn't request the migration again
		reconcile([]*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})
		Expect(cloudProvider.MigrateToReservationCalls).To(HaveLen(1))
		expectFenced(node, true)

		// Once the provider observes the move, the node is relabeled and unfenced
		created := cloudProvider.CreatedNodeClaims[nodeClaim.Status.ProviderID]
		created.Labels[v1.CapacityTypeLabelKey] = v1.CapacityTypeReserved
		created.Labels[v1alpha1.LabelReservationID] = reservationID
		reconcile([]*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})
		Expect(cloudProvider.MigrateToReservationCalls).To(HaveLen(1))
		expectReserved(nodeClaim, node, reservationID)
	})
	It("should re-request a committed migration after the reissue interval", func() {
		cloudProvider.SkipMigrationRelabel = true
		reconcile([]*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})
		settle()
		reconcile([]*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})
		Expect(cloudProvider.MigrateToReservationCalls).To(HaveLen(1))
		env.Clock.Step(reservationmigration.ReissueInterval)
		reconcile([]*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})
		Expect(cloudProvider.MigrateToReservationCalls).To(HaveLen(2))
		expectFenced(node, true)
	})
	It("should resume a committed migration after a restart", func() {
		node.Spec.Taints = append(node.Spec.Taints, v1.ReservationMigrationNoScheduleTaint)
		nodeClaim.Annotations = lo.Assign(nodeClaim.Annotations, map[string]string{v1.ReservationMigrationTargetAnnotationKey: reservationID})
		ExpectApplied(ctx, env.Client, nodeClaim, node)
		reconcile([]*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})
		Expect(cloudProvider.MigrateToReservationCalls).To(HaveLen(1))
		expectReserved(nodeClaim, node, reservationID)
	})
	It("should finish a committed migration without requesting it again if the instance already moved", func() {
		node.Spec.Taints = append(node.Spec.Taints, v1.ReservationMigrationNoScheduleTaint)
		nodeClaim.Annotations = lo.Assign(nodeClaim.Annotations, map[string]string{v1.ReservationMigrationTargetAnnotationKey: reservationID})
		ExpectApplied(ctx, env.Client, nodeClaim, node)
		created := cloudProvider.CreatedNodeClaims[nodeClaim.Status.ProviderID]
		created.Labels[v1.CapacityTypeLabelKey] = v1.CapacityTypeReserved
		created.Labels[v1alpha1.LabelReservationID] = reservationID
		reconcile([]*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})
		Expect(cloudProvider.MigrateToReservationCalls).To(BeEmpty())
		expectReserved(nodeClaim, node, reservationID)
	})
	It("should keep migrating other nodes when one in-flight migration errors", func() {
		other, otherNode := newNodeClaimAndNode()
		ExpectApplied(ctx, env.Client, other, otherNode)
		ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController, []*corev1.Node{otherNode}, []*v1.NodeClaim{other})
		other = ExpectExists(ctx, env.Client, other)
		otherNode = ExpectExists(ctx, env.Client, otherNode)
		for _, n := range []*corev1.Node{node, otherNode} {
			n.Spec.Taints = append(n.Spec.Taints, v1.ReservationMigrationNoScheduleTaint)
		}
		for _, nc := range []*v1.NodeClaim{nodeClaim, other} {
			nc.Annotations = lo.Assign(nc.Annotations, map[string]string{v1.ReservationMigrationTargetAnnotationKey: reservationID})
		}
		ExpectApplied(ctx, env.Client, nodeClaim, node, other, otherNode)
		nodes, nodeClaims := []*corev1.Node{node, otherNode}, []*v1.NodeClaim{nodeClaim, other}
		expectStateUpdated(nodes, nodeClaims)

		// The first cloud provider Get fails, the other migration still completes
		cloudProvider.NextGetErr = fmt.Errorf("throttled")
		_, err := singleton.AsReconciler(controller).Reconcile(ctx, crreconcile.Request{})
		Expect(err).To(HaveOccurred())
		reserved := lo.Filter(nodeClaims, func(nc *v1.NodeClaim, _ int) bool {
			return ExpectExists(ctx, env.Client, nc).Labels[v1.CapacityTypeLabelKey] == v1.CapacityTypeReserved
		})
		Expect(reserved).To(HaveLen(1))

		reconcile(nodes, nodeClaims)
		expectReserved(nodeClaim, node, reservationID)
		expectReserved(other, otherNode, reservationID)
	})
	It("should finish a committed migration after the feature gate is disabled", func() {
		node.Spec.Taints = append(node.Spec.Taints, v1.ReservationMigrationNoScheduleTaint)
		nodeClaim.Annotations = lo.Assign(nodeClaim.Annotations, map[string]string{v1.ReservationMigrationTargetAnnotationKey: reservationID})
		ExpectApplied(ctx, env.Client, nodeClaim, node)
		ctx = migrationOptions(false)
		reconcile([]*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})
		expectReserved(nodeClaim, node, reservationID)
	})
	It("should lift an uncommitted fence after the feature gate is disabled", func() {
		reconcile([]*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})
		expectFenced(node, true)
		ctx = migrationOptions(false)
		settle()
		reconcile([]*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})
		expectFenced(node, false)
		Expect(cloudProvider.MigrateToReservationCalls).To(BeEmpty())
		expectOnDemand(nodeClaim, node)
	})
	It("should re-evaluate a stale fence left by a restart once the node settles", func() {
		// A fence without a commitment, e.g. from a controller that restarted before committing, is re-evaluated after
		// the settle window from when it was first observed.
		node.Spec.Taints = append(node.Spec.Taints, v1.ReservationMigrationNoScheduleTaint)
		ExpectApplied(ctx, env.Client, node)
		reconcile([]*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})
		expectFenced(node, true)
		Expect(cloudProvider.MigrateToReservationCalls).To(BeEmpty())
		settle()
		reconcile([]*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})
		expectReserved(nodeClaim, node, reservationID)
	})
	It("should not migrate when the feature gate is disabled", func() {
		ctx = migrationOptions(false)
		reconcile([]*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})
		expectFenced(node, false)
	})
	It("should not migrate when ReservedCapacity is disabled", func() {
		ctx = options.ToContext(ctx, test.Options(test.OptionsFields{FeatureGates: test.FeatureGates{
			ReservedCapacity:          new(false),
			ReservedCapacityMigration: new(true),
		}}))
		reconcile([]*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})
		expectFenced(node, false)
	})
	It("should abandon the migration if an incompatible pod binds while the node settles", func() {
		reconcile([]*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})
		expectFenced(node, true)
		pod := test.Pod(test.PodOptions{NodeSelector: map[string]string{v1.CapacityTypeLabelKey: v1.CapacityTypeOnDemand}})
		ExpectApplied(ctx, env.Client, pod)
		ExpectManualBinding(ctx, env.Client, pod, node)
		settle()
		reconcile([]*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})
		expectFenced(node, false)
		Expect(cloudProvider.MigrateToReservationCalls).To(BeEmpty())
		expectOnDemand(nodeClaim, node)

		// The NodeClaim is backed off, so it isn't fenced again right away even once the pod is gone
		ExpectDeleted(ctx, env.Client, pod)
		reconcile([]*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})
		expectFenced(node, false)
		env.Clock.Step(reservationmigration.FailureBackoff)
		reconcile([]*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})
		expectFenced(node, true)
	})
	It("should abandon the migration and back off when the cloud provider rejects it", func() {
		reconcile([]*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})
		settle()
		cloudProvider.NextMigrateToReservationErr = cloudprovider.NewReservationMigrationRejectedError(fmt.Errorf("reservation is full"))
		reconcile([]*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})
		expectFenced(node, false)
		expectOnDemand(nodeClaim, node)
		Expect(recorder.Calls(events.ReservationMigrationFailed)).To(Equal(1))
		reconcile([]*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})
		expectFenced(node, false)
	})
	It("should finish the migration when the cloud provider fails the request but moved the instance", func() {
		reconcile([]*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})
		settle()
		cloudProvider.NextMigrateToReservationErrAfterMove = fmt.Errorf("request timed out")
		reconcile([]*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})
		expectReserved(nodeClaim, node, reservationID)
		Expect(recorder.Calls(events.ReservationMigrationFailed)).To(Equal(0))
	})
	It("should retry a failed request whose outcome is unknown, and abandon it if it's never accepted", func() {
		reconcile([]*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})
		settle()
		cloudProvider.NextMigrateToReservationErr = fmt.Errorf("request timed out")
		reconcile([]*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})
		expectFenced(node, true)
		Expect(ExpectExists(ctx, env.Client, nodeClaim).Annotations).To(HaveKeyWithValue(v1.ReservationMigrationTargetAnnotationKey, reservationID))
		Expect(recorder.Calls(events.ReservationMigrationFailed)).To(Equal(0))

		// Retried after the reissue interval, still failing
		env.Clock.Step(reservationmigration.ReissueInterval)
		cloudProvider.NextMigrateToReservationErr = fmt.Errorf("request timed out")
		reconcile([]*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})
		expectFenced(node, true)

		// Never accepted within the observation timeout, so it is abandoned
		env.Clock.Step(reservationmigration.ObservationTimeout)
		reconcile([]*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})
		expectFenced(node, false)
		expectOnDemand(nodeClaim, node)
		Expect(recorder.Calls(events.ReservationMigrationFailed)).To(Equal(1))
		Expect(cloudProvider.MigrateToReservationCalls).To(BeEmpty())
	})
	It("should never abandon an accepted migration on a failed re-request", func() {
		cloudProvider.SkipMigrationRelabel = true
		reconcile([]*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})
		settle()
		reconcile([]*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})
		Expect(cloudProvider.MigrateToReservationCalls).To(HaveLen(1))

		env.Clock.Step(reservationmigration.ReissueInterval)
		cloudProvider.NextMigrateToReservationErr = cloudprovider.NewReservationMigrationRejectedError(fmt.Errorf("throttled"))
		reconcile([]*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})
		expectFenced(node, true)
		Expect(ExpectExists(ctx, env.Client, nodeClaim).Annotations).To(HaveKeyWithValue(v1.ReservationMigrationTargetAnnotationKey, reservationID))

		// The reservation stops being offered, the instance may still move
		instanceType.Offerings = instanceType.Offerings[:len(instanceType.Offerings)-1]
		env.Clock.Step(reservationmigration.ReissueInterval)
		reconcile([]*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})
		expectFenced(node, true)

		created := cloudProvider.CreatedNodeClaims[nodeClaim.Status.ProviderID]
		created.Labels[v1.CapacityTypeLabelKey] = v1.CapacityTypeReserved
		created.Labels[v1alpha1.LabelReservationID] = reservationID
		reconcile([]*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})
		expectReserved(nodeClaim, node, reservationID)
	})
	It("should abandon an accepted migration the cloud provider never reports", func() {
		cloudProvider.SkipMigrationRelabel = true
		reconcile([]*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})
		settle()
		reconcile([]*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})
		env.Clock.Step(reservationmigration.ObservationTimeout + reservationmigration.ReissueInterval)
		reconcile([]*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})
		expectFenced(node, true)
		env.Clock.Step(reservationmigration.AbandonTimeout)
		reconcile([]*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})
		expectFenced(node, false)
		expectOnDemand(nodeClaim, node)

		// The provider may still move the instance, e.g. once a full reservation frees up, so it isn't requested again
		env.Clock.Step(reservationmigration.FailureBackoff)
		reconcile([]*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})
		expectFenced(node, false)
	})
	It("should abandon a committed migration if an incompatible pod binds before the request is accepted", func() {
		reconcile([]*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})
		settle()
		cloudProvider.NextMigrateToReservationErr = fmt.Errorf("request timed out")
		reconcile([]*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})
		expectFenced(node, true)

		// e.g. a pod that tolerates the fence, or binds through spec.nodeName
		pod := test.Pod(test.PodOptions{
			NodeSelector: map[string]string{v1.CapacityTypeLabelKey: v1.CapacityTypeOnDemand},
			Tolerations:  []corev1.Toleration{{Operator: corev1.TolerationOpExists}},
		})
		ExpectApplied(ctx, env.Client, pod)
		ExpectManualBinding(ctx, env.Client, pod, node)
		env.Clock.Step(reservationmigration.ReissueInterval)
		reconcile([]*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})
		Expect(cloudProvider.MigrateToReservationCalls).To(BeEmpty())
		expectFenced(node, false)
		expectOnDemand(nodeClaim, node)
	})
	It("should re-apply the fence while a migration is committed", func() {
		cloudProvider.SkipMigrationRelabel = true
		reconcile([]*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})
		settle()
		reconcile([]*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})
		n := ExpectExists(ctx, env.Client, node)
		n.Spec.Taints = nil
		ExpectApplied(ctx, env.Client, n)
		expectFenced(node, false)
		reconcile([]*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})
		expectFenced(node, true)
	})
	It("should relabel the node for the reservation the cloud provider reports, even if another was planned", func() {
		node.Spec.Taints = append(node.Spec.Taints, v1.ReservationMigrationNoScheduleTaint)
		nodeClaim.Annotations = lo.Assign(nodeClaim.Annotations, map[string]string{v1.ReservationMigrationTargetAnnotationKey: reservationID})
		ExpectApplied(ctx, env.Client, nodeClaim, node)
		created := cloudProvider.CreatedNodeClaims[nodeClaim.Status.ProviderID]
		created.Labels[v1.CapacityTypeLabelKey] = v1.CapacityTypeReserved
		created.Labels[v1alpha1.LabelReservationID] = "r-earlier"
		reconcile([]*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})
		Expect(cloudProvider.MigrateToReservationCalls).To(BeEmpty())
		expectReserved(nodeClaim, node, "r-earlier")
		Expect(recorder.Calls(events.ReservationMigrationMismatched)).To(Equal(1))
	})
	It("should let repair, but not other disruption, ignore a migrating node", func() {
		reconcile([]*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})
		expectStateUpdated([]*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})
		err := ExpectStateNodeExists(cluster, node).ValidateNodeDisruptable(env.Clock)
		Expect(state.IsNodeMigratingError(err)).To(BeTrue())
		Expect(state.IgnoreNodeMigratingError(err)).To(Succeed())
	})
	It("should stop migrating when the cloud provider doesn't implement it", func() {
		reconcile([]*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})
		settle()
		cloudProvider.NextMigrateToReservationErr = cloudprovider.NewReservationMigrationNotImplementedError()
		reconcile([]*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})
		expectFenced(node, false)
		expectOnDemand(nodeClaim, node)

		other, otherNode := newNodeClaimAndNode()
		ExpectApplied(ctx, env.Client, other, otherNode)
		ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController, []*corev1.Node{otherNode}, []*v1.NodeClaim{other})
		reconcile([]*corev1.Node{node, otherNode}, []*v1.NodeClaim{nodeClaim, other})
		expectFenced(otherNode, false)
	})
	It("should never migrate a node again once the cloud provider reports it can't be migrated", func() {
		reconcile([]*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})
		settle()
		cloudProvider.NextMigrateToReservationErr = cloudprovider.NewNodeClaimNotMigratableError(fmt.Errorf("instance launched with an open capacity reservation preference"))
		reconcile([]*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})
		expectFenced(node, false)
		expectOnDemand(nodeClaim, node)
		env.Clock.Step(reservationmigration.FailureBackoff)
		reconcile([]*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})
		expectFenced(node, false)

		// Other nodes still migrate
		other, otherNode := newNodeClaimAndNode()
		ExpectApplied(ctx, env.Client, other, otherNode)
		ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController, []*corev1.Node{otherNode}, []*v1.NodeClaim{other})
		reconcile([]*corev1.Node{node, otherNode}, []*v1.NodeClaim{nodeClaim, other})
		expectFenced(otherNode, true)
	})
	It("should not migrate more nodes into a reservation than it has free capacity", func() {
		instanceType.Offerings[len(instanceType.Offerings)-1].ReservationCapacity = 1
		other, otherNode := newNodeClaimAndNode()
		ExpectApplied(ctx, env.Client, other, otherNode)
		ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController, []*corev1.Node{otherNode}, []*v1.NodeClaim{other})
		nodes, nodeClaims := []*corev1.Node{node, otherNode}, []*v1.NodeClaim{nodeClaim, other}

		reconcile(nodes, nodeClaims)
		fenced := lo.Filter(nodes, func(n *corev1.Node, _ int) bool {
			return lo.ContainsBy(ExpectExists(ctx, env.Client, n).Spec.Taints, func(t corev1.Taint) bool { return t.MatchTaint(&v1.ReservationMigrationNoScheduleTaint) })
		})
		Expect(fenced).To(HaveLen(1))

		// The in-flight migration keeps holding the slot, even across passes
		reconcile(nodes, nodeClaims)
		settle()
		cloudProvider.SkipMigrationRelabel = true
		reconcile(nodes, nodeClaims)
		Expect(cloudProvider.MigrateToReservationCalls).To(HaveLen(1))
		fenced = lo.Filter(nodes, func(n *corev1.Node, _ int) bool {
			return lo.ContainsBy(ExpectExists(ctx, env.Client, n).Spec.Taints, func(t corev1.Taint) bool { return t.MatchTaint(&v1.ReservationMigrationNoScheduleTaint) })
		})
		Expect(fenced).To(HaveLen(1))
	})
	It("should prefer the cheapest compatible reservation", func() {
		expensive := reservedOffering("r-0", 1, true)
		expensive.Price = 0.1
		instanceType.Offerings = append(instanceType.Offerings, expensive)
		reconcile([]*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})
		settle()
		reconcile([]*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})
		expectReserved(nodeClaim, node, reservationID)
	})
	It("should migrate into a reservation a pod's node selector requires", func() {
		// The required reservation is the more expensive one, so it's only chosen because of the pod
		required := reservedOffering("r-0", 1, true)
		required.Price = 0.1
		instanceType.Offerings = append(instanceType.Offerings, required)
		pod := test.Pod(test.PodOptions{NodeSelector: map[string]string{v1alpha1.LabelReservationID: "r-0"}})
		ExpectApplied(ctx, env.Client, pod)
		ExpectManualBinding(ctx, env.Client, pod, node)
		// The pod can't be bound to the node today, but the check only cares about the labels the node will carry.
		reconcile([]*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})
		settle()
		reconcile([]*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})
		expectReserved(nodeClaim, node, "r-0")
	})

	DescribeTable("should not migrate",
		func(setup func()) {
			setup()
			reconcile([]*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})
			expectFenced(node, false)
			settle()
			reconcile([]*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})
			Expect(cloudProvider.MigrateToReservationCalls).To(BeEmpty())
			expectOnDemand(nodeClaim, node)
		},
		Entry("when the reservation can't be migrated into", func() {
			instanceType.Offerings[len(instanceType.Offerings)-1].Migratable = false
		}),
		Entry("when the reservation is full", func() {
			instanceType.Offerings[len(instanceType.Offerings)-1].ReservationCapacity = 0
		}),
		Entry("when the reservation is unavailable", func() {
			instanceType.Offerings[len(instanceType.Offerings)-1].Available = false
		}),
		Entry("when the reservation is incompatible with the NodeClass", func() {
			instanceType.Offerings[len(instanceType.Offerings)-1].Compatible = false
		}),
		Entry("when the reservation is in another zone", func() {
			instanceType.Offerings[len(instanceType.Offerings)-1].Requirements[corev1.LabelTopologyZone] = scheduling.NewRequirement(corev1.LabelTopologyZone, corev1.NodeSelectorOpIn, "test-zone-2")
		}),
		Entry("when the reservation contradicts another label of the node", func() {
			nodeClaim.Labels["test-zone-id"] = "zone-id-1"
			node.Labels["test-zone-id"] = "zone-id-1"
			ExpectApplied(ctx, env.Client, nodeClaim, node)
			instanceType.Offerings[len(instanceType.Offerings)-1].Requirements.Add(scheduling.NewRequirement("test-zone-id", corev1.NodeSelectorOpIn, "zone-id-2"))
		}),
		Entry("when the NodePool doesn't allow reserved capacity", func() {
			nodePool.Spec.Template.Spec.Requirements = []v1.NodeSelectorRequirementWithMinValues{{
				Key: v1.CapacityTypeLabelKey, Operator: corev1.NodeSelectorOpIn, Values: []string{v1.CapacityTypeOnDemand},
			}}
			ExpectApplied(ctx, env.Client, nodePool)
		}),
		Entry("when the NodePool excludes the reservation", func() {
			nodePool.Spec.Template.Spec.Requirements = append(nodePool.Spec.Template.Spec.Requirements, v1.NodeSelectorRequirementWithMinValues{
				Key: v1alpha1.LabelReservationID, Operator: corev1.NodeSelectorOpNotIn, Values: []string{reservationID},
			})
			ExpectApplied(ctx, env.Client, nodePool)
		}),
		Entry("when a pod selects on-demand capacity", func() {
			pod := test.Pod(test.PodOptions{NodeSelector: map[string]string{v1.CapacityTypeLabelKey: v1.CapacityTypeOnDemand}})
			ExpectApplied(ctx, env.Client, pod)
			ExpectManualBinding(ctx, env.Client, pod, node)
		}),
		Entry("when a pod requires nodes outside of reservations", func() {
			pod := test.Pod(test.PodOptions{NodeRequirements: []corev1.NodeSelectorRequirement{{
				Key: v1alpha1.LabelReservationID, Operator: corev1.NodeSelectorOpDoesNotExist,
			}}})
			ExpectApplied(ctx, env.Client, pod)
			ExpectManualBinding(ctx, env.Client, pod, node)
		}),
		Entry("when a pod requires another reservation", func() {
			pod := test.Pod(test.PodOptions{NodeRequirements: []corev1.NodeSelectorRequirement{{
				Key: v1alpha1.LabelReservationID, Operator: corev1.NodeSelectorOpIn, Values: []string{"r-other"},
			}}})
			ExpectApplied(ctx, env.Client, pod)
			ExpectManualBinding(ctx, env.Client, pod, node)
		}),
		Entry("when a DaemonSet would newly select the node", func() {
			ExpectApplied(ctx, env.Client, test.DaemonSet(test.DaemonSetOptions{PodOptions: test.PodOptions{
				NodeSelector: map[string]string{v1.CapacityTypeLabelKey: v1.CapacityTypeReserved},
			}}))
		}),
		Entry("when a pod on the node spreads across capacity types", func() {
			pod := test.Pod(test.PodOptions{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "spread"}},
				TopologySpreadConstraints: []corev1.TopologySpreadConstraint{{
					MaxSkew: 1, TopologyKey: v1.CapacityTypeLabelKey, WhenUnsatisfiable: corev1.DoNotSchedule,
					LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "spread"}},
				}},
			})
			ExpectApplied(ctx, env.Client, pod)
			ExpectManualBinding(ctx, env.Client, pod, node)
		}),
		Entry("when a pod on the node is anti-affine by capacity type to pods elsewhere", func() {
			pod := test.Pod(test.PodOptions{PodAntiRequirements: []corev1.PodAffinityTerm{{
				TopologyKey:   v1.CapacityTypeLabelKey,
				LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "elsewhere"}},
			}}})
			ExpectApplied(ctx, env.Client, pod)
			ExpectManualBinding(ctx, env.Client, pod, node)
		}),
		Entry("when a pod elsewhere is anti-affine to a pod on the node by capacity type", func() {
			pod := test.Pod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "target"}}})
			ExpectApplied(ctx, env.Client, pod)
			ExpectManualBinding(ctx, env.Client, pod, node)
			other, otherNode := newNodeClaimAndNode()
			otherNode.Labels[v1.CapacityTypeLabelKey] = v1.CapacityTypeSpot
			other.Labels[v1.CapacityTypeLabelKey] = v1.CapacityTypeSpot
			ExpectApplied(ctx, env.Client, other, otherNode)
			ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController, []*corev1.Node{otherNode}, []*v1.NodeClaim{other})
			antiAffine := test.Pod(test.PodOptions{PodAntiRequirements: []corev1.PodAffinityTerm{{
				TopologyKey:   v1.CapacityTypeLabelKey,
				LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "target"}},
			}}})
			ExpectApplied(ctx, env.Client, antiAffine)
			ExpectManualBinding(ctx, env.Client, antiAffine, otherNode)
		}),
		Entry("when the node opts out of disruption", func() {
			node.Annotations = lo.Assign(node.Annotations, map[string]string{v1.DoNotDisruptAnnotationKey: "true"})
			ExpectApplied(ctx, env.Client, node)
		}),
		Entry("when the node is disrupted", func() {
			node.Spec.Taints = append(node.Spec.Taints, v1.DisruptedNoScheduleTaint)
			ExpectApplied(ctx, env.Client, node)
		}),
		Entry("when the node is marked for deletion", func() {
			expectStateUpdated([]*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})
			cluster.MarkForDeletion(nodeClaim.Status.ProviderID)
		}),
	)
	It("should not migrate a node that isn't on-demand", func() {
		nodeClaim.Labels[v1.CapacityTypeLabelKey] = v1.CapacityTypeSpot
		node.Labels[v1.CapacityTypeLabelKey] = v1.CapacityTypeSpot
		ExpectApplied(ctx, env.Client, nodeClaim, node)
		reconcile([]*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})
		expectFenced(node, false)
		settle()
		reconcile([]*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})
		Expect(cloudProvider.MigrateToReservationCalls).To(BeEmpty())
		Expect(ExpectExists(ctx, env.Client, nodeClaim).Labels).To(HaveKeyWithValue(v1.CapacityTypeLabelKey, v1.CapacityTypeSpot))
	})

	It("should not offer a migrating node for disruption", func() {
		reconcile([]*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})
		expectStateUpdated([]*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})
		Expect(ExpectStateNodeExists(cluster, node).ValidateNodeDisruptable(env.Clock)).To(MatchError(ContainSubstring("migrating into a capacity reservation")))
	})
	It("should leave DaemonSets that already select the node alone", func() {
		// Selected by on-demand and reserved nodes alike
		ExpectApplied(ctx, env.Client, test.DaemonSet(test.DaemonSetOptions{PodOptions: test.PodOptions{
			NodeRequirements: []corev1.NodeSelectorRequirement{{
				Key: v1.CapacityTypeLabelKey, Operator: corev1.NodeSelectorOpIn, Values: []string{v1.CapacityTypeOnDemand, v1.CapacityTypeReserved},
			}},
		}}))
		// Selected by neither
		ExpectApplied(ctx, env.Client, test.DaemonSet(test.DaemonSetOptions{PodOptions: test.PodOptions{
			NodeSelector: map[string]string{v1.CapacityTypeLabelKey: v1.CapacityTypeSpot},
		}}))
		reconcile([]*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})
		settle()
		reconcile([]*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})
		expectReserved(nodeClaim, node, reservationID)
	})
})
