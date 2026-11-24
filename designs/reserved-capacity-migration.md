# Reserved Capacity Migration

## Background

When a NodePool allows both `reserved` and `on-demand` capacity, Karpenter launches into capacity reservations first and
falls back to on-demand once they are full ([#2132](https://github.com/kubernetes-sigs/karpenter/issues/2132)). When
reservation capacity frees up later, for example after a scale-down, the remaining on-demand nodes keep running as
on-demand. Consolidation can move them into the reservation, but only by launching replacement nodes and evicting pods,
even though the only thing that differs is billing.

Cloud providers can move running instances into reservations in place. EC2 [lets a running instance target an active
On-Demand Capacity Reservation](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/capacity-reservations-modify-instance.html)
if its capacity reservation preference is `none`, which is how the AWS provider launches on-demand instances whenever
reserved capacity is enabled.

Moving a node into a reservation changes its labels, though: `karpenter.sh/capacity-type` becomes `reserved`, and the
provider's reserved capacity labels (e.g. the reservation ID) appear. Pods, NodePools, and DaemonSets may select on those
labels, and scheduling decisions made against the old labels must keep holding.

## Proposal

Add an alpha `ReservedCapacityMigration` feature gate, a `CloudProvider.MigrateToReservation` method, and a
`nodeclaim.reservationmigration` controller that moves compatible on-demand nodes into reservations with free capacity in
place. This covers the two cases called out in the issue where some action is possible: no pod on the node is
compatible with the reservation (nothing happens), or every pod is (the node migrates). Splitting a node whose pods are
only partly compatible is out of scope.

### CloudProvider interface

```go
// MigrateToReservation moves the running instance backing the NodeClaim into the capacity reservation of the given
// reserved offering in place, without stopping, replacing, or otherwise interrupting it. ...
MigrateToReservation(ctx context.Context, nodeClaim *v1.NodeClaim, offering *Offering) error
```

- The call is idempotent, and once the instance runs in the reservation, `Get` and `List` report the NodeClaim with the
  offering's capacity type and reserved capacity labels. The provider stays the source of truth for the labels.
- `Offering.Migratable` marks reserved offerings that running instances can move into (e.g. not EC2 Capacity Blocks or
  interruptible reservations), so Karpenter never fences a node for a reservation it can't migrate into.
- Providers that can't migrate return `ReservationMigrationNotImplementedError`, which disables migration. A provider
  returns `NodeClaimNotMigratableError` for an instance that can never move while it runs (e.g. an EC2 instance launched
  with an `open` preference), and Karpenter stops trying for that NodeClaim. It returns
  `ReservationMigrationRejectedError` when it knows the request wasn't applied (e.g. the reservation is full). Any other
  error leaves it unknown whether the request was applied.

### Eligibility

An on-demand node migrates into a `Migratable`, `Available`, `Compatible` reserved offering of its instance type with
free capacity (not already claimed by another in-flight migration) when:

1. The offering is in the node's zone and agrees with every other label the node carries.
2. The NodePool's requirements are compatible with the relabeled NodeClaim, using the same check as requirements drift,
   so the migrated node isn't drifted.
3. Every non-terminal pod bound to the node matches the relabeled Node on required node affinity and node selector,
   using the kube-scheduler's own `nodeaffinity` matcher.
4. No DaemonSet that doesn't select the node today selects the relabeled node, since its new pod might not fit.
5. No bound pod has a required pod (anti-)affinity term or `DoNotSchedule` topology spread constraint whose topology key
   is a label that changes, if the pod runs on the node or the term selects a pod on the node. Unparsable selectors
   select everything, and namespace selectors select every namespace.
6. The node is initialized and isn't deleting, marked for deletion, disrupted, rebooting, nominated for pending pods, or
   annotated with `karpenter.sh/do-not-disrupt: "true"`.

The labels that change are `karpenter.sh/capacity-type`, `cloudprovider.ReservationIDLabel`, and
`cloudprovider.ReservedCapacityLabels`. Their new values come from the offering's single-valued requirements.

### Lifecycle

The controller is a singleton that reconciles every 30 seconds, or sooner while migrations are in flight. It requires
cluster state to be synced, and migrates at most 10 nodes at a time.

1. **Fence.** The Node gets a `karpenter.sh/reservation-migration:NoSchedule` taint, so no pod binds against its
   on-demand labels.
2. **Settle.** After 10 seconds, so bindings the scheduler decided before it saw the taint have landed, eligibility is
   evaluated again from scratch. If it no longer holds, the taint is removed and the NodeClaim backs off for 10 minutes.
3. **Commit.** The target reservation is recorded in the NodeClaim's `karpenter.sh/reservation-migration-target`
   annotation. The fence is re-applied on every reconcile while the migration is committed.
4. **Migrate.** Right before each request, the node's pods are checked again, and the migration is abandoned if they no
   longer fit. `MigrateToReservation` is then called:
   - Until the provider accepts the request, the migration is abandoned (and backed off) when the provider rejects it,
     reports the NodeClaim as not migratable, or doesn't accept it within 5 minutes. Errors of unknown outcome are
     retried every minute.
   - Once the provider accepts it, the instance may move at any time, so the migration is never abandoned on an error:
     the node stays fenced and the request is re-issued every minute until `Get` reports the instance in a reservation.
     A warning event is emitted after 5 minutes, and the migration is abandoned only after 30 minutes, when the
     provider's view of the instance, which the node's labels already match, is trusted.
5. **Relabel.** The NodeClaim and Node take the provider-reported values of the changing labels, then the annotation and
   the taint are removed. If the provider reports another reservation than planned, or the relabeled node no longer
   suits a bound pod (a pod that tolerates the fence or bound through `spec.nodeName` after the last check), the node
   is relabeled anyway, since its labels must match where it runs, and a warning event is emitted.

The taint and the annotation persist the phase, so a restarted controller resumes a committed migration and
re-evaluates an uncommitted fence after a fresh settle window. In-flight migrations finish even after the feature gate is
disabled.

### Interactions

- **Disruption.** `StateNode.ValidateNodeDisruptable` rejects nodes with the migration taint or annotation with a
  `NodeMigratingError`, so no voluntary disruption method acts on a node that's about to be relabeled. Repair ignores
  it, so a broken node isn't stranded; terminating the node ends the migration. The controller in turn skips disrupted
  nodes and nodes marked for deletion.
- **Cost tracking.** Cluster cost tracking re-tracks a NodeClaim whose capacity type changed in place under its new
  offering, which also covers reserved nodes demoted to on-demand.
- **Provider label sync.** Providers that sync capacity type labels from the instance, like the AWS provider's demotion
  of nodes whose reservation expired, must not undo a relabel they raced with, and should skip NodeClaims with the
  migration annotation.
- **Consolidation.** After migrating, the node is priced as reserved, so consolidation won't replace it with reserved
  capacity. Consolidation can still win the race for a node, which falls back to today's behavior.
- **Provisioning.** The migration consumes reservation capacity the provider tracks (the AWS provider decrements its
  cached availability), so later scheduling simulations see the slot as used. While fenced, the node doesn't accept
  pending pods, which may launch new capacity that consolidation later removes.

### Limitations

- Pods that tolerate the fence taint, or are bound with `spec.nodeName`, between the last check and the relabel aren't
  fenced. The node is relabeled anyway and a warning event names the pod.
- `NodeClaimNotMigratableError` and back-offs are kept in memory, so after a restart a NodeClaim that can't migrate is
  fenced once more for the settle window before the provider rejects it again.
- Downgrading to a version without this controller while a migration is in flight leaves the taint and annotation on
  the node; remove them manually.

## Alternatives Considered

- **A disruption method.** Disruption commands are built around launching replacements and deleting candidates. A
  relabel-in-place action fits neither, and would hold the disruption loop through the settle window.
- **Relabeling without a taint.** A pod that requires on-demand capacity could bind between the eligibility check and
  the relabel, ending up on a reserved node.
- **Labels derived only from the offering.** The provider reports the labels of where the instance actually runs.
  Relabeling from it means the node's labels match reality even if the provider resolved the move differently.
