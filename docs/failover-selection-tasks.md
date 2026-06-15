# Failover Selection Tasks

This tracks the post-`v0.1.0` failover selection work discussed after the
initial release tag.

## Implemented

- Add a PVC annotation for preferred failover order:
  `simple-volume.shipstuff.io/failover-node-priority`.
- Filter failover candidates to Ready, schedulable nodes with ready agents,
  healthy replica freshness, and `lastSuccessfulSync` within
  `failover-max-staleness`.
- Walk the preferred node list first and pick the first eligible replica.
- Fall back to freshest eligible replica when no preferred node qualifies.
- Maintain a per-volume candidate label on fresh eligible replica nodes:
  `simple-volume.shipstuff.io/<namespace>.<claim>-candidate=true`.
- Remove candidate labels from offline, stale, unavailable, or
  resource-ineligible nodes during reconciliation.
- Add a conservative CPU/memory resource-fit precheck using workload pod
  requests and node allocatable capacity.
- Keep PV/PVC annotations as the status source of truth and keep CSI as the
  final mount authorization guard.

## Still Future

- Let workloads opt into candidate-label scheduling so Kubernetes can choose
  among fresh replicas by normal resource placement.
- If a replacement pod remains Pending after promotion, retry another eligible
  replica instead of relying on CSI mount failure as the normal selection
  mechanism.
- Consider a mutating admission webhook only if explicit workload selectors
  become too burdensome across real workloads.
