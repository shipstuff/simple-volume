# Simple Volume V0 Plan

## Status

V0 is complete for the initial `v0.1.0` release boundary. It is still under
real-workload validation, but the first usable shape is stable: dynamic CSI
volumes, local storage pools, watch-driven async replication, freshness-gated
failover, active-node scheduling labels, and backup-before-restore for returning
old active nodes.

The v0 PV size is not enforced as a disk quota. Requested capacity is recorded
for Kubernetes API shape, placement decisions, and operator visibility, similar
to `local-path`; actual usage is bounded by the backing local pool unless the
host filesystem enforces its own limits.

## Summary

- Build shipstuff/simple-volume as a Kubernetes-native async replicated local-volume system with full dynamic provisioning.
- Apps use normal PVCs; Kubernetes sees CSI volumes; app charts never mount raw hostPath.
- Custom code owns only the async-replica policy gap: provisioning, freshness, promotion, scheduling signals, and local replica
    lifecycle.

- Byte replication runs outside CSI in a node-agent DaemonSet using existing tools like rsync/rclone.
- No production Windrose/Enshrouded adoption in V0; ship disposable demo workloads first.

## Public API And Kubernetes Model

- API group: storage.shipstuff.io/v1alpha1.
- Add two CRDs with matching spec/status where practical:
      - SimpleVolume: namespaced.
      - ClusterSimpleVolume: cluster-scoped.

- Add a CSI StorageClass such as simple-volume, supporting dynamic PVC provisioning.
- Dynamically provisioned PVs use CSI, not local-path PVs:
      - spec.csi.driver: simple-volume.shipstuff.io
      - spec.csi.volumeHandle: <logical volume id>

- PVCs remain app-facing:
      - App requests storageClassName: simple-volume.
      - Controller creates/tracks the backing SimpleVolume object and CSI PV.

- Helm chart config defines allowed backing pools:
      - pool name
      - host backing path
      - default replica count or policy
      - agent scheduling via nodeSelector, affinity, and tolerations.

- Eligible nodes default to healthy node-agent pods for the requested pool.
- Per-volume node restrictions are optional and additive, via selectors or explicit constraints, not required for V0.

## Implementation

- Scaffold a Go repo with controller-runtime, CSI components, Helm chart, examples, CI, GHCR image publishing, OCI chart
    publishing, and scripts/release.sh following the existing Shipstuff standalone-controller release pattern.

- Controller:
      - Watches PVC/PV/CRD/node-agent state.
      - Provisions logical volumes from PVCs.
      - Chooses initial active node and replica targets from healthy agents in the requested pool.
      - Computes per-replica freshness from sync reports.
      - Labels nodes per volume for scheduler/topology hints.
      - Owns promotion state, fencing conditions, Events, and metrics.
      - Supports opt-in automatic promotion with per-volume grace/deadline settings.

- CSI driver:
      - Implements the minimal dynamic provisioning and node mount path.
      - CreateVolume creates/claims the logical volume object and returns a stable volumeHandle.
      - NodePublishVolume validates that the local node is authorized for the volume, then bind-mounts the node-local replica path.
      - NodeUnpublishVolume unmounts.
      - CSI does not replicate bytes and does not decide promotion.

- Node-agent DaemonSet:
      - Runs on storage-capable nodes selected by Helm values.
      - Mounts chart-defined backing pool paths via hostPath.
      - Creates/removes backing directories only inside configured pools.
      - Sets ownership/permissions.
      - Runs rsync/rclone replication between agents over Kubernetes networking.
      - Reports heartbeat, capacity, replica generation, sync time, bytes, duration, and errors.

- Replication:
      - Controller schedules desired syncs; agents execute them.
      - Use Kubernetes Service/Pod networking only, not SSH/WireGuard node shortcuts.
      - Protect agent sync endpoints with Kubernetes Secret bearer tokens and NetworkPolicy.
      - Replication remains async and single-writer.

## Promotion And Scheduling

- Promotion is controller-owned cluster policy:
      - old active unavailable or explicitly demoted
      - per-volume notReadyGracePeriod elapsed
      - target replica within maxStaleness
      - target agent healthy
      - old writer marked demoted/stale in API state
      - returning old active is treated as stale local state, not as the leader

- Returning-node restore:
      - promoted node remains the source of truth
      - old active rejoins as a replica target only
      - before restore, the node agent moves its existing local volume into a timestamped `.simple-volume-backups/` path
      - restore then overwrites the live replica path from the current leader
      - automatic move-back is out of scope; planned move-back must be explicit

- Auto-promotion is opt-in per volume and controlled by per-volume grace/deadline fields.
- Workload movement uses normal Kubernetes scheduling:
      - controller records the promoted active node in volume status/annotations
      - controller moves a per-volume active label to the promoted node
      - workloads select the stable active label rather than a concrete hostname
      - controller removes stale PVC selected-node hints after failover
      - CSI refuses mounts on unauthorized nodes
      - pods reschedule naturally after node failure or eviction

- Optional workloadRef can be added for V0 demos to delete/restart stuck pods and replace old hard hostname node selectors
    with the stable active-label selector after promotion; production use can remain Kubernetes-eviction-driven until validated.

## Test Plan

- Unit tests:
      - dynamic provisioning decisions
      - eligible node discovery from healthy agents
      - pool/path validation
      - freshness calculation
      - promotion blocking and selection
      - CSI mount authorization
      - label generation

- Agent tests:
      - backing directory creation under allowed pools only
      - rsync/rclone command construction
      - token auth
      - sync status reporting

- Integration tests:
      - create PVC with simple-volume StorageClass
      - verify CSI PV and logical volume creation
      - simulate two healthy agents
      - sync demo data
      - mark active unavailable
      - promote fresh replica
      - verify CSI mount is allowed only on promoted node

- Packaging checks:
      - go test ./...
      - Helm lint/template
      - shellcheck scripts
      - image build
      - demo chart render

## Assumptions

- V0 is single-writer async replication only.
- No multi-writer semantics, no synchronous writes, and no live Postgres file replication.
- CSI is a thin Kubernetes volume boundary, not the replication engine.
- Node-agent DaemonSet placement defines the default storage-capable node set.
- Production workload adoption requires a later explicit rollout plan, restore drill, and failure-mode test.

## Next Revision

- Add a failover node priority annotation or spec field.
- Select the first preferred node that is a healthy, fresh replica on a Ready
  and schedulable node.
- Fall back to the current freshest-replica behavior if no preferred node is
  eligible.
- Add a conservative resource-fit precheck based on pod requests and node
  allocatable capacity. This should catch obvious bad promotions without trying
  to perfectly duplicate scheduler behavior.
- Keep Kubernetes as the final scheduling authority. If the replacement pod
  stays Pending after promotion, the controller can retry another eligible
  replica; CSI mount failure should not be the normal promotion trigger.
