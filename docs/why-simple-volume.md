# Why simple-volume Exists

`simple-volume` exists because the workloads we care about need a narrow
Kubernetes-local storage behavior that existing storage products do not provide
cleanly:

- application charts should use normal PVCs, not raw `hostPath` mounts
- hot writes should stay on node-local disk for game/server runtime performance
- replication should be async and single-writer, not a distributed filesystem
- promotion must be gated on replica freshness and workload RPO
- pod movement should use Kubernetes scheduling, not custom pod orchestration
- returning stale nodes must not overwrite the promoted source of truth

The project is intentionally not trying to become a general-purpose storage
platform. CSI is only the Kubernetes volume boundary. The controller owns
policy, and node agents own local filesystem work and rclone-based replication.

## Replication Model

For v0, a volume has one active node and zero or more replica nodes. The active
node serves the mounted PVC and watches configured durable paths. Replica agents
receive batched sync requests and pull changed files from the active node's
read-only rclone WebDAV endpoint. A scheduled full sync is still useful as a
safety net for missed events, agent restarts, or watch gaps.

```mermaid
flowchart TD
  pod["Stateful workload pod"] --> pvc["PVC\nstorageClassName: simple-volume"]
  pvc --> csi["simple-volume CSI driver\nmount boundary only"]
  csi --> active["Active node local pool\n/live/<volume>"]

  active --> watch["Active node agent\nfsnotify includePaths"]
  watch --> events["Debounced event batches"]
  events --> replica1["Replica agent A\npull changed files"]
  events --> replica2["Replica agent B\npull changed files"]

  active --> webdav["Read-only rclone WebDAV\nsource endpoint"]
  webdav --> replica1
  webdav --> replica2

  replica1 --> fresh["Freshness status\nlast sync, errors, staleness"]
  replica2 --> fresh
  fresh --> controller["simple-volume controller\npromotion policy"]

  controller --> pvcpv["PV/PVC annotations\nactive node, previous active,\nselected-node cleanup"]
  controller --> label["Per-volume active-node label"]
  label --> scheduler["Kubernetes scheduler"]
  scheduler --> pod

  failed["Pod/node failure"] --> gate["Freshness gate\nwithin max staleness?"]
  gate --> controller
  controller --> promote["Promote fresh replica"]
  promote --> label
  promote --> restart["Delete stale pod / let Kubernetes reschedule"]

  rejoin["Old active node rejoins"] --> backup["Backup stale local copy\n.simple-volume-backups/"]
  backup --> restore["Restore from promoted leader"]
  restore --> replica1
```

The default replica set is the set of healthy node-agent DaemonSet pods for the
configured storage pool. Per-volume node constraints can be added later, but
v0 should not require manually maintaining a node list for every volume.

## What This Is Not

`simple-volume` is not:

- multi-writer storage
- synchronous replication
- a distributed filesystem
- a replacement for database-native replication
- a backup system by itself
- a generic Ceph/Longhorn/OpenEBS alternative

Replication copies current state. Backups still need retention so a bad write,
delete, or corrupt save does not simply replicate everywhere.

## Why CSI At All

The abstraction is over Kubernetes PVCs, not over a storage product's internal
replication engine. CSI gives workloads a normal Kubernetes storage interface:

- apps request `storageClassName: simple-volume`
- the controller provisions a logical volume and CSI PV
- the CSI node plugin only mounts the authorized local path
- CSI refuses mounts on nodes that are not allowed to serve the active volume

Replication and promotion stay out of CSI because they are policy decisions.
That keeps the driver small and avoids hiding failover behavior inside the
mount path.

## Scheduling And Promotion

PV/PVC annotations are status and debugging signals. They are useful for
operators, but they do not reschedule pods by themselves.

For v0, promotion works through mutable node labels and normal Kubernetes pod
scheduling:

- the controller records active/previous active node annotations on the PV/PVC
- the controller removes stale PVC `volume.kubernetes.io/selected-node` hints
- the controller moves the per-volume active-node label to the promoted node
- the workload selects the stable active-node label instead of a fixed hostname
- the controller deletes stale pods when needed, then Kubernetes schedules the
  replacement pod to the currently labeled active node
- the CSI node plugin validates that the mounting node is authorized

This avoids relying on mutable PV `nodeAffinity` after a PV has already been
bound.

## Storage Options Evaluated

| Option | Decision | Reason |
| --- | --- | --- |
| VolSync | Tried as a PVC copy primitive, not selected as the main abstraction. | VolSync can copy PVC data, but it does not provide the freshness-aware promotion, fencing, active-node scheduling, or single-writer policy this project needs. |
| Longhorn | Tried as a general replicated storage layer, not selected for these workloads. | Longhorn solves a broader replicated block-storage problem. Our target is local hot writes with async recovery, not a synchronous storage platform in the hot path. |
| OpenEBS | Ruled out before POC. | LocalPV still leaves us to build promotion/freshness/rescheduling policy, while replicated OpenEBS engines add another storage platform to operate. |
| Ceph/Rook | Ruled out before POC. | Ceph solves distributed storage, but the operational footprint is too high for this use case and it moves game/runtime state into a general cluster storage layer. |
| `simple-volume` | Selected v0 direction. | It keeps the hot path local, uses PVC/CSI for Kubernetes integration, delegates byte movement to rclone/WebDAV agents, and makes freshness-gated promotion explicit. |

## Workloads That Fit

Good initial candidates:

- game saves and server config where large game binaries can be excluded
- local file-tree app state with one writer
- reconstructable artifacts that benefit from warm node-local copies
- disposable demo workloads for failover drills

Poor candidates:

- live Postgres data directories
- multi-writer shared application storage
- workloads that need zero-RPO synchronous writes
- data that needs backup retention but only has current-state replication

For game servers such as Enshrouded, the intended shape is to replicate saves
and config while excluding reconstructable downloads. That keeps the watch path
small and makes failover data movement proportional to durable state rather
than the full installed game tree.
