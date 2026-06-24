# simple-volume

`simple-volume` is a Kubernetes-native async replicated local-volume system.
Applications use normal PVCs backed by the `simple-volume.shipstuff.io` CSI
driver. The controller and node agents handle the local replica lifecycle,
freshness policy, and promotion state.

## Why

We want local-disk performance for workloads such as game servers while still
having a Kubernetes-native recovery path when a node fails. Most existing
options are overkill for that shape. VolSync is useful as a copy primitive but
does not own promotion or scheduling policy. Longhorn was the strongest tested
alternative, but it added many containers to already resource-constrained nodes.
OpenEBS and Ceph/Rook add even more storage-platform surface than this use case
needs.

`simple-volume` keeps the hot path local, uses PVC/CSI so applications do not
mount raw `hostPath`, replicates selected durable paths asynchronously, and
promotes only replicas that are fresh enough for the workload's RPO. The trade
off is intentional: this is ongoing backup plus automatic failover to the
freshest acceptable copy on another node, not synchronous HA storage. That is
lightweight, covers the fundamental failure-recovery features, and is an
acceptable fit for many single-writer workloads.

See [docs/why-simple-volume.md](docs/why-simple-volume.md) for the full design
rationale, replication model, and storage options we evaluated.

## Initial Release Scope

The `v0.2.0` release is intentionally narrow and ready for initial real
workload use with understood async-replication tradeoffs:

- dynamic PVC provisioning into logical SimpleVolumes
- chart-configured local storage pools
- node-agent DaemonSet path lifecycle and rsync/rclone execution helpers
- thin CSI node bind-mount authorization
- async single-writer promotion policy
- watch-driven rclone/WebDAV replication primitives
- requested PVC size is recorded but not enforced as a disk quota

Replication logic does not run inside CSI. CSI is the Kubernetes mount boundary;
the controller owns policy and the node agent owns local filesystem operations.
Like `local-path`, `simple-volume` does not enforce PV capacity at the
filesystem layer. The PVC request is used for Kubernetes API shape, placement
decisions, and operator visibility; workloads can still consume available space
in the backing local pool unless the host filesystem enforces its own limits.

## Replication Model

The intended replication path is watch-driven and, by default, shadow-backed:

- the active node-agent watches configured durable paths inside the active
  volume
- file events are debounced into batches
- in `shadow` consistency mode, the source agent first stages those changes into
  a local shadow tree
- replica agents receive batch sync requests and pull from the completed shadow
  tree through the active agent's read-only rclone WebDAV endpoint
- the WebDAV server uses a short directory cache so newly written files are
  visible to watch-triggered pulls
- a generation is considered successfully delivered only after the configured
  number of replicas confirms it
- an off-hours full resync schedule, such as `0 4 * * *`, provides a safety net
  for missed events or agent restarts

Volumes can replicate only selected folders/files. For game servers, this keeps
large reconstructable game downloads out of the hot replication path while still
protecting save/config state.

The default chart resources are sized for a low-memory steady state. The node
agent uses conservative rclone settings for replication work: single-transfer
copying, one checker, no explicit transfer buffer, and no multi-threaded stream
copying. That keeps memory predictable on small clusters, including Pi-class
nodes, but it also means large initial seeds can be slow. Operators that want
faster whole-volume seeding should raise the agent memory limit and the rclone
concurrency/buffer settings together once those knobs are exposed. Raising
memory alone only increases headroom; it does not make a single-transfer sync
faster.

Treat 1:1 full-volume seeding as an operational bootstrap path, not the normal
replication profile. It walks many more files, includes reconstructable game
runtime data, and can consume noticeably more accounted memory from kernel page
cache and rclone directory/check state than scoped steady-state replication.

The operator chooses the storage-capable node set when installing the chart by
scheduling the node-agent DaemonSet with normal Kubernetes selectors, affinity,
and tolerations. Workloads that consume `simple-volume` storage should also
declare matching scheduling intent in their own manifests. `simple-volume` owns
the per-volume active/fresh labels, but it should not depend on a mutating
admission webhook to make unrelated workload charts schedulable.

The node agent exposes the watch control surface used by the controller and
manual E2E validation:

- `POST /replication/watch/start` starts or replaces an active watch for a
  volume and pushes event batches to replica agents.
- `POST /replication/watch/stop` stops a running watch.
- `GET /replication/watch/status` lists active and stopped watches.
- `POST /replication/sync-batch` receives a batch on a replica and pulls changed
  files from the source agent's WebDAV endpoint.
- `POST /replication/full-sync` runs a scoped `rclone sync` from the source
  WebDAV endpoint into the local replica path.

Example start request:

```json
{
  "namespace": "default",
  "volume": "pvc-1234",
  "source": {
    "webdavUrl": "http://10.233.1.10:8081"
  },
  "targets": [
    {
      "url": "http://10.233.1.11:8080",
      "token": "change-me"
    }
  ],
  "includePaths": ["saves/**", "server.json"],
  "excludePaths": ["downloads/**"],
  "debounce": "5s"
}
```

The controller reconciles this same behavior for annotated PVCs. It
discovers the active/source node from the running pod that mounts the claim,
uses all other ready node-agent pods as replicas, starts the source watch, and
optionally runs a scoped full sync against each replica.

```yaml
metadata:
  annotations:
    simple-volume.shipstuff.io/replication-enabled: "true"
    simple-volume.shipstuff.io/replication-include-paths: "writes.log,saves/**"
    simple-volume.shipstuff.io/replication-exclude-paths: "downloads/**"
    simple-volume.shipstuff.io/replication-prune-excluded: "false"
    simple-volume.shipstuff.io/replication-debounce: "2s"
    simple-volume.shipstuff.io/replication-consistency-mode: "shadow"
    simple-volume.shipstuff.io/replication-confirmed-replicas: "1"
    simple-volume.shipstuff.io/replication-full-sync-on-start: "true"
    simple-volume.shipstuff.io/replication-full-sync-schedule: "0 4 * * *"
```

`shadow` is the safe default for DB-like file trees because replicas pull from a
source-side staged copy instead of the live directory while the app is mutating
it. Workloads that do not need this can opt out with
`replication-consistency-mode: "live"`. Shadow mode writes an additional local
copy of the included paths on the active node, so narrow include paths are the
main mitigation for disk growth and SSD wear.

Excluded paths are preserved on replicas by default. This is important for
large reconstructable caches such as Steam game installs: they can be excluded
from replication without being deleted during a full sync. Set
`simple-volume.shipstuff.io/replication-prune-excluded: "true"` only when the
replica must be an exact filtered mirror and deleting excluded files is
acceptable.

Replication uses rclone metadata preservation and the controller infers replica
file ownership from the active pod's `runAsUser`, `runAsGroup`, or `fsGroup`
when available. PVC annotations can override this for workloads without a pod
security context. File-mode overrides preserve existing executable bits so
launch scripts and runtimes are not made non-executable by normalization:

```yaml
metadata:
  annotations:
    simple-volume.shipstuff.io/replication-owner-uid: "10000"
    simple-volume.shipstuff.io/replication-owner-gid: "10000"
    simple-volume.shipstuff.io/replication-file-mode: "0664"
    simple-volume.shipstuff.io/replication-dir-mode: "0775"
```

The current schedule parser intentionally supports exact minute/hour cron
windows such as `0 4 * * *`; ranges and step expressions are left for the
controller-runtime implementation.

Opt-in automatic failover is PVC annotation driven:

```yaml
metadata:
  annotations:
    simple-volume.shipstuff.io/failover-enabled: "true"
    simple-volume.shipstuff.io/failover-grace-period: "1m"
    simple-volume.shipstuff.io/failover-max-staleness: "2m"
    simple-volume.shipstuff.io/failover-node-priority: "fresno-west-1,sf-west-1,kapolei-pacific-1"
```

When no healthy pod using the claim is running on a Ready, schedulable node for
the grace period, the controller selects an eligible replica node whose last
successful sync is within `failover-max-staleness`. If
`failover-node-priority` is set, the controller walks that list first and picks
the first fresh, healthy, Ready, schedulable replica that passes a conservative
CPU/memory request fit check. If no preferred node qualifies, it falls back to
the freshest eligible replica. Promotion records
`simple-volume.shipstuff.io/active-node` on the PV/PVC, removes the stale
Kubernetes `volume.kubernetes.io/selected-node` PVC annotation, moves the
volume's active node label to the promoted node, and deletes stale pods using
the claim.

Workload charts own their own scheduling intent. A pod template that consumes a
simple-volume claim should select the stable per-volume active label:

```yaml
nodeSelector:
  simple-volume.shipstuff.io/default.mydata-role: active
```

That keeps failover generic across Deployments, StatefulSets, and other pod
owners. `simple-volume` moves node labels and deletes stale pods; Kubernetes
and the workload's owner controller create the replacement pod on the currently
labeled active node.

The old active node is not promoted back automatically when it returns. If the
old active rejoins as a replica target, the next full sync asks that agent to
move its existing local volume into `.simple-volume-backups/` before restoring
from the current leader. That keeps a rollback copy of the pre-restore local
state while avoiding split-brain.

The controller also maintains a per-volume candidate label on fresh eligible
replica nodes:

```yaml
simple-volume.shipstuff.io/default.mydata-candidate: "true"
```

Candidate labels are derived state. They are removed from offline, stale, or
resource-ineligible nodes and should not be treated as the source of truth. The
PV/PVC active-node annotations remain the operator-facing status boundary, and
CSI still validates that a node is authorized before mounting.

## Development

```bash
go test ./...
helm lint ./helm/simple-volume
helm template simple-volume ./helm/simple-volume >/dev/null
```

## Install

```bash
helm upgrade --install simple-volume ./helm/simple-volume \
  --namespace simple-volume-system \
  --create-namespace
```

The chart uses the standard CSI external-provisioner metadata path so
`simple-volume` can bind storage to the source PVC namespace/name.

Image publishing is handled by `.github/workflows/publish-images.yml`.
Pushes to `main` publish `latest` and `sha-*` tags to
`ghcr.io/shipstuff/simple-volume`; `v*` tags publish semver image tags.

## Storage Pool Safety

Node-local storage pools are initialized with a `.simple-volume-pool` marker.
By default, the agent and CSI node plugin refuse to adopt a non-empty pool path
that does not already have this marker. This prevents accidentally pointing the
chart at a directory with unrelated data.

To intentionally adopt a non-empty path, set:

```yaml
storagePools:
  - name: default
    path: /mnt/shipstuff8tb/simple-volume
    allowNonEmpty: true
```

The override should only be used after manually checking the target directory.

## Demo

Render the demo PVC/workload:

```bash
helm template simple-volume ./helm/simple-volume -f examples/values-demo.yaml >/dev/null
kubectl apply -f examples/demo-pvc.yaml
```

The demo is disposable by design. Production workload adoption should happen
only after a separate restore drill and failover test.
