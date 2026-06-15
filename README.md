# simple-volume

`simple-volume` is a Kubernetes-native async replicated local-volume prototype.
Applications use normal PVCs backed by the `simple-volume.shipstuff.io` CSI
driver. The controller and node agents handle the local replica lifecycle,
freshness policy, and promotion state.

## Why

We want local-disk performance for workloads such as game servers while still
having a Kubernetes-native recovery path when a node fails. Existing storage
options did not match that shape cleanly: VolSync is useful as a copy primitive
but does not own promotion or scheduling policy, Longhorn is a broader
replicated block-storage layer, and OpenEBS/Ceph add more storage-platform
surface than this use case needs.

`simple-volume` keeps the hot path local, uses PVC/CSI so applications do not
mount raw `hostPath`, replicates selected durable paths asynchronously, and
promotes only replicas that are fresh enough for the workload's RPO. See
[docs/why-simple-volume.md](docs/why-simple-volume.md) for the full design
rationale, replication model, and storage options we evaluated.

## V0 Scope

The V0 scope is intentionally narrow:

- dynamic PVC provisioning into logical SimpleVolumes
- chart-configured local storage pools
- node-agent DaemonSet path lifecycle and rsync/rclone execution helpers
- thin CSI node bind-mount authorization
- async single-writer promotion policy
- watch-driven rclone/WebDAV replication primitives

Replication logic does not run inside CSI. CSI is the Kubernetes mount boundary;
the controller owns policy and the node agent owns local filesystem operations.

## Replication Model

The intended V0 replication path is watch-driven:

- the active node-agent watches configured durable paths inside the active
  volume
- file events are debounced into batches
- replica agents receive batch sync requests
- replica agents pull changed files from the active agent's read-only rclone
  WebDAV endpoint
- the WebDAV server uses a short directory cache so newly written files are
  visible to watch-triggered pulls
- an off-hours full resync schedule, such as `0 4 * * *`, provides a safety net
  for missed events or agent restarts

Volumes can replicate only selected folders/files. For game servers, this keeps
large reconstructable game downloads out of the hot replication path while still
protecting save/config state.

The node agent exposes the V0 watch control surface used by the controller and
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

The controller reconciles this same behavior for annotated PVCs. For V0 it
discovers the active/source node from the running pod that mounts the claim,
uses all other ready node-agent pods as replicas, starts the source watch, and
optionally runs a scoped full sync against each replica.

```yaml
metadata:
  annotations:
    simple-volume.shipstuff.io/replication-enabled: "true"
    simple-volume.shipstuff.io/replication-include-paths: "writes.log,saves/**"
    simple-volume.shipstuff.io/replication-exclude-paths: "downloads/**"
    simple-volume.shipstuff.io/replication-debounce: "2s"
    simple-volume.shipstuff.io/replication-full-sync-on-start: "true"
    simple-volume.shipstuff.io/replication-full-sync-schedule: "0 4 * * *"
```

The current schedule parser intentionally supports exact minute/hour cron
windows such as `0 4 * * *`; ranges and step expressions are left for the
controller-runtime implementation.

Opt-in V0 failover is also PVC annotation driven and currently supports
Deployment workloads:

```yaml
metadata:
  annotations:
    simple-volume.shipstuff.io/failover-enabled: "true"
    simple-volume.shipstuff.io/failover-workload-kind: "Deployment"
    simple-volume.shipstuff.io/failover-workload-name: "my-writer"
    simple-volume.shipstuff.io/failover-grace-period: "1m"
    simple-volume.shipstuff.io/failover-max-staleness: "2m"
```

When no healthy pod using the claim is running on a Ready, schedulable node for
the grace period, the controller selects a ready replica node whose last
successful sync is within `failover-max-staleness`, records
`simple-volume.shipstuff.io/active-node` on the PV/PVC, removes the stale
Kubernetes `volume.kubernetes.io/selected-node` PVC annotation, moves the
volume's active node label to the promoted node, and deletes stale pods using
the claim. Demo Deployment support removes an old hard
`kubernetes.io/hostname` node selector and replaces it with a stable selector
for the volume's active-node label, so future failovers move by relabeling
nodes rather than repatching a concrete hostname.

The old active node is not promoted back automatically when it returns. If the
old active rejoins as a replica target, the next full sync asks that agent to
move its existing local volume into `.simple-volume-backups/` before restoring
from the current leader. That keeps a rollback copy of the pre-restore local
state while avoiding split-brain.

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
