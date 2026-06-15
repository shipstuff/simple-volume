# Simple Volume E2E Deployment Tasks

This is the implementation checklist to get `simple-volume` from the current
V0 scaffold to a cluster-tested E2E flow.

## 1. Publish The Image

- Commit the current scaffold and push `main`.
- Confirm `.github/workflows/publish-images.yml` publishes
  `ghcr.io/shipstuff/simple-volume:sha-<sha>` on the `main` push.
- Use the `sha-<sha>` tag for first cluster installs.
- Cut `v0.1.0` only after the CSI smoke test passes.

## 2. Define A Safe Test Pool

- Pick a disposable path on storage-capable nodes, for example
  `/mnt/shipstuff8tb/simple-volume-test`.
- Label/select only nodes that should run the node DaemonSet for the first
  test.
- Keep `storagePools[0].allowNonEmpty=false` by default.
- If the pool path exists and contains unrelated files, the agent/CSI node must
  fail instead of adopting it.
- To intentionally adopt a non-empty path, set
  `storagePools[0].allowNonEmpty=true`; the agent will write the pool marker.

## 3. Install The Chart

Use the published image tag:

```bash
helm upgrade --install simple-volume ./helm/simple-volume \
  --namespace simple-volume-system \
  --create-namespace \
  --set image.tag=sha-REPLACE_ME \
  --set storagePools[0].path=/mnt/shipstuff8tb/simple-volume-test
```

Verify:

```bash
kubectl -n simple-volume-system get pods -o wide
kubectl get csidriver simple-volume.shipstuff.io
kubectl get storageclass simple-volume
```

## 4. CSI Smoke Test

- Apply `examples/demo-pvc.yaml`.
- Confirm the PVC binds through the `simple-volume` StorageClass.
- Confirm the demo pod schedules and mounts `/data`.
- Write and read a file through the pod.
- Verify the backing path exists under the configured pool on the scheduled
  node and contains the `.simple-volume-pool` marker at the pool root.

Commands:

```bash
kubectl apply -f examples/demo-pvc.yaml
kubectl get pvc simple-volume-demo
kubectl get pod -l app=simple-volume-demo -o wide
kubectl exec deploy/simple-volume-demo -- sh -c 'echo ok >> /data/e2e.txt && cat /data/e2e.txt'
```

## 5. Implement True Replication E2E

- Add a live controller reconcile loop for PVC/PV, `SimpleVolume`, agents, and
  node state.
- Persist `SimpleVolume` status with active node, replica nodes, freshness, and
  conditions.
- Add agent heartbeat and pool capacity reporting.
- Add active-agent fsnotify watching for configured `sync.includePaths`.
- Add debounced event batches sent from the active node to replica agents.
- Add agent-to-agent rclone execution over Kubernetes networking using the
  source agent's read-only WebDAV endpoint.
- Add cron-style off-hours full resync from the controller as a safety net,
  using `sync.fullResyncSchedule` such as `0 4 * * *`; do not create one
  Kubernetes CronJob per volume in V0.
- Make CSI node authorization read the controller/volume status instead of the
  current local static authorization scaffold.
- Add per-volume node labels for active/healthy scheduling.
- Add promotion state transitions and stale-replica blocking.
- Add an E2E test script that writes data, syncs it to a second node, simulates
  active-node loss, promotes a fresh replica, reschedules the demo pod, and
  verifies data on the promoted node.
- Extend the failover drill so the old active node rejoins, backs up its stale
  local copy under `.simple-volume-backups/`, restores from the promoted leader,
  and remains a replica unless an explicit move-back is requested.

Recommended V0 replication policy shape:

```yaml
sync:
  mode: watch
  includePaths:
    - savegame/**
    - enshrouded_server.json
  excludePaths:
    - steamapps/**
    - downloads/**
  debounce: 5s
  fullResyncSchedule: "0 4 * * *"
```

The include list is important for game servers whose PVC contains both durable
save/config state and reconstructable game files.

## 6. Release Gate

Before tagging `v0.1.0`, all of these should pass:

```bash
go test ./...
helm lint ./helm/simple-volume
helm template simple-volume ./helm/simple-volume --include-crds >/tmp/simple-volume.yaml
helm template simple-volume ./helm/simple-volume -f examples/values-demo.yaml --include-crds >/tmp/simple-volume-demo.yaml
```

And in-cluster:

- controller, CSI controller, node agents, CSI node plugins, provisioner, and
  registrar are healthy.
- demo PVC binds.
- demo pod mounts and writes data.
- non-empty uninitialized pool adoption fails by default.
