# simple-volume

`simple-volume` is a Kubernetes-native async replicated local-volume prototype.
Applications use normal PVCs backed by the `simple-volume.shipstuff.io` CSI
driver. The controller and node agents handle the local replica lifecycle,
freshness policy, and promotion state.

The V0 scope is intentionally narrow:

- dynamic PVC provisioning into logical SimpleVolumes
- chart-configured local storage pools
- node-agent DaemonSet path lifecycle and rsync/rclone execution helpers
- thin CSI node bind-mount authorization
- async single-writer promotion policy

Replication logic does not run inside CSI. CSI is the Kubernetes mount boundary;
the controller owns policy and the node agent owns local filesystem operations.

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
