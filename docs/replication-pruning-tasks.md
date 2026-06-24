# Replication Pruning Tasks

## Decision

Excluded paths are preserved on replicas by default. Scoped replication treats
included paths as authoritative durable data and excluded paths as node-local
cache unless an operator explicitly opts into pruning.

Use:

```yaml
metadata:
  annotations:
    simple-volume.shipstuff.io/replication-prune-excluded: "true"
```

only when the replica should be an exact filtered mirror and deleting excluded
files is acceptable.

## Rationale

For game servers, excluded paths often contain large reconstructable runtime
data such as Steam libraries, game binaries, shader caches, or logs. Deleting
those directories during full sync saves disk but can turn failover into a slow
redownload. Preserving them gives each node a warm local cache while still
replicating the durable save/config paths.

## Tasks

- [x] Default rclone full sync to preserve excluded paths.
- [x] Add opt-in `replication-prune-excluded` PVC annotation.
- [x] Pass prune policy from controller to source shadow prepare and target
  full sync requests.
- [x] Cover default preservation and opt-in pruning with unit tests.
- [x] Run a Windrose canary failover after a confirmed full sync and verify the
  already-warm replica does not redownload the excluded game install on
  promotion.
- [ ] Document any workloads that require exact filtered mirrors and should set
  `replication-prune-excluded: "true"`.
