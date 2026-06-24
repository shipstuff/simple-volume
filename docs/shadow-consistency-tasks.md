# Shadow Consistency Tasks

This tracks the next replication reliability step after the Windrose canary
fire drills showed that live file replication can still expose an app-level
inconsistent RocksDB state.

## Implemented In This Pass

- Add `simple-volume.shipstuff.io/replication-consistency-mode`.
- Default consistency mode to `shadow`; allow opt-out with `live`.
- Add global `simple-volume.shipstuff.io/replication-confirmed-replicas`.
- Default confirmed replicas to `1`.
- Let the source agent prepare a local shadow copy under
  `.simple-volume-shadows/<namespace>/<volume>/current/data`.
- Keep shadow metadata beside the payload so internal marker files are not
  restored into the user volume.
- Let full sync replicas pull from the prepared shadow path instead of the live
  volume path when shadow mode is active.
- Let watch batches update the source shadow before replicas pull changed files.
- Treat deletes from shadow batches as authoritative so replicas can prune files
  without waiting for an off-hours full sync.
- Mark a generation successful only after the configured number of replicas
  confirms delivery.

## Validated

- Windrose canary two-leg fire drills pass with shadow mode enabled.
- Startup full sync completes before the source watch starts.
- Each promotion runs a post-promotion startup full sync and restarts the watch
  from the promoted active node.
- Returning stale nodes back up local state before restoring from the promoted
  leader.

## Still To Measure

- Source-node disk growth during active gameplay.
- Shadow bytes copied per batch for RocksDB-heavy workloads.
- Scheduled maintenance-window full sync behavior over longer runs.

## Operational Caveats

Shadow mode increases source-node writes because durable data is written once by
the app and again into the local shadow. The expected max active-node footprint
is approximately:

```text
live included data + shadow included data
```

This is why scoped `replication-include-paths` matter. Avoid `**` for workloads
that constantly rewrite large binary trees unless the extra disk use and SSD
wear are acceptable.

Mitigations:

- replicate only durable paths, not reconstructable runtime downloads
- debounce hot file changes into batches
- keep full sync as startup, repair, and maintenance-window work
- expose metrics for shadow bytes copied, generation duration, confirmation
  lag, and failed confirmations
- later prefer reflink copies where the filesystem supports them
- later add a snapshot backend for ZFS or Btrfs hosts

## Backend Boundary

The default backend remains file-shadow plus rclone/WebDAV because it keeps
`simple-volume` portable and lightweight.

ZFS would be useful as an optional future backend:

- atomic source snapshots
- cheap local generation creation
- block-level incremental send/receive
- stronger checksums and rollback tools

It should stay optional. Making ZFS mandatory would turn this project into a
Kubernetes operator for replicated ZFS datasets instead of a simple local-volume
controller with async recovery points.
