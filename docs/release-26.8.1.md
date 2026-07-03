# ArcadeDB v.26.8.1 Release Highlights

This is a living document: fixes, improvements, new features, and breaking changes are collected here as
they land during the 26.8.1 development cycle, so the release notes are ready at tag time.

This release hardens **High Availability (Raft)** recovery. Raft storage is now **durable by default**
(previously ephemeral outside Kubernetes), which removes a class of permanent follower divergence after a
full-cluster cold restart - see **Breaking Changes** below. It also removes a diverged-follower log flood
that could starve the very snapshot resync meant to heal the node.

### Improvements

- **HA: throttled diverged-follower resync logging.** When a follower detects a WAL page-version gap it
  quarantines the affected database and downloads a fresh snapshot from the leader. Previously every
  subsequent committed entry for that database re-logged a full `SEVERE` stack trace (observed in the
  field at tens per second for the whole resync), which both flooded the logs and, on small nodes, stole
  the CPU/IO the snapshot download needed to complete. Now only the **first** gap logs loudly and triggers
  the download; subsequent entries emit a throttled one-line notice (at most once per 5s per database)
  and the redundant per-entry stack trace in `applyTransaction` is suppressed while the database is
  quarantined. Genuine (non-diverged) replication errors still log loudly. Recovery behaviour is
  otherwise unchanged.

## Breaking Changes (migration notes)

### 1. `raftPersistStorage` now defaults to `true` (durable Raft storage)

`arcadedb.ha.raftPersistStorage` now defaults to **`true`**. Previously it defaulted to `false`
(ephemeral) outside Kubernetes - the Raft storage directory was wiped and re-formatted on every server
start.

- **Why:** wiping the Raft log on restart means a follower that was merely lagging (had not yet applied
  every committed entry to its data files) loses the log that would let it catch up. On a full-cluster
  cold restart the cluster then starts a fresh log and treats the elected leader's on-disk database as
  ground truth; the lagging follower applies new deltas on top of stale pages and fails permanently with
  `WALVersionGapException` (page-version gaps), recoverable only by a full snapshot resync. On a
  single-seed cluster, wiping storage can also silently re-form a fresh empty single-node cluster (data
  loss / split brain). Durable storage lets a restarted node rejoin by replaying its persisted log.
  Persisting was already the default under Kubernetes, where a PersistentVolume made it essential
  ([#4835](https://github.com/ArcadeData/arcadedb/issues/4835)); it is now the default everywhere.
- **Impact:** existing HA deployments that relied on the wipe-on-restart behaviour will now preserve the
  Raft storage directory across restarts. This is the safer, faster path (log replay instead of forced
  full resync) and requires no action for most operators. The Raft storage directory now persists on
  disk between restarts; ensure it lives on durable storage (see `arcadedb.ha.raftStorageDirectory`).
- **Migration / opt-out:** a throwaway or test cluster that really wants ephemeral storage can still opt
  out explicitly with `arcadedb.ha.raftPersistStorage=false` (config file, server settings, or
  `-Darcadedb.ha.raftPersistStorage=false`). An explicit value is always honored.

**Full Changelog**: https://github.com/ArcadeData/arcadedb/compare/26.7.1...26.8.1
