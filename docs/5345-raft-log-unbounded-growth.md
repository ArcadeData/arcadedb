# Issue #5345 - Raft log grows unbounded until disk-full

## Problem

On a low/moderate-write HA cluster the segmented Raft log under `raftStorageDirectory` grows
without bound until it fills the volume. Log purge is gated on the snapshot index
(`arcadedb.ha.logPurgeUptoSnapshot=true`), and the snapshot index only advances when Ratis's
auto-snapshot fires, which happens every `arcadedb.ha.snapshotThreshold` (default 100000)
applied entries. A cluster committing ~8500 entries/day needs ~12 days of uninterrupted uptime
to reach that count, so in practice the only thing advancing the purge point is the snapshot
taken at process start. A node that stays up long enough fills its PVC; once the log writer hits
`java.io.IOException: No space left on device`, Ratis marks the log permanently failed at that
index and rejects every subsequent `APPEND_ENTRIES`, wedging the follower until an operator
restarts the pod.

## Root cause

`RaftPropertiesBuilder` configures only a **count-based** auto-snapshot trigger
(`RaftServerConfigKeys.Snapshot.setAutoTriggerThreshold`). There is no time-based or
disk-pressure-based trigger, so on a low-write cluster the state machine never checkpoints and
Ratis never purges. The log size is driven by **bytes** (~45 KB/entry in the reported cluster,
1.9 GB for 44k entries) while the trigger is expressed in **entries**, so the count threshold
gives no bound on disk usage at all.

## Fix

ArcadeDB's Raft snapshot is a zero-byte marker file (`ArcadeStateMachine.takeSnapshot()` - the
database files on disk are already the durable state), so taking one is essentially free. That
makes a periodic trigger the right shape for this bug.

New `RaftLogCompactionScheduler` (module `ha-raft`) runs on **every** node - leader and followers
alike, since each Ratis server purges its own log against its own snapshot index - and on each
tick asks the local Ratis server to create a snapshot via
`RaftServer.snapshotManagement(SnapshotManagementRequest.newCreate(...))`. Ratis short-circuits
the request when fewer than `creationGap` entries have been applied since the last snapshot, so an
idle cluster does no work.

- `arcadedb.ha.snapshotInterval` (default `300000` ms = 5 min, `0` disables) - how often the tick runs.
- `arcadedb.ha.snapshotMinEntries` (default `64`) - creation gap for a normal tick; below this many
  new entries the tick is a no-op.
- `arcadedb.ha.raftStorageMinFreeSpacePerc` (default `20`) - when the volume hosting
  `raftStorageDirectory` drops below this percentage of free space, the tick escalates: the creation
  gap drops to 1 so the snapshot/purge fires as aggressively as Ratis allows, and a throttled
  (60 s) WARNING names the directory and the free/total bytes so the condition is visible before
  the disk fills.

The count-based `arcadedb.ha.snapshotThreshold` is left untouched, so existing deployments keep
their current behaviour on top of the new periodic floor.

## Scope note

The issue lists five suggested fixes. This change implements (1) making auto-snapshot fire on a
low-write cluster, (2) snapshot + purge under disk pressure, and (5) sizing documentation. Items
(3) "make a `No space left` wedge recoverable without a restart" and (4) "account for disk space in
stale-follower recovery" are Ratis-internal recovery work with a materially different blast radius
and are tracked separately - preventing the disk from filling is the change that removes the
trigger for both.

## Verification

- `RaftLogCompactionSchedulerTest` (new, `ha-raft`): unit-tests the tick logic - disabled when the
  interval is not positive, normal ticks use the configured creation gap, disk pressure drops the
  gap to 1, the shutdown flag suppresses the tick, a failing snapshot request never escapes the
  scheduler thread, the disk warning is throttled, and unknown volume size is not misread as
  pressure.
- `HAConfigDefaultsTest` (existing, extended): pins the three new configuration defaults.
- `mvn -pl ha-raft test` for the ha-raft module.
