# ha-raft CLAUDE.md

Guidance for working under `ha-raft/`. This file records things the code does not tell you, or actively misleads you about. It is not a tour of the module - use `Glob` and the package names for that.

## Replay position comes only from the Ratis snapshot marker

What a restarted node replays is decided **solely** by the Ratis snapshot marker file `snapshot.<term>_<index>` under `.raft-storage/.../sm/`. `ArcadeStateMachine.reinitialize()` seeds `lastAppliedIndex` from `storage.getLatestSnapshot()`; with no marker it seeds -1 and Ratis replays the whole retained log.

The persisted `.raft/applied-index` JSON is read in `reinitialize()` but **never** feeds the replay position. It has exactly three consumers:

1. the snapshot-gap check that decides whether to download from the leader,
2. the per-database bootstrap replay-skip in `applyBootstrapFingerprintEntry`,
3. `hasNeverAppliedApplicationEntry()`, the offline-bootstrap gate.

**Consequence:** to make a committed-but-unapplied entry replayable you must control what `takeSnapshot()` reports. Clamping the applied-index file changes bootstrap behavior and does nothing for durability. This is the single most common wrong turn when chasing a lost-write bug in this module.

Note that `globalAppliedIndex` and `lastAppliedIndex` track the same value on the apply path but are seeded independently, so they can briefly differ right after `reinitialize()`. Do not assert equality across that window.

## Snapshots are taken far more often than you would guess

Three triggers, not one: the Ratis auto-trigger (`arcadedb.ha.snapshotThreshold`, default 100k entries), ArcadeDB's own wall-clock `RaftLogCompactionScheduler` (`arcadedb.ha.snapshotInterval` 300 s, `arcadedb.ha.snapshotMinEntries` 64), **and shutdown**, via Ratis's `StateMachineUpdater.stop()`.

The shutdown trigger has a counterintuitive consequence when reproducing durability bugs: a graceful `server.stop()` takes a snapshot and makes an apply-ordering gap **permanent**, while a `SIGKILL` takes no snapshot and replays cleanly. If a durability test passes under kill -9 and fails under a clean stop, this is why. Choose the shutdown mode deliberately.

## Peer-list filtering is duplicated across three methods

`RaftHAServer` iterates `raftGroup.getPeers()` and applies its own copy of the leader-exclusion criterion in three places:

- `getStats()` - feeds `ha.network.replicas` in `/api/v1/server?mode=cluster`
- `getReplicaAddresses()` - feeds `ha.replicaAddresses` in the same response
- the Bolt routing address resolver

Each has its own local `excludeId` derivation (`leaderId != null ? leaderId : localPeerId`, or the leader directly). They are not factored into a shared helper, so a fix applied to one silently leaves the others wrong and the cluster API response internally inconsistent.

**Any change to what "replica" means must be applied to all three.** Grep `RaftHAServer.java` for `getPeers()` before claiming such a fix is complete. This has already caused one incomplete fix that review caught.

## Known sharp edge: materialized views break page-version replication

Unresolved as of 2026-07, tracked in #5492 - delete this section when that closes. Established by a controlled A/B run on a 3-node cluster, identical load, only the schema differing:

- **With** a `REFRESH INCREMENTAL` materialized view: 2041/3000 writes fail, 12 snapshot-resync cycles in ~9 s, nodes never converge.
- **Without** it: 3000/3000 writes, zero errors, immediate convergence.

The leader ships a `TX_ENTRY` whose page version is ahead of what followers hold, first appearing on the view's own backing bucket. Followers throw `WALVersionGapException`, trigger a full snapshot resync, and the resync does not repair it - the next entry gaps again on a different file. Meanwhile every request to a resyncing follower is deflected with a 503, so writes routed through it are dropped. The end state is lost committed writes, not split-brain.

The exact leader-side path is **not** proven, and it has never been reproduced in-process: a 3-node harness on `BaseRaftHATest` sees zero gaps with writes on the leader, writes through a follower, with and without a unique index on the source type. The container A/B is the only known reproduction. Two theories are ruled out and should not be re-proposed: `LocalSchema` is constructed with `wrappedDatabaseInstance`, so the refresh does go through `RaftReplicatedDatabase` rather than committing on the inner `LocalDatabase`; and #5503 (concurrent writes on a shared follower handle) is a different failure mode - `Invalid record size ... deleting record`, no version gap - whose control arm was clean.

One leader-side hole of exactly this shape **has** been found and closed: `recordFileChanges` drained the buffered WAL of commits that ran inside the DDL callback but omitted it from the send guard, so a callback that wrote records without creating a file or moving the schema version had its pages applied on the leader and never shipped. That is fixed, and `applySchemaEntry` now recognizes the resulting WAL-only entry and skips the schema reload for it, as it already did for sealed-only TimeSeries entries. It did not close the materialized-view A/B, so the section stays.

If you are investigating unexplained replication gaps, check whether the failing database has a materialized view before going further.
