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

## Anything that commits must hold the WRAPPED database instance, not the inner one

`LocalDatabase.commit()` writes pages locally. `RaftReplicatedDatabase.commit()` proposes them to Raft and *then* writes them. Code holding the wrong one of the two commits successfully, applies its pages on the leader, and replicates nothing. Followers trail by exactly those page versions, and the next replicated entry touching one of them fails its version check - `WALVersionGapException`, database marked diverged, snapshot resync, and the entry after it breaks the same way. Nothing throws at the point of the mistake, so the symptom always surfaces somewhere else.

`getWrappedDatabaseInstance()` returns the Raft wrapper under HA and the instance itself off it, so resolving it is always correct and never costs anything.

This was #5492. `SQLQueryEngine` handed statements the raw instance, so `CommandContext.getDatabase()` was the inner `LocalDatabase`, and every statement that commits mid-execution bypassed replication: `TRUNCATE TYPE`/`BUCKET` batching every `TRUNCATE_BATCH_SIZE` (default **1000**) records, `REBUILD INDEX`, `BatchStep`. `SQLScriptQueryEngine` had always resolved the wrapper before running the *same statement classes*, which is why an identical `TRUNCATE` replicated under `sqlscript` and not under `sql`.

Two things about how it hid, both worth generalizing:

- **A default-sized threshold can look like a heisenbug.** It reproduced only above 1000 records in one statement, so every in-process attempt (180 to 450 records) saw a clean cluster and the failure was written off as needing container-level concurrency. If a replication bug reproduces in containers and not in process, compare the *sizes* before theorizing about timing.
- **A materialized view was the trigger only because it truncates constantly.** `MaterializedViewRefresher` truncates and rebuilds its backing type on every source commit, so a view over 1000+ rows crossed the boundary on every refresh. The view was the amplifier, not the defect - `TRUNCATE TYPE` over 1000 records through `/api/v1/command` with `language=sql` was enough on its own.

When adding any code path that commits, ask which instance it holds. `Issue5492TruncateBatchNotReplicatedIT` guards the statement path; it asserts the WAL gap counter *before* querying the follower, because the resync reinstalls the follower's database and a stale handle then throws `DatabaseIsClosed`, which reads like infrastructure noise rather than divergence.
