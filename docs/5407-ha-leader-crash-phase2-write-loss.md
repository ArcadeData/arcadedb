# Issue #5407 - leader crash between Raft commit and phase-2 apply permanently loses a write

## Symptom

`RaftLeaderCrashBetweenCommitAndApplyIT` and `RaftLeaderCrashWithExternalPropertyIT` failed
deterministically (4/4 and 3/3). A leader is stopped in the window between Raft replication and its
local phase-2 apply; on restart it never recovers the injected record:

```
expected: 101L but was: 100L
Suppressed: DatabaseAreNotIdentical: Page PageId(graph/28/0) DB1 v2 <> DB2 v1
```

Followers keep the write (Raft durability holds), so the cluster majority is correct. The recovered
ex-leader diverges silently and permanently.

## Root cause

On the leader the local page write is **phase 2** (`RaftReplicatedDatabase.commit` ->
`commit2ndPhase`), which runs *after* Raft replication. When Ratis commits the entry it calls
`ArcadeStateMachine.applyTransaction`, which origin-skips the data apply (`originatedLocally == true`
- correct, phase 2 owns the write) but still advances `lastAppliedIndex`. If phase 2 never runs, the
node holds a committed entry it never applied.

What makes that permanent is `takeSnapshot()`: it reported `lastAppliedIndex` as Ratis's durability
checkpoint and wrote a `snapshot.<term>_<index>` marker at it. `reinitialize()` seeds the replay
position **exclusively** from that marker (the persisted `.raft/applied-index` file is *not*
consulted for it - that file only feeds the snapshot-gap check, the per-database bootstrap
replay-skip, and `hasNeverAppliedApplicationEntry()`). So the restart began replaying *above* the
unapplied entry, and neither replay nor follower catch-up ever revisited it.

Evidence from a failing run - a checkpoint sitting exactly on the lost entry (index 9):

```
databases1/.raft/applied-index                 {"global":9,"db":{"graph":9}}
databases1/.raft-storage/.../sm/snapshot.1_9   0B
```

**Ratis takes a snapshot on shutdown**, which is what makes the loss durable. This is therefore not
only a hard-crash window: any stop after an aborted phase 2 (graceful stop, or `triggerCriticalHalt()`'s
async `server.stop()`) buries the entry. Under a true SIGKILL no shutdown snapshot is taken at all,
so the entry would still replay.

This is the crash analog of #4790, whose `abandonedLocalTransactions` marker is in-memory only and
cannot survive the window.

## Fix

Keep committed-but-not-locally-applied entries inside the Raft replay window until their local apply
is proven.

**`ArcadeStateMachine`**
- `beginLocalPhase2()` records the applied index observed *before* replication (the entry's "replay
  floor") and returns a ticket; `endLocalPhase2(ticket)` releases it. Recording the floor before the
  entry exists is what makes the clamp race-free: the entry is guaranteed to land above it.
- `takeSnapshot()` clamps the reported checkpoint to the lowest in-flight floor, so it can never
  cover an entry whose phase 2 has not confirmed. It refuses to regress the marker below an existing
  one - that would ask Ratis to replay from entries a previous checkpoint already authorised for
  purging - reporting `INVALID_LOG_INDEX` instead.
- After a clamp the marker's term may not match its index. That is deliberate and already tolerated
  (`reinitialize()` seeds an inflated marker term as-is and realigns on the first replayed entry,
  issues #575/#593). It must not be "corrected" by looking up the term at the clamped index, since
  that entry may already be purged.

**`RaftReplicatedDatabase`**
- `commit()` takes a ticket before replication and delegates to `replicateAndCommitLocally(...)`.
- The ticket is **retained by default**, released only where the local pages are provably settled:
  phase 2 succeeded, a failed phase 2 reconciled successfully, ALL-quorum recovery settled the pages,
  the commit is on a replica (no phase 2 expected), or replication failed outright so no entry
  exists. Any other exit - notably "replication succeeded but phase 2 never ran" - keeps the hold.
- `applyLocallyAfterMajorityCommit(...)` returns whether the pages ended up written, so the caller
  releases only when they did.

A blanket `try/finally` release does **not** work, and is the trap to avoid here: ordinary exception
unwinding drops the guard before the shutdown snapshot fires, and both crash ITs still fail. Retain
by default, release on proof.

Replay is idempotent: `applyChanges` page-version guards re-apply an equal-version entry with the
same absolute bytes and skip lower-version ones.

## Cost and observability

A retained ticket pins the snapshot checkpoint, and therefore Raft log purge, until the node
restarts - at which point replay applies the entry and the hold disappears with the process. That is
a deliberate trade of log-compaction progress for durability.

To keep it from being silent, each ticket records its start time and `takeSnapshot()` emits a
throttled WARNING once one has been held beyond 5 minutes, naming the pinned index and pointing at
Raft-storage disk usage. The check runs on a snapshot attempt rather than a timer, so it can lag a
stuck ticket by up to one compaction interval (`arcadedb.ha.snapshotInterval`, 5 min by default).

Note that a stuck ticket does not make `takeSnapshot()` return `INVALID_LOG_INDEX` every time: it
keeps returning the stalled floor and re-registering the marker there. Only a clamp that would
regress below an existing marker returns `INVALID_LOG_INDEX`. Either way the log is not purged past
the pinned index.

## Tests

- `ArcadeStateMachinePendingPhase2SnapshotTest` (5 cases): the checkpoint clamps to the pending
  floor; advances again once phase 2 completes; tracks the oldest of several in-flight commits; never
  regresses below an existing marker; is suppressed entirely when a phase 2 starts before anything
  has been applied.
- `Issue5407Phase2TicketLifecycleTest` (7 cases): pins *which* exit releases the ticket - phase-2
  success, replication failure, replica commit and settled ALL-quorum recovery release; a fault
  between replication and phase 2, an unreconciled phase-2 failure and an unsettled ALL-quorum
  recovery retain. This is the surface most exposed to a future refactor: the retain cases fail if
  the guard is moved back into a blanket `finally`.

## Verification

- 730 `ha-raft` unit tests pass.
- `RaftLeaderCrashBetweenCommitAndApplyIT` and `RaftLeaderCrashWithExternalPropertyIT`, which failed
  100% before, pass.
- IT sweep over the commit path, origin-skip, phase-2 failure handling, snapshot/compaction and crash
  recovery: 22 tests across 15 IT classes, 0 failures (`Issue4790PhantomCommitOriginSkipIT`,
  `OriginNodeSkipIT`, `Issue4740Phase2ReconcileIT`, `Issue5064CommittedRemotelyContractIT`,
  `RaftLeaderCrashAndRecoverIT`, `RaftPeriodicSnapshotCompactionIT`, `RaftLogCompactionWiringIT`,
  `RaftFullSnapshotResyncIT`, `Raft3PhaseCommitIT`, `RaftReplication3NodesIT`,
  `RaftReplicaCrashAndRecoverIT`, `RaftIdleReplicaRestartIT`, `RaftBootstrapDoesNotEngageOnRestartIT`).

```
mvn -pl ha-raft verify -DskipTests -Pintegration \
  -Dit.test='RaftLeaderCrashBetweenCommitAndApplyIT,RaftLeaderCrashWithExternalPropertyIT' \
  -Dfailsafe.failIfNoSpecifiedTests=false

mvn -pl ha-raft test -Dtest='ArcadeStateMachinePendingPhase2SnapshotTest,Issue5407Phase2TicketLifecycleTest'
```

`Raft3PhaseCommitIT.concurrentWritersReplicateCorrectly` flaked once during a batch run (HTTP
non-200 on the first insert while the previous IT's servers were still shutting down). It passed 3/3
in isolation and the full batch re-ran clean: cross-test startup contention, not a regression.

## Known gaps

- **The #4790 path holds its ticket until restart even on the happy outcome.** The
  `ReplicationDispatchedTimeoutException` branch retains the ticket, which makes that indeterminate
  window crash-safe as a side effect. But when such an entry later reaches quorum and is applied by
  `applyTxEntry` - the usual resolution of a #4790 timeout - its pages become durable and nothing
  releases the ticket, so compaction stays pinned until the node restarts. Since those timeouts are
  transient, this can happen without any crash. Releasing on `applyTxEntry` needs a
  ticket-to-`walTxId` correlation that does not exist today; adding one to that path was judged worse
  than the documented gap for a change whose purpose is durability. Tracked in **#5410**.
- **No metric for a pinned checkpoint.** `pendingLocalPhase2Count()` exists but this class has no
  Micrometer wiring; exposing it (plus the oldest held floor) as a gauge would make a stalled node
  visible on a dashboard rather than only in the log. Also tracked in **#5410**.

## Design notes

- The persisted `.raft/applied-index` file is intentionally left unclamped: it never feeds the replay
  position, so clamping it would change bootstrap behaviour without improving durability.
- `beginLocalPhase2` boxes a `Long` key and allocates one small record per leader commit. Kept: the
  same path already allocates the payload, a copy of the bucket-delta map and the WAL byte array,
  then makes a gRPC round trip, and the map churns back to empty in steady state.
