# 5407 - HA: leader crash between Raft commit and phase-2 apply permanently loses a locally-originated write

Issue: https://github.com/ArcadeData/arcadedb/issues/5407

## Problem

On the leader, a locally-originated transaction is written to local pages by
`RaftReplicatedDatabase.commit()`'s **phase 2** (`commit2ndPhase`), which runs *after* Raft
replication succeeds. When Ratis commits the entry it independently calls
`ArcadeStateMachine.applyTransaction`, which:

1. **origin-skips** the data apply (`originatedLocally == true`) - correct, phase 2 owns the write; but
2. still advances `lastAppliedIndex` to that entry's index.

If phase 2 never runs, the node is left holding a committed entry it never applied. That state was
recoverable in memory, but not durably: `takeSnapshot()` reported `lastAppliedIndex` as the Ratis
durability checkpoint and wrote a `snapshot.<term>_<index>` marker at it. Since `reinitialize()`
seeds the replay position **exclusively** from that marker, a restart began replaying *above* the
unapplied entry. Neither Raft replay nor follower catch-up ever revisited it, so the write was
silently and permanently lost on that node.

This is the hard-crash analog of #4790, whose `abandonedLocalTransactions` marker is in-memory only
and therefore cannot survive the window.

## Root cause evidence

Reproduced deterministically on `main` (4/4 runs). After the failing run, the crashed old leader's
on-disk Raft state showed the smoking gun - a snapshot checkpoint covering the entry whose pages
were never written:

```
databases1/.raft/applied-index                 {"global":9,"db":{"graph":9}}
databases1/.raft-storage/.../sm/snapshot.1_9   0B      <- checkpoint at the lost entry (index 9)
```

The restarted node consequently reported itself caught up within ~125 ms of rejoining, replayed
nothing, and ended at 100 records instead of 101 (`Page(graph/28/0) DB1 v2 <> DB2 v1`).

Ratis takes a snapshot on shutdown, which is what makes the loss durable: an aborted phase 2
followed by any shutdown (graceful stop, or the `triggerCriticalHalt()` path) buries the entry.

## Fix

Keep committed-but-not-locally-applied entries inside the Raft replay window until their local
apply is proven.

**`ArcadeStateMachine`**
- New in-memory registry of in-flight leader-side phase-2 applies: `beginLocalPhase2()` records the
  applied index observed *before* replication (the entry's "replay floor") and returns a ticket;
  `endLocalPhase2(ticket)` releases it.
- `takeSnapshot()` clamps the reported checkpoint to the lowest in-flight floor, so it can never
  cover an entry whose phase 2 has not confirmed. It also refuses to regress the marker below an
  existing one (that would ask Ratis to replay from entries a previous checkpoint already authorised
  for purging), reporting `INVALID_LOG_INDEX` instead.

**`RaftReplicatedDatabase`**
- `commit()` takes a ticket before replication and delegates to the new
  `replicateAndCommitLocally(...)`.
- The ticket is **retained by default** and released only where the local pages are known to be
  settled: phase 2 succeeded, a failed phase 2 reconciled successfully, ALL-quorum recovery settled
  the pages, the commit is on a replica (no phase 2 expected), or replication failed outright so no
  entry exists. Any other exit - notably "replication succeeded but phase 2 never ran" - keeps the
  hold, which is exactly the #5407 window.
- `applyLocallyAfterMajorityCommit(...)` now returns whether the pages ended up written, so the
  caller only releases when they did.

Retaining a ticket costs a stalled compaction checkpoint until the node restarts, at which point
replay applies the entry and the hold disappears with the process. That is a deliberate trade of
log-compaction progress for durability. Replay is idempotent: `applyChanges` page-version guards
re-apply an equal-version entry with the same absolute bytes and skip lower-version ones.

## Verification

How this change was verified:

- **New regression test** `ArcadeStateMachinePendingPhase2SnapshotTest` (5 cases): the checkpoint
  clamps to the pending floor; it advances again once phase 2 completes; it tracks the oldest of
  several in-flight commits; it never regresses below an existing marker; and it is suppressed
  entirely when a phase 2 starts before anything has been applied.
- **The two ITs from the issue**, which failed 100% before and pass after:
  - `RaftLeaderCrashBetweenCommitAndApplyIT`
  - `RaftLeaderCrashWithExternalPropertyIT`
- **Regression sweep** over the `ha-raft` unit suite and the ITs that exercise the commit path,
  origin-skip, phase-2 failure handling, snapshot/compaction, and crash recovery.

Repro commands:

```
mvn -pl ha-raft verify -DskipTests -Pintegration \
  -Dit.test='RaftLeaderCrashBetweenCommitAndApplyIT,RaftLeaderCrashWithExternalPropertyIT' \
  -Dfailsafe.failIfNoSpecifiedTests=false

mvn -pl ha-raft test -Dtest=ArcadeStateMachinePendingPhase2SnapshotTest
```

## Review cycle 1 (PR #5408, head `198274bd9`)

`claude[bot]` reviewed and confirmed the correctness of the clamp direction, the no-regression guard,
and the idempotent ticket release. `gemini-code-assist` did not respond within the polling window.
Applied:

- **Stalled-compaction observability** (the reviewer's only pre-merge ask). A retained ticket pins the
  checkpoint until restart, previously signalled only by a `HALog.BASIC` line. `pendingLocalPhase2`
  now records each ticket's start time and `takeSnapshot()` emits a throttled `WARNING` once a held
  ticket exceeds 5 minutes, naming the pinned index and pointing at Raft-storage disk usage.
  Implemented rather than left as a TODO: it is a hazard this change introduces.
  - One detail in the review was slightly off: a stuck ticket does *not* make every `takeSnapshot()`
    return `INVALID_LOG_INDEX`. It keeps returning the stalled floor and re-registering the marker
    there; only a clamp that would regress below an existing marker returns `INVALID_LOG_INDEX`. The
    operational consequence (log not purged past that index) is the same.
- **Term/index comment** in `takeSnapshot()` explaining that after a clamp the marker term may not
  match the clamped index, why that is already tolerated (#575/#593), and why it must not be
  "corrected" by looking up the term at the clamped index (that entry may be purged).
- **`Issue5407Phase2TicketLifecycleTest`** (5 cases) pinning which exit releases the ticket -
  phase-2 success releases, a fault between replication and phase 2 retains, an unreconciled phase-2
  failure retains, replication failure releases, replica commit releases. `replicateAndCommitLocally`
  was made package-private for this (existing convention in the module). This is the part of the fix
  most exposed to a future refactor, and the retain case fails if the guard is moved back into a
  blanket `finally`.

Not applied: a Micrometer gauge for `pendingLocalPhase2` size. This class has no metrics wiring
today, so adding it is genuine follow-up scope rather than part of this fix; the WARNING covers the
operator-visibility gap in the meantime.

## Notes for follow-up

- The `ReplicationDispatchedTimeoutException` path (#4790) now also retains its ticket, which makes
  that indeterminate window crash-safe as a side effect. The in-memory
  `abandonedLocalTransactions` marker it relies on is still lost on a real crash; the retained
  ticket means replay covers the entry instead.
- The persisted `.raft/applied-index` file is intentionally left unclamped. It never feeds the
  replay position (only the snapshot-gap check, the per-database bootstrap replay-skip, and the
  "never applied anything" bootstrap signal), so clamping it would change bootstrap behaviour
  without improving durability.
- Worth a follow-up issue: expose `pendingLocalPhase2Count()` (and the oldest held floor) as a
  Micrometer gauge so a node whose compaction is pinned is visible on a dashboard, not only in the
  log.
