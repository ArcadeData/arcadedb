# #5410 - Release the phase-2 ticket when an abandoned entry applies

Follow-up to #5407 / PR #5408. Operational bug, not data loss.

## Problem

PR #5408 made `RaftReplicatedDatabase.commit()` take a phase-2 ticket from `ArcadeStateMachine`
before replication. The ticket records a Raft replay floor, and `takeSnapshot()` clamps its
durability checkpoint to the lowest in-flight floor so a Raft-committed but locally-unapplied entry
can never be buried below the replay position.

Tickets are retained by default and released only where the local pages are provably settled.

The `ReplicationDispatchedTimeoutException` branch (#4790, indeterminate replication outcome)
deliberately retains its ticket - correct, because the entry may still reach quorum and the retained
ticket is what keeps it replayable across a crash.

But the *normal* resolution of a #4790 timeout is that the entry later reaches quorum and is applied
by `ArcadeStateMachine.applyTxEntry` through the `abandonedLocalTransactions` consumption path. At
that moment the pages become durable, yet nothing released the ticket. The checkpoint stayed pinned
and Ratis stopped purging the Raft log past that index until the process restarted - on a healthy
node, with no crash involved.

Related to #5345 (unbounded Raft log growth until disk-full).

### Why it was not fixed in #5408

The release needs a ticket-to-`walTxId` correlation that did not exist: the ticket is created in
`commit()` before replication (no WAL txId associated yet), while
`markTransactionAbandonedForLocalApply` knows the `walTxId` but not the ticket.

## Fix

Carry the ticket alongside the abandoned marker so the two reconcile.

1. `abandonedLocalTransactions` changes from `Map<String, Long>` (key -> insertion time) to
   `Map<String, AbandonedPhase2>`, where `AbandonedPhase2(phase2Ticket, insertedAt)` records both.
2. `markLocalTransactionAbandoned(databaseName, walTxId, phase2Ticket)` stores the ticket;
   `RaftReplicatedDatabase` passes the ticket it already holds on the dispatched-timeout branch.
3. `applyTxEntry` releases the recorded ticket **only** on the branch that actually applied the
   transaction, and only **after** `applyChanges` returned without throwing - i.e. once the pages are
   on disk. The origin-skip branch never releases: releasing there would reintroduce #5407.
4. TTL pruning of markers is unchanged, and a pruned marker deliberately does **not** release its
   ticket: pruning is not proof the entry never committed, and if it does commit later it will be
   origin-skipped (unapplied), so the entry must stay replayable.

Ordering note: `lastAppliedIndex` advances in `applyTransaction` *after* `applyTxEntry` returns, so
at the moment of release a concurrent `takeSnapshot()` cannot yet report a checkpoint covering this
entry.

## Known gaps

`applyTxEntry` consumes the abandoned mark before `applyChanges` runs. If `applyChanges` throws
`WALVersionGapException`, the mark is gone but the ticket is (correctly) not released, and a snapshot
resync is triggered. Once the resync makes the entry durable, nothing releases that ticket: every
later replay origin-skips the entry because the mark is gone, so the checkpoint stays pinned until
the node restarts.

This is not a regression - before this change no release existed on any path - and the
version-gap-during-abandoned-apply combination is exotic. Retaining is also the safe direction, since
the resync may itself fail. It is left as a documented gap rather than fixed here because releasing on
a failed apply is exactly the shape of the #5407 bug, and the condition is not silent: it trips
`warnIfPhase2StallingCompaction` and shows up on the `arcadedb.ha.phase2.*` gauges below.

### Observability

The issue also asked for the pinned-compaction condition to be visible on a dashboard rather than
only in the throttled WARNING. Added, reusing the existing framework-agnostic provider seam so the
`ha-raft` module still needs no Micrometer dependency:

- `ArcadeStateMachine` exposes `pendingLocalPhase2Count()`, `oldestPendingLocalPhase2HeldMs()` and
  `lowestPendingLocalPhase2ReplayFloor()`.
- `HAReplicationStatsProvider.PendingPhase2Stats` carries them; `RaftHAServer` / `RaftHAPlugin`
  implement it.
- `HAReplicationMetrics` registers `arcadedb.ha.phase2.pending`,
  `arcadedb.ha.phase2.oldest_held_ms` and `arcadedb.ha.phase2.lowest_replay_floor`.

## Verification

Written before the fix (TDD), failing on the pre-fix code:

- `Issue5410AbandonedPhase2TicketTest` (9 cases) - the correlation and the release/retain branches,
  driving the real `replicateAndCommitLocally` dispatched-timeout path, plus the TTL-prune retain
  invariant this doc leans on.
- `Issue5410AbandonedTicketReleaseIT` - end-to-end on a 3-node Raft cluster, reusing the #4790
  fault injection: after the abandoned entry converges, the leader's pending phase-2 count must
  return to zero and no replay floor may stay pinned.
- `HAPendingPhase2MetricsTest` (4 cases) - the new gauges, including the HA-disabled and
  provider-default fallbacks.

The IT was confirmed to fail on the pre-fix code (the ticket stayed held through a 30 s poll) and to
pass after, with its clean-commit baseline assertion passing in both runs so the test is known to
discriminate. Regression: `Issue4790PhantomCommitOriginSkipIT`,
`RaftLeaderCrashBetweenCommitAndApplyIT`, `RaftLeaderCrashWithExternalPropertyIT`,
`Issue5407Phase2TicketLifecycleTest`, `ArcadeStateMachinePendingPhase2SnapshotTest` all pass; full
`ha-raft` unit suite 769 tests green, server monitor suite 46 tests green.

## PR and review history

https://github.com/ArcadeData/arcadedb/pull/5472

| Cycle | Head | Outcome |
|---|---|---|
| 1 | `7f2219c1a` | claude[bot]: "high-quality, well-scoped fix", nothing blocking. Flagged the WAL-version-gap residual edge and the unnecessarily widened accessor visibility. |
| 2 | `74a621f65` | Documented the residual edge in code and here; narrowed the three stat accessors back to package-private. claude[bot]: "nothing blocking", suggested reusing the named ticket sentinel. |
| 3 | `38640d8cd` | Replaced the bare `-1L` in `commit()` with `ArcadeStateMachine.NO_PHASE2_TICKET`. claude[bot]: "looks correct and safe to merge", suggested pinning the TTL-prune retain invariant in a test. |
| 4 | this commit | Added `aTtlPrunedMarkerKeepsItsTicketHeld`. |

gemini-code-assist did not respond in any cycle.

Deferred, for the developer to decide: claude[bot] suggested filing a follow-up issue so the two
"pinned until restart" residuals (TTL-pruned marker, WAL-version-gap) stay tracked against #5345.
Not filed here - the #5408 precedent was that filing the follow-up (#5410) was explicitly authorized
by the owner first.
