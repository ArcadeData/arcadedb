# Issue #6373: RaftGroupCommitter Quorum.ALL watch() loop has no per-batch deadline

## Root cause

`RaftGroupCommitter.flushBatch(...)` (`ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftGroupCommitter.java`)
computes a single `deadlineNanos` budget for the whole batch before its result-wait loop. The
`futures[i].get(remainingNanos, TimeUnit.NANOSECONDS)` call correctly derives its wait from what
remains of that budget (fixed by #6109 for #5848).

The `Quorum.ALL` branch immediately below it does not: it calls the synchronous, timeout-less
`raftClient.io().watch(logIndex, ALL_COMMITTED)` overload once per committed entry, with no
reference to `deadlineNanos` at all. Under `arcadedb.ha.quorum=all` with one follower down or
partitioned, majority is reached quickly (so the `get()` deadline gives no protection), but each
entry's `ALL_COMMITTED` watch then blocks for as long as the follower is unreachable - serially, on
the single `arcadedb-raft-group-committer` thread, for every entry in the batch. This is the same
per-entry-vs-per-batch granularity bug #5848 was about, in a code path #6109 did not touch.

## Affected components

- `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftGroupCommitter.java` (`flushBatch`)

## Expected vs actual behavior

- **Expected:** the `Quorum.ALL` watch shares the same per-batch deadline as the result-wait loop
  above it. A follower that never confirms `ALL_COMMITTED` within the remaining batch budget must
  fail that entry as `MajorityCommittedAllFailedException` (committed at MAJORITY, ALL unconfirmed)
  rather than blocking the flusher indefinitely.
- **Actual (before fix):** the watch call has no timeout at all. A stalled `ALL` confirmation blocks
  the flusher thread for as long as the follower is unreachable, once per entry in the batch, with no
  bound - not even `quorumTimeout`.

## Fix

Ratis's `BlockingApi.watch(long, ReplicationLevel)` has no timeout-bearing overload (verified against
`ratis-client:3.3.0` sources: `org.apache.ratis.client.api.BlockingApi#watch` takes only
`(long index, ReplicationLevel replication)`). `AsyncApi.watch(long, ReplicationLevel)` returns a
`CompletableFuture<RaftClientReply>`, so the same pattern already used for the result-wait loop
applies directly: call `raftClient.async().watch(index, level)` and bound it with
`.get(remainingNanos, TimeUnit.NANOSECONDS)`, computing `remainingNanos` from the same
`deadlineNanos` the entry's `futures[i].get()` already used. An expired budget (`TimeoutException`)
completes the entry as `MajorityCommittedAllFailedException`, the same outcome already used for a
negative `watchReply`.

## Test plan

- `RaftGroupCommitterTest.allQuorumWatchGivesUpAtTheBatchDeadlineInsteadOfBlockingIndefinitely`:
  mocks a `RaftClient` where `async().send()` succeeds immediately (MAJORITY reached) but ALL
  confirmation never arrives (both the blocking `io().watch()` and async `async().watch()` overloads
  never return/complete, simulating a down/partitioned follower). With `quorumTimeout` set short
  (200 ms), the fixed code must fail the entry with `MajorityCommittedAllFailedException` within a
  few hundred ms; before the fix this test hangs past its bounded `outcome.get(3, SECONDS)` and fails
  with a `TimeoutException`.
- Full `ha-raft` unit test module rerun to confirm no regressions.

## PR

https://github.com/ArcadeData/arcadedb/pull/6486

## Review cycles

- **Cycle 1** - head `bec509a9dc3094e812bf9ccc03f904e0aadec05e` (original PR push). `claude[bot]` review
  flagged one correctness bug plus two minor nits:
  - The new inner `catch (final Exception e)` around the `async().watch(...).get(remainingWatchNanos, ...)`
    call swallowed `InterruptedException` instead of restoring the interrupt flag and letting it reach the
    outer `catch (final InterruptedException ie)` fast-abort handler, so an interrupt during the `ALL`
    watch would silently clear the flag and let the rest of the batch fall through to full-length waits -
    the same unbounded-stall shape this PR was fixing, relocated to the interrupt path. Fixed by adding an
    explicit `catch (final InterruptedException ie)` before the generic `catch (Exception e)` that restores
    the flag and rethrows.
  - Minor: the `TimeoutException` branch dropped the cause when constructing
    `MajorityCommittedAllFailedException`. Fixed to pass `te` as the cause via the two-arg constructor.
  - Minor: the regression test still stubbed the now-unused `client.io().watch(...)` overload (dead since
    the fix moved to `async().watch()`). Removed the dead stub.
  - Also added `allQuorumWatchSucceedsWithinBudgetReturnsLogIndex`, a happy-path companion test for the
    `Quorum.ALL` success case (no prior test covered it).
  - Addressed in commit `476329f95332c34a907a320d30819aab78757ca3`, pushed back to the same branch.
- **Cycle 2** - head `476329f95332c34a907a320d30819aab78757ca3`. `claude[bot]` review (issue comment
  `5352530125`, posted 2026-08-20T07:01:15Z) was clean/non-blocking: it confirmed the interrupt-flag fix,
  approved the new regression + happy-path tests as deterministic and consistent with the file's existing
  mocking style, and raised only non-blocking notes (comment density, an existing/pre-existing
  non-cancellation-of-timed-out-futures characteristic, doc-file convention). No code changes were required.

## Final state

`clean-approval` after 2 review cycles. Merge remains the developer's responsibility.
