# Issue #5848: RaftGroupCommitter applies quorumTimeout per entry instead of per batch

## Root cause

`RaftGroupCommitter.flushBatch()` dispatches a whole batch of entries to Ratis concurrently
(`raftClient.async().send(msg)` for every entry first), then waits on each resulting future
**sequentially**, each with the **full** `quorumTimeout`:

```java
for (int i = 0; i < batch.size(); i++) {
  ...
  final RaftClientReply reply = futures[i].get(quorumTimeout, TimeUnit.MILLISECONDS);
```

Since every future was already in flight before the wait loop starts, `quorumTimeout` is clearly
meant as a deadline for the whole batch. As written it was a fresh budget re-applied to every
entry, so a batch where N entries never reply (stalled/partitioned quorum) wedges the single
flusher thread (`arcadedb-raft-group-committer`) for `N x quorumTimeout` instead of
`quorumTimeout`. At the shipped defaults (`quorumTimeout=10000`, `groupCommitBatchSize=500`) the
worst case is 500 x 10s = 5000s (~83 minutes) with the replication queue filling up and every
client seeing `ReplicationQueueFullException` in the meantime.

A follow-up comment on the issue additionally points out a correctness consequence: entries later
in the batch can have their outcome resolved (by `flushBatch`) after the *caller's own*
`submitAndWait` budget (`3 x quorumTimeout`) has already expired. Those callers see a
`ReplicationDispatchedTimeoutException` (retry) for a write that may have actually committed
cluster-wide, risking a duplicate on client retry. Fixing the per-entry-vs-per-batch timeout also
narrows that window (an entry can only be reported "indeterminate" once, not after redundant waits
on preceding stalled entries burn through the caller's own budget).

## Fix

Compute a single deadline before the per-entry wait loop and derive each `Future#get` timeout from
what remains of it, mirroring the fix suggested in the issue:

```java
final long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(quorumTimeout);
for (int i = 0; i < batch.size(); i++) {
  ...
  final long remainingNanos = deadlineNanos - System.nanoTime();
  final RaftClientReply reply = futures[i].get(remainingNanos, TimeUnit.NANOSECONDS);
```

Negative/zero `remainingNanos` is passed straight through rather than special-cased: `CompletableFuture#get(timeout, unit)`
checks completion **before** looking at the timeout value, so an entry that already has a reply
by the time we reach it still completes instantly - it is not punished for sharing a batch with
earlier, slower/stalled entries. Only a not-yet-done future turns a non-positive remaining budget
into an immediate `TimeoutException`, which is exactly the existing "entry dispatched to Raft;
outcome unknown" classification the code already used for a real timeout (issue #4790).

No new imports were required (`TimeoutException` was already imported and used elsewhere in the
class).

## Test

Added `RaftGroupCommitterTest.stalledEntriesInOneBatchDoNotWedgeTheFlusherForTheNextEntry`:

- Mocks `RaftClient.async().send()` so the first 5 dispatched entries never reply (simulated
  stalled quorum) and any entry dispatched after that replies successfully and immediately.
- Submits 5 entries concurrently (fire-and-forget background threads) so they land in one batch
  with a stalled quorum, waits briefly for them to enqueue, then submits one more entry from the
  main thread.
- Asserts the extra entry's `submitAndWait` call **succeeds** and returns the expected log index.

This is deterministic regardless of exactly how the 5 stalling submissions split across
`drainTo()` calls: on the buggy code the flusher pays a full `quorumTimeout` per stalled entry
before it can loop back to the queue, *regardless* of how those entries were grouped into
batches, so the extra entry sits `PENDING` long enough that its own submitter-side
`2 x quorumTimeout` budget expires first and it gets cancelled
(`QuorumNotReachedException: ... cancelled before dispatch`) - reproducing the "queue fills up,
clients see errors" symptom from the issue. On the fixed code the flusher frees up within about
one `quorumTimeout` regardless of batch size, so the extra entry is dispatched normally and
completes.

Confirmed the test fails against the pre-fix code with exactly that cancellation error, then
passes after the fix.

### Test results

- `mvn -pl ha-raft test -Dtest=RaftGroupCommitterTest` - 20/20 passed (including the new
  regression test).
- `mvn -pl ha-raft test` (full module) - 615/615 passed before the run was interrupted by an
  unrelated, pre-existing environmental collision: a concurrently-running agent building the same
  `ha-raft` module in a sibling worktree on this shared machine held the fixed port `WaitForApplyTest`
  binds, producing `java.net.BindException: Address already in use`. Confirmed via `ps aux` that
  another `mvn ... -pl ha-raft ... -Dtest=...,WaitForApplyTest` process was running concurrently
  from a different worktree at the time. Not related to this change (`WaitForApplyTest` does not
  reference `RaftGroupCommitter`).

## Impact

- Bounds the group-committer flusher's stall on a stuck/partitioned quorum to `quorumTimeout`
  instead of `batchSize x quorumTimeout`, restoring throughput as soon as the cluster recovers
  instead of minutes to hours later.
- Reduces (does not eliminate - `submitAndWait`'s own `3 x quorumTimeout` budget is unchanged and
  intentionally so) the window in which a write that actually committed cluster-wide is reported to
  the client as indeterminate/retryable purely because of its position in a stalled batch.
- No behavioral change for the healthy-quorum path: every entry's real wait time is unchanged when
  replies arrive promptly.
