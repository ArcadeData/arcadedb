# Issue #5492 - a dropped materialized view refresh leaves the view permanently stale

- Issue: https://github.com/ArcadeData/arcadedb/issues/5492
- PR: https://github.com/ArcadeData/arcadedb/pull/5502 (refs #5492, does **not** close it)
- Split out: https://github.com/ArcadeData/arcadedb/issues/5503
- Branch: `fix/5492-ha-mv-page-version-replication-gap`
- Base: `main` at d3cb57b9f

## Scope

Issue #5492 bundles two defects. This branch fixes the second one only, and deliberately does **not**
close the issue.

| Defect | Status here |
|---|---|
| A refresh arriving while another is in flight is dropped, not queued, so the view can be silently stale | **Fixed** |
| The leader ships page versions followers never received (`WALVersionGapException`, snapshot-resync loop, lost writes) | **Not fixed - not reproduced** |

The issue itself records the replication root cause as "not yet proven". It still is. Shipping a
speculative change to the Raft commit path without a reproduction would risk making a data-loss bug
worse, so that half stays open.

## What was investigated for the replication half

A 3-node in-process Raft harness (`BaseRaftHATest`, whose `checkDatabasesAreIdentical()` already
routes through `DatabaseComparator` and fails on any cross-node page-version mismatch) was built and
run against an `INCREMENTAL` view under concurrent single-record transactions. Findings:

- **No page-version gap reproduced.** Zero `WALVersionGapException`, zero resync loops, across every
  configuration tried (writes on the leader, writes through a follower, with and without a UNIQUE
  index on the source type, 180 and 450 records).
- Two theories from the issue were checked against current code. `LocalSchema` is constructed with
  `wrappedDatabaseInstance` (`LocalDatabase.java:2423`), so the refresh does go through
  `RaftReplicatedDatabase` - the issue is right to rule that theory out.
- One genuine leader-side asymmetry was found but **not** shown to be reachable from the view path:
  `RaftReplicatedDatabase.recordFileChanges` drains buffered WAL into `walEntries`, but its
  replication guard omits `walEntries` from the condition, and the `finally` then clears the buffer.
  Its sibling `runWithCompactionReplication` guards correctly. Left alone here for lack of a
  reproduction; worth a look on its own.
- An **unrelated** failure surfaced: concurrent single-record transactions issued through a follower
  lose 60-70% of records and log `Invalid record size ... deleting record` on the affected buckets.
  The A/B control with no view fails identically, so it is not attributable to materialized views.
  Filed separately.

## The defect that is fixed

`MaterializedViewRefresher.fullRefresh` began with:

```java
if (!view.tryBeginRefresh())
  return;   // another refresh is running
```

The refresh is triggered from a post-commit callback, so the request that gets dropped belongs to a
transaction that has **already committed**. The in-flight refresh it deferred to started before that
commit and therefore reads a snapshot without it. Nothing reschedules. The view stays missing that
record until some later write happens to win the race - or forever, if none does.

This needs no HA to reproduce: a 3-node run showed the **leader's own** view holding 449 of 450
committed records.

It also explains part of the throughput collapse in the issue's A/B table. Every committing
transaction that touches the source type schedules a full `TRUNCATE` + re-run of the defining query,
so under single-record transactions the work degenerates towards a full rebuild per row.

## Fix

Requests are coalesced onto the running refresh instead of being dropped.

`MaterializedViewImpl` replaces the `AtomicBoolean refreshInProgress` with a three-state
`AtomicInteger`: `IDLE`, `RUNNING`, `RUNNING_PENDING`. Two operations are added:

- `markRefreshPendingIfRunning()` - hands a request to the running refresh; returns `false` if none
  is running, in which case the caller must run it itself.
- `finishRefreshPassAndCheckPending()` - ends one pass, returning `true` if a request arrived during
  it (ownership retained, another pass required) or releasing ownership and returning `false`.

Releasing ownership and testing for a pending request is one atomic step. Split into two, a request
registered in between would be seen by neither the outgoing owner nor the requester (which observed
the refresh as still running) - the same lost-wakeup that the original code had unconditionally.

`fullRefresh` now registers its request when it cannot take ownership, and the owner loops until no
request is outstanding. `tryBeginRefresh()` and `endRefresh()` keep their existing contracts, so the
existing guard tests are untouched.

Repeated requests during one pass collapse into a single further pass, which is the intended
behaviour: one rebuild that sees all of them is equivalent to N sequential rebuilds and far cheaper.

## Deliberately not done

True incremental view maintenance. `MaterializedViewChangeListener` still schedules a full refresh
regardless of `REFRESH INCREMENTAL`, as its comment says. Implementing per-record maintenance is a
feature with real correctness risk (wrong view contents rather than slow ones), not a bug fix, and it
is out of scope here. Coalescing already removes the redundant rebuilds, which is where the cost
actually was.

## Tests

`engine/src/test/java/com/arcadedb/schema/MaterializedViewConcurrentRefreshTest.java`:

- `refresherRegistersItsRequestWhenAnotherRefreshIsInFlight` - the regression guard. Drives the exact
  interleaving with no timing dependence: a refresh is in flight, `fullRefresh` is called, and the
  request must still be there when the in-flight pass ends. **Verified to fail** against the old
  drop-behaviour and pass with the fix.
- `refreshRequestedWhileRunningIsServicedByTheRunningRefresh`,
  `repeatedRequestsDuringOnePassCollapseIntoASingleFurtherPass`,
  `requestArrivingAfterTheRefreshFinishedIsRunByTheCaller` - state-machine transitions.
- `concurrentWritersLeaveTheViewReflectingEveryCommit` - end-to-end convergence under 4 concurrent
  writers. Note this one passes against the old code too (the race needs load to surface), so it
  guards against deadlock or a non-terminating drain loop rather than against the original defect.

## Verification

| Suite | Result |
|---|---|
| `engine` `MaterializedView*` (7 classes) | 61 pass |
| `engine` `com.arcadedb.schema.*Test` | 310 pass |
| `server` `HTTPMaterializedViewIT`, `RemoteMaterializedViewIT`, `Issue3941AsyncRefreshMaterializedViewIT` | 11 pass |
| `ha-raft` `RaftReplicationMaterializedViewIT` | 1 pass |

## Review cycles

### Cycle 1 - `e23c1947c`

`claude[bot]` raised three items. All resolved.

1. **Two unrelated MCP hybrid_search doc files in the diff.** Real, but not added by this work: the
   branch was cut from a local `main` carrying two unpushed commits (`f7ee51d01`, `a1959e343`), so
   they showed up in the diff against `origin/main`. Resolved by rebasing onto `origin/main` with the
   author's explicit authorization to force-push; the branch now carries only this work's commits.
2. **The error path could still drop a pending request.** Correct, and the sharper form of the very
   bug being fixed: `endRefresh()` did a plain `set(REFRESH_IDLE)`, clobbering a request registered
   during a failing pass. Fixed by `releaseRefreshAfterFailure()`, which CASes the release and reports
   whether a request was discarded. The request is deliberately not retried - a pass that just failed
   would likely fail again, and retrying a persistent failure would spin - so the discard is logged at
   WARNING and the view is left in a non-VALID status, making the staleness visible rather than
   silent. Covered by `aFailedPassReportsTheRequestItDiscardsInsteadOfClobberingIt`, verified to fail
   against the plain-write release.
3. **Tag the end-to-end test slow.** Applied `@Tag("slow")` at method level.

Declined: dropping `docs/5492-...md`. `docs/NNNN-*.md` is the established per-issue convention in this
repo, and this file records the negative result on the replication half, which the PR description only
summarizes.

Noted without action: under sustained single-record load one writer thread can bear repeated rebuilds
on others' behalf. Inherent to full-refresh-per-change, strictly better than the previous behaviour,
and genuinely addressed only by true incremental maintenance, which is out of scope here.

`gemini-code-assist` did not respond within the polling window.

### Cycle 2 - `f98df0708`

`claude[bot]` reported nothing blocking and independently walked the caller-run fallback
(`markRefreshPendingIfRunning()` returns `false` then `tryBeginRefresh()` succeeds), confirming the
new owner's snapshot necessarily includes the requesting thread's committed record. Three
comment-only nits taken:

1. The unreachable third branch of `finishRefreshPassAndCheckPending()` still pointed at
   `endRefresh()` as the error-path release, which it no longer is. Reworded as the defensive guard
   it actually is.
2. `endRefresh()` is public and releases with a plain write, so it silently discards a pending
   request - the class of bug this change removes elsewhere. It stays for the existing guard tests,
   with javadoc steering callers to the two CAS releases.
3. `newView(...)` builds a view over a type that does not exist; noted that this is fine because the
   state-machine tests never run a refresh.

Noted without action, both already covered above: the weaker recovery guarantee for non-INTERVAL
views after a discarded request (mitigated by the visible non-VALID status), and owner starvation
under sustained single-record load.

`gemini-code-assist` again did not respond.

### Cycle 3 - `4c45fa490`

`claude[bot]` again reported nothing blocking, and independently verified the coalesced-pass snapshot
ordering (the pass that clears `RUNNING_PENDING` is pass K; the pass that re-reads source data is
K+1, whose transaction begins after the flag was set, so every committed record is seen by some later
pass). One wording fix taken: the discard WARNING named `ERROR`, but
`MaterializedViewChangeListener` overwrites the status with `STALE` afterwards, so the message named
a state the operator never sees. It now says "left non-VALID and stale".

Two items raised and deliberately **not** fixed here:

- **The drain loop runs synchronously on the committing thread.** After-commit callbacks fire on the
  committing thread, so one unlucky writer can be pinned running repeated full rebuilds on behalf of
  the others, unbounded while writes keep arriving. This is strictly better than dropping the
  requests, but it concentrates unbounded work on a user commit path. Moving the refresh onto
  `DatabaseAsyncExecutor` or a dedicated bounded pool (per the concurrency guidance in `CLAUDE.md`)
  would keep the coalescing and get it off the hot path. Worth its own issue.
- **`ContinuousAggregate` has the identical defect.** `ContinuousAggregateImpl` still uses the
  `AtomicBoolean` drop-on-contention guard, and `ContinuousAggregateRefresher.incrementalRefresh` is
  driven post-commit from `SaveElementStep.java:154-156` - the same shape as the materialized view
  listener, so the same silent staleness. Verified, not fixed here: it needs its own tests, and the
  same three-state primitives port over mechanically.

Declined as optional: a test asserting end-to-end re-convergence after a discarded request. Forcing a
refresh to fail deterministically needs either a droppable source type (blocked - dropping the source
of a view is rejected) or a view constructed outside the schema with a deliberately invalid query,
which tests the fixture more than the behaviour. The state-machine level recovery (ownership released,
next caller takes it) is already covered.

### Final state

`clean-approval` - the last two reviews raised no blocking items and the only follow-ups were
comments and wording.
