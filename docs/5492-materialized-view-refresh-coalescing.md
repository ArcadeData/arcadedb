# Issue #5492 - a dropped materialized view refresh leaves the view permanently stale

- Issue: https://github.com/ArcadeData/arcadedb/issues/5492
- Branch: `fix/5492-ha-mv-page-version-replication-gap`
- Base: `main` at f7ee51d01

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
