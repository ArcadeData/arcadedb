# Issue #6632 (item 1): an open() still loads a Graph Analytical View's CSR even when the session never uses it

Issue: https://github.com/ArcadeData/arcadedb/issues/6632

Addresses item 1 only. Item 2 of that issue is a documentation gap in `arcadedb-docs` for the
pre-existing `arcadedb.gavRestoreAwaitTimeout` setting, which is a different repository.

## Root cause / analysis

`LocalDatabase.open()` calls `GraphAnalyticalViewPersistence.restoreAll(database)`, which for each
persisted definition calls `builder.skipPersistence().restoreFromDiskOrBuildAsync()`. Since #6588 (merged) that
reads the persisted CSR from disk instead of rebuilding it by a full graph scan, which is a large win:
at 10M vertices / 40M edges the scan was 52,923 ms and the restore is 984 ms, 53x cheaper.

The read still happens on the open path, unconditionally, for every session. A database that HAS a view
therefore pays to load it at every open whether or not that session goes on to use it. On the same 10M
graph an open with no view configured at all is ~3 ms, so a session that opens the database, reads a
document type and closes spends ~984 ms of its ~987 ms lifetime loading a structure it never touches.

That is a different question from the one #6583 filed, not a smaller version of it. Creating a Graph
Analytical View declares that it should be *available*; it is not a statement that every process which
subsequently opens the database intends to traverse it. The sessions that pay and get nothing are the
short-lived ones, which are exactly the ones where a fixed cost dominates: a CLI query against a
document type, a backup, a schema inspection, a health check, an embedded session in a test suite that
opens and closes a database per test.

## Fix

Add a new `SCOPE.DATABASE` setting, `GAV_LAZY_RESTORE` (`arcadedb.gavLazyRestore`, `Boolean`, default
`false`). When `false` (the default), behaviour is unchanged: `open()` reads the persisted CSR exactly as
it does today. When `true`, `restoreFromDiskOrBuildAsync()` registers the view and reads nothing, and the
first lookup that would actually use the view pays the restore instead.

The hook is a new `GraphTraversalProvider.tryLazyActivate()` default method, called from the single
chokepoint in `GraphTraversalProviderRegistry.findProvider()` when a provider is not ready. It returns
`false` on the interface, so a provider that does not implement it is skipped exactly as it always was,
and `GraphAnalyticalView`'s override is itself gated on the flag, so with the flag off every state
returns `false` and the default path is untouched.

Three callers deliberately do *not* trigger a load:

- **The query planner.** `StatisticsProvider.exactMeanFromTraversalProvider()` asks whether acceleration
  exists while *costing a plan*. It now uses a new non-triggering `findProviderIfReady()`. Loading a
  structure there would move a multi-hundred-millisecond stall out of `open()`, where it is predictable
  and happens once, and into an arbitrary query's planning phase.
- **A `STALE` view.** Staleness is not-ready for a completely different reason than
  deferred-and-not-yet-loaded. Whether a stale view may serve a query belongs to `GAV_USE_WHEN_STALE` and
  whether it gets refreshed belongs to the update mode; a first-use load must not override either by
  starting a rebuild that would not otherwise run.
- **`restoreAll()`'s `GAV_RESTORE_AWAIT_TIMEOUT` wait.** A deferred view is `NOT_BUILT` with no build in
  flight, and `readyLatch` is created at construction rather than left null, so `awaitReady()` would sit
  on a latch nobody counts down and burn the whole budget before returning `false`. The two settings on
  together would otherwise give an `open()` that blocks for the full timeout *and* still has not loaded
  the view.

## Behaviour changes when the flag is on

- The first query that could use the view pays the restore. The work is conserved, not removed: it
  reappears in full on that query and the second query is unaffected.
- A restore failure (missing file, changed definition, invalidated certificate) is discovered at first
  use rather than at open, and falls back to `buildAsync()` exactly as the eager path does.
- Where there is **no usable persisted CSR**, the eager path has the rebuild running from `open()`, so a
  session that opens and queries some seconds later can find the view ready; under lazy the rebuild does
  not start until the first query asks, so that session now misses. Laziness cannot both do no work at
  open and have the work already done. This is the price, and the main reason the default is `false`.

## Files changed

- `engine/src/main/java/com/arcadedb/GlobalConfiguration.java` - new `GAV_LAZY_RESTORE` setting.
- `engine/src/main/java/com/arcadedb/graph/GraphTraversalProvider.java` - new `tryLazyActivate()` default method.
- `engine/src/main/java/com/arcadedb/graph/GraphTraversalProviderRegistry.java` - `findProvider()` gives a not-ready provider one chance to activate; new non-triggering `findProviderIfReady()`.
- `engine/src/main/java/com/arcadedb/graph/olap/GraphAnalyticalView.java` - `tryLazyActivate()` override, gated on the flag, leaving `STALE` alone.
- `engine/src/main/java/com/arcadedb/graph/olap/GraphAnalyticalViewBuilder.java` - `restoreFromDiskOrBuildAsync()` returns without reading anything under the flag.
- `engine/src/main/java/com/arcadedb/graph/olap/GraphAnalyticalViewPersistence.java` - `restoreAll()` skips the await when there is nothing to await.
- `engine/src/main/java/com/arcadedb/query/opencypher/optimizer/statistics/StatisticsProvider.java` - planner uses the non-triggering lookup.
- `engine/src/test/java/com/arcadedb/graph/olap/GraphAnalyticalViewTest.java` - five regression tests.

## Test plan

- `lazyRestoreLeavesTheViewUnloadedAtOpenAndLoadsItOnFirstUse` - after reopen the view is `NOT_BUILT` and
  not restored; the first `findProvider()` returns it, and it is then `READY`, restored from the
  persisted CSR (not a fresh scan), with the right node and edge counts.
- `lazyRestoreIsOffByDefaultSoOpenStillRestoresEagerly` - the default path is unchanged.
- `lazyRestoreIsNotTriggeredByThePlannersReadinessCheck` - `findProviderIfReady()` returns `null` and
  leaves the status exactly as it was, while `findProvider()` still loads.
- `lazyActivationLeavesAStaleViewAlone` - a stale view stays `STALE`. Fails on unpatched code with
  `BUILDING`.
- `lazyRestoreDoesNotWaitOutTheRestoreAwaitTimeout` - `open()` does not sit out a 60s await budget.
  Fails on unpatched code at 60.33 s against a 20 s tripwire (1.79 s with the fix). Measured with
  `StallAwareStopwatch`, as a tripwire between "did not wait" and "waited out the budget", not a
  latency budget.
- Full `GraphAnalyticalViewTest`, `GraphTraversalProvider*`, `CSR*`, `StatisticsProviderTest` and the
  openCypher cardinality/optimizer suites run for regressions: 242 tests, all green.
