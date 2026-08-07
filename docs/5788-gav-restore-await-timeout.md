# Issue #5788: Graph Analytical View is never READY in time to serve the query that triggered it

Issue: https://github.com/ArcadeData/arcadedb/issues/5788

## Root cause / analysis

`LocalDatabase.open()` calls `GraphAnalyticalViewPersistence.restoreAll(database)` after
`openInternal()`. `restoreAll()` reads each persisted GAV definition and calls
`builder.skipPersistence().buildAsync()`, which fires the CSR rebuild on the shared
`gav-worker-` virtual-thread executor and returns immediately - the method does not wait for
any of the triggered builds to reach `READY`.

The reporter measured (26.8.1.dev23, 10k/100k/1M/5M-vertex graphs) that a query issued right
after `open()` always beats the background rebuild, always falls back to the unaccelerated OLTP
path, and at 3 of the 4 sizes the "cold" session (view present, still building) is *slower*
end-to-end than a database with no view configured at all, because the query competes with the
background scan for CPU.

The issue frames this as an open question rather than a bug report and proposes a much larger
change (persisting the CSR itself to disk, so a reopen loads it instead of rebuilding it) but
explicitly declines to prototype it ("I did not want to write several hundred lines of new
on-disk format against a guess"). Persisting the derived CSR structure is a large, high-risk
undertaking (new on-disk paginated format, staleness handling, a format review) that does not
belong in an unattended single-issue fix.

The `GraphAnalyticalView` class already has the building block the issue's ask actually needs:
`awaitReady(timeout, unit)`, plus `GraphAnalyticalViewRegistry.getAll()`/`getReady()`. What is
missing is a *supported, opt-in* way to make `open()` itself wait for restored views, so that
for callers who choose it, the session that pays for the rebuild is once again the session that
benefits - without inventing a new persistence format.

## Fix

Add a new `SCOPE.DATABASE` setting, `GAV_RESTORE_AWAIT_TIMEOUT`
(`arcadedb.gavRestoreAwaitTimeout`, `Long`, default `0`). When `0` (the default), behavior is
byte-for-byte unchanged: `restoreAll()` triggers the async rebuild and returns immediately, same
as today. When set to a positive number of milliseconds, `restoreAll()` still triggers every
build asynchronously (so a single stuck/slow view cannot block the others beyond the shared
budget), then blocks the calling `open()` for up to that many milliseconds total, waiting for
each triggered view to reach `READY` (or fail/go `STALE`) via the existing `awaitReady()`.

This is scoped to the actual reproducible defect (the session that rebuilds is never the one
that benefits) without taking on CSR-to-disk persistence, which remains open ground for a
follow-up if the maintainers want it (tracked by the issue's own text, and related issue #5747).

## Files changed

- `engine/src/main/java/com/arcadedb/GlobalConfiguration.java` - new `GAV_RESTORE_AWAIT_TIMEOUT` setting.
- `engine/src/main/java/com/arcadedb/graph/olap/GraphAnalyticalViewPersistence.java` - `restoreAll()` optionally awaits restored views up to the configured budget.
- `engine/src/test/java/com/arcadedb/graph/olap/GraphAnalyticalViewTest.java` - regression test.

## Test plan

- New test reproducing the issue: reopen a database with a persisted GAV and
  `GAV_RESTORE_AWAIT_TIMEOUT` set to a positive value - the view must be `READY` with the correct
  node/edge counts *immediately* after `open()` returns, with no additional `awaitReady()` call
  from the test.
- Existing `gavRestoredOnDatabaseReopen` test continues to prove the default (`0`, non-blocking)
  behavior is unchanged.
- Full `GraphAnalyticalViewTest` suite run to check for regressions.

## Results

- TDD: the new test (`gavRestoreAwaitTimeoutBlocksOpenUntilViewsAreReady`) was first proven unable
  to compile without `GAV_RESTORE_AWAIT_TIMEOUT` (the setting did not exist), then, once the
  setting existed but the wait logic in `restoreAll()` was stubbed out (`if (false && ...)`),
  proven to fail with `Expecting value to be true but was false` - confirming the assertion
  actually exercises the fix rather than passing vacuously. Restoring the real wait logic turns it
  green.
- `mvn -pl engine -am test -Dtest=GraphAnalyticalViewTest`: **133/133 passed**, 0 failures, 0
  errors (11.74s). No regressions in any existing GAV persistence/reopen/incremental-update test.
- `mvn -pl engine -am test -Dtest=GlobalConfigurationTest,ContextConfigurationTest,ConfigurationTest`:
  all passed (10 + 30 + 4 tests).
- `mvn -pl engine -am compile`: clean.

## Impact analysis

- **Default behavior is unchanged.** `GAV_RESTORE_AWAIT_TIMEOUT` defaults to `0`, so
  `restoreAll()` triggers the async rebuild and returns immediately exactly as before; existing
  deployments see no behavior change and no performance cost unless they opt in.
- **Opt-in cost is a slower `open()`.** Setting the timeout trades `open()` latency for the
  restored GAV being immediately usable, bounded by the configured budget (shared across all
  restored views in that database, not per-view) so one slow/stuck view cannot block the others
  beyond the configured total.
- Does not address CSR-to-disk persistence (the issue's larger, explicitly-speculative ask);
  the async rebuild itself is unchanged, only whether `open()` waits for it.

## Recommendations / follow-up

- Persisting the CSR itself to disk (issue's original ask) remains open ground for a dedicated,
  reviewed follow-up if the maintainers want it - the issue explicitly frames that as a design
  question, and #5747 is called out as a cautionary precedent for a persisted-vs-rebuilt check
  the load path.
- Consider surfacing `GAV_RESTORE_AWAIT_TIMEOUT` in the embedded-deployment docs alongside
  `GAV_USE_WHEN_STALE`, since both trade correctness/latency in the same "read-mostly analytics"
  use case the issue describes.
