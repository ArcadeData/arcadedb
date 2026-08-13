# Issue #5850: HAReplicationMetrics leaks a ScheduledExecutorService per bindTo()

## Root cause

`HAReplicationMetrics` already carried the fix infrastructure from a prior "chore: fixed flaky
tests" commit (`ff764e79a`): it implements `Closeable`, holds the per-follower gauge refresh
scheduler in a `followerMetricsScheduler` field, and `close()` shuts it down. That commit's own
message says the class was fixed "even though nothing in `src/main` currently binds it" - and
that was the residual bug. `ArcadeDBServer.startMetrics()` called
`new HAReplicationMetrics(this).bindTo(Metrics.globalRegistry)` and discarded the instance
immediately; nothing in production ever called `close()`, so the daemon thread named
`arcadedb-ha-follower-metrics` (and the `ScheduledExecutorService` behind it) outlived every
server stop/restart cycle.

## Fix

`server/src/main/java/com/arcadedb/server/ArcadeDBServer.java`:
- Added a `haReplicationMetrics` field, mirroring the existing `metricsJvmGc` field/lifecycle
  pattern already used for `JvmGcMetrics`.
- `startMetrics()` now keeps the `HAReplicationMetrics` instance in that field instead of
  discarding it.
- `stopMetrics()` now closes it (guarded with `CodeUtils.executeIgnoringExceptions`, matching how
  the other metrics binders are torn down) and nulls the field.

This follows the same start/stop symmetry already established for `metricsJvmGc`,
`metricsRegistry`, and `metricsLoggingRegistry` in the same method pair.

## Regression test

`server/src/test/java/com/arcadedb/server/monitor/ServerMetricsLifecycleTest.java`:
added `followerMetricsSchedulerThreadStopsWhenTheServerStops`, which starts a real embedded
`ArcadeDBServer`, asserts the `arcadedb-ha-follower-metrics` daemon thread is alive after
`start()`, then asserts (via Awaitility, since `shutdownNow()` interrupts asynchronously) that the
thread is gone within 5s of `stop()`.

Verified the test fails without the fix (red): `ConditionTimeoutException` - thread still alive 5s
after `stop()`. Passes with the fix (green).

## Test results

- `ServerMetricsLifecycleTest`: 7/7 passing (new test included).
- `HAReplicationMetricsTest`, `HAPendingPhase2MetricsTest`, `DefaultServerMetricsTest`,
  `EngineMetricsBinderTest`, `PoolMetricsTest`: 28/28 passing, no regressions.
- `mvn -pl server -am install -DskipTests`: BUILD SUCCESS.

## Scope note

The issue's final paragraph mentions two sibling leaks in the same "startService creates it,
stopService doesn't clean it up" class: `HAReplicationMetrics` (this issue) and
`PrometheusMetricsPlugin`. Checked `PrometheusMetricsPlugin.java` on this branch: it already
removes and closes its registry in `stopService()` (added in the same `ff764e79a` commit that
built `HAReplicationMetrics`'s `close()`), so no further action was needed there. `RaftHAPlugin`
(the third instance, issue #5890) was already fixed and merged in PR #5981. This PR is scoped to
`HAReplicationMetrics` only, per issue #5850.
