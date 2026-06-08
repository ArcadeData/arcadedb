# Issue #4519: TimeSeriesMaintenanceScheduler leaks DatabaseContext.INSTANCE ThreadLocal

## Root cause

`TimeSeriesMaintenanceScheduler.schedule()` ran each periodic maintenance pass on a fixed-size
`ScheduledExecutorService` pool (4 threads) shared across every database registered with the
scheduler. Each pass called `DatabaseContext.INSTANCE.init((DatabaseInternal) db)` to install the
database into the worker thread's ThreadLocal, but there was no matching cleanup in a `finally`
block.

Because the pool threads are reused across databases:

- The thread kept a strong reference to a (possibly closed) `DatabaseInternal`, adding GC pressure.
- A maintenance pass for DB-B could inherit DB-A's still-installed transaction context, so
  begin/commit could target the wrong database.

## Fix

`engine/src/main/java/com/arcadedb/engine/timeseries/TimeSeriesMaintenanceScheduler.java`

- Extracted the maintenance body into a package-private static `runMaintenance(Database, LocalTimeSeriesType, String)`
  so the init/cleanup contract is directly testable. The scheduled lambda now delegates to it.
- Wrapped the body in `try { ... } finally { DatabaseContext.INSTANCE.removeContext(dbInternal.getDatabasePath()); }`.

`removeContext(databasePath)` is preferred over the raw `ThreadLocal.remove()` suggested in the
issue: it removes only this database's per-thread entry and, when the thread's map becomes empty,
also clears the inherited ThreadLocal plus the `CONTEXTS` registry entry, keeping both structures
consistent. The `finally` also guarantees cleanup on the early `return` paths inside the body
(null engine) and on any thrown error.

## Tests

`engine/src/test/java/com/arcadedb/engine/timeseries/TimeSeriesMaintenanceSchedulerContextLeakTest.java`

- `maintenancePassRemovesDatabaseContextFromWorkerThread` - after a single pass on a pooled worker
  thread, the thread's `DatabaseContext` for the DB path is null.
- `maintenancePassDoesNotLeakContextAcrossDatabasesOnSameThread` - runs a pass for DB-A then DB-B on
  the same single-thread executor and asserts DB-A's context never survives into the DB-B pass and
  both contexts are cleaned afterward.

Both tests were confirmed to FAIL against the un-fixed code (DB-A context survived; AssertionFailedError)
and PASS with the fix.

## Verification

- `mvn -pl engine test -Dtest=TimeSeriesMaintenanceSchedulerContextLeakTest` -> 2/2 pass.
- Regression run with related TS tests (`TimeSeriesRetentionTest`, `TimeSeriesDownsamplingTest`,
  `DropTimeSeriesTypeTest`, `TimeSeriesGapAnalysisTest`) -> 42/42 pass.

## Impact

Behavior of maintenance passes is unchanged; the only difference is the per-thread context is now
removed after each pass. No public API change. The extracted static method is package-private.
