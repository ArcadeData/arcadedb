# Issue #5031 - Server registry: lock on request hot path, registry mutation bypasses lock, HA fields lack volatile

## Symptom
Three related defects in `ArcadeDBServer`'s database registry and HA wiring:
1. Every database-scoped request serialized on a single JVM-wide `synchronized (databasesLock)` monitor, even the pure lookup of an already-open database. The same monitor is held across long I/O (create/open, HA snapshot install close-swap-reopen), blocking all ~500 Undertow workers on every database.
2. `registerDatabase`/`removeDatabase` mutated the registry map without `databasesLock`, so they could race `getDatabase`'s check-then-open into two `DatabaseInternal` instances over the same directory.
3. `haServer` and `databaseWrapper` (written by the HA plugin during `startPlugins(AFTER_HTTP_ON)`, after the HTTP server already accepts requests) were non-`volatile`; under the JMM concurrent readers on worker threads could observe `null` indefinitely.

## Root cause
- Defect 1: the lookup fast path was inside the monitor with no lock-free short-circuit.
- Defect 2: the two registry mutators used `ConcurrentMap` atomics only, not the documented registry monitor that `getDatabase`/`createDatabase`/`rewrapDatabases` serialize on.
- Defect 3: missing `volatile` on fields mutated after startup.

## Fix
`server/src/main/java/com/arcadedb/server/ArcadeDBServer.java`:
- `getDatabase(...)`: added a lock-free fast path - `databases.get(name)` on the `ConcurrentMap`, returned immediately if present and open. Only a miss (absent/closed) falls through to the existing locked slow path, which re-checks under the lock, so `createIfNotExists`/`allowLoad` semantics are preserved.
- `registerDatabase`/`removeDatabase`: now wrap their map mutation in `synchronized (databasesLock)`.
- `haServer`, `databaseWrapper`, `security`, `httpServer`: declared `volatile` (last two for symmetry, same after-startup publication pattern).

## Tests
`server/src/test/java/com/arcadedb/server/Issue5031ServerRegistryConcurrencyIT.java` (4 tests, all pass; the three defect tests were confirmed red before the fix):
- `openDatabaseLookupDoesNotBlockOnRegistryLock` - holds `databasesLock` in a background thread and asserts a concurrent lookup of the open database still returns promptly (defect 1).
- `concurrentLookupsReturnSameInstance` - 32 threads x 100 lookups all observe one identity (defect 1 correctness).
- `removeDatabaseAcquiresRegistryLock` - `removeDatabase` blocks while the lock is held, completes after release (defect 2).
- `haRelatedFieldsAreVolatile` - reflection assertion that the four fields are `volatile` (defect 3).

Regression: `PluginManagerTest` (15) and `RemoteSafeCloseDatabaseIT` (1) pass. `HTTPGraphIT` is unrunnable in this environment because an unrelated external ArcadeDB server occupies port 2480 (401/403 to the test client); not caused by this change.

## Impact
Request hot path no longer serializes on a JVM-wide monitor for already-open databases; registry mutations are race-safe against concurrent opens; HA fields are reliably visible to worker threads immediately after plugin start. No API changes; behavior for the miss/slow path is unchanged.
