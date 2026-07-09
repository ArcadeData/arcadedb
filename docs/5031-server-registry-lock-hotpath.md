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

## PR
https://github.com/ArcadeData/arcadedb/pull/5168

## Review cycles
Bots: claude (issue comments) + gemini-code-assist (inline). Gemini re-posted the same "unwrapped DB during startup" comment on every SHA; its suggested code matched the already-applied ONLINE gate from cycle 1 onward, so it was a stale false-positive after the first cycle.

- Cycle 1 - `8a7debe`: Gemini + Claude flagged the fast path could return a pre-wrap (non-replicated) database while HA `rewrapDatabases()` runs during startup. Applied: gate the fast path on `status == STATUS.ONLINE` (status flips to ONLINE only after re-wrap completes), added `fastPathIsDisabledWhileServerNotOnline` regression test, tagged the IT `@Tag("slow")`. -> `63ffd17`.
- Cycle 2 - `63ffd17`: Claude approval with minor notes; asked to document the runtime snapshot-install interaction and fix the PR test-count. Applied: extended the fast-path comment for the runtime installer case; updated PR body to list 5 tests. Gemini re-posted the stale comment (no action). -> `aa4901a`.
- Cycle 3 - `aa4901a`: Claude "production change looks correct"; one pre-merge test-robustness ask - the negative gate test could pass for the wrong reason if the lookup thread was unscheduled. Applied: latch-gate the lookup thread so it counts down immediately before `getDatabase` and is awaited before the liveness check. Gemini stale re-post (no action). -> `caa22e0`.
- Cycle 4 - `caa22e0`: Claude "Neither blocks merge"; two non-blocking polish asks. Applied: routed the startup restore-path `databases.remove` through `removeDatabase()` for full defect-2 consistency; softened the "deflected with 503" comment so it does not read as a hard guarantee. Gemini stale re-post (no action). -> `7226a89`.

## Final state
max-cycles-reached (4 cycles run). All actionable review feedback was applied; the remaining bot output was Gemini's stale duplicate. Merge is the developer's responsibility. Note: `HTTPGraphIT` could not be run locally because an unrelated external ArcadeDB server (homebrew v26.6.1) held port 2480; the new IT and the lifecycle regressions (`PluginManagerTest`, `RemoteSafeCloseDatabaseIT`) pass. CI `mvn -pl server verify` should confirm green.
