# Issue #5033 - Shared mutable state cleanup (server module audit)

## Symptom
Several small shared-mutable-state hazards in the `server` module:
1. (HIGH) `FileServerEventLog.dateFormat` is a single shared `SimpleDateFormat`, formatted from many threads via `reportEvent()`. `SimpleDateFormat` is not thread-safe: concurrent `format()` corrupts its internal `Calendar`, yielding garbage timestamps or throwing `ArrayIndexOutOfBoundsException` into the caller thread.
2. (LOW) `HttpAuthSessionManager.close()` clears the session `HashMap` without the write lock; the `sessions.isEmpty()` pre-check and `getActiveSessionCount()` read the map with no lock. `HttpSessionManager.checkSessionsValidity()` pre-check and `getActiveSessions()` do the same.
3. (LOW) `PluginManager.plugins` is a plain `LinkedHashMap`; `getPluginNames()` returns a live `keySet()` view that throws `ConcurrentModificationException` if a late `registerPlugin` mutates the map while a request thread iterates.
4. (LATENT) `ServerMonitor.lastHeapWarningReported` / `lastDiskSpaceWarningReported` are written by the monitor thread and read by `getStatus()` from any thread without `volatile`.

## Root cause
Non-thread-safe shared state accessed concurrently without synchronization / immutability.

## Fix
1. `FileServerEventLog`: replace the shared `SimpleDateFormat` field with a `static final DateTimeFormatter` (immutable, thread-safe). Timestamp formatting extracted into package-visible `formatEventTime(long)` for testability.
2. Session managers: wrap `close()` clear, the `isEmpty()` pre-checks and the active-session reads under the read/write lock.
3. `PluginManager`: back `plugins` with `Collections.synchronizedMap(new LinkedHashMap<>())` (preserves insertion order used by start/stop) and return an insertion-ordered snapshot copy from `getPluginNames()` under the map monitor.
4. `ServerMonitor`: mark the two cross-thread warning-timestamp fields `volatile`.

## Tests
- `FileServerEventLogConcurrencyTest` - hammers `formatEventTime` from many threads with a fixed instant; asserts every result equals the expected well-formed timestamp and no exception escapes (fails before the fix with the shared `SimpleDateFormat`).
- `HttpAuthSessionManagerConcurrencyTest` - concurrent create / read-count / validity / close; asserts no exception and consistent counts.
- `PluginManagerConcurrencyTest` - iterates `getPluginNames()` while a thread keeps calling `registerPlugin`; asserts no `ConcurrentModificationException`.

## Impact
Server event-log timestamps are now correct under load; session-manager and plugin-manager reads no longer race their mutators. No public API change.

## Pull request
https://github.com/ArcadeData/arcadedb/pull/5170

## Review cycles
- Cycle 1 (ee26ad8a6): both bots flagged that the `PluginManager` hardening was incomplete - `getPlugins()`
  (called from the HA metrics thread), `startPlugins()`, `stopPlugins()` and the discovery-warning loop
  still iterated the synchronized-map views without holding the monitor. Snapshotted those under the
  `plugins` monitor before iterating. Also made `ServerMonitor.safepointMonitoringAvailable` volatile and
  made the plugin stress test hermetic / lighter (20k registrations, `@AfterEach` stops the server).
- Cycle 2 (ec8163f45): removed the internal review-notes file that had been committed by mistake.
- Cycle 3 (612d30081): clean approval - Claude reported no blocking issues; Gemini only re-posted
  already-addressed comments. Timezone stays `ZoneId.systemDefault()` to preserve the prior
  `SimpleDateFormat` semantics (a UTC switch would change existing deployments' log timestamps and is out
  of scope for a thread-safety fix).

## Final state
clean-approval - merge remains with the developer.
