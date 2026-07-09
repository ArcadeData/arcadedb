# Review notes for PR #5170 - head ee26ad8a6

## Applied (cycle 1)
- PluginManager: synchronize the remaining synchronized-map view iterations under the `plugins` monitor
  (`getPlugins()`, `startPlugins()`, `stopPlugins()` incl. the classloader-close loop, and the discovery
  warning loop) by snapshotting into a local list before iterating. Both bots flagged that these were
  still exposed to `ConcurrentModificationException`, `getPlugins()` most importantly since it is called
  from the HA metrics thread while `registerPlugin` may run at startup.
- ServerMonitor: `safepointMonitoringAvailable` made `volatile` (written by the monitor thread, read by
  `getStatus()`), matching the two warning-timestamp fields.
- PluginManagerConcurrencyTest: added `@AfterEach` stopping the server (hermetic), and reduced the stress
  registration count 200k -> 20k to keep the test light in CI.

## Skipped - disagree with justification

### Gemini: use `ZoneId.of("UTC")` instead of `ZoneId.systemDefault()` in FileServerEventLog
Skipped to preserve existing behavior. The previous `SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")` formatted
in the JVM default timezone; switching to UTC would silently change the timestamp values written to every
existing deployment's event log, which is a behavior change out of scope for a thread-safety fix. Claude's
review independently confirmed that `.withZone(ZoneId.systemDefault())` "preserves the prior SimpleDateFormat
zone/format semantics". A UTC switch is a reasonable separate enhancement but should be its own decision.

### Claude/Gemini: add `@Tag("slow")` to the concurrency tests
Skipped. Each new test runs sub-second (FileServerEventLogConcurrencyTest ~0.2s, PluginManagerConcurrencyTest
~0.1s, HttpAuthSessionManagerConcurrencyTest ~1s), so none meet the "noticeably long / multi-second" bar for
`@Tag("slow")`. Tagging them would exclude them from the standard CI build, removing their regression value.
The memory concern behind Gemini's suggestion was addressed instead by lowering the registration count to 20k.
