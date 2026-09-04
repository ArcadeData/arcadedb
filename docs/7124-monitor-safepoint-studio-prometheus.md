# Issue #7124 - four small findings from the round-6 module sweep

Tracking doc for `fix/7124-monitor-safepoint-studio-prometheus`.

The issue groups five independent one-site defects. Each is analysed, tested and fixed separately below.

## 1. `ServerMonitor`'s low-disk warning measures the wrong directory (server)

**Root cause** - `ServerMonitor.checkDiskSpace()` measures `new File(".")`, the JVM working directory. When the
databases live on a mounted volume (the normal container/Kubernetes layout) the check reports the container
filesystem and stays quiet while the data volume fills.

**Fix** - resolve `GlobalConfiguration.SERVER_DATABASE_DIRECTORY` and measure that. The directory may not exist yet
at startup, so the resolver walks up to the closest existing ancestor, which sits on the same filesystem the
databases will be created on. It falls back to the working directory when the setting is blank or unresolvable, so
the check never goes silent. The warning text now names the directory it measured.

## 2. The safepoint "spike" check compares two lifetime cumulative averages (server)

**Root cause** - `checkJVMHotSpot()` compared `TotalSafepointTime / SafepointCount` at this sample against the same
ratio at the previous sample. Both are lifetime cumulative averages, so after the first few minutes both are
dominated by history and their difference tends to zero. The message claims the spike is "from the last sampling",
which the numbers cannot express. Net effect: the warning fires during startup and then never again.

**Fix** - a `SafepointSpikeDetector` keeps the previous sample's raw counters and computes the average over the
INTERVAL (`deltaTime / deltaCount`), then compares that against the previous interval's average. Two samples are
needed before the first interval average exists and three before the first comparison, which is what "from the last
sampling" actually means.

## 3. Studio's BUILDING materialized-view poller is never cancelled (studio)

**Root cause** - `showMaterializedViewDetail()` starts a 3s `setInterval` when the view is BUILDING and clears it
only on re-entry into itself. Navigating to a regular type (`showTypeDetail`) or to a graph analytical view
(`showGavDetail`) rewrites `#dbTypeDetail` without cancelling the poller, which then keeps firing and rewrites the
status badge inside the pane the user is now looking at.

**Fix** - a `stopMaterializedViewAutoRefresh()` helper, called from every function that takes over the detail pane:
`showTypeDetail`, `showGavDetail`, `showMaterializedViewDetail` and the `displaySchema` reset path.

## 4. `PrometheusMetricsPlugin` parses `requireAuthentication` with `Boolean.valueOf` on a String-typed lookup (metrics, security)

**Root cause** - `Boolean.valueOf(configuration.getValue(key, "true"))`. Two defects:
1. `ContextConfiguration.getValue(String, T)` infers `T` from the `"true"` default and casts the stored value to
   `String`, so a value set programmatically as a `Boolean` throws `ClassCastException` at the call site.
2. `Boolean.valueOf` returns `false` for anything it cannot parse, so `requireAuthentication=ture` **fails open** and
   silently exposes `/prometheus` unauthenticated.

**Fix** - the key is declared as `SERVER_METRICS_PROMETHEUS_REQUIRE_AUTHENTICATION` in `GlobalConfiguration`
(`Boolean.class`, default `true`) so it is documented and discoverable, and the plugin reads it through a strict
parser that accepts a `Boolean`, `"true"` or `"false"` (case-insensitive, trimmed) and **fails closed with a WARNING
on anything else**. `GlobalConfiguration.coerce()` itself stays permissive on purpose - it runs inside the class's
static initializer, where a throw becomes an `ExceptionInInitializerError` that takes the engine down - so the
fail-closed rule lives at this security-relevant read site.

## 5. `ChannelBinary` error messages name a setting that does not exist (network)

**Root cause** - four chunk-size errors tell the operator to adjust `NETWORK_BINARY_MAX_CONTENT_LENGTH`. No such
setting exists; `maxChunkSize` is fed by `GlobalConfiguration.HA_REPLICATION_CHUNK_MAXSIZE`
(`arcadedb.ha.replicationChunkMaxSize`) from both `ChannelBinaryServer` and `ChannelBinaryClient`.

**Fix** - the four messages name `HA_REPLICATION_CHUNK_MAXSIZE.getKey()` rather than a hard-coded string, so a rename
of the setting cannot desynchronise them again.

## Caveat worth recording

`ServerMonitor` is currently **not instantiated** by `ArcadeDBServer` - the field is commented out at
`ArcadeDBServer.java:200`. Findings 1 and 2 are therefore correctness fixes to a class that runs no code today;
re-enabling the monitor is a separate decision and is deliberately out of scope here.

## Changes

| File | Change |
|---|---|
| `server/src/main/java/com/arcadedb/server/monitor/ServerMonitor.java` | `resolveDiskSpaceDirectory(ContextConfiguration)` + `SafepointSpikeDetector`/`SafepointSpike` |
| `engine/src/main/java/com/arcadedb/GlobalConfiguration.java` | new `SERVER_METRICS_PROMETHEUS_REQUIRE_AUTHENTICATION` (Boolean, default `true`) |
| `metrics/src/main/java/com/arcadedb/metrics/prometheus/PrometheusMetricsPlugin.java` | `isAuthenticationRequired(ContextConfiguration)`, strict parse, fails closed |
| `network/src/main/java/com/arcadedb/network/binary/ChannelBinary.java` | four chunk-size messages name `HA_REPLICATION_CHUNK_MAXSIZE.getKey()` |
| `studio/src/main/resources/static/js/studio-database.js` | `stopMaterializedViewAutoRefresh()` called by every detail-pane switcher |

## Tests

| Test | Covers |
|---|---|
| `server/src/test/java/com/arcadedb/server/monitor/Issue7124ServerMonitorTest.java` | findings 1 and 2 - 8 tests |
| `metrics/src/test/java/com/arcadedb/metrics/prometheus/Issue7124PrometheusRequireAuthenticationTest.java` | finding 4 - 7 tests |
| `network/src/test/java/com/arcadedb/network/binary/Issue7124ChunkSizeErrorMessageTest.java` | finding 5 - 5 tests |
| `studio/test/mv-autorefresh-cancel.test.js` | finding 3 - 9 tests |

The safepoint test pins the exact numbers the old check could not see: 100,000 safepoints totalling 100,000ms, then
an interval whose average pause TRIPLES (100 safepoints in 300ms). The lifetime average moves 1.0000ms -> 1.0020ms,
+0.2%, well under the 20% threshold - which is why the old check stayed silent - while the interval average moves
1.0ms -> 3.0ms, +200%.

Each new test was confirmed to fail before its fix: the studio and Java suites could not even resolve the new
symbols, and the `ChannelBinary` fix (a string change) was reverted in place to show 4 of its 5 tests go red.

## Test results

```
studio            npm test                                        67 tests, 0 failures
network           mvn -o -pl network test                        466 tests, 0 failures
metrics           mvn -o -pl metrics test                         17 tests, 0 failures
engine (config)   GlobalConfiguration*/ContextConfiguration*      42 tests, 0 failures
server (config)   *Settings*/*Config*/*ServerMonitor*            105 tests, 0 failures
```

## Impact

Findings 3, 4 and 5 change live behaviour: the Studio pane stops being overwritten, `/prometheus` no longer opens on
a typo (an operator with a typo'd value who was relying on the unauthenticated endpoint will now get 401 - the
warning in the log names the value and the setting), and four error messages point at a lever that exists. Findings
1 and 2 fix a class that is not instantiated today, so they change nothing at runtime until the monitor is
re-enabled.
