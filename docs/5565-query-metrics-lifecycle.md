# #5565 - query metrics die across an in-process server restart (PostgresQueryMetricsIT red on main)

## Symptom

`PostgresQueryMetricsIT.postgresQueriesTaggedWithPostgresProtocol` fails in CI, never in isolation:

```
Expecting actual: 0L to be greater than or equal to: 1L
  at PostgresQueryMetricsIT.java:76
```

The preceding `assertThat(timer).isNotNull()` passes, so the `arcadedb.query.duration` timer tagged
`protocol=postgres` exists in the registry but reports a count of zero.

## Root cause

Three facts combine.

1. `ArcadeDBServer.start()` called `Metrics.addRegistry(new SimpleMeterRegistry())` on every start and
   nothing removed it on stop. Micrometer's `Metrics.globalRegistry` is a JVM-wide composite, so a JVM
   that starts servers in sequence (every IT suite does, `reuseForks=true` and `forkCount=1`) accumulates
   one child registry per start. The full IT suite reaches double digits.

2. Registering a meter on a composite creates a `CompositeTimer` that holds one child meter **per child
   registry**, and `CompositeTimer.count()` returns `firstChild().count()` - the children live in an
   `IdentityHashMap`, so which child answers is arbitrary. When a registry is added *after* a recording,
   the child meter it gets is brand new and reads zero.

   A timer created and recorded in generation 1 therefore reports either its real count or 0 in
   generation 2, depending on identity hash order.

3. `Search.timer()` returns an arbitrary match. The postgresw IT suite produces several
   `protocol=postgres` timers, differing in `db`/`language`/`type`
   (`PostgresPreparedStatementMetricsIT` alone contributes `db=postgresdb, language=sql`), so the
   assertion is not necessarily handed the timer its own query recorded.

Put together: the test resolves a `protocol=postgres` timer created by an earlier server generation and
reads it through a child registry that never recorded into it, hence non-null with count 0.

The product defect this exposes is bigger than the test. Because `MicrometerQueryMetricsRecorder` caches
the resolved `Timer` in a static map and the meters themselves are never deregistered, after an
in-process restart `arcadedb.query.duration` is fed into meters whose values are spread across
abandoned registry generations, and every scrape reads one arbitrary generation. The same shape applies
to `arcadedb.http.requests` (same static cache in `AbstractServerHttpHandler`) and to the gauges bound
by `PoolMetrics`/`EngineMetricsBinder`/`HAReplicationMetrics`, whose re-registration on the second start
is silently ignored by Micrometer because a meter with that id already exists - so they keep reporting
the *stopped* server. Production runs one server per JVM, but embedded applications and tests restart in
place.

Two smaller leaks belonged to the same asymmetry: `new JvmGcMetrics()` was never closed (it holds
notification listeners on the GC MXBeans), and the `MeterFilter` backstop was re-appended to the global
config on every start, which is also what produced the `A MeterFilter is being configured after a Meter
has been registered` warnings all over the CI logs.

## Fix

`ArcadeDBServer` now installs and uninstalls the metrics subsystem symmetrically, in `startMetrics()`
and `stopMetrics()`:

- the server keeps a reference to the `SimpleMeterRegistry` (and the optional `LoggingMeterRegistry`) it
  added, and on stop removes it from the global composite and closes it;
- `JvmGcMetrics` is kept and closed on stop;
- the `MeterFilter` backstop is installed once per JVM, before any meter exists, which is what
  Micrometer requires;
- on stop the meters the server generations registered are removed from the global composite. The set of
  meters present before the *first* install is snapshotted and excluded, so meters an embedding
  application registered itself are left alone;
- the two static timer caches (`MicrometerQueryMetricsRecorder`, `AbstractServerHttpHandler`) are
  invalidated at the same point, because a cached `Timer` whose meter has been deregistered silently
  discards every sample - that would have converted the reporting bug into a total loss of query metrics
  after a restart;
- `QueryMetricsRecorder.Holder` is reset to `NO_OP`, so with no server left the engine stops timing
  queries instead of feeding a dismantled registry.

The install is **reference counted** (`METRICS_INSTALL_MUTEX`, `metricsInstalls`): HA and embedded
setups run several servers in one JVM, and each one removes only its own registry. The shared state -
meters, caches, recorder - is dismantled by the last server out, so a stopping node never strips the
meters its still-running siblings publish.

Registering the backing registry per server start is left as is: it is what makes each server's meters
reachable through the composite that the Prometheus and OTLP plugins attach to.

## Verification

`server/src/test/java/com/arcadedb/server/monitor/ServerMetricsLifecycleTest.java` drives real
`ArcadeDBServer` start/stop cycles. All five tests fail on the pre-fix code:

| test | pre-fix failure |
|---|---|
| `meterRegistryIsRemovedWhenTheServerStops` | the stopped server's registry is still in the composite |
| `queryTimersDoNotOutliveTheServerThatRecordedThem` | the meter survives the shutdown |
| `queryTimerRecordsIntoTheLiveRegistryAfterARestart` | two registries after one restart |
| `queryRecorderIsRetiredWhenTheServerStops` | the Holder keeps the recorder of the dead server |
| `metricsSurviveWhileAnotherServerInTheSameJvmIsRunning` | (guards the reference count) |

The recorder is exercised through `QueryMetricsRecorder.Holder.get()`, i.e. the same entry point the
engine uses, under a `protocol` tag unique to the test so its meters cannot collide with another test's.

```
mvn -pl server -am -Dtest=ServerMetricsLifecycleTest test
mvn -pl server -am -Dtest='MicrometerQueryMetricsRecorderTest,DefaultServerMetricsTest' test
mvn -pl postgresw -am verify -Pintegration            # the suite from the report, one JVM
```

## Impact

- Query and HTTP RED metrics keep recording, and keep reporting the values they recorded, across an
  in-process server restart.
- The per-start registry, GC-listener and MeterFilter leaks are gone, and with them the
  `MeterFilter is being configured after a Meter has been registered` warning.
- The postgresw metrics ITs become independent of suite order, because each server generation starts
  from a clean set of meters.
- Reported counters restart from zero when a server restarts in-process, which is the intended reading:
  the values belong to the server that recorded them.
