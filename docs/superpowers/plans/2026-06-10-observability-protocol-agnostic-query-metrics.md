# Protocol-Agnostic Query Metrics Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**GitHub issue:** [#4535](https://github.com/ArcadeData/arcadedb/issues/4535) - follow-up to #4465 (Observability Pillar 1), part of #4463.

**Goal:** Record the `arcadedb.query.duration` RED timer once at the engine boundary (`LocalDatabase.query()/command()`), tagged by the originating wire `protocol`, so Bolt, Postgres, Mongo, Redis, gRPC and HTTP are all covered by a single instrumentation point.

**Architecture:** The engine module has Micrometer only at **test** scope, so engine main code cannot call Micrometer (the same constraint that put `PoolMetrics`/`EngineMetricsBinder` in `server`). The engine therefore exposes a framework-agnostic `QueryMetricsRecorder` hook (plain Java) plus a `ProtocolContext` thread-local; `LocalDatabase` times each query/command and invokes the hook. The `server` module registers a Micrometer-backed recorder that emits `arcadedb.query.duration{protocol, db, language, type}`. Each wire listener sets `ProtocolContext` at its request boundary and clears it in `finally`. The HTTP path drops its now-redundant per-handler timer.

**Tech Stack:** Java 21, Micrometer 1.16.5 (server scope only), JUnit 5 + AssertJ. Modules touched: `engine`, `server`, `bolt`, `postgresw`, `mongodbw`, `redisw`, `grpcw`.

---

## File Structure

- **Create** `engine/src/main/java/com/arcadedb/database/QueryMetricsRecorder.java` - framework-agnostic hook interface + static `Holder` (no-op default, `startNanos()`/`record()` helpers). No Micrometer.
- **Create** `engine/src/main/java/com/arcadedb/database/ProtocolContext.java` - `ThreadLocal<String>` carrying the originating wire protocol (default `internal`).
- **Modify** `engine/src/main/java/com/arcadedb/database/LocalDatabase.java` - wrap the 7 terminal `query`/`command` call sites with gated timing.
- **Create** `server/src/main/java/com/arcadedb/server/monitor/MicrometerQueryMetricsRecorder.java` - Micrometer impl emitting `arcadedb.query.duration`.
- **Modify** `server/src/main/java/com/arcadedb/server/ArcadeDBServer.java` - register the recorder in the `SERVER_METRICS` block.
- **Modify** `server/src/main/java/com/arcadedb/server/http/handler/AbstractServerHttpHandler.java` - set/clear `ProtocolContext` = `http`.
- **Modify** `server/src/main/java/com/arcadedb/server/http/handler/PostCommandHandler.java` and `PostQueryHandler.java` - remove the now-redundant `timeExecution` wrapper.
- **Modify** the wire listeners: `bolt/.../BoltNetworkExecutor.java`, `postgresw/.../PostgresNetworkExecutor.java`, `mongodbw/.../MongoDBDatabaseWrapper.java`, `redisw/.../RedisNetworkExecutor.java`, `grpcw/.../ArcadeDbGrpcService.java` - set/clear `ProtocolContext`.
- **Create** tests under each affected module's `src/test`.

---

## Phasing

- **Phase A (engine core):** Tasks 1-3. Hook + thread-local + engine timing. Self-contained, no behavior change (no-op until a recorder registers).
- **Phase B (server + HTTP):** Task 4. Micrometer recorder, registration, HTTP refactor. After this, `arcadedb.query.duration{protocol=http}` is live and Pillar 1's `QueryRedMetricsIT` still passes.
- **Phase C (other protocols):** Tasks 5-9 (Bolt, Postgres, Mongo, gRPC, Redis). Each independent.
- **Phase D:** Task 10. Cardinality guard, docs, broader suites.

---

## Task 1: `QueryMetricsRecorder` hook (engine, no Micrometer)

**Files:**
- Create: `engine/src/main/java/com/arcadedb/database/QueryMetricsRecorder.java`
- Test: `engine/src/test/java/com/arcadedb/database/QueryMetricsRecorderTest.java`

- [ ] **Step 1: Write the failing test**

```java
package com.arcadedb.database;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class QueryMetricsRecorderTest {
  @AfterEach
  void reset() {
    QueryMetricsRecorder.Holder.register(null); // back to NO_OP
  }

  @Test
  void defaultIsNoOpAndStartNanosReturnsZero() {
    assertThat(QueryMetricsRecorder.Holder.get()).isSameAs(QueryMetricsRecorder.NO_OP);
    assertThat(QueryMetricsRecorder.Holder.startNanos()).isZero();
  }

  @Test
  void registeredRecorderReceivesRecordWhenTimingActive() {
    final AtomicReference<String> captured = new AtomicReference<>();
    QueryMetricsRecorder.Holder.register(
        (protocol, database, language, type, durationNanos) -> captured.set(protocol + "/" + database + "/" + language + "/" + type));

    final long start = QueryMetricsRecorder.Holder.startNanos();
    assertThat(start).isNotZero(); // an active recorder makes startNanos() time
    QueryMetricsRecorder.Holder.record(start, "db1", "sql", "query");

    assertThat(captured.get()).isEqualTo(ProtocolContext.get() + "/db1/sql/query");
  }

  @Test
  void recordWithZeroStartIsIgnored() {
    final AtomicReference<Boolean> called = new AtomicReference<>(false);
    QueryMetricsRecorder.Holder.register((p, d, l, t, n) -> called.set(true));
    QueryMetricsRecorder.Holder.record(0L, "db1", "sql", "query");
    assertThat(called.get()).isFalse();
  }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `mvn -q -pl engine test -Dtest=QueryMetricsRecorderTest`
Expected: FAIL - `QueryMetricsRecorder` does not exist (compile error). (`ProtocolContext` is created in Task 2; if compilation blocks, create Task 2's file first, then return.)

- [ ] **Step 3: Implement `QueryMetricsRecorder`**

```java
package com.arcadedb.database;

/**
 * Framework-agnostic hook the engine calls to report the duration of a query/command execution.
 * Lives in the engine (which has Micrometer only at test scope); the server registers a
 * Micrometer-backed implementation. Default is a no-op, so when no recorder is registered the
 * engine pays only a volatile read and a sentinel check per query - no timing, no allocation.
 */
@FunctionalInterface
public interface QueryMetricsRecorder {
  QueryMetricsRecorder NO_OP = (protocol, database, language, type, durationNanos) -> {
  };

  /**
   * @param protocol     originating wire protocol (e.g. http, bolt, postgres, internal)
   * @param database     database name
   * @param language     query language (sql, opencypher, ...)
   * @param type         query or command
   * @param durationNanos elapsed nanoseconds
   */
  void record(String protocol, String database, String language, String type, long durationNanos);

  /** Static registration + timing helpers, kept off the interface's public surface. */
  final class Holder {
    private static volatile QueryMetricsRecorder current = NO_OP;

    private Holder() {
    }

    public static void register(final QueryMetricsRecorder recorder) {
      current = recorder != null ? recorder : NO_OP;
    }

    public static QueryMetricsRecorder get() {
      return current;
    }

    /** Returns System.nanoTime() when a recorder is active, otherwise 0 (the "not timing" sentinel). */
    public static long startNanos() {
      return current == NO_OP ? 0L : System.nanoTime();
    }

    /** Records elapsed time since {@code start}; a {@code start} of 0 means timing was inactive and is ignored. */
    public static void record(final long start, final String database, final String language, final String type) {
      if (start == 0L)
        return;
      current.record(ProtocolContext.get(), database, language, type, System.nanoTime() - start);
    }
  }
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `mvn -q -pl engine test -Dtest=QueryMetricsRecorderTest`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add engine/src/main/java/com/arcadedb/database/QueryMetricsRecorder.java \
        engine/src/test/java/com/arcadedb/database/QueryMetricsRecorderTest.java
git commit -m "feat(observability): engine QueryMetricsRecorder hook (no Micrometer) (#4535)"
```

---

## Task 2: `ProtocolContext` thread-local (engine)

**Files:**
- Create: `engine/src/main/java/com/arcadedb/database/ProtocolContext.java`
- Test: `engine/src/test/java/com/arcadedb/database/ProtocolContextTest.java`

- [ ] **Step 1: Write the failing test**

```java
package com.arcadedb.database;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ProtocolContextTest {
  @Test
  void defaultsToInternal() {
    ProtocolContext.clear();
    assertThat(ProtocolContext.get()).isEqualTo(ProtocolContext.INTERNAL);
  }

  @Test
  void setAndClear() {
    ProtocolContext.set("bolt");
    assertThat(ProtocolContext.get()).isEqualTo("bolt");
    ProtocolContext.clear();
    assertThat(ProtocolContext.get()).isEqualTo(ProtocolContext.INTERNAL);
  }

  @Test
  void nullResetsToInternal() {
    ProtocolContext.set("http");
    ProtocolContext.set(null);
    assertThat(ProtocolContext.get()).isEqualTo(ProtocolContext.INTERNAL);
  }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `mvn -q -pl engine test -Dtest=ProtocolContextTest`
Expected: FAIL - `ProtocolContext` does not exist.

- [ ] **Step 3: Implement `ProtocolContext`**

```java
package com.arcadedb.database;

/**
 * Carries the originating wire protocol of the current request on a thread-local so the engine can
 * tag {@code arcadedb.query.duration} without each protocol re-implementing query timing. Wire
 * listeners {@link #set} it at the request boundary and {@link #clear} it in a finally block (worker
 * and connection threads are pooled/reused). Defaults to {@link #INTERNAL} for engine-internal and
 * embedded-API callers.
 */
public final class ProtocolContext {
  public static final String INTERNAL = "internal";

  private static final ThreadLocal<String> CURRENT = ThreadLocal.withInitial(() -> INTERNAL);

  private ProtocolContext() {
  }

  public static void set(final String protocol) {
    CURRENT.set(protocol != null ? protocol : INTERNAL);
  }

  public static String get() {
    return CURRENT.get();
  }

  public static void clear() {
    CURRENT.remove();
  }
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `mvn -q -pl engine test -Dtest=ProtocolContextTest`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add engine/src/main/java/com/arcadedb/database/ProtocolContext.java \
        engine/src/test/java/com/arcadedb/database/ProtocolContextTest.java
git commit -m "feat(observability): engine ProtocolContext thread-local (#4535)"
```

---

## Task 3: Time query/command in `LocalDatabase`

**Files:**
- Modify: `engine/src/main/java/com/arcadedb/database/LocalDatabase.java` (the 7 terminal `query`/`command` overloads, ~lines 1559-1631)
- Test: `engine/src/test/java/com/arcadedb/database/LocalDatabaseQueryMetricsTest.java`

Context: each terminal overload increments `stats.{queries,commands}` then calls `getQueryEngine(language).{query,command}(...)`. We wrap each engine call in a gated try/finally. The gate (`startNanos()` returns 0 when no recorder is registered) keeps the disabled path allocation-free with negligible overhead.

- [ ] **Step 1: Write the failing test**

```java
package com.arcadedb.database;

import com.arcadedb.engine.ComponentFile;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class LocalDatabaseQueryMetricsTest {
  private final String dbPath = "./target/databases/LocalDatabaseQueryMetricsTest";

  @AfterEach
  void reset() {
    QueryMetricsRecorder.Holder.register(null);
    ProtocolContext.clear();
  }

  @Test
  void queryAndCommandAreRecordedWithProtocolAndType() {
    final AtomicReference<String> lastQuery = new AtomicReference<>();
    final AtomicReference<String> lastCommand = new AtomicReference<>();
    QueryMetricsRecorder.Holder.register((protocol, database, language, type, nanos) -> {
      if ("query".equals(type)) lastQuery.set(protocol + "|" + language + "|" + type);
      else if ("command".equals(type)) lastCommand.set(protocol + "|" + language + "|" + type);
    });

    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      if (factory.exists()) factory.open().drop();
      try (final Database db = factory.create()) {
        ProtocolContext.set("test");
        db.command("sql", "CREATE DOCUMENT TYPE Doc");
        db.query("sql", "SELECT FROM Doc");
      }
    }

    assertThat(lastCommand.get()).isEqualTo("test|sql|command");
    assertThat(lastQuery.get()).isEqualTo("test|sql|query");
  }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `mvn -q -pl engine test -Dtest=LocalDatabaseQueryMetricsTest`
Expected: FAIL - recorder never invoked (no timing wired yet); `lastCommand`/`lastQuery` are null.

- [ ] **Step 3: Wrap the 7 terminal overloads**

In `LocalDatabase.java`, replace each terminal `command`/`query` body. The no-parameter command overload (~line 1559) becomes:

```java
  @Override
  public ResultSet command(final String language, final String query) {
    checkDatabaseIsOpen(true, "Cannot execute command on a read only database");
    stats.commands.incrementAndGet();
    final long start = QueryMetricsRecorder.Holder.startNanos();
    try {
      return getQueryEngine(language).command(query, new ContextConfiguration());
    } finally {
      QueryMetricsRecorder.Holder.record(start, name, language, "command");
    }
  }
```

The varargs command overload (~line 1566):

```java
  @Override
  public ResultSet command(final String language, final String query, final Object... parameters) {
    checkDatabaseIsOpen(true, "Cannot execute command on a read only database");
    stats.commands.incrementAndGet();
    final long start = QueryMetricsRecorder.Holder.startNanos();
    try {
      return getQueryEngine(language).command(query, new ContextConfiguration(), parameters);
    } finally {
      QueryMetricsRecorder.Holder.record(start, name, language, "command");
    }
  }
```

The configuration+varargs command overload (~line 1573):

```java
  @Override
  public ResultSet command(final String language, final String query, final ContextConfiguration configuration,
      final Object... parameters) {
    checkDatabaseIsOpen(true, "Cannot execute command on a read only database");
    stats.commands.incrementAndGet();
    final long start = QueryMetricsRecorder.Holder.startNanos();
    try {
      return getQueryEngine(language).command(query, configuration, parameters);
    } finally {
      QueryMetricsRecorder.Holder.record(start, name, language, "command");
    }
  }
```

The configuration+Map command overload (~line 1586) (note: the `command(language, query, Map)` overload at ~1581 just delegates here, so leave it untouched to avoid double counting):

```java
  @Override
  public ResultSet command(final String language, final String query, final ContextConfiguration configuration,
      final Map<String, Object> parameters) {
    checkDatabaseIsOpen(true, "Cannot execute command on a read only database");
    stats.commands.incrementAndGet();
    final long start = QueryMetricsRecorder.Holder.startNanos();
    try {
      return getQueryEngine(language).command(query, configuration, parameters);
    } finally {
      QueryMetricsRecorder.Holder.record(start, name, language, "command");
    }
  }
```

The no-parameter query overload (~line 1613):

```java
  @Override
  public ResultSet query(final String language, final String query) {
    checkDatabaseIsOpen();
    stats.queries.incrementAndGet();
    final long start = QueryMetricsRecorder.Holder.startNanos();
    try {
      return getQueryEngine(language).query(query, new ContextConfiguration());
    } finally {
      QueryMetricsRecorder.Holder.record(start, name, language, "query");
    }
  }
```

The varargs query overload (~line 1620):

```java
  @Override
  public ResultSet query(final String language, final String query, final Object... parameters) {
    checkDatabaseIsOpen();
    stats.queries.incrementAndGet();
    final long start = QueryMetricsRecorder.Holder.startNanos();
    try {
      return getQueryEngine(language).query(query, new ContextConfiguration(), parameters);
    } finally {
      QueryMetricsRecorder.Holder.record(start, name, language, "query");
    }
  }
```

The Map query overload (~line 1627):

```java
  @Override
  public ResultSet query(final String language, final String query, final Map<String, Object> parameters) {
    checkDatabaseIsOpen();
    stats.queries.incrementAndGet();
    final long start = QueryMetricsRecorder.Holder.startNanos();
    try {
      return getQueryEngine(language).query(query, new ContextConfiguration(), parameters);
    } finally {
      QueryMetricsRecorder.Holder.record(start, name, language, "query");
    }
  }
```

(`name` is the existing `LocalDatabase` database-name field used by `getName()`. Verify it is in scope; if the field is named differently, use the accessor `getName()`.)

- [ ] **Step 4: Run the test to verify it passes**

Run: `mvn -q -pl engine test -Dtest=LocalDatabaseQueryMetricsTest`
Expected: PASS.

- [ ] **Step 5: Run nearby engine query tests for regressions**

Run: `mvn -q -pl engine test -Dtest="*Query*Test,LocalDatabaseQueryMetricsTest"`
Expected: PASS (timing is transparent; no behavior change).

- [ ] **Step 6: Commit**

```bash
git add engine/src/main/java/com/arcadedb/database/LocalDatabase.java \
        engine/src/test/java/com/arcadedb/database/LocalDatabaseQueryMetricsTest.java
git commit -m "feat(observability): time query/command at the engine boundary (#4535)"
```

---

## Task 4: Micrometer recorder + register + HTTP refactor (server)

**Files:**
- Create: `server/src/main/java/com/arcadedb/server/monitor/MicrometerQueryMetricsRecorder.java`
- Modify: `server/src/main/java/com/arcadedb/server/ArcadeDBServer.java`
- Modify: `server/src/main/java/com/arcadedb/server/http/handler/AbstractServerHttpHandler.java`
- Modify: `server/src/main/java/com/arcadedb/server/http/handler/PostCommandHandler.java`
- Modify: `server/src/main/java/com/arcadedb/server/http/handler/PostQueryHandler.java`
- Test: `server/src/test/java/com/arcadedb/server/monitor/QueryMetricsProtocolIT.java`

- [ ] **Step 1: Write the failing test**

```java
package com.arcadedb.server.monitor;

import com.arcadedb.server.BaseGraphServerTest;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import org.junit.jupiter.api.Test;

import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

class QueryMetricsProtocolIT extends BaseGraphServerTest {
  @Override
  protected int getServerCount() {
    return 1;
  }

  @Test
  void httpQueryRecordedWithProtocolTag() throws Exception {
    final HttpURLConnection c = (HttpURLConnection) new URL("http://localhost:2480/api/v1/query/graph").openConnection();
    c.setRequestMethod("POST");
    c.setDoOutput(true);
    c.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    c.setRequestProperty("Content-Type", "application/json");
    c.getOutputStream().write("{\"language\":\"sql\",\"command\":\"select 1 as one\"}".getBytes(StandardCharsets.UTF_8));
    c.connect();
    assertThat(c.getResponseCode()).isEqualTo(200);
    c.disconnect();

    final Timer timer = Metrics.globalRegistry.find("arcadedb.query.duration")
        .tag("protocol", "http").tag("db", "graph").tag("language", "sql").tag("type", "query").timer();
    assertThat(timer).isNotNull();
    assertThat(timer.count()).isGreaterThanOrEqualTo(1L);
  }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `mvn -q -pl server test -Dtest=QueryMetricsProtocolIT`
Expected: FAIL - no `protocol` tag (recorder not registered; HTTP doesn't set `ProtocolContext`).

- [ ] **Step 3: Implement `MicrometerQueryMetricsRecorder`**

```java
package com.arcadedb.server.monitor;

import com.arcadedb.database.QueryMetricsRecorder;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;

import java.util.concurrent.TimeUnit;

/**
 * Server-side {@link QueryMetricsRecorder} that emits the always-on {@code arcadedb.query.duration}
 * RED timer tagged by originating protocol, database, language and type. Registered when server
 * metrics are enabled; otherwise the engine keeps the no-op recorder and pays no timing cost.
 */
public final class MicrometerQueryMetricsRecorder implements QueryMetricsRecorder {
  @Override
  public void record(final String protocol, final String database, final String language, final String type,
      final long durationNanos) {
    Timer.builder("arcadedb.query.duration")
        .description("Query/command execution duration")
        .tag("protocol", protocol)
        .tag("db", database)
        .tag("language", language)
        .tag("type", type)
        .publishPercentileHistogram()
        .register(Metrics.globalRegistry)
        .record(durationNanos, TimeUnit.NANOSECONDS);
  }
}
```

- [ ] **Step 4: Register it at server startup**

In `ArcadeDBServer.java`, inside the `if (configuration.getValueAsBoolean(GlobalConfiguration.SERVER_METRICS)) { ... }` block, immediately after `new EngineMetricsBinder().bindTo(Metrics.globalRegistry);`, add:

```java
      QueryMetricsRecorder.Holder.register(new MicrometerQueryMetricsRecorder());
```

Add imports `import com.arcadedb.database.QueryMetricsRecorder;` and `import com.arcadedb.server.monitor.MicrometerQueryMetricsRecorder;`.

- [ ] **Step 5: Set/clear `ProtocolContext` on the HTTP path**

In `AbstractServerHttpHandler.handleRequest`, add `import com.arcadedb.database.ProtocolContext;`. Immediately after `final long httpStartNanos = System.nanoTime();` set the protocol:

```java
    ProtocolContext.set("http");
```

In the existing `finally` block (next to `LogManager.instance().setContext(null);` and the `arcadedb.http.requests` timer), add:

```java
      ProtocolContext.clear();
```

- [ ] **Step 6: Remove the redundant HTTP query timer**

In `PostCommandHandler.java`, revert `executeCommand` to the un-timed body and delete the `timeExecution` helper (the engine now times it):

```java
  protected ResultSet executeCommand(final Database database, final String language, final String command,
      final Map<String, Object> paramMap) {
    final Object params = mapParams(paramMap);
    if (params instanceof Object[] objects)
      return database.command(language, command, httpServer.getServer().getConfiguration(), objects);
    return database.command(language, command, httpServer.getServer().getConfiguration(), (Map<String, Object>) params);
  }
```

Remove the now-unused imports `io.micrometer.core.instrument.Timer` and `java.util.function.Supplier` if they are no longer referenced elsewhere in the file (keep `Metrics` and `TimeUnit` - they are still used by `recordProfilerMetrics`).

In `PostQueryHandler.java`, revert `executeCommand` to:

```java
  protected ResultSet executeCommand(final Database database, final String language, final String command, final Map<String, Object> paramMap) {
    final Object params = mapParams(paramMap);
    if (params instanceof Object[] objects)
      return database.query(language, command, objects);
    return database.query(language, command, (Map<String, Object>) params);
  }
```

- [ ] **Step 7: Run the new test and the Pillar-1 query test**

Run: `mvn -q -pl server install -DskipTests`
Run: `mvn -pl server test -Dtest="QueryMetricsProtocolIT,QueryRedMetricsIT,HttpRedMetricsIT"`
Expected: PASS. `QueryRedMetricsIT` still passes because the engine emits `arcadedb.query.duration` with the same `db`/`language`/`type` tags (now plus `protocol=http`); its `find().tag(...)` assertions match a superset.

- [ ] **Step 8: Commit**

```bash
git add server/src/main/java/com/arcadedb/server/monitor/MicrometerQueryMetricsRecorder.java \
        server/src/main/java/com/arcadedb/server/ArcadeDBServer.java \
        server/src/main/java/com/arcadedb/server/http/handler/AbstractServerHttpHandler.java \
        server/src/main/java/com/arcadedb/server/http/handler/PostCommandHandler.java \
        server/src/main/java/com/arcadedb/server/http/handler/PostQueryHandler.java \
        server/src/test/java/com/arcadedb/server/monitor/QueryMetricsProtocolIT.java
git commit -m "feat(observability): Micrometer query recorder + HTTP protocol tag, drop redundant HTTP timer (#4535)"
```

---

## Task 5: Bolt protocol tag

**Files:**
- Modify: `bolt/src/main/java/com/arcadedb/bolt/BoltNetworkExecutor.java` (the `run()` loop, ~line 167; `processMessage` dispatch ~line 333)
- Test: `bolt/src/test/java/com/arcadedb/bolt/BoltQueryMetricsIT.java`

- [ ] **Step 1: Write the failing test**

Model the server bootstrap and Bolt client connection on the existing Bolt IT in `bolt/src/test/java/com/arcadedb/bolt/` (use the same `BaseGraphServerTest` subclass + Neo4j Java driver the other Bolt tests use). The assertion is the new part:

```java
package com.arcadedb.bolt;

import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

// extends the same base + @BeforeEach/@AfterEach server setup the other Bolt ITs use
class BoltQueryMetricsIT extends BaseBoltServerTest {
  @Test
  void boltQueryRecordedWithProtocolTag() {
    runCypherViaBoltDriver("RETURN 1 AS one"); // helper that opens a Bolt session and runs a query

    final Timer timer = Metrics.globalRegistry.find("arcadedb.query.duration").tag("protocol", "bolt").timer();
    assertThat(timer).isNotNull();
    assertThat(timer.count()).isGreaterThanOrEqualTo(1L);
  }
}
```

(If the module has no shared `BaseBoltServerTest`, place the server bootstrap + driver call inline in this test, copied from the existing Bolt IT in the module.)

- [ ] **Step 2: Run the test to verify it fails**

Run: `mvn -q -pl bolt test -Dtest=BoltQueryMetricsIT`
Expected: FAIL - no timer with `protocol=bolt`.

- [ ] **Step 3: Set/clear `ProtocolContext` in the message loop**

In `BoltNetworkExecutor.java`, add `import com.arcadedb.database.ProtocolContext;`. In `run()` (~line 167), set the protocol once before the message loop and clear it in the loop's `finally` (~line 221):

```java
    ProtocolContext.set("bolt");
    try {
      // ... existing while (state != State.DISCONNECTED) { ... processMessage(message); } ...
    } finally {
      ProtocolContext.clear();
    }
```

(Bolt is thread-per-connection, so setting once per connection covers every message that connection runs. If `run()` already has a `try/finally`, add the two lines into the existing blocks rather than nesting a new one.)

- [ ] **Step 4: Run the test to verify it passes**

Run: `mvn -q -pl bolt test -Dtest=BoltQueryMetricsIT`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add bolt/src/main/java/com/arcadedb/bolt/BoltNetworkExecutor.java \
        bolt/src/test/java/com/arcadedb/bolt/BoltQueryMetricsIT.java
git commit -m "feat(observability): tag Bolt queries with protocol=bolt (#4535)"
```

---

## Task 6: Postgres protocol tag

**Files:**
- Modify: `postgresw/src/main/java/com/arcadedb/postgres/PostgresNetworkExecutor.java` (the `run()` loop ~line 142, finally ~line 222)
- Test: `postgresw/src/test/java/com/arcadedb/postgres/PostgresQueryMetricsIT.java`

- [ ] **Step 1: Write the failing test**

Model the server bootstrap + JDBC connection on the existing Postgres IT in `postgresw/src/test/java/com/arcadedb/postgres/`. Assertion:

```java
package com.arcadedb.postgres;

import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.Statement;

import static org.assertj.core.api.Assertions.assertThat;

class PostgresQueryMetricsIT extends BasePostgresServerTest {
  @Test
  void postgresQueryRecordedWithProtocolTag() throws Exception {
    try (final Connection conn = openJdbc(); final Statement st = conn.createStatement()) {
      st.executeQuery("SELECT 1");
    }
    final Timer timer = Metrics.globalRegistry.find("arcadedb.query.duration").tag("protocol", "postgres").timer();
    assertThat(timer).isNotNull();
    assertThat(timer.count()).isGreaterThanOrEqualTo(1L);
  }
}
```

(Use the same JDBC URL/credentials helper the existing Postgres ITs use; inline the bootstrap if no shared base exists.)

- [ ] **Step 2: Run the test to verify it fails**

Run: `mvn -q -pl postgresw test -Dtest=PostgresQueryMetricsIT`
Expected: FAIL - no `protocol=postgres` timer.

- [ ] **Step 3: Set/clear `ProtocolContext` in the message loop**

In `PostgresNetworkExecutor.java`, add `import com.arcadedb.database.ProtocolContext;`. In `run()` (~line 142), set the protocol before the dispatch loop and clear it in the existing `finally` (~line 222):

```java
    ProtocolContext.set("postgres");
    try {
      // ... existing message read/dispatch loop ...
    } finally {
      ProtocolContext.clear();
    }
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `mvn -q -pl postgresw test -Dtest=PostgresQueryMetricsIT`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add postgresw/src/main/java/com/arcadedb/postgres/PostgresNetworkExecutor.java \
        postgresw/src/test/java/com/arcadedb/postgres/PostgresQueryMetricsIT.java
git commit -m "feat(observability): tag Postgres queries with protocol=postgres (#4535)"
```

---

## Task 7: MongoDB protocol tag

**Files:**
- Modify: `mongodbw/src/main/java/com/arcadedb/mongo/MongoDBDatabaseWrapper.java` (`handleCommand`, ~line 84)
- Test: `mongodbw/src/test/java/com/arcadedb/mongo/MongoQueryMetricsIT.java`

- [ ] **Step 1: Write the failing test**

Model the server bootstrap + Mongo driver call on the existing Mongo IT in `mongodbw/src/test/java/com/arcadedb/mongo/`. Assertion:

```java
package com.arcadedb.mongo;

import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class MongoQueryMetricsIT extends BaseMongoServerTest {
  @Test
  void mongoQueryRecordedWithProtocolTag() {
    runMongoFind(); // helper that issues a find() over the Mongo wire protocol

    final Timer timer = Metrics.globalRegistry.find("arcadedb.query.duration").tag("protocol", "mongo").timer();
    assertThat(timer).isNotNull();
    assertThat(timer.count()).isGreaterThanOrEqualTo(1L);
  }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `mvn -q -pl mongodbw test -Dtest=MongoQueryMetricsIT`
Expected: FAIL - no `protocol=mongo` timer.

- [ ] **Step 3: Set/clear `ProtocolContext` in `handleCommand`**

In `MongoDBDatabaseWrapper.java`, add `import com.arcadedb.database.ProtocolContext;`. Wrap the body of `handleCommand` (~line 84):

```java
  public Document handleCommand(final Channel channel, final String command, final Document document,
      final DatabaseResolver databaseResolver, final Oplog opLog) {
    ProtocolContext.set("mongo");
    try {
      // ... existing handleCommand body ...
    } finally {
      ProtocolContext.clear();
    }
  }
```

(The library calls `handleCommand` once per client request on its network thread, so set/clear per call.)

- [ ] **Step 4: Run the test to verify it passes**

Run: `mvn -q -pl mongodbw test -Dtest=MongoQueryMetricsIT`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add mongodbw/src/main/java/com/arcadedb/mongo/MongoDBDatabaseWrapper.java \
        mongodbw/src/test/java/com/arcadedb/mongo/MongoQueryMetricsIT.java
git commit -m "feat(observability): tag Mongo queries with protocol=mongo (#4535)"
```

---

## Task 8: gRPC protocol tag

**Files:**
- Modify: `grpcw/src/main/java/com/arcadedb/server/grpc/ArcadeDbGrpcService.java` (`executeQueryInternal` ~line 906, `executeCommandInternal` ~line 263; or set once in `GrpcMetricsInterceptor`)
- Test: `grpcw/src/test/java/com/arcadedb/server/grpc/GrpcQueryMetricsIT.java`

Context: the gRPC service executes on gRPC's own threads. Setting `ProtocolContext` in `GrpcMetricsInterceptor` (the universal per-RPC chokepoint) is cleanest, but interceptor `onMessage`/`onHalfClose` may run on a different thread than the service method in streaming cases. For unary query/command (the RED-relevant paths) the simplest robust placement is at the start of `executeQueryInternal`/`executeCommandInternal`, cleared in a finally.

- [ ] **Step 1: Write the failing test**

Model the server bootstrap + gRPC stub on the existing gRPC IT in `grpcw/src/test/java/com/arcadedb/server/grpc/`. Assertion:

```java
package com.arcadedb.server.grpc;

import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class GrpcQueryMetricsIT extends BaseGrpcServerTest {
  @Test
  void grpcQueryRecordedWithProtocolTag() {
    executeQueryViaStub("SELECT 1"); // helper using the generated blocking stub

    final Timer timer = Metrics.globalRegistry.find("arcadedb.query.duration").tag("protocol", "grpc").timer();
    assertThat(timer).isNotNull();
    assertThat(timer.count()).isGreaterThanOrEqualTo(1L);
  }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `mvn -q -pl grpcw test -Dtest=GrpcQueryMetricsIT`
Expected: FAIL - the query is tagged `protocol=internal`, not `grpc`.

- [ ] **Step 3: Set/clear `ProtocolContext` around the query/command execution**

In `ArcadeDbGrpcService.java`, add `import com.arcadedb.database.ProtocolContext;`. Wrap the bodies of `executeQueryInternal` (~line 906) and `executeCommandInternal` (~line 263):

```java
    ProtocolContext.set("grpc");
    try {
      // ... existing method body that calls database.query(...) / db.command(...) ...
    } finally {
      ProtocolContext.clear();
    }
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `mvn -q -pl grpcw test -Dtest=GrpcQueryMetricsIT`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add grpcw/src/main/java/com/arcadedb/server/grpc/ArcadeDbGrpcService.java \
        grpcw/src/test/java/com/arcadedb/server/grpc/GrpcQueryMetricsIT.java
git commit -m "feat(observability): tag gRPC queries with protocol=grpc (#4535)"
```

---

## Task 9: Redis protocol tag

**Files:**
- Modify: `redisw/src/main/java/com/arcadedb/redis/RedisNetworkExecutor.java` (`run()` loop ~line 86)
- Test: `redisw/src/test/java/com/arcadedb/redis/RedisQueryMetricsIT.java`

Context: Redis mostly serves in-memory key/value ops and only touches `database.*` inside transaction blocks, so `arcadedb.query.duration` will fire rarely (or never) for pure Redis traffic. Setting the protocol is still correct so any DB-backed path is attributed to `redis` rather than `internal`. Keep the test tolerant: assert the protocol tag is correct *when* the timer fires by issuing a Redis command that hits the database; if the module has no such command, assert only that no Redis op is mis-tagged as `internal` by checking the executor sets the context (a unit test on the run loop is acceptable here instead of an IT).

- [ ] **Step 1: Write the test**

```java
package com.arcadedb.redis;

import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class RedisQueryMetricsIT extends BaseRedisServerTest {
  @Test
  void redisDbBackedOpRecordedWithProtocolTag() {
    runRedisCommandThatPersists(); // a command whose handler enters a database transaction

    final Timer timer = Metrics.globalRegistry.find("arcadedb.query.duration").tag("protocol", "redis").timer();
    // Redis rarely runs engine queries; if it did, it must be tagged redis (never internal).
    if (timer != null)
      assertThat(timer.count()).isGreaterThanOrEqualTo(1L);
    assertThat(Metrics.globalRegistry.find("arcadedb.query.duration").tag("protocol", "internal").timer())
        .as("a Redis-thread DB op must not leak as protocol=internal")
        .isNull();
  }
}
```

- [ ] **Step 2: Run the test to verify it fails (or is inconclusive)**

Run: `mvn -q -pl redisw test -Dtest=RedisQueryMetricsIT`
Expected: FAIL if a DB-backed Redis op is mis-tagged `internal`.

- [ ] **Step 3: Set/clear `ProtocolContext` in the run loop**

In `RedisNetworkExecutor.java`, add `import com.arcadedb.database.ProtocolContext;`. In `run()` (~line 86), set before the loop and clear in the catch/exit (~line 95):

```java
    ProtocolContext.set("redis");
    try {
      // ... existing while loop: executeCommand(parseNext()); ...
    } finally {
      ProtocolContext.clear();
    }
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `mvn -q -pl redisw test -Dtest=RedisQueryMetricsIT`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add redisw/src/main/java/com/arcadedb/redis/RedisNetworkExecutor.java \
        redisw/src/test/java/com/arcadedb/redis/RedisQueryMetricsIT.java
git commit -m "feat(observability): tag Redis DB ops with protocol=redis (#4535)"
```

---

## Task 10: Cardinality guard, docs, broader suites

**Files:**
- Create: `docs/observability-metrics.md` (operator-facing metrics catalogue)
- Test: extend `server/src/test/java/com/arcadedb/server/monitor/QueryMetricsProtocolIT.java`

- [ ] **Step 1: Add a cardinality guard assertion**

Add to `QueryMetricsProtocolIT` a test asserting the `protocol` tag is one of the known bounded values and the query text never appears as a tag:

```java
  @Test
  void protocolTagIsBoundedAndQueryTextIsNeverATag() throws Exception {
    httpQueryRecordedWithProtocolTag(); // reuse to populate at least one series

    final var timers = Metrics.globalRegistry.find("arcadedb.query.duration").timers();
    assertThat(timers).isNotEmpty();
    for (final Timer t : timers) {
      final String protocol = t.getId().getTag("protocol");
      assertThat(protocol).isIn("http", "bolt", "postgres", "mongo", "grpc", "redis", "internal");
      // No tag value should contain SQL/Cypher text.
      t.getId().getTags().forEach(tag -> assertThat(tag.getValue()).doesNotContain("select").doesNotContain("SELECT"));
    }
  }
```

- [ ] **Step 2: Run it**

Run: `mvn -q -pl server test -Dtest=QueryMetricsProtocolIT`
Expected: PASS.

- [ ] **Step 3: Write the metrics catalogue doc**

Create `docs/observability-metrics.md` documenting `arcadedb.query.duration` (tags: `protocol`, `db`, `language`, `type`; note `protocol=internal` covers engine-internal/embedded callers and dashboards filter it out for wire RED), alongside the Pillar-1 `arcadedb.http.requests` and `arcadedb.engine.*` gauges. Include a sample PromQL: `histogram_quantile(0.95, sum(rate(arcadedb_query_duration_seconds_bucket{protocol!="internal"}[5m])) by (le, protocol))`.

- [ ] **Step 4: Run the broader suites**

Run: `mvn -q -pl engine test -Dtest="QueryMetricsRecorderTest,ProtocolContextTest,LocalDatabaseQueryMetricsTest"`
Run: `mvn -q -pl server test -Dtest="*Metrics*,*RedMetrics*"`
Run: `mvn -q -pl bolt,postgresw,mongodbw,grpcw,redisw test -Dtest="*QueryMetrics*"`
Expected: PASS. Investigate any meter-count test that breaks on the additive series and switch it to containment assertions.

- [ ] **Step 5: Commit**

```bash
git add docs/observability-metrics.md \
        server/src/test/java/com/arcadedb/server/monitor/QueryMetricsProtocolIT.java
git commit -m "test(observability): cardinality guard + metrics catalogue doc (#4535)"
```

---

## Self-Review Notes

- **Spec/issue coverage:** engine hook (T1) + protocol thread-local (T2) + engine timing (T3) = the single instrumentation point; server Micrometer recorder + registration + HTTP refactor (T4); Bolt/Postgres/Mongo/gRPC/Redis tags (T5-T9); cardinality guard + docs (T10). Acceptance criteria from #4535: emitted for all protocols (T4-T9), no HTTP double-count (T4 step 6 removes the old timer), engine has no Micrometer dependency (T1 hook is plain Java), per-protocol regression tests (each task).
- **No-Micrometer-in-engine invariant:** verified - `QueryMetricsRecorder`/`ProtocolContext`/`LocalDatabase` use only plain Java; the Micrometer type appears only in `MicrometerQueryMetricsRecorder` (server).
- **Allocation/perf:** the disabled path is `startNanos()` (one volatile read + ref compare returning 0) + a `record()` call that returns on the `start==0` sentinel - no `System.nanoTime()`, no lambda, no allocation. Aligned with the GC mantra.
- **Retro-compat with Pillar 1:** moving the timer to the engine keeps `db`/`language`/`type` tags and adds `protocol`; `QueryRedMetricsIT`'s subset `find().tag(...)` assertions still match, so no existing test is modified (only `PostCommandHandler`/`PostQueryHandler` lose the duplicate timer).
- **Type consistency:** `record(String protocol, String database, String language, String type, long durationNanos)` and `Holder.record(long start, String database, String language, String type)` are used identically across T1, T3, T4. `ProtocolContext.set/get/clear`/`INTERNAL` consistent across T2 and all protocol tasks. Tag names `protocol`/`db`/`language`/`type` identical in T4 recorder and all assertions.
- **Known limitation (documented in T10 doc):** timing wraps the `query()/command()` invocation (parse/plan/open), not full lazy-`ResultSet` iteration - identical semantics to Pillar 1's HTTP timer.
- **Per-module test harness:** each protocol task says to mirror that module's existing IT bootstrap (the exact base class / client helper varies per module); the executor reads the existing IT in the module before writing the new one.
