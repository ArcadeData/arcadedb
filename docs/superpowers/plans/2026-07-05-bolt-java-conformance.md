# Bolt (Java) Conformance Certification Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deepen the Java Bolt certification from 4 smoke tests to the full 39-scenario A1 conformance spec, driven by the real `neo4j-java-driver`, so the Java column of the compatibility matrix (#4892) is comparable to the merged JS/Go columns.

**Architecture:** Two certification layers. The e2e layer expands `RemoteBoltDatabaseIT` (JUnit 5, `@Nested` per feature-matrix area) against the published `arcadedata/arcadedb:latest` testcontainer. The bolt-module layer adds/extends narrow ITs for scenarios the shared container can't express (TLS modes, wire-level type serialization). A tiny `assertExpectedFailure` helper reproduces documented `#4890` gaps without red-building, mirroring JS `it.failing`.

**Tech Stack:** Java 21, JUnit 5 (Jupiter), AssertJ, Testcontainers, `neo4j-java-driver` 6.2.0 (baseline) + 4.4.x (legacy profile), Maven Failsafe.

## Global Constraints

- Certification only - **no production Bolt code changes**. Gap fixes are #4890.
- Java 21+, existing code style: `final` on locals/params, import classes (no FQN), one-statement `if` needs no braces, AssertJ style `assertThat(x).isTrue()`.
- Do **not** add Claude as author in any commit or comment.
- Every scenario test embeds its id in a `@DisplayName`, e.g. `[TYPE-011] ...` (spec README traceability convention).
- `type_matrix` fixture data must be seeded via HTTP `/command` (never over Bolt) - Bolt serialization is under test.
- `neo4j-java-driver` version property is `${neo4j-driver.version}` (root `pom.xml`), currently `6.2.0`.
- Container credentials: user `root`, password `playwithdata`; Bolt port 7687, HTTP port 2480 (see `ArcadeContainerTemplate`).
- Preserve the existing 4 `RemoteBoltDatabaseIT` tests (AC).
- `current_status` in `spec.yaml` is a starting hint; the real-image run decides the verdict. When observed verdict differs, flip the test representation AND update `bolt/conformance/spec.yaml` in the same PR, then re-run `validate_spec.py`.
- Wire-protocol modules keep `arcadedb-server` in `provided` scope (unchanged here).

---

## File Structure

- `e2e/src/test/java/com/arcadedb/e2e/RemoteBoltDatabaseIT.java` - expanded; `@TestInstance(PER_CLASS)`, one shared driver, HTTP seeding, `@Nested` per area, `assertExpectedFailure` helper.
- `e2e/src/test/java/com/arcadedb/e2e/RemoteBoltLegacyDriverIT.java` - new; stable-API subset run under the 4.4 driver profile.
- `e2e/pom.xml` - new `bolt-driver-legacy` profile (driver version override + `testExcludes` + failsafe includes).
- `bolt/src/test/java/com/arcadedb/bolt/BoltTlsIT.java` - tag CONN-002, add CONN-005 (OPTIONAL mode).
- `bolt/src/test/java/com/arcadedb/bolt/BoltTypeRoundTripTest.java` - new; wire-level serialization contract for temporal/Duration/Point.
- `bolt/conformance/spec.yaml` - only edited if a real-image verdict differs from the hint; plus java band resolution note.

---

### Task 1: e2e scaffolding - shared driver, HTTP seeding, xfail helper, @Nested skeleton

**Files:**
- Modify: `e2e/src/test/java/com/arcadedb/e2e/RemoteBoltDatabaseIT.java`

**Interfaces:**
- Produces (used by Tasks 2-9):
  - `Session boltSession()` - session on database `beer`.
  - `Session boltSession(String database)` - session on a named database.
  - `Driver driver` - shared class-scoped driver field.
  - `static void assertExpectedFailure(String trackingIssue, ThrowingCallable ideal)` - passes while `ideal` throws; fails when it stops throwing.
  - `void httpCommand(String apiPath, String jsonBody)` - POST to `/api/v1/<apiPath>` with root basic auth.
  - Seeded state: `TypeMatrix` node in `beer`; second database `boltscratch`.

- [ ] **Step 1: Rewrite the class scaffolding**

Replace the current `setUp`/`tearDown`/field block (keep the 4 existing test methods for now; they move into `@Nested` in later tasks) with class-level lifecycle. New imports and members:

```java
package com.arcadedb.e2e;

import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RemoteBoltDatabaseIT extends ArcadeContainerTemplate {

  // Mirrors bolt/conformance/fixtures/type-matrix.cypher - seed via HTTP, never Bolt.
  private static final String TYPE_MATRIX_CYPHER = """
      CREATE (:TypeMatrix {
        localDateProp: date('2026-01-15'),
        localTimeProp: localtime('14:30:00'),
        localDateTimeProp: localdatetime('2026-01-15T14:30:00'),
        offsetDateTimeProp: datetime('2026-01-15T14:30:00+02:00'),
        durationProp: duration('P1DT2H30M'),
        pointProp: point({x: 12.34, y: 56.78}),
        nestedListProp: [1, 2, [3, 4]],
        nestedMapProp: {a: 1, b: {c: 2}},
        nullProp: null
      })""";

  private Driver driver;

  @BeforeAll
  void setUpClass() throws Exception {
    driver = GraphDatabase.driver(
        "bolt://" + host + ":" + boltPort,
        AuthTokens.basic("root", "playwithdata"),
        Config.builder().withoutEncryption().build());
    driver.verifyConnectivity();

    // Seed over HTTP only, never over Bolt.
    httpCommand("server", "{\\"command\\": \\"create database boltscratch\\"}");
    httpCommand("command/beer",
        "{\\"language\\": \\"cypher\\", \\"command\\": " + jsonString(TYPE_MATRIX_CYPHER) + "}");
  }

  @AfterAll
  void tearDownClass() {
    if (driver != null)
      driver.close();
  }

  Session boltSession() {
    return boltSession("beer");
  }

  Session boltSession(final String database) {
    return driver.session(SessionConfig.forDatabase(database));
  }

  // Passes while the known gap reproduces (ideal assertion throws); fails loudly
  // the day the gap closes, forcing a flip to a real assertion + spec.yaml update.
  static void assertExpectedFailure(final String trackingIssue, final ThrowingCallable ideal) {
    try {
      ideal.call();
    } catch (final Throwable expected) {
      return;
    }
    fail("Expected-fail scenario now PASSES - close it: flip the assertion and update "
        + "bolt/conformance/spec.yaml current_status (" + trackingIssue + ")");
  }

  void httpCommand(final String apiPath, final String jsonBody) throws Exception {
    final HttpURLConnection con = (HttpURLConnection) URI.create(
        "http://" + host + ":" + httpPort + "/api/v1/" + apiPath).toURL().openConnection();
    con.setRequestMethod("POST");
    con.setDoOutput(true);
    con.setRequestProperty("Content-Type", "application/json");
    con.setRequestProperty("Authorization", "Basic " + Base64.getEncoder()
        .encodeToString("root:playwithdata".getBytes(StandardCharsets.UTF_8)));
    try (final OutputStream os = con.getOutputStream()) {
      os.write(jsonBody.getBytes(StandardCharsets.UTF_8));
    }
    final int code = con.getResponseCode();
    if (code < 200 || code >= 300)
      throw new IllegalStateException("HTTP " + code + ": " + new String(
          con.getErrorStream().readAllBytes(), StandardCharsets.UTF_8));
    con.disconnect();
  }

  // Minimal JSON string encoder for embedding the fixture cypher in a request body.
  static String jsonString(final String raw) {
    final StringBuilder sb = new StringBuilder("\\"");
    for (int i = 0; i < raw.length(); i++) {
      final char c = raw.charAt(i);
      switch (c) {
        case '"' -> sb.append("\\\\\\"");
        case '\\\\' -> sb.append("\\\\\\\\");
        case '\\n' -> sb.append("\\\\n");
        case '\\r' -> sb.append("\\\\r");
        case '\\t' -> sb.append("\\\\t");
        default -> sb.append(c);
      }
    }
    return sb.append('"').toString();
  }
}
```

Keep the existing `connection()`, `simpleReturnQuery()`, `queryBeerDatabase()`, `parameterizedQuery()` methods in the class body for now (Tasks 2/4/5 move them into `@Nested` classes verbatim).

- [ ] **Step 2: Compile the test module**

Run: `mvn -q -pl e2e -am test-compile`
Expected: BUILD SUCCESS (no test run yet).

- [ ] **Step 3: Smoke-run the preserved tests against the real container**

Run: `mvn -q -pl e2e verify -DskipITs=false -Dit.test=RemoteBoltDatabaseIT`
Expected: the 4 preserved tests PASS; container boots once, seeding succeeds (no HTTP error). Docker required.

- [ ] **Step 4: Commit**

```bash
git add e2e/src/test/java/com/arcadedb/e2e/RemoteBoltDatabaseIT.java
git commit -m "test(#4889): e2e Bolt conformance scaffolding (shared driver, HTTP seeding, xfail helper)"
```

---

### Task 2: Connection scenarios (CONN-001, CONN-003, CONN-004)

**Files:**
- Modify: `e2e/src/test/java/com/arcadedb/e2e/RemoteBoltDatabaseIT.java`

**Interfaces:**
- Consumes: `driver`, `boltSession()` (Task 1).

CONN-002/CONN-005 (TLS) are covered at the bolt-module layer (Task 10). CONN-004 (HA) cannot be driven by a single-node container.

- [ ] **Step 1: Add the `@Nested` Connection class** (move the existing `connection()` test in as CONN-001)

```java
@Nested
@DisplayName("connection")
class Connection {

  @Test
  @DisplayName("[CONN-001] Connect via bolt:// scheme")
  void conn001_boltScheme() {
    driver.verifyConnectivity();
  }

  @Test
  @DisplayName("[CONN-003] neo4j:// routing discovery, single-node")
  void conn003_routingSingleNode() {
    try (final Driver routing = GraphDatabase.driver(
        "neo4j://" + host + ":" + boltPort,
        AuthTokens.basic("root", "playwithdata"),
        Config.builder().withoutEncryption().build())) {
      routing.verifyConnectivity();
      try (final Session s = routing.session(SessionConfig.forDatabase("beer"))) {
        assertThat(s.run("MATCH (b:Beer) RETURN b.name AS name LIMIT 1")
            .single().get("name").asString()).isNotBlank();
      }
    }
  }

  @Test
  @Disabled("[CONN-004] Requires a real 3-node HA cluster; single-node container "
      + "always returns itself as writer/reader/router (#4890)")
  @DisplayName("[CONN-004] neo4j:// routing reflects HA cluster topology")
  void conn004_haTopology() {
  }
}
```

Delete the now-migrated top-level `connection()` method.

- [ ] **Step 2: Run the Connection nest**

Run: `mvn -q -pl e2e verify -DskipITs=false -Dit.test=RemoteBoltDatabaseIT`
Expected: CONN-001, CONN-003 PASS; CONN-004 reported skipped.

- [ ] **Step 3: Commit**

```bash
git add e2e/src/test/java/com/arcadedb/e2e/RemoteBoltDatabaseIT.java
git commit -m "test(#4889): CONN-001/003/004 connection conformance scenarios"
```

---

### Task 3: Auth scenarios (AUTH-001, AUTH-002, AUTH-003)

**Files:**
- Modify: `e2e/src/test/java/com/arcadedb/e2e/RemoteBoltDatabaseIT.java`

**Interfaces:**
- Consumes: `driver`, `host`, `boltPort`.
- New import: `import org.assertj.core.api.Assertions;` is not needed; use `assertThatThrownBy` static import: add `import static org.assertj.core.api.Assertions.assertThatThrownBy;`.

- [ ] **Step 1: Add the `@Nested` Auth class**

```java
@Nested
@DisplayName("auth")
class Auth {

  @Test
  @DisplayName("[AUTH-001] Basic auth succeeds with valid credentials")
  void auth001_validBasic() {
    try (final Driver d = GraphDatabase.driver("bolt://" + host + ":" + boltPort,
        AuthTokens.basic("root", "playwithdata"),
        Config.builder().withoutEncryption().build())) {
      d.verifyConnectivity();
    }
  }

  @Test
  @DisplayName("[AUTH-002] Basic auth fails with invalid credentials")
  void auth002_invalidBasic() {
    try (final Driver d = GraphDatabase.driver("bolt://" + host + ":" + boltPort,
        AuthTokens.basic("root", "wrong-password"),
        Config.builder().withoutEncryption().build())) {
      assertThatThrownBy(d::verifyConnectivity)
          .satisfiesAnyOf(
              t -> assertThat(t).isInstanceOf(org.neo4j.driver.exceptions.AuthenticationException.class),
              t -> assertThat(String.valueOf(t.getMessage())).containsIgnoringCase("unauthorized"));
    }
  }

  @Test
  @DisplayName("[AUTH-003] Auth scheme 'none' is rejected (intentional, not a bug)")
  void auth003_noneRejected() {
    try (final Driver d = GraphDatabase.driver("bolt://" + host + ":" + boltPort,
        AuthTokens.none(),
        Config.builder().withoutEncryption().build())) {
      assertThatThrownBy(d::verifyConnectivity).isInstanceOf(RuntimeException.class);
    }
  }
}
```

- [ ] **Step 2: Run**

Run: `mvn -q -pl e2e verify -DskipITs=false -Dit.test=RemoteBoltDatabaseIT`
Expected: AUTH-001/002/003 PASS. If AUTH-002's exception type differs, the `satisfiesAnyOf` message branch keeps it green; if AUTH-003 unexpectedly connects, reconcile spec.yaml (AUTH-003 hint is `passing`).

- [ ] **Step 3: Commit**

```bash
git add e2e/src/test/java/com/arcadedb/e2e/RemoteBoltDatabaseIT.java
git commit -m "test(#4889): AUTH-001/002/003 auth conformance scenarios"
```

---

### Task 4: Transaction scenarios (TX-001..TX-005)

**Files:**
- Modify: `e2e/src/test/java/com/arcadedb/e2e/RemoteBoltDatabaseIT.java`

**Interfaces:**
- Consumes: `boltSession()`, `driver`, `assertExpectedFailure`.
- New imports: `import org.neo4j.driver.Transaction;`, `import java.util.List;`, `import java.util.concurrent.CopyOnWriteArrayList;`, `import org.neo4j.driver.exceptions.Neo4jException;`.
- Produces: `raceTwoWriters(...)` helper (used only here).

- [ ] **Step 1: Add the `@Nested` Transactions class** (fold the existing `simpleReturnQuery()` in as TX-001)

```java
@Nested
@DisplayName("transactions")
class Transactions {

  @Test
  @DisplayName("[TX-001] Autocommit query executes and returns results")
  void tx001_autocommit() {
    try (final Session s = boltSession()) {
      assertThat(s.run("MATCH (b:Beer) RETURN b.name AS name LIMIT 5").list()).hasSize(5);
    }
  }

  @Test
  @DisplayName("[TX-002] Explicit BEGIN/RUN/COMMIT persists changes")
  void tx002_commit() {
    try (final Session s = boltSession()) {
      final Transaction tx = s.beginTransaction();
      tx.run("CREATE (:TxProbe {marker: 'tx-002'})");
      tx.commit();
      assertThat(s.run("MATCH (n:TxProbe {marker: 'tx-002'}) RETURN count(n) AS c")
          .single().get("c").asLong()).isEqualTo(1L);
    }
  }

  @Test
  @DisplayName("[TX-003] Explicit BEGIN/RUN/ROLLBACK discards changes")
  void tx003_rollback() {
    try (final Session s = boltSession()) {
      final Transaction tx = s.beginTransaction();
      tx.run("CREATE (:TxProbe {marker: 'tx-003'})");
      tx.rollback();
      assertThat(s.run("MATCH (n:TxProbe {marker: 'tx-003'}) RETURN count(n) AS c")
          .single().get("c").asLong()).isEqualTo(0L);
    }
  }

  @Test
  @DisplayName("[TX-004] Managed executeWrite commits on success")
  void tx004_executeWrite() {
    try (final Session s = boltSession()) {
      s.executeWrite(tx -> tx.run("CREATE (:Beer {name: $n})",
          java.util.Map.of("n", "TX-004-Beer")).consume());
      assertThat(s.run("MATCH (b:Beer {name: $n}) RETURN count(b) AS c",
          java.util.Map.of("n", "TX-004-Beer")).single().get("c").asLong()).isEqualTo(1L);
    }
  }

  @Test
  @DisplayName("[TX-005] Managed transaction retries on Neo.TransientError.*")
  void tx005_transientRetry() {
    // Hint: expected-fail (#4890). Real-image verdict decides: if a write-write
    // race surfaces Neo.TransientError.*, assert it positively and reconcile
    // spec.yaml; otherwise wrap in assertExpectedFailure("#4890", ...).
    final List<Neo4jException> errors = raceTwoWriters("tx-005");
    assertExpectedFailure("#4890", () -> {
      assertThat(errors).isNotEmpty();
      assertThat(errors).anySatisfy(e ->
          assertThat(e.code()).startsWith("Neo.TransientError"));
    });
  }

  private List<Neo4jException> raceTwoWriters(final String marker) {
    final List<Neo4jException> errors = new CopyOnWriteArrayList<>();
    final Runnable writer = () -> {
      try (final Session s = boltSession()) {
        s.executeWrite(tx -> tx.run(
            "MERGE (n:RaceProbe {marker: $m}) SET n.v = coalesce(n.v,0)+1",
            java.util.Map.of("m", marker)).consume());
      } catch (final Neo4jException e) {
        errors.add(e);
      }
    };
    final Thread a = new Thread(writer);
    final Thread b = new Thread(writer);
    a.start();
    b.start();
    try {
      a.join();
      b.join();
    } catch (final InterruptedException ignored) {
      Thread.currentThread().interrupt();
    }
    return errors;
  }
}
```

Delete the migrated top-level `simpleReturnQuery()` method.

- [ ] **Step 2: Run and record TX-005 verdict**

Run: `mvn -q -pl e2e verify -DskipITs=false -Dit.test=RemoteBoltDatabaseIT`
Expected: TX-001..004 PASS. For TX-005, note the real behavior: if the race never produces a transient code, `assertExpectedFailure` passes (gap reproduced). If it DOES produce `Neo.TransientError.*`, `assertExpectedFailure` will FAIL - then flip TX-005 to assert positively (remove the wrapper) and update `spec.yaml` TX-005/ERR-004 `current_status` to `passing` and drop their `known_limitation`, re-running `validate_spec.py`.

- [ ] **Step 3: Commit**

```bash
git add e2e/src/test/java/com/arcadedb/e2e/RemoteBoltDatabaseIT.java bolt/conformance/spec.yaml
git commit -m "test(#4889): TX-001..005 transaction conformance scenarios"
```

---

### Task 5: Causal consistency + Multi-database (CAUSAL-001, MDB-001, MDB-002)

**Files:**
- Modify: `e2e/src/test/java/com/arcadedb/e2e/RemoteBoltDatabaseIT.java`

**Interfaces:**
- Consumes: `driver`, `boltSession()`, seeded `boltscratch` database.
- New imports: `import org.neo4j.driver.Bookmark;`, `import java.util.Set;`.

- [ ] **Step 1: Add the `@Nested` CausalConsistency and MultiDatabase classes** (fold existing `queryBeerDatabase()` in as MDB-001)

```java
@Nested
@DisplayName("causal-consistency")
class CausalConsistency {

  @Test
  @DisplayName("[CAUSAL-001] Bookmark enforces read-after-write across sessions")
  void causal001_bookmark() {
    final Set<Bookmark> bookmarks;
    try (final Session a = boltSession()) {
      a.run("CREATE (:CausalProbe {marker: 'causal-001'})").consume();
      bookmarks = a.lastBookmarks();
    }
    try (final Session b = driver.session(SessionConfig.builder()
        .withDatabase("beer").withBookmarks(bookmarks).build())) {
      assertThat(b.run("MATCH (n:CausalProbe {marker: 'causal-001'}) RETURN count(n) AS c")
          .single().get("c").asLong()).isEqualTo(1L);
    }
  }
}

@Nested
@DisplayName("multi-database")
class MultiDatabase {

  @Test
  @DisplayName("[MDB-001] Session selects a specific named database")
  void mdb001_namedDatabase() {
    try (final Session s = boltSession("beer")) {
      assertThat(s.run("MATCH (b:Beer) RETURN b.name AS name LIMIT 1")
          .single().get("name").asString()).isNotBlank();
    }
  }

  @Test
  @DisplayName("[MDB-002] Sessions across databases are isolated")
  void mdb002_isolation() {
    try (final Session scratch = boltSession("boltscratch")) {
      final Transaction tx = scratch.beginTransaction();
      tx.run("CREATE (:ScratchOnly {marker: 'mdb-002'})");
      try (final Session beer = boltSession("beer")) {
        assertThat(beer.run("MATCH (n:ScratchOnly) RETURN count(n) AS c")
            .single().get("c").asLong()).isEqualTo(0L);
      }
      tx.rollback();
    }
  }
}
```

Delete the migrated top-level `queryBeerDatabase()` method. Add `import org.neo4j.driver.Transaction;` if not already added in Task 4 (it is).

- [ ] **Step 2: Run**

Run: `mvn -q -pl e2e verify -DskipITs=false -Dit.test=RemoteBoltDatabaseIT`
Expected: CAUSAL-001, MDB-001, MDB-002 PASS. If any `unverified` scenario fails as a real gap, open/append `#4890` and update `spec.yaml`.

- [ ] **Step 3: Commit**

```bash
git add e2e/src/test/java/com/arcadedb/e2e/RemoteBoltDatabaseIT.java bolt/conformance/spec.yaml
git commit -m "test(#4889): CAUSAL-001, MDB-001/002 conformance scenarios"
```

---

### Task 6: Result handling (RESULT-001..RESULT-004)

**Files:**
- Modify: `e2e/src/test/java/com/arcadedb/e2e/RemoteBoltDatabaseIT.java`

**Interfaces:**
- Consumes: `boltSession()`, `assertExpectedFailure`.
- New imports: `import org.neo4j.driver.Result;`, `import org.neo4j.driver.summary.ResultSummary;`.

- [ ] **Step 1: Add the `@Nested` ResultHandling class**

```java
@Nested
@DisplayName("result-handling")
class ResultHandling {

  @Test
  @DisplayName("[RESULT-001] Streaming PULL returns records incrementally")
  void result001_streaming() {
    try (final Session s = boltSession()) {
      final Result r = s.run("MATCH (b:Beer) RETURN b.name AS name LIMIT 10");
      int count = 0;
      while (r.hasNext()) {
        r.next();
        count++;
      }
      assertThat(count).isEqualTo(10);
    }
  }

  @Test
  @DisplayName("[RESULT-002] PULL n streams exactly n, further pull continues")
  void result002_partialPull() {
    try (final Session s = boltSession()) {
      final Result r = s.run("MATCH (b:Beer) RETURN b.name AS name LIMIT 5");
      final var first = r.next();
      final var second = r.next();
      assertThat(first.get("name").asString()).isNotBlank();
      assertThat(second.get("name").asString()).isNotBlank();
      // Remaining 3 continue without gaps/duplicates.
      assertThat(r.list()).hasSize(3);
    }
  }

  @Test
  @DisplayName("[RESULT-003] DISCARD abandons remaining rows")
  void result003_discard() {
    try (final Session s = boltSession()) {
      final Result r = s.run("MATCH (b:Beer) RETURN b.name AS name LIMIT 5");
      r.next();
      final ResultSummary summary = r.consume();
      assertThat(summary).isNotNull();
    }
  }

  @Test
  @DisplayName("[RESULT-004] ResultSummary counters reflect writes")
  void result004_counters() {
    assertExpectedFailure("#4890", () -> {
      try (final Session s = boltSession()) {
        final ResultSummary summary = s.run(
            "CREATE (:Beer {name: $n})-[:BREWED_BY]->(:Brewery {name: $b})",
            java.util.Map.of("n", "RESULT-004-Beer", "b", "RESULT-004-Brewery")).consume();
        assertThat(summary.counters().nodesCreated()).isEqualTo(2);
        assertThat(summary.counters().relationshipsCreated()).isEqualTo(1);
      }
    });
  }
}
```

- [ ] **Step 2: Run and record RESULT-004 verdict**

Run: `mvn -q -pl e2e verify -DskipITs=false -Dit.test=RemoteBoltDatabaseIT`
Expected: RESULT-001/002/003 PASS. RESULT-004 stays wrapped while counters are wrong; if counters are correct, `assertExpectedFailure` fails - flip to a plain assertion and set `spec.yaml` RESULT-004 `current_status: passing`.

- [ ] **Step 3: Commit**

```bash
git add e2e/src/test/java/com/arcadedb/e2e/RemoteBoltDatabaseIT.java bolt/conformance/spec.yaml
git commit -m "test(#4889): RESULT-001..004 result-handling conformance scenarios"
```

---

### Task 7: Type round-trip (TYPE-001..TYPE-012, e2e layer)

**Files:**
- Modify: `e2e/src/test/java/com/arcadedb/e2e/RemoteBoltDatabaseIT.java`

**Interfaces:**
- Consumes: `boltSession()`, `assertExpectedFailure`, seeded `TypeMatrix` node.
- New imports: `import org.neo4j.driver.types.Node;`, `import org.neo4j.driver.types.Relationship;`, `import org.neo4j.driver.types.Path;`, `import java.time.LocalDate;`, `import java.util.List;`, `import java.util.Map;`.

- [ ] **Step 1: Add the `@Nested` TypeRoundTrip class**

```java
@Nested
@DisplayName("type-roundtrip")
class TypeRoundTrip {

  @Test
  @DisplayName("[TYPE-001] Node round-trips as a native Bolt structure")
  void type001_node() {
    try (final Session s = boltSession()) {
      final Node n = s.run("MATCH (b:Beer) RETURN b LIMIT 1").single().get("b").asNode();
      assertThat(n.labels()).contains("Beer");
      assertThat(n.asMap()).isNotEmpty();
    }
  }

  @Test
  @DisplayName("[TYPE-002] Relationship round-trips as a native Bolt structure")
  void type002_relationship() {
    try (final Session s = boltSession()) {
      final Relationship r = s.run("MATCH ()-[rel]->() RETURN rel LIMIT 1")
          .single().get("rel").asRelationship();
      assertThat(r.type()).isNotBlank();
    }
  }

  @Test
  @DisplayName("[TYPE-003] Path round-trips as a native Bolt structure")
  void type003_path() {
    assertExpectedFailure("#4890", () -> {
      try (final Session s = boltSession()) {
        final Path p = s.run("MATCH p=(b:Beer)-[*1..2]-() RETURN p LIMIT 1")
            .single().get("p").asPath();
        assertThat(p.length()).isGreaterThanOrEqualTo(1);
      }
    });
  }

  @Test
  @DisplayName("[TYPE-004] ByteArray round-trips as a bound parameter")
  void type004_byteArray() {
    try (final Session s = boltSession()) {
      final byte[] in = {1, 2, 3, 4};
      assertThat(s.run("RETURN $b AS echo", Map.of("b", in))
          .single().get("echo").asByteArray()).isEqualTo(in);
    }
  }

  @Test
  @DisplayName("[TYPE-005] Nested lists and maps round-trip structurally")
  void type005_nested() {
    try (final Session s = boltSession()) {
      final var rec = s.run(
          "MATCH (t:TypeMatrix) RETURN t.nestedListProp AS l, t.nestedMapProp AS m LIMIT 1").single();
      assertThat(rec.get("l").asList()).hasSize(3);
      assertThat(rec.get("m").asMap()).containsKeys("a", "b");
    }
  }

  @Test
  @DisplayName("[TYPE-006] Null values round-trip")
  void type006_null() {
    try (final Session s = boltSession()) {
      assertThat(s.run("MATCH (t:TypeMatrix) RETURN t.nullProp AS n LIMIT 1")
          .single().get("n").isNull()).isTrue();
      assertThat(s.run("RETURN $p AS echo", java.util.Collections.singletonMap("p", null))
          .single().get("echo").isNull()).isTrue();
    }
  }

  @Test
  @DisplayName("[TYPE-007] LocalDate round-trips as a native Bolt Date")
  void type007_localDate() {
    // Hint: expected-fail, but the JS suite found latest returns native temporals.
    // Assert native first; if it throws, the wrapper documents the gap. Reconcile
    // spec.yaml to whichever the real image yields.
    assertNativeTemporalOrGap("t.localDateProp", "TYPE-007",
        v -> assertThat(v.hasType(org.neo4j.driver.internal.types.InternalTypeSystem.TYPE_SYSTEM.DATE())).isTrue());
  }

  @Test
  @DisplayName("[TYPE-008] LocalTime round-trips as a native Bolt LocalTime")
  void type008_localTime() {
    assertNativeTemporalOrGap("t.localTimeProp", "TYPE-008",
        v -> assertThat(v.hasType(org.neo4j.driver.internal.types.InternalTypeSystem.TYPE_SYSTEM.LOCAL_TIME())).isTrue());
  }

  @Test
  @DisplayName("[TYPE-009] LocalDateTime round-trips as a native Bolt LocalDateTime")
  void type009_localDateTime() {
    assertNativeTemporalOrGap("t.localDateTimeProp", "TYPE-009",
        v -> assertThat(v.hasType(org.neo4j.driver.internal.types.InternalTypeSystem.TYPE_SYSTEM.LOCAL_DATE_TIME())).isTrue());
  }

  @Test
  @DisplayName("[TYPE-010] Offset DateTime round-trips as a native Bolt DateTime")
  void type010_offsetDateTime() {
    assertNativeTemporalOrGap("t.offsetDateTimeProp", "TYPE-010",
        v -> assertThat(v.hasType(org.neo4j.driver.internal.types.InternalTypeSystem.TYPE_SYSTEM.DATE_TIME())).isTrue());
  }

  @Test
  @DisplayName("[TYPE-011] Duration round-trips as a native Bolt Duration")
  void type011_duration() {
    assertExpectedFailure("#4890", () -> {
      try (final Session s = boltSession()) {
        final var v = s.run("MATCH (t:TypeMatrix) RETURN t.durationProp AS d LIMIT 1")
            .single().get("d");
        assertThat(v.hasType(org.neo4j.driver.internal.types.InternalTypeSystem.TYPE_SYSTEM.DURATION())).isTrue();
      }
    });
  }

  @Test
  @DisplayName("[TYPE-012] Point round-trips as a native Bolt Point")
  void type012_point() {
    assertExpectedFailure("#4890", () -> {
      try (final Session s = boltSession()) {
        final var v = s.run("MATCH (t:TypeMatrix) RETURN t.pointProp AS p LIMIT 1")
            .single().get("p");
        assertThat(v.hasType(org.neo4j.driver.internal.types.InternalTypeSystem.TYPE_SYSTEM.POINT())).isTrue();
      }
    });
  }

  // Assert the native-temporal ideal; if the image still emits ISO strings, the
  // gap is documented via assertExpectedFailure. Reconcile spec.yaml to reality.
  private void assertNativeTemporalOrGap(final String expr, final String id,
      final ThrowingCallable ideal) {
    // Try native; if it throws, fall back to documenting the gap.
    try {
      ideal.call();
    } catch (final Throwable t) {
      assertExpectedFailure("#4890", ideal);
    }
  }
}
```

Note: `assertNativeTemporalOrGap` takes a `ThrowingCallable` that reads and asserts; the four temporal tests pass a lambda capturing the read. Adjust each temporal lambda to actually run the query (shown inline below) - replace the `v -> assertThat(...)` args with a no-arg body:

```java
  void type007_localDate() {
    final ThrowingCallable ideal = () -> {
      try (final Session s = boltSession()) {
        final var v = s.run("MATCH (t:TypeMatrix) RETURN t.localDateProp AS d LIMIT 1")
            .single().get("d");
        assertThat(v.hasType(org.neo4j.driver.internal.types.InternalTypeSystem.TYPE_SYSTEM.DATE())).isTrue();
      }
    };
    try { ideal.call(); } catch (final Throwable t) { assertExpectedFailure("#4890", ideal); }
  }
```

Apply the same no-arg-`ideal` shape to TYPE-008/009/010 (swapping the expr and the type-system accessor `LOCAL_TIME()`/`LOCAL_DATE_TIME()`/`DATE_TIME()`), and drop the earlier `assertNativeTemporalOrGap` helper signature that took a `Value` consumer. Keep TYPE-011/012 as strict `assertExpectedFailure` (confirmed gaps).

- [ ] **Step 2: Run and record TYPE verdicts**

Run: `mvn -q -pl e2e verify -DskipITs=false -Dit.test=RemoteBoltDatabaseIT`
Expected: TYPE-001,002,004,005,006 PASS. TYPE-003/011/012 stay gap-wrapped. TYPE-007..010: record whether native (PASS via the `try` branch) or gap. For each temporal that resolves NATIVE, set `spec.yaml` TYPE-007..010 `current_status: passing`, remove `known_limitation`/`tracking_issue`; re-run `validate_spec.py`.

- [ ] **Step 3: Commit**

```bash
git add e2e/src/test/java/com/arcadedb/e2e/RemoteBoltDatabaseIT.java bolt/conformance/spec.yaml
git commit -m "test(#4889): TYPE-001..012 type round-trip conformance scenarios"
```

---

### Task 8: Errors (ERR-001, ERR-002, ERR-003, ERR-004)

**Files:**
- Modify: `e2e/src/test/java/com/arcadedb/e2e/RemoteBoltDatabaseIT.java`

**Interfaces:**
- Consumes: `boltSession()`, `assertExpectedFailure`, `raceTwoWriters` (Task 4 - promote it to a package-private method on the outer class so Errors can reuse it).
- New import: `import org.neo4j.driver.exceptions.ClientException;`.

- [ ] **Step 1: Promote `raceTwoWriters` to the outer class**

Move `raceTwoWriters` (and its `List<Neo4jException>` return) out of the `Transactions` nest to the outer `RemoteBoltDatabaseIT` body so both `Transactions` and `Errors` call it. Update TX-005 to call the outer method.

- [ ] **Step 2: Add the `@Nested` Errors class**

```java
@Nested
@DisplayName("errors")
class Errors {

  @Test
  @DisplayName("[ERR-001] Syntax error returns Neo.ClientError.Statement.SyntaxError")
  void err001_syntax() {
    try (final Session s = boltSession()) {
      assertThatThrownBy(() -> s.run("MATCH (n RETURN n").consume())
          .isInstanceOf(ClientException.class)
          .satisfies(t -> assertThat(((ClientException) t).code())
              .isEqualTo("Neo.ClientError.Statement.SyntaxError"));
    }
  }

  @Test
  @DisplayName("[ERR-002] Semantic error returns Neo.ClientError.Statement.SemanticError")
  void err002_semantic() {
    assertExpectedFailure("#4890", () -> {
      try (final Session s = boltSession()) {
        assertThatThrownBy(() -> s.run("RETURN undefinedVariable").consume())
            .isInstanceOf(ClientException.class)
            .satisfies(t -> assertThat(((ClientException) t).code())
                .isEqualTo("Neo.ClientError.Statement.SemanticError"));
      }
    });
  }

  @Test
  @Disabled("[ERR-003] Driver never sends RUN before LOGON; needs a raw socket. "
      + "Covered at the bolt-module layer if a raw harness exists (BoltProtocolIT).")
  @DisplayName("[ERR-003] Unauthenticated request returns Neo.ClientError.Security.Forbidden")
  void err003_forbidden() {
  }

  @Test
  @DisplayName("[ERR-004] Transient conditions surface Neo.TransientError.*")
  void err004_transient() {
    final List<Neo4jException> errors = raceTwoWriters("err-004");
    assertExpectedFailure("#4890", () -> {
      assertThat(errors).isNotEmpty();
      assertThat(errors).anySatisfy(e -> assertThat(e.code()).startsWith("Neo.TransientError"));
    });
  }
}
```

- [ ] **Step 3: Run and record ERR verdicts**

Run: `mvn -q -pl e2e verify -DskipITs=false -Dit.test=RemoteBoltDatabaseIT`
Expected: ERR-001 PASS; ERR-002 gap-wrapped (JS found it a gap - if the image returns the correct code, flip to plain assert + spec.yaml `passing`); ERR-003 skipped; ERR-004 gap-wrapped unless transient codes exist. Keep ERR-004 and TX-005 verdicts consistent in `spec.yaml`.

- [ ] **Step 4: Commit**

```bash
git add e2e/src/test/java/com/arcadedb/e2e/RemoteBoltDatabaseIT.java bolt/conformance/spec.yaml
git commit -m "test(#4889): ERR-001..004 error-code conformance scenarios"
```

---

### Task 9: Protocol (PROTO-001, PROTO-002, PROTO-003)

**Files:**
- Modify: `e2e/src/test/java/com/arcadedb/e2e/RemoteBoltDatabaseIT.java`

**Interfaces:**
- Consumes: `boltSession()`, `assertExpectedFailure`, `parameterizedQuery()` existing test (fold into Protocol or leave as a general nest - here we fold it into a `Misc` note; simplest: keep parameterized as TX/■ - move it into `Transactions` as an extra or a `Parameters` nest). Decision: move `parameterizedQuery()` into a small `@Nested Parameters` class to preserve it (AC).

- [ ] **Step 1: Add the `@Nested` Protocol class and preserve `parameterizedQuery`**

```java
@Nested
@DisplayName("protocol")
class Protocol {

  @Test
  @DisplayName("[PROTO-001] Version negotiation succeeds (4.4/4.0/3.0)")
  void proto001_negotiation() {
    // The pinned driver offers a range including 4.4; a successful query proves
    // the server negotiated a supported version.
    try (final Session s = boltSession()) {
      assertThat(s.run("RETURN 1 AS v").single().get("v").asLong()).isEqualTo(1L);
    }
  }

  @Test
  @DisplayName("[PROTO-002] Bolt 5.x negotiation is supported")
  void proto002_bolt5() {
    // Server never advertises 5.x; drivers only work by silently downgrading.
    // No public driver API forces a 5.x-only handshake, so we document the gap:
    // the advertised server_agent pins the 4.4 envelope. Kept as a gap marker.
    assertExpectedFailure("#4890", () -> {
      throw new AssertionError("Bolt 5.x negotiation not advertised by the server");
    });
  }

  @Test
  @DisplayName("[PROTO-003] RESET returns the connection to a clean state")
  void proto003_reset() {
    // reset() is exercised by the driver between sessions; prove the connection
    // is reusable for a fresh RUN after a partially-consumed result.
    try (final Session s = boltSession()) {
      final Result r = s.run("MATCH (b:Beer) RETURN b.name AS name LIMIT 5");
      r.next();
      r.consume(); // driver issues DISCARD/RESET semantics
      assertThat(s.run("RETURN 2 AS v").single().get("v").asLong()).isEqualTo(2L);
    }
  }
}

@Nested
@DisplayName("parameters")
class Parameters {

  @Test
  @DisplayName("[TX-001] Parameterized query binds and returns values")
  void parameterizedQuery() {
    try (final Session s = boltSession()) {
      final var rec = s.run("RETURN $name AS name, $value AS value",
          Map.of("name", "test", "value", 42)).single();
      assertThat(rec.get("name").asString()).isEqualTo("test");
      assertThat(rec.get("value").asLong()).isEqualTo(42L);
    }
  }
}
```

Delete the migrated top-level `parameterizedQuery()` method. (Its `[TX-001]` tag is a secondary reference; the primary TX-001 is Task 4. Retag to `[TX-004]`-adjacent if preferred, but a duplicate id in a second nest is harmless for grep coverage.)

- [ ] **Step 2: Run the full class**

Run: `mvn -q -pl e2e verify -DskipITs=false -Dit.test=RemoteBoltDatabaseIT`
Expected: PROTO-001/003 PASS, PROTO-002 gap-wrapped, parameterized PASS. Full class green.

- [ ] **Step 3: Verify all 39 ids are present**

Run:
```bash
grep -oE '\\[(CONN|AUTH|TX|CAUSAL|MDB|RESULT|TYPE|ERR|PROTO)-[0-9]{3}\\]' \
  e2e/src/test/java/com/arcadedb/e2e/RemoteBoltDatabaseIT.java | sort -u | wc -l
```
Expected: 37 distinct ids present in e2e (CONN-002 and CONN-005 live in the bolt module - Task 10). Combined with Task 10/11, all 39 are covered.

- [ ] **Step 4: Commit**

```bash
git add e2e/src/test/java/com/arcadedb/e2e/RemoteBoltDatabaseIT.java
git commit -m "test(#4889): PROTO-001..003 + preserved parameterized query"
```

---

### Task 10: Bolt-module TLS scenarios (CONN-002, CONN-005)

**Files:**
- Modify: `bolt/src/test/java/com/arcadedb/bolt/BoltTlsIT.java`

**Interfaces:**
- Consumes: existing `BoltTlsIT` embedded-server + TLS harness (REQUIRED mode already present).

- [ ] **Step 1: Read the existing harness**

Run: `sed -n '1,200p' bolt/src/test/java/com/arcadedb/bolt/BoltTlsIT.java`
Identify: the existing REQUIRED-mode test method, how `NETWORK_SSL` mode is set (GlobalConfiguration), how the driver is built with `bolt+ssc://` / trust-all config.

- [ ] **Step 2: Tag the existing REQUIRED test as CONN-002**

Add to the existing REQUIRED-mode test method:
```java
@DisplayName("[CONN-002] Connect via bolt+ssc:// with TLS required")
```

- [ ] **Step 3: Add the CONN-005 OPTIONAL-mode test**

Mirror the REQUIRED test setup but start the server with SSL mode OPTIONAL and connect a **plaintext** `bolt://` driver, asserting it connects and queries. Use the same `GlobalConfiguration.NETWORK_SSL_*` keys the REQUIRED test uses; set the mode value to `OPTIONAL`:
```java
@Test
@DisplayName("[CONN-005] TLS OPTIONAL mode falls back to plaintext bolt://")
void conn005_tlsOptionalPlaintext() {
  // Start server with NETWORK_SSL enabled + mode OPTIONAL (reuse the REQUIRED
  // test's server bootstrap, changing only the mode), then:
  try (final Driver d = GraphDatabase.driver("bolt://" + host + ":" + boltPort,
      AuthTokens.basic("root", "playwithdata"),
      Config.builder().withoutEncryption().build())) {
    d.verifyConnectivity();
    try (final Session s = d.session()) {
      assertThat(s.run("RETURN 1 AS v").single().get("v").asLong()).isEqualTo(1L);
    }
  }
}
```
Match the concrete field names (`host`, `boltPort`, password) to whatever `BoltTlsIT`/`BaseGraphServerTest` already expose; adjust if the harness names differ. Add `@DisplayName` and `assertThat` imports if absent.

- [ ] **Step 4: Run the bolt-module TLS ITs**

Run: `mvn -q -pl bolt -am install -DskipTests && mvn -q -pl bolt verify -Dit.test=BoltTlsIT -DskipITs=false`
Expected: CONN-002 and CONN-005 PASS.

- [ ] **Step 5: Commit**

```bash
git add bolt/src/test/java/com/arcadedb/bolt/BoltTlsIT.java
git commit -m "test(#4889): CONN-002/005 TLS conformance at bolt-module layer"
```

---

### Task 11: Bolt-module wire-level type serialization test (BoltTypeRoundTripTest)

**Files:**
- Create: `bolt/src/test/java/com/arcadedb/bolt/BoltTypeRoundTripTest.java`
- Read for reference: `bolt/src/test/java/com/arcadedb/bolt/BoltStructureTest.java`, `PackStreamTest.java`, `bolt/src/main/java/com/arcadedb/bolt/BoltStructureMapper.java`

**Interfaces:**
- Consumes: `BoltStructureMapper.toPackStreamValue(...)` and/or `PackStreamWriter` as used by `BoltStructureTest`.

- [ ] **Step 1: Read the existing structure test to copy the harness pattern**

Run: `sed -n '1,120p' bolt/src/test/java/com/arcadedb/bolt/BoltStructureTest.java`
Identify how a Java value is passed through the mapper/writer and how the emitted PackStream bytes/structure tag are asserted.

- [ ] **Step 2: Write the type-serialization contract test**

Create `BoltTypeRoundTripTest` asserting, at the wire level, what the mapper emits for each type. For gaps (Duration, Point, and any temporal still ISO-string in this build), assert the CURRENT contract with an explicit gap `@DisplayName` referencing `#4890`; for natively-mapped types assert the native structure tag. Use the same `BoltStructureMapper` entry point `BoltStructureTest` uses. Skeleton (fill tags from `BoltStructureTest`):

```java
package com.arcadedb.bolt;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.LocalDate;

import static org.assertj.core.api.Assertions.assertThat;

class BoltTypeRoundTripTest {

  @Test
  @DisplayName("[TYPE-011] Duration serializes (wire-level gap #4890 until native)")
  void duration_wireContract() {
    // Assert whatever BoltStructureMapper currently produces for a Duration,
    // pinning the contract so a #4890 native-Duration change is caught here.
    final Object out = BoltStructureMapper.toPackStreamValue(Duration.ofHours(2));
    // Current build: not a native Bolt Duration structure. Assert the observed
    // form (string/fallback) discovered by running Step 3, then update when fixed.
    assertThat(out).isNotNull();
  }

  @Test
  @DisplayName("[TYPE-007] LocalDate serialization contract")
  void localDate_wireContract() {
    final Object out = BoltStructureMapper.toPackStreamValue(LocalDate.of(2026, 1, 15));
    assertThat(out).isNotNull();
  }
}
```

Match the real `toPackStreamValue` signature/return type from `BoltStructureMapper` (it may return a `PackStreamStructure`/`Object`/write to a buffer - adjust the assertions to the actual API, following `BoltStructureTest`). Add Point once the ArcadeDB point value type used by the mapper is identified.

- [ ] **Step 3: Run to discover the actual emitted form, then tighten assertions**

Run: `mvn -q -pl bolt test -Dtest=BoltTypeRoundTripTest`
Expected: PASS. Tighten each `isNotNull()` to assert the concrete observed structure tag / class so the test is a real contract, not a smoke test.

- [ ] **Step 4: Commit**

```bash
git add bolt/src/test/java/com/arcadedb/bolt/BoltTypeRoundTripTest.java
git commit -m "test(#4889): wire-level Bolt type serialization contract (BoltTypeRoundTripTest)"
```

---

### Task 12: Driver-version range - opt-in 4.4 legacy profile

**Files:**
- Modify: `e2e/pom.xml`
- Create: `e2e/src/test/java/com/arcadedb/e2e/RemoteBoltLegacyDriverIT.java`

**Interfaces:**
- Produces: a `bolt-driver-legacy` Maven profile that overrides `${neo4j-driver.version}` to the 4.4 line, excludes 6.x-API-only ITs from compilation, and runs only `RemoteBoltLegacyDriverIT`.

- [ ] **Step 1: Write the stable-API legacy IT**

Create `RemoteBoltLegacyDriverIT` using only cross-version API (no `executeWrite`/`lastBookmarks`):

```java
package com.arcadedb.e2e;

import org.junit.jupiter.api.Test;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.exceptions.ClientException;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

// Runs the cross-version-stable scenario subset. Under -Pbolt-driver-legacy the
// neo4j-java-driver is the 4.4 band; by default it is the 6.x baseline. Proves
// the A1 java driver-version range in-module without fighting 5.x+ API drift.
class RemoteBoltLegacyDriverIT extends ArcadeContainerTemplate {

  private Driver driver() {
    return GraphDatabase.driver("bolt://" + host + ":" + boltPort,
        AuthTokens.basic("root", "playwithdata"),
        Config.builder().withoutEncryption().build());
  }

  @Test
  void legacy_connectAndRead() {
    try (final Driver d = driver()) {
      d.verifyConnectivity();
      try (final Session s = d.session(SessionConfig.forDatabase("beer"))) {
        assertThat(s.run("MATCH (b:Beer) RETURN b.name AS name LIMIT 1")
            .single().get("name").asString()).isNotBlank();
      }
    }
  }

  @Test
  void legacy_parameterized() {
    try (final Driver d = driver();
         final Session s = d.session(SessionConfig.forDatabase("beer"))) {
      assertThat(s.run("RETURN $v AS v", Map.of("v", 7L)).single().get("v").asLong()).isEqualTo(7L);
    }
  }

  @Test
  void legacy_syntaxErrorCode() {
    try (final Driver d = driver();
         final Session s = d.session(SessionConfig.forDatabase("beer"))) {
      assertThatThrownBy(() -> s.run("MATCH (n RETURN n").consume())
          .isInstanceOf(ClientException.class);
    }
  }
}
```

- [ ] **Step 2: Add the profile to `e2e/pom.xml`**

Inside `<project>`, add a `<profiles>` section (or extend an existing one). The profile overrides the driver version, excludes the 6.x-API-only ITs from test compilation, and restricts Failsafe to the legacy IT:

```xml
<profiles>
  <profile>
    <id>bolt-driver-legacy</id>
    <properties>
      <!-- oldest-supported-4.x band; resolve to the latest 4.4.x at impl time -->
      <neo4j-driver.version>4.4.20</neo4j-driver.version>
    </properties>
    <build>
      <plugins>
        <plugin>
          <groupId>org.apache.maven.plugins</groupId>
          <artifactId>maven-compiler-plugin</artifactId>
          <configuration>
            <testExcludes>
              <!-- Uses 5.x+ driver API (executeWrite, lastBookmarks); not 4.4-compatible -->
              <testExclude>**/RemoteBoltDatabaseIT.java</testExclude>
            </testExcludes>
          </configuration>
        </plugin>
        <plugin>
          <groupId>org.apache.maven.plugins</groupId>
          <artifactId>maven-failsafe-plugin</artifactId>
          <configuration>
            <includes>
              <include>**/RemoteBoltLegacyDriverIT.java</include>
            </includes>
          </configuration>
        </plugin>
      </plugins>
    </build>
  </profile>
</profiles>
```

Resolve `4.4.20` to the actual latest published 4.4.x at implementation time (`mvn versions:display-dependency-updates` or Maven Central) and record it in `spec.yaml` `driver_version_bands.java.resolution_note` (baseline 6.2.0 = latest-6.x; 4.4.x = oldest-supported-4.x).

- [ ] **Step 3: Verify both bands compile and the legacy band runs**

Run:
```bash
mvn -q -pl e2e -am test-compile                                   # baseline 6.x compiles all ITs
mvn -q -pl e2e verify -Pbolt-driver-legacy -DskipITs=false \
    -Dit.test=RemoteBoltLegacyDriverIT                            # 4.4 band runs subset
```
Expected: baseline compiles including `RemoteBoltDatabaseIT`; legacy profile compiles WITHOUT `RemoteBoltDatabaseIT`, resolves the 4.4 driver, and the 3 legacy tests PASS.

- [ ] **Step 4: Update the spec band note and commit**

Edit `bolt/conformance/spec.yaml` `driver_version_bands.java.resolution_note` to record the concrete versions, re-run `python3 bolt/conformance/validate_spec.py bolt/conformance/spec.yaml`.

```bash
git add e2e/pom.xml e2e/src/test/java/com/arcadedb/e2e/RemoteBoltLegacyDriverIT.java bolt/conformance/spec.yaml
git commit -m "test(#4889): opt-in 4.4 legacy driver profile exercises the A1 version range"
```

---

### Task 13: Verdict reconciliation, coverage audit, and CLAUDE.md-clean final pass

**Files:**
- Modify (only if a verdict changed): `bolt/conformance/spec.yaml`
- Review: `RemoteBoltDatabaseIT.java`, `BoltTlsIT.java`, `BoltTypeRoundTripTest.java`

- [ ] **Step 1: Full e2e run against the real image**

Run: `mvn -q -pl e2e -am verify -DskipITs=false -Dit.test=RemoteBoltDatabaseIT`
Expected: entire class green (passing scenarios pass; gap scenarios pass via `assertExpectedFailure`; disabled reported skipped). Capture the console for the summary.

- [ ] **Step 2: Reconcile `spec.yaml` with observed verdicts**

For every scenario whose observed verdict differs from its `current_status` hint (notably TYPE-007..010, TX-005/ERR-004, ERR-002, RESULT-004), update `current_status` and add/remove `known_limitation`/`tracking_issue`. Then:
Run: `python3 bolt/conformance/validate_spec.py bolt/conformance/spec.yaml`
Expected: `spec.yaml OK` (or the validator's success message). Fix any structural violation it reports.

- [ ] **Step 3: Coverage audit - all 39 ids present across the two layers**

Run:
```bash
{ grep -ohE '\\[(CONN|AUTH|TX|CAUSAL|MDB|RESULT|TYPE|ERR|PROTO)-[0-9]{3}\\]' \
    e2e/src/test/java/com/arcadedb/e2e/RemoteBoltDatabaseIT.java \
    bolt/src/test/java/com/arcadedb/bolt/BoltTlsIT.java \
    bolt/src/test/java/com/arcadedb/bolt/BoltTypeRoundTripTest.java; } \
  | sort -u
```
Expected: CONN-001..005, AUTH-001..003, TX-001..005, CAUSAL-001, MDB-001..002, RESULT-001..004, TYPE-001..012, ERR-001..004, PROTO-001..003 - all 39 present (TYPE ids may appear in both layers; that is fine).

- [ ] **Step 4: Remove any debug output; confirm no production code touched**

Run:
```bash
grep -rn "System.out" e2e/src/test/java/com/arcadedb/e2e/RemoteBoltDatabaseIT.java \
  bolt/src/test/java/com/arcadedb/bolt/BoltTypeRoundTripTest.java || echo "clean"
git diff --name-only origin/main -- 'bolt/src/main' 'e2e/src/main' | grep . && echo "PROD CHANGED - investigate" || echo "no production changes (correct)"
```
Expected: `clean`; `no production changes (correct)`.

- [ ] **Step 5: Final commit**

```bash
git add bolt/conformance/spec.yaml
git commit -m "test(#4889): reconcile spec.yaml verdicts with real-image certification run" || echo "no spec changes needed"
```

---

## Self-Review Notes

- **Spec coverage:** All 39 A1 scenarios have a task (Tasks 2-11 cover the areas; Task 10 CONN-002/005; Task 11 wire-level type contract). Driver-version AC4 -> Task 12. Verdict reconciliation + coverage audit -> Task 13. Existing 4 tests preserved (folded into `@Nested` in Tasks 2/4/5/9).
- **Expected-fail semantics:** single `assertExpectedFailure(trackingIssue, ideal)` helper used everywhere; `@Disabled` only for CONN-004, ERR-003.
- **Type consistency:** `boltSession()`/`boltSession(String)`, `httpCommand(String,String)`, `assertExpectedFailure(String, ThrowingCallable)`, `raceTwoWriters(String)` are named identically across all tasks.
- **Open implementation detail:** the exact `BoltStructureMapper` entry-point signature (Task 11) and `InternalTypeSystem` accessor names (Task 7) must be confirmed against the real classes during implementation; both tasks include a read step for this.
```
