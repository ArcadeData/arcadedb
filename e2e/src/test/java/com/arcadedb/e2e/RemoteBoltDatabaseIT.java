/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.e2e;

import com.arcadedb.serializer.json.JSONObject;

import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Bookmark;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.Neo4jException;
import org.neo4j.driver.exceptions.SecurityException;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.exceptions.SessionExpiredException;
import org.neo4j.driver.exceptions.TransientException;
import org.neo4j.driver.summary.ResultSummary;
import org.neo4j.driver.types.IsoDuration;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Path;
import org.neo4j.driver.types.Point;
import org.neo4j.driver.types.Relationship;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.temporal.Temporal;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.*;

/**
 * End-to-end certification of the Bolt protocol against the real
 * {@code neo4j-java-driver}, covering the shared conformance spec
 * (bolt/conformance/spec.yaml, issue #4883, part of epic #4882). Each test
 * embeds its scenario id in a {@link DisplayName} so coverage is grep-checkable
 * and comparable to the JS/Go suites.
 * <p>
 * These ITs run against the published {@code arcadedata/arcadedb:latest} image
 * (see {@link ArcadeContainerTemplate}), so their verdicts reflect the last
 * released server and lag this branch by one release. Branch-local Bolt wire
 * behavior is guarded by the embedded-server tests in the {@code bolt} module
 * (e.g. {@code BoltTypeRoundTripTest}, {@code BoltTlsIT}).
 */
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

    // Seed over HTTP only, never over Bolt - Bolt serialization is under test.
    httpCommand("server", new JSONObject().put("command", "create database boltscratch").toString());
    httpCommand("command/beer", new JSONObject()
        .put("language", "cypher").put("command", TYPE_MATRIX_CYPHER).toString());
  }

  @AfterAll
  void tearDownClass() {
    if (driver != null) {
      // Sweep the probe nodes the write scenarios leave in the shared beer
      // database so a later suite that asserts an unscoped count is not surprised.
      try (final Session s = boltSession()) {
        s.run("MATCH (n) WHERE n:TxProbe OR n:RaceProbe OR n:CausalProbe OR n:R004Src OR n:R004Dst DETACH DELETE n").consume();
        s.run("MATCH (b:Beer) WHERE b.name = 'TX-004-Beer' DETACH DELETE b").consume();
      } catch (final RuntimeException ignored) {
        // best-effort cleanup; never fail the suite on teardown
      }
      driver.close();
    }
  }

  Session boltSession() {
    return boltSession("beer");
  }

  Session boltSession(final String database) {
    return driver.session(SessionConfig.forDatabase(database));
  }

  // Passes only when `ideal` fails the way a documented gap should: an
  // AssertionError (the ideal assertion did not hold) or a server-side
  // Neo4jException that is NOT an infra/transient failure (the server rejected
  // or mis-typed the value). Connectivity and transient exceptions - which also
  // extend Neo4jException - are re-thrown so a flaky hiccup can never mask a
  // real regression as "gap still present". Fails loudly the day the gap
  // closes. Java analogue of the JS suite's it.failing.
  static void assertExpectedFailure(final String trackingIssue, final ThrowingCallable ideal) {
    try {
      ideal.call();
    } catch (final AssertionError expectedGap) {
      return;
    } catch (final ServiceUnavailableException | SessionExpiredException | TransientException infra) {
      throw new AssertionError("Expected-fail scenario " + trackingIssue
          + " hit an infra/transient error, not the tracked gap: " + infra, infra);
    } catch (final Neo4jException serverRejection) {
      return;
    } catch (final Throwable unexpected) {
      throw new AssertionError("Expected-fail scenario " + trackingIssue
          + " threw an unexpected error type (not the tracked gap): " + unexpected, unexpected);
    }
    fail("Expected-fail scenario now PASSES - close it: flip the assertion and update "
        + "bolt/conformance/spec.yaml current_status (" + trackingIssue + ")");
  }

  // Shared concurrency helper for TX-005 and ERR-004. Two sessions race to
  // update the same node inside explicit transactions, each held open past the
  // other's write, so the losing commit hits ArcadeDB's optimistic concurrency
  // check. Returns the collected driver-side errors; callers assert on the
  // whole set (timing-sensitive by nature), not a single index. Deliberately
  // does NOT use managed executeWrite - that would auto-retry and swallow the
  // transient error this helper exists to surface.
  List<Neo4jException> raceTwoWriters(final String marker) {
    try (final Session setup = boltSession()) {
      setup.run("MERGE (n:RaceProbe {marker: $m}) SET n.value = 0", Map.of("m", marker)).consume();
    }
    final List<Neo4jException> errors = new CopyOnWriteArrayList<>();
    // A barrier makes both transactions provably hold their uncommitted write
    // open before either commits, so the write-write conflict is deterministic
    // rather than dependent on two sleep windows overlapping (avoids a
    // scheduler-serialized false negative under CI load).
    final CyclicBarrier bothHaveWritten = new CyclicBarrier(2);
    // Records a barrier failure so an inconclusive run (the two writes never
    // held concurrently) is distinguishable from a genuine "no transient error"
    // result, rather than both surfacing as an empty errors list.
    final AtomicReference<Throwable> barrierFailure = new AtomicReference<>();
    final Runnable racingWrite = () -> {
      // try-with-resources closes (and thus rolls back an uncommitted) tx and
      // session before the catch runs, so a losing commit still surfaces here.
      try (final Session s = boltSession();
          final Transaction tx = s.beginTransaction()) {
        tx.run("MATCH (n:RaceProbe {marker: $m}) SET n.value = n.value + 1 RETURN n",
            Map.of("m", marker)).consume();
        bothHaveWritten.await(15, TimeUnit.SECONDS);
        tx.commit();
      } catch (final Neo4jException e) {
        errors.add(e);
      } catch (final BrokenBarrierException | TimeoutException e) {
        barrierFailure.compareAndSet(null, e);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    };
    final Thread a = new Thread(racingWrite);
    final Thread b = new Thread(racingWrite);
    a.start();
    b.start();
    try {
      a.join();
      b.join();
    } catch (final InterruptedException ignored) {
      Thread.currentThread().interrupt();
    }
    if (errors.isEmpty() && barrierFailure.get() != null)
      throw new IllegalStateException("""
          Two-writer race was inconclusive: the writes never held \
          concurrently (barrier did not sync), so no conflict could form""", barrierFailure.get());
    return errors;
  }

  // A racing write-write conflict must surface a retryable Neo.TransientError.*,
  // never a non-retryable ClientError/DatabaseError - the precondition for
  // driver-side managed-transaction retry.
  static void assertTransientRace(final List<Neo4jException> errors) {
    assertThat(errors).isNotEmpty();
    assertThat(errors).anySatisfy(e -> assertThat(e.code()).startsWith("Neo.TransientError"));
    assertThat(errors).allSatisfy(e -> assertThat(e.code()).doesNotMatch("Neo\\.(ClientError|DatabaseError)\\..*"));
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
    if (code < 200 || code >= 300) {
      final InputStream es = con.getErrorStream();
      final String detail = es != null ? new String(es.readAllBytes(), StandardCharsets.UTF_8) : "no error body";
      throw new IllegalStateException("HTTP " + code + ": " + detail);
    }
    con.disconnect();
  }

  // ==================== connection ====================

  @Nested
  @DisplayName("connection")
  class Connection {

    @Test
    @DisplayName("[CONN-001] Connect via bolt:// scheme")
    void conn001_boltScheme() {
      driver.verifyConnectivity();
    }

    @Test
    @DisabledOnOs(value = { OS.MAC, OS.WINDOWS }, disabledReason = """
        neo4j:// single-node routing advertises the container bridge IP, \
        which is not host-routable on Docker Desktop (macOS/Windows); verified in Linux CI only""")
    @DisplayName("[CONN-003] neo4j:// routing discovery, single-node")
    void conn003_routingSingleNode() {
      // handleRoute advertises the server's own bound address
      // (socket.getLocalAddress(), i.e. the container bridge IP such as
      // 172.17.0.x). On the Linux CI runner the Docker bridge subnet is routable
      // from the host, so the driver reaches that address and the scenario passes
      // - identical to the e2e-js/e2e-go/e2e-python suites. On Docker Desktop
      // (macOS/Windows) container IPs are not host-routable, so this scenario can
      // only be fully verified in CI; bolt:// scenarios are unaffected because
      // they use the mapped host port.
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
    @Disabled("""
        [CONN-004] Requires a real 3-node HA cluster; single-node container \
        always returns itself as writer/reader/router (#4890)""")
    @DisplayName("[CONN-004] neo4j:// routing reflects HA cluster topology")
    void conn004_haTopology() {
    }
  }

  // ==================== auth ====================

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
        assertThatThrownBy(d::verifyConnectivity).isInstanceOf(SecurityException.class);
      }
    }

    @Test
    @DisplayName("[AUTH-003] Auth scheme 'none' is rejected (intentional, not a bug)")
    void auth003_noneRejected() {
      try (final Driver d = GraphDatabase.driver("bolt://" + host + ":" + boltPort,
          AuthTokens.none(),
          Config.builder().withoutEncryption().build())) {
        assertThatThrownBy(d::verifyConnectivity).isInstanceOf(SecurityException.class);
      }
    }
  }

  // ==================== transactions ====================

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
        try (final Transaction tx = s.beginTransaction()) {
          tx.run("CREATE (:TxProbe {marker: 'tx-002'})");
          tx.commit();
        }
        assertThat(s.run("MATCH (n:TxProbe {marker: 'tx-002'}) RETURN count(n) AS c")
            .single().get("c").asLong()).isEqualTo(1L);
      }
    }

    @Test
    @DisplayName("[TX-003] Explicit BEGIN/RUN/ROLLBACK discards changes")
    void tx003_rollback() {
      try (final Session s = boltSession()) {
        try (final Transaction tx = s.beginTransaction()) {
          tx.run("CREATE (:TxProbe {marker: 'tx-003'})");
          tx.rollback();
        }
        assertThat(s.run("MATCH (n:TxProbe {marker: 'tx-003'}) RETURN count(n) AS c")
            .single().get("c").asLong()).isEqualTo(0L);
      }
    }

    @Test
    @DisplayName("[TX-004] Managed executeWrite commits on success")
    void tx004_executeWrite() {
      try (final Session s = boltSession()) {
        s.executeWrite(tx -> tx.run("CREATE (:Beer {name: $n})",
            Map.of("n", "TX-004-Beer")).consume());
        assertThat(s.run("MATCH (b:Beer {name: $n}) RETURN count(b) AS c",
            Map.of("n", "TX-004-Beer")).single().get("c").asLong()).isEqualTo(1L);
      }
    }

    @Test
    @DisplayName("[TX-005] Managed transaction retries on Neo.TransientError.*")
    void tx005_transientRetry() {
      // The write-write conflict must be classified retryable (Neo.TransientError.*)
      // so the driver's managed-transaction retry policy can act on it.
      assertTransientRace(raceTwoWriters("tx-005"));
    }
  }

  // ==================== causal-consistency ====================

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

  // ==================== multi-database ====================

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
      try (final Session scratch = boltSession("boltscratch");
          final Transaction tx = scratch.beginTransaction()) {
        tx.run("CREATE (:ScratchOnly {marker: 'mdb-002'})");
        try (final Session beer = boltSession("beer")) {
          assertThat(beer.run("MATCH (n:ScratchOnly) RETURN count(n) AS c")
              .single().get("c").asLong()).isEqualTo(0L);
        }
        tx.rollback();
      }
    }
  }

  // ==================== result-handling ====================

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
        final Record first = r.next();
        final Record second = r.next();
        assertThat(first.get("name").asString()).isNotBlank();
        assertThat(second.get("name").asString()).isNotBlank();
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
      // Certifies that the RUN summary carries write counters. Uses dedicated types rather than the
      // fixture's Beer/Brewery: the ArcadeDB container is shared across all e2e IT classes, and
      // RemoteDatabaseJavaApiIT renames the Beer type (ALTER TYPE ... NAME), which corrupts Beer's
      // edge buckets for any later suite. Dedicated types keep this scenario order-independent.
      try (final Session s = boltSession()) {
        final ResultSummary summary = s.run(
            "CREATE (:R004Src {name: $n})-[:R004_MADE]->(:R004Dst {name: $b})",
            Map.of("n", "R004-Src", "b", "R004-Dst")).consume();
        assertThat(summary.counters().nodesCreated()).isEqualTo(2);
        assertThat(summary.counters().relationshipsCreated()).isEqualTo(1);
      }
    }
  }

  // ==================== type-roundtrip ====================

  @Nested
  @DisplayName("type-roundtrip")
  class TypeRoundTrip {

    @Test
    @DisplayName("[TYPE-001] Node round-trips as a native Bolt structure")
    void type001_node() {
      // Per the spec, this certifies a native Node with .labels() and .asMap()
      // populated - not a specific label string. The exact label is a dataset
      // detail, so assert the structure round-tripped, not the value.
      try (final Session s = boltSession()) {
        final Node n = s.run("MATCH (b:Beer) RETURN b LIMIT 1").single().get("b").asNode();
        assertThat(n.labels()).isNotEmpty();
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
      try (final Session s = boltSession()) {
        final Object v = s.run("MATCH p=(b:Beer)-[*1..2]-() RETURN p LIMIT 1").single().get("p").asObject();
        assertThat(v).isInstanceOf(Path.class);
      }
    }

    @Test
    @DisplayName("[TYPE-004] ByteArray round-trips as a bound parameter")
    void type004_byteArray() {
      try (final Session s = boltSession()) {
        final byte[] in = { 1, 2, 3, 4 };
        assertThat(s.run("RETURN $b AS echo", Map.of("b", in))
            .single().get("echo").asByteArray()).isEqualTo(in);
      }
    }

    @Test
    @DisplayName("[TYPE-005] Nested lists and maps round-trip structurally")
    void type005_nested() {
      try (final Session s = boltSession()) {
        final Record rec = s.run(
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
        assertThat(s.run("RETURN $p AS echo", Collections.singletonMap("p", null))
            .single().get("echo").isNull()).isTrue();
      }
    }

    @Test
    @DisplayName("[TYPE-007] LocalDate round-trips as a native Bolt Date")
    void type007_localDate() {
      assertNativeTemporal("localDateProp");
    }

    @Test
    @DisplayName("[TYPE-008] LocalTime round-trips as a native Bolt LocalTime")
    void type008_localTime() {
      assertNativeTemporal("localTimeProp");
    }

    @Test
    @DisplayName("[TYPE-009] LocalDateTime round-trips as a native Bolt LocalDateTime")
    void type009_localDateTime() {
      assertNativeTemporal("localDateTimeProp");
    }

    @Test
    @DisplayName("[TYPE-010] Offset DateTime round-trips as a native Bolt DateTime")
    void type010_offsetDateTime() {
      assertNativeTemporal("offsetDateTimeProp");
    }

    @Test
    @DisplayName("[TYPE-011] Duration round-trips as a native Bolt Duration")
    void type011_duration() {
      try (final Session s = boltSession()) {
        final Object v = s.run("MATCH (t:TypeMatrix) RETURN t.durationProp AS d LIMIT 1").single().get("d").asObject();
        assertThat(v).isInstanceOf(IsoDuration.class);
      }
    }

    @Test
    @DisplayName("[TYPE-012] Point round-trips as a native Bolt Point")
    void type012_point() {
      try (final Session s = boltSession()) {
        final Object v = s.run("MATCH (t:TypeMatrix) RETURN t.pointProp AS p LIMIT 1").single().get("p").asObject();
        assertThat(v).isInstanceOf(Point.class);
      }
    }

    // A native Bolt temporal deserializes to a java.time.temporal.Temporal; the
    // legacy ISO-string fallback would yield a String. spec.yaml marks
    // TYPE-007..010 passing/native (pinned at the wire level by
    // BoltTypeRoundTripTest), so this is a plain assertion that fails loudly if
    // the server ever regresses to strings - real e2e regression coverage.
    private void assertNativeTemporal(final String prop) {
      try (final Session s = boltSession()) {
        final Object v = s.run("MATCH (t:TypeMatrix) RETURN t." + prop + " AS x LIMIT 1")
            .single().get("x").asObject();
        assertThat(v).isInstanceOf(Temporal.class);
      }
    }
  }

  // ==================== errors ====================

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
      try (final Session s = boltSession()) {
        assertThatThrownBy(() -> s.run("RETURN undefinedVariable").consume())
            .isInstanceOf(ClientException.class)
            .satisfies(t -> assertThat(((ClientException) t).code())
                .isEqualTo("Neo.ClientError.Statement.SemanticError"));
      }
    }

    @Test
    @Disabled("""
        [ERR-003] Driver never sends RUN before LOGON; needs a raw socket. \
        Covered at the bolt-module layer (BoltProtocolIT) if a raw harness exists.""")
    @DisplayName("[ERR-003] Unauthenticated request returns Neo.ClientError.Security.Forbidden")
    void err003_forbidden() {
    }

    @Test
    @DisplayName("[ERR-004] Transient conditions surface Neo.TransientError.*")
    void err004_transient() {
      assertTransientRace(raceTwoWriters("err-004"));
    }
  }

  // ==================== protocol ====================

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
      // Assert on the version actually negotiated with the pinned (5.x-capable)
      // driver. The server now advertises Bolt 5.0-5.4 (in addition to legacy
      // 3.0/4.0/4.4), so it negotiates a 5.x version with this driver.
      final String negotiated;
      try (final Session s = boltSession()) {
        negotiated = s.run("RETURN 1").consume().server().protocolVersion();
      }
      // Compare on the integer major so 5.10 is not mis-parsed (Double would
      // read it as 5.1).
      final int major = Integer.parseInt(negotiated.split("\\.")[0]);
      assertThat(major).isGreaterThanOrEqualTo(5);
    }

    @Test
    @DisplayName("[PROTO-003] RESET returns the connection to a clean state")
    void proto003_reset() {
      try (final Session s = boltSession()) {
        final Result r = s.run("MATCH (b:Beer) RETURN b.name AS name LIMIT 5");
        r.next();
        r.consume();
        assertThat(s.run("RETURN 2 AS v").single().get("v").asLong()).isEqualTo(2L);
      }
    }
  }

  // ============== parameters (preserves the original parameterized test) ==============

  @Nested
  @DisplayName("parameters")
  class Parameters {

    @Test
    @DisplayName("Parameterized query binds and returns values (preserved original coverage)")
    void parameterizedQuery() {
      try (final Session s = boltSession()) {
        final Record rec = s.run("RETURN $name AS name, $value AS value",
            Map.of("name", "test", "value", 42)).single();
        assertThat(rec.get("name").asString()).isEqualTo("test");
        assertThat(rec.get("value").asLong()).isEqualTo(42L);
      }
    }
  }
}
