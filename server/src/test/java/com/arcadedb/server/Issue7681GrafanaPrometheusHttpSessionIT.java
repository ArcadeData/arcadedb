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
package com.arcadedb.server;

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.http.HttpSessionManager;
import com.arcadedb.server.http.handler.AbstractBinaryHttpHandler;
import com.arcadedb.server.http.handler.DatabaseAbstractHandler;
import com.arcadedb.server.http.handler.GetGrafanaHealthHandler;
import com.arcadedb.server.http.handler.GetGrafanaMetadataHandler;
import com.arcadedb.server.http.handler.GetPromQLLabelValuesHandler;
import com.arcadedb.server.http.handler.GetPromQLLabelsHandler;
import com.arcadedb.server.http.handler.GetPromQLQueryHandler;
import com.arcadedb.server.http.handler.GetPromQLQueryRangeHandler;
import com.arcadedb.server.http.handler.GetPromQLSeriesHandler;
import com.arcadedb.server.http.handler.PostGrafanaQueryHandler;
import com.arcadedb.server.http.handler.PostPrometheusReadHandler;
import com.arcadedb.server.http.handler.PostPrometheusWriteHandler;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.Label;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.LabelMatcher;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.MatchType;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.Query;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.ReadRequest;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.ReadResponse;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.Sample;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.TimeSeries;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.WriteRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.xerial.snappy.Snappy;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7681: the ten Grafana and Prometheus routes registered under {@code /api/v1/ts/{database}} extended
 * {@code AbstractServerHttpHandler} (or {@code AbstractBinaryHttpHandler}, which extended it), and only
 * {@code DatabaseAbstractHandler.setTransactionInThreadLocal} reads {@code arcadedb-session-id}. Issue #7402
 * had moved the three documented {@code /api/v1/ts} routes; these ten kept the hand-rolled
 * {@code checkAuthorizationOnDatabase} call whose own javadoc said it existed <i>"Because these handlers do
 * not extend {@code DatabaseAbstractHandler}"</i>. A client that had opened a session with
 * {@code POST /api/v1/begin/{db}} and presented its id on, say, {@code POST /ts/{db}/grafana/query} or
 * {@code POST /ts/{db}/prom/write} was answered from outside that session: no session transaction, no session
 * lock, no principal, and no refresh of the idle timer that decides when its transaction is rolled back
 * underneath it. The header was accepted by the transport and dropped by the handler, which is the failure
 * mode that cannot be noticed from the answer.
 * <p>
 * <b>What these tests assert, and why it is the echo.</b> The {@code arcadedb-session-id} response header is
 * emitted in exactly one place - {@code DatabaseAbstractHandler.setTransactionInThreadLocal}, on the branch
 * that binds the session's transaction onto this thread - so its presence is the observable proof that the
 * route resolved the session rather than ignoring the header, and its absence is what every one of these
 * routes produced before this change. A time-series append is not part of the enclosing transaction either way
 * ({@code TimeSeriesShard.appendSamples} wraps each shard write in its own {@code begin}/{@code commit} and an
 * ArcadeDB nested transaction is an independent transaction rather than a savepoint, issue #7410), so
 * "the samples appeared/disappeared with the transaction" is not available as an assertion here: it would pass
 * before and after. The things that DO change are asserted instead, each false against the unfixed server:
 * <ul>
 * <li>each of the ten routes echoes the session id it was handed;</li>
 * <li>{@code POST /prom/write} naming a session this server cannot resolve is refused instead of silently
 * ingesting outside it, and naming another principal's session is refused for the same reason;</li>
 * <li>the nine reads naming an unresolvable session still answer - the deliberate asymmetry with the write,
 * and the same split {@code GET /query} and the {@code /api/v1/ts} reads already have;</li>
 * <li>a read taken inside a session leaves that session's own uncommitted work intact, which the base class's
 * detach-don't-roll-back teardown is what guarantees;</li>
 * <li>concurrent remote-writes do not ingest each other's bodies, which the byte body moving off a field
 * shared by the singleton handler is what guarantees (the shape of issue #7683, in the binary base class).</li>
 * </ul>
 */
class Issue7681GrafanaPrometheusHttpSessionIT extends BaseGraphServerTest {

  private static final String TYPE       = "Ts7681";
  private static final String PROM_TYPE  = "Ts7681Prom";
  private static final String RACE_TYPE  = "Ts7681Race";
  private static final String DOC_TYPE   = "Ts7681Doc";
  private static final String OTHER_USER = "ts7681-other";
  private static final String OTHER_PWD  = "ts7681pwd1";
  /** Shaped like a real id ("AS-" + UUID) so a refusal cannot be attributed to a malformed value. */
  private static final String UNKNOWN_SESSION = "AS-00000000-0000-0000-0000-000000007681";

  private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();

  /**
   * The server's user registry outlives a single test class - one JVM runs the whole IT phase with
   * {@code reuseForks} - so a principal left behind here would be inherited by every class that runs after
   * this one. Dropped before {@code super.endTest()} stops the server that owns the registry.
   */
  @AfterEach
  @Override
  public void endTest() {
    try {
      final var security = getServer(0).getSecurity();
      if (security.existsUser(OTHER_USER))
        security.dropUser(OTHER_USER);
    } catch (final Exception ignore) {
      // the server may already be down, or the test may already have failed; do not mask it with a teardown error
    }
    super.endTest();
  }

  // ─────────────────────────────────────────────────────────────────────────────
  //  Each of the ten routes resolves the session it is handed
  // ─────────────────────────────────────────────────────────────────────────────

  @Test
  void theGrafanaHealthRouteRunsInsideTheSessionItIsHanded() throws Exception {
    seed();
    final String session = beginSession(rootAuth());
    try {
      final HttpResponse<String> response = get("/ts/" + getDatabaseName() + "/grafana/health", rootAuth(), session);

      assertThat(response.statusCode()).as("health failed: %s", response.body()).isEqualTo(200);
      assertThat(sessionIdOf(response))
          .as("#7681: GET /ts/{db}/grafana/health must resolve the session id it is handed, not drop it")
          .isEqualTo(session);
      assertThat(new JSONObject(response.body()).getString("database")).isEqualTo(getDatabaseName());
    } finally {
      rollback(rootAuth(), session);
    }
  }

  @Test
  void theGrafanaMetadataRouteRunsInsideTheSessionItIsHanded() throws Exception {
    seed();
    final String session = beginSession(rootAuth());
    try {
      final HttpResponse<String> response = get("/ts/" + getDatabaseName() + "/grafana/metadata", rootAuth(), session);

      assertThat(response.statusCode()).as("metadata failed: %s", response.body()).isEqualTo(200);
      assertThat(sessionIdOf(response))
          .as("#7681: GET /ts/{db}/grafana/metadata must resolve the session id it is handed, not drop it")
          .isEqualTo(session);
      assertThat(typeNamesOf(response))
          .as("a route that binds the session and then answers the wrong thing would be the worse regression")
          .contains(TYPE);
    } finally {
      rollback(rootAuth(), session);
    }
  }

  @Test
  void theGrafanaQueryRouteRunsInsideTheSessionItIsHanded() throws Exception {
    seed();
    final String session = beginSession(rootAuth());
    try {
      final HttpResponse<String> response = grafanaQuery(rootAuth(), session);

      assertThat(response.statusCode()).as("grafana query failed: %s", response.body()).isEqualTo(200);
      assertThat(sessionIdOf(response))
          .as("#7681: POST /ts/{db}/grafana/query must resolve the session id it is handed, not drop it")
          .isEqualTo(session);
      assertThat(grafanaTimestampsOf(response)).containsExactly(1_000L, 2_000L);
    } finally {
      rollback(rootAuth(), session);
    }
  }

  @Test
  void thePromQLInstantQueryRouteRunsInsideTheSessionItIsHanded() throws Exception {
    assertReadEchoesSession("/ts/" + getDatabaseName() + "/prom/api/v1/query?query=" + TYPE + "&time=2",
        "GET /ts/{db}/prom/api/v1/query");
  }

  @Test
  void thePromQLRangeQueryRouteRunsInsideTheSessionItIsHanded() throws Exception {
    assertReadEchoesSession(
        "/ts/" + getDatabaseName() + "/prom/api/v1/query_range?query=" + TYPE + "&start=0&end=10&step=1s",
        "GET /ts/{db}/prom/api/v1/query_range");
  }

  @Test
  void thePromQLLabelsRouteRunsInsideTheSessionItIsHanded() throws Exception {
    final HttpResponse<String> response =
        assertReadEchoesSession("/ts/" + getDatabaseName() + "/prom/api/v1/labels", "GET /ts/{db}/prom/api/v1/labels");
    assertThat(promDataArrayOf(response)).contains("location");
  }

  @Test
  void thePromQLLabelValuesRouteRunsInsideTheSessionItIsHanded() throws Exception {
    final HttpResponse<String> response = assertReadEchoesSession(
        "/ts/" + getDatabaseName() + "/prom/api/v1/label/location/values", "GET /ts/{db}/prom/api/v1/label/{name}/values");
    assertThat(promDataArrayOf(response)).contains("us-east");
  }

  @Test
  void thePromQLSeriesRouteRunsInsideTheSessionItIsHanded() throws Exception {
    assertReadEchoesSession(
        "/ts/" + getDatabaseName() + "/prom/api/v1/series?match[]=" + URLEncoder.encode(TYPE, StandardCharsets.UTF_8),
        "GET /ts/{db}/prom/api/v1/series");
  }

  @Test
  void thePrometheusReadRouteRunsInsideTheSessionItIsHanded() throws Exception {
    seed();
    final String session = beginSession(rootAuth());
    try {
      final HttpResponse<byte[]> response = promRead(rootAuth(), session, TYPE);

      assertThat(response.statusCode()).isEqualTo(200);
      assertThat(sessionIdOf(response))
          .as("#7681: POST /ts/{db}/prom/read must resolve the session id it is handed, not drop it")
          .isEqualTo(session);
      assertThat(decodeReadResponse(response).getResults()).isNotEmpty();
    } finally {
      rollback(rootAuth(), session);
    }
  }

  @Test
  void thePrometheusWriteRouteRunsInsideTheSessionItIsHanded() throws Exception {
    seed();
    final String session = beginSession(rootAuth());
    try {
      final HttpResponse<String> response = promWrite(rootAuth(), session, PROM_TYPE, "server1", 3_000L, 30.0);

      assertThat(response.statusCode()).as("remote write failed: %s", response.body()).isEqualTo(204);
      assertThat(sessionIdOf(response))
          .as("#7681: POST /ts/{db}/prom/write must resolve the session id it is handed, not drop it")
          .isEqualTo(session);

      // Stated as narrowly as it is true: the sample is visible, and NOT because the read joined anything.
      // TimeSeriesShard.appendSamples committed it in its own nested transaction before this call returned
      // (issue #7410), so it is already global. Asserted so a future change to that mechanism is noticed here.
      assertThat(timestampsOf(tsQuery(rootAuth(), null, PROM_TYPE, 0, 10_000))).containsExactly(3_000L);
    } finally {
      rollback(rootAuth(), session);
    }

    // And the same, stated after the session it was written in has been rolled back, because that is the
    // question a reader of this change asks next: a remote write inside a session AUTO-CREATES a type, and the
    // handler no longer opens a transaction of its own to commit it in (see the ownTransaction gate in
    // PostPrometheusWriteHandler). Neither the type nor the sample is undone: schema creation is not part of
    // the caller's transaction, and TimeSeriesShard.appendSamples committed the sample in its own nested one
    // (issue #7410). Asserted rather than assumed, so a future change that makes either of them transactional
    // is noticed here rather than by whoever finds the type gone.
    assertThat(existsType(PROM_TYPE))
        .as("the type the remote write auto-created inside the session survives that session's rollback")
        .isTrue();
    assertThat(timestampsOf(tsQuery(rootAuth(), null, PROM_TYPE, 0, 10_000)))
        .as("and so does the sample")
        .containsExactly(3_000L);
  }

  // ─────────────────────────────────────────────────────────────────────────────
  //  A session id this server cannot resolve: refused on the write, degraded on the nine reads
  // ─────────────────────────────────────────────────────────────────────────────

  /**
   * The defect with teeth on this prefix. A remote write naming a transaction that no longer exists - reaped by
   * the idle sweep, already committed, or never opened - used to be ingested anyway and answered 204, so the
   * client was told its samples were inside a transaction it could still roll back when they were already
   * durable and global. {@code PostPrometheusWriteHandler.rejectsUnresolvableSession()} is what refuses it now,
   * and the second assertion is what stops a 404 raised for some unrelated reason from passing this test:
   * nothing must have been ingested, which for an auto-created metric means the type must not exist at all.
   */
  @Test
  void aPrometheusWriteNamingAnUnknownSessionIsRefusedAndWritesNothing() throws Exception {
    seed();

    final HttpResponse<String> refused = promWrite(rootAuth(), UNKNOWN_SESSION, PROM_TYPE, "server1", 9_000L, 99.0);

    assertThat(refused.statusCode())
        .as("#7681: an unresolvable session on a remote WRITE must be refused, not silently run outside it")
        .isEqualTo(404);
    assertThat(refused.body()).contains(UNKNOWN_SESSION);
    assertThat(existsType(PROM_TYPE))
        .as("the refusal must happen before the append and before the auto-create, or the 404 is a lie about "
            + "what the server did")
        .isFalse();
  }

  /**
   * The boundary the refusal above must not cross, and the reason it is a deliberate split rather than an
   * oversight: on the nine READ routes an unresolvable id degrades to a session-less read, exactly as
   * {@code GET /query} and the two {@code /api/v1/ts} reads do, because a read has no pending work for the
   * caller to be wrong about. Pinned so a later change that tightens the reads has to argue with this test
   * rather than silently break every client that retries a read after its session expired.
   */
  @Test
  void theNineReadRoutesNamingAnUnknownSessionStillAnswer() throws Exception {
    seed();

    for (final String path : readRoutes()) {
      final HttpResponse<String> response = get(path, rootAuth(), UNKNOWN_SESSION);
      assertThat(response.statusCode())
          .as("#7681: %s overrides requiresTransaction() to false, so a stale id degrades: %s", path, response.body())
          .isEqualTo(200);
      assertThat(sessionIdOf(response)).as("%s: nothing was resolved, so there is no id to echo", path).isNull();
    }

    final HttpResponse<byte[]> read = promRead(rootAuth(), UNKNOWN_SESSION, TYPE);
    assertThat(read.statusCode()).as("#7681: prom/read is a read too, so a stale id degrades").isEqualTo(200);
    assertThat(sessionIdOf(read)).isNull();
  }

  /**
   * A session id that resolves for its owner but not for this caller. {@code HttpSessionManager.getSessionById}
   * answers null to any principal that does not own the session, so this reaches the same branch as an expired
   * id - and a write must be refused there too, rather than performed outside a transaction whose id the caller
   * was not entitled to name in the first place.
   * <p>
   * The last assertion is the control that stops this passing for the wrong reason: the same user, the same
   * body, no session header, is accepted. Without it a 404 caused by the user simply lacking access to the
   * database would read as a pass.
   */
  @Test
  void aPrometheusWriteNamingAnotherPrincipalsSessionIsRefused() throws Exception {
    seed();
    createOtherUser();

    final String rootSession = beginSession(rootAuth());
    try {
      final HttpResponse<String> refused =
          promWrite(basicAuth(OTHER_USER, OTHER_PWD), rootSession, PROM_TYPE, "server1", 9_000L, 99.0);

      assertThat(refused.statusCode())
          .as("#7681: a session belonging to another principal resolves to nothing, and a write is refused")
          .isEqualTo(404);
      assertThat(existsType(PROM_TYPE)).isFalse();
    } finally {
      rollback(rootAuth(), rootSession);
    }

    final HttpResponse<String> accepted =
        promWrite(basicAuth(OTHER_USER, OTHER_PWD), null, PROM_TYPE, "server1", 3_000L, 30.0);
    assertThat(accepted.statusCode())
        .as("control: the same caller and body without a session header is accepted, so the 404 above was about "
            + "the session and not about authorization")
        .isEqualTo(204);
  }

  // ─────────────────────────────────────────────────────────────────────────────
  //  The session survives the reads taken inside it
  // ─────────────────────────────────────────────────────────────────────────────

  /**
   * {@code DatabaseAbstractHandler}'s teardown for a session request DETACHES the thread context
   * ({@code removeContext}) rather than rolling the transaction back, which is what lets a session outlive the
   * requests made in it. Driving the Grafana and PromQL reads through that teardown for the first time is
   * exactly the kind of change that could have got it wrong, so the session's own uncommitted row is asserted
   * to still be there afterwards - and to still be rollback-able.
   */
  @Test
  void theReadsLeaveTheSessionsOwnUncommittedWorkAlone() throws Exception {
    seed();
    final String session = beginSession(rootAuth());
    try {
      command(rootAuth(), session, "INSERT INTO " + DOC_TYPE + " SET name = 'witness'");
      assertThat(countDocuments(rootAuth(), session)).isEqualTo(1L);

      for (final String path : readRoutes())
        assertThat(get(path, rootAuth(), session).statusCode()).as("%s inside the session", path).isEqualTo(200);
      assertThat(grafanaQuery(rootAuth(), session).statusCode()).isEqualTo(200);
      assertThat(promRead(rootAuth(), session, TYPE).statusCode()).isEqualTo(200);

      assertThat(countDocuments(rootAuth(), session))
          .as("#7681: the ten routes must detach from the session, never commit or roll it back")
          .isEqualTo(1L);
    } finally {
      rollback(rootAuth(), session);
    }

    assertThat(countDocuments(rootAuth(), null))
        .as("the rollback still took the witness, so the transaction was real throughout")
        .isZero();
  }

  // ─────────────────────────────────────────────────────────────────────────────
  //  One request's body must not be another request's body (issue #7683, in the binary base class)
  // ─────────────────────────────────────────────────────────────────────────────

  /**
   * The behavioural half of the fix below. Six threads release together on a barrier, each posting one sample
   * at a timestamp only it uses, and every timestamp must come back exactly once with none that nobody sent.
   * <p>
   * <b>This test does not prove the old code was broken.</b> It was run against the instance field and passed:
   * with no session header and no {@code X-Request-Id}, {@code parseRequestPayload} and {@code execute} are
   * close enough together on the worker thread that six writers did not interleave between them in this
   * harness. It is kept as what it honestly is - a check that concurrent remote writes are each ingested once
   * now - and {@link #theBinaryBaseHandlerKeepsNoRequestStateInAnInstanceField()} is the assertion that
   * actually fails against the field. The type is created by a first, serial write so the concurrent ones
   * cannot race on auto-create, which is a different question from the one this test asks.
   */
  @Test
  void concurrentRemoteWritesDoNotIngestEachOthersBodies() throws Exception {
    seed();
    assertThat(promWrite(rootAuth(), null, RACE_TYPE, "server0", 100_000L, 0.5).statusCode()).isEqualTo(204);

    final int writers = 6;
    final CyclicBarrier releaseTogether = new CyclicBarrier(writers);
    final ExecutorService pool = Executors.newFixedThreadPool(writers);
    try {
      final List<Callable<Integer>> posts = new ArrayList<>(writers);
      for (int w = 0; w < writers; w++) {
        final long timestamp = 100_001L + w;
        posts.add(() -> {
          releaseTogether.await(30, TimeUnit.SECONDS);
          return promWrite(rootAuth(), null, RACE_TYPE, "server0", timestamp, timestamp - 100_000L).statusCode();
        });
      }

      for (final Future<Integer> answered : pool.invokeAll(posts, 60, TimeUnit.SECONDS))
        assertThat(answered.get()).as("every concurrent remote write must be accepted").isEqualTo(204);
    } finally {
      pool.shutdownNow();
      assertThat(pool.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
    }

    final List<Long> expected = new ArrayList<>();
    for (int w = 0; w <= writers; w++)
      expected.add(100_000L + w);

    assertThat(timestampsOf(tsQuery(rootAuth(), null, RACE_TYPE, 0, 200_000)))
        .as("each concurrent body must be ingested once and only once")
        .containsExactlyInAnyOrderElementsOf(expected);
  }

  /**
   * The deterministic half, and the one that fails against the code this branch replaces.
   * {@code AbstractBinaryHttpHandler} declared {@code protected byte[] rawBytes}, and a handler is a SINGLETON:
   * one instance is registered on the route and serves every request.
   * <pre>
   * $ grep -n "new PostPrometheusWriteHandler\|new PostPrometheusReadHandler" \
   *       server/src/main/java/com/arcadedb/server/http/HttpServer.java
   * 285:        .post("/ts/{database}/prom/write", new PostPrometheusWriteHandler(this))
   * 286:        .post("/ts/{database}/prom/read", new PostPrometheusReadHandler(this))
   * </pre>
   * {@code parseRequestPayload} and {@code execute} are two separate calls from {@code handleRequest}, with
   * authentication, the idempotency reservation and - since this change - the session resolution in between,
   * so two concurrent requests could interleave as: T1 parses body1, T2 overwrites the field with body2, T1
   * executes against T2's bytes, T2 executes against them again. That is the defect issue #7683 reported for
   * {@code PostTimeSeriesWriteHandler}'s string body, in the class that holds the byte one; moving these
   * handlers onto {@code DatabaseAbstractHandler} widens the window rather than narrowing it, so it is fixed
   * in the same change.
   * <p>
   * Asserted as "no per-request state in any instance field" rather than as "no field named rawBytes", because
   * the name is incidental and the property is not: anything a request writes to an instance field of a
   * singleton handler is visible to, and overwritable by, every other request in flight.
   */
  @Test
  void theBinaryBaseHandlerKeepsNoRequestStateInAnInstanceField() {
    for (final Field field : AbstractBinaryHttpHandler.class.getDeclaredFields())
      assertThat(Modifier.isStatic(field.getModifiers()))
          .as("#7683: '%s' is an instance field of a SINGLETON handler, so every concurrent request shares it; "
              + "per-request state belongs on the exchange", field.getName())
          .isTrue();
  }

  // ─────────────────────────────────────────────────────────────────────────────
  //  The structural claim the behaviour rests on
  // ─────────────────────────────────────────────────────────────────────────────

  /**
   * Every behavioural assertion above is a consequence of this one fact. Kept as its own test so a handler
   * added under {@code /api/v1/ts} that repeats the old shape has somewhere to fail, and so that a revert of
   * the reparenting is reported as one clear failure rather than as ten confusing ones.
   */
  @Test
  void allTenGrafanaAndPrometheusRoutesAreOnTheSessionAwareBaseHandler() {
    assertThat(DatabaseAbstractHandler.class).isAssignableFrom(GetGrafanaHealthHandler.class);
    assertThat(DatabaseAbstractHandler.class).isAssignableFrom(GetGrafanaMetadataHandler.class);
    assertThat(DatabaseAbstractHandler.class).isAssignableFrom(PostGrafanaQueryHandler.class);
    assertThat(DatabaseAbstractHandler.class).isAssignableFrom(PostPrometheusWriteHandler.class);
    assertThat(DatabaseAbstractHandler.class).isAssignableFrom(PostPrometheusReadHandler.class);
    assertThat(DatabaseAbstractHandler.class).isAssignableFrom(GetPromQLQueryHandler.class);
    assertThat(DatabaseAbstractHandler.class).isAssignableFrom(GetPromQLQueryRangeHandler.class);
    assertThat(DatabaseAbstractHandler.class).isAssignableFrom(GetPromQLLabelsHandler.class);
    assertThat(DatabaseAbstractHandler.class).isAssignableFrom(GetPromQLLabelValuesHandler.class);
    assertThat(DatabaseAbstractHandler.class).isAssignableFrom(GetPromQLSeriesHandler.class);
  }

  // ─────────────────────────────────────────────────────────────────────────────
  //  Plumbing
  // ─────────────────────────────────────────────────────────────────────────────

  /** The seven GET reads under this prefix, each already answering 200 against the seeded data. */
  private List<String> readRoutes() {
    final String db = getDatabaseName();
    return List.of(
        "/ts/" + db + "/grafana/health",
        "/ts/" + db + "/grafana/metadata",
        "/ts/" + db + "/prom/api/v1/query?query=" + TYPE + "&time=2",
        "/ts/" + db + "/prom/api/v1/query_range?query=" + TYPE + "&start=0&end=10&step=1s",
        "/ts/" + db + "/prom/api/v1/labels",
        "/ts/" + db + "/prom/api/v1/label/location/values",
        "/ts/" + db + "/prom/api/v1/series?match[]=" + URLEncoder.encode(TYPE, StandardCharsets.UTF_8));
  }

  private HttpResponse<String> assertReadEchoesSession(final String path, final String route) throws Exception {
    seed();
    final String session = beginSession(rootAuth());
    try {
      final HttpResponse<String> response = get(path, rootAuth(), session);

      assertThat(response.statusCode()).as("%s failed: %s", route, response.body()).isEqualTo(200);
      assertThat(sessionIdOf(response))
          .as("#7681: %s must resolve the session id it is handed, not drop it", route)
          .isEqualTo(session);
      return response;
    } finally {
      rollback(rootAuth(), session);
    }
  }

  /** The port the server actually bound: a stranger already on 2480 pushes this one up the configured range. */
  private String base() {
    return "http://127.0.0.1:" + getServer(0).getHttpServer().getPort() + "/api/v1";
  }

  private static String basicAuth(final String user, final String password) {
    return "Basic " + Base64.getEncoder().encodeToString((user + ":" + password).getBytes(StandardCharsets.UTF_8));
  }

  private String rootAuth() {
    return basicAuth("root", DEFAULT_PASSWORD_FOR_TESTS);
  }

  private HttpRequest.Builder request(final String url, final String auth, final String sessionId) {
    final HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(url))
        .timeout(Duration.ofSeconds(30))
        .header("Authorization", auth);
    if (sessionId != null)
      builder.header(HttpSessionManager.ARCADEDB_SESSION_ID, sessionId);
    return builder;
  }

  private HttpResponse<String> get(final String path, final String auth, final String sessionId)
      throws IOException, InterruptedException {
    return http.send(request(base() + path, auth, sessionId).GET().build(), HttpResponse.BodyHandlers.ofString());
  }

  private HttpResponse<String> post(final String path, final String auth, final String sessionId, final String body,
      final String contentType) throws IOException, InterruptedException {
    return http.send(request(base() + path, auth, sessionId)
        .header("Content-Type", contentType)
        .POST(HttpRequest.BodyPublishers.ofString(body))
        .build(), HttpResponse.BodyHandlers.ofString());
  }

  private static String sessionIdOf(final HttpResponse<?> response) {
    return response.headers().firstValue(HttpSessionManager.ARCADEDB_SESSION_ID).orElse(null);
  }

  private String beginSession(final String auth) throws Exception {
    final HttpResponse<String> begun = post("/begin/" + getDatabaseName(), auth, null, "", "application/json");
    assertThat(begun.statusCode()).isEqualTo(204);
    final String sessionId = sessionIdOf(begun);
    assertThat(sessionId).as("POST /begin must hand back a session id for this test to have anything to present")
        .isNotBlank();
    return sessionId;
  }

  private void rollback(final String auth, final String sessionId) throws Exception {
    post("/rollback/" + getDatabaseName(), auth, sessionId, "", "application/json");
  }

  private void command(final String auth, final String sessionId, final String sql) throws Exception {
    final HttpResponse<String> response = post("/command/" + getDatabaseName(), auth, sessionId,
        new JSONObject().put("language", "sql").put("command", sql).toString(), "application/json");
    assertThat(response.statusCode()).as("command failed: %s", response.body()).isEqualTo(200);
  }

  private long countDocuments(final String auth, final String sessionId) throws Exception {
    final HttpResponse<String> response = post("/command/" + getDatabaseName(), auth, sessionId,
        new JSONObject().put("language", "sql").put("command", "SELECT count(*) AS cnt FROM " + DOC_TYPE).toString(),
        "application/json");
    assertThat(response.statusCode()).isEqualTo(200);
    return new JSONObject(response.body()).getJSONArray("result").getJSONObject(0).getLong("cnt");
  }

  private boolean existsType(final String typeName) {
    return getServer(0).getDatabase(getDatabaseName()).getSchema().existsType(typeName);
  }

  /** Two committed samples at t=1000 and t=2000, plus a DOCUMENT type used as a transaction witness. */
  private void seed() throws Exception {
    command(rootAuth(), null, "CREATE TIMESERIES TYPE " + TYPE
        + " TIMESTAMP ts TAGS (location STRING) FIELDS (temperature DOUBLE)");
    command(rootAuth(), null, "CREATE DOCUMENT TYPE " + DOC_TYPE);
    final HttpResponse<String> written = post("/ts/" + getDatabaseName() + "/write?precision=ms", rootAuth(), null,
        TYPE + ",location=us-east temperature=10.0 1000\n" + TYPE + ",location=us-east temperature=20.0 2000",
        "text/plain");
    assertThat(written.statusCode()).as("seed ingest failed: %s", written.body()).isEqualTo(204);
  }

  private HttpResponse<String> grafanaQuery(final String auth, final String sessionId) throws Exception {
    final JSONObject body = new JSONObject()
        .put("targets", new JSONArray().put(new JSONObject().put("refId", "A").put("type", TYPE)))
        .put("from", 0)
        .put("to", 10_000);
    return post("/ts/" + getDatabaseName() + "/grafana/query", auth, sessionId, body.toString(), "application/json");
  }

  private HttpResponse<String> tsQuery(final String auth, final String sessionId, final String type, final long from,
      final long to) throws Exception {
    final HttpResponse<String> response = post("/ts/" + getDatabaseName() + "/query", auth, sessionId,
        new JSONObject().put("type", type).put("from", from).put("to", to).toString(), "application/json");
    assertThat(response.statusCode()).as("ts query failed: %s", response.body()).isEqualTo(200);
    return response;
  }

  private static List<Long> timestampsOf(final HttpResponse<String> response) {
    final JSONArray rows = new JSONObject(response.body()).getJSONArray("rows");
    final List<Long> timestamps = new ArrayList<>(rows.length());
    for (int i = 0; i < rows.length(); i++)
      timestamps.add(rows.getJSONArray(i).getLong(0));
    return timestamps;
  }

  /** The time column of the first frame of target A, which the Grafana envelope carries column-major. */
  private static List<Long> grafanaTimestampsOf(final HttpResponse<String> response) {
    final JSONArray values = new JSONObject(response.body())
        .getJSONObject("results").getJSONObject("A")
        .getJSONArray("frames").getJSONObject(0)
        .getJSONObject("data").getJSONArray("values").getJSONArray(0);
    final List<Long> timestamps = new ArrayList<>(values.length());
    for (int i = 0; i < values.length(); i++)
      timestamps.add(values.getLong(i));
    return timestamps;
  }

  private static List<String> typeNamesOf(final HttpResponse<String> response) {
    final JSONArray types = new JSONObject(response.body()).getJSONArray("types");
    final List<String> names = new ArrayList<>(types.length());
    for (int i = 0; i < types.length(); i++)
      names.add(types.getJSONObject(i).getString("name"));
    return names;
  }

  /** The 'data' array of the Prometheus envelope, which both discovery endpoints answer with. */
  private static List<String> promDataArrayOf(final HttpResponse<String> response) {
    final JSONArray data = new JSONObject(response.body()).getJSONArray("data");
    final List<String> values = new ArrayList<>(data.length());
    for (int i = 0; i < data.length(); i++)
      values.add(data.getString(i));
    return values;
  }

  private HttpResponse<String> promWrite(final String auth, final String sessionId, final String metric,
      final String host, final long timestampMs, final double value) throws Exception {
    final byte[] body = Snappy.compress(new WriteRequest(List.of(
        new TimeSeries(List.of(new Label("__name__", metric), new Label("host", host)),
            List.of(new Sample(value, timestampMs))))).encode());

    return http.send(request(base() + "/ts/" + getDatabaseName() + "/prom/write", auth, sessionId)
        .header("Content-Type", "application/x-protobuf")
        .header("Content-Encoding", "snappy")
        .POST(HttpRequest.BodyPublishers.ofByteArray(body))
        .build(), HttpResponse.BodyHandlers.ofString());
  }

  private HttpResponse<byte[]> promRead(final String auth, final String sessionId, final String metric)
      throws Exception {
    final byte[] body = Snappy.compress(new ReadRequest(List.of(
        new Query(Long.MIN_VALUE, Long.MAX_VALUE,
            List.of(new LabelMatcher(MatchType.EQ, "__name__", metric))))).encode());

    return http.send(request(base() + "/ts/" + getDatabaseName() + "/prom/read", auth, sessionId)
        .header("Content-Type", "application/x-protobuf")
        .header("Content-Encoding", "snappy")
        .POST(HttpRequest.BodyPublishers.ofByteArray(body))
        .build(), HttpResponse.BodyHandlers.ofByteArray());
  }

  private static ReadResponse decodeReadResponse(final HttpResponse<byte[]> response) throws Exception {
    return ReadResponse.decode(Snappy.uncompress(response.body()));
  }

  /** A second principal with full access to the test database, so only session OWNERSHIP can refuse it. */
  private void createOtherUser() throws Exception {
    final var security = getServer(0).getSecurity();
    if (security.existsUser(OTHER_USER))
      security.dropUser(OTHER_USER);

    final JSONObject payload = new JSONObject()
        .put("name", OTHER_USER)
        .put("password", OTHER_PWD)
        .put("databases", new JSONObject().put(getDatabaseName(), new JSONArray().put("admin")));

    final HttpResponse<String> created = post("/server/users", rootAuth(), null, payload.toString(),
        "application/json");
    assertThat(created.statusCode()).as("could not create the second principal: %s", created.body()).isEqualTo(201);
  }
}
