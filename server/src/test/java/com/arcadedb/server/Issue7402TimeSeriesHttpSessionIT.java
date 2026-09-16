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
import com.arcadedb.server.http.handler.DatabaseAbstractHandler;
import com.arcadedb.server.http.handler.GetTimeSeriesLatestHandler;
import com.arcadedb.server.http.handler.PostTimeSeriesQueryHandler;
import com.arcadedb.server.http.handler.PostTimeSeriesWriteHandler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7402: the three {@code /api/v1/ts} routes extended {@code AbstractServerHttpHandler}, which never reads
 * {@code arcadedb-session-id} - only {@code DatabaseAbstractHandler.setTransactionInThreadLocal} does. A client
 * that had opened an HTTP session and presented its id on {@code POST /ts/{db}/query}, {@code GET /ts/{db}/latest}
 * or {@code POST /ts/{db}/write} was answered from outside that session: on a database handle bound to no
 * transaction, without the session's lock, without its principal, and without refreshing the idle timer that
 * decides when its transaction is rolled back underneath it. The header was accepted by the transport and
 * dropped by the handler, which is the failure mode that cannot be noticed from the answer.
 * <p>
 * <b>What each test can and cannot prove.</b> A time-series append is not part of the enclosing transaction
 * either way - {@code TimeSeriesShard.appendSamples} wraps each shard write in its own {@code begin}/{@code commit}
 * and an ArcadeDB nested transaction is an independent transaction rather than a savepoint, so a sample is
 * durable and global before the caller commits anything (issue #7410, pinned by
 * {@code Issue7370GrpcTimeSeriesInTransactionIT.timeSeriesAppendsAreNotPartOfTheEnclosingTransaction}). So none
 * of these tests can assert "the samples appeared/disappeared with the transaction": that would pass before and
 * after. They assert the things that DO change, each of which is false against the unfixed server:
 * <ul>
 * <li>the response echoes the session id, which only the session-binding branch emits;</li>
 * <li>a write naming a session this server cannot resolve is refused instead of silently committed outside it;</li>
 * <li>a write naming another principal's session is refused for the same reason, where before it was performed;</li>
 * <li>a read naming an unresolvable session still answers - the deliberate asymmetry with the write, and the same
 * split {@code GET /query} already has - so the refusal above cannot be mistaken for a blanket one;</li>
 * <li>a read taken inside a session leaves that session's own uncommitted work intact, which the base class's
 * detach-don't-roll-back teardown is what guarantees.</li>
 * </ul>
 */
class Issue7402TimeSeriesHttpSessionIT extends BaseGraphServerTest {

  private static final String TYPE        = "Ts7402";
  private static final String DOC_TYPE    = "Ts7402Doc";
  private static final String OTHER_USER  = "ts7402-other";
  private static final String OTHER_PWD   = "ts7402pwd1";
  /** Shaped like a real id ("AS-" + UUID) so the refusal cannot be attributed to a malformed value. */
  private static final String UNKNOWN_SESSION = "AS-00000000-0000-0000-0000-000000007402";

  private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();

  /**
   * The server's user registry outlives a single test class - one JVM runs the whole IT phase with
   * {@code reuseForks} - so a principal left behind here would be inherited by every class that runs after this
   * one. Dropped before {@code super.endTest()} stops the server that owns the registry.
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

  /** The port the server actually bound: a stranger already on 2480 pushes this one up the configured range. */
  private String base() {
    return "http://127.0.0.1:" + getServer(0).getHttpServer().getPort() + "/api/v1";
  }

  private static String basicAuth(final String user, final String password) {
    return "Basic " + Base64.getEncoder().encodeToString((user + ":" + password).getBytes(StandardCharsets.UTF_8));
  }

  private HttpRequest.Builder request(final String url, final String auth, final String sessionId) {
    final HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(url))
        .timeout(Duration.ofSeconds(30))
        .header("Authorization", auth);
    if (sessionId != null)
      builder.header(HttpSessionManager.ARCADEDB_SESSION_ID, sessionId);
    return builder;
  }

  private HttpResponse<String> post(final String path, final String auth, final String sessionId, final String body,
      final String contentType) throws IOException, InterruptedException {
    return http.send(request(base() + path, auth, sessionId)
        .header("Content-Type", contentType)
        .POST(HttpRequest.BodyPublishers.ofString(body))
        .build(), HttpResponse.BodyHandlers.ofString());
  }

  private HttpResponse<String> get(final String path, final String auth, final String sessionId)
      throws IOException, InterruptedException {
    return http.send(request(base() + path, auth, sessionId).GET().build(), HttpResponse.BodyHandlers.ofString());
  }

  private String rootAuth() {
    return basicAuth("root", DEFAULT_PASSWORD_FOR_TESTS);
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

  /** Two committed samples at t=1000 and t=2000, plus a DOCUMENT type used as a transaction witness. */
  private void seed() throws Exception {
    command(rootAuth(), null, "CREATE TIMESERIES TYPE " + TYPE
        + " TIMESTAMP ts TAGS (location STRING) FIELDS (temperature DOUBLE)");
    command(rootAuth(), null, "CREATE DOCUMENT TYPE " + DOC_TYPE);
    final HttpResponse<String> written = writeSamples(rootAuth(), null,
        TYPE + ",location=us-east temperature=10.0 1000\n" + TYPE + ",location=us-east temperature=20.0 2000");
    assertThat(written.statusCode()).as("seed ingest failed: %s", written.body()).isEqualTo(204);
  }

  private HttpResponse<String> writeSamples(final String auth, final String sessionId, final String lineProtocol)
      throws Exception {
    return post("/ts/" + getDatabaseName() + "/write?precision=ms", auth, sessionId, lineProtocol, "text/plain");
  }

  private HttpResponse<String> queryWholeRange(final String auth, final String sessionId) throws Exception {
    return post("/ts/" + getDatabaseName() + "/query", auth, sessionId,
        new JSONObject().put("type", TYPE).put("from", 0).put("to", 10_000).toString(), "application/json");
  }

  private HttpResponse<String> latest(final String auth, final String sessionId) throws Exception {
    return get("/ts/" + getDatabaseName() + "/latest?type=" + URLEncoder.encode(TYPE, StandardCharsets.UTF_8),
        auth, sessionId);
  }

  private static JSONArray timestampsOf(final HttpResponse<String> response) {
    final JSONArray rows = new JSONObject(response.body()).getJSONArray("rows");
    final JSONArray timestamps = new JSONArray();
    for (int i = 0; i < rows.length(); i++)
      timestamps.put(rows.getJSONArray(i).getLong(0));
    return timestamps;
  }

  // ─────────────────────────────────────────────────────────────────────────────
  //  Each of the three routes now resolves the session it is handed
  // ─────────────────────────────────────────────────────────────────────────────

  /**
   * The echo of {@code arcadedb-session-id} is emitted in exactly one place -
   * {@code DatabaseAbstractHandler.setTransactionInThreadLocal}, on the branch that binds the session's
   * transaction onto this thread - so its presence is the observable proof the route resolved the session
   * rather than ignoring the header. The rows are asserted alongside it because a route that binds the session
   * and then answers the wrong thing would be a worse regression than the one being fixed.
   */
  @Test
  void theQueryRouteRunsInsideTheSessionItIsHanded() throws Exception {
    seed();
    final String session = beginSession(rootAuth());
    try {
      final HttpResponse<String> response = queryWholeRange(rootAuth(), session);

      assertThat(response.statusCode()).isEqualTo(200);
      assertThat(sessionIdOf(response))
          .as("#7402: POST /ts/{db}/query must resolve the session id it is handed, not drop it")
          .isEqualTo(session);
      assertThat(timestampsOf(response).toList()).containsExactly(1_000L, 2_000L);
    } finally {
      rollback(rootAuth(), session);
    }
  }

  @Test
  void theLatestRouteRunsInsideTheSessionItIsHanded() throws Exception {
    seed();
    final String session = beginSession(rootAuth());
    try {
      final HttpResponse<String> response = latest(rootAuth(), session);

      assertThat(response.statusCode()).isEqualTo(200);
      assertThat(sessionIdOf(response))
          .as("#7402: GET /ts/{db}/latest must resolve the session id it is handed, not drop it")
          .isEqualTo(session);
      assertThat(new JSONObject(response.body()).getJSONArray("latest").getLong(0)).isEqualTo(2_000L);
    } finally {
      rollback(rootAuth(), session);
    }
  }

  @Test
  void theWriteRouteRunsInsideTheSessionItIsHanded() throws Exception {
    seed();
    final String session = beginSession(rootAuth());
    try {
      final HttpResponse<String> response = writeSamples(rootAuth(), session,
          TYPE + ",location=us-east temperature=30.0 3000");

      assertThat(response.statusCode()).as("write failed: %s", response.body()).isEqualTo(204);
      assertThat(sessionIdOf(response))
          .as("#7402: POST /ts/{db}/write must resolve the session id it is handed, not drop it")
          .isEqualTo(session);

      // Stated as narrowly as it is true: the sample is visible, and NOT because the read joined anything.
      // TimeSeriesShard.appendSamples committed it in its own nested transaction before this call returned
      // (issue #7410), so it is already global. Asserted so a future change to that mechanism is noticed here.
      assertThat(timestampsOf(queryWholeRange(rootAuth(), session)).toList())
          .containsExactly(1_000L, 2_000L, 3_000L);
    } finally {
      rollback(rootAuth(), session);
    }
  }

  // ─────────────────────────────────────────────────────────────────────────────
  //  A session id this server cannot resolve: refused on the write, degraded on the reads
  // ─────────────────────────────────────────────────────────────────────────────

  /**
   * The defect with teeth. A write naming a transaction that no longer exists - reaped by the idle sweep,
   * already committed, or never opened - used to be performed anyway and answered 204, so the client was told
   * its samples were inside a transaction it could still roll back when they were already durable and global.
   * {@code PostTimeSeriesWriteHandler.rejectsUnresolvableSession()} is what refuses it now, and the second
   * assertion is what stops a 404 raised for some unrelated reason from passing this test: nothing must have
   * been written.
   */
  @Test
  void aTimeSeriesWriteNamingAnUnknownSessionIsRefusedAndWritesNothing() throws Exception {
    seed();

    final HttpResponse<String> refused = writeSamples(rootAuth(), UNKNOWN_SESSION,
        TYPE + ",location=us-east temperature=99.0 9000");

    assertThat(refused.statusCode())
        .as("#7402: an unresolvable session on a WRITE must be refused, not silently run outside it")
        .isEqualTo(404);
    assertThat(refused.body()).contains(UNKNOWN_SESSION);
    assertThat(timestampsOf(queryWholeRange(rootAuth(), null)).toList())
        .as("the refusal must happen before the append, or the 404 is a lie about what the server did")
        .containsExactly(1_000L, 2_000L);
  }

  /**
   * The boundary the refusal above must not cross, and the reason it is a deliberate split rather than an
   * oversight: on the two READ routes an unresolvable id degrades to a session-less read, exactly as
   * {@code GET /query} does, because a read has no pending work for the caller to be wrong about. Pinned so a
   * later change that tightens the reads has to argue with this test rather than silently break every client
   * that retries a read after its session expired.
   */
  @Test
  void aTimeSeriesReadNamingAnUnknownSessionStillAnswers() throws Exception {
    seed();

    final HttpResponse<String> query = queryWholeRange(rootAuth(), UNKNOWN_SESSION);
    assertThat(query.statusCode())
        .as("#7402: the read routes override requiresTransaction() to false, so a stale id degrades")
        .isEqualTo(200);
    assertThat(timestampsOf(query).toList()).containsExactly(1_000L, 2_000L);
    assertThat(sessionIdOf(query)).as("nothing was resolved, so there is no id to echo").isNull();

    final HttpResponse<String> newest = latest(rootAuth(), UNKNOWN_SESSION);
    assertThat(newest.statusCode()).isEqualTo(200);
    assertThat(new JSONObject(newest.body()).getJSONArray("latest").getLong(0)).isEqualTo(2_000L);
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
  void aTimeSeriesWriteNamingAnotherPrincipalsSessionIsRefused() throws Exception {
    seed();
    createOtherUser();

    final String rootSession = beginSession(rootAuth());
    try {
      final HttpResponse<String> refused = writeSamples(basicAuth(OTHER_USER, OTHER_PWD), rootSession,
          TYPE + ",location=us-east temperature=99.0 9000");

      assertThat(refused.statusCode())
          .as("#7402: a session belonging to another principal resolves to nothing, and a write is refused")
          .isEqualTo(404);
      assertThat(timestampsOf(queryWholeRange(rootAuth(), null)).toList()).containsExactly(1_000L, 2_000L);
    } finally {
      rollback(rootAuth(), rootSession);
    }

    final HttpResponse<String> accepted = writeSamples(basicAuth(OTHER_USER, OTHER_PWD), null,
        TYPE + ",location=us-east temperature=30.0 3000");
    assertThat(accepted.statusCode())
        .as("control: the same caller and body without a session header is accepted, so the 404 above was "
            + "about the session and not about authorization")
        .isEqualTo(204);
  }

  // ─────────────────────────────────────────────────────────────────────────────
  //  The session survives the read taken inside it
  // ─────────────────────────────────────────────────────────────────────────────

  /**
   * {@code DatabaseAbstractHandler}'s teardown for a session request DETACHES the thread context
   * ({@code removeContext}) rather than rolling the transaction back, which is what lets a session outlive the
   * requests made in it. Driving a time-series read through that teardown for the first time is exactly the
   * kind of change that could have got it wrong, so the session's own uncommitted row is asserted to still be
   * there afterwards - and to still be rollback-able.
   */
  @Test
  void aTimeSeriesReadLeavesTheSessionsOwnUncommittedWorkAlone() throws Exception {
    seed();
    final String session = beginSession(rootAuth());
    try {
      command(rootAuth(), session, "INSERT INTO " + DOC_TYPE + " SET name = 'witness'");
      assertThat(countDocuments(rootAuth(), session)).isEqualTo(1L);

      assertThat(queryWholeRange(rootAuth(), session).statusCode()).isEqualTo(200);
      assertThat(latest(rootAuth(), session).statusCode()).isEqualTo(200);

      assertThat(countDocuments(rootAuth(), session))
          .as("#7402: the two time-series reads must detach from the session, never commit or roll it back")
          .isEqualTo(1L);
    } finally {
      rollback(rootAuth(), session);
    }

    assertThat(countDocuments(rootAuth(), null))
        .as("the rollback still took the witness, so the transaction was real throughout")
        .isZero();
  }

  // ─────────────────────────────────────────────────────────────────────────────
  //  The structural claim the behaviour rests on
  // ─────────────────────────────────────────────────────────────────────────────

  /**
   * The inverse of the pin {@code Issue7370GrpcTimeSeriesInTransactionIT} carried while #7402 was open. Kept
   * because every behavioural assertion above is a consequence of this one fact, and because a future handler
   * added under {@code /api/v1/ts} that repeats the old shape has somewhere to fail.
   */
  @Test
  void allThreeTimeSeriesRoutesAreOnTheSessionAwareBaseHandler() {
    assertThat(DatabaseAbstractHandler.class).isAssignableFrom(PostTimeSeriesQueryHandler.class);
    assertThat(DatabaseAbstractHandler.class).isAssignableFrom(GetTimeSeriesLatestHandler.class);
    assertThat(DatabaseAbstractHandler.class).isAssignableFrom(PostTimeSeriesWriteHandler.class);
  }

  // ─────────────────────────────────────────────────────────────────────────────
  //  Plumbing
  // ─────────────────────────────────────────────────────────────────────────────

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
