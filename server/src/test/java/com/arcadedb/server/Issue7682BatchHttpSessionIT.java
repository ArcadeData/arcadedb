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

import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.remote.RemoteGraphBatch;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.http.HttpSessionManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7682: {@code POST /api/v1/batch/{database}} never read {@code arcadedb-session-id}. A client that had
 * opened an HTTP session and presented its id on a bulk load was answered from outside that session - without the
 * session's lock, without its principal, and without refreshing the idle clock that decides when its transaction
 * is rolled back underneath it - and an id this server could not resolve at all was loaded anyway.
 * <p>
 * <b>What these tests can and cannot prove.</b> A bulk load is not part of the caller's transaction either way:
 * GraphBatch commits every {@code commitEvery} records, so the loaded records are durable before the caller
 * commits or rolls back anything, and that is the documented contract the fix keeps. None of these tests can
 * therefore assert "the vertices disappear with a rollback". They assert what DOES change, each of which is false
 * against the unfixed server:
 * <ul>
 * <li>a load naming a session this server cannot resolve is refused (404) and loads nothing, on both encodings;</li>
 * <li>a load naming ANOTHER principal's session is refused the same way instead of performed;</li>
 * <li>a load naming a live session echoes its id, which only the session-binding path emits;</li>
 * <li>and - the half a naive port would break - the load leaves the session's own transaction exactly as it found
 * it: GraphBatch's {@code beginTx()} joins whatever transaction is active on its thread, so binding the session's
 * transaction there would have the first chunk COMMIT the caller's pending work and every chunk after it run
 * outside any transaction the caller owns. A failed load must not roll that transaction back either.</li>
 * </ul>
 */
class Issue7682BatchHttpSessionIT extends BaseGraphServerTest {

  private static final String VERTEX_TYPE     = "B7682";
  private static final String DOC_TYPE        = "D7682";
  private static final String NDJSON          = "application/x-ndjson";
  private static final String OTHER_USER      = "batch7682-other";
  private static final String OTHER_PWD       = "batch7682pwd1";
  /** Shaped like a real id ("AS-" + UUID) so the refusal cannot be attributed to a malformed value. */
  private static final String UNKNOWN_SESSION = "AS-00000000-0000-0000-0000-000000007682";

  private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();

  /** The server's user registry outlives this class, so a principal left behind would leak into the next one. */
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

  @Test
  void aBufferedLoadNamingAnUnresolvableSessionIsRefusedAndLoadsNothing() throws Exception {
    seed();

    final HttpResponse<String> response = batch(rootAuth(), UNKNOWN_SESSION, vertices(1, 3), false);

    assertThat(response.statusCode())
        .as("a load naming a session the server cannot resolve must be refused, not run outside it: %s", response.body())
        .isEqualTo(404);
    assertThat(countVertices(rootAuth(), null)).as("a refused load must not have loaded anything").isZero();
  }

  @Test
  void aStreamedLoadNamingAnUnresolvableSessionIsRefusedWithItsRealStatus() throws Exception {
    seed();

    final HttpResponse<String> response = batch(rootAuth(), UNKNOWN_SESSION, vertices(1, 3), true);

    assertThat(response.statusCode())
        .as("the refusal happens before any progress line, so the status line can still say it: %s", response.body())
        .isEqualTo(404);
    assertThat(countVertices(rootAuth(), null)).isZero();
  }

  @Test
  void aLoadNamingAnotherPrincipalsSessionIsRefused() throws Exception {
    seed();
    createOtherUser();

    final String rootSession = beginSession(rootAuth());
    try {
      final HttpResponse<String> response = batch(basicAuth(OTHER_USER, OTHER_PWD), rootSession, vertices(1, 3), false);

      assertThat(response.statusCode())
          .as("another principal's session id must not be usable to load data: %s", response.body())
          .isEqualTo(404);
      assertThat(countVertices(rootAuth(), null)).isZero();
    } finally {
      rollback(rootAuth(), rootSession);
    }
  }

  @Test
  void aBufferedLoadInsideASessionEchoesItAndLeavesTheSessionTransactionUntouched() throws Exception {
    loadInsideASessionLeavesItsTransactionUntouched(false);
  }

  @Test
  void aStreamedLoadInsideASessionEchoesItAndLeavesTheSessionTransactionUntouched() throws Exception {
    loadInsideASessionLeavesItsTransactionUntouched(true);
  }

  private void loadInsideASessionLeavesItsTransactionUntouched(final boolean streaming) throws Exception {
    seed();

    final String sessionId = beginSession(rootAuth());
    try {
      // The witness: one document written inside the session and not committed.
      command(rootAuth(), sessionId, "INSERT INTO " + DOC_TYPE + " SET name = 'pending'");
      assertThat(countDocuments(rootAuth(), sessionId)).isEqualTo(1);
      assertThat(countDocuments(rootAuth(), null)).isZero();

      final HttpResponse<String> response = batch(rootAuth(), sessionId, vertices(1, 3), streaming);
      assertThat(response.statusCode()).as("load failed: %s", response.body()).isEqualTo(200);
      assertThat(response.headers().firstValue(HttpSessionManager.ARCADEDB_SESSION_ID).orElse(null))
          .as("the load ran under the session it named, so it says so")
          .isEqualTo(sessionId);

      assertThat(countDocuments(rootAuth(), null))
          .as("the load must not have committed the session's pending work: GraphBatch commits whatever "
              + "transaction is active on its thread")
          .isZero();
      assertThat(countDocuments(rootAuth(), sessionId))
          .as("and must have left the session's transaction open, holding that work")
          .isEqualTo(1);
      assertThat(countVertices(rootAuth(), null))
          .as("the loaded records are durable on their own, as the endpoint documents")
          .isEqualTo(3);

      final HttpResponse<String> committed = post("/commit/" + getDatabaseName(), rootAuth(), sessionId, "",
          "application/json");
      assertThat(committed.statusCode()).as("commit failed: %s", committed.body()).isEqualTo(204);
      assertThat(countDocuments(rootAuth(), null)).isEqualTo(1);
    } finally {
      rollback(rootAuth(), sessionId);
    }
  }

  /**
   * Two failures, because they leave the handler by different doors: a query parameter refused with an exception
   * (which is what reaches {@code HttpSession.execute}'s rollback arm) and a payload refused as a 400 answer the
   * handler builds itself. Neither may take the caller's transaction down with it.
   */
  @Test
  void aFailedLoadInsideASessionDoesNotRollTheSessionTransactionBack() throws Exception {
    seed();

    final String sessionId = beginSession(rootAuth());
    try {
      command(rootAuth(), sessionId, "INSERT INTO " + DOC_TYPE + " SET name = 'pending'");

      final HttpResponse<String> thrown = batch(rootAuth(), sessionId, "?vertexBatchSize=0", vertices(1, 1), false);
      assertThat(thrown.statusCode()).as("the load was expected to be refused: %s", thrown.body()).isEqualTo(400);
      assertThat(countDocuments(rootAuth(), sessionId))
          .as("the load does not participate in the session's transaction, so its failure must not roll it back")
          .isEqualTo(1);

      // An edge naming a temporary id no vertex declared: refused as client input, after the vertex committed.
      final HttpResponse<String> answered = batch(rootAuth(), sessionId, "",
          vertices(1, 1) + "{\"@type\":\"edge\",\"@class\":\"E1\",\"@from\":\"v1\",\"@to\":\"nobody\"}\n", false);
      assertThat(answered.statusCode()).as("the load was expected to fail: %s", answered.body()).isEqualTo(400);
      assertThat(countDocuments(rootAuth(), sessionId)).isEqualTo(1);
    } finally {
      rollback(rootAuth(), sessionId);
    }
  }

  /**
   * The Java driver sends {@code arcadedb-session-id} on {@code POST /batch} whenever a remote transaction is open
   * ({@code RemoteDatabase.createRequestBuilder}), so it is the most common caller that ever reached this path.
   */
  @Test
  void aRemoteDatabaseLoadInsideATransactionLeavesThatTransactionUntouched() throws Exception {
    seed();

    try (final RemoteDatabase remote = new RemoteDatabase("127.0.0.1", getServerHttpPort(0), getDatabaseName(), "root",
        DEFAULT_PASSWORD_FOR_TESTS)) {
      remote.begin();
      remote.command("sql", "INSERT INTO " + DOC_TYPE + " SET name = 'pending'");

      try (final RemoteGraphBatch batch = remote.batch().build()) {
        for (int i = 1; i <= 3; i++)
          batch.createVertex(VERTEX_TYPE, "n", i);
      }

      assertThat(countDocuments(rootAuth(), null)).as("the load must not commit the driver's open transaction").isZero();

      remote.rollback();
      assertThat(countDocuments(rootAuth(), null)).isZero();
      assertThat(countVertices(rootAuth(), null)).isEqualTo(3);
    }
  }

  // ---------------------------------------------------------------------------------------------------------------

  private void seed() throws Exception {
    command(rootAuth(), null, "CREATE VERTEX TYPE " + VERTEX_TYPE + " IF NOT EXISTS");
    command(rootAuth(), null, "CREATE DOCUMENT TYPE " + DOC_TYPE + " IF NOT EXISTS");
  }

  private static String vertices(final int from, final int count) {
    final StringBuilder body = new StringBuilder();
    for (int i = from; i < from + count; i++)
      body.append("{\"@type\":\"vertex\",\"@class\":\"").append(VERTEX_TYPE).append("\",\"@id\":\"v").append(i)
          .append("\",\"n\":").append(i).append("}\n");
    return body.toString();
  }

  private HttpResponse<String> batch(final String auth, final String sessionId, final String body,
      final boolean streaming) throws IOException, InterruptedException {
    return batch(auth, sessionId, "", body, streaming);
  }

  private HttpResponse<String> batch(final String auth, final String sessionId, final String query, final String body,
      final boolean streaming) throws IOException, InterruptedException {
    final HttpRequest.Builder builder = request("/batch/" + getDatabaseName() + query, auth, sessionId)
        .header("Content-Type", NDJSON)
        .POST(HttpRequest.BodyPublishers.ofString(body));
    if (streaming)
      builder.header("Accept", NDJSON);
    return http.send(builder.build(), HttpResponse.BodyHandlers.ofString());
  }

  private HttpRequest.Builder request(final String path, final String auth, final String sessionId) {
    final HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(getServerHttpUrl(0, "/api/v1" + path)))
        .timeout(Duration.ofSeconds(30))
        .header("Authorization", auth);
    if (sessionId != null)
      builder.header(HttpSessionManager.ARCADEDB_SESSION_ID, sessionId);
    return builder;
  }

  private HttpResponse<String> post(final String path, final String auth, final String sessionId, final String body,
      final String contentType) throws IOException, InterruptedException {
    return http.send(request(path, auth, sessionId)
        .header("Content-Type", contentType)
        .POST(HttpRequest.BodyPublishers.ofString(body))
        .build(), HttpResponse.BodyHandlers.ofString());
  }

  private static String basicAuth(final String user, final String password) {
    return "Basic " + Base64.getEncoder().encodeToString((user + ":" + password).getBytes(StandardCharsets.UTF_8));
  }

  private static String rootAuth() {
    return basicAuth("root", DEFAULT_PASSWORD_FOR_TESTS);
  }

  private String beginSession(final String auth) throws Exception {
    final HttpResponse<String> begun = post("/begin/" + getDatabaseName(), auth, null, "", "application/json");
    assertThat(begun.statusCode()).isEqualTo(204);
    final String sessionId = begun.headers().firstValue(HttpSessionManager.ARCADEDB_SESSION_ID).orElse(null);
    assertThat(sessionId).isNotBlank();
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

  private long count(final String auth, final String sessionId, final String type) throws Exception {
    final HttpResponse<String> response = post("/command/" + getDatabaseName(), auth, sessionId,
        new JSONObject().put("language", "sql").put("command", "SELECT count(*) AS cnt FROM " + type).toString(),
        "application/json");
    assertThat(response.statusCode()).as("count failed: %s", response.body()).isEqualTo(200);
    return new JSONObject(response.body()).getJSONArray("result").getJSONObject(0).getLong("cnt");
  }

  private long countDocuments(final String auth, final String sessionId) throws Exception {
    return count(auth, sessionId, DOC_TYPE);
  }

  private long countVertices(final String auth, final String sessionId) throws Exception {
    return count(auth, sessionId, VERTEX_TYPE);
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
