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
package com.arcadedb.server.http.handler;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.security.ServerSecurity;
import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9680: over HTTP a client can list the statements a server is running, find its own by the label it gave it in
 * {@code X-ArcadeDB-Query-Tag}, terminate it, be told the work stopped, and see it gone from the list - with or without
 * a transaction session, as root or as the user who runs it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9680ListTerminateQueriesTest extends BaseGraphServerTest {
  /** About 16 s on one core when left alone, all of it inside one aggregation (the statement of the issue). */
  private static final String LONG_CYPHER =
      "UNWIND range(1, 10000) AS i UNWIND range(1, 10000) AS j RETURN sum(sin(toFloat(i * j))) AS total";

  private static final String TAG_HEADER   = "X-ArcadeDB-Query-Tag";
  private static final String OWNER        = "owner9680";
  private static final String OTHER        = "other9680";
  private static final String PASSWORD     = "pwd9680-secret";
  private static final String ROOT         = "root";

  @AfterEach
  void cleanUp() {
    GlobalConfiguration.QUERY_MAX_CONCURRENT.reset();
    GlobalConfiguration.QUERY_QUEUE_TIMEOUT.reset();
    final ServerSecurity security = getServer(0).getSecurity();
    for (final String user : new String[] { OWNER, OTHER })
      if (security.getUser(user) != null)
        security.dropUser(user);
  }

  @Test
  void aTaggedStatementIsListedTerminatedAndThenGone() throws Exception {
    final CompletableFuture<Response> running = runAsync(ROOT, "/api/v1/command/graph", cypher(LONG_CYPHER), Map.of(TAG_HEADER, "bench-1"));

    final JSONObject entry = awaitSingleEntry(ROOT, "bench-1");
    assertThat(entry.getString("language")).isEqualTo("opencypher");
    assertThat(entry.getString("text")).isEqualTo(LONG_CYPHER);
    assertThat(entry.getString("user")).isEqualTo(ROOT);
    assertThat(entry.getString("database")).isEqualTo("graph");
    assertThat(entry.getString("protocol")).isEqualTo("http");
    assertThat(entry.has("sessionId")).isFalse();

    final StallAwareStopwatch watch = StallAwareStopwatch.start();
    final JSONObject terminated = serverCommand(ROOT, "terminate query " + entry.getString("id")).getJSONObject("result");
    assertThat(terminated.getString("id")).isEqualTo(entry.getString("id"));
    assertThat(terminated.getString("status")).isEqualTo("terminated");

    final Response response = running.get(30, TimeUnit.SECONDS);
    watch.assertGaveUpWithin(10_000, "a terminated statement stopping at its next check, against one that runs for 16 s");
    assertThat(response.status).isEqualTo(409);
    assertThat(response.body).contains("Query terminated").contains("QueryTerminatedException");

    assertThat(listQueries(ROOT, "bench-1")).isEmpty();
    assertThat(serverCommand(ROOT, "terminate query " + entry.getString("id")).getJSONObject("result").getString("status"))
        .isEqualTo("not found");

    final Response fresh = call(ROOT, "POST", "/api/v1/command/graph", cypher("RETURN 42 AS answer"), Map.of());
    assertThat(fresh.status).isEqualTo(200);
    assertThat(fresh.body).contains("42");
  }

  @Test
  void aLongSqlStatementIsTerminatedByItsTag() throws Exception {
    final Database database = getServerDatabase(0, "graph");
    if (!database.getSchema().existsType("Node9680")) {
      database.getSchema().createVertexType("Node9680");
      database.transaction(() -> {
        for (int i = 0; i < 5_000; i++)
          database.newVertex("Node9680").set("v", i).save();
      });
    }
    final String sql = "MATCH {type: Node9680, as: a}, {type: Node9680, as: b, where: (v + $matched.a.v = -1)} RETURN a.v";
    final CompletableFuture<Response> running = runAsync(ROOT, "/api/v1/query/graph",
        new JSONObject().put("language", "sql").put("command", sql), Map.of(TAG_HEADER, "bench-sql"));
    awaitSingleEntry(ROOT, "bench-sql");

    final JSONArray results = serverCommand(ROOT, "terminate queries tag bench-sql").getJSONArray("result");
    assertThat(results.length()).isEqualTo(1);
    assertThat(results.getJSONObject(0).getString("status")).isEqualTo("terminated");
    assertThat(running.get(30, TimeUnit.SECONDS).status).isEqualTo(409);
    assertThat(listQueries(ROOT, "bench-sql")).isEmpty();
  }

  @Test
  void terminatingAStatementInASessionRollsTheSessionBackAndEndsIt() throws Exception {
    final Database database = getServerDatabase(0, "graph");
    if (!database.getSchema().existsType("Tx9680"))
      database.getSchema().createDocumentType("Tx9680");

    // The tag is given to the session, and the statements run in it inherit it
    final Response begin = call(ROOT, "POST", "/api/v1/begin/graph", new JSONObject(), Map.of(TAG_HEADER, "bench-tx"));
    assertThat(begin.status).isEqualTo(204);
    final String session = begin.sessionId;
    assertThat(session).isNotNull();

    final Response write = call(ROOT, "POST", "/api/v1/command/graph",
        new JSONObject().put("language", "sql").put("command", "INSERT INTO Tx9680 SET x = 1"), Map.of("arcadedb-session-id", session));
    assertThat(write.status).isEqualTo(200);

    final JSONArray transactions = serverCommand(ROOT, "list transactions").getJSONArray("result");
    assertThat(findById(transactions, session)).isNotNull();
    assertThat(findById(transactions, session).getString("tag")).isEqualTo("bench-tx");

    final CompletableFuture<Response> running = runAsync(ROOT, "/api/v1/command/graph", cypher(LONG_CYPHER),
        Map.of("arcadedb-session-id", session));
    final JSONObject entry = awaitSingleEntry(ROOT, "bench-tx");
    assertThat(entry.getString("sessionId")).isEqualTo(session);

    assertThat(serverCommand(ROOT, "terminate query " + entry.getString("id")).getJSONObject("result").getString("status"))
        .isEqualTo("terminated");
    final Response response = running.get(30, TimeUnit.SECONDS);
    assertThat(response.status).isEqualTo(409);

    // The session is gone and what it wrote with it. A commit naming it publishes nothing and is answered as for any
    // session that no longer resolves (issue #7714): the idempotent 204, with the header saying the session is gone
    assertThat(findById(serverCommand(ROOT, "list transactions").getJSONArray("result"), session)).isNull();
    assertCommitFindsNoSession(session);
    assertThat(database.countType("Tx9680", false)).isZero();
  }

  @Test
  void anIdleSessionIsTerminatedAndRolledBack() throws Exception {
    final Database database = getServerDatabase(0, "graph");
    if (!database.getSchema().existsType("Idle9680"))
      database.getSchema().createDocumentType("Idle9680");

    final Response begin = call(ROOT, "POST", "/api/v1/begin/graph", new JSONObject(), Map.of());
    final String session = begin.sessionId;
    assertThat(call(ROOT, "POST", "/api/v1/command/graph",
        new JSONObject().put("language", "sql").put("command", "INSERT INTO Idle9680 SET x = 1"),
        Map.of("arcadedb-session-id", session)).status).isEqualTo(200);

    assertThat(serverCommand(ROOT, "terminate transaction " + session).getJSONObject("result").getString("status"))
        .isEqualTo("terminated");
    assertThat(serverCommand(ROOT, "terminate transaction " + session).getJSONObject("result").getString("status"))
        .isEqualTo("not found");
    assertCommitFindsNoSession(session);
    assertThat(database.countType("Idle9680", false)).isZero();
  }

  @Test
  void terminatingABusySessionStopsItsStatementAndEndsIt() throws Exception {
    final Database database = getServerDatabase(0, "graph");
    if (!database.getSchema().existsType("Busy9680"))
      database.getSchema().createDocumentType("Busy9680");

    final String session = call(ROOT, "POST", "/api/v1/begin/graph", new JSONObject(), Map.of(TAG_HEADER, "bench-busy")).sessionId;
    assertThat(call(ROOT, "POST", "/api/v1/command/graph",
        new JSONObject().put("language", "sql").put("command", "INSERT INTO Busy9680 SET x = 1"),
        Map.of("arcadedb-session-id", session)).status).isEqualTo(200);

    final CompletableFuture<Response> running = runAsync(ROOT, "/api/v1/command/graph", cypher(LONG_CYPHER),
        Map.of("arcadedb-session-id", session));
    final JSONObject entry = awaitSingleEntry(ROOT, "bench-busy");
    final JSONObject listed = findById(serverCommand(ROOT, "list transactions").getJSONArray("result"), session);
    assertThat(listed.getBoolean("busy")).isTrue();
    assertThat(listed.getJSONArray("runningQueries").getString(0)).isEqualTo(entry.getString("id"));

    assertThat(serverCommand(ROOT, "terminate transaction " + session).getJSONObject("result").getString("status"))
        .isEqualTo("terminated");
    assertThat(running.get(30, TimeUnit.SECONDS).status).isEqualTo(409);
    assertThat(listQueries(ROOT, "bench-busy")).isEmpty();
    assertThat(findById(serverCommand(ROOT, "list transactions").getJSONArray("result"), session)).isNull();
    assertCommitFindsNoSession(session);
    assertThat(database.countType("Busy9680", false)).isZero();
  }

  @Test
  void aUserSeesAndStopsOnlyTheirOwnStatements() throws Exception {
    createUser(OWNER);
    createUser(OTHER);

    final CompletableFuture<Response> running = runAsync(OWNER, "/api/v1/command/graph", cypher(LONG_CYPHER),
        Map.of(TAG_HEADER, "bench-owner"));
    final JSONObject entry = awaitSingleEntry(ROOT, "bench-owner");
    assertThat(entry.getString("user")).isEqualTo(OWNER);

    // Another user neither sees it nor can stop it, and cannot tell it exists
    assertThat(listQueries(OTHER, "bench-owner")).isEmpty();
    assertThat(serverCommand(OTHER, "terminate query " + entry.getString("id")).getJSONObject("result").getString("status"))
        .isEqualTo("not found");
    assertThat(listQueries(ROOT, "bench-owner")).hasSize(1);

    // Its owner sees it and stops it
    assertThat(listQueries(OWNER, "bench-owner")).hasSize(1);
    assertThat(serverCommand(OWNER, "terminate query " + entry.getString("id")).getJSONObject("result").getString("status"))
        .isEqualTo("terminated");
    assertThat(running.get(30, TimeUnit.SECONDS).status).isEqualTo(409);

    // The other commands stay root-only
    assertThat(call(OWNER, "POST", "/api/v1/server", new JSONObject().put("command", "list backups graph"), Map.of()).status)
        .isEqualTo(403);
  }

  @Test
  void aSaturatedAdmissionGateDoesNotHoldBackTheTermination() throws Exception {
    GlobalConfiguration.QUERY_MAX_CONCURRENT.setValue(1);
    GlobalConfiguration.QUERY_QUEUE_TIMEOUT.setValue(60_000L);

    final CompletableFuture<Response> running = runAsync(ROOT, "/api/v1/command/graph", cypher(LONG_CYPHER),
        Map.of(TAG_HEADER, "bench-gate"));
    final JSONObject entry = awaitSingleEntry(ROOT, "bench-gate");
    // The only slot is taken: a second statement waits for it, and is not listed while it waits
    final CompletableFuture<Response> queued = runAsync(ROOT, "/api/v1/command/graph", cypher("RETURN 1 AS one"),
        Map.of(TAG_HEADER, "bench-queued"));
    Thread.sleep(200);
    assertThat(listQueries(ROOT, "bench-queued")).isEmpty();

    assertThat(serverCommand(ROOT, "terminate query " + entry.getString("id")).getJSONObject("result").getString("status"))
        .isEqualTo("terminated");
    assertThat(running.get(30, TimeUnit.SECONDS).status).isEqualTo(409);
    assertThat(queued.get(30, TimeUnit.SECONDS).status).isEqualTo(200);
  }

  @Test
  void anUnusableTagIsRefused() throws Exception {
    final Response response = call(ROOT, "POST", "/api/v1/command/graph", cypher("RETURN 1 AS one"),
        Map.of(TAG_HEADER, "x".repeat(AbstractServerHttpHandler.MAX_QUERY_TAG_LENGTH + 1)));
    assertThat(response.status).isEqualTo(400);

    assertThat(AbstractServerHttpHandler.validateQueryTag("  bench  ")).isEqualTo("bench");
    assertThat(AbstractServerHttpHandler.validateQueryTag("   ")).isNull();
    assertThatThrownBy(() -> AbstractServerHttpHandler.validateQueryTag("a\nb")).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void malformedCommandsAreRefused() throws Exception {
    for (final String command : new String[] { "terminate query", "terminate queries q1", "list queries foo", "list transactions x",
        "terminate transaction" })
      assertThat(call(ROOT, "POST", "/api/v1/server", new JSONObject().put("command", command), Map.of()).status)
          .as(command).isEqualTo(400);
  }

  // ---------------------------------------------------------------------------------------------

  private record Response(int status, String body, String sessionId, String sessionExpired) {
  }

  private void assertCommitFindsNoSession(final String session) throws Exception {
    final Response commit = call(ROOT, "POST", "/api/v1/commit/graph", new JSONObject(), Map.of("arcadedb-session-id", session));
    assertThat(commit.status).isEqualTo(204);
    assertThat(commit.sessionExpired).isEqualTo(session);
  }

  private static JSONObject cypher(final String text) {
    return new JSONObject().put("language", "opencypher").put("command", text);
  }

  private void createUser(final String name) {
    final ServerSecurity security = getServer(0).getSecurity();
    if (security.getUser(name) == null)
      security.createUser(new JSONObject().put("name", name).put("password", security.encodePassword(PASSWORD))
          .put("databases", new JSONObject().put(getDatabaseName(), new JSONArray(new String[] { "admin" }))));
  }

  private CompletableFuture<Response> runAsync(final String user, final String path, final JSONObject payload,
      final Map<String, String> headers) {
    return CompletableFuture.supplyAsync(() -> {
      try {
        return call(user, "POST", path, payload, headers);
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    });
  }

  /** Waits for the statement with {@code tag} to be listed, and returns its entry: there must be exactly one. */
  private JSONObject awaitSingleEntry(final String user, final String tag) throws Exception {
    final long deadline = System.currentTimeMillis() + 15_000;
    while (System.currentTimeMillis() < deadline) {
      final JSONArray entries = listQueries(user, tag);
      if (!entries.isEmpty()) {
        assertThat(entries.length()).isEqualTo(1);
        assertThat(entries.getJSONObject(0).getString("tag")).isEqualTo(tag);
        return entries.getJSONObject(0);
      }
      Thread.sleep(20);
    }
    throw new AssertionError("The statement tagged '" + tag + "' was never listed");
  }

  private JSONArray listQueries(final String user, final String tag) throws Exception {
    return serverCommand(user, "list queries tag " + tag).getJSONArray("result");
  }

  private JSONObject serverCommand(final String user, final String command) throws Exception {
    final Response response = call(user, "POST", "/api/v1/server", new JSONObject().put("command", command), Map.of());
    assertThat(response.status).as("%s: %s", command, response.body).isEqualTo(200);
    return new JSONObject(response.body);
  }

  private static JSONObject findById(final JSONArray array, final String id) {
    for (int i = 0; i < array.length(); i++)
      if (id.equals(array.getJSONObject(i).getString("id")))
        return array.getJSONObject(i);
    return null;
  }

  private Response call(final String user, final String method, final String path, final JSONObject payload,
      final Map<String, String> headers) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URI(getServerHttpUrl(0, path)).toURL().openConnection();
    try {
      connection.setRequestMethod(method);
      final String password = ROOT.equals(user) ? DEFAULT_PASSWORD_FOR_TESTS : PASSWORD;
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString((user + ":" + password).getBytes(StandardCharsets.UTF_8)));
      connection.setReadTimeout(60_000);
      for (final Map.Entry<String, String> header : headers.entrySet())
        connection.setRequestProperty(header.getKey(), header.getValue());
      if (payload != null)
        formatPayload(connection, payload);
      final int status = connection.getResponseCode();
      return new Response(status, body(connection, status), connection.getHeaderField("arcadedb-session-id"),
          connection.getHeaderField(DatabaseAbstractHandler.SESSION_EXPIRED));
    } finally {
      connection.disconnect();
    }
  }

  private static String body(final HttpURLConnection connection, final int status) throws IOException {
    try (final InputStream in = status < 400 ? connection.getInputStream() : connection.getErrorStream()) {
      return in == null ? "" : new String(in.readAllBytes(), StandardCharsets.UTF_8);
    }
  }
}
