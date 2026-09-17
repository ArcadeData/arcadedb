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
package com.arcadedb.server.ws;

import com.arcadedb.database.Database;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.http.HttpSession;
import com.arcadedb.server.security.ServerSecurity;
import com.arcadedb.utility.FileUtils;

import org.junit.jupiter.api.Test;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Part 1 of issue #7403: a {@code /ws} insert session that writes into a transaction the client began over HTTP
 * with {@code POST /api/v1/begin}, instead of opening one of its own.
 * <p>
 * The gRPC {@code InsertBidirectional} RPC has had this since issue #6795 ({@code Start.transaction} plus
 * {@code TransactionMode.NONE}); until this issue the {@code /ws} session had no way to NAME an existing
 * transaction, so {@code transactionMode: "none"} was refused outright.
 */
class Issue7403ExternalTransactionIT extends BaseGraphServerTest {
  private static final String OTHER_USER     = "issue7403user";
  private static final String OTHER_PASSWORD = "ThisIsALongPassword1!";
  private static final String OTHER_DATABASE = "issue7403other";

  /**
   * The verification the issue asks for: rows sent under an externally-begun transaction join it, and the
   * caller's own HTTP rollback undoes them - including the ones the session was acknowledged for and the ones
   * its own {@code commit} frame "committed".
   */
  @Test
  void rowsJoinTheCallersTransactionAndItsHttpRollbackUndoesThem() throws Throwable {
    final Database database = getServerDatabase(0, getDatabaseName());
    assertThat(database.countType("Person", false)).isZero();

    final String transactionId = httpBegin();

    try (final var client = newClient()) {
      final JSONObject started = new JSONObject(client.send(startExternal(transactionId)));
      assertThat(started.getString("action", "")).isEqualTo("started");
      assertThat(started.getString("transactionMode", "")).isEqualTo("none");
      assertThat(started.getString("transactionId", "")).isEqualTo(transactionId);

      assertThat(new JSONObject(client.send(chunk(started.getString("sessionId"), 1, "a", "b")))
          .getLong("inserted", -1)).isEqualTo(2);

      final JSONObject committed = new JSONObject(client.send(control("commit", started.getString("sessionId"))));
      // Not "commit": the frame decided nothing, the HTTP /commit or /rollback below does.
      assertThat(committed.getString("outcome", "")).isEqualTo("detached");
      assertThat(committed.getJSONObject("summary").getBoolean("externalTransaction", false)).isTrue();
      assertThat(committed.getJSONObject("summary").getBoolean("partialCommit", true)).isFalse();
      assertThat(committed.getJSONObject("summary").getLong("inserted", -1)).isEqualTo(2);
    }

    // The /ws session is gone but the transaction it wrote into is not: it is still the caller's to end.
    assertThat(database.countType("Person", false)).isZero();
    assertThat(httpTransaction("rollback", transactionId)).isEqualTo(204);
    assertThat(database.countType("Person", false)).isZero();
  }

  /** The companion: the caller's HTTP commit is what makes the session's rows durable. */
  @Test
  void theCallersHttpCommitIsWhatPersistsTheRows() throws Throwable {
    final Database database = getServerDatabase(0, getDatabaseName());
    final String transactionId = httpBegin();

    try (final var client = newClient()) {
      final String sessionId = new JSONObject(client.send(startExternal(transactionId))).getString("sessionId");
      assertThat(new JSONObject(client.send(chunk(sessionId, 1, "a", "b", "c"))).getLong("inserted", -1)).isEqualTo(3);
      assertThat(new JSONObject(client.send(control("commit", sessionId))).getString("outcome", "")).isEqualTo("detached");
    }

    assertThat(database.countType("Person", false)).isZero();
    assertThat(httpTransaction("commit", transactionId)).isEqualTo(204);
    assertThat(database.countType("Person", false)).isEqualTo(3);
  }

  /**
   * A connection that simply goes away must not roll the caller's transaction back. Every other way a {@code /ws}
   * session ends is a rollback precisely because the client did not say what it wanted - but here the client
   * still holds the transaction over HTTP and can still say.
   */
  @Test
  void aDroppedConnectionLeavesTheCallersTransactionAlone() throws Throwable {
    final Database database = getServerDatabase(0, getDatabaseName());
    final String transactionId = httpBegin();

    final var client = newClient();
    final String sessionId = new JSONObject(client.send(startExternal(transactionId))).getString("sessionId");
    assertThat(new JSONObject(client.send(chunk(sessionId, 1, "orphan"))).getLong("inserted", -1)).isEqualTo(1);
    client.breakConnection();

    assertThat(httpTransaction("commit", transactionId)).isEqualTo(204);
    assertThat(database.countType("Person", false)).isEqualTo(1);
  }

  /**
   * The transaction ending underneath the session - the client's own HTTP {@code /commit}, or the HTTP idle
   * sweep - is refused on the next chunk rather than silently applied somewhere else. Each chunk runs inside
   * {@code HttpSession.execute}, which re-validates that the session is still registered under its lock; without
   * that the chunk would be written into a rolled-back {@code TransactionContext} and acknowledged as if it had
   * landed.
   */
  @Test
  void aChunkAfterTheCallerEndedTheTransactionIsRefused() throws Throwable {
    final Database database = getServerDatabase(0, getDatabaseName());
    final String transactionId = httpBegin();

    try (final var client = newClient()) {
      final String sessionId = new JSONObject(client.send(startExternal(transactionId))).getString("sessionId");
      assertThat(new JSONObject(client.send(chunk(sessionId, 1, "first"))).getLong("inserted", -1)).isEqualTo(1);

      // The client commits over HTTP while the /ws session is still open.
      assertThat(httpTransaction("commit", transactionId)).isEqualTo(204);

      final JSONObject refused = new JSONObject(client.send(chunk(sessionId, 2, "second")));
      assertThat(refused.getString("result", "")).isEqualTo("error");
      assertThat(refused.getString("detail", "")).contains(transactionId);
    }

    // The first chunk is durable because the client committed it; the second never happened.
    assertThat(database.countType("Person", false)).isEqualTo(1);
  }

  /**
   * A chunk that cannot take the caller's session lock within {@code HttpSession}'s five seconds is answered as
   * an insert-session error naming the contention, not as an "Internal error".
   * <p>
   * {@code HttpSession.execute} signals that with a {@link com.arcadedb.exception.LockTimeoutException}, which
   * is a {@code NeedRetryException} and therefore not one of the types
   * {@code WebSocketInsertProtocol.execute} names - it would have fallen through to the generic catch and told
   * the client the server had broken, when in truth its own two clients contended on one transaction
   * (claude-review on PR #7811).
   */
  @Test
  void aBusyExternalTransactionIsReportedAsAnInsertSessionErrorRatherThanAnInternalOne() throws Throwable {
    final String transactionId = httpBegin();
    final HttpSession httpSession = getServer(0).getHttpServer().getSessionManager()
        .getSessionById(getServer(0).getSecurity().getUser("root"), transactionId);
    assertThat(httpSession).isNotNull();

    final CountDownLatch holding = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    final Thread hog = new Thread(() -> {
      try {
        // Occupies the session exactly as a long-running HTTP command on that transaction would.
        httpSession.execute(getServer(0).getSecurity().getUser("root"), () -> {
          holding.countDown();
          release.await();
          return null;
        });
      } catch (final Exception ignored) {
      }
    }, "issue7403-session-hog");
    hog.setDaemon(true);
    hog.start();

    try (final var client = newClient()) {
      assertThat(holding.await(10, TimeUnit.SECONDS)).isTrue();

      final String sessionId = new JSONObject(client.send(startExternal(transactionId))).getString("sessionId");

      // HttpSession.execute waits 5s for the lock, so the answer takes longer than the client helper's default.
      client.sendWithoutWaiting(chunk(sessionId, 1, "blocked"));
      final JSONObject refused = new JSONObject(client.popMessage(20_000));
      assertThat(refused.getString("result", "")).isEqualTo("error");
      assertThat(refused.getString("error", ""))
          .as("contention on the caller's own transaction is not an internal error").isEqualTo("Insert session error");
      assertThat(refused.getString("detail", "")).contains("is busy with another command");

      release.countDown();
      hog.join(10_000);

      new JSONObject(client.send(control("rollback", sessionId)));
    } finally {
      release.countDown();
    }

    assertThat(httpTransaction("rollback", transactionId)).isEqualTo(204);
  }

  /** An unknown or expired transaction id is refused, never served against a fresh server-managed transaction. */
  @Test
  void anUnknownTransactionIdIsRefused() throws Throwable {
    try (final var client = newClient()) {
      final JSONObject error = new JSONObject(client.send(startExternal("AS-does-not-exist")));
      assertThat(error.getString("result", "")).isEqualTo("error");
      assertThat(error.getString("detail", "")).contains("not found or expired");
    }

    assertThat(getServer(0).getHttpServer().getInsertSessionManager().getOpenSessionCount()).isZero();
  }

  /**
   * A transaction another principal opened is refused, because the resolution goes THROUGH
   * {@code HttpSessionManager.getSessionById(user, id)} rather than around it - the same gate the HTTP routes
   * use. The second principal is a real server user, not a mock: the gate compares
   * {@code ServerSecurityUser} identity.
   */
  @Test
  void aTransactionOpenedByAnotherPrincipalIsRefused() throws Throwable {
    // Created through the server API rather than over HTTP: BaseGraphServerTest's command() helper hardcodes
    // port 2480, which a locally installed ArcadeDB may already hold - the test server then binds 2481 and the
    // helper would talk to the wrong process entirely.
    final ServerSecurity security = getServer(0).getSecurity();
    if (security.existsUser(OTHER_USER))
      security.dropUser(OTHER_USER);
    security.createUser(new JSONObject()
        .put("name", OTHER_USER)
        .put("password", security.encodePassword(OTHER_PASSWORD))
        .put("databases", new JSONObject().put(getDatabaseName(), new JSONArray())));
    try {
      final String transactionId = httpBegin(OTHER_USER, OTHER_PASSWORD);

      // root is a different principal from the one that opened it, and gets exactly the answer an id that never
      // existed gets - the gate does not distinguish, and neither should the message.
      try (final var client = newClient()) {
        final JSONObject error = new JSONObject(client.send(startExternal(transactionId)));
        assertThat(error.getString("result", "")).isEqualTo("error");
        assertThat(error.getString("detail", "")).contains("not found or expired");
      }

      assertThat(httpTransaction("rollback", transactionId, OTHER_USER, OTHER_PASSWORD)).isEqualTo(204);
    } finally {
      security.dropUser(OTHER_USER);
    }
  }

  /**
   * A transaction on another database is refused. The {@code start} frame names the database and the
   * transaction id independently, so a principal with access to both could otherwise open a transaction on one
   * and have the session write into the other.
   */
  @Test
  void aTransactionOnAnotherDatabaseIsRefused() throws Throwable {
    getServer(0).createDatabase(OTHER_DATABASE, ComponentFile.MODE.READ_WRITE);
    try {
      final String transactionId = httpBegin(OTHER_DATABASE);

      try (final var client = newClient()) {
        final JSONObject error = new JSONObject(client.send(startExternal(transactionId)));
        assertThat(error.getString("result", "")).isEqualTo("error");
        assertThat(error.getString("detail", "")).contains("belongs to database '" + OTHER_DATABASE + "'");
      }

      assertThat(httpTransaction("rollback", transactionId, "root", DEFAULT_PASSWORD_FOR_TESTS, OTHER_DATABASE))
          .isEqualTo(204);
    } finally {
      getServer(0).getDatabase(OTHER_DATABASE).getEmbedded().drop();
    }
  }

  /** {@code transactionId} and a server-managed mode contradict each other, and the contradiction is refused. */
  @Test
  void aTransactionIdWithAServerManagedModeIsRefused() throws Throwable {
    final String transactionId = httpBegin();
    try {
      try (final var client = newClient()) {
        final JSONObject message = new JSONObject();
        message.put("action", "start");
        message.put("database", getDatabaseName());
        message.put("transactionId", transactionId);
        message.put("options", new JSONObject().put("targetType", "Person").put("transactionMode", "per_batch"));

        final JSONObject error = new JSONObject(client.send(message.toString()));
        assertThat(error.getString("result", "")).isEqualTo("error");
        assertThat(error.getString("detail", "")).contains("goes with transactionMode 'none'");
      }
    } finally {
      assertThat(httpTransaction("rollback", transactionId)).isEqualTo(204);
    }
  }

  /**
   * {@code none} without a {@code transactionId} is still refused, and still points at this issue - the message
   * now names the field that resolves it rather than saying the feature does not exist.
   */
  @Test
  void noneWithoutATransactionIdIsStillRefusedWithThePointerToItsIssue() throws Throwable {
    try (final var client = newClient()) {
      final JSONObject message = new JSONObject();
      message.put("action", "start");
      message.put("database", getDatabaseName());
      message.put("options", new JSONObject().put("targetType", "Person").put("transactionMode", "none"));

      final JSONObject error = new JSONObject(client.send(message.toString()));
      assertThat(error.getString("result", "")).isEqualTo("error");
      assertThat(error.getString("detail", "")).contains("#7403").contains("transactionId");
    }

    assertThat(getServer(0).getHttpServer().getInsertSessionManager().getOpenSessionCount()).isZero();
  }

  // ---------------------------------------------------------------------------------------------------------

  private WebSocketClientHelper newClient() throws Exception {
    return new WebSocketClientHelper(
        "ws://localhost:" + getServer(0).getHttpServer().getPort() + "/ws", "root", DEFAULT_PASSWORD_FOR_TESTS);
  }

  private String startExternal(final String transactionId) {
    final JSONObject message = new JSONObject();
    message.put("action", "start");
    message.put("database", getDatabaseName());
    message.put("transactionId", transactionId);
    message.put("options", new JSONObject().put("targetType", "Person").put("transactionMode", "none"));
    return message.toString();
  }

  private static String chunk(final String sessionId, final long chunkSeq, final String... names) {
    final JSONArray records = new JSONArray();
    for (final String name : names)
      records.put(new JSONObject().put("name", name));

    final JSONObject message = new JSONObject();
    message.put("action", "chunk");
    message.put("sessionId", sessionId);
    message.put("chunkSeq", chunkSeq);
    message.put("records", records);
    return message.toString();
  }

  private static String control(final String action, final String sessionId) {
    return new JSONObject().put("action", action).put("sessionId", sessionId).toString();
  }

  private String httpBegin() throws Exception {
    return httpBegin("root", DEFAULT_PASSWORD_FOR_TESTS);
  }

  private String httpBegin(final String user, final String password) throws Exception {
    return httpBegin(getDatabaseName(), user, password);
  }

  private String httpBegin(final String databaseName) throws Exception {
    return httpBegin(databaseName, "root", DEFAULT_PASSWORD_FOR_TESTS);
  }

  private String httpBegin(final String databaseName, final String user, final String password) throws Exception {
    final HttpURLConnection connection = open("begin/" + databaseName, user, password);
    try {
      writeEmptyBody(connection);
      assertThat(connection.getResponseCode()).isEqualTo(204);
      final String sessionId = connection.getHeaderField("arcadedb-session-id");
      assertThat(sessionId).as("POST /api/v1/begin must mint a session id").isNotBlank();
      return sessionId;
    } finally {
      connection.disconnect();
    }
  }

  private int httpTransaction(final String action, final String transactionId) throws Exception {
    return httpTransaction(action, transactionId, "root", DEFAULT_PASSWORD_FOR_TESTS);
  }

  private int httpTransaction(final String action, final String transactionId, final String user, final String password)
      throws Exception {
    return httpTransaction(action, transactionId, user, password, getDatabaseName());
  }

  private int httpTransaction(final String action, final String transactionId, final String user, final String password,
      final String databaseName) throws Exception {
    final HttpURLConnection connection = open(action + "/" + databaseName, user, password);
    connection.setRequestProperty("arcadedb-session-id", transactionId);
    try {
      writeEmptyBody(connection);
      final int code = connection.getResponseCode();
      if (code >= 400 && connection.getErrorStream() != null)
        FileUtils.readStreamAsString(connection.getErrorStream(), "utf8");
      return code;
    } finally {
      connection.disconnect();
    }
  }

  private HttpURLConnection open(final String path, final String user, final String password) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URI(
        "http://127.0.0.1:" + getServer(0).getHttpServer().getPort() + "/api/v1/" + path).toURL().openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString((user + ":" + password).getBytes(StandardCharsets.UTF_8)));
    connection.setDoOutput(true);
    return connection;
  }

  /**
   * Writes the body and connects. Separate from {@link #open} so a caller can still set request headers - an
   * {@code HttpURLConnection} refuses them with "Already connected" once the output stream has been opened.
   */
  private static void writeEmptyBody(final HttpURLConnection connection) throws Exception {
    try (final OutputStream out = connection.getOutputStream()) {
      out.write("{}".getBytes(StandardCharsets.UTF_8));
    }
  }
}
