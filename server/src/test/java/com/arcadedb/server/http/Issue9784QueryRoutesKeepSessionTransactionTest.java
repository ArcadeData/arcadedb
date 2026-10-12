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
package com.arcadedb.server.http;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
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
 * Issue #9784: {@code GET /api/v1/query} and {@code POST /api/v1/query} inherited the default of
 * {@code DatabaseAbstractHandler.participatesInSessionTransaction()}, whose {@code true} makes
 * {@code HttpSession.execute} roll the caller's transaction back when the request fails. The justification of that default - the
 * command failed part way, so what it was running in cannot be trusted - does not hold for a request that provably wrote
 * nothing: a 413 raised after a read-only statement has run, and a 400 raised before the statement is even handed to the engine.
 * Both destroyed a transaction the client had opened with {@code /begin}, and since #9006 the later {@code /commit} was refused
 * instead of reporting success, which made the loss visible but did not give the work back.
 * <p>
 * Two questions were folded into one flag, and they are separate: whether a request RUNS INSIDE the caller's transaction (a query
 * must, for read-your-writes, and it must be refused once a failed command has rolled the transaction back, #9312), and whether
 * its failure DESTROYS that transaction. The query routes keep the first answer and give up the second.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9784QueryRoutesKeepSessionTransactionTest extends BaseGraphServerTest {
  private static final String DOC_TYPE = "Doc9784";
  private static final String NDJSON   = "application/x-ndjson";

  private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();

  private static String rootAuth() {
    return "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8));
  }

  private HttpResponse<String> send(final String method, final String path, final String sessionId, final String accept,
      final String body) throws IOException, InterruptedException {
    final HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(getServerHttpUrl(0, "/api/v1" + path)))
        .timeout(Duration.ofSeconds(30)).header("Authorization", rootAuth());
    if (sessionId != null)
      builder.header(HttpSessionManager.ARCADEDB_SESSION_ID, sessionId);
    if (accept != null)
      builder.header("Accept", accept);
    if (body != null)
      builder.header("Content-Type", "application/json");
    builder.method(method, body != null ? HttpRequest.BodyPublishers.ofString(body) : HttpRequest.BodyPublishers.noBody());
    return http.send(builder.build(), HttpResponse.BodyHandlers.ofString());
  }

  private static String json(final String command, final Integer limit, final String serializer) {
    final JSONObject payload = new JSONObject().put("language", "sql").put("command", command);
    if (limit != null)
      payload.put("limit", limit);
    if (serializer != null)
      payload.put("serializer", serializer);
    return payload.toString();
  }

  private String begin() throws Exception {
    final HttpResponse<String> begun = send("POST", "/begin/" + getDatabaseName(), null, null, "");
    assertThat(begun.statusCode()).isEqualTo(204);
    final String sessionId = begun.headers().firstValue(HttpSessionManager.ARCADEDB_SESSION_ID).orElse(null);
    assertThat(sessionId).isNotBlank();
    return sessionId;
  }

  private void rollback(final String sessionId) throws Exception {
    send("POST", "/rollback/" + getDatabaseName(), sessionId, null, "");
  }

  private HttpResponse<String> command(final String sessionId, final String sql) throws Exception {
    return send("POST", "/command/" + getDatabaseName(), sessionId, null, json(sql, null, null));
  }

  /** The transaction under test: three documents written inside the session, which a read inside it sees (read-your-writes). */
  private String beginWithWitnesses() throws Exception {
    assertThat(command(null, "CREATE DOCUMENT TYPE " + DOC_TYPE + " IF NOT EXISTS").statusCode()).isEqualTo(200);
    final String sessionId = begin();
    for (int i = 0; i < 3; i++)
      assertThat(command(sessionId, "INSERT INTO " + DOC_TYPE + " SET name = 'witness" + i + "'").statusCode()).isEqualTo(200);
    return sessionId;
  }

  private long countInSession(final String sessionId) throws Exception {
    final HttpResponse<String> response = command(sessionId, "SELECT count(*) AS cnt FROM " + DOC_TYPE);
    assertThat(response.statusCode()).as("the session must still be usable: %s", response.body()).isEqualTo(200);
    return new JSONObject(response.body()).getJSONArray("result").getJSONObject(0).getLong("cnt");
  }

  private long committedCount() {
    final Database database = getServerDatabase(0, getDatabaseName());
    return database.countType(DOC_TYPE, false);
  }

  /** The session still holds its three documents and commits them: the work was never lost. */
  private void assertTransactionIntactAndCommit(final String sessionId, final String why) throws Exception {
    assertThat(countInSession(sessionId)).as(why).isEqualTo(3L);
    final HttpResponse<String> committed = send("POST", "/commit/" + getDatabaseName(), sessionId, null, "");
    assertThat(committed.statusCode()).as("the commit must be accepted: %s", committed.body()).isEqualTo(204);
    assertThat(committedCount()).as("and the witnesses are in the database").isEqualTo(3L);
  }

  private static String encoded(final String text) {
    return URLEncoder.encode(text, StandardCharsets.UTF_8).replace("+", "%20");
  }

  /** Runs {@code check} with the result ceiling at 2, below the three rows the session has, restoring it afterwards. */
  private void withLowCeiling(final ThrowingConsumer check) throws Exception {
    final int ceiling = getServer(0).getConfiguration().getValueAsInteger(GlobalConfiguration.SERVER_HTTP_QUERY_MAX_RESULT_ROWS);
    getServer(0).getConfiguration().setValue(GlobalConfiguration.SERVER_HTTP_QUERY_MAX_RESULT_ROWS, 2);
    String sessionId = null;
    try {
      sessionId = beginWithWitnesses();
      check.accept(sessionId);
    } finally {
      getServer(0).getConfiguration().setValue(GlobalConfiguration.SERVER_HTTP_QUERY_MAX_RESULT_ROWS, ceiling);
      if (sessionId != null)
        rollback(sessionId);
    }
  }

  @Test
  void getQueryRefusedForTooManyRowsKeepsTheSessionTransaction() throws Exception {
    withLowCeiling(sessionId -> {
      final HttpResponse<String> refused = send("GET",
          "/query/" + getDatabaseName() + "/sql/" + encoded("SELECT FROM " + DOC_TYPE) + "?limit=1000000", sessionId, null, null);
      assertThat(refused.statusCode()).as("the ceiling cuts the result: %s", refused.body()).isEqualTo(413);

      assertTransactionIntactAndCommit(sessionId, "a read that declines to answer must not destroy the write it was read inside");
    });
  }

  @Test
  void postQueryRefusedForTooManyRowsKeepsTheSessionTransaction() throws Exception {
    withLowCeiling(sessionId -> {
      final HttpResponse<String> refused = send("POST", "/query/" + getDatabaseName(), sessionId, null,
          json("SELECT FROM " + DOC_TYPE, 1_000_000, null));
      assertThat(refused.statusCode()).as("the ceiling cuts the result: %s", refused.body()).isEqualTo(413);

      assertTransactionIntactAndCommit(sessionId, "a read that declines to answer must not destroy the write it was read inside");
    });
  }

  @Test
  void getQueryRefusedBeforeTheStatementRunsKeepsTheSessionTransaction() throws Exception {
    final String sessionId = beginWithWitnesses();
    try {
      // the 'graph' serializer has no row stream: refused with a 400 before the query is handed to the engine
      final HttpResponse<String> refused = send("GET",
          "/query/" + getDatabaseName() + "/sql/" + encoded("SELECT FROM " + DOC_TYPE) + "?serializer=graph", sessionId, NDJSON, null);
      assertThat(refused.statusCode()).as("%s", refused.body()).isEqualTo(400);

      assertTransactionIntactAndCommit(sessionId, "a request refused before the engine was touched must not destroy the transaction");
    } finally {
      rollback(sessionId);
    }
  }

  @Test
  void postQueryRefusedBeforeTheStatementRunsKeepsTheSessionTransaction() throws Exception {
    final String sessionId = beginWithWitnesses();
    try {
      final HttpResponse<String> refused = send("POST", "/query/" + getDatabaseName(), sessionId, NDJSON,
          json("SELECT FROM " + DOC_TYPE, null, "graph"));
      assertThat(refused.statusCode()).as("%s", refused.body()).isEqualTo(400);

      assertTransactionIntactAndCommit(sessionId, "a request refused before the engine was touched must not destroy the transaction");
    } finally {
      rollback(sessionId);
    }
  }

  /**
   * {@code POST /command} reaches the same two streaming preconditions before it hands the statement to the engine, and they are
   * the same kind of refusal: nothing ran, so nothing in the transaction can be untrustworthy.
   */
  @Test
  void commandRefusedBeforeTheStatementRunsKeepsTheSessionTransaction() throws Exception {
    final String sessionId = beginWithWitnesses();
    try {
      // a write cannot be streamed: refused up front, and the INSERT never runs
      final HttpResponse<String> refused = send("POST", "/command/" + getDatabaseName(), sessionId, NDJSON,
          json("INSERT INTO " + DOC_TYPE + " SET name = 'never'", null, null));
      assertThat(refused.statusCode()).as("%s", refused.body()).isEqualTo(400);

      assertTransactionIntactAndCommit(sessionId, "a command refused before the engine was touched must not destroy the transaction");
    } finally {
      rollback(sessionId);
    }
  }

  /** The fourth place a request is refused before the engine is touched: a streamed response has no envelope to profile in. */
  @Test
  void commandAskingToProfileAStreamedResponseIsRefusedAndKeepsTheSessionTransaction() throws Exception {
    final String sessionId = beginWithWitnesses();
    try {
      final String body = new JSONObject().put("language", "sql").put("command", "SELECT FROM " + DOC_TYPE)
          .put("profileExecution", "basic").toString();
      final HttpResponse<String> refused = send("POST", "/command/" + getDatabaseName(), sessionId, NDJSON, body);
      assertThat(refused.statusCode()).as("%s", refused.body()).isEqualTo(400);

      assertTransactionIntactAndCommit(sessionId, "a request refused before the engine was touched must not destroy the transaction");
    } finally {
      rollback(sessionId);
    }
  }

  /**
   * The invariant the query routes' lack of a rollback relies on: {@code database.query} accepts idempotent statements only, so a
   * query that calls a function which writes is refused and leaves NOTHING in the session transaction - not the three documents
   * the caller wrote before it destroyed, and not the one the function tried to add.
   */
  @Test
  void aQueryCallingAFunctionThatWritesIsRefusedAndLeavesTheSessionTransactionUntouched() throws Exception {
    assertThat(command(null, "CREATE DOCUMENT TYPE " + DOC_TYPE + " IF NOT EXISTS").statusCode()).isEqualTo(200);
    assertThat(command(null, "DEFINE FUNCTION fw9784.add \"INSERT INTO " + DOC_TYPE + " SET name = 'smuggled'\" LANGUAGE sql")
        .statusCode()).isEqualTo(200);
    final String sessionId = beginWithWitnesses();
    try {
      final HttpResponse<String> viaGet = send("GET",
          "/query/" + getDatabaseName() + "/sql/" + encoded("SELECT fw9784.add() AS r"), sessionId, null, null);
      assertThat(viaGet.statusCode()).as("%s", viaGet.body()).isGreaterThanOrEqualTo(400);
      final HttpResponse<String> viaPost = send("POST", "/query/" + getDatabaseName(), sessionId, null,
          json("SELECT fw9784.add() AS r", null, null));
      assertThat(viaPost.statusCode()).as("%s", viaPost.body()).isGreaterThanOrEqualTo(400);

      // exactly the three witnesses: the function wrote nothing, and the refusals did not roll anything back
      assertTransactionIntactAndCommit(sessionId, "a refused query must neither write nor destroy what the caller wrote");
    } finally {
      rollback(sessionId);
    }
  }

  /**
   * The counter-case that keeps the tests above meaningful: a command that fails INSIDE the engine, after it was handed the
   * statement, still rolls the transaction back - that is what a half-applied command needs. And a query sent to a session in
   * that state is still refused (#9312) instead of running outside the transaction it believes it is in.
   */
  @Test
  void aCommandFailingInTheEngineStillRollsTheSessionTransactionBack() throws Exception {
    final String sessionId = beginWithWitnesses();
    try {
      final HttpResponse<String> failed = command(sessionId, "INSERT INTO NoSuchType9784 SET name = 'x'");
      assertThat(failed.statusCode()).as("%s", failed.body()).isGreaterThanOrEqualTo(400);

      final HttpResponse<String> query = send("GET",
          "/query/" + getDatabaseName() + "/sql/" + encoded("SELECT FROM " + DOC_TYPE), sessionId, null, null);
      assertThat(query.statusCode()).as("a query must not run outside a transaction that was rolled back: %s", query.body())
          .isGreaterThanOrEqualTo(400);

      final HttpResponse<String> commit = send("POST", "/commit/" + getDatabaseName(), sessionId, null, "");
      assertThat(commit.statusCode()).as("the commit must not report a success it cannot honour").isGreaterThanOrEqualTo(400);
      assertThat(committedCount()).isZero();
    } finally {
      rollback(sessionId);
    }
  }

  @FunctionalInterface
  private interface ThrowingConsumer {
    void accept(String sessionId) throws Exception;
  }
}
