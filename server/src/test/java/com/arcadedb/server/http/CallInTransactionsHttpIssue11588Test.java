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

import com.arcadedb.exception.ImplicitTransactionRequiredException;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Base64;

import static com.arcadedb.server.http.HttpSessionManager.ARCADEDB_SESSION_ID;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11588 over HTTP: CALL { ... } IN TRANSACTIONS keeps running in batches in the transaction the command handler
 * opens around a session-less request, an implicit transaction, and is refused inside the transaction a client opened with
 * /begin, whose rollback could not undo the batches.
 */
class CallInTransactionsHttpIssue11588Test extends BaseGraphServerTest {

  private static final String STATEMENT = "UNWIND range(1, 25) AS i CALL { WITH i CREATE (:%s {i: i}) } IN TRANSACTIONS OF 10 ROWS";

  @Test
  void runsInBatchesInTheAutoCommitTransactionOfTheCommandHandler() throws Exception {
    testEachServer(serverIndex -> {
      final String label = "AutoCommit11588_" + serverIndex;
      final HttpURLConnection connection = post(serverIndex, "command", null, cypher(STATEMENT.formatted(label)));
      try {
        final int status = connection.getResponseCode();
        assertThat(status).as(status == 200 ? "" : readError(connection)).isEqualTo(200);
      } finally {
        connection.disconnect();
      }
      assertThat(count(serverIndex, label)).isEqualTo(25);
    });
  }

  /**
   * Issue #13172: a label written before the CALL replaces the node by a record the batch's own transaction could not see while
   * the auto-commit transaction of the command handler held it uncommitted. That transaction is committed before the batch.
   */
  @Test
  void batchSeesTheLabelWrittenBeforeTheCallInTheAutoCommitTransaction() throws Exception {
    testEachServer(serverIndex -> {
      final String label = "Probe13172_" + serverIndex;
      getServerDatabase(serverIndex, "graph").transaction(() -> getServerDatabase(serverIndex, "graph").command("opencypher",
          "CREATE (:" + label + " {id: 1})-[:R13172]->(:" + label + " {id: 2})"));

      final HttpURLConnection connection = post(serverIndex, "command", null, cypher(("MATCH (a:%1$s)-[:R13172]->(b:%1$s) "
          + "FOREACH (n IN [b] | SET n:TmpReview13172) FOREACH (n IN [b] | REMOVE n:TmpReview13172) "
          + "CALL (b) { FOREACH (n IN [b] | SET n:TmpReview13172) FOREACH (n IN [b] | REMOVE n:TmpReview13172) RETURN 1 AS k } "
          + "IN TRANSACTIONS OF 10 ROWS RETURN a.id AS a, b.id AS b, k").formatted(label)));
      try {
        final int status = connection.getResponseCode();
        assertThat(status).as(status == 200 ? "" : readError(connection)).isEqualTo(200);
        final JSONObject row = new JSONObject(readResponse(connection)).getJSONArray("result").getJSONObject(0);
        assertThat(row.getInt("a")).isEqualTo(1);
        assertThat(row.getInt("b")).isEqualTo(2);
        assertThat(row.getInt("k")).isEqualTo(1);
      } finally {
        connection.disconnect();
      }
      assertThat(count(serverIndex, label)).isEqualTo(2);
    });
  }

  @Test
  void refusedInTheTransactionTheClientBegan() throws Exception {
    testEachServer(serverIndex -> {
      final String label = "Session11588_" + serverIndex;
      final String sessionId;
      HttpURLConnection connection = post(serverIndex, "begin", null, null);
      try {
        assertThat(connection.getResponseCode()).isEqualTo(204);
        sessionId = connection.getHeaderField(ARCADEDB_SESSION_ID).trim();
      } finally {
        connection.disconnect();
      }

      connection = post(serverIndex, "command", sessionId, cypher(STATEMENT.formatted(label)));
      try {
        // the client's transaction state is the cause: 409 Conflict, not the 500 of a server fault (issue #12161)
        assertThat(connection.getResponseCode()).isEqualTo(409);
        final String error = readError(connection);
        assertThat(error).contains("can only be executed in an implicit transaction");
        assertThat(error).contains(ImplicitTransactionRequiredException.class.getName());
      } finally {
        connection.disconnect();
      }

      connection = post(serverIndex, "rollback", sessionId, null);
      try {
        assertThat(connection.getResponseCode()).isEqualTo(204);
      } finally {
        connection.disconnect();
      }
      assertThat(count(serverIndex, label)).isZero();
    });
  }

  private static JSONObject cypher(final String command) {
    final JSONObject payload = new JSONObject();
    payload.put("language", "opencypher");
    payload.put("command", command);
    return payload;
  }

  private long count(final int serverIndex, final String label) {
    return getServerDatabase(serverIndex, "graph").query("opencypher", "MATCH (x:" + label + ") RETURN count(x) AS c").next()
        .<Number>getProperty("c").longValue();
  }

  private HttpURLConnection post(final int serverIndex, final String endpoint, final String sessionId, final JSONObject payload)
      throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URL(
        getServerHttpUrl(serverIndex, "/api/v1/" + endpoint + "/graph")).openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    if (sessionId != null)
      connection.setRequestProperty(ARCADEDB_SESSION_ID, sessionId);
    if (payload != null)
      formatPayload(connection, payload);
    connection.connect();
    return connection;
  }
}
