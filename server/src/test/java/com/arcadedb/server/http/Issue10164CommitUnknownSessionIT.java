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

import com.arcadedb.database.Database;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Base64;
import java.util.HashMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for GitHub issue #10164: {@code POST /api/v1/commit} naming a session the server does not hold
 * answered 204, the same answer as a real commit, although nothing was committed. It now answers 404, like any other
 * call on that session, while a retried commit of a session that DID commit stays an idempotent 204 (issue #5026).
 */
class Issue10164CommitUnknownSessionIT extends BaseGraphServerTest {
  private static final String DATABASE_NAME = "graph";

  private String baseUrl(final int serverIndex) {
    return "http://127.0.0.1:" + getServerHttpPort(serverIndex) + "/api/v1";
  }

  private static String basicAuth(final String user, final String password) {
    return "Basic " + Base64.getEncoder().encodeToString((user + ":" + password).getBytes());
  }

  private static String rootAuth() {
    return basicAuth("root", DEFAULT_PASSWORD_FOR_TESTS);
  }

  private String begin(final int serverIndex, final String auth) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URL(baseUrl(serverIndex) + "/begin/" + DATABASE_NAME).openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization", auth);
    connection.connect();
    try {
      readResponse(connection);
      assertThat(connection.getResponseCode()).isEqualTo(204);
      return connection.getHeaderField(HttpSessionManager.ARCADEDB_SESSION_ID).trim();
    } finally {
      connection.disconnect();
    }
  }

  private int insert(final int serverIndex, final String auth, final String sessionId, final String name) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URL(baseUrl(serverIndex) + "/command/" + DATABASE_NAME).openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization", auth);
    connection.setRequestProperty(HttpSessionManager.ARCADEDB_SESSION_ID, sessionId);
    formatPayload(connection, "sql", "INSERT INTO Person SET name = '" + name + "'", null, new HashMap<>());
    connection.connect();
    try {
      final int code = connection.getResponseCode();
      if (code == 200)
        readResponse(connection);
      else
        readError(connection);
      return code;
    } finally {
      connection.disconnect();
    }
  }

  /** Answers the status code, and the error body into {@code error[0]} when there is one. */
  private int post(final int serverIndex, final String verb, final String auth, final String sessionId, final String[] error)
      throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URL(baseUrl(serverIndex) + "/" + verb + "/" + DATABASE_NAME).openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization", auth);
    if (sessionId != null)
      connection.setRequestProperty(HttpSessionManager.ARCADEDB_SESSION_ID, sessionId);
    connection.connect();
    try {
      final int code = connection.getResponseCode();
      if (code >= 400) {
        final String body = readError(connection);
        if (error != null)
          error[0] = body;
      } else
        readResponse(connection);
      return code;
    } finally {
      connection.disconnect();
    }
  }

  private int post(final int serverIndex, final String verb, final String auth, final String sessionId) throws Exception {
    return post(serverIndex, verb, auth, sessionId, null);
  }

  private String terminateTransaction(final int serverIndex, final String sessionId) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URL(baseUrl(serverIndex) + "/server").openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization", rootAuth());
    formatPayload(connection, new JSONObject().put("command", "terminate transaction " + sessionId));
    connection.connect();
    try {
      assertThat(connection.getResponseCode()).isEqualTo(200);
      return new JSONObject(readResponse(connection)).getJSONObject("result").getString("status");
    } finally {
      connection.disconnect();
    }
  }

  private long countPersons(final int serverIndex, final String name) {
    final Database db = getServerDatabase(serverIndex, DATABASE_NAME);
    return db.query("sql", "SELECT count(*) AS c FROM Person WHERE name = ?", name).next().<Long>getProperty("c");
  }

  @Test
  void commitOfATerminatedTransactionAnswers404() throws Exception {
    testEachServer(serverIndex -> {
      final String sessionId = begin(serverIndex, rootAuth());
      assertThat(insert(serverIndex, rootAuth(), sessionId, "terminated10164")).isEqualTo(200);

      assertThat(terminateTransaction(serverIndex, sessionId)).isEqualTo("terminated");

      final String[] error = new String[1];
      assertThat(post(serverIndex, "commit", rootAuth(), sessionId, error)).as("commit of a terminated transaction").isEqualTo(404);
      assertThat(error[0]).contains("not found or expired");
      assertThat(countPersons(serverIndex, "terminated10164")).as("the insert was rolled back by the terminate").isZero();

      // Rollback answering 204 stays: the outcome the caller asked for holds
      assertThat(post(serverIndex, "rollback", rootAuth(), sessionId)).isEqualTo(204);
    });
  }

  @Test
  void commitOfASessionTheServerNeverIssuedAnswers404() throws Exception {
    testEachServer(serverIndex -> {
      final String[] error = new String[1];
      assertThat(post(serverIndex, "commit", rootAuth(), "AS-nonexistent", error)).isEqualTo(404);
      assertThat(error[0]).contains("not found or expired");
    });
  }

  @Test
  void commitAfterRollbackAnswers404() throws Exception {
    testEachServer(serverIndex -> {
      final String sessionId = begin(serverIndex, rootAuth());
      assertThat(insert(serverIndex, rootAuth(), sessionId, "rolledback10164")).isEqualTo(200);
      assertThat(post(serverIndex, "rollback", rootAuth(), sessionId)).isEqualTo(204);

      assertThat(post(serverIndex, "commit", rootAuth(), sessionId)).as("commit of a rolled-back transaction").isEqualTo(404);
      assertThat(countPersons(serverIndex, "rolledback10164")).isZero();
    });
  }

  @Test
  void commitOfAnExpiredSessionAnswers404() throws Exception {
    testEachServer(serverIndex -> {
      final String sessionId = begin(serverIndex, rootAuth());
      assertThat(insert(serverIndex, rootAuth(), sessionId, "expired10164")).isEqualTo(200);

      // What the idle sweep does to a session past its timeout: roll it back and forget it
      final HttpSessionManager manager = getServer(serverIndex).getHttpServer().getSessionManager();
      manager.removeSession(sessionId).cancel();

      assertThat(post(serverIndex, "commit", rootAuth(), sessionId)).as("commit of an expired session").isEqualTo(404);
      assertThat(countPersons(serverIndex, "expired10164")).isZero();
    });
  }

  @Test
  void retriedCommitOfACommittedSessionStays204() throws Exception {
    testEachServer(serverIndex -> {
      final String sessionId = begin(serverIndex, rootAuth());
      assertThat(insert(serverIndex, rootAuth(), sessionId, "committed10164")).isEqualTo(200);
      assertThat(post(serverIndex, "commit", rootAuth(), sessionId)).isEqualTo(204);

      // The first answer may have been lost on the way back: the retry must not tell the client its work is gone
      assertThat(post(serverIndex, "commit", rootAuth(), sessionId)).as("retried commit").isEqualTo(204);
      assertThat(countPersons(serverIndex, "committed10164")).isEqualTo(1L);
    });
  }

  @Test
  void retryOfAFailedCommitAnswers404() throws Exception {
    testEachServer(serverIndex -> {
      final Database database = getServerDatabase(serverIndex, DATABASE_NAME);
      if (!database.getSchema().existsType("Unique10164")) {
        database.getSchema().createDocumentType("Unique10164").createProperty("k", Type.INTEGER);
        database.getSchema().getType("Unique10164").createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "k");
      }
      database.transaction(() -> database.newDocument("Unique10164").set("k", serverIndex).save());

      final String sessionId = begin(serverIndex, rootAuth());
      final HttpURLConnection connection = (HttpURLConnection) new URL(baseUrl(serverIndex) + "/command/" + DATABASE_NAME).openConnection();
      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization", rootAuth());
      connection.setRequestProperty(HttpSessionManager.ARCADEDB_SESSION_ID, sessionId);
      formatPayload(connection, "sql", "INSERT INTO Unique10164 SET k = " + serverIndex, null, new HashMap<>());
      connection.connect();
      try {
        // The unique key is checked when the transaction commits, so the insert itself may be accepted
        if (connection.getResponseCode() == 200)
          readResponse(connection);
        else
          readError(connection);
      } finally {
        connection.disconnect();
      }

      final int first = post(serverIndex, "commit", rootAuth(), sessionId);
      assertThat(first).as("the commit breaks the unique key, or the session was already refused").isGreaterThanOrEqualTo(400);

      // A failed commit is not remembered as one: its retry must not answer 204
      assertThat(post(serverIndex, "commit", rootAuth(), sessionId)).as("retry of a failed commit").isEqualTo(404);
    });
  }

  @Test
  void anotherUserCannotLearnACommittedSessionByRetryingIt() throws Exception {
    testEachServer(serverIndex -> {
      getServer(serverIndex).getSecurity().createUser("other10164", "other10164pwd");
      try {
        final String sessionId = begin(serverIndex, rootAuth());
        assertThat(post(serverIndex, "commit", rootAuth(), sessionId)).isEqualTo(204);

        // The committed-session memory is owner-gated like the sessions themselves
        assertThat(post(serverIndex, "commit", basicAuth("other10164", "other10164pwd"), sessionId)).isEqualTo(404);
      } finally {
        getServer(serverIndex).getSecurity().dropUser("other10164");
      }
    });
  }

  @Test
  void commitWithoutASessionStays204() throws Exception {
    testEachServer(serverIndex -> assertThat(post(serverIndex, "commit", rootAuth(), null)).isEqualTo(204));
  }
}
