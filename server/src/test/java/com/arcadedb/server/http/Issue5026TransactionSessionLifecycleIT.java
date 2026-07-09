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
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Base64;
import java.util.HashMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end regression tests for GitHub issue #5026 (server audit, HTTP transaction-session lifecycle).
 *
 * <p>Defect 1: REST {@code /commit} and {@code /rollback} must remove the server-side session so a stale
 * session id is no longer resolvable - a follow-up write must be rejected instead of silently auto-committing,
 * and a retried commit/rollback must be idempotent (HTTP 204, never 500).
 *
 * <p>Defect 3: dropping (or changing the password of) a user must invalidate that principal's live HTTP
 * transaction sessions, so a recreated same-name principal cannot adopt the prior principal's open session.
 *
 * @see <a href="https://github.com/ArcadeData/arcadedb/issues/5026">GitHub Issue #5026</a>
 */
public class Issue5026TransactionSessionLifecycleIT extends BaseGraphServerTest {
  private static final String DATABASE_NAME = "graph";

  private String baseUrl(final int serverIndex) {
    return "http://127.0.0.1:248" + serverIndex + "/api/v1";
  }

  private static String rootAuth() {
    return "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes());
  }

  private String beginSession(final int serverIndex, final String auth) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URL(baseUrl(serverIndex) + "/begin/" + DATABASE_NAME).openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization", auth);
    connection.connect();
    try {
      readResponse(connection);
      assertThat(connection.getResponseCode()).isEqualTo(204);
      final String sessionId = connection.getHeaderField(HttpSessionManager.ARCADEDB_SESSION_ID).trim();
      assertThat(sessionId).isNotNull();
      return sessionId;
    } finally {
      connection.disconnect();
    }
  }

  private int insert(final int serverIndex, final String auth, final String sessionId, final String name) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URL(baseUrl(serverIndex) + "/command/" + DATABASE_NAME).openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization", auth);
    if (sessionId != null)
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

  private int post(final int serverIndex, final String verb, final String auth, final String sessionId) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URL(baseUrl(serverIndex) + "/" + verb + "/" + DATABASE_NAME).openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization", auth);
    if (sessionId != null)
      connection.setRequestProperty(HttpSessionManager.ARCADEDB_SESSION_ID, sessionId);
    connection.connect();
    try {
      final int code = connection.getResponseCode();
      if (code >= 400)
        readError(connection);
      else
        readResponse(connection);
      return code;
    } finally {
      connection.disconnect();
    }
  }

  private long countPersons(final int serverIndex, final String name) {
    final Database db = getServerDatabase(serverIndex, DATABASE_NAME);
    return db.query("sql", "SELECT count(*) AS c FROM Person WHERE name = ?", name).next().<Long>getProperty("c");
  }

  @Test
  void commitRemovesSessionAndRejectsStaleWrite() throws Exception {
    testEachServer(serverIndex -> {
      final String auth = rootAuth();
      final String sessionId = beginSession(serverIndex, auth);

      assertThat(insert(serverIndex, auth, sessionId, "commit-visible")).isEqualTo(200);
      assertThat(post(serverIndex, "commit", auth, sessionId)).isEqualTo(204);

      // The committed row is durable.
      assertThat(countPersons(serverIndex, "commit-visible")).isEqualTo(1L);

      // A follow-up WRITE reusing the now-removed session id must be rejected (explicit 4xx), never silently
      // auto-committed and never a 500.
      final int staleWrite = insert(serverIndex, auth, sessionId, "must-not-persist");
      assertThat(staleWrite).as("stale-session write rejected").isEqualTo(404);
      assertThat(countPersons(serverIndex, "must-not-persist")).as("stale write must not persist").isEqualTo(0L);
    });
  }

  @Test
  void rollbackRemovesSessionAndRejectsStaleWrite() throws Exception {
    testEachServer(serverIndex -> {
      final String auth = rootAuth();
      final String sessionId = beginSession(serverIndex, auth);

      assertThat(insert(serverIndex, auth, sessionId, "rolledback")).isEqualTo(200);
      assertThat(post(serverIndex, "rollback", auth, sessionId)).isEqualTo(204);

      assertThat(countPersons(serverIndex, "rolledback")).as("rolled-back row is not durable").isEqualTo(0L);

      final int staleWrite = insert(serverIndex, auth, sessionId, "after-rollback");
      assertThat(staleWrite).as("stale-session write rejected").isEqualTo(404);
      assertThat(countPersons(serverIndex, "after-rollback")).isEqualTo(0L);
    });
  }

  @Test
  void retriedCommitIsIdempotent() throws Exception {
    testEachServer(serverIndex -> {
      final String auth = rootAuth();
      final String sessionId = beginSession(serverIndex, auth);

      assertThat(post(serverIndex, "commit", auth, sessionId)).isEqualTo(204);
      // A second /commit with the same (now removed) session id must be a no-op 204, not a 500.
      assertThat(post(serverIndex, "commit", auth, sessionId)).as("retried commit idempotent").isEqualTo(204);
    });
  }

  @Test
  void retriedRollbackIsIdempotent() throws Exception {
    testEachServer(serverIndex -> {
      final String auth = rootAuth();
      final String sessionId = beginSession(serverIndex, auth);

      assertThat(post(serverIndex, "rollback", auth, sessionId)).isEqualTo(204);
      assertThat(post(serverIndex, "rollback", auth, sessionId)).as("retried rollback idempotent").isEqualTo(204);
    });
  }

  @Test
  void droppingUserInvalidatesItsHttpSessions() throws Exception {
    testEachServer(serverIndex -> {
      getServer(serverIndex).getSecurity().createUser("sess5026", "sess5026pwd");
      final String userAuth = "Basic " + Base64.getEncoder().encodeToString("sess5026:sess5026pwd".getBytes());

      final String sessionId = beginSession(serverIndex, userAuth);
      final HttpSessionManager manager = getServer(serverIndex).getHttpServer().getSessionManager();
      assertThat(manager.getActiveSessions()).isGreaterThanOrEqualTo(1);

      // Drop the principal: its live HTTP transaction session must be invalidated.
      getServer(serverIndex).getSecurity().dropUser("sess5026");

      // A recreated same-name principal must not be able to adopt the prior principal's session id.
      getServer(serverIndex).getSecurity().createUser("sess5026", "sess5026pwd");
      final int staleWrite = insert(serverIndex, userAuth, sessionId, "adopted");
      assertThat(staleWrite).as("recreated same-name user cannot adopt prior session").isEqualTo(404);
      assertThat(countPersons(serverIndex, "adopted")).isEqualTo(0L);

      getServer(serverIndex).getSecurity().dropUser("sess5026");
    });
  }
}
