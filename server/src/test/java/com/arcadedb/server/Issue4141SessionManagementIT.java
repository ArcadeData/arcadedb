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

import com.arcadedb.serializer.json.JSONObject;

import org.junit.jupiter.api.Test;

import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;

import static com.arcadedb.server.http.HttpSessionManager.ARCADEDB_SESSION_ID;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #4141 (ISO/IEC 39075 GQL, section 2): end-to-end HTTP test for Session Management. Proves a value set
 * with {@code SESSION SET $x = ...} is visible as {@code $x} to a later command in the same HTTP session, that
 * {@code SESSION RESET} clears it, and that {@code SESSION CLOSE} invalidates the session.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue4141SessionManagementIT extends BaseGraphServerTest {
  @Test
  void sessionParametersFlowAcrossCommandsThenCloseInvalidates() throws Exception {
    testEachServer(serverIndex -> {
      final String baseUrl = "http://127.0.0.1:248" + serverIndex + "/api/v1";
      final String sessionId = beginSession(baseUrl);

      // SESSION SET binds a parameter on the session.
      JSONObject res = command(baseUrl, sessionId, "SESSION SET $threshold = 21");
      assertThat(res.getJSONArray("result").getJSONObject(0).getString("operation")).isEqualTo("set");

      // A later command in the same session sees it as $threshold.
      res = command(baseUrl, sessionId, "RETURN $threshold AS t");
      assertThat(res.getJSONArray("result").getJSONObject(0).getInt("t")).isEqualTo(21);

      // SESSION RESET clears it: $threshold is now unbound, RETURN $threshold yields null.
      command(baseUrl, sessionId, "SESSION RESET");
      res = command(baseUrl, sessionId, "RETURN $threshold AS t");
      assertThat(res.getJSONArray("result").getJSONObject(0).isNull("t")).isTrue();

      // SESSION CLOSE invalidates the session: reusing the id afterwards is rejected.
      command(baseUrl, sessionId, "SESSION CLOSE");
      final int code = commandResponseCode(baseUrl, sessionId, "RETURN 1 AS one");
      assertThat(code).isNotEqualTo(200);
    });
  }

  @Test
  void sessionParametersDoNotBleedToNonSessionRequests() throws Exception {
    testEachServer(serverIndex -> {
      final String baseUrl = "http://127.0.0.1:248" + serverIndex + "/api/v1";
      final String sessionId = beginSession(baseUrl);
      command(baseUrl, sessionId, "SESSION SET $threshold = 21");

      // A request WITHOUT the session header must not inherit the session's parameters, even when it lands
      // on the same pooled worker thread (session-bleed guard in DatabaseAbstractHandler).
      final JSONObject res = commandNoSession(baseUrl, "RETURN $threshold AS t");
      assertThat(res.getJSONArray("result").getJSONObject(0).isNull("t")).isTrue();

      command(baseUrl, sessionId, "SESSION CLOSE");
    });
  }

  @Test
  void sessionParametersResolveOnTheQueryEndpoint() throws Exception {
    testEachServer(serverIndex -> {
      final String baseUrl = "http://127.0.0.1:248" + serverIndex + "/api/v1";
      final String sessionId = beginSession(baseUrl);
      command(baseUrl, sessionId, "SESSION SET $threshold = 21");

      // The /query (GET) endpoint must also see session parameters, not just /command.
      final JSONObject res = queryGet(baseUrl, sessionId, "RETURN $threshold AS t");
      assertThat(res.getJSONArray("result").getJSONObject(0).getInt("t")).isEqualTo(21);

      command(baseUrl, sessionId, "SESSION CLOSE");
    });
  }

  private JSONObject queryGet(final String baseUrl, final String sessionId, final String cypher) throws Exception {
    final String encoded = URLEncoder.encode(cypher, StandardCharsets.UTF_8).replace("+", "%20");
    final HttpURLConnection connection = (HttpURLConnection) new URL(
        baseUrl + "/query/" + getDatabaseName() + "/opencypher/" + encoded).openConnection();
    connection.setRequestMethod("GET");
    connection.setRequestProperty(ARCADEDB_SESSION_ID, sessionId);
    connection.setRequestProperty("Authorization", basicAuth());
    connection.connect();
    try {
      final String response = readResponse(connection);
      assertThat(connection.getResponseCode()).isEqualTo(200);
      return new JSONObject(response);
    } finally {
      connection.disconnect();
    }
  }

  @Test
  void sessionCloseAfterCommitDoesNotFail() throws Exception {
    testEachServer(serverIndex -> {
      final String baseUrl = "http://127.0.0.1:248" + serverIndex + "/api/v1";
      final String sessionId = beginSession(baseUrl);

      // Commit the session transaction so it is no longer active.
      command(baseUrl, sessionId, "COMMIT");

      // SESSION CLOSE must still succeed with no active transaction (cancel() guards with isActive()).
      final JSONObject res = command(baseUrl, sessionId, "SESSION CLOSE");
      assertThat(res.getJSONArray("result").getJSONObject(0).getString("operation")).isEqualTo("close");
    });
  }

  private String beginSession(final String baseUrl) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URL(baseUrl + "/begin/" + getDatabaseName()).openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization", basicAuth());
    connection.connect();
    try {
      readResponse(connection);
      assertThat(connection.getResponseCode()).isEqualTo(204);
      final String sessionId = connection.getHeaderField(ARCADEDB_SESSION_ID).trim();
      assertThat(sessionId).isNotNull();
      return sessionId;
    } finally {
      connection.disconnect();
    }
  }

  private JSONObject command(final String baseUrl, final String sessionId, final String cypher) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URL(baseUrl + "/command/" + getDatabaseName()).openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty(ARCADEDB_SESSION_ID, sessionId);
    connection.setRequestProperty("Authorization", basicAuth());
    formatPayload(connection, "opencypher", cypher, null, new HashMap<>());
    connection.connect();
    try {
      final String response = readResponse(connection);
      assertThat(connection.getResponseCode()).isEqualTo(200);
      return new JSONObject(response);
    } finally {
      connection.disconnect();
    }
  }

  private JSONObject commandNoSession(final String baseUrl, final String cypher) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URL(baseUrl + "/command/" + getDatabaseName()).openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization", basicAuth());
    formatPayload(connection, "opencypher", cypher, null, new HashMap<>());
    connection.connect();
    try {
      final String response = readResponse(connection);
      assertThat(connection.getResponseCode()).isEqualTo(200);
      return new JSONObject(response);
    } finally {
      connection.disconnect();
    }
  }

  private int commandResponseCode(final String baseUrl, final String sessionId, final String cypher) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URL(baseUrl + "/command/" + getDatabaseName()).openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty(ARCADEDB_SESSION_ID, sessionId);
    connection.setRequestProperty("Authorization", basicAuth());
    formatPayload(connection, "opencypher", cypher, null, new HashMap<>());
    connection.connect();
    try {
      // getResponseCode() never throws on 4xx/5xx (unlike readResponse, which reads the input stream).
      final int code = connection.getResponseCode();
      if (code != 200)
        readError(connection); // drain the error stream
      return code;
    } finally {
      connection.disconnect();
    }
  }

  private static String basicAuth() {
    return "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes());
  }
}
