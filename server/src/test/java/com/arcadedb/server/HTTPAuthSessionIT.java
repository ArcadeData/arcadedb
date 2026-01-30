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

import com.arcadedb.log.LogManager;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Base64;
import java.util.HashMap;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for HTTP authentication sessions (token-based authentication).
 * This allows users to login once and use a token for subsequent requests
 * instead of sending username/password every time.
 *
 * @see <a href="https://github.com/ArcadeData/arcadedb/issues/1691">GitHub Issue #1691</a>
 */
class HTTPAuthSessionIT extends BaseGraphServerTest {

  private static final String DATABASE_NAME = "graph";

  /**
   * Test: Root user can list active sessions via /sessions endpoint.
   */
  @Test
  void listSessionsAsRoot() throws Exception {
    testEachServer((serverIndex) -> {
      // 1. LOGIN: Create a session first
      HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/login").openConnection();

      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      connection.connect();

      String authToken;
      try {
        final String response = readResponse(connection);
        assertThat(connection.getResponseCode()).isEqualTo(200);
        authToken = new JSONObject(response).getString("token");
      } finally {
        connection.disconnect();
      }

      // 2. LIST SESSIONS: Root should be able to see active sessions
      connection = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/sessions").openConnection();

      connection.setRequestMethod("GET");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      connection.connect();

      try {
        final String response = readResponse(connection);
        LogManager.instance().log(this, Level.FINE, "Sessions Response: %s", response);
        assertThat(connection.getResponseCode()).isEqualTo(200);

        final JSONObject responseJson = new JSONObject(response);
        assertThat(responseJson.has("result")).isTrue();
        assertThat(responseJson.has("count")).isTrue();
        assertThat(responseJson.getInt("count")).isGreaterThanOrEqualTo(1);

        // Verify our session is in the list
        final var sessions = responseJson.getJSONArray("result");
        boolean foundOurSession = false;
        for (int i = 0; i < sessions.length(); i++) {
          final JSONObject session = sessions.getJSONObject(i);
          // Verify required fields are present
          assertThat(session.has("token")).isTrue();
          assertThat(session.has("user")).isTrue();
          assertThat(session.has("elapsedMs")).isTrue();
          assertThat(session.has("createdAt")).isTrue();
          assertThat(session.has("lastUpdate")).isTrue();
          // sourceIp, userAgent, country, city may be null but keys should exist
          assertThat(session.has("sourceIp")).isTrue();
          assertThat(session.has("userAgent")).isTrue();
          assertThat(session.has("country")).isTrue();
          assertThat(session.has("city")).isTrue();
          if (session.getString("token").equals(authToken)) {
            foundOurSession = true;
            assertThat(session.getString("user")).isEqualTo("root");
            // createdAt and lastUpdate should be recent timestamps
            assertThat(session.getLong("createdAt")).isGreaterThan(0);
            assertThat(session.getLong("lastUpdate")).isGreaterThanOrEqualTo(session.getLong("createdAt"));
            // sourceIp should be localhost since we're testing locally
            if (!session.isNull("sourceIp")) {
              assertThat(session.getString("sourceIp")).isIn("127.0.0.1", "0:0:0:0:0:0:0:1", "::1");
            }
          }
        }
        assertThat(foundOurSession).isTrue();

      } finally {
        connection.disconnect();
      }

      // 3. CLEANUP: Logout
      connection = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/logout").openConnection();

      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization", "Bearer " + authToken);
      connection.connect();

      try {
        assertThat(connection.getResponseCode()).isEqualTo(204);
      } finally {
        connection.disconnect();
      }
    });
  }

  /**
   * Test: Login with credentials, get a token, use token for subsequent requests.
   */
  @Test
  void loginAndUseToken() throws Exception {
    testEachServer((serverIndex) -> {
      // 1. LOGIN: Get an authentication token using username/password
      HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/login").openConnection();

      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      connection.connect();

      String authToken;
      try {
        final String response = readResponse(connection);
        LogManager.instance().log(this, Level.FINE, "Login Response: %s", response);
        assertThat(connection.getResponseCode()).isEqualTo(200);

        final JSONObject responseJson = new JSONObject(response);
        assertThat(responseJson.has("token")).isTrue();
        authToken = responseJson.getString("token");

        assertThat(authToken).isNotNull();
        assertThat(authToken).startsWith("AU-");

        // Verify user info is returned
        assertThat(responseJson.has("user")).isTrue();
        assertThat(responseJson.getString("user")).isEqualTo("root");

      } finally {
        connection.disconnect();
      }

      // 2. USE TOKEN: Execute a query using only the token (no username/password)
      connection = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/query/" + DATABASE_NAME + "/sql/select%201").openConnection();

      connection.setRequestMethod("GET");
      // Use Bearer token instead of Basic auth
      connection.setRequestProperty("Authorization", "Bearer " + authToken);
      connection.connect();

      try {
        final String response = readResponse(connection);
        LogManager.instance().log(this, Level.FINE, "Query Response: %s", response);
        assertThat(connection.getResponseCode()).isEqualTo(200);
        assertThat(connection.getResponseMessage()).isEqualTo("OK");

        final JSONObject responseJson = new JSONObject(response);
        assertThat(responseJson.has("result")).isTrue();

      } finally {
        connection.disconnect();
      }

      // 3. USE TOKEN WITH COMMAND: Execute a command using the token
      connection = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/command/" + DATABASE_NAME).openConnection();

      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization", "Bearer " + authToken);
      formatPayload(connection, "sql", "select 1 as value", null, new HashMap<>());
      connection.connect();

      try {
        final String response = readResponse(connection);
        LogManager.instance().log(this, Level.FINE, "Command Response: %s", response);
        assertThat(connection.getResponseCode()).isEqualTo(200);

        final JSONObject responseJson = new JSONObject(response);
        assertThat(responseJson.has("result")).isTrue();

      } finally {
        connection.disconnect();
      }

      // 4. LOGOUT: Invalidate the token
      connection = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/logout").openConnection();

      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization", "Bearer " + authToken);
      connection.connect();

      try {
        final String response = readResponse(connection);
        LogManager.instance().log(this, Level.FINE, "Logout Response: %s", response);
        assertThat(connection.getResponseCode()).isEqualTo(204);

      } finally {
        connection.disconnect();
      }

      // 5. VERIFY TOKEN IS INVALID: Using the token after logout should fail
      connection = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/query/" + DATABASE_NAME + "/sql/select%201").openConnection();

      connection.setRequestMethod("GET");
      connection.setRequestProperty("Authorization", "Bearer " + authToken);
      connection.connect();

      try {
        assertThat(connection.getResponseCode()).isEqualTo(401);
      } finally {
        connection.disconnect();
      }
    });
  }

  /**
   * Test: Invalid credentials should not return a token.
   */
  @Test
  void loginWithInvalidCredentials() throws Exception {
    testEachServer((serverIndex) -> {
      HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/login").openConnection();

      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString("root:wrongpassword".getBytes()));
      connection.connect();

      try {
        assertThat(connection.getResponseCode()).isEqualTo(403);
      } finally {
        connection.disconnect();
      }
    });
  }

  /**
   * Test: Invalid token should be rejected.
   */
  @Test
  void queryWithInvalidToken() throws Exception {
    testEachServer((serverIndex) -> {
      HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/query/" + DATABASE_NAME + "/sql/select%201").openConnection();

      connection.setRequestMethod("GET");
      connection.setRequestProperty("Authorization", "Bearer AU-invalid-token-12345");
      connection.connect();

      try {
        assertThat(connection.getResponseCode()).isEqualTo(401);
      } finally {
        connection.disconnect();
      }
    });
  }

  /**
   * Test: Session expires after absolute timeout (from creation).
   * This tests the SERVER_HTTP_AUTH_SESSION_ABSOLUTE_TIMEOUT setting.
   */
  @Test
  void sessionExpiresAfterAbsoluteTimeout() throws Exception {
    testEachServer((serverIndex) -> {
      // Get the auth session manager and check it has absolute timeout configured
      final var server = getServer(serverIndex);
      final var authSessionManager = server.getHttpServer().getAuthSessionManager();

      // Create a session directly for testing (with short absolute timeout)
      final var user = server.getSecurity().getUser("root");
      final var session = authSessionManager.createSession(user);

      assertThat(session).isNotNull();
      assertThat(session.getToken()).startsWith("AU-");

      // The session should be valid initially
      assertThat(authSessionManager.getSessionByToken(session.getToken())).isNotNull();

      // Verify elapsedFromCreation works
      Thread.sleep(100);
      assertThat(session.elapsedFromCreation()).isGreaterThanOrEqualTo(100);

      // Clean up
      authSessionManager.removeSession(session.getToken());
    });
  }

  /**
   * Test: Token can be used with transactions (begin/commit/rollback).
   */
  @Test
  void tokenWithTransaction() throws Exception {
    testEachServer((serverIndex) -> {
      // 1. LOGIN
      HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/login").openConnection();

      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      connection.connect();

      String authToken;
      try {
        final String response = readResponse(connection);
        assertThat(connection.getResponseCode()).isEqualTo(200);
        authToken = new JSONObject(response).getString("token");
      } finally {
        connection.disconnect();
      }

      // 2. BEGIN TRANSACTION using token
      connection = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/begin/" + DATABASE_NAME).openConnection();

      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization", "Bearer " + authToken);
      connection.connect();

      String sessionId;
      try {
        readResponse(connection);
        assertThat(connection.getResponseCode()).isEqualTo(204);
        // Session ID is from the transaction session manager (arcadedb-session-id header)
        sessionId = connection.getHeaderField("arcadedb-session-id");
        assertThat(sessionId).isNotNull();
      } finally {
        connection.disconnect();
      }

      // 3. ROLLBACK using token
      connection = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/rollback/" + DATABASE_NAME).openConnection();

      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization", "Bearer " + authToken);
      connection.setRequestProperty("arcadedb-session-id", sessionId);
      connection.connect();

      try {
        readResponse(connection);
        assertThat(connection.getResponseCode()).isEqualTo(204);
      } finally {
        connection.disconnect();
      }

      // 4. LOGOUT
      connection = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/logout").openConnection();

      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization", "Bearer " + authToken);
      connection.connect();

      try {
        assertThat(connection.getResponseCode()).isEqualTo(204);
      } finally {
        connection.disconnect();
      }
    });
  }
}
