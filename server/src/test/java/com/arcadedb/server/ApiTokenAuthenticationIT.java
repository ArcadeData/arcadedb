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
import com.arcadedb.server.security.ApiTokenConfiguration;

import org.junit.jupiter.api.Test;

import java.io.*;
import java.net.*;
import java.nio.file.Files;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

class ApiTokenAuthenticationIT extends BaseGraphServerTest {

  @Test
  void createTokenViaApi() throws Exception {
    testEachServer((serverIndex) -> {
      final String tokenValue = createApiToken(serverIndex, "Test Token", "graph", 0,
          new JSONObject()
              .put("types", new JSONObject()
                  .put("*", new JSONObject().put("access", new JSONArray().put("readRecord"))))
              .put("database", new JSONArray()));

      assertThat(tokenValue).startsWith("at-");
    });
  }

  @Test
  void listTokensViaApi() throws Exception {
    testEachServer((serverIndex) -> {
      createApiToken(serverIndex, "Token1", "graph", 0, new JSONObject());
      createApiToken(serverIndex, "Token2", "graph", 0, new JSONObject());

      final HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/server/api-tokens").openConnection();
      connection.setRequestMethod("GET");
      connection.setRequestProperty("Authorization", basicAuth());
      connection.connect();

      try {
        assertThat(connection.getResponseCode()).isEqualTo(200);
        final JSONObject response = new JSONObject(readResponse(connection));
        assertThat(response.getInt("count")).isGreaterThanOrEqualTo(2);

        // Verify hashed fields are present and plaintext is not
        final JSONArray result = response.getJSONArray("result");
        for (int i = 0; i < result.length(); i++) {
          final JSONObject token = result.getJSONObject(i);
          assertThat(token.has("tokenHash")).isTrue();
          assertThat(token.has("tokenSuffix")).isTrue();
          assertThat(token.has("token")).isFalse();
        }
      } finally {
        connection.disconnect();
      }
    });
  }

  @Test
  void useApiTokenForQuery() throws Exception {
    testEachServer((serverIndex) -> {
      final JSONObject permissions = new JSONObject()
          .put("types", new JSONObject()
              .put("*", new JSONObject().put("access",
                  new JSONArray().put("createRecord").put("readRecord").put("updateRecord").put("deleteRecord"))))
          .put("database", new JSONArray());

      final String tokenValue = createApiToken(serverIndex, "Full Access", "graph", 0, permissions);

      // Use token to query
      final HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/query/graph/sql/select%201%20as%20value").openConnection();
      connection.setRequestMethod("GET");
      connection.setRequestProperty("Authorization", "Bearer " + tokenValue);
      connection.connect();

      try {
        assertThat(connection.getResponseCode()).isEqualTo(200);
        final JSONObject response = new JSONObject(readResponse(connection));
        assertThat(response.has("result")).isTrue();
      } finally {
        connection.disconnect();
      }
    });
  }

  @Test
  void expiredTokenReturns401() throws Exception {
    testEachServer((serverIndex) -> {
      final long pastTime = System.currentTimeMillis() - 10000;
      final String tokenValue = createApiToken(serverIndex, "Expired", "graph", pastTime, new JSONObject());

      final HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/query/graph/sql/select%201").openConnection();
      connection.setRequestMethod("GET");
      connection.setRequestProperty("Authorization", "Bearer " + tokenValue);
      connection.connect();

      try {
        assertThat(connection.getResponseCode()).isEqualTo(401);
      } finally {
        connection.disconnect();
      }
    });
  }

  @Test
  void readOnlyTokenCannotInsert() throws Exception {
    testEachServer((serverIndex) -> {
      final JSONObject permissions = new JSONObject()
          .put("types", new JSONObject()
              .put("*", new JSONObject().put("access", new JSONArray().put("readRecord"))))
          .put("database", new JSONArray());

      final String tokenValue = createApiToken(serverIndex, "Read Only", "graph", 0, permissions);

      // Try to insert with read-only token
      final HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/command/graph").openConnection();
      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization", "Bearer " + tokenValue);
      connection.setDoOutput(true);
      connection.setRequestProperty("Content-Type", "application/json");

      final JSONObject payload = new JSONObject();
      payload.put("language", "sql");
      payload.put("command", "INSERT INTO V1 SET id = 9999, name = 'test'");
      connection.getOutputStream().write(payload.toString().getBytes());
      connection.connect();

      try {
        assertThat(connection.getResponseCode()).isEqualTo(403);
      } finally {
        connection.disconnect();
      }
    });
  }

  @Test
  void deleteTokenViaApi() throws Exception {
    testEachServer((serverIndex) -> {
      final String tokenValue = createApiToken(serverIndex, "ToDelete", "graph", 0, new JSONObject());
      final String tokenHash = ApiTokenConfiguration.hashToken(tokenValue);

      // Delete using token hash (not plaintext)
      final HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/server/api-tokens?token=" +
              URLEncoder.encode(tokenHash, "UTF-8")).openConnection();
      connection.setRequestMethod("DELETE");
      connection.setRequestProperty("Authorization", basicAuth());
      connection.connect();

      try {
        assertThat(connection.getResponseCode()).isEqualTo(200);
      } finally {
        connection.disconnect();
      }

      // Verify token no longer works
      final HttpURLConnection connection2 = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/query/graph/sql/select%201").openConnection();
      connection2.setRequestMethod("GET");
      connection2.setRequestProperty("Authorization", "Bearer " + tokenValue);
      connection2.connect();

      try {
        assertThat(connection2.getResponseCode()).isEqualTo(401);
      } finally {
        connection2.disconnect();
      }
    });
  }

  @Test
  void deleteTokenRejectsPlaintext() throws Exception {
    testEachServer((serverIndex) -> {
      final String tokenValue = createApiToken(serverIndex, "NoPlaintext", "graph", 0, new JSONObject());

      // Try to delete using plaintext token — should be rejected
      final HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/server/api-tokens?token=" +
              URLEncoder.encode(tokenValue, "UTF-8")).openConnection();
      connection.setRequestMethod("DELETE");
      connection.setRequestProperty("Authorization", basicAuth());
      connection.connect();

      try {
        assertThat(connection.getResponseCode()).isEqualTo(400);
      } finally {
        connection.disconnect();
      }
    });
  }

  @Test
  void deleteTokenByHash() throws Exception {
    testEachServer((serverIndex) -> {
      final String tokenValue = createApiToken(serverIndex, "ToDeleteByHash", "graph", 0, new JSONObject());
      final String tokenHash = ApiTokenConfiguration.hashToken(tokenValue);

      final HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/server/api-tokens?token=" +
              URLEncoder.encode(tokenHash, "UTF-8")).openConnection();
      connection.setRequestMethod("DELETE");
      connection.setRequestProperty("Authorization", basicAuth());
      connection.connect();

      try {
        assertThat(connection.getResponseCode()).isEqualTo(200);
      } finally {
        connection.disconnect();
      }

      // Verify token no longer works
      final HttpURLConnection connection2 = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/query/graph/sql/select%201").openConnection();
      connection2.setRequestMethod("GET");
      connection2.setRequestProperty("Authorization", "Bearer " + tokenValue);
      connection2.connect();

      try {
        assertThat(connection2.getResponseCode()).isEqualTo(401);
      } finally {
        connection2.disconnect();
      }
    });
  }

  @Test
  void plaintextNotPersistedOnDisk() throws Exception {
    testEachServer((serverIndex) -> {
      final String tokenValue = createApiToken(serverIndex, "PersistTest", "graph", 0, new JSONObject());

      // Read the token file and verify no plaintext token is stored
      final String configPath = getServer(serverIndex).getRootPath() + "/config";
      final File tokenFile = new File(configPath, "server-api-tokens.json");
      assertThat(tokenFile.exists()).isTrue();

      final String content = new String(Files.readAllBytes(tokenFile.toPath()));
      assertThat(content).doesNotContain(tokenValue);
      assertThat(content).contains("tokenHash");
      assertThat(content).contains("tokenSuffix");
      assertThat(content).doesNotContain("\"token\"");
    });
  }

  @Test
  void nonRootCannotManageTokens() throws Exception {
    testEachServer((serverIndex) -> {
      // Create a non-root user first (if not already existing)
      if (!getServer(serverIndex).getSecurity().existsUser("testuser"))
        getServer(serverIndex).getSecurity().createUser("testuser", "testpass");

      final String nonRootAuth = "Basic " + Base64.getEncoder()
          .encodeToString("testuser:testpass".getBytes());

      final HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/server/api-tokens").openConnection();
      connection.setRequestMethod("GET");
      connection.setRequestProperty("Authorization", nonRootAuth);
      connection.connect();

      try {
        assertThat(connection.getResponseCode()).isEqualTo(403);
      } finally {
        connection.disconnect();
      }
    });
  }

  @Test
  void wildcardTypePermissions() throws Exception {
    testEachServer((serverIndex) -> {
      // Token with * type having readRecord only, but Account having full CRUD
      final JSONObject permissions = new JSONObject()
          .put("types", new JSONObject()
              .put("*", new JSONObject().put("access", new JSONArray().put("readRecord")))
              .put("V", new JSONObject().put("access",
                  new JSONArray().put("createRecord").put("readRecord").put("updateRecord").put("deleteRecord"))))
          .put("database", new JSONArray());

      final String tokenValue = createApiToken(serverIndex, "Mixed Perms", "graph", 0, permissions);

      // Should be able to read
      final HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/query/graph/sql/select%201%20as%20value").openConnection();
      connection.setRequestMethod("GET");
      connection.setRequestProperty("Authorization", "Bearer " + tokenValue);
      connection.connect();

      try {
        assertThat(connection.getResponseCode()).isEqualTo(200);
      } finally {
        connection.disconnect();
      }
    });
  }

  @Test
  void duplicateTokenNameReturns409() throws Exception {
    testEachServer((serverIndex) -> {
      createApiToken(serverIndex, "Unique Name", "graph", 0, new JSONObject());

      // Second token with the same name should return 409
      final HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/server/api-tokens").openConnection();
      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization", basicAuth());
      connection.setDoOutput(true);
      connection.setRequestProperty("Content-Type", "application/json");

      final JSONObject payload = new JSONObject();
      payload.put("name", "Unique Name");
      payload.put("database", "graph");
      payload.put("expiresAt", 0);
      payload.put("permissions", new JSONObject());

      connection.getOutputStream().write(payload.toString().getBytes());
      connection.connect();

      try {
        assertThat(connection.getResponseCode()).isEqualTo(409);
      } finally {
        connection.disconnect();
      }
    });
  }

  @Test
  void apiTokenInvalidReturns401() throws Exception {
    testEachServer((serverIndex) -> {
      final HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/query/graph/sql/select%201").openConnection();
      connection.setRequestMethod("GET");
      connection.setRequestProperty("Authorization", "Bearer at-invalid-nonexistent-token");
      connection.connect();

      try {
        assertThat(connection.getResponseCode()).isEqualTo(401);
      } finally {
        connection.disconnect();
      }
    });
  }

  private String createApiToken(final int serverIndex, final String name, final String database, final long expiresAt,
      final JSONObject permissions) throws Exception {
    // Delete any pre-existing token with the same name (e.g., left over from a previous test run)
    final ApiTokenConfiguration tokenConfig =
        getServer(serverIndex).getSecurity().getApiTokenConfiguration();
    tokenConfig.listTokens().stream()
        .filter(t -> name.equals(t.getString("name", "")))
        .forEach(t -> tokenConfig.deleteToken(t.getString("tokenHash")));

    final HttpURLConnection connection = (HttpURLConnection) new URL(
        "http://127.0.0.1:248" + serverIndex + "/api/v1/server/api-tokens").openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization", basicAuth());
    connection.setDoOutput(true);
    connection.setRequestProperty("Content-Type", "application/json");

    final JSONObject payload = new JSONObject();
    payload.put("name", name);
    payload.put("database", database);
    payload.put("expiresAt", expiresAt);
    payload.put("permissions", permissions);

    connection.getOutputStream().write(payload.toString().getBytes());
    connection.connect();

    try {
      assertThat(connection.getResponseCode()).isEqualTo(201);
      final JSONObject response = new JSONObject(readResponse(connection));
      return response.getJSONObject("result").getString("token");
    } finally {
      connection.disconnect();
    }
  }

  private String basicAuth() {
    return "Basic " + Base64.getEncoder()
        .encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes());
  }
}
