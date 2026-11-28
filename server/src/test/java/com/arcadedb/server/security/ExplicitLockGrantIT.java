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
package com.arcadedb.server.security;

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code LOCK TYPE} and {@code LOCK BUCKET} take the commit locks of the files a write to the type touches: a user is asked for a
 * create, an update or a delete grant on the type, over HTTP as in process. A read-only token is refused (403) and the lock the
 * request would have held is never taken, while a token that may write, and the administrator, still lock.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class ExplicitLockGrantIT extends BaseGraphServerTest {

  @Test
  void aTokenWithReadGrantsOnlyIsRefusedTheLock() throws Exception {
    testEachServer(serverIndex -> {
      assertThat(adminCommand(serverIndex, "CREATE DOCUMENT TYPE Registry")).isEqualTo(200);
      final String bucketName = getServerDatabase(serverIndex, getDatabaseName()).getSchema().getType("Registry").getBuckets(false)
          .getFirst().getName();

      final String token = "Bearer " + createToken(serverIndex, "lock-read-token", "readRecord");
      try {
        assertThat(command(serverIndex, token, "LOCK TYPE Registry")).as("read-only token must not LOCK TYPE").isEqualTo(403);
        assertThat(command(serverIndex, token, "LOCK BUCKET " + bucketName)).as("read-only token must not LOCK BUCKET").isEqualTo(403);
      } finally {
        deleteToken(serverIndex, "lock-read-token");
      }

      // Positive control: the administrator locks the same type and bucket
      assertThat(adminCommand(serverIndex, "LOCK TYPE Registry")).isEqualTo(200);
      assertThat(adminCommand(serverIndex, "LOCK BUCKET " + bucketName)).isEqualTo(200);
    });
  }

  @Test
  void aTokenThatMayWriteStillLocks() throws Exception {
    testEachServer(serverIndex -> {
      assertThat(adminCommand(serverIndex, "CREATE DOCUMENT TYPE Journal")).isEqualTo(200);
      final String bucketName = getServerDatabase(serverIndex, getDatabaseName()).getSchema().getType("Journal").getBuckets(false)
          .getFirst().getName();

      for (final String write : new String[] { "createRecord", "updateRecord", "deleteRecord" }) {
        final String name = "lock-" + write + "-token";
        final String token = "Bearer " + createToken(serverIndex, name, "readRecord", write);
        try {
          assertThat(command(serverIndex, token, "LOCK TYPE Journal")).as("LOCK TYPE with %s", write).isEqualTo(200);
          assertThat(command(serverIndex, token, "LOCK BUCKET " + bucketName)).as("LOCK BUCKET with %s", write).isEqualTo(200);
        } finally {
          deleteToken(serverIndex, name);
        }
      }
    });
  }

  private int adminCommand(final int serverIndex, final String sql) throws Exception {
    return command(serverIndex, basicAuth(), sql);
  }

  private int command(final int serverIndex, final String auth, final String sql) throws Exception {
    final HttpURLConnection connection = open(serverIndex, "/api/v1/command/" + getDatabaseName(), "POST", auth);
    connection.setDoOutput(true);
    connection.setRequestProperty("Content-Type", "application/json");
    final JSONObject payload = new JSONObject().put("language", "sql").put("command", sql);
    connection.getOutputStream().write(payload.toString().getBytes());
    connection.connect();
    try {
      return connection.getResponseCode();
    } finally {
      connection.disconnect();
    }
  }

  private String createToken(final int serverIndex, final String name, final String... access) throws Exception {
    deleteToken(serverIndex, name);

    final JSONArray granted = new JSONArray();
    for (final String grant : access)
      granted.put(grant);
    final JSONObject permissions = new JSONObject()
        .put("types", new JSONObject().put("*", new JSONObject().put("access", granted)))
        .put("database", new JSONArray());

    final HttpURLConnection connection = open(serverIndex, "/api/v1/server/api-tokens", "POST", basicAuth());
    connection.setDoOutput(true);
    connection.setRequestProperty("Content-Type", "application/json");
    final JSONObject payload = new JSONObject()
        .put("name", name)
        .put("database", getDatabaseName())
        .put("expiresAt", 0)
        .put("permissions", permissions);
    connection.getOutputStream().write(payload.toString().getBytes());
    connection.connect();
    try {
      assertThat(connection.getResponseCode()).isEqualTo(201);
      return new JSONObject(readResponse(connection)).getJSONObject("result").getString("token");
    } finally {
      connection.disconnect();
    }
  }

  private void deleteToken(final int serverIndex, final String name) {
    try {
      final ApiTokenConfiguration tokenConfig = getServer(serverIndex).getSecurity().getApiTokenConfiguration();
      tokenConfig.listTokens().stream()
          .filter(t -> name.equals(t.getString("name", "")))
          .forEach(t -> tokenConfig.deleteToken(t.getString("tokenHash")));
    } catch (final Exception ignore) {
    }
  }

  private HttpURLConnection open(final int serverIndex, final String path, final String method, final String auth)
      throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URL("http://127.0.0.1:" + getServerHttpPort(serverIndex) + path).openConnection();
    connection.setRequestMethod(method);
    connection.setRequestProperty("Authorization", auth);
    return connection;
  }

  private String basicAuth() {
    return "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes());
  }
}
