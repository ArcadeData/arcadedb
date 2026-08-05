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
 * Security report (ArcadeData/arcadedb-operations#654, email #650): {@code EXPORT DATABASE} and {@code BACKUP DATABASE}
 * carry no per-statement authorization check and, unlike the other DDL statements flagged in the same report, do not
 * route through any gated schema/record path either. Both read the entire database - bypassing the per-type read grants
 * the identity actually holds - and write a file to the server filesystem. Their sibling whole-database operation
 * {@code IMPORT DATABASE} already requires {@code UPDATE_SECURITY} ({@code ImportDatabaseStatement.java:55}), so a
 * read-only identity being able to dump the whole database is an authorization inconsistency (CRITICAL: data
 * exfiltration).
 *
 * <p>A read-only API token (no {@code updateSecurity}) must be rejected with HTTP 403 on EXPORT and BACKUP, while an
 * administrator must still be able to perform them.</p>
 */
class ExportBackupAuthorizationIT extends BaseGraphServerTest {

  @Test
  void readOnlyTokenCannotExportOrBackup() throws Exception {
    testEachServer(serverIndex -> {
      // ARRANGE (as admin): a type with a record, so the export/backup have real data to exfiltrate
      assertThat(adminCommand(serverIndex, "CREATE DOCUMENT TYPE Secret")).isEqualTo(200);
      assertThat(adminCommand(serverIndex, "INSERT INTO Secret SET value = 'top-secret'")).isEqualTo(200);

      final String token = "Bearer " + createReadOnlyToken(serverIndex, "export-backup-token");
      try {
        assertThat(command(serverIndex, token, "EXPORT DATABASE file://probe-export.jsonl.tgz WITH `overwrite` = true"))
            .as("read-only token must not EXPORT the whole database").isEqualTo(403);
        assertThat(command(serverIndex, token, "BACKUP DATABASE"))
            .as("read-only token must not BACKUP the whole database").isEqualTo(403);
      } finally {
        deleteToken(serverIndex, "export-backup-token");
      }
    });
  }

  @Test
  void adminCanStillExportAndBackup() throws Exception {
    testEachServer(serverIndex -> {
      assertThat(adminCommand(serverIndex, "CREATE DOCUMENT TYPE Doc")).isEqualTo(200);
      assertThat(adminCommand(serverIndex, "INSERT INTO Doc SET title = 'hello'")).isEqualTo(200);

      // Positive controls: an administrator (root) must not be blocked by the new checks
      assertThat(adminCommand(serverIndex, "EXPORT DATABASE file://admin-export.jsonl.tgz WITH `overwrite` = true")).isEqualTo(200);
      assertThat(adminCommand(serverIndex, "BACKUP DATABASE")).isEqualTo(200);
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

  private String createReadOnlyToken(final int serverIndex, final String name) throws Exception {
    final ApiTokenConfiguration tokenConfig = getServer(serverIndex).getSecurity().getApiTokenConfiguration();
    tokenConfig.listTokens().stream()
        .filter(t -> name.equals(t.getString("name", "")))
        .forEach(t -> tokenConfig.deleteToken(t.getString("tokenHash")));

    final JSONObject permissions = new JSONObject()
        .put("types", new JSONObject().put("*", new JSONObject().put("access", new JSONArray().put("readRecord"))))
        .put("database", new JSONArray()); // no updateSecurity

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
    final HttpURLConnection connection = (HttpURLConnection) new URL("http://127.0.0.1:248" + serverIndex + path).openConnection();
    connection.setRequestMethod(method);
    connection.setRequestProperty("Authorization", auth);
    return connection;
  }

  private String basicAuth() {
    return "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes());
  }
}
