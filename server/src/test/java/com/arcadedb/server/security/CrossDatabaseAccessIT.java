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
 * Regression test for the authorization bypass where an authenticated user or API token scoped to a single database could
 * still read/write records on any other database on the same server.
 *
 * Root cause: {@code ServerSecurityUser.getDatabaseUser()} used to return a database user with an uninitialized
 * {@code fileAccessMap} when the database was not in the user's allowed list. The null map was then interpreted as
 * allow-all in {@code ServerSecurityDatabaseUser.requestAccessOnFile()}, silently bypassing per-database scoping.
 */
class CrossDatabaseAccessIT extends BaseGraphServerTest {

  private static final String OTHER_DB = "otherdb";

  @Test
  void scopedUserCannotReadOtherDatabase() throws Exception {
    testEachServer((serverIndex) -> {
      createDatabase(serverIndex, OTHER_DB);
      try {
        createType(serverIndex, OTHER_DB, "Memory");
        final String userAuth = createScopedUser(serverIndex, "scoped-reader", "secret-pwd-1");

        final int status = commandStatus(serverIndex, OTHER_DB, userAuth, "SELECT FROM Memory");
        assertThat(status).as("scoped user must not SELECT on an unrelated database").isGreaterThanOrEqualTo(400);
      } finally {
        dropUser(serverIndex, "scoped-reader");
        dropDatabase(serverIndex, OTHER_DB);
      }
    });
  }

  @Test
  void scopedUserCannotWriteOtherDatabase() throws Exception {
    testEachServer((serverIndex) -> {
      createDatabase(serverIndex, OTHER_DB);
      try {
        createType(serverIndex, OTHER_DB, "Memory");
        final String userAuth = createScopedUser(serverIndex, "scoped-writer", "secret-pwd-2");

        final int status = commandStatus(serverIndex, OTHER_DB, userAuth,
            "INSERT INTO Memory SET leaked = true");
        assertThat(status).as("scoped user must not INSERT on an unrelated database").isGreaterThanOrEqualTo(400);
      } finally {
        dropUser(serverIndex, "scoped-writer");
        dropDatabase(serverIndex, OTHER_DB);
      }
    });
  }

  @Test
  void scopedApiTokenCannotReadOtherDatabase() throws Exception {
    testEachServer((serverIndex) -> {
      createDatabase(serverIndex, OTHER_DB);
      try {
        createType(serverIndex, OTHER_DB, "Memory");
        final String tokenAuth = "Bearer " + createReadOnlyToken(serverIndex, "scoped-token-read");

        final int status = commandStatus(serverIndex, OTHER_DB, tokenAuth, "SELECT FROM Memory");
        assertThat(status).as("scoped API token must not SELECT on an unrelated database").isGreaterThanOrEqualTo(400);
      } finally {
        deleteToken(serverIndex, "scoped-token-read");
        dropDatabase(serverIndex, OTHER_DB);
      }
    });
  }

  @Test
  void readOnlyApiTokenCannotInsertOnOwnDatabase() throws Exception {
    // Mirrors the exact repro sent by the reporter (Art): a read-only token scoped to 'live' must not be able to
    // INSERT INTO Memory on the very same 'live' database. Token payload uses no 'database' key inside permissions,
    // matching the reporter's minimised token-only repro.
    testEachServer((serverIndex) -> {
      createDatabase(serverIndex, OTHER_DB);
      try {
        createType(serverIndex, OTHER_DB, "Memory");

        final JSONObject permissions = new JSONObject()
            .put("types", new JSONObject()
                .put("*", new JSONObject().put("access", new JSONArray().put("readRecord"))));
        final String token = createTokenForDatabase(serverIndex, "art-readonly-token", OTHER_DB, permissions);
        final String tokenAuth = "Bearer " + token;

        final int readStatus = commandStatus(serverIndex, OTHER_DB, tokenAuth, "SELECT FROM Memory");
        assertThat(readStatus).as("read-only token must be allowed to SELECT on its own database").isEqualTo(200);

        final int insertStatus = commandStatus(serverIndex, OTHER_DB, tokenAuth,
            "INSERT INTO Memory SET content = 'token-write-attempt', salience = 0.2");
        assertThat(insertStatus).as("read-only token must not INSERT on its own database").isGreaterThanOrEqualTo(400);

        final int schemaStatus = commandStatus(serverIndex, OTHER_DB, tokenAuth,
            "CREATE PROPERTY Memory.scope_probe_api_token STRING");
        assertThat(schemaStatus).as("read-only token must not mutate schema on its own database").isGreaterThanOrEqualTo(400);
      } finally {
        deleteToken(serverIndex, "art-readonly-token");
        dropDatabase(serverIndex, OTHER_DB);
      }
    });
  }

  @Test
  void scopedApiTokenCannotWriteOtherDatabase() throws Exception {
    testEachServer((serverIndex) -> {
      createDatabase(serverIndex, OTHER_DB);
      try {
        createType(serverIndex, OTHER_DB, "Memory");
        final String tokenAuth = "Bearer " + createCrudToken(serverIndex, "scoped-token-crud");

        final int status = commandStatus(serverIndex, OTHER_DB, tokenAuth,
            "INSERT INTO Memory SET leaked = true");
        assertThat(status).as("scoped API token must not INSERT on an unrelated database").isGreaterThanOrEqualTo(400);
      } finally {
        deleteToken(serverIndex, "scoped-token-crud");
        dropDatabase(serverIndex, OTHER_DB);
      }
    });
  }

  private String createScopedUser(final int serverIndex, final String name, final String password) throws Exception {
    if (getServer(serverIndex).getSecurity().existsUser(name))
      getServer(serverIndex).getSecurity().dropUser(name);

    final JSONObject payload = new JSONObject();
    payload.put("name", name);
    payload.put("password", password);
    payload.put("databases", new JSONObject().put(getDatabaseName(), new JSONArray().put("admin")));

    final HttpURLConnection connection = open(serverIndex, "/api/v1/server/users", "POST", basicAuth());
    connection.setDoOutput(true);
    connection.setRequestProperty("Content-Type", "application/json");
    connection.getOutputStream().write(payload.toString().getBytes());
    connection.connect();
    try {
      assertThat(connection.getResponseCode()).isEqualTo(201);
    } finally {
      connection.disconnect();
    }
    return "Basic " + Base64.getEncoder().encodeToString((name + ":" + password).getBytes());
  }

  private void dropUser(final int serverIndex, final String name) {
    try {
      if (getServer(serverIndex) != null && getServer(serverIndex).getSecurity().existsUser(name))
        getServer(serverIndex).getSecurity().dropUser(name);
    } catch (final Exception ignore) {
    }
  }

  private String createReadOnlyToken(final int serverIndex, final String name) throws Exception {
    final JSONObject permissions = new JSONObject()
        .put("types", new JSONObject()
            .put("*", new JSONObject().put("access", new JSONArray().put("readRecord"))))
        .put("database", new JSONArray());
    return createToken(serverIndex, name, permissions);
  }

  private String createCrudToken(final int serverIndex, final String name) throws Exception {
    final JSONObject permissions = new JSONObject()
        .put("types", new JSONObject()
            .put("*", new JSONObject().put("access",
                new JSONArray().put("createRecord").put("readRecord").put("updateRecord").put("deleteRecord"))))
        .put("database", new JSONArray());
    return createToken(serverIndex, name, permissions);
  }

  private String createToken(final int serverIndex, final String name, final JSONObject permissions) throws Exception {
    return createTokenForDatabase(serverIndex, name, getDatabaseName(), permissions);
  }

  private String createTokenForDatabase(final int serverIndex, final String name, final String database,
      final JSONObject permissions) throws Exception {
    final ApiTokenConfiguration tokenConfig = getServer(serverIndex).getSecurity().getApiTokenConfiguration();
    tokenConfig.listTokens().stream()
        .filter(t -> name.equals(t.getString("name", "")))
        .forEach(t -> tokenConfig.deleteToken(t.getString("tokenHash")));

    final HttpURLConnection connection = open(serverIndex, "/api/v1/server/api-tokens", "POST", basicAuth());
    connection.setDoOutput(true);
    connection.setRequestProperty("Content-Type", "application/json");

    final JSONObject payload = new JSONObject();
    payload.put("name", name);
    payload.put("database", database);
    payload.put("expiresAt", 0);
    payload.put("permissions", permissions);
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

  private void createDatabase(final int serverIndex, final String database) throws Exception {
    final HttpURLConnection connection = open(serverIndex, "/api/v1/server", "POST", basicAuth());
    connection.setDoOutput(true);
    connection.setRequestProperty("Content-Type", "application/json");
    connection.getOutputStream().write(new JSONObject().put("command", "create database " + database).toString().getBytes());
    connection.connect();
    try {
      assertThat(connection.getResponseCode()).isEqualTo(200);
    } finally {
      connection.disconnect();
    }
  }

  private void dropDatabase(final int serverIndex, final String database) {
    try {
      final HttpURLConnection connection = open(serverIndex, "/api/v1/server", "POST", basicAuth());
      connection.setDoOutput(true);
      connection.setRequestProperty("Content-Type", "application/json");
      connection.getOutputStream().write(new JSONObject().put("command", "drop database " + database).toString().getBytes());
      connection.connect();
      try {
        connection.getResponseCode();
      } finally {
        connection.disconnect();
      }
    } catch (final Exception ignore) {
    }
  }

  private void createType(final int serverIndex, final String database, final String typeName) throws Exception {
    final int status = commandStatus(serverIndex, database, basicAuth(), "CREATE DOCUMENT TYPE " + typeName);
    assertThat(status).isEqualTo(200);
  }

  private int commandStatus(final int serverIndex, final String database, final String auth, final String sql) throws Exception {
    final HttpURLConnection connection = open(serverIndex, "/api/v1/command/" + database, "POST", auth);
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
