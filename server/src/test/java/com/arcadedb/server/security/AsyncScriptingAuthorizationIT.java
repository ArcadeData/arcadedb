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
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for GHSA-5j4x-3jfw-8xv3: a user with only read-level privileges could escalate to server-wide
 * administrator by submitting an asynchronous {@code language: "js"} command over HTTP ({@code awaitResponse:false}).
 * The async command runs on an {@code arcadedb-async-*} worker thread whose {@link com.arcadedb.database.DatabaseContext}
 * never had the authenticated principal bound, so the polyglot scripting gate (a documented no-op when no user is bound)
 * silently passed and the script could call {@code database.getSecurity().createUser(...)} to mint an all-databases admin.
 * <p>
 * The fix binds the submitting principal onto the dispatched async task so the engine per-user gate enforces on the
 * worker thread exactly as it does on the synchronous HTTP, Postgres and Bolt paths.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class AsyncScriptingAuthorizationIT extends BaseGraphServerTest {

  private static final String READER_USER = "async-scripting-reader";
  private static final String READER_PWD  = "readerpass1";

  // The published proof-of-concept: escalate to a server-wide admin from a JS script.
  private static final String PWN_USER    = "pwn5j4x";
  private static final String PoC_SCRIPT  = "database.getSecurity().createUser('" + PWN_USER + "', 'pwn'); true";

  @Test
  void readerCannotEscalateViaAsyncJs() throws Exception {
    testEachServer((serverIndex) -> {
      createReaderUser(serverIndex);
      final ServerSecurity security = getServer(serverIndex).getSecurity();
      try {
        final String readerAuth = basicAuth(READER_USER, READER_PWD);

        // The attack: submit the escalation script asynchronously. The HTTP layer accepts it (202) because the
        // command is only queued; the authorization decision happens later on the worker thread.
        assertThat(command(serverIndex, readerAuth, "js", PoC_SCRIPT, false))
            .as("async command is accepted for scheduling").isEqualTo(202);

        // Drain the async pipeline so the worker thread has certainly attempted the script.
        getServer(serverIndex).getDatabase(getDatabaseName()).async().waitCompletion();

        // The escalation must NOT have happened: the worker-thread principal binding makes the polyglot gate reject it.
        assertThat(security.existsUser(PWN_USER))
            .as("reader must NOT be able to create a server admin via async js").isFalse();

        // Positive control: root submitting the same async js path DOES create a user, proving the async js pipeline is
        // live and would have created 'pwn' for the reader had the gate not enforced.
        assertThat(command(serverIndex, basicAuth("root", DEFAULT_PASSWORD_FOR_TESTS), "js",
            "database.getSecurity().createUser('admin5j4x', 'pwn'); true", false))
            .as("admin async command accepted").isEqualTo(202);
        getServer(serverIndex).getDatabase(getDatabaseName()).async().waitCompletion();
        assertThat(security.existsUser("admin5j4x"))
            .as("admin async js path is live and creates users").isTrue();
      } finally {
        if (security.existsUser(PWN_USER))
          security.dropUser(PWN_USER);
        if (security.existsUser("admin5j4x"))
          security.dropUser("admin5j4x");
        deleteUser(serverIndex, READER_USER);
      }
    });
  }

  @Test
  void readerCannotWriteViaAsyncSql() throws Exception {
    testEachServer((serverIndex) -> {
      createReaderUser(serverIndex);
      final ServerSecurity security = getServer(serverIndex).getSecurity();
      final String marker = "async5j4x-sql-marker";
      try {
        final String readerAuth = basicAuth(READER_USER, READER_PWD);

        // Control: the read-only user must be blocked on the SYNCHRONOUS write path (record UPDATE permission gate).
        assertThat(command(serverIndex, readerAuth, "sql", "INSERT INTO " + VERTEX1_TYPE_NAME + " SET tag = '" + marker + "'", true))
            .as("reader must be denied a synchronous write").isEqualTo(403);

        // The attack: the same write submitted asynchronously must NOT bypass the gate on the worker thread.
        assertThat(command(serverIndex, readerAuth, "sql",
            "INSERT INTO " + VERTEX1_TYPE_NAME + " SET tag = '" + marker + "'", false))
            .as("async command is accepted for scheduling").isEqualTo(202);
        getServer(serverIndex).getDatabase(getDatabaseName()).async().waitCompletion();

        assertThat(countMarker(serverIndex, marker))
            .as("reader must NOT be able to insert a record via async sql").isZero();
      } finally {
        // Remove any marker records that may have leaked, so the shared server database stays clean.
        getServer(serverIndex).getDatabase(getDatabaseName())
            .command("sql", "DELETE FROM " + VERTEX1_TYPE_NAME + " WHERE tag = ?", marker);
        deleteUser(serverIndex, READER_USER);
      }
    });
  }

  private long countMarker(final int serverIndex, final String marker) {
    return getServer(serverIndex).getDatabase(getDatabaseName())
        .query("sql", "SELECT count(*) AS c FROM " + VERTEX1_TYPE_NAME + " WHERE tag = ?", marker)
        .next().<Number>getProperty("c").longValue();
  }

  private void createReaderUser(final int serverIndex) throws Exception {
    final ServerSecurity security = getServer(serverIndex).getSecurity();

    // A group that grants record reads but no database-level (admin) permission.
    security.getDatabaseGroupsConfiguration(getDatabaseName()).put("asyncScriptReader",
        new JSONObject().put("access", new JSONArray()).put("types",
            new JSONObject().put("*", new JSONObject().put("access", new JSONArray().put("readRecord")))));
    security.saveGroups();

    if (security.existsUser(READER_USER))
      security.dropUser(READER_USER);

    final JSONObject payload = new JSONObject()
        .put("name", READER_USER)
        .put("password", READER_PWD)
        .put("databases", new JSONObject().put(getDatabaseName(), new JSONArray().put("asyncScriptReader")));

    final HttpURLConnection connection = open(serverIndex, "/api/v1/server/users", basicAuth("root", DEFAULT_PASSWORD_FOR_TESTS));
    connection.setDoOutput(true);
    connection.setRequestProperty("Content-Type", "application/json");
    connection.getOutputStream().write(payload.toString().getBytes());
    connection.connect();
    try {
      assertThat(connection.getResponseCode()).isEqualTo(201);
    } finally {
      connection.disconnect();
    }
  }

  private void deleteUser(final int serverIndex, final String name) throws Exception {
    final HttpURLConnection connection = open(serverIndex,
        "/api/v1/server/users?name=" + URLEncoder.encode(name, StandardCharsets.UTF_8), basicAuth("root", DEFAULT_PASSWORD_FOR_TESTS));
    connection.setRequestMethod("DELETE");
    connection.connect();
    try {
      connection.getResponseCode();
    } finally {
      connection.disconnect();
    }
  }

  private int command(final int serverIndex, final String auth, final String language, final String script,
      final boolean awaitResponse) throws Exception {
    final HttpURLConnection connection = open(serverIndex, "/api/v1/command/" + getDatabaseName(), auth);
    connection.setDoOutput(true);
    connection.setRequestProperty("Content-Type", "application/json");
    final JSONObject payload = new JSONObject().put("language", language).put("command", script)
        .put("awaitResponse", awaitResponse);
    connection.getOutputStream().write(payload.toString().getBytes());
    connection.connect();
    try {
      return connection.getResponseCode();
    } finally {
      connection.disconnect();
    }
  }

  private HttpURLConnection open(final int serverIndex, final String path, final String auth) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URL("http://127.0.0.1:248" + serverIndex + path).openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization", auth);
    return connection;
  }

  private String basicAuth(final String user, final String password) {
    return "Basic " + Base64.getEncoder().encodeToString((user + ":" + password).getBytes());
  }
}
