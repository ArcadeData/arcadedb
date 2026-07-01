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
 * Regression test for GHSA-48qw-824m-86pr: a user with only read-level privileges on a database could execute arbitrary
 * scripts through the {@code language: "js"} option of {@code /api/v1/command/{database}}, escaping the database scope and
 * reading host files. Polyglot script execution must now require database-administrator privileges, so a non-admin user is
 * rejected with HTTP 403 while still being able to run ordinary read-only SQL, and an administrator is unaffected.
 */
class PolyglotScriptingAuthorizationIT extends BaseGraphServerTest {

  private static final String READER_USER = "scripting-reader";
  private static final String READER_PWD  = "readerpass1";

  // The published proof-of-concept: walk from the bound database object to the class loader and read a host file.
  private static final String PoC_SCRIPT =
      "var cl=database.getClass().getClassLoader();var F=cl.loadClass('java.io.File');F.getName();";

  @Test
  void readerCannotExecuteScript() throws Exception {
    testEachServer((serverIndex) -> {
      createReaderUser(serverIndex);
      try {
        final String readerAuth = basicAuth(READER_USER, READER_PWD);

        // Control: the reader account is live and authenticates fine on the ordinary read-only SQL path.
        assertThat(command(serverIndex, readerAuth, "sql", "SELECT FROM schema:types"))
            .as("reader must be able to run read-only SQL").isEqualTo(200);

        // The scripting privilege-escalation path must be denied.
        assertThat(command(serverIndex, readerAuth, "js", PoC_SCRIPT))
            .as("reader must not execute the proof-of-concept js script").isEqualTo(403);
        assertThat(command(serverIndex, readerAuth, "js", "1 + 1"))
            .as("reader must not execute any js, even a harmless expression").isEqualTo(403);
      } finally {
        deleteUser(serverIndex, READER_USER);
      }
    });
  }

  @Test
  void adminCanStillExecuteScript() throws Exception {
    testEachServer((serverIndex) -> {
      // Positive control: an administrator (root) must not be blocked by the new check.
      assertThat(command(serverIndex, basicAuth("root", DEFAULT_PASSWORD_FOR_TESTS), "js", "1 + 1"))
          .as("admin must still execute js").isEqualTo(200);
    });
  }

  private void createReaderUser(final int serverIndex) throws Exception {
    final ServerSecurity security = getServer(serverIndex).getSecurity();

    // A group that grants record reads but no database-level (admin) permission.
    security.getDatabaseGroupsConfiguration(getDatabaseName()).put("scriptReader",
        new JSONObject().put("access", new JSONArray()).put("types",
            new JSONObject().put("*", new JSONObject().put("access", new JSONArray().put("readRecord")))));
    security.saveGroups();

    if (security.existsUser(READER_USER))
      security.dropUser(READER_USER);

    final JSONObject payload = new JSONObject()
        .put("name", READER_USER)
        .put("password", READER_PWD)
        .put("databases", new JSONObject().put(getDatabaseName(), new JSONArray().put("scriptReader")));

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

  private int command(final int serverIndex, final String auth, final String language, final String script) throws Exception {
    final HttpURLConnection connection = open(serverIndex, "/api/v1/command/" + getDatabaseName(), auth);
    connection.setDoOutput(true);
    connection.setRequestProperty("Content-Type", "application/json");
    final JSONObject payload = new JSONObject().put("language", language).put("command", script);
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
