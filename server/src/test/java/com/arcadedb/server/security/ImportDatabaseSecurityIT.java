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
 * Regression test for the SSRF (CWE-918) / arbitrary local file read (CWE-22) report on {@code IMPORT DATABASE}.
 *
 * <p>The core fix is a privilege gate: {@code IMPORT DATABASE} requires the administrative {@code updateSecurity}
 * permission, so a low-privilege authenticated SQL user can no longer trigger imports (and therefore neither SSRF nor
 * arbitrary file read). This test reproduces the reporter's scenario: a non-admin user attempting to import a
 * {@code file://} path and a link-local SSRF URL must be rejected with HTTP 403.</p>
 */
class ImportDatabaseSecurityIT extends BaseGraphServerTest {

  @Test
  void nonAdminUserCannotImportLocalFile() throws Exception {
    testEachServer(serverIndex -> {
      final String userAuth = createNonAdminUser(serverIndex, "pwn", "pwn123secret");
      try {
        final int status = commandStatus(serverIndex, getDatabaseName(), userAuth,
            "IMPORT DATABASE WITH vertices = \"file:///etc/passwd\", verticesFileType = \"csv\", vertexType = \"PwLeak\"");
        assertThat(status).as("non-admin user must not read local files via IMPORT DATABASE").isEqualTo(403);
      } finally {
        dropUser(serverIndex, "pwn");
      }
    });
  }

  @Test
  void nonAdminUserCannotImportRemoteSSRF() throws Exception {
    testEachServer(serverIndex -> {
      final String userAuth = createNonAdminUser(serverIndex, "pwn2", "pwn123secret");
      try {
        final int status = commandStatus(serverIndex, getDatabaseName(), userAuth,
            "IMPORT DATABASE \"http://169.254.169.254/latest/meta-data/\" WITH vertexType = \"Leak\"");
        assertThat(status).as("non-admin user must not trigger SSRF via IMPORT DATABASE").isEqualTo(403);
      } finally {
        dropUser(serverIndex, "pwn2");
      }
    });
  }

  /**
   * Creates a user that is a member of the database but only of the default (non-admin) group, so it has none of the
   * {@code updateSecurity / updateSchema / updateDatabaseSettings} permissions.
   */
  private String createNonAdminUser(final int serverIndex, final String name, final String password) throws Exception {
    if (getServer(serverIndex).getSecurity().existsUser(name))
      getServer(serverIndex).getSecurity().dropUser(name);

    final JSONObject payload = new JSONObject();
    payload.put("name", name);
    payload.put("password", password);
    // "reader" is not a defined group, so the user falls back to the default group which has empty access
    payload.put("databases", new JSONObject().put(getDatabaseName(), new JSONArray().put("reader")));

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
