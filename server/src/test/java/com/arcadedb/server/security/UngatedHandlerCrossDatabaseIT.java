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
 * Regression test for GHSA-x8mg-6r4p-87pf: the time-series, batch, Prometheus and Grafana HTTP handlers do
 * not extend {@code DatabaseAbstractHandler} and previously resolved the database named in the path without
 * any {@code canAccessToDatabase} check, so a user authorized for one database could read and write another
 * (cross-database IDOR). After the fix every such handler enforces database authorization and returns HTTP
 * 403 when the user is not entitled to the target database, while remaining usable on the user's own database.
 */
class UngatedHandlerCrossDatabaseIT extends BaseGraphServerTest {

  private static final String RESTRICTED_DB = "restricted-cross-db";
  private static final String SCOPED_USER   = "ungated-scoped-user";
  private static final String SCOPED_PWD    = "scopeduser1";

  @Test
  void unauthorizedDatabaseIsRejectedOnUngatedHandlers() throws Exception {
    testEachServer((serverIndex) -> {
      // A real, existing database the scoped user has NO grant for - this is the IDOR target.
      getServer(serverIndex).getOrCreateDatabase(RESTRICTED_DB);
      createScopedUser(serverIndex);
      try {
        final String auth = basicAuth(SCOPED_USER, SCOPED_PWD);

        // Negative: every representative ungated endpoint must deny access to the restricted database.
        assertThat(post(serverIndex, "/api/v1/batch/" + RESTRICTED_DB, auth, "{}"))
            .as("batch on unauthorized db").isEqualTo(403);
        assertThat(post(serverIndex, "/api/v1/ts/" + RESTRICTED_DB + "/write", auth, "{}"))
            .as("ts write on unauthorized db").isEqualTo(403);
        assertThat(post(serverIndex, "/api/v1/ts/" + RESTRICTED_DB + "/query", auth, "{}"))
            .as("ts query on unauthorized db").isEqualTo(403);
        assertThat(get(serverIndex, "/api/v1/ts/" + RESTRICTED_DB + "/prom/api/v1/query?query=up", auth))
            .as("promQL query on unauthorized db").isEqualTo(403);
        assertThat(get(serverIndex, "/api/v1/ts/" + RESTRICTED_DB + "/grafana/health", auth))
            .as("grafana health on unauthorized db").isEqualTo(403);

        // Positive control: the same user hitting its OWN authorized database must NOT be blocked by the
        // authorization gate. The payload is intentionally empty so the response may be 200/400/500, but it
        // must never be 403 - proving the 403s above are due to database authorization, not a blanket denial.
        assertThat(post(serverIndex, "/api/v1/ts/" + getDatabaseName() + "/write", auth, "{}"))
            .as("ts write on the user's own db must not be a security 403").isNotEqualTo(403);
      } finally {
        deleteUser(serverIndex, SCOPED_USER);
      }
    });
  }

  private void createScopedUser(final int serverIndex) throws Exception {
    final ServerSecurity security = getServer(serverIndex).getSecurity();

    security.getDatabaseGroupsConfiguration(getDatabaseName()).put("ungatedScoped",
        new JSONObject().put("access", new JSONArray())
            .put("types", new JSONObject().put("*", new JSONObject().put("access",
                new JSONArray().put("readRecord").put("createRecord").put("updateRecord")))));
    security.saveGroups();

    if (security.existsUser(SCOPED_USER))
      security.dropUser(SCOPED_USER);

    // Authorized ONLY for the default test database, never for RESTRICTED_DB.
    final JSONObject payload = new JSONObject()
        .put("name", SCOPED_USER)
        .put("password", SCOPED_PWD)
        .put("databases", new JSONObject().put(getDatabaseName(), new JSONArray().put("ungatedScoped")));

    final HttpURLConnection connection = openPost(serverIndex, "/api/v1/server/users",
        basicAuth("root", DEFAULT_PASSWORD_FOR_TESTS));
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
    final HttpURLConnection connection = openPost(serverIndex,
        "/api/v1/server/users?name=" + URLEncoder.encode(name, StandardCharsets.UTF_8),
        basicAuth("root", DEFAULT_PASSWORD_FOR_TESTS));
    connection.setRequestMethod("DELETE");
    connection.connect();
    try {
      connection.getResponseCode();
    } finally {
      connection.disconnect();
    }
  }

  private int post(final int serverIndex, final String path, final String auth, final String body) throws Exception {
    final HttpURLConnection connection = openPost(serverIndex, path, auth);
    connection.setDoOutput(true);
    connection.setRequestProperty("Content-Type", "application/json");
    connection.getOutputStream().write(body.getBytes());
    connection.connect();
    try {
      return connection.getResponseCode();
    } finally {
      connection.disconnect();
    }
  }

  private int get(final int serverIndex, final String path, final String auth) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URL(
        "http://127.0.0.1:248" + serverIndex + path).openConnection();
    connection.setRequestMethod("GET");
    connection.setRequestProperty("Authorization", auth);
    connection.connect();
    try {
      return connection.getResponseCode();
    } finally {
      connection.disconnect();
    }
  }

  private HttpURLConnection openPost(final int serverIndex, final String path, final String auth) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URL("http://127.0.0.1:248" + serverIndex + path).openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization", auth);
    return connection;
  }

  private String basicAuth(final String user, final String password) {
    return "Basic " + Base64.getEncoder().encodeToString((user + ":" + password).getBytes());
  }
}
