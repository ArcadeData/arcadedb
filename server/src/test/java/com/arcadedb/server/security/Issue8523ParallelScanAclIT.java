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

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;

import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #8523: the producers of a parallel type scan read with the permissions of the user who ran the query, exactly
 * as the sequential scan does. A polymorphic scan is where it shows: the type in the FROM is checked when the query is
 * planned, the buckets of its subtypes when the scan opens them, which a parallel scan does on its producers. Pinned
 * for the plain scan, the filtered scan and the aggregation computed in the producers, against the same query run
 * sequentially.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8523ParallelScanAclIT extends BaseGraphServerTest {
  private static final String SCOPED_USER     = "scan-scoped-user";
  private static final String SCOPED_PWD      = "scanscopeduser1";
  private static final String RESTRICTED_TYPE = "SecretRows";
  private static final String AUTHORIZED_TYPE = "PublicRows";

  @Test
  void parallelScansActAsTheUserWhoRanTheQuery() throws Exception {
    testEachServer((serverIndex) -> {
      // AN AUTHORIZED PARENT WITH A DENIED SUBTYPE: THE PLANNER CHECKS THE PARENT, AND THE SUBTYPE'S BUCKETS ARE CHECKED
      // ONLY WHEN THE SCAN OPENS THEM - ON THE PRODUCERS WHEN IT RUNS IN PARALLEL. FOUR BUCKETS EACH: THE SCAN RUNS IN
      // PARALLEL WHATEVER THE PAGE-RANGE SETTINGS
      command(serverIndex, "CREATE DOCUMENT TYPE " + AUTHORIZED_TYPE + " BUCKETS 4");
      command(serverIndex, "CREATE DOCUMENT TYPE " + RESTRICTED_TYPE + " EXTENDS " + AUTHORIZED_TYPE + " BUCKETS 4");
      for (final String type : new String[] { AUTHORIZED_TYPE, RESTRICTED_TYPE })
        for (int x = 1; x <= 8; x++)
          command(serverIndex, "INSERT INTO " + type + " SET x = " + x);

      createScopedUser(serverIndex);
      try {
        final DatabaseInternal database = (DatabaseInternal) getServerDatabase(serverIndex, getDatabaseName());
        final ServerSecurityUser user = getServer(serverIndex).getSecurity().getUser(SCOPED_USER);
        for (final String template : QUERIES) {
          final String polymorphic = String.format(template, AUTHORIZED_TYPE);
          assertThat(explain(database, user, polymorphic)).as(polymorphic).contains("(parallel)");

          // OUTSIDE A TRANSACTION, AS THE SCOPED USER: THE PATH THAT RUNS ON THE PRODUCERS
          assertThatThrownBy(() -> DatabaseUserContext.runAs(database, user, () -> drain(database, polymorphic)))
              .as("'%s' reaches the denied subtype's buckets on the producers: it must be refused there too", polymorphic)
              .satisfies(e -> assertThat(rootCause(e)).isInstanceOf(SecurityException.class));

          // THE SAME QUERY RUN SEQUENTIALLY, INSIDE A TRANSACTION, IS REFUSED THE SAME WAY: THE PARALLEL SCAN MUST NOT
          // BE THE ONE PLACE WHERE THE SUBTYPE'S ROWS COME BACK
          assertThatThrownBy(() -> DatabaseUserContext.runAs(database, user, () -> {
            database.begin();
            try {
              return drain(database, polymorphic);
            } finally {
              database.rollback();
            }
          })).satisfies(e -> assertThat(rootCause(e)).isInstanceOf(SecurityException.class));
        }

        // POSITIVE CONTROL: THE SUBTYPE ALONE IS REFUSED BY THE PLANNER, THE PARENT'S OWN ROWS ARE READABLE AS root
        assertThat(drain(database, "SELECT FROM " + AUTHORIZED_TYPE)).isEqualTo(16);
      } finally {
        deleteUser(serverIndex);
      }
    });
  }

  private static final String[] QUERIES = { "SELECT FROM %s", "SELECT FROM %s WHERE x > 0", "SELECT sum(x) AS s FROM %s" };

  private static int drain(final DatabaseInternal database, final String query) {
    int rows = 0;
    try (final ResultSet rs = database.query("sql", query)) {
      while (rs.hasNext()) {
        rs.next();
        ++rows;
      }
    }
    return rows;
  }

  private static String explain(final DatabaseInternal database, final ServerSecurityUser user, final String query) {
    return DatabaseUserContext.runAs(database, user, () -> {
      try (final ResultSet rs = database.query("sql", "EXPLAIN " + query)) {
        return rs.next().<String>getProperty("executionPlanAsString");
      }
    });
  }

  private static Throwable rootCause(Throwable e) {
    while (e.getCause() != null && e.getCause() != e)
      e = e.getCause();
    return e;
  }

  private void createScopedUser(final int serverIndex) throws Exception {
    final ServerSecurity security = getServer(serverIndex).getSecurity();
    security.getDatabaseGroupsConfiguration(getDatabaseName()).put("scanScoped",
        new JSONObject().put("access", new JSONArray())
            .put("types", new JSONObject()
                .put("*", new JSONObject().put("access", new JSONArray().put("readRecord")))
                .put(RESTRICTED_TYPE, new JSONObject().put("access", new JSONArray()))));
    security.saveGroups();

    if (security.existsUser(SCOPED_USER))
      security.dropUser(SCOPED_USER);

    final JSONObject payload = new JSONObject().put("name", SCOPED_USER).put("password", SCOPED_PWD)
        .put("databases", new JSONObject().put(getDatabaseName(), new JSONArray().put("scanScoped")));
    final HttpURLConnection connection = open(serverIndex, "POST", "/api/v1/server/users", basicAuth("root", DEFAULT_PASSWORD_FOR_TESTS));
    connection.setDoOutput(true);
    connection.setRequestProperty("Content-Type", "application/json");
    connection.getOutputStream().write(payload.toString().getBytes(StandardCharsets.UTF_8));
    connection.connect();
    try {
      assertThat(connection.getResponseCode()).isEqualTo(201);
    } finally {
      connection.disconnect();
    }
  }

  private void deleteUser(final int serverIndex) throws Exception {
    final HttpURLConnection connection = open(serverIndex, "DELETE",
        "/api/v1/server/users?name=" + URLEncoder.encode(SCOPED_USER, StandardCharsets.UTF_8), basicAuth("root", DEFAULT_PASSWORD_FOR_TESTS));
    connection.connect();
    try {
      connection.getResponseCode();
    } finally {
      connection.disconnect();
    }
  }

  private HttpURLConnection open(final int serverIndex, final String method, final String path, final String auth) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) URI.create("http://127.0.0.1:248" + serverIndex + path).toURL()
        .openConnection();
    connection.setRequestMethod(method);
    connection.setRequestProperty("Authorization", auth);
    return connection;
  }

  private static String basicAuth(final String user, final String password) {
    return "Basic " + Base64.getEncoder().encodeToString((user + ":" + password).getBytes(StandardCharsets.UTF_8));
  }
}
