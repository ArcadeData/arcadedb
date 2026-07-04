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
 * Regression test for the incomplete-fix sibling of CVE-2026-44221: the original fix added an UPDATE_SCHEMA permission
 * check to {@code LocalDocumentType.createProperty} only, leaving the other schema mutators (DROP PROPERTY, ALTER TYPE
 * SUPERTYPE +/-, ALTER TYPE NAME, ALTER PROPERTY ...) reachable by a read-only identity.
 *
 * <p>A read-only API token (no {@code updateSchema}) must be rejected with HTTP 403 on every schema mutation, while an
 * administrator must still be able to perform them.</p>
 */
class SchemaMutationAuthorizationIT extends BaseGraphServerTest {

  @Test
  void readOnlyTokenCannotMutateSchema() throws Exception {
    testEachServer(serverIndex -> {
      // ARRANGE (as admin): a type with a property, a second type, and a subtype candidate
      assertThat(adminCommand(serverIndex, "CREATE DOCUMENT TYPE Memory")).isEqualTo(200);
      assertThat(adminCommand(serverIndex, "CREATE PROPERTY Memory.salience DOUBLE")).isEqualTo(200);
      assertThat(adminCommand(serverIndex, "CREATE DOCUMENT TYPE Persisted")).isEqualTo(200);

      final String token = "Bearer " + createReadOnlyToken(serverIndex, "schema-probe-token");
      try {
        // Each of these reaches a previously-unchecked mutator and must be denied
        assertThat(command(serverIndex, token, "DROP PROPERTY Memory.salience"))
            .as("read-only token must not DROP PROPERTY").isEqualTo(403);
        assertThat(command(serverIndex, token, "ALTER TYPE Memory SUPERTYPE +Persisted"))
            .as("read-only token must not add a SUPERTYPE").isEqualTo(403);
        assertThat(command(serverIndex, token, "ALTER TYPE Memory SUPERTYPE -Persisted"))
            .as("read-only token must not remove a SUPERTYPE").isEqualTo(403);
        assertThat(command(serverIndex, token, "ALTER TYPE Memory NAME MemoryRenamed"))
            .as("read-only token must not rename a type").isEqualTo(403);
        assertThat(command(serverIndex, token, "ALTER PROPERTY Memory.salience MANDATORY true"))
            .as("read-only token must not alter a property constraint").isEqualTo(403);
        // GHSA-8vr5-263f-x5r3: the two type-level setters missed by the general UPDATE_SCHEMA hardening
        assertThat(command(serverIndex, token, "ALTER TYPE Memory CUSTOM description = 'unauthorized-schema-write'"))
            .as("read-only token must not write type CUSTOM metadata").isEqualTo(403);
        assertThat(command(serverIndex, token, "ALTER TYPE Memory BUCKETSELECTIONSTRATEGY `round-robin`"))
            .as("read-only token must not change the bucket-selection strategy").isEqualTo(403);
        // GHSA-8vr5-263f-x5r3 defense-in-depth: index rebuild is UPDATE_SCHEMA-gated (statsOnly path used to bypass it)
        assertThat(command(serverIndex, token, "REBUILD INDEX *"))
            .as("read-only token must not REBUILD INDEX").isEqualTo(403);
      } finally {
        deleteToken(serverIndex, "schema-probe-token");
      }
    });
  }

  @Test
  void adminCanStillMutateSchema() throws Exception {
    testEachServer(serverIndex -> {
      assertThat(adminCommand(serverIndex, "CREATE DOCUMENT TYPE Doc")).isEqualTo(200);
      assertThat(adminCommand(serverIndex, "CREATE PROPERTY Doc.title STRING")).isEqualTo(200);
      assertThat(adminCommand(serverIndex, "CREATE DOCUMENT TYPE Base")).isEqualTo(200);

      // Positive controls: an administrator (root) must not be blocked by the new checks
      assertThat(adminCommand(serverIndex, "ALTER TYPE Doc SUPERTYPE +Base")).isEqualTo(200);
      assertThat(adminCommand(serverIndex, "ALTER PROPERTY Doc.title MANDATORY true")).isEqualTo(200);
      assertThat(adminCommand(serverIndex, "DROP PROPERTY Doc.title")).isEqualTo(200);
      // GHSA-8vr5-263f-x5r3 positive controls: the new guards must not block an administrator
      assertThat(adminCommand(serverIndex, "ALTER TYPE Doc CUSTOM description = 'authorized'")).isEqualTo(200);
      assertThat(adminCommand(serverIndex, "ALTER TYPE Doc BUCKETSELECTIONSTRATEGY `round-robin`")).isEqualTo(200);
      assertThat(adminCommand(serverIndex, "REBUILD INDEX *")).isEqualTo(200);
    });
  }

  /**
   * GHSA-8vr5-263f-x5r3 sibling sweep: the same UPDATE_SCHEMA gap existed on materialized views, continuous aggregates,
   * time-series downsampling policies, and function-library registration. A read-only identity must be denied (403) on
   * every one, while the administrator must still be able to perform them.
   */
  @Test
  void readOnlyTokenCannotMutateViewsAggregatesAndFunctions() throws Exception {
    testEachServer(serverIndex -> {
      // ARRANGE (as admin): a source type, a materialized view, a time-series type and a continuous aggregate over it
      assertThat(adminCommand(serverIndex, "CREATE DOCUMENT TYPE Account")).isEqualTo(200);
      assertThat(adminCommand(serverIndex, "CREATE PROPERTY Account.active BOOLEAN")).isEqualTo(200);
      assertThat(adminCommand(serverIndex,
          "CREATE MATERIALIZED VIEW ActiveAccounts AS SELECT name FROM Account WHERE active = true")).isEqualTo(200);
      assertThat(adminCommand(serverIndex,
          "CREATE TIMESERIES TYPE Sensor TIMESTAMP ts TAGS (id STRING) FIELDS (value DOUBLE) SHARDS 1")).isEqualTo(200);
      assertThat(adminCommand(serverIndex, "CREATE CONTINUOUS AGGREGATE sensor_hourly AS "
          + "SELECT id, ts.timeBucket('1h', ts) AS hour, avg(value) AS avg_value FROM Sensor GROUP BY id, hour"))
          .isEqualTo(200);

      final String token = "Bearer " + createReadOnlyToken(serverIndex, "schema-siblings-token");
      try {
        assertThat(command(serverIndex, token, "ALTER MATERIALIZED VIEW ActiveAccounts REFRESH MANUAL"))
            .as("read-only token must not ALTER a materialized view").isEqualTo(403);
        assertThat(command(serverIndex, token, "DROP MATERIALIZED VIEW ActiveAccounts"))
            .as("read-only token must not DROP a materialized view").isEqualTo(403);
        assertThat(command(serverIndex, token, "DROP CONTINUOUS AGGREGATE sensor_hourly"))
            .as("read-only token must not DROP a continuous aggregate").isEqualTo(403);
        assertThat(command(serverIndex, token,
            "ALTER TIMESERIES TYPE Sensor ADD DOWNSAMPLING POLICY AFTER 7 DAYS GRANULARITY 1 HOURS"))
            .as("read-only token must not change a time-series downsampling policy").isEqualTo(403);
        assertThat(command(serverIndex, token, "DEFINE FUNCTION Probe.f \"return 1\" LANGUAGE js"))
            .as("read-only token must not register a function library").isEqualTo(403);
      } finally {
        deleteToken(serverIndex, "schema-siblings-token");
      }

      // Positive controls: the administrator must still be able to perform each guarded operation
      assertThat(adminCommand(serverIndex,
          "ALTER TIMESERIES TYPE Sensor ADD DOWNSAMPLING POLICY AFTER 7 DAYS GRANULARITY 1 HOURS")).isEqualTo(200);
      assertThat(adminCommand(serverIndex, "DEFINE FUNCTION Probe.f \"return 1\" LANGUAGE js")).isEqualTo(200);
      assertThat(adminCommand(serverIndex, "ALTER MATERIALIZED VIEW ActiveAccounts REFRESH MANUAL")).isEqualTo(200);
      assertThat(adminCommand(serverIndex, "DROP CONTINUOUS AGGREGATE sensor_hourly")).isEqualTo(200);
      assertThat(adminCommand(serverIndex, "DROP MATERIALIZED VIEW ActiveAccounts")).isEqualTo(200);
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
        .put("database", new JSONArray()); // no updateSchema

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
