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
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.Label;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.Sample;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.TimeSeries;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.WriteRequest;
import org.junit.jupiter.api.Test;
import org.xerial.snappy.Snappy;

import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The per-type access of a group is registered under the canonical name of a type, and a TimeSeries type can also be
 * reached by one of its aliases. Counting the samples of a type and the up-front check of a Prometheus remote write both
 * have to be made on the name the type is registered under, whichever of its names the request used: through an alias they
 * must be refused exactly as they are through the canonical name, and nothing of a refused remote write may be stored.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class TimeSeriesAliasPerTypeAclIT extends BaseGraphServerTest {

  private static final String SCOPED_USER      = "ts-alias-scoped-user";
  private static final String SCOPED_PWD       = "tsaliasscopeduser1";
  private static final String RESTRICTED_TYPE  = "ReservedMetrics";
  private static final String RESTRICTED_ALIAS = "ReservedMetricsAlias";
  private static final String AUTHORIZED_TYPE  = "OpenMetrics";
  private static final String AUTHORIZED_ALIAS = "OpenMetricsAlias";

  private String lastResponseBody = "";

  @Test
  void aliasOfADeniedTypeIsRefusedLikeItsName() throws Exception {
    testEachServer((serverIndex) -> {
      // Types must exist BEFORE the scoped user's per-type map is built, so the map has an entry for each of them
      for (final String typeName : new String[] { RESTRICTED_TYPE, AUTHORIZED_TYPE }) {
        command(serverIndex, "CREATE TIMESERIES TYPE " + typeName + " TIMESTAMP ts FIELDS (value DOUBLE)");
        command(serverIndex, "INSERT INTO " + typeName + " SET ts = 1000, value = 42.0");
        command(serverIndex, "INSERT INTO " + typeName + " SET ts = 2000, value = 1337.0");
      }
      command(serverIndex, "ALTER TYPE " + RESTRICTED_TYPE + " ALIASES " + RESTRICTED_ALIAS);
      command(serverIndex, "ALTER TYPE " + AUTHORIZED_TYPE + " ALIASES " + AUTHORIZED_ALIAS);

      createScopedUser(serverIndex);
      try {
        final String auth = basicAuth(SCOPED_USER, SCOPED_PWD);

        // --- count of the samples ---
        assertThat(sqlStatus(serverIndex, auth, "SELECT count(*) AS c FROM " + RESTRICTED_TYPE))
            .as("counting a denied TimeSeries type by its name must be rejected").isEqualTo(403);
        assertThat(sqlStatus(serverIndex, auth, "SELECT count(*) AS c FROM " + RESTRICTED_ALIAS))
            .as("counting a denied TimeSeries type by its alias must be rejected the same way: %s", lastResponseBody)
            .isEqualTo(403);
        assertThat(sqlStatus(serverIndex, auth, "SELECT count(*) AS c FROM " + AUTHORIZED_ALIAS))
            .as("counting an authorized TimeSeries type by its alias must still work: %s", lastResponseBody).isEqualTo(200);
        assertThat(new JSONObject(lastResponseBody).getJSONArray("result").getJSONObject(0).getLong("c")).isEqualTo(2L);

        // --- Prometheus remote write: the up-front check has to refuse the whole request before the first series is stored ---
        assertThat(postPromWrite(serverIndex, auth, AUTHORIZED_TYPE, RESTRICTED_TYPE))
            .as("a remote write naming a denied type must be rejected").isEqualTo(403);
        assertThat(postPromWrite(serverIndex, auth, AUTHORIZED_TYPE, RESTRICTED_ALIAS))
            .as("a remote write naming a denied type by its alias must be rejected the same way: %s", lastResponseBody)
            .isEqualTo(403);
        assertThat(countSamples(serverIndex, AUTHORIZED_TYPE))
            .as("no series of a rejected remote write may have been stored").isEqualTo(2L);
        assertThat(countSamples(serverIndex, RESTRICTED_TYPE)).isEqualTo(2L);

        assertThat(postPromWrite(serverIndex, auth, AUTHORIZED_TYPE, AUTHORIZED_ALIAS))
            .as("a remote write naming an authorized type by its alias must still work: %s", lastResponseBody)
            .isEqualTo(204);
        assertThat(countSamples(serverIndex, AUTHORIZED_TYPE)).as("both series of the accepted remote write are stored")
            .isEqualTo(4L);
        assertThat(countSamples(serverIndex, RESTRICTED_TYPE)).isEqualTo(2L);
      } finally {
        deleteUser(serverIndex, SCOPED_USER);
      }
    });
  }

  private long countSamples(final int serverIndex, final String typeName) throws Exception {
    final JSONArray result = new JSONObject(command(serverIndex, "SELECT count(*) AS c FROM " + typeName))
        .getJSONArray("result");
    return result.isEmpty() ? 0 : result.getJSONObject(0).getLong("c");
  }

  private void createScopedUser(final int serverIndex) throws Exception {
    final ServerSecurity security = getServer(serverIndex).getSecurity();

    security.getDatabaseGroupsConfiguration(getDatabaseName()).put("tsAliasScoped",
        new JSONObject().put("access", new JSONArray().put("updateSecurity").put("updateSchema"))
            .put("types", new JSONObject()
                .put("*", new JSONObject().put("access",
                    new JSONArray().put("readRecord").put("createRecord").put("updateRecord").put("deleteRecord")))
                .put(RESTRICTED_TYPE, new JSONObject().put("access", new JSONArray()))));
    security.saveGroups();

    if (security.existsUser(SCOPED_USER))
      security.dropUser(SCOPED_USER);

    final JSONObject payload = new JSONObject()
        .put("name", SCOPED_USER)
        .put("password", SCOPED_PWD)
        .put("databases", new JSONObject().put(getDatabaseName(), new JSONArray().put("tsAliasScoped")));

    final HttpURLConnection connection = open(serverIndex, "POST", "/api/v1/server/users",
        basicAuth("root", DEFAULT_PASSWORD_FOR_TESTS));
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

  private void deleteUser(final int serverIndex, final String name) throws Exception {
    final HttpURLConnection connection = open(serverIndex, "DELETE",
        "/api/v1/server/users?name=" + URLEncoder.encode(name, StandardCharsets.UTF_8),
        basicAuth("root", DEFAULT_PASSWORD_FOR_TESTS));
    connection.connect();
    try {
      connection.getResponseCode();
    } finally {
      connection.disconnect();
    }
  }

  private int sqlStatus(final int serverIndex, final String auth, final String sql) throws Exception {
    final HttpURLConnection connection = open(serverIndex, "POST", "/api/v1/query/" + getDatabaseName(), auth);
    connection.setDoOutput(true);
    connection.setRequestProperty("Content-Type", "application/json");
    connection.getOutputStream().write(
        new JSONObject().put("language", "sql").put("command", sql).toString().getBytes(StandardCharsets.UTF_8));
    connection.connect();
    try {
      return statusOf(connection);
    } finally {
      connection.disconnect();
    }
  }

  /** A remote write carrying one series for each of the two metric names, in the order given. */
  private int postPromWrite(final int serverIndex, final String auth, final String firstMetric, final String secondMetric)
      throws Exception {
    final WriteRequest request = new WriteRequest(List.of(
        new TimeSeries(List.of(new Label("__name__", firstMetric)), List.of(new Sample(-1.0, 8888))),
        new TimeSeries(List.of(new Label("__name__", secondMetric)), List.of(new Sample(-2.0, 9999)))));
    final HttpURLConnection connection = open(serverIndex, "POST", "/api/v1/ts/" + getDatabaseName() + "/prom/write", auth);
    connection.setDoOutput(true);
    connection.setRequestProperty("Content-Type", "application/x-protobuf");
    connection.setRequestProperty("Content-Encoding", "snappy");
    connection.getOutputStream().write(Snappy.compress(request.encode()));
    connection.connect();
    try {
      return statusOf(connection);
    } finally {
      connection.disconnect();
    }
  }

  /** Keeps the response body of the last call, so an unexpected status reports why the server answered it. */
  private int statusOf(final HttpURLConnection connection) throws Exception {
    final int status = connection.getResponseCode();
    final var stream = status < 400 ? connection.getInputStream() : connection.getErrorStream();
    lastResponseBody = stream == null ? "" : new String(stream.readAllBytes(), StandardCharsets.UTF_8);
    return status;
  }

  private HttpURLConnection open(final int serverIndex, final String method, final String path, final String auth)
      throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URL(
        "http://127.0.0.1:" + getServerHttpPort(serverIndex) + path).openConnection();
    connection.setRequestMethod(method);
    connection.setRequestProperty("Authorization", auth);
    return connection;
  }

  private String basicAuth(final String user, final String password) {
    return "Basic " + Base64.getEncoder().encodeToString((user + ":" + password).getBytes(StandardCharsets.UTF_8));
  }
}
