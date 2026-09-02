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
 * A TimeSeries type owns no record bucket, so the bucket/file-id permission map a group's {@code types} ACL is
 * compiled into has no entry for it and every check on it used to fall back to "allow". A user explicitly denied
 * on a TimeSeries type could therefore read its samples and insert new ones - through SQL and through every
 * TimeSeries, Prometheus and Grafana endpoint - while the identical policy correctly restricted a normal document
 * type.
 * <p>
 * This test grants the user every access on every type EXCEPT the TimeSeries type it names explicitly, then walks
 * the read, aggregate and write paths. A positive control on a second, authorized TimeSeries type keeps each 403
 * honest: it proves the refusal is per-type authorization and not a blanket lockout of the TimeSeries endpoints.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class TimeSeriesPerTypeAclIT extends BaseGraphServerTest {

  private static final String SCOPED_USER     = "ts-scoped-user";
  private static final String SCOPED_PWD      = "tsscopeduser1";
  private static final String RESTRICTED_TYPE = "SecretMetrics";
  private static final String AUTHORIZED_TYPE = "PublicMetrics";

  private String lastResponseBody = "";

  @Test
  void perTypeAclEnforcedOnEveryTimeSeriesPath() throws Exception {
    testEachServer((serverIndex) -> {
      // Types must exist BEFORE the scoped user's per-type map is built, so the map segments the restricted one.
      for (final String typeName : new String[] { RESTRICTED_TYPE, AUTHORIZED_TYPE }) {
        command(serverIndex, "CREATE TIMESERIES TYPE " + typeName + " TIMESTAMP ts FIELDS (value DOUBLE)");
        command(serverIndex, "INSERT INTO " + typeName + " SET ts = 1000, value = 42.0");
        command(serverIndex, "INSERT INTO " + typeName + " SET ts = 2000, value = 1337.0");
      }

      createScopedUser(serverIndex);
      try {
        final String auth = basicAuth(SCOPED_USER, SCOPED_PWD);

        // --- SQL read ---
        assertThat(sqlStatus(serverIndex, auth, "query", "SELECT FROM " + RESTRICTED_TYPE))
            .as("reading a denied TimeSeries type's samples must be rejected").isEqualTo(403);
        assertThat(sqlStatus(serverIndex, auth, "query", "SELECT FROM " + AUTHORIZED_TYPE))
            .as("reading an authorized TimeSeries type must still work").isEqualTo(200);

        // --- SQL push-down aggregation: a different execution step, same samples ---
        final String aggregate = "SELECT ts.timeBucket('1s', ts) AS b, avg(value) AS v FROM %s GROUP BY b";
        assertThat(sqlStatus(serverIndex, auth, "query", String.format(aggregate, RESTRICTED_TYPE)))
            .as("the TimeSeries push-down aggregation must be gated like a plain scan").isEqualTo(403);
        assertThat(sqlStatus(serverIndex, auth, "query", String.format(aggregate, AUTHORIZED_TYPE)))
            .as("the same aggregation on an authorized type must still work").isEqualTo(200);

        // --- SQL write ---
        assertThat(sqlStatus(serverIndex, auth, "command", "INSERT INTO " + RESTRICTED_TYPE + " SET ts = 9999, value = -1.0"))
            .as("inserting into a denied TimeSeries type must be rejected").isEqualTo(403);

        // --- TimeSeries endpoints ---
        assertThat(postJsonStatus(serverIndex, "/api/v1/ts/" + getDatabaseName() + "/query", auth,
            new JSONObject().put("type", RESTRICTED_TYPE).toString()))
            .as("the TimeSeries query endpoint must apply the same per-type check").isEqualTo(403);
        assertThat(postJsonStatus(serverIndex, "/api/v1/ts/" + getDatabaseName() + "/query", auth,
            new JSONObject().put("type", AUTHORIZED_TYPE).toString()))
            .as("the same endpoint on an authorized type must still work: %s", lastResponseBody).isEqualTo(200);

        assertThat(getStatus(serverIndex, "/api/v1/ts/" + getDatabaseName() + "/latest?type=" + RESTRICTED_TYPE, auth))
            .as("the TimeSeries latest endpoint must apply the same per-type check").isEqualTo(403);

        assertThat(postTextStatus(serverIndex,
            "/api/v1/ts/" + getDatabaseName() + "/write?precision=ms", auth, RESTRICTED_TYPE + " value=-1.0 9999"))
            .as("the TimeSeries line-protocol write endpoint must apply the same per-type check").isEqualTo(403);
        assertThat(postTextStatus(serverIndex,
            "/api/v1/ts/" + getDatabaseName() + "/write?precision=ms", auth, AUTHORIZED_TYPE + " value=-1.0 9999"))
            // 204, not 200: the line-protocol endpoint answers an accepted write the InfluxDB way, with no body.
            .as("the same endpoint on an authorized type must still work: %s", lastResponseBody).isEqualTo(204);

        // --- Grafana ---
        assertThat(postJsonStatus(serverIndex, "/api/v1/ts/" + getDatabaseName() + "/grafana/query", auth,
            new JSONObject().put("targets", new JSONArray().put(
                new JSONObject().put("refId", "A").put("type", RESTRICTED_TYPE))).toString()))
            .as("the Grafana query endpoint must apply the same per-type check").isEqualTo(403);

        final JSONObject metadata = new JSONObject(
            getBody(serverIndex, "/api/v1/ts/" + getDatabaseName() + "/grafana/metadata", auth));
        final JSONArray listedTypes = metadata.getJSONArray("types");
        final JSONArray listedNames = new JSONArray();
        for (int i = 0; i < listedTypes.length(); i++)
          listedNames.put(listedTypes.getJSONObject(i).getString("name"));
        assertThat(listedNames.toList()).as("a denied TimeSeries type must not be listed, nor its tags/fields")
            .doesNotContain(RESTRICTED_TYPE).contains(AUTHORIZED_TYPE);

        // --- PromQL ---
        assertThat(getStatus(serverIndex,
            "/api/v1/ts/" + getDatabaseName() + "/prom/api/v1/query?query=" + RESTRICTED_TYPE, auth))
            .as("PromQL must not be a side door onto a denied TimeSeries type").isEqualTo(403);

        final JSONObject labelValues = new JSONObject(getBody(serverIndex,
            "/api/v1/ts/" + getDatabaseName() + "/prom/api/v1/label/__name__/values", auth));
        assertThat(labelValues.getJSONArray("data").toList())
            .as("a denied TimeSeries type's name must not be discoverable through PromQL label values")
            .doesNotContain(RESTRICTED_TYPE).contains(AUTHORIZED_TYPE);

        // --- Decisive check (as root): nothing was written to the denied type ---
        assertThat(countSamples(serverIndex, RESTRICTED_TYPE))
            .as("no sample may have been written to the denied type").isEqualTo(2);
        assertThat(countSamples(serverIndex, AUTHORIZED_TYPE))
            .as("the authorized line-protocol write must have been persisted").isEqualTo(3);
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

    security.getDatabaseGroupsConfiguration(getDatabaseName()).put("tsScoped",
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
        .put("databases", new JSONObject().put(getDatabaseName(), new JSONArray().put("tsScoped")));

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

  private int sqlStatus(final int serverIndex, final String auth, final String endpoint, final String sql) throws Exception {
    return postJsonStatus(serverIndex, "/api/v1/" + endpoint + "/" + getDatabaseName(), auth,
        new JSONObject().put("language", "sql").put("command", sql).toString());
  }

  private int postJsonStatus(final int serverIndex, final String path, final String auth, final String body) throws Exception {
    return postStatus(serverIndex, path, auth, "application/json", body);
  }

  private int postTextStatus(final int serverIndex, final String path, final String auth, final String body) throws Exception {
    return postStatus(serverIndex, path, auth, "text/plain", body);
  }

  private int postStatus(final int serverIndex, final String path, final String auth, final String contentType,
      final String body) throws Exception {
    final HttpURLConnection connection = open(serverIndex, "POST", path, auth);
    connection.setDoOutput(true);
    connection.setRequestProperty("Content-Type", contentType);
    connection.getOutputStream().write(body.getBytes(StandardCharsets.UTF_8));
    connection.connect();
    try {
      return statusOf(connection);
    } finally {
      connection.disconnect();
    }
  }

  private int getStatus(final int serverIndex, final String path, final String auth) throws Exception {
    final HttpURLConnection connection = open(serverIndex, "GET", path, auth);
    connection.connect();
    try {
      return statusOf(connection);
    } finally {
      connection.disconnect();
    }
  }

  /**
   * Keeps the response body of the last call in {@link #lastResponseBody} so an unexpected status reports WHY the
   * server refused instead of only that it did - the difference between a per-type denial and a malformed request
   * is exactly what this test has to tell apart.
   */
  private int statusOf(final HttpURLConnection connection) throws Exception {
    final int status = connection.getResponseCode();
    final var stream = status < 400 ? connection.getInputStream() : connection.getErrorStream();
    lastResponseBody = stream == null ? "" : new String(stream.readAllBytes(), StandardCharsets.UTF_8);
    return status;
  }

  private String getBody(final int serverIndex, final String path, final String auth) throws Exception {
    final HttpURLConnection connection = open(serverIndex, "GET", path, auth);
    connection.connect();
    try {
      assertThat(connection.getResponseCode()).isEqualTo(200);
      return new String(connection.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
    } finally {
      connection.disconnect();
    }
  }

  private HttpURLConnection open(final int serverIndex, final String method, final String path, final String auth)
      throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URL(
        "http://127.0.0.1:248" + serverIndex + path).openConnection();
    connection.setRequestMethod(method);
    connection.setRequestProperty("Authorization", auth);
    return connection;
  }

  private String basicAuth(final String user, final String password) {
    return "Basic " + Base64.getEncoder().encodeToString((user + ":" + password).getBytes(StandardCharsets.UTF_8));
  }
}
