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
package com.arcadedb.server.http.handler;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #5711: a response the row cap cut short must never be indistinguishable from a complete one, and the
 * cap must never override a limit the caller stated itself - neither the {@code limit} field of the request nor
 * the LIMIT clause of the query.
 * <p>
 * The server default cap is lowered to {@link #CAP} rows for these tests so the same semantics can be checked
 * without materializing 20,000 records.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class HttpQueryTruncationIT extends BaseGraphServerTest {
  private static final int    CAP        = 50;
  private static final int    TOTAL_ROWS = 60;
  private static final String TYPE_NAME  = "TruncatedRow";

  private final HttpClient client = HttpClient.newHttpClient();

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    config.setValue(GlobalConfiguration.SERVER_HTTP_QUERY_DEFAULT_LIMIT, CAP);
  }

  @BeforeEach
  void createRows() {
    final Database database = getServerDatabase(0, getDatabaseName());
    // A vertex type so the same rows are reachable from SQL and from Cypher.
    database.getSchema().createVertexType(TYPE_NAME);
    database.transaction(() -> {
      for (int i = 0; i < TOTAL_ROWS; i++)
        database.newVertex(TYPE_NAME).set("i", i).save();
    });
  }

  @Test
  void queryLimitAboveTheDefaultCapIsHonored() throws Exception {
    final JSONObject response = query(new JSONObject()
        .put("language", "sql")
        .put("command", "SELECT i FROM " + TYPE_NAME + " LIMIT " + TOTAL_ROWS));

    assertThat(response.getJSONArray("result").length()).isEqualTo(TOTAL_ROWS);
    assertThat(response.getInt("returned")).isEqualTo(TOTAL_ROWS);
    assertThat(response.getInt("limit")).isEqualTo(TOTAL_ROWS);
    assertThat(response.getBoolean("truncated")).isFalse();
  }

  @Test
  void truncationByTheDefaultCapIsReported() throws Exception {
    final JSONObject response = query(new JSONObject()
        .put("language", "sql")
        .put("command", "SELECT i FROM " + TYPE_NAME));

    assertThat(response.getJSONArray("result").length()).isEqualTo(CAP);
    assertThat(response.getInt("returned")).isEqualTo(CAP);
    assertThat(response.getInt("limit")).isEqualTo(CAP);
    assertThat(response.getBoolean("truncated")).isTrue();
  }

  @Test
  void aCompleteResultIsNeverFlaggedAsTruncated() throws Exception {
    // The result ends exactly at the cap: no row was left behind, so nothing must be reported.
    final JSONObject response = query(new JSONObject()
        .put("language", "sql")
        .put("command", "SELECT i FROM " + TYPE_NAME + " LIMIT " + CAP));

    assertThat(response.getJSONArray("result").length()).isEqualTo(CAP);
    assertThat(response.getInt("returned")).isEqualTo(CAP);
    assertThat(response.getBoolean("truncated")).isFalse();
  }

  @Test
  void theRequestLimitWinsOverTheQueryLimitAndIsReported() throws Exception {
    final JSONObject response = query(new JSONObject()
        .put("language", "sql")
        .put("command", "SELECT i FROM " + TYPE_NAME + " LIMIT " + TOTAL_ROWS)
        .put("limit", 10));

    assertThat(response.getJSONArray("result").length()).isEqualTo(10);
    assertThat(response.getInt("returned")).isEqualTo(10);
    assertThat(response.getInt("limit")).isEqualTo(10);
    assertThat(response.getBoolean("truncated")).isTrue();
  }

  @Test
  void anUnlimitedRequestLimitReturnsEveryRow() throws Exception {
    final JSONObject response = query(new JSONObject()
        .put("language", "sql")
        .put("command", "SELECT i FROM " + TYPE_NAME)
        .put("limit", -1));

    assertThat(response.getJSONArray("result").length()).isEqualTo(TOTAL_ROWS);
    assertThat(response.getInt("returned")).isEqualTo(TOTAL_ROWS);
    assertThat(response.getInt("limit")).isEqualTo(-1);
    assertThat(response.getBoolean("truncated")).isFalse();
  }

  @Test
  void aParameterizedLimitIsHonoredOnEveryExecution() throws Exception {
    // The cap is read back from the execution plan, and a plan reports the LIMIT value of the execution it was
    // built for. Today a plan carrying a LIMIT step is never cached, so the value is always fresh; the cap is
    // nevertheless never lowered below the configured default, so a plan that did come back from the cache
    // could not cut the second query down to the 5 rows the first one asked for.
    final String command = "SELECT i FROM " + TYPE_NAME + " LIMIT :max";

    final JSONObject first = query(new JSONObject()
        .put("language", "sql")
        .put("command", command)
        .put("params", new JSONObject().put("max", 5)));
    assertThat(first.getJSONArray("result").length()).isEqualTo(5);
    assertThat(first.getBoolean("truncated")).isFalse();

    final JSONObject second = query(new JSONObject()
        .put("language", "sql")
        .put("command", command)
        .put("params", new JSONObject().put("max", 40)));
    assertThat(second.getJSONArray("result").length()).isEqualTo(40);
    assertThat(second.getBoolean("truncated")).isFalse();
  }

  @Test
  void aZeroRequestLimitMeansUnlimited() throws Exception {
    // 0 means unlimited in the serializer, so it must mean unlimited on the way in too: it used to be pushed
    // down as a literal LIMIT 0 and return nothing.
    final JSONObject response = query(new JSONObject()
        .put("language", "sql")
        .put("command", "SELECT i FROM " + TYPE_NAME)
        .put("limit", 0));

    assertThat(response.getJSONArray("result").length()).isEqualTo(TOTAL_ROWS);
    assertThat(response.getInt("limit")).isEqualTo(-1);
    assertThat(response.getBoolean("truncated")).isFalse();
  }

  @Test
  void theGraphSerializerReportsTruncation() throws Exception {
    final JSONObject response = query(new JSONObject()
        .put("language", "sql")
        .put("command", "SELECT FROM " + TYPE_NAME)
        .put("serializer", "graph"));

    assertThat(response.getJSONObject("result").getJSONArray("vertices").length()).isEqualTo(CAP);
    assertThat(response.getInt("returned")).isEqualTo(CAP);
    assertThat(response.getBoolean("truncated")).isTrue();
  }

  @Test
  void theHttpAndTheEmbeddedSurfaceAgreeOnTheSameQuery() throws Exception {
    final String command = "SELECT i FROM " + TYPE_NAME + " LIMIT " + TOTAL_ROWS;

    int embeddedRows = 0;
    try (final ResultSet resultSet = getServerDatabase(0, getDatabaseName()).query("sql", command)) {
      while (resultSet.hasNext()) {
        resultSet.next();
        embeddedRows++;
      }
    }

    final JSONObject response = query(new JSONObject().put("language", "sql").put("command", command));

    assertThat(embeddedRows).isEqualTo(TOTAL_ROWS);
    assertThat(response.getInt("returned")).isEqualTo(embeddedRows);
  }

  @Test
  void theStudioSerializerReportsTruncation() throws Exception {
    final JSONObject response = query(new JSONObject()
        .put("language", "sql")
        .put("command", "SELECT i FROM " + TYPE_NAME)
        .put("serializer", "studio"));

    assertThat(response.getJSONObject("result").getJSONArray("records").length()).isEqualTo(CAP);
    assertThat(response.getInt("returned")).isEqualTo(CAP);
    assertThat(response.getBoolean("truncated")).isTrue();
  }

  @Test
  void theDefaultSerializerEmitsExactlyTheCap() throws Exception {
    // Any unknown serializer name falls back to the default one, which used to emit one row above the cap.
    final JSONObject response = query(new JSONObject()
        .put("language", "sql")
        .put("command", "SELECT i FROM " + TYPE_NAME)
        .put("serializer", "json"));

    assertThat(response.getJSONArray("result").length()).isEqualTo(CAP);
    assertThat(response.getInt("returned")).isEqualTo(CAP);
    assertThat(response.getBoolean("truncated")).isTrue();
  }

  @Test
  void aCypherQueryTruncatedByTheDefaultCapIsReported() throws Exception {
    // Cypher exposes no execution plan on the query path, so its own LIMIT cannot be read back: the cap still
    // applies, but the response says so instead of looking complete.
    final JSONObject response = query(new JSONObject()
        .put("language", "opencypher")
        .put("command", "MATCH (n:" + TYPE_NAME + ") RETURN n.i AS i"));

    assertThat(response.getJSONArray("result").length()).isEqualTo(CAP);
    assertThat(response.getBoolean("truncated")).isTrue();
  }

  @Test
  void aWriteCommandAlwaysReportsTheLimitFields() throws Exception {
    final JSONObject response = command(new JSONObject()
        .put("language", "sql")
        .put("command", "CREATE VERTEX " + TYPE_NAME + " SET i = 1000"));

    assertThat(response.getInt("returned")).isEqualTo(1);
    assertThat(response.getBoolean("truncated")).isFalse();
  }

  @Test
  void theGetEndpointHonorsTheQueryLimit() throws Exception {
    final JSONObject response = get("SELECT i FROM " + TYPE_NAME + " LIMIT " + TOTAL_ROWS, null);

    assertThat(response.getJSONArray("result").length()).isEqualTo(TOTAL_ROWS);
    assertThat(response.getInt("limit")).isEqualTo(TOTAL_ROWS);
    assertThat(response.getBoolean("truncated")).isFalse();
  }

  @Test
  void theGetEndpointReportsTruncationByTheDefaultCap() throws Exception {
    final JSONObject response = get("SELECT i FROM " + TYPE_NAME, null);

    assertThat(response.getJSONArray("result").length()).isEqualTo(CAP);
    assertThat(response.getInt("limit")).isEqualTo(CAP);
    assertThat(response.getBoolean("truncated")).isTrue();
  }

  @Test
  void theGetEndpointLimitParameterWinsOverTheQueryLimit() throws Exception {
    final JSONObject response = get("SELECT i FROM " + TYPE_NAME + " LIMIT " + TOTAL_ROWS, "10");

    assertThat(response.getJSONArray("result").length()).isEqualTo(10);
    assertThat(response.getInt("limit")).isEqualTo(10);
    assertThat(response.getBoolean("truncated")).isTrue();
  }

  @Test
  void theRemoteDriverHonorsTheQueryLimit() {
    try (final RemoteDatabase remote = newRemoteDatabase()) {
      int rows = 0;
      try (final ResultSet resultSet = remote.query("sql", "SELECT i FROM " + TYPE_NAME + " LIMIT " + TOTAL_ROWS)) {
        while (resultSet.hasNext()) {
          resultSet.next();
          rows++;
        }
      }
      assertThat(rows).isEqualTo(TOTAL_ROWS);
    }
  }

  @Test
  void theRemoteDriverCanRemoveTheCap() {
    try (final RemoteDatabase remote = newRemoteDatabase()) {
      int rows = 0;
      try (final ResultSet resultSet = remote.query("sql", "SELECT i FROM " + TYPE_NAME)) {
        while (resultSet.hasNext()) {
          resultSet.next();
          rows++;
        }
      }
      assertThat(rows).isEqualTo(CAP);

      remote.setMaxResultRows(-1);
      rows = 0;
      try (final ResultSet resultSet = remote.query("sql", "SELECT i FROM " + TYPE_NAME)) {
        while (resultSet.hasNext()) {
          resultSet.next();
          rows++;
        }
      }
      assertThat(rows).isEqualTo(TOTAL_ROWS);
    }
  }

  private RemoteDatabase newRemoteDatabase() {
    return new RemoteDatabase("127.0.0.1", 2480, getDatabaseName(), "root", DEFAULT_PASSWORD_FOR_TESTS);
  }

  private JSONObject query(final JSONObject payload) throws Exception {
    return post("query", payload);
  }

  private JSONObject command(final JSONObject payload) throws Exception {
    return post("command", payload);
  }

  private JSONObject post(final String endpoint, final JSONObject payload) throws Exception {
    final HttpRequest request = HttpRequest.newBuilder()
        .uri(new URI("http://127.0.0.1:2480/api/v1/" + endpoint + "/" + getDatabaseName()))
        .POST(HttpRequest.BodyPublishers.ofString(payload.toString()))
        .setHeader("Content-Type", "application/json")
        .setHeader("Authorization",
            "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()))
        .build();

    final HttpResponse<String> response = client.send(request, BodyHandlers.ofString());
    assertThat(response.statusCode()).isEqualTo(200);
    return new JSONObject(response.body());
  }

  private JSONObject get(final String command, final String limit) throws Exception {
    // The command travels as a path segment: URLEncoder emits '+' for a space, which is only a space in a
    // query string.
    final String url = "http://127.0.0.1:2480/api/v1/query/" + getDatabaseName() + "/sql/"
        + URLEncoder.encode(command, StandardCharsets.UTF_8).replace("+", "%20")
        + (limit == null ? "" : "?limit=" + limit);

    final HttpRequest request = HttpRequest.newBuilder()
        .uri(new URI(url))
        .GET()
        .setHeader("Authorization",
            "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()))
        .build();

    final HttpResponse<String> response = client.send(request, BodyHandlers.ofString());
    assertThat(response.statusCode()).isEqualTo(200);
    return new JSONObject(response.body());
  }
}
