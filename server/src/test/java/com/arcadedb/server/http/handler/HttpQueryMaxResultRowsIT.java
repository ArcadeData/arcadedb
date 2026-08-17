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
import com.arcadedb.serializer.json.JSONArray;
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
 * Issue #5719: {@code arcadedb.server.httpQueryDefaultLimit} protects only the callers that state no limit of
 * their own, so {@code SELECT FROM HugeType LIMIT 100000000} - or a request carrying {@code "limit": -1} - made
 * the server serialize an arbitrarily large result into a single JSON response.
 * <p>
 * {@code arcadedb.server.httpQueryMaxResultRows} is the ceiling no caller can widen. It refuses with HTTP 413
 * instead of truncating, because a response cut short without saying so is the defect issue #5711 fixed, and
 * re-introducing it for the callers that DO state a limit would only move it.
 * <p>
 * The ceiling is lowered to {@link #CEILING} rows here, and the default cap to {@link #DEFAULT_LIMIT}, so the
 * interaction between the two can be checked without materializing a million records.
 */
class HttpQueryMaxResultRowsIT extends BaseGraphServerTest {
  private static final int    CEILING       = 20;
  private static final int    DEFAULT_LIMIT = 10;
  private static final int    TOTAL_ROWS    = 30;
  private static final String TYPE_NAME     = "CeilingRow";
  private static final String SETTING       = GlobalConfiguration.SERVER_HTTP_QUERY_MAX_RESULT_ROWS.getKey();

  private final HttpClient client = HttpClient.newHttpClient();

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    config.setValue(GlobalConfiguration.SERVER_HTTP_QUERY_DEFAULT_LIMIT, DEFAULT_LIMIT);
    config.setValue(GlobalConfiguration.SERVER_HTTP_QUERY_MAX_RESULT_ROWS, CEILING);
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
  void aQueryLimitAboveTheCeilingCannotWidenTheResponse() throws Exception {
    // The exact shape the issue describes: a LIMIT far above the ceiling written into the query text. Before
    // the ceiling existed it was honored as written, because #5716 made a stated LIMIT win over the default cap.
    final HttpResponse<String> response = send("query", new JSONObject()
        .put("language", "sql")
        .put("command", "SELECT i FROM " + TYPE_NAME + " LIMIT 100000000"));

    assertThat(response.statusCode()).isEqualTo(413);
    assertThat(response.body()).contains(SETTING);
    assertThat(response.body()).contains(String.valueOf(CEILING));
  }

  @Test
  void anUnlimitedRequestLimitIsRefusedInsteadOfServed() throws Exception {
    final HttpResponse<String> response = send("query", new JSONObject()
        .put("language", "sql")
        .put("command", "SELECT i FROM " + TYPE_NAME)
        .put("limit", -1));

    assertThat(response.statusCode()).isEqualTo(413);
    assertThat(response.body()).contains(SETTING);
  }

  @Test
  void aResultThatFitsUnderTheCeilingIsStillServedUnlimited() throws Exception {
    // The ceiling refuses a response, it does not lower a cap: a caller asking for everything still gets
    // everything as long as everything fits, and the response keeps reporting the cap the caller stated.
    final JSONObject response = query(new JSONObject()
        .put("language", "sql")
        .put("command", "SELECT i FROM " + TYPE_NAME + " WHERE i < " + (CEILING - 5))
        .put("limit", -1));

    assertThat(response.getJSONArray("result").length()).isEqualTo(CEILING - 5);
    assertThat(response.getInt("returned")).isEqualTo(CEILING - 5);
    assertThat(response.getInt("limit")).isEqualTo(-1);
    assertThat(response.getBoolean("truncated")).isFalse();
  }

  @Test
  void aResultOfExactlyTheCeilingIsNotRefused() throws Exception {
    // The boundary: the ceiling is a maximum, not a strict upper bound. Nothing was left behind, so the
    // response is complete and must be delivered.
    final JSONObject response = query(new JSONObject()
        .put("language", "sql")
        .put("command", "SELECT i FROM " + TYPE_NAME + " WHERE i < " + CEILING)
        .put("limit", -1));

    assertThat(response.getJSONArray("result").length()).isEqualTo(CEILING);
    assertThat(response.getInt("returned")).isEqualTo(CEILING);
    assertThat(response.getBoolean("truncated")).isFalse();
  }

  @Test
  void aRequestLimitAtTheCeilingIsHonoredAndTruncatesAsUsual() throws Exception {
    // A cap the caller stated and the ceiling allows is the caller's own bound: reaching it is an ordinary
    // truncation reported with 'truncated', not a refusal.
    final JSONObject response = query(new JSONObject()
        .put("language", "sql")
        .put("command", "SELECT i FROM " + TYPE_NAME)
        .put("limit", CEILING));

    assertThat(response.getJSONArray("result").length()).isEqualTo(CEILING);
    assertThat(response.getInt("limit")).isEqualTo(CEILING);
    assertThat(response.getBoolean("truncated")).isTrue();
  }

  @Test
  void theDefaultCapStillTruncatesQuietlyBelowTheCeiling() throws Exception {
    // Nothing about #5711/#5716 changed for a caller that states no limit: the default cap truncates and says so.
    final JSONObject response = query(new JSONObject()
        .put("language", "sql")
        .put("command", "SELECT i FROM " + TYPE_NAME));

    assertThat(response.getJSONArray("result").length()).isEqualTo(DEFAULT_LIMIT);
    assertThat(response.getInt("limit")).isEqualTo(DEFAULT_LIMIT);
    assertThat(response.getBoolean("truncated")).isTrue();
  }

  @Test
  void theGetEndpointEnforcesTheCeilingToo() throws Exception {
    // Both surfaces answer the same way: the GET endpoint runs the query as written, with no LIMIT pushed down,
    // so it exercises the serializer bound rather than the pushdown one.
    assertThat(sendGet("SELECT i FROM " + TYPE_NAME, "-1").statusCode()).isEqualTo(413);
    assertThat(sendGet("SELECT i FROM " + TYPE_NAME + " LIMIT 100000000", null).statusCode()).isEqualTo(413);
  }

  @Test
  void theGetEndpointStillServesWhatFits() throws Exception {
    final JSONObject response = get("SELECT i FROM " + TYPE_NAME + " WHERE i < " + (CEILING - 5), "-1");

    assertThat(response.getJSONArray("result").length()).isEqualTo(CEILING - 5);
    assertThat(response.getInt("limit")).isEqualTo(-1);
    assertThat(response.getBoolean("truncated")).isFalse();
  }

  @Test
  void aCypherQueryIsBoundedByTheCeiling() throws Exception {
    // Cypher exposes no execution plan on the query path and gets no pushed-down LIMIT either, so the ceiling
    // is enforced purely while serializing - the path every non-SQL language takes.
    final HttpResponse<String> response = send("query", new JSONObject()
        .put("language", "opencypher")
        .put("command", "MATCH (n:" + TYPE_NAME + ") RETURN n.i AS i")
        .put("limit", -1));

    assertThat(response.statusCode()).isEqualTo(413);
    assertThat(response.body()).contains(SETTING);
  }

  @Test
  void theGraphSerializerIsBoundedByTheCeiling() throws Exception {
    final HttpResponse<String> response = send("query", new JSONObject()
        .put("language", "sql")
        .put("command", "SELECT FROM " + TYPE_NAME)
        .put("serializer", "graph")
        .put("limit", -1));

    assertThat(response.statusCode()).isEqualTo(413);
  }

  @Test
  void aCollectionInsideASingleRowCannotExpandPastTheCeiling() throws Exception {
    // The graph serializers count elements, not rows, and one row can refer to arbitrarily many of them
    // through a single property: `collect()` returns exactly ONE row holding the whole set. A cap tested only
    // between rows and between properties expanded that row whole, and - being the only row - left nothing in
    // the result set for the truncation probe to notice, so the response came back 200 with as many elements
    // as the query found.
    final HttpResponse<String> response = send("query", new JSONObject()
        .put("language", "opencypher")
        .put("command", "MATCH (n:" + TYPE_NAME + ") RETURN collect(n) AS vs")
        .put("serializer", "graph")
        .put("limit", -1));

    assertThat(response.statusCode()).isEqualTo(413);
    assertThat(response.body()).contains(SETTING);
  }

  @Test
  void aCollectionThatFitsIsStillExpandedWhole() throws Exception {
    // The guard above must not cut an expansion that fits: the same query over fewer rows is complete, and a
    // complete response is never flagged.
    final JSONObject response = query(new JSONObject()
        .put("language", "opencypher")
        .put("command", "MATCH (n:" + TYPE_NAME + ") WHERE n.i < " + (CEILING - 5) + " RETURN collect(n) AS vs")
        .put("serializer", "graph")
        .put("limit", -1));

    assertThat(response.getJSONObject("result").getJSONArray("vertices").length()).isEqualTo(CEILING - 5);
    assertThat(response.getBoolean("truncated")).isFalse();
  }

  @Test
  void aRefusedCommandIsRolledBackInsteadOfCommittedBehindTheError() throws Exception {
    // A write whose result nobody will ever see must not be left committed: the ceiling refuses by raising out
    // of the handler, which rolls the auto-commit transaction back.
    final HttpResponse<String> response = send("command", new JSONObject()
        .put("language", "sql")
        .put("command", "UPDATE " + TYPE_NAME + " SET touched = true RETURN AFTER")
        .put("limit", -1));

    assertThat(response.statusCode()).isEqualTo(413);
    assertThat(countRows("SELECT FROM " + TYPE_NAME + " WHERE touched = true")).isZero();
  }

  @Test
  void theProfiledPathIsBoundedTooInsteadOfMaterializingEverything() throws Exception {
    // 'profileExecution: detailed' drains the whole result set into memory before serializing it, and a command
    // carrying its own LIMIT gets no pushed-down bound: without a cap on that drain the ceiling would refuse the
    // response only after the rows it exists to keep out of the heap were already there.
    final HttpResponse<String> response = send("query", new JSONObject()
        .put("language", "sql")
        .put("command", "SELECT i FROM " + TYPE_NAME + " LIMIT 100000000")
        .put("profileExecution", "detailed"));

    assertThat(response.statusCode()).isEqualTo(413);
  }

  @Test
  void theTimeSeriesQueryEndpointEnforcesTheCeilingToo() throws Exception {
    // The endpoint the issue calls out separately: it materializes the whole range before any limit is known,
    // so the ceiling cannot keep the fetch out of the heap - but it can, and must, refuse to build the JSON
    // copy of it rather than serve an unbounded response to a caller stating 'limit: -1'.
    assertThat(send("command", new JSONObject()
        .put("language", "sql")
        .put("command", "CREATE TIMESERIES TYPE ceilingts TIMESTAMP ts TAGS (location STRING) FIELDS (value DOUBLE)"))
        .statusCode()).isEqualTo(200);

    final StringBuilder lines = new StringBuilder();
    for (int i = 1; i <= TOTAL_ROWS; i++)
      lines.append("ceilingts,location=us value=").append(i).append(".0 ").append(i * 1000L).append('\n');
    assertThat(postLineProtocol(lines.toString())).isEqualTo(204);

    final HttpResponse<String> refused = postTsQuery(new JSONObject().put("type", "ceilingts").put("limit", -1));
    assertThat(refused.statusCode()).isEqualTo(413);
    assertThat(refused.body()).contains(SETTING);

    // What fits is still served, unlimited cap and all.
    final HttpResponse<String> served = postTsQuery(new JSONObject()
        .put("type", "ceilingts")
        .put("to", (CEILING - 5) * 1000L)
        .put("limit", -1));
    assertThat(served.statusCode()).isEqualTo(200);
    assertThat(new JSONObject(served.body()).getInt("count")).isEqualTo(CEILING - 5);
  }

  @Test
  void theTimeSeriesAggregationBranchIsBoundedToo() throws Exception {
    // The aggregated shape of the same endpoint reads no 'limit' at all, so the ceiling is the only bound it
    // can ever have: a small 'bucketInterval' over a wide range produces one response row per bucket, which is
    // the very shape the raw branch is refused for.
    assertThat(send("command", new JSONObject()
        .put("language", "sql")
        .put("command", "CREATE TIMESERIES TYPE aggceilingts TIMESTAMP ts TAGS (location STRING) FIELDS (value DOUBLE)"))
        .statusCode()).isEqualTo(200);

    final StringBuilder lines = new StringBuilder();
    for (int i = 1; i <= TOTAL_ROWS; i++)
      lines.append("aggceilingts,location=us value=").append(i).append(".0 ").append(i * 1000L).append('\n');
    assertThat(postLineProtocol(lines.toString())).isEqualTo(204);

    // One bucket per sample: TOTAL_ROWS buckets, above the ceiling.
    final HttpResponse<String> refused = postTsQuery(aggregationRequest("aggceilingts", 1_000L));
    assertThat(refused.statusCode()).isEqualTo(413);
    assertThat(refused.body()).contains(SETTING);

    // A bucket interval wide enough to fit under the ceiling is served as before.
    final HttpResponse<String> served = postTsQuery(aggregationRequest("aggceilingts", TOTAL_ROWS * 1_000L));
    assertThat(served.statusCode()).isEqualTo(200);
    assertThat(new JSONObject(served.body()).getJSONArray("buckets").length()).isLessThanOrEqualTo(CEILING);
  }

  private static JSONObject aggregationRequest(final String type, final long bucketInterval) {
    return new JSONObject()
        .put("type", type)
        .put("from", 0L)
        .put("to", (TOTAL_ROWS + 1) * 1_000L)
        .put("aggregation", new JSONObject()
            .put("bucketInterval", bucketInterval)
            .put("requests", new JSONArray().put(new JSONObject()
                .put("field", "value")
                .put("type", "AVG")
                .put("alias", "avg_value"))));
  }

  @Test
  void aCeilingBelowTheDefaultCapTruncatesInsteadOfRefusing() throws Exception {
    // A deployment whose two settings disagree must not turn ordinary default-cap traffic into 413s: the
    // refusal is for a caller that asked to go past the ceiling, and this caller asked for nothing at all.
    // The default is lowered to the ceiling instead, and the truncation is reported as it always was.
    final int lowCeiling = DEFAULT_LIMIT / 2;
    getServer(0).getConfiguration().setValue(GlobalConfiguration.SERVER_HTTP_QUERY_MAX_RESULT_ROWS, lowCeiling);
    try {
      final JSONObject response = query(new JSONObject()
          .put("language", "sql")
          .put("command", "SELECT i FROM " + TYPE_NAME));

      assertThat(response.getJSONArray("result").length()).isEqualTo(lowCeiling);
      assertThat(response.getInt("returned")).isEqualTo(lowCeiling);
      assertThat(response.getInt("limit")).isEqualTo(lowCeiling);
      assertThat(response.getBoolean("truncated")).isTrue();

      // The GET endpoint, which pushes no LIMIT down, must agree.
      final JSONObject viaGet = get("SELECT i FROM " + TYPE_NAME, null);
      assertThat(viaGet.getJSONArray("result").length()).isEqualTo(lowCeiling);
      assertThat(viaGet.getBoolean("truncated")).isTrue();

      // A caller that DOES state a cap above the ceiling is still refused, so the clamp above has not turned
      // the ceiling off for everybody.
      assertThat(send("query", new JSONObject()
          .put("language", "sql")
          .put("command", "SELECT i FROM " + TYPE_NAME)
          .put("limit", -1)).statusCode()).isEqualTo(413);
    } finally {
      getServer(0).getConfiguration().setValue(GlobalConfiguration.SERVER_HTTP_QUERY_MAX_RESULT_ROWS, CEILING);
    }
  }

  @Test
  void theLargestStatableLimitIsClampedBeforeTheProbeCanOverflow() throws Exception {
    // The two guards against Integer.MAX_VALUE meet here. With the ceiling on, the clamp runs first, so the
    // LIMIT pushed down is the ceiling + 1 and truncationProbeLimit's saturation is never even reached.
    final JSONObject maxLimit = new JSONObject()
        .put("language", "sql")
        .put("command", "SELECT i FROM " + TYPE_NAME)
        .put("limit", Integer.MAX_VALUE);

    assertThat(send("query", maxLimit).statusCode()).isEqualTo(413);

    // With the ceiling off, the clamp is a no-op and the saturation guard is what has to hold: 'limit
    // 2147483648' would be pushed down otherwise, and the request would not come back with every row.
    getServer(0).getConfiguration().setValue(GlobalConfiguration.SERVER_HTTP_QUERY_MAX_RESULT_ROWS, -1);
    try {
      final JSONObject served = query(maxLimit);
      assertThat(served.getJSONArray("result").length()).isEqualTo(TOTAL_ROWS);
      assertThat(served.getBoolean("truncated")).isFalse();
    } finally {
      getServer(0).getConfiguration().setValue(GlobalConfiguration.SERVER_HTTP_QUERY_MAX_RESULT_ROWS, CEILING);
    }
  }

  @Test
  void theCeilingCanBeTurnedOff() throws Exception {
    // -1 restores the pre-#5719 behaviour for a deployment that needs an unbounded escape hatch.
    getServer(0).getConfiguration().setValue(GlobalConfiguration.SERVER_HTTP_QUERY_MAX_RESULT_ROWS, -1);
    try {
      final JSONObject response = query(new JSONObject()
          .put("language", "sql")
          .put("command", "SELECT i FROM " + TYPE_NAME)
          .put("limit", -1));

      assertThat(response.getJSONArray("result").length()).isEqualTo(TOTAL_ROWS);
      assertThat(response.getInt("limit")).isEqualTo(-1);
      assertThat(response.getBoolean("truncated")).isFalse();
    } finally {
      getServer(0).getConfiguration().setValue(GlobalConfiguration.SERVER_HTTP_QUERY_MAX_RESULT_ROWS, CEILING);
    }
  }

  /**
   * Address of the server this test started, read back from it rather than hardcoded: when 2480 is already
   * taken the server binds the next free port, and a hardcoded URL would then quietly send every request of
   * this class to whatever else is listening there.
   */
  private String baseUrl() {
    return "http://127.0.0.1:" + getServer(0).getHttpServer().getPort();
  }

  private int countRows(final String query) {
    int rows = 0;
    try (final ResultSet resultSet = getServerDatabase(0, getDatabaseName()).query("sql", query)) {
      while (resultSet.hasNext()) {
        resultSet.next();
        rows++;
      }
    }
    return rows;
  }

  private JSONObject query(final JSONObject payload) throws Exception {
    final HttpResponse<String> response = send("query", payload);
    assertThat(response.statusCode()).isEqualTo(200);
    return new JSONObject(response.body());
  }

  private HttpResponse<String> send(final String endpoint, final JSONObject payload) throws Exception {
    final HttpRequest request = HttpRequest.newBuilder()
        .uri(new URI(baseUrl() + "/api/v1/" + endpoint + "/" + getDatabaseName()))
        .POST(HttpRequest.BodyPublishers.ofString(payload.toString()))
        .setHeader("Content-Type", "application/json")
        .setHeader("Authorization",
            "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()))
        .build();

    return client.send(request, BodyHandlers.ofString());
  }

  private int postLineProtocol(final String body) throws Exception {
    final HttpRequest request = HttpRequest.newBuilder()
        .uri(new URI(baseUrl() + "/api/v1/ts/" + getDatabaseName() + "/write?precision=ms"))
        .POST(HttpRequest.BodyPublishers.ofString(body))
        .setHeader("Content-Type", "text/plain")
        .setHeader("Authorization",
            "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()))
        .build();

    return client.send(request, BodyHandlers.ofString()).statusCode();
  }

  private HttpResponse<String> postTsQuery(final JSONObject payload) throws Exception {
    final HttpRequest request = HttpRequest.newBuilder()
        .uri(new URI(baseUrl() + "/api/v1/ts/" + getDatabaseName() + "/query"))
        .POST(HttpRequest.BodyPublishers.ofString(payload.toString()))
        .setHeader("Content-Type", "application/json")
        .setHeader("Authorization",
            "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()))
        .build();

    return client.send(request, BodyHandlers.ofString());
  }

  private JSONObject get(final String command, final String limit) throws Exception {
    final HttpResponse<String> response = sendGet(command, limit);
    assertThat(response.statusCode()).isEqualTo(200);
    return new JSONObject(response.body());
  }

  private HttpResponse<String> sendGet(final String command, final String limit) throws Exception {
    // The command travels as a path segment: URLEncoder emits '+' for a space, which is only a space in a
    // query string.
    final String url = baseUrl() + "/api/v1/query/" + getDatabaseName() + "/sql/"
        + URLEncoder.encode(command, StandardCharsets.UTF_8).replace("+", "%20")
        + (limit == null ? "" : "?limit=" + limit);

    final HttpRequest request = HttpRequest.newBuilder()
        .uri(new URI(url))
        .GET()
        .setHeader("Authorization",
            "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()))
        .build();

    return client.send(request, BodyHandlers.ofString());
  }
}
