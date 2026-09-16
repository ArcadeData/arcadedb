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
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7336: {@code POST /api/v1/ts/{database}/query} read the caller's {@code limit} and then asked the
 * engine for the WHOLE range, so {@code {"from": 0, "to": 9999999999999, "limit": 10}} over a type holding
 * millions of samples cost O(N) heap and O(N log N) time to serialize ten rows. The bound now belongs to the
 * fetch: the handler asks for at most the effective ceiling plus one row, and that one extra row is what still
 * tells a cut response from a complete one.
 * <p>
 * The residency itself is asserted at engine level by {@code Issue7336AscendingLimitTest}, which counts the
 * blocks decompressed. What this class pins is the part a caller can see, because a bounded fetch is only a fix
 * if none of it changed: {@code count}, {@code limit}, {@code truncated}, the row content and ordering, and the
 * HTTP 413 the hard ceiling answers with.
 */
class Issue7336TimeSeriesQueryFetchBoundIT extends BaseGraphServerTest {
  private static final int    CEILING       = 20;
  private static final int    DEFAULT_LIMIT = 10;
  private static final int    TOTAL_ROWS    = 30;
  private static final String TYPE_NAME     = "boundedts";
  private static final String SETTING       = GlobalConfiguration.SERVER_HTTP_QUERY_MAX_RESULT_ROWS.getKey();

  private final HttpClient client = HttpClient.newHttpClient();

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    config.setValue(GlobalConfiguration.SERVER_HTTP_QUERY_DEFAULT_LIMIT, DEFAULT_LIMIT);
    config.setValue(GlobalConfiguration.SERVER_HTTP_QUERY_MAX_RESULT_ROWS, CEILING);
  }

  @BeforeEach
  void createSeries() throws Exception {
    assertThat(command("CREATE TIMESERIES TYPE " + TYPE_NAME
        + " TIMESTAMP ts TAGS (location STRING) FIELDS (value DOUBLE)")).isEqualTo(200);

    final StringBuilder lines = new StringBuilder();
    for (int i = 1; i <= TOTAL_ROWS; i++)
      lines.append(TYPE_NAME).append(",location=").append(i % 2 == 0 ? "eu" : "us")
          .append(" value=").append(i).append(".0 ").append(i * 1000L).append('\n');
    assertThat(postLineProtocol(lines.toString())).isEqualTo(204);
  }

  /**
   * The request shape the issue names: a small limit over the widest range the caller can state. It must answer
   * the OLDEST rows, in ascending order, and say it cut the answer short.
   */
  @Test
  void aSmallLimitOverAWideRangeStillAnswersTheOldestRows() throws Exception {
    final JSONObject result = queryOk(new JSONObject()
        .put("type", TYPE_NAME)
        .put("from", 0L)
        .put("to", 9_999_999_999_999L)
        .put("limit", 10));

    assertThat(result.getInt("count")).isEqualTo(10);
    assertThat(result.getInt("limit")).isEqualTo(10);
    assertThat(result.getBoolean("truncated")).isTrue();

    final JSONArray rows = result.getJSONArray("rows");
    assertThat(rows.length()).isEqualTo(10);
    for (int i = 0; i < rows.length(); i++) {
      assertThat(rows.getJSONArray(i).getLong(0)).isEqualTo((i + 1) * 1000L);
      assertThat(rows.getJSONArray(i).getDouble(2)).isEqualTo(i + 1.0);
    }
  }

  /**
   * The boundary the extra row exists for: a range holding exactly {@code limit} rows is NOT truncated, and one
   * holding {@code limit + 1} is. Off by one here and every complete response starts claiming it was cut.
   */
  @Test
  void theLimitBoundaryDecidesTruncationExactly() throws Exception {
    final JSONObject exact = queryOk(new JSONObject()
        .put("type", TYPE_NAME).put("from", 1000L).put("to", 5000L).put("limit", 5));
    assertThat(exact.getInt("count")).isEqualTo(5);
    assertThat(exact.getBoolean("truncated")).isFalse();

    final JSONObject cut = queryOk(new JSONObject()
        .put("type", TYPE_NAME).put("from", 1000L).put("to", 6000L).put("limit", 5));
    assertThat(cut.getInt("count")).isEqualTo(5);
    assertThat(cut.getBoolean("truncated")).isTrue();

    // One row short of the limit is complete, and reports the limit the caller stated regardless.
    final JSONObject under = queryOk(new JSONObject()
        .put("type", TYPE_NAME).put("from", 1000L).put("to", 4000L).put("limit", 5));
    assertThat(under.getInt("count")).isEqualTo(4);
    assertThat(under.getInt("limit")).isEqualTo(5);
    assertThat(under.getBoolean("truncated")).isFalse();
  }

  /**
   * The hard ceiling still refuses rather than truncates, and it must decide on the real row count and not on
   * the bounded fetch: a range of exactly the ceiling is served, one row more is a 413.
   */
  @Test
  void theCeilingStillRefusesAnUnlimitedRequestThatDoesNotFit() throws Exception {
    final HttpResponse<String> refused = postTsQuery(new JSONObject()
        .put("type", TYPE_NAME).put("limit", -1));
    assertThat(refused.statusCode()).isEqualTo(413);
    assertThat(refused.body()).contains(SETTING);

    // Exactly the ceiling fits and is served whole, unlimited cap and all. Both spellings of "unlimited"
    // mean the same thing here, as they do on the query and command endpoints: 0 must not reach
    // Math.min(rows, 0) and return nothing (issue #5711).
    for (final int unlimited : new int[] { -1, 0 }) {
      final JSONObject served = queryOk(new JSONObject()
          .put("type", TYPE_NAME).put("to", CEILING * 1000L).put("limit", unlimited));
      assertThat(served.getInt("count")).isEqualTo(CEILING);
      assertThat(served.getInt("limit")).isEqualTo(-1);
      assertThat(served.getBoolean("truncated")).isFalse();
      assertThat(served.getJSONArray("rows").length()).isEqualTo(CEILING);
    }

    assertThat(postTsQuery(new JSONObject().put("type", TYPE_NAME).put("limit", 0)).statusCode()).isEqualTo(413);

    // One row past it is refused.
    final HttpResponse<String> overByOne = postTsQuery(new JSONObject()
        .put("type", TYPE_NAME).put("to", (CEILING + 1) * 1000L).put("limit", -1));
    assertThat(overByOne.statusCode()).isEqualTo(413);
  }

  /**
   * A stated limit ABOVE the ceiling is the other arm of the refusal: it fails only when the result really does
   * not fit, and is otherwise served with the caller's own cap echoed back.
   */
  @Test
  void aStatedLimitAboveTheCeilingIsRefusedOnlyWhenTheResultDoesNotFit() throws Exception {
    final HttpResponse<String> refused = postTsQuery(new JSONObject()
        .put("type", TYPE_NAME).put("limit", 100_000_000));
    assertThat(refused.statusCode()).isEqualTo(413);
    assertThat(refused.body()).contains(SETTING);

    final JSONObject served = queryOk(new JSONObject()
        .put("type", TYPE_NAME).put("to", (CEILING - 5) * 1000L).put("limit", 100_000_000));
    assertThat(served.getInt("count")).isEqualTo(CEILING - 5);
    assertThat(served.getInt("limit")).isEqualTo(100_000_000);
    assertThat(served.getBoolean("truncated")).isFalse();
  }

  /**
   * A caller that states nothing is bounded by the default cap and truncated quietly, never refused - the
   * refusal is for a caller that asked to go past the ceiling.
   */
  @Test
  void aRequestWithNoLimitIsTruncatedByTheDefaultCap() throws Exception {
    final JSONObject result = queryOk(new JSONObject().put("type", TYPE_NAME));

    assertThat(result.getInt("count")).isEqualTo(DEFAULT_LIMIT);
    assertThat(result.getInt("limit")).isEqualTo(DEFAULT_LIMIT);
    assertThat(result.getBoolean("truncated")).isTrue();
    assertThat(result.getJSONArray("rows").getJSONArray(0).getLong(0)).isEqualTo(1000L);
  }

  /**
   * The tag filter is applied by the bounded scan itself, not after it: the limit must count the rows that
   * SURVIVE the filter, or a selective query over a wide range comes back short.
   */
  @Test
  void theLimitCountsOnlyTheRowsThatSurviveTheTagFilter() throws Exception {
    final JSONObject result = queryOk(new JSONObject()
        .put("type", TYPE_NAME)
        .put("from", 0L)
        .put("to", 9_999_999_999_999L)
        .put("tags", new JSONObject().put("location", "eu"))
        .put("limit", 5));

    assertThat(result.getInt("count")).isEqualTo(5);
    assertThat(result.getBoolean("truncated")).isTrue();

    final JSONArray rows = result.getJSONArray("rows");
    assertThat(rows.length()).isEqualTo(5);
    for (int i = 0; i < rows.length(); i++) {
      // 'eu' carries the even samples only, so the i-th surviving row is sample 2*(i+1).
      assertThat(rows.getJSONArray(i).getLong(0)).isEqualTo((i + 1) * 2000L);
      assertThat(rows.getJSONArray(i).getString(1)).isEqualTo("eu");
      assertThat(rows.getJSONArray(i).getDouble(2)).isEqualTo((i + 1) * 2.0);
    }
  }

  /**
   * The field projection travels with the bound too. The filtered column has to be part of the projection -
   * {@code TagFilter.matchesMapped} cannot satisfy a condition on a column the row does not carry, which is
   * the pre-existing contract this path must keep rather than change.
   */
  @Test
  void aProjectionCarryingTheFilteredColumnIsStillBounded() throws Exception {
    final JSONObject result = queryOk(new JSONObject()
        .put("type", TYPE_NAME)
        .put("from", 0L)
        .put("to", 9_999_999_999_999L)
        .put("tags", new JSONObject().put("location", "us"))
        .put("fields", new JSONArray().put("location"))
        .put("limit", 4));

    assertThat(result.getInt("count")).isEqualTo(4);
    assertThat(result.getBoolean("truncated")).isTrue();

    final JSONArray columns = result.getJSONArray("columns");
    assertThat(columns.length()).isEqualTo(2);
    assertThat(columns.getString(1)).isEqualTo("location");

    final JSONArray rows = result.getJSONArray("rows");
    for (int i = 0; i < rows.length(); i++) {
      // 'us' carries the odd samples, so the i-th surviving row is sample 2*i + 1.
      assertThat(rows.getJSONArray(i).length()).isEqualTo(2);
      assertThat(rows.getJSONArray(i).getLong(0)).isEqualTo((2L * i + 1) * 1000L);
      assertThat(rows.getJSONArray(i).getString(1)).isEqualTo("us");
    }
  }

  /**
   * A range holding nothing is still a complete, empty answer and not a truncated one.
   */
  @Test
  void anEmptyRangeIsCompleteRatherThanTruncated() throws Exception {
    final JSONObject result = queryOk(new JSONObject()
        .put("type", TYPE_NAME).put("from", 900_000L).put("to", 999_000L).put("limit", 5));

    assertThat(result.getInt("count")).isZero();
    assertThat(result.getJSONArray("rows").length()).isZero();
    assertThat(result.getBoolean("truncated")).isFalse();
  }

  /**
   * A limit an int cannot hold is still a client error and not a wrapped negative read as "unlimited": the
   * bounded fetch must not become the place that check is skipped.
   */
  @Test
  void aLimitTooLargeForAnIntIsStillRejected() throws Exception {
    assertThat(postTsQuery(new JSONObject().put("type", TYPE_NAME).put("limit", 3_000_000_000L)).statusCode())
        .isEqualTo(400);
  }

  private JSONObject queryOk(final JSONObject payload) throws Exception {
    final HttpResponse<String> response = postTsQuery(payload);
    assertThat(response.statusCode()).isEqualTo(200);
    return new JSONObject(response.body());
  }

  private HttpResponse<String> postTsQuery(final JSONObject payload) throws Exception {
    return client.send(HttpRequest.newBuilder()
        .uri(new URI(baseUrl() + "/api/v1/ts/" + getDatabaseName() + "/query"))
        .POST(HttpRequest.BodyPublishers.ofString(payload.toString()))
        .setHeader("Content-Type", "application/json")
        .setHeader("Authorization", basicAuth())
        .build(), BodyHandlers.ofString());
  }

  private int command(final String sql) throws Exception {
    return client.send(HttpRequest.newBuilder()
        .uri(new URI(baseUrl() + "/api/v1/command/" + getDatabaseName()))
        .POST(HttpRequest.BodyPublishers.ofString(
            new JSONObject().put("language", "sql").put("command", sql).toString()))
        .setHeader("Content-Type", "application/json")
        .setHeader("Authorization", basicAuth())
        .build(), BodyHandlers.ofString()).statusCode();
  }

  private int postLineProtocol(final String body) throws Exception {
    return client.send(HttpRequest.newBuilder()
        .uri(new URI(baseUrl() + "/api/v1/ts/" + getDatabaseName() + "/write?precision=ms"))
        .POST(HttpRequest.BodyPublishers.ofString(body))
        .setHeader("Content-Type", "text/plain")
        .setHeader("Authorization", basicAuth())
        .build(), BodyHandlers.ofString()).statusCode();
  }

  private static String basicAuth() {
    return "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes());
  }

  /**
   * The server's real port, never a hardcoded one: when 2480 is taken the server binds the next free port.
   */
  private String baseUrl() {
    return "http://127.0.0.1:" + getServer(0).getHttpServer().getPort();
  }
}
