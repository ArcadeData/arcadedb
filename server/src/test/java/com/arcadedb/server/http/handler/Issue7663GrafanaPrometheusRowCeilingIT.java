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
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.LabelMatcher;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.MatchType;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.Query;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.ReadRequest;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.ReadResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.xerial.snappy.Snappy;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7663: {@code POST /ts/{database}/grafana/query} and the Prometheus remote-read endpoint both reached
 * {@code TimeSeriesEngine.query}, which merges every shard's full range into one sorted {@code ArrayList} before
 * a single row is looked at. Neither read a {@code limit}, and neither consulted
 * {@code arcadedb.server.httpQueryMaxResultRows} - the hard ceiling (issue #5719) that {@code /query},
 * {@code /command}, both branches of {@code /ts/{database}/query} and the gRPC time-series RPC (#7390) all
 * enforce. They were the remaining hole.
 * <p>
 * Both now fetch through {@code TimeSeriesEngine.queryAscending} bounded by what the response can still carry,
 * plus the one row that proves it would have carried more.
 * <p>
 * The budget is for the WHOLE response and not for each read in it, which is what
 * {@link #theCeilingIsSpreadAcrossEveryTargetOfOneRequest} and
 * {@link #theCeilingIsSpreadAcrossEveryQueryOfOneReadRequest} pin: a per-read ceiling would let a request with
 * twenty targets return twenty times the maximum, which is a narrower spelling of the same hole rather than a fix
 * for it.
 *
 * @see <a href="https://github.com/ArcadeData/arcadedb/issues/7663">issue #7663</a>
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue7663GrafanaPrometheusRowCeilingIT extends BaseGraphServerTest {

  private static final int    CEILING    = 20;
  private static final String TYPE_NAME  = "cpu_usage";
  private static final int    TOTAL_ROWS = 30;
  private static final String SETTING    = GlobalConfiguration.SERVER_HTTP_QUERY_MAX_RESULT_ROWS.getKey();

  private final HttpClient client = HttpClient.newHttpClient();

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    config.setValue(GlobalConfiguration.SERVER_HTTP_QUERY_MAX_RESULT_ROWS, CEILING);
  }

  @BeforeEach
  void createSeries() throws Exception {
    assertThat(command("CREATE TIMESERIES TYPE " + TYPE_NAME
        + " TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE)")).isEqualTo(200);

    final StringBuilder lines = new StringBuilder();
    for (int i = 1; i <= TOTAL_ROWS; i++)
      lines.append(TYPE_NAME).append(",host=").append(i % 2 == 0 ? "web2" : "web1")
          .append(" value=").append(i).append(".0 ").append(i * 1000L).append('\n');
    assertThat(postLineProtocol(lines.toString())).isEqualTo(204);
  }

  // ---------------------------------------------------------------------------------------------------
  // Grafana
  // ---------------------------------------------------------------------------------------------------

  /**
   * The defect on the Grafana raw branch: a panel over the whole series used to be answered in full, whatever the
   * ceiling said.
   */
  @Test
  void aGrafanaRawQueryPastTheCeilingIsRefused() throws Exception {
    final HttpResponse<String> response = postGrafana(new JSONObject()
        .put("targets", new JSONArray().put(new JSONObject().put("refId", "A").put("type", TYPE_NAME))));

    assertThat(response.statusCode()).isEqualTo(413);
    assertThat(response.body()).contains(SETTING);
  }

  /**
   * The boundary the extra fetched row exists for: exactly the ceiling is served whole, one row more is refused.
   * Off by one here and every panel that fits exactly starts failing.
   */
  @Test
  void theGrafanaRawBranchServesExactlyTheCeilingAndRefusesOneMore() throws Exception {
    final JSONObject frame = grafanaFrameOk(new JSONObject()
        .put("to", CEILING * 1000L)
        .put("targets", new JSONArray().put(new JSONObject().put("refId", "A").put("type", TYPE_NAME))));

    final JSONArray values = frame.getJSONObject("data").getJSONArray("values");
    assertThat(values.getJSONArray(0).length()).isEqualTo(CEILING);
    // The rows are the OLDEST of the range, in ascending order: the bounded fetch must not reorder the answer.
    assertThat(values.getJSONArray(0).getLong(0)).isEqualTo(1_000L);
    assertThat(values.getJSONArray(0).getLong(CEILING - 1)).isEqualTo(CEILING * 1000L);

    assertThat(postGrafana(new JSONObject()
        .put("to", (CEILING + 1) * 1000L)
        .put("targets", new JSONArray().put(new JSONObject().put("refId", "A").put("type", TYPE_NAME))))
        .statusCode()).isEqualTo(413);
  }

  /**
   * One budget for the whole response, not one per target. Two targets of 15 rows each fit individually and must
   * NOT both be served: 30 rows in one response is exactly what the ceiling forbids.
   */
  @Test
  void theCeilingIsSpreadAcrossEveryTargetOfOneRequest() throws Exception {
    final JSONObject oneTarget = new JSONObject()
        .put("to", 15_000L)
        .put("targets", new JSONArray().put(new JSONObject().put("refId", "A").put("type", TYPE_NAME)));
    assertThat(postGrafana(oneTarget).statusCode())
        .as("15 rows fit under a ceiling of %d on their own", CEILING).isEqualTo(200);

    final JSONObject twoTargets = new JSONObject()
        .put("to", 15_000L)
        .put("targets", new JSONArray()
            .put(new JSONObject().put("refId", "A").put("type", TYPE_NAME))
            .put(new JSONObject().put("refId", "B").put("type", TYPE_NAME)));

    final HttpResponse<String> response = postGrafana(twoTargets);
    assertThat(response.statusCode())
        .as("30 rows in ONE response is what the ceiling forbids, however they are split across targets")
        .isEqualTo(413);
    assertThat(response.body()).contains(SETTING);
  }

  /**
   * The aggregation branch shares the budget. Its buckets cannot be bounded in the fetch - {@code aggregateMulti}
   * has to visit the range to fill them - but the response they produce is refused by the same ceiling, exactly as
   * on the sibling {@code /ts/{database}/query} endpoint.
   */
  @Test
  void theGrafanaAggregationBranchIsBoundedByTheSameCeiling() throws Exception {
    final JSONObject aggregation = new JSONObject()
        .put("bucketInterval", 1000L)
        .put("requests", new JSONArray().put(new JSONObject().put("field", "value").put("type", "avg")));

    final HttpResponse<String> refused = postGrafana(new JSONObject()
        .put("from", 1_000L).put("to", TOTAL_ROWS * 1000L)
        .put("targets", new JSONArray().put(new JSONObject()
            .put("refId", "A").put("type", TYPE_NAME).put("aggregation", aggregation))));

    assertThat(refused.statusCode())
        .as("one bucket per sample over the whole series is %d rows, past the ceiling of %d", TOTAL_ROWS, CEILING)
        .isEqualTo(413);
    assertThat(refused.body()).contains(SETTING);

    // A wider bucket brings the response back under the ceiling and it is served normally.
    final JSONObject served = grafanaFrameOk(new JSONObject()
        .put("from", 1_000L).put("to", TOTAL_ROWS * 1000L)
        .put("targets", new JSONArray().put(new JSONObject()
            .put("refId", "A").put("type", TYPE_NAME)
            .put("aggregation", new JSONObject()
                .put("bucketInterval", 10_000L)
                .put("requests", new JSONArray()
                    .put(new JSONObject().put("field", "value").put("type", "avg")))))));

    assertThat(served.getJSONObject("data").getJSONArray("values").getJSONArray(0).length())
        .isLessThanOrEqualTo(CEILING);
  }

  /**
   * The tag filter is applied by the bounded scan itself, not after it, so the budget counts the rows that
   * SURVIVE it. 'web1' carries 15 of the 30 samples, which fits.
   */
  @Test
  void theBudgetCountsOnlyTheRowsThatSurviveTheTagFilter() throws Exception {
    final JSONObject frame = grafanaFrameOk(new JSONObject()
        .put("targets", new JSONArray().put(new JSONObject()
            .put("refId", "A").put("type", TYPE_NAME)
            .put("tags", new JSONObject().put("host", "web1")))));

    final JSONArray values = frame.getJSONObject("data").getJSONArray("values");
    assertThat(values.getJSONArray(0).length()).isEqualTo(TOTAL_ROWS / 2);
    // 'web1' carries the odd samples, so the oldest is sample 1 and they ascend from there.
    assertThat(values.getJSONArray(0).getLong(0)).isEqualTo(1_000L);
    assertThat(values.getJSONArray(0).getLong(1)).isEqualTo(3_000L);
  }

  /**
   * A field projection travels with the bound: the response still carries the named column's values, under its
   * own name, and the rows are still the oldest of the range (issue #7305's contract, kept).
   */
  @Test
  void aProjectedGrafanaQueryIsStillBoundedAndStillCorrect() throws Exception {
    final JSONObject frame = grafanaFrameOk(new JSONObject()
        .put("to", 10_000L)
        .put("targets", new JSONArray().put(new JSONObject()
            .put("refId", "A").put("type", TYPE_NAME)
            .put("fields", new JSONArray().put("value")))));

    final JSONArray fields = frame.getJSONObject("schema").getJSONArray("fields");
    assertThat(fields.length()).isEqualTo(2);
    assertThat(fields.getJSONObject(1).getString("name")).isEqualTo("value");

    final JSONArray values = frame.getJSONObject("data").getJSONArray("values");
    assertThat(values.getJSONArray(0).length()).isEqualTo(10);
    assertThat(values.getJSONArray(1).getDouble(0)).isEqualTo(1.0);
    assertThat(values.getJSONArray(1).getDouble(9)).isEqualTo(10.0);
  }

  // ---------------------------------------------------------------------------------------------------
  // Prometheus remote-read
  // ---------------------------------------------------------------------------------------------------

  /**
   * The defect on the Prometheus remote-read path: a selector over the whole series was answered in full.
   */
  @Test
  void aPrometheusReadPastTheCeilingIsRefused() throws Exception {
    final HttpURLConnection connection = postPromRead(new ReadRequest(List.of(
        new Query(0, TOTAL_ROWS * 1000L,
            List.of(new LabelMatcher(MatchType.EQ, "__name__", TYPE_NAME))))));

    assertThat(connection.getResponseCode()).isEqualTo(413);
  }

  /**
   * Exactly the ceiling is served whole, with its samples intact and in ascending timestamp order, and one row
   * more is refused.
   */
  @Test
  void thePrometheusReadServesExactlyTheCeilingAndRefusesOneMore() throws Exception {
    final ReadResponse response = promReadOk(new ReadRequest(List.of(
        new Query(0, CEILING * 1000L,
            List.of(new LabelMatcher(MatchType.EQ, "__name__", TYPE_NAME))))));

    assertThat(response.getResults()).hasSize(1);
    final int samples = response.getResults().getFirst().getTimeSeries().stream()
        .mapToInt(ts -> ts.getSamples().size()).sum();
    assertThat(samples).isEqualTo(CEILING);

    assertThat(postPromRead(new ReadRequest(List.of(
        new Query(0, (CEILING + 1) * 1000L,
            List.of(new LabelMatcher(MatchType.EQ, "__name__", TYPE_NAME))))))
        .getResponseCode()).isEqualTo(413);
  }

  /**
   * One budget for the whole ReadResponse, not one per Query: two selectors of 15 samples each fit individually
   * and must not both be served.
   */
  @Test
  void theCeilingIsSpreadAcrossEveryQueryOfOneReadRequest() throws Exception {
    final Query fits = new Query(0, 15_000L, List.of(new LabelMatcher(MatchType.EQ, "__name__", TYPE_NAME)));

    assertThat(postPromRead(new ReadRequest(List.of(fits))).getResponseCode())
        .as("15 samples fit under a ceiling of %d on their own", CEILING).isEqualTo(200);

    assertThat(postPromRead(new ReadRequest(List.of(fits, fits))).getResponseCode())
        .as("30 samples in ONE remote-read response is what the ceiling forbids")
        .isEqualTo(413);
  }

  /**
   * A label matcher narrows the fetch, so the budget counts what survives it and a selective read still fits.
   */
  @Test
  void aLabelMatcherNarrowsWhatTheBudgetIsChargedFor() throws Exception {
    final ReadResponse response = promReadOk(new ReadRequest(List.of(
        new Query(0, TOTAL_ROWS * 1000L, List.of(
            new LabelMatcher(MatchType.EQ, "__name__", TYPE_NAME),
            new LabelMatcher(MatchType.EQ, "host", "web1"))))));

    final int samples = response.getResults().getFirst().getTimeSeries().stream()
        .mapToInt(ts -> ts.getSamples().size()).sum();
    assertThat(samples).isEqualTo(TOTAL_ROWS / 2);
  }

  /**
   * A selector matching no series is a complete, empty answer and never a refusal.
   */
  @Test
  void anEmptySelectionIsServedRatherThanRefused() throws Exception {
    final ReadResponse response = promReadOk(new ReadRequest(List.of(
        new Query(0, TOTAL_ROWS * 1000L,
            List.of(new LabelMatcher(MatchType.EQ, "__name__", "no_such_metric"))))));

    assertThat(response.getResults()).hasSize(1);
    assertThat(response.getResults().getFirst().getTimeSeries()).isEmpty();
  }

  // ---------------------------------------------------------------------------------------------------
  // Plumbing
  // ---------------------------------------------------------------------------------------------------

  private JSONObject grafanaFrameOk(final JSONObject request) throws Exception {
    final HttpResponse<String> response = postGrafana(request);
    assertThat(response.statusCode()).isEqualTo(200);
    return new JSONObject(response.body()).getJSONObject("results").getJSONObject("A")
        .getJSONArray("frames").getJSONObject(0);
  }

  private HttpResponse<String> postGrafana(final JSONObject request) throws Exception {
    return client.send(HttpRequest.newBuilder()
        .uri(new URI(baseUrl() + "/api/v1/ts/" + getDatabaseName() + "/grafana/query"))
        .POST(HttpRequest.BodyPublishers.ofString(request.toString()))
        .setHeader("Content-Type", "application/json")
        .setHeader("Authorization", basicAuth())
        .build(), BodyHandlers.ofString());
  }

  private ReadResponse promReadOk(final ReadRequest request) throws Exception {
    final HttpURLConnection connection = postPromRead(request);
    assertThat(connection.getResponseCode()).isEqualTo(200);

    try (final InputStream is = connection.getInputStream()) {
      final ByteArrayOutputStream baos = new ByteArrayOutputStream();
      is.transferTo(baos);
      return ReadResponse.decode(Snappy.uncompress(baos.toByteArray()));
    }
  }

  private HttpURLConnection postPromRead(final ReadRequest request) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URI(
        baseUrl() + "/api/v1/ts/" + getDatabaseName() + "/prom/read").toURL().openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization", basicAuth());
    connection.setRequestProperty("Content-Type", "application/x-protobuf");
    connection.setRequestProperty("Content-Encoding", "snappy");
    connection.setDoOutput(true);

    try (final OutputStream os = connection.getOutputStream()) {
      os.write(Snappy.compress(request.encode()));
      os.flush();
    }
    return connection;
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
    return "Basic " + Base64.getEncoder()
        .encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8));
  }

  /**
   * The server's real port, never a hardcoded one: when 2480 is taken the server binds the next free port.
   */
  private String baseUrl() {
    return "http://127.0.0.1:" + getServer(0).getHttpServer().getPort();
  }
}
