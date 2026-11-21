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
package com.arcadedb.server;

import com.arcadedb.database.Database;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.Label;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.LabelMatcher;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.MatchType;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.Query;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.QueryResult;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.ReadRequest;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.ReadResponse;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.Sample;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.TimeSeries;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.WriteRequest;
import org.junit.jupiter.api.Test;
import org.xerial.snappy.Snappy;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The time-series HTTP endpoints ({@code /api/v1/ts/{database}/...}): the native and Grafana query endpoints, the line
 * protocol write and the Prometheus remote write/read. Absent aggregates must reach a client as JSON {@code null} rather than
 * a fake {@code 0}, the bucket grid must be movable, and the idempotency key must bind to a binary body.
 * <p>
 * Each group of tests below keeps the explanation of the issue it was written for.
 */
class TimeSeriesHttpIT extends BaseGraphServerTest {

  private static final String MIN_MAX_TYPE = "nanmetric";
  private static final String SUM_AVG_TYPE = "nansummetric";

  /*
   * Issue #7043, over HTTP: an absent MIN/MAX must not reach a client as the number {@code 0}.
   * <p>
   * The engine now answers {@code NaN} - the absent marker - for a MIN/MAX bucket that received no real sample, and
   * {@code JSONArray.put(Number)} resolves JSON's lack of a NaN literal by rewriting NaN to {@code 0}. Feeding the
   * aggregate straight into a response array therefore turned "no measurement" into a measurement of zero:
   * indistinguishable from real data, and for a Grafana dashboard a dip to zero where the series has a gap. Both
   * time-series response builders emit JSON {@code null} instead.
   */

  // Regression: #7043 an absent MIN/MAX reaches the client as JSON null, not 0
  @Test
  void anAllNaNBucketIsNullNotZeroOnTheTimeSeriesAndGrafanaEndpoints() throws Exception {
    testEachServer(serverIndex -> {
      createTypeWithOnlyNaNSamples(serverIndex, MIN_MAX_TYPE);

      // --- /ts/<db>/query, aggregation branch ---
      final JSONObject tsBucket = postJson(serverIndex, "/api/v1/ts/graph/query", minMaxAggregationRequest())
          .getJSONArray("buckets").getJSONObject(0);
      final JSONArray tsValues = tsBucket.getJSONArray("values");
      assertThat(tsValues.isNull(0)).as("MIN over an all-NaN bucket must be JSON null, not 0").isTrue();
      assertThat(tsValues.isNull(1)).as("MAX over an all-NaN bucket must be JSON null, not 0").isTrue();

      // --- /ts/<db>/grafana/query, aggregation branch: columnar, timestamps first ---
      final JSONObject request = minMaxAggregationRequest();
      request.put("type", MIN_MAX_TYPE);
      final JSONArray targets = new JSONArray();
      final JSONObject target = new JSONObject();
      target.put("refId", "A");
      target.put("type", MIN_MAX_TYPE);
      target.put("aggregation", request.getJSONObject("aggregation"));
      targets.put(target);

      final JSONObject grafana = new JSONObject();
      grafana.put("from", 0L);
      grafana.put("to", 10_000L);
      grafana.put("targets", targets);

      final JSONArray frames = postJson(serverIndex, "/api/v1/ts/graph/grafana/query", grafana)
          .getJSONObject("results").getJSONObject("A").getJSONArray("frames");
      final JSONArray columns = frames.getJSONObject(0).getJSONObject("data").getJSONArray("values");

      // Column 0 is the timestamps; the aggregations follow in request order.
      assertThat(columns.getJSONArray(1).isNull(0)).as("Grafana MIN column must carry a gap, not a zero").isTrue();
      assertThat(columns.getJSONArray(2).isNull(0)).as("Grafana MAX column must carry a gap, not a zero").isTrue();
    });
  }

  // Regression: #7043 an absent MIN/MAX reaches the client as JSON null, not 0
  /**
   * The RAW (non-aggregated) branches carry the same exposure: a NaN sample there means the same "no
   * measurement", and JSON has no literal for it either way.
   */
  @Test
  void aRawNaNSampleIsNullNotZeroOnBothEndpoints() throws Exception {
    testEachServer(serverIndex -> {
      createTypeWithOnlyNaNSamples(serverIndex, MIN_MAX_TYPE);

      // --- /ts/<db>/query, raw branch: rows of [ts, value] ---
      final JSONObject raw = new JSONObject();
      raw.put("type", MIN_MAX_TYPE);
      raw.put("from", 0L);
      raw.put("to", 10_000L);
      final JSONArray rows = postJson(serverIndex, "/api/v1/ts/graph/query", raw).getJSONArray("rows");
      assertThat(rows.length()).isEqualTo(2);
      assertThat(rows.getJSONArray(0).isNull(1)).as("a raw NaN sample must be JSON null, not 0").isTrue();

      // --- /ts/<db>/grafana/query, raw branch: columnar, value column last ---
      final JSONArray targets = new JSONArray();
      final JSONObject target = new JSONObject();
      target.put("refId", "A");
      target.put("type", MIN_MAX_TYPE);
      targets.put(target);

      final JSONObject grafana = new JSONObject();
      grafana.put("from", 0L);
      grafana.put("to", 10_000L);
      grafana.put("targets", targets);

      final JSONArray columns = postJson(serverIndex, "/api/v1/ts/graph/grafana/query", grafana)
          .getJSONObject("results").getJSONObject("A").getJSONArray("frames")
          .getJSONObject(0).getJSONObject("data").getJSONArray("values");
      final JSONArray valueColumn = columns.getJSONArray(columns.length() - 1);
      assertThat(valueColumn.isNull(0)).as("Grafana raw column must carry a gap, not a zero").isTrue();
    });
  }

  // Regression: #7043 an absent MIN/MAX reaches the client as JSON null, not 0
  /**
   * The same endpoint must still return real numbers: the null is the absent marker, not a blanket rule.
   */
  @Test
  void aRealValueIsStillReturnedAsANumber() throws Exception {
    testEachServer(serverIndex -> {
      createTypeWithOnlyNaNSamples(serverIndex, MIN_MAX_TYPE);

      final Database database = getServerDatabase(serverIndex, getDatabaseName());
      final LocalTimeSeriesType tsType = (LocalTimeSeriesType) database.getSchema().getType(MIN_MAX_TYPE);
      database.begin();
      tsType.getEngine().appendSamples(new long[] { 3_000L }, new Object[] { 4.25 });
      database.commit();

      final JSONArray values = postJson(serverIndex, "/api/v1/ts/graph/query", minMaxAggregationRequest())
          .getJSONArray("buckets").getJSONObject(0).getJSONArray("values");
      assertThat(values.isNull(0)).isFalse();
      assertThat(((Number) values.get(0)).doubleValue()).isEqualTo(4.25);
      assertThat(((Number) values.get(1)).doubleValue()).isEqualTo(4.25);
    });
  }

  private JSONObject minMaxAggregationRequest() {
    final JSONArray requests = new JSONArray();
    final JSONObject min = new JSONObject();
    min.put("field", "value");
    min.put("type", "MIN");
    min.put("alias", "min");
    requests.put(min);
    final JSONObject max = new JSONObject();
    max.put("field", "value");
    max.put("type", "MAX");
    max.put("alias", "max");
    requests.put(max);

    final JSONObject aggregation = new JSONObject();
    // One bucket wide enough to hold every sample, so the answer is a single bucket to assert on.
    aggregation.put("bucketInterval", 1_000_000L);
    aggregation.put("requests", requests);

    final JSONObject payload = new JSONObject();
    payload.put("type", MIN_MAX_TYPE);
    payload.put("from", 0L);
    payload.put("to", 10_000L);
    payload.put("aggregation", aggregation);
    return payload;
  }

  private JSONObject postJson(final int serverIndex, final String path, final JSONObject body) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URI("http://127.0.0.1:" + getServerHttpPort(serverIndex) + path)
        .toURL().openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    connection.setRequestProperty("Content-Type", "application/json");
    connection.setDoOutput(true);

    try (final OutputStream os = connection.getOutputStream()) {
      os.write(body.toString().getBytes(StandardCharsets.UTF_8));
      os.flush();
    }

    assertThat(connection.getResponseCode()).isEqualTo(200);
    try (final InputStream is = connection.getInputStream()) {
      return new JSONObject(new String(is.readAllBytes(), StandardCharsets.UTF_8));
    }
  }

  /*
   * Issue #7584, over HTTP: the SUM/AVG half of what the #7043 tests above pin for MIN/MAX.
   * <p>
   * Issue #7089 put SUM and AVG under the NaN-as-absent policy, so a bucket that received no real sample now reads
   * back {@code NaN} where it used to read back {@code 0.0}. That is the change #7584 asked to be said out loud,
   * because it is what a Grafana panel renders: {@code JSONArray.put(Number)} answers JSON's lack of a NaN literal by
   * rewriting NaN to {@code 0}, so feeding the aggregate straight into a response array would draw a dip to zero
   * where the series actually has a gap - and for SUM that zero is especially plausible as data. Both time-series
   * response builders route through {@code putSampleValue} and emit JSON {@code null} instead; these tests are what
   * keeps that true for SUM and AVG.
   */

  // Regression: #7584 an absent SUM/AVG reaches the client as JSON null, not 0
  @Test
  void anAllNaNBucketIsNullNotZeroForSumAndAvgOnBothEndpoints() throws Exception {
    testEachServer(serverIndex -> {
      createTypeWithOnlyNaNSamples(serverIndex, SUM_AVG_TYPE);

      // --- /ts/<db>/query, aggregation branch ---
      final JSONArray tsValues = postJson(serverIndex, "/api/v1/ts/graph/query", sumAvgAggregationRequest())
          .getJSONArray("buckets").getJSONObject(0).getJSONArray("values");
      assertThat(tsValues.isNull(0)).as("SUM over an all-NaN bucket must be JSON null, not 0").isTrue();
      assertThat(tsValues.isNull(1)).as("AVG over an all-NaN bucket must be JSON null, not 0").isTrue();

      // --- /ts/<db>/grafana/query, aggregation branch: columnar, timestamps first ---
      final JSONArray columns = sumAvgGrafanaColumns(serverIndex);
      assertThat(columns.getJSONArray(1).isNull(0)).as("Grafana SUM column must carry a gap, not a zero").isTrue();
      assertThat(columns.getJSONArray(2).isNull(0)).as("Grafana AVG column must carry a gap, not a zero").isTrue();
    });
  }

  // Regression: #7584 an absent SUM/AVG reaches the client as JSON null, not 0
  /**
   * The null is the absent marker, not a blanket rule for SUM: a bucket whose real samples happen to cancel to
   * zero must still come back as the number {@code 0}. This is the distinction the whole policy exists to make,
   * and the one a {@code counts == 0} guard could never express.
   */
  @Test
  void aSumOfRealSamplesThatCancelsToZeroIsTheNumberZeroNotNull() throws Exception {
    testEachServer(serverIndex -> {
      createTypeWithOnlyNaNSamples(serverIndex, SUM_AVG_TYPE);

      final Database database = getServerDatabase(serverIndex, getDatabaseName());
      final LocalTimeSeriesType tsType = (LocalTimeSeriesType) database.getSchema().getType(SUM_AVG_TYPE);
      database.begin();
      tsType.getEngine().appendSamples(new long[] { 3_000L, 4_000L }, new Object[] { 2.5, -2.5 });
      database.commit();

      final JSONArray values = postJson(serverIndex, "/api/v1/ts/graph/query", sumAvgAggregationRequest())
          .getJSONArray("buckets").getJSONObject(0).getJSONArray("values");
      assertThat(values.isNull(0)).as("a real total of zero is data, not a gap").isFalse();
      assertThat(((Number) values.get(0)).doubleValue()).isEqualTo(0.0);
      // The two NaN samples are skipped, so AVG divides by the 2 real ones rather than by 4.
      assertThat(((Number) values.get(1)).doubleValue()).isEqualTo(0.0);
    });
  }

  // Regression: #7584 an absent SUM/AVG reaches the client as JSON null, not 0
  /**
   * And a populated SUM/AVG still answers real numbers, with the absent samples skipped rather than propagated.
   */
  @Test
  void realSamplesStillAnswerNumbersWithTheAbsentOnesSkipped() throws Exception {
    testEachServer(serverIndex -> {
      createTypeWithOnlyNaNSamples(serverIndex, SUM_AVG_TYPE);

      final Database database = getServerDatabase(serverIndex, getDatabaseName());
      final LocalTimeSeriesType tsType = (LocalTimeSeriesType) database.getSchema().getType(SUM_AVG_TYPE);
      database.begin();
      tsType.getEngine().appendSamples(new long[] { 3_000L, 4_000L }, new Object[] { 4.0, 6.0 });
      database.commit();

      final JSONArray values = postJson(serverIndex, "/api/v1/ts/graph/query", sumAvgAggregationRequest())
          .getJSONArray("buckets").getJSONObject(0).getJSONArray("values");
      assertThat(((Number) values.get(0)).doubleValue()).as("SUM of the real samples only").isEqualTo(10.0);
      assertThat(((Number) values.get(1)).doubleValue()).as("AVG divides by the 2 real samples, not by 4")
          .isEqualTo(5.0);

      final JSONArray columns = sumAvgGrafanaColumns(serverIndex);
      assertThat(((Number) columns.getJSONArray(1).get(0)).doubleValue()).isEqualTo(10.0);
      assertThat(((Number) columns.getJSONArray(2).get(0)).doubleValue()).isEqualTo(5.0);
    });
  }

  /**
   * The columnar Grafana frame for {@link #SUM_AVG_TYPE}: column 0 is the timestamps, the aggregations follow in request
   * order.
   */
  private JSONArray sumAvgGrafanaColumns(final int serverIndex) throws Exception {
    final JSONArray targets = new JSONArray();
    final JSONObject target = new JSONObject();
    target.put("refId", "A");
    target.put("type", SUM_AVG_TYPE);
    target.put("aggregation", sumAvgAggregationRequest().getJSONObject("aggregation"));
    targets.put(target);

    final JSONObject grafana = new JSONObject();
    grafana.put("from", 0L);
    grafana.put("to", 10_000L);
    grafana.put("targets", targets);

    return postJson(serverIndex, "/api/v1/ts/graph/grafana/query", grafana)
        .getJSONObject("results").getJSONObject("A").getJSONArray("frames")
        .getJSONObject(0).getJSONObject("data").getJSONArray("values");
  }

  private JSONObject sumAvgAggregationRequest() {
    final JSONArray requests = new JSONArray();
    final JSONObject sum = new JSONObject();
    sum.put("field", "value");
    sum.put("type", "SUM");
    sum.put("alias", "sum");
    requests.put(sum);
    final JSONObject avg = new JSONObject();
    avg.put("field", "value");
    avg.put("type", "AVG");
    avg.put("alias", "avg");
    requests.put(avg);

    final JSONObject aggregation = new JSONObject();
    // One bucket wide enough to hold every sample, so the answer is a single bucket to assert on.
    aggregation.put("bucketInterval", 1_000_000L);
    aggregation.put("requests", requests);

    final JSONObject payload = new JSONObject();
    payload.put("type", SUM_AVG_TYPE);
    payload.put("from", 0L);
    payload.put("to", 10_000L);
    payload.put("aggregation", aggregation);
    return payload;
  }

  /*
   * #8798: the native aggregation endpoint buckets from the Unix epoch, a Thursday, so a one-week bucket started on
   * Thursday and a one-day bucket at 08:00 in UTC+8. {@code aggregation.bucketOrigin} moves the grid, and it must be the
   * grid {@code ts.timeBucket} uses.
   */

  private static final long DAY  = 86_400_000L;

  private static final long WEEK = 7 * DAY;

  private static long ms(final String iso) {
    return Instant.parse(iso).toEpochMilli();
  }

  // Regression: #8798 aggregation.bucketOrigin moves the bucket grid like ts.timeBucket does
  @Test
  void bucketOriginMovesTheGrid() throws Exception {
    testEachServer(serverIndex -> {
      command(serverIndex, "CREATE TIMESERIES TYPE energy TIMESTAMP ts TAGS (site STRING) FIELDS (kwh DOUBLE)");

      // Sun 2025-06-15 18:30 UTC (a Thursday-grid week of 2025-06-12), Mon 2025-06-16 01:00, Tue 2025-06-10 12:00
      final String lineProtocol = "energy,site=a kwh=1.0 " + ms("2025-06-15T18:30:00Z") + "\n"
          + "energy,site=a kwh=2.0 " + ms("2025-06-16T01:00:00Z") + "\n"
          + "energy,site=a kwh=4.0 " + ms("2025-06-10T12:00:00Z") + "\n";
      assertThat(postLineProtocol(serverIndex, lineProtocol)).isEqualTo(204);

      // default grid: Thursday weeks
      JSONArray buckets = aggregate(serverIndex, WEEK, null).getJSONArray("buckets");
      assertThat(buckets.length()).isEqualTo(2);
      assertThat(buckets.getJSONObject(0).getLong("timestamp")).isEqualTo(ms("2025-06-05T00:00:00Z"));
      assertThat(buckets.getJSONObject(1).getLong("timestamp")).isEqualTo(ms("2025-06-12T00:00:00Z"));

      // Monday weeks
      buckets = aggregate(serverIndex, WEEK, ms("2024-01-01T00:00:00Z")).getJSONArray("buckets");
      assertThat(buckets.length()).isEqualTo(2);
      assertThat(buckets.getJSONObject(0).getLong("timestamp")).isEqualTo(ms("2025-06-09T00:00:00Z"));
      assertThat(buckets.getJSONObject(1).getLong("timestamp")).isEqualTo(ms("2025-06-16T00:00:00Z"));
      assertThat(buckets.getJSONObject(0).getJSONArray("values").getDouble(0)).isEqualTo(5.0);
      assertThat(buckets.getJSONObject(1).getJSONArray("values").getDouble(0)).isEqualTo(2.0);

      // local days in UTC+8: the origin is local midnight, which is 16:00 UTC of the day before
      buckets = aggregate(serverIndex, DAY, ms("2025-06-14T16:00:00Z")).getJSONArray("buckets");
      assertThat(buckets.getJSONObject(buckets.length() - 1).getLong("timestamp")).isEqualTo(ms("2025-06-15T16:00:00Z"));

      // the same query through SQL lands in the same buckets
      final JSONObject sql = new JSONObject(command(serverIndex,
          "SELECT ts.timeBucket('1w', ts, {'origin': '2024-01-01T00:00:00Z'}) AS wk, sum(kwh) AS s FROM energy GROUP BY wk ORDER BY wk"));
      assertThat(sql.getJSONArray("result").length()).isEqualTo(2);
    });
  }

  // Regression: #8798 aggregation.bucketOrigin moves the bucket grid like ts.timeBucket does
  @Test
  void bucketOriginMustBeANumber() throws Exception {
    testEachServer(serverIndex -> {
      command(serverIndex, "CREATE TIMESERIES TYPE energy TIMESTAMP ts TAGS (site STRING) FIELDS (kwh DOUBLE)");
      final JSONObject request = request(WEEK, null);
      request.getJSONObject("aggregation").put("bucketOrigin", "monday");
      assertThat(post(serverIndex, request).getResponseCode()).isEqualTo(400);
    });
  }

  private JSONObject request(final long interval, final Long origin) {
    final JSONObject req = new JSONObject();
    req.put("type", "energy");
    final JSONObject aggregation = new JSONObject();
    aggregation.put("bucketInterval", interval);
    if (origin != null)
      aggregation.put("bucketOrigin", origin);
    final JSONArray requests = new JSONArray();
    final JSONObject sum = new JSONObject();
    sum.put("field", "kwh");
    sum.put("type", "SUM");
    requests.put(sum);
    aggregation.put("requests", requests);
    req.put("aggregation", aggregation);
    return req;
  }

  private JSONObject aggregate(final int serverIndex, final long interval, final Long origin) throws Exception {
    final HttpURLConnection connection = post(serverIndex, request(interval, origin));
    assertThat(connection.getResponseCode()).isEqualTo(200);
    try (final InputStream is = connection.getInputStream()) {
      return new JSONObject(new String(is.readAllBytes(), StandardCharsets.UTF_8));
    }
  }

  private HttpURLConnection post(final int serverIndex, final JSONObject body) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URI(
        "http://127.0.0.1:" + getServerHttpPort(serverIndex) + "/api/v1/ts/graph/query").toURL().openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    connection.setRequestProperty("Content-Type", "application/json");
    connection.setDoOutput(true);
    try (final OutputStream os = connection.getOutputStream()) {
      os.write(body.toString().getBytes(StandardCharsets.UTF_8));
    }
    return connection;
  }

  private int postLineProtocol(final int serverIndex, final String body) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URI(
        "http://127.0.0.1:" + getServerHttpPort(serverIndex) + "/api/v1/ts/graph/write?precision=ms").toURL().openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    connection.setDoOutput(true);
    try (final OutputStream os = connection.getOutputStream()) {
      os.write(body.getBytes(StandardCharsets.UTF_8));
    }
    return connection.getResponseCode();
  }

  /*
   * Issue #7704: the idempotency key must bind to the request body on the BINARY routes too.
   * <p>
   * {@code AbstractServerHttpHandler.handleRequest} folds the body into the key precisely so a client reusing one
   * {@code X-Request-Id} across two distinct writes cannot be answered with the first write's cached response. What
   * it folded in was {@code payloadAsString}, and {@code AbstractBinaryHttpHandler.parseRequestPayload} returns
   * {@code null} for that BY CONSTRUCTION - the body is bytes, and {@code execute} reads them back off the exchange.
   * So on {@code POST /api/v1/ts/{database}/prom/write} and {@code .../prom/read} the key was the request id, the
   * method, the path and the database, and nothing else: two remote-write requests carrying DIFFERENT samples under
   * one correlation id hashed to the same key, the second was a cache hit replayed the first's {@code 204}, and its
   * samples were never appended. The very protection the body was added to the key to provide was absent on exactly
   * the routes whose body is not text.
   * <p>
   * Latent rather than live against Prometheus itself, which sends no {@code X-Request-Id}; reachable for any
   * collector or proxy that stamps a correlation id per connection, per scrape loop or per retry batch - which is
   * the usual reason to stamp one at all.
   * <p>
   * The text routes were never affected ({@code PostTimeSeriesWriteHandler.parseRequestPayload} returns the body),
   * and the last test here says so, so a future change that fixes the binary side by weakening the text side has
   * somewhere to fail.
   */

  private static final String SHARED_REQUEST_ID = "collector-connection-1";

  // Regression: #7704 the idempotency key binds to the body on the binary Prometheus routes too
  /**
   * The defect itself: two remote-write bodies, one correlation id, and both sets of samples must land.
   */
  @Test
  void twoRemoteWritesSharingOneRequestIdBothLand() throws Exception {
    assertThat(postPromWrite(writeOf("ts7704_first", 1.5, 100_001L), SHARED_REQUEST_ID)).isEqualTo(204);
    assertThat(postPromWrite(writeOf("ts7704_second", 2.5, 100_002L), SHARED_REQUEST_ID))
        .as("a second, DIFFERENT body under the same correlation id is not a retry of the first")
        .isEqualTo(204);

    assertThat(storedSamples("ts7704_first", "ts7704_second"))
        .as("#7704: with the body out of the key the second write is a cache hit replayed the first's 204, and "
            + "its samples are never appended")
        .containsExactlyInAnyOrder("ts7704_first=1.5", "ts7704_second=2.5");
  }

  // Regression: #7704 the idempotency key binds to the body on the binary Prometheus routes too
  /**
   * The other half of the contract, which the fix must not break: the SAME body under the same correlation id is
   * a genuine retry and must still be answered from the cache rather than appended twice. Without this the fix
   * could be "never cache a binary route", which would take the replay protection away instead of correcting it.
   */
  @Test
  void anIdenticalRemoteWriteRetryIsStillDeduplicated() throws Exception {
    final WriteRequest body = writeOf("ts7704_retried", 7.5, 100_003L);

    assertThat(postPromWrite(body, "retry-batch-9")).isEqualTo(204);
    assertThat(postPromWrite(body, "retry-batch-9")).isEqualTo(204);

    assertThat(storedSamples("ts7704_retried"))
        .as("a byte-identical retry under the same id is the case the idempotency cache exists for")
        .containsExactly("ts7704_retried=7.5");
  }

  // Regression: #7704 the idempotency key binds to the body on the binary Prometheus routes too
  /**
   * {@code remote_read} shares the base class, and two concurrent reads under one correlation id must each come
   * back with THEIR OWN series.
   * <p>
   * <b>What this test can and cannot detect, stated plainly so the next reader does not assume more of it.</b>
   * {@code PostPrometheusReadHandler} writes its response itself and returns {@code null}, which ABORTS the
   * reservation, so the route never populates the cache and nothing can be replayed from it - the same shape a
   * streaming route has (#7311). What the body-agnostic key COULD still do is make two concurrent reads collide
   * on a single key, the second finding the first in flight and waiting for a result that was never going to be
   * its own. That collision is not reachable from a test: a remote read of a handful of samples finishes and
   * aborts its reservation in microseconds, long before a second HTTP request gets to the handler. A timing
   * assertion on the pair was written and MEASURED against the unfixed key here - it passed, i.e. it separated
   * nothing - so it was removed rather than shipped as a bound that never fires. Manufacturing the window (a
   * six-figure-sample first read) would be engineering a delay to make a bound separate, for a latent collision
   * on a route that does not cache.
   * <p>
   * The deterministic proof that these two requests are two keys is
   * {@code IdempotencyKeyTest.sameRequestIdDifferentBinaryBodyProducesDifferentKey}, which needs no server and no
   * clock. This test is the end-to-end guard beside it: it runs the reads concurrently, so a future change making
   * this route return an {@code ExecutionResponse} - which WOULD cache, and would replay one caller's series to
   * another - lands on an assertion rather than on a Grafana panel.
   */
  @Test
  void twoConcurrentRemoteReadsSharingOneRequestIdEachAnswerTheirOwnQuery() throws Exception {
    assertThat(postPromWrite(writeOf("ts7704_read_a", 3.5, 100_004L), null)).isEqualTo(204);
    assertThat(postPromWrite(writeOf("ts7704_read_b", 4.5, 100_005L), null)).isEqualTo(204);

    final String[] metrics = { "ts7704_read_a", "ts7704_read_b" };
    final CyclicBarrier releaseTogether = new CyclicBarrier(metrics.length);
    final ExecutorService pool = Executors.newFixedThreadPool(metrics.length);
    final List<String> answers;
    try {
      final List<Callable<String>> calls = new ArrayList<>(metrics.length);
      for (final String metric : metrics)
        calls.add(() -> {
          releaseTogether.await(30, TimeUnit.SECONDS);
          return readBack(metric, SHARED_REQUEST_ID);
        });

      answers = new ArrayList<>(metrics.length);
      for (final Future<String> answered : pool.invokeAll(calls, 120, TimeUnit.SECONDS))
        answers.add(answered.get());
    } finally {
      pool.shutdownNow();
      assertThat(pool.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
    }

    assertThat(answers)
        .as("#7704: each concurrent read must be answered from its OWN body")
        .containsExactlyInAnyOrder("ts7704_read_a -> 3.5@100004", "ts7704_read_b -> 4.5@100005");
  }

  // ---- Helpers ----

  private static WriteRequest writeOf(final String metric, final double value, final long timestampMs) {
    return new WriteRequest(List.of(new TimeSeries(
        List.of(new Label("__name__", metric)),
        List.of(new Sample(value, timestampMs)))));
  }

  /** Every sample of the named metrics in the database, as {@code metric=value}, read back embedded. */
  private List<String> storedSamples(final String... metrics) {
    final Database database = getServer(0).getDatabaseForOperator(getDatabaseName());
    final List<String> found = new ArrayList<>();
    for (final String type : metrics) {
      if (!database.getSchema().existsType(type))
        continue;
      final Iterator<Result> records = database.query("sql", "SELECT FROM " + type);
      while (records.hasNext())
        found.add(type + "=" + records.next().getProperty("value"));
    }
    return found;
  }

  private String readBack(final String metric, final String requestId) throws Exception {
    final ReadResponse response = postPromRead(new ReadRequest(List.of(
        new Query(0, 200_000, List.of(new LabelMatcher(MatchType.EQ, "__name__", metric))))), requestId);

    final List<QueryResult> results = response.getResults();
    if (results.size() != 1 || results.getFirst().getTimeSeries().size() != 1)
      return metric + " -> " + results.size() + " result(s), "
          + (results.isEmpty() ? "-" : results.getFirst().getTimeSeries().size()) + " series";

    final Sample sample = results.getFirst().getTimeSeries().getFirst().getSamples().getFirst();
    return metric + " -> " + sample.value() + "@" + sample.timestampMs();
  }

  private int postPromWrite(final WriteRequest writeRequest, final String requestId) throws Exception {
    final HttpURLConnection connection = createPromConnection("prom/write", requestId);
    connection.setDoOutput(true);
    try (final OutputStream os = connection.getOutputStream()) {
      os.write(Snappy.compress(writeRequest.encode()));
      os.flush();
    }
    return connection.getResponseCode();
  }

  private ReadResponse postPromRead(final ReadRequest readRequest, final String requestId) throws Exception {
    final HttpURLConnection connection = createPromConnection("prom/read", requestId);
    connection.setDoOutput(true);
    try (final OutputStream os = connection.getOutputStream()) {
      os.write(Snappy.compress(readRequest.encode()));
      os.flush();
    }

    assertThat(connection.getResponseCode()).isEqualTo(200);

    try (final InputStream is = connection.getInputStream()) {
      final ByteArrayOutputStream baos = new ByteArrayOutputStream();
      final byte[] buf = new byte[4096];
      int n;
      while ((n = is.read(buf)) != -1)
        baos.write(buf, 0, n);
      return ReadResponse.decode(Snappy.uncompress(baos.toByteArray()));
    }
  }

  private HttpURLConnection createPromConnection(final String path, final String requestId) throws Exception {
    final HttpURLConnection connection = createConnection("ts/" + getDatabaseName() + "/" + path, requestId);
    connection.setRequestProperty("Content-Type", "application/x-protobuf");
    connection.setRequestProperty("Content-Encoding", "snappy");
    return connection;
  }

  /** The port the server actually bound: a stranger already on 2480 pushes this one up the configured range. */
  private HttpURLConnection createConnection(final String path, final String requestId) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URI(
        "http://127.0.0.1:" + getServer(0).getHttpServer().getPort() + "/api/v1/" + path)
        .toURL()
        .openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    if (requestId != null)
      connection.setRequestProperty("X-Request-Id", requestId);
    return connection;
  }

  /**
   * A NaN sample has no line-protocol or SQL literal, so the samples go in through the server's own database.
   */
  private void createTypeWithOnlyNaNSamples(final int serverIndex, final String typeName) throws Exception {
    final Database database = getServerDatabase(serverIndex, getDatabaseName());
    if (database.getSchema().existsType(typeName))
      database.getSchema().dropType(typeName);

    database.command("sql", "CREATE TIMESERIES TYPE " + typeName + " TIMESTAMP ts FIELDS (value DOUBLE) SHARDS 1");

    final LocalTimeSeriesType tsType = (LocalTimeSeriesType) database.getSchema().getType(typeName);
    database.begin();
    tsType.getEngine().appendSamples(new long[] { 1_000L, 2_000L }, new Object[] { Double.NaN, Double.NaN });
    database.commit();
  }
}
