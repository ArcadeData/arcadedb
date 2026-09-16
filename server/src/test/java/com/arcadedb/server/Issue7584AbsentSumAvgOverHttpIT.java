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
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7584, over HTTP: the SUM/AVG half of what {@code Issue7043AbsentMinMaxOverHttpIT} pins for MIN/MAX.
 * <p>
 * Issue #7089 put SUM and AVG under the NaN-as-absent policy, so a bucket that received no real sample now reads
 * back {@code NaN} where it used to read back {@code 0.0}. That is the change #7584 asked to be said out loud,
 * because it is what a Grafana panel renders: {@code JSONArray.put(Number)} answers JSON's lack of a NaN literal by
 * rewriting NaN to {@code 0}, so feeding the aggregate straight into a response array would draw a dip to zero
 * where the series actually has a gap - and for SUM that zero is especially plausible as data. Both time-series
 * response builders route through {@code putSampleValue} and emit JSON {@code null} instead; these tests are what
 * keeps that true for SUM and AVG.
 */
class Issue7584AbsentSumAvgOverHttpIT extends BaseGraphServerTest {

  private static final String TYPE = "nansummetric";

  @Test
  void anAllNaNBucketIsNullNotZeroForSumAndAvgOnBothEndpoints() throws Exception {
    testEachServer(serverIndex -> {
      createTypeWithOnlyNaNSamples(serverIndex);

      // --- /ts/<db>/query, aggregation branch ---
      final JSONArray tsValues = postJson(serverIndex, "/api/v1/ts/graph/query", aggregationRequest())
          .getJSONArray("buckets").getJSONObject(0).getJSONArray("values");
      assertThat(tsValues.isNull(0)).as("SUM over an all-NaN bucket must be JSON null, not 0").isTrue();
      assertThat(tsValues.isNull(1)).as("AVG over an all-NaN bucket must be JSON null, not 0").isTrue();

      // --- /ts/<db>/grafana/query, aggregation branch: columnar, timestamps first ---
      final JSONArray columns = grafanaColumns(serverIndex);
      assertThat(columns.getJSONArray(1).isNull(0)).as("Grafana SUM column must carry a gap, not a zero").isTrue();
      assertThat(columns.getJSONArray(2).isNull(0)).as("Grafana AVG column must carry a gap, not a zero").isTrue();
    });
  }

  /**
   * The null is the absent marker, not a blanket rule for SUM: a bucket whose real samples happen to cancel to
   * zero must still come back as the number {@code 0}. This is the distinction the whole policy exists to make,
   * and the one a {@code counts == 0} guard could never express.
   */
  @Test
  void aSumOfRealSamplesThatCancelsToZeroIsTheNumberZeroNotNull() throws Exception {
    testEachServer(serverIndex -> {
      createTypeWithOnlyNaNSamples(serverIndex);

      final Database database = getServerDatabase(serverIndex, getDatabaseName());
      final LocalTimeSeriesType tsType = (LocalTimeSeriesType) database.getSchema().getType(TYPE);
      database.begin();
      tsType.getEngine().appendSamples(new long[] { 3_000L, 4_000L }, new Object[] { 2.5, -2.5 });
      database.commit();

      final JSONArray values = postJson(serverIndex, "/api/v1/ts/graph/query", aggregationRequest())
          .getJSONArray("buckets").getJSONObject(0).getJSONArray("values");
      assertThat(values.isNull(0)).as("a real total of zero is data, not a gap").isFalse();
      assertThat(((Number) values.get(0)).doubleValue()).isEqualTo(0.0);
      // The two NaN samples are skipped, so AVG divides by the 2 real ones rather than by 4.
      assertThat(((Number) values.get(1)).doubleValue()).isEqualTo(0.0);
    });
  }

  /**
   * And a populated SUM/AVG still answers real numbers, with the absent samples skipped rather than propagated.
   */
  @Test
  void realSamplesStillAnswerNumbersWithTheAbsentOnesSkipped() throws Exception {
    testEachServer(serverIndex -> {
      createTypeWithOnlyNaNSamples(serverIndex);

      final Database database = getServerDatabase(serverIndex, getDatabaseName());
      final LocalTimeSeriesType tsType = (LocalTimeSeriesType) database.getSchema().getType(TYPE);
      database.begin();
      tsType.getEngine().appendSamples(new long[] { 3_000L, 4_000L }, new Object[] { 4.0, 6.0 });
      database.commit();

      final JSONArray values = postJson(serverIndex, "/api/v1/ts/graph/query", aggregationRequest())
          .getJSONArray("buckets").getJSONObject(0).getJSONArray("values");
      assertThat(((Number) values.get(0)).doubleValue()).as("SUM of the real samples only").isEqualTo(10.0);
      assertThat(((Number) values.get(1)).doubleValue()).as("AVG divides by the 2 real samples, not by 4")
          .isEqualTo(5.0);

      final JSONArray columns = grafanaColumns(serverIndex);
      assertThat(((Number) columns.getJSONArray(1).get(0)).doubleValue()).isEqualTo(10.0);
      assertThat(((Number) columns.getJSONArray(2).get(0)).doubleValue()).isEqualTo(5.0);
    });
  }

  /**
   * The columnar Grafana frame for {@link #TYPE}: column 0 is the timestamps, the aggregations follow in request
   * order.
   */
  private JSONArray grafanaColumns(final int serverIndex) throws Exception {
    final JSONArray targets = new JSONArray();
    final JSONObject target = new JSONObject();
    target.put("refId", "A");
    target.put("type", TYPE);
    target.put("aggregation", aggregationRequest().getJSONObject("aggregation"));
    targets.put(target);

    final JSONObject grafana = new JSONObject();
    grafana.put("from", 0L);
    grafana.put("to", 10_000L);
    grafana.put("targets", targets);

    return postJson(serverIndex, "/api/v1/ts/graph/grafana/query", grafana)
        .getJSONObject("results").getJSONObject("A").getJSONArray("frames")
        .getJSONObject(0).getJSONObject("data").getJSONArray("values");
  }

  private JSONObject aggregationRequest() {
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
    payload.put("type", TYPE);
    payload.put("from", 0L);
    payload.put("to", 10_000L);
    payload.put("aggregation", aggregation);
    return payload;
  }

  /**
   * A NaN sample has no line-protocol or SQL literal, so the samples go in through the server's own database.
   */
  private void createTypeWithOnlyNaNSamples(final int serverIndex) throws Exception {
    final Database database = getServerDatabase(serverIndex, getDatabaseName());
    if (database.getSchema().existsType(TYPE))
      database.getSchema().dropType(TYPE);

    database.command("sql", "CREATE TIMESERIES TYPE " + TYPE + " TIMESTAMP ts FIELDS (value DOUBLE) SHARDS 1");

    final LocalTimeSeriesType tsType = (LocalTimeSeriesType) database.getSchema().getType(TYPE);
    database.begin();
    tsType.getEngine().appendSamples(new long[] { 1_000L, 2_000L }, new Object[] { Double.NaN, Double.NaN });
    database.commit();
  }

  private JSONObject postJson(final int serverIndex, final String path, final JSONObject body) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URI("http://127.0.0.1:248" + serverIndex + path)
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
}
