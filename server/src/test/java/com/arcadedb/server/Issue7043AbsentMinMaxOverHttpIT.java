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
 * Issue #7043, over HTTP: an absent MIN/MAX must not reach a client as the number {@code 0}.
 * <p>
 * The engine now answers {@code NaN} - the absent marker - for a MIN/MAX bucket that received no real sample, and
 * {@code JSONArray.put(Number)} resolves JSON's lack of a NaN literal by rewriting NaN to {@code 0}. Feeding the
 * aggregate straight into a response array therefore turned "no measurement" into a measurement of zero:
 * indistinguishable from real data, and for a Grafana dashboard a dip to zero where the series has a gap. Both
 * time-series response builders emit JSON {@code null} instead.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7043AbsentMinMaxOverHttpIT extends BaseGraphServerTest {

  private static final String TYPE = "nanmetric";

  @Test
  void anAllNaNBucketIsNullNotZeroOnTheTimeSeriesAndGrafanaEndpoints() throws Exception {
    testEachServer(serverIndex -> {
      createTypeWithOnlyNaNSamples(serverIndex);

      // --- /ts/<db>/query, aggregation branch ---
      final JSONObject tsBucket = postJson(serverIndex, "/api/v1/ts/graph/query", aggregationRequest())
          .getJSONArray("buckets").getJSONObject(0);
      final JSONArray tsValues = tsBucket.getJSONArray("values");
      assertThat(tsValues.isNull(0)).as("MIN over an all-NaN bucket must be JSON null, not 0").isTrue();
      assertThat(tsValues.isNull(1)).as("MAX over an all-NaN bucket must be JSON null, not 0").isTrue();

      // --- /ts/<db>/grafana/query, aggregation branch: columnar, timestamps first ---
      final JSONObject request = aggregationRequest();
      request.put("type", TYPE);
      final JSONArray targets = new JSONArray();
      final JSONObject target = new JSONObject();
      target.put("refId", "A");
      target.put("type", TYPE);
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

  /**
   * The RAW (non-aggregated) branches carry the same exposure: a NaN sample there means the same "no
   * measurement", and JSON has no literal for it either way.
   */
  @Test
  void aRawNaNSampleIsNullNotZeroOnBothEndpoints() throws Exception {
    testEachServer(serverIndex -> {
      createTypeWithOnlyNaNSamples(serverIndex);

      // --- /ts/<db>/query, raw branch: rows of [ts, value] ---
      final JSONObject raw = new JSONObject();
      raw.put("type", TYPE);
      raw.put("from", 0L);
      raw.put("to", 10_000L);
      final JSONArray rows = postJson(serverIndex, "/api/v1/ts/graph/query", raw).getJSONArray("rows");
      assertThat(rows.length()).isEqualTo(2);
      assertThat(rows.getJSONArray(0).isNull(1)).as("a raw NaN sample must be JSON null, not 0").isTrue();

      // --- /ts/<db>/grafana/query, raw branch: columnar, value column last ---
      final JSONArray targets = new JSONArray();
      final JSONObject target = new JSONObject();
      target.put("refId", "A");
      target.put("type", TYPE);
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

  /**
   * The same endpoint must still return real numbers: the null is the absent marker, not a blanket rule.
   */
  @Test
  void aRealValueIsStillReturnedAsANumber() throws Exception {
    testEachServer(serverIndex -> {
      createTypeWithOnlyNaNSamples(serverIndex);

      final Database database = getServerDatabase(serverIndex, getDatabaseName());
      final LocalTimeSeriesType tsType = (LocalTimeSeriesType) database.getSchema().getType(TYPE);
      database.begin();
      tsType.getEngine().appendSamples(new long[] { 3_000L }, new Object[] { 4.25 });
      database.commit();

      final JSONArray values = postJson(serverIndex, "/api/v1/ts/graph/query", aggregationRequest())
          .getJSONArray("buckets").getJSONObject(0).getJSONArray("values");
      assertThat(values.isNull(0)).isFalse();
      assertThat(((Number) values.get(0)).doubleValue()).isEqualTo(4.25);
      assertThat(((Number) values.get(1)).doubleValue()).isEqualTo(4.25);
    });
  }

  private JSONObject aggregationRequest() {
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
