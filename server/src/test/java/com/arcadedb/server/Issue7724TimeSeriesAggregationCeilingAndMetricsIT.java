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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.search.Search;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The HTTP half of issues #7724 and #7717, on the two aggregation surfaces that answer over HTTP.
 * <p>
 * #7724: the ceiling on the number of buckets a response may carry was enforced on the RESULT, so a request with
 * a tiny bucket interval over a wide range paid the whole scan and the whole bucket allocation and was refused
 * afterwards. The bound is now carried into the scan. What must not change is the REFUSAL - the same status and
 * the same wording, naming the setting that decided - because that is the contract a caller reads; what changes
 * is only its price, which the engine test {@code Issue7724AggregationBucketCeilingTest} pins on block counts.
 * <p>
 * #7717: every {@code /ts/**} handler passed {@code null} where the engine takes its read counters, so nothing
 * an operator can see recorded whether the sealed push-downs were working. This asserts they now reach the
 * sink for a real request over a real server, which is the verification the issue asks for.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7724TimeSeriesAggregationCeilingAndMetricsIT extends BaseGraphServerTest {

  private static final String TYPE          = "ceilingmetric";
  private static final int    MAX_ROWS      = 20;
  private static final int    SAMPLES       = 200;
  private static final long   BUCKET        = 1_000L;

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    // A ceiling low enough that a one-bucket-per-sample request is refused, and the metrics subsystem on so the
    // read counters have somewhere to go.
    config.setValue(GlobalConfiguration.SERVER_HTTP_QUERY_MAX_RESULT_ROWS, MAX_ROWS);
    config.setValue(GlobalConfiguration.SERVER_METRICS, true);
  }

  @Override
  protected int getServerCount() {
    return 1;
  }

  /**
   * One bucket per sample over 200 samples, against a ceiling of 20: refused, with the wording unchanged. The
   * request is the cheap abusive shape - a small bucket interval over the whole range - and it is exactly the
   * one the bound now stops early.
   */
  @Test
  void anOversizedBucketCountIsStillRefusedInTheSameWords() throws Exception {
    createSealedType(0);

    final Response refused = post(0, "/api/v1/ts/graph/query", aggregationPayload(BUCKET));

    assertThat(refused.status).as("the ceiling answers 413, as it does on every other read surface").isEqualTo(413);
    assertThat(refused.body)
        .as("the refusal has to name the setting that decided, or an operator cannot act on it")
        .contains(GlobalConfiguration.SERVER_HTTP_QUERY_MAX_RESULT_ROWS.getKey());

    // And the Grafana aggregation branch, which had no ceiling of its own at all, now refuses the same request.
    final Response grafana = post(0, "/api/v1/ts/graph/grafana/query", grafanaPayload(BUCKET));
    assertThat(grafana.status).isEqualTo(413);
    assertThat(grafana.body).contains(GlobalConfiguration.SERVER_HTTP_QUERY_MAX_RESULT_ROWS.getKey());
  }

  /**
   * The counter-case that keeps the refusal above meaningful: the same data with an interval wide enough to
   * bring the bucket count under the ceiling is answered in full. A bound that refused both would be a bug, not
   * a ceiling.
   */
  @Test
  void aRequestUnderTheCeilingIsAnsweredInFull() throws Exception {
    createSealedType(0);

    // Ten buckets over the whole range, comfortably under MAX_ROWS.
    final Response answered = post(0, "/api/v1/ts/graph/query", aggregationPayload(SAMPLES * BUCKET / 10));

    assertThat(answered.status).isEqualTo(200);
    final JSONObject body = new JSONObject(answered.body);
    assertThat(body.getJSONArray("buckets").length()).isPositive().isLessThanOrEqualTo(MAX_ROWS);
    assertThat(body.getInt("count")).isEqualTo(body.getJSONArray("buckets").length());
  }

  /**
   * Issue #7717: the read counters reach the sink. The type's samples are sealed, so the read has blocks to
   * decide about, and the meter is tagged with the database, the type and the endpoint - which is what makes the
   * ratio readable per tenant rather than as one server-wide number.
   */
  @Test
  void theReadCountersReachTheMetricsSink() throws Exception {
    createSealedType(0);

    final Response answered = post(0, "/api/v1/ts/graph/query", aggregationPayload(SAMPLES * BUCKET / 10));
    assertThat(answered.status).isEqualTo(200);

    assertThat(blocksCounted("ts-query"))
        .as("a read over a type with sealed blocks must report the blocks it decided about")
        .isPositive();

    // The raw branch counts too, and lands on the same tagged series.
    final JSONObject raw = new JSONObject();
    raw.put("type", TYPE);
    raw.put("from", 0L);
    raw.put("to", SAMPLES * BUCKET);
    raw.put("limit", 5);
    assertThat(post(0, "/api/v1/ts/graph/query", raw).status).isEqualTo(200);

    assertThat(Search.in(Metrics.globalRegistry).name("arcadedb.timeseries.read.rows")
        .tag("db", getDatabaseName()).tag("type", TYPE).tag("surface", "ts-query").counter())
        .as("the raw branch reports the rows it materialised").isNotNull();
  }

  private double blocksCounted(final String surface) {
    double total = 0;
    for (final Counter counter : Search.in(Metrics.globalRegistry)
        .name("arcadedb.timeseries.read.blocks").tag("db", getDatabaseName()).tag("type", TYPE)
        .tag("surface", surface).counters())
      total += counter.count();
    return total;
  }

  // ---- fixtures ----

  /** One sample per bucket, all of them compacted into sealed blocks before anything is read. */
  private void createSealedType(final int serverIndex) throws Exception {
    final Database database = getServerDatabase(serverIndex, getDatabaseName());
    if (database.getSchema().existsType(TYPE))
      database.getSchema().dropType(TYPE);

    database.command("sql", "CREATE TIMESERIES TYPE " + TYPE
        + " TIMESTAMP ts FIELDS (value DOUBLE) SHARDS 1 COMPACTION_INTERVAL 1 SECONDS");

    final LocalTimeSeriesType tsType = (LocalTimeSeriesType) database.getSchema().getType(TYPE);
    final long[] timestamps = new long[SAMPLES];
    final Object[] values = new Object[SAMPLES];
    for (int i = 0; i < SAMPLES; i++) {
      timestamps[i] = i * BUCKET;
      values[i] = (double) i;
    }

    database.begin();
    tsType.getEngine().appendSamples(timestamps, values);
    database.commit();

    tsType.getEngine().compactAll();
  }

  private JSONObject aggregationPayload(final long bucketInterval) {
    final JSONObject sum = new JSONObject();
    sum.put("field", "value");
    sum.put("type", "SUM");
    sum.put("alias", "s");

    final JSONObject aggregation = new JSONObject();
    aggregation.put("bucketInterval", bucketInterval);
    aggregation.put("requests", new JSONArray().put(sum));

    final JSONObject payload = new JSONObject();
    payload.put("type", TYPE);
    payload.put("from", 0L);
    payload.put("to", SAMPLES * BUCKET);
    payload.put("aggregation", aggregation);
    return payload;
  }

  private JSONObject grafanaPayload(final long bucketInterval) {
    final JSONObject target = new JSONObject();
    target.put("refId", "A");
    target.put("type", TYPE);
    target.put("aggregation", aggregationPayload(bucketInterval).getJSONObject("aggregation"));

    final JSONObject grafana = new JSONObject();
    grafana.put("from", 0L);
    grafana.put("to", SAMPLES * BUCKET);
    grafana.put("targets", new JSONArray().put(target));
    return grafana;
  }

  /** The status and body of one request, because a refusal is asserted on both. */
  private record Response(int status, String body) {
  }

  private Response post(final int serverIndex, final String path, final JSONObject body) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URI(
        "http://127.0.0.1:" + getServer(serverIndex).getHttpServer().getPort() + path).toURL().openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    connection.setRequestProperty("Content-Type", "application/json");
    connection.setDoOutput(true);

    try (final OutputStream os = connection.getOutputStream()) {
      os.write(body.toString().getBytes(StandardCharsets.UTF_8));
      os.flush();
    }

    final int status = connection.getResponseCode();
    final InputStream stream = status < 400 ? connection.getInputStream() : connection.getErrorStream();
    try (stream) {
      return new Response(status, stream == null ? "" : new String(stream.readAllBytes(), StandardCharsets.UTF_8));
    }
  }
}
