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
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.Label;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.Sample;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.TimeSeries;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.WriteRequest;
import org.junit.jupiter.api.Test;
import org.xerial.snappy.Snappy;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Time-series HTTP ingest must append each measurement's samples as ONE batched shard transaction,
 * not one transaction per sample. Covers both ingest handlers: InfluxDB line protocol
 * ({@code PostTimeSeriesWriteHandler}) and Prometheus remote_write ({@code PostPrometheusWriteHandler}).
 * <p>
 * The per-sample shape is not merely slow in the local case: on a Raft HA leader every shard commit is
 * a replicated quorum round trip, so a 100-sample request costs 100 sequential round trips, all
 * serialized behind the per-shard append lock. Sustained ingest then falls far below the rate a single
 * client can post at, which is what surfaces as a stalled three-node time-series load.
 * <p>
 * The {@code writeTx} database statistic counts commits that produced WAL, so it counts shard append
 * transactions directly and gives a deterministic, timing-free assertion for the batching contract.
 */
class TimeSeriesIngestBatchAppendIT extends BaseGraphServerTest {

  private static final int SAMPLES = 200;

  @Test
  void lineProtocolSingleMeasurementIsOneAppendTransaction() throws Exception {
    command(0, "CREATE TIMESERIES TYPE batched TIMESTAMP ts TAGS (host STRING) FIELDS (v DOUBLE) SHARDS 1");

    final StringBuilder body = new StringBuilder();
    for (int i = 0; i < SAMPLES; i++)
      body.append("batched,host=h1 v=").append((double) i).append(' ').append(1_000 + i).append('\n');

    final long before = writeTxCount();
    assertThat(postLineProtocol(0, body.toString())).isEqualTo(204);
    final long delta = writeTxCount() - before;

    // One shard, one measurement: a single batched append transaction. Allow one extra commit of slack
    // for incidental bookkeeping; the pre-fix per-sample shape produced SAMPLES transactions, so the
    // bound separates the two shapes unambiguously.
    assertThat(delta).as("append transactions for %d samples in one measurement", SAMPLES)
        .isLessThanOrEqualTo(2);

    assertThat(rowCount("batched")).isEqualTo(SAMPLES);
  }

  @Test
  void lineProtocolBatchesPerMeasurement() throws Exception {
    command(0, "CREATE TIMESERIES TYPE batched_a TIMESTAMP ts TAGS (host STRING) FIELDS (v DOUBLE) SHARDS 1");
    command(0, "CREATE TIMESERIES TYPE batched_b TIMESTAMP ts TAGS (host STRING) FIELDS (v DOUBLE) SHARDS 1");

    // Interleave the two measurements so grouping cannot rely on the samples arriving contiguously.
    final StringBuilder body = new StringBuilder();
    for (int i = 0; i < SAMPLES; i++) {
      final String measurement = (i % 2 == 0) ? "batched_a" : "batched_b";
      body.append(measurement).append(",host=h1 v=").append((double) i).append(' ').append(1_000 + i).append('\n');
    }

    final long before = writeTxCount();
    assertThat(postLineProtocol(0, body.toString())).isEqualTo(204);
    final long delta = writeTxCount() - before;

    assertThat(delta).as("append transactions for %d samples across 2 measurements", SAMPLES)
        .isLessThanOrEqualTo(3);

    assertThat(rowCount("batched_a")).isEqualTo(SAMPLES / 2);
    assertThat(rowCount("batched_b")).isEqualTo(SAMPLES / 2);
  }

  @Test
  void remoteWriteSeriesIsOneAppendTransactionPerShard() throws Exception {
    // The type is auto-created by the handler from the series labels, so there is no CREATE TIMESERIES
    // TYPE here - and an auto-created type gets ASYNC_WORKER_THREADS shards, not one. appendBatch fans a
    // batch out round-robin and opens at most one transaction per shard that received samples, so the
    // bound is the shard count, read back from the type rather than hardcoded: it derives from the CPU
    // count and would otherwise differ between machines. Post once first so the measured request runs
    // against a settled schema and the delta counts append transactions, not the type-creation commits.
    assertThat(postPromWrite(0, singleSeries("batched_prom", "server1", 1_000))).isEqualTo(204);

    final int shards = shardCount("batched_prom");
    assertThat(shards).as("shard count must stay well below the sample count for the bound to mean anything")
        .isLessThan(SAMPLES / 4);

    final long before = writeTxCount();
    assertThat(postPromWrite(0, singleSeries("batched_prom", "server1", 10_000))).isEqualTo(204);
    final long delta = writeTxCount() - before;

    assertThat(delta).as("append transactions for %d samples in one remote-write series over %d shards",
        SAMPLES, shards).isLessThanOrEqualTo(shards + 1);

    assertThat(rowCount("batched_prom")).isEqualTo(SAMPLES * 2);
  }

  @Test
  void remoteWriteBatchesPerSeries() throws Exception {
    // Two series of the SAME metric differing only by tag: they share one auto-created type, so this
    // also proves each series is appended as its own batch rather than one transaction per sample.
    // Each series is a separate appendBatch, so the bound is one transaction per shard per series.
    assertThat(postPromWrite(0, twoSeries("batched_multi", 1_000))).isEqualTo(204);

    final int shards = shardCount("batched_multi");
    assertThat(shards).as("shard count must stay well below the sample count for the bound to mean anything")
        .isLessThan(SAMPLES / 4);

    final long before = writeTxCount();
    assertThat(postPromWrite(0, twoSeries("batched_multi", 10_000))).isEqualTo(204);
    final long delta = writeTxCount() - before;

    assertThat(delta).as("append transactions for %d samples across 2 remote-write series over %d shards",
        SAMPLES, shards).isLessThanOrEqualTo(2L * shards + 1);

    assertThat(rowCount("batched_multi")).isEqualTo(SAMPLES * 2);
  }

  private WriteRequest singleSeries(final String metric, final String host, final long baseTs) {
    return new WriteRequest(List.of(series(metric, host, baseTs, SAMPLES)));
  }

  private WriteRequest twoSeries(final String metric, final long baseTs) {
    return new WriteRequest(List.of(
        series(metric, "server1", baseTs, SAMPLES / 2),
        series(metric, "server2", baseTs, SAMPLES / 2)));
  }

  private TimeSeries series(final String metric, final String host, final long baseTs, final int count) {
    final List<Sample> samples = new ArrayList<>(count);
    for (int i = 0; i < count; i++)
      samples.add(new Sample(i, baseTs + i));
    return new TimeSeries(List.of(new Label("__name__", metric), new Label("host", host)), samples);
  }

  private long writeTxCount() {
    final Database database = getServerDatabase(0, getDatabaseName());
    return ((Number) database.getStats().get("writeTx")).longValue();
  }

  private int shardCount(final String type) {
    final Database database = getServerDatabase(0, getDatabaseName());
    return ((LocalTimeSeriesType) database.getSchema().getType(type)).getShardCount();
  }

  private int rowCount(final String type) throws Exception {
    final JSONObject result = executeCommand(0, "sql", "SELECT FROM " + type);
    assertThat(result).isNotNull();
    final JSONArray records = result.getJSONObject("result").getJSONArray("records");
    return records.length();
  }

  private int postLineProtocol(final int serverIndex, final String body) throws Exception {
    final HttpURLConnection connection = openConnection(serverIndex, "write?precision=ms");
    connection.setRequestProperty("Content-Type", "text/plain");
    return post(connection, body.getBytes(StandardCharsets.UTF_8));
  }

  private int postPromWrite(final int serverIndex, final WriteRequest writeRequest) throws Exception {
    final HttpURLConnection connection = openConnection(serverIndex, "prom/write");
    connection.setRequestProperty("Content-Type", "application/x-protobuf");
    connection.setRequestProperty("Content-Encoding", "snappy");
    return post(connection, Snappy.compress(writeRequest.encode()));
  }

  private HttpURLConnection openConnection(final int serverIndex, final String path) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URI(
        "http://127.0.0.1:248" + serverIndex + "/api/v1/ts/" + getDatabaseName() + "/" + path)
        .toURL().openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    connection.setDoOutput(true);
    return connection;
  }

  private int post(final HttpURLConnection connection, final byte[] body) throws Exception {
    try (final OutputStream os = connection.getOutputStream()) {
      os.write(body);
      os.flush();
    }
    try {
      return connection.getResponseCode();
    } finally {
      connection.disconnect();
    }
  }
}
