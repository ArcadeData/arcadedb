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
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Line protocol ingestion must append each measurement's samples as ONE batched shard transaction,
 * not one transaction per sample.
 * <p>
 * The per-sample shape is not merely slow in the local case: on a Raft HA leader every shard commit is
 * a replicated quorum round trip, so a 100-sample request costs 100 sequential round trips, all
 * serialized behind the per-shard append lock. Sustained ingest then falls far below the rate a single
 * client can post at, which is what surfaces as a stalled three-node time-series load.
 * <p>
 * The {@code writeTx} database statistic counts commits that produced WAL, so it counts shard append
 * transactions directly and gives a deterministic, timing-free assertion for the batching contract.
 */
class PostTimeSeriesWriteHandlerBatchAppendIT extends BaseGraphServerTest {

  private static final int SAMPLES = 200;

  @Test
  void singleMeasurementBatchIsOneAppendTransaction() throws Exception {
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
  void multipleMeasurementsBatchPerMeasurement() throws Exception {
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

  private long writeTxCount() {
    final Database database = getServerDatabase(0, getDatabaseName());
    return ((Number) database.getStats().get("writeTx")).longValue();
  }

  private int rowCount(final String type) throws Exception {
    final JSONObject result = executeCommand(0, "sql", "SELECT FROM " + type);
    assertThat(result).isNotNull();
    final JSONArray records = result.getJSONObject("result").getJSONArray("records");
    return records.length();
  }

  private int postLineProtocol(final int serverIndex, final String body) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URI(
        "http://127.0.0.1:248" + serverIndex + "/api/v1/ts/" + getDatabaseName() + "/write?precision=ms")
        .toURL().openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    connection.setRequestProperty("Content-Type", "text/plain");
    connection.setDoOutput(true);
    try (final OutputStream os = connection.getOutputStream()) {
      os.write(body.getBytes(StandardCharsets.UTF_8));
      os.flush();
    }
    try {
      return connection.getResponseCode();
    } finally {
      connection.disconnect();
    }
  }
}
