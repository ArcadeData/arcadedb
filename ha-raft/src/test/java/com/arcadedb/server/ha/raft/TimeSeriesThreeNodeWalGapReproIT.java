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
package com.arcadedb.server.ha.raft;

import com.arcadedb.database.Database;
import com.arcadedb.engine.timeseries.TimeSeriesEngine;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.server.BaseGraphServerTest;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Attempts to reproduce the production failure of {@code ThreeNodesTimeSeriesLoadTestIT}: under
 * concurrent time-series ingestion on a 3-node Raft cluster, compaction ships SCHEMA_ENTRYs that can
 * race the append TX_ENTRYs and open a WAL page-version gap on a follower. The follower then
 * force-resyncs from a snapshot; while it does, its schema is transiently empty - surfacing as
 * "Type with name '' was not found" on the SQL/gRPC path and as silently-dropped points on the line
 * protocol path. This test drives sustained line protocol load while the leader compacts in a loop
 * and asserts no points are lost and all three nodes converge.
 */
@Tag("slow")
class TimeSeriesThreeNodeWalGapReproIT extends BaseRaftHATest {

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Test
  void threeNodeLineProtocolUnderCompactionLosesNoPoints() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("leader elected").isGreaterThanOrEqualTo(0);

    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());
    leaderDb.command("sql", "CREATE TIMESERIES TYPE sensor TIMESTAMP ts FIELDS (v DOUBLE) SHARDS 1");
    waitForAllServers();

    final int writerThreads = 4;
    final int batchesPerThread = 30;
    final int samplesPerBatch = 100;
    final int expected = writerThreads * batchesPerThread * samplesPerBatch;

    final ConcurrentLinkedQueue<String> failures = new ConcurrentLinkedQueue<>();
    final AtomicBoolean writersDone = new AtomicBoolean(false);
    final AtomicInteger postedBatches = new AtomicInteger();
    // Compaction runs on its own executor so the writer pool can be awaited independently. If both shared
    // one pool, awaitTermination() would wait for the compaction task, which loops until writersDone - a
    // flag we could only set after the wait returned, deadlocking the wait for its full timeout.
    final ExecutorService writerExec = Executors.newFixedThreadPool(writerThreads);
    final ExecutorService compactionExec = Executors.newSingleThreadExecutor();

    // Continuous compaction on the leader to keep shipping SCHEMA_ENTRYs alongside the append load.
    final TimeSeriesEngine engine = ((LocalTimeSeriesType) leaderDb.getSchema().getType("sensor")).getEngine();
    compactionExec.submit(() -> {
      while (!writersDone.get()) {
        try {
          engine.compactAll();
          Thread.sleep(2);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          return;
        } catch (final Throwable e) {
          failures.add("compaction: " + e.getMessage());
        }
      }
    });

    for (int t = 0; t < writerThreads; t++) {
      final int ti = t;
      writerExec.submit(() -> {
        final long base = 1_000_000L + (long) ti * 1_000_000L;
        for (int b = 0; b < batchesPerThread; b++) {
          final StringBuilder body = new StringBuilder();
          for (int i = 0; i < samplesPerBatch; i++) {
            final long ts = base + (long) b * samplesPerBatch + i;
            body.append("sensor v=").append((double) ts).append(' ').append(ts).append('\n');
          }
          try {
            final int status = postLineProtocol(leaderIndex, body.toString());
            if (status != 204)
              failures.add("HTTP " + status + " batch " + b + " writer " + ti);
            else
              postedBatches.incrementAndGet();
          } catch (final Exception e) {
            failures.add("post batch " + b + " writer " + ti + ": " + e.getMessage());
          }
        }
      });
    }

    // Wait for the writers to finish FIRST (compaction is still running alongside them), then stop and
    // await compaction. This ordering keeps compaction racing the append load for the whole run.
    writerExec.shutdown();
    assertThat(writerExec.awaitTermination(3, TimeUnit.MINUTES)).as("writers complete").isTrue();
    writersDone.set(true);
    compactionExec.shutdown();
    assertThat(compactionExec.awaitTermination(1, TimeUnit.MINUTES)).as("compaction stopped").isTrue();

    assertThat(failures).as("no ingest/compaction failures").isEmpty();

    // Converge all three nodes.
    for (int i = 0; i < getServerCount(); i++) {
      final Database db = getServerDatabase(i, getDatabaseName());
      final long deadline = System.currentTimeMillis() + 60_000;
      long count = -1;
      while (System.currentTimeMillis() < deadline) {
        count = ((Number) db.command("sql", "SELECT count(*) AS cnt FROM sensor").next().getProperty("cnt")).longValue();
        if (count == expected)
          break;
        Thread.sleep(1_000);
      }
      assertThat(count).as("node %d converged to full count (posted batches=%d)", i, postedBatches.get())
          .isEqualTo((long) expected);
    }
  }

  private int postLineProtocol(final int serverIndex, final String body) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URI(
        "http://127.0.0.1:248" + serverIndex + "/api/v1/ts/" + getDatabaseName() + "/write?precision=ms")
        .toURL().openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    connection.setRequestProperty("Content-Type", "text/plain");
    connection.setConnectTimeout(5_000);
    connection.setReadTimeout(30_000);
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
