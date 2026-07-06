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
package com.arcadedb.engine.timeseries;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.schema.Type;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #4957: {@link TimeSeriesEngine#appendBatch} dispatched every shard write to the
 * shared {@code shardExecutor} even when the caller had an enclosing transaction open. Each TS-Shard thread
 * then ran with its own fresh {@code DatabaseContext}/transaction, so the shard page writes were published
 * out of band with the enclosing commit and, under HA, the page versions were reordered on the Raft log -
 * exactly the hazard the {@link TimeSeriesEngine#appendSamples} javadoc documents and forbids.
 * <p>
 * The HA page-version reordering itself needs a cluster to observe, so this test verifies the fix's
 * observable single-node contract instead: when a transaction is active on the calling thread the shard
 * writes MUST run in-thread (sequential fallback), never on the {@code ArcadeDB-TS-Shard-*} executor
 * threads. The executor's threads are created lazily on first dispatch, so their very existence after the
 * call is a deterministic signal of which path ran.
 */
class Issue4957AppendBatchTransactionTest extends TestHelper {

  private static final int SHARDS = 2;

  @Test
  void appendBatchInsideEnclosingTransactionRunsInThread() throws Exception {
    final String typeName = "tx_batch";
    final TimeSeriesEngine engine = createEngine(typeName);
    try {
      final int n = 10;
      final long[] timestamps = new long[n];
      final Object[][] columnValues = new Object[1][n];
      for (int i = 0; i < n; i++) {
        timestamps[i] = 1000L + i;
        columnValues[0][i] = (double) i;
      }

      database.begin();
      engine.appendBatch(timestamps, columnValues);
      database.commit();

      // #4957: with an enclosing transaction active, no shard write may be routed to the executor.
      // The fixed-pool threads are created lazily on the first submitted task, so any live
      // ArcadeDB-TS-Shard thread for this type proves the forbidden dispatch happened.
      assertThat(shardThreadsOf(typeName))
          .as("appendBatch inside an enclosing transaction must write the shards on the calling thread")
          .isEmpty();

      database.begin();
      final List<Object[]> rows = engine.query(0L, 10_000L, null, null);
      database.commit();

      assertThat(rows).hasSize(n);
      for (int i = 0; i < n; i++)
        assertThat((long) rows.get(i)[0]).isEqualTo(1000L + i);
    } finally {
      engine.close();
    }
  }

  @Test
  void appendBatchOutsideTransactionStillWritesAllSamplesInParallel() throws Exception {
    final String typeName = "notx_batch";
    final TimeSeriesEngine engine = createEngine(typeName);
    try {
      final int n = 25;
      final long[] timestamps = new long[n];
      final Object[][] columnValues = new Object[1][n];
      for (int i = 0; i < n; i++) {
        timestamps[i] = 1000L + i;
        columnValues[0][i] = (double) i;
      }

      // No enclosing transaction: the parallel per-shard dispatch path stays in effect.
      engine.appendBatch(timestamps, columnValues);

      assertThat(shardThreadsOf(typeName))
          .as("without an enclosing transaction the parallel shard dispatch must be preserved")
          .isNotEmpty();

      database.begin();
      final List<Object[]> rows = engine.query(0L, 10_000L, null, null);
      database.commit();

      assertThat(rows).hasSize(n);
      for (int i = 0; i < n; i++)
        assertThat((long) rows.get(i)[0]).isEqualTo(1000L + i);
    } finally {
      engine.close();
    }
  }

  private TimeSeriesEngine createEngine(final String typeName) throws Exception {
    final List<ColumnDefinition> cols = List.of(
        new ColumnDefinition("ts", Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP),
        new ColumnDefinition("value", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD));

    database.begin();
    final TimeSeriesEngine engine = new TimeSeriesEngine((DatabaseInternal) database, typeName, cols, SHARDS);
    database.commit();
    return engine;
  }

  private static Set<String> shardThreadsOf(final String typeName) {
    final String prefix = "ArcadeDB-TS-Shard-" + typeName + "-";
    return Thread.getAllStackTraces().keySet().stream()
        .map(Thread::getName)
        .filter(name -> name.startsWith(prefix))
        .collect(Collectors.toSet());
  }
}
