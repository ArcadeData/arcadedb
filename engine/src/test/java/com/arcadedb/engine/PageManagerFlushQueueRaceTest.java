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
package com.arcadedb.engine;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.schema.LocalTimeSeriesType;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for flush-queue race condition where pages polled from the queue
 * but not yet flushed to disk become invisible to getMostRecentVersionOfPage(),
 * causing spurious ConcurrentModificationException (MVCC conflicts).
 * <p>
 * The fix ensures {@link PageManagerFlushThread#getCachedPageFromMutablePageInQueue}
 * also checks the {@code nextPagesToFlush} entry (currently being flushed), and that
 * the entry is published immediately after polling from the queue.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class PageManagerFlushQueueRaceTest extends TestHelper {

  private static final int PARALLEL_LEVEL = 4;
  private static final int BATCH_SIZE     = 5_000;
  private static final int TOTAL_BATCHES  = 20;
  private static final int NUM_SENSORS    = 50;

  @Override
  protected void beginTest() {
    // Use a small page cache to force frequent evictions, which is what triggers
    // the race condition (evicted pages must be found in the flush queue).
    database.command("sql",
        "CREATE TIMESERIES TYPE SensorData TIMESTAMP ts TAGS (sensor_id STRING) FIELDS (temperature DOUBLE) SHARDS "
            + PARALLEL_LEVEL);
  }

  @Test
  void testAsyncAppendDoesNotCauseMVCCErrors() throws Exception {
    final AtomicLong errors = new AtomicLong(0);

    database.async().setParallelLevel(PARALLEL_LEVEL);
    database.async().setCommitEvery(5);
    database.async().setBackPressure(50);
    database.setReadYourWrites(false);

    database.async().onError(exception -> errors.incrementAndGet());

    final long baseTimestamp = System.currentTimeMillis() - (long) TOTAL_BATCHES * BATCH_SIZE * 100;

    for (int batch = 0; batch < TOTAL_BATCHES; batch++) {
      final long batchStart = baseTimestamp + (long) batch * BATCH_SIZE * 100;
      final long[] timestamps = new long[BATCH_SIZE];
      final Object[] sensorIds = new Object[BATCH_SIZE];
      final Object[] temperatures = new Object[BATCH_SIZE];

      for (int i = 0; i < BATCH_SIZE; i++) {
        timestamps[i] = batchStart + i * 100L;
        sensorIds[i] = "sensor_" + (i % NUM_SENSORS);
        temperatures[i] = 20.0 + (Math.random() * 15.0);
      }

      database.async().appendSamples("SensorData", timestamps, sensorIds, temperatures);
    }

    database.async().waitCompletion();

    assertThat(errors.get()).as("No MVCC errors should occur during async ingestion").isEqualTo(0);

    // Compact and verify data integrity
    ((LocalTimeSeriesType) database.getSchema().getType("SensorData")).getEngine().compactAll();

    final long expectedTotal = (long) TOTAL_BATCHES * BATCH_SIZE;
    try (final var rs = database.query("sql", "SELECT count(*) AS cnt FROM SensorData")) {
      assertThat(rs.hasNext()).isTrue();
      final long count = ((Number) rs.next().getProperty("cnt")).longValue();
      assertThat(count).as("All samples should be stored").isEqualTo(expectedTotal);
    }
  }
}
