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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for TimeSeries retention policy execution.
 * Retention is block-granular: entire blocks are removed when their maxTimestamp < cutoff.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class TimeSeriesRetentionTest extends TestHelper {

  private List<ColumnDefinition> createTestColumns() {
    return List.of(
        new ColumnDefinition("ts", Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP),
        new ColumnDefinition("sensor_id", Type.STRING, ColumnDefinition.ColumnRole.TAG),
        new ColumnDefinition("temperature", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD)
    );
  }

  @Test
  void testRetentionRemovesOldBlocks() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    final List<ColumnDefinition> columns = createTestColumns();

    database.begin();
    final TimeSeriesEngine engine = new TimeSeriesEngine(db, "retention_test", columns, 1);

    // Insert first batch (timestamps 1000-2000) and compact → block 1
    engine.appendSamples(
        new long[] { 1000, 2000 },
        new Object[] { "sensor_A", "sensor_A" },
        new Object[] { 10.0, 20.0 }
    );
    database.commit();

    try {
      database.begin();
      engine.compactAll();
      database.commit();

      // Insert second batch (timestamps 3000-4000) and compact → block 2
      database.begin();
      engine.appendSamples(
          new long[] { 3000, 4000 },
          new Object[] { "sensor_A", "sensor_A" },
          new Object[] { 30.0, 40.0 }
      );
      database.commit();

      database.begin();
      engine.compactAll();
      database.commit();

      // Insert third batch (timestamps 5000-6000) and compact → block 3
      database.begin();
      engine.appendSamples(
          new long[] { 5000, 6000 },
          new Object[] { "sensor_A", "sensor_A" },
          new Object[] { 50.0, 60.0 }
      );
      database.commit();

      database.begin();
      engine.compactAll();
      database.commit();

      // Verify 3 sealed blocks and 6 total samples
      assertThat(engine.getShard(0).getSealedStore().getBlockCount()).isEqualTo(3);

      database.begin();
      final List<Object[]> allBefore = engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, null);
      assertThat(allBefore).hasSize(6);
      database.commit();

      // Apply retention: remove blocks with maxTimestamp < 2500
      // This removes block 1 (maxTs=2000), keeps blocks 2 and 3
      engine.applyRetention(2500L);

      assertThat(engine.getShard(0).getSealedStore().getBlockCount()).isEqualTo(2);

      database.begin();
      final List<Object[]> allAfter = engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, null);
      assertThat(allAfter).hasSize(4);
      assertThat((long) allAfter.get(0)[0]).isEqualTo(3000L);
      database.commit();
    } finally {
      engine.close();
    }
  }

  @Test
  void testRetentionWithNoDataToRemove() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    final List<ColumnDefinition> columns = createTestColumns();

    database.begin();
    final TimeSeriesEngine engine = new TimeSeriesEngine(db, "retention_noop_test", columns, 1);

    engine.appendSamples(
        new long[] { 1000, 2000, 3000 },
        new Object[] { "sensor_B", "sensor_B", "sensor_B" },
        new Object[] { 10.0, 20.0, 30.0 }
    );
    database.commit();

    try {
      database.begin();
      engine.compactAll();
      database.commit();

      // Apply retention with a cutoff older than all data
      engine.applyRetention(500L);

      // All data should remain (block maxTs=3000 >= 500)
      assertThat(engine.getShard(0).getSealedStore().getBlockCount()).isEqualTo(1);

      database.begin();
      final List<Object[]> allData = engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, null);
      assertThat(allData).hasSize(3);
      database.commit();
    } finally {
      engine.close();
    }
  }

  @Test
  void testRetentionRemovesAllBlocks() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    final List<ColumnDefinition> columns = createTestColumns();

    database.begin();
    final TimeSeriesEngine engine = new TimeSeriesEngine(db, "retention_all_test", columns, 1);

    engine.appendSamples(
        new long[] { 1000, 2000, 3000 },
        new Object[] { "sensor_C", "sensor_C", "sensor_C" },
        new Object[] { 10.0, 20.0, 30.0 }
    );
    database.commit();

    try {
      database.begin();
      engine.compactAll();
      database.commit();

      // Apply retention with cutoff newer than all data
      engine.applyRetention(10000L);

      // All blocks removed
      assertThat(engine.getShard(0).getSealedStore().getBlockCount()).isEqualTo(0);

      database.begin();
      final List<Object[]> allData = engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, null);
      assertThat(allData).isEmpty();
      database.commit();
    } finally {
      engine.close();
    }
  }

  @Test
  void testRetentionWithMultipleShards() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    final List<ColumnDefinition> columns = createTestColumns();

    database.begin();
    final TimeSeriesEngine engine = new TimeSeriesEngine(db, "retention_shards_test", columns, 2);

    // Insert old data into shard 0 and compact → block with maxTs=2000
    engine.getShard(0).appendSamples(
        new long[] { 1000, 2000 },
        new Object[] { "sensor_1", "sensor_1" },
        new Object[] { 10.0, 20.0 }
    );
    database.commit();

    try {
      database.begin();
      engine.getShard(0).compact();
      database.commit();

      // Insert recent data into shard 0 and compact → block with maxTs=4000
      database.begin();
      engine.getShard(0).appendSamples(
          new long[] { 3000, 4000 },
          new Object[] { "sensor_1", "sensor_1" },
          new Object[] { 30.0, 40.0 }
      );
      database.commit();

      database.begin();
      engine.getShard(0).compact();
      database.commit();

      // Insert old data into shard 1 and compact → block with maxTs=1500
      database.begin();
      engine.getShard(1).appendSamples(
          new long[] { 500, 1500 },
          new Object[] { "sensor_2", "sensor_2" },
          new Object[] { 5.0, 15.0 }
      );
      database.commit();

      database.begin();
      engine.getShard(1).compact();
      database.commit();

      // Insert recent data into shard 1 and compact → block with maxTs=5000
      database.begin();
      engine.getShard(1).appendSamples(
          new long[] { 4500, 5000 },
          new Object[] { "sensor_2", "sensor_2" },
          new Object[] { 45.0, 50.0 }
      );
      database.commit();

      database.begin();
      engine.getShard(1).compact();
      database.commit();

      // Verify: 2 blocks in each shard, 8 total samples
      assertThat(engine.getShard(0).getSealedStore().getBlockCount()).isEqualTo(2);
      assertThat(engine.getShard(1).getSealedStore().getBlockCount()).isEqualTo(2);

      database.begin();
      final List<Object[]> allBefore = engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, null);
      assertThat(allBefore).hasSize(8);
      database.commit();

      // Apply retention: remove blocks with maxTs < 2500
      // Shard 0: removes block(maxTs=2000), keeps block(maxTs=4000)
      // Shard 1: removes block(maxTs=1500), keeps block(maxTs=5000)
      engine.applyRetention(2500L);

      assertThat(engine.getShard(0).getSealedStore().getBlockCount()).isEqualTo(1);
      assertThat(engine.getShard(1).getSealedStore().getBlockCount()).isEqualTo(1);

      database.begin();
      final List<Object[]> allAfter = engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, null);
      assertThat(allAfter).hasSize(4);
      for (final Object[] row : allAfter)
        assertThat((long) row[0]).isGreaterThanOrEqualTo(3000L);
      database.commit();
    } finally {
      engine.close();
    }
  }

  @Test
  void testRetentionOnEmptyEngine() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    final List<ColumnDefinition> columns = createTestColumns();

    database.begin();
    final TimeSeriesEngine engine = new TimeSeriesEngine(db, "retention_empty_test", columns, 1);
    database.commit();

    try {
      // Apply retention on empty engine — should not throw
      engine.applyRetention(5000L);

      assertThat(engine.getShard(0).getSealedStore().getBlockCount()).isEqualTo(0);

      database.begin();
      final List<Object[]> allData = engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, null);
      assertThat(allData).isEmpty();
      database.commit();
    } finally {
      engine.close();
    }
  }
}
