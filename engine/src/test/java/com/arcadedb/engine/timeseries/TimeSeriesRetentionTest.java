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
import com.arcadedb.engine.timeseries.TimeSeriesSealedStore.BlockEntry;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for TimeSeries retention policy execution.
 * Retention removes every sample older than the cutoff: a block entirely older than it is dropped, and a block that
 * straddles it is rewritten from the samples it keeps (issue #12887).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class TimeSeriesRetentionTest extends TestHelper {

  private static final long DAY = 86_400_000L;
  /** A fixed "now": retention is applied with an explicit cutoff, never by the clock or the scheduler. */
  private static final long NOW = 1_800_000_000_000L;

  /** Samples to append in one transaction: the timestamp, the sensor tag and the temperature of each. */
  private record Batch(long[] timestamps, Object[] sensors, Object[] values) {
  }

  private List<ColumnDefinition> createTestColumns() {
    return List.of(
        new ColumnDefinition("ts", Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP),
        new ColumnDefinition("sensor_id", Type.STRING, ColumnDefinition.ColumnRole.TAG),
        new ColumnDefinition("temperature", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD)
    );
  }

  @Test
  void retentionRemovesOldBlocks() throws Exception {
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
  void retentionWithNoDataToRemove() throws Exception {
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
  void retentionRemovesAllBlocks() throws Exception {
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
  void retentionWithMultipleShards() throws Exception {
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
  void retentionOnEmptyEngine() throws Exception {
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

  /**
   * The issue as reported: a slow series seals a small block per pass and the merge of small blocks folds them together whatever the
   * time gap, so 500 samples 90 days old and 500 from the last minute end up in ONE block (compacted apart, then merged as
   * {@code COMPACT TIMESERIES TYPE} does). A 30-day retention kept that block whole, because its newest sample is recent, and the
   * expired samples stayed stored and queryable until the block filled up and aged out as a whole.
   */
  // #12887
  @Test
  void retentionRemovesTheExpiredSamplesOfABlockThatStraddlesTheCutoff() throws Exception {
    final TimeSeriesEngine engine = createEngine("retention_straddling_test", 1);
    try {
      final TimeSeriesShard shard = engine.getShard(0);
      appendAndCompact(shard, samples(NOW - 90 * DAY, 1_000L, 500, 1_000.0));
      appendAndCompact(shard, samples(NOW - 60_000L, 100L, 500, 0.0));
      engine.mergeSmallBlocks();

      final TimeSeriesSealedStore sealed = shard.getSealedStore();
      assertThat(sealed.getBlockCount()).as("the old and the recent samples share one block").isEqualTo(1);
      assertThat(sealed.getTotalSampleCount()).isEqualTo(1_000L);

      final long cutoff = NOW - 30 * DAY;
      engine.applyRetention(cutoff);

      assertThat(sealed.getBlockCount()).isEqualTo(1);
      assertThat(sealed.getTotalSampleCount()).as("only the samples within the retention are stored").isEqualTo(500L);
      assertThat(sealed.getGlobalMinTimestamp()).isEqualTo(NOW - 60_000L);

      database.begin();
      try {
        assertThat(engine.query(Long.MIN_VALUE, cutoff - 1, null, null)).as("no sample older than the retention").isEmpty();

        final List<Object[]> rows = engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, null);
        assertThat(rows).hasSize(500);
        assertThat((long) rows.getFirst()[0]).isEqualTo(NOW - 60_000L);
        assertThat((long) rows.getLast()[0]).isEqualTo(NOW - 60_000L + 499 * 100L);

        // The range covers the whole rewritten block, so it is answered from the block's statistics: they must be those of the rows
        // it kept, not of the block it was rebuilt from
        final MultiColumnAggregationResult stats = engine.aggregateMulti(Long.MIN_VALUE, Long.MAX_VALUE, List.of(
            MultiColumnAggregationRequest.count("cnt"),
            new MultiColumnAggregationRequest(2, AggregationType.MIN, "min"),
            new MultiColumnAggregationRequest(2, AggregationType.MAX, "max"),
            new MultiColumnAggregationRequest(2, AggregationType.SUM, "sum")), 0L, null);
        final long bucket = stats.getBucketTimestamps().getFirst();
        assertThat(stats.getValue(bucket, 0)).isEqualTo(500.0);
        assertThat(stats.getValue(bucket, 1)).isEqualTo(0.0);
        assertThat(stats.getValue(bucket, 2)).isEqualTo(499.0);
        assertThat(stats.getValue(bucket, 3)).isEqualTo(499.0 * 500 / 2);
      } finally {
        database.commit();
      }

      assertThat(sealed.checkIntegrity(TimeSeriesIntegrity.Options.deepOnly()).problems()).isEmpty();
    } finally {
      engine.close();
    }
  }

  /**
   * The three cases of one pass side by side: a block entirely older than the cutoff is still dropped without being read, the block
   * that straddles it is rebuilt under a fresh identity (its rows are no longer those of the block it replaces), and a block entirely
   * newer is copied as it is, keeping its identity.
   */
  // #12887
  @Test
  void retentionDropsOlderBlocksTrimsTheStraddlingOneAndLeavesNewerOnesAsTheyAre() throws Exception {
    final TimeSeriesEngine engine = createEngine("retention_three_blocks_test", 1);
    try {
      final TimeSeriesShard shard = engine.getShard(0);
      appendAndCompact(shard, samples(NOW - 90 * DAY, 1_000L, 10, 0.0));
      // daily from 40 to 21 days ago: the first ten are older than a 30-day cutoff, the one exactly at it is kept
      appendAndCompact(shard, samples(NOW - 40 * DAY, DAY, 20, 100.0));
      appendAndCompact(shard, samples(NOW - DAY, 1_000L, 10, 200.0));

      final List<BlockEntry> before = blocks(shard);
      assertThat(before).hasSize(3);

      final long cutoff = NOW - 30 * DAY;
      engine.applyRetention(cutoff);

      final List<BlockEntry> after = blocks(shard);
      assertThat(after).hasSize(2);

      final BlockEntry trimmed = after.getFirst();
      assertThat(trimmed.minTimestamp).isEqualTo(cutoff);
      assertThat(trimmed.maxTimestamp).isEqualTo(NOW - 21 * DAY);
      assertThat(trimmed.sampleCount).isEqualTo(10);
      assertThat(trimmed.blockId).as("a rebuilt block takes a fresh identity").isNotEqualTo(before.get(1).blockId);

      final BlockEntry newer = after.get(1);
      assertThat(newer.blockId).as("a block entirely within the retention is not rewritten").isEqualTo(before.get(2).blockId);
      assertThat(newer.sampleCount).isEqualTo(10);

      database.begin();
      try {
        final List<Object[]> rows = engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, null);
        assertThat(rows).hasSize(20);
        for (int i = 0; i < 10; i++) {
          assertThat((double) rows.get(i)[2]).isEqualTo(110.0 + i);
          assertThat((double) rows.get(10 + i)[2]).isEqualTo(200.0 + i);
        }
      } finally {
        database.commit();
      }

      // A second pass at the same cutoff finds nothing older than it and rewrites nothing
      engine.applyRetention(cutoff);
      assertThat(blocks(shard).stream().map(b -> b.blockId).toList()).isEqualTo(after.stream().map(b -> b.blockId).toList());
    } finally {
      engine.close();
    }
  }

  // #12887
  @Test
  void retentionTrimsTheStraddlingBlockOfEveryShard() throws Exception {
    final TimeSeriesEngine engine = createEngine("retention_straddling_shards_test", 3);
    try {
      // shard 0: 10 of its 20 daily samples expired; shard 1: 5 of 10; shard 2: recent samples only
      appendAndCompact(engine.getShard(0), samples(NOW - 40 * DAY, DAY, 20, 0.0));
      appendAndCompact(engine.getShard(1), samples(NOW - 35 * DAY, DAY, 10, 100.0));
      appendAndCompact(engine.getShard(2), samples(NOW - 5 * DAY, DAY, 5, 200.0));
      final long untouchedId = blocks(engine.getShard(2)).getFirst().blockId;

      final long cutoff = NOW - 30 * DAY;
      engine.applyRetention(cutoff);

      final long[] expected = { 10, 5, 5 };
      for (int s = 0; s < 3; s++) {
        final TimeSeriesSealedStore sealed = engine.getShard(s).getSealedStore();
        assertThat(sealed.getBlockCount()).as("shard %d", s).isEqualTo(1);
        assertThat(sealed.getTotalSampleCount()).as("shard %d", s).isEqualTo(expected[s]);
        assertThat(sealed.getGlobalMinTimestamp()).as("shard %d", s).isGreaterThanOrEqualTo(cutoff);
      }
      assertThat(blocks(engine.getShard(2)).getFirst().blockId).isEqualTo(untouchedId);

      database.begin();
      try {
        final List<Object[]> rows = engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, null);
        assertThat(rows).hasSize(20);
        for (final Object[] row : rows)
          assertThat((long) row[0]).isGreaterThanOrEqualTo(cutoff);
      } finally {
        database.commit();
      }
    } finally {
      engine.close();
    }
  }

  /**
   * A downsampled block that straddles the cutoff is rebuilt at the granularity it was downsampled to, so the next downsampling
   * pass still knows it is done with it. Two series share each daily bucket, so the density fallback alone would select the
   * block again: only the marker the rebuild carries keeps the pass idempotent.
   */
  // #12887
  @Test
  void aDownsampledBlockThatStraddlesTheCutoffKeepsItsGranularity() throws Exception {
    final TimeSeriesEngine engine = createEngine("retention_downsampled_test", 1);
    try {
      // two sensors sampled hourly from 90 to 10 days ago, one block
      final int hours = 80 * 24;
      final long[] timestamps = new long[hours * 2];
      final Object[] sensors = new Object[hours * 2];
      final Object[] values = new Object[hours * 2];
      for (int h = 0; h < hours; h++) {
        for (int s = 0; s < 2; s++) {
          timestamps[h * 2 + s] = NOW - 90 * DAY + h * 3_600_000L + s * 60_000L;
          sensors[h * 2 + s] = "sensor_" + s;
          values[h * 2 + s] = (double) s;
        }
      }
      final TimeSeriesShard shard = engine.getShard(0);
      appendAndCompact(shard, new Batch(timestamps, sensors, values));
      assertThat(shard.getSealedStore().getBlockCount()).isEqualTo(1);

      final List<DownsamplingTier> tiers = List.of(new DownsamplingTier(5 * DAY, DAY));
      engine.applyDownsampling(tiers, NOW);
      final TimeSeriesSealedStore sealed = shard.getSealedStore();
      assertThat(sealed.getDownsampleRewriteCount()).isEqualTo(1L);

      final long cutoff = NOW - 30 * DAY;
      assertThat(blocks(shard)).as("a downsampled block straddles the cutoff")
          .anyMatch(b -> b.minTimestamp < cutoff && b.maxTimestamp >= cutoff && b.downsampledGranularityMs == DAY);

      engine.applyRetention(cutoff);

      assertThat(blocks(shard)).isNotEmpty().allSatisfy(b -> {
        assertThat(b.minTimestamp).isGreaterThanOrEqualTo(cutoff);
        assertThat(b.downsampledGranularityMs).isEqualTo(DAY);
      });
      database.begin();
      try {
        assertThat(engine.query(Long.MIN_VALUE, cutoff - 1, null, null)).isEmpty();
        assertThat(engine.query(cutoff, Long.MAX_VALUE, null, null)).isNotEmpty();
      } finally {
        database.commit();
      }

      engine.applyDownsampling(tiers, NOW);
      assertThat(sealed.getDownsampleRewriteCount()).as("the rebuilt block is not downsampled a second time").isEqualTo(1L);
    } finally {
      engine.close();
    }
  }

  private TimeSeriesEngine createEngine(final String name, final int shards) throws Exception {
    database.begin();
    final TimeSeriesEngine engine = new TimeSeriesEngine((DatabaseInternal) database, name, createTestColumns(), shards);
    database.commit();
    return engine;
  }

  private static Batch samples(final long firstTs, final long stepMs, final int count, final double firstValue) {
    final long[] timestamps = new long[count];
    final Object[] sensors = new Object[count];
    final Object[] values = new Object[count];
    for (int i = 0; i < count; i++) {
      timestamps[i] = firstTs + i * stepMs;
      sensors[i] = "sensor_A";
      values[i] = firstValue + i;
    }
    return new Batch(timestamps, sensors, values);
  }

  /** Appends the batch to the shard and compacts it into a sealed block of its own. */
  private void appendAndCompact(final TimeSeriesShard shard, final Batch batch) throws Exception {
    database.begin();
    shard.appendSamples(batch.timestamps(), batch.sensors(), batch.values());
    database.commit();

    database.begin();
    shard.compact();
    database.commit();
  }

  private static List<BlockEntry> blocks(final TimeSeriesShard shard) {
    return shard.getSealedStore().snapshotBlockDirectory(Long.MIN_VALUE, Long.MAX_VALUE).blocks();
  }
}
