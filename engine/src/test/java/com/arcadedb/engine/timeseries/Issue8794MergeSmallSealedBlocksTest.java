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
import com.arcadedb.engine.timeseries.TimeSeriesSealedStore.BlockDirectorySnapshot;
import com.arcadedb.schema.LocalTimeSeriesType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * #8794: a continuous feed slower than one full block per maintenance pass sealed one small block per shard per
 * pass, and nothing ever merged them: 26 to 234 samples per block, a directory that grew with wall-clock time
 * instead of with data volume, and a retention pass that had that many blocks to copy.
 * <p>
 * {@link TimeSeriesEngine#mergeSmallBlocks()} is the missing merge. It must be LOSSLESS (same rows, same order,
 * same statistics), it must leave a block that already sits in one compaction bucket in that bucket, and a read
 * that is walking the old blocks while it lands must be answered whole (issue #9488; it used to be refused).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8794MergeSmallSealedBlocksTest extends TestHelper {
  private static final long T0 = 1_700_000_000_000L;

  private TimeSeriesEngine engine(final String type) {
    return ((LocalTimeSeriesType) database.getSchema().getType(type)).getEngine();
  }

  /** One maintenance pass worth of samples, sealed by itself: exactly what the 60 s scheduler does to a slow feed. */
  private void feedAndCompact(final TimeSeriesEngine engine, final int passes, final int samplesPerPass, final long stepMs)
      throws Exception {
    long ts = T0;
    for (int p = 0; p < passes; p++) {
      final long[] timestamps = new long[samplesPerPass];
      final Object[] tags = new Object[samplesPerPass];
      final Object[] values = new Object[samplesPerPass];
      for (int i = 0; i < samplesPerPass; i++) {
        timestamps[i] = ts;
        tags[i] = "series-" + (i % 7);
        values[i] = (double) (p * samplesPerPass + i);
        ts += stepMs;
      }
      engine.appendSamples(timestamps, tags, values);
      engine.compactAll();
    }
  }

  private int sealedBlocks(final TimeSeriesEngine engine) {
    int blocks = 0;
    for (int s = 0; s < engine.getShardCount(); s++)
      blocks += engine.getShard(s).getSealedStore().getBlockCount();
    return blocks;
  }

  private List<Object[]> allRows(final TimeSeriesEngine engine) throws Exception {
    return engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, null);
  }

  @Test
  void smallBlocksAreMergedWithoutLosingARow() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE Slow TIMESTAMP ts TAGS (id STRING) FIELDS (v DOUBLE) SHARDS 1");
    final TimeSeriesEngine engine = engine("Slow");
    feedAndCompact(engine, 60, 25, 1_000L);

    assertThat(sealedBlocks(engine)).as("one small block per pass").isEqualTo(60);
    final List<Object[]> before = allRows(engine);
    assertThat(before).hasSize(1_500);

    engine.mergeSmallBlocks();

    assertThat(sealedBlocks(engine)).as("1500 samples fit in one block").isEqualTo(1);
    final List<Object[]> after = allRows(engine);
    assertThat(after).hasSize(before.size());
    for (int i = 0; i < before.size(); i++)
      assertThat(after.get(i)).as("row " + i).containsExactly(before.get(i));

    // the numbers the aggregation push-down answers from the block header must still be those of the data
    final TimeSeriesSealedStore sealed = engine.getShard(0).getSealedStore();
    assertThat(sealed.checkIntegrity(TimeSeriesIntegrity.Options.deepOnly()).problems()).isEmpty();
    assertThat(engine.checkIntegrity().problems()).isEmpty();
  }

  @Test
  void theMergedLayoutSurvivesAReopen() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE Slow TIMESTAMP ts TAGS (id STRING) FIELDS (v DOUBLE) SHARDS 1");
    feedAndCompact(engine("Slow"), 40, 30, 500L);
    final List<Object[]> before = allRows(engine("Slow"));
    engine("Slow").mergeSmallBlocks();
    assertThat(sealedBlocks(engine("Slow"))).isEqualTo(1);

    reopenDatabase();

    assertThat(sealedBlocks(engine("Slow"))).isEqualTo(1);
    final List<Object[]> after = allRows(engine("Slow"));
    assertThat(after).hasSameSizeAs(before);
    for (int i = 0; i < before.size(); i++)
      assertThat(after.get(i)).containsExactly(before.get(i));
  }

  @Test
  void aSecondMergeHasNothingToDo() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE Slow TIMESTAMP ts TAGS (id STRING) FIELDS (v DOUBLE) SHARDS 1");
    final TimeSeriesEngine engine = engine("Slow");
    feedAndCompact(engine, 10, 20, 1_000L);
    engine.mergeSmallBlocks();

    final TimeSeriesSealedStore sealed = engine.getShard(0).getSealedStore();
    final long idBefore = sealed.snapshotBlockDirectory(Long.MIN_VALUE, Long.MAX_VALUE).blocks().getFirst().blockId;
    engine.mergeSmallBlocks();

    assertThat(sealedBlocks(engine)).isEqualTo(1);
    assertThat(sealed.snapshotBlockDirectory(Long.MIN_VALUE, Long.MAX_VALUE).blocks().getFirst().blockId)
        .as("a merge with nothing to merge must not rewrite the store").isEqualTo(idBefore);
  }

  @Test
  void theNextPassFoldsIntoTheTailBlockInsteadOfAddingOne() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE Slow TIMESTAMP ts TAGS (id STRING) FIELDS (v DOUBLE) SHARDS 1");
    final TimeSeriesEngine engine = engine("Slow");

    long ts = T0;
    for (int p = 0; p < 12; p++) {
      final long[] timestamps = new long[40];
      final Object[] tags = new Object[40];
      final Object[] values = new Object[40];
      for (int i = 0; i < 40; i++) {
        timestamps[i] = ts;
        tags[i] = "s" + (i % 3);
        values[i] = (double) ts;
        ts += 1_000L;
      }
      engine.appendSamples(timestamps, tags, values);
      engine.compactAll();
      engine.mergeSmallBlocks();
      assertThat(sealedBlocks(engine)).as("after pass " + p).isEqualTo(1);
    }
    assertThat(allRows(engine)).hasSize(480);
  }

  @Test
  void nanNullAndExtremeValuesSurviveTheMerge() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE Odd TIMESTAMP ts TAGS (id STRING) FIELDS (v DOUBLE, n LONG) SHARDS 1");
    final TimeSeriesEngine engine = engine("Odd");

    final double[][] vals = { { Double.NaN, 1.5 }, { 2.5, Double.MAX_VALUE }, { -0.0, Double.MIN_VALUE } };
    for (int p = 0; p < 3; p++) {
      engine.appendSamples(new long[] { T0 + p * 10, T0 + p * 10 + 1 }, new Object[] { "a", "" },
          new Object[] { vals[p][0], vals[p][1] }, new Object[] { -(1L << 40) + p, (1L << 40) - p });
      engine.compactAll();
    }
    final List<Object[]> before = allRows(engine);
    assertThat(sealedBlocks(engine)).isEqualTo(3);

    engine.mergeSmallBlocks();

    assertThat(sealedBlocks(engine)).isEqualTo(1);
    final List<Object[]> after = allRows(engine);
    assertThat(after).hasSameSizeAs(before);
    for (int i = 0; i < before.size(); i++)
      assertThat(after.get(i)).as("row " + i).containsExactly(before.get(i));
    assertThat(engine.getShard(0).getSealedStore().checkIntegrity(TimeSeriesIntegrity.Options.deepOnly()).problems()).isEmpty();
  }

  @Test
  void aBlockNeverCrossesACompactionBucketWhenTheTypeAligns() throws Exception {
    database.command("sql",
        "CREATE TIMESERIES TYPE Aligned TIMESTAMP ts TAGS (id STRING) FIELDS (v DOUBLE) SHARDS 1 COMPACTION_INTERVAL 1 HOURS");
    final TimeSeriesEngine engine = engine("Aligned");
    final long hour = 3_600_000L;
    final long base = Math.floorDiv(T0, hour) * hour;

    // 6 passes of 10 samples in each of 3 consecutive hours
    for (int h = 0; h < 3; h++)
      for (int p = 0; p < 6; p++) {
        final long[] timestamps = new long[10];
        final Object[] tags = new Object[10];
        final Object[] values = new Object[10];
        for (int i = 0; i < 10; i++) {
          timestamps[i] = base + h * hour + p * 60_000L + i * 1_000L;
          tags[i] = "x";
          values[i] = (double) i;
        }
        engine.appendSamples(timestamps, tags, values);
        engine.compactAll();
      }
    assertThat(sealedBlocks(engine)).isGreaterThanOrEqualTo(18);

    engine.mergeSmallBlocks();

    final BlockDirectorySnapshot snapshot = engine.getShard(0).getSealedStore().snapshotBlockDirectory(Long.MIN_VALUE,
        Long.MAX_VALUE);
    assertThat(snapshot.blocks()).as("one block per hour").hasSize(3);
    snapshot.blocks().forEach(
        b -> assertThat(Math.floorDiv(b.minTimestamp, hour)).as("block stays inside its hour").isEqualTo(Math.floorDiv(b.maxTimestamp, hour)));
    assertThat(allRows(engine)).hasSize(180);
  }

  @Test
  void aWalkThatCrossesAMergeIsAnsweredWholeNotShort() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE Slow TIMESTAMP ts TAGS (id STRING) FIELDS (v DOUBLE) SHARDS 1");
    final TimeSeriesEngine engine = engine("Slow");
    feedAndCompact(engine, 5, 20, 1_000L);

    final TimeSeriesSealedStore sealed = engine.getShard(0).getSealedStore();
    final BlockDirectorySnapshot snapshot = sealed.snapshotBlockDirectory(Long.MIN_VALUE, Long.MAX_VALUE);
    assertThat(snapshot.blocks()).hasSize(5);

    engine.mergeSmallBlocks();

    // a merge keeps every row, so the walk follows it to the merged block instead of being refused (issue #9488)
    final List<Object[]> rows = new ArrayList<>();
    sealed.forEachRow(snapshot, Long.MIN_VALUE, Long.MAX_VALUE, null, null, new AggregationMetrics(), rows::add);
    assertThat(rows).as("the walk is whole").hasSize(100);
    assertThat(allRows(engine)).as("a fresh read is whole").hasSize(100);
  }

  @Test
  void theSchedulerPassMergesToo() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE Slow TIMESTAMP ts TAGS (id STRING) FIELDS (v DOUBLE) SHARDS 1");
    final LocalTimeSeriesType type = (LocalTimeSeriesType) database.getSchema().getType("Slow");
    final TimeSeriesEngine engine = type.getEngine();
    feedAndCompact(engine, 20, 15, 1_000L);
    assertThat(sealedBlocks(engine)).isEqualTo(20);

    TimeSeriesMaintenanceScheduler.runMaintenance(database, type, "Slow");

    assertThat(sealedBlocks(engine)).isEqualTo(1);
    assertThat(allRows(engine)).hasSize(300);
  }

  @Test
  void fullBlocksAreLeftAlone() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE Big TIMESTAMP ts TAGS (id STRING) FIELDS (v DOUBLE) SHARDS 1");
    final TimeSeriesEngine engine = engine("Big");
    // two passes that each seal a full block (SEALED_BLOCK_SIZE rows) plus a small tail
    final int rows = TimeSeriesShard.SEALED_BLOCK_SIZE;
    for (int p = 0; p < 2; p++) {
      final long[] timestamps = new long[rows];
      final Object[] tags = new Object[rows];
      final Object[] values = new Object[rows];
      for (int i = 0; i < rows; i++) {
        timestamps[i] = T0 + (long) p * rows + i;
        tags[i] = "x";
        values[i] = (double) i;
      }
      engine.appendSamples(timestamps, tags, values);
      engine.compactAll();
    }
    final int blocks = sealedBlocks(engine);
    final TimeSeriesSealedStore sealed = engine.getShard(0).getSealedStore();
    final long firstId = sealed.snapshotBlockDirectory(Long.MIN_VALUE, Long.MAX_VALUE).blocks().getFirst().blockId;

    engine.mergeSmallBlocks();

    assertThat(sealedBlocks(engine)).isEqualTo(blocks);
    assertThat(sealed.snapshotBlockDirectory(Long.MIN_VALUE, Long.MAX_VALUE).blocks().getFirst().blockId).isEqualTo(firstId);
  }

  @Test
  void theSchedulerWaitsUntilAMergeIsWorthAFileRewrite() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE Slow TIMESTAMP ts TAGS (id STRING) FIELDS (v DOUBLE) SHARDS 1");
    final LocalTimeSeriesType type = (LocalTimeSeriesType) database.getSchema().getType("Slow");
    final TimeSeriesEngine engine = type.getEngine();
    feedAndCompact(engine, 5, 15, 1_000L);

    TimeSeriesMaintenanceScheduler.runMaintenance(database, type, "Slow");
    assertThat(sealedBlocks(engine)).as("5 blocks to save 4 is below the scheduler's threshold").isEqualTo(5);

    engine.mergeSmallBlocks(4);
    assertThat(sealedBlocks(engine)).isEqualTo(1);
  }

  @Test
  void aMergeAndARetentionPassRunningTogetherLeaveAConsistentStore() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE Slow TIMESTAMP ts TAGS (id STRING) FIELDS (v DOUBLE) SHARDS 1");
    final TimeSeriesEngine engine = engine("Slow");
    feedAndCompact(engine, 40, 25, 1_000L);
    final long cutoff = T0 + 10 * 25 * 1_000L;

    final Thread retention = new Thread(() -> {
      try {
        for (int i = 0; i < 20; i++)
          engine.applyRetention(cutoff);
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    });
    retention.start();
    for (int i = 0; i < 20; i++)
      engine.mergeSmallBlocks();
    retention.join();
    engine.mergeSmallBlocks();

    // retention drops whole blocks, so how many rows survive depends on how far the merge got: never more than were
    // fed, never fewer than those at or after the cutoff
    final List<Object[]> rows = allRows(engine);
    assertThat(rows.size()).isBetween(750, 1_000);
    final int survivors = rows.size();
    assertThat(engine.checkIntegrity().problems()).isEmpty();
    reopenDatabase();
    assertThat(allRows(engine("Slow"))).hasSize(survivors);
  }
}
