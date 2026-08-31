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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Regression test for issue #6937: bucketed {@code aggregateMulti()} used to size its flat bucket
 * array from the sealed stores only, so every sample in the un-sealed mutable bucket that fell past
 * the sealed maximum resolved to an out-of-range flat index and was silently discarded.
 * <p>
 * The shape that triggers it: run a compaction (so the sealed store is non-empty and drives the
 * sizing), then append a tail that spans more than two bucket intervals without compacting again.
 */
class TimeSeriesMutableTailAggregationTest {

  private static final String DB_PATH = "target/databases/TimeSeriesMutableTailAggregationTest";

  /** Sealed part: 50,000 samples, 10 ms apart -> [0 .. 499_990] ms, i.e. ~8.3 minutes. */
  private static final int  SEALED_SAMPLES     = 50_000;
  private static final long SEALED_INTERVAL_MS = 10L;

  /** Mutable tail: 600 samples, 1 s apart, starting right after the sealed part -> 10 more minutes. */
  private static final int  TAIL_SAMPLES     = 600;
  private static final long TAIL_INTERVAL_MS = 1_000L;
  private static final long TAIL_START_MS    = SEALED_SAMPLES * SEALED_INTERVAL_MS;

  private static final long MINUTE_MS = 60_000L;

  private Database database;

  @BeforeEach
  void setUp() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    database = new DatabaseFactory(DB_PATH).create();
  }

  @AfterEach
  void tearDown() {
    if (database != null && database.isOpen())
      database.close();
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  @Test
  void bucketedAggregationKeepsMutableSamplesPastTheSealedMaximum() throws Exception {
    final TimeSeriesEngine engine = createSealedPlusMutableTail();

    final List<MultiColumnAggregationRequest> requests = List.of(
        new MultiColumnAggregationRequest(2, AggregationType.SUM, "sum_val"),
        new MultiColumnAggregationRequest(2, AggregationType.MIN, "min_val"),
        new MultiColumnAggregationRequest(2, AggregationType.MAX, "max_val"),
        new MultiColumnAggregationRequest(-1, AggregationType.COUNT, "cnt"));

    // Flat mode: 1-minute buckets, so the 10-minute mutable tail spans far more than the
    // two spare buckets the sealed-only sizing used to allocate.
    final MultiColumnAggregationResult flat = engine.aggregateMulti(
        Long.MIN_VALUE, Long.MAX_VALUE, requests, MINUTE_MS, null);

    long totalCount = 0;
    double totalSum = 0;
    double min = Double.MAX_VALUE;
    double max = -Double.MAX_VALUE;
    for (final long bucketTs : flat.getBucketTimestamps()) {
      totalCount += (long) flat.getValue(bucketTs, 3);
      totalSum += flat.getValue(bucketTs, 0);
      min = Math.min(min, flat.getValue(bucketTs, 1));
      max = Math.max(max, flat.getValue(bucketTs, 2));
    }

    assertThat(totalCount)
        .as("every sample must be accounted for, mutable tail included")
        .isEqualTo(SEALED_SAMPLES + TAIL_SAMPLES);
    assertThat(totalSum).isCloseTo(expectedTotalSum(), within(1.0));
    assertThat(min).isEqualTo(1.0);
    assertThat(max).isEqualTo(SEALED_SAMPLES + TAIL_SAMPLES);

    // The last bucket of the tail must actually be present in the result.
    final long lastTailBucket = Math.floorDiv(TAIL_START_MS + (TAIL_SAMPLES - 1) * TAIL_INTERVAL_MS, MINUTE_MS) * MINUTE_MS;
    assertThat(flat.getBucketTimestamps()).contains(lastTailBucket);
    assertThat(flat.getCount(lastTailBucket, 3)).isPositive();

    // The overflow map is the safety net, not the fix: the engine must size the flat window from the
    // mutable range too, so nothing needs parking in the first place.
    assertThat(flat.getOverflowBucketCount())
        .as("flat window must be sized to cover the mutable tail, not backfilled by the overflow map")
        .isZero();
    assertThat(flat.getBucketTimestamps()).isSorted();
  }

  @Test
  void bucketedAggregationMatchesTheUnbucketedTotal() throws Exception {
    final TimeSeriesEngine engine = createSealedPlusMutableTail();

    final List<MultiColumnAggregationRequest> requests = List.of(
        new MultiColumnAggregationRequest(2, AggregationType.SUM, "sum_val"),
        new MultiColumnAggregationRequest(-1, AggregationType.COUNT, "cnt"));

    // bucketIntervalMs = 0 -> map mode, which never had the sizing problem: use it as the oracle.
    final MultiColumnAggregationResult mapMode = engine.aggregateMulti(
        Long.MIN_VALUE, Long.MAX_VALUE, requests, 0L, null);
    final long mapBucket = mapMode.getBucketTimestamps().getFirst();

    final MultiColumnAggregationResult flat = engine.aggregateMulti(
        Long.MIN_VALUE, Long.MAX_VALUE, requests, MINUTE_MS, null);

    long flatCount = 0;
    double flatSum = 0;
    for (final long bucketTs : flat.getBucketTimestamps()) {
      flatCount += (long) flat.getValue(bucketTs, 1);
      flatSum += flat.getValue(bucketTs, 0);
    }

    assertThat(flatCount).isEqualTo((long) mapMode.getValue(mapBucket, 1));
    assertThat(flatSum).isCloseTo(mapMode.getValue(mapBucket, 0), within(1.0));
    assertThat(flatCount).isEqualTo(engine.countSamples());
  }

  /**
   * Same shape on a sharded type, which takes the parallel branch of {@code aggregateMulti()}:
   * the per-shard results are built independently and merged, so the tail has to survive the merge too.
   */
  @Test
  void shardedBucketedAggregationKeepsMutableSamplesPastTheSealedMaximum() throws Exception {
    database.command("sql",
        """
        CREATE TIMESERIES TYPE Metric TIMESTAMP ts TAGS (sensor STRING) FIELDS (value DOUBLE) \
        SHARDS 4 COMPACTION_INTERVAL 1 HOURS""");

    final TimeSeriesEngine engine = ((LocalTimeSeriesType) database.getSchema().getType("Metric")).getEngine();
    assertThat(engine.getShardCount()).isEqualTo(4);

    appendBatch(engine, SEALED_SAMPLES, 0, 0L, SEALED_INTERVAL_MS);
    engine.compactAll();
    appendBatch(engine, TAIL_SAMPLES, SEALED_SAMPLES, TAIL_START_MS, TAIL_INTERVAL_MS);

    final MultiColumnAggregationResult flat = engine.aggregateMulti(
        Long.MIN_VALUE, Long.MAX_VALUE,
        List.of(
            new MultiColumnAggregationRequest(2, AggregationType.SUM, "sum_val"),
            new MultiColumnAggregationRequest(-1, AggregationType.COUNT, "cnt")),
        MINUTE_MS, null);

    long totalCount = 0;
    double totalSum = 0;
    for (final long bucketTs : flat.getBucketTimestamps()) {
      totalCount += (long) flat.getValue(bucketTs, 1);
      totalSum += flat.getValue(bucketTs, 0);
    }

    assertThat(totalCount).isEqualTo(SEALED_SAMPLES + TAIL_SAMPLES);
    assertThat(totalSum).isCloseTo(expectedTotalSum(), within(1.0));
    assertThat(flat.getOverflowBucketCount())
        .as("flat window must be sized to cover the mutable tail, not backfilled by the overflow map")
        .isZero();
    assertThat(flat.getBucketTimestamps()).isSorted();
  }

  /**
   * Out-of-range buckets must never be dropped on the floor: even when the caller sizes the flat
   * array too small, the samples have to land somewhere. Exercises the overflow path directly.
   */
  @Test
  void flatModeResultDoesNotDiscardBucketsPastTheAllocatedRange() {
    final List<MultiColumnAggregationRequest> requests = List.of(
        new MultiColumnAggregationRequest(2, AggregationType.SUM, "sum_val"),
        new MultiColumnAggregationRequest(-1, AggregationType.COUNT, "cnt"));

    // Room for exactly 2 buckets: [0, 60_000) and [60_000, 120_000).
    final MultiColumnAggregationResult result = new MultiColumnAggregationResult(requests, 0L, MINUTE_MS, 2);
    assertThat(result.isFlatMode()).isTrue();

    result.accumulateRow(0L, new double[] { 10.0, 1.0 });
    result.accumulateRow(60_000L, new double[] { 20.0, 1.0 });
    result.accumulateRow(180_000L, new double[] { 30.0, 1.0 });  // past the end
    result.accumulateRow(180_000L, new double[] { 40.0, 1.0 });
    result.accumulateRow(-60_000L, new double[] { 5.0, 1.0 });   // before the start

    assertThat(result.getBucketTimestamps()).containsExactly(-60_000L, 0L, 60_000L, 180_000L);
    assertThat(result.size()).isEqualTo(4);
    assertThat(result.getOverflowBucketCount()).isEqualTo(2);
    assertThat(result.getValue(180_000L, 0)).isEqualTo(70.0);
    assertThat(result.getCount(180_000L, 0)).isEqualTo(2);
    assertThat(result.getValue(-60_000L, 0)).isEqualTo(5.0);
    assertThat(result.getValue(0L, 0)).isEqualTo(10.0);
  }

  // ---- helpers ----

  private TimeSeriesEngine createSealedPlusMutableTail() throws Exception {
    database.command("sql",
        """
        CREATE TIMESERIES TYPE Sensor TIMESTAMP ts TAGS (sensor STRING) FIELDS (value DOUBLE) \
        SHARDS 1 COMPACTION_INTERVAL 1 HOURS""");

    final TimeSeriesEngine engine = ((LocalTimeSeriesType) database.getSchema().getType("Sensor")).getEngine();

    appendBatch(engine, SEALED_SAMPLES, 0, 0L, SEALED_INTERVAL_MS);

    engine.compactAll();

    // Sanity: the sealed store must be non-empty, otherwise the buggy sizing path is not reached.
    long sealedBlocks = 0;
    for (int s = 0; s < engine.getShardCount(); s++)
      sealedBlocks += engine.getShard(s).getSealedStore().getBlockCount();
    assertThat(sealedBlocks).as("compaction must have sealed at least one block").isPositive();

    // Tail appended AFTER compaction: it lives only in the mutable bucket.
    appendBatch(engine, TAIL_SAMPLES, SEALED_SAMPLES, TAIL_START_MS, TAIL_INTERVAL_MS);

    return engine;
  }

  private void appendBatch(final TimeSeriesEngine engine, final int count, final int valueOffset,
      final long startTs, final long intervalMs) throws Exception {
    final long[] timestamps = new long[count];
    final Object[] sensors = new Object[count];
    final Object[] values = new Object[count];
    for (int i = 0; i < count; i++) {
      timestamps[i] = startTs + i * intervalMs;
      sensors[i] = "s1";
      values[i] = (double) (valueOffset + i + 1);
    }

    database.begin();
    engine.appendSamples(timestamps, sensors, values);
    database.commit();
  }

  private double expectedTotalSum() {
    final long n = SEALED_SAMPLES + TAIL_SAMPLES;
    return n * (n + 1) / 2.0;
  }
}
