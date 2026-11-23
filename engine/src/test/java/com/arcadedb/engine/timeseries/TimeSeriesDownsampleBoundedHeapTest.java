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

import com.arcadedb.engine.codec.DeltaOfDeltaCodec;
import com.arcadedb.engine.codec.DictionaryCodec;
import com.arcadedb.engine.codec.GorillaXORCodec;
import com.arcadedb.engine.codec.Simple8bCodec;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #12586: {@code downsampleBlocks} decoded every block a tier selected into one map of (tag set, bucket) accumulators, one
 * list of rebuilt rows and one list of output blocks before it wrote a byte, all under the directory write lock. The first pass
 * after a policy was added to a type with a long history therefore needed a heap as large as the whole history, and could end in
 * an {@code OutOfMemoryError}.
 * <p>
 * The pass now sweeps the selected blocks in minimum-timestamp order and folds a bucket into its row, and drops its accumulator,
 * as soon as no block still to come can reach it. These tests pin the two halves of that: the output is exactly what folding the
 * whole history at once produces - a bucket that straddles a block edge is still ONE row, overlapping blocks are still folded
 * together, an integral mean is still exact - and the accumulators held at once are bounded by what a block and the blocks that
 * overlap it hold, not by the length of the history. The bound is read from a counter ({@code getDownsamplePeakAccumulators}),
 * never from a clock.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class TimeSeriesDownsampleBoundedHeapTest {

  private static final String BASE_DIR   = "target/databases/TimeSeriesDownsampleBoundedHeapTest";
  private static final String STORE_PATH = BASE_DIR + "/store";

  /** Beyond 2^53 and with a total that overflows a long, so only an exact, 128-bit sum gets the mean right. */
  private static final long BIG = (1L << 62) + 12_345L;

  private static final List<Integer> TAG_COLUMNS     = List.of(1);
  private static final List<Integer> NUMERIC_COLUMNS = List.of(2, 3);

  private static final List<ColumnDefinition> COLUMNS = List.of(
      new ColumnDefinition("ts", Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP),
      new ColumnDefinition("host", Type.STRING, ColumnDefinition.ColumnRole.TAG),
      new ColumnDefinition("temp", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD),
      new ColumnDefinition("hits", Type.LONG, ColumnDefinition.ColumnRole.FIELD));

  /** One sample, and one row of the downsampled output: {@code hits} is {@code null} for a sample with no value. */
  private record Sample(long ts, String host, double temp, Long hits) {
  }

  @BeforeEach
  void setUp() {
    FileUtils.deleteRecursively(new File(BASE_DIR));
    new File(BASE_DIR).mkdirs();
  }

  @AfterEach
  void tearDown() {
    FileUtils.deleteRecursively(new File(BASE_DIR));
  }

  /** The bug at its smallest: the bucket [0, 15s) holds the last six samples of the first block and the first of the second. */
  @Test
  void aBucketThatStraddlesTwoBlocksIsOneRowWithTheMeanOfBoth() throws Exception {
    final List<Sample> first = new ArrayList<>();
    final List<Sample> second = new ArrayList<>();
    for (int i = 0; i < 10; i++)
      first.add(new Sample(i * 1_000L, "a", i, (long) i));
    for (int i = 10; i < 20; i++)
      second.add(new Sample(i * 1_000L, "a", i, (long) i));

    try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(STORE_PATH, COLUMNS)) {
      appendBlock(store, first);
      appendBlock(store, second);
      store.downsampleBlocks(Long.MAX_VALUE, 15_000L, 0L, 0, TAG_COLUMNS, NUMERIC_COLUMNS);

      final List<Sample> rows = readAll(store);
      assertThat(rows).as("one row per bucket, not one per block that touches it").hasSize(2);
      assertThat(rows.get(0)).isEqualTo(new Sample(0L, "a", 7.0, 7L));
      assertThat(rows.get(1)).isEqualTo(new Sample(15_000L, "a", 17.0, 17L));
    }
  }

  /**
   * Many blocks, none of them aligned to the bucket, two tag sets, NaN and missing samples, an integral field past 2^53, and a grid
   * shifted by an offset: the rows are those of the reference fold, and those of the same samples written as ONE block.
   */
  @ParameterizedTest
  @ValueSource(longs = { 0L, 3_000L, 6_999L })
  void aLongHistoryOfBlocksFoldsToTheRowsOfOneSingleBlock(final long gridOffsetMs) throws Exception {
    final long granularityMs = 7_000L;
    final List<List<Sample>> blocks = new ArrayList<>();
    final List<Sample> all = new ArrayList<>();
    for (int b = 0; b < 30; b++) {
      final List<Sample> block = new ArrayList<>();
      for (int i = 0; i < 40; i++) {
        final int n = b * 40 + i;
        block.add(sample(n * 1_000L, n));
      }
      blocks.add(block);
      all.addAll(block);
    }

    final List<Sample> expected = reference(all, granularityMs, gridOffsetMs);

    try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(STORE_PATH, COLUMNS)) {
      for (final List<Sample> block : blocks)
        appendBlock(store, block);
      store.downsampleBlocks(Long.MAX_VALUE, granularityMs, gridOffsetMs, 0, TAG_COLUMNS, NUMERIC_COLUMNS);

      assertThat(readAll(store)).containsExactlyElementsOf(expected);
      assertThat(store.checkIntegrity(TimeSeriesIntegrity.Options.deepOnly()).problems()).isEmpty();
    }

    try (final TimeSeriesSealedStore single = new TimeSeriesSealedStore(BASE_DIR + "/single", COLUMNS)) {
      appendBlock(single, all);
      single.downsampleBlocks(Long.MAX_VALUE, granularityMs, gridOffsetMs, 0, TAG_COLUMNS, NUMERIC_COLUMNS);
      assertThat(readAll(single)).as("the fold of the one big block").containsExactlyElementsOf(expected);
    }
  }

  /** Blocks that overlap in time - late data, and a block nested inside another - are folded into the same buckets. */
  @Test
  void overlappingBlocksFoldIntoTheSameBuckets() throws Exception {
    final long granularityMs = 10_000L;
    final List<List<Sample>> blocks = new ArrayList<>();
    // a wide block and a block nested inside it, then late data that starts before both end
    blocks.add(samples(0L, 100, 1_000L, "a", 0));
    blocks.add(samples(30_000L, 20, 1_000L, "b", 100));
    blocks.add(samples(55_000L, 80, 1_000L, "a", 200));
    blocks.add(samples(56_000L, 10, 3_000L, "c", 300));
    final List<Sample> all = new ArrayList<>();
    for (final List<Sample> block : blocks)
      all.addAll(block);

    try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(STORE_PATH, COLUMNS)) {
      for (final List<Sample> block : blocks)
        appendBlock(store, block);
      store.downsampleBlocks(Long.MAX_VALUE, granularityMs, 0L, 0, TAG_COLUMNS, NUMERIC_COLUMNS);

      assertThat(readAll(store)).containsExactlyElementsOf(reference(all, granularityMs, 0L));
      assertThat(store.checkIntegrity(TimeSeriesIntegrity.Options.deepOnly()).problems()).isEmpty();
    }
  }

  /**
   * The measure of the fix. 200 blocks of 60 samples, 1.2k buckets and two tag sets: folding them at once held every one of the
   * (tag set, bucket) accumulators. A sweep holds the ones a block adds and the ones its neighbour still reaches into.
   */
  @Test
  void theAccumulatorsHeldAtOnceDoNotGrowWithTheHistory() throws Exception {
    final long granularityMs = 7_000L;
    final int blockCount = 200;
    final int samplesPerBlock = 60;
    final List<Sample> all = new ArrayList<>();

    try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(STORE_PATH, COLUMNS)) {
      for (int b = 0; b < blockCount; b++) {
        final List<Sample> block = new ArrayList<>();
        for (int i = 0; i < samplesPerBlock; i++) {
          final long n = (long) b * samplesPerBlock + i;
          block.add(new Sample(n * 1_000L, n % 2 == 0 ? "a" : "b", n, n));
        }
        appendBlock(store, block);
        all.addAll(block);
      }
      store.downsampleBlocks(Long.MAX_VALUE, granularityMs, 0L, 0, TAG_COLUMNS, NUMERIC_COLUMNS);

      final List<Sample> rows = readAll(store);
      assertThat(rows).containsExactlyElementsOf(reference(all, granularityMs, 0L));
      assertThat(rows.size()).as("the history holds well over a thousand (tag set, bucket) pairs").isGreaterThan(1_500);

      // a block of 60 s spans 9 buckets of 7 s, for each of the two tag sets, plus the bucket its neighbour still reaches into
      final long bucketsPerBlock = 2 * (samplesPerBlock * 1_000L / granularityMs + 2);
      assertThat(store.getDownsamplePeakAccumulators())
          .as("accumulators held at once: a block's worth of buckets, not the %d the whole history needs", rows.size())
          .isLessThanOrEqualTo(2 * bucketsPerBlock);
    }
  }

  /** Blocks that all cover the same stretch of time cannot be swept apart, and are not: every bucket still has ONE row. */
  @Test
  void blocksThatAllCoverTheSameTimeAreFoldedTogetherNotSwept() throws Exception {
    final long granularityMs = 10_000L;
    final List<List<Sample>> blocks = new ArrayList<>();
    final List<Sample> all = new ArrayList<>();
    for (int b = 0; b < 12; b++) {
      // twelve devices that each report every 5 s over the same hour, loaded device by device
      final List<Sample> block = samples(0L, 720, 5_000L, "device" + b, b * 1_000);
      blocks.add(block);
      all.addAll(block);
    }

    try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(STORE_PATH, COLUMNS)) {
      for (final List<Sample> block : blocks)
        appendBlock(store, block);
      store.downsampleBlocks(Long.MAX_VALUE, granularityMs, 0L, 0, TAG_COLUMNS, NUMERIC_COLUMNS);

      final List<Sample> rows = readAll(store);
      assertThat(rows).containsExactlyElementsOf(reference(all, granularityMs, 0L));
      assertThat(rows).hasSize(12 * 360);
      assertThat(store.getDownsamplePeakAccumulators()).as("every bucket of the hour is open until the last block is read")
          .isLessThanOrEqualTo(12 * 360);
    }
  }

  /**
   * The output is cut into blocks of {@code MAX_BLOCK_SIZE} rows as it was when every row was held: no block per source block,
   * and no short block in the middle where a source block ended.
   */
  @Test
  void theOutputIsCutIntoFullBlocksWhateverTheNumberOfSourceBlocks() throws Exception {
    final int maxBlockSize = DeltaOfDeltaCodec.MAX_BLOCK_SIZE;
    final int sourceBlocks = 140;
    final int samplesPerBlock = 1_000;

    try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(STORE_PATH, COLUMNS)) {
      long total = 0;
      for (int b = 0; b < sourceBlocks; b++) {
        final List<Sample> block = new ArrayList<>(samplesPerBlock);
        for (int i = 0; i < samplesPerBlock; i++)
          block.add(new Sample((long) (b * samplesPerBlock + i) * 1_000L, "a", i, (long) i));
        appendBlock(store, block);
        total += samplesPerBlock;
      }
      // 140,000 samples one second apart into 2 s buckets: 70,000 rows, so a full block and the 4,464 that are left
      store.downsampleBlocks(Long.MAX_VALUE, 2_000L, 0L, 0, TAG_COLUMNS, NUMERIC_COLUMNS);

      final List<TimeSeriesSealedStore.BlockEntry> blocks = store.snapshotBlockDirectory(Long.MIN_VALUE, Long.MAX_VALUE).blocks();
      assertThat(blocks).extracting(entry -> entry.sampleCount).containsExactly(maxBlockSize, (int) (total / 2) - maxBlockSize);
      assertThat(store.getTotalSampleCount()).isEqualTo(total / 2);
      assertThat(store.checkIntegrity(TimeSeriesIntegrity.Options.deepOnly()).problems()).isEmpty();
    }
  }

  /** The exact integral mean (issue #9718) does not care which block a sample was read from. */
  @Test
  void anIntegralMeanStaysExactWhenItsSamplesSitInDifferentBlocks() throws Exception {
    final List<Sample> first = new ArrayList<>();
    final List<Sample> second = new ArrayList<>();
    final List<Sample> third = new ArrayList<>();
    // one 100 s bucket spread over three blocks, every value past 2^62: the sum of the bucket overflows a long
    for (int i = 0; i < 10; i++)
      first.add(new Sample(i * 1_000L, "a", 1.0, BIG + i));
    for (int i = 10; i < 20; i++)
      second.add(new Sample(i * 1_000L, "a", 1.0, BIG + i));
    for (int i = 20; i < 30; i++)
      third.add(new Sample(i * 1_000L, "a", 1.0, BIG + i));

    try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(STORE_PATH, COLUMNS)) {
      appendBlock(store, first);
      appendBlock(store, second);
      appendBlock(store, third);
      store.downsampleBlocks(Long.MAX_VALUE, 100_000L, 0L, 0, TAG_COLUMNS, NUMERIC_COLUMNS);

      final List<Sample> rows = readAll(store);
      assertThat(rows).hasSize(1);
      // the mean of BIG + 0 .. BIG + 29 is BIG + 14.5, truncated toward zero
      assertThat(rows.getFirst().hits()).isEqualTo(BIG + 14);
    }
  }

  /** A bucket whose samples carry no value, in every block it spans, is the absent marker and not a zero. */
  @Test
  void aBucketWithNoRealSampleInAnyOfItsBlocksStaysAbsent() throws Exception {
    final List<Sample> first = new ArrayList<>();
    final List<Sample> second = new ArrayList<>();
    for (int i = 0; i < 10; i++)
      first.add(new Sample(i * 1_000L, "a", Double.NaN, null));
    for (int i = 10; i < 20; i++)
      second.add(new Sample(i * 1_000L, "a", i < 12 ? Double.NaN : 5.0, i < 12 ? null : 7L));

    try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(STORE_PATH, COLUMNS)) {
      appendBlock(store, first);
      appendBlock(store, second);
      // the bucket [0, 15s) has real samples only from 12 s on, in the second block; [15s, 30s) is whole
      store.downsampleBlocks(Long.MAX_VALUE, 15_000L, 0L, 0, TAG_COLUMNS, NUMERIC_COLUMNS);

      final List<Sample> rows = readAll(store);
      assertThat(rows).hasSize(2);
      assertThat(rows.get(0)).isEqualTo(new Sample(0L, "a", 5.0, 7L));
      assertThat(rows.get(1)).isEqualTo(new Sample(15_000L, "a", 5.0, 7L));

      // and a bucket that holds nothing real at all
      final String other = BASE_DIR + "/absent";
      try (final TimeSeriesSealedStore allAbsent = new TimeSeriesSealedStore(other, COLUMNS)) {
        appendBlock(allAbsent, first);
        appendBlock(allAbsent, List.of(new Sample(10_000L, "a", Double.NaN, null), new Sample(14_000L, "a", Double.NaN, null)));
        allAbsent.downsampleBlocks(Long.MAX_VALUE, 15_000L, 0L, 0, TAG_COLUMNS, NUMERIC_COLUMNS);
        final List<Sample> absent = readAll(allAbsent);
        assertThat(absent).hasSize(1);
        assertThat(absent.getFirst().temp()).isNaN();
        assertThat(absent.getFirst().hits()).isNull();
      }
    }
  }

  /**
   * A history that reaches the lowest timestamps has buckets whose start cannot be represented, and {@code TimeBucketGrid} answers
   * them modulo 2^64: they sort AFTER every other bucket. The sweep must neither fold them twice nor emit its rows out of order.
   */
  @Test
  void aHistoryReachingTheLowestTimestampsIsFoldedOnceAndInOrder() throws Exception {
    final long granularityMs = 1_000_000L;
    final List<List<Sample>> blocks = new ArrayList<>();
    final List<Sample> all = new ArrayList<>();
    final long base = Long.MIN_VALUE + 10;
    for (int b = 0; b < 3; b++) {
      final List<Sample> block = new ArrayList<>();
      for (int i = 0; i < 20; i++)
        block.add(new Sample(base + (long) (b * 20 + i) * 100_000L, "a", i, (long) i));
      blocks.add(block);
      all.addAll(block);
    }
    final List<Sample> tail = samples(-5_000_000L, 40, 250_000L, "a", 0);
    blocks.add(tail);
    all.addAll(tail);

    try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(STORE_PATH, COLUMNS)) {
      for (final List<Sample> block : blocks)
        appendBlock(store, block);
      store.downsampleBlocks(Long.MAX_VALUE, granularityMs, 0L, 0, TAG_COLUMNS, NUMERIC_COLUMNS);

      final List<Sample> rows = readAll(store);
      assertThat(rows).containsExactlyElementsOf(reference(all, granularityMs, 0L));
      assertThat(rows).as("one row per (tag set, bucket)").extracting(Sample::ts).doesNotHaveDuplicates();
    }
  }

  /**
   * The sweep trusts the first timestamp a block declares: a bucket is closed once no block still to come starts in it. A block
   * that holds a sample below its declared first timestamp would be folded into a bucket that already left as a row - a second row
   * for one bucket, out of order - so the pass refuses, and the file is untouched.
   */
  @Test
  void aBlockHoldingASampleBelowItsDeclaredFirstTimestampRefusesTheRewrite() throws Exception {
    try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(STORE_PATH, COLUMNS)) {
      appendBlock(store, samples(0L, 10, 1_000L, "a", 0));
      appendBlock(store, samples(5_000L, 8, 5_000L, "a", 0), 30_000L);

      assertThatThrownBy(() -> store.downsampleBlocks(Long.MAX_VALUE, 10_000L, 0L, 0, TAG_COLUMNS, NUMERIC_COLUMNS))
          .isInstanceOf(IOException.class).hasMessageContaining("cannot fold it into a bucket it has already closed");

      assertThat(store.getDownsampleRewriteCount()).as("nothing was rewritten").isZero();
      assertThat(store.getBlockCount()).isEqualTo(2);
      assertThat(store.getTotalSampleCount()).isEqualTo(18);
    }
  }

  /** What the sweep leaves behind is at the tier's resolution: the next pass selects nothing and does not rewrite the file. */
  @Test
  void aSecondPassOverTheSweptHistoryRewritesNothing() throws Exception {
    try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(STORE_PATH, COLUMNS)) {
      for (int b = 0; b < 20; b++)
        appendBlock(store, samples(b * 60_000L, 60, 1_000L, "a", b));
      store.downsampleBlocks(Long.MAX_VALUE, 10_000L, 0L, 0, TAG_COLUMNS, NUMERIC_COLUMNS);
      final long rewrites = store.getDownsampleRewriteCount();
      final List<Sample> afterFirst = readAll(store);

      store.downsampleBlocks(Long.MAX_VALUE, 10_000L, 0L, 0, TAG_COLUMNS, NUMERIC_COLUMNS);

      assertThat(store.getDownsampleRewriteCount()).isEqualTo(rewrites);
      assertThat(readAll(store)).containsExactlyElementsOf(afterFirst);
    }
  }

  // ---- Helpers ----

  /** A sample whose values exercise the corner cases: a NaN every seventh sample, no hits every fifth, two tag sets. */
  private static Sample sample(final long ts, final int n) {
    final double temp = n % 7 == 3 ? Double.NaN : (n % 11) * 0.5;
    final Long hits = n % 5 == 4 ? null : BIG + (n % 13) - 6;
    return new Sample(ts, n % 3 == 0 ? "a" : "b", temp, hits);
  }

  private static List<Sample> samples(final long firstTs, final int count, final long stepMs, final String host, final int seed) {
    final List<Sample> list = new ArrayList<>(count);
    for (int i = 0; i < count; i++)
      list.add(new Sample(firstTs + i * stepMs, host, (seed + i) % 17 * 0.25, (long) (seed + i)));
    return list;
  }

  private static List<Sample> readAll(final TimeSeriesSealedStore store) throws IOException {
    final List<Sample> rows = new ArrayList<>();
    long previousTs = Long.MIN_VALUE;
    for (final Object[] row : store.scanRange(Long.MIN_VALUE, Long.MAX_VALUE, null, null)) {
      final long ts = (long) row[0];
      assertThat(ts).as("rows come out in timestamp order").isGreaterThanOrEqualTo(previousTs);
      previousTs = ts;
      rows.add(new Sample(ts, (String) row[1], (double) row[2], row[3] != null ? ((Number) row[3]).longValue() : null));
    }
    // rows of one bucket have no defined order between tag sets
    rows.sort(Comparator.comparingLong(Sample::ts).thenComparing(Sample::host));
    return rows;
  }

  /**
   * The downsampled rows of {@code samples}, folded with the plain definition: the mean of the real samples of each (tag, bucket),
   * and the exact mean, truncated toward zero, of the integral ones.
   */
  private static List<Sample> reference(final List<Sample> samples, final long granularityMs, final long gridOffsetMs) {
    final class Fold {
      double     sum;
      int        count;
      BigInteger hitsSum = BigInteger.ZERO;
      int        hitsCount;
    }
    final Map<String, Fold> folds = new HashMap<>();
    final Map<String, Sample> keys = new HashMap<>();
    for (final Sample sample : samples) {
      final long bucket = TimeBucketGrid.bucketStart(sample.ts(), granularityMs, gridOffsetMs);
      final String key = sample.host() + "@" + bucket;
      keys.putIfAbsent(key, new Sample(bucket, sample.host(), 0, null));
      final Fold fold = folds.computeIfAbsent(key, k -> new Fold());
      if (!Double.isNaN(sample.temp())) {
        fold.sum += sample.temp();
        fold.count++;
      }
      if (sample.hits() != null) {
        fold.hitsSum = fold.hitsSum.add(BigInteger.valueOf(sample.hits()));
        fold.hitsCount++;
      }
    }
    final List<Sample> rows = new ArrayList<>(folds.size());
    for (final Map.Entry<String, Fold> entry : folds.entrySet()) {
      final Fold fold = entry.getValue();
      final Sample key = keys.get(entry.getKey());
      rows.add(new Sample(key.ts(), key.host(), fold.count > 0 ? fold.sum / fold.count : TimeSeriesNaN.ABSENT,
          fold.hitsCount > 0 ? fold.hitsSum.divide(BigInteger.valueOf(fold.hitsCount)).longValueExact() : null));
    }
    rows.sort(Comparator.comparingLong(Sample::ts).thenComparing(Sample::host));
    return rows;
  }

  /** Writes {@code block} (ascending timestamps) as one sealed block, with the statistics a compaction would declare. */
  private static void appendBlock(final TimeSeriesSealedStore store, final List<Sample> block) throws IOException {
    appendBlock(store, block, block.getFirst().ts());
  }

  /** The same, declaring {@code declaredMinTs} as the block's first timestamp, which is a lie when it is not the first sample's. */
  private static void appendBlock(final TimeSeriesSealedStore store, final List<Sample> block, final long declaredMinTs)
      throws IOException {
    final int n = block.size();
    final long[] timestamps = new long[n];
    final String[] hosts = new String[n];
    final double[] temps = new double[n];
    final long[] hits = new long[n];
    final long[] hitsAbsent = new long[(n + 63) >>> 6];
    final Set<String> distinctHosts = new LinkedHashSet<>();
    double min = Double.NaN;
    double max = Double.NaN;
    double sum = 0;
    long realTemps = 0;
    long realHits = 0;
    for (int i = 0; i < n; i++) {
      final Sample sample = block.get(i);
      timestamps[i] = sample.ts();
      hosts[i] = sample.host();
      distinctHosts.add(sample.host());
      temps[i] = sample.temp();
      if (!Double.isNaN(sample.temp())) {
        min = Double.isNaN(min) ? sample.temp() : Math.min(min, sample.temp());
        max = Double.isNaN(max) ? sample.temp() : Math.max(max, sample.temp());
        sum += sample.temp();
        realTemps++;
      }
      if (sample.hits() == null)
        hitsAbsent[i >>> 6] |= 1L << (i & 63);
      else {
        hits[i] = sample.hits();
        realHits++;
      }
    }
    store.appendBlock(n, declaredMinTs, timestamps[n - 1], new byte[][] {
            DeltaOfDeltaCodec.encode(timestamps),
            DictionaryCodec.encode(hosts),
            GorillaXORCodec.encode(temps),
            Simple8bCodec.encode(hits, hitsAbsent) },
        new double[] { Double.NaN, Double.NaN, min, Double.NaN }, new double[] { Double.NaN, Double.NaN, max, Double.NaN },
        new double[] { Double.NaN, Double.NaN, realTemps > 0 ? sum : Double.NaN, Double.NaN },
        new long[] { 0, 0, realTemps, realHits },
        new String[][] { null, distinctHosts.toArray(new String[0]), null, null });
  }
}
