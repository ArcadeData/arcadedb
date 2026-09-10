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
import com.arcadedb.engine.timeseries.codec.DeltaOfDeltaCodec;
import com.arcadedb.engine.timeseries.codec.GorillaXORCodec;
import com.arcadedb.engine.timeseries.simd.ScalarTimeSeriesVectorOps;
import com.arcadedb.engine.timeseries.simd.SimdTimeSeriesVectorOps;
import com.arcadedb.engine.timeseries.simd.TimeSeriesVectorOps;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.zip.CRC32;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Issue #7089: SUM and AVG were not NaN-transparent. MIN/MAX already skipped NaN as the absent marker (issues
 * #7039/#7043, {@link TimeSeriesNaN}), but every SUM accumulator was a plain {@code +=}, so a single NaN sample
 * made a whole block declare {@code sum = NaN} in its header, the block-statistics fast path answered SUM/AVG
 * straight out of that header, and a Grafana panel lost the entire bucket to one absent sample.
 * <p>
 * The fix puts SUM/AVG under the one policy - skip NaN, and an all-absent window is {@link TimeSeriesNaN#ABSENT} -
 * and, so that the fast path can still answer AVG exactly, records in every sealed block the count of REAL samples
 * per column next to its sum. Blocks written before that carry no count; this test pins how they are read.
 */
class Issue7089NaNTransparentSumAvgTest extends TestHelper {

  private static final String TEST_DIR = "target/databases/Issue7089NaNTransparentSumAvgTest";
  private static final long   HOUR     = 3_600_000L;

  private List<ColumnDefinition> columns;

  @BeforeEach
  void setUp() {
    FileUtils.deleteRecursively(new File(TEST_DIR));
    new File(TEST_DIR).mkdirs();
    columns = List.of(
        new ColumnDefinition("ts", Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP),
        new ColumnDefinition("value", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD));
  }

  @AfterEach
  void tearDown() {
    FileUtils.deleteRecursively(new File(TEST_DIR));
  }

  // ---- the policy itself ----

  @Test
  void theFoldSkipsNaNAndAnAllAbsentWindowStaysAbsent() {
    double sum = TimeSeriesNaN.ABSENT;
    long count = 0;
    for (final double d : new double[] { Double.NaN, 1.0, Double.NaN, 3.0 }) {
      sum = TimeSeriesNaN.sum(sum, count, d);
      count = TimeSeriesNaN.countIfPresent(count, d);
    }
    assertThat(sum).isEqualTo(4.0);
    assertThat(count).isEqualTo(2);

    assertThat(TimeSeriesNaN.sum(TimeSeriesNaN.ABSENT, 0, Double.NaN)).isNaN();
    assertThat(TimeSeriesNaN.countIfPresent(0, Double.NaN)).isZero();
    // Merging partials is the same rule: an absent partial cannot poison a real one, whichever side it is on.
    assertThat(TimeSeriesNaN.mergeSum(5.0, 1, TimeSeriesNaN.ABSENT, 0)).isEqualTo(5.0);
    assertThat(TimeSeriesNaN.mergeSum(TimeSeriesNaN.ABSENT, 0, 5.0, 1)).isEqualTo(5.0);
  }

  /**
   * The fold is keyed on the count of real samples seen, not on the accumulator being NaN: a total that turned
   * NaN by arithmetic ({@code +Infinity + -Infinity}) after real samples is an undefined total, not an absence,
   * and the next real sample must not overwrite it - the vectorized reduction keeps it, so must every other path.
   */
  @Test
  void anArithmeticNaNOverRealSamplesIsKeptNotReplaced() {
    final double[] data = { Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, 5.0 };

    double sum = TimeSeriesNaN.ABSENT;
    long count = 0;
    for (final double d : data) {
      sum = TimeSeriesNaN.sum(sum, count, d);
      count = TimeSeriesNaN.countIfPresent(count, d);
    }
    assertThat(sum).isNaN();
    assertThat(count).isEqualTo(3);

    final double[] stats = TimeSeriesSealedStore.reduceNumericStats(data);
    assertThat(stats[2]).isNaN();
    assertThat(stats[3]).isEqualTo(3.0);

    assertThat(new ScalarTimeSeriesVectorOps().sum(data, 0, 3)).isNaN();
    assertThat(new SimdTimeSeriesVectorOps().sum(data, 0, 3)).isNaN();
    final double[] wide = new double[70];
    java.util.Arrays.fill(wide, 1.0);
    wide[3] = Double.POSITIVE_INFINITY;
    wide[4] = Double.NEGATIVE_INFINITY;
    assertThat(new SimdTimeSeriesVectorOps().sum(wide, 0, wide.length)).isNaN();
    assertThat(new ScalarTimeSeriesVectorOps().sum(wide, 0, wide.length)).isNaN();

    // Merging partials: an undefined partial over real samples stays undefined; an absent partial is skipped.
    assertThat(TimeSeriesNaN.mergeSum(Double.NaN, 2, 5.0, 1)).isNaN();
    assertThat(TimeSeriesNaN.mergeSum(5.0, 1, Double.NaN, 2)).isNaN();
    assertThat(TimeSeriesNaN.mergeSum(5.0, 1, TimeSeriesNaN.ABSENT, 0)).isEqualTo(5.0);
    assertThat(TimeSeriesNaN.mergeSum(TimeSeriesNaN.ABSENT, 0, 5.0, 1)).isEqualTo(5.0);

    final List<MultiColumnAggregationRequest> requests = List.of(new MultiColumnAggregationRequest(1, AggregationType.SUM, "s"));
    final MultiColumnAggregationResult result = new MultiColumnAggregationResult(requests, 0L, HOUR, 1);
    for (final double d : data)
      result.accumulateRow(0L, new double[] { d });
    assertThat(result.getValue(0L, 0)).isNaN();
    assertThat(result.getCount(0L, 0)).isEqualTo(3);

    final AggregationResult left = new AggregationResult();
    left.addBucket(0L, Double.NaN, 2);
    final AggregationResult right = new AggregationResult();
    right.addBucket(0L, 5.0, 1);
    left.merge(right, AggregationType.SUM);
    assertThat(left.getValue(0)).isNaN();
  }

  /**
   * The count recorded next to a value is of the samples that contributed to it, on every path and for every
   * request - not only where AVG happens to read it. A block that straddles two buckets takes the vectorized
   * segment path; its counts must be what the block-aligned path and the per-row path record for the same data.
   */
  @Test
  void theVectorizedPathCountsTheRealSamplesForEveryRequest() throws Exception {
    final String path = TEST_DIR + "/segments";
    final List<MultiColumnAggregationRequest> requests = List.of(
        new MultiColumnAggregationRequest(1, AggregationType.SUM, "sum"),
        new MultiColumnAggregationRequest(1, AggregationType.MIN, "min"),
        new MultiColumnAggregationRequest(1, AggregationType.MAX, "max"),
        new MultiColumnAggregationRequest(1, AggregationType.AVG, "avg"),
        new MultiColumnAggregationRequest(1, AggregationType.COUNT, "count"));
    try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(path, columns)) {
      // One block over two hourly buckets: [NaN, 2] in the first, [NaN, NaN, 3] in the second.
      store.appendBlock(5, 1_000L, HOUR + 3_000L, new byte[][] {
          DeltaOfDeltaCodec.encode(new long[] { 1_000L, 2_000L, HOUR + 1_000L, HOUR + 2_000L, HOUR + 3_000L }),
          GorillaXORCodec.encode(new double[] { Double.NaN, 2.0, Double.NaN, Double.NaN, 3.0 })
      }, new double[] { Double.NaN, 2.0 }, new double[] { Double.NaN, 3.0 }, new double[] { Double.NaN, 5.0 },
          new long[] { 0, 2 }, null);
      store.flushHeader();

      final AggregationMetrics metrics = new AggregationMetrics();
      final MultiColumnAggregationResult result = new MultiColumnAggregationResult(requests, 0L, HOUR, 2);
      store.aggregateMultiBlocks(Long.MIN_VALUE, Long.MAX_VALUE, requests, HOUR, result, metrics, null);
      result.finalizeAvg();
      assertThat(metrics.getSlowPathBlocks()).as("the block straddles two buckets").isEqualTo(1);

      for (int r = 0; r < 4; r++) {
        assertThat(result.getCount(0L, r)).as("bucket 0, request " + r + ": one real sample").isEqualTo(1);
        assertThat(result.getCount(HOUR, r)).as("bucket 1, request " + r + ": one real sample").isEqualTo(1);
      }
      assertThat(result.getCount(0L, 4)).as("COUNT counts rows").isEqualTo(2);
      assertThat(result.getCount(HOUR, 4)).isEqualTo(3);
      assertThat(result.getValue(0L, 0)).isEqualTo(2.0);
      assertThat(result.getValue(0L, 3)).isEqualTo(2.0);
      assertThat(result.getValue(HOUR, 0)).isEqualTo(3.0);
      assertThat(result.getValue(HOUR, 3)).isEqualTo(3.0);
    }
  }

  @Test
  void theBlockStatisticsDeclareTheSumAndCountOfTheRealSamples() {
    final double[] stats = TimeSeriesSealedStore.reduceNumericStats(new double[] { 1.0, Double.NaN, 3.0, Double.NaN });
    assertThat(stats[0]).as("min").isEqualTo(1.0);
    assertThat(stats[1]).as("max").isEqualTo(3.0);
    assertThat(stats[2]).as("sum of the real samples, not NaN").isEqualTo(4.0);
    assertThat(stats[3]).as("count of the real samples").isEqualTo(2.0);

    final double[] absent = TimeSeriesSealedStore.reduceNumericStats(new double[] { Double.NaN, Double.NaN });
    assertThat(absent[2]).as("no real sample: the sum is absent, not 0").isNaN();
    assertThat(absent[3]).isEqualTo(0.0);
  }

  @Test
  void vectorizedAndScalarSumSkipNaNAlike() {
    final TimeSeriesVectorOps simd = new SimdTimeSeriesVectorOps();
    final TimeSeriesVectorOps scalar = new ScalarTimeSeriesVectorOps();
    // Sized so one NaN lands inside the SIMD-reduced prefix and one in the scalar tail.
    final double[] data = new double[67];
    double expected = 0;
    for (int i = 0; i < data.length; i++) {
      data[i] = i + 1;
      expected += i + 1;
    }
    data[10] = Double.NaN;
    data[65] = Double.NaN;
    expected -= 11 + 66;

    assertThat(simd.sum(data, 0, data.length)).isCloseTo(expected, within(1e-9));
    assertThat(scalar.sum(data, 0, data.length)).isCloseTo(expected, within(1e-9));
    assertThat(simd.countPresent(data, 0, data.length)).isEqualTo(65);
    assertThat(scalar.countPresent(data, 0, data.length)).isEqualTo(65);

    final double[] allNaN = new double[40];
    java.util.Arrays.fill(allNaN, Double.NaN);
    for (final TimeSeriesVectorOps ops : new TimeSeriesVectorOps[] { simd, scalar }) {
      assertThat(ops.sum(allNaN, 0, allNaN.length)).as("all-NaN range is absent, not 0").isNaN();
      assertThat(ops.sum(data, 0, 0)).as("empty range is absent, not 0").isNaN();
      assertThat(ops.countPresent(allNaN, 0, allNaN.length)).isZero();
    }
  }

  @Test
  void mergingPartialResultsSkipsTheAbsentSide() {
    // Single-column: a shard that saw only NaN merges into one that saw data without touching its total.
    final AggregationResult left = new AggregationResult();
    left.addBucket(0L, 5.0, 2);
    final AggregationResult right = new AggregationResult();
    right.addBucket(0L, TimeSeriesNaN.ABSENT, 0);
    left.merge(right, AggregationType.SUM);
    assertThat(left.getValue(0)).isEqualTo(5.0);

    // A partial AVG is weighted by its REAL count, and a zero-count side is not multiplied into the merge.
    final AggregationResult avgLeft = new AggregationResult();
    avgLeft.addBucket(0L, TimeSeriesNaN.ABSENT, 0);
    final AggregationResult avgRight = new AggregationResult();
    avgRight.addBucket(0L, 4.0, 3);
    avgLeft.merge(avgRight, AggregationType.AVG);
    assertThat(avgLeft.getValue(0)).isEqualTo(4.0);
    assertThat(avgLeft.getCount(0)).isEqualTo(3);

    // Multi-column, flat mode: the same fold.
    final List<MultiColumnAggregationRequest> requests = List.of(
        new MultiColumnAggregationRequest(1, AggregationType.SUM, "s"),
        new MultiColumnAggregationRequest(1, AggregationType.AVG, "a"));
    final MultiColumnAggregationResult a = new MultiColumnAggregationResult(requests, 0L, HOUR, 2);
    final MultiColumnAggregationResult b = new MultiColumnAggregationResult(requests, 0L, HOUR, 2);
    a.accumulateRow(0L, new double[] { Double.NaN, Double.NaN });
    b.accumulateRow(0L, new double[] { 2.0, 2.0 });
    b.accumulateRow(0L, new double[] { 4.0, 4.0 });
    a.mergeFrom(b);
    a.finalizeAvg();
    assertThat(a.getValue(0L, 0)).isEqualTo(6.0);
    assertThat(a.getValue(0L, 1)).isEqualTo(3.0);
    assertThat(a.getCount(0L, 1)).as("the count AVG divided by is of the real samples").isEqualTo(2);
  }

  // ---- end to end, through the engine ----

  @Test
  void oneNaNSampleNoLongerPoisonsTheBucketOnAnyPath() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE Sensor TIMESTAMP ts FIELDS (value DOUBLE)");
    final TimeSeriesEngine engine = ((LocalTimeSeriesType) database.getSchema().getType("Sensor")).getEngine();

    // Bucket 0: 10, NaN, 30, 40 -> sum 80, avg 80/3, count 4 (rows), min 10, max 40.
    // Bucket 1: NaN, NaN -> everything absent, count 2.
    database.transaction(() -> {
      insert(0L, 10.0);
      insert(1_000L, Double.NaN);
      insert(2_000L, 30.0);
      insert(3_000L, 40.0);
      insert(HOUR, Double.NaN);
      insert(HOUR + 1_000L, Double.NaN);
    });

    // Mutable bucket only.
    assertBucketsAreNaNTransparent(engine, "mutable");
    assertSqlPushDownIsNaNTransparent();

    // Sealed: the block-statistics fast path answers from the header, which must now carry the real sum and count.
    engine.compactAll();
    final AggregationMetrics metrics = new AggregationMetrics();
    assertBucketsAreNaNTransparent(engine, "sealed", metrics);
    assertThat(metrics.getFastPathBlocks()).as("answered from the block header").isGreaterThan(0);
    assertThat(metrics.getSlowPathBlocks()).isZero();
    assertSqlPushDownIsNaNTransparent();

    // The single-column path agrees with the multi-column one.
    database.begin();
    final AggregationResult sum = engine.aggregate(Long.MIN_VALUE, Long.MAX_VALUE, 0, AggregationType.SUM, HOUR, null);
    final AggregationResult avg = engine.aggregate(Long.MIN_VALUE, Long.MAX_VALUE, 0, AggregationType.AVG, HOUR, null);
    database.commit();
    assertThat(sum.getValue(sum.findBucketIndex(0L))).isEqualTo(80.0);
    assertThat(avg.getValue(avg.findBucketIndex(0L))).isCloseTo(80.0 / 3, within(1e-9));
    assertThat(sum.getValue(sum.findBucketIndex(HOUR))).isNaN();
    assertThat(avg.getValue(avg.findBucketIndex(HOUR))).isNaN();
  }

  private void insert(final long ts, final double value) {
    database.command("sql", "INSERT INTO Sensor SET ts = :ts, value = :v", Map.of("ts", ts, "v", value));
  }

  private void assertBucketsAreNaNTransparent(final TimeSeriesEngine engine, final String where) throws IOException {
    assertBucketsAreNaNTransparent(engine, where, null);
  }

  private void assertBucketsAreNaNTransparent(final TimeSeriesEngine engine, final String where,
      final AggregationMetrics metrics) throws IOException {
    final List<MultiColumnAggregationRequest> requests = List.of(
        new MultiColumnAggregationRequest(1, AggregationType.SUM, "sum"),
        new MultiColumnAggregationRequest(1, AggregationType.AVG, "avg"),
        new MultiColumnAggregationRequest(1, AggregationType.COUNT, "count"),
        new MultiColumnAggregationRequest(1, AggregationType.MIN, "min"),
        new MultiColumnAggregationRequest(1, AggregationType.MAX, "max"));
    database.begin();
    final MultiColumnAggregationResult result = engine.aggregateMulti(Long.MIN_VALUE, Long.MAX_VALUE, requests, HOUR, null,
        metrics);
    database.commit();

    assertThat(result.getValue(0L, 0)).as(where + ": SUM is the sum of the real samples").isEqualTo(80.0);
    assertThat(result.getValue(0L, 1)).as(where + ": AVG divides by the real samples").isCloseTo(80.0 / 3, within(1e-9));
    assertThat(result.getValue(0L, 2)).as(where + ": COUNT counts rows, like count(*)").isEqualTo(4.0);
    assertThat(result.getValue(0L, 3)).isEqualTo(10.0);
    assertThat(result.getValue(0L, 4)).isEqualTo(40.0);

    assertThat(result.getValue(HOUR, 0)).as(where + ": all-NaN bucket SUM is absent").isNaN();
    assertThat(result.getValue(HOUR, 1)).as(where + ": all-NaN bucket AVG is absent, not 0/0 or 0").isNaN();
    assertThat(result.getValue(HOUR, 2)).isEqualTo(2.0);
    assertThat(result.getValue(HOUR, 3)).isNaN();
  }

  private void assertSqlPushDownIsNaNTransparent() {
    final ResultSet rs = database.query("sql",
        "SELECT ts.timeBucket('1h', ts) AS hour, sum(value) AS s, avg(value) AS a, count(*) AS c FROM Sensor GROUP BY hour");
    final List<Result> rows = new ArrayList<>();
    while (rs.hasNext())
      rows.add(rs.next());
    rows.sort((x, y) -> ((LocalDateTime) x.getProperty("hour")).compareTo((LocalDateTime) y.getProperty("hour")));
    assertThat(rows).hasSize(2);
    assertThat(((Number) rows.get(0).getProperty("s")).doubleValue()).isEqualTo(80.0);
    assertThat(((Number) rows.get(0).getProperty("a")).doubleValue()).isCloseTo(80.0 / 3, within(1e-9));
    assertThat(((Number) rows.get(0).getProperty("c")).longValue()).isEqualTo(4);
    assertThat(((Number) rows.get(1).getProperty("s")).doubleValue()).isNaN();
    assertThat(((Number) rows.get(1).getProperty("c")).longValue()).isEqualTo(2);
  }

  // ---- the sealed block format ----

  @Test
  void aLegacyBlockIsReadAndItsPoisonedSumIsAnsweredFromTheValues() throws Exception {
    final String path = TEST_DIR + "/legacy";

    // Three blocks in the layout every block had before this fix: a [min, max, sum] triplet, the sum a plain +=.
    // Block A summed over a NaN, so its header says sum = NaN and cannot tell an all-NaN column from a poisoned
    // one; block B held no NaN, so its finite sum proves every sample real. Block C only exists to be truncated.
    writeLegacyFile(path,
        new long[][] { { 0L, 500L }, { 1_000L, 2_000L, 3_000L }, { HOUR, HOUR + 1_000L } },
        new double[][] { { 7.0, 7.0 }, { 1.0, Double.NaN, 3.0 }, { 10.0, 20.0 } });

    final List<MultiColumnAggregationRequest> requests = List.of(
        new MultiColumnAggregationRequest(1, AggregationType.SUM, "sum"),
        new MultiColumnAggregationRequest(1, AggregationType.AVG, "avg"),
        new MultiColumnAggregationRequest(1, AggregationType.MIN, "min"),
        new MultiColumnAggregationRequest(1, AggregationType.MAX, "max"),
        new MultiColumnAggregationRequest(1, AggregationType.COUNT, "count"));

    try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(path, columns)) {
      assertThat(store.getBlockCount()).isEqualTo(3);

      final AggregationMetrics metrics = new AggregationMetrics();
      final MultiColumnAggregationResult result = new MultiColumnAggregationResult(requests);
      store.aggregateMultiBlocks(600L, Long.MAX_VALUE, requests, HOUR, result, metrics, null);
      result.finalizeAvg();

      assertThat(result.getValue(0L, 0)).as("legacy block with a NaN: SUM from the values").isEqualTo(4.0);
      assertThat(result.getValue(0L, 1)).as("legacy block with a NaN: AVG over the real samples").isEqualTo(2.0);
      assertThat(result.getValue(0L, 2)).isEqualTo(1.0);
      assertThat(result.getValue(0L, 3)).isEqualTo(3.0);
      assertThat(result.getValue(0L, 4)).isEqualTo(3.0);
      assertThat(result.getValue(HOUR, 0)).as("legacy block without NaN: SUM from the header").isEqualTo(30.0);
      assertThat(result.getValue(HOUR, 1)).isEqualTo(15.0);
      assertThat(metrics.getSlowPathBlocks()).as("only the block whose header cannot answer is decoded").isEqualTo(1);
      assertThat(metrics.getFastPathBlocks()).isEqualTo(1);

      // The block is a block of its time, not a damaged one.
      assertThat(store.checkIntegrity(TimeSeriesIntegrity.Options.deepOnly()).problems()).isEmpty();

      // A rewrite upgrades what it copies: the retained legacy blocks come back in the current layout, with the
      // count computed from their values, so the next query answers both from the header.
      store.truncateBefore(600L);
      assertThat(store.getBlockCount()).isEqualTo(2);
    }

    try (final RandomAccessFile raf = new RandomAccessFile(path + ".ts.sealed", "r")) {
      raf.seek(4);
      assertThat(raf.readByte()).as("a rewritten file carries the current version").isEqualTo((byte) 1);
    }

    try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(path, columns)) {
      final AggregationMetrics metrics = new AggregationMetrics();
      final MultiColumnAggregationResult result = new MultiColumnAggregationResult(requests);
      store.aggregateMultiBlocks(Long.MIN_VALUE, Long.MAX_VALUE, requests, HOUR, result, metrics, null);
      result.finalizeAvg();

      assertThat(result.getValue(0L, 0)).isEqualTo(4.0);
      assertThat(result.getValue(0L, 1)).isEqualTo(2.0);
      assertThat(result.getValue(HOUR, 0)).isEqualTo(30.0);
      assertThat(metrics.getSlowPathBlocks()).isZero();
      assertThat(metrics.getFastPathBlocks()).isEqualTo(2);
      assertThat(store.checkIntegrity(TimeSeriesIntegrity.Options.deepOnly()).problems()).isEmpty();
    }
  }

  @Test
  void aVersionZeroFileWithoutLegacyNaNIsStampedCurrentOnItsNextAppend() throws Exception {
    final String path = TEST_DIR + "/stamped";
    writeLegacyFile(path, new long[][] { { 1_000L, 2_000L } }, new double[][] { { 1.0, 2.0 } });

    try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(path, columns)) {
      store.appendBlock(2, 5_000L, 6_000L, new byte[][] {
          DeltaOfDeltaCodec.encode(new long[] { 5_000L, 6_000L }),
          GorillaXORCodec.encode(new double[] { 5.0, Double.NaN })
      }, new double[] { Double.NaN, 5.0 }, new double[] { Double.NaN, 5.0 }, new double[] { Double.NaN, 5.0 },
          new long[] { 0, 1 }, null);
      store.flushHeader();
    }

    try (final RandomAccessFile raf = new RandomAccessFile(path + ".ts.sealed", "r")) {
      raf.seek(4);
      assertThat(raf.readByte()).isEqualTo((byte) 1);
    }

    // Mixed file: the first block is legacy, the second current; both read, both answered from the header.
    try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(path, columns)) {
      assertThat(store.getBlockCount()).isEqualTo(2);
      final List<MultiColumnAggregationRequest> requests = List.of(
          new MultiColumnAggregationRequest(1, AggregationType.AVG, "avg"));
      final AggregationMetrics metrics = new AggregationMetrics();
      final MultiColumnAggregationResult result = new MultiColumnAggregationResult(requests);
      store.aggregateMultiBlocks(Long.MIN_VALUE, Long.MAX_VALUE, requests, HOUR, result, metrics, null);
      result.finalizeAvg();
      assertThat(result.getValue(0L, 0)).as("(1 + 2 + 5) / 3 real samples, the NaN not counted").isEqualTo(8.0 / 3);
      assertThat(metrics.getFastPathBlocks()).isEqualTo(2);
      assertThat(store.checkIntegrity(TimeSeriesIntegrity.Options.deepOnly()).problems()).isEmpty();
    }
  }

  @Test
  void theDeepCheckVerifiesTheDeclaredCount() throws Exception {
    final String path = TEST_DIR + "/lying";
    try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(path, columns)) {
      // Values hold three real samples; the header claims two, and a sum that matches neither.
      store.appendBlock(3, 1_000L, 3_000L, new byte[][] {
          DeltaOfDeltaCodec.encode(new long[] { 1_000L, 2_000L, 3_000L }),
          GorillaXORCodec.encode(new double[] { 1.0, 2.0, 3.0 })
      }, new double[] { Double.NaN, 1.0 }, new double[] { Double.NaN, 3.0 }, new double[] { Double.NaN, 6.0 },
          new long[] { 0, 2 }, null);
      store.flushHeader();

      final List<String> problems = store.checkIntegrity(TimeSeriesIntegrity.Options.deepOnly()).problems();
      assertThat(problems).anyMatch(p -> p.contains("declares 2 real sample(s) for column 'value' but its values hold 3"));
    }
  }

  @Test
  void downsamplingAveragesTheRealSamplesOnly() throws Exception {
    final String path = TEST_DIR + "/downsample";
    try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(path, columns)) {
      // Old block, to be downsampled to one 5s bucket: the mean of the real samples is 2, not NaN.
      store.appendBlock(3, 6_000L, 8_000L, new byte[][] {
          DeltaOfDeltaCodec.encode(new long[] { 6_000L, 7_000L, 8_000L }),
          GorillaXORCodec.encode(new double[] { 1.0, Double.NaN, 3.0 })
      }, new double[] { Double.NaN, 1.0 }, new double[] { Double.NaN, 3.0 }, new double[] { Double.NaN, 4.0 },
          new long[] { 0, 2 }, null);
      // An old bucket with no real sample at all is downsampled to the absent marker, not to 0.
      store.appendBlock(2, 11_000L, 12_000L, new byte[][] {
          DeltaOfDeltaCodec.encode(new long[] { 11_000L, 12_000L }),
          GorillaXORCodec.encode(new double[] { Double.NaN, Double.NaN })
      }, new double[] { Double.NaN, Double.NaN }, new double[] { Double.NaN, Double.NaN },
          new double[] { Double.NaN, Double.NaN }, new long[] { 0, 0 }, null);
      // Newer block, retained.
      store.appendBlock(1, 100_000L, 100_000L, new byte[][] {
          DeltaOfDeltaCodec.encode(new long[] { 100_000L }),
          GorillaXORCodec.encode(new double[] { 9.0 })
      }, new double[] { Double.NaN, 9.0 }, new double[] { Double.NaN, 9.0 }, new double[] { Double.NaN, 9.0 },
          new long[] { 0, 1 }, null);
      store.flushHeader();

      store.downsampleBlocks(50_000L, 5_000L, 0, List.of(), List.of(1));

      final List<Object[]> rows = store.scanRange(Long.MIN_VALUE, Long.MAX_VALUE, null, null);
      assertThat(rows).hasSize(3);
      assertThat((long) rows.get(0)[0]).isEqualTo(5_000L);
      assertThat((double) rows.get(0)[1]).isEqualTo(2.0);
      assertThat((long) rows.get(1)[0]).isEqualTo(10_000L);
      assertThat((double) rows.get(1)[1]).isNaN();
      assertThat((double) rows.get(2)[1]).isEqualTo(9.0);
      assertThat(store.checkIntegrity(TimeSeriesIntegrity.Options.deepOnly()).problems()).isEmpty();
    }
  }

  /**
   * Writes a sealed file exactly as every build before this fix wrote it: header version 0, and per block the
   * "TSBL" magic with a {@code [min, max, sum]} triplet whose sum is a plain {@code +=} over every sample.
   */
  private static void writeLegacyFile(final String path, final long[][] timestamps, final double[][] values)
      throws IOException {
    long globalMin = Long.MAX_VALUE;
    long globalMax = Long.MIN_VALUE;
    for (final long[] ts : timestamps) {
      globalMin = Math.min(globalMin, ts[0]);
      globalMax = Math.max(globalMax, ts[ts.length - 1]);
    }

    try (final RandomAccessFile raf = new RandomAccessFile(path + ".ts.sealed", "rw")) {
      raf.setLength(0);
      final ByteBuffer header = ByteBuffer.allocate(27);
      header.putInt(0x54534958); // "TSIX"
      header.put((byte) 0);
      header.putShort((short) 2);
      header.putInt(timestamps.length);
      header.putLong(globalMin);
      header.putLong(globalMax);
      raf.write(header.array());

      for (int b = 0; b < timestamps.length; b++) {
        final byte[] tsBytes = DeltaOfDeltaCodec.encode(timestamps[b]);
        final byte[] valBytes = GorillaXORCodec.encode(values[b]);

        double min = TimeSeriesNaN.ABSENT;
        double max = TimeSeriesNaN.ABSENT;
        double legacySum = 0;
        for (final double v : values[b]) {
          min = TimeSeriesNaN.min(min, v);
          max = TimeSeriesNaN.max(max, v);
          legacySum += v;
        }

        final ByteBuffer meta = ByteBuffer.allocate(4 + 8 + 8 + 4 + 4 * 2 + 4 + 24 + 2);
        meta.putInt(0x5453424C); // "TSBL"
        meta.putLong(timestamps[b][0]);
        meta.putLong(timestamps[b][timestamps[b].length - 1]);
        meta.putInt(timestamps[b].length);
        meta.putInt(tsBytes.length);
        meta.putInt(valBytes.length);
        meta.putInt(1); // one numeric column
        meta.putDouble(min);
        meta.putDouble(max);
        meta.putDouble(legacySum);
        meta.putShort((short) 0); // no TAG column
        final CRC32 crc = new CRC32();
        crc.update(meta.array());
        crc.update(tsBytes);
        crc.update(valBytes);
        raf.write(meta.array());
        raf.write(tsBytes);
        raf.write(valBytes);
        raf.writeInt((int) crc.getValue());
      }
    }
  }
}
