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
import com.arcadedb.engine.codec.DeltaOfDeltaCodec;
import com.arcadedb.engine.codec.GorillaXORCodec;
import com.arcadedb.engine.codec.Simple8bCodec;
import com.arcadedb.engine.timeseries.simd.ScalarTimeSeriesVectorOps;
import com.arcadedb.engine.timeseries.simd.SimdTimeSeriesVectorOps;
import com.arcadedb.engine.timeseries.simd.TimeSeriesVectorOps;
import com.arcadedb.function.sql.math.SQLFunctionSum;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.zip.CRC32;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

/**
 * Regression test for issue #4596: inconsistent NaN handling across the MIN/MAX paths
 * (sealed block-stats vs row-iter vs cross-shard merge vs SIMD vs scalar).
 * <p>
 * The agreed policy is to SKIP NaN: a {@code NaN} sample is treated as absent, so MIN/MAX
 * over a window that contains at least one real value always returns that real min/max,
 * deterministically, regardless of which code path or storage layout produced it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class TimeSeriesNaNHandlingTest extends TestHelper {

  /**
   * SIMD and scalar MIN/MAX must agree when the data contains NaN. The array is sized so the
   * NaN falls inside the SIMD-reduced prefix (not only the scalar tail).
   */
  @Test
  void simdAndScalarSkipNaN() {
    final TimeSeriesVectorOps simd = new SimdTimeSeriesVectorOps();
    final TimeSeriesVectorOps scalar = new ScalarTimeSeriesVectorOps();

    final double[] data = new double[64];
    for (int i = 0; i < data.length; i++)
      data[i] = i + 1; // 1..64
    data[10] = Double.NaN;
    data[40] = Double.NaN;

    assertThat(simd.min(data, 0, data.length)).isEqualTo(1.0);
    assertThat(simd.max(data, 0, data.length)).isEqualTo(64.0);

    // SIMD and scalar must be identical (deterministic, NaN skipped)
    assertThat(simd.min(data, 0, data.length)).isEqualTo(scalar.min(data, 0, data.length));
    assertThat(simd.max(data, 0, data.length)).isEqualTo(scalar.max(data, 0, data.length));
  }

  /**
   * Cross-shard merge must skip NaN. Whichever side carries the NaN, the merged MIN/MAX must
   * be the real value from the other side.
   */
  @Test
  void crossShardMergeSkipsNaN() {
    for (final AggregationType type : new AggregationType[] { AggregationType.MIN, AggregationType.MAX }) {
      // NaN on the right
      final AggregationResult left = new AggregationResult();
      left.addBucket(0L, 5.0, 1);
      final AggregationResult right = new AggregationResult();
      right.addBucket(0L, Double.NaN, 1);
      left.merge(right, type);
      assertThat(left.getValue(0)).as("NaN on right, type=%s", type).isEqualTo(5.0);

      // NaN on the left (the seeded/running value is NaN, a real value must win)
      final AggregationResult left2 = new AggregationResult();
      left2.addBucket(0L, Double.NaN, 1);
      final AggregationResult right2 = new AggregationResult();
      right2.addBucket(0L, 7.0, 1);
      left2.merge(right2, type);
      assertThat(left2.getValue(0)).as("NaN on left, type=%s", type).isEqualTo(7.0);
    }
  }

  /**
   * End-to-end: a window that mixes NaN samples with real values must yield the real MIN/MAX,
   * and the single-column ({@code aggregate}) and multi-column ({@code aggregateMulti}) paths
   * must agree.
   */
  @Test
  void aggregateSkipsNaNConsistently() throws Exception {
    final List<ColumnDefinition> cols = List.of(
        new ColumnDefinition("ts", Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP),
        new ColumnDefinition("value", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD)
    );

    database.begin();
    final TimeSeriesEngine engine = new TimeSeriesEngine((DatabaseInternal) database, "test_nan", cols, 1);

    // First sample is NaN on purpose: it seeds the running MIN/MAX value
    engine.appendSamples(
        new long[] { 1000L, 2000L, 3000L, 4000L },
        new Object[] { Double.NaN, 20.0, 5.0, Double.NaN });
    database.commit();

    database.begin();
    // Single bucket (bucketIntervalMs <= 0). columnIndex is a position in the ENGINE ROW, so the value column is
    // at index 1 and 0 would be the timestamp.
    final List<MultiColumnAggregationRequest> requests = List.of(
        new MultiColumnAggregationRequest(1, AggregationType.MIN, "min"),
        new MultiColumnAggregationRequest(1, AggregationType.MAX, "max"));
    final MultiColumnAggregationResult multi = engine.aggregateMulti(Long.MIN_VALUE, Long.MAX_VALUE, requests, 0, null);
    assertThat(multi.size()).isEqualTo(1);
    final long bucketTs = multi.getBucketTimestamps().get(0);
    assertThat(multi.getValue(bucketTs, 0)).as("multi-column MIN skips NaN").isEqualTo(5.0);
    assertThat(multi.getValue(bucketTs, 1)).as("multi-column MAX skips NaN").isEqualTo(20.0);
    database.commit();

    engine.drop();
  }

  // #7089
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
  @Nested
  class NaNTransparentSumAvg {

    /**
     * Where the stand-alone sealed stores of the format tests live. It must not be {@link #getDatabasePath()}: the
     * {@code tearDown} below runs before {@link TestHelper#afterTest()}, so wiping the database directory there deleted the
     * engine's own sealed files under the still-open database, and the post-test integrity check, which reads a sealed
     * store's length from disk since #10372, reported them missing (issue #10638).
     */
    private static final String TEST_DIR = "target/databases/Issue7089NaNTransparentSumAvgTest-stores";

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

    /**
     * {@code SHARDS 1 COMPACTION_INTERVAL 1 HOURS} is what makes the sealed half of this test mean the same thing on
     * every machine, and it is not decoration (issue #7495).
     * <p>
     * Compaction seals whole data pages, not time buckets, unless the type asks for bucket-aligned blocks - so
     * without {@code COMPACTION_INTERVAL} the six rows become one block per shard, each straddling the two hour
     * buckets and therefore unanswerable from its header. And without {@code SHARDS} the shard count defaults to
     * {@code arcadedb.asyncWorkerThreads}, i.e. {@code availableProcessors() - 1}: rows are handed to shards
     * round-robin, so the number of straddling blocks - and with it {@link AggregationMetrics#getSlowPathBlocks()} -
     * was a function of the core count. It was 0 on an 11-shard developer machine and 2 on a 3-shard CI runner, which
     * is how this test came to fail on CI and nowhere else. Pinned, compaction yields exactly two blocks, one per
     * hour bucket, and the block-header fast path is the one that answers - which is the path issue #7089 was about.
     */
    @Test
    void oneNaNSampleNoLongerPoisonsTheBucketOnAnyPath() throws Exception {
      database.command("sql",
          "CREATE TIMESERIES TYPE Sensor TIMESTAMP ts FIELDS (value DOUBLE) SHARDS 1 COMPACTION_INTERVAL 1 HOURS");
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
      assertThat(metrics.getFastPathBlocks()).as("answered from the block header, one block per hour bucket")
          .isEqualTo(2);
      assertThat(metrics.getSlowPathBlocks()).isZero();
      assertSqlPushDownIsNaNTransparent();

      // The same numbers read one request at a time rather than three in one pass, so a per-request accumulator
      // that only works when its siblings are present would not pass here either.
      database.begin();
      final MultiColumnAggregationResult sum = engine.aggregateMulti(Long.MIN_VALUE, Long.MAX_VALUE,
          List.of(new MultiColumnAggregationRequest(1, AggregationType.SUM, "sum")), HOUR, null);
      final MultiColumnAggregationResult avg = engine.aggregateMulti(Long.MIN_VALUE, Long.MAX_VALUE,
          List.of(new MultiColumnAggregationRequest(1, AggregationType.AVG, "avg")), HOUR, null);
      database.commit();
      assertThat(sum.getValue(0L, 0)).isEqualTo(80.0);
      assertThat(avg.getValue(0L, 0)).isCloseTo(80.0 / 3, within(1e-9));
      assertThat(sum.getValue(HOUR, 0)).isNaN();
      assertThat(avg.getValue(HOUR, 0)).isNaN();
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
      // The second bucket held no real sample, so its SUM is absent. At the SQL boundary that is spelled NULL
      // since issue #7743 - the marker is translated by the step rather than handed out raw - which is also what
      // the generic aggregation and the raw-row projection answer for it. COUNT still counts rows.
      assertThat(rows.get(1).<Object>getProperty("s")).as("absent SUM reaches SQL as NULL").isNull();
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
        assertThat(raf.readByte()).as("a rewritten file carries the current version")
            .isEqualTo((byte) TimeSeriesSealedStore.CURRENT_VERSION);
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

    /**
     * A legacy block whose column summed over a NaN has no count of real samples either, and the count recorded next
     * to a MIN or a MAX is of the samples that contributed. Answering those from the header would record the
     * "unknown" marker as a count; the block is decoded for them too, and only COUNT reads the header.
     */
    @Test
    void aMinOrMaxOverALegacyPoisonedBlockIsAnsweredFromTheValuesSoItsCountIsReal() throws Exception {
      final String path = TEST_DIR + "/legacy-minmax";
      writeLegacyFile(path, new long[][] { { 1_000L, 2_000L, 3_000L } }, new double[][] { { 1.0, Double.NaN, 3.0 } });

      final List<MultiColumnAggregationRequest> requests = List.of(
          new MultiColumnAggregationRequest(1, AggregationType.MIN, "min"),
          new MultiColumnAggregationRequest(1, AggregationType.MAX, "max"),
          new MultiColumnAggregationRequest(1, AggregationType.COUNT, "count"));
      try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(path, columns)) {
        final AggregationMetrics metrics = new AggregationMetrics();
        final MultiColumnAggregationResult result = new MultiColumnAggregationResult(requests);
        store.aggregateMultiBlocks(Long.MIN_VALUE, Long.MAX_VALUE, requests, HOUR, result, metrics, null);

        assertThat(metrics.getSlowPathBlocks()).isEqualTo(1);
        assertThat(result.getValue(0L, 0)).isEqualTo(1.0);
        assertThat(result.getValue(0L, 1)).isEqualTo(3.0);
        assertThat(result.getCount(0L, 0)).as("real samples, never the unknown marker").isEqualTo(2);
        assertThat(result.getCount(0L, 1)).isEqualTo(2);
        assertThat(result.getValue(0L, 2)).isEqualTo(3.0);
        assertThat(result.getCount(0L, 2)).isEqualTo(3);
      }

      // A COUNT alone still reads the header: the block's row count was never in doubt.
      final List<MultiColumnAggregationRequest> countOnly = List.of(new MultiColumnAggregationRequest(1, AggregationType.COUNT, "count"));
      try (final TimeSeriesSealedStore store = new TimeSeriesSealedStore(path, columns)) {
        final AggregationMetrics metrics = new AggregationMetrics();
        final MultiColumnAggregationResult result = new MultiColumnAggregationResult(countOnly);
        store.aggregateMultiBlocks(Long.MIN_VALUE, Long.MAX_VALUE, countOnly, HOUR, result, metrics, null);
        assertThat(metrics.getFastPathBlocks()).isEqualTo(1);
        assertThat(result.getValue(0L, 0)).isEqualTo(3.0);
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
        // the newest version of its own 27-byte layout: an in-place header rewrite cannot make room for the owner of version 4,
        // which only a rewrite of the whole file writes (issue #11274)
        assertThat(raf.readByte()).isEqualTo((byte) (TimeSeriesSealedStore.CURRENT_VERSION - 1));
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

  // ---- #7043 ----
  // Regression test for issue #7043 (follow-up on #4596): an all-NaN bucket leaked the MIN/MAX init sentinel.
  // <p>
  // {@code isUnusedMinMax} keyed off the raw sample count, and a NaN sample increments that count, so a bucket whose
  // samples were all NaN looked "touched" while its accumulator still held the untouched {@code ±Double.MAX_VALUE}.
  // The single-column path seeded its bucket with NaN and answered NaN over the same data, so the two paths
  // disagreed - which is how the defect was found. That path is gone (issue #8189: it carried a second
  // column-index convention and had no production caller), so what is asserted here is the one remaining answer.
  // <p>
  // The fix removes the sentinel rather than guarding it: MIN/MAX start at {@link TimeSeriesNaN#ABSENT} and fold
  // through the one NaN policy of the subsystem, so "no real sample arrived" is the value itself.
  //
  // @author Luca Garulli (l.garulli@arcadedata.com)

  private static final List<ColumnDefinition> COLUMNS = List.of(
      new ColumnDefinition("ts", Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP),
      new ColumnDefinition("value", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD));

  // #7043
  /**
   * The issue's own repro: an all-NaN bucket must be ABSENT and not the MIN/MAX init sentinel.
   */
  @Test
  void anAllNaNBucketIsAbsent() throws Exception {
    database.begin();
    final TimeSeriesEngine engine = new TimeSeriesEngine((DatabaseInternal) database, "ts7043_allnan", COLUMNS, 1);
    engine.appendSamples(new long[] { 1000L, 2000L }, new Object[] { Double.NaN, Double.NaN });
    database.commit();

    try {
      database.begin();
      final List<MultiColumnAggregationRequest> requests = List.of(
          new MultiColumnAggregationRequest(1, AggregationType.MIN, "min"),
          new MultiColumnAggregationRequest(1, AggregationType.MAX, "max"));
      final MultiColumnAggregationResult multi = engine.aggregateMulti(Long.MIN_VALUE, Long.MAX_VALUE, requests, 0, null);
      assertThat(multi.size()).isEqualTo(1);
      final long bucketTs = multi.getBucketTimestamps().getFirst();

      // These two were Double.MAX_VALUE and -Double.MAX_VALUE.
      assertThat(multi.getValue(bucketTs, 0)).as("multi-column MIN over an all-NaN window").isNaN();
      assertThat(multi.getValue(bucketTs, 1)).as("multi-column MAX over an all-NaN window").isNaN();
      database.commit();
    } finally {
      engine.drop();
    }
  }

  // #7043
  /**
   * The guard the sentinel needed must not have cost the ordinary answer: a bucket that holds real values among
   * the NaN ones still reports those values, and a real {@code Double.MAX_VALUE} sample is data, not a sentinel.
   */
  @Test
  void realValuesStillWinAndTheExtremeDoubleIsData() throws Exception {
    database.begin();
    final TimeSeriesEngine engine = new TimeSeriesEngine((DatabaseInternal) database, "ts7043_mixed", COLUMNS, 1);
    engine.appendSamples(new long[] { 1000L, 2000L, 3000L },
        new Object[] { Double.NaN, Double.MAX_VALUE, -Double.MAX_VALUE });
    database.commit();

    try {
      database.begin();
      final List<MultiColumnAggregationRequest> requests = List.of(
          new MultiColumnAggregationRequest(1, AggregationType.MIN, "min"),
          new MultiColumnAggregationRequest(1, AggregationType.MAX, "max"));
      final MultiColumnAggregationResult multi = engine.aggregateMulti(Long.MIN_VALUE, Long.MAX_VALUE, requests, 0, null);
      final long bucketTs = multi.getBucketTimestamps().getFirst();

      assertThat(multi.getValue(bucketTs, 0)).isEqualTo(-Double.MAX_VALUE);
      assertThat(multi.getValue(bucketTs, 1)).isEqualTo(Double.MAX_VALUE);
      database.commit();
    } finally {
      engine.drop();
    }
  }

  // #7043
  /**
   * The same question asked of a SEALED block, where MIN/MAX are answered from the block header the compaction
   * wrote rather than from the samples: {@code reduceNumericStats} used to seed the very same
   * {@code ±Double.MAX_VALUE} into that header, so the aggregation push-down handed the sentinel back as a
   * measurement without ever decompressing a column.
   */
  @Test
  void anAllNaNSealedBlockIsAbsentToo() throws Exception {
    database.begin();
    final TimeSeriesEngine engine = new TimeSeriesEngine((DatabaseInternal) database, "ts7043_sealed", COLUMNS, 1);
    final long[] timestamps = new long[64];
    final Object[] values = new Object[64];
    for (int i = 0; i < timestamps.length; i++) {
      timestamps[i] = 1000L + i;
      values[i] = Double.NaN;
    }
    engine.appendSamples(timestamps, values);
    database.commit();

    try {
      database.begin();
      engine.compactAll();
      database.commit();

      database.begin();
      final List<MultiColumnAggregationRequest> requests = List.of(
          new MultiColumnAggregationRequest(1, AggregationType.MIN, "min"),
          new MultiColumnAggregationRequest(1, AggregationType.MAX, "max"));
      final MultiColumnAggregationResult multi = engine.aggregateMulti(Long.MIN_VALUE, Long.MAX_VALUE, requests, 0, null);
      final long bucketTs = multi.getBucketTimestamps().getFirst();
      assertThat(multi.getValue(bucketTs, 0)).as("sealed-block MIN over an all-NaN column").isNaN();
      assertThat(multi.getValue(bucketTs, 1)).as("sealed-block MAX over an all-NaN column").isNaN();

      // The DEEP check reconciles the declared statistics against the decoded values, and NaN is now a
      // legitimate declaration: a plain '!=' there would report this healthy block as damaged, because
      // NaN != NaN.
      assertThat(engine.checkIntegrity(TimeSeriesIntegrity.Options.deepOnly()).problems())
          .as("an all-NaN block declares NaN statistics and that is correct, not damage").isEmpty();
      database.commit();
    } finally {
      engine.drop();
    }
  }

  // #7043
  /**
   * A sealed block whose columns are a mix of numeric and text must still declare each column's statistics against
   * the right column. The writer emitted a triplet per column with non-NaN statistics and the reader consumed one
   * per column that is neither a TIMESTAMP nor a TAG - two different sets as soon as a FIELD is a STRING, which
   * shifted every following column's min/max/sum onto the wrong column.
   */
  @Test
  void blockStatisticsStayAlignedWithATextFieldBetweenTheNumericOnes() throws Exception {
    final List<ColumnDefinition> columns = List.of(
        new ColumnDefinition("ts", Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP),
        new ColumnDefinition("label", Type.STRING, ColumnDefinition.ColumnRole.FIELD),
        new ColumnDefinition("value", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD));

    database.begin();
    final TimeSeriesEngine engine = new TimeSeriesEngine((DatabaseInternal) database, "ts7043_textfield", columns, 1);
    final long[] timestamps = new long[32];
    final Object[] labels = new Object[32];
    final Object[] values = new Object[32];
    for (int i = 0; i < timestamps.length; i++) {
      timestamps[i] = 1000L + i;
      labels[i] = "l" + i;
      values[i] = 10.0 + i;
    }
    engine.appendSamples(timestamps, labels, values);
    database.commit();

    try {
      database.begin();
      engine.compactAll();
      database.commit();

      database.begin();
      final List<MultiColumnAggregationRequest> requests = List.of(
          new MultiColumnAggregationRequest(2, AggregationType.MIN, "min"),
          new MultiColumnAggregationRequest(2, AggregationType.MAX, "max"));
      final MultiColumnAggregationResult multi = engine.aggregateMulti(Long.MIN_VALUE, Long.MAX_VALUE, requests, 0, null);
      final long bucketTs = multi.getBucketTimestamps().getFirst();
      assertThat(multi.getValue(bucketTs, 0)).isEqualTo(10.0);
      assertThat(multi.getValue(bucketTs, 1)).isEqualTo(41.0);

      // The same misalignment would make the DEEP check reconcile a column against another column's statistics.
      assertThat(engine.checkIntegrity(TimeSeriesIntegrity.Options.deepOnly()).problems()).isEmpty();
      database.commit();
    } finally {
      engine.drop();
    }
  }

  // ---- #7694 ----
  // Issue #7694: the one absent-SUM/AVG contract, driven through every entry point that can decide it.
  // <p>
  // A SUM or AVG request holding no real sample in a bucket answers {@link TimeSeriesNaN#ABSENT}, whether it was
  // offered only absent samples or offered none at all. Issue #7506 briefly made those two cases differ, seeding SUM
  // at the additive identity so a request nothing was ever offered could answer the empty sum; #7694 removed the
  // distinction, because no query produces that state (only the single-request
  // {@link MultiColumnAggregationResult#accumulate(long, int, double)} constructs it, as the tests below do, and it
  // has no caller in {@code src/main}) and because it disagreed with SQL, where {@code SUM} is NULL over an empty
  // group and over an all-NULL group alike.
  // <p>
  // Every entry point issue #7506's coverage table listed is still driven below; what changed is the answer each one
  // asserts. The #7089 half - a request that was offered samples and found none real - is unchanged, and is kept here
  // alongside so the two can no longer drift apart.
  //
  // @see <a href="https://github.com/ArcadeData/arcadedb/issues/7694">issue #7694</a>

  private static final long HOUR = 3_600_000L;

  private static List<MultiColumnAggregationRequest> sumThenCount() {
    return List.of(new MultiColumnAggregationRequest(1, AggregationType.SUM, "sumA"),
        new MultiColumnAggregationRequest(2, AggregationType.COUNT, "countB"));
  }

  private static List<MultiColumnAggregationRequest> sumAndAvg() {
    return List.of(new MultiColumnAggregationRequest(1, AggregationType.SUM, "sumA"),
        new MultiColumnAggregationRequest(1, AggregationType.AVG, "avgA"),
        new MultiColumnAggregationRequest(2, AggregationType.COUNT, "countB"));
  }

  /**
   * Asserting the bucket is really there is what makes an absent answer read afterwards evidence about the SEED
   * that {@code newInitializedValues()} laid down for that request index, rather than about the bucket's
   * existence - which, since issue #7698, answers absent as well.
   */
  private static void assertBucketExists(final MultiColumnAggregationResult result, final long bucketTs) {
    assertThat(result.getBucketTimestamps())
        .as("the bucket must exist, or an absent answer proves nothing about the per-request seed")
        .contains(bucketTs);
  }

  // #7694
  // ---- the fold itself ----

  /**
   * The fold leaves an absent sample where it found the accumulator, which is why the SEED carries the whole
   * contract: with the absent seed "nothing real arrived" IS the value, and with a numeric seed the fold would
   * hand that number straight back. The last assertion is the one that would have to change for SUM to answer
   * zero again, and it is here so that it cannot change quietly.
   */
  @Test
  void anAbsentSampleLeavesTheAccumulatorWhereItFoundIt() {
    assertThat(TimeSeriesNaN.sum(TimeSeriesNaN.ABSENT, 0, Double.NaN)).as("absent seed, absent sample: absent")
        .isNaN();
    assertThat(TimeSeriesNaN.sum(TimeSeriesNaN.ABSENT, 0, 5.0)).as("the first real sample replaces the seed")
        .isEqualTo(5.0);
    assertThat(TimeSeriesNaN.sum(5.0, 1, Double.NaN)).as("a real total is not disturbed").isEqualTo(5.0);
    assertThat(TimeSeriesNaN.sum(0.0, 0, Double.NaN))
        .as("the fold does not manufacture an absence: a numeric seed survives, so the seed is the contract")
        .isEqualTo(0.0);
  }

  // #7694
  /**
   * Merging is the same rule: a partial with no real sample behind it is skipped whatever its value, so the
   * running accumulator - absent until a real sample reaches it - is what comes back.
   */
  @Test
  void mergingAPartialWithNoRealSampleLeavesTheAccumulator() {
    assertThat(TimeSeriesNaN.mergeSum(TimeSeriesNaN.ABSENT, 0, TimeSeriesNaN.ABSENT, 0)).as("absent + absent")
        .isNaN();
    assertThat(TimeSeriesNaN.mergeSum(TimeSeriesNaN.ABSENT, 0, 5.0, 1)).as("a real partial replaces an absent run")
        .isEqualTo(5.0);
    assertThat(TimeSeriesNaN.mergeSum(5.0, 1, TimeSeriesNaN.ABSENT, 0)).as("a real total is not disturbed")
        .isEqualTo(5.0);
    assertThat(TimeSeriesNaN.mergeSum(2.0, 1, 3.0, 1)).as("two totals are added").isEqualTo(5.0);
  }

  // #7694
  // ---- accumulate(bucketTs, requestIndex, value): the state the two PRs disagreed about ----

  @Test
  void mapModeUntouchedSumIsAbsentWhenAnotherRequestTouchedTheBucket() {
    final MultiColumnAggregationResult result = new MultiColumnAggregationResult(sumThenCount());
    result.accumulate(1000L, 1, 7.0);

    assertBucketExists(result, 1000L);
    assertThat(result.getValue(1000L, 0)).as("a SUM holding no real sample is absent, not a total of zero").isNaN();
    assertThat(result.getCount(1000L, 0)).isZero();
    assertThat(result.getValue(1000L, 1)).as("COUNT still counts rows").isEqualTo(1.0);
  }

  // #7694
  @Test
  void flatModeUntouchedSumIsAbsentWhenAnotherRequestTouchedTheBucket() {
    final MultiColumnAggregationResult result = new MultiColumnAggregationResult(sumThenCount(), 0L, 1000L, 16);
    result.accumulate(1000L, 1, 7.0);

    assertThat(result.isFlatMode()).isTrue();
    assertBucketExists(result, 1000L);
    assertThat(result.getValue(1000L, 0)).isNaN();
    assertThat(result.getValue(1000L, 1)).isEqualTo(1.0);
  }

  // #7694
  /**
   * A bucket outside the pre-allocated flat window is parked in the overflow map (issue #6937), which seeds its
   * accumulators through the same helper and must answer the same way.
   */
  @Test
  void flatModeOverflowBucketUntouchedSumIsAbsent() {
    final MultiColumnAggregationResult result = new MultiColumnAggregationResult(sumThenCount(), 0L, 1000L, 2);
    result.accumulate(5000L, 1, 7.0);

    assertThat(result.isFlatMode()).isTrue();
    assertThat(result.getOverflowBucketCount()).as("the bucket fell outside the window").isEqualTo(1);
    assertBucketExists(result, 5000L);
    assertThat(result.getValue(5000L, 0)).isNaN();
    assertThat(result.getValue(5000L, 1)).isEqualTo(1.0);
  }

  // #7694
  /**
   * A bucket NO request ever touched used to be the one residue of this decision - it answered {@code 0.0} for
   * every aggregate, SUM and MIN alike, from an arm of {@code getValue} that predates the whole policy. Issue
   * #7698 closed it: the bucket that does not exist now answers absent like the request that was offered nothing,
   * because they are the same question. It is still absent from {@code getBucketTimestamps()}, which is why no
   * reader in {@code src/main} could observe either answer.
   *
   * @see MultiColumnAggregationResultTest
   */
  @Test
  void aBucketNoRequestEverTouchedAnswersAbsentAndIsNotInTheTimestamps() {
    final MultiColumnAggregationResult mapMode = new MultiColumnAggregationResult(sumThenCount());
    mapMode.accumulate(1000L, 1, 7.0);
    assertThat(mapMode.getBucketTimestamps()).as("no reader can reach 2000L: it is not a bucket")
        .doesNotContain(2000L);
    assertThat(mapMode.getValue(2000L, 0)).isNaN();

    final MultiColumnAggregationResult flatMode = new MultiColumnAggregationResult(sumThenCount(), 0L, 1000L, 16);
    flatMode.accumulate(1000L, 1, 7.0);
    assertThat(flatMode.getBucketTimestamps()).doesNotContain(2000L);
    assertThat(flatMode.getValue(2000L, 0)).isNaN();
  }

  // #7694
  // ---- the #7089 half: offered, but nothing real ----

  @Test
  void aSumOfferedOnlyAbsentSamplesStaysAbsentInBothModes() {
    final MultiColumnAggregationResult mapMode = new MultiColumnAggregationResult(sumThenCount());
    mapMode.accumulate(1000L, 0, Double.NaN);
    assertThat(mapMode.getValue(1000L, 0)).as("offered an absent sample: absent, not zero").isNaN();
    assertThat(mapMode.getCount(1000L, 0)).isZero();

    final MultiColumnAggregationResult flatMode = new MultiColumnAggregationResult(sumThenCount(), 0L, 1000L, 16);
    flatMode.accumulate(1000L, 0, Double.NaN);
    assertThat(flatMode.getValue(1000L, 0)).isNaN();
  }

  // #7694
  @Test
  void accumulateRowOfAbsentSamplesKeepsTheSumAbsent() {
    final MultiColumnAggregationResult result = new MultiColumnAggregationResult(sumAndAvg(), 0L, HOUR, 2);
    result.accumulateRow(0L, new double[] { Double.NaN, Double.NaN, 1.0 });
    result.accumulateRow(0L, new double[] { Double.NaN, Double.NaN, 1.0 });
    result.finalizeAvg();

    assertThat(result.getValue(0L, 0)).as("all-absent SUM").isNaN();
    assertThat(result.getValue(0L, 1)).as("all-absent AVG").isNaN();
    assertThat(result.getValue(0L, 2)).as("COUNT counts rows").isEqualTo(2.0);
  }

  // #7694
  /**
   * The block-statistics fast path hands over a value with the count of the real samples behind it. A count of
   * zero means the block's column carried nothing real, and the merge must not let the seed show through.
   */
  @Test
  void blockStatsWithAZeroRealCountKeepsTheSumAbsent() {
    final MultiColumnAggregationResult result = new MultiColumnAggregationResult(sumAndAvg(), 0L, HOUR, 2);
    result.accumulateBlockStats(0L, new double[] { TimeSeriesNaN.ABSENT, TimeSeriesNaN.ABSENT, 4.0 },
        new long[] { 0, 0, 4 });
    result.finalizeAvg();

    assertThat(result.getValue(0L, 0)).isNaN();
    assertThat(result.getValue(0L, 1)).isNaN();
    assertThat(result.getValue(0L, 2)).isEqualTo(4.0);
  }

  // #7694
  /**
   * The vectorized segment path, and the decision itself in one test: a request that was never asked and one that
   * was asked and saw nothing real now answer the SAME thing. That agreement is what #7694 bought, and the
   * assertion that would go red if the seed moved back.
   */
  @Test
  void singleStatAnswersAbsentForAnUntouchedRequestAndForAnAbsentOneAlike() {
    final MultiColumnAggregationResult untouched = new MultiColumnAggregationResult(sumThenCount(), 0L, HOUR, 2);
    untouched.accumulateSingleStat(0L, 1, 3.0, 3);
    assertBucketExists(untouched, 0L);
    assertThat(untouched.getValue(0L, 0)).as("the SUM request was never asked").isNaN();

    final MultiColumnAggregationResult absent = new MultiColumnAggregationResult(sumThenCount(), 0L, HOUR, 2);
    absent.accumulateSingleStat(0L, 0, TimeSeriesNaN.ABSENT, 0);
    absent.accumulateSingleStat(0L, 1, 3.0, 3);
    assertThat(absent.getValue(0L, 0)).as("the SUM request was asked and saw nothing real").isNaN();
  }

  // #7694
  /**
   * A SUM that really did total to zero must stay the number zero. This is the distinction the absent marker
   * exists to make, and the reason "absent" is not simply "zero by another name".
   */
  @Test
  void aSumOfRealSamplesThatCancelToZeroIsStillTheNumberZero() {
    final MultiColumnAggregationResult result = new MultiColumnAggregationResult(sumAndAvg(), 0L, HOUR, 2);
    result.accumulateRow(0L, new double[] { 2.5, 2.5, 1.0 });
    result.accumulateRow(0L, new double[] { -2.5, -2.5, 1.0 });
    result.finalizeAvg();

    assertThat(result.getValue(0L, 0)).as("a real total of zero is data, not a gap").isEqualTo(0.0);
    assertThat(result.getValue(0L, 1)).isEqualTo(0.0);
    assertThat(result.getCount(0L, 0)).isEqualTo(2);
  }

  // #7694
  // ---- merging ----

  @Test
  void mergingFlatResultsKeepsAnEmptySumAbsentOnBothSides() {
    final MultiColumnAggregationResult untouchedLeft = new MultiColumnAggregationResult(sumThenCount(), 0L, HOUR, 2);
    final MultiColumnAggregationResult untouchedRight = new MultiColumnAggregationResult(sumThenCount(), 0L, HOUR, 2);
    untouchedRight.accumulate(0L, 1, 7.0);
    untouchedLeft.mergeFrom(untouchedRight);
    assertBucketExists(untouchedLeft, 0L);
    assertThat(untouchedLeft.getValue(0L, 0)).as("neither side ever asked the SUM request").isNaN();
    assertThat(untouchedLeft.getValue(0L, 1)).isEqualTo(1.0);

    final MultiColumnAggregationResult absentLeft = new MultiColumnAggregationResult(sumThenCount(), 0L, HOUR, 2);
    final MultiColumnAggregationResult absentRight = new MultiColumnAggregationResult(sumThenCount(), 0L, HOUR, 2);
    absentRight.accumulate(0L, 0, Double.NaN);
    absentLeft.mergeFrom(absentRight);
    assertThat(absentLeft.getValue(0L, 0)).as("an absent partial leaves the merged sum absent").isNaN();

    // A real total still wins over both shapes of nothing.
    final MultiColumnAggregationResult realLeft = new MultiColumnAggregationResult(sumThenCount(), 0L, HOUR, 2);
    final MultiColumnAggregationResult realRight = new MultiColumnAggregationResult(sumThenCount(), 0L, HOUR, 2);
    realLeft.accumulate(0L, 0, 2.0);
    realRight.accumulate(0L, 0, 3.0);
    realLeft.mergeFrom(realRight);
    assertThat(realLeft.getValue(0L, 0)).isEqualTo(5.0);
  }

  // #7694
  @Test
  void mergingMapModeResultsAgreesWithFlatMode() {
    final MultiColumnAggregationResult left = new MultiColumnAggregationResult(sumThenCount());
    final MultiColumnAggregationResult right = new MultiColumnAggregationResult(sumThenCount());
    right.accumulate(1000L, 1, 7.0);
    left.mergeFrom(right);

    assertBucketExists(left, 1000L);
    assertThat(left.getValue(1000L, 0)).isNaN();
    assertThat(left.getValue(1000L, 1)).isEqualTo(1.0);

    final MultiColumnAggregationResult absentLeft = new MultiColumnAggregationResult(sumThenCount());
    final MultiColumnAggregationResult absentRight = new MultiColumnAggregationResult(sumThenCount());
    absentRight.accumulate(1000L, 0, Double.NaN);
    absentLeft.mergeFrom(absentRight);
    assertThat(absentLeft.getValue(1000L, 0)).isNaN();
  }

  // #7694
  // ---- the other three aggregates ----

  /**
   * SUM and AVG now answer alike for an empty request, which is the agreement issue #7506's seed gave up. COUNT
   * is the one aggregate outside the policy and still answers the number zero.
   */
  @Test
  void anUntouchedSumAndAnUntouchedAvgBothStayAbsentWhileCountStaysZero() {
    final MultiColumnAggregationResult mapMode = new MultiColumnAggregationResult(sumAndAvg());
    mapMode.accumulate(1000L, 2, 7.0);
    mapMode.finalizeAvg();
    assertBucketExists(mapMode, 1000L);
    assertThat(mapMode.getValue(1000L, 0)).as("SUM of nothing is absent").isNaN();
    assertThat(mapMode.getValue(1000L, 1)).as("AVG of nothing is absent").isNaN();

    final MultiColumnAggregationResult flatMode = new MultiColumnAggregationResult(sumAndAvg(), 0L, 1000L, 16);
    flatMode.accumulate(1000L, 2, 7.0);
    flatMode.finalizeAvg();
    assertThat(flatMode.getValue(1000L, 0)).isNaN();
    assertThat(flatMode.getValue(1000L, 1)).isNaN();

    // The counter-case, so "absent" cannot quietly become "everything empty is absent".
    final MultiColumnAggregationResult countEmpty = new MultiColumnAggregationResult(sumThenCount());
    countEmpty.accumulate(1000L, 0, 7.0);
    assertThat(countEmpty.getValue(1000L, 1)).as("COUNT of no rows is 0, not absent").isEqualTo(0.0);
  }

  // #7694
  /**
   * MIN/MAX keep the answer issue #4596 needed: absent, whether the request was asked or not, because there is no
   * identity element to fall back on.
   */
  @Test
  void anUntouchedMinOrMaxStaysAbsent() {
    final MultiColumnAggregationResult result = new MultiColumnAggregationResult(
        List.of(new MultiColumnAggregationRequest(1, AggregationType.MIN, "minA"),
            new MultiColumnAggregationRequest(1, AggregationType.MAX, "maxA"),
            new MultiColumnAggregationRequest(2, AggregationType.COUNT, "countB")));
    result.accumulate(1000L, 2, 7.0);

    assertBucketExists(result, 1000L);
    assertThat(result.getValue(1000L, 0)).isNaN();
    assertThat(result.getValue(1000L, 1)).isNaN();
  }

  // ---- #7584 ----
  // Issue #7584, on the SQL push-down: the third reader of {@code MultiColumnAggregationResult.getValue()}.
  // <p>
  // Issue #7089 put SUM and AVG under the NaN-as-absent policy, so a bucket with no real sample for a column now
  // answers {@code NaN} rather than {@code 0.0}. Unlike the two HTTP handlers, {@code AggregateFromTimeSeriesStep}
  // sets the raw double as a result property instead of routing it through {@code putSampleValue}. That is still
  // correct at the response boundary, but only because {@link JSONObject#put(String, Object)} sends a {@code Number}
  // to the NaN-aware overload, which maps it to JSON {@code null} - where {@code JSONArray.put(Number)} would have
  // mapped it to {@code 0}. The coupling is implicit, so these tests pin both ends of it.

  private static final String TYPE  = "PushDownMetric";

  // ts.timeBucket takes the INTERVAL first and the timestamp column second, and the planner only pushes the
  // aggregation down when GROUP BY names the bucket alias. Get either wrong and the query still answers, from the
  // generic aggregation path - which is not the path this class is about, hence the plan assertion below.
  private static final String QUERY = "SELECT ts.timeBucket('1h', ts) AS tsBucket, sum(value) AS s, avg(value) AS a"
      + " FROM " + TYPE + " GROUP BY tsBucket";

  // #7584
  /**
   * Every sample in the single bucket is absent, so the push-down's SUM and AVG are absent too - and the JSON a
   * client receives carries {@code null}, not a measurement of zero.
   * <p>
   * The RESULT PROPERTY is {@code null} as well since issue #7743: the step translates the marker at the SQL
   * boundary rather than handing the raw NaN out and relying on {@link JSONObject#put(String, Object)} to map it
   * on the way to the client. The coupling this class was written to pin is therefore no longer load-bearing -
   * it is still asserted below, because the serializer must keep answering {@code null} whichever value reaches
   * it - and the embedded SQL caller now reads the same word the HTTP one always did, and the same word the
   * generic aggregation answers for a group it had nothing to average.
   */
  @Test
  void anAllAbsentBucketPushesDownAsAbsentAndSerializesToJsonNull() {
    createTypeWithSamples(new long[] { 1_000L, 2_000L }, new Object[] { Double.NaN, Double.NaN });

    final Result row = singleRow();
    assertThat(row.<Double>getProperty("s")).as("SUM over no real sample is absent, spelled NULL in SQL").isNull();
    assertThat(row.<Double>getProperty("a")).as("AVG over no real sample is absent, spelled NULL in SQL").isNull();

    final JSONObject json = row.toJSON();
    assertThat(json.isNull("s")).as("the absent SUM must serialize to JSON null, never to 0").isTrue();
    assertThat(json.isNull("a")).as("the absent AVG must serialize to JSON null, never to 0").isTrue();
  }

  // #7584
  /**
   * The counter-case that makes the null above meaningful: real samples that cancel to zero stay the number zero
   * through both the push-down and the serializer.
   */
  @Test
  void aRealTotalOfZeroStaysTheNumberZero() {
    createTypeWithSamples(new long[] { 1_000L, 2_000L }, new Object[] { 2.5, -2.5 });

    final Result row = singleRow();
    assertThat(row.<Double>getProperty("s")).isEqualTo(0.0);

    final JSONObject json = row.toJSON();
    assertThat(json.isNull("s")).as("a real total of zero is data, not a gap").isFalse();
    assertThat(json.getDouble("s")).isEqualTo(0.0);
  }

  // #7584
  /**
   * And an absent sample among real ones is skipped rather than propagated: the SUM totals the real samples and
   * the AVG divides by their count.
   */
  @Test
  void absentSamplesAreSkippedRatherThanPoisoningThePushedDownTotal() {
    createTypeWithSamples(new long[] { 1_000L, 2_000L, 3_000L },
        new Object[] { 4.0, Double.NaN, 6.0 });

    final Result row = singleRow();
    assertThat(row.<Double>getProperty("s")).as("SUM of the real samples only").isEqualTo(10.0);
    assertThat(row.<Double>getProperty("a")).as("AVG divides by the 2 real samples, not by 3").isEqualTo(5.0);
  }

  /**
   * Runs {@link #QUERY} and returns its single bucket, having first proved the row came from
   * {@code AggregateFromTimeSeriesStep} rather than from the generic aggregation path, which would answer the same
   * query - with the same numbers - if the push-down declined it, and so would let this class pass without ever
   * touching the code it is named for.
   */
  private Result singleRow() {
    try (final ResultSet rs = database.query("sql", QUERY)) {
      final String plan = rs.getExecutionPlan()
          .orElseThrow(() -> new AssertionError("the query produced no execution plan to check"))
          .prettyPrint(0, 2);
      // AggregateFromTimeSeriesStep renders itself as this label, which is what the plan carries - the class
      // name never appears in prettyPrint() output.
      assertThat(plan).as("this test is about the TimeSeries push-down, so the plan must contain its step")
          .contains("AGGREGATE FROM TIMESERIES " + TYPE);

      assertThat(rs.hasNext()).as("the push-down must produce the one bucket the samples fall in").isTrue();
      final Result row = rs.next();
      assertThat(rs.hasNext()).as("one bucket only").isFalse();
      return row;
    }
  }

  /**
   * A NaN sample has no SQL literal, so the samples go in through the engine rather than through INSERT.
   */
  private void createTypeWithSamples(final long[] timestamps, final Object[] values) {
    database.command("sql", "CREATE TIMESERIES TYPE " + TYPE + " TIMESTAMP ts FIELDS (value DOUBLE) SHARDS 1");

    final LocalTimeSeriesType tsType = (LocalTimeSeriesType) database.getSchema().getType(TYPE);
    database.transaction(() -> {
      try {
        tsType.getEngine().appendSamples(timestamps, values);
      } catch (final Exception e) {
        throw new IllegalStateException("cannot append the test samples", e);
      }
    });
  }

  // ---- #9351 ----
  // Issue #9351: the generic SQL {@code sum()} answered the Integer 0 for a group holding no value (an empty group or an
  // all-NULL/absent one) while the time-series push-down answered NULL for the same samples. SQL says SUM over no value is
  // NULL, which is also what {@link TimeSeriesNaN} documents as its model.
  //
  // @author Luca Garulli (l.garulli@arcadedata.com)

  // #9351
  @Test
  void genericSumOverAnAllAbsentTimeSeriesWindowIsNull() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE P9351 TIMESTAMP ts TAGS (host STRING) FIELDS (v DOUBLE) SHARDS 1");
    final int n = 20;
    final long[] ts = new long[n];
    final Object[] host = new Object[n];
    final Object[] v = new Object[n];
    for (int i = 0; i < n; i++) {
      ts[i] = (i < 10 ? 0L : HOUR) + (i % 10) * 60_000L;
      host[i] = "h1";
      v[i] = i < 10 ? (double) (i + 1) : Double.NaN;
    }
    ((LocalTimeSeriesType) database.getSchema().getType("P9351")).getEngine().appendSamples(ts, host, v);

    final String window = "ts >= " + HOUR + " AND ts < " + (2 * HOUR);

    try (final ResultSet rs = database.query("sql", "SELECT sum(v) AS s, count(*) AS n FROM P9351 WHERE " + window)) {
      final Result r = rs.next();
      assertThat(r.<Object>getProperty("s")).as("generic SUM over only absent samples").isNull();
      assertThat(r.<Long>getProperty("n")).isEqualTo(10L);
    }
    try (final ResultSet rs = database.query("sql",
        "SELECT host, sum(v) AS s FROM P9351 WHERE " + window + " GROUP BY host")) {
      assertThat(rs.next().<Object>getProperty("s")).isNull();
    }
    // the window that holds real samples still sums
    try (final ResultSet rs = database.query("sql", "SELECT sum(v) AS s FROM P9351 WHERE ts < " + HOUR)) {
      assertThat(rs.next().<Number>getProperty("s").doubleValue()).isEqualTo(55.0);
    }
  }

  // #9351
  @Test
  void aGroupWithSomeNullsStillSumsTheRest() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Partial9351");
      database.newDocument("Partial9351").set("g", "a").set("x", 5).save();
      database.newDocument("Partial9351").set("g", "a").save();
      database.newDocument("Partial9351").set("g", "b").save();
    });
    try (final ResultSet rs = database.query("sql", "SELECT g, sum(x) AS s FROM Partial9351 GROUP BY g ORDER BY g")) {
      assertThat(rs.next().<Number>getProperty("s").intValue()).isEqualTo(5);
      assertThat(rs.next().<Object>getProperty("s")).as("the all-NULL group").isNull();
    }
  }

  // #9351
  /** Cypher keeps Neo4j's answer: sum() over no value is 0, on an empty match and on all-null values alike. */
  @Test
  void cypherSumOverNoValueStaysZero() {
    database.transaction(() -> {
      database.getSchema().createVertexType("V9351");
      database.newVertex("V9351").set("k", 1).save();
    });
    try (final ResultSet rs = database.query("opencypher", "MATCH (n:V9351) WHERE n.k > 100 RETURN sum(n.x) AS s")) {
      assertThat(rs.next().<Number>getProperty("s").intValue()).as("empty match").isZero();
    }
    try (final ResultSet rs = database.query("opencypher", "MATCH (n:V9351) RETURN sum(n.x) AS s")) {
      assertThat(rs.next().<Number>getProperty("s").intValue()).as("all-null values").isZero();
    }
    // an explicit sql.sum() keeps the SQL answer
    try (final ResultSet rs = database.query("opencypher", "MATCH (n:V9351) RETURN sql.sum(n.x) AS s")) {
      assertThat(rs.next().<Object>getProperty("s")).as("sql.sum over all-null values").isNull();
    }
  }

  // #9351
  @Test
  void mergingAPartialThatSawNoValueKeepsTheOtherSide() {
    final SQLFunctionSum seen = new SQLFunctionSum();
    seen.execute(null, null, null, new Object[] { 4 }, null);
    final SQLFunctionSum none = new SQLFunctionSum();

    seen.mergePartial(none);
    assertThat(seen.getResult()).isEqualTo(4);

    none.mergePartial(new SQLFunctionSum());
    assertThat(none.getResult()).as("neither side saw a value").isNull();
  }

  // #9351
  @Test
  void sumOverNoValueOnADocumentTypeIsNull() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("D9351");
      for (int k = 0; k < 4; k++)
        database.newDocument("D9351").set("k", k).save();
    });

    try (final ResultSet rs = database.query("sql", "SELECT sum(x) AS s, count(*) AS n FROM D9351 WHERE k > 1000")) {
      assertThat(rs.next().<Object>getProperty("s")).as("empty group").isNull();
    }
    try (final ResultSet rs = database.query("sql", "SELECT sum(x) AS s, count(*) AS n FROM D9351")) {
      final Result r = rs.next();
      assertThat(r.<Object>getProperty("s")).as("all-NULL group").isNull();
      assertThat(r.<Long>getProperty("n")).isEqualTo(4L);
    }
    try (final ResultSet rs = database.query("sql", "SELECT sum(k) AS s FROM D9351")) {
      assertThat(rs.next().<Number>getProperty("s").intValue()).isEqualTo(6);
    }
  }

  // ---- #7743 ----
  // Issue #7743: a {@code null} field value - the natural way a client says "no measurement here" - was stored and
  // read back as a real {@code 0.0}, so AVG counted it and MIN returned it.
  // <p>
  // Two samples in one bucket, one of 10.0 and one null, answered {@code avg=5.0} and {@code min=0.0}: the null had
  // become a measurement smaller than every real one, and the sum was right only by the accident that zero is the
  // additive identity. An explicit {@code Double.NaN} behaved correctly all along, because NaN IS the absent marker
  // of the whole subsystem ({@link TimeSeriesNaN}), which is what makes the null the odd one out rather than the
  // NaN policy being wrong.
  // <p>
  // The fix stores a null measurement AS the absent marker wherever the column can carry one - a floating-point
  // column, whose NaN both layers round-trip: {@code TimeSeriesBatch.rawNull} on the mutable page,
  // {@link ColumnDefinition#storedNumericValueOf} in the {@code GORILLA_XOR} encoder and in the block statistics the
  // push-down answers from. An integer column has no value to spend on absence and keeps storing zero, which is what
  // it always did on both layers, so nothing there changes or diverges.
  //
  // @see <a href="https://github.com/ArcadeData/arcadedb/issues/7743">issue #7743</a>
  //
  // @author Luca Garulli (l.garulli@arcadedata.com)

  private static final long BASE_TS = 1_700_000_000_000L;

  private TimeSeriesEngine create(final String typeName, final String fieldType) {
    database.command("sql",
        "CREATE TIMESERIES TYPE " + typeName + " TIMESTAMP ts FIELDS (value " + fieldType + ") SHARDS 1");
    return ((LocalTimeSeriesType) database.getSchema().getType(typeName)).getEngine();
  }

  private static void appendRealAndNull(final TimeSeriesEngine engine) throws IOException {
    engine.appendSamples(new long[] { BASE_TS, BASE_TS + 1_000 }, new Object[][] { new Object[] { 10.0d, null } });
  }

  /** {@code SELECT sum/avg/min/count} over the two samples, as the issue's repro runs it. */
  private void assertAggregates(final String typeName, final double sum, final double avg, final double min,
      final long count) {
    try (final ResultSet rs = database.query("sql",
        "SELECT sum(value) AS s, avg(value) AS a, min(value) AS mn, count(*) AS c FROM " + typeName)) {
      final Result row = rs.next();
      assertThat(((Number) row.getProperty("s")).doubleValue()).as("sum").isEqualTo(sum);
      assertThat(((Number) row.getProperty("a")).doubleValue()).as("avg").isEqualTo(avg);
      assertThat(((Number) row.getProperty("mn")).doubleValue()).as("min").isEqualTo(min);
      assertThat(((Number) row.getProperty("c")).longValue()).as("count counts ROWS, absent sample included")
          .isEqualTo(count);
    }
  }

  // #7743
  @Test
  void theMutableLayerTreatsANullAsNoSampleAtAll() throws Exception {
    final TimeSeriesEngine engine = create("Mutable", "DOUBLE");
    appendRealAndNull(engine);

    assertAggregates("Mutable", 10.0d, 10.0d, 10.0d, 2);
  }

  // #7743
  @Test
  void theSealedLayerAnswersTheSameAfterCompaction() throws Exception {
    final TimeSeriesEngine engine = create("Sealed", "DOUBLE");
    appendRealAndNull(engine);
    engine.compactAll();

    assertAggregates("Sealed", 10.0d, 10.0d, 10.0d, 2);
  }

  // #7743
  /**
   * The two layers have to agree sample by sample, not only in aggregate: a read of the raw rows is what the
   * projection endpoints hand back, and a phantom 0.0 there is a measurement the client never took.
   */
  @Test
  void bothLayersReadTheNullBackAsTheAbsentMarker() throws Exception {
    final TimeSeriesEngine engine = create("RawRows", "DOUBLE");
    appendRealAndNull(engine);

    database.begin();
    try {
      assertThat(measurements(engine)).containsExactly(10.0d, TimeSeriesNaN.ABSENT);
    } finally {
      database.commit();
    }

    engine.compactAll();

    database.begin();
    try {
      assertThat(measurements(engine))
          .as("compaction is not a question the caller asked, so it cannot change the answer")
          .containsExactly(10.0d, TimeSeriesNaN.ABSENT);
    } finally {
      database.commit();
    }
  }

  /** The ENGINE boundary, where absence is the NaN marker; the SQL boundary spells the same thing NULL. */
  private static List<Double> measurements(final TimeSeriesEngine engine) throws IOException {
    return engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, null).stream()
        .map(row -> ((Number) row[1]).doubleValue())
        .toList();
  }

  // #7743
  /** And the SQL boundary on the same rows: a null measurement reads back as SQL NULL, not as NaN. */
  @Test
  void aProjectedNullMeasurementReadsBackAsSqlNull() throws Exception {
    final TimeSeriesEngine engine = create("Projected", "DOUBLE");
    appendRealAndNull(engine);

    try (final ResultSet rs = database.query("sql", "SELECT value FROM Projected ORDER BY ts")) {
      assertThat(rs.next().<Object>getProperty("value")).isEqualTo(10.0d);
      assertThat(rs.next().<Object>getProperty("value")).as("no measurement is NULL, not NaN and not 0.0").isNull();
    }
  }

  // #7743
  /** A FLOAT column carries the marker too, and its narrower codec round-trips it. */
  @Test
  void aFloatColumnCarriesTheMarkerAsWell() throws Exception {
    final TimeSeriesEngine engine = create("Floats", "FLOAT");
    engine.appendSamples(new long[] { BASE_TS, BASE_TS + 1_000 }, new Object[][] { new Object[] { 10.0f, null } });

    assertAggregates("Floats", 10.0d, 10.0d, 10.0d, 2);
    engine.compactAll();
    assertAggregates("Floats", 10.0d, 10.0d, 10.0d, 2);
  }

  // #7743
  /**
   * An integer column has no absent marker in its stored form, so its rows and blocks carry a null bitmap since issue #9721:
   * a null there is absent too, on both layers, exactly as on a floating-point column.
   */
  @Test
  void anIntegerColumnStoresANullAsAbsentOnBothLayers() throws Exception {
    final TimeSeriesEngine engine = create("Longs", "LONG");
    engine.appendSamples(new long[] { BASE_TS, BASE_TS + 1_000 }, new Object[][] { new Object[] { 10L, null } });

    assertAggregates("Longs", 10.0d, 10.0d, 10.0d, 2);
    engine.compactAll();
    assertAggregates("Longs", 10.0d, 10.0d, 10.0d, 2);
  }

  // #7743
  /**
   * A DOUBLE column explicitly sealed with {@code DICTIONARY}, which is a supported pairing (issue #7689): the
   * codec stores the text form, so the null used to compact to {@code "0.0"} and now compacts to {@code "NaN"} -
   * which {@code ColumnDefinition.boxString} parses straight back to the marker. Fixed as a side effect rather
   * than by its own code path, so it gets its own test (code review on PR #7747).
   */
  @Test
  void aDictionaryEncodedDoubleColumnCarriesTheMarkerToo() throws Exception {
    database.command("sql",
        "CREATE TIMESERIES TYPE Dict TIMESTAMP ts FIELDS (value DOUBLE CODEC DICTIONARY) SHARDS 1");
    final TimeSeriesEngine engine = ((LocalTimeSeriesType) database.getSchema().getType("Dict")).getEngine();
    appendRealAndNull(engine);

    assertAggregates("Dict", 10.0d, 10.0d, 10.0d, 2);
    engine.compactAll();
    assertAggregates("Dict", 10.0d, 10.0d, 10.0d, 2);
  }

  // #7743
  /** An explicit NaN and a null are now the same thing, which is what made the NaN the working case. */
  @Test
  void anExplicitNaNAndANullAreIndistinguishable() throws Exception {
    final TimeSeriesEngine withNaN = create("NanSamples", "DOUBLE");
    withNaN.appendSamples(new long[] { BASE_TS, BASE_TS + 1_000 },
        new Object[][] { new Object[] { 10.0d, Double.NaN } });
    final TimeSeriesEngine withNull = create("NullSamples", "DOUBLE");
    appendRealAndNull(withNull);

    assertAggregates("NanSamples", 10.0d, 10.0d, 10.0d, 2);
    assertAggregates("NullSamples", 10.0d, 10.0d, 10.0d, 2);
  }

  // #7743
  /**
   * A whole bucket of nulls is an absent window, not a window that measured zero: MIN and AVG have nothing to
   * answer with and say so.
   * <p>
   * {@code SUM} is left out here on purpose. What SUM answers over a group with no value is the generic SQL
   * function's question, not this one's - it answers 0 for an empty group whatever the source - and #7694 owns
   * it. What this test is about is that no null CONTRIBUTED a zero.
   */
  @Test
  void aWindowOfNothingButNullsIsAbsentRatherThanZero() throws Exception {
    final TimeSeriesEngine engine = create("AllNull", "DOUBLE");
    engine.appendSamples(new long[] { BASE_TS, BASE_TS + 1_000 }, new Object[][] { new Object[] { null, null } });
    engine.compactAll();

    try (final ResultSet rs = database.query("sql",
        "SELECT avg(value) AS a, min(value) AS mn, count(*) AS c FROM AllNull")) {
      final Result row = rs.next();
      assertThat(isAbsent(row.getProperty("a"))).as("avg over no real sample").isTrue();
      assertThat(isAbsent(row.getProperty("mn"))).as("min over no real sample, NOT a phantom 0.0").isTrue();
      assertThat(((Number) row.getProperty("c")).longValue()).as("the rows are still there").isEqualTo(2);
    }
  }

  /**
   * At the SQL boundary "absent" is spelled {@code NULL} and nothing else - not NaN, which is the STORAGE
   * spelling. Asserted strictly on purpose (code review on PR #7747): a helper that accepted either would let
   * the marker leak into a result property without failing anything.
   */
  private static boolean isAbsent(final Object value) {
    return value == null;
  }

  // #7743
  // ---- the aggregation push-down, which is a different plan for the same question ----

  /**
   * The query shape the issue reports, and the one that is PUSHED DOWN: {@code GROUP BY} on the time-bucket alias
   * routes through {@code AggregateFromTimeSeriesStep} instead of the generic aggregators, so it reads the
   * engine's numbers directly rather than the rows. Both layers, because compaction must not change the answer.
   */
  @Test
  void theGroupedPushDownAnswersTheRealSamplesOnBothLayers() throws Exception {
    final TimeSeriesEngine engine = create("Grouped", "DOUBLE");
    appendRealAndNull(engine);

    assertGroupedAggregates("Grouped", 10.0d, 10.0d, 10.0d);
    engine.compactAll();
    assertGroupedAggregates("Grouped", 10.0d, 10.0d, 10.0d);
  }

  // #7743
  /**
   * A pushed-down bucket that measured nothing answers {@code NULL}, the same word the generic aggregation and
   * the raw-row projection answer - not the raw NaN marker (code review on PR #7747). A client that read NaN
   * from one plan and NULL from the other would be reading the PLAN rather than the data, which is the very
   * divergence this PR exists to remove.
   */
  @Test
  void aPushedDownBucketWithNoRealSampleAnswersNullRatherThanTheMarker() throws Exception {
    final TimeSeriesEngine engine = create("GroupedAllNull", "DOUBLE");
    engine.appendSamples(new long[] { BASE_TS, BASE_TS + 1_000 }, new Object[][] { new Object[] { null, null } });
    engine.compactAll();

    try (final ResultSet rs = database.query("sql", groupedQuery("GroupedAllNull"))) {
      final Result row = rs.next();
      assertThat(isAbsent(row.getProperty("s"))).as("SUM over no real sample").isTrue();
      assertThat(isAbsent(row.getProperty("a"))).as("AVG over no real sample").isTrue();
      assertThat(isAbsent(row.getProperty("mn"))).as("MIN over no real sample, NOT a phantom 0.0").isTrue();
    }
  }

  // #7743
  /**
   * The other NaN, and why the translation is keyed on the COUNT of real contributors rather than on the value
   * (CodeRabbit on PR #7747): a SUM over {@code +Infinity} and {@code -Infinity} is NaN with real samples behind
   * it - an undefined TOTAL, which IEEE keeps and so does {@link TimeSeriesNaN#sum} - not an absence. Turning
   * that into NULL would report "nothing was measured" about two measurements.
   */
  @Test
  void anArithmeticNaNOverRealSamplesIsNotTurnedIntoNull() throws Exception {
    final TimeSeriesEngine engine = create("Undefined", "DOUBLE");
    engine.appendSamples(new long[] { BASE_TS, BASE_TS + 1_000 },
        new Object[][] { new Object[] { Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY } });

    try (final ResultSet rs = database.query("sql", groupedQuery("Undefined"))) {
      final Result row = rs.next();
      assertThat(row.<Object>getProperty("s")).as("an undefined total is an answer, not an absence").isNotNull();
      assertThat(((Number) row.getProperty("s")).doubleValue()).isNaN();
      assertThat(((Number) row.getProperty("c")).longValue()).isEqualTo(2);
    }

    engine.compactAll();
    try (final ResultSet rs = database.query("sql", groupedQuery("Undefined"))) {
      assertThat(rs.next().<Object>getProperty("s")).as("and the sealed layer agrees").isNotNull();
    }
  }

  private static String groupedQuery(final String typeName) {
    // ts.timeBucket takes the INTERVAL first and the timestamp column second, and the planner pushes the
    // aggregation down only when GROUP BY names the bucket alias.
    return "SELECT ts.timeBucket('1h', ts) AS b, sum(value) AS s, avg(value) AS a, min(value) AS mn, count(*) AS c"
        + " FROM " + typeName + " GROUP BY b";
  }

  private void assertGroupedAggregates(final String typeName, final double sum, final double avg, final double min) {
    try (final ResultSet rs = database.query("sql", groupedQuery(typeName))) {
      final Result row = rs.next();
      assertThat(((Number) row.getProperty("s")).doubleValue()).as("sum").isEqualTo(sum);
      assertThat(((Number) row.getProperty("a")).doubleValue()).as("avg").isEqualTo(avg);
      assertThat(((Number) row.getProperty("mn")).doubleValue()).as("min").isEqualTo(min);
      assertThat(((Number) row.getProperty("c")).longValue()).as("count counts ROWS").isEqualTo(2);
    }
  }

  // ---- #9721 ----
  // Issue #9721: a LONG/INTEGER (and SHORT, BYTE, BOOLEAN, DATETIME) TimeSeries field written without a value read back as 0, so
  // {@code min}/{@code avg}/{@code count} counted a missing measurement as a measured 0. Such a column has no value of its own to
  // spend on absence, so the mutable row now carries a null bitmap and a sealed SIMPLE8B block a null bitmap of its own: a sample
  // without a value reads back as NULL and every aggregate skips it, exactly as on a document twin.
  //
  // @author Luca Garulli (l.garulli@arcadedata.com)

  private static final long T0 = 1_767_225_600_000L;

  // #9721
  @Test
  void theReportedScenario() {
    database.command("sql", "CREATE TIMESERIES TYPE T TIMESTAMP ts TAGS (host STRING) FIELDS (bg LONG, f LONG)");
    database.command("sql", "INSERT INTO T SET ts = 1767225600000, host = 'a', bg = null, f = 0");
    database.command("sql", "INSERT INTO T SET ts = 1767225601000, host = 'a', bg = 5, f = 1");
    for (int pass = 0; pass < 2; pass++) {
      assertThat(column("SELECT bg FROM T ORDER BY ts", "bg")).containsExactly(null, 5L);
      assertThat(column("SELECT f FROM T ORDER BY ts", "f")).containsExactly(0L, 1L);
      assertThat(row("SELECT min(bg) AS mn, max(bg) AS mx, count(bg) AS c, avg(bg) AS a, sum(bg) AS s, count(*) AS n FROM T")).isEqualTo(
          "[a=5.0(Double), c=1(Long), mn=5(Long), mx=5(Long), n=2(Long), s=5(Long)]");
      database.command("sql", "COMPACT TIMESERIES TYPE T");
    }
  }

  // #9721
  @Test
  void everyNullableTypeAnswersLikeADocumentTwin() {
    for (final int shards : new int[] { 1, 3 }) {
      database.command("sql", "CREATE TIMESERIES TYPE T TIMESTAMP ts TAGS (host STRING) FIELDS (l LONG, i INTEGER, s SHORT, b BYTE, "
          + "o BOOLEAN, d DOUBLE) SHARDS " + shards);
      database.command("sql", "CREATE DOCUMENT TYPE D");
      database.command("sql", "CREATE PROPERTY D.l LONG");
      database.command("sql", "CREATE PROPERTY D.i INTEGER");
      database.command("sql", "CREATE PROPERTY D.s SHORT");
      database.command("sql", "CREATE PROPERTY D.b BYTE");
      database.command("sql", "CREATE PROPERTY D.o BOOLEAN");
      database.command("sql", "CREATE PROPERTY D.d DOUBLE");
      load(0, 90);

      final String[] queries = {
          "SELECT min(l) AS a, max(l) AS b, count(l) AS c, avg(l) AS d, sum(l) AS e FROM X",
          "SELECT min(i) AS a, max(i) AS b, count(i) AS c, avg(i) AS d, sum(i) AS e FROM X",
          "SELECT min(s) AS a, max(s) AS b, count(s) AS c, sum(s) AS e FROM X",
          "SELECT min(b) AS a, max(b) AS b, count(b) AS c FROM X",
          "SELECT count(o) AS c FROM X",
          "SELECT min(l) AS a, max(i) AS b, count(l) AS c, count(*) AS n FROM X WHERE host = 'h1'",
          "SELECT min(l) AS a, avg(i) AS b, count(d) AS c FROM X WHERE l >= 20" };
      for (int pass = 0; pass < 3; pass++) {
        for (final String sql : queries)
          assertThat(row(sql.replace("FROM X", "FROM T"))).as(sql + " pass " + pass).isEqualTo(row(sql.replace("FROM X", "FROM D")));
        assertThat(rows("SELECT l, i, s, b, o FROM T ORDER BY ts")).isEqualTo(rows("SELECT l, i, s, b, o FROM D ORDER BY ts"));
        if (pass == 0)
          database.command("sql", "COMPACT TIMESERIES TYPE T"); // sealed blocks
        else if (pass == 1)
          load(90, 120); // sealed blocks plus a mutable tail
      }
      database.command("sql", "DROP TYPE T");
      database.command("sql", "DROP TYPE D");
    }
  }

  // #9721
  @Test
  void groupedAndBucketedAggregatesSkipTheMissingSamples() {
    database.command("sql", "CREATE TIMESERIES TYPE T TIMESTAMP ts TAGS (host STRING) FIELDS (l LONG, i INTEGER, s SHORT, b BYTE, "
        + "o BOOLEAN, d DOUBLE) SHARDS 2");
    database.command("sql", "CREATE DOCUMENT TYPE D");
    load(0, 90);
    final String grouped = "SELECT host, min(l) AS a, max(i) AS b, count(l) AS c, avg(l) AS d, sum(i) AS e FROM X GROUP BY host ORDER BY host";
    final String bucketed = "SELECT ts.timeBucket('20s', ts) AS t, min(l) AS a, max(l) AS b, avg(i) AS d FROM X GROUP BY t ORDER BY t";
    for (int pass = 0; pass < 2; pass++) {
      assertThat(rows(grouped.replace("FROM X", "FROM T"))).isEqualTo(rows(grouped.replace("FROM X", "FROM D")));
      assertThat(rows(bucketed.replace("FROM X", "FROM T"))).as(bucketed).isEqualTo(rows(bucketed.replace("FROM X", "FROM D")));
      database.command("sql", "COMPACT TIMESERIES TYPE T");
    }
  }

  // #9721
  @Test
  void aBatchColumnLeftUntouchedIsAMissingSampleOnBothLayers() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE T TIMESTAMP ts TAGS (host STRING) FIELDS (l LONG, i INTEGER, d DOUBLE)");
    final TimeSeriesEngine engine = ((LocalTimeSeriesType) database.getSchema().getType("T")).getEngine();
    final TimeSeriesBatch batch = engine.newBatch(4);
    for (int r = 0; r < 4; r++) {
      final int row = batch.addRow(T0 + r * 1_000L);
      batch.setString(row, 0, "h");
      if (r % 2 == 0)
        batch.setLong(row, 1, 10L + r);
      batch.setValue(row, 2, r == 1 ? null : r);
      batch.setDouble(row, 3, r);
    }
    database.transaction(() -> {
      try {
        engine.appendBatch(batch);
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    });
    for (int pass = 0; pass < 2; pass++) {
      assertThat(column("SELECT l FROM T ORDER BY ts", "l")).containsExactly(10L, null, 12L, null);
      assertThat(column("SELECT i FROM T ORDER BY ts", "i")).containsExactly(0, null, 2, 3);
      assertThat(row("SELECT min(l) AS a, count(l) AS c, min(i) AS b FROM T")).isEqualTo("[a=10(Long), b=0(Integer), c=2(Long)]");
      database.command("sql", "COMPACT TIMESERIES TYPE T");
    }

    // A refilled batch starts every row over: a value set on the previous fill does not leak into an untouched column
    batch.clear();
    final int row = batch.addRow(T0 + 10_000L);
    batch.setString(row, 0, "h");
    assertThat(batch.isNull(row, 1)).isTrue();
    assertThat(batch.isNull(row, 2)).isTrue();
  }

  // #9721
  @Test
  void theEngineApiAndTheIntegrityCheckSeeTheMissingSamples() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE T TIMESTAMP ts TAGS (host STRING) FIELDS (l LONG)");
    database.command("sql", "INSERT INTO T SET ts = ?, host = 'a', l = null", T0);
    database.command("sql", "INSERT INTO T SET ts = ?, host = 'a', l = 7", T0 + 1_000L);
    final TimeSeriesEngine engine = ((LocalTimeSeriesType) database.getSchema().getType("T")).getEngine();
    for (int pass = 0; pass < 2; pass++) {
      final List<Object> values = new ArrayList<>();
      final Iterator<Object[]> it = engine.iterateQuery(Long.MIN_VALUE, Long.MAX_VALUE, null, null);
      while (it.hasNext())
        values.add(it.next()[2]);
      assertThat(values).containsExactly(null, 7L);

      final MultiColumnAggregationResult result = engine.aggregateMulti(Long.MIN_VALUE, Long.MAX_VALUE,
          List.of(new MultiColumnAggregationRequest(2, AggregationType.MIN, "mn"), MultiColumnAggregationRequest.count("c")), 0, null);
      final long bucket = result.getBucketTimestamps().getFirst();
      assertThat(result.getValue(bucket, 0)).isEqualTo(7.0);
      assertThat(result.getCount(bucket, 0)).isEqualTo(1L);
      assertThat(result.getValue(bucket, 1)).isEqualTo(2.0);
      database.command("sql", "COMPACT TIMESERIES TYPE T");
    }
    // the DEEP check decodes every block and reconciles it with the exact statistics its header declares
    try (final ResultSet check = database.command("sql", "CHECK DATABASE DEEP")) {
      final Result result = check.next();
      assertThat(result.<Collection<?>>getProperty("corruptedTimeSeries")).isEmpty();
      assertThat(result.<Collection<?>>getProperty("warnings")).isEmpty();
    }
  }

  // #9721
  @Test
  void theMissingSamplesSurviveEveryRouteToASealedBlock() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE T TIMESTAMP ts TAGS (host STRING) FIELDS (l LONG, i INTEGER, s SHORT, b BYTE, "
        + "o BOOLEAN, d DOUBLE) SHARDS 1");
    database.command("sql", "CREATE DOCUMENT TYPE D");
    final TimeSeriesEngine engine = ((LocalTimeSeriesType) database.getSchema().getType("T")).getEngine();
    // several small compactions, each sealing a block with missing samples, then a merge of those blocks into one, then a
    // downsampling pass: every route that rewrites a sealed block has to carry the null bitmap through
    for (int round = 0; round < 6; round++) {
      load(round * 15, round * 15 + 15);
      engine.compactAll();
    }
    assertThat(engine.getShard(0).getSealedStore().getBlockCount()).isGreaterThan(1);
    engine.mergeSmallBlocks();
    assertThat(engine.getShard(0).getSealedStore().getBlockCount()).isEqualTo(1);

    final String[] queries = { "SELECT min(l) AS a, max(l) AS b, count(l) AS c, avg(l) AS d, sum(l) AS e FROM X",
        "SELECT min(i) AS a, count(i) AS c, avg(i) AS d FROM X", "SELECT count(s) AS c, count(b) AS c2, count(o) AS c3 FROM X",
        "SELECT host, min(l) AS a, count(l) AS c FROM X GROUP BY host ORDER BY host" };
    for (final String sql : queries)
      assertThat(rows(sql.replace("FROM X", "FROM T"))).as(sql).isEqualTo(rows(sql.replace("FROM X", "FROM D")));
    assertThat(rows("SELECT l, i, s, b, o FROM T ORDER BY ts")).isEqualTo(rows("SELECT l, i, s, b, o FROM D ORDER BY ts"));
    try (final ResultSet check = database.command("sql", "CHECK DATABASE DEEP")) {
      assertThat(check.next().<Collection<?>>getProperty("corruptedTimeSeries")).isEmpty();
    }
  }

  // #9721
  @Test
  void aNullBitmapSurvivesTheSimple8bRoundTrip() throws Exception {
    final long[] values = new long[200];
    final long[] nulls = new long[Simple8bCodec.bitmapWords(values.length)];
    for (int i = 0; i < values.length; i++) {
      if (i % 7 == 3)
        nulls[i >>> 6] |= 1L << i;
      else
        values[i] = i % 5 == 0 ? Long.MAX_VALUE - i : i;
    }
    final byte[] encoded = Simple8bCodec.encode(values, nulls);
    assertThat(Simple8bCodec.decode(encoded)).containsExactly(values);
    assertThat(Simple8bCodec.nulls(encoded)).containsExactly(nulls);
    // without an absent sample the layout is the one every older block has
    assertThat(Simple8bCodec.encode(values, new long[nulls.length])).containsExactly(Simple8bCodec.encode(values));
    assertThat(Simple8bCodec.nulls(Simple8bCodec.encode(values))).isNull();
  }

  // #9721
  @Test
  void theNullBitmapRoundTripsAtItsWordBoundaries() throws Exception {
    for (final int count : new int[] { 1, 63, 64, 65, 128, 129 })
      for (final boolean wide : new boolean[] { false, true }) {
        final long[] values = new long[count];
        final long[] nulls = new long[Simple8bCodec.bitmapWords(count)];
        for (int i = 0; i < count; i++)
          if (i == count - 1 || i % 3 == 0)
            nulls[i >>> 6] |= 1L << i; // the last sample is absent, so the bit at the boundary is the one tested
          else
            values[i] = wide ? Long.MIN_VALUE + i : i; // a wide value forces the raw layout under the bitmap
        final byte[] encoded = Simple8bCodec.encode(values, nulls);
        assertThat(Simple8bCodec.decode(encoded)).as(count + (wide ? " wide" : "")).containsExactly(values);
        assertThat(Simple8bCodec.nulls(encoded)).as(count + (wide ? " wide" : "")).containsExactly(nulls);
      }
  }

  // #9721
  @Test
  void bitsPastTheLastSampleAreNotEncoded() throws Exception {
    final long[] values = { 1, 2, 3 };
    final byte[] tidy = Simple8bCodec.encode(values, new long[] { 0b010L });
    assertThat(Simple8bCodec.encode(values, new long[] { 0b010L | (1L << 40) })).containsExactly(tidy);
    assertThat(Simple8bCodec.nulls(tidy)).containsExactly(0b010L);
    // only a bit past the last sample: no sample is absent, so the block is the one without a bitmap
    final byte[] none = Simple8bCodec.encode(values, new long[] { 1L << 40 });
    assertThat(none).containsExactly(Simple8bCodec.encode(values));
    assertThat(Simple8bCodec.nulls(none)).isNull();
  }

  // #9721
  @Test
  void aCorruptNullableHeaderIsRefusedBeforeAnythingIsAllocated() {
    // the nullable marker followed by a count whose bitmap the buffer cannot hold: refused, not a 256 MB array or a wrapped size
    for (final int count : new int[] { Integer.MAX_VALUE, Integer.MAX_VALUE - 10, 1_000_000 }) {
      final byte[] corrupt = ByteBuffer.allocate(16).putInt(Integer.MIN_VALUE).putInt(count).array();
      assertThatThrownBy(() -> Simple8bCodec.nulls(corrupt)).as("count " + count).isInstanceOf(IOException.class);
      assertThatThrownBy(() -> Simple8bCodec.decode(corrupt)).as("count " + count).isInstanceOf(IOException.class);
    }
    assertThat(Simple8bCodec.bitmapWords(Integer.MAX_VALUE)).isEqualTo(33_554_432);
  }

  // #9721
  @Test
  void aTypeCreatedBeforeTheNullBitmapKeepsItsRowLayout() throws Exception {
    // Version 1 rows have no bitmap, so their stride - and the rows already on their pages - must stay what they were
    final List<ColumnDefinition> columns = List.of(new ColumnDefinition("ts", Type.DATETIME, ColumnDefinition.ColumnRole.TIMESTAMP),
        new ColumnDefinition("host", Type.STRING, ColumnDefinition.ColumnRole.TAG),
        new ColumnDefinition("l", Type.LONG, ColumnDefinition.ColumnRole.FIELD));
    final TimeSeriesEngine v1 = new TimeSeriesEngine((DatabaseInternal) database, "LegacyRows", columns, 1, 0,
        TimeSeriesBucket.VERSION_DICTIONARY_TAGS);
    final TimeSeriesEngine v2 = new TimeSeriesEngine((DatabaseInternal) database, "BitmapRows", columns, 1, 0,
        TimeSeriesBucket.VERSION_NULL_BITMAP);
    try {
      assertThat(v1.getShard(0).getMutableBucket().getRowSize()).isEqualTo(8 + 4 + 8);
      assertThat(v2.getShard(0).getMutableBucket().getRowSize()).isEqualTo(8 + 1 + 4 + 8);
      database.transaction(() -> {
        try {
          v1.appendSamples(new long[] { T0 }, new Object[] { "a" }, new Object[] { null });
          v2.appendSamples(new long[] { T0 }, new Object[] { "a" }, new Object[] { null });
        } catch (final Exception e) {
          throw new RuntimeException(e);
        }
      });
      assertThat(v1.iterateQuery(Long.MIN_VALUE, Long.MAX_VALUE, null, null).next()[2]).isEqualTo(0L);
      assertThat(v2.iterateQuery(Long.MIN_VALUE, Long.MAX_VALUE, null, null).next()[2]).isNull();
    } finally {
      v1.close();
      v2.close();
    }
  }

  private void load(final int from, final int to) {
    database.transaction(() -> {
      for (int i = from; i < to; i++)
        for (final String type : new String[] { "T", "D" })
          database.command("sql", "INSERT INTO " + type + " SET ts = ?, host = ?, l = ?, i = ?, s = ?, b = ?, o = ?, d = ?", T0 + i * 1_000L,
              "h" + (i % 3), i % 4 == 0 ? null : (long) i * 3, i % 5 == 1 ? null : i - 40, i % 6 == 2 ? null : (short) (i * 2),
              i % 3 == 0 ? null : (byte) (i - 60), i % 7 == 0 ? null : i % 2 == 0, i % 8 == 0 ? null : (double) i);
    });
  }

  /** The only row of an ungrouped answer, each value spelled with its Java class. */
  private String row(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      final String row = describe(rs.next());
      assertThat(rs.hasNext()).isFalse();
      return row;
    }
  }

  private List<String> rows(final String sql) {
    final List<String> out = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", sql)) {
      while (rs.hasNext())
        out.add(describe(rs.next()));
    }
    return out;
  }

  private List<Object> column(final String sql, final String name) {
    final List<Object> out = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", sql)) {
      while (rs.hasNext())
        out.add(rs.next().getProperty(name));
    }
    return out;
  }

  private static String describe(final Result r) {
    final List<String> out = new ArrayList<>();
    for (final String name : r.getPropertyNames().stream().sorted().toList()) {
      final Object value = r.getProperty(name);
      out.add(name + "=" + value + (value == null ? "" : "(" + value.getClass().getSimpleName() + ")"));
    }
    return out.toString();
  }
}
