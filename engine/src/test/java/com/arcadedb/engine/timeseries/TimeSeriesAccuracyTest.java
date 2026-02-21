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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Deterministic accuracy test for the full TimeSeries pipeline:
 * mutable insert → compaction → sealed blocks → query → aggregation.
 * <p>
 * Uses 200,000 samples with pre-computable values (value = i + 1) so every
 * COUNT, SUM, MIN, MAX, AVG can be verified exactly against closed-form formulas.
 * Exercises multi-page mutable storage, multi-block sealed storage, compression
 * codecs (DeltaOfDelta, Dictionary, GorillaXOR), and bucket-aligned compaction.
 */
class TimeSeriesAccuracyTest {

  private static final String DB_PATH = "target/databases/TimeSeriesAccuracyTest";
  private static final int TOTAL_SAMPLES = 200_000;
  private static final long INTERVAL_MS = 54L; // 3h / 200K ≈ 54ms
  private static final long HOUR_MS = 3_600_000L;

  // Per-bucket sample index ranges (value[i] = i + 1, timestamp[i] = i * 54)
  // Bucket 0: [0, 3_600_000) → i in [0, 66666] → 66667 samples
  // Bucket 1: [3_600_000, 7_200_000) → i in [66667, 133333] → 66667 samples
  // Bucket 2: [7_200_000, ...) → i in [133334, 199999] → 66666 samples
  private static final int BUCKET_0_START = 0;
  private static final int BUCKET_0_END = 66_666;
  private static final int BUCKET_1_START = 66_667;
  private static final int BUCKET_1_END = 133_333;
  private static final int BUCKET_2_START = 133_334;
  private static final int BUCKET_2_END = 199_999;

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
  void testTotalCountMatchesInserted() throws Exception {
    final TimeSeriesEngine engine = createAndPopulate();

    // Verify via direct API
    assertThat(engine.countSamples()).isEqualTo(TOTAL_SAMPLES);

    // Verify via SQL
    final ResultSet rs = database.query("sql", "SELECT count(*) AS cnt FROM Sensor");
    assertThat(rs.hasNext()).isTrue();
    final Result row = rs.next();
    assertThat(((Number) row.getProperty("cnt")).longValue()).isEqualTo(TOTAL_SAMPLES);
    rs.close();
  }

  @Test
  void testFullRangeScanReturnsAllSamples() throws Exception {
    final TimeSeriesEngine engine = createAndPopulate();

    final List<Object[]> rows = engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, null);

    assertThat(rows).hasSize(TOTAL_SAMPLES);

    // First sample: ts=0, sensor="s1", value=1.0
    assertThat((long) rows.get(0)[0]).isEqualTo(0L);
    assertThat(rows.get(0)[1]).isEqualTo("s1");
    assertThat((double) rows.get(0)[2]).isEqualTo(1.0);

    // Last sample: ts=199999*54, sensor="s1", value=200000.0
    final Object[] last = rows.get(TOTAL_SAMPLES - 1);
    assertThat((long) last[0]).isEqualTo((long) (TOTAL_SAMPLES - 1) * INTERVAL_MS);
    assertThat(last[1]).isEqualTo("s1");
    assertThat((double) last[2]).isEqualTo((double) TOTAL_SAMPLES);
  }

  @Test
  void testPerBucketAggregationExact() throws Exception {
    createAndPopulate();

    final ResultSet rs = database.query("sql",
        "SELECT ts.timeBucket('1h', ts) AS hour, count(*) AS cnt, sum(value) AS sum_val, " +
            "min(value) AS min_val, max(value) AS max_val, avg(value) AS avg_val " +
            "FROM Sensor GROUP BY hour ORDER BY hour");

    final List<Result> results = collectResults(rs);
    assertThat(results).hasSize(3);

    // Sort by hour to ensure deterministic order
    results.sort((a, b) -> ((Date) a.getProperty("hour")).compareTo((Date) b.getProperty("hour")));

    // Bucket 0
    assertBucketAggregates(results.get(0), BUCKET_0_START, BUCKET_0_END);

    // Bucket 1
    assertBucketAggregates(results.get(1), BUCKET_1_START, BUCKET_1_END);

    // Bucket 2
    assertBucketAggregates(results.get(2), BUCKET_2_START, BUCKET_2_END);
  }

  @Test
  void testGlobalAggregationExact() throws Exception {
    createAndPopulate();

    // Global: N=200000, values 1..200000
    // SUM = 200000 * 200001 / 2 = 20,000,100,000
    // MIN = 1.0, MAX = 200000.0, AVG = 100000.5, COUNT = 200000
    final ResultSet rs = database.query("sql",
        "SELECT count(*) AS cnt, sum(value) AS sum_val, min(value) AS min_val, " +
            "max(value) AS max_val, avg(value) AS avg_val FROM Sensor");

    assertThat(rs.hasNext()).isTrue();
    final Result row = rs.next();
    rs.close();

    assertThat(((Number) row.getProperty("cnt")).longValue()).isEqualTo(TOTAL_SAMPLES);
    assertThat(((Number) row.getProperty("sum_val")).doubleValue()).isCloseTo(20_000_100_000.0, within(1.0));
    assertThat(((Number) row.getProperty("min_val")).doubleValue()).isEqualTo(1.0);
    assertThat(((Number) row.getProperty("max_val")).doubleValue()).isEqualTo(200_000.0);
    assertThat(((Number) row.getProperty("avg_val")).doubleValue()).isCloseTo(100_000.5, within(0.01));
  }

  @Test
  void testDirectApiAggregationMatchesSQL() throws Exception {
    final TimeSeriesEngine engine = createAndPopulate();

    // Direct API aggregation (column index 2 = value, -1 = count)
    final MultiColumnAggregationResult apiResult = engine.aggregateMulti(
        Long.MIN_VALUE, Long.MAX_VALUE,
        List.of(
            new MultiColumnAggregationRequest(2, AggregationType.SUM, "sum_val"),
            new MultiColumnAggregationRequest(2, AggregationType.MIN, "min_val"),
            new MultiColumnAggregationRequest(2, AggregationType.MAX, "max_val"),
            new MultiColumnAggregationRequest(2, AggregationType.AVG, "avg_val"),
            new MultiColumnAggregationRequest(-1, AggregationType.COUNT, "cnt")
        ),
        HOUR_MS, null);

    // SQL aggregation per bucket
    final ResultSet rs = database.query("sql",
        "SELECT ts.timeBucket('1h', ts) AS hour, sum(value) AS sum_val, min(value) AS min_val, " +
            "max(value) AS max_val, avg(value) AS avg_val, count(*) AS cnt " +
            "FROM Sensor GROUP BY hour ORDER BY hour");
    final List<Result> sqlResults = collectResults(rs);
    sqlResults.sort((a, b) -> ((Date) a.getProperty("hour")).compareTo((Date) b.getProperty("hour")));

    // Compare API vs SQL for each bucket
    final List<Long> bucketTimestamps = apiResult.getBucketTimestamps();
    assertThat(bucketTimestamps).hasSize(sqlResults.size());

    for (int i = 0; i < bucketTimestamps.size(); i++) {
      final long bucketTs = bucketTimestamps.get(i);
      final Result sqlRow = sqlResults.get(i);

      assertThat(apiResult.getValue(bucketTs, 0))
          .as("SUM bucket %d", i)
          .isCloseTo(((Number) sqlRow.getProperty("sum_val")).doubleValue(), within(1.0));
      assertThat(apiResult.getValue(bucketTs, 1))
          .as("MIN bucket %d", i)
          .isCloseTo(((Number) sqlRow.getProperty("min_val")).doubleValue(), within(0.01));
      assertThat(apiResult.getValue(bucketTs, 2))
          .as("MAX bucket %d", i)
          .isCloseTo(((Number) sqlRow.getProperty("max_val")).doubleValue(), within(0.01));
      assertThat(apiResult.getValue(bucketTs, 3))
          .as("AVG bucket %d", i)
          .isCloseTo(((Number) sqlRow.getProperty("avg_val")).doubleValue(), within(0.01));
      assertThat((long) apiResult.getValue(bucketTs, 4))
          .as("COUNT bucket %d", i)
          .isEqualTo(((Number) sqlRow.getProperty("cnt")).longValue());
    }
  }

  @Test
  void testRangeQueryAccuracy() throws Exception {
    final TimeSeriesEngine engine = createAndPopulate();

    // Query hour 1 only: timestamps [3_600_000, 7_200_000)
    // Samples: i in [66667, 133333], values = 66668..133334
    final long fromTs = BUCKET_1_START * INTERVAL_MS;
    final long toTs = BUCKET_1_END * INTERVAL_MS;

    final List<Object[]> rows = engine.query(fromTs, toTs, null, null);

    final int expectedCount = BUCKET_1_END - BUCKET_1_START + 1;
    assertThat(rows).hasSize(expectedCount);

    // Verify SUM via direct API on the same range
    final MultiColumnAggregationResult result = engine.aggregateMulti(
        fromTs, toTs,
        List.of(
            new MultiColumnAggregationRequest(2, AggregationType.SUM, "sum_val"),
            new MultiColumnAggregationRequest(-1, AggregationType.COUNT, "cnt")
        ),
        0L, null);

    final long bucketTs = result.getBucketTimestamps().get(0);
    final double expectedSum = rangeSum(BUCKET_1_START, BUCKET_1_END);

    assertThat((long) result.getValue(bucketTs, 1)).isEqualTo(expectedCount);
    assertThat(result.getValue(bucketTs, 0)).isCloseTo(expectedSum, within(1.0));
  }

  // ---- helpers ----

  private TimeSeriesEngine createAndPopulate() throws Exception {
    database.command("sql",
        "CREATE TIMESERIES TYPE Sensor TIMESTAMP ts TAGS (sensor STRING) FIELDS (value DOUBLE) " +
            "SHARDS 1 COMPACTION_INTERVAL 1 HOURS");

    final TimeSeriesEngine engine = ((LocalTimeSeriesType) database.getSchema().getType("Sensor")).getEngine();

    final long[] timestamps = new long[TOTAL_SAMPLES];
    final Object[] sensors = new Object[TOTAL_SAMPLES];
    final Object[] values = new Object[TOTAL_SAMPLES];
    for (int i = 0; i < TOTAL_SAMPLES; i++) {
      timestamps[i] = i * INTERVAL_MS;
      sensors[i] = "s1";
      values[i] = (double) (i + 1);
    }

    database.begin();
    engine.appendSamples(timestamps, sensors, values);
    database.commit();

    engine.compactAll();

    return engine;
  }

  /** Sum of values for sample indices [start, end] where value[i] = i + 1. */
  private static double rangeSum(final int start, final int end) {
    // Sum of (start+1) + (start+2) + ... + (end+1)
    // = sum(1..end+1) - sum(1..start)
    // = (end+1)*(end+2)/2 - start*(start+1)/2
    return (long) (end + 1) * (end + 2) / 2.0 - (long) start * (start + 1) / 2.0;
  }

  private void assertBucketAggregates(final Result row, final int start, final int end) {
    final int count = end - start + 1;
    final double sum = rangeSum(start, end);
    final double min = start + 1.0;
    final double max = end + 1.0;
    final double avg = sum / count;

    assertThat(((Number) row.getProperty("cnt")).longValue()).isEqualTo(count);
    assertThat(((Number) row.getProperty("sum_val")).doubleValue()).isCloseTo(sum, within(1.0));
    assertThat(((Number) row.getProperty("min_val")).doubleValue()).isEqualTo(min);
    assertThat(((Number) row.getProperty("max_val")).doubleValue()).isEqualTo(max);
    assertThat(((Number) row.getProperty("avg_val")).doubleValue()).isCloseTo(avg, within(0.01));
  }

  private List<Result> collectResults(final ResultSet rs) {
    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());
    rs.close();
    return results;
  }
}
