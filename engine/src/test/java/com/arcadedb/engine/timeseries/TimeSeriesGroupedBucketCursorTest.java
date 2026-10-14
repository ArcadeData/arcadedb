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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigInteger;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * A grouped aggregation over sealed data keeps the bucket of the previous row, as a window {@code [windowStart, windowEnd)}
 * in {@link TimeSeriesSealedStore} and as a cursor in {@link MultiColumnAggregationResult}, so that the next row of the
 * same bucket skips the division that finds it. That changes the speed and must never change an answer: every test here
 * computes {@code GROUP BY host, ts.timeBucket('1h', ts)} by brute force and compares it with what the engine answers
 * from compacted data, for the two sealed layouts (blocks that straddle several buckets, and blocks aligned to the hour).
 */
class TimeSeriesGroupedBucketCursorTest extends TestHelper {
  private static final long   HOUR           = 3_600_000L;
  private static final long   MINUTE         = 60_000L;
  private static final String STRADDLING     = "";
  private static final String HOUR_ALIGNED   = "COMPACTION_INTERVAL 1 HOURS";
  private static final String GROUPED_SQL    =
      "SELECT host, ts.timeBucket('1h', ts) AS b, count(*) AS c, sum(v) AS s, avg(v) AS a, min(v) AS mn, max(v) AS mx FROM S GROUP BY host, b";
  private static final String OFFSET_SQL     =
      "SELECT host, ts.timeBucket('1h', ts, {'offset': '15m'}) AS b, count(*) AS c, sum(v) AS s, avg(v) AS a, min(v) AS mn, max(v) AS mx FROM S GROUP BY host, b";
  private static final String OFFSET_50M_SQL =
      "SELECT host, ts.timeBucket('1h', ts, {'offset': '50m'}) AS b, count(*) AS c, sum(v) AS s, avg(v) AS a, min(v) AS mn, max(v) AS mx FROM S GROUP BY host, b";
  private static final String UNGROUPED_SQL  =
      "SELECT ts.timeBucket('1h', ts) AS b, count(*) AS c, sum(v) AS s, avg(v) AS a, min(v) AS mn, max(v) AS mx FROM S GROUP BY b";

  /** One sample: the series it belongs to, its timestamp, and its value. */
  private record Sample(String host, long ts, double v) {
  }

  /** The aggregates of one (host, bucket) group. */
  private record Group(long count, double sum, double avg, double min, double max) {
  }

  private void createType(final String layout) {
    database.command("sql", "CREATE TIMESERIES TYPE S TIMESTAMP ts TAGS (host STRING) FIELDS (v DOUBLE) SHARDS 1 " + layout);
  }

  private void insert(final List<Sample> samples) {
    database.transaction(() -> {
      for (final Sample s : samples)
        database.command("sql", "INSERT INTO S SET ts = ?, host = ?, v = ?", s.ts(), s.host(), s.v());
    });
  }

  private void compact() {
    database.command("sql", "COMPACT TIMESERIES TYPE S");
  }

  /** Computed exactly: {@code ts - offset} wraps for a timestamp within the offset of Long.MIN_VALUE. */
  private static long bucketOf(final long ts, final long offset) {
    final BigInteger t = BigInteger.valueOf(ts);
    return t.subtract(t.subtract(BigInteger.valueOf(offset)).mod(BigInteger.valueOf(HOUR))).longValueExact();
  }

  /** The brute-force answer, keyed by host then bucket start; {@code byHost} false keys every group under the empty host. */
  private static Map<String, Map<Long, Group>> expected(final List<Sample> samples, final long offset, final boolean byHost) {
    final Map<String, Map<Long, List<Double>>> values = new TreeMap<>();
    for (final Sample s : samples)
      values.computeIfAbsent(byHost ? s.host() : "", h -> new TreeMap<>()).computeIfAbsent(bucketOf(s.ts(), offset), b -> new ArrayList<>())
          .add(s.v());
    final Map<String, Map<Long, Group>> out = new TreeMap<>();
    values.forEach((host, buckets) -> buckets.forEach((bucket, list) -> {
      double sum = 0;
      double min = Double.POSITIVE_INFINITY;
      double max = Double.NEGATIVE_INFINITY;
      for (final double d : list) {
        sum += d;
        min = Math.min(min, d);
        max = Math.max(max, d);
      }
      out.computeIfAbsent(host, h -> new TreeMap<>()).put(bucket, new Group(list.size(), sum, sum / list.size(), min, max));
    }));
    return out;
  }

  private Map<String, Map<Long, Group>> engineAnswer(final String sql) {
    final Map<String, Map<Long, Group>> out = new TreeMap<>();
    try (final ResultSet rs = database.query("sql", sql)) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        final String host = r.hasProperty("host") ? r.getProperty("host") : "";
        final long bucket = ((LocalDateTime) r.getProperty("b")).toInstant(ZoneOffset.UTC).toEpochMilli();
        final Group g = new Group(((Number) r.getProperty("c")).longValue(), ((Number) r.getProperty("s")).doubleValue(),
            ((Number) r.getProperty("a")).doubleValue(), ((Number) r.getProperty("mn")).doubleValue(),
            ((Number) r.getProperty("mx")).doubleValue());
        assertThat(out.computeIfAbsent(host, h -> new TreeMap<>()).put(bucket, g)).as("one row per group: " + host + " " + bucket)
            .isNull();
      }
    }
    return out;
  }

  private void assertSameGroups(final Map<String, Map<Long, Group>> actual, final Map<String, Map<Long, Group>> expected) {
    assertThat(actual.keySet()).isEqualTo(expected.keySet());
    expected.forEach((host, buckets) -> {
      assertThat(actual.get(host).keySet()).as("buckets of " + host).isEqualTo(buckets.keySet());
      buckets.forEach((bucket, e) -> {
        final Group a = actual.get(host).get(bucket);
        final String where = host + " @ " + bucket;
        assertThat(a.count()).as("count " + where).isEqualTo(e.count());
        assertThat(a.sum()).as("sum " + where).isCloseTo(e.sum(), within(1e-9));
        assertThat(a.avg()).as("avg " + where).isCloseTo(e.avg(), within(1e-9));
        assertThat(a.min()).as("min " + where).isEqualTo(e.min());
        assertThat(a.max()).as("max " + where).isEqualTo(e.max());
      });
    });
  }

  private void assertEngineMatches(final List<Sample> samples) {
    // the aggregation under test is the one pushed into the engine, not the generic one
    try (final ResultSet rs = database.query("sql", GROUPED_SQL)) {
      assertThat(rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2)).contains("bucket=3600000ms");
    }
    assertSameGroups(engineAnswer(GROUPED_SQL), expected(samples, 0L, true));
    assertSameGroups(engineAnswer(UNGROUPED_SQL), expected(samples, 0L, false));
    assertSameGroups(engineAnswer(OFFSET_SQL), expected(samples, 15 * MINUTE, true));
  }

  /** Three hosts, ten hours, a sample every seven minutes, hosts interleaved, values that make every group's min and max distinct. */
  @ParameterizedTest
  @ValueSource(strings = { STRADDLING, HOUR_ALIGNED })
  void severalTagsAndSeveralBucketsInTimeOrder(final String layout) {
    createType(layout);
    final List<Sample> samples = new ArrayList<>();
    final String[] hosts = { "a", "b", "c" };
    for (int i = 0; i < 10 * 60 / 7; i++)
      samples.add(new Sample(hosts[i % 3], 1_700_000_000_000L + i * 7 * MINUTE, (i * 37 % 101) * 0.5 - 20));
    insert(samples);
    compact();
    assertEngineMatches(samples);
  }

  /** Timestamps on a boundary, one millisecond before it, and one after, for the plain grid and the shifted one. */
  @ParameterizedTest
  @ValueSource(strings = { STRADDLING, HOUR_ALIGNED })
  void samplesCrossBucketBoundariesExactly(final String layout) {
    createType(layout);
    final long base = 1_700_000_000_000L - 1_700_000_000_000L % HOUR;
    final List<Sample> samples = new ArrayList<>();
    double v = 1;
    for (int h = 1; h <= 5; h++) {
      for (final long edge : new long[] { base + h * HOUR, base + h * HOUR + 15 * MINUTE }) {
        for (final long delta : new long[] { -1, 0, 1 })
          samples.add(new Sample(delta == 0 ? "a" : "b", edge + delta, v++));
      }
    }
    samples.sort((x, y) -> Long.compare(x.ts(), y.ts()));
    insert(samples);
    compact();
    assertEngineMatches(samples);
  }

  /** A series that jumps over empty buckets: no group may appear for them, and a revisited bucket value must not leak. */
  @ParameterizedTest
  @ValueSource(strings = { STRADDLING, HOUR_ALIGNED })
  void aSeriesThatJumpsOverEmptyBuckets(final String layout) {
    createType(layout);
    final long base = 1_700_000_000_000L - 1_700_000_000_000L % HOUR;
    final List<Sample> samples = new ArrayList<>();
    final long[] hours = { 0, 0, 1, 7, 7, 8, 30, 30, 31, 200, 200, 200, 1000 };
    for (int i = 0; i < hours.length; i++) {
      samples.add(new Sample("a", base + hours[i] * HOUR + i * MINUTE, i + 1));
      samples.add(new Sample("b", base + hours[i] * HOUR + i * MINUTE + 1, 100 - i));
    }
    insert(samples);
    compact();
    assertEngineMatches(samples);
    assertThat(engineAnswer(GROUPED_SQL).get("a")).hasSize(8);
  }

  /** Samples inserted out of order, in two batches with a compaction after each, so that blocks overlap in time. */
  @ParameterizedTest
  @ValueSource(strings = { STRADDLING, HOUR_ALIGNED })
  void samplesInsertedOutOfOrderAcrossTwoCompactions(final String layout) {
    createType(layout);
    final long base = 1_700_000_000_000L - 1_700_000_000_000L % HOUR;
    final List<Sample> late = new ArrayList<>();
    final List<Sample> early = new ArrayList<>();
    for (int i = 0; i < 120; i++) {
      late.add(new Sample(i % 2 == 0 ? "a" : "b", base + 6 * HOUR + i * 5 * MINUTE, i * 0.25));
      early.add(new Sample(i % 2 == 0 ? "b" : "a", base + 4 * HOUR + i * 5 * MINUTE, 50 - i * 0.5));
    }
    insert(late);
    compact();
    insert(early);
    compact();
    final List<Sample> all = new ArrayList<>(late);
    all.addAll(early);
    assertEngineMatches(all);
  }

  /**
   * The last bucket of the timeline ends past Long.MAX_VALUE, so the end of its window overflows: the rows of it must still
   * land in the right bucket, one by one, and the rows before it must not be disturbed.
   */
  @ParameterizedTest
  @ValueSource(strings = { STRADDLING, HOUR_ALIGNED })
  void aTimestampNearLongMaxValue(final String layout) {
    createType(layout);
    final long base = 1_700_000_000_000L - 1_700_000_000_000L % HOUR;
    final List<Sample> samples = new ArrayList<>();
    samples.add(new Sample("a", base, 1));
    samples.add(new Sample("a", base + 1, 2));
    samples.add(new Sample("a", Long.MAX_VALUE - 5, 3));
    samples.add(new Sample("a", Long.MAX_VALUE - 3, 4));
    samples.add(new Sample("a", Long.MAX_VALUE - 1, 5));
    samples.add(new Sample("a", Long.MAX_VALUE, 6));
    insert(samples);
    compact();
    assertSameGroups(engineAnswer(GROUPED_SQL), expected(samples, 0L, true));
  }

  /**
   * The first bucket of the timeline starts within its offset of Long.MIN_VALUE (code review on PR #10004): {@code ts - offset}
   * wraps for the rows at its start, and the bucket arithmetic must not let that split one bucket in two, neither when the row
   * computes its bucket nor when the next row reuses the window of the previous one. On the 1h grid shifted by 50m, Long.MIN_VALUE
   * is 2,824,192 ms past an hour boundary, so the first whole bucket starts 175,808 ms after it and the subtraction wraps for its
   * first 2,824,192 ms.
   */
  @ParameterizedTest
  @ValueSource(strings = { STRADDLING, HOUR_ALIGNED })
  void aTimestampNearLongMinValueWithAnOffset(final String layout) {
    createType(layout);
    final long offset = 50 * MINUTE;
    final long first = Long.MIN_VALUE + 175_808L;
    assertThat(bucketOf(first, offset)).isEqualTo(first);
    final List<Sample> samples = new ArrayList<>();
    final long[] deltas = { 0, 1, 824_191, 2_824_191, 2_824_192, 2_824_193, HOUR - 1, HOUR, HOUR + 5, 3 * HOUR + 7 };
    for (int i = 0; i < deltas.length; i++)
      samples.add(new Sample(i % 3 == 2 ? "b" : "a", first + deltas[i], i + 1));
    insert(samples);
    compact();
    assertSameGroups(engineAnswer(OFFSET_50M_SQL), expected(samples, offset, true));
    assertThat(engineAnswer(OFFSET_50M_SQL).get("a")).hasSize(3);
  }
}
