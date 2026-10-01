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
import com.arcadedb.function.sql.time.SQLFunctionTimeBucket;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.parser.Statement;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.LocalTimeSeriesType;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * #8798: {@code ts.timeBucket} and the native aggregation floored timestamps to multiples of the interval counted from
 * the Unix epoch, a Thursday: '1w' buckets always started on Thursday 00:00 UTC and '1d' buckets at 08:00 in UTC+8,
 * with no way to move the grid. An origin / offset / fixed-offset timezone now moves it, and EVERY path that buckets
 * must agree on it, or one query would put the same sample in two buckets depending on how it was planned.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8798TimeBucketOriginTest extends TestHelper {
  private static final long HOUR = 3_600_000L;
  private static final long DAY  = 24 * HOUR;
  private static final long WEEK = 7 * DAY;

  private final SQLFunctionTimeBucket fn = new SQLFunctionTimeBucket();

  private static long ms(final String iso) {
    return Instant.parse(iso).toEpochMilli();
  }

  private static long bucketMs(final Object result) {
    return ((LocalDateTime) result).toInstant(ZoneOffset.UTC).toEpochMilli();
  }

  private long bucket(final String interval, final long ts, final Object options) {
    return bucketMs(fn.execute(null, null, null, new Object[] { interval, ts, options }, null));
  }

  @Test
  void theDefaultGridIsUnchanged() {
    // Sun 2023-12-31 09:00 UTC lands in the bucket starting Thu 2023-12-28, the behaviour the issue measured
    assertThat(bucketMs(fn.execute(null, null, null, new Object[] { "1w", ms("2023-12-31T09:00:00Z") }, null)))
        .isEqualTo(ms("2023-12-28T00:00:00Z"));
    assertThat(bucket("1w", ms("2023-12-31T09:00:00Z"), null)).isEqualTo(ms("2023-12-28T00:00:00Z"));
    assertThat(bucket("1w", ms("2023-12-31T09:00:00Z"), new HashMap<>())).isEqualTo(ms("2023-12-28T00:00:00Z"));
  }

  @Test
  void anOriginMovesWeeksToMonday() {
    final Map<String, Object> options = Map.of("origin", "2024-01-01T00:00:00Z");
    // the four rows of the issue's table, now on Monday
    assertThat(bucket("1w", ms("2023-12-31T09:00:00Z"), options)).isEqualTo(ms("2023-12-25T00:00:00Z"));
    assertThat(bucket("1w", ms("2024-02-29T13:00:00Z"), options)).isEqualTo(ms("2024-02-26T00:00:00Z"));
    assertThat(bucket("1w", ms("2025-06-15T18:30:00Z"), options)).isEqualTo(ms("2025-06-09T00:00:00Z"));
    assertThat(bucket("1w", ms("2026-03-03T02:00:00Z"), options)).isEqualTo(ms("2026-03-02T00:00:00Z"));
    // an origin AFTER the point, and a point exactly on a boundary
    assertThat(bucket("1w", ms("2023-12-25T00:00:00Z"), options)).isEqualTo(ms("2023-12-25T00:00:00Z"));
    assertThat(bucket("1w", ms("2023-12-24T23:59:59.999Z"), options)).isEqualTo(ms("2023-12-18T00:00:00Z"));
  }

  @Test
  void aBareInstantMeansTheOrigin() {
    assertThat(bucket("1w", ms("2025-06-15T18:30:00Z"), "2024-01-01T00:00:00Z")).isEqualTo(ms("2025-06-09T00:00:00Z"));
    assertThat(bucket("1w", ms("2025-06-15T18:30:00Z"), ms("2024-01-01T00:00:00Z"))).isEqualTo(ms("2025-06-09T00:00:00Z"));
  }

  @Test
  void anOffsetGivesLocalDays() {
    // UTC+8 local midnight is 16:00 UTC of the day before
    final Map<String, Object> options = Map.of("offset", "-8h");
    assertThat(bucket("1d", ms("2024-03-01T20:00:00Z"), options)).isEqualTo(ms("2024-03-01T16:00:00Z"));
    assertThat(bucket("1d", ms("2024-03-01T15:59:59Z"), options)).isEqualTo(ms("2024-02-29T16:00:00Z"));
    assertThat(bucket("1d", ms("2024-03-01T16:00:00Z"), options)).isEqualTo(ms("2024-03-01T16:00:00Z"));
    // positive and unsigned spellings
    assertThat(bucket("1d", ms("2024-03-01T20:00:00Z"), Map.of("offset", "+16h"))).isEqualTo(ms("2024-03-01T16:00:00Z"));
    assertThat(bucket("1d", ms("2024-03-01T20:00:00Z"), Map.of("offset", "16h"))).isEqualTo(ms("2024-03-01T16:00:00Z"));
    assertThat(bucket("1d", ms("2024-03-01T20:00:00Z"), Map.of("offset", -8 * HOUR))).isEqualTo(ms("2024-03-01T16:00:00Z"));
  }

  @Test
  void aFixedOffsetTimezoneGivesLocalDaysAndLocalMondays() {
    final Map<String, Object> options = Map.of("timezone", "+08:00");
    assertThat(bucket("1d", ms("2024-03-01T20:00:00Z"), options)).isEqualTo(ms("2024-03-01T16:00:00Z"));
    // Sun 2025-06-15 18:30 UTC is Mon 2025-06-16 02:30 local: the local week of Mon 2025-06-16 00:00 +08:00
    assertThat(bucket("1w", ms("2025-06-15T18:30:00Z"), options)).isEqualTo(ms("2025-06-15T16:00:00Z"));
    assertThat(bucket("1w", ms("2025-06-15T15:59:59Z"), options)).isEqualTo(ms("2025-06-08T16:00:00Z"));
    assertThat(bucket("1d", ms("2024-03-01T20:00:00Z"), Map.of("timezone", "UTC+8"))).isEqualTo(ms("2024-03-01T16:00:00Z"));
  }

  @Test
  void aZoneWithDaylightSavingIsRefusedNotApproximated() {
    assertThatThrownBy(() -> bucket("1d", ms("2024-03-01T20:00:00Z"), Map.of("timezone", "Europe/Rome")))
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("daylight saving").hasMessageContaining("+08:00");
    assertThatThrownBy(() -> bucket("1d", ms("2024-03-01T20:00:00Z"), Map.of("timezone", "Not/AZone")))
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Unknown ts.timeBucket timezone");
  }

  @Test
  void malformedOptionsAreRefusedByName() {
    assertThatThrownBy(() -> bucket("1d", 0L, Map.of("origine", "x"))).isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("origine");
    assertThatThrownBy(() -> bucket("1d", 0L, Map.of("origin", "2024-01-01T00:00:00Z", "offset", "1h")))
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("only one of");
    assertThatThrownBy(() -> bucket("1d", 0L, Map.of("offset", "soon"))).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void aPreEpochTimestampFloorsOnTheShiftedGrid() {
    final Map<String, Object> options = Map.of("offset", "-8h");
    assertThat(bucket("1d", -1L, options)).isEqualTo(-8 * HOUR);
    assertThat(bucket("1d", -8 * HOUR, options)).isEqualTo(-8 * HOUR);
    assertThat(bucket("1d", -8 * HOUR - 1, options)).isEqualTo(-8 * HOUR - DAY);
  }

  @Test
  void theSqlSyntaxReachesTheFunction() {
    try (final ResultSet rs = database.query("sql",
        "SELECT ts.timeBucket('1w', ?, {'origin': '2024-01-01T00:00:00Z'}) AS b, ts.timeBucket('1d', ?, {'offset': '-8h'}) AS d",
        ms("2025-06-15T18:30:00Z"), ms("2024-03-01T20:00:00Z"))) {
      final Result row = rs.next();
      assertThat(bucketMs(row.getProperty("b"))).isEqualTo(ms("2025-06-09T00:00:00Z"));
      assertThat(bucketMs(row.getProperty("d"))).isEqualTo(ms("2024-03-01T16:00:00Z"));
    }
  }

  // ---- the aggregation push-down must bucket exactly as the function does ----

  private static final int N = 3_000;

  /** One sample every 41 minutes from before the epoch-aligned week start, spanning a few weeks. */
  private long sampleTs(final int i) {
    return ms("2025-05-20T03:07:00Z") + i * 41L * 60_000L;
  }

  private Map<Long, double[]> expected(final long interval, final long offset) {
    final TreeMap<Long, double[]> buckets = new TreeMap<>();
    for (int i = 0; i < N; i++) {
      final long b = TimeBucketGrid.bucketStart(sampleTs(i), interval, offset);
      final double[] acc = buckets.computeIfAbsent(b, k -> new double[2]);
      acc[0] += i;
      acc[1]++;
    }
    return buckets;
  }

  private void loadSamples(final boolean sealPart) throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE M TIMESTAMP ts TAGS (host STRING) FIELDS (v DOUBLE) SHARDS 2");
    final TimeSeriesEngine engine = ((LocalTimeSeriesType) database.getSchema().getType("M")).getEngine();
    final int sealed = sealPart ? N * 2 / 3 : 0;
    // sealed part first (its blocks answer from their header statistics), then the mutable tail
    for (int from = 0; from < N; ) {
      final int to = Math.min(N, from < sealed ? Math.min(sealed, from + 500) : from + 500);
      final long[] ts = new long[to - from];
      final Object[] hosts = new Object[to - from];
      final Object[] vals = new Object[to - from];
      for (int i = from; i < to; i++) {
        ts[i - from] = sampleTs(i);
        hosts[i - from] = "h";
        vals[i - from] = (double) i;
      }
      engine.appendSamples(ts, hosts, vals);
      from = to;
      if (from == sealed)
        engine.compactAll();
    }
  }

  private void assertSqlMatches(final String interval, final long intervalMs, final String options, final long offset) {
    final Map<Long, double[]> expected = expected(intervalMs, offset);
    final String sql = "SELECT ts.timeBucket('" + interval + "', ts" + (options != null ? ", " + options : "")
        + ") AS b, sum(v) AS s, count(*) AS c FROM M GROUP BY b ORDER BY b";
    final List<Long> buckets = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", sql)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        final long b = bucketMs(row.getProperty("b"));
        buckets.add(b);
        final double[] e = expected.get(b);
        assertThat(e).as("bucket " + Instant.ofEpochMilli(b) + " must exist on the grid").isNotNull();
        assertThat(row.<Number>getProperty("s").doubleValue()).as("sum of " + Instant.ofEpochMilli(b)).isEqualTo(e[0]);
        assertThat(row.<Number>getProperty("c").longValue()).as("count of " + Instant.ofEpochMilli(b)).isEqualTo((long) e[1]);
      }
    }
    assertThat(buckets).containsExactlyElementsOf(expected.keySet());
  }

  @Test
  void theAggregationPushDownHonoursTheOrigin() throws Exception {
    loadSamples(true);

    // the planner really did push this down, with the grid it was asked for
    try (final ResultSet rs = database.query("sql",
        "EXPLAIN SELECT ts.timeBucket('1w', ts, {'origin': '2024-01-01T00:00:00Z'}) AS b, sum(v) AS s FROM M GROUP BY b")) {
      assertThat(rs.next().<String>getProperty("executionPlanAsString")).contains("AGGREGATE FROM TIMESERIES").contains("offset=");
    }

    final long monday = TimeBucketGrid.normalizeOffset(ms("2024-01-01T00:00:00Z"), WEEK);
    assertSqlMatches("1w", WEEK, "{'origin': '2024-01-01T00:00:00Z'}", monday);
    assertSqlMatches("1d", DAY, "{'offset': '-8h'}", TimeBucketGrid.normalizeOffset(-8 * HOUR, DAY));
    assertSqlMatches("1d", DAY, "{'timezone': '+08:00'}", TimeBucketGrid.normalizeOffset(-8 * HOUR, DAY));
    assertSqlMatches("1w", WEEK, "{'timezone': '+08:00'}", TimeBucketGrid.normalizeOffset(4 * DAY - 8 * HOUR, WEEK));
    assertSqlMatches("1w", WEEK, null, 0L);
  }

  @Test
  void theMutableOnlyPathHonoursTheOriginToo() throws Exception {
    loadSamples(false);
    assertSqlMatches("1w", WEEK, "{'origin': '2024-01-01T00:00:00Z'}", TimeBucketGrid.normalizeOffset(ms("2024-01-01T00:00:00Z"), WEEK));
  }

  @Test
  void theEngineAggregatesOnTheGivenGridAcrossSealedAndMutable() throws Exception {
    loadSamples(true);
    final TimeSeriesEngine engine = ((LocalTimeSeriesType) database.getSchema().getType("M")).getEngine();
    final long offset = TimeBucketGrid.normalizeOffset(ms("2024-01-01T00:00:00Z"), WEEK);
    final MultiColumnAggregationResult result = engine.aggregateMulti(Long.MIN_VALUE, Long.MAX_VALUE,
        List.of(MultiColumnAggregationRequest.count("c")), WEEK, offset, null, null, 0);

    final Map<Long, double[]> expected = expected(WEEK, offset);
    assertThat(result.getBucketTimestamps()).containsExactlyElementsOf(expected.keySet());
    for (final Map.Entry<Long, double[]> e : expected.entrySet())
      assertThat(result.getValue(e.getKey(), 0)).isEqualTo(e.getValue()[1]);
  }

  @Test
  void aContinuousAggregateAcceptsAnOriginAndKeepsItAcrossRefreshes() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE S TIMESTAMP ts TAGS (host STRING) FIELDS (v DOUBLE) SHARDS 1");
    final TimeSeriesEngine engine = ((LocalTimeSeriesType) database.getSchema().getType("S")).getEngine();
    final long start = ms("2025-06-09T00:00:00Z");
    for (int d = 0; d < 20; d++)
      engine.appendSamples(new long[] { start + d * DAY + 5 * HOUR }, new Object[] { "h" }, new Object[] { 1.0 });

    database.getSchema().buildContinuousAggregate().withName("weekly")
        .withQuery("SELECT ts.timeBucket('1w', ts, {'origin': '2024-01-01T00:00:00Z'}) AS wk, count(*) AS n FROM S GROUP BY wk")
        .create();

    // 20 daily samples from Monday 2025-06-09 05:00: weeks of 7, 7 and 6
    final List<Long> weeks = new ArrayList<>();
    final List<Long> counts = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT wk, n FROM weekly ORDER BY wk")) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        weeks.add(bucketMs(row.getProperty("wk")));
        counts.add(row.<Number>getProperty("n").longValue());
      }
    }
    assertThat(weeks).containsExactly(start, start + WEEK, start + 2 * WEEK);
    assertThat(counts).containsExactly(7L, 7L, 6L);

    // more data, refresh: the incremental refresh keeps the same grid and does not duplicate a week
    engine.appendSamples(new long[] { start + 20 * DAY + 5 * HOUR }, new Object[] { "h" }, new Object[] { 1.0 });
    database.getSchema().getContinuousAggregate("weekly").refresh();
    final List<Long> after = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT n FROM weekly ORDER BY wk")) {
      while (rs.hasNext())
        after.add(rs.next().<Number>getProperty("n").longValue());
    }
    assertThat(after).containsExactly(7L, 7L, 7L);
  }

  @Test
  void aDownsamplingTierBucketsOnItsOwnGridAndTheDdlRoundTrips() throws Exception {
    database.command("sql",
        "CREATE TIMESERIES TYPE D TIMESTAMP ts TAGS (host STRING) FIELDS (v DOUBLE) SHARDS 1"
            + " DOWNSAMPLING POLICY AFTER 1 HOURS GRANULARITY 1 DAYS OFFSET -8 HOURS");
    final LocalTimeSeriesType type = (LocalTimeSeriesType) database.getSchema().getType("D");
    assertThat(type.getDownsamplingTiers()).containsExactly(new DownsamplingTier(HOUR, DAY, -8 * HOUR));

    // the printed statement parses back to the same tier
    final Statement parsed = ((DatabaseInternal) database).getStatementCache()
        .get("ALTER TIMESERIES TYPE D ADD DOWNSAMPLING POLICY AFTER 1 HOURS GRANULARITY 1 DAYS OFFSET -8 HOURS");
    final String printed = parsed.toString();
    assertThat(printed).contains("OFFSET -8 HOURS");
    assertThat(((DatabaseInternal) database).getStatementCache().get(printed)).isEqualTo(parsed);
    database.command("sql", "ALTER TIMESERIES TYPE D DROP DOWNSAMPLING POLICY");
    database.command("sql", "ALTER TIMESERIES TYPE D ADD DOWNSAMPLING POLICY AFTER 1 HOURS GRANULARITY 1 DAYS OFFSET -8 HOURS");
    assertThat(type.getDownsamplingTiers().getFirst().offsetMs()).isEqualTo(-8 * HOUR);

    // a sample every 30 minutes over three UTC days, sealed
    final long start = ms("2024-03-01T00:00:00Z");
    final TimeSeriesEngine engine = type.getEngine();
    final long[] ts = new long[144];
    final Object[] hosts = new Object[144];
    final Object[] vals = new Object[144];
    for (int i = 0; i < 144; i++) {
      ts[i] = start + i * 30 * 60_000L;
      hosts[i] = "h";
      vals[i] = 1.0;
    }
    engine.appendSamples(ts, hosts, vals);
    engine.compactAll();
    engine.applyDownsampling(type.getDownsamplingTiers(), ms("2024-04-01T00:00:00Z"));

    final List<Object[]> rows = engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, null);
    assertThat(rows).isNotEmpty();
    for (final Object[] row : rows)
      assertThat(Math.floorMod((long) row[0] + 8 * HOUR, DAY)).as("bucket " + Instant.ofEpochMilli((long) row[0])).isZero();

    // and the schema keeps it across a reopen
    reopenDatabase();
    assertThat(((LocalTimeSeriesType) database.getSchema().getType("D")).getDownsamplingTiers().getFirst().offsetMs())
        .isEqualTo(-8 * HOUR);
  }

  @Test
  void theBuilderPrintsTheTierOffsetBack() {
    final List<String> sql = database.getSchema().buildTimeSeriesType().withName("B").withTimestamp("ts")
        .withTag("host", Type.STRING).withField("v", Type.DOUBLE)
        .withDownsamplingTiers(List.of(new DownsamplingTier(HOUR, DAY, -8 * HOUR))).toSQL();
    assertThat(sql.getFirst()).contains("GRANULARITY 1 DAYS OFFSET -8 HOURS");
    sql.forEach(statement -> database.command("sql", statement));
    final LocalTimeSeriesType type = (LocalTimeSeriesType) database.getSchema().getType("B");
    assertThat(type.getDownsamplingTiers().getFirst().offsetMs()).isEqualTo(-8 * HOUR);
    assertThat(type.toJSON().getJSONArray("downsamplingTiers").getJSONObject(0).getLong("offsetMs")).isEqualTo(-8 * HOUR);
  }

  @Test
  void aNonPositiveIntervalIsRefusedByTheGrid() {
    assertThatThrownBy(() -> TimeBucketGrid.normalizeOffset(5L, 0L)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void aPerRowOptionsArgumentIsNotPushedDownOnTheEpochGrid() {
    database.command("sql", "CREATE TIMESERIES TYPE P TIMESTAMP ts TAGS (host STRING) FIELDS (v DOUBLE, shift LONG) SHARDS 1");
    final long t = ms("2024-03-01T20:00:00Z");
    database.command("sql", "INSERT INTO P SET ts = ?, host = 'h', v = 1.0, shift = ?", t, ms("2024-03-01T16:00:00Z"));
    final String sql = "SELECT ts.timeBucket('1d', ts, shift) AS b, count(*) AS c FROM P GROUP BY b";

    try (final ResultSet rs = database.query("sql", "EXPLAIN " + sql)) {
      assertThat(rs.next().<String>getProperty("executionPlanAsString")).doesNotContain("AGGREGATE FROM TIMESERIES");
    }
    try (final ResultSet rs = database.query("sql", sql)) {
      assertThat(bucketMs(rs.next().getProperty("b"))).isEqualTo(ms("2024-03-01T16:00:00Z"));
    }
  }

  @Test
  void anExpressionOverTheRecordIsNotPushedDownEither() {
    database.command("sql", "CREATE TIMESERIES TYPE Q TIMESTAMP ts TAGS (host STRING) FIELDS (v DOUBLE, shift LONG) SHARDS 1");
    database.command("sql", "INSERT INTO Q SET ts = ?, host = 'h', v = 1.0, shift = ?", ms("2024-03-01T20:00:00Z"),
        ms("2024-03-01T16:00:00Z"));
    final String sql = "SELECT ts.timeBucket('1d', ts, coalesce(shift, 0)) AS b, count(*) AS c FROM Q GROUP BY b";
    try (final ResultSet rs = database.query("sql", "EXPLAIN " + sql)) {
      assertThat(rs.next().<String>getProperty("executionPlanAsString")).doesNotContain("AGGREGATE FROM TIMESERIES");
    }
    try (final ResultSet rs = database.query("sql", sql)) {
      assertThat(bucketMs(rs.next().getProperty("b"))).isEqualTo(ms("2024-03-01T16:00:00Z"));
    }
  }
}
