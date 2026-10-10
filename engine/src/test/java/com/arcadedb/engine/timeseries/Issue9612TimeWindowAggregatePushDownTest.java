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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9612: {@code SELECT count(*) FROM T WHERE ts >= ? AND ts < ?} over a compacted TIMESERIES type decoded every
 * sample, although every sealed block carries its sample count and time bounds, and a field predicate such as
 * {@code uu > 90} built a row for every sample before the SQL filter dropped nine in ten of them.
 * <p>
 * An aggregate with no GROUP BY is now pushed into the engine as one bucket over the range, which answers every block
 * wholly inside the range from its statistics; range predicates on numeric FIELD columns are pushed into the engine as
 * a {@link FieldFilter}, judged on each block's min/max before it is read and on the primitive columns of the blocks it
 * has to decode. Every query is checked against a document twin holding the same rows, in every storage state.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9612TimeWindowAggregatePushDownTest extends TestHelper {
  private static final long T0    = 1_767_225_600_000L;
  private static final long HOUR  = 3_600_000L;
  private static final int  HOSTS = 5;

  private static final String RANGE = "ts >= " + (T0 + 20 * 60_000L) + " AND ts < " + (T0 + 3 * HOUR + 7 * 60_000L);

  private void createTypes(final int shards) {
    database.command("sql", "CREATE TIMESERIES TYPE T TIMESTAMP ts TAGS (host STRING) FIELDS (uu DOUBLE, ui LONG, uf FLOAT, us INTEGER) SHARDS "
        + shards + " COMPACTION_INTERVAL 1 HOURS");
    database.command("sql", "CREATE DOCUMENT TYPE D");
  }

  private void load(final int fromSample, final int toSample) {
    database.transaction(() -> {
      for (int s = fromSample; s < toSample; s++)
        for (int h = 0; h < HOSTS; h++) {
          final long ts = T0 + s * 60_000L + h * 1_000L; // one sample a minute per host
          final Double uu = (s + h) % 9 == 0 ? null : (double) ((s * 31 + h * 17) % 101);
          final long ui = s * 3L + h;
          final float uf = ((s + h) % 10) / 10f;
          final int us = (s * 7 + h) % 50;
          final String host = "host_" + h;
          database.command("sql", "INSERT INTO T SET ts = ?, host = ?, uu = ?, ui = ?, uf = ?, us = ?", ts, host, uu, ui, uf, us);
          database.command("sql", "INSERT INTO D SET ts = ?, host = ?, uu = ?, ui = ?, uf = ?, us = ?", ts, host, uu, ui, uf, us);
        }
    });
  }

  private void forEachState(final int shards, final Runnable check) {
    createTypes(shards);
    load(0, 150);
    check.run(); // everything in the mutable buffer
    database.command("sql", "COMPACT TIMESERIES TYPE T");
    check.run(); // everything sealed
    load(150, 220);
    check.run(); // sealed blocks plus a fresh tail
  }

  /** The rows as a sorted list of strings, with temporal values and numbers spelled the same whatever the plan hands back. */
  private List<String> rows(final String sql, final Object... args) {
    final List<String> out = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", sql, args)) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        final TreeMap<String, String> sorted = new TreeMap<>();
        for (final String name : r.getPropertyNames()) {
          final Object value = r.getProperty(name);
          final String text;
          // the twin stores the timestamp as the number it was given, the TIMESERIES type hands it back as a date-time
          if (value instanceof LocalDateTime d)
            text = String.format(Locale.ROOT, "%.6f", (double) d.toInstant(ZoneOffset.UTC).toEpochMilli());
          else if (value instanceof Date d)
            text = String.format(Locale.ROOT, "%.6f", (double) d.getTime());
          else if (value instanceof Number n)
            // the values, not their Java types, which aPushedDownAggregateAnswersTheJavaTypesOfTheGenericPlan compares
            text = String.format(Locale.ROOT, "%.6f", n.doubleValue());
          else
            text = String.valueOf(value);
          sorted.put(name, text);
        }
        out.add(sorted.toString());
      }
    }
    out.sort(null);
    return out;
  }

  private String plan(final String sql, final Object... args) {
    try (final ResultSet rs = database.query("sql", sql, args)) {
      return rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
    }
  }

  /** {@code sql} names the type {@code X}; it is run against the TIMESERIES type and against its document twin. */
  private void assertSameAsTheTwin(final String sql, final Object... args) {
    final List<String> expected = rows(sql.replace(" X", " D"), args);
    assertThat(expected).as("the twin answers something: " + sql).isNotEmpty();
    assertThat(rows(sql.replace(" X", " T"), args)).as(sql).isEqualTo(expected);
  }

  private static final String AGGREGATES = "count(*) AS c, avg(uu) AS a, min(uu) AS mn, max(uu) AS mx, sum(uu) AS sm, sum(ui) AS si, max(us) AS mu";

  @Test
  void anUngroupedAggregateIsPushedDownAndAnswersLikeTheTwin() {
    for (final int shards : new int[] { 1, 3 }) {
      forEachState(shards, () -> {
        final String sql = "SELECT " + AGGREGATES + " FROM X WHERE " + RANGE;
        assertThat(plan(sql.replace(" X", " T"))).contains("AGGREGATE FROM TIMESERIES").contains("ungrouped")
            .doesNotContain("FETCH FROM TIMESERIES");
        assertSameAsTheTwin(sql);
        assertSameAsTheTwin("SELECT count(*) AS c FROM X WHERE " + RANGE);
        assertSameAsTheTwin("SELECT avg(uu) AS a, count(*) AS c FROM X");
        assertSameAsTheTwin("SELECT " + AGGREGATES + " FROM X WHERE host = 'host_2' AND " + RANGE);
        assertSameAsTheTwin("SELECT count(*) AS c, sum(uu) AS s FROM X WHERE (host = 'host_1' OR host = 'host_3') AND " + RANGE);
        assertSameAsTheTwin("SELECT count(*) AS c FROM X WHERE ts >= ? AND ts <= ?", T0 + HOUR, T0 + 2 * HOUR);
      });
      database.command("sql", "DROP TYPE T");
      database.command("sql", "DROP TYPE D");
    }
  }

  @Test
  void anUngroupedAggregateOverNoSampleIsOneRowLikeTheTwin() {
    forEachState(2, () -> {
      // a COUNT of 0 and NULL for every other aggregate, as SQL answers an aggregate with no GROUP BY over no rows
      final String sql = "SELECT " + AGGREGATES + " FROM X WHERE ts >= " + (T0 + 100 * HOUR) + " AND ts < " + (T0 + 101 * HOUR);
      assertThat(plan(sql.replace(" X", " T"))).contains("ungrouped");
      assertSameAsTheTwin(sql);
      assertThat(rows(sql.replace(" X", " T"))).hasSize(1);
      assertSameAsTheTwin("SELECT count(*) AS c, avg(uu) AS a FROM X WHERE uu > 50 AND uu < 10");
      assertSameAsTheTwin("SELECT count(*) AS c, sum(uu) AS s FROM X WHERE host = 'nobody'");
      // a reversed BETWEEN matches nothing on both plans
      assertThat(plan("SELECT count(*) AS c FROM T WHERE uu BETWEEN 30 AND 10")).contains("FIELDS uu >= 30.0 AND uu <= 10.0");
      assertSameAsTheTwin("SELECT count(*) AS c, max(uu) AS m FROM X WHERE uu BETWEEN 30 AND 10");
      assertSameAsTheTwin("SELECT count(*) AS c, max(ui) AS m FROM X WHERE ui BETWEEN 300 AND 100");
    });
  }

  /** The Java type of each column of the only row a query answers. */
  private List<String> types(final String sql) {
    final List<String> out = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", sql)) {
      final Result r = rs.next();
      for (final String name : r.getPropertyNames().stream().sorted().toList()) {
        final Object value = r.getProperty(name);
        out.add(name + ":" + (value == null ? "null" : value.getClass().getSimpleName()));
      }
    }
    return out;
  }

  @Test
  void aPushedDownAggregateAnswersTheJavaTypesOfTheGenericPlan() {
    forEachState(2, () -> {
      // min/max hand back a sample of the column, sum keeps an integral total integral and a FLOAT total a FLOAT, avg is a Double
      final String projection = "count(*) AS c, sum(ui) AS si, sum(us) AS su, sum(uf) AS sf, sum(uu) AS sd, min(us) AS mnu, max(ui) AS mxi, "
          + "min(uf) AS mnf, max(uu) AS mxd, avg(ui) AS ai";
      final String ungrouped = "SELECT " + projection + " FROM X WHERE " + RANGE;
      assertThat(plan(ungrouped.replace(" X", " T"))).contains("ungrouped");
      assertThat(types(ungrouped.replace(" X", " T"))).isEqualTo(types(ungrouped.replace(" X", " D")));

      final String filtered = "SELECT " + projection + " FROM X WHERE uu > 30 AND " + RANGE;
      assertThat(types(filtered.replace(" X", " T"))).isEqualTo(types(filtered.replace(" X", " D")));

      final String byHost = "SELECT host, " + projection + " FROM X WHERE host = 'host_1' GROUP BY host";
      assertThat(plan(byHost.replace(" X", " T"))).contains("group by host");
      assertThat(types(byHost.replace(" X", " T"))).isEqualTo(types(byHost.replace(" X", " D")));

      // the time-bucket push-down answered Doubles before 26.11.1; it answers the generic plan's types too now
      final String byHour = "SELECT ts.timeBucket('1h', ts) AS h, " + projection + " FROM X WHERE " + RANGE + " GROUP BY h";
      assertThat(plan(byHour.replace(" X", " T"))).contains("bucket=3600000ms");
      final List<String> typesOfTheBucket = types(byHour.replace(" X", " T"));
      typesOfTheBucket.removeIf(t -> t.startsWith("h:"));
      final List<String> typesOfTheTwin = types(byHour.replace(" X", " D"));
      typesOfTheTwin.removeIf(t -> t.startsWith("h:"));
      assertThat(typesOfTheBucket).isEqualTo(typesOfTheTwin);
    });
  }

  @Test
  void anIntegralTotalPastWhatADoubleHoldsExactlyStaysADouble() {
    database.command("sql", "CREATE TIMESERIES TYPE T TIMESTAMP ts TAGS (host STRING) FIELDS (ui LONG) SHARDS 1");
    final long big = 1L << 60;
    database.transaction(() -> {
      for (int s = 0; s < 3; s++)
        database.command("sql", "INSERT INTO T SET ts = ?, host = 'a', ui = ?", T0 + s * 1_000L, big + s);
    });
    database.command("sql", "COMPACT TIMESERIES TYPE T");
    // the engine accumulates in doubles, so a total past 2^53 may have been rounded: it is not handed back as an exact Long
    try (final ResultSet rs = database.query("sql", "SELECT sum(ui) AS s, max(ui) AS m, count(*) AS c FROM T")) {
      final Result r = rs.next();
      assertThat(r.<Object>getProperty("s")).isInstanceOf(Double.class);
      assertThat(r.<Object>getProperty("m")).isInstanceOf(Double.class);
      assertThat(r.<Long>getProperty("c")).isEqualTo(3L);
    }
    try (final ResultSet rs = database.query("sql", "SELECT sum(ui) AS s FROM T WHERE ui < 0")) {
      assertThat(rs.next().<Object>getProperty("s")).isNull();
    }
  }

  @Test
  void aFieldFilteredWalkFollowsAMergeOfSmallBlocksAndEmitsEachPassingRowOnce() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE T TIMESTAMP ts TAGS (host STRING) FIELDS (uu DOUBLE) SHARDS 1");
    final TimeSeriesEngine engine = engine();
    // six small blocks of 20 samples each, uu = 0..119 in time order
    for (int block = 0; block < 6; block++) {
      final long[] timestamps = new long[20];
      final Object[] hosts = new Object[20];
      final Object[] values = new Object[20];
      for (int i = 0; i < 20; i++) {
        final int v = block * 20 + i;
        timestamps[i] = T0 + v * 1_000L;
        hosts[i] = "h" + (v % 3);
        values[i] = (double) v;
      }
      engine.appendSamples(timestamps, hosts, values);
      engine.compactAll();
    }
    final TimeSeriesSealedStore sealed = engine.getShard(0).getSealedStore();
    final BlockDirectorySnapshot snapshot = sealed.snapshotBlockDirectory(Long.MIN_VALUE, Long.MAX_VALUE);
    assertThat(snapshot.blocks()).hasSize(6);

    // 10 <= uu < 110: the first and the fifth block straddle it, the last block cannot match
    final List<ColumnDefinition> columns = engine.getColumns();
    final FieldFilter filter = FieldFilter.range(1, columns.get(2), 10, true, 110, false);
    final Iterator<Object[]> it = sealed.iterateRange(snapshot, Long.MIN_VALUE, Long.MAX_VALUE, null, null, filter, new AggregationMetrics());
    final List<Double> seen = new ArrayList<>();
    while (it.hasNext()) {
      seen.add(((Number) it.next()[2]).doubleValue());
      // the merge lands while the walk is in its second block, so the rest of the walk reads the merged block in part
      if (seen.size() == 15) {
        engine.mergeSmallBlocks();
        assertThat(sealed.getBlockCount()).isEqualTo(1);
      }
    }
    final List<Double> expected = new ArrayList<>();
    for (int v = 10; v < 110; v++)
      expected.add((double) v);
    seen.sort(null);
    assertThat(seen).isEqualTo(expected);
  }

  @Test
  void fieldPredicatesArePushedIntoAnUngroupedAggregate() {
    forEachState(3, () -> {
      final String[] predicates = { "uu > 50", "uu >= 50", "uu < 20", "uu <= 20", "uu = 42", "42 < uu", "50 >= uu", "uu > 50.5",
          "uu BETWEEN 10 AND 30", "uu > 10 AND uu < 30", "ui > 300", "ui >= 300 AND ui <= 450", "ui = 77", "us < 7", "us BETWEEN 3 AND 9",
          "uu > 50 AND ui < 400", "uu > -1", "host = 'host_2' AND uu > 50", "host = 'host_0' AND ui BETWEEN 100 AND 400" };
      for (final String predicate : predicates) {
        final String sql = "SELECT " + AGGREGATES + " FROM X WHERE " + predicate + " AND " + RANGE;
        assertThat(plan(sql.replace(" X", " T"))).as(predicate).contains("AGGREGATE FROM TIMESERIES").contains(" FIELDS ")
            .doesNotContain("FETCH FROM TIMESERIES");
        assertSameAsTheTwin(sql);
        assertSameAsTheTwin("SELECT count(*) AS c FROM X WHERE " + predicate);
      }
      // the same statement with other values: the operands are read when the statement is planned, so a second run must not
      // reuse the first one's filter
      assertSameAsTheTwin("SELECT count(*) AS c, avg(uu) AS a FROM X WHERE uu > ? AND ui <= ?", 30, 500L);
      assertSameAsTheTwin("SELECT count(*) AS c, avg(uu) AS a FROM X WHERE uu > ? AND ui <= ?", 80, 150L);
      assertSameAsTheTwin("SELECT count(*) AS c, avg(uu) AS a FROM X WHERE uu > ? AND ui <= ?", 30, 500L);
    });
  }

  @Test
  void fieldPredicatesArePushedIntoBucketedAndTagGroupedAggregates() {
    forEachState(2, () -> {
      final String bucketed = "SELECT ts.timeBucket('1h', ts) AS h, " + AGGREGATES + " FROM X WHERE uu > 40 AND " + RANGE + " GROUP BY h";
      assertThat(plan(bucketed.replace(" X", " T"))).contains("AGGREGATE FROM TIMESERIES").contains("FIELDS uu > 40.0");
      assertSameAsTheTwin(bucketed);

      final String byHost = "SELECT host, " + AGGREGATES + " FROM X WHERE ui BETWEEN 100 AND 500 GROUP BY host";
      assertThat(plan(byHost.replace(" X", " T"))).contains("group by host").contains("FIELDS ui BETWEEN 100 AND 500");
      assertSameAsTheTwin(byHost);

      assertSameAsTheTwin("SELECT host, ts.timeBucket('30m', ts) AS h, count(*) AS c, max(uu) AS m FROM X WHERE uu <= 60 GROUP BY host, h");
    });
  }

  @Test
  void aRowScanWithAFieldPredicateFiltersInTheEngine() {
    forEachState(3, () -> {
      final String sql = "SELECT ts, host, uu, ui FROM X WHERE uu > 70 AND " + RANGE;
      assertThat(plan(sql.replace(" X", " T"))).contains("FETCH FROM TIMESERIES").contains("FIELDS uu > 70.0");
      assertSameAsTheTwin(sql);
      assertSameAsTheTwin("SELECT ts, uu FROM X WHERE ui >= 200 AND ui < 260 AND host = 'host_4'");
      assertSameAsTheTwin("SELECT ts, host, uu, ui, uf, us FROM X WHERE us = 0");
      // a field predicate leaves a residual filter, so a LIMIT is not pushed into the read as a cap: the read stays the
      // unbounded one, which carries the field filter, and the LIMIT step above it stops pulling
      assertThat(plan("SELECT ts, uu FROM T WHERE uu > 70 LIMIT 5")).contains("FIELDS uu > 70.0").doesNotContain(" TOP ");
      assertThat(rows("SELECT ts, uu FROM T WHERE uu > 70 ORDER BY ts LIMIT 5")).isEqualTo(rows("SELECT ts, uu FROM D WHERE uu > 70 ORDER BY ts LIMIT 5"));
    });
  }

  @Test
  void predicatesTheEngineCannotReproduceStayWithTheSqlFilter() {
    forEachState(2, () -> {
      // column against column, a FLOAT column, an inequality, an OR, a null operand, a string operand, a function of the field
      final String[] predicates = { "uu > ui", "uf > 0.5", "uu <> 50", "(uu > 90 OR uu < 5)", "uu > null", "uu > '50'", "abs(uu) > 50",
          "ui > 300.5", "uu IN [10, 20, 30]" };
      for (final String predicate : predicates) {
        final String sql = "SELECT count(*) AS c, sum(uu) AS s FROM X WHERE " + predicate + " AND " + RANGE;
        assertThat(plan(sql.replace(" X", " T"))).as(predicate).doesNotContain(" FIELDS ");
        assertSameAsTheTwin(sql);
      }
    });
  }

  // ---- engine level: what the block statistics save

  private TimeSeriesEngine engine() {
    return ((LocalTimeSeriesType) database.getSchema().getType("T")).getEngine();
  }

  /** One shard, one sample a minute for 6 hours, {@code uu} growing with time so that every hourly block covers its own value range. */
  private void loadMonotonic() {
    database.command("sql", "CREATE TIMESERIES TYPE T TIMESTAMP ts TAGS (host STRING) FIELDS (uu DOUBLE, ui LONG) SHARDS 1 COMPACTION_INTERVAL 1 HOURS");
    database.transaction(() -> {
      for (int s = 0; s < 360; s++)
        database.command("sql", "INSERT INTO T SET ts = ?, host = 'a', uu = ?, ui = ?", T0 + s * 60_000L, (double) s, (long) s);
    });
    database.command("sql", "COMPACT TIMESERIES TYPE T");
  }

  @Test
  void aCountOverAWindowDecodesOnlyTheBlocksStraddlingItsBounds() throws Exception {
    loadMonotonic();
    final AggregationMetrics metrics = new AggregationMetrics();
    // [00:30, 04:30]: two boundary blocks decoded, the three hours in between answered from their statistics
    final MultiColumnAggregationResult result = engine().aggregateMulti(T0 + 30 * 60_000L, T0 + 270 * 60_000L,
        List.of(MultiColumnAggregationRequest.count("c")), 0, 0, null, null, metrics, 0);
    assertThat(result.getBucketTimestamps()).hasSize(1);
    assertThat((long) result.getValue(result.getBucketTimestamps().getFirst(), 0)).isEqualTo(241L);
    assertThat(metrics.getSlowPathBlocks()).isEqualTo(2);
    assertThat(metrics.getFastPathBlocks()).isEqualTo(3);
  }

  @Test
  void aFieldPredicateSkipsTheBlocksItsStatisticsRuleOut() throws Exception {
    loadMonotonic();
    final AggregationMetrics metrics = new AggregationMetrics();
    final List<ColumnDefinition> columns = engine().getColumns();
    // uu > 150.5 with uu = minute index: hours 0-1 cannot match, hour 2 straddles it, hours 3-5 match whole
    final FieldFilter filter = FieldFilter.range(1, columns.get(2), 150.5, false, null, false);
    final MultiColumnAggregationResult result = engine().aggregateMulti(Long.MIN_VALUE, Long.MAX_VALUE,
        List.of(MultiColumnAggregationRequest.count("c")), 0, 0, null, filter, metrics, 0);
    assertThat((long) result.getValue(result.getBucketTimestamps().getFirst(), 0)).isEqualTo(209L);
    assertThat(metrics.getSkippedBlocks()).isEqualTo(2);
    assertThat(metrics.getSlowPathBlocks()).isEqualTo(1);
    assertThat(metrics.getFastPathBlocks()).isEqualTo(3);

    // the row read builds only the rows that pass
    final AggregationMetrics rowMetrics = new AggregationMetrics();
    final FieldFilter integral = FieldFilter.range(2, columns.get(3), 100, true, 119, true);
    final Iterator<Object[]> it = engine().iterateQuery(Long.MIN_VALUE, Long.MAX_VALUE, null, null, integral, rowMetrics);
    int rows = 0;
    while (it.hasNext()) {
      final Object[] row = it.next();
      assertThat(((Number) row[3]).longValue()).isBetween(100L, 119L);
      rows++;
    }
    assertThat(rows).isEqualTo(20);
    assertThat(rowMetrics.getMaterializedRows()).isEqualTo(20L);
    assertThat(rowMetrics.getSkippedBlocks()).isEqualTo(5);
  }

  // ---- the filter itself

  @Test
  void aFieldFilterOnlyTakesTheComparisonsSqlMakesAsPlainNumbers() {
    final ColumnDefinition dbl = new ColumnDefinition("d", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD);
    final ColumnDefinition lng = new ColumnDefinition("l", Type.LONG, ColumnDefinition.ColumnRole.FIELD);
    final ColumnDefinition flt = new ColumnDefinition("f", Type.FLOAT, ColumnDefinition.ColumnRole.FIELD);
    final ColumnDefinition tag = new ColumnDefinition("t", Type.LONG, ColumnDefinition.ColumnRole.TAG);

    assertThat(FieldFilter.supports(dbl, 5)).isTrue();
    assertThat(FieldFilter.supports(dbl, 5L)).isTrue();
    assertThat(FieldFilter.supports(dbl, 5.5)).isTrue();
    assertThat(FieldFilter.supports(dbl, Double.NaN)).isFalse();
    assertThat(FieldFilter.supports(dbl, 5.5f)).isFalse();
    assertThat(FieldFilter.supports(dbl, new BigDecimal("5.5"))).isFalse();
    assertThat(FieldFilter.supports(dbl, "5")).isFalse();
    assertThat(FieldFilter.supports(lng, 5)).isTrue();
    assertThat(FieldFilter.supports(lng, 5.5)).isFalse();
    assertThat(FieldFilter.supports(flt, 5)).isFalse();
    assertThat(FieldFilter.supports(tag, 5)).isFalse();

    // integral bounds are normalised to inclusive longs, and an exclusive bound past the extreme matches nothing
    assertThat(FieldFilter.range(0, lng, 5, false, 9, false).describe()).isEqualTo("l BETWEEN 6 AND 8");
    assertThat(FieldFilter.range(0, lng, Long.MAX_VALUE, false, null, false).matchesNothing()).isTrue();
    assertThat(FieldFilter.range(0, lng, null, false, Long.MIN_VALUE, false).matchesNothing()).isTrue();
    final FieldFilter atTheTop = FieldFilter.range(0, lng, Long.MAX_VALUE, true, null, false);
    assertThat(atTheTop.matchesNothing()).isFalse();
    assertThat(atTheTop.matches(new Object[] { 1L, Long.MAX_VALUE })).isTrue();
    assertThat(atTheTop.matches(new Object[] { 1L, Long.MAX_VALUE - 1 })).isFalse();
    assertThat(atTheTop.describe()).isEqualTo("l = " + Long.MAX_VALUE); // nothing is above it
    assertThat(FieldFilter.range(0, lng, null, false, Long.MIN_VALUE, true).matches(new Object[] { 1L, Long.MIN_VALUE })).isTrue();
    // a DOUBLE column against a Long operand past 2^53 compares as doubles, as SQL widens that pair
    final FieldFilter pastExact = FieldFilter.range(0, dbl, (1L << 53) + 1, false, null, false);
    assertThat(pastExact.matches(new Object[] { 1L, 0x1p53 })).isFalse();
    assertThat(pastExact.matches(new Object[] { 1L, 0x1p53 + 2 })).isTrue();
    assertThat(FieldFilter.range(0, dbl, 5, false, 5, true).matchesNothing()).isTrue();
    assertThat(FieldFilter.range(0, dbl, 5, true, 5, true).describe()).isEqualTo("d = 5.0");

    // a missing measurement never matches, and a row is read by its non-timestamp index
    final FieldFilter positive = FieldFilter.range(0, dbl, 0, false, null, false);
    assertThat(positive.matches(new Object[] { 1L, 2.0 })).isTrue();
    assertThat(positive.matches(new Object[] { 1L, -2.0 })).isFalse();
    assertThat(positive.matches(new Object[] { 1L, null })).isFalse();
    assertThat(positive.matches(new Object[] { 1L, Double.NaN })).isFalse();
    // a mutable row may hold any Number for a DOUBLE column
    assertThat(positive.matches(new Object[] { 1L, 3 })).isTrue();
    assertThat(positive.matches(new Object[] { 1L, -3L })).isFalse();
  }

  @Test
  void selectKeepsTheRowsEveryConditionPassesInAscendingOrder() {
    final ColumnDefinition dbl = new ColumnDefinition("d", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD);
    final ColumnDefinition lng = new ColumnDefinition("l", Type.LONG, ColumnDefinition.ColumnRole.FIELD);
    final FieldFilter filter = FieldFilter.range(0, dbl, 2, false, null, false).and(FieldFilter.range(1, lng, null, false, 40, true));
    final double[] d = { 1, 3, Double.NaN, 5, 7, 9 };
    final long[] l = { 10, 20, 30, 40, 50, 30 };
    final int[] selected = new int[d.length];
    // rows 1 and 3 pass both; 0 fails d > 2, 2 is absent, 4 fails l <= 40, and 5 is outside the range handed in
    final int count = FieldFilter.select(filter.getConditions(), new Object[] { d, l }, 0, 5, selected);
    assertThat(count).isEqualTo(2);
    assertThat(selected[0]).isEqualTo(1);
    assertThat(selected[1]).isEqualTo(3);
    for (int i = 0; i < 5; i++)
      assertThat(FieldFilter.matchesAt(filter.getConditions(), new Object[] { d, l }, i)).isEqualTo(i == 1 || i == 3);
  }

  @Test
  void blockStatisticsOfAnIntegerColumnAreReadConservatively() {
    final ColumnDefinition lng = new ColumnDefinition("l", Type.LONG, ColumnDefinition.ColumnRole.FIELD);
    // 2^53 + 1 rounds to 2^53 as a double, so a block whose statistics read [2^53, 2^53] may hold 2^53 + 1
    final long big = (1L << 53) + 1;
    final FieldFilter.Condition above = FieldFilter.range(0, lng, big, true, null, false).getConditions().getFirst();
    final double rounded = (double) (1L << 53);
    assertThat(FieldFilter.blockMatch(above, rounded, rounded, 10, 10)).isEqualTo(FieldFilter.BlockMatch.SOME);
    assertThat(FieldFilter.blockMatch(above, rounded + 4, rounded + 8, 10, 10)).isEqualTo(FieldFilter.BlockMatch.ALL);
    assertThat(FieldFilter.blockMatch(above, 0, 100, 10, 10)).isEqualTo(FieldFilter.BlockMatch.NONE);
    // within 2^53 a block whose minimum IS the inclusive bound matches whole
    final FieldFilter.Condition atLeastTen = FieldFilter.range(0, lng, 10, true, 20, true).getConditions().getFirst();
    assertThat(FieldFilter.blockMatch(atLeastTen, 10, 20, 10, 10)).isEqualTo(FieldFilter.BlockMatch.ALL);
    assertThat(FieldFilter.blockMatch(atLeastTen, 9, 20, 10, 10)).isEqualTo(FieldFilter.BlockMatch.SOME);

    final ColumnDefinition dbl = new ColumnDefinition("d", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD);
    final FieldFilter.Condition over = FieldFilter.range(0, dbl, 10, false, null, false).getConditions().getFirst();
    assertThat(FieldFilter.blockMatch(over, 0, 10, 5, 5)).isEqualTo(FieldFilter.BlockMatch.NONE);
    assertThat(FieldFilter.blockMatch(over, 11, 20, 5, 5)).isEqualTo(FieldFilter.BlockMatch.ALL);
    // an absent sample in the block fails every condition, so the block cannot be answered as a whole
    assertThat(FieldFilter.blockMatch(over, 11, 20, 4, 5)).isEqualTo(FieldFilter.BlockMatch.SOME);
    assertThat(FieldFilter.blockMatch(over, Double.NaN, Double.NaN, 0, 5)).isEqualTo(FieldFilter.BlockMatch.NONE);
    // a legacy block with no count of its real samples is decoded
    assertThat(FieldFilter.blockMatch(over, 11, 20, -1, 5)).isEqualTo(FieldFilter.BlockMatch.SOME);
  }

  @Test
  void aFieldFilterBuiltForAnotherSchemaIsRefused() {
    loadMonotonic();
    final ColumnDefinition other = new ColumnDefinition("zz", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD);
    final FieldFilter filter = FieldFilter.range(1, other, 1, true, null, false);
    assertThatThrownBy(() -> engine().aggregateMulti(Long.MIN_VALUE, Long.MAX_VALUE, List.of(MultiColumnAggregationRequest.count("c")), 0, 0,
        null, filter, null, 0)).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("zz");
  }
}
