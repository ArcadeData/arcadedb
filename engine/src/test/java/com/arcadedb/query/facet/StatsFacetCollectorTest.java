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
package com.arcadedb.query.facet;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.index.columnar.ColumnDefinition;
import com.arcadedb.index.columnar.ColumnKind;
import com.arcadedb.index.columnar.ColumnReader;
import com.arcadedb.index.columnar.ColumnarSegmentView;
import com.arcadedb.index.columnar.ColumnarSnapshot;
import com.arcadedb.index.columnar.FacetOracle;
import com.arcadedb.index.columnar.GlobalDictionary;
import com.arcadedb.index.columnar.OrdinalConsumer;
import com.arcadedb.index.columnar.PositionBitmap;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.TreeMap;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The STATS collector: count, missing, min, max, sum and average of the matched live rows, as {@code count(p)}, {@code min(p)},
 * {@code max(p)}, {@code sum(p)} and {@code avg(p)} give them (value and Java type). The first tests drive a collector by hand over
 * hand-built segment views; the last ones go through SQL.
 */
class StatsFacetCollectorTest extends TestHelper {
  private static final int    MAX_RECORDS = 128;
  private static final long   WAIT_MS     = 1_000L;
  private static final String PROPERTY    = "p";

  private static ColumnDefinition longColumn(final Type type) {
    return new ColumnDefinition(-1, PROPERTY, type, ColumnKind.LONG, false, null);
  }

  private static ColumnDefinition doubleColumn(final Type type) {
    return new ColumnDefinition(-1, PROPERTY, type, ColumnKind.DOUBLE, false, null);
  }

  private static StatsFacetSpec spec() {
    return new StatsFacetSpec("s", PROPERTY, null);
  }

  private StatsFacetCollector collector(final ColumnDefinition column) {
    return new StatsFacetCollector(spec(), column, 100, (DatabaseInternal) database, WAIT_MS);
  }

  private static PositionBitmap matches(final long... positions) {
    final PositionBitmap matches = new PositionBitmap(MAX_RECORDS);
    for (final long position : positions)
      matches.set(position);
    return matches;
  }

  private static FacetStats stats(final FacetResult result) {
    assertThat(result.kind()).isEqualTo("stats");
    assertThat(result.source()).as(result.reason()).isEqualTo("index");
    assertThat(result.missing()).isEqualTo(result.stats().missing());
    return result.stats();
  }

  /** Asserts value and Java type of every member: an Integer min is not a Long min. */
  private static void assertStats(final FacetStats actual, final FacetStats expected) {
    assertThat(actual.count()).as("count").isEqualTo(expected.count());
    assertThat(actual.missing()).as("missing").isEqualTo(expected.missing());
    assertSameValue("min", actual.min(), expected.min());
    assertSameValue("max", actual.max(), expected.max());
    assertSameValue("sum", actual.sum(), expected.sum());
    assertSameValue("avg", actual.avg(), expected.avg());
  }

  private static void assertSameValue(final String member, final Object actual, final Object expected) {
    assertThat(actual).as(member).isEqualTo(expected);
    if (expected != null)
      assertThat(actual.getClass()).as(member + " type").isEqualTo(expected.getClass());
  }

  private static PatchedValue longValue(final long value) {
    final PatchedValue patched = new PatchedValue();
    patched.longValue(value);
    return patched;
  }

  private static PatchedValue doubleValue(final double value) {
    final PatchedValue patched = new PatchedValue();
    patched.doubleValue(value);
    return patched;
  }

  private static PatchedValue missing() {
    final PatchedValue patched = new PatchedValue();
    patched.missing();
    return patched;
  }

  private static PatchedValue unknown(final Object value) {
    final PatchedValue patched = new PatchedValue();
    patched.unknownValue(value);
    return patched;
  }

  /**
   * A segment view of hand-written rows of one LONG or DOUBLE column: the live rows are those added by {@link #value}/{@link #empty};
   * its bounds are what {@link #bounds} says (the real view's include its dead rows); it counts the values it decoded.
   */
  private static final class Rows implements ColumnarSegmentView, ColumnReader {
    private final ColumnKind          kind;
    private final TreeMap<Long, Long> values = new TreeMap<>();
    private final PositionBitmap      live   = new PositionBitmap(MAX_RECORDS);
    private       long                min    = Long.MAX_VALUE;
    private       long                max    = Long.MIN_VALUE;
    private       int                 decoded;

    Rows(final ColumnKind kind) {
      this.kind = kind;
    }

    /** A live row of a LONG (the value) column. */
    Rows value(final long position, final long value) {
      deadValue(position, value);
      live.set(position);
      return this;
    }

    /** A live row of a DOUBLE column. */
    Rows value(final long position, final double value) {
      return value(position, Double.doubleToLongBits(value));
    }

    /** A row the view still holds but that is dead (deleted, or superseded by an update in a later view). */
    Rows deadValue(final long position, final long value) {
      values.put(position, value);
      if (kind == ColumnKind.LONG) {
        min = Math.min(min, value);
        max = Math.max(max, value);
      } else if (min == Long.MAX_VALUE || Double.longBitsToDouble(value) < Double.longBitsToDouble(min))
        min = value;
      if (kind == ColumnKind.DOUBLE && (max == Long.MIN_VALUE || Double.longBitsToDouble(value) > Double.longBitsToDouble(max)))
        max = value;
      return this;
    }

    /** A live row without a value. */
    Rows empty(final long position) {
      live.set(position);
      return this;
    }

    /** The view's bounds, as {@link ColumnarSegmentView#minLong} encodes them (the bits of the double for a DOUBLE column). */
    Rows bounds(final long min, final long max) {
      this.min = min;
      this.max = max;
      return this;
    }

    @Override
    public PositionBitmap live() {
      return live;
    }

    @Override
    public PositionBitmap live(final int fromPage, final int toPageExclusive) {
      throw new UnsupportedOperationException();
    }

    @Override
    public ColumnReader column(final String property) {
      return PROPERTY.equals(property) ? this : null;
    }

    @Override
    public long minLong(final String property) {
      return min;
    }

    @Override
    public long maxLong(final String property) {
      return max;
    }

    @Override
    public ColumnKind kind() {
      return kind;
    }

    @Override
    public boolean isPresent(final long position) {
      // a point read answers the stored row whatever the dead set says
      return values.containsKey(position);
    }

    @Override
    public int ordinal(final long position) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int ordinals(final long position, final int[] out) {
      throw new UnsupportedOperationException();
    }

    @Override
    public long longValue(final long position) {
      decoded++;
      return values.getOrDefault(position, 0L);
    }

    @Override
    public double doubleValue(final long position) {
      decoded++;
      return Double.longBitsToDouble(values.getOrDefault(position, 0L));
    }

    @Override
    public void forEachOrdinal(final PositionBitmap matches, final OrdinalConsumer consumer) {
      throw new IllegalStateException("not an ordinal column");
    }
  }

  /** A snapshot that only answers its bucket (the collector reads nothing else of it). */
  private record Snapshot(int bucketId) implements ColumnarSnapshot {
    @Override
    public List<ColumnarSegmentView> segments() {
      return List.of();
    }

    @Override
    public PositionBitmap pendingPositions() {
      return new PositionBitmap(MAX_RECORDS);
    }

    @Override
    public long oldestPendingEpochStartMs() {
      return 0;
    }

    @Override
    public boolean isUsable() {
      return true;
    }

    @Override
    public GlobalDictionary dictionary(final String property) {
      return null;
    }

    @Override
    public boolean publishes(final String property) {
      return true;
    }

    @Override
    public ColumnDefinition publishedColumn(final String property) {
      return null;
    }

    @Override
    public void close() {
    }
  }

  // ------------------------------------------------------------------------------------------------------- hand-built inputs

  @Test
  void basicStatsSkipMissingAndNull() {
    final StatsFacetCollector collector = collector(longColumn(Type.INTEGER));
    collector.beginBucket(0, new Snapshot(0), null);
    collector.collect(new Rows(ColumnKind.LONG).value(0, 5L).value(1, -3L).empty(2).value(3, 12L).empty(4), matches(0, 1, 2, 3, 4));
    // a patched record without a value, one with a value
    collector.collectPatched(10, missing());
    collector.collectPatched(11, longValue(7L));
    // 5 - 3 + 12 + 7: an Integer sum of Integer values, a double mean
    assertStats(stats(collector.finish()), new FacetStats(4L, 3L, -3, 12, 21, 21 / 4d));
  }

  @Test
  void integerSumWidensLikeSqlSum() {
    final StatsFacetCollector collector = collector(longColumn(Type.INTEGER));
    collector.beginBucket(0, new Snapshot(0), null);
    collector.collect(new Rows(ColumnKind.LONG).value(0, Integer.MAX_VALUE).value(1, Integer.MAX_VALUE).value(2, 2L), matches(0, 1, 2));
    final FacetStats stats = stats(collector.finish());
    assertSameValue("sum", stats.sum(), 2L * Integer.MAX_VALUE + 2);
    assertSameValue("max", stats.max(), Integer.MAX_VALUE);

    // through SQL: the facet's sum is what sum() of the same records gives, a Long
    database.command("sql", "CREATE DOCUMENT TYPE Wide");
    database.command("sql", "CREATE PROPERTY Wide.n INTEGER");
    database.command("sql", "CREATE INDEX ON Wide (n) COLUMNAR");
    FacetOracle.awaitCaughtUp(database);
    database.transaction(() -> {
      for (final int n : new int[] { Integer.MAX_VALUE, Integer.MAX_VALUE, 5, -1 })
        database.newDocument("Wide").set("n", n).save();
    });
    FacetOracle.awaitCaughtUp(database);
    final FacetStats sql = FacetOracle.stats(database, "Wide", "n", null);
    assertThat(sql.sum()).isInstanceOf(Long.class);
    assertStats(stats(facet("SELECT FROM Wide FACETS s: n STATS")), sql);
  }

  @Test
  void longOverflowBecomesBigDecimal() {
    final StatsFacetCollector collector = collector(longColumn(Type.LONG));
    collector.beginBucket(0, new Snapshot(0), null);
    collector.collect(new Rows(ColumnKind.LONG).value(0, Long.MAX_VALUE).value(1, 1L), matches(0, 1));
    final FacetStats stats = stats(collector.finish());
    assertSameValue("sum", stats.sum(), new BigDecimal("9223372036854775808"));
    // a long sum widened by an overflow: the inputs were no decimals, the mean is a double
    assertSameValue("avg", stats.avg(), new BigDecimal("9223372036854775808").divide(BigDecimal.valueOf(2)).doubleValue());
    assertSameValue("min", stats.min(), 1L);
  }

  @Test
  void emptyMatchGivesCountZeroAndNulls() {
    final StatsFacetCollector collector = collector(longColumn(Type.LONG));
    collector.beginBucket(0, new Snapshot(0), null);
    // no match is live
    collector.collect(new Rows(ColumnKind.LONG).value(0, 1L).deadValue(1, 2L), matches(1, 5));
    assertStats(stats(collector.finish()), new FacetStats(0L, 0L, null, null, null, null));
  }

  @Test
  void dateColumnHasNoSumOrAvg() {
    final long day2020 = LocalDate.of(2020, 1, 1).toEpochDay();
    final StatsFacetCollector days = collector(longColumn(Type.DATE));
    days.beginBucket(0, new Snapshot(0), null);
    days.collect(new Rows(ColumnKind.LONG).value(0, day2020 + 3).value(1, day2020).empty(2), matches(0, 1, 2));
    final FacetStats dates = stats(days.finish());
    // the values a row of a DATE property holds, as GROUP BY returns them
    final DatabaseInternal db = (DatabaseInternal) database;
    assertStats(dates, new FacetStats(2L, 1L, db.getSerializer().deserializeTemporal(db, day2020, Type.DATE.getBinaryType()),
        db.getSerializer().deserializeTemporal(db, day2020 + 3, Type.DATE.getBinaryType()), null, null));

    final StatsFacetCollector instants = collector(longColumn(Type.DATETIME_MICROS));
    instants.beginBucket(0, new Snapshot(0), null);
    final long micros = LocalDateTime.of(2020, 1, 1, 0, 0).toEpochSecond(ZoneOffset.UTC) * 1_000_000L;
    instants.collect(new Rows(ColumnKind.LONG).value(0, micros + 7).value(1, micros + 3), matches(0, 1));
    final FacetStats atMicros = stats(instants.finish());
    assertSameValue("min", atMicros.min(), db.getSerializer().deserializeTemporal(db, micros + 3, Type.DATETIME_MICROS.getBinaryType()));
    assertSameValue("max", atMicros.max(), db.getSerializer().deserializeTemporal(db, micros + 7, Type.DATETIME_MICROS.getBinaryType()));
    assertThat(atMicros.sum()).isNull();
    assertThat(atMicros.avg()).isNull();
  }

  @Test
  void segmentWithNoMatchesIsNotDecoded() {
    final StatsFacetCollector collector = collector(longColumn(Type.LONG));
    collector.beginBucket(0, new Snapshot(0), null);
    final Rows first = new Rows(ColumnKind.LONG).value(0, 4L).value(1, 8L);
    // matched only on dead rows: nothing live to count
    final Rows second = new Rows(ColumnKind.LONG).deadValue(0, 100L).deadValue(1, -100L).value(2, 1L);
    collector.collect(first, matches(0, 1));
    collector.collect(second, matches(0, 1));
    assertThat(second.decoded).isZero();
    assertThat(first.decoded).isEqualTo(2);
    assertStats(stats(collector.finish()), new FacetStats(2L, 0L, 4L, 8L, 12L, 6d));
    assertThat(collector.stats().segmentsSkipped()).isEqualTo(1L);
    assertThat(collector.stats().segmentsScanned()).isEqualTo(1L);
  }

  @Test
  void boundsInsideTheCurrentRangeOnlyFeedTheSum() {
    final StatsFacetCollector collector = collector(longColumn(Type.LONG));
    collector.beginBucket(0, new Snapshot(0), null);
    collector.collect(new Rows(ColumnKind.LONG).value(0, -50L).value(1, 50L), matches(0, 1));
    // bounds inside [-50, 50] (a dead row widens them): min and max stay, count and sum move
    collector.collect(new Rows(ColumnKind.LONG).value(0, 10L).value(1, 20L).deadValue(2, -40L), matches(0, 1, 2));
    // bounds reaching past the current max: the new max is found
    collector.collect(new Rows(ColumnKind.LONG).value(0, 70L).value(1, 0L), matches(0, 1));
    // a view whose column holds no value carries the sentinels [MAX_VALUE, MIN_VALUE]: its rows are missing
    collector.collect(new Rows(ColumnKind.LONG).empty(0).empty(1), matches(0, 1));
    assertStats(stats(collector.finish()), new FacetStats(6L, 2L, -50L, 70L, 100L, 100 / 6d));
  }

  @Test
  void negativeDoublesDoNotSkipByBounds() {
    final StatsFacetCollector collector = collector(doubleColumn(Type.DOUBLE));
    collector.beginBucket(0, new Snapshot(0), null);
    collector.collect(new Rows(ColumnKind.DOUBLE).value(0, -1.0).value(1, 2.0), matches(0, 1));
    // the bits of -5.0 and 1.0 lie inside [bits(-1.0), bits(2.0)] as longs: a skip by bounds would keep -1.0 as the min
    final Rows negative = new Rows(ColumnKind.DOUBLE).value(0, -5.0).value(1, 1.0);
    assertThat(negative.minLong(PROPERTY)).isBetween(Double.doubleToLongBits(-1.0), Double.doubleToLongBits(2.0));
    assertThat(negative.maxLong(PROPERTY)).isBetween(Double.doubleToLongBits(-1.0), Double.doubleToLongBits(2.0));
    collector.collect(negative, matches(0, 1));
    assertStats(stats(collector.finish()), new FacetStats(4L, 0L, -5.0, 2.0, -3.0, -0.75));
  }

  @Test
  void floatColumnKeepsFloatArithmetic() {
    final StatsFacetCollector collector = collector(doubleColumn(Type.FLOAT));
    collector.beginBucket(0, new Snapshot(0), null);
    // the column holds the widened floats
    collector.collect(new Rows(ColumnKind.DOUBLE).value(0, Type.widenFloat(0.1f)).value(1, Type.widenFloat(0.2f)), matches(0, 1));
    collector.collectPatched(5, doubleValue(Type.widenFloat(-0.7f)));
    final float sum = 0.1f + 0.2f + -0.7f;
    assertStats(stats(collector.finish()), new FacetStats(3L, 0L, -0.7f, 0.2f, sum, (double) sum / 3));
  }

  @Test
  void mergeOfTasks() {
    final StatsFacetCollector a = collector(longColumn(Type.LONG));
    a.beginBucket(0, new Snapshot(0), null);
    a.collect(new Rows(ColumnKind.LONG).value(0, 5L).value(1, Long.MAX_VALUE).empty(2), matches(0, 1, 2));
    final StatsFacetCollector b = collector(longColumn(Type.LONG));
    b.beginBucket(1, new Snapshot(1), null);
    b.collect(new Rows(ColumnKind.LONG).value(0, -7L).value(1, 3L), matches(0, 1));
    // a scanned bucket of an INTEGER type: its values arrive through collectPatched
    final StatsFacetCollector scanned = collector(longColumn(Type.INTEGER));
    scanned.beginBucket(2, null, null);
    scanned.collectPatched(0, longValue(9L));
    scanned.collectPatched(1, missing());
    a.merge(b);
    a.merge(scanned);
    // min of the mins, max of the maxes, an exact sum (past Long.MAX_VALUE), counts and missing added
    final BigDecimal sum = BigDecimal.valueOf(Long.MAX_VALUE).add(BigDecimal.valueOf(5 - 7 + 3 + 9));
    final FacetStats merged = stats(a.finish());
    assertStats(merged, new FacetStats(5L, 2L, -7L, Long.MAX_VALUE, sum, sum.divide(BigDecimal.valueOf(5)).doubleValue()));
    assertThat(a.stats().segmentsScanned()).isEqualTo(2L);
  }

  @Test
  void validationOnlyCollectorFinishesEmpty() {
    final FacetCollector collector = FacetExecutor.collectorFor(spec(), null, null, 0);
    assertThat(collector).isInstanceOf(StatsFacetCollector.class);
    collector.release();
    final FacetResult result = collector.finish();
    assertStats(stats(result), new FacetStats(0L, 0L, null, null, null, null));
    assertThat(result.buckets()).isEmpty();
  }

  @Test
  void deletedAndUpdatedRowsAreNotCounted() {
    // the matches are not ANDed with live by the caller: dead rows 1 (deleted) and 2 (superseded by the overlay below) are matched
    final StatsFacetCollector collector = collector(longColumn(Type.LONG));
    collector.beginBucket(0, new Snapshot(0), null);
    collector.collect(new Rows(ColumnKind.LONG).value(0, 10L).deadValue(1, -1_000L).deadValue(2, 1_000L), matches(0, 1, 2));
    collector.collect(new Rows(ColumnKind.LONG).value(2, 13L), matches(0, 1, 2));
    // a dead row is no missing value either
    assertStats(stats(collector.finish()), new FacetStats(2L, 0L, 10L, 13L, 23L, 11.5));
  }

  @Test
  void rawValuesFollowSqlOrAreUnavailable() {
    // a property the bucket's type does not declare: raw values, a DECIMAL summed exactly
    final StatsFacetCollector raw = collector(null);
    raw.beginBucket(3, null, null);
    raw.collectPatched(0, unknown(new BigDecimal("0.10000000000000000001")));
    raw.collectPatched(1, unknown(2));
    raw.collectPatched(2, missing());
    final FacetStats decimals = stats(raw.finish());
    assertThat(decimals.count()).isEqualTo(2L);
    assertSameValue("sum", decimals.sum(), new BigDecimal("2.10000000000000000001"));
    assertSameValue("min", decimals.min(), new BigDecimal("0.10000000000000000001"));

    // a string has no sum and no order with numbers: SQL fails, the facet is unavailable
    final StatsFacetCollector strings = collector(null);
    strings.beginBucket(3, null, null);
    strings.collectPatched(0, unknown("x"));
    final FacetResult refused = strings.finish();
    assertThat(refused.source()).isEqualTo(FacetResult.UNAVAILABLE);
    assertThat(refused.reason()).isEqualTo(StatsFacetCollector.NOT_A_NUMBER_OR_DATE);

    // numbers and dates in one facet: min() cannot compare them
    final StatsFacetCollector numbers = collector(longColumn(Type.LONG));
    numbers.beginBucket(0, new Snapshot(0), null);
    numbers.collectPatched(0, longValue(1L));
    final StatsFacetCollector dates = collector(longColumn(Type.DATE));
    dates.beginBucket(1, new Snapshot(1), null);
    dates.collectPatched(0, longValue(18_000L));
    numbers.merge(dates);
    final FacetResult mixed = numbers.finish();
    assertThat(mixed.source()).isEqualTo(FacetResult.UNAVAILABLE);
    assertThat(mixed.reason()).isEqualTo(StatsFacetCollector.MIXED_KINDS);
  }

  @Test
  void budgetRefusalIsUnavailableAndGivesEverythingBack() {
    final FacetResult refused = withBudget(0, () -> {
      final StatsFacetCollector collector = new StatsFacetCollector(spec(), longColumn(Type.LONG), 10, (DatabaseInternal) database, 10L);
      collector.beginBucket(0, new Snapshot(0), null);
      collector.collect(new Rows(ColumnKind.LONG).value(0, 4L), matches(0));
      collector.collectPatched(3, longValue(5L));
      return collector.finish();
    });
    assertThat(refused.kind()).isEqualTo("stats");
    assertThat(refused.source()).isEqualTo(FacetResult.UNAVAILABLE);
    assertThat(refused.reason()).isEqualTo("memory budget");
    assertThat(refused.stats()).isNull();
  }

  /** Runs {@code counting} under a budget of {@code bytes} for the whole JVM, restored afterwards. */
  private static FacetResult withBudget(final long bytes, final Supplier<FacetResult> counting) {
    final Object previous = GlobalConfiguration.COLUMNAR_QUERY_MEMORY_BUDGET.getValue();
    GlobalConfiguration.COLUMNAR_QUERY_MEMORY_BUDGET.setValue(bytes);
    try {
      final long before = FacetMemoryBudget.instance().available();
      final FacetResult result = counting.get();
      assertThat(FacetMemoryBudget.instance().available()).as("everything given back").isEqualTo(before);
      return result;
    } finally {
      GlobalConfiguration.COLUMNAR_QUERY_MEMORY_BUDGET.setValue(previous);
      FacetMemoryBudget.instance();
    }
  }

  // ------------------------------------------------------------------------------------------------------- through SQL

  private FacetResult facet(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      return rs.getFacets().get("s");
    }
  }

  @Test
  @Timeout(120)
  void deletedAndUpdatedRowsAreNotCountedThroughSql() {
    database.command("sql", "CREATE DOCUMENT TYPE Sized");
    database.command("sql", "CREATE PROPERTY Sized.size LONG");
    database.command("sql", "CREATE INDEX ON Sized (size) COLUMNAR");
    FacetOracle.awaitCaughtUp(database);
    database.transaction(() -> {
      for (int i = 0; i < 300; i++)
        database.newDocument("Sized").set("size", (long) i - 100).save();
    });
    FacetOracle.awaitCaughtUp(database);
    // the extremes are deleted or moved: a dead row counted would show in min, max and sum
    database.transaction(() -> {
      database.command("sql", "DELETE FROM Sized WHERE size < -90");
      database.command("sql", "UPDATE Sized SET size = size + 1000 WHERE size > 190");
      database.command("sql", "UPDATE Sized SET size = null WHERE size = 0");
    });
    FacetOracle.awaitCaughtUp(database);
    final FacetResult result = facet("SELECT FROM Sized FACETS s: size STATS");
    assertThat(result.source()).isEqualTo("index");
    assertStats(stats(result), FacetOracle.stats(database, "Sized", "size", null));
    assertThat(result.stats().missing()).isEqualTo(1L);
  }
}
