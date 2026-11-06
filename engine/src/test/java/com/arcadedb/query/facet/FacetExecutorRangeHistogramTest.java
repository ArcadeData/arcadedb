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
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.RID;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.index.columnar.ColumnarIndexManager;
import com.arcadedb.index.columnar.ColumnarTestData;
import com.arcadedb.index.columnar.FacetOracle;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * RANGES and HISTOGRAM facets (D17, D18, D19): equal to the GROUP BY of the same bucketing, over the projection and while it lags.
 * Every histogram test draws {@code publishDate} with a time of day, so values straddle the zone boundaries.
 */
class FacetExecutorRangeHistogramTest extends TestHelper {
  private static final String       TYPE   = ColumnarTestData.TYPE;
  private static final long         MIB    = 1_048_576L;
  private static final List<String> WHERES = Arrays.asList(null, "fileSize < 1048576", "author = 'author-7'", "tags CONTAINS 'tag-5'");
  private static final String       CASE   = "(CASE WHEN fileSize < 1048576 THEN 'small' WHEN fileSize < 2097152 THEN 'medium' ELSE 'large' END)";

  private ColumnarTestData data(final int buckets) {
    final ColumnarTestData data = new ColumnarTestData(11);
    data.tagVocabulary = 300;
    data.timeOfDay = true;
    data.buckets = buckets;
    return data;
  }

  private ColumnarIndexManager manager() {
    return ((LocalDatabase) ((DatabaseInternal) database).getEmbedded()).getColumnarIndexManager();
  }

  private MatchSet matches(final String type, final String where, final int facetCount) {
    final MatchSet.Builder builder = MatchSet.builder((DatabaseInternal) database, 0, 0);
    try (builder; final ResultSet rs = database.query("sql", "SELECT @rid AS rid FROM " + type + (where == null ? "" : " WHERE " + where))) {
      while (rs.hasNext())
        assertThat(builder.add(rs.next().<Identifiable>getProperty("rid").getIdentity(), 0)).isTrue();
      final MatchSet matches = builder.build();
      final int[] noFilter = new int[facetCount];
      Arrays.fill(noFilter, -1);
      matches.setFacetFilterIndex(noFilter);
      return matches;
    }
  }

  private FacetResults execute(final String where, final FacetSpec... facets) {
    return executeOver(TYPE, where, facets);
  }

  private FacetResults executeOver(final String type, final String where, final FacetSpec... facets) {
    try (final MatchSet matches = matches(type, where, facets.length)) {
      assertThat(matches.size()).as("records of " + type + " matching " + where).isPositive();
      return FacetExecutor.execute((DatabaseInternal) database, matches, List.of(facets), new BasicCommandContext());
    }
  }

  private static RangeFacetSpec ranges(final String alias, final String property, final String defaultLabel, final RangeBound... ranges) {
    return new RangeFacetSpec(alias, property, null, null, List.of(ranges), defaultLabel);
  }

  private static RangeFacetSpec sizeRanges() {
    return ranges("size", "fileSize", null, new RangeBound(null, MIB, "small"), new RangeBound(MIB, 2 * MIB, "medium"),
        new RangeBound(2 * MIB, null, "large"));
  }

  private static HistogramFacetSpec histogram(final String alias, final String property, final boolean calendar, final Object interval,
      final String timezone, final int minCount, final Object boundsFrom, final Object boundsTo) {
    return histogram(alias, property, calendar, interval, timezone, null, minCount, boundsFrom, boundsTo);
  }

  private static HistogramFacetSpec histogram(final String alias, final String property, final boolean calendar, final Object interval,
      final String timezone, final String offset, final int minCount, final Object boundsFrom, final Object boundsTo) {
    return new HistogramFacetSpec(alias, property, null, null, FacetSpecCompiler.parseInterval(calendar, interval), timezone, offset, minCount,
        boundsFrom, boundsTo);
  }

  /** The bucket counts of a histogram by {@code from}. */
  private static Map<Object, Long> byFrom(final FacetResult histogram) {
    final Map<Object, Long> counts = new TreeMap<>();
    for (final FacetBucket bucket : histogram.buckets())
      assertThat(counts.put(bucket.from(), bucket.count())).as("from " + bucket.from() + " returned twice").isNull();
    return counts;
  }

  /** The values of a property of every record of the type (nulls left out). */
  private List<Object> values(final String type, final String property) {
    final List<Object> values = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT " + property + " AS v FROM " + type)) {
      while (rs.hasNext()) {
        final Object value = rs.next().getProperty("v");
        if (value != null)
          values.add(value);
      }
    }
    return values;
  }

  /** {@code SELECT <key> AS k, count(*) AS c ... GROUP BY k}: the counts by key, null keys left out. */
  private Map<Object, Long> groupBy(final String key, final String type, final String where, final Map<String, Object> parameters) {
    final Map<Object, Long> counts = new LinkedHashMap<>();
    final String sql = "SELECT " + key + " AS k, count(*) AS c FROM " + type + (where == null ? "" : " WHERE " + where) + " GROUP BY k";
    try (final ResultSet rs = database.query("sql", sql, parameters)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        if (row.getProperty("k") != null)
          counts.put(row.getProperty("k"), row.<Number>getProperty("c").longValue());
      }
    }
    return counts;
  }

  private long count(final String type, final String where) {
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM " + type + (where == null ? "" : " WHERE " + where))) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }

  private static String and(final String where, final String condition) {
    return where == null ? condition : "(" + where + ") AND " + condition;
  }

  private static Map<String, Long> byKey(final FacetResult result) {
    final Map<String, Long> counts = new LinkedHashMap<>();
    for (final FacetBucket bucket : result.buckets())
      assertThat(counts.put(bucket.key(), bucket.count())).as("key " + bucket.key() + " returned twice").isNull();
    return counts;
  }

  private static List<String> keys(final FacetResult result) {
    final List<String> keys = new ArrayList<>();
    for (final FacetBucket bucket : result.buckets())
      keys.add(bucket.key());
    return keys;
  }

  private void insertWithoutFileSize(final int times) {
    database.transaction(() -> {
      for (int i = 0; i < times; i++)
        database.newDocument(TYPE).set("brand", "brand-1").save();
    });
  }

  // ------------------------------------------------------------------------------------------------------------------ ranges

  @Test
  @Timeout(300)
  void rangesEqualGroupByOfCaseExpression() {
    final ColumnarTestData data = data(2);
    data.createSchema(database);
    data.insertAssets(database, 3_000);
    FacetOracle.awaitCaughtUp(database);

    for (final String where : WHERES) {
      final FacetResult size = execute(where, sizeRanges()).get("size");
      assertThat(size.kind()).isEqualTo("range");
      assertThat(size.source()).isEqualTo("index");
      assertThat(keys(size)).as("declaration order").containsExactly("small", "medium", "large");
      final Map<Object, Long> expected = groupBy(CASE, TYPE, and(where, "fileSize IS NOT NULL"), Map.of());
      assertThat(expected).as("WHERE " + where + " matches records").isNotEmpty();
      final Map<String, Long> counts = byKey(size);
      for (final String key : List.of("small", "medium", "large"))
        assertThat(counts.get(key)).as(key + " WHERE " + where).isEqualTo(expected.getOrDefault(key, 0L));
      assertThat(size.missing()).isEqualTo(count(TYPE, and(where, "fileSize IS NULL")));
      assertThat(size.buckets().getFirst().from()).isNull();
      assertThat(size.buckets().getFirst().to()).isEqualTo(MIB);
    }
  }

  @Test
  @Timeout(300)
  void overlappingRangesCountTwice() {
    final ColumnarTestData data = data(1);
    data.createSchema(database);
    data.insertAssets(database, 2_000);
    FacetOracle.awaitCaughtUp(database);

    final FacetResult result = execute(null,
        ranges("size", "fileSize", null, new RangeBound(0L, 2 * MIB, "low"), new RangeBound(MIB, null, "high"))).get("size");
    final long low = count(TYPE, "fileSize >= 0 AND fileSize < 2097152");
    final long high = count(TYPE, "fileSize >= 1048576");
    assertThat(byKey(result)).isEqualTo(Map.of("low", low, "high", high));
    assertThat(low + high).as("the overlap [1 MiB, 2 MiB) is counted in both").isGreaterThan(count(TYPE, null));
  }

  @Test
  @Timeout(300)
  void defaultBucketAndMissing() {
    final ColumnarTestData data = data(1);
    data.createSchema(database);
    data.insertAssets(database, 2_000);
    insertWithoutFileSize(7);
    FacetOracle.awaitCaughtUp(database);

    final FacetResult result = execute(null, ranges("size", "fileSize", "other", new RangeBound(0L, MIB, "small"))).get("size");
    assertThat(keys(result)).containsExactly("small", "other");
    assertThat(result.buckets().get(1).isDefault()).isTrue();
    assertThat(result.buckets().get(0).count()).isEqualTo(count(TYPE, "fileSize < 1048576"));
    assertThat(result.buckets().get(1).count()).as("present values in no range").isEqualTo(count(TYPE, "fileSize >= 1048576")).isPositive();
    assertThat(result.missing()).as("records without a value").isEqualTo(count(TYPE, "fileSize IS NULL")).isGreaterThanOrEqualTo(7);
  }

  @Test
  @Timeout(300)
  void segmentSkippedWhenInsideOneInterval() {
    final ColumnarTestData data = data(1);
    data.createSchema(database);
    data.insertAssets(database, 2_000);
    FacetOracle.awaitCaughtUp(database);
    final long present = count(TYPE, "fileSize IS NOT NULL");

    // every value is in [0, *): each view lies in one interval and is counted by presence alone
    final FacetResults one = execute(null, ranges("size", "fileSize", null, new RangeBound(0L, null, "all")));
    final FacetExecutionStats skipped = one.executionStats().get("size");
    assertThat(skipped.segmentsSkipped()).isPositive();
    assertThat(skipped.segmentsScanned()).isZero();
    assertThat(one.get("size").buckets().getFirst().count()).isEqualTo(present);

    // a bound inside the values: the same views are decoded, the overlapping range counts the same
    final FacetResults split = execute(null,
        ranges("size", "fileSize", null, new RangeBound(0L, null, "all"), new RangeBound(0L, MIB, "small")));
    assertThat(split.executionStats().get("size").segmentsScanned()).isPositive();
    assertThat(byKey(split.get("size"))).isEqualTo(Map.of("all", present, "small", count(TYPE, "fileSize < 1048576")));
  }

  @Test
  @Timeout(300)
  void emptyRangeAfterConversionIsRefused() {
    final ColumnarTestData data = data(1);
    data.createSchema(database);
    data.insertAssets(database, 100);
    FacetOracle.awaitCaughtUp(database);

    // [1.2, 1.5) holds no integer: both ends round up to 2
    assertThatThrownBy(() -> execute(null, ranges("size", "fileSize", null, new RangeBound(1.2d, 1.5d, "none"))))
        .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("FACETS size: empty range [1.2, 1.5)");
    // a fractional bound keeps its meaning on an integral column: [v - 0.5, v + 0.5) is [v, v + 1)
    final long value;
    try (final ResultSet rs = database.query("sql", "SELECT fileSize FROM " + TYPE + " WHERE fileSize > 0 LIMIT 1")) {
      value = rs.next().<Number>getProperty("fileSize").longValue();
    }
    final FacetResult result = execute(null, ranges("size", "fileSize", null, new RangeBound(value - 0.5d, value + 0.5d, "v"))).get("size");
    assertThat(result.buckets().getFirst().count()).isEqualTo(count(TYPE, "fileSize = " + value)).isPositive();
  }

  @Test
  @Timeout(300)
  void rangesWhileLaggingStayExact() throws Exception {
    final ColumnarTestData data = data(2);
    data.createSchema(database);
    final List<RID> rids = data.insertAssets(database, 2_000);
    FacetOracle.awaitCaughtUp(database);

    try (final AutoCloseable hold = manager().suspendApply()) {
      database.transaction(() -> {
        for (int i = 0; i < 120; i++)
          database.lookupByRID(rids.get(i * 13), true).asDocument().modify().set("fileSize", (i % 3) * MIB + 512L).save();
      });
      data.mutate(database, 300);
      for (final String where : WHERES) {
        final FacetResults results = execute(where, sizeRanges(),
            histogram("pub", "publishDate", true, "1M", "Europe/Rome", 1, null, null));
        assertThat(results.exact()).isTrue();
        assertThat(results.lagMs()).isPositive();
        final Map<Object, Long> expected = groupBy(CASE, TYPE, and(where, "fileSize IS NOT NULL"), Map.of());
        final Map<String, Long> counts = byKey(results.get("size"));
        for (final String key : List.of("small", "medium", "large"))
          assertThat(counts.get(key)).as(key + " WHERE " + where).isEqualTo(expected.getOrDefault(key, 0L));
        assertThat(monthCounts(results.get("pub"))).as("months WHERE " + where).isEqualTo(romeMonths(where));
        if (where == null)
          assertThat(results.patched()).isPositive();
      }
    }
  }

  @Test
  @Timeout(120)
  void siblingTypesWithDifferentPropertyTypesMergeOnConvertedValues() {
    database.command("sql", "CREATE DOCUMENT TYPE Media");
    for (final String[] sibling : new String[][] { { "Clip", "DATE" }, { "Shot", "DATETIME" } }) {
      database.command("sql", "CREATE DOCUMENT TYPE " + sibling[0] + " EXTENDS Media");
      database.command("sql", "CREATE PROPERTY " + sibling[0] + ".ts " + sibling[1]);
      database.command("sql", "CREATE INDEX ON " + sibling[0] + " (ts) COLUMNAR");
    }
    FacetOracle.awaitCaughtUp(database);
    final LocalDate first = LocalDate.of(2020, 1, 1);
    final TreeMap<Long, Long> perDay = new TreeMap<>();
    database.transaction(() -> {
      for (int i = 0; i < 6; i++) {
        final LocalDate day = first.plusDays(i % 3);
        database.newDocument("Clip").set("ts", day).save();
        perDay.merge(day.toEpochDay() * 86_400_000L, 1L, Long::sum);
      }
      for (int i = 0; i < 5; i++) {
        final long ms = first.toEpochDay() * 86_400_000L + i * 13 * 3_600_000L;
        database.newDocument("Shot").set("ts", new Date(ms)).save();
        perDay.merge(Math.floorDiv(ms, 86_400_000L) * 86_400_000L, 1L, Long::sum);
      }
    });
    FacetOracle.awaitCaughtUp(database);

    final FacetResults results = executeOver("Media", null,histogram("day", "ts", true, "1d", null, 1, null, null),
        ranges("from2", "ts", null, new RangeBound("2020-01-02", null, "from2")));
    final TreeMap<Long, Long> counted = new TreeMap<>();
    for (final FacetBucket bucket : results.get("day").buckets())
      counted.put((Long) bucket.from(), bucket.count());
    assertThat(counted).as("a DATE and a DATETIME of one day share its bucket").isEqualTo(perDay);
    final long fromSecondDay = perDay.tailMap(LocalDate.of(2020, 1, 2).toEpochDay() * 86_400_000L).values().stream().mapToLong(Long::longValue)
        .sum();
    assertThat(results.get("from2").buckets().getFirst().count()).isEqualTo(fromSecondDay).isPositive();
  }

  // --------------------------------------------------------------------------------------------------------------- histograms

  /** The month counts of a histogram as {@code yyyy-MM} of its keys. */
  private static Map<Object, Long> monthCounts(final FacetResult histogram) {
    final Map<Object, Long> counts = new LinkedHashMap<>();
    for (final FacetBucket bucket : histogram.buckets())
      if (bucket.count() > 0)
        counts.put(bucket.key().substring(0, 7), bucket.count());
    return counts;
  }

  /**
   * GROUP BY the Rome month. Formatted from the epoch milliseconds: a DATETIME reads back as a UTC wall-clock {@code LocalDateTime},
   * which {@code format(pattern, zone)} renders as if it were already the zone's local time (no conversion).
   */
  private Map<Object, Long> romeMonths(final String where) {
    return groupBy("publishDate.asLong().asDate().format('yyyy-MM', 'Europe/Rome')",TYPE, and(where, "publishDate IS NOT NULL"), Map.of());
  }

  @Test
  @Timeout(300)
  void monthHistogramEqualsZonedGroupBy() {
    final ColumnarTestData data = data(2);
    data.createSchema(database);
    data.insertAssets(database, 3_000);
    FacetOracle.awaitCaughtUp(database);
    final ZoneId rome = ZoneId.of("Europe/Rome");

    for (final String where : WHERES) {
      final FacetResult pub = execute(where, histogram("pub", "publishDate", true, "1M", "Europe/Rome", 1, null, null)).get("pub");
      assertThat(pub.kind()).isEqualTo("histogram");
      assertThat(pub.source()).isEqualTo("index");
      assertThat(monthCounts(pub)).as("WHERE " + where).isEqualTo(romeMonths(where));
      assertThat(pub.missing()).isEqualTo(count(TYPE, and(where, "publishDate IS NULL")));
      for (final FacetBucket bucket : pub.buckets()) {
        final ZonedDateTime from = Instant.ofEpochMilli((Long) bucket.from()).atZone(rome);
        assertThat(from.getDayOfMonth()).isEqualTo(1);
        assertThat(from.toLocalTime().toSecondOfDay()).as("local midnight").isZero();
        assertThat(Instant.ofEpochMilli((Long) bucket.to()).atZone(rome)).isEqualTo(from.plusMonths(1));
        assertThat(bucket.key()).isEqualTo(OffsetDateTime.ofInstant(from.toInstant(), rome).toString());
      }
    }
  }

  @Test
  @Timeout(300)
  void minCountZeroFillsGaps() {
    final ColumnarTestData data = data(1);
    data.createSchema(database);
    data.insertAssets(database, 2_000);
    FacetOracle.awaitCaughtUp(database);
    final String where = "author = 'author-7'";

    final FacetResult filled = execute(where, histogram("pub", "publishDate", true, "1M", null, 0, "2019-07-01", "2025-06-30")).get("pub");
    // the extended bounds give every month from 2019-07 to 2025-06, the empty ones at count 0
    assertThat(filled.buckets()).hasSize(72);
    assertThat(filled.buckets().getFirst().key()).isEqualTo("2019-07-01T00:00Z");
    assertThat(filled.buckets().getLast().key()).isEqualTo("2025-06-01T00:00Z");
    for (int i = 1; i < filled.buckets().size(); i++)
      assertThat(filled.buckets().get(i).from()).as("contiguous").isEqualTo(filled.buckets().get(i - 1).to());
    assertThat(filled.buckets().getFirst().count()).isZero();
    final long total = filled.buckets().stream().mapToLong(FacetBucket::count).sum();
    assertThat(total).isEqualTo(count(TYPE, and(where, "publishDate IS NOT NULL")));

    // MIN_COUNT 1 drops exactly the empty months
    final FacetResult dense = execute(where, histogram("pub", "publishDate", true, "1M", null, 1, "2019-07-01", "2025-06-30")).get("pub");
    assertThat(keys(dense)).isEqualTo(filled.buckets().stream().filter(b -> b.count() > 0).map(FacetBucket::key).toList());
    assertThat(monthCounts(dense)).isEqualTo(groupBy("publishDate.format('yyyy-MM', 'UTC')", TYPE, and(where, "publishDate IS NOT NULL"),
        Map.of()));
  }

  @Test
  @Timeout(300)
  void fixedHistogramAlignsWithTimeBucket() {
    final ColumnarTestData data = data(1);
    data.createSchema(database);
    data.insertAssets(database, 2_000);
    FacetOracle.awaitCaughtUp(database);
    final ZoneOffset plus8 = ZoneOffset.ofHours(8);

    for (final String where : WHERES) {
      final FacetResult hours = execute(where, histogram("h", "publishDate", false, "1h", "+08:00", 1, null, null)).get("h");
      final Map<Object, Long> expected = new TreeMap<>();
      groupBy("ts.timeBucket('1h', publishDate, :options)", TYPE, and(where, "publishDate IS NOT NULL"),
          Map.of("options", Map.of("timezone", "+08:00"))).forEach(
          (bucket, count) -> expected.put(((LocalDateTime) bucket).toInstant(ZoneOffset.UTC).toEpochMilli(), count));
      final Map<Object, Long> counted = new TreeMap<>();
      for (final FacetBucket bucket : hours.buckets()) {
        counted.put(bucket.from(), bucket.count());
        assertThat((Long) bucket.to() - (Long) bucket.from()).isEqualTo(3_600_000L);
        assertThat(bucket.key()).isEqualTo(OffsetDateTime.ofInstant(Instant.ofEpochMilli((Long) bucket.from()), plus8).toString());
      }
      assertThat(counted).as("WHERE " + where).isEqualTo(expected);
    }
    // a day histogram in +08:00 starts at the local midnight, as ts.timeBucket('1d', ..., {timezone}) does
    final FacetResult days = execute(null, histogram("d", "publishDate", false, "1d", "+08:00", 1, null, null)).get("d");
    assertThat(Instant.ofEpochMilli((Long) days.buckets().getFirst().from()).atOffset(plus8).toLocalTime().toSecondOfDay()).isZero();
  }

  @Test
  @Timeout(120)
  void datePrecisionMicros() {
    for (final String[] type : new String[][] { { "MicroEvent", "DATETIME_MICROS" }, { "MilliEvent", "DATETIME" } }) {
      database.command("sql", "CREATE DOCUMENT TYPE " + type[0]);
      database.command("sql", "CREATE PROPERTY " + type[0] + ".ts " + type[1]);
      database.command("sql", "CREATE INDEX ON " + type[0] + " (ts) COLUMNAR");
    }
    FacetOracle.awaitCaughtUp(database);
    final Random random = new Random(5);
    final ZoneId rome = ZoneId.of("Europe/Rome");
    final List<LocalDateTime> values = new ArrayList<>();
    for (int i = 0; i < 400; i++)
      values.add(LocalDateTime.of(2020, 1, 1, 0, 0).plusSeconds(random.nextInt(2 * 365 * 86_400)).plusNanos(random.nextInt(1_000_000) * 1_000L));
    // half a millisecond before a Rome midnight: the microsecond value rounds down into the day before, as the millisecond one
    values.add(LocalDate.of(2020, 6, 15).atStartOfDay(rome).toLocalDateTime().atZone(rome).withZoneSameInstant(ZoneOffset.UTC)
        .toLocalDateTime().minusNanos(500_000));
    database.transaction(() -> {
      for (final LocalDateTime value : values) {
        database.newDocument("MicroEvent").set("ts", value).save();
        database.newDocument("MilliEvent").set("ts", value).save();
      }
    });
    FacetOracle.awaitCaughtUp(database);

    final List<FacetSpec> specs = List.of(histogram("day", "ts", true, "1d", "Europe/Rome", 1, null, null),
        histogram("hour", "ts", false, "1h", null, 1, "2019-12-31", "2022-01-02"),
        ranges("r", "ts", null, new RangeBound("2020-06-01", "2021-01-01", "second-half"), new RangeBound(null, "2020-06-15", "early")));
    final FacetResults micro = executeOver("MicroEvent", null, specs.toArray(new FacetSpec[0]));
    final FacetResults milli = executeOver("MilliEvent", null, specs.toArray(new FacetSpec[0]));
    for (final String alias : List.of("day", "hour", "r")) {
      final List<FacetBucket> a = micro.get(alias).buckets();
      final List<FacetBucket> b = milli.get(alias).buckets();
      assertThat(a).as(alias).hasSameSizeAs(b);
      for (int i = 0; i < a.size(); i++) {
        assertThat(a.get(i).key()).as(alias).isEqualTo(b.get(i).key());
        assertThat(a.get(i).from()).as(alias).isEqualTo(b.get(i).from());
        assertThat(a.get(i).to()).as(alias).isEqualTo(b.get(i).to());
        assertThat(a.get(i).count()).as(alias + " " + a.get(i).key()).isEqualTo(b.get(i).count());
      }
    }
    final long secondHalf = values.stream().filter(v -> !v.isBefore(LocalDateTime.of(2020, 6, 1, 0, 0)) && v.isBefore(LocalDateTime.of(2021, 1, 1, 0, 0)))
        .count();
    assertThat(byKey(micro.get("r")).get("second-half")).isEqualTo(secondHalf).isPositive();
    assertThat(micro.get("day").buckets().stream().mapToLong(FacetBucket::count).sum()).isEqualTo(values.size());
    // the day buckets against the Rome calendar date of each value (a LocalDateTime is a UTC wall clock)
    final Map<String, Long> romeDays = new TreeMap<>();
    for (final LocalDateTime value : values)
      romeDays.merge(value.atZone(ZoneOffset.UTC).withZoneSameInstant(rome).toLocalDate().toString(), 1L, Long::sum);
    final Map<String, Long> dayBuckets = new TreeMap<>();
    for (final FacetBucket bucket : micro.get("day").buckets())
      dayBuckets.put(bucket.key().substring(0, 10), bucket.count());
    assertThat(dayBuckets).isEqualTo(romeDays);
  }

  @Test
  @Timeout(300)
  void dataDerivedBoundsTakeAPreciseFirstPass() {
    final ColumnarTestData data = data(1);
    data.createSchema(database);
    data.insertAssets(database, 2_000);
    FacetOracle.awaitCaughtUp(database);

    // one bucket per byte: the views span 0 to ~4 MiB, the matched values 0 to 63 KiB only
    final String where = "fileSize < 65536";
    final FacetResult bytes = execute(where, histogram("b", "fileSize", false, 1, null, 1, null, null)).get("b");
    final Map<Object, Long> counted = new TreeMap<>();
    for (final FacetBucket bucket : bytes.buckets()) {
      assertThat(bucket.key()).isEqualTo(bucket.from().toString());
      assertThat((Long) bucket.to()).isEqualTo((Long) bucket.from() + 1);
      counted.put(bucket.from(), bucket.count());
    }
    assertThat(counted).isEqualTo(new TreeMap<>(groupBy("fileSize", TYPE, where, Map.of())));

    // over every record the matched values themselves span more than 100,000 buckets
    assertThatThrownBy(() -> execute(null, histogram("b", "fileSize", false, 1, null, 1, null, null))).isInstanceOf(
        CommandExecutionException.class).hasMessageContaining("HISTOGRAM b would produce more than 100000 buckets");
  }

  @Test
  @Timeout(300)
  void histogramCountsAreChargedToTheBudget() {
    final ColumnarTestData data = data(1);
    data.createSchema(database);
    data.insertAssets(database, 2_000);
    FacetOracle.awaitCaughtUp(database);

    final long saved = GlobalConfiguration.COLUMNAR_QUERY_MEMORY_BUDGET.getValueAsLong();
    final long savedWait = GlobalConfiguration.COLUMNAR_QUERY_BUDGET_WAIT.getValueAsLong();
    final long headroom = 64 * 1024;
    try (final MatchSet matches = matches(TYPE, null, 1)) {
      // ~43,800 hourly buckets over five years: 350 KB of counters, more than the headroom
      final long held = saved - FacetMemoryBudget.instance().available();
      GlobalConfiguration.COLUMNAR_QUERY_MEMORY_BUDGET.setValue(held + headroom);
      GlobalConfiguration.COLUMNAR_QUERY_BUDGET_WAIT.setValue(0);
      final List<FacetSpec> hours = List.of(histogram("h", "publishDate", false, "1h", null, 1, null, null));
      final FacetResult refused = FacetExecutor.execute((DatabaseInternal) database, matches, hours, new BasicCommandContext()).get("h");
      assertThat(refused.source()).isEqualTo(FacetResult.UNAVAILABLE);
      assertThat(refused.reason()).isEqualTo("memory budget");
      assertThat(FacetMemoryBudget.instance().available()).as("everything the facet reserved is released").isEqualTo(headroom);

      GlobalConfiguration.COLUMNAR_QUERY_MEMORY_BUDGET.setValue(saved);
      final FacetResults counted = FacetExecutor.execute((DatabaseInternal) database, matches, hours, new BasicCommandContext());
      assertThat(counted.get("h").source()).isEqualTo("index");
      assertThat(counted.executionStats().get("h").budgetBytes()).isGreaterThan(headroom);
      assertThat(FacetMemoryBudget.instance().available()).isEqualTo(saved - held);
    } finally {
      GlobalConfiguration.COLUMNAR_QUERY_MEMORY_BUDGET.setValue(saved);
      GlobalConfiguration.COLUMNAR_QUERY_BUDGET_WAIT.setValue(savedWait);
    }
  }

  // ------------------------------------------------------------------------------------------------------------- fix round 1

  @Test
  @Timeout(120)
  void rowsWithoutAValueAreMissingEvenWhenNoRowHasOne() {
    database.command("sql", "CREATE DOCUMENT TYPE P");
    database.command("sql", "CREATE PROPERTY P.v LONG");
    database.command("sql", "CREATE INDEX ON P (v) COLUMNAR");
    FacetOracle.awaitCaughtUp(database);
    database.transaction(() -> {
      for (int i = 0; i < 10; i++)
        database.newDocument("P").set("name", "p" + i).save();
    });
    FacetOracle.awaitCaughtUp(database);

    final FacetResults results = executeOver("P", null, histogram("h", "v", false, 10, null, 1, null, null),
        ranges("r", "v", null, new RangeBound(0L, null, "all")));
    final FacetResult histogram = results.get("h");
    assertThat(histogram.source()).isEqualTo("index");
    assertThat(histogram.buckets()).isEmpty();
    assertThat(histogram.missing()).as("as GROUP BY's null group").isEqualTo(10);
    assertThat(results.executionStats().get("h").source()).isEqualTo("index");
    assertThat(results.get("r").missing()).isEqualTo(10);
  }

  @Test
  @Timeout(300)
  void boundsFillEveryBucketWhenNothingIsCounted() {
    final ColumnarTestData data = data(1);
    data.createSchema(database);
    final List<RID> rids = data.insertAssets(database, 300);
    FacetOracle.awaitCaughtUp(database);
    final HistogramFacetSpec months = histogram("pub", "publishDate", true, "1M", null, 0, "2020-01-01", "2020-12-31");

    // a WHERE that matches nothing: the match set holds no bucket, so no column converts the bounds
    try (final MatchSet.Builder builder = MatchSet.builder((DatabaseInternal) database, 0, 0); final MatchSet empty = builder.build()) {
      empty.setFacetFilterIndex(new int[] { -1 });
      assertThat(empty.size()).isZero();
      assertTwelveEmptyMonths(FacetExecutor.execute((DatabaseInternal) database, empty, List.of(months), new BasicCommandContext()).get("pub"));
    }

    // another facet's FILTER excludes every record: this facet counts nothing, over a bucket that has a column
    final MatchSet.Builder builder = MatchSet.builder((DatabaseInternal) database, 1, 0);
    try (builder) {
      for (final RID rid : rids)
        assertThat(builder.add(rid, 1)).isTrue();
      try (final MatchSet filtered = builder.build()) {
        filtered.setFacetFilterIndex(new int[] { -1, 0 });
        final FacetResults results = FacetExecutor.execute((DatabaseInternal) database, filtered,
            List.of(months, new TermsFacetSpec("brand", "brand", null, null, 10, 1, FacetOrder.COUNT_DESC, null, false, null)),
            new BasicCommandContext());
        assertTwelveEmptyMonths(results.get("pub"));
        assertThat(results.get("brand").buckets()).as("the filtered facet still counts its own failures").isNotEmpty();
      }
    }
  }

  @Test
  @Timeout(120)
  void emptyMatchSetConvertsBoundsAsTheColumnDoes() {
    database.command("sql", "CREATE DOCUMENT TYPE Ev");
    database.command("sql", "CREATE PROPERTY Ev.day DATE");
    database.command("sql", "CREATE PROPERTY Ev.ts DATETIME_MICROS");
    database.command("sql", "CREATE INDEX ON Ev (day) COLUMNAR");
    database.command("sql", "CREATE INDEX ON Ev (ts) COLUMNAR");
    FacetOracle.awaitCaughtUp(database);
    // schema formats that are not ISO: the column path reads BOUNDS with them
    database.getSchema().setDateFormat("dd/MM/yyyy");
    database.getSchema().setDateTimeFormat("dd/MM/yyyy HH:mm:ss");
    database.transaction(() -> {
      for (int i = 0; i < 3; i++)
        database.newDocument("Ev").set("day", LocalDate.of(2021, 3, 4 + i)).set("ts", LocalDateTime.of(2021, 3, 4, 10, i)).save();
    });
    FacetOracle.awaitCaughtUp(database);

    // a number on a DATE property is a day count (2020-01-01 and 2024-12-31), on a DATETIME_MICROS epoch milliseconds
    final List<FacetSpec> specs = List.of(histogram("dayNumber", "day", true, "1y", null, 0, 18_262L, 20_088L),
        histogram("dayString", "day", true, "1y", null, 0, "01/01/2020", "31/12/2024"),
        histogram("tsNumber", "ts", true, "1y", null, 0, 1_577_836_800_000L, 1_735_603_200_000L),
        histogram("tsString", "ts", true, "1y", null, 0, "01/01/2020 00:00:00", "31/12/2024 00:00:00"));
    final FacetResults counted = executeOver("Ev", null, specs.toArray(new FacetSpec[0]));
    final FacetResults none;
    try (final MatchSet.Builder builder = MatchSet.builder((DatabaseInternal) database, 0, 0); final MatchSet empty = builder.build()) {
      final int[] noFilter = new int[specs.size()];
      Arrays.fill(noFilter, -1);
      empty.setFacetFilterIndex(noFilter);
      none = FacetExecutor.execute((DatabaseInternal) database, empty, specs, new BasicCommandContext(), database.getSchema().getType("Ev"));
    }
    for (final FacetSpec spec : specs) {
      final List<FacetBucket> withRows = counted.get(spec.alias()).buckets();
      final List<FacetBucket> withoutRows = none.get(spec.alias()).buckets();
      assertThat(keys(counted.get(spec.alias()))).as(spec.alias()).containsExactly("2020-01-01T00:00Z", "2021-01-01T00:00Z",
          "2022-01-01T00:00Z", "2023-01-01T00:00Z", "2024-01-01T00:00Z");
      assertThat(keys(none.get(spec.alias()))).as(spec.alias() + " over an empty match set").isEqualTo(keys(counted.get(spec.alias())));
      for (int i = 0; i < withRows.size(); i++) {
        assertThat(withoutRows.get(i).from()).isEqualTo(withRows.get(i).from());
        assertThat(withoutRows.get(i).to()).isEqualTo(withRows.get(i).to());
        assertThat(withoutRows.get(i).count()).isZero();
      }
      assertThat(withRows.get(1).count()).as(spec.alias() + " 2021").isEqualTo(3);
    }
  }

  private static void assertTwelveEmptyMonths(final FacetResult result) {
    assertThat(result.source()).isEqualTo("index");
    assertThat(result.buckets()).hasSize(12);
    assertThat(result.buckets().getFirst().key()).isEqualTo("2020-01-01T00:00Z");
    assertThat(result.buckets().getLast().key()).isEqualTo("2020-12-01T00:00Z");
    assertThat(result.buckets()).allSatisfy(bucket -> assertThat(bucket.count()).isZero());
    assertThat(result.missing()).isZero();
  }

  @Test
  @Timeout(300)
  void numericOffsetShiftsTheBuckets() {
    final ColumnarTestData data = data(1);
    data.createSchema(database);
    data.insertAssets(database, 1_000);
    FacetOracle.awaitCaughtUp(database);

    final FacetResult result = execute(null, histogram("size", "fileSize", false, MIB, null, "+512", 1, null, null)).get("size");
    final Map<Object, Long> expected = new TreeMap<>();
    for (final Object value : values(TYPE, "fileSize")) {
      final long v = ((Number) value).longValue();
      expected.merge(Math.floorDiv(v - 512, MIB) * MIB + 512, 1L, Long::sum);
    }
    assertThat(byFrom(result)).isEqualTo(expected);
    for (final FacetBucket bucket : result.buckets()) {
      assertThat(Math.floorMod((Long) bucket.from(), MIB)).as("buckets start at the offset").isEqualTo(512);
      assertThat(bucket.key()).isEqualTo(bucket.from().toString());
    }
  }

  @Test
  @Timeout(120)
  void infiniteBoundsAreUnbounded() {
    final ColumnarTestData data = data(1);
    data.createSchema(database);
    data.insertAssets(database, 300);
    database.transaction(() -> database.newDocument(TYPE).set("fileSize", Long.MAX_VALUE).save());
    FacetOracle.awaitCaughtUp(database);

    final FacetResult result = execute(null, ranges("size", "fileSize", null, new RangeBound(Double.NEGATIVE_INFINITY, MIB, "low"),
        new RangeBound(MIB, Double.POSITIVE_INFINITY, "high"))).get("size");
    assertThat(byKey(result)).as("Long.MAX_VALUE is below +Infinity")
        .isEqualTo(Map.of("low", count(TYPE, "fileSize < 1048576"), "high", count(TYPE, "fileSize >= 1048576")));
  }

  @Test
  @Timeout(300)
  void doubleGridHistogramCountsNaNAsMissing() {
    final ColumnarTestData data = data(1);
    data.doubleColumn = true;
    data.createSchema(database);
    data.insertAssets(database, 1_000);
    database.transaction(() -> {
      database.newDocument(TYPE).set("score", Double.NaN).save();
      database.newDocument(TYPE).set("score", Double.NaN).save();
      database.newDocument(TYPE).set("brand", "brand-1").save();
    });
    FacetOracle.awaitCaughtUp(database);

    final FacetResult result = execute(null, histogram("s", "score", false, 12.5d, null, "-2.5", 1, null, null)).get("s");
    final Map<Object, Long> expected = new TreeMap<>();
    long nan = 0;
    for (final Object value : values(TYPE, "score")) {
      final double v = ((Number) value).doubleValue();
      if (Double.isNaN(v))
        nan++;
      else
        expected.merge(-2.5 + Math.floor((v + 2.5) / 12.5) * 12.5, 1L, Long::sum);
    }
    assertThat(nan).isEqualTo(2);
    assertThat(byFrom(result)).isEqualTo(expected);
    for (final FacetBucket bucket : result.buckets()) {
      assertThat(bucket.key()).isEqualTo(Double.toString((Double) bucket.from()));
      assertThat((Double) bucket.to() - (Double) bucket.from()).isEqualTo(12.5);
    }
    assertThat(result.buckets().getFirst().key()).isEqualTo("-2.5");
    assertThat(result.missing()).as("a NaN is missing, as a record without a score").isEqualTo(nan + count(TYPE, "score IS NULL"));
    final long total = result.buckets().stream().mapToLong(FacetBucket::count).sum() + result.missing();
    assertThat(total).as("buckets + missing = matched rows").isEqualTo(count(TYPE, null));
  }

  @Test
  @Timeout(300)
  void calendarWithOffsetShiftsTheBoundaries() {
    final ColumnarTestData data = data(1);
    data.createSchema(database);
    data.insertAssets(database, 1_000);
    FacetOracle.awaitCaughtUp(database);

    final FacetResult days = execute(null, histogram("d", "publishDate", true, "1d", null, "+6h", 1, null, null)).get("d");
    final Map<Object, Long> expected = new TreeMap<>();
    groupBy("ts.timeBucket('1d', publishDate, :options)", TYPE, "publishDate IS NOT NULL", Map.of("options", Map.of("offset", "6h")))
        .forEach((bucket, count) -> expected.put(((LocalDateTime) bucket).toInstant(ZoneOffset.UTC).toEpochMilli(), count));
    assertThat(byFrom(days)).isEqualTo(expected);
    assertThat(days.buckets().getFirst().key()).endsWith("T06:00Z");
  }
}
