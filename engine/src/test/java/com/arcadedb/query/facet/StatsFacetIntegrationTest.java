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

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.index.columnar.ColumnarIndexManager;
import com.arcadedb.index.columnar.ColumnarTestData;
import com.arcadedb.index.columnar.ColumnarTestHooks;
import com.arcadedb.index.columnar.FacetOracle;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.withinPercentage;

/**
 * STATS facets end to end, through SQL, against {@link FacetOracle#stats} ({@code count(p)}, {@code min(p)}, {@code max(p)},
 * {@code sum(p)}, {@code avg(p)}): value and Java type of every member, on the projection, while lagging, on a scanned bucket and
 * across declaring types, for every property type STATS takes.
 */
class StatsFacetIntegrationTest extends TestHelper {
  private static final String       TYPE    = ColumnarTestData.TYPE;
  private static final String       WHERE   = "fileSize > 500";
  /** The properties summed in a different order than SQL sums them: sum and average equal within rounding, not to the last bit. */
  private static final List<String> FLOATED = List.of("score", "ratio");
  /** A hang detector, never a latency bound. */
  private static final long         HANG_MS = 60_000L;

  /** Every property type STATS takes, with the type a declaration names. */
  private static final String[][] TYPED = { { "b", "BYTE" }, { "sh", "SHORT" }, { "i", "INTEGER" }, { "l", "LONG" }, { "f", "FLOAT" },
      { "d", "DOUBLE" }, { "day", "DATE" }, { "at", "DATETIME" }, { "sec", "DATETIME_SECOND" }, { "us", "DATETIME_MICROS" },
      { "ns", "DATETIME_NANOS" } };

  private ColumnarTestData load(final long seed, final int buckets) {
    final ColumnarTestData data = new ColumnarTestData(seed);
    data.buckets = buckets;
    data.doubleColumn = true;
    data.dateColumn = true;
    data.createSchema(database);
    database.command("sql", "CREATE PROPERTY " + TYPE + ".ratio FLOAT");
    database.command("sql", "CREATE INDEX ON " + TYPE + " (ratio) COLUMNAR");
    FacetOracle.awaitCaughtUp(database);
    data.insertAssets(database, 5_000);
    final Random random = new Random(seed);
    database.transaction(() -> {
      try (final ResultSet rs = database.query("sql", "SELECT FROM " + TYPE)) {
        while (rs.hasNext()) {
          final MutableDocument record = rs.next().getRecord().get().asDocument().modify();
          // a tenth without a ratio
          if (random.nextInt(10) > 0)
            record.set("ratio", random.nextFloat() * 10f - 2f);
          record.save();
        }
      }
    });
    FacetOracle.awaitCaughtUp(database);
    return data;
  }

  private ColumnarIndexManager manager() {
    return ((LocalDatabase) ((DatabaseInternal) database).getEmbedded()).getColumnarIndexManager();
  }

  private FacetResults facets(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      return rs.getFacets();
    }
  }

  private FacetResult facet(final String type, final String property, final String where) {
    return facets("SELECT FROM " + type + (where == null ? "" : " WHERE " + where) + " FACETS s: " + property + " STATS").get("s");
  }

  /** The facet equals the SQL oracle in every member, value and type; the floating sums within rounding. */
  private void assertEqualsOracle(final String type, final String property, final String where, final String... sources) {
    final FacetResult result = facet(type, property, where);
    assertThat(result.kind()).isEqualTo("stats");
    assertThat(result.source()).as(property + ": " + result.reason()).isIn((Object[]) sources);
    assertStats(property, result.stats(), FacetOracle.stats(database, type, property, where));
    assertThat(result.missing()).isEqualTo(result.stats().missing());
  }

  private static void assertStats(final String property, final FacetStats actual, final FacetStats expected) {
    assertThat(actual.count()).as(property + " count").isEqualTo(expected.count());
    assertThat(actual.missing()).as(property + " missing").isEqualTo(expected.missing());
    assertSameValue(property + " min", actual.min(), expected.min());
    assertSameValue(property + " max", actual.max(), expected.max());
    if (FLOATED.contains(property) && expected.sum() != null) {
      // addition order differs (segments in their order, then patched records): the last bits may too, more of them in float arithmetic
      final double percentage = expected.sum() instanceof Float ? 1e-3 : 1e-9;
      assertThat(actual.sum().getClass()).as(property + " sum type").isEqualTo(expected.sum().getClass());
      assertThat(actual.sum().doubleValue()).as(property + " sum").isCloseTo(expected.sum().doubleValue(), withinPercentage(percentage));
      assertThat(actual.avg().getClass()).as(property + " avg type").isEqualTo(expected.avg().getClass());
      assertThat(((Number) actual.avg()).doubleValue()).as(property + " avg")
          .isCloseTo(((Number) expected.avg()).doubleValue(), withinPercentage(percentage));
    } else {
      assertSameValue(property + " sum", actual.sum(), expected.sum());
      assertSameValue(property + " avg", actual.avg(), expected.avg());
    }
  }

  private static void assertSameValue(final String member, final Object actual, final Object expected) {
    assertThat(actual).as(member).isEqualTo(expected);
    if (expected != null)
      assertThat(actual.getClass()).as(member + " type").isEqualTo(expected.getClass());
  }

  @ParameterizedTest
  @ValueSource(longs = { 1L, 7L, 42L })
  @Timeout(300)
  void statsEqualSql(final long seed) {
    load(seed, 1);
    for (final String property : List.of("fileSize", "score", "publishDate", "day", "ratio")) {
      assertEqualsOracle(TYPE, property, WHERE, "index");
      assertEqualsOracle(TYPE, property, null, "index");
    }
    // the oracle sees values: a facet answering empty everywhere would not pass
    assertThat(FacetOracle.stats(database, TYPE, "fileSize", WHERE).count()).isGreaterThan(1_000);
    assertThat(FacetOracle.stats(database, TYPE, "ratio", null).missing()).isGreaterThan(100);
    assertThat(FacetOracle.stats(database, TYPE, "ratio", null).sum()).isInstanceOf(Float.class);
  }

  /**
   * Review focus 11: a FLOAT property, every member against SQL with its class, the sum in float arithmetic. Inserted once and caught up,
   * the segment holds the records in the order SQL scans them, so even the float sum is SQL's to the bit (an update moves a row to a
   * later view, and float addition is not associative: {@link #statsEqualSql} compares within rounding).
   */
  @Test
  @Timeout(300)
  void floatRatioEqualsSqlIncludingClass() {
    database.command("sql", "CREATE DOCUMENT TYPE Gauge");
    database.command("sql", "CREATE PROPERTY Gauge.ratio FLOAT");
    database.command("sql", "CREATE INDEX ON Gauge (ratio) COLUMNAR");
    FacetOracle.awaitCaughtUp(database);
    final Random random = new Random(3);
    database.transaction(() -> {
      for (int i = 0; i < 3_000; i++)
        database.newDocument("Gauge").set("ratio", i % 10 == 0 ? null : random.nextFloat() * 10f - 2f).save();
    });
    FacetOracle.awaitCaughtUp(database);
    for (final String where : new String[] { null, "ratio > 1" }) {
      final FacetStats expected = FacetOracle.stats(database, "Gauge", "ratio", where);
      final FacetResult result = facet("Gauge", "ratio", where);
      assertThat(result.source()).isEqualTo("index");
      assertThat(expected.min()).isInstanceOf(Float.class);
      assertThat(expected.sum()).isInstanceOf(Float.class);
      assertThat(expected.avg()).isInstanceOf(Double.class);
      assertStats("ratio exact", result.stats(), expected);
    }
  }

  @Test
  @Timeout(300)
  void lagging() throws Exception {
    final ColumnarTestData data = load(7L, 1);
    try (final AutoCloseable hold = manager().suspendApply()) {
      data.insertAssets(database, 300);
      database.transaction(() -> database.command("sql", "DELETE FROM " + TYPE + " WHERE " + WHERE + " LIMIT 100"));
      // extremes only the new records hold
      database.transaction(() -> {
        database.newDocument(TYPE).set("fileSize", 999_999_999_999L).set("score", -1.5).set("ratio", 1e6f)
            .set("day", LocalDate.of(1990, 1, 1)).save();
        database.command("sql", "UPDATE " + TYPE + " SET fileSize = 501, ratio = -100 WHERE " + WHERE + " LIMIT 50");
      });
      for (final String property : List.of("fileSize", "score", "publishDate", "day", "ratio")) {
        final FacetResults results = facets("SELECT FROM " + TYPE + " WHERE " + WHERE + " FACETS s: " + property + " STATS");
        assertThat(results.exact()).isTrue();
        assertThat(results.patched()).isGreaterThan(0);
        assertStats(property, results.get("s").stats(), FacetOracle.stats(database, TYPE, property, WHERE));
      }
    }
  }

  @Test
  @Timeout(300)
  void multiBucketTypeEqualsSql() {
    load(42L, 3);
    assertThat(database.getSchema().getType(TYPE).getBuckets(false)).hasSize(3);
    for (final String property : List.of("fileSize", "score", "publishDate", "day", "ratio"))
      assertEqualsOracle(TYPE, property, WHERE, "index");
  }

  @Test
  @Timeout(300)
  void scannedBucketEqualsOracle() throws Exception {
    load(13L, 1);
    database.command("sql", "DROP INDEX `" + TYPE + "[fileSize]_columnar`");
    FacetOracle.awaitCaughtUp(database);
    // no COLUMNAR index at all: every bucket is read from its records, by the declared type
    assertEqualsOracle(TYPE, "fileSize", WHERE, "scan");

    final CountDownLatch reached = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    ColumnarTestHooks.killPoint = name -> {
      if ("rebuild.scanned".equals(name)) {
        reached.countDown();
        try {
          release.await(HANG_MS, TimeUnit.MILLISECONDS);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    };
    try {
      database.command("sql", "CREATE INDEX ON " + TYPE + " (fileSize) COLUMNAR");
      assertThat(reached.await(HANG_MS, TimeUnit.MILLISECONDS)).as("the create rebuild reached the end of its scan").isTrue();
      assertEqualsOracle(TYPE, "fileSize", WHERE, "scan");
      // a column whose projection is published is still answered from it
      assertEqualsOracle(TYPE, "score", WHERE, "index");
    } finally {
      ColumnarTestHooks.killPoint = null;
      release.countDown();
    }
    FacetOracle.awaitCaughtUp(database);
    assertEqualsOracle(TYPE, "fileSize", WHERE, "index");
  }

  /**
   * Every property type STATS takes, projected (Typed) and scanned (Plain, the same properties without index), with records that lack
   * the property or hold an explicit null, and a type of one record (a one-row sum of a SHORT or BYTE is a Short or Byte, as SQL's).
   */
  @Test
  @Timeout(300)
  void everyPropertyTypeEqualsSql() {
    createTyped("Typed", true);
    createTyped("Plain", false);
    createTyped("One", true);
    FacetOracle.awaitCaughtUp(database);
    final Random random = new Random(77);
    database.transaction(() -> {
      for (int i = 0; i < 400; i++) {
        insertTyped("Typed", random, i % 13 == 0);
        insertTyped("Plain", random, i % 11 == 0);
      }
      insertTyped("One", random, false);
    });
    FacetOracle.awaitCaughtUp(database);
    for (final String[] typed : TYPED) {
      assertEqualsOracle("Typed", typed[0], null, "index");
      assertEqualsOracle("Typed", typed[0], "i > 0", "index");
      assertEqualsOracle("Plain", typed[0], null, "scan");
      assertEqualsOracle("One", typed[0], null, "index");
    }
    assertThat(FacetOracle.stats(database, "One", "sh", null).sum()).isInstanceOf(Short.class);
    assertThat(FacetOracle.stats(database, "One", "b", null).sum()).isInstanceOf(Byte.class);
    assertThat(FacetOracle.stats(database, "Typed", "b", null).min()).isInstanceOf(Byte.class);
    assertThat(FacetOracle.stats(database, "Typed", "f", null).sum()).isInstanceOf(Float.class);
  }

  private void createTyped(final String type, final boolean indexed) {
    database.command("sql", "CREATE DOCUMENT TYPE " + type);
    for (final String[] typed : TYPED) {
      database.command("sql", "CREATE PROPERTY " + type + "." + typed[0] + " " + typed[1]);
      if (indexed)
        database.command("sql", "CREATE INDEX ON " + type + " (" + typed[0] + ") COLUMNAR");
    }
  }

  private void insertTyped(final String type, final Random random, final boolean empty) {
    final MutableDocument record = database.newDocument(type);
    if (!empty) {
      final LocalDateTime instant = LocalDateTime.of(2020, 1, 1, 0, 0).plusSeconds(random.nextInt(10_000_000))
          .plusNanos(random.nextInt(1_000_000_000));
      record.set("b", (byte) (random.nextInt(256) - 128));
      record.set("sh", (short) (random.nextInt(65_536) - 32_768));
      record.set("i", random.nextInt());
      record.set("l", random.nextLong() >> 2);
      record.set("f", random.nextFloat() * 100f - 50f);
      record.set("d", random.nextDouble() * 1e6 - 5e5);
      record.set("day", LocalDate.of(2000, 1, 1).plusDays(random.nextInt(9_000)));
      record.set("at", instant);
      record.set("sec", instant);
      record.set("us", instant);
      record.set("ns", instant);
    } else if (random.nextBoolean())
      // an explicit null is no value either
      for (final String[] typed : TYPED)
        record.set(typed[0], null);
    record.save();
  }

  /**
   * Item C5: a DATETIME_MICROS or DATETIME_NANOS min and max are SQL's to the micro and nanosecond, on the projection and on the scan
   * path (a scanned bucket is read at its declared precision), and across the two (a supertype over both).
   */
  @Test
  @Timeout(300)
  void microAndNanoDatetimeMinMaxMatchSql() {
    database.command("sql", "CREATE DOCUMENT TYPE Clock");
    database.command("sql", "CREATE PROPERTY Clock.us DATETIME_MICROS");
    database.command("sql", "CREATE PROPERTY Clock.ns DATETIME_NANOS");
    database.command("sql", "CREATE DOCUMENT TYPE IndexedClock EXTENDS Clock");
    database.command("sql", "CREATE INDEX ON IndexedClock (us) COLUMNAR");
    database.command("sql", "CREATE INDEX ON IndexedClock (ns) COLUMNAR");
    FacetOracle.awaitCaughtUp(database);
    final LocalDateTime base = LocalDateTime.of(2020, 1, 1, 0, 0);
    database.transaction(() -> {
      // the scanned extremes are one microsecond (one nanosecond) past the projected ones: a millisecond read would tie them
      for (int i = 1; i <= 3; i++)
        database.newDocument("Clock").set("us", base.plusNanos(1_000_000 + i * 1_000L)).set("ns", base.plusNanos(1_000_000 + i)).save();
      database.newDocument("IndexedClock").set("us", base.plusNanos(1_002_000)).set("ns", base.plusNanos(1_000_002)).save();
      database.newDocument("IndexedClock").set("us", base.plusNanos(1_000_000)).set("ns", base.plusNanos(1_000_000)).save();
    });
    FacetOracle.awaitCaughtUp(database);
    for (final String property : List.of("us", "ns")) {
      assertEqualsOracle("IndexedClock", property, null, "index");
      assertEqualsOracle("Clock", property, null, "scan");
      assertEqualsOracle("Clock", property, "@type = 'Clock'", "scan");
    }
    final FacetStats ns = FacetOracle.stats(database, "Clock", "ns", "@type = 'Clock'");
    assertThat(ns.min()).isNotEqualTo(ns.max());
  }

  /**
   * Across declaring types the members follow SQL: an INTEGER and a LONG subtype give Long extremes and a Long sum (SQL casts the
   * extremes to a common class), a scanned SHORT subtype joins them, and a property a subtype does not declare is read raw.
   */
  @Test
  @Timeout(300)
  void mergeAcrossDeclaringTypesFollowsSql() {
    database.command("sql", "CREATE DOCUMENT TYPE Num");
    database.command("sql", "CREATE DOCUMENT TYPE NumInt EXTENDS Num");
    database.command("sql", "CREATE PROPERTY NumInt.n INTEGER");
    database.command("sql", "CREATE INDEX ON NumInt (n) COLUMNAR");
    database.command("sql", "CREATE DOCUMENT TYPE NumLong EXTENDS Num");
    database.command("sql", "CREATE PROPERTY NumLong.n LONG");
    database.command("sql", "CREATE INDEX ON NumLong (n) COLUMNAR");
    database.command("sql", "CREATE DOCUMENT TYPE NumShort EXTENDS Num");
    database.command("sql", "CREATE PROPERTY NumShort.n SHORT");
    database.command("sql", "CREATE DOCUMENT TYPE NumRaw EXTENDS Num");
    FacetOracle.awaitCaughtUp(database);
    database.transaction(() -> {
      for (int i = 0; i < 50; i++) {
        database.newDocument("NumInt").set("n", i * 1_000 - 7).save();
        database.newDocument("NumLong").set("n", (long) i * 3 + 1).save();
        database.newDocument("NumShort").set("n", (short) (i - 25)).save();
        database.newDocument("NumRaw").set("n", i % 2 == 0 ? (Object) (i * 10) : (Object) ((long) i << 33)).save();
      }
      database.newDocument("NumRaw").save();
    });
    FacetOracle.awaitCaughtUp(database);
    assertEqualsOracle("Num", "n", null, "scan");
    assertEqualsOracle("Num", "n", "@type <> 'NumRaw'", "scan");
    assertEqualsOracle("Num", "n", "@type = 'NumInt' OR @type = 'NumLong'", "index");
    assertThat(FacetOracle.stats(database, "Num", "n", "@type = 'NumInt' OR @type = 'NumLong'").min()).isInstanceOf(Long.class);

    // a raw string: SQL cannot sum it, the facet is unavailable
    database.transaction(() -> database.newDocument("NumRaw").set("n", "text").save());
    final FacetResult refused = facet("Num", "n", null);
    assertThat(refused.source()).isEqualTo(FacetResult.UNAVAILABLE);
    assertThat(refused.reason()).isEqualTo(StatsFacetCollector.NOT_A_NUMBER_OR_DATE);
  }

  /**
   * DATE and DATETIME subtypes under one supertype: SQL's min() and max() compare a DATE with a DATETIME (the date at its midnight), the
   * facet gives the same extremes; sum and average are null.
   */
  @Test
  @Timeout(300)
  void dateAndDatetimeFollowSql() {
    database.command("sql", "CREATE DOCUMENT TYPE Event");
    database.command("sql", "CREATE DOCUMENT TYPE OnDay EXTENDS Event");
    database.command("sql", "CREATE PROPERTY OnDay.at DATE");
    database.command("sql", "CREATE INDEX ON OnDay (at) COLUMNAR");
    database.command("sql", "CREATE DOCUMENT TYPE AtMs EXTENDS Event");
    database.command("sql", "CREATE PROPERTY AtMs.at DATETIME");
    database.command("sql", "CREATE INDEX ON AtMs (at) COLUMNAR");
    FacetOracle.awaitCaughtUp(database);
    database.transaction(() -> {
      for (final LocalDate day : List.of(LocalDate.of(2020, 1, 1), LocalDate.of(1999, 12, 31)))
        database.newDocument("OnDay").set("at", day).save();
      for (final LocalDateTime at : List.of(LocalDateTime.of(2021, 5, 5, 12, 0), LocalDateTime.of(1999, 12, 31, 23, 0)))
        database.newDocument("AtMs").set("at", at).save();
    });
    FacetOracle.awaitCaughtUp(database);
    assertEqualsOracle("Event", "at", null, "index");
    final FacetStats expected = FacetOracle.stats(database, "Event", "at", null);
    assertThat(expected.count()).isEqualTo(4L);
    assertThat(expected.sum()).isNull();
  }
}
