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
import com.arcadedb.index.columnar.ColumnarIndexManager;
import com.arcadedb.index.columnar.ColumnarTestData;
import com.arcadedb.index.columnar.ColumnarTestHooks;
import com.arcadedb.index.columnar.FacetOracle;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * DISTINCT facets end to end, through SQL, against {@link FacetOracle#distinctCount} ({@code count(DISTINCT p)}, or the groups of the
 * unwound items of a list), over 5000 assets: a dictionary column (brand, author), a list (tags) and a LONG column (fileSize).
 */
class DistinctFacetIntegrationTest extends TestHelper {
  private static final String       TYPE       = ColumnarTestData.TYPE;
  private static final String       WHERE      = "fileSize > 500";
  private static final List<String> PROPERTIES = List.of("brand", "author", "tags", "fileSize");
  /** A hang detector, never a latency bound. */
  private static final long         HANG_MS    = 60_000L;

  private ColumnarTestData load(final long seed, final int buckets) {
    final ColumnarTestData data = new ColumnarTestData(seed);
    data.buckets = buckets;
    data.createSchema(database);
    data.insertAssets(database, 5_000);
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

  private FacetResult facet(final String sql, final String alias) {
    return facets(sql).get(alias);
  }

  private void assertEqualsOracle(final String type, final String property, final String where, final String... sources) {
    final FacetResult result = facet("SELECT FROM " + type + (where == null ? "" : " WHERE " + where) + " FACETS d: " + property + " DISTINCT",
        "d");
    assertThat(result.kind()).isEqualTo("distinct");
    assertThat(result.source()).as(property + ": " + result.reason()).isIn((Object[]) sources);
    assertThat(result.distinct()).as(property).isEqualTo(new DistinctValue(FacetOracle.distinctCount(database, type, property, where), true));
  }

  @ParameterizedTest
  @ValueSource(longs = { 1L, 7L, 42L })
  @Timeout(300)
  void distinctEqualsCountDistinct(final long seed) {
    load(seed, 1);
    for (final String p : PROPERTIES)
      assertThat(facet("SELECT FROM " + TYPE + " WHERE " + WHERE + " FACETS d: " + p + " DISTINCT", "d").distinct()).as(p)
          .isEqualTo(new DistinctValue(FacetOracle.distinctCount(database, TYPE, p, WHERE), true));
    // the oracle counts something: a facet answering 0 everywhere would not pass
    assertThat(FacetOracle.distinctCount(database, TYPE, "brand", WHERE)).isGreaterThan(100);
    assertThat(FacetOracle.distinctCount(database, TYPE, "tags", WHERE)).isGreaterThan(100);
  }

  @Test
  @Timeout(300)
  void lagging() throws Exception {
    final ColumnarTestData data = load(7L, 1);
    try (final AutoCloseable hold = manager().suspendApply()) {
      data.insertAssets(database, 300);
      database.transaction(() -> database.command("sql", "DELETE FROM " + TYPE + " WHERE " + WHERE + " LIMIT 100"));
      // distinct values only the new records hold
      database.transaction(() -> {
        for (int i = 0; i < 20; i++)
          database.newDocument(TYPE).set("brand", "late-" + i).set("tags", List.of("late-tag-" + i)).set("fileSize", 7_777_777L + i).save();
      });
      for (final String p : PROPERTIES) {
        final FacetResults results = facets("SELECT FROM " + TYPE + " WHERE " + WHERE + " FACETS d: " + p + " DISTINCT");
        assertThat(results.exact()).isTrue();
        assertThat(results.patched()).isGreaterThan(0);
        assertThat(results.get("d").distinct()).as(p).isEqualTo(new DistinctValue(FacetOracle.distinctCount(database, TYPE, p, WHERE), true));
      }
    }
  }

  @Test
  @Timeout(300)
  void multiBucketTypeEqualsCountDistinct() {
    load(42L, 3);
    assertThat(database.getSchema().getType(TYPE).getBuckets(false)).hasSize(3);
    for (final String p : List.of("brand", "tags", "fileSize"))
      assertEqualsOracle(TYPE, p, WHERE, "index");
  }

  @Test
  @Timeout(300)
  void approximateIsFlaggedInMeta() {
    load(1L, 1);
    final FacetResults results = facets("SELECT FROM " + TYPE + " FACETS d: fileSize DISTINCT PRECISION 10, b: brand DISTINCT");
    assertThat(results.exact()).isFalse();
    final DistinctValue approximate = results.get("d").distinct();
    assertThat(approximate.exact()).isFalse();
    final long expected = FacetOracle.distinctCount(database, TYPE, "fileSize", null);
    assertThat((double) approximate.value()).isCloseTo(expected, within(expected * 0.03));
    // a dictionary column is always exact
    assertThat(results.get("b").distinct()).isEqualTo(new DistinctValue(FacetOracle.distinctCount(database, TYPE, "brand", null), true));
    assertThat(results.toJSON().getJSONObject("meta").getBoolean("exact")).isFalse();
  }

  @Test
  @Timeout(300)
  void scannedBucketEqualsOracle() throws Exception {
    load(13L, 1);
    database.command("sql", "DROP INDEX `" + TYPE + "[brand]_columnar`");
    FacetOracle.awaitCaughtUp(database);

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
      database.command("sql", "CREATE INDEX ON " + TYPE + " (brand) COLUMNAR");
      assertThat(reached.await(HANG_MS, TimeUnit.MILLISECONDS)).as("the create rebuild reached the end of its scan").isTrue();
      assertEqualsOracle(TYPE, "brand", WHERE, "scan");
      // a column whose projection is published is still answered from it
      assertEqualsOracle(TYPE, "fileSize", WHERE, "index");
    } finally {
      ColumnarTestHooks.killPoint = null;
      release.countDown();
    }
    FacetOracle.awaitCaughtUp(database);
    assertEqualsOracle(TYPE, "brand", WHERE, "index");
  }

  /**
   * Carried from Task 6 (R3): the index only on a subtype, so the supertype's own buckets have no column. A STRING property there is
   * read by its raw values (it used to go to the double reader, where every string is missing); a LONG one by its declared type; a
   * DECIMAL one by its raw values, canonicalized as {@code count(DISTINCT)} keys numbers.
   */
  @Test
  @Timeout(300)
  void unindexedSupertypeBucketsReadRawValues() {
    database.command("sql", "CREATE DOCUMENT TYPE Media");
    database.command("sql", "CREATE PROPERTY Media.label STRING");
    database.command("sql", "CREATE PROPERTY Media.size LONG");
    database.command("sql", "CREATE PROPERTY Media.price DECIMAL");
    database.command("sql", "CREATE DOCUMENT TYPE Photo EXTENDS Media");
    database.command("sql", "CREATE INDEX ON Photo (label) COLUMNAR");
    database.command("sql", "CREATE INDEX ON Photo (size) COLUMNAR");
    FacetOracle.awaitCaughtUp(database);
    database.transaction(() -> {
      for (int i = 0; i < 60; i++) {
        database.newDocument("Media").set("label", "m" + (i % 7)).set("size", (long) (i % 5)).set("price", new BigDecimal(i % 4 + ".50"))
            .save();
        // m0..m6 and p0..p2: the strings in both kinds of bucket count once
        database.newDocument("Photo").set("label", i % 2 == 0 ? "m" + (i % 7) : "p" + (i % 3)).set("size", (long) (i % 9)).save();
      }
      database.newDocument("Media").save();
    });
    FacetOracle.awaitCaughtUp(database);

    assertThat(FacetOracle.distinctCount(database, "Media", "label", null)).isEqualTo(10L);
    assertEqualsOracle("Media", "label", null, "scan");
    assertEqualsOracle("Media", "size", null, "scan");
    assertEqualsOracle("Media", "price", null, "scan");
    final FacetResult label = facet("SELECT FROM Media FACETS d: label DISTINCT", "d");
    assertThat(label.missing()).isEqualTo(1L);
    // the indexed subtype alone, from its projection
    assertEqualsOracle("Photo", "label", null, "index");
  }

  /**
   * Review I2: count(DISTINCT) keys numbers with Type.normalizeNumberForKey, so a DECIMAL no double holds is its own value (0.1 and
   * 0.10000000000000000001, 2^53 and 2^53 + 1), while a DECIMAL 0.1 and a DOUBLE 0.1 of another type are one.
   */
  @Test
  @Timeout(120)
  void decimalsAreKeyedAsCountDistinct() {
    database.command("sql", "CREATE DOCUMENT TYPE Priced");
    database.command("sql", "CREATE DOCUMENT TYPE PricedDecimal EXTENDS Priced");
    database.command("sql", "CREATE PROPERTY PricedDecimal.price DECIMAL");
    database.command("sql", "CREATE DOCUMENT TYPE PricedDouble EXTENDS Priced");
    database.command("sql", "CREATE PROPERTY PricedDouble.price DOUBLE");
    database.transaction(() -> {
      for (final String decimal : List.of("0.1", "0.10000000000000000001", "9007199254740992", "9007199254740993", "0.50", "2.5"))
        database.newDocument("PricedDecimal").set("price", new BigDecimal(decimal)).save();
      for (final double value : new double[] { 0.1, 0.25, 2.5, 9007199254740992d })
        database.newDocument("PricedDouble").set("price", value).save();
    });
    assertThat(FacetOracle.distinctCount(database, "Priced", "price", null)).isEqualTo(7L);
    assertEqualsOracle("Priced", "price", null, "scan");
    assertEqualsOracle("PricedDecimal", "price", null, "scan");
  }

  /**
   * Review I3, I4 and M4: a DATE and a DATETIME of the same day are two values for count(DISTINCT), the DATETIME family (ms, us, ns) is
   * one key space of instants, whether a bucket is projected (indexed subtypes) or scanned (an unindexed subtype at its declared
   * precision, a type that does not declare the property and holds raw temporals).
   */
  @Test
  @Timeout(300)
  void datesKeepTheirKeySpaces() {
    database.command("sql", "CREATE DOCUMENT TYPE Event");
    final String[][] subtypes = { { "OnDay", "DATE", "true" }, { "AtMs", "DATETIME", "true" }, { "AtUs", "DATETIME_MICROS", "true" },
        { "OnDayRaw", "DATE", "false" }, { "AtUsRaw", "DATETIME_MICROS", "false" } };
    for (final String[] subtype : subtypes) {
      database.command("sql", "CREATE DOCUMENT TYPE " + subtype[0] + " EXTENDS Event");
      database.command("sql", "CREATE PROPERTY " + subtype[0] + ".at " + subtype[1]);
      if (Boolean.parseBoolean(subtype[2]))
        database.command("sql", "CREATE INDEX ON " + subtype[0] + " (at) COLUMNAR");
    }
    database.command("sql", "CREATE DOCUMENT TYPE Loose EXTENDS Event");
    FacetOracle.awaitCaughtUp(database);
    final LocalDate day = LocalDate.of(2020, 1, 1);
    final LocalDateTime midnight = day.atStartOfDay();
    database.transaction(() -> {
      for (final LocalDate value : List.of(day, LocalDate.of(1999, 12, 31)))
        database.newDocument("OnDay").set("at", value).save();
      for (final LocalDateTime value : List.of(midnight, midnight.plusNanos(1_000_000)))
        database.newDocument("AtMs").set("at", value).save();
      for (final LocalDateTime value : List.of(midnight.plusNanos(1_000_000), midnight.plusNanos(1_002_000)))
        database.newDocument("AtUs").set("at", value).save();
      for (final LocalDate value : List.of(day, LocalDate.of(1888, 8, 8)))
        database.newDocument("OnDayRaw").set("at", value).save();
      // sub-ms instants a millisecond read would fold into one
      for (final LocalDateTime value : List.of(midnight.plusNanos(1_002_000), midnight.plusNanos(1_003_000), midnight.plusNanos(1_004_000)))
        database.newDocument("AtUsRaw").set("at", value).save();
      for (final Object value : List.<Object>of(day, midnight, LocalDateTime.of(2021, 5, 5, 12, 0)))
        database.newDocument("Loose").set("at", value).save();
    });
    FacetOracle.awaitCaughtUp(database);
    // dates: 2020-01-01, 1999-12-31, 1888-08-08; instants: midnight, +1 ms, +1.002 ms, +1.003 ms, +1.004 ms, 2021-05-05T12:00
    assertThat(FacetOracle.distinctCount(database, "Event", "at", null)).isEqualTo(9L);
    assertEqualsOracle("Event", "at", null, "scan");
    assertEqualsOracle("AtUsRaw", "at", null, "scan");
  }

  /** Review I4: a scanned DATETIME_MICROS or DATETIME_NANOS bucket is read at its declared precision, merged with a projected one. */
  @Test
  @Timeout(300)
  void scannedMicrosAndNanosKeepTheirPrecision() {
    database.command("sql", "CREATE DOCUMENT TYPE Clock");
    database.command("sql", "CREATE PROPERTY Clock.us DATETIME_MICROS");
    database.command("sql", "CREATE PROPERTY Clock.ns DATETIME_NANOS");
    database.command("sql", "CREATE DOCUMENT TYPE IndexedClock EXTENDS Clock");
    database.command("sql", "CREATE INDEX ON IndexedClock (us) COLUMNAR");
    database.command("sql", "CREATE INDEX ON IndexedClock (ns) COLUMNAR");
    FacetOracle.awaitCaughtUp(database);
    final LocalDateTime base = LocalDateTime.of(2020, 1, 1, 0, 0);
    database.transaction(() -> {
      for (int i = 1; i <= 3; i++)
        database.newDocument("Clock").set("us", base.plusNanos(1_000_000 + i * 1_000L)).set("ns", base.plusNanos(i)).save();
      database.newDocument("IndexedClock").set("us", base.plusNanos(1_002_000)).set("ns", base.plusNanos(5)).save();
    });
    FacetOracle.awaitCaughtUp(database);
    assertThat(FacetOracle.distinctCount(database, "Clock", "us", null)).isEqualTo(3L);
    assertThat(FacetOracle.distinctCount(database, "Clock", "ns", null)).isEqualTo(4L);
    assertEqualsOracle("Clock", "us", null, "scan");
    assertEqualsOracle("Clock", "ns", null, "scan");
  }

  /**
   * Review r1 N1: a number is one value whether a dictionary column (a numeric LIST indexed BY ITEM) holds it as an ordinal or another
   * collector as a numeric key, in both merge orders (the projected collectors come first in plan order, by bucket).
   */
  @Test
  @Timeout(120)
  void numbersOfADictionaryColumnMeetNumericKeys() {
    // the dictionary collector first: L's indexed subtype, then the scanned one holding raw numbers
    database.command("sql", "CREATE DOCUMENT TYPE L");
    database.command("sql", "CREATE PROPERTY L.nums LIST");
    database.command("sql", "CREATE DOCUMENT TYPE LIdx EXTENDS L");
    database.command("sql", "CREATE INDEX ON LIdx (nums BY ITEM) COLUMNAR");
    database.command("sql", "CREATE DOCUMENT TYPE LRaw EXTENDS L");
    FacetOracle.awaitCaughtUp(database);
    database.transaction(() -> {
      database.newDocument("LIdx").set("nums", List.of(1, 2, 3, 2.5)).save();
      database.newDocument("LRaw").set("nums", List.of(2, 3, 4, 2.5)).save();
    });
    FacetOracle.awaitCaughtUp(database);
    assertThat(FacetOracle.distinctCount(database, "L", "nums", null)).isEqualTo(5L);
    assertEqualsOracle("L", "nums", null, "scan");

    // the numeric collector first: a LONG column created before the list column, both projected
    database.command("sql", "CREATE DOCUMENT TYPE M");
    database.command("sql", "CREATE DOCUMENT TYPE MNum EXTENDS M");
    database.command("sql", "CREATE PROPERTY MNum.n LONG");
    database.command("sql", "CREATE INDEX ON MNum (n) COLUMNAR");
    database.command("sql", "CREATE DOCUMENT TYPE MList EXTENDS M");
    database.command("sql", "CREATE PROPERTY MList.n LIST");
    database.command("sql", "CREATE INDEX ON MList (n BY ITEM) COLUMNAR");
    FacetOracle.awaitCaughtUp(database);
    database.transaction(() -> {
      database.newDocument("MNum").set("n", 2L).save();
      database.newDocument("MNum").set("n", 3L).save();
      database.newDocument("MList").set("n", List.of(1, 2, 3)).save();
    });
    FacetOracle.awaitCaughtUp(database);
    long groups = 0;
    try (final ResultSet rs = database.query("sql", "SELECT n AS v, count(*) AS c FROM (SELECT n FROM M UNWIND n) GROUP BY n")) {
      while (rs.hasNext())
        if (rs.next().getProperty("v") != null)
          groups++;
    }
    assertThat(groups).isEqualTo(3L);
    final FacetResult mixed = facet("SELECT FROM M FACETS d: n DISTINCT", "d");
    assertThat(mixed.source()).isEqualTo("index");
    assertThat(mixed.distinct()).isEqualTo(new DistinctValue(groups, true));
  }

  /** Review r1 N2: an instant after 2262 (beyond nanoseconds in a long) read raw meets the same instant of a DATETIME column. */
  @Test
  @Timeout(120)
  void instantAfter2262MeetsItsDatetime() {
    database.command("sql", "CREATE DOCUMENT TYPE Far");
    database.command("sql", "CREATE DOCUMENT TYPE FarMs EXTENDS Far");
    database.command("sql", "CREATE PROPERTY FarMs.at DATETIME");
    database.command("sql", "CREATE INDEX ON FarMs (at) COLUMNAR");
    database.command("sql", "CREATE DOCUMENT TYPE FarRaw EXTENDS Far");
    FacetOracle.awaitCaughtUp(database);
    final LocalDateTime far = LocalDateTime.of(2300, 1, 1, 0, 0);
    database.transaction(() -> {
      database.newDocument("FarMs").set("at", far).save();
      database.newDocument("FarRaw").set("at", far).save();
    });
    FacetOracle.awaitCaughtUp(database);
    assertThat(FacetOracle.distinctCount(database, "Far", "at", null)).isEqualTo(1L);
    assertEqualsOracle("Far", "at", null, "scan");
  }

  @Test
  void scannedStringSourceReadsRawValues() {
    database.command("sql", "CREATE DOCUMENT TYPE Plain");
    database.command("sql", "CREATE PROPERTY Plain.label STRING");
    database.command("sql", "CREATE PROPERTY Plain.amount DOUBLE");
    final DatabaseInternal db = (DatabaseInternal) database;
    final DocumentType type = database.getSchema().getType("Plain");
    assertThat(FacetFallbackScanner.sourceOf(db, type, new DistinctFacetSpec("d", "label", null, Long.MAX_VALUE)).read())
        .isEqualTo(FacetFallbackScanner.Read.TERMS);
    assertThat(FacetFallbackScanner.sourceOf(db, type, new DistinctFacetSpec("d", "undeclared", null, Long.MAX_VALUE)).read())
        .isEqualTo(FacetFallbackScanner.Read.TERMS);
    assertThat(FacetFallbackScanner.sourceOf(db, type, new DistinctFacetSpec("d", "amount", null, Long.MAX_VALUE)).read())
        .isEqualTo(FacetFallbackScanner.Read.NUMBER);
  }
}
