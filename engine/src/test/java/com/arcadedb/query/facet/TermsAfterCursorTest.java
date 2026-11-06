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
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.index.columnar.ColumnarIndexManager;
import com.arcadedb.index.columnar.FacetOracle;
import com.arcadedb.index.columnar.GlobalDictionary;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The AFTER cursor of a terms facet, end to end through SQL: paging with the cursor each result returns ({@link FacetResult#after()},
 * the row type of the property) must return exactly the values of {@code GROUP BY p ORDER BY p}, each once, on every selection path of
 * {@link TermsCollector}: one dictionary bucket without a rank view (26 values) and with one plus an unranked tail (1130 values), merged
 * ordinals of a multi-bucket type, values only patched records hold, a bucket scanned because the transaction wrote it, the merge across
 * declaring types, and LONG, DOUBLE and DATETIME terms.
 */
class TermsAfterCursorTest extends TestHelper {
  private static final String TYPE = "Item";
  /** A hang detector, never a latency bound. */
  private static final int    PAGES_MAX = 2_000;

  private void createSchema(final String type, final int buckets) {
    database.command("sql", "CREATE DOCUMENT TYPE " + type + " BUCKETS " + buckets);
    database.command("sql", "CREATE PROPERTY " + type + ".brand STRING");
    database.command("sql", "CREATE PROPERTY " + type + ".fileSize LONG");
    database.command("sql", "CREATE PROPERTY " + type + ".score DOUBLE");
    database.command("sql", "CREATE PROPERTY " + type + ".publishDate DATETIME");
    for (final String property : List.of("brand", "fileSize", "score", "publishDate"))
      database.command("sql", "CREATE INDEX ON " + type + " (" + property + ") COLUMNAR");
  }

  /** 26 brands "a".."z", brand b held by {@code b % 4 + 1} records, and 3 records without a brand. */
  private void load(final int buckets) {
    createSchema(TYPE, buckets);
    database.transaction(() -> {
      int j = 0;
      for (int b = 0; b < 26; b++)
        for (int k = 0; k <= b % 4; k++)
          insert(TYPE, String.valueOf((char) ('a' + b)), j++);
      for (int k = 0; k < 3; k++)
        insert(TYPE, null, j++);
    });
    FacetOracle.awaitCaughtUp(database);
  }

  private void insert(final String type, final String brand, final int j) {
    database.newDocument(type).set("brand", brand).set("fileSize", (long) ((j * 37) % 41 - 15) * 100L)
        .set("score", (double) ((j * 7) % 17 - 8) + 0.25).set("publishDate", LocalDateTime.of(2021, 1, 1, 0, 0).plusDays((j * 11) % 30).plusHours(j % 2))
        .save();
  }

  private ColumnarIndexManager manager() {
    return ((LocalDatabase) ((DatabaseInternal) database).getEmbedded()).getColumnarIndexManager();
  }

  private FacetResults facets(final String sql, final Object cursor) {
    final Map<String, Object> parameters = new HashMap<>();
    if (cursor != null)
      parameters.put("c", cursor);
    try (final ResultSet rs = database.query("sql", sql, parameters)) {
      return rs.getFacets();
    }
  }

  private FacetResult facet(final String sql) {
    return facets(sql, null).get("f");
  }

  /** {@code GROUP BY p ORDER BY p} as (value, count) pairs, the groups with at least {@code minCount} records. */
  private List<List<Object>> oracle(final String type, final String property, final int minCount) {
    final List<List<Object>> groups = new ArrayList<>();
    try (final FacetOracle.NormalPlan normal = FacetOracle.normalPlan(database);
        final ResultSet rs = database.query("sql",
        "SELECT " + property + " AS v, count(*) AS c FROM " + type + " WHERE " + property + " IS NOT NULL GROUP BY " + property
            + " ORDER BY v")) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        final long count = ((Number) row.getProperty("c")).longValue();
        if (count >= minCount)
          groups.add(List.of(row.getProperty("v"), count));
      }
    }
    return groups;
  }

  private static List<List<Object>> pairs(final List<FacetBucket> buckets) {
    final List<List<Object>> pairs = new ArrayList<>(buckets.size());
    for (final FacetBucket bucket : buckets)
      pairs.add(List.of(bucket.isMissing() ? "<missing>" : bucket.value(), bucket.count()));
    return pairs;
  }

  /**
   * Pages through the facet {@code f} with TOP {@code top}: the first page ordered by value without AFTER, every next one AFTER the
   * cursor the previous page returned (without ORDER BY: AFTER implies it), until a page returns a null cursor.
   *
   * @param echo turns a returned cursor into what the client sends back (itself, or a formatted string)
   */
  private List<FacetBucket> pageAll(final String type, final String property, final String options, final int top, final String source,
      final Function<Object, Object> echo) {
    final List<FacetBucket> all = new ArrayList<>();
    Object cursor = null;
    for (int page = 0; page < PAGES_MAX; page++) {
      final String sql = "SELECT FROM " + type + " FACETS f: " + property + " TERMS TOP " + top + options + (cursor == null ?
          " ORDER BY VALUE" :
          " AFTER :c");
      final FacetResult result = facets(sql, cursor == null ? null : echo.apply(cursor)).get("f");
      if (source != null)
        assertThat(result.source()).as(sql + ": " + result.reason()).isEqualTo(source);
      assertThat(result.hasAfter()).as(sql).isTrue();
      final List<FacetBucket> values = new ArrayList<>();
      for (final FacetBucket bucket : result.buckets())
        if (!bucket.isMissing())
          values.add(bucket);
      all.addAll(values);
      if (result.after() == null)
        return all;
      // a cursor is returned only with a full page, and it is the last value of the page, as a row holds it
      assertThat(values).as(sql).hasSize(top);
      assertThat(result.after()).as(sql).isEqualTo(values.getLast().value());
      cursor = result.after();
    }
    throw new AssertionError("More than " + PAGES_MAX + " pages");
  }

  private List<FacetBucket> pageAll(final String type, final String property, final String options, final int top, final String source) {
    return pageAll(type, property, options, top, source, c -> c);
  }

  private List<Object> values(final String sql, final Object cursor) {
    final List<Object> values = new ArrayList<>();
    for (final FacetBucket bucket : facets(sql, cursor).get("f").buckets())
      values.add(bucket.isMissing() ? "<missing>" : bucket.value());
    return values;
  }

  @Test
  @Timeout(120)
  void pagesThroughAllValuesInValueOrder() {
    load(1);
    // 26 values: no rank view, every ordinal ranks 0 and the value check decides
    assertThat(manager().projection(database.getSchema().getType(TYPE).getBuckets(false).getFirst().getFileId()).dictionary("brand")
        .sortedOrdinals()).isEmpty();
    final List<List<Object>> expected = oracle(TYPE, "brand", 1);
    assertThat(expected).hasSize(26);
    assertThat(pairs(pageAll(TYPE, "brand", "", 10, "index"))).containsExactlyElementsOf(expected);
    assertThat(pairs(pageAll(TYPE, "brand", "", 1, "index"))).containsExactlyElementsOf(expected);
    assertThat(pairs(pageAll(TYPE, "brand", "", 26, "index"))).containsExactlyElementsOf(expected);
  }

  @Test
  @Timeout(120)
  void cursorNotInDictionaryStartsAtLowerBound() {
    load(1);
    assertThat(values("SELECT FROM " + TYPE + " FACETS f: brand TERMS TOP 3 AFTER 'cz'", null)).containsExactly("d", "e", "f");
    assertThat(values("SELECT FROM " + TYPE + " FACETS f: brand TERMS TOP 3 AFTER :c", "c")).containsExactly("d", "e", "f");
    assertThat(values("SELECT FROM " + TYPE + " FACETS f: brand TERMS TOP 3 AFTER ''", null)).containsExactly("a", "b", "c");
    // a value whose records are gone stays in the dictionary: the page starts after it all the same
    database.transaction(() -> database.command("sql", "DELETE FROM " + TYPE + " WHERE brand = 'e'"));
    FacetOracle.awaitCaughtUp(database);
    assertThat(values("SELECT FROM " + TYPE + " FACETS f: brand TERMS TOP 3 AFTER 'd'", null)).containsExactly("f", "g", "h");
    assertThat(values("SELECT FROM " + TYPE + " FACETS f: brand TERMS TOP 3 AFTER 'e'", null)).containsExactly("f", "g", "h");
    final FacetResult beyond = facet("SELECT FROM " + TYPE + " FACETS f: brand TERMS TOP 3 AFTER 'zz'");
    assertThat(beyond.buckets()).isEmpty();
    assertThat(beyond.hasAfter()).isTrue();
    assertThat(beyond.after()).isNull();
  }

  @Test
  @Timeout(120)
  void lastPageHasNullAfter() {
    load(1);
    final FacetResult full = facet("SELECT FROM " + TYPE + " FACETS f: brand TERMS TOP 3 AFTER 'v'");
    assertThat(pairs(full.buckets())).extracting(p -> p.getFirst()).containsExactly("w", "x", "y");
    assertThat(full.after()).isEqualTo("y");
    // exactly the last values: no value follows, so no cursor
    final FacetResult last = facet("SELECT FROM " + TYPE + " FACETS f: brand TERMS TOP 3 AFTER 'w'");
    assertThat(pairs(last.buckets())).extracting(p -> p.getFirst()).containsExactly("x", "y", "z");
    assertThat(last.hasAfter()).isTrue();
    assertThat(last.after()).isNull();
    assertThat(last.toJSON().has("after")).isTrue();
    assertThat(last.toJSON().isNull("after")).isTrue();
    // otherCount counts every matched value outside the page, before the cursor too
    final long all = oracle(TYPE, "brand", 1).stream().mapToLong(p -> (Long) p.get(1)).sum();
    final long page = last.buckets().stream().mapToLong(FacetBucket::count).sum();
    assertThat(last.otherCount()).isEqualTo(all - page);
  }

  @Test
  @Timeout(120)
  void minCountStillApplies() {
    load(1);
    final List<List<Object>> expected = oracle(TYPE, "brand", 3);
    assertThat(expected).hasSizeBetween(5, 20);
    assertThat(pairs(pageAll(TYPE, "brand", " MIN_COUNT 3", 4, "index"))).containsExactlyElementsOf(expected);
    // "y" and "z" follow the last value that qualifies, below MIN_COUNT: no cursor after it
    final Object lastQualifying = expected.getLast().getFirst();
    final FacetResult tail = facets("SELECT FROM " + TYPE + " FACETS f: brand TERMS TOP 1 MIN_COUNT 3 AFTER :c",
        expected.get(expected.size() - 2).getFirst()).get("f");
    assertThat(pairs(tail.buckets())).extracting(p -> p.getFirst()).containsExactly(lastQualifying);
    assertThat(tail.after()).isNull();
  }

  @Test
  @Timeout(120)
  void afterOnNumericTermsComparesNumerically() {
    load(1);
    final List<Object> above = new ArrayList<>();
    for (final List<Object> pair : oracle(TYPE, "fileSize", 1))
      if ((Long) pair.getFirst() > 1000L)
        above.add(pair.getFirst());
    assertThat(above).isNotEmpty();
    assertThat(values("SELECT FROM " + TYPE + " FACETS f: fileSize TERMS TOP 1000 AFTER 1000", null)).containsExactlyElementsOf(above);
    // a fractional cursor on integral terms: 999.5 < 1000
    assertThat(values("SELECT FROM " + TYPE + " FACETS f: fileSize TERMS TOP 1 AFTER 999.5", null)).containsExactly(1000L);
    // a negative one: -100 > -100.5, which truncating the cursor to -100 would skip
    assertThat(values("SELECT FROM " + TYPE + " FACETS f: fileSize TERMS TOP 1 AFTER -100.5", null)).containsExactly(-100L);
    // a cursor typed as a string is converted with the property's type
    assertThat(values("SELECT FROM " + TYPE + " FACETS f: fileSize TERMS TOP 1 AFTER '999'", null)).containsExactly(1000L);
    assertThat(pairs(pageAll(TYPE, "fileSize", "", 7, "index"))).containsExactlyElementsOf(oracle(TYPE, "fileSize", 1));

    // double terms are counted by their bit patterns, where -2.0 sorts above -1.0: compared as numbers
    final List<Object> scoresAbove = new ArrayList<>();
    for (final List<Object> pair : oracle(TYPE, "score", 1))
      if ((Double) pair.getFirst() > -1.5d)
        scoresAbove.add(pair.getFirst());
    assertThat(scoresAbove).contains(-0.75d, 0.25d);
    assertThat(values("SELECT FROM " + TYPE + " FACETS f: score TERMS TOP 1000 AFTER -1.5", null)).containsExactlyElementsOf(scoresAbove);
    assertThat(pairs(pageAll(TYPE, "score", "", 4, "index"))).containsExactlyElementsOf(oracle(TYPE, "score", 1));
  }

  @Test
  @Timeout(120)
  void multiBucketPagesInValueOrder() {
    load(3);
    assertThat(database.getSchema().getType(TYPE).getBuckets(false)).hasSize(3);
    final List<List<Object>> expected = oracle(TYPE, "brand", 1);
    assertThat(expected).hasSize(26);
    assertThat(pairs(pageAll(TYPE, "brand", "", 4, "index"))).containsExactlyElementsOf(expected);
    assertThat(pairs(pageAll(TYPE, "fileSize", "", 5, "index"))).containsExactlyElementsOf(oracle(TYPE, "fileSize", 1));
    assertThat(values("SELECT FROM " + TYPE + " FACETS f: brand TERMS TOP 3 AFTER 'cz'", null)).containsExactly("d", "e", "f");
  }

  @Test
  @Timeout(300)
  void unrankedTailPagesInValueOrder() {
    createSchema(TYPE, 1);
    // 1100 even names, then (after the rank view is built) 30 odd ones that fall in its gaps
    database.transaction(() -> {
      for (int i = 0; i < 1_100; i++)
        insert(TYPE, String.format("v%05d", i * 2), i);
    });
    FacetOracle.awaitCaughtUp(database);
    final GlobalDictionary dictionary = manager().projection(database.getSchema().getType(TYPE).getBuckets(false).getFirst().getFileId())
        .dictionary("brand");
    dictionary.rebuildRankView();
    assertThat(dictionary.rankViewSize()).isEqualTo(1_100);
    database.transaction(() -> {
      for (int i = 0; i < 30; i++)
        insert(TYPE, String.format("v%05d", i * 74 + 1), i);
    });
    FacetOracle.awaitCaughtUp(database);
    assertThat(dictionary.size()).isEqualTo(1_130);
    assertThat(dictionary.rankViewSize()).isEqualTo(1_100);

    final List<List<Object>> expected = oracle(TYPE, "brand", 1);
    assertThat(expected).hasSize(1_130);
    assertThat(pairs(pageAll(TYPE, "brand", "", 50, "index"))).containsExactlyElementsOf(expected);
    assertThat(pairs(pageAll(TYPE, "brand", "", 7, "index"))).containsExactlyElementsOf(expected);
    // cursors on a ranked value, on an unranked one, and on no value at all, inside a gap that holds unranked values
    assertThat(values("SELECT FROM " + TYPE + " FACETS f: brand TERMS TOP 3 AFTER 'v00072'", null)).containsExactly("v00074", "v00075",
        "v00076");
    assertThat(values("SELECT FROM " + TYPE + " FACETS f: brand TERMS TOP 3 AFTER 'v00073'", null)).containsExactly("v00074", "v00075",
        "v00076");
    assertThat(values("SELECT FROM " + TYPE + " FACETS f: brand TERMS TOP 3 AFTER 'v00074'", null)).containsExactly("v00075", "v00076",
        "v00078");
    assertThat(values("SELECT FROM " + TYPE + " FACETS f: brand TERMS TOP 3 AFTER 'v00074a'", null)).containsExactly("v00075", "v00076",
        "v00078");
  }

  @Test
  @Timeout(120)
  void dateCursorEchoedAsString() {
    load(1);
    final DateTimeFormatter format = DateTimeFormatter.ofPattern(database.getSchema().getDateTimeFormat());
    final List<List<Object>> expected = oracle(TYPE, "publishDate", 1);
    assertThat(expected).hasSizeGreaterThan(20);
    // the cursor is the row type, and a client echoing it as the row's formatted string pages on without skip or repeat
    final FacetResult first = facet("SELECT FROM " + TYPE + " FACETS f: publishDate TERMS TOP 4 ORDER BY VALUE");
    assertThat(first.after()).isInstanceOf(LocalDateTime.class);
    assertThat(pairs(pageAll(TYPE, "publishDate", "", 4, "index", c -> format.format((LocalDateTime) c)))).containsExactlyElementsOf(
        expected);
    assertThat(pairs(pageAll(TYPE, "publishDate", "", 4, "index"))).containsExactlyElementsOf(expected);
    // typed by hand, not a value of the property
    final LocalDateTime cursor = LocalDateTime.of(2021, 1, 10, 0, 30);
    final List<Object> after = new ArrayList<>();
    for (final List<Object> pair : expected)
      if (((LocalDateTime) pair.getFirst()).isAfter(cursor) && after.size() < 3)
        after.add(pair.getFirst());
    assertThat(values("SELECT FROM " + TYPE + " FACETS f: publishDate TERMS TOP 3 AFTER '2021-01-10 00:30:00'", null))
        .containsExactlyElementsOf(after);
  }

  @Test
  @Timeout(120)
  void afterWithIncludeOrDescIsRefused() {
    load(1);
    assertThatThrownBy(() -> facet("SELECT FROM " + TYPE + " FACETS f: brand TERMS INCLUDE ['a'] AFTER 'b'"))
        .isInstanceOf(CommandSQLParsingException.class).hasMessage("AFTER and INCLUDE cannot be combined");
    assertThatThrownBy(() -> facet("SELECT FROM " + TYPE + " FACETS f: brand TERMS ORDER BY VALUE DESC AFTER 'b'"))
        .isInstanceOf(CommandSQLParsingException.class).hasMessage("AFTER pages in ascending value order: remove ORDER BY VALUE DESC");
    assertThatThrownBy(() -> facet("SELECT FROM " + TYPE + " FACETS f: brand TERMS ORDER BY COUNT AFTER 'b'"))
        .isInstanceOf(CommandSQLParsingException.class).hasMessage("AFTER pages in value order: remove ORDER BY COUNT");
  }

  /**
   * In value order the records without a value sort last: the MISSING bucket is returned once, outside TOP, on the last page (the one
   * whose cursor is null), never on an earlier one, so a paging loop sees every value once and MISSING once.
   */
  @Test
  @Timeout(120)
  void missingOnceOnLastPage() {
    load(1);
    final List<List<Object>> expected = new ArrayList<>(oracle(TYPE, "brand", 1));
    expected.add(List.of("<missing>", 3L));
    for (final int top : new int[] { 1, 3, 10, 26, 30 }) {
      final List<List<Object>> seen = new ArrayList<>();
      Object cursor = null;
      for (int page = 0; page < PAGES_MAX; page++) {
        final String sql = "SELECT FROM " + TYPE + " FACETS f: brand TERMS TOP " + top + " MISSING" + (cursor == null ?
            " ORDER BY VALUE" :
            " AFTER :c");
        final FacetResult result = facets(sql, cursor).get("f");
        final List<List<Object>> buckets = pairs(result.buckets());
        seen.addAll(buckets);
        // the count stays in the result on every page
        assertThat(result.missing()).as(sql).isEqualTo(3L);
        cursor = result.after();
        if (cursor == null) {
          assertThat(buckets).as(sql).last().isEqualTo(List.of("<missing>", 3L));
          break;
        }
        assertThat(buckets).as(sql).hasSize(top).extracting(List::getFirst).doesNotContain("<missing>");
      }
      assertThat(seen).as("TOP " + top).containsExactlyElementsOf(expected);
    }
    // outside TOP: the last page holds its TOP values and the bucket besides
    final FacetResult last = facet("SELECT FROM " + TYPE + " FACETS f: brand TERMS TOP 3 MISSING AFTER 'w'");
    assertThat(pairs(last.buckets())).extracting(List::getFirst).containsExactly("x", "y", "z", "<missing>");
    assertThat(last.after()).isNull();
    final FacetResult notLast = facet("SELECT FROM " + TYPE + " FACETS f: brand TERMS TOP 3 MISSING AFTER 'v'");
    assertThat(pairs(notLast.buckets())).extracting(List::getFirst).containsExactly("w", "x", "y");
  }

  /**
   * A cursor that is not a value of the property is refused with one error whatever path counts the facet: a projected LONG, DOUBLE or
   * DATETIME column, and the records of an unindexed supertype read by value. Returning page 1 instead would loop a client forever.
   */
  @Test
  @Timeout(120)
  void unconvertibleCursorIsRefusedOnEveryPath() {
    load(1);
    for (final String[] property : new String[][] { { "fileSize", "LONG" }, { "score", "DOUBLE" }, { "publishDate", "DATETIME" } })
      assertThatThrownBy(() -> facet("SELECT FROM " + TYPE + " FACETS f: " + property[0] + " TERMS TOP 3 AFTER 'abc'"))
          .isInstanceOf(CommandExecutionException.class)
          .hasMessage("FACETS f: the AFTER cursor 'abc' is not a value of '" + property[0] + "' (" + property[1] + ")");

    database.command("sql", "CREATE DOCUMENT TYPE Media");
    database.command("sql", "CREATE PROPERTY Media.publishDate DATETIME");
    database.command("sql", "CREATE PROPERTY Media.size LONG");
    database.command("sql", "CREATE DOCUMENT TYPE Photo EXTENDS Media");
    database.command("sql", "CREATE INDEX ON Photo (publishDate) COLUMNAR");
    database.transaction(() -> {
      database.newDocument("Media").set("publishDate", LocalDateTime.of(2022, 1, 1, 0, 0)).set("size", 5L).save();
      database.newDocument("Photo").set("publishDate", LocalDateTime.of(2022, 1, 2, 0, 0)).set("size", 7L).save();
    });
    FacetOracle.awaitCaughtUp(database);
    assertThatThrownBy(() -> facet("SELECT FROM Media FACETS f: publishDate TERMS TOP 3 AFTER 'abc'"))
        .isInstanceOf(CommandExecutionException.class)
        .hasMessage("FACETS f: the AFTER cursor 'abc' is not a value of 'publishDate' (DATETIME)");
    // no index anywhere: the raw values of the declared LONG
    assertThatThrownBy(() -> facet("SELECT FROM Media FACETS f: size TERMS TOP 3 AFTER 'abc'"))
        .isInstanceOf(CommandExecutionException.class).hasMessage("FACETS f: the AFTER cursor 'abc' is not a value of 'size' (LONG)");
    // a well-formed cursor on the same paths still pages
    assertThat(values("SELECT FROM Media FACETS f: publishDate TERMS TOP 3 AFTER '2022-01-01 00:00:00'", null)).containsExactly(
        LocalDateTime.of(2022, 1, 2, 0, 0));
    assertThat(values("SELECT FROM Media FACETS f: size TERMS TOP 3 AFTER '5'", null)).containsExactly(7L);
  }

  @Test
  @Timeout(120)
  void laggingValuesPageInValueOrder() throws Exception {
    load(1);
    try (final AutoCloseable hold = manager().suspendApply()) {
      // values only the patched records hold, some before and some after existing ones
      database.transaction(() -> {
        int j = 1_000;
        for (final String brand : List.of("c2", "m", "m2", "zz", "0", "c2"))
          insert(TYPE, brand, j++);
        database.newDocument(TYPE).set("fileSize", 123_456L).set("score", -99.5d).save();
      });
      final List<List<Object>> expected = oracle(TYPE, "brand", 1);
      assertThat(expected).hasSize(30);
      assertThat(pairs(pageAll(TYPE, "brand", "", 4, "index"))).containsExactlyElementsOf(expected);
      assertThat(pairs(pageAll(TYPE, "fileSize", "", 6, "index"))).containsExactlyElementsOf(oracle(TYPE, "fileSize", 1));
      assertThat(pairs(pageAll(TYPE, "score", "", 6, "index"))).containsExactlyElementsOf(oracle(TYPE, "score", 1));
      assertThat(facets("SELECT FROM " + TYPE + " FACETS f: brand TERMS TOP 4 AFTER 'c'", null).patched()).isGreaterThan(0);
    }
  }

  @Test
  @Timeout(120)
  void ownTransactionScannedBucketPagesInValueOrder() {
    load(1);
    // no patch budget: the bucket the transaction wrote is read from its records rather than patched
    final Object budget = GlobalConfiguration.COLUMNAR_PATCH_BUDGET.getValue();
    final Object ratio = GlobalConfiguration.COLUMNAR_PATCH_BUDGET_RATIO.getValue();
    GlobalConfiguration.COLUMNAR_PATCH_BUDGET.setValue(0);
    GlobalConfiguration.COLUMNAR_PATCH_BUDGET_RATIO.setValue(0d);
    database.begin();
    try {
      int j = 2_000;
      for (final String brand : List.of("c2", "m2", "zz"))
        insert(TYPE, brand, j++);
      final List<List<Object>> expected = oracle(TYPE, "brand", 1);
      assertThat(expected).hasSize(29);
      // the bucket the transaction wrote is read from its records: every value arrives by value
      assertThat(pairs(pageAll(TYPE, "brand", "", 5, "scan"))).containsExactlyElementsOf(expected);
      assertThat(pairs(pageAll(TYPE, "publishDate", "", 5, "scan"))).containsExactlyElementsOf(oracle(TYPE, "publishDate", 1));
      assertThat(pairs(pageAll(TYPE, "score", "", 5, "scan"))).containsExactlyElementsOf(oracle(TYPE, "score", 1));
    } finally {
      database.rollback();
      GlobalConfiguration.COLUMNAR_PATCH_BUDGET.setValue(budget);
      GlobalConfiguration.COLUMNAR_PATCH_BUDGET_RATIO.setValue(ratio);
    }
  }

  @Test
  @Timeout(120)
  void acrossDeclaringTypesPagesInValueOrder() {
    database.command("sql", "CREATE DOCUMENT TYPE Media");
    database.command("sql", "CREATE PROPERTY Media.brand STRING");
    database.command("sql", "CREATE PROPERTY Media.publishDate DATETIME");
    for (final String subtype : List.of("Photo", "Video")) {
      database.command("sql", "CREATE DOCUMENT TYPE " + subtype + " EXTENDS Media");
      database.command("sql", "CREATE INDEX ON " + subtype + " (brand) COLUMNAR");
      database.command("sql", "CREATE INDEX ON " + subtype + " (publishDate) COLUMNAR");
    }
    final int[] j = { 0 };
    database.transaction(() -> {
      for (int b = 0; b < 26; b++) {
        final String brand = String.valueOf((char) ('a' + b));
        // a value of one declaring type or of both
        if (b % 3 != 1)
          database.newDocument("Photo").set("brand", brand).set("publishDate", LocalDateTime.of(2022, 3, 1, 0, 0).plusHours(j[0]++ * 5)).save();
        if (b % 3 != 0)
          database.newDocument("Video").set("brand", brand).set("publishDate", LocalDateTime.of(2022, 3, 1, 0, 0).plusHours(j[0]++ * 5)).save();
      }
    });
    FacetOracle.awaitCaughtUp(database);
    final DateTimeFormatter format = DateTimeFormatter.ofPattern(database.getSchema().getDateTimeFormat());
    // two declaring types, each with its own dictionary and date column: merged by value
    assertThat(oracle("Media", "brand", 1)).hasSize(26);
    assertThat(pairs(pageAll("Media", "brand", "", 4, "index"))).containsExactlyElementsOf(oracle("Media", "brand", 1));
    assertThat(pairs(pageAll("Media", "publishDate", "", 4, "index", c -> format.format((LocalDateTime) c)))).containsExactlyElementsOf(
        oracle("Media", "publishDate", 1));

    // records of the unindexed supertype itself, read from their records: the merge converts every term to the value it reads back as
    database.transaction(() -> {
      for (final String brand : List.of("b", "bb", "y", "zz"))
        database.newDocument("Media").set("brand", brand).set("publishDate", LocalDateTime.of(2022, 3, 1, 2, 30).plusHours(j[0]++ * 7)).save();
    });
    FacetOracle.awaitCaughtUp(database);
    assertThat(oracle("Media", "brand", 1)).hasSize(28);
    assertThat(pairs(pageAll("Media", "brand", "", 4, "scan"))).containsExactlyElementsOf(oracle("Media", "brand", 1));
    assertThat(pairs(pageAll("Media", "publishDate", "", 4, "scan"))).containsExactlyElementsOf(oracle("Media", "publishDate", 1));
    assertThat(pairs(pageAll("Media", "publishDate", "", 3, "scan", c -> format.format((LocalDateTime) c)))).containsExactlyElementsOf(
        oracle("Media", "publishDate", 1));
  }

  @Test
  @Timeout(120)
  void valueInsertedBetweenPagesAppearsOnlyAfterCursor() {
    load(1);
    final FacetResult first = facet("SELECT FROM " + TYPE + " FACETS f: brand TERMS TOP 5 ORDER BY VALUE");
    assertThat(first.after()).isEqualTo("e");
    final List<FacetBucket> seen = new ArrayList<>(first.buckets());
    database.transaction(() -> {
      insert(TYPE, "c2", 3_000);
      insert(TYPE, "f2", 3_001);
      insert(TYPE, "f2", 3_002);
    });
    FacetOracle.awaitCaughtUp(database);
    Object cursor = first.after();
    for (int page = 0; cursor != null && page < PAGES_MAX; page++) {
      final FacetResult result = facets("SELECT FROM " + TYPE + " FACETS f: brand TERMS TOP 5 AFTER :c", cursor).get("f");
      seen.addAll(result.buckets());
      cursor = result.after();
    }
    final List<List<Object>> expected = new ArrayList<>();
    for (final List<Object> pair : oracle(TYPE, "brand", 1))
      if (!"c2".equals(pair.getFirst()))
        expected.add(pair);
    // "c2" sorts before the cursor: never seen; "f2" after it: on a later page, once
    assertThat(pairs(seen)).containsExactlyElementsOf(expected);
    assertThat(pairs(seen)).contains(List.of("f2", 2L));
  }
}
