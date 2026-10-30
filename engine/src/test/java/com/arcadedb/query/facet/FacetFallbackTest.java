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
import com.arcadedb.database.MutableDocument;
import com.arcadedb.index.columnar.ColumnarIndexManager;
import com.arcadedb.index.columnar.ColumnarTestData;
import com.arcadedb.index.columnar.ColumnarTestHooks;
import com.arcadedb.index.columnar.FacetOracle;
import com.arcadedb.query.sql.antlr.SQLAntlrParser;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.parser.SelectStatement;
import com.arcadedb.utility.DateUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The fallback scan (spec 7.8, D9, D14): a bucket the projection cannot answer for a facet (no COLUMNAR index on the property, an
 * expression, a projection being rebuilt, a column not published yet) is counted from its matched records, exactly as GROUP BY; the
 * scans of a database are capped, and a facet that needed a refused scan is unavailable.
 */
class FacetFallbackTest extends TestHelper {
  private static final String       TYPE   = ColumnarTestData.TYPE;
  private static final long         MIB    = 1_048_576L;
  private static final int          ALL    = 1_000;
  private static final List<String> WHERES = Arrays.asList(null, "fileSize < 1048576", "author = 'author-7'", "tags CONTAINS 'tag-5'");
  private static final String       CASE   = "(CASE WHEN fileSize < 1048576 THEN 'small' WHEN fileSize < 2097152 THEN 'medium' ELSE 'large' END)";

  private ColumnarIndexManager manager() {
    return ((LocalDatabase) ((DatabaseInternal) database).getEmbedded()).getColumnarIndexManager();
  }

  /** The fixture data with only a COLUMNAR index on brand: the other properties have no projection. */
  private ColumnarTestData brandOnly() {
    final ColumnarTestData data = new ColumnarTestData(13);
    data.tagVocabulary = 300;
    data.columnarIndexes = false;
    data.createSchema(database);
    database.command("sql", "CREATE INDEX ON " + TYPE + " (brand) COLUMNAR");
    return data;
  }

  private MatchSet matches(final String type, final String where, final int facetCount) {
    final MatchSet.Builder builder = MatchSet.builder((DatabaseInternal) database, 0, 0);
    try (builder; final ResultSet rs = database.query("sql", "SELECT @rid AS rid FROM " + type + (where == null ? "" : " WHERE " + where))) {
      while (rs.hasNext())
        builder.add(rs.next().<Identifiable>getProperty("rid").getIdentity(), 0);
      final MatchSet matches = builder.build();
      final int[] noFilter = new int[facetCount];
      Arrays.fill(noFilter, -1);
      matches.setFacetFilterIndex(noFilter);
      return matches;
    }
  }

  private FacetResults execute(final String type, final String where, final List<FacetSpec> facets) {
    try (final MatchSet matches = matches(type, where, facets.size())) {
      assertThat(matches.size()).as("records of " + type + " matching " + where).isPositive();
      // the query's context, as the planner passes it: expressions are evaluated in it
      return FacetExecutor.execute((DatabaseInternal) database, matches, facets, new BasicCommandContext().setDatabase(database));
    }
  }

  private static TermsFacetSpec terms(final String property) {
    return new TermsFacetSpec(property, property, null, null, ALL, 1, FacetOrder.COUNT_DESC, null, false, null);
  }

  private static RangeFacetSpec sizeRanges() {
    return new RangeFacetSpec("s", "fileSize", null, null,
        List.of(new RangeBound(null, MIB, "small"), new RangeBound(MIB, 2 * MIB, "medium"), new RangeBound(2 * MIB, null, "large")), null);
  }

  /** The facets of a FACETS clause, as the planner compiles them (expressions included). */
  private static List<FacetSpec> compile(final String facets) {
    final SelectStatement select = (SelectStatement) new SQLAntlrParser(null).parse("SELECT FROM A FACETS " + facets);
    return FacetSpecCompiler.compile(select.getFacets(), new BasicCommandContext());
  }

  /** {@code SELECT <key> AS k, count(*) AS c ... GROUP BY k}: the counts by key, null keys left out. */
  private Map<Object, Long> groupBy(final String key, final String type, final String where) {
    return countsOf("SELECT " + key + " AS k, count(*) AS c FROM " + type + (where == null ? "" : " WHERE " + where) + " GROUP BY k");
  }

  /** GROUP BY of the distinct items of a list property: each item once per document. */
  private Map<Object, Long> groupByItem(final String property, final String type, final String where) {
    return countsOf("SELECT k, count(*) AS c FROM (SELECT " + property + ".asSet() AS k FROM " + type + (where == null ? "" : " WHERE " + where)
        + " UNWIND k) GROUP BY k");
  }

  private Map<Object, Long> countsOf(final String sql) {
    final Map<Object, Long> counts = new HashMap<>();
    try (final ResultSet rs = database.query("sql", sql)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        if (row.getProperty("k") != null)
          counts.put(row.getProperty("k"), row.<Number>getProperty("c").longValue());
      }
    }
    return counts;
  }

  /** The records whose list property holds no item that is not null (absent, empty, or only nulls). */
  private long withoutItems(final String property, final String type, final String where) {
    long missing = 0;
    try (final ResultSet rs = database.query("sql", "SELECT " + property + " AS v FROM " + type + (where == null ? "" : " WHERE " + where))) {
      while (rs.hasNext()) {
        final Object value = rs.next().getProperty("v");
        boolean any = false;
        if (value instanceof Iterable<?> items)
          for (final Object item : items)
            any |= item != null;
        else
          any = value != null;
        if (!any)
          missing++;
      }
    }
    return missing;
  }

  private long count(final String type, final String where) {
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM " + type + (where == null ? "" : " WHERE " + where))) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }

  private static String and(final String where, final String condition) {
    return where == null ? condition : "(" + where + ") AND " + condition;
  }

  private static Map<Object, Long> termCounts(final FacetResult result) {
    final Map<Object, Long> counts = new HashMap<>();
    for (final FacetBucket bucket : result.buckets())
      if (!bucket.isMissing())
        assertThat(counts.put(bucket.value(), bucket.count())).as("value " + bucket.value() + " returned twice").isNull();
    return counts;
  }

  private static Map<String, Long> rangeCounts(final FacetResult result) {
    final Map<String, Long> counts = new HashMap<>();
    for (final FacetBucket bucket : result.buckets())
      counts.put(bucket.key(), bucket.count());
    return counts;
  }

  /** The terms facet equals GROUP BY of the property over the WHERE, and its missing count the records without a value. */
  private void assertTermsEqualGroupBy(final FacetResult result, final String key, final String type, final String where) {
    final Map<Object, Long> expected = groupBy(key, type, where);
    assertThat(expected).as(key + " WHERE " + where + " groups records").isNotEmpty();
    assertThat(termCounts(result)).as(key + " WHERE " + where).isEqualTo(expected);
    assertThat(result.missing()).as(key + " missing WHERE " + where).isEqualTo(count(type, and(where, key + " IS NULL")));
  }

  private void assertSizeRangesEqualGroupBy(final FacetResult result, final String where) {
    final Map<Object, Long> expected = groupBy(CASE, TYPE, and(where, "fileSize IS NOT NULL"));
    assertThat(expected).as("WHERE " + where + " matches records").isNotEmpty();
    final Map<String, Long> counts = rangeCounts(result);
    for (final String key : List.of("small", "medium", "large"))
      assertThat(counts.get(key)).as(key + " WHERE " + where).isEqualTo(expected.getOrDefault(key, 0L));
    assertThat(result.missing()).isEqualTo(count(TYPE, and(where, "fileSize IS NULL")));
  }

  /** Holds every rebuild that reaches the end of its scan until {@code release} is counted down. */
  private static void holdRebuildScans(final CountDownLatch reached, final CountDownLatch release) {
    ColumnarTestHooks.killPoint = name -> {
      if ("rebuild.scanned".equals(name)) {
        reached.countDown();
        try {
          release.await(120, TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    };
  }

  // ------------------------------------------------------------------------------------------------------------------- tests

  @Test
  @Timeout(300)
  void unindexedPropertyIsScanned() {
    final ColumnarTestData data = brandOnly();
    data.insertAssets(database, 2_000);
    FacetOracle.awaitCaughtUp(database);

    for (final String where : WHERES) {
      final long scansBefore = manager().getStats().queryFallbackScansCount();
      final FacetResults results = execute(TYPE, where, List.of(terms("author"), terms("brand")));
      final FacetResult author = results.get("author");
      assertThat(author.source()).as("WHERE " + where).isEqualTo("scan");
      assertThat(results.executionStats().get("author").source()).isEqualTo("scan");
      assertThat(results.exact()).isTrue();
      assertTermsEqualGroupBy(author, "author", TYPE, where);
      // the projected facet of the same query is not affected
      assertThat(results.get("brand").source()).isEqualTo("index");
      assertTermsEqualGroupBy(results.get("brand"), "brand", TYPE, where);
      // one scan permit for the query, released afterwards
      assertThat(manager().getStats().queryFallbackScansCount()).isEqualTo(scansBefore + 1);
      assertThat(manager().fallbackScans().availablePermits()).isEqualTo(2);
    }
  }

  @Test
  @Timeout(300)
  void unindexedListCountsEachDistinctItemOnce() {
    final ColumnarTestData data = brandOnly();
    data.insertAssets(database, 2_000);
    // records without tags and with an empty list: each one missing document
    database.transaction(() -> {
      for (int i = 0; i < 9; i++) {
        final MutableDocument record = database.newDocument(TYPE).set("brand", "brand-1").set("fileSize", 10L).set("author", "author-7");
        if (i % 2 == 1)
          record.set("tags", new ArrayList<>());
        record.save();
      }
    });
    FacetOracle.awaitCaughtUp(database);

    for (final String where : WHERES) {
      final FacetResult tags = execute(TYPE, where, List.of(terms("tags"))).get("tags");
      assertThat(tags.source()).isEqualTo("scan");
      final Map<Object, Long> expected = groupByItem("tags", TYPE, where);
      assertThat(expected).as("WHERE " + where + " has tags").isNotEmpty();
      assertThat(termCounts(tags)).as("WHERE " + where).isEqualTo(expected);
      final long missing = withoutItems("tags", TYPE, where);
      if (!"tags CONTAINS 'tag-5'".equals(where))
        assertThat(missing).as("WHERE " + where + " matches records without tags").isGreaterThanOrEqualTo(9);
      assertThat(tags.missing()).as("missing WHERE " + where).isEqualTo(missing);
    }
  }

  @Test
  @Timeout(300)
  void expressionFacetIsScanned() {
    final ColumnarTestData data = new ColumnarTestData(13);
    data.tagVocabulary = 300;
    data.createSchema(database);
    data.insertAssets(database, 2_000);
    FacetOracle.awaitCaughtUp(database);

    // upper case as well: its values differ from the raw property, so a facet that read the property instead would fail
    final List<FacetSpec> facets = compile("x: brand.toLowerCase() TERMS TOP 1000, y: brand.toUpperCase() TERMS TOP 1000");
    assertThat(facets.getFirst().property()).isNull();
    for (final String where : WHERES) {
      final FacetResults results = execute(TYPE, where, facets);
      assertThat(results.get("x").source()).isEqualTo("scan");
      assertThat(results.get("y").source()).isEqualTo("scan");
      assertThat(results.exact()).isTrue();
      assertThat(termCounts(results.get("x"))).as("WHERE " + where).isEqualTo(groupBy("brand.toLowerCase()", TYPE, where));
      final Map<Object, Long> upper = groupBy("brand.toUpperCase()", TYPE, where);
      assertThat(upper).isNotEmpty();
      assertThat(termCounts(results.get("y"))).as("WHERE " + where).isEqualTo(upper);
    }
  }

  @Test
  @Timeout(300)
  void rebuildingProjectionIsScanned() throws InterruptedException {
    final ColumnarTestData data = new ColumnarTestData(13);
    data.tagVocabulary = 300;
    data.createSchema(database);
    data.insertAssets(database, 2_000);
    FacetOracle.awaitCaughtUp(database);
    database.command("sql", "DROP INDEX `" + TYPE + "[brand]_columnar`");
    FacetOracle.awaitCaughtUp(database);

    final CountDownLatch reached = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    holdRebuildScans(reached, release);
    try {
      database.command("sql", "CREATE INDEX ON " + TYPE + " (brand) COLUMNAR");
      assertThat(reached.await(60, TimeUnit.SECONDS)).as("the create rebuild reached the end of its scan").isTrue();
      for (final String where : WHERES) {
        final FacetResults results = execute(TYPE, where, List.of(terms("brand")));
        assertThat(results.get("brand").source()).as("WHERE " + where).isEqualTo("scan");
        assertThat(results.exact()).isTrue();
        assertTermsEqualGroupBy(results.get("brand"), "brand", TYPE, where);
      }
    } finally {
      ColumnarTestHooks.killPoint = null;
      release.countDown();
    }
    FacetOracle.awaitCaughtUp(database);

    // published: counted from the projection again, with the same counts
    for (final String where : WHERES) {
      final FacetResult brand = execute(TYPE, where, List.of(terms("brand"))).get("brand");
      assertThat(brand.source()).isEqualTo("index");
      assertTermsEqualGroupBy(brand, "brand", TYPE, where);
    }
  }

  @Test
  @Timeout(300)
  void capReachedIsUnavailable() {
    final ColumnarTestData data = brandOnly();
    data.insertAssets(database, 1_000);
    FacetOracle.awaitCaughtUp(database);

    final Semaphore permits = manager().fallbackScans();
    final int held = permits.drainPermits();
    assertThat(held).isEqualTo(2);
    final long scansBefore = manager().getStats().queryFallbackScansCount();
    try {
      final FacetResults results = execute(TYPE, null, List.of(terms("author"), terms("brand"), sizeRanges()));
      for (final String alias : List.of("author", "s")) {
        final FacetResult result = results.get(alias);
        assertThat(result.source()).as(alias).isEqualTo(FacetResult.UNAVAILABLE);
        assertThat(result.reason()).as(alias).isEqualTo("fallback scans capped");
        assertThat(result.buckets()).as(alias).isEmpty();
        assertThat(results.executionStats().get(alias).source()).isEqualTo(FacetResult.UNAVAILABLE);
      }
      // a facet the projection answers does not need the permit
      assertThat(results.get("brand").source()).isEqualTo("index");
      assertTermsEqualGroupBy(results.get("brand"), "brand", TYPE, null);
      assertThat(manager().getStats().queryFallbackScansCount()).isEqualTo(scansBefore);
    } finally {
      permits.release(held);
    }
    // a permit back: scanned again
    assertThat(execute(TYPE, null, List.of(terms("author"))).get("author").source()).isEqualTo("scan");
  }

  @Test
  @Timeout(300)
  void mixedIndexAndScanIsScan() {
    database.command("sql", "CREATE DOCUMENT TYPE Base");
    database.command("sql", "CREATE PROPERTY Base.brand STRING");
    database.command("sql", "CREATE DOCUMENT TYPE Leaf EXTENDS Base");
    database.command("sql", "CREATE INDEX ON Leaf (brand) COLUMNAR");
    FacetOracle.awaitCaughtUp(database);
    database.transaction(() -> {
      for (int i = 0; i < 1_500; i++) {
        final MutableDocument leaf = database.newDocument("Leaf");
        if (i % 11 != 0)
          leaf.set("brand", "brand-" + i % 37);
        leaf.save();
      }
      for (int i = 0; i < 700; i++) {
        final MutableDocument base = database.newDocument("Base");
        if (i % 13 != 0)
          base.set("brand", "brand-" + i % 53);
        base.save();
      }
    });
    FacetOracle.awaitCaughtUp(database);

    for (final String where : Arrays.asList(null, "brand IN ['brand-1', 'brand-40', 'brand-52']")) {
      final FacetResults base = execute("Base", where, List.of(terms("brand")));
      assertThat(base.get("brand").source()).as("WHERE " + where).isEqualTo("scan");
      assertThat(base.exact()).isTrue();
      assertTermsEqualGroupBy(base.get("brand"), "brand", "Base", where);
      // values only Base has (brand-40 and up) and values of both types merge
      assertThat(termCounts(base.get("brand"))).containsKey("brand-40");

      final FacetResults leaf = execute("Leaf", where, List.of(terms("brand")));
      assertThat(leaf.get("brand").source()).isEqualTo("index");
      assertTermsEqualGroupBy(leaf.get("brand"), "brand", "Leaf", where);
    }
  }

  @Test
  @Timeout(300)
  void memoryBudgetRefusalIsUnavailable() {
    final ColumnarTestData data = brandOnly();
    data.insertAssets(database, 1_000);
    FacetOracle.awaitCaughtUp(database);

    final Object budget = GlobalConfiguration.COLUMNAR_QUERY_MEMORY_BUDGET.getValue();
    GlobalConfiguration.COLUMNAR_QUERY_MEMORY_BUDGET.setValue(1024L);
    try (final MatchSet matches = matches(TYPE, null, 2)) {
      final FacetResults results = FacetExecutor.execute((DatabaseInternal) database, matches, List.of(terms("author"), terms("brand")),
          new BasicCommandContext());
      for (final String alias : List.of("author", "brand")) {
        assertThat(results.get(alias).source()).as(alias).isEqualTo(FacetResult.UNAVAILABLE);
        assertThat(results.get(alias).reason()).as(alias).isEqualTo("memory budget");
      }
    } finally {
      GlobalConfiguration.COLUMNAR_QUERY_MEMORY_BUDGET.setValue(budget);
    }
  }

  @Test
  void capFollowsTheSetting() {
    assertThat(manager().fallbackScans().availablePermits()).isEqualTo(GlobalConfiguration.COLUMNAR_MAX_FALLBACK_SCANS.getValueAsInteger());
    final Object before = GlobalConfiguration.COLUMNAR_MAX_FALLBACK_SCANS.getValue();
    GlobalConfiguration.COLUMNAR_MAX_FALLBACK_SCANS.setValue(1);
    try {
      reopenDatabase();
      assertThat(manager().fallbackScans().availablePermits()).isEqualTo(1);
    } finally {
      GlobalConfiguration.COLUMNAR_MAX_FALLBACK_SCANS.setValue(before);
    }
  }

  @Test
  @Timeout(300)
  void unpublishedColumnIsScanned() throws InterruptedException {
    final ColumnarTestData data = brandOnly();
    data.insertAssets(database, 2_000);
    FacetOracle.awaitCaughtUp(database);

    final CountDownLatch reached = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    holdRebuildScans(reached, release);
    final List<FacetSpec> facets = new ArrayList<>(List.of(terms("brand"), sizeRanges()));
    try {
      database.command("sql", "CREATE INDEX ON " + TYPE + " (fileSize) COLUMNAR");
      assertThat(reached.await(60, TimeUnit.SECONDS)).as("the create rebuild reached the end of its scan").isTrue();
      for (final String where : WHERES) {
        final FacetResults results = execute(TYPE, where, facets);
        assertThat(results.get("brand").source()).as("WHERE " + where).isEqualTo("index");
        assertThat(results.get("s").source()).as("WHERE " + where).isEqualTo("scan");
        assertThat(results.exact()).isTrue();
        assertTermsEqualGroupBy(results.get("brand"), "brand", TYPE, where);
        assertSizeRangesEqualGroupBy(results.get("s"), where);
      }
    } finally {
      ColumnarTestHooks.killPoint = null;
      release.countDown();
    }
    FacetOracle.awaitCaughtUp(database);

    for (final String where : WHERES) {
      final FacetResults results = execute(TYPE, where, facets);
      assertThat(results.get("s").source()).isEqualTo("index");
      assertSizeRangesEqualGroupBy(results.get("s"), where);
    }
  }

  // ------------------------------------------------------------------------------------------------------- fix round 1

  private static HistogramFacetSpec histogram(final String alias, final String property, final boolean calendar, final Object interval) {
    return new HistogramFacetSpec(alias, property, null, null, FacetSpecCompiler.parseInterval(calendar, interval), null, null, 1, null,
        null);
  }

  private static RangeFacetSpec dateRanges(final String property) {
    return new RangeFacetSpec("d", property, null, null, List.of(new RangeBound(null, "2021-01-01 00:00:00", "early"),
        new RangeBound("2021-01-01 00:00:00", "2023-01-01 00:00:00", "mid"), new RangeBound("2023-01-01 00:00:00", null, "late")), null);
  }

  /** The month histogram as GROUP BY of the UTC month of each value, keyed by the bucket's UTC month. */
  private static Map<Object, Long> monthsByFrom(final FacetResult histogram) {
    final Map<Object, Long> counts = new HashMap<>();
    for (final FacetBucket bucket : histogram.buckets())
      counts.put(DateTimeFormatter.ofPattern("yyyy-MM").withZone(ZoneOffset.UTC).format(Instant.ofEpochMilli((Long) bucket.from())),
          bucket.count());
    return counts;
  }

  @Test
  @Timeout(300)
  void scannedValuesAreChargedToTheBudget() {
    database.command("sql", "CREATE DOCUMENT TYPE Big");
    database.command("sql", "CREATE PROPERTY Big.id STRING");
    database.command("sql", "CREATE PROPERTY Big.grp STRING");
    database.command("sql", "CREATE INDEX ON Big (grp) COLUMNAR");
    FacetOracle.awaitCaughtUp(database);
    final int records = 20_000;
    database.transaction(() -> {
      for (int i = 0; i < records; i++)
        database.newDocument("Big").set("id", "a-rather-long-record-identifier-" + i).set("grp", "g-" + i % 7).save();
    });
    FacetOracle.awaitCaughtUp(database);

    final Object budget = GlobalConfiguration.COLUMNAR_QUERY_MEMORY_BUDGET.getValue();
    final Object wait = GlobalConfiguration.COLUMNAR_QUERY_BUDGET_WAIT.getValue();
    // the match set (1 MiB steps) and the grp counter fit; 20,000 distinct ids held by value (several MiB) do not
    GlobalConfiguration.COLUMNAR_QUERY_MEMORY_BUDGET.setValue(2L * MIB);
    GlobalConfiguration.COLUMNAR_QUERY_BUDGET_WAIT.setValue(0L);
    try {
      final FacetResults results = execute("Big", null, List.of(terms("grp"), terms("id")));
      assertThat(results.get("id").source()).isEqualTo(FacetResult.UNAVAILABLE);
      assertThat(results.get("id").reason()).isEqualTo("memory budget");
      assertThat(results.get("grp").source()).isEqualTo("index");
      assertTermsEqualGroupBy(results.get("grp"), "grp", "Big", null);
      // everything the refused facet held was given back
      assertThat(FacetMemoryBudget.instance().available()).isEqualTo(2L * MIB);
    } finally {
      GlobalConfiguration.COLUMNAR_QUERY_MEMORY_BUDGET.setValue(budget);
      GlobalConfiguration.COLUMNAR_QUERY_BUDGET_WAIT.setValue(wait);
    }

    // with the default budget the same scan is answered, and charged while it counted
    final FacetResults results = execute("Big", null, List.of(terms("id")));
    final FacetResult id = results.get("id");
    assertThat(id.source()).isEqualTo("scan");
    long counted = id.otherCount();
    for (final FacetBucket bucket : id.buckets())
      counted += bucket.count();
    assertThat(counted).isEqualTo(records);
    assertThat(results.executionStats().get("id").budgetBytes()).isGreaterThanOrEqualTo(records * TermsCollector.foreignEntryBytes("x"));
  }

  @Test
  @Timeout(300)
  void unindexedDatesAndIntegersAreScannedByTheirType() {
    final ColumnarTestData data = new ColumnarTestData(13);
    data.tagVocabulary = 300;
    data.timeOfDay = true;
    data.columnarIndexes = false;
    data.createSchema(database);
    database.command("sql", "CREATE INDEX ON " + TYPE + " (brand) COLUMNAR");
    data.insertAssets(database, 2_000);
    FacetOracle.awaitCaughtUp(database);

    final List<FacetSpec> facets = List.of(histogram("h", "publishDate", true, "1M"), dateRanges("publishDate"),
        histogram("f", "fileSize", false, MIB));
    final Map<String, FacetResults> scanned = new HashMap<>();
    for (final String where : WHERES) {
      final FacetResults results = execute(TYPE, where, facets);
      scanned.put(String.valueOf(where), results);
      for (final String alias : List.of("h", "d", "f"))
        assertThat(results.get(alias).source()).as(alias + " WHERE " + where).isEqualTo("scan");

      // the month histogram equals GROUP BY of the UTC month
      final Map<Object, Long> months = groupBy("publishDate.asLong().asDate().format('yyyy-MM', 'UTC')", TYPE, where);
      assertThat(months).as("WHERE " + where + " has dates").isNotEmpty();
      assertThat(monthsByFrom(results.get("h"))).as("months WHERE " + where).isEqualTo(months);
      assertThat(results.get("h").missing()).isEqualTo(count(TYPE, and(where, "publishDate IS NULL")));

      // the integral property is bucketed on a LONG grid: keys are integers, equal to GROUP BY of the bucket start
      final Map<Object, Long> sizes = new HashMap<>();
      for (final Map.Entry<Object, Long> entry : groupBy("fileSize", TYPE, where).entrySet())
        sizes.merge(Math.floorDiv(((Number) entry.getKey()).longValue(), MIB) * MIB, entry.getValue(), Long::sum);
      final Map<Object, Long> fixed = new HashMap<>();
      for (final FacetBucket bucket : results.get("f").buckets()) {
        assertThat(bucket.from()).isInstanceOf(Long.class);
        assertThat(bucket.key()).isEqualTo(bucket.from().toString());
        fixed.put(bucket.from(), bucket.count());
      }
      assertThat(fixed).as("fileSize WHERE " + where).isEqualTo(sizes);

      final Map<String, Long> ranges = rangeCounts(results.get("d"));
      assertThat(ranges.values().stream().mapToLong(Long::longValue).sum()).isEqualTo(count(TYPE, and(where, "publishDate IS NOT NULL")));
    }

    // the same facets over a projection of the same properties: identical answers
    database.command("sql", "CREATE INDEX ON " + TYPE + " (publishDate) COLUMNAR");
    database.command("sql", "CREATE INDEX ON " + TYPE + " (fileSize) COLUMNAR");
    FacetOracle.awaitCaughtUp(database);
    for (final String where : WHERES) {
      final FacetResults indexed = execute(TYPE, where, facets);
      final FacetResults before = scanned.get(String.valueOf(where));
      for (final String alias : List.of("h", "d", "f")) {
        assertThat(indexed.get(alias).source()).as(alias).isEqualTo("index");
        assertThat(indexed.get(alias).toJSON().getJSONArray("buckets").toString()).as(alias + " WHERE " + where)
            .isEqualTo(before.get(alias).toJSON().getJSONArray("buckets").toString());
        assertThat(indexed.get(alias).missing()).isEqualTo(before.get(alias).missing());
      }
      assertThat(rangeCounts(indexed.get("d")).get("mid")).as("WHERE " + where + " has mid dates").isPositive();
    }
  }

  @Test
  @Timeout(300)
  void mixedHierarchyDateHistogramIsScanned() {
    database.command("sql", "CREATE DOCUMENT TYPE Base");
    database.command("sql", "CREATE PROPERTY Base.stamp DATETIME");
    database.command("sql", "CREATE DOCUMENT TYPE Leaf EXTENDS Base");
    database.command("sql", "CREATE INDEX ON Leaf (stamp) COLUMNAR");
    FacetOracle.awaitCaughtUp(database);
    final long start = Instant.parse("2020-01-01T00:00:00Z").toEpochMilli();
    database.transaction(() -> {
      for (int i = 0; i < 1_200; i++) {
        final MutableDocument leaf = database.newDocument("Leaf");
        if (i % 17 != 0)
          leaf.set("stamp", new Date(start + i * 7_200_000L * 13));
        leaf.save();
      }
      for (int i = 0; i < 800; i++) {
        final MutableDocument base = database.newDocument("Base");
        if (i % 19 != 0)
          base.set("stamp", new Date(start + i * 3_600_000L * 61));
        base.save();
      }
    });
    FacetOracle.awaitCaughtUp(database);

    final String month = "stamp.asLong().asDate().format('yyyy-MM', 'UTC')";
    for (final String where : Arrays.asList(null, "stamp >= '2020-06-01 00:00:00'")) {
      final FacetResults base = execute("Base", where, List.of(histogram("h", "stamp", true, "1M")));
      assertThat(base.get("h").source()).as("WHERE " + where).isEqualTo("scan");
      final Map<Object, Long> expected = groupBy(month, "Base", where);
      assertThat(expected).isNotEmpty();
      assertThat(monthsByFrom(base.get("h"))).as("Base WHERE " + where).isEqualTo(expected);
      assertThat(base.get("h").missing()).isEqualTo(count("Base", and(where, "stamp IS NULL")));

      final FacetResults leaf = execute("Leaf", where, List.of(histogram("h", "stamp", true, "1M")));
      assertThat(leaf.get("h").source()).isEqualTo("index");
      assertThat(monthsByFrom(leaf.get("h"))).as("Leaf WHERE " + where).isEqualTo(groupBy(month, "Leaf", where));
    }
  }

  // ------------------------------------------------------------------------------------------------------- fix round 2

  /**
   * A type that does not declare the property, under a supertype query whose other types hold it as a date: its values are read as
   * dates (a time histogram is not refused for the whole query), and a value that is no date counts as missing.
   */
  @Test
  @Timeout(300)
  void undeclaredPropertyReadsAsDatesWhenAnotherTypeHoldsDates() {
    database.command("sql", "CREATE DOCUMENT TYPE Base");
    database.command("sql", "CREATE DOCUMENT TYPE Leaf EXTENDS Base");
    database.command("sql", "CREATE PROPERTY Leaf.stamp DATETIME");
    database.command("sql", "CREATE DOCUMENT TYPE Plain EXTENDS Base");
    database.command("sql", "CREATE PROPERTY Plain.stamp DATETIME");
    database.command("sql", "CREATE INDEX ON Leaf (stamp) COLUMNAR");
    FacetOracle.awaitCaughtUp(database);
    final long start = Instant.parse("2021-03-01T00:00:00Z").toEpochMilli();
    database.transaction(() -> {
      for (int i = 0; i < 900; i++) {
        database.newDocument("Leaf").set("stamp", new Date(start + i * 3_600_000L * 23)).save();
        // Plain declares the date but has no index: a raw EPOCH_MS read
        database.newDocument("Plain").set("stamp", new Date(start + i * 3_600_000L * 31)).save();
        // Base declares nothing: dates stored schema-less, and some values that are not dates
        final MutableDocument base = database.newDocument("Base");
        if (i % 10 == 0)
          base.set("stamp", "not a date");
        else
          base.set("stamp", new Date(start + i * 3_600_000L * 47));
        base.save();
      }
    });
    FacetOracle.awaitCaughtUp(database);

    final String month = "stamp.asLong().asDate().format('yyyy-MM', 'UTC')";
    final FacetResults results = execute("Base", null, List.of(histogram("h", "stamp", true, "1M")));
    final FacetResult h = results.get("h");
    assertThat(h.source()).isEqualTo("scan");
    // the oracle groups only the dates
    final Map<Object, Long> expected = new HashMap<>();
    for (final String type : List.of("Leaf", "Plain"))
      groupBy(month, type, null).forEach((key, count) -> expected.merge(key, count, Long::sum));
    // Base's own records, schema-less: the UTC month of each value that is not the string, in Java
    long baseDates = 0;
    try (final ResultSet rs = database.query("sql", "SELECT stamp AS v FROM Base WHERE @type = 'Base'")) {
      while (rs.hasNext()) {
        final Object value = rs.next().getProperty("v");
        if (value != null && !(value instanceof String)) {
          final long epochMs = DateUtils.dateTimeToTimestamp(value, ChronoUnit.MILLIS);
          expected.merge(DateTimeFormatter.ofPattern("yyyy-MM").withZone(ZoneOffset.UTC).format(Instant.ofEpochMilli(epochMs)), 1L,
              Long::sum);
          baseDates++;
        }
      }
    }
    assertThat(baseDates).as("Base's own date values").isEqualTo(810);
    assertThat(monthsByFrom(h)).isEqualTo(expected);
    assertThat(h.missing()).isEqualTo(90);
  }
}
