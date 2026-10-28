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
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.index.columnar.ColumnarIndexManager;
import com.arcadedb.index.columnar.ColumnarTestData;
import com.arcadedb.index.columnar.FacetOracle;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * FACETS through SQL: the planner chains the collect step, the rows keep their SKIP, LIMIT and ORDER BY while the facets count every
 * match of the WHERE, whatever source the matches come from (rows, the projection with no WHERE, a full-text index).
 */
class SQLFacetsIntegrationTest extends TestHelper {
  private static final String       TYPE    = ColumnarTestData.TYPE;
  private static final int          RECORDS = 20_000;
  private static final List<String> WHERES  = Arrays.asList(null, "brand = 'brand-7'", "fileSize < 1048576",
      "brand = 'brand-3' OR fileSize >= 3145728", "brand IN ['brand-1', 'brand-2', 'brand-3']", "author LIKE 'author-1%'");
  private static final String       SHOE    = "search_fields(['title'], 'shoe')";

  private ColumnarTestData data() {
    final ColumnarTestData data = new ColumnarTestData(42);
    data.tagVocabulary = 500;
    return data;
  }

  private void load(final ColumnarTestData data, final int records) {
    data.createSchema(database);
    data.insertAssets(database, records);
    FacetOracle.awaitCaughtUp(database);
  }

  private ColumnarIndexManager manager() {
    return ((LocalDatabase) ((DatabaseInternal) database).getEmbedded()).getColumnarIndexManager();
  }

  private long count(final String where) {
    try (final ResultSet rs = database.query("sql", "SELECT count(@rid) AS c FROM " + TYPE + (where == null ? "" : " WHERE " + where))) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }

  private boolean cached(final String sql) {
    return ((DatabaseInternal) database).getExecutionPlanCache().contains(sql);
  }

  private long readRecords() {
    return (Long) database.getStats().get("readRecord");
  }

  /** The facets of a query whose rows are not read. */
  private FacetResults facets(final String sql, final Object... args) {
    try (final ResultSet rs = database.query("sql", sql, args)) {
      return rs.getFacets();
    }
  }

  private String profile(final String sql) {
    try (final ResultSet rs = database.query("sql", "PROFILE " + sql)) {
      return rs.next().getProperty("executionPlanAsString");
    }
  }

  private static Map<Object, Long> counts(final FacetResult result) {
    final Map<Object, Long> counts = new HashMap<>();
    for (final FacetBucket bucket : result.buckets())
      if (!bucket.isMissing())
        counts.put(bucket.value(), bucket.count());
    if (result.missing() > 0)
      counts.put(FacetOracle.MISSING, result.missing());
    return counts;
  }

  private static long sum(final FacetResult result) {
    long sum = 0;
    for (final FacetBucket bucket : result.buckets())
      if (!bucket.isMissing())
        sum += bucket.count();
    return sum + result.otherCount();
  }

  private static List<RID> rids(final ResultSet rs) {
    final List<RID> rids = new ArrayList<>();
    while (rs.hasNext())
      rids.add(rs.next().getIdentity().orElseThrow());
    return rids;
  }

  /** Adds {@code title} with a FULL_TEXT index; about 5% of the titles hold "shoe". */
  private void addTitles() {
    database.command("sql", "CREATE PROPERTY " + TYPE + ".title STRING");
    final String[] words = { "red", "blue", "boot", "sandal", "jacket", "hat", "running", "leather", "light", "classic" };
    final Random random = new Random(7);
    final List<RID> all = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT @rid AS rid FROM " + TYPE)) {
      while (rs.hasNext())
        all.add(rs.next().getProperty("rid"));
    }
    database.transaction(() -> {
      for (final RID rid : all) {
        final String title = words[random.nextInt(words.length)] + " " + (random.nextInt(20) == 0 ? "shoe" : words[random.nextInt(
            words.length)]);
        database.lookupByRID(rid, true).asDocument().modify().set("title", title).save();
      }
    });
    database.command("sql", "CREATE INDEX ON " + TYPE + " (title) FULL_TEXT");
    FacetOracle.awaitCaughtUp(database);
  }

  @Test
  @Timeout(600)
  void facetsEqualGroupByAtLagZero() {
    load(data(), RECORDS);
    for (final String where : WHERES) {
      assertThat(count(where)).as("records matching " + where).isPositive();
      FacetOracle.assertFacetsEqualGroupBy(database, TYPE, "brand", where);
      FacetOracle.assertFacetsEqualGroupBy(database, TYPE, "tags", where);
    }
  }

  @Test
  @Timeout(600)
  void facetsEqualGroupByWhileLagging() throws Exception {
    final ColumnarTestData data = data();
    data.deleteRatio = 0.2;
    data.reuseRatio = 0.1;
    data.bigRecordRatio = 0.02;
    load(data, RECORDS);

    try (final AutoCloseable hold = manager().suspendApply()) {
      data.mutate(database, 300);
      long patched = 0;
      for (final String where : WHERES) {
        assertThat(count(where)).as("records matching " + where).isPositive();
        FacetOracle.assertFacetsEqualGroupBy(database, TYPE, "brand", where);
        FacetOracle.assertFacetsEqualGroupBy(database, TYPE, "tags", where);
        final FacetResults results = facets("SELECT FROM " + TYPE + (where == null ? "" : " WHERE " + where) + " FACETS brand TERMS TOP 1000");
        assertThat(results.exact()).as("exact WHERE " + where).isTrue();
        patched += results.patched();
      }
      assertThat(patched).isPositive();
    }
  }

  @Test
  @Timeout(300)
  void limitDoesNotShrinkFacets() {
    load(data(), 5_000);
    final String where = "fileSize < 1048576";
    try (final ResultSet rs = database.query("sql", "SELECT FROM " + TYPE + " WHERE " + where + " FACETS brand TERMS TOP 1000 LIMIT 5")) {
      assertThat(rids(rs)).hasSize(5);
      final FacetResult brand = rs.getFacets().get("brand");
      assertThat(brand.source()).isEqualTo("index");
      assertThat(sum(brand)).isEqualTo(count(where + " AND brand IS NOT NULL")).isGreaterThan(5);
      assertThat(counts(brand)).isEqualTo(FacetOracle.expectedCounts(database, TYPE, "brand", where));
    }
  }

  @Test
  @Timeout(300)
  void skipLimitAndOrderByKeepRows() {
    load(data(), 5_000);
    for (final String where : List.of("fileSize < 1048576", "brand IN ['brand-1', 'brand-2', 'brand-3']")) {
      final String head = "SELECT FROM " + TYPE + " WHERE " + where + " ORDER BY fileSize DESC";
      final List<RID> expected;
      try (final ResultSet rs = database.query("sql", head + " SKIP 3 LIMIT 7")) {
        expected = rids(rs);
      }
      assertThat(expected).hasSize(7);
      try (final ResultSet rs = database.query("sql", head + " FACETS brand TERMS TOP 1000 SKIP 3 LIMIT 7")) {
        assertThat(rids(rs)).as(where).isEqualTo(expected);
        assertThat(counts(rs.getFacets().get("brand"))).as(where).isEqualTo(FacetOracle.expectedCounts(database, TYPE, "brand", where));
      }
    }
  }

  @Test
  @Timeout(300)
  void facetsComputedOnlyWhenAskedAfterPartialRead() {
    load(data(), 5_000);
    final long capacity = FacetMemoryBudget.instance().available();
    final String where = "fileSize < 1048576";
    try (final ResultSet rs = database.query("sql", "SELECT FROM " + TYPE + " WHERE " + where + " FACETS brand TERMS TOP 1000")) {
      rs.next();
      rs.next();
      assertThat(counts(rs.getFacets().get("brand"))).isEqualTo(FacetOracle.expectedCounts(database, TYPE, "brand", where));
    }

    // closed after two rows without asking for the facets: the rows' match set and the projection's snapshots are released
    for (final String sql : List.of("SELECT FROM " + TYPE + " WHERE " + where + " FACETS brand TERMS",
        "SELECT FROM " + TYPE + " FACETS brand TERMS")) {
      final ResultSet rs = database.query("sql", sql);
      rs.next();
      rs.next();
      rs.close();
      assertThat(FacetMemoryBudget.instance().available()).as(sql).isEqualTo(capacity);
    }

    database.command("sql", "DROP INDEX `" + TYPE + "[brand]_columnar`");
    database.command("sql", "CREATE INDEX ON " + TYPE + " (brand) COLUMNAR");
    FacetOracle.awaitCaughtUp(database);
    FacetOracle.assertFacetsEqualGroupBy(database, TYPE, "brand", null);
  }

  @Test
  @Timeout(300)
  void fullTextFacetsCoverEveryMatch() {
    load(data(), 5_000);
    addTitles();
    final long matches = count(SHOE);
    assertThat(matches).isGreaterThan(100);
    try (final ResultSet rs = database.query("sql", "SELECT FROM " + TYPE + " WHERE " + SHOE + " FACETS brand TERMS TOP 1000 LIMIT 3")) {
      assertThat(rids(rs)).hasSize(3);
      final FacetResult brand = rs.getFacets().get("brand");
      assertThat(sum(brand) + brand.missing()).isEqualTo(matches);
      assertThat(counts(brand)).isEqualTo(FacetOracle.expectedCounts(database, TYPE, "brand", SHOE));
    }
  }

  @Test
  @Timeout(300)
  void fullTextFastPathLoadsNoRecordForDrain() {
    load(data(), 5_000);
    addTitles();
    final long matches = count(SHOE);
    assertThat(matches).isGreaterThan(100);
    final String sql = "SELECT FROM " + TYPE + " WHERE " + SHOE + " FACETS brand TERMS TOP 1000 LIMIT 3";
    assertThat(profile(sql)).contains("matchSource=index");

    final long before = readRecords();
    final FacetResult brand;
    try (final ResultSet rs = database.query("sql", sql)) {
      assertThat(rids(rs)).hasSize(3);
      brand = rs.getFacets().get("brand");
    }
    // the three rows are loaded, the other matches are counted from the projection
    assertThat(readRecords() - before).as("records read").isLessThan(matches / 2);
    assertThat(sum(brand) + brand.missing()).isEqualTo(matches);
    assertThat(counts(brand)).isEqualTo(FacetOracle.expectedCounts(database, TYPE, "brand", SHOE));
  }

  @Test
  @Tag("vector")
  @Timeout(300)
  void vectorFacetsCoverReturnedNeighbors() {
    load(data(), 2_000);
    database.command("sql", "CREATE PROPERTY " + TYPE + ".embedding ARRAY_OF_FLOATS");
    final Random random = new Random(11);
    final List<RID> all = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT @rid AS rid FROM " + TYPE)) {
      while (rs.hasNext())
        all.add(rs.next().getProperty("rid"));
    }
    database.transaction(() -> {
      for (final RID rid : all) {
        final float[] embedding = new float[8];
        for (int i = 0; i < embedding.length; i++)
          embedding[i] = random.nextFloat();
        database.lookupByRID(rid, true).asDocument().modify().set("embedding", embedding).save();
      }
    });
    database.command("sql",
        "CREATE INDEX ON " + TYPE + " (embedding) LSM_VECTOR METADATA { \"dimensions\": 8, \"similarity\": \"COSINE\" }");
    FacetOracle.awaitCaughtUp(database);

    final float[] query = new float[8];
    Arrays.fill(query, 0.5f);
    try (final ResultSet rs = database.query("sql",
        "SELECT FROM (SELECT expand(vector.neighbors('" + TYPE + "[embedding]', ?, 25))) FACETS brand TERMS TOP 1000", (Object) query)) {
      assertThat(rids(rs)).hasSize(25);
      final FacetResult brand = rs.getFacets().get("brand");
      assertThat(brand.source()).isNotEqualTo(FacetResult.UNAVAILABLE);
      assertThat(sum(brand) + brand.missing()).isEqualTo(25);
    }
  }

  @Test
  void rowsWithoutIdentityAreRefused() {
    assertThatThrownBy(() -> {
      try (final ResultSet rs = database.query("sql", "SELECT FROM (SELECT 1 AS x) FACETS x TERMS")) {
        rids(rs);
        rs.getFacets();
      }
    }).isInstanceOf(CommandExecutionException.class).hasMessageContaining("FACETS needs rows that are records");
  }

  @Test
  void targetWithoutTypeIsRefused() {
    assertThatThrownBy(() -> database.query("sql", "SELECT FROM #1:0 FACETS x TERMS").close())
        .hasMessageContaining("FACETS needs a type, a bucket or a subquery as target");
    assertThatThrownBy(() -> database.query("sql", "SELECT FROM schema:types FACETS name TERMS").close())
        .hasMessageContaining("FACETS needs a type, a bucket or a subquery as target");
  }

  /** A script answers the result set of its last statement, and with it the facets of a {@code SELECT ... FACETS} there. */
  @Test
  @Timeout(300)
  void scriptEndingInFacetsKeepsFacets() {
    load(data(), 2_000);
    final String sql = "SELECT FROM " + TYPE + " WHERE fileSize < 1048576 FACETS brand TERMS TOP 1000";
    try (final ResultSet rs = database.command("sqlscript", "LET n = SELECT count(*) FROM " + TYPE + ";\n" + sql + ";")) {
      assertThat(rids(rs)).hasSize((int) count("fileSize < 1048576"));
      final FacetResults facets = rs.getFacets();
      assertThat(facets).as("the facets of the script's last statement").isNotNull();
      assertThat(counts(facets.get("brand"))).isEqualTo(FacetOracle.expectedCounts(database, TYPE, "brand", "fileSize < 1048576"));
    }
    try (final ResultSet rs = database.command("sqlscript", sql + ";\nSELECT count(*) AS c FROM " + TYPE + ";")) {
      assertThat(rs.next().<Number>getProperty("c").longValue()).isEqualTo(2_000L);
      assertThat(rs.getFacets()).as("facets of a statement that is not the last one are not the script's").isNull();
    }
  }

  /**
   * Final fix batch 2 (Task 18 minor): an IF answers the rows of the branch that ran, so a {@code SELECT ... FACETS} ending that branch
   * gives its facets to the script; a WHILE answers no row of its body, and no facets either.
   */
  @Test
  @Timeout(300)
  void facetsOfAStatementEndingAnIfBranchAreTheScripts() {
    load(data(), 2_000);
    final String where = "fileSize < 1048576";
    final String sql = "SELECT FROM " + TYPE + " WHERE " + where + " FACETS brand TERMS TOP 1000";
    try (final ResultSet rs = database.command("sqlscript", "IF (1 = 1) {\n" + sql + ";\n}")) {
      assertThat(rids(rs)).hasSize((int) count(where));
      assertThat(rs.getFacets()).as("the facets of the branch that ran").isNotNull();
      assertThat(counts(rs.getFacets().get("brand"))).isEqualTo(FacetOracle.expectedCounts(database, TYPE, "brand", where));
    }
    try (final ResultSet rs = database.command("sqlscript", "IF (1 = 2) {\n" + sql + ";\n} ELSE {\nSELECT 1 AS one;\n}")) {
      assertThat(rs.next().<Number>getProperty("one").intValue()).isEqualTo(1);
      assertThat(rs.getFacets()).as("the branch that ran has no facets").isNull();
    }
    try (final ResultSet rs = database.command("sqlscript", "LET i = 0;\nWHILE ($i < 1) {\n" + sql + ";\nLET i = $i + 1;\n}")) {
      assertThat(rs.hasNext()).as("a WHILE answers no row of its body").isFalse();
      assertThat(rs.getFacets()).as("nor its facets").isNull();
    }
  }

  @Test
  @Timeout(300)
  void cachedPlanReusedGivesFreshFacets() {
    load(data(), 2_000);
    final String sql = "SELECT FROM " + TYPE + " WHERE fileSize < 1048576 FACETS brand TERMS TOP 1000";
    assertThat(counts(facets(sql).get("brand"))).doesNotContainKey("brand-new");
    database.transaction(() -> {
      for (int i = 0; i < 3; i++)
        database.newDocument(TYPE).set("brand", "brand-new", "fileSize", 10L).save();
    });
    FacetOracle.awaitCaughtUp(database);
    // the second run is served by a copy of the cached plan
    assertThat(cached(sql)).isTrue();
    final Map<Object, Long> second = counts(facets(sql).get("brand"));
    assertThat(second.get("brand-new")).isEqualTo(3L);
    assertThat(second).isEqualTo(FacetOracle.expectedCounts(database, TYPE, "brand", "fileSize < 1048576"));
  }

  /** The plan cache copies a plan without a context: an INCLUDE expression must still be evaluated against the execution's. */
  @Test
  @Timeout(300)
  void cachedPlanWithIncludeExpression() {
    load(data(), 2_000);
    final String where = "fileSize < 1048576 AND brand = 'brand-1'";
    final String sql = "SELECT FROM " + TYPE + " WHERE fileSize < 1048576 FACETS t: brand TERMS INCLUDE [format('%s', 'brand-1')]";
    assertThat(count(where)).isPositive();
    for (int run = 0; run < 2; run++) {
      if (run == 1)
        assertThat(cached(sql)).as("cache hit").isTrue();
      final FacetResult t = facets(sql).get("t");
      assertThat(counts(t)).as("run " + run).isEqualTo(Map.of("brand-1", count(where)));
    }
  }

  @Test
  @Timeout(300)
  void cachedPlanWithOtherParametersGivesFreshBounds() {
    load(data(), 2_000);
    final String sql = "SELECT FROM " + TYPE + " FACETS s: fileSize RANGES ([:lo, *))";
    for (final long lo : new long[] { 0, 1_048_576 }) {
      try (final ResultSet rs = database.query("sql", sql, Map.of("lo", lo))) {
        assertThat(rs.getFacets().get("s").buckets().getFirst().count()).as("from " + lo).isEqualTo(count("fileSize >= " + lo));
      }
      // R15: a plan whose facets read an input parameter is not cached
      assertThat(cached(sql)).as("cached after lo = " + lo).isFalse();
    }
    assertThat(count("fileSize >= 1048576")).isLessThan(count("fileSize >= 0"));

    // the same clause with the bound written out is cached: the cache is not refusing FACETS plans as such
    final String literal = "SELECT FROM " + TYPE + " FACETS s: fileSize RANGES ([1048576, *))";
    facets(literal);
    assertThat(cached(literal)).isTrue();
    assertThat(facets(literal).get("s").buckets().getFirst().count()).isEqualTo(count("fileSize >= 1048576"));
  }

  /** A LIMIT reached computes the facets and releases everything, as an ordinary plan does: closing is not needed for that. */
  @Test
  @Timeout(300)
  void limitReadToTheEndWithoutCloseReleasesEverything() {
    load(data(), 5_000);
    addTitles();
    final long capacity = FacetMemoryBudget.instance().available();
    final String where = "fileSize < 1048576";
    final Map<String, String> sourceByHead = Map.of("SELECT FROM " + TYPE + " WHERE " + where, "rows", "SELECT FROM " + TYPE, "projection",
        "SELECT FROM " + TYPE + " WHERE " + SHOE, "index");
    for (final Map.Entry<String, String> entry : sourceByHead.entrySet()) {
      final String head = entry.getKey();
      final String condition = head.contains(" WHERE ") ? head.substring(head.indexOf(" WHERE ") + 7) : "true";
      assertThat(profile(head + " FACETS brand TERMS TOP 1000 LIMIT 5")).as(head).contains("matchSource=" + entry.getValue() + " ");
      final ResultSet rs = database.query("sql", head + " FACETS brand TERMS TOP 1000 LIMIT 5");
      assertThat(rids(rs)).as(head).hasSize(5);
      assertThat(FacetMemoryBudget.instance().available()).as(head).isEqualTo(capacity);
      assertThat(counts(rs.getFacets().get("brand"))).as(head).isEqualTo(FacetOracle.expectedCounts(database, TYPE, "brand", condition));
      rs.close();
    }
  }

  /** A record the caller's transaction deleted can still be in the full-text index: the index source does not count it. */
  @Test
  @Timeout(300)
  void fullTextIndexSourceSkipsDeletedRecords() {
    load(data(), 5_000);
    addTitles();
    final String sql = "SELECT FROM " + TYPE + " WHERE " + SHOE + " FACETS brand TERMS TOP 1000 LIMIT 3";
    database.begin();
    try {
      final List<RID> shoes = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", "SELECT @rid AS rid FROM " + TYPE + " WHERE " + SHOE + " LIMIT 5")) {
        while (rs.hasNext())
          shoes.add(rs.next().getProperty("rid"));
      }
      final long before = count(SHOE);
      for (final RID rid : shoes)
        database.deleteRecord(database.lookupByRID(rid, true));
      final long matches = count(SHOE);
      assertThat(matches).isEqualTo(before - shoes.size());

      assertThat(profile(sql)).contains("matchSource=index");
      try (final ResultSet rs = database.query("sql", sql)) {
        assertThat(rids(rs)).hasSize(3);
        final FacetResult brand = rs.getFacets().get("brand");
        assertThat(sum(brand) + brand.missing()).isEqualTo(matches);
      }
    } finally {
      database.rollback();
    }
  }

  /** Some buckets changed by the caller's transaction: they are scanned, the others come from the projection. */
  @Test
  @Timeout(300)
  void noWhereInOwnTransactionScansOnlyTheChangedBuckets() {
    final ColumnarTestData data = data();
    data.buckets = 4;
    load(data, 4_000);
    database.begin();
    try {
      database.newDocument(TYPE).set("brand", "brand-mine").save();
      final String sql = "SELECT FROM " + TYPE + " FACETS brand TERMS TOP 1000";
      assertThat(profile(sql)).contains("matchSource=projection+scan");
      final Map<Object, Long> counts = counts(facets(sql).get("brand"));
      assertThat(counts.get("brand-mine")).isEqualTo(1L);
      assertThat(counts).isEqualTo(FacetOracle.expectedCounts(database, TYPE, "brand", "true"));
    } finally {
      database.rollback();
    }
  }

  @Test
  @Timeout(600)
  void noWhereBuildsMatchSetFromProjection() throws Exception {
    final ColumnarTestData data = data();
    data.deleteRatio = 0.2;
    data.reuseRatio = 0.1;
    load(data, RECORDS);
    final String sql = "SELECT FROM " + TYPE + " FACETS brand TERMS TOP 1000 LIMIT 2";
    assertThat(profile(sql)).contains("matchSource=projection");

    // what reading two rows costs without FACETS (a sequential scan reads its first page; a parallel one reads ahead at random)
    final Object parallel = database.getConfiguration().getValue(GlobalConfiguration.QUERY_PARALLEL_SCAN);
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN, false);
    final FacetResult brand;
    try {
      long before = readRecords();
      try (final ResultSet rs = database.query("sql", "SELECT FROM " + TYPE + " LIMIT 2")) {
        assertThat(rids(rs)).hasSize(2);
      }
      final long rowsOnly = readRecords() - before;
      assertThat(rowsOnly).isLessThan(RECORDS / 10);

      before = readRecords();
      try (final ResultSet rs = database.query("sql", sql)) {
        assertThat(rids(rs)).hasSize(2);
        brand = rs.getFacets().get("brand");
      }
      assertThat(readRecords() - before).as("records read for two rows and the facets").isEqualTo(rowsOnly);
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN, parallel);
    }
    assertThat(counts(brand)).isEqualTo(FacetOracle.expectedCounts(database, TYPE, "brand", "true"));

    try (final AutoCloseable hold = manager().suspendApply()) {
      data.mutate(database, 300);
      assertThat(profile(sql)).contains("matchSource=projection");
      try (final ResultSet rs = database.query("sql", sql)) {
        assertThat(rids(rs)).hasSize(2);
        final FacetResults facets = rs.getFacets();
        assertThat(facets.exact()).isTrue();
        assertThat(counts(facets.get("brand"))).isEqualTo(FacetOracle.expectedCounts(database, TYPE, "brand", "true"));
      }
    }
  }

  /**
   * Final review C-I4: no WHERE, and FILTERs the INDEX and PROJECTION evaluators answer by RID. The match set is the projection's and
   * each match gets its fail mask by RID, so computing the facets reads no record; the rows still come from the scan, masked.
   */
  @Test
  @Timeout(300)
  void noWhereWithFiltersAnsweredByRidTakesTheProjection() {
    load(data(), RECORDS);
    database.command("sql", "CREATE INDEX ON " + TYPE + " (author) NOTUNIQUE");
    final String author = "author = 'author-3'";
    final String small = "fileSize < 1048576";
    final String sql = "SELECT FROM " + TYPE + " FACETS brand TERMS TOP 1000 FILTER " + author + ", author TERMS TOP 1000 FILTER " + small
        + ", tags TERMS TOP 1000";
    assertThat(profile(sql)).contains("[INDEX rids=", "[PROJECTION", "matchSource=projection ");

    // LIMIT 0: no row is read, so every record read is one the facets read
    final long before = readRecords();
    final FacetResults facets = facets(sql + " LIMIT 0");
    assertThat(readRecords() - before).as("records read to compute the facets").isZero();
    assertThat(facets.exact()).isTrue();
    // a FILTER applies to the other facets: each counts the matches that pass every FILTER but its own
    assertThat(counts(facets.get("brand"))).isEqualTo(FacetOracle.expectedCounts(database, TYPE, "brand", small));
    assertThat(counts(facets.get("author"))).isEqualTo(FacetOracle.expectedCounts(database, TYPE, "author", author));
    assertThat(counts(facets.get("tags"))).isEqualTo(FacetOracle.expectedCounts(database, TYPE, "tags", author + " AND " + small));

    // the rows are those that pass every FILTER
    try (final ResultSet rs = database.query("sql", sql)) {
      assertThat((long) rids(rs).size()).isEqualTo(count(author + " AND " + small));
    }
  }

  @Test
  @Timeout(300)
  void noWhereInOwnTransactionRecordsRows() {
    load(data(), 2_000);
    database.begin();
    try {
      for (int i = 0; i < 3; i++) {
        final MutableDocument document = database.newDocument(TYPE).set("brand", "brand-mine");
        document.save();
      }
      final String sql = "SELECT FROM " + TYPE + " FACETS brand TERMS TOP 1000";
      assertThat(profile(sql)).contains("matchSource=rows");
      final Map<Object, Long> counts = counts(facets(sql).get("brand"));
      assertThat(counts.get("brand-mine")).isEqualTo(3L);
      assertThat(counts).isEqualTo(FacetOracle.expectedCounts(database, TYPE, "brand", "true"));
    } finally {
      database.rollback();
    }
  }

  @Test
  @Timeout(300)
  void budgetRefusalKeepsRows() {
    load(data(), 2_000);
    final Object budget = GlobalConfiguration.COLUMNAR_QUERY_MEMORY_BUDGET.getValue();
    GlobalConfiguration.COLUMNAR_QUERY_MEMORY_BUDGET.setValue(1024L);
    try {
      for (final String where : Arrays.asList("fileSize < 1048576", null)) {
        try (final ResultSet rs = database.query("sql",
            "SELECT FROM " + TYPE + (where == null ? "" : " WHERE " + where) + " FACETS brand TERMS, tags TERMS")) {
          assertThat((long) rids(rs).size()).as("rows WHERE " + where).isEqualTo(count(where));
          final FacetResults facets = rs.getFacets();
          for (final String alias : List.of("brand", "tags")) {
            assertThat(facets.get(alias).source()).as(alias + " WHERE " + where).isEqualTo(FacetResult.UNAVAILABLE);
            assertThat(facets.get(alias).reason()).as(alias + " WHERE " + where).isEqualTo("memory budget");
          }
        }
      }
    } finally {
      GlobalConfiguration.COLUMNAR_QUERY_MEMORY_BUDGET.setValue(budget);
    }
  }

  /** LIMIT 0 returns no row and still every facet; a WHERE false for every record gives empty, exact facets. */
  @Test
  @Timeout(300)
  void facetsWithoutRows() {
    load(data(), 2_000);
    try (final ResultSet rs = database.query("sql", "SELECT FROM " + TYPE + " WHERE fileSize < 1048576 FACETS brand TERMS TOP 1000 LIMIT 0")) {
      assertThat(rs.hasNext()).isFalse();
      assertThat(counts(rs.getFacets().get("brand"))).isEqualTo(FacetOracle.expectedCounts(database, TYPE, "brand", "fileSize < 1048576"));
    }
    try (final ResultSet rs = database.query("sql", "SELECT FROM " + TYPE + " WHERE 1 = 0 FACETS brand TERMS")) {
      assertThat(rs.hasNext()).isFalse();
      final FacetResults facets = rs.getFacets();
      assertThat(facets.exact()).isTrue();
      assertThat(facets.get("brand").buckets()).isEmpty();
      assertThat(facets.get("brand").missing()).isZero();
    }
  }

  /** A facet over a row the query projects is still over the record: the projection runs after the collect step. */
  @Test
  @Timeout(300)
  void projectionDoesNotHideTheRecord() {
    load(data(), 2_000);
    try (final ResultSet rs = database.query("sql",
        "SELECT fileSize AS size FROM " + TYPE + " WHERE fileSize < 1048576 FACETS brand TERMS TOP 1000 LIMIT 4")) {
      final List<Result> rows = new ArrayList<>();
      rs.forEachRemaining(rows::add);
      assertThat(rows).hasSize(4);
      assertThat(rows.getFirst().getPropertyNames()).containsExactly("size");
      assertThat(counts(rs.getFacets().get("brand"))).isEqualTo(FacetOracle.expectedCounts(database, TYPE, "brand", "fileSize < 1048576"));
    }
  }

  /** Final review B-M1: a bucket named by an input parameter is a target as the same bucket named in the text is (no planning NPE). */
  @Test
  @Timeout(300)
  void aBucketNamedByAnInputParameterIsATarget() {
    final ColumnarTestData data = data();
    data.buckets = 2;
    load(data, 2_000);
    final String bucket = database.getSchema().getType(TYPE).getBuckets(false).getFirst().getName();
    final Map<Object, Long> expected;
    try (final ResultSet rs = database.query("sql", "SELECT FROM bucket:" + bucket + " FACETS brand TERMS TOP 1000")) {
      expected = counts(rs.getFacets().get("brand"));
    }
    assertThat(expected).isNotEmpty();
    try (final ResultSet rs = database.query("sql", "SELECT FROM bucket::b FACETS brand TERMS TOP 1000", Map.of("b", bucket))) {
      assertThat(counts(rs.getFacets().get("brand"))).isEqualTo(expected);
    }
    try (final ResultSet rs = database.query("sql", "SELECT FROM bucket:? FACETS brand TERMS TOP 1000", bucket)) {
      assertThat(counts(rs.getFacets().get("brand"))).isEqualTo(expected);
    }
  }
}
