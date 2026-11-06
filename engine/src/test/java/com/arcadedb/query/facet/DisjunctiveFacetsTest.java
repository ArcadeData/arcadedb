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
import com.arcadedb.database.RID;
import com.arcadedb.index.columnar.ColumnarIndexManager;
import com.arcadedb.index.columnar.ColumnarTestAccess;
import com.arcadedb.index.columnar.ColumnarTestData;
import com.arcadedb.index.columnar.FacetOracle;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Disjunctive faceting (spec 7.5) in one pass: each facet counts the matches of the WHERE and of every FILTER but its own, the rows are
 * the matches of the WHERE and of every FILTER. The oracle is the separate GROUP BY query each facet stands for.
 */
class DisjunctiveFacetsTest extends TestHelper {
  private static final String TYPE   = ColumnarTestData.TYPE;
  private static final String WHERE  = "publishDate >= '2024-01-01'";
  private static final String BRANDS = "brand IN ['brand-1', 'brand-2']";
  private static final String SMALL  = "fileSize < 1048576";

  private ColumnarTestData data() {
    final ColumnarTestData data = new ColumnarTestData(42);
    data.brands = 20;
    data.tagVocabulary = 300;
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
    try (final ResultSet rs = database.query("sql", "SELECT count(@rid) AS c FROM " + TYPE + " WHERE " + where)) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }

  private Map<Object, Long> groupBy(final String property, final String where) {
    assertThat(count(where)).as("records matching " + where).isPositive();
    return FacetOracle.expectedCounts(database, TYPE, property, where);
  }

  private Map<Object, Long> rangeGroupBy(final String where) {
    final Map<Object, Long> counts = new HashMap<>();
    counts.put("small", count(where + " AND fileSize >= 0 AND fileSize < 1048576"));
    counts.put("big", count(where + " AND fileSize >= 1048576"));
    return counts;
  }

  /** Terms by value (records without a value under {@link FacetOracle#MISSING}), ranges by key. */
  private static Map<Object, Long> asMap(final FacetResult result) {
    assertThat(result.source()).as("source of " + result.kind()).isIn("index", "scan");
    assertThat(result.otherCount()).isZero();
    final Map<Object, Long> counts = new HashMap<>();
    for (final FacetBucket bucket : result.buckets())
      if (bucket.key() != null)
        counts.put(bucket.key(), bucket.count());
      else if (!bucket.isMissing())
        counts.put(bucket.value(), bucket.count());
    if (result.missing() > 0)
      counts.put(FacetOracle.MISSING, result.missing());
    return counts;
  }

  private static String query(final String where, final String brandFilter, final String sizeFilter) {
    return "SELECT FROM " + TYPE + " WHERE " + where + " FACETS brand TERMS TOP 1000 FILTER " + brandFilter + ", "
        + "size: fileSize RANGES ([0, 1048576) AS 'small', [1048576, *) AS 'big') FILTER " + sizeFilter + ", tags TERMS TOP 1000";
  }

  private record Answer(long rows, Set<RID> rids, Map<Object, Long> brand, Map<Object, Long> size, Map<Object, Long> tags) {
  }

  private Answer run(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      final Set<RID> rids = new HashSet<>();
      long rows = 0;
      while (rs.hasNext()) {
        rids.add(rs.next().getIdentity().orElseThrow());
        rows++;
      }
      final FacetResults f = rs.getFacets();
      return new Answer(rows, rids, asMap(f.get("brand")), asMap(f.get("size")), asMap(f.get("tags")));
    }
  }

  /** The brief's query, checked against the separate GROUP BY queries. */
  private Answer assertDisjunctive(final String sql) {
    final Answer answer = run(sql);
    assertThat(answer.brand()).isEqualTo(groupBy("brand", WHERE + " AND " + SMALL));
    assertThat(answer.size()).isEqualTo(rangeGroupBy(WHERE + " AND " + BRANDS));
    assertThat(answer.tags()).isEqualTo(groupBy("tags", WHERE + " AND " + BRANDS + " AND " + SMALL));
    assertThat(answer.rows()).isEqualTo(count(WHERE + " AND " + BRANDS + " AND " + SMALL));
    assertThat(answer.rids()).hasSize((int) answer.rows());
    return answer;
  }

  private String explain(final String sql) {
    try (final ResultSet rs = database.query("sql", "EXPLAIN " + sql)) {
      return rs.next().getProperty("executionPlanAsString");
    }
  }

  @Test
  @Timeout(300)
  void eachFacetCountsAllFiltersButItsOwn() {
    load(data(), 5_000);
    final String w = WHERE;
    final ResultSet rs = database.query("sql", "SELECT FROM Asset WHERE " + w + " FACETS "
        + "brand TERMS TOP 1000 FILTER brand IN ['brand-1', 'brand-2'], "
        + "size: fileSize RANGES ([0, 1048576) AS 'small', [1048576, *) AS 'big') FILTER fileSize < 1048576, "
        + "tags TERMS TOP 1000");
    final long rows = rs.stream().count();
    final FacetResults f = rs.getFacets();
    rs.close();
    assertThat(asMap(f.get("brand"))).isEqualTo(groupBy("brand", w + " AND fileSize < 1048576"));
    assertThat(asMap(f.get("size"))).isEqualTo(rangeGroupBy(w + " AND brand IN ['brand-1', 'brand-2']"));
    assertThat(asMap(f.get("tags"))).isEqualTo(groupBy("tags", w + " AND brand IN ['brand-1', 'brand-2'] AND fileSize < 1048576"));
    assertThat(rows).isEqualTo(count(w + " AND brand IN ['brand-1', 'brand-2'] AND fileSize < 1048576"));
    // the filters do cut: neither facet's own filter is a no-op
    assertThat(asMap(f.get("brand")).size()).isGreaterThan(2);
    assertThat(asMap(f.get("size")).get("big")).isPositive();
  }

  @Test
  @Timeout(300)
  void sameResultsForEveryEvaluatorKind() {
    load(data(), 5_000);
    final String projectionQuery = query(WHERE, BRANDS, SMALL);
    assertThat(explain(projectionQuery)).contains("[PROJECTION]").doesNotContain("[RECORD]", "[INDEX]");
    final Answer projection = assertDisjunctive(projectionQuery);

    final String recordQuery = query(WHERE, "brand.toLowerCase() IN ['brand-1', 'brand-2']", "fileSize + 0 < 1048576");
    assertThat(explain(recordQuery)).contains("[RECORD]").doesNotContain("[PROJECTION]", "[INDEX]");
    assertThat(run(recordQuery)).isEqualTo(projection);

    database.command("sql", "CREATE INDEX ON " + TYPE + " (brand) NOTUNIQUE");
    assertThat(explain(projectionQuery)).contains("[INDEX]", "[PROJECTION]");
    assertThat(run(projectionQuery)).isEqualTo(projection);
  }

  @Test
  @Timeout(300)
  void whileLaggingStillEquivalent() throws Exception {
    final ColumnarTestData data = data();
    data.deleteRatio = 0.2;
    data.reuseRatio = 0.1;
    data.bigRecordRatio = 0.02;
    load(data, 5_000);
    database.command("sql", "CREATE INDEX ON " + TYPE + " (author) NOTUNIQUE");

    try (final AutoCloseable hold = manager().suspendApply()) {
      data.mutate(database, 300);
      final Answer answer = assertDisjunctive(query(WHERE, BRANDS, SMALL));
      assertThat(answer.rows()).isPositive();

      // an INDEX filter on top, read in the open transaction that writes one of its matches
      final String authors = "author IN ['author-1', 'author-2', 'author-3']";
      final String sql = "SELECT FROM " + TYPE + " WHERE " + WHERE + " FACETS brand TERMS TOP 1000 FILTER " + BRANDS
          + ", author TERMS TOP 1000 FILTER " + authors;
      assertThat(explain(sql)).contains("[PROJECTION]", "[INDEX]");
      database.begin();
      try {
        final RID moved;
        try (final ResultSet rs = database.query("sql", "SELECT @rid AS rid FROM " + TYPE + " WHERE " + WHERE + " AND author = 'author-9' LIMIT 1")) {
          moved = rs.next().getProperty("rid");
        }
        database.lookupByRID(moved, true).asDocument().modify().set("author", "author-1").set("brand", "brand-2").save();
        try (final ResultSet rs = database.query("sql", sql)) {
          long rows = 0;
          boolean seen = false;
          while (rs.hasNext()) {
            seen |= rs.next().getIdentity().orElseThrow().equals(moved);
            rows++;
          }
          assertThat(seen).isTrue();
          assertThat(rows).isEqualTo(count(WHERE + " AND " + BRANDS + " AND " + authors));
          assertThat(asMap(rs.getFacets().get("brand"))).isEqualTo(groupBy("brand", WHERE + " AND " + authors));
          assertThat(asMap(rs.getFacets().get("author"))).isEqualTo(groupBy("author", WHERE + " AND " + BRANDS));
        }
      } finally {
        database.rollback();
      }
    }
  }

  @Test
  @Timeout(300)
  void filterWithoutOtherFacetsFiltersRows() {
    load(data(), 5_000);
    final String sql = "SELECT FROM " + TYPE + " WHERE " + WHERE + " FACETS brand TERMS TOP 1000 FILTER brand = 'brand-1'";
    try (final ResultSet rs = database.query("sql", sql)) {
      long rows = 0;
      while (rs.hasNext()) {
        assertThat(rs.next().<String>getProperty("brand")).isEqualTo("brand-1");
        rows++;
      }
      assertThat(rows).isEqualTo(count(WHERE + " AND brand = 'brand-1'")).isPositive();
      // its own FILTER does not narrow the facet: every brand of the WHERE
      assertThat(asMap(rs.getFacets().get("brand"))).isEqualTo(groupBy("brand", WHERE));
    }

    // no WHERE either: the rows are the FILTER's matches, the facet counts every record
    try (final ResultSet rs = database.query("sql", "SELECT FROM " + TYPE + " FACETS brand TERMS TOP 1000 FILTER brand = 'brand-1'")) {
      assertThat(rs.stream().count()).isEqualTo(count("brand = 'brand-1'"));
      assertThat(asMap(rs.getFacets().get("brand"))).isEqualTo(groupBy("brand", "1 = 1"));
    }

    // LIMIT: the rows are cut, the facets are not, and the PROJECTION filter's snapshots are released with the result set
    final int bucketId = database.getSchema().getType(TYPE).getBuckets(true).getFirst().getFileId();
    final int references = ColumnarTestAccess.stateReferences(manager(), bucketId);
    try (final ResultSet rs = database.query("sql", sql + " LIMIT 3")) {
      assertThat(rs.stream().count()).isEqualTo(3);
      assertThat(asMap(rs.getFacets().get("brand"))).isEqualTo(groupBy("brand", WHERE));
    }
    assertThat(ColumnarTestAccess.stateReferences(manager(), bucketId)).isEqualTo(references);
    // closed after one row, the facets never asked
    try (final ResultSet rs = database.query("sql", sql)) {
      assertThat(rs.hasNext()).isTrue();
      rs.next();
      assertThat(ColumnarTestAccess.stateReferences(manager(), bucketId)).isEqualTo(references + 1);
    }
    assertThat(ColumnarTestAccess.stateReferences(manager(), bucketId)).isEqualTo(references);
  }

  /**
   * An INDEX FILTER whose RID set the memory budget refuses answers from the rows instead: rows and facets stay exact, the facets
   * available, the refusal shows in PROFILE, and every reservation is given back.
   */
  @Test
  @Timeout(300)
  void indexSetOverBudgetAnswersFromRows() {
    database.command("sql", "CREATE DOCUMENT TYPE Item");
    database.command("sql", "CREATE PROPERTY Item.k STRING");
    database.command("sql", "CREATE INDEX ON Item (k) NOTUNIQUE");
    // 80,000 RIDs for the FILTER: more than one 1 MiB step of the RID set
    for (int batch = 0; batch < 17; batch++) {
      final int from = batch * 5_000;
      database.transaction(() -> {
        for (int i = from; i < from + 5_000; i++)
          database.newDocument("Item").set("k", i >= 80_000 ? "c" : i % 2 == 0 ? "a" : "b").save();
      });
    }
    final String sql = "SELECT FROM Item FACETS k TERMS FILTER k IN ['a', 'b']";
    final Map<Object, Long> expected = Map.of("a", 40_000L, "b", 40_000L, "c", 5_000L);
    assertThat(explain(sql)).contains("[INDEX]");
    assertThat(profile(sql)).contains("[INDEX]").doesNotContain("fallback=");
    // granted (two steps), the RID set's reservations are given back with the result set
    assertThat(FacetMemoryBudget.instance().available()).isEqualTo(GlobalConfiguration.COLUMNAR_QUERY_MEMORY_BUDGET.getValueAsLong());

    final Object budget = GlobalConfiguration.COLUMNAR_QUERY_MEMORY_BUDGET.getValue();
    final Object wait = GlobalConfiguration.COLUMNAR_QUERY_BUDGET_WAIT.getValue();
    final long capacity = 1_536L * 1024;
    try {
      // room for the match set (one step) but not for the RID set (two)
      GlobalConfiguration.COLUMNAR_QUERY_MEMORY_BUDGET.setValue(capacity);
      GlobalConfiguration.COLUMNAR_QUERY_BUDGET_WAIT.setValue(0L);
      try (final ResultSet rs = database.query("sql", sql)) {
        assertThat(rs.stream().count()).isEqualTo(80_000L);
        final FacetResult k = rs.getFacets().get("k");
        assertThat(k.source()).isIn("index", "scan");
        assertThat(asMap(k)).isEqualTo(expected);
      }
      assertThat(FacetMemoryBudget.instance().available()).isEqualTo(capacity);
      assertThat(profile(sql)).contains("[INDEX fallback=RECORD (memory budget)]");
      assertThat(FacetMemoryBudget.instance().available()).isEqualTo(capacity);
    } finally {
      GlobalConfiguration.COLUMNAR_QUERY_MEMORY_BUDGET.setValue(budget);
      GlobalConfiguration.COLUMNAR_QUERY_BUDGET_WAIT.setValue(wait);
    }
  }

  private String profile(final String sql) {
    try (final ResultSet rs = database.query("sql", "PROFILE " + sql)) {
      return rs.next().getProperty("executionPlanAsString");
    }
  }
}
