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
package com.arcadedb.function.sql.fulltext;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.exception.QueryTerminatedException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.function.sql.text.SQLFunctionSearchFields;
import com.arcadedb.function.sql.text.SQLFunctionSearchFieldsMore;
import com.arcadedb.function.sql.text.SQLFunctionSearchIndex;
import com.arcadedb.function.sql.text.SQLFunctionSearchIndexMore;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.fulltext.FullTextSearch;
import com.arcadedb.query.RunningQuery;
import com.arcadedb.query.RunningQueryRegistry;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.InternalExecutionPlan;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.executor.WorkGuard;
import com.arcadedb.query.sql.parser.BaseExpression;
import com.arcadedb.query.sql.parser.Expression;
import com.arcadedb.query.sql.parser.FromClause;
import com.arcadedb.query.sql.parser.SelectStatement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Regression tests of the planner and executor handling of the full-text search functions (SEARCH_INDEX, SEARCH_FIELDS):
 * limit pushdown, cancellation, @rid and filter pushdown, OR merging and field restriction. Each nested class keeps its own
 * fixture.
 */
class FullTextSearchPlanningTest {
  /**
   * Regression for #10022: {@code WHERE (SEARCH_INDEX('T[a]', q) = true OR SEARCH_INDEX('T[b]', q) = true) ... ORDER BY $score DESC
   * LIMIT k} ran the branches one after the other, deduplicated them and sorted every row, so every match of every index was
   * loaded before the first row came back. Without the ORDER BY the query was cheap but not ranked across the indexes: all the
   * rows of the first branch came first, and under a LIMIT the second index could never contribute.
   * <p>
   * Each branch yields its rows best first (#9895), so the plan now merges them by score, lazily: a record matching several
   * branches is served once, at its best score, and a LIMIT loads about as many records as it returns. The expected answers are
   * computed here from one search per index: a record's score is the best of its branches' scores, the order is score
   * descending then RID ascending.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class SearchIndexOrMerge extends TestHelper {
    private static final int    DOCUMENTS = 600;
    private static final String A         = "SEARCH_INDEX('Doc[a]', 'common') = true";
    private static final String B         = "SEARCH_INDEX('Doc[b]', 'common') = true";
    private static final String C         = "SEARCH_INDEX('Doc[c]', 'common') = true";

    @ParameterizedTest
    @ValueSource(strings = { "BM25_1", "BM25_4", "CLASSIC_1", "CLASSIC_4" })
    void mergedAnswerIsTheRankingOfEveryBranch(final String variant) {
      createCorpus(variant);
      final List<Scored> ab = expected(n -> true, "a", "b");
      final List<Scored> abc = expected(n -> true, "a", "b", "c");
      final List<Scored> abEven = expected(n -> n % 2 == 0, "a", "b");
      assertThat(ab.size()).isGreaterThan(100);

      for (final String orderBy : new String[] { " ORDER BY $score DESC", "" }) {
        // two and three indexes, the whole answer and a prefix of it
        assertRows("SELECT @rid AS rid, $score AS score FROM Doc WHERE " + A + " OR " + B + orderBy, ab);
        assertRows("SELECT @rid AS rid, $score AS score FROM Doc WHERE " + A + " OR " + B + " OR " + C + orderBy, abc);
        for (final int limit : new int[] { 1, 10, 37, ab.size() - 1, ab.size() + 5 })
          assertRows("SELECT @rid AS rid, $score AS score FROM Doc WHERE " + A + " OR " + B + orderBy + " LIMIT " + limit,
              ab.subList(0, Math.min(limit, ab.size())));
        assertRows("SELECT @rid AS rid, $score AS score FROM Doc WHERE " + A + " OR " + B + " OR " + C + orderBy + " SKIP 13 LIMIT 20",
            abc.subList(13, 33));

        // a residual filter stays in each branch
        assertRows("SELECT @rid AS rid, $score AS score FROM Doc WHERE (" + A + " OR " + B + ") AND n % 2 = 0" + orderBy + " LIMIT 25",
            abEven.subList(0, 25));
        assertRows("SELECT @rid AS rid, $score AS score FROM Doc WHERE (" + A + " OR " + B + ") AND n % 2 = 0" + orderBy, abEven);

        // the score keeps its meaning when it is projected under another name, or not at all
        assertThat(rids("SELECT FROM Doc WHERE " + A + " OR " + B + orderBy + " LIMIT 30")).isEqualTo(ridsOf(ab.subList(0, 30)));
        assertThat(rids("SELECT @rid AS @rid, $score AS s FROM Doc WHERE " + A + " OR " + B + orderBy + " LIMIT 30"))
            .isEqualTo(ridsOf(ab.subList(0, 30)));
      }

      // the plan merges instead of sorting, and hands every branch the rows the LIMIT needs
      final String limited = plan("SELECT FROM Doc WHERE " + A + " OR " + B + " ORDER BY $score DESC LIMIT 10");
      assertThat(limited).contains("MERGE BY $score DESC").doesNotContain("ORDER BY $score").doesNotContain("DISTINCT");
      assertThat(limited.split("best 10 needed", -1)).hasSize(3);
      // a residual filter can reject rows a limit counted: the branches are merged, but not limited
      assertThat(plan("SELECT FROM Doc WHERE (" + A + " OR " + B + ") AND n % 2 = 0 ORDER BY $score DESC LIMIT 10"))
          .contains("MERGE BY $score DESC").doesNotContain(" needed)");
    }

    @ParameterizedTest
    @ValueSource(strings = { "BM25_1", "BM25_4" })
    void aLimitLoadsAboutTheRowsItReturns(final String variant) {
      createCorpus(variant);
      // The first search of each index computes its corpus statistics with a scan of the type, which counts as reads: not the
      // subject
      rids("SELECT FROM Doc WHERE " + A + " OR " + B + " OR " + C + " LIMIT 1");
      final int matches = expected(n -> true, "a", "b").size();

      // Before the fix the ORDER BY loaded every match of both indexes before the first row
      assertThat(recordsLoadedBy("SELECT FROM Doc WHERE " + A + " OR " + B + " ORDER BY $score DESC LIMIT 10")).isLessThan(20);
      assertThat(recordsLoadedBy("SELECT FROM Doc WHERE " + A + " OR " + B + " OR " + C + " ORDER BY $score DESC LIMIT 10"))
          .isLessThan(20);
      // The residual filter rejects half the rows, which have to be loaded to be rejected: still nowhere near every match
      assertThat(recordsLoadedBy("SELECT FROM Doc WHERE (" + A + " OR " + B + ") AND n % 2 = 0 ORDER BY $score DESC LIMIT 10"))
          .isLessThan(matches / 4);
    }

    /** BM25 only: the classic similarity scores a single hit the same in a long text as in a short one, so nothing is weak. */
    @ParameterizedTest
    @ValueSource(strings = { "BM25_1", "BM25_4" })
    void theSecondIndexContributesUnderALimit(final String variant) {
      final String similarity = variant.substring(0, variant.indexOf('_'));
      final int buckets = Integer.parseInt(variant.substring(variant.indexOf('_') + 1));
      database.transaction(() -> {
        database.command("sql", "CREATE DOCUMENT TYPE Pair BUCKETS " + buckets);
        database.command("sql", "CREATE PROPERTY Pair.a STRING");
        database.command("sql", "CREATE PROPERTY Pair.b STRING");
        database.command("sql", "CREATE INDEX ON Pair (a) FULL_TEXT METADATA {\"similarity\": \"" + similarity + "\"}");
        database.command("sql", "CREATE INDEX ON Pair (b) FULL_TEXT METADATA {\"similarity\": \"" + similarity + "\"}");
        // 20 weak matches in `a` (one hit in a long text), 20 strong ones in `b` (nothing else in the text)
        for (int i = 0; i < 20; i++)
          database.newDocument("Pair").set("kind", "weak", "a", "common " + "filler ".repeat(30), "b", "nothing").save();
        for (int i = 0; i < 20; i++)
          database.newDocument("Pair").set("kind", "strong", "a", "nothing", "b", "common common common").save();
        // the rest of each index's corpus, so neither index's statistics are degenerate
        for (int i = 0; i < 40; i++)
          database.newDocument("Pair").set("kind", "none", "a", "other words " + i, "b", "other words " + i).save();
      });

      final String or = "SEARCH_INDEX('Pair[a]', 'common') = true OR SEARCH_INDEX('Pair[b]', 'common') = true";
      for (final String query : new String[] { "SELECT kind FROM Pair WHERE " + or + " LIMIT 10",
          "SELECT kind FROM Pair WHERE " + or + " ORDER BY $score DESC LIMIT 10" }) {
        final List<String> kinds = new ArrayList<>();
        try (final ResultSet rs = database.query("sql", query)) {
          rs.forEachRemaining(r -> kinds.add(r.getProperty("kind")));
        }
        assertThat(kinds).as(query).hasSize(10).containsOnly("strong");
      }
    }

    @ParameterizedTest
    @ValueSource(strings = { "BM25_1", "CLASSIC_4" })
    void shapesTheMergeDoesNotServeKeepTheirAnswer(final String variant) {
      createCorpus(variant);
      final List<Scored> ab = expected(n -> true, "a", "b");

      // a branch that is not a scored search: no merge, the sort still ranks the answer
      final String mixed = "SELECT @rid AS rid FROM Doc WHERE " + A + " OR n = 3 ORDER BY $score DESC";
      assertThat(plan(mixed)).doesNotContain("MERGE BY");
      assertThat(rids("SELECT FROM Doc WHERE " + A + " OR n = 3")).hasSize(expected(n -> true, "a").size() + (aMatches(3) ? 0 : 1));

      // another order, an aggregation, a second search inside a branch: no merge, same rows as before
      for (final String query : new String[] { "SELECT FROM Doc WHERE " + A + " OR " + B + " ORDER BY $score ASC",
          "SELECT FROM Doc WHERE " + A + " OR " + B + " ORDER BY n",
          "SELECT count(*) FROM Doc WHERE " + A + " OR " + B,
          "SELECT FROM Doc WHERE (" + A + " AND " + C + ") OR " + B + " ORDER BY $score DESC",
          // every other exclusion of the planner: rows changed before the order, or a branch that may set $score again
          "SELECT FROM Doc WHERE " + A + " OR " + B + " ORDER BY $score DESC UNWIND n",
          "SELECT expand(a) FROM Doc WHERE " + A + " OR " + B + " ORDER BY $score DESC",
          "SELECT DISTINCT n FROM Doc WHERE " + A + " OR " + B,
          "SELECT n, count(*) FROM Doc WHERE " + A + " OR " + B + " GROUP BY n",
          "SELECT FROM Doc WHERE (" + A + " AND n < sysdate().asLong()) OR " + B + " ORDER BY $score DESC",
          "SELECT FROM Doc LET $even = n % 2 WHERE (" + A + " AND $even = 0) OR " + B + " ORDER BY $score DESC" })
        assertThat(plan(query)).as(query).doesNotContain("MERGE BY");
      assertThat(database.query("sql", "SELECT count(*) AS c FROM Doc WHERE " + A + " OR " + B).next().<Long>getProperty("c"))
          .isEqualTo(ab.size());
      final List<RID> ascending = rids("SELECT FROM Doc WHERE " + A + " OR " + B + " ORDER BY n");
      assertThat(ascending).hasSize(ab.size());
    }

    /**
     * A score projected under another name is still the score: the planner rewrites {@code ORDER BY $score} to the alias it
     * projects the key under, and that alias - or the statement's own - resolves to {@code $score}. A projection that gives the
     * name {@code $score} to something else is not the score.
     */
    @ParameterizedTest
    @ValueSource(strings = { "BM25_1" })
    void anOrderByAliasOfTheScoreIsTheScore(final String variant) {
      createCorpus(variant);
      final List<Scored> ab = expected(n -> true, "a", "b");

      for (final String query : new String[] {
          "SELECT @rid AS rid, $score AS score FROM Doc WHERE " + A + " OR " + B + " ORDER BY $score DESC LIMIT 10",
          "SELECT @rid AS rid, $score AS score FROM Doc WHERE " + A + " OR " + B + " ORDER BY score DESC LIMIT 10" }) {
        assertThat(plan(query)).as(query).contains("MERGE BY $score DESC").contains("best 10 needed");
        assertRows(query, ab.subList(0, 10));
      }
      // a single search takes the limit the same way (issue #9895)
      assertThat(plan("SELECT @rid AS rid, $score AS score FROM Doc WHERE " + A + " ORDER BY $score DESC LIMIT 10"))
          .contains("(best 10 needed)");
      assertThat(plan("SELECT @rid AS rid, $score AS score FROM Doc WHERE " + A + " ORDER BY score DESC LIMIT 10"))
          .contains("(best 10 needed)");

      final String shadowed = "SELECT n AS $score FROM Doc WHERE " + A + " OR " + B + " ORDER BY $score DESC LIMIT 10";
      assertThat(plan(shadowed)).doesNotContain("MERGE BY").doesNotContain(" needed)");
    }

    /**
     * The merge reads each branch's score right after it pulls the row, which holds whatever batch size the consumer asks
     * for: a branch evaluates its rows one at a time. Pulled one row, a few rows or many rows per batch, the plan answers the
     * same rows with the same scores.
     */
    @ParameterizedTest
    @ValueSource(strings = { "BM25_1", "BM25_4" })
    void anyBatchSizeAnswersTheSameRowsAndScores(final String variant) {
      createCorpus(variant);
      final List<Scored> abc = expected(n -> true, "a", "b", "c");
      final String query = "SELECT @rid AS rid, $score AS score FROM Doc WHERE " + A + " OR " + B + " OR " + C + " ORDER BY $score DESC";

      for (final int batch : new int[] { 1, 2, 3, 7, 64, 10_000 }) {
        final BasicCommandContext context = new BasicCommandContext();
        context.setDatabase((DatabaseInternal) database);
        final SelectStatement statement = (SelectStatement) ((DatabaseInternal) database).getStatementCache().get(query);
        final InternalExecutionPlan plan = statement.createExecutionPlanNoCache(context);
        assertThat(plan.prettyPrint(0, 2)).contains("MERGE BY $score DESC");

        final List<Scored> actual = new ArrayList<>();
        try {
          while (true) {
            final ResultSet page = plan.fetchNext(batch);
            if (!page.hasNext())
              break;
            while (page.hasNext()) {
              final Result row = page.next();
              actual.add(new Scored(row.getProperty("rid"), row.<Float>getProperty("score")));
            }
          }
        } finally {
          plan.close();
        }
        assertThat(actual).as("batch " + batch).containsExactlyElementsOf(abc);
      }
    }

    // ---------- helpers ----------

    private record Scored(RID rid, float score) {
    }

    /**
     * Doc {@code n} matches `a` when even, `b` when a multiple of 3 and `c` when a multiple of 7, with term frequencies and
     * lengths that vary per index, so the scores differ between the indexes of one record, and tie inside each index.
     */
    private void createCorpus(final String variant) {
      final String similarity = variant.substring(0, variant.indexOf('_'));
      final int buckets = Integer.parseInt(variant.substring(variant.indexOf('_') + 1));
      database.transaction(() -> {
        database.command("sql", "CREATE DOCUMENT TYPE Doc BUCKETS " + buckets);
        database.command("sql", "CREATE PROPERTY Doc.a STRING");
        database.command("sql", "CREATE PROPERTY Doc.b STRING");
        database.command("sql", "CREATE PROPERTY Doc.c STRING");
        database.command("sql", "CREATE PROPERTY Doc.n INTEGER");
        for (final String field : new String[] { "a", "b", "c" })
          database.command("sql", "CREATE INDEX ON Doc (" + field + ") FULL_TEXT METADATA {\"similarity\": \"" + similarity + "\"}");
        for (int n = 0; n < DOCUMENTS; n++)
          database.newDocument("Doc").set("n", n)
              .set("a", aMatches(n) ? "common " + "common ".repeat(n % 3) + "filler ".repeat(n % 4) : "nothing here " + n)
              .set("b", n % 3 == 0 ? "common " + "common ".repeat(n % 5) + "padding ".repeat(n % 2) : "nothing there " + n)
              .set("c", n % 7 == 0 ? "common " + "other ".repeat(n % 6) : "nothing at all " + n).save();
      });
    }

    private static boolean aMatches(final int n) {
      return n % 2 == 0;
    }

    /** The ranking of the union of the given indexes' matches whose `n` passes the filter, computed one index at a time. */
    private List<Scored> expected(final Predicate<Integer> filter, final String... fields) {
      final Map<RID, Float> best = new HashMap<>();
      for (final String field : fields)
        try (final ResultSet rs = database.query("sql",
            "SELECT @rid AS rid, n, $score AS score FROM Doc WHERE SEARCH_INDEX('Doc[" + field + "]', 'common') = true")) {
          rs.forEachRemaining(r -> {
            if (filter.test(r.getProperty("n")))
              best.merge(r.getProperty("rid"), r.<Float>getProperty("score"), Math::max);
          });
        }
      final List<Scored> ranked = new ArrayList<>(best.size());
      best.forEach((rid, score) -> ranked.add(new Scored(rid, score)));
      ranked.sort((x, y) -> {
        final int byScore = Float.compare(y.score(), x.score());
        return byScore != 0 ? byScore : x.rid().compareTo(y.rid());
      });
      return ranked;
    }

    private void assertRows(final String query, final List<Scored> expected) {
      final List<Scored> actual = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", query)) {
        rs.forEachRemaining(r -> actual.add(new Scored(r.getProperty("rid"), r.<Float>getProperty("score"))));
      }
      assertThat(actual).as(query).containsExactlyElementsOf(expected);
    }

    private static List<RID> ridsOf(final List<Scored> scored) {
      final List<RID> rids = new ArrayList<>(scored.size());
      for (final Scored s : scored)
        rids.add(s.rid());
      return rids;
    }

    private List<RID> rids(final String query) {
      final List<RID> rids = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", query)) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          rids.add(row.getIdentity().orElseGet(() -> row.getProperty("@rid")));
        }
      }
      return rids;
    }

    private long recordsLoadedBy(final String query) {
      final long before = readRecords();
      try (final ResultSet rs = database.query("sql", query)) {
        int rows = 0;
        while (rs.hasNext()) {
          rs.next();
          ++rows;
        }
        assertThat(rows).as(query).isEqualTo(10);
      }
      return readRecords() - before;
    }

    private long readRecords() {
      return ((Number) ((DatabaseInternal) database).getStats().get("readRecord")).longValue();
    }

    private String plan(final String query) {
      try (final ResultSet rs = database.query("sql", query)) {
        return rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
      }
    }
  }

  /**
   * Regression for #11069: a full-text search combined with a filter on another property. The SQL planner gave up the limit for any
   * conjunct besides the search and loaded matches until enough passed the filter, nearly every match for a restrictive tenant;
   * {@code db.index.fulltext.queryNodes} had no way to filter before its own {@code limit}, so a {@code WHERE} after the
   * {@code CALL} answered too few rows.
   * <p>
   * Corpus: {@value #DOCUMENTS} vertices that all match {@code common}. Tenant {@code a} owns most of them and scores highest;
   * tenant {@code b} owns {@value #TENANT_B_DOCUMENTS} and scores lowest, the worst case for a filter that rejects the best
   * matches first.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class FilteredFullTextSearch extends TestHelper {
    private static final int    DOCUMENTS          = 2_000;
    private static final int    TENANT_B_DOCUMENTS = 20;
    private static final int    WANTED             = 10;
    /** Loads tolerated beyond WANTED: none are needed when the filter is decided before the load. */
    private static final int    SLACK              = 5;
    private static final String SEARCH             = "SEARCH_INDEX('Doc[content]', 'common') = true";

    @Test
    void sqlRestrictiveFilterIsDecidedBeforeTheLoad() {
      createCorpus(1, true);
      final String query = "SELECT FROM Doc WHERE " + SEARCH + " AND tenant = 'b' LIMIT " + WANTED;

      assertThat(plan(query)).contains("(best " + WANTED + " needed)").contains("INDEX PRE-FILTER (tenant = 'b')");
      assertThat(recordsLoadedBy(query)).as("records loaded for LIMIT " + WANTED + " of " + DOCUMENTS + " matches")
          .isLessThanOrEqualTo(WANTED + SLACK);
      assertThat(tenantsOf(query)).containsOnly("b").hasSize(WANTED);

      // a null value lists nothing through the index: the condition is left to the records, and answers as without a search
      final String nullTenant = "SELECT FROM Doc WHERE " + SEARCH + " AND tenant = :t LIMIT " + WANTED;
      final Map<String, Object> nullParameter = new HashMap<>();
      nullParameter.put("t", null);
      try (final ResultSet rs = database.query("sql", nullTenant, nullParameter);
          final ResultSet scan = database.query("sql", "SELECT count(@rid) AS c FROM Doc WHERE tenant = :t", nullParameter)) {
        int rows = 0;
        while (rs.hasNext()) {
          rs.next();
          ++rows;
        }
        assertThat((long) rows).isEqualTo(Math.min(WANTED, scan.next().<Number>getProperty("c").longValue()));
      }
    }

    @Test
    void sqlRestrictiveFilterOverSeveralBuckets() {
      createCorpus(4, true);
      final String query = "SELECT @rid AS rid, $score AS score FROM Doc WHERE " + SEARCH + " AND tenant = :t ORDER BY $score DESC LIMIT " + WANTED;

      assertThat(recordsLoadedBy(query, Map.of("t", "b"))).isLessThanOrEqualTo(WANTED + SLACK);
      assertSameAnswerAsTheUnfilteredSearch(query, Map.of("t", "b"));
    }

    @Test
    void sqlPermissiveFilterStaysCheap() {
      createCorpus(1, true);
      final String query = "SELECT FROM Doc WHERE " + SEARCH + " AND tenant = 'a' LIMIT " + WANTED;
      assertThat(recordsLoadedBy(query)).isLessThanOrEqualTo(WANTED + SLACK);
      assertThat(tenantsOf(query)).containsOnly("a").hasSize(WANTED);
    }

    @Test
    void sqlFilterWithoutAnIndexStaysCorrect() {
      createCorpus(4, false);
      final String query = "SELECT @rid AS rid, $score AS score FROM Doc WHERE " + SEARCH + " AND tenant = :t ORDER BY $score DESC LIMIT " + WANTED;
      // no index lists the tenant: the records are tested once loaded, and the limit is not cut before the test
      assertSameAnswerAsTheUnfilteredSearch(query, Map.of("t", "b"));
      assertThat(tenantsOf("SELECT FROM Doc WHERE " + SEARCH + " AND tenant = 'b' SKIP 15 LIMIT " + WANTED)).containsOnly("b")
          .hasSize(TENANT_B_DOCUMENTS - 15);
    }

    @Test
    void sqlConditionOnAPerRecordVariableIsNotPushedAsAConstant() {
      createCorpus(1, true);
      // $own changes from row to row: the condition stays a filter on the loaded records, and holds for each of them
      final String perRecord = "SELECT FROM Doc LET $own = tenant WHERE " + SEARCH + " AND tenant = $own LIMIT " + WANTED;
      assertThat(plan(perRecord)).doesNotContain("INDEX PRE-FILTER");
      assertThat(tenantsOf(perRecord)).hasSize(WANTED);

      // a global LET holds one value for the whole statement: it is pushed
      final String global = "SELECT FROM Doc LET $t = (SELECT 'b' AS v).v[0] WHERE " + SEARCH + " AND tenant = $t LIMIT " + WANTED;
      assertThat(tenantsOf(global)).containsOnly("b").hasSize(WANTED);
    }

    @Test
    void sqlCaseInsensitiveIndexListsASuperset() {
      createCorpus(2, false);
      database.command("sql", "CREATE INDEX ON Doc (tenant COLLATE ci) NOTUNIQUE");
      // 'B' matches no record with the case-sensitive '=', although the case-insensitive index lists every 'b'
      assertThat(tenantsOf("SELECT FROM Doc WHERE " + SEARCH + " AND tenant = 'B' LIMIT " + WANTED)).isEmpty();
      assertSameAnswerAsTheUnfilteredSearch("SELECT @rid AS rid, $score AS score FROM Doc WHERE " + SEARCH + " AND tenant = :t ORDER BY $score DESC LIMIT "
          + WANTED, Map.of("t", "b"));
    }

    @Test
    void sqlFilterWithAnotherConjunctAndARidCondition() {
      createCorpus(1, true);
      final List<RID> tenantB = new ArrayList<>(matchesByScore("b").keySet());
      final RID cursor = tenantB.stream().sorted().toList().get(4);
      final String query = "SELECT FROM Doc WHERE " + SEARCH + " AND tenant = 'b' AND @rid > :cursor AND k >= 0 LIMIT " + WANTED;
      final List<RID> expected = new ArrayList<>();
      for (final RID rid : tenantB)
        if (rid.compareTo(cursor) > 0 && expected.size() < WANTED)
          expected.add(rid);
      final List<RID> found = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", query, Map.of("cursor", cursor))) {
        while (rs.hasNext())
          found.add(rs.next().getIdentity().orElseThrow());
      }
      assertThat(found).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    void cypherWhereOptionFiltersBeforeTheLimit() {
      createCorpus(2, true);
      // The WHERE after the CALL only sees the procedure's own best rows, as in Neo4j: none of them is of tenant b
      assertThat(cypherTenants("CALL db.index.fulltext.queryNodes('Doc[content]', 'common', {limit: " + WANTED + "}) YIELD node "
          + "WHERE node.tenant = 'b' RETURN node", Map.of())).isEmpty();

      final long before = readRecords();
      assertThat(cypherTenants("CALL db.index.fulltext.queryNodes('Doc[content]', 'common', {limit: " + WANTED
          + ", where: {tenant: $t}}) YIELD node RETURN node", Map.of("t", "b"))).containsOnly("b").hasSize(WANTED);
      assertThat(readRecords() - before).isLessThanOrEqualTo(2L * (WANTED + SLACK));
    }

    @Test
    void cypherWhereOptionWithoutAnIndexAndAsACondition() {
      createCorpus(2, false);
      assertThat(cypherTenants("CALL db.index.fulltext.queryNodes('Doc[content]', 'common', {limit: " + WANTED
          + ", where: {tenant: 'b'}}) YIELD node RETURN node", Map.of())).containsOnly("b").hasSize(WANTED);
      assertThat(cypherTenants("CALL db.index.fulltext.queryNodes('Doc[content]', 'common', {skip: 15, limit: " + WANTED
          + ", where: 'tenant = :t'}) YIELD node RETURN node", Map.of("t", "b"))).containsOnly("b").hasSize(TENANT_B_DOCUMENTS - 15);

      // the scores of the filtered rows are those of the unfiltered search
      final Map<RID, Float> unfiltered = matchesByScore(null);
      try (final ResultSet rs = database.query("opencypher", "CALL db.index.fulltext.queryNodes('Doc[content]', 'common', {limit: 3, "
          + "where: {tenant: 'b'}}) YIELD node, score RETURN node, score")) {
        final Map<RID, Float> expected = new LinkedHashMap<>();
        for (final Map.Entry<RID, Float> entry : matchesByScore("b").entrySet())
          if (expected.size() < 3)
            expected.put(entry.getKey(), entry.getValue());
        final Map<RID, Float> found = new LinkedHashMap<>();
        while (rs.hasNext()) {
          final Result row = rs.next();
          final RID rid = row.<Document>getProperty("node").getIdentity();
          found.put(rid, ((Number) row.getProperty("score")).floatValue());
          assertThat(found.get(rid)).isEqualTo(unfiltered.get(rid));
        }
        assertThat(found).containsExactlyEntriesOf(expected);
      }
    }

    @Test
    void cypherFilterOption() {
      createCorpus(1, false);
      final List<RID> some = new ArrayList<>(matchesByScore("b").keySet()).subList(0, 3);
      assertThat(cypherTenants("CALL db.index.fulltext.queryNodes('Doc[content]', 'common', {limit: " + WANTED
          + ", filter: $rids}) YIELD node RETURN node", Map.of("rids", some))).containsOnly("b").hasSize(3);
      assertThat(cypherTenants("CALL db.index.fulltext.queryNodes('Doc[content]', 'common', {filter: []}) YIELD node RETURN node",
          Map.of())).isEmpty();
      assertThatThrownBy(() -> cypherTenants("CALL db.index.fulltext.queryNodes('Doc[content]', 'common', {other: 1}) YIELD node "
          + "RETURN node", Map.of())).hasMessageContaining("where").hasMessageContaining("filter");
    }

    @Test
    void searchIndexRefusesAThirdArgument() {
      createCorpus(1, false);
      assertThatThrownBy(() -> tenantsOf("SELECT FROM Doc WHERE SEARCH_INDEX('Doc[content]', 'common', {}) = true LIMIT 1"))
          .isInstanceOf(CommandSemanticException.class);
    }

    private void createCorpus(final int buckets, final boolean tenantIndex) {
      database.transaction(() -> {
        database.command("sql", "CREATE VERTEX TYPE Doc BUCKETS " + buckets);
        database.command("sql", "CREATE PROPERTY Doc.content STRING");
        database.command("sql", "CREATE PROPERTY Doc.tenant STRING");
        database.command("sql", "CREATE PROPERTY Doc.k INTEGER");
        database.command("sql", "CREATE INDEX ON Doc (content) FULL_TEXT METADATA {\"similarity\": \"BM25\"}");
        if (tenantIndex)
          database.command("sql", "CREATE INDEX ON Doc (tenant) NOTUNIQUE");
        for (int i = 0; i < DOCUMENTS; i++) {
          final boolean tenantB = i % (DOCUMENTS / TENANT_B_DOCUMENTS) == 0;
          // tenant b: one occurrence in a long text of varying length (lowest BM25, all different); tenant a: the term repeated
          database.newVertex("Doc").set("tenant", tenantB ? "b" : "a").set("k", i)
              .set("content", tenantB ? "common " + "filler ".repeat(20 + i % 37) : "common common common common").save();
        }
      });
      // The first search computes the index's corpus statistics with a scan of the type, which counts as reads: not the subject
      tenantsOf("SELECT FROM Doc WHERE " + SEARCH + " LIMIT 1");
    }

    /** The matches of the unfiltered search of tenant {@code tenant} (every one when null), best first. */
    private Map<RID, Float> matchesByScore(final String tenant) {
      final Map<RID, Float> matches = new LinkedHashMap<>();
      try (final ResultSet rs = database.query("sql", "SELECT @rid AS rid, tenant, $score AS score FROM Doc WHERE " + SEARCH
          + " ORDER BY score DESC, rid")) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          if (tenant == null || tenant.equals(row.getProperty("tenant")))
            matches.put(row.getProperty("rid"), ((Number) row.getProperty("score")).floatValue());
        }
      }
      return matches;
    }

    private void assertSameAnswerAsTheUnfilteredSearch(final String query, final Map<String, Object> parameters) {
      final Map<RID, Float> expected = new LinkedHashMap<>();
      for (final Map.Entry<RID, Float> entry : matchesByScore((String) parameters.get("t")).entrySet())
        if (expected.size() < WANTED)
          expected.put(entry.getKey(), entry.getValue());

      final Map<RID, Float> found = new LinkedHashMap<>();
      try (final ResultSet rs = database.query("sql", query, parameters)) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          found.put(row.getProperty("rid"), ((Number) row.getProperty("score")).floatValue());
        }
      }
      assertThat(found).containsExactlyEntriesOf(expected);
    }

    private long recordsLoadedBy(final String query) {
      return recordsLoadedBy(query, Map.of());
    }

    private long recordsLoadedBy(final String query, final Map<String, Object> parameters) {
      final long before = readRecords();
      try (final ResultSet rs = database.query("sql", query, parameters)) {
        int rows = 0;
        while (rs.hasNext()) {
          rs.next();
          ++rows;
        }
        assertThat(rows).as(query).isEqualTo(WANTED);
      }
      return readRecords() - before;
    }

    private long readRecords() {
      return ((Number) ((DatabaseInternal) database).getStats().get("readRecord")).longValue();
    }

    private String plan(final String query) {
      try (final ResultSet rs = database.query("sql", "EXPLAIN " + query)) {
        return rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
      }
    }

    private List<String> tenantsOf(final String query) {
      final List<String> tenants = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", query)) {
        while (rs.hasNext())
          tenants.add(rs.next().getProperty("tenant"));
      }
      return tenants;
    }

    private List<String> cypherTenants(final String query, final Map<String, Object> parameters) {
      final List<String> tenants = new ArrayList<>();
      try (final ResultSet rs = database.query("opencypher", query, parameters)) {
        while (rs.hasNext())
          tenants.add(rs.next().<Document>getProperty("node").getString("tenant"));
      }
      return tenants;
    }
  }

  /**
   * Regression for #9894: SEARCH_INDEX and the other full-text functions did all their work - posting walks, scoring, sorting and
   * loading every match - before the first row, where no check of the executor reaches. A terminated statement, or one past
   * its deadline, kept a thread and the heap busy until the whole search was over.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class SearchIndexCancellation extends TestHelper {
    /** Well past the 1024 postings between two checks of a guard. */
    private static final int DOCUMENTS = 3_000;

    private final RunningQueryRegistry registry = new RunningQueryRegistry();

    @ParameterizedTest
    @ValueSource(strings = { "BM25_1", "BM25_4", "CLASSIC_1", "CLASSIC_4" })
    void terminatedStatementStopsInsideTheSearch(final String variant) {
      createCorpus(variant);

      for (final String query : new String[] { "common", "common OR word*", "+common +word1*" }) {
        final Throwable failure;
        try (final RunningQuery q = registry.register(database.getName(), "admin", "test", null, null, RunningQuery.Client.NONE)) {
          q.terminate("root");
          failure = catchThrowable(() -> {
            try (final ResultSet rs = database.query("sql",
                "SELECT FROM Doc WHERE SEARCH_INDEX('Doc[content]', ?) = true LIMIT 5", query)) {
              while (rs.hasNext())
                rs.next();
            }
          });
        }

        assertThat(failure).as(variant + " '" + query + "'").isInstanceOf(QueryTerminatedException.class);
        // Before the fix the search ran to the end and a later check of the executor reported the termination
        assertThat(raisedInsideTheFullTextSearch(failure))
            .as(variant + " '" + query + "' must stop inside the search, not after it: " + Arrays.toString(failure.getStackTrace()))
            .isTrue();
      }
    }

    @ParameterizedTest
    @ValueSource(strings = { "BM25_1", "BM25_4", "CLASSIC_1", "CLASSIC_4" })
    void searchPastTheDeadlineTimesOut(final String variant) {
      createCorpus(variant);
      final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName("Doc[content]");

      final BasicCommandContext context = new BasicCommandContext();
      context.setDatabase((DatabaseInternal) database);
      context.setCommandDeadline(System.currentTimeMillis() - 1, "test deadline");

      final Throwable failure = catchThrowable(() -> FullTextSearch.search(index, "common", -1, WorkGuard.forCommandDeadline(context)));
      assertThat(failure).as(variant).isInstanceOf(TimeoutException.class);

      // The same search with no bound answers every document
      final Map<RID, Float> all = FullTextSearch.search(index, "common", -1, WorkGuard.forCommandDeadline(null));
      assertThat(all).hasSize(DOCUMENTS);
    }

    @ParameterizedTest
    @ValueSource(strings = { "BM25_1", "CLASSIC_4" })
    void searchFieldsAndMoreLikeThisStopInsideTheSearch(final String variant) {
      createCorpus(variant);
      // These functions are evaluated row by row after a scan, whose own check catches a statement terminated before its first
      // row: what was unguarded is the search their first evaluation runs, so they are called with a deadline already past
      final Document source = database.iterateType("Doc", false).next().asDocument();
      final BasicCommandContext context = new BasicCommandContext();
      context.setDatabase((DatabaseInternal) database);
      context.setCommandDeadline(System.currentTimeMillis() - 1, "test deadline");

      assertThat(catchThrowable(() -> new SQLFunctionSearchFields().execute(null, source, null,
          new Object[] { List.of("content"), "common" }, context))).as("SEARCH_FIELDS").isInstanceOf(TimeoutException.class);
      assertThat(catchThrowable(() -> new SQLFunctionSearchIndexMore().execute(null, source, null,
          new Object[] { "Doc[content]", List.of(source.getIdentity()) }, context))).as("SEARCH_INDEX_MORE")
          .isInstanceOf(TimeoutException.class);
      assertThat(catchThrowable(() -> new SQLFunctionSearchFieldsMore().execute(null, source, null,
          new Object[] { List.of("content"), List.of(source.getIdentity()) }, context))).as("SEARCH_FIELDS_MORE")
          .isInstanceOf(TimeoutException.class);
    }

    private void createCorpus(final String variant) {
      final String similarity = variant.substring(0, variant.indexOf('_'));
      final int buckets = Integer.parseInt(variant.substring(variant.indexOf('_') + 1));
      database.transaction(() -> {
        database.command("sql", "CREATE DOCUMENT TYPE Doc BUCKETS " + buckets);
        database.command("sql", "CREATE PROPERTY Doc.content STRING");
        database.command("sql", "CREATE INDEX ON Doc (content) FULL_TEXT METADATA {\"similarity\": \"" + similarity + "\"}");
        for (int i = 0; i < DOCUMENTS; i++)
          database.newDocument("Doc").set("content", "common word" + i + " " + "common ".repeat(i % 7)).save();
      });
    }

    private static boolean raisedInsideTheFullTextSearch(final Throwable failure) {
      for (final StackTraceElement frame : failure.getStackTrace())
        if (frame.getClassName().startsWith("com.arcadedb.index.fulltext."))
          return true;
      return false;
    }
  }

  /**
   * Regression for #9895: {@code SEARCH_INDEX(...) ... LIMIT n} returned n rows but first matched, sorted and LOADED every match. The
   * records are now loaded lazily, best first, and when the statement provably consumes only the best n rows the search keeps
   * only those.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class SearchIndexLimit extends TestHelper {
    private static final int    DOCUMENTS = 2_000;
    private static final String SEARCH    = "SEARCH_INDEX('Doc[content]', 'common') = true";

    @ParameterizedTest
    @ValueSource(strings = { "BM25_1", "BM25_4", "CLASSIC_1", "CLASSIC_4" })
    void limitedSearchesAnswerWhatTheUnlimitedOneAnswers(final String variant) {
      createCorpus(variant);

      // The complete answer, in the order SEARCH_INDEX produces it (score descending, then RID)
      final List<RID> ordered = rids("SELECT FROM Doc WHERE " + SEARCH + " ORDER BY $score DESC");
      assertThat(ordered).hasSize(DOCUMENTS);
      final List<RID> unordered = rids("SELECT FROM Doc WHERE " + SEARCH);
      assertThat(unordered).as("SEARCH_INDEX produces its rows best first").isEqualTo(ordered);

      for (final int limit : new int[] { 1, 7, 10, 100, DOCUMENTS - 1, DOCUMENTS, DOCUMENTS + 10 }) {
        final List<RID> expected = ordered.subList(0, Math.min(limit, ordered.size()));
        assertThat(rids("SELECT FROM Doc WHERE " + SEARCH + " LIMIT " + limit)).as(variant + " LIMIT " + limit).isEqualTo(expected);
        assertThat(rids("SELECT FROM Doc WHERE " + SEARCH + " ORDER BY $score DESC LIMIT " + limit))
            .as(variant + " ORDER BY $score DESC LIMIT " + limit).isEqualTo(expected);
        assertThat(rids("SELECT FROM Doc WHERE " + SEARCH + " LIMIT :limit", limit)).as(variant + " LIMIT :limit " + limit)
            .isEqualTo(expected);
      }

      // SKIP is served out of the same rows
      assertThat(rids("SELECT FROM Doc WHERE " + SEARCH + " SKIP 5 LIMIT 10")).isEqualTo(ordered.subList(5, 15));
      assertThat(rids("SELECT FROM Doc WHERE " + SEARCH + " LIMIT 0")).isEmpty();
      // $score projected under another name: the order is still the score's
      assertThat(rids("SELECT @rid AS @rid, $score AS s FROM Doc WHERE " + SEARCH + " ORDER BY $score DESC LIMIT 10"))
          .isEqualTo(ordered.subList(0, 10));

      // A residual filter, an OR, another order and an aggregation must not be cut short by the limit
      final List<RID> even = new ArrayList<>();
      for (final RID rid : ordered)
        if (rid.asDocument().getInteger("n") % 2 == 0)
          even.add(rid);
      assertThat(rids("SELECT FROM Doc WHERE " + SEARCH + " AND n % 2 = 0 LIMIT 10")).isEqualTo(even.subList(0, 10));
      assertThat(rids("SELECT FROM Doc WHERE " + SEARCH + " ORDER BY n ASC LIMIT 3"))
          .containsExactlyElementsOf(rids("SELECT FROM Doc WHERE n < 3 ORDER BY n ASC"));
      assertThat(database.query("sql", "SELECT count(*) AS c FROM Doc WHERE " + SEARCH + " LIMIT 5").next().<Long>getProperty("c"))
          .isEqualTo(DOCUMENTS);
      final Set<RID> union = new HashSet<>(rids("SELECT FROM Doc WHERE " + SEARCH + " OR SEARCH_INDEX('Doc[content]', 'rare') = true"));
      assertThat(rids("SELECT FROM Doc WHERE " + SEARCH + " OR SEARCH_INDEX('Doc[content]', 'rare') = true LIMIT 10"))
          .hasSize(10).allMatch(union::contains);
    }

    @ParameterizedTest
    @ValueSource(strings = { "BM25_1", "BM25_4", "CLASSIC_1", "CLASSIC_4" })
    void scoresOfALimitedSearchAreTheCompleteSearchScores(final String variant) {
      createCorpus(variant);
      final List<Result> complete = rows("SELECT @rid AS rid, $score AS score FROM Doc WHERE " + SEARCH + " ORDER BY $score DESC");
      final List<Result> limited = rows("SELECT @rid AS rid, $score AS score FROM Doc WHERE " + SEARCH + " LIMIT 25");
      assertThat(limited).hasSize(25);
      for (int i = 0; i < limited.size(); i++) {
        assertThat(limited.get(i).<Object>getProperty("rid")).isEqualTo(complete.get(i).getProperty("rid"));
        assertThat(limited.get(i).<Float>getProperty("score")).isEqualTo(complete.get(i).<Float>getProperty("score"));
      }
    }

    @ParameterizedTest
    @ValueSource(strings = { "BM25_1", "BM25_4" })
    void recordsAreLoadedOnlyAsTheyArePulled(final String variant) {
      createCorpus(variant);
      // The first search computes the index's corpus statistics with a scan of the type, which counts as reads: not the subject
      rids("SELECT FROM Doc WHERE " + SEARCH + " LIMIT 1");

      // Pushed-down limit: the search keeps the best 10 matches and loads exactly those
      assertThat(recordsLoadedBy("SELECT FROM Doc WHERE " + SEARCH + " LIMIT 10")).isEqualTo(10);
      assertThat(recordsLoadedBy("SELECT FROM Doc WHERE " + SEARCH + " ORDER BY $score DESC LIMIT 10")).isEqualTo(10);

      // No pushdown (residual filter): the records are still loaded lazily, until the filter let 10 rows through. Before the
      // fix every one of the DOCUMENTS matches was loaded before the first row
      assertThat(recordsLoadedBy("SELECT FROM Doc WHERE " + SEARCH + " AND n % 2 = 0 LIMIT 10")).isLessThan(DOCUMENTS / 4);
      // An OR runs its branches one after the other: the LIMIT stops the first one long before its last match
      assertThat(recordsLoadedBy("SELECT FROM Doc WHERE " + SEARCH + " OR SEARCH_INDEX('Doc[content]', 'rare') = true LIMIT 10"))
          .isLessThan(DOCUMENTS / 4);
    }

    @ParameterizedTest
    @ValueSource(strings = { "BM25_4", "CLASSIC_4" })
    void theLimitIsPushedDownOnlyWhenItCannotChangeTheAnswer(final String variant) {
      createCorpus(variant);

      assertThat(plan("SELECT FROM Doc WHERE " + SEARCH + " LIMIT 10")).contains("(best 10 needed)");
      assertThat(plan("SELECT FROM Doc WHERE " + SEARCH + " ORDER BY $score DESC LIMIT 10")).contains("(best 10 needed)");
      assertThat(plan("SELECT title, $score FROM Doc WHERE " + SEARCH + " ORDER BY $score DESC LIMIT 10")).contains("(best 10 needed)");
      assertThat(plan("SELECT FROM Doc WHERE " + SEARCH + " SKIP 5 LIMIT 10")).contains("(best 15 needed)");
      // The branches of an OR of searches are merged by score (issue #10022), so each one is asked for the rows the LIMIT needs
      assertThat(plan("SELECT FROM Doc WHERE " + SEARCH + " OR SEARCH_INDEX('Doc[content]', 'rare') = true LIMIT 10").split(
          "\\(best 10 needed\\)", -1)).hasSize(3);

      for (final String query : new String[] { "SELECT FROM Doc WHERE " + SEARCH,
          "SELECT FROM Doc WHERE " + SEARCH + " AND n > 5 LIMIT 10",
          "SELECT FROM Doc WHERE " + SEARCH + " OR n < 10 LIMIT 10",
          "SELECT FROM Doc WHERE " + SEARCH + " ORDER BY n LIMIT 10",
          "SELECT FROM Doc WHERE " + SEARCH + " ORDER BY $score ASC LIMIT 10",
          "SELECT FROM Doc WHERE " + SEARCH + " ORDER BY $score DESC, n LIMIT 10",
          "SELECT n AS $score FROM Doc WHERE " + SEARCH + " ORDER BY $score DESC LIMIT 10",
          "SELECT count(*) FROM Doc WHERE " + SEARCH + " LIMIT 10",
          "SELECT n, count(*) FROM Doc WHERE " + SEARCH + " GROUP BY n LIMIT 10",
          "SELECT DISTINCT n FROM Doc WHERE " + SEARCH + " LIMIT 10",
          "SELECT expand(tags) FROM Doc WHERE " + SEARCH + " LIMIT 10",
          "SELECT FROM Doc WHERE " + SEARCH + " UNWIND tags LIMIT 10",
          "SELECT FROM bucket:Doc_0 WHERE " + SEARCH + " LIMIT 10" })
        assertThat(plan(query)).as(query).doesNotContain(" needed)");

      // A bucket target keeps only that bucket's rows: the limit counts after the bucket filter, not before
      final List<RID> ordered = rids("SELECT FROM Doc WHERE " + SEARCH + " ORDER BY $score DESC");
      final int bucketId = database.getSchema().getBucketByName("Doc_0").getFileId();
      final List<RID> inBucket = new ArrayList<>();
      for (final RID rid : ordered)
        if (rid.getBucketId() == bucketId)
          inBucket.add(rid);
      assertThat(rids("SELECT FROM bucket:Doc_0 WHERE " + SEARCH + " LIMIT 10")).isEqualTo(inBucket.subList(0, 10));
    }

    @ParameterizedTest
    @ValueSource(strings = { "BM25_1", "CLASSIC_4" })
    void aSubtypeTargetOverTheSupertypeIndexIsNotCutShort(final String variant) {
      createCorpus(variant);
      database.transaction(() -> {
        database.command("sql", "CREATE DOCUMENT TYPE SubDoc EXTENDS Doc");
        for (int i = 0; i < 20; i++)
          database.newDocument("SubDoc").set("n", DOCUMENTS + i).set("content", "common common common common common common").save();
      });

      // The index of the supertype answers Doc rows too, which the type filter drops after the fetch: the function declines the
      // limit the plan offers, or the best matches - Doc rows among them - would leave the subtype short
      final List<RID> subtypeRows = rids("SELECT FROM SubDoc WHERE SEARCH_INDEX('Doc[content]', 'common') = true ORDER BY $score DESC");
      assertThat(rids("SELECT FROM SubDoc WHERE SEARCH_INDEX('Doc[content]', 'common') = true LIMIT 5"))
          .isEqualTo(subtypeRows.subList(0, Math.min(5, subtypeRows.size())));
    }

    @ParameterizedTest
    @ValueSource(strings = { "BM25_1", "BM25_4", "CLASSIC_4" })
    void theBestMatchesContinueWithTheCompleteSearchWhenMoreRowsArePulled(final String variant) {
      createCorpus(variant);
      final List<RID> ordered = rids("SELECT FROM Doc WHERE " + SEARCH + " ORDER BY $score DESC");

      // A consumer that pulls past the limit it announced - as one does when a record of the best matches turns out to be gone -
      // gets the rest of the complete order, without a row twice or a row missing
      final SelectStatement statement = (SelectStatement) ((DatabaseInternal) database).getStatementCache()
          .get("SELECT SEARCH_INDEX('Doc[content]', 'common') FROM Doc");
      final FromClause target = statement.getTarget();
      final BaseExpression call = (BaseExpression) statement.getProjection().getItems().getFirst().getExpression().mathExpression;
      final Expression[] arguments = call.identifier.levelZero.functionCall.getParams().toArray(new Expression[0]);

      final BasicCommandContext context = new BasicCommandContext();
      context.setDatabase((DatabaseInternal) database);
      final List<RID> pulled = new ArrayList<>();
      for (final Record record : new SQLFunctionSearchIndex().searchFromTarget(target, null, true, context, arguments, 10))
        pulled.add(record.getIdentity());
      assertThat(pulled).isEqualTo(ordered);
    }

    @ParameterizedTest
    @ValueSource(strings = { "BM25_1", "BM25_4", "CLASSIC_4" })
    void theConditionEvaluatedAgainAnswersLikeTheCompleteSearch(final String variant) {
      createCorpus(variant);
      final List<Result> complete = rows("SELECT @rid AS rid, $score AS score FROM Doc WHERE " + SEARCH + " ORDER BY $score DESC");

      // The plan evaluates the condition again on every row, and here the projection evaluates the same search once more: both
      // answer from the best matches of the pushed-down fetch, with the score the complete search gives
      final String again = "SELECT @rid AS rid, $score AS score, SEARCH_INDEX('Doc[content]', 'common') AS again FROM Doc WHERE " + SEARCH
          + " LIMIT 10";
      assertThat(plan(again)).contains("(best 10 needed)");
      assertSamePrefix(rows(again), complete, true);

      // A residual filter keeps the complete search (no pushdown): same rows, same scores
      final String residual = "SELECT @rid AS rid, $score AS score FROM Doc WHERE " + SEARCH + " AND n >= 0 LIMIT 10";
      assertThat(plan(residual)).doesNotContain(" needed)");
      assertSamePrefix(rows(residual), complete, false);
    }

    @ParameterizedTest
    @ValueSource(strings = { "BM25_1", "BM25_4", "CLASSIC_1", "CLASSIC_4" })
    void matchesWhoseRecordIsGoneAreReplacedFromTheCompleteSearch(final String variant) {
      createCorpus(variant);
      final List<RID> ordered = rids("SELECT FROM Doc WHERE " + SEARCH + " ORDER BY $score DESC");

      // Records removed from their bucket behind the index's back: the index still answers them, the load does not. Three of
      // the best ten, among tied scores, so the rows that replace them come from past the best matches the limit kept
      final List<RID> gone = List.of(ordered.get(0), ordered.get(4), ordered.get(9));
      database.transaction(() -> {
        for (final RID rid : gone)
          ((DatabaseInternal) database).getSchema().getBucketById(rid.getBucketId()).deleteRecord(rid);
      });

      final List<RID> alive = new ArrayList<>(ordered);
      alive.removeAll(gone);
      assertThat(plan("SELECT FROM Doc WHERE " + SEARCH + " LIMIT 10")).contains("(best 10 needed)");
      assertThat(rids("SELECT FROM Doc WHERE " + SEARCH + " LIMIT 10")).isEqualTo(alive.subList(0, 10));
      assertThat(rids("SELECT FROM Doc WHERE " + SEARCH + " ORDER BY $score DESC LIMIT 10")).isEqualTo(alive.subList(0, 10));
      assertThat(rids("SELECT FROM Doc WHERE " + SEARCH + " SKIP 5 LIMIT 10")).isEqualTo(alive.subList(5, 15));

      // The dangling index entries and the bucket counter were left wrong on purpose: drop them before the integrity check
      database.getSchema().dropType("Doc");
    }

    private static void assertSamePrefix(final List<Result> actual, final List<Result> complete, final boolean matchedAgain) {
      assertThat(actual).hasSize(10);
      for (int i = 0; i < actual.size(); i++) {
        assertThat(actual.get(i).<Object>getProperty("rid")).isEqualTo(complete.get(i).getProperty("rid"));
        assertThat(actual.get(i).<Float>getProperty("score")).isEqualTo(complete.get(i).<Float>getProperty("score"));
        if (matchedAgain)
          assertThat(actual.get(i).<Boolean>getProperty("again")).isTrue();
      }
    }

    private void createCorpus(final String variant) {
      final String similarity = variant.substring(0, variant.indexOf('_'));
      final int buckets = Integer.parseInt(variant.substring(variant.indexOf('_') + 1));
      database.transaction(() -> {
        database.command("sql", "CREATE DOCUMENT TYPE Doc BUCKETS " + buckets);
        database.command("sql", "CREATE PROPERTY Doc.content STRING");
        database.command("sql", "CREATE PROPERTY Doc.n INTEGER");
        database.command("sql", "CREATE INDEX ON Doc (content) FULL_TEXT METADATA {\"similarity\": \"" + similarity + "\"}");
        for (int i = 0; i < DOCUMENTS; i++)
          // varying term frequency and document length, so scores differ and also tie
          database.newDocument("Doc").set("n", i).set("tags", List.of("a" + i, "b" + i))
              .set("content", "common " + "common ".repeat(i % 5) + "filler ".repeat(i % 3) + (i % 50 == 0 ? "rare" : "")).save();
      });
    }

    private long recordsLoadedBy(final String query) {
      final long before = readRecords();
      try (final ResultSet rs = database.query("sql", query)) {
        int rows = 0;
        while (rs.hasNext()) {
          rs.next();
          ++rows;
        }
        assertThat(rows).as(query).isEqualTo(10);
      }
      return readRecords() - before;
    }

    private long readRecords() {
      return ((Number) ((DatabaseInternal) database).getStats().get("readRecord")).longValue();
    }

    private String plan(final String query) {
      try (final ResultSet rs = database.query("sql", query)) {
        return rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
      }
    }

    private List<RID> rids(final String query, final Object... args) {
      final List<RID> rids = new ArrayList<>();
      try (final ResultSet rs = args.length == 0 ? database.query("sql", query) : database.query("sql", query, Map.of("limit", args[0]))) {
        while (rs.hasNext())
          rids.add(rs.next().getIdentity().orElseThrow());
      }
      return rids;
    }

    private List<Result> rows(final String query) {
      final List<Result> rows = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", query)) {
        while (rs.hasNext())
          rows.add(rs.next());
      }
      return rows;
    }
  }

  /**
   * Regression for #9909: the {@code @rid} conditions ANDed with {@code SEARCH_INDEX} were a filter after the fetch, so every match was
   * loaded - and, since the filter was a residual, the LIMIT was not pushed down - before {@code @rid > :cursor} rejected
   * most of them. Keyset pagination over a full-text result paid for every match on every page.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class RidConditionPushdown extends TestHelper {
    private static final int    DOCUMENTS = 1_200;
    private static final String SEARCH    = "SEARCH_INDEX('Doc[content]', 'common') = true";

    @ParameterizedTest
    @ValueSource(strings = { "BM25_1", "BM25_4", "CLASSIC_1", "CLASSIC_4" })
    void theRidConditionsAnswerWhatTheFilterAnswered(final String variant) {
      createCorpus(variant);
      final List<RID> ordered = rids("SELECT FROM Doc WHERE " + SEARCH + " ORDER BY $score DESC");
      assertThat(ordered).hasSize(DOCUMENTS);
      final List<RID> byRid = new ArrayList<>(ordered);
      byRid.sort(Comparator.naturalOrder());
      final RID cursor = byRid.get(DOCUMENTS / 3);
      final RID upper = byRid.get(2 * DOCUMENTS / 3);
      final List<RID> some = List.of(byRid.get(5), byRid.get(DOCUMENTS / 2), byRid.get(DOCUMENTS - 1));

      final Map<String, Object> params = new HashMap<>();
      params.put("cursor", cursor);
      params.put("upper", upper);
      params.put("some", some);

      assertSame("@rid > :cursor", r -> r.compareTo(cursor) > 0, ordered, params);
      assertSame("@rid >= :cursor", r -> r.compareTo(cursor) >= 0, ordered, params);
      assertSame("@rid < :cursor", r -> r.compareTo(cursor) < 0, ordered, params);
      assertSame("@rid <= :cursor", r -> r.compareTo(cursor) <= 0, ordered, params);
      assertSame("@rid > :cursor AND @rid < :upper", r -> r.compareTo(cursor) > 0 && r.compareTo(upper) < 0, ordered, params);
      assertSame("@rid <> :cursor", r -> !r.equals(cursor), ordered, params);
      assertSame("@rid NOT IN :some", r -> !some.contains(r), ordered, params);
      assertSame("@rid NOT IN [" + some.get(0) + ", " + some.get(1) + "]", r -> !r.equals(some.get(0)) && !r.equals(some.get(1)),
          ordered, params);
      // A literal and a RID's string spelling, which the comparison accepts too
      assertSame("@rid > " + cursor, r -> r.compareTo(cursor) > 0, ordered, params);
      assertSame("@rid > '" + cursor + "'", r -> r.compareTo(cursor) > 0, ordered, params);
      // Contradictory conditions: nothing to search for
      assertSame("@rid > :upper AND @rid < :cursor", r -> false, ordered, params);
      assertSame("@rid >= :cursor AND @rid < :cursor", r -> false, ordered, params);
      // Shapes decided by evaluating the condition per RID: an empty list, a list mixing RIDs and other values, a null bound
      assertSame("@rid NOT IN []", r -> true, ordered, params);
      assertSame("@rid NOT IN [" + some.get(0) + ", 'not a rid', 7]", r -> !r.equals(some.get(0)), ordered, params);
      params.put("nothing", null);
      assertSame("@rid > :nothing", r -> false, ordered, params);
      assertSame("@rid <> :nothing", r -> false, ordered, params);
      // IN, alone or with a range, takes the direct fetch by RID, as before, which answers in the list's order
      final List<RID> inAndRange = new ArrayList<>();
      for (final RID rid : some)
        if (rid.compareTo(cursor) > 0)
          inAndRange.add(rid);
      assertThat(rids("SELECT FROM Doc WHERE @rid IN :some AND @rid > :cursor AND " + SEARCH, params))
          .containsExactlyInAnyOrderElementsOf(inAndRange);
    }

    @ParameterizedTest
    @ValueSource(strings = { "BM25_1", "BM25_4", "CLASSIC_4" })
    void theRidConditionsAreDecidedBeforeTheRecordsAreLoaded(final String variant) {
      createCorpus(variant);
      final List<RID> byRid = rids("SELECT FROM Doc WHERE " + SEARCH + " ORDER BY @rid");
      final RID cursor = byRid.get(DOCUMENTS - 50);
      // The first search computes the index's corpus statistics with a scan of the type: not the subject
      rids("SELECT FROM Doc WHERE " + SEARCH + " LIMIT 1");

      final String filtered = "SELECT FROM Doc WHERE @rid > :cursor AND " + SEARCH;
      // Decided by the fetch, and no longer by the filter after it, which evaluates the search's own condition only
      final String plan = plan(filtered, cursor);
      assertThat(plan).contains("RID FILTER (@rid > :cursor)");
      assertThat(plan.indexOf("@rid > :cursor")).isEqualTo(plan.lastIndexOf("@rid > :cursor"));
      assertThat(recordsLoadedBy(filtered, cursor)).isEqualTo(49);

      // No residual filter left: the LIMIT is pushed down again
      final String limited = "SELECT FROM Doc WHERE @rid > :cursor AND " + SEARCH + " LIMIT 10";
      assertThat(plan(limited, cursor)).contains("(best 10 needed)");
      assertThat(recordsLoadedBy(limited, cursor)).isEqualTo(10);
    }

    @ParameterizedTest
    @ValueSource(strings = { "BM25_1", "BM25_4", "CLASSIC_1", "CLASSIC_4" })
    void keysetPaginationInRidOrderLoadsOnlyEachPage(final String variant) {
      createCorpus(variant);
      final List<RID> byRid = rids("SELECT FROM Doc WHERE " + SEARCH + " ORDER BY @rid");
      final List<RID> expected = new ArrayList<>(byRid);
      expected.sort(Comparator.naturalOrder());
      assertThat(byRid).isEqualTo(expected);
      rids("SELECT FROM Doc WHERE " + SEARCH + " LIMIT 1");

      final String page = "SELECT @rid AS rid, $score AS score FROM Doc WHERE @rid > :cursor AND " + SEARCH + " ORDER BY @rid LIMIT 100";
      final String planned = plan(page, new RID(-1, -1));
      assertThat(planned).contains("ORDER BY @rid ASC (first 100 needed)").doesNotContain("ORDER BY @rid ASC\n").doesNotContain("+ ORDER BY");

      final Map<Object, Float> scores = new HashMap<>();
      for (final Result row : rows("SELECT @rid AS rid, $score AS score FROM Doc WHERE " + SEARCH, Map.of()))
        scores.put(row.getProperty("rid"), row.<Float>getProperty("score"));

      final List<RID> paged = new ArrayList<>();
      RID cursor = new RID(-1, -1);
      while (true) {
        final long before = readRecords();
        final List<Result> rows = rows(page, Map.of("cursor", cursor));
        if (rows.isEmpty())
          break;
        // Each page loads its own rows, and nothing past them
        assertThat(readRecords() - before).isEqualTo(rows.size());
        for (final Result row : rows) {
          final RID rid = row.getProperty("rid");
          assertThat(row.<Float>getProperty("score")).isEqualTo(scores.get(rid));
          paged.add(rid);
        }
        cursor = paged.getLast();
      }
      assertThat(paged).isEqualTo(byRid);

      // Descending, from the end, and a limit past the last match
      final List<RID> descending = rids("SELECT FROM Doc WHERE @rid < :cursor AND " + SEARCH + " ORDER BY @rid DESC LIMIT 5",
          Map.of("cursor", byRid.getLast()));
      assertThat(descending).isEqualTo(byRid.reversed().subList(1, 6));
      assertThat(rids("SELECT FROM Doc WHERE @rid < :cursor AND " + SEARCH + " ORDER BY @rid DESC LIMIT 50", Map.of("cursor",
          byRid.get(9)))).isEqualTo(byRid.subList(0, 9).reversed());

      // SKIP is served out of the same RID order
      final String skipped = "SELECT FROM Doc WHERE @rid > :cursor AND " + SEARCH + " ORDER BY @rid SKIP 3 LIMIT 5";
      assertThat(plan(skipped, new RID(-1, -1))).contains("ORDER BY @rid ASC (first 8 needed)");
      assertThat(rids(skipped, Map.of("cursor", new RID(-1, -1)))).isEqualTo(byRid.subList(3, 8));

      // An order on more than @rid is sorted after the fetch as before
      final String twoKeys = "SELECT FROM Doc WHERE @rid > :cursor AND " + SEARCH + " ORDER BY @rid, n LIMIT 5";
      assertThat(plan(twoKeys, new RID(-1, -1))).doesNotContain("ORDER BY @rid ASC (").contains("ORDER BY");
      assertThat(rids(twoKeys, Map.of("cursor", new RID(-1, -1)))).isEqualTo(byRid.subList(0, 5));
    }

    @ParameterizedTest
    @ValueSource(strings = { "BM25_4", "CLASSIC_4" })
    void ridOrderIsOneOrderAcrossTheBucketsOfTheHierarchy(final String variant) {
      createCorpus(variant);
      database.transaction(() -> {
        database.command("sql", "CREATE DOCUMENT TYPE SubDoc EXTENDS Doc BUCKETS 3");
        for (int i = 0; i < 60; i++)
          database.newDocument("SubDoc").set("n", DOCUMENTS + i).set("content", "common filler").save();
      });
      final List<RID> all = new ArrayList<>(rids("SELECT FROM Doc WHERE " + SEARCH));
      all.sort(Comparator.naturalOrder());
      assertThat(all).hasSize(DOCUMENTS + 60);

      final String descending = "SELECT FROM Doc WHERE @rid > :cursor AND " + SEARCH + " ORDER BY @rid DESC LIMIT 25";
      assertThat(plan(descending, new RID(-1, -1))).contains("ORDER BY @rid DESC (first 25 needed)");
      assertThat(rids(descending, Map.of("cursor", new RID(-1, -1)))).isEqualTo(all.reversed().subList(0, 25));
      assertThat(rids("SELECT FROM Doc WHERE @rid > :cursor AND " + SEARCH + " ORDER BY @rid LIMIT 25", Map.of("cursor",
          all.get(100)))).isEqualTo(all.subList(101, 126));
    }

    @ParameterizedTest
    @ValueSource(strings = { "BM25_1", "CLASSIC_4" })
    void aMatchWhoseRecordIsGoneIsSkippedInRidOrder(final String variant) {
      createCorpus(variant);
      final List<RID> byRid = rids("SELECT FROM Doc WHERE " + SEARCH + " ORDER BY @rid");
      // Removed from its bucket behind the index's back: the index still answers it, the load does not
      final RID gone = byRid.get(3);
      database.transaction(() -> ((DatabaseInternal) database).getSchema().getBucketById(gone.getBucketId()).deleteRecord(gone));

      final List<RID> alive = new ArrayList<>(byRid);
      alive.remove(gone);
      final String page = "SELECT FROM Doc WHERE @rid > :cursor AND " + SEARCH + " ORDER BY @rid LIMIT 10";
      assertThat(rids(page, Map.of("cursor", new RID(-1, -1)))).isEqualTo(alive.subList(0, 10));
      assertThat(rids(page.replace("ORDER BY @rid", "ORDER BY @rid DESC"), Map.of("cursor", byRid.get(0))))
          .isEqualTo(alive.reversed().subList(0, 10));

      // The dangling index entry and the bucket counter were left wrong on purpose: drop them before the integrity check
      database.getSchema().dropType("Doc");
    }

    @Test
    void conditionsTheFunctionCannotDecideStayInTheFilter() {
      createCorpus("BM25_4");
      // A residual condition on a property keeps its filter; the @rid conjunct is still pushed
      final String mixed = "SELECT FROM Doc WHERE @rid > :cursor AND n % 2 = 0 AND " + SEARCH;
      final RID cursor = new RID(-1, -1);
      assertThat(plan(mixed, cursor)).contains("RID FILTER (@rid > :cursor)").contains("n % 2 = 0");
      assertThat(rids(mixed, Map.of("cursor", cursor))).hasSize(DOCUMENTS / 2);

      // A right-hand side that reads the record is not decidable on the RID
      final String perRecord = "SELECT FROM Doc WHERE @rid > link AND " + SEARCH;
      assertThat(plan(perRecord, cursor)).doesNotContain("RID FILTER");

      // A variable a per-record LET computes after the fetch changes from row to row: the condition stays in the filter
      final String perRecordLet = "SELECT FROM Doc LET $self = @rid WHERE @rid >= $self AND " + SEARCH;
      assertThat(plan(perRecordLet, cursor)).doesNotContain("RID FILTER");
      assertThat(rids(perRecordLet, Map.of())).hasSize(DOCUMENTS);
      assertThat(rids(perRecordLet.replace(">=", ">"), Map.of())).isEmpty();

      // An OR: each branch pushes its own conditions
      final List<RID> byRid = rids("SELECT FROM Doc WHERE " + SEARCH + " ORDER BY @rid");
      final RID first = byRid.get(10);
      final RID last = byRid.get(DOCUMENTS - 10);
      final List<RID> union = rids("SELECT FROM Doc WHERE (@rid < :first AND " + SEARCH + ") OR (@rid > :last AND " + SEARCH + ")",
          Map.of("first", first, "last", last));
      assertThat(union).hasSize(10 + 9);

      // Each branch remembers the matches it fetched: the rows of both are answered from their union, with the scores of the
      // complete search
      final Map<Object, Float> scores = new HashMap<>();
      for (final Result row : rows("SELECT @rid AS rid, $score AS score FROM Doc WHERE " + SEARCH, Map.of()))
        scores.put(row.getProperty("rid"), row.<Float>getProperty("score"));
      final List<Result> unionRows = rows("SELECT @rid AS rid, $score AS score FROM Doc WHERE (@rid < :first AND " + SEARCH
          + ") OR (@rid > :last AND " + SEARCH + ")", Map.of("first", first, "last", last));
      assertThat(unionRows).hasSize(10 + 9);

      // Only one branch has an @rid condition: it pushes it, the other searches unfiltered
      final List<RID> oneBranch = rids("SELECT FROM Doc WHERE (@rid < :first AND " + SEARCH + ") OR (" + SEARCH + " AND n < 0)",
          Map.of("first", first));
      assertThat(oneBranch).containsExactlyInAnyOrderElementsOf(byRid.subList(0, 10));

      // A variable a global LET computes is known before the fetch: pushed, with the answer of the parameter it holds
      final String globalLet = "SELECT FROM Doc LET $c = :cursor WHERE @rid > $c AND " + SEARCH;
      assertThat(plan(globalLet, first)).contains("RID FILTER (@rid > $c)");
      assertThat(rids(globalLet, Map.of("cursor", last))).containsExactlyInAnyOrderElementsOf(byRid.subList(DOCUMENTS - 9, DOCUMENTS));
      for (final Result row : unionRows)
        assertThat(row.<Float>getProperty("score")).isEqualTo(scores.get(row.<Object>getProperty("rid")));
    }

    @Test
    void matchPushesTheRidConditionsOfTheRootNode() {
      createCorpus("BM25_4");
      final List<RID> byRid = rids("SELECT FROM Doc WHERE " + SEARCH + " ORDER BY @rid");
      final RID cursor = byRid.get(DOCUMENTS - 30);
      rids("SELECT FROM Doc WHERE " + SEARCH + " LIMIT 1");

      final String match = "MATCH {type: Doc, as: d, where: (@rid > :cursor AND " + SEARCH + ")} RETURN d.@rid AS rid";
      final long before = readRecords();
      final List<Object> matched = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", match, Map.of("cursor", cursor))) {
        while (rs.hasNext())
          matched.add(rs.next().getProperty("rid"));
      }
      assertThat(matched).containsExactlyInAnyOrderElementsOf(byRid.subList(DOCUMENTS - 29, DOCUMENTS));
      // MATCH loads the records it returns, and not the matches the cursor excludes
      assertThat(readRecords() - before).isLessThan(DOCUMENTS / 4);
    }

    @Test
    void geospatialPredicatesDecideTheRidConditionsBeforeLoading() {
      database.transaction(() -> {
        database.command("sql", "CREATE DOCUMENT TYPE Place BUCKETS 4");
        database.command("sql", "CREATE PROPERTY Place.coords STRING");
        database.command("sql", "CREATE INDEX ON Place (coords) GEOSPATIAL");
        for (int i = 0; i < 200; i++)
          database.newDocument("Place").set("coords", "POINT(" + (10 + (i % 10) * 0.01) + " " + (45 + (i / 10) * 0.01) + ")").save();
      });
      final String within = "geo.within(coords, geo.geomFromText('POLYGON((9 44, 11 44, 11 46, 9 46, 9 44))')) = true";
      final List<RID> all = rids("SELECT FROM Place WHERE " + within + " ORDER BY @rid");
      assertThat(all).hasSize(200);
      final RID cursor = all.get(150);

      final String query = "SELECT FROM Place WHERE @rid > :cursor AND " + within;
      assertThat(plan(query, cursor)).contains("RID FILTER (@rid > :cursor)");
      final List<RID> after = rids(query + " ORDER BY @rid", Map.of("cursor", cursor));
      assertThat(after).isEqualTo(all.subList(151, 200));
      final long before = readRecords();
      assertThat(rids(query, Map.of("cursor", cursor))).hasSize(49);
      assertThat(readRecords() - before).isEqualTo(49);

      // '= false' asks for the places outside the shape, which the index cannot answer: the type is scanned
      final String westHalf = "geo.within(coords, geo.geomFromText('POLYGON((9 44, 10.045 44, 10.045 46, 9 46, 9 44))'))";
      assertThat(rids("SELECT FROM Place WHERE " + westHalf + " = true")).hasSize(100);
      assertThat(plan("SELECT FROM Place WHERE " + westHalf + " = false", cursor)).doesNotContain("INDEXED FUNCTION");
      assertThat(rids("SELECT FROM Place WHERE " + westHalf + " = false")).hasSize(100);
    }

    private void assertSame(final String ridCondition, final Predicate<RID> accepted, final List<RID> ordered,
        final Map<String, Object> params) {
      final List<RID> expected = new ArrayList<>();
      for (final RID rid : ordered)
        if (accepted.test(rid))
          expected.add(rid);

      final String pushed = "SELECT FROM Doc WHERE " + ridCondition + " AND " + SEARCH;
      assertThat(rids(pushed, params)).as(pushed).isEqualTo(expected);
      assertThat(rids(pushed + " ORDER BY $score DESC", params)).as(pushed).isEqualTo(expected);
      assertThat(rids(pushed + " LIMIT 7", params)).as(pushed + " LIMIT 7").isEqualTo(expected.subList(0, Math.min(7, expected.size())));
      final List<RID> expectedByRid = new ArrayList<>(expected);
      expectedByRid.sort(Comparator.naturalOrder());
      assertThat(rids(pushed + " ORDER BY @rid LIMIT 7", params)).as(pushed + " ORDER BY @rid LIMIT 7")
          .isEqualTo(expectedByRid.subList(0, Math.min(7, expectedByRid.size())));
      assertThat(rids(pushed + " ORDER BY @rid DESC", params)).as(pushed + " ORDER BY @rid DESC").isEqualTo(expectedByRid.reversed());

      // The same scores as without the pushdown
      final Map<Object, Float> scores = new HashMap<>();
      for (final Result row : rows("SELECT @rid AS rid, $score AS score FROM Doc WHERE " + SEARCH, Map.of()))
        scores.put(row.getProperty("rid"), row.<Float>getProperty("score"));
      for (final Result row : rows("SELECT @rid AS rid, $score AS score FROM Doc WHERE " + ridCondition + " AND " + SEARCH, params))
        assertThat(row.<Float>getProperty("score")).as(pushed).isEqualTo(scores.get(row.<Object>getProperty("rid")));
      // Also on the limit path, where each bucket keeps only its best accepted matches and the plan answers from them
      final List<Result> limitedRows = rows("SELECT @rid AS rid, $score AS score FROM Doc WHERE " + ridCondition + " AND " + SEARCH
          + " LIMIT 7", params);
      assertThat(limitedRows).hasSize(Math.min(7, expected.size()));
      for (int i = 0; i < limitedRows.size(); i++) {
        assertThat(limitedRows.get(i).<Object>getProperty("rid")).as(pushed + " LIMIT 7").isEqualTo(expected.get(i));
        assertThat(limitedRows.get(i).<Float>getProperty("score")).as(pushed + " LIMIT 7")
            .isEqualTo(scores.get(limitedRows.get(i).<Object>getProperty("rid")));
      }

      // And what a scan with the same condition answers
      assertThat(rids("SELECT FROM Doc WHERE " + ridCondition + " AND content LIKE '%common%' ORDER BY @rid", params)).as(ridCondition)
          .isEqualTo(expectedByRid);
    }

    private void createCorpus(final String variant) {
      final String similarity = variant.substring(0, variant.indexOf('_'));
      final int buckets = Integer.parseInt(variant.substring(variant.indexOf('_') + 1));
      database.transaction(() -> {
        database.command("sql", "CREATE DOCUMENT TYPE Doc BUCKETS " + buckets);
        database.command("sql", "CREATE PROPERTY Doc.content STRING");
        database.command("sql", "CREATE PROPERTY Doc.n INTEGER");
        database.command("sql", "CREATE INDEX ON Doc (content) FULL_TEXT METADATA {\"similarity\": \"" + similarity + "\"}");
        for (int i = 0; i < DOCUMENTS; i++)
          // varying term frequency and document length, so scores differ and also tie
          database.newDocument("Doc").set("n", i).set("link", new RID(0, i))
              .set("content", "common " + "common ".repeat(i % 5) + "filler ".repeat(i % 3)).save();
      });
    }

    private long recordsLoadedBy(final String query, final RID cursor) {
      final long before = readRecords();
      try (final ResultSet rs = database.query("sql", query, Map.of("cursor", cursor))) {
        while (rs.hasNext())
          rs.next();
      }
      return readRecords() - before;
    }

    private long readRecords() {
      return ((Number) ((DatabaseInternal) database).getStats().get("readRecord")).longValue();
    }

    private String plan(final String query, final RID cursor) {
      try (final ResultSet rs = database.query("sql", query, Map.of("cursor", cursor))) {
        return rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
      }
    }

    private List<RID> rids(final String query) {
      return rids(query, Map.of());
    }

    private List<RID> rids(final String query, final Map<String, Object> params) {
      final List<RID> rids = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", query, params)) {
        while (rs.hasNext())
          rids.add(rs.next().getIdentity().orElseThrow());
      }
      return rids;
    }

    private List<Result> rows(final String query, final Map<String, Object> params) {
      final List<Result> rows = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", query, params)) {
        while (rs.hasNext())
          rows.add(rs.next());
      }
      return rows;
    }
  }

  /**
   * Regression for #9910: {@code SEARCH_FIELDS(['a'], q)} searched every field of the index it picked, so a term found only in another
   * field matched too, and it was evaluated row by row after a scan of the whole type instead of through the index.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class SearchFieldsRestriction extends TestHelper {
    private static final int NOTES = 400;

    @ParameterizedTest
    @ValueSource(strings = { "BM25_1", "BM25_4", "CLASSIC_1", "CLASSIC_4" })
    void theSearchIsRestrictedToTheGivenFields(final String variant) {
      createCorpus(variant);

      // 'zebra' is only in field b of one note
      assertThat(count("SEARCH_FIELDS(['a'], 'zebra') = true")).isZero();
      assertThat(count("SEARCH_FIELDS(['b'], 'zebra') = true")).isEqualTo(1);
      assertThat(count("SEARCH_FIELDS(['a', 'b'], 'zebra') = true")).isEqualTo(1);
      assertThat(count("SEARCH_FIELDS(['b', 'a'], 'zebra') = true")).isEqualTo(1);

      // 'fever' is in a for the even notes and in b for every third note: listing both matches either
      final long inA = NOTES / 2;
      final long inB = (NOTES + 2) / 3;
      final long inBoth = (NOTES + 5) / 6;
      assertThat(count("SEARCH_FIELDS(['a'], 'fever') = true")).isEqualTo(inA);
      assertThat(count("SEARCH_FIELDS(['b'], 'fever') = true")).isEqualTo(inB);
      assertThat(count("SEARCH_FIELDS(['a', 'b'], 'fever') = true")).isEqualTo(inA + inB - inBoth);

      // Every kind of clause is restricted, not only a term: a phrase, a prefix, a wildcard, a fuzzy term, a negation
      assertThat(count("SEARCH_FIELDS(['a'], '\"zebra stripes\"') = true")).isZero();
      assertThat(count("SEARCH_FIELDS(['b'], '\"zebra stripes\"') = true")).isEqualTo(1);
      assertThat(count("SEARCH_FIELDS(['a'], 'zeb*') = true")).isZero();
      assertThat(count("SEARCH_FIELDS(['a'], 'z?bra') = true")).isZero();
      assertThat(count("SEARCH_FIELDS(['a'], 'zebre~1') = true")).isZero();
      assertThat(count("SEARCH_FIELDS(['b'], 'zebre~1') = true")).isEqualTo(1);
      assertThat(count("SEARCH_FIELDS(['a'], 'fever AND -cough') = true")).isEqualTo(count("SEARCH_INDEX('Note[a,b]', 'a:fever AND -a:cough') = true"));

      // A clause the query qualifies itself keeps its field
      assertThat(count("SEARCH_FIELDS(['a'], 'b:zebra') = true")).isEqualTo(1);
    }

    @ParameterizedTest
    @ValueSource(strings = { "BM25_1", "BM25_4", "CLASSIC_1", "CLASSIC_4" })
    void scoresAreThoseOfTheEquivalentFieldQualifiedSearchIndex(final String variant) {
      createCorpus(variant);
      assertSameScores("SEARCH_FIELDS(['a'], 'fever')", "SEARCH_INDEX('Note[a,b]', 'a:fever')");
      assertSameScores("SEARCH_FIELDS(['a'], 'fever cough')", "SEARCH_INDEX('Note[a,b]', 'a:fever a:cough')");
      assertSameScores("SEARCH_FIELDS(['b'], 'fever OR zebra')", "SEARCH_INDEX('Note[a,b]', 'b:fever OR b:zebra')");
      // Over exactly the fields of the index, it is SEARCH_INDEX on that index
      assertSameScores("SEARCH_FIELDS(['a', 'b'], 'fever')", "SEARCH_INDEX('Note[a,b]', 'fever')");
    }

    @ParameterizedTest
    @ValueSource(strings = { "BM25_1", "BM25_4", "CLASSIC_4" })
    void theConditionIsAnsweredByTheIndex(final String variant) {
      createCorpus(variant);

      final String plan = plan("SELECT FROM Note WHERE SEARCH_FIELDS(['a'], 'fever') = true");
      assertThat(plan).contains("FETCH FROM INDEXED FUNCTION").doesNotContain("FETCH FROM TYPE");

      // The pushdowns of SEARCH_INDEX apply: the limit, best first, and the lazy loading
      assertThat(plan("SELECT FROM Note WHERE SEARCH_FIELDS(['a'], 'fever') = true LIMIT 10")).contains("(best 10 needed)");
      final List<RID> ordered = rids("SELECT FROM Note WHERE SEARCH_FIELDS(['a'], 'fever') = true ORDER BY $score DESC");
      assertThat(rids("SELECT FROM Note WHERE SEARCH_FIELDS(['a'], 'fever') = true LIMIT 10")).isEqualTo(ordered.subList(0, 10));
      assertThat(rids("SELECT FROM Note WHERE SEARCH_FIELDS(['a'], 'fever') = true")).isEqualTo(ordered);

      // The first search computes the index's corpus statistics with a scan of the type: not the subject
      rids("SELECT FROM Note WHERE SEARCH_FIELDS(['a'], 'cough') = true LIMIT 1");
      final long before = readRecords();
      assertThat(rids("SELECT FROM Note WHERE SEARCH_FIELDS(['a'], 'cough') = true LIMIT 10")).hasSize(10);
      assertThat(readRecords() - before).isEqualTo(10);

      // Inside an OR with a plain condition the type is scanned and the condition evaluated per record: same answer
      assertThat(count("SEARCH_FIELDS(['a'], 'zebra') = true OR n < 0")).isZero();
      assertThat(count("SEARCH_FIELDS(['b'], 'zebra') = true OR n < 0")).isEqualTo(1);
    }

    @Test
    void aNegatedConditionIsNotAnsweredWithTheMatches() {
      createCorpus("BM25_4");
      // The index answers the records that match: '= false' asks for the others, which only a scan can find
      assertThat(count("SEARCH_FIELDS(['b'], 'zebra') = false")).isEqualTo(NOTES - 1);
      assertThat(count("SEARCH_INDEX('Note[a,b]', 'zebra') = false")).isEqualTo(NOTES - 1);
      assertThat(count("SEARCH_INDEX('Note[a,b]', 'zebra') <> true")).isEqualTo(NOTES - 1);
      assertThat(count("SEARCH_INDEX('Note[a,b]', 'zebra') = true")).isEqualTo(1);
      assertThat(plan("SELECT FROM Note WHERE SEARCH_INDEX('Note[a,b]', 'zebra') = false")).doesNotContain("INDEXED FUNCTION");
    }

    @Test
    void theIndexOverTheFewestPropertiesIsSearched() {
      createCorpus("BM25_1");
      database.command("sql", "CREATE INDEX ON Note (a) FULL_TEXT");

      // The single-property index answers the search without restriction, and its own statistics give the scores
      assertSameScores("SEARCH_FIELDS(['a'], 'fever')", "SEARCH_INDEX('Note[a]', 'fever')");
      assertThat(count("SEARCH_FIELDS(['a'], 'zebra') = true")).isZero();
      // b is only in the two-property index
      assertThat(count("SEARCH_FIELDS(['b'], 'zebra') = true")).isEqualTo(1);
    }

    @Test
    void fieldNamesAreASetAndAByItemPropertyIsNamedByItsBaseName() {
      createCorpus("BM25_1");
      // Order and repetitions change nothing
      assertSameScores("SEARCH_FIELDS(['b', 'a', 'a'], 'fever')", "SEARCH_INDEX('Note[a,b]', 'fever')");
      assertSameScores("SEARCH_FIELDS(['a', 'a'], 'fever')", "SEARCH_INDEX('Note[a,b]', 'a:fever')");
      assertThat(catchThrowable(() -> count("SEARCH_FIELDS([null], 'fever') = true"))).hasMessageContaining("cannot be null");

      database.transaction(() -> {
        database.command("sql", "CREATE PROPERTY Note.tags LIST OF STRING");
        database.command("sql", "CREATE INDEX ON Note (a, tags BY ITEM) FULL_TEXT");
        database.newVertex("Note").set("n", NOTES).set("a", "nothing").set("b", "nothing").set("tags", List.of("okapi", "fever"))
            .save();
      });
      assertThat(count("SEARCH_FIELDS(['tags'], 'okapi') = true")).isEqualTo(1);
      assertThat(count("SEARCH_FIELDS(['tags'], 'patient') = true")).isZero();
      assertThat(count("SEARCH_FIELDS(['a'], 'okapi') = true")).isZero();
    }

    @Test
    void amongCoveringIndexesOfTheSameSizeTheFirstByNameIsSearched() {
      createCorpus("BM25_1");
      database.transaction(() -> {
        database.command("sql", "CREATE PROPERTY Note.c STRING");
        database.command("sql", "UPDATE Note SET c = 'filler words for a longer document' WHERE n % 2 = 0");
        database.command("sql", "CREATE INDEX ON Note (a, c) FULL_TEXT");
      });
      // Note[a,b] and Note[a,c] both cover a with two properties: Note[a,b] comes first. Their corpus statistics differ (c
      // makes documents longer), so the scores tell which one was searched
      assertThat(scores("SEARCH_INDEX('Note[a,b]', 'a:fever')")).isNotEqualTo(scores("SEARCH_INDEX('Note[a,c]', 'a:fever')"));
      assertSameScores("SEARCH_FIELDS(['a'], 'fever')", "SEARCH_INDEX('Note[a,b]', 'a:fever')");
      assertSameScores("SEARCH_FIELDS(['c'], 'filler')", "SEARCH_INDEX('Note[a,c]', 'c:filler')");
    }

    @Test
    void aSubtypeIsSearchedThroughTheIndexOfItsSupertype() {
      createCorpus("BM25_1");
      database.transaction(() -> {
        database.command("sql", "CREATE VERTEX TYPE UrgentNote EXTENDS Note");
        database.newVertex("UrgentNote").set("n", NOTES).set("a", "zebra crossing").set("b", "nothing").save();
      });

      assertThat(count("SEARCH_FIELDS(['a'], 'zebra') = true")).isEqualTo(1);
      assertThat(rids("SELECT FROM UrgentNote WHERE SEARCH_FIELDS(['b'], 'zebra') = true")).isEmpty();

      // The subtype defines its own index over a only. A search FROM the supertype is fetched through the supertype's index, and
      // the subtype's rows are evaluated again on that same index, with its scores, not on the subtype's own
      database.command("sql", "CREATE INDEX ON UrgentNote (a) FULL_TEXT");
      assertThat(rids("SELECT FROM UrgentNote WHERE SEARCH_FIELDS(['a'], 'zebra') = true")).hasSize(1);
      final Map<Object, Float> viaSupertype = new LinkedHashMap<>();
      try (final ResultSet rs = database.query("sql",
          "SELECT @rid AS rid, $score AS score FROM Note WHERE SEARCH_FIELDS(['a'], 'zebra crossing') = true")) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          viaSupertype.put(row.getProperty("rid"), row.<Float>getProperty("score"));
        }
      }
      assertThat(viaSupertype).isEqualTo(scores("SEARCH_INDEX('Note[a,b]', 'a:zebra a:crossing')"));
    }

    private void assertSameScores(final String searchFields, final String searchIndex) {
      final Map<Object, Float> expected = scores(searchIndex);
      assertThat(expected).as(searchIndex).isNotEmpty();
      assertThat(scores(searchFields)).as(searchFields).isEqualTo(expected);
    }

    private Map<Object, Float> scores(final String condition) {
      final Map<Object, Float> scores = new LinkedHashMap<>();
      try (final ResultSet rs = database.query("sql",
          "SELECT @rid AS rid, $score AS score FROM Note WHERE " + condition + " = true ORDER BY $score DESC")) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          scores.put(row.getProperty("rid"), row.<Float>getProperty("score"));
        }
      }
      return scores;
    }

    private void createCorpus(final String variant) {
      final String similarity = variant.substring(0, variant.indexOf('_'));
      final int buckets = Integer.parseInt(variant.substring(variant.indexOf('_') + 1));
      database.transaction(() -> {
        database.command("sql", "CREATE VERTEX TYPE Note BUCKETS " + buckets);
        database.command("sql", "CREATE PROPERTY Note.a STRING");
        database.command("sql", "CREATE PROPERTY Note.b STRING");
        database.command("sql", "CREATE PROPERTY Note.n INTEGER");
        database.command("sql", "CREATE INDEX ON Note (a, b) FULL_TEXT METADATA {\"similarity\": \"" + similarity + "\"}");
        for (int i = 0; i < NOTES; i++) {
          final String a = (i % 2 == 0 ? "fever " : "") + (i % 4 == 0 ? "cough " : "") + "patient ".repeat(1 + i % 3);
          final String b = (i % 3 == 0 ? "fever " : "") + (i == 7 ? "zebra stripes " : "") + "notes ".repeat(1 + i % 2);
          database.newVertex("Note").set("n", i).set("a", a).set("b", b).save();
        }
      });
    }

    private long count(final String condition) {
      try (final ResultSet rs = database.query("sql", "SELECT count(@rid) AS c FROM Note WHERE " + condition)) {
        return rs.next().<Long>getProperty("c");
      }
    }

    private long readRecords() {
      return ((Number) ((DatabaseInternal) database).getStats().get("readRecord")).longValue();
    }

    private String plan(final String query) {
      try (final ResultSet rs = database.query("sql", query)) {
        return rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
      }
    }

    private List<RID> rids(final String query) {
      final List<RID> rids = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", query)) {
        while (rs.hasNext())
          rids.add(rs.next().getIdentity().orElseThrow());
      }
      return rids;
    }
  }
}
