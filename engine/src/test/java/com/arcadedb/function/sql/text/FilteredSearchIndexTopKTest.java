/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.function.sql.text;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.fulltext.FullTextPostingsReuse;
import com.arcadedb.index.fulltext.FullTextSearch;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.IndexedFunctionPushdown;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.executor.WorkGuard;
import com.arcadedb.query.sql.method.AbstractSQLMethod;
import com.arcadedb.query.sql.method.DefaultSQLMethodFactory;
import com.arcadedb.query.sql.parser.FromClause;
import com.arcadedb.query.sql.parser.SelectStatement;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.IntPredicate;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code SEARCH_INDEX(...) = true AND <filter> [ORDER BY $score DESC] [SKIP s] LIMIT k}: the search starts from the limit and goes
 * on past it in order, so the answer must be exactly the complete ranking filtered and cut, for any share of rows the filter
 * rejects, including a filter that rejects the best matches, after deletes and updates, with ties, in one bucket or many.
 */
class FilteredSearchIndexTopKTest extends TestHelper {
  private static final String SCORED = "SELECT @rid AS rid, $score AS score, n FROM Doc WHERE SEARCH_INDEX('Doc[body]', :q) = true";

  private record Row(RID rid, float score, int n) {
  }

  /** Filters by the share of rows they reject, and one that rejects the shortest documents, which BM25 ranks first. */
  private static final Map<String, IntPredicate> FILTERS = Map.of( //
      "n % 100 <> 0", n -> n % 100 != 0, //
      "n % 2 = 0", n -> n % 2 == 0, //
      "n % 100 = 0", n -> n % 100 == 0, //
      "n >= 1500", n -> n >= 1500);

  @ParameterizedTest
  @CsvSource({ "1,16,2000,mixed", "2,1,1500,mixed", "3,8,2000,ties", "4,4,1200,mixed" })
  void filteredLimitedAnswerIsTheCompleteRankingFilteredAndCut(final long seed, final int buckets, final int docs,
      final String shape) {
    final Random rnd = new Random(seed);
    createCorpus(rnd, buckets, docs, shape);
    int checks = check(rnd, "after the load");

    database.transaction(() -> {
      for (int i = 0; i < docs / 10; i++)
        database.command("sql", "DELETE FROM Doc WHERE n = ?", rnd.nextInt(docs));
      for (int i = 0; i < docs / 10; i++)
        database.command("sql", "UPDATE Doc SET body = ? WHERE n = ?", body(rnd, docs, rnd.nextInt(docs), shape), rnd.nextInt(docs));
    });
    checks += check(rnd, "after deletes and updates");
    assertThat(checks).isGreaterThan(500);
  }

  @Test
  void aFilterAfterTheSearchKeepsTheLimitAndNeedsNoSort() {
    createCorpus(new Random(7), 8, 3000, "mixed");
    final String sql = SCORED + " AND n % 100 <> 0 ORDER BY $score DESC LIMIT 50";
    final String plan = database.query("sql", "EXPLAIN " + sql, Map.of("q", "w1 w3 w7 w20")).getExecutionPlan().orElseThrow()
        .prettyPrint(0, 2);
    assertThat(plan).contains("(best 50 needed)").doesNotContain("ORDER BY");

    // rows the filter rejects are replaced from the complete search, at most once per statement
    final long before = FullTextFunctionSearch.getCompleteSearchesForTesting();
    final List<Row> rows = rows(sql, "w1 w3 w7 w20");
    assertThat(FullTextFunctionSearch.getCompleteSearchesForTesting() - before).isLessThanOrEqualTo(1L);
    assertThat(rows).isEqualTo(expected("w1 w3 w7 w20", FILTERS.get("n % 100 <> 0"), 0, 50));
  }

  @Test
  void theCompleteSearchAfterTheLimitScoresThePostingsAlreadyRead() {
    createCorpus(new Random(11), 8, 3000, "mixed");
    // 99 % of the rows rejected: the 50 best are used up, and the complete search continues from the postings already read
    final String sql = SCORED + " AND n % 100 = 0 ORDER BY $score DESC LIMIT 50";
    final List<Row> expected = expected("w1 w2 w4", FILTERS.get("n % 100 = 0"), 0, 50);
    final long searches = FullTextFunctionSearch.getCompleteSearchesForTesting();
    final long reused = FullTextPostingsReuse.getReusedReadsForTesting();
    assertThat(rows(sql, "w1 w2 w4")).isEqualTo(expected);
    assertThat(FullTextFunctionSearch.getCompleteSearchesForTesting() - searches).isEqualTo(1L);
    assertThat(FullTextPostingsReuse.getReusedReadsForTesting() - reused).isEqualTo(1L);
  }

  @Test
  void aSecondSearchInTheFilterKeepsTheSortItsScoreNeeds() {
    createCorpus(new Random(9), 4, 1500, "mixed");
    // the second search sets $score on every row it keeps, so its own ranking, not the fetch's, decides the order
    final String both = "SELECT @rid AS rid, $score AS score, n FROM Doc WHERE SEARCH_INDEX('Doc[body]', :q) = true"
        + " AND SEARCH_INDEX('Doc[body]', 'w2 w5') = true ORDER BY $score DESC";
    for (final String q : new String[] { "w0 w1", "w3", "w1 w9 w14" }) {
      final List<Row> all = rows(both, q);
      for (final int limit : new int[] { 1, 10, 50 })
        assertThat(rows(both + " LIMIT " + limit, q)).as("q='%s' LIMIT %d", q, limit)
            .isEqualTo(all.subList(0, Math.min(limit, all.size())));
    }
  }

  @Test
  void aScoringCallNestedInTheFilterKeepsTheSortItsScoreNeeds() {
    createCorpus(new Random(13), 4, 1500, "mixed");
    // a search nested in another call runs on every row the filter evaluates and sets $score as a bare one does
    for (final String filter : new String[] { "ifnull(SEARCH_INDEX('Doc[body]', 'w2 w5'), false) = true",
        "(SEARCH_INDEX('Doc[body]', 'w2 w5') = true) = true", "SEARCH_FIELDS(['body'], 'w2 w5') = true",
        "ifnull(SEARCH_FIELDS(['body'], 'w2 w5'), false) = true",
        // eval() runs a condition the planner cannot see, and a search in it sets $score too
        "eval(\"SEARCH_INDEX('Doc[body]', 'w2 w5') = true\") = true" }) {
      final String sql = SCORED + " AND " + filter + " ORDER BY $score DESC";
      final List<Row> all = rows(sql, "w0 w1");
      assertThat(all).as(filter).isNotEmpty();
      for (final int limit : new int[] { 1, 3, 10, 50 })
        assertThat(rows(sql + " LIMIT " + limit, "w0 w1")).as("%s LIMIT %d", filter, limit)
            .isEqualTo(all.subList(0, Math.min(limit, all.size())));
    }
    // a call that sets no score, at any depth, keeps the limit
    final String plain = SCORED + " AND ifnull(n, 0) % 100 <> 0 ORDER BY $score DESC LIMIT 50";
    assertThat(database.query("sql", "EXPLAIN " + plain, Map.of("q", "w0 w1")).getExecutionPlan().orElseThrow().prettyPrint(0, 2))
        .contains("(best 50 needed)").doesNotContain("ORDER BY");
  }

  /** Sets {@code $score} from the value it is applied to, as a method an application registers may. */
  private static final class RankByNumber extends AbstractSQLMethod {
    static final String NAME = "rankbynumber";

    RankByNumber() {
      super(NAME, 0, 0);
    }

    @Override
    public Object execute(final Object value, final Identifiable currentRecord, final CommandContext context, final Object[] params) {
      context.setVariable("$score", 1f / (1f + ((Number) value).floatValue()));
      return value;
    }
  }

  @Test
  void aMethodTransformDispatchesByNameKeepsTheSortItsScoreNeeds() {
    createCorpus(new Random(19), 4, 1500, "mixed");
    // transform() resolves the method it applies from its argument at run time: the planner cannot see that this one sets $score
    final DefaultSQLMethodFactory methods = DefaultSQLMethodFactory.getInstance();
    methods.register(RankByNumber.NAME, new RankByNumber());
    try {
      final String sql = SCORED + " AND [n].transform('" + RankByNumber.NAME + "').size() = 1 ORDER BY $score DESC";
      final List<Row> all = rows(sql, "w0 w1");
      assertThat(all).hasSizeGreaterThan(50);
      // the method's score ranks the rows, so the best match of the search is no longer the first row
      for (int i = 1; i < all.size(); i++)
        assertThat(all.get(i).n).isGreaterThan(all.get(i - 1).n);
      for (final int limit : new int[] { 1, 3, 10, 50 })
        assertThat(rows(sql + " LIMIT " + limit, "w0 w1")).as("LIMIT %d", limit).isEqualTo(all.subList(0, limit));
      assertThat(database.query("sql", "EXPLAIN " + sql + " LIMIT 10", Map.of("q", "w0 w1")).getExecutionPlan().orElseThrow()
          .prettyPrint(0, 2)).contains("ORDER BY").doesNotContain(" needed)");
    } finally {
      methods.getMethods().remove(RankByNumber.NAME);
    }
  }

  /**
   * What runs on every row after the fetch and leaves {@code $score} at a value of its own: the order the fetch answers in is not the
   * order of the {@code $score} the statement reads, so the sort must stay.
   */
  private void assertLimitKeepsTheOrderOf(final String projection, final String let) {
    for (final String filter : new String[] { "", " AND n % 2 = 0", " AND n % 100 <> 0" }) {
      final String sql = "SELECT @rid AS rid, " + projection + "$score AS score, n FROM Doc " + let
          + " WHERE SEARCH_INDEX('Doc[body]', :q) = true" + filter + " ORDER BY $score DESC";
      final String what = projection + let + filter;
      final List<Row> all = rows(sql, "w0 w1");
      assertThat(all).as(what).isNotEmpty();
      for (final int limit : new int[] { 1, 3, 10, 50 })
        assertThat(rows(sql + " LIMIT " + limit, "w0 w1")).as("%s LIMIT %d", what, limit)
            .isEqualTo(all.subList(0, Math.min(limit, all.size())));
    }
  }

  @Test
  void aLetThatScoresKeepsTheSortItsScoreNeeds() {
    createCorpus(new Random(21), 4, 1500, "mixed");
    // a search nested in a call, evaluated per record, and the variable assigned outright whatever the expression
    assertLimitKeepsTheOrderOf("", "LET $other = ifnull(SEARCH_INDEX('Doc[body]', 'w2 w5'), n)");
    assertLimitKeepsTheOrderOf("", "LET $score = n");
    assertLimitKeepsTheOrderOf("", "LET score = n");
  }

  @Test
  void aProjectionThatScoresKeepsTheSortItsScoreNeeds() {
    createCorpus(new Random(22), 4, 1500, "mixed");
    // a projection item that runs a search sets $score before the item that reads it
    assertLimitKeepsTheOrderOf("ifnull(SEARCH_INDEX('Doc[body]', 'w2 w5'), false) AS hit, ", "");
    assertLimitKeepsTheOrderOf("SEARCH_FIELDS(['body'], 'w2 w5') AS hit, ", "");
  }

  @Test
  void whatSetsNoScoreAfterTheFetchKeepsTheLimit() {
    createCorpus(new Random(26), 4, 1500, "mixed");
    // a LET of another name, a call of no consequence, a subquery (it runs in a context of its own)
    for (final String[] shape : new String[][] { //
        { "", "LET $c = n % 7" }, //
        { "ifnull(n, 0) AS m, ", "" }, //
        { "(SELECT count(*) AS c FROM Doc WHERE n = 1) AS subquery, ", "" } }) {
      final String sql = "SELECT @rid AS rid, " + shape[0] + "$score AS score, n FROM Doc " + shape[1]
          + " WHERE SEARCH_INDEX('Doc[body]', :q) = true AND n % 100 <> 0 ORDER BY $score DESC";
      assertThat(database.query("sql", "EXPLAIN " + sql + " LIMIT 10", Map.of("q", "w0 w1")).getExecutionPlan().orElseThrow()
          .prettyPrint(0, 2)).as(shape[0] + shape[1]).contains("(best 10 needed)").doesNotContain("ORDER BY");
      assertThat(rows(sql + " LIMIT 10", "w0 w1")).as(shape[0] + shape[1])
          .isEqualTo(rows(sql, "w0 w1").subList(0, 10));
    }
  }

  @Test
  void theContinuationAnswersEachRowOnceWhateverTheQueryShapeAndTheCommitsBetween() {
    createCorpus(new Random(31), 4, 1500, "mixed");
    // a phrase is no disjunction of terms: the complete search that continues the limited one reads the postings and the corpus
    // statistics again, and a commit between the two can order the rest differently than the first rows were served
    final String phrase = "\"w0 w1\"";
    final int matches = rows(SCORED, phrase).size();
    assertThat(matches).isGreaterThan(40);
    final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName("Doc[body]");
    final FromClause target = ((SelectStatement) ((DatabaseInternal) database).getStatementCache().get("SELECT FROM Doc"))
        .getTarget();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);
    final Iterator<Record> answer = new FullTextFunctionSearch(index, phrase, null).search(target, context,
        new IndexedFunctionPushdown(10, null, IndexedFunctionPushdown.Order.FUNCTION, true)).iterator();
    final List<RID> pulled = new ArrayList<>();
    for (int i = 0; i < 10; i++)
      pulled.add(answer.next().getIdentity());

    database.transaction(() -> {
      for (int i = 0; i < 3000; i++)
        database.newDocument("Doc").set("n", 100000 + i).set("body", "filler ".repeat(120)).save();
    });
    while (answer.hasNext())
      pulled.add(answer.next().getIdentity());

    // every match once: none repeated and none dropped, whatever order the corpus the commit left ranks the rest in
    assertThat(pulled).doesNotHaveDuplicates().hasSize(matches);
  }

  @Test
  void theContinuationScoresWithTheStatisticsOfTheLimitedSearch() {
    createCorpus(new Random(23), 4, 1500, "mixed");
    final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName("Doc[body]");
    final WorkGuard guard = WorkGuard.forCommandDeadline(null);
    final String query = "w1 w3 w7";
    final Map<RID, Float> complete = FullTextSearch.search(index, query, -1, guard, null, null);

    final FullTextPostingsReuse reuse = new FullTextPostingsReuse();
    FullTextSearch.search(index, query, 50, guard, null, null, reuse);
    // a commit between the limited search and its continuation moves the corpus statistics (the document count, the average
    // length) the scores depend on: many long documents, none of them a match
    database.transaction(() -> {
      for (int i = 0; i < 400; i++)
        database.newDocument("Doc").set("n", 100000 + i).set("body", "filler ".repeat(120)).save();
    });
    final long reused = FullTextPostingsReuse.getReusedReadsForTesting();
    final Map<RID, Float> continued = FullTextSearch.search(index, query, -1, guard, null, null, reuse);
    assertThat(FullTextPostingsReuse.getReusedReadsForTesting() - reused).isEqualTo(1L);
    // the postings of the limited search, the statistics of the limited search: the order the continuation goes on in is the
    // one the first rows were served in
    assertThat(continued).isEqualTo(complete);
  }

  @Test
  void onlyAFilteredLimitedSearchKeepsItsPostingsForTheContinuation() {
    createCorpus(new Random(29), 4, 1500, "mixed");
    final List<RID> all = rows(SCORED + " ORDER BY $score DESC", "w1 w3").stream().map(Row::rid).toList();
    final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName("Doc[body]");
    final FromClause target = ((SelectStatement) ((DatabaseInternal) database).getStatementCache().get("SELECT FROM Doc"))
        .getTarget();

    for (final boolean filtered : new boolean[] { false, true }) {
      final BasicCommandContext context = new BasicCommandContext();
      context.setDatabase(database);
      final long searches = FullTextFunctionSearch.getCompleteSearchesForTesting();
      final long reused = FullTextPostingsReuse.getReusedReadsForTesting();
      final Iterator<Record> answer = new FullTextFunctionSearch(index, "w1 w3", null).search(target, context,
          new IndexedFunctionPushdown(5, null, IndexedFunctionPushdown.Order.FUNCTION, filtered)).iterator();
      // the plan pulls past the limit when a filter rejected rows: the answer goes on, in the same order, from the complete search
      final List<RID> pulled = new ArrayList<>();
      while (pulled.size() < 8 && answer.hasNext())
        pulled.add(answer.next().getIdentity());
      assertThat(pulled).as("filtered=%s", filtered).isEqualTo(all.subList(0, 8));
      assertThat(FullTextFunctionSearch.getCompleteSearchesForTesting() - searches).isEqualTo(1L);
      // the postings of the limited search are kept only when a filter follows the fetch, which is what asks for the continuation
      assertThat(FullTextPostingsReuse.getReusedReadsForTesting() - reused).as("filtered=%s", filtered)
          .isEqualTo(filtered ? 1L : 0L);
    }
  }

  @Test
  void aWriteBetweenTwoStatementsOfAScriptIsSeen() {
    createCorpus(new Random(17), 4, 1200, "mixed");
    // the limited search of the first statement reads the postings; the second statement must read the committed insert
    final String search = "SELECT n FROM Doc WHERE SEARCH_INDEX('Doc[body]', 'w1') = true";
    final long before = database.query("sql", "SELECT count(*) AS c FROM (" + search + ")").next().<Number>getProperty("c")
        .longValue();
    try (final ResultSet rs = database.command("sqlscript", search + " ORDER BY $score DESC LIMIT 1;"
        + "BEGIN; INSERT INTO Doc SET n = 100000, body = 'w1 w1'; COMMIT;" + "SELECT count(*) AS c FROM (" + search + ");")) {
      assertThat(rs.next().<Number>getProperty("c").longValue()).isEqualTo(before + 1);
    }
    try (final ResultSet rs = database.command("sqlscript", search + " ORDER BY $score DESC LIMIT 1;"
        + "BEGIN; INSERT INTO Doc SET n = 100001, body = 'w1 w1 w1'; COMMIT;" + search
        + " AND n >= 100000 ORDER BY $score DESC LIMIT 5;")) {
      final List<Integer> found = new ArrayList<>();
      while (rs.hasNext())
        found.add(rs.next().<Number>getProperty("n").intValue());
      assertThat(found).containsExactlyInAnyOrder(100000, 100001);
    }
  }

  private int check(final Random rnd, final String stage) {
    int checks = 0;
    for (int q = 0; q < 12; q++) {
      final StringBuilder query = new StringBuilder();
      final int terms = 1 + rnd.nextInt(5);
      for (int t = 0; t < terms; t++)
        query.append(t == 0 ? "" : " ").append("w").append(skewed(rnd));
      // the complete ranking, which no limit reaches: once per query, every combination below filters and cuts it in Java
      final List<Row> all = rows(SCORED + " ORDER BY $score DESC", query.toString());
      for (final Map.Entry<String, IntPredicate> filter : FILTERS.entrySet())
        for (final int limit : new int[] { 1, 10, 50, 333 })
          for (final int skip : new int[] { 0, 7 })
            for (final boolean ordered : new boolean[] { true, false }) {
              final String sql = SCORED + " AND " + filter.getKey() + (ordered ? " ORDER BY $score DESC" : "") + (skip > 0 ?
                  " SKIP " + skip :
                  "") + " LIMIT " + limit;
              assertThat(rows(sql, query.toString())).as("%s: %s with q='%s'", stage, sql, query)
                  .isEqualTo(filteredAndCut(all, filter.getValue(), skip, limit));
              ++checks;
            }
    }
    return checks;
  }

  /** The complete ranking of a query, filtered and cut in Java. */
  private List<Row> expected(final String query, final IntPredicate filter, final int skip, final int limit) {
    return filteredAndCut(rows(SCORED + " ORDER BY $score DESC", query), filter, skip, limit);
  }

  private static List<Row> filteredAndCut(final List<Row> all, final IntPredicate filter, final int skip, final int limit) {
    final List<Row> kept = new ArrayList<>();
    for (final Row r : all)
      if (filter.test(r.n))
        kept.add(r);
    return kept.subList(Math.min(skip, kept.size()), Math.min(skip + limit, kept.size()));
  }

  private List<Row> rows(final String sql, final String query) {
    final List<Row> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", sql, Map.of("q", query))) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        rows.add(new Row(r.getProperty("rid"), ((Number) r.getProperty("score")).floatValue(), r.<Number>getProperty("n").intValue()));
      }
    }
    return rows;
  }

  private void createCorpus(final Random rnd, final int buckets, final int docs, final String shape) {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc BUCKETS " + buckets);
      database.command("sql", "CREATE PROPERTY Doc.body STRING");
      database.command("sql", "CREATE PROPERTY Doc.n INTEGER");
      database.command("sql", "CREATE INDEX ON Doc (body) FULL_TEXT METADATA {\"similarity\": \"BM25\"}");
    });
    database.transaction(() -> {
      for (int n = 0; n < docs; n++)
        database.newDocument("Doc").set("n", n).set("body", body(rnd, docs, n, shape)).save();
    });
  }

  private final List<String> templates = new ArrayList<>();

  /** Short bodies for small n, so the best matches of a query are the ones {@code n >= 1500} rejects. */
  private String body(final Random rnd, final int docs, final int n, final String shape) {
    if (shape.equals("ties")) {
      if (templates.size() < 5 || rnd.nextInt(25) == 0)
        templates.add(body(rnd, docs, rnd.nextInt(docs), "mixed"));
      return templates.get(rnd.nextInt(templates.size()));
    }
    final int words = 2 + n * 40 / docs + rnd.nextInt(4);
    final StringBuilder sb = new StringBuilder();
    for (int w = 0; w < words; w++)
      sb.append(w == 0 ? "" : " ").append("w").append(skewed(rnd));
    return sb.toString();
  }

  private static int skewed(final Random rnd) {
    return (int) Math.floor(Math.pow(rnd.nextDouble(), 2.5) * 60);
  }
}
