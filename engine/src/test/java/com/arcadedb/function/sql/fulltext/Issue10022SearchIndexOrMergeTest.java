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
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.InternalExecutionPlan;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.parser.SelectStatement;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10022: {@code WHERE (SEARCH_INDEX('T[a]', q) = true OR SEARCH_INDEX('T[b]', q) = true) ... ORDER BY $score DESC
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
class Issue10022SearchIndexOrMergeTest extends TestHelper {
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
