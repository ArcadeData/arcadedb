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
import com.arcadedb.database.Record;
import com.arcadedb.function.sql.text.SQLFunctionSearchIndex;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.parser.BaseExpression;
import com.arcadedb.query.sql.parser.Expression;
import com.arcadedb.query.sql.parser.FromClause;
import com.arcadedb.query.sql.parser.SelectStatement;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9895: {@code SEARCH_INDEX(...) ... LIMIT n} returned n rows but first matched, sorted and LOADED every match. The
 * records are now loaded lazily, best first, and when the statement provably consumes only the best n rows the search keeps
 * only those.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9895SearchIndexLimitTest extends TestHelper {
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

    assertThat(plan("SELECT FROM Doc WHERE " + SEARCH + " LIMIT 10")).contains("(best 10 first)");
    assertThat(plan("SELECT FROM Doc WHERE " + SEARCH + " ORDER BY $score DESC LIMIT 10")).contains("(best 10 first)");
    assertThat(plan("SELECT title, $score FROM Doc WHERE " + SEARCH + " ORDER BY $score DESC LIMIT 10")).contains("(best 10 first)");
    assertThat(plan("SELECT FROM Doc WHERE " + SEARCH + " SKIP 5 LIMIT 10")).contains("(best 15 first)");

    for (final String query : new String[] { "SELECT FROM Doc WHERE " + SEARCH,
        "SELECT FROM Doc WHERE " + SEARCH + " AND n > 5 LIMIT 10",
        "SELECT FROM Doc WHERE " + SEARCH + " OR SEARCH_INDEX('Doc[content]', 'rare') = true LIMIT 10",
        "SELECT FROM Doc WHERE " + SEARCH + " ORDER BY n LIMIT 10",
        "SELECT FROM Doc WHERE " + SEARCH + " ORDER BY $score ASC LIMIT 10",
        "SELECT FROM Doc WHERE " + SEARCH + " ORDER BY $score DESC, n LIMIT 10",
        "SELECT n AS $score FROM Doc WHERE " + SEARCH + " ORDER BY $score DESC LIMIT 10",
        "SELECT count(*) FROM Doc WHERE " + SEARCH + " LIMIT 10",
        "SELECT n, count(*) FROM Doc WHERE " + SEARCH + " GROUP BY n LIMIT 10",
        "SELECT DISTINCT n FROM Doc WHERE " + SEARCH + " LIMIT 10",
        "SELECT expand(tags) FROM Doc WHERE " + SEARCH + " LIMIT 10",
        "SELECT FROM Doc WHERE " + SEARCH + " UNWIND tags LIMIT 10" })
      assertThat(plan(query)).as(query).doesNotContain(" first)");
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
