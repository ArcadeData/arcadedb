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
package com.arcadedb.index;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * #6640: {@code WHERE prop IN (v1, v2, ..., vN)} - the parenthesized literal-list form of IN, the most common
 * shape in hand-written SQL - never used the index. {@code InCondition.isIndexAware()} only recognised the
 * bracket-array form ({@code IN [v1,...]}) and the bound-parameter form ({@code IN ?}/{@code IN :name}); a
 * parenthesized literal list fell back to a full bucket scan that re-evaluated the whole N-item list, by
 * linear search, against every row. A 15,000-item list on an indexed property was reported at ~350 rows/sec
 * (a 36M-row production table) and reproduced here at essentially the same rate on 200,000 rows - proportional
 * to row count, not list size, which is what gives a full scan away.
 * <p>
 * Fixing that alone uncovered two further bugs the literal-list path had never been able to reach before:
 * {@link com.arcadedb.query.sql.executor.FetchFromIndexStep#init} expanding a multi-value key
 * ({@code cartesianProduct}) deep-copied the entire remaining key - including the N-item list itself - once
 * per expanded element (O(n^2)), and {@code SQLASTBuilder.visitInCondition} built the list by calling the
 * ANTLR-generated indexed accessor {@code ctx.expression(i)} in a loop, which rescans the whole parse-tree
 * child list on every call (also O(n^2)). All three combined made a 15,000-value literal {@code IN()} on an
 * indexed property ~1000x slower than the identical values passed as a bound parameter; fixed, the two forms
 * perform the same.
 * <p>
 * Making the parenthesized list index-aware also exposed a pre-existing correctness bug shared with the bracket
 * form: {@code isIndexAware()} never checked {@code not}, so {@code trackId NOT IN [2,5,9]} was already fetching
 * the LISTED rows through the index instead of excluding them (same class of bug as #6575 for the native Select
 * API - a negated leaf has no complement in {@code FetchFromIndexStep}). Fixed alongside this change by declining
 * to use the index whenever {@code not} is set, for every right-hand shape.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6640ParenthesizedInListIndexTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().createVertexType("Track");
      database.getSchema().getType("Track").createProperty("trackId", Type.LONG);
      database.getSchema().buildTypeIndex("Track", new String[] { "trackId" })
          .withType(Schema.INDEX_TYPE.LSM_TREE)
          .withUnique(true)
          .create();
    });
  }

  @Test
  void literalParenthesizedInListUsesTheIndex() {
    database.transaction(() -> {
      final ResultSet rs = database.query("sql", "explain select from Track where trackId in (1, 2, 3)");
      final String plan = rs.getExecutionPlan().get().prettyPrint(0, 2);
      assertThat(plan).contains("FETCH FROM INDEX");
    });
  }

  @Test
  void singleItemParenthesizedInListUsesTheIndex() {
    database.transaction(() -> {
      final ResultSet rs = database.query("sql", "explain select from Track where trackId in (1)");
      final String plan = rs.getExecutionPlan().get().prettyPrint(0, 2);
      assertThat(plan).contains("FETCH FROM INDEX");
    });
  }

  @Test
  void literalParenthesizedInListReturnsExactRowsWithDuplicatesAndMisses() {
    database.transaction(() -> {
      for (int i = 0; i < 10; i++)
        database.newVertex("Track").set("trackId", (long) i).save();
    });

    database.transaction(() -> {
      // 9 is repeated in the list and 100 does not exist: the index-based fetch must neither duplicate
      // nor invent rows, exactly like the pre-fix full-scan evaluator.
      final ResultSet rs = database.query("sql", "select trackId from Track where trackId in (2, 5, 9, 9, 100) order by trackId");
      final List<Long> ids = rs.stream().map(r -> r.<Long>getProperty("trackId")).toList();
      assertThat(ids).containsExactly(2L, 5L, 9L);
    });
  }

  @Test
  void negatedParenthesizedInListDoesNotUseTheIndexAndExcludesTheListedRows() {
    database.transaction(() -> {
      for (int i = 0; i < 10; i++)
        database.newVertex("Track").set("trackId", (long) i).save();
    });

    // A negated IN has no complement in FetchFromIndexStep: every code path there builds a cursor over the
    // values that DO match, not their complement. isIndexAware() must decline whenever `not` is set, for
    // every right-hand shape - parenthesized list, bracket array, bound parameter - not just the one this
    // PR touches, otherwise the query silently returns the listed rows instead of excluding them. The
    // bracket-array and bound-parameter forms already reached the index before this PR, so this locks in
    // the guard fixing a pre-existing bug for them too, not just guarding against the new parenthesized path.
    database.transaction(() -> {
      assertNotInDeclinesTheIndexAndExcludesTheListedRows("select from Track where trackId not in (2, 5, 9)",
          "select trackId from Track where trackId not in (2, 5, 9) order by trackId");
      assertNotInDeclinesTheIndexAndExcludesTheListedRows("select from Track where trackId not in [2, 5, 9]",
          "select trackId from Track where trackId not in [2, 5, 9] order by trackId");
    });
    database.transaction(() -> {
      final ResultSet explain = database.query("sql", "explain select from Track where trackId not in :ids", Map.of("ids", List.of(2L, 5L, 9L)));
      assertThat(explain.getExecutionPlan().get().prettyPrint(0, 2)).doesNotContain("FETCH FROM INDEX");

      final List<Long> ids = database.query("sql", "select trackId from Track where trackId not in :ids order by trackId", Map.of("ids", List.of(2L, 5L, 9L)))
          .stream().map(r -> r.<Long>getProperty("trackId")).toList();
      assertThat(ids).containsExactly(0L, 1L, 3L, 4L, 6L, 7L, 8L);
    });
  }

  private void assertNotInDeclinesTheIndexAndExcludesTheListedRows(final String explainQuery, final String selectQuery) {
    final ResultSet explain = database.query("sql", "explain " + explainQuery);
    assertThat(explain.getExecutionPlan().get().prettyPrint(0, 2)).doesNotContain("FETCH FROM INDEX");

    final List<Long> ids = database.query("sql", selectQuery).stream().map(r -> r.<Long>getProperty("trackId")).toList();
    assertThat(ids).containsExactly(0L, 1L, 3L, 4L, 6L, 7L, 8L);
  }

  @Test
  @Tag("slow")
  void literalAndBoundParameterFormsAgreeOnLargeLists() {
    final int ROWS = 20_000;
    database.transaction(() -> {
      for (int i = 0; i < ROWS; i++)
        database.newVertex("Track").set("trackId", (long) i).save();
    });

    final List<Long> chosen = new Random(42).longs(0, ROWS).distinct().limit(6_000).boxed().collect(Collectors.toList());
    final String literalInList = chosen.stream().map(String::valueOf).collect(Collectors.joining(","));

    database.transaction(() -> {
      final List<Long> viaLiteral = database.query("sql", "select from Track where trackId in (" + literalInList + ")")
          .stream().map(r -> r.<Long>getProperty("trackId")).toList();
      final List<Long> viaBoundParam = database.query("sql", "select from Track where trackId in :ids", Map.of("ids", chosen))
          .stream().map(r -> r.<Long>getProperty("trackId")).toList();

      assertThat(viaLiteral).containsExactlyInAnyOrderElementsOf(chosen);
      assertThat(viaLiteral).containsExactlyInAnyOrderElementsOf(viaBoundParam);
    });
  }

  @Test
  @Tag("slow")
  void largeLiteralInListStaysNearLinearNotQuadratic() {
    final int ROWS = 30_000;
    database.transaction(() -> {
      for (int i = 0; i < ROWS; i++)
        database.newVertex("Track").set("trackId", (long) i).save();
    });

    // A 10x larger literal IN-list must not take anywhere near 100x longer. Before the fix, three compounding
    // O(n^2) costs - a full bucket scan, FetchFromIndexStep.cartesianProduct() copying the whole remaining key
    // per expanded element, and the ANTLR AST builder rescanning the parse tree per list item - made a
    // 15,000-item literal IN() take ~40s where the equivalent bound parameter took ~50ms (#6640).
    database.transaction(() -> database.query("sql", buildInQuery(2_000)).close());

    // assertStayedUnder, not assertGaveUpWithin: the bound IS the assertion here, standing in for the
    // near-linear complexity claim - there is no other practical way to express "not quadratic". Widening
    // this bound would not be a safe loosening, it would quietly delete the regression coverage.
    final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
    database.transaction(() -> database.query("sql", buildInQuery(20_000)).close());
    stopwatch.assertStayedUnder(5_000,
        "a near-linear 20,000-item literal IN() against an indexed property, not the pre-fix quadratic full-scan/copy/parse cost");
  }

  private String buildInQuery(final int n) {
    final StringBuilder sql = new StringBuilder("select from Track where trackId in (");
    for (int i = 0; i < n; i++) {
      if (i > 0)
        sql.append(',');
      sql.append(i);
    }
    sql.append(')');
    return sql.toString();
  }
}
