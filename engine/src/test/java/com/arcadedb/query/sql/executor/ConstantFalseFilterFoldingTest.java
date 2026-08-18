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
package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.ArithmeticErrorException;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Pins the planner fold of a filter that is false for every row ({@code WHERE 1=0}) and of {@code LIMIT 0}: the
 * statement is knowable from its own text, so the plan must not carry a scan the filter would only drain. That is
 * the shape Spark and several BI tools send over the Postgres wire to discover a query's schema. Issue #6174.
 * <p>
 * The fold is asserted on the plan shape and on the {@code readRecord} statistic, not only on the rows: the current
 * behaviour already returns zero rows, it just reads the whole type to do it. {@code LIMIT 0} also carries a wrong
 * answer, the SQL twin of the Cypher one fixed in #5715 - the hardwired {@code count(*)} plan returns before the
 * {@code LIMIT} step is chained, so {@code SELECT count(*) FROM T LIMIT 0} used to return a row.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class ConstantFalseFilterFoldingTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.getSchema().createDocumentType("Character");

    database.transaction(() -> {
      database.newDocument("Character").set("name", "Arya").set("age", 11).save();
      database.newDocument("Character").set("name", "Jon").set("age", 24).save();
      database.newDocument("Character").set("name", "Tyrion").set("age", 39).save();
    });
  }

  @Test
  void sparkSchemaProbeDoesNotScanItsTarget() {
    final ResultSet rs = database.query("sql", "SELECT * FROM (SELECT name FROM Character) SPARK_GEN_SUBQ_0 WHERE 1=0");
    final ExecutionPlan plan = rs.getExecutionPlan().orElseThrow();

    assertThat(rs.hasNext()).isFalse();
    rs.close();

    assertNoScan(plan);
  }

  @Test
  void constantFalseFilterOnATypeDoesNotScanIt() {
    final ResultSet rs = database.query("sql", "SELECT name FROM Character WHERE 1=0");
    final ExecutionPlan plan = rs.getExecutionPlan().orElseThrow();

    assertThat(rs.hasNext()).isFalse();
    rs.close();

    assertNoScan(plan);
  }

  @Test
  void constantFalseFilterIsRecognizedThroughParenthesesAndAnd() {
    assertNoScan(planOf("SELECT FROM Character WHERE (1=0)"));
    assertNoScan(planOf("SELECT FROM Character WHERE 1=0 AND name = 'Arya'"));
    assertNoScan(planOf("SELECT FROM Character WHERE name = 'Arya' AND (2=3)"));
    assertNoScan(planOf("SELECT FROM Character WHERE 1=0 OR 'a'='b'"));
    assertNoScan(planOf("SELECT FROM Character WHERE (1=0 AND name = 'Arya') OR (name = 'Jon' AND 2 > 3)"));
    assertNoScan(planOf("SELECT FROM Character WHERE 1 = 1 + 1"));
  }

  @Test
  void aLiteralFalseFilterIsFoldedToo() {
    // the parser answers a bare boolean literal with the BooleanExpression.FALSE/TRUE sentinels rather than a
    // condition, so they carry their own verdict
    assertNoScan(planOf("SELECT FROM Character WHERE false"));
    assertNoScan(planOf("SELECT FROM Character WHERE true AND false"));

    assertScan(planOf("SELECT FROM Character WHERE true"));
    assertThat(names("SELECT FROM Character WHERE true")).containsExactlyInAnyOrder("Arya", "Jon", "Tyrion");
    assertThat(names("SELECT FROM Character WHERE false")).isEmpty();
  }

  @Test
  void aNegatedConstantFilterIsReadWithItsNegation() {
    // the hazard the NOT handling exists to avoid: reading "NOT (1=0)" as false would answer a filter that matches
    // every record with no rows at all
    assertScan(planOf("SELECT FROM Character WHERE NOT (1=0)"));
    assertThat(names("SELECT FROM Character WHERE NOT (1=0)")).containsExactlyInAnyOrder("Arya", "Jon", "Tyrion");

    assertScan(planOf("SELECT FROM Character WHERE NOT (name = 'Arya')"));
    assertThat(names("SELECT FROM Character WHERE NOT (name = 'Arya')")).containsExactlyInAnyOrder("Jon", "Tyrion");
  }

  @Test
  void aSatisfiableFilterIsStillPlannedAsAScan() {
    assertScan(planOf("SELECT FROM Character WHERE name = 'Arya'"));
    assertScan(planOf("SELECT FROM Character WHERE 1=1"));
    // one alternative of the OR is satisfiable, so the statement is not empty by construction
    assertScan(planOf("SELECT FROM Character WHERE 1=0 OR name = 'Jon'"));

    assertThat(names("SELECT FROM Character WHERE 1=0 OR name = 'Jon'")).containsExactly("Jon");
    assertThat(names("SELECT FROM Character WHERE 1=0 AND name = 'Jon'")).isEmpty();
    assertThat(names("SELECT FROM Character WHERE (1=0 AND name = 'Arya') OR (name = 'Jon' AND 2 > 3)")).isEmpty();
  }

  @Test
  void aFilterResolvedByAParameterIsNeverFolded() {
    // the plan is cached and reused across bindings: a fold decided on this execution's parameters would
    // answer the next execution with the wrong rows
    assertScan(planOf("SELECT FROM Character WHERE ? = 0", 1));
    assertThat(names("SELECT FROM Character WHERE ? = 0", 1)).isEmpty();
    assertThat(names("SELECT FROM Character WHERE ? = 0", 0)).containsExactlyInAnyOrder("Arya", "Jon", "Tyrion");
  }

  @Test
  void aFilterCallingAFunctionIsNeverFolded() {
    // nothing on SQLFunction marks a function as pure, so a comparison involving one must not be evaluated
    // at plan time just to classify the statement
    assertScan(planOf("SELECT FROM Character WHERE uuid() = 'not-a-uuid'"));
    assertThat(names("SELECT FROM Character WHERE uuid() = 'not-a-uuid'")).isEmpty();
  }

  /**
   * Issue #6190: {@code SQLFunction#isDeterministic()} lets a call to a marked-pure built-in over literal arguments
   * fold exactly like a plain literal comparison would - the sibling of {@link #aFilterCallingAFunctionIsNeverFolded()}
   * for the one shape that is now safe to evaluate at plan time.
   */
  @Test
  void aFilterCallingADeterministicFunctionIsFolded() {
    assertNoScan(planOf("SELECT FROM Character WHERE abs(-1) = 999"));
    assertThat(names("SELECT FROM Character WHERE abs(-1) = 999")).isEmpty();

    assertScan(planOf("SELECT FROM Character WHERE abs(-1) = 1"));
    assertThat(names("SELECT FROM Character WHERE abs(-1) = 1")).containsExactlyInAnyOrder("Arya", "Jon", "Tyrion");
  }

  /**
   * The plan-caching consumer of the same marker: a statement whose only function call targets a deterministic
   * built-in over cacheable arguments is no longer excluded from the plan cache by {@code FunctionCall#isCacheable()}
   * returning {@code false} unconditionally.
   */
  @Test
  void aStatementWithADeterministicFunctionInTheFilterIsCacheable() {
    final String sql = "SELECT name FROM Character WHERE age > abs(-1)";
    final DatabaseInternal db = (DatabaseInternal) database;

    // the type creation in beginTest() invalidates the plan cache with a millisecond-resolution stamp the plan has
    // to beat: see the sibling guard below and RidInScanOptimizationTest for the same pattern
    final long setupMillis = System.currentTimeMillis();
    while (System.currentTimeMillis() == setupMillis)
      Thread.onSpinWait();

    assertThat(names(sql)).containsExactlyInAnyOrder("Arya", "Jon", "Tyrion");
    assertThat(db.getExecutionPlanCache().contains(sql)).as("a deterministic function call must not block plan caching")
        .isTrue();
  }

  /**
   * The mirror of the previous test: a non-deterministic call in the filter still blocks caching, exactly as before
   * this marker existed.
   */
  @Test
  void aStatementWithANonDeterministicFunctionInTheFilterIsNotCacheable() {
    final String sql = "SELECT name FROM Character WHERE age > 0 AND uuid() IS NOT NULL";
    final DatabaseInternal db = (DatabaseInternal) database;

    assertThat(names(sql)).containsExactlyInAnyOrder("Arya", "Jon", "Tyrion");
    assertThat(db.getExecutionPlanCache().contains(sql)).isFalse();
  }

  @Test
  void limitZeroDoesNotScanItsTarget() {
    final ResultSet rs = database.query("sql", "SELECT name FROM Character LIMIT 0");
    final ExecutionPlan plan = rs.getExecutionPlan().orElseThrow();

    assertThat(rs.hasNext()).isFalse();
    rs.close();

    assertNoScan(plan);
  }

  @Test
  void aNonZeroOrParameterizedLimitIsNeverFolded() {
    assertScan(planOf("SELECT FROM Character LIMIT 1"));
    assertScan(planOf("SELECT FROM Character LIMIT ?", 0));

    assertThat(names("SELECT FROM Character LIMIT ?", 0)).isEmpty();
    assertThat(names("SELECT FROM Character LIMIT ?", 2)).hasSize(2);

    // -1 is the sentinel LimitExecutionStep reads as "unlimited": the fold must not confuse it with zero
    assertScan(planOf("SELECT FROM Character LIMIT -1"));
    assertThat(names("SELECT FROM Character LIMIT -1")).containsExactlyInAnyOrder("Arya", "Jon", "Tyrion");
  }

  @Test
  void limitZeroIsFoldedWhateverTheFilterSays() {
    // a LIMIT 0 truncates the result to nothing no matter what the filter would have done, so the filter is not
    // evaluated at all. The one thing that can tell the difference is a predicate that raises: it no longer does.
    assertThatThrownBy(() -> names("SELECT FROM Character WHERE 1/0 = 1")).isInstanceOf(ArithmeticErrorException.class);

    assertNoScan(planOf("SELECT FROM Character WHERE 1/0 = 1 LIMIT 0"));
    assertThat(names("SELECT FROM Character WHERE 1/0 = 1 LIMIT 0")).isEmpty();
  }

  @Test
  void countStarWithAConstantFalseFilterStillReturnsZero() {
    final ResultSet rs = database.query("sql", "SELECT count(*) AS total FROM Character WHERE 1=0");

    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Number>getProperty("total").intValue()).isZero();
    assertThat(rs.hasNext()).isFalse();
    rs.close();
  }

  @Test
  void countStarWithAConstantFalseFilterAndGroupByReturnsNoRow() {
    final ResultSet rs = database.query("sql", "SELECT name, count(*) AS total FROM Character WHERE 1=0 GROUP BY name");

    assertThat(rs.hasNext()).isFalse();
    rs.close();
  }

  @Test
  void countStarWithLimitZeroReturnsNoRow() {
    final ResultSet rs = database.query("sql", "SELECT count(*) AS total FROM Character LIMIT 0");

    assertThat(rs.hasNext()).isFalse();
    rs.close();
  }

  @Test
  void maxOnAnIndexedPropertyWithASkipReturnsNoRow() {
    // same hazard as the count one below: MaxMinFromIndexStep also replaces the whole chain, and it is only reached
    // when the property carries a range index
    database.getSchema().getType("Character").createProperty("age", Type.INTEGER);
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "Character", "age");

    try (final ResultSet rs = database.query("sql", "SELECT max(age) AS oldest FROM Character SKIP 1")) {
      assertThat(rs.hasNext()).isFalse();
    }

    try (final ResultSet rs = database.query("sql", "SELECT max(age) AS oldest FROM Character")) {
      assertThat(rs.next().<Number>getProperty("oldest").intValue()).isEqualTo(39);
    }
  }

  @Test
  void countStarWithASkipReturnsNoRow() {
    // the hardwired count plan replaces the whole step chain, so it has to chain the statement's SKIP itself: it
    // used to hand back the very row it was told to skip
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS total FROM Character SKIP 1")) {
      assertThat(rs.hasNext()).isFalse();
    }

    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS total FROM Character SKIP 0 LIMIT 5")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Number>getProperty("total").intValue()).isEqualTo(3);
    }
  }

  @Test
  void anUnknownTargetIsStillReported() {
    // folding must not cost the statement its target validation
    assertThatThrownBy(() -> database.query("sql", "SELECT FROM ThisTypeDoesNotExist WHERE 1=0").close())
        .hasMessageContaining("ThisTypeDoesNotExist");
  }

  @Test
  void aConstantFalseFilterInsideTheSubqueryIsFoldedToo() {
    final ResultSet rs = database.query("sql", "SELECT * FROM (SELECT name FROM Character WHERE 1=0)");
    final ExecutionPlan plan = rs.getExecutionPlan().orElseThrow();

    assertThat(rs.hasNext()).isFalse();
    rs.close();

    assertNoScan(plan);
  }

  @Test
  void theFoldedPlanIsCacheableAndReusable() {
    final String sql = "SELECT name FROM Character WHERE 1=0";
    final DatabaseInternal db = (DatabaseInternal) database;

    // the type creation in beginTest() invalidates the plan cache with a millisecond-resolution stamp the plan has
    // to beat: see RidInScanOptimizationTest for the same guard
    final long setupMillis = System.currentTimeMillis();
    while (System.currentTimeMillis() == setupMillis)
      Thread.onSpinWait();

    assertThat(names(sql)).isEmpty();
    assertThat(db.getExecutionPlanCache().contains(sql)).as("the fold depends on the statement alone, so it is cacheable")
        .isTrue();

    // reuse: the cached plan is copied, so the folded source has to survive the copy
    assertThat(names(sql)).isEmpty();
    assertNoScan(planOf(sql));
  }

  @Test
  void aLetEvaluatedOncePerStatementStillRuns() {
    // a LET that needs no record is executed once, ahead of the fetch, and its side effects are not the fold's to
    // skip: only the per-record work disappears with the rows that were never going to come back
    database.transaction(() -> database.command("sql",
        "SELECT FROM Character LET $ghost = (INSERT INTO Character SET name = 'Ghost') WHERE 1=0").close());

    assertThat(names("SELECT FROM Character WHERE name = 'Ghost'")).containsExactly("Ghost");
  }

  @Test
  void aDeleteWithAConstantFalseFilterDoesNotScanItsTarget() {
    final ResultSet rs = database.command("sql", "DELETE FROM Character WHERE 1=0");
    final ExecutionPlan plan = rs.getExecutionPlan().orElseThrow();
    rs.close();

    assertNoScan(plan);
    assertThat(names("SELECT FROM Character")).hasSize(3);
  }

  @Test
  void aFoldedStatementReadsNoRecordAtAll() {
    // the plan shape says the scan is not there; this says nothing reads a record when the plan runs
    assertThat(recordsReadBy("SELECT * FROM (SELECT name FROM Character) SPARK_GEN_SUBQ_0 WHERE 1=0")).isZero();
    assertThat(recordsReadBy("SELECT name FROM Character WHERE 1=0")).isZero();
    assertThat(recordsReadBy("SELECT name FROM Character LIMIT 0")).isZero();

    assertThat(recordsReadBy("SELECT name FROM Character")).isEqualTo(3);
  }

  @Test
  void anUpdateWithAConstantFalseFilterDoesNotScanItsTarget() {
    final ResultSet rs = database.command("sql", "UPDATE Character SET name = 'changed' WHERE 1=0");
    final ExecutionPlan plan = rs.getExecutionPlan().orElseThrow();
    rs.close();

    assertNoScan(plan);
    assertThat(names("SELECT FROM Character")).containsExactlyInAnyOrder("Arya", "Jon", "Tyrion");
  }

  @Test
  void orderByAndProjectionsSurviveTheFold() {
    final ResultSet rs = database.query("sql",
        "SELECT name.toUpperCase() AS upper FROM Character WHERE 1=0 ORDER BY name DESC LIMIT 10");

    assertThat(rs.hasNext()).isFalse();
    rs.close();
  }

  @Test
  void anArithmeticParenthesisFoldsAtAnyNestingDepth() {
    // MathExpression.isLiteral()'s javadoc names an array literal and a CASE as the shapes whose operands sit
    // outside childExpressions and would be missed folds without their own override; an arithmetic parenthesis is
    // NOT one of them (issue #6186) - ParenthesisExpression's own isLiteral() override answers for it too, so
    // nesting one, or wrapping it in further arithmetic, still folds
    assertNoScan(planOf("SELECT FROM Character WHERE (1) = 0"));
    assertNoScan(planOf("SELECT FROM Character WHERE (1+1) = 0"));
    assertNoScan(planOf("SELECT FROM Character WHERE (1+1)*2 = 0"));
  }

  @Test
  void anArrayLiteralAccessAndACaseAreNotFoldedYet() {
    // the two shapes MathExpression.isLiteral()'s javadoc does name: pinned here so the comment cannot drift the
    // other way either. Both are SCAN WITH FILTER today, not EMPTY RESULT - unlike the arithmetic parenthesis above
    assertScan(planOf("SELECT FROM Character WHERE [1,2][0] = 0"));
    assertScan(planOf("SELECT FROM Character WHERE (CASE WHEN 1=1 THEN 1 ELSE 2 END) = 0"));
  }

  private ExecutionPlan planOf(final String sql, final Object... params) {
    try (final ResultSet rs = params.length == 0 ? database.query("sql", sql) : database.query("sql", sql, params)) {
      return rs.getExecutionPlan().orElseThrow();
    }
  }

  private List<String> names(final String sql, final Object... params) {
    final List<String> names = new ArrayList<>();
    try (final ResultSet rs = params.length == 0 ? database.query("sql", sql) : database.query("sql", sql, params)) {
      while (rs.hasNext())
        names.add(rs.next().getProperty("name"));
    }
    return names;
  }

  /** The records the query materialized: zero when the statement was answered from its own text. */
  private long recordsReadBy(final String query) {
    final long before = readRecords();
    try (final ResultSet rs = database.query("sql", query)) {
      while (rs.hasNext())
        rs.next().getPropertyNames();
    }
    return readRecords() - before;
  }

  private long readRecords() {
    return ((Number) database.getStats().get("readRecord")).longValue();
  }

  private static void assertNoScan(final ExecutionPlan plan) {
    assertThat(fetchSteps(plan))
        .as("an empty-by-construction statement must not plan a fetch step: %s", plan.prettyPrint(0, 2))
        .isEmpty();
  }

  private static void assertScan(final ExecutionPlan plan) {
    assertThat(fetchSteps(plan))
        .as("a statement that can return rows must still plan a fetch step: %s", plan.prettyPrint(0, 2))
        .isNotEmpty();
  }

  private static List<String> fetchSteps(final ExecutionPlan plan) {
    final List<String> found = new ArrayList<>();
    for (final ExecutionStep step : plan.getSteps())
      collectFetchSteps(step, found);
    return found;
  }

  private static void collectFetchSteps(final ExecutionStep step, final List<String> found) {
    if (step.getClass().getSimpleName().startsWith("FetchFrom") || step.getClass().getSimpleName().startsWith("CountFrom"))
      found.add(step.getClass().getSimpleName());

    if (step.getSubSteps() != null)
      for (final ExecutionStep sub : step.getSubSteps())
        collectFetchSteps(sub, found);

    if (step instanceof ExecutionStepInternal internal && internal.getSubExecutionPlans() != null)
      for (final ExecutionPlan sub : internal.getSubExecutionPlans())
        for (final ExecutionStep subStep : sub.getSteps())
          collectFetchSteps(subStep, found);
  }
}
