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
import com.arcadedb.query.sql.parser.AndBlock;
import com.arcadedb.query.sql.parser.OrBlock;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The mirror of {@link ConstantFalseFilterFoldingTest}: a filter that is true for every record ({@code WHERE 1=1},
 * {@code WHERE true}) says nothing the statement did not already say, so the planner must drop it and build the plan
 * the same statement without a WHERE would have got. Before issue #6184 no leaf could answer {@code isAlwaysTrue()} -
 * the method took no context, so a comparison had no way to fold itself - and the recursion always bottomed out at
 * false, leaving a per-record predicate evaluation and a fetch-with-filter step in place of a plain bucket fetch.
 * <p>
 * Dropping a filter is a wider step than folding one to an empty result, because it changes which fetch step is
 * chosen. So the assertions here are not only "no filter step": the folded plan is compared step by step, and row by
 * row, against the plan of the very same statement written without its WHERE.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class ConstantTrueFilterFoldingTest extends TestHelper {

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
  void anAlwaysTrueFilterIsNotEvaluatedPerRecord() {
    assertNoFilter(planOf("SELECT FROM Character WHERE 1=1"));
    assertNoFilter(planOf("SELECT FROM Character WHERE true"));

    // the point of the fold: the plain fetch is back, instead of the fetch-with-filter the WHERE used to force
    assertThat(stepNames(planOf("SELECT FROM Character WHERE 1=1"))).contains("FetchFromTypeExecutionStep");
  }

  @Test
  void theFoldedPlanIsTheOneTheStatementWithoutAWhereWouldHaveGot() {
    // step for step, which pins the fetch step chosen AND the bucket steps under it
    assertThat(stepNames(planOf("SELECT FROM Character WHERE 1=1")))
        .isEqualTo(stepNames(planOf("SELECT FROM Character")));

    assertThat(stepNames(planOf("SELECT name FROM Character WHERE 1=1 ORDER BY name DESC")))
        .isEqualTo(stepNames(planOf("SELECT name FROM Character ORDER BY name DESC")));
  }

  @Test
  void theFoldedPlanReturnsTheSameRecordsInTheSameOrder() {
    assertThat(rids("SELECT FROM Character WHERE 1=1")).isEqualTo(rids("SELECT FROM Character"));
    assertThat(rids("SELECT FROM Character WHERE true")).isEqualTo(rids("SELECT FROM Character"));

    assertThat(names("SELECT FROM Character WHERE 1=1")).containsExactlyInAnyOrder("Arya", "Jon", "Tyrion");
  }

  @Test
  void theFoldedPlanReadsExactlyTheRecordsTheUnfilteredOneReads() {
    final long unfiltered = recordsReadBy("SELECT FROM Character");

    assertThat(unfiltered).isEqualTo(3);
    assertThat(recordsReadBy("SELECT FROM Character WHERE 1=1")).isEqualTo(unfiltered);
    assertThat(recordsReadBy("SELECT FROM Character WHERE true")).isEqualTo(unfiltered);
  }

  @Test
  void anAlwaysTrueFilterIsRecognizedThroughParenthesesAndBooleanBlocks() {
    assertNoFilter(planOf("SELECT FROM Character WHERE (1=1)"));
    assertNoFilter(planOf("SELECT FROM Character WHERE 1=1 AND 'a'='a'"));
    assertNoFilter(planOf("SELECT FROM Character WHERE 2 = 1 + 1"));
    assertNoFilter(planOf("SELECT FROM Character WHERE true AND 2 > 1"));
    // one alternative of the OR is enough to keep every record
    assertNoFilter(planOf("SELECT FROM Character WHERE 1=1 OR name = 'nobody'"));
    assertNoFilter(planOf("SELECT FROM Character WHERE (1=0 AND name = 'Arya') OR (2 > 1)"));

    assertThat(names("SELECT FROM Character WHERE 1=1 OR name = 'nobody'"))
        .containsExactlyInAnyOrder("Arya", "Jon", "Tyrion");
    assertThat(names("SELECT FROM Character WHERE (1=0 AND name = 'Arya') OR (2 > 1)"))
        .containsExactlyInAnyOrder("Arya", "Jon", "Tyrion");
  }

  @Test
  void aNegatedAlwaysFalseFilterFoldsToAlwaysTrue() {
    // NotBlock delegates its negated branch to isAlwaysFalse, so this comes for free once a leaf can answer
    assertNoFilter(planOf("SELECT FROM Character WHERE NOT (1=0)"));
    assertThat(names("SELECT FROM Character WHERE NOT (1=0)")).containsExactlyInAnyOrder("Arya", "Jon", "Tyrion");
  }

  @Test
  void aNegatedAlwaysTrueFilterFoldsToAnEmptyResult() {
    // the other half of the #6181 delegation, which could not fire while no leaf answered isAlwaysTrue: NOT (1=1)
    // is false for every record, so the statement is answered without a fetch at all
    assertThat(fetchSteps(planOf("SELECT FROM Character WHERE NOT (1=1)")))
        .as("NOT (1=1) is false for every record, so nothing is fetched")
        .isEmpty();

    assertThat(names("SELECT FROM Character WHERE NOT (1=1)")).isEmpty();
    assertThat(recordsReadBy("SELECT FROM Character WHERE NOT (1=1)")).isZero();
  }

  @Test
  void aFilterThatIsNotTrueForEveryRecordKeepsItsFilter() {
    // the fold is all-or-nothing: it drops a WHERE, it does not prune the satisfied terms out of one
    assertFilter(planOf("SELECT FROM Character WHERE name = 'Arya'"));
    assertFilter(planOf("SELECT FROM Character WHERE 1=1 AND name = 'Arya'"));
    assertFilter(planOf("SELECT FROM Character WHERE age > 20"));

    assertThat(names("SELECT FROM Character WHERE 1=1 AND name = 'Arya'")).containsExactly("Arya");
    assertThat(names("SELECT FROM Character WHERE age > 20")).containsExactlyInAnyOrder("Jon", "Tyrion");
  }

  @Test
  void anAlwaysTrueTermLeftBehindByAnIndexSearchCostsNoFilterStep() {
    // the whole-clause fold declines here - the AND is not true for every record - so the interaction to pin is what
    // the index search leaves behind: it takes `name = 'Arya'` as its key and hands the `1=1` back as a residual
    // condition, which used to be chained as a FILTER ITEMS WHERE step and evaluated once per index entry
    database.getSchema().getType("Character").createProperty("name", Type.STRING);
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Character", "name");

    assertThat(fetchSteps(planOf("SELECT FROM Character WHERE 1=1 AND name = 'Arya'")))
        .as("the satisfiable term still drives the index search")
        .anyMatch(name -> name.startsWith("FetchFromIndex"));

    assertThat(stepNames(planOf("SELECT FROM Character WHERE 1=1 AND name = 'Arya'")))
        .isEqualTo(stepNames(planOf("SELECT FROM Character WHERE name = 'Arya'")));

    assertThat(names("SELECT FROM Character WHERE 1=1 AND name = 'Arya'")).containsExactly("Arya");
    // ...and a residual that can still discard a record is of course kept
    assertFilter(planOf("SELECT FROM Character WHERE age > 20 AND name = 'Jon'"));
    assertThat(names("SELECT FROM Character WHERE age > 20 AND name = 'Jon'")).containsExactly("Jon");
  }

  @Test
  void anEmptyDisjunctionIsDeclinedByBothVerdicts() {
    // The parser does not produce an empty OrBlock, so this is pinned directly rather than by reachability. Both
    // verdicts decline it instead of reading it as the neutral element (which would be FALSE), and deliberately:
    // each answer is load-bearing - isAlwaysTrue drops the filter, isAlwaysFalse replaces the plan with an empty
    // source - so deducing either one for a block that only ever arrives by accident trades correctness for an
    // optimisation nobody can trigger.
    assertThat(new OrBlock().isAlwaysTrue(null)).isFalse();
    assertThat(new OrBlock().isAlwaysFalse(null)).isFalse();

    // an empty AND block, by contrast, is what a filter with no terms left actually looks like, and it keeps every
    // record: that verdict is reachable and is what lets an emptied residual condition drop its filter step
    assertThat(new AndBlock().isAlwaysTrue(null)).isTrue();
    assertThat(new AndBlock().isAlwaysFalse(null)).isFalse();
  }

  @Test
  void aFilterResolvedByAParameterIsNeverFolded() {
    // the plan is cached and reused across bindings: dropping the filter on this execution's parameters would
    // answer the next execution with every row
    assertFilter(planOf("SELECT FROM Character WHERE ? = 1", 1));

    assertThat(names("SELECT FROM Character WHERE ? = 1", 1)).containsExactlyInAnyOrder("Arya", "Jon", "Tyrion");
    assertThat(names("SELECT FROM Character WHERE ? = 1", 0)).isEmpty();
  }

  @Test
  void aFilterCallingAFunctionIsNeverFolded() {
    // nothing on SQLFunction marks a function as pure, so a comparison involving one must not be evaluated at plan
    // time just to classify the statement
    assertFilter(planOf("SELECT FROM Character WHERE uuid() IS NOT NULL"));
    assertThat(names("SELECT FROM Character WHERE uuid() IS NOT NULL")).containsExactlyInAnyOrder("Arya", "Jon", "Tyrion");
  }

  @Test
  void anAlwaysFalseFilterIsStillFoldedToAnEmptyResult() {
    // the two folds share the same literal restriction and must not shadow each other
    assertThat(fetchSteps(planOf("SELECT FROM Character WHERE 1=0"))).isEmpty();
    assertThat(names("SELECT FROM Character WHERE 1=0")).isEmpty();
    assertThat(names("SELECT FROM Character WHERE false")).isEmpty();
  }

  @Test
  void countStarWithAnAlwaysTrueFilterCountsEveryRecord() {
    // dropping the filter hands the statement to the hardwired count(*) plan, which must still answer 3
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS total FROM Character WHERE 1=1")) {
      assertThat(rs.next().<Number>getProperty("total").intValue()).isEqualTo(3);
    }

    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS total FROM Character WHERE 1=1 AND name = 'Arya'")) {
      assertThat(rs.next().<Number>getProperty("total").intValue()).isEqualTo(1);
    }
  }

  @Test
  void anAlwaysTrueFilterInsideASubqueryIsFoldedToo() {
    assertNoFilter(planOf("SELECT * FROM (SELECT name FROM Character WHERE 1=1)"));
    assertThat(names("SELECT * FROM (SELECT name FROM Character WHERE 1=1)"))
        .containsExactlyInAnyOrder("Arya", "Jon", "Tyrion");
  }

  @Test
  void theFoldedPlanIsCacheableAndReusable() {
    final String sql = "SELECT name FROM Character WHERE 1=1";
    final DatabaseInternal db = (DatabaseInternal) database;

    // the type creation in beginTest() invalidates the plan cache with a millisecond-resolution stamp the plan has
    // to beat: see ConstantFalseFilterFoldingTest for the same guard
    final long setupMillis = System.currentTimeMillis();
    while (System.currentTimeMillis() == setupMillis)
      Thread.onSpinWait();

    assertThat(names(sql)).hasSize(3);
    assertThat(db.getExecutionPlanCache().contains(sql)).as("the fold depends on the statement alone, so it is cacheable")
        .isTrue();

    // reuse: the cached plan is copied, so the folded source has to survive the copy
    assertThat(names(sql)).hasSize(3);
    assertNoFilter(planOf(sql));
  }

  @Test
  void anUpdateWithAnAlwaysTrueFilterStillTouchesEveryRecord() {
    // the fold must not cost a write statement its target: an always-true WHERE selects everything, exactly as no
    // WHERE at all does
    database.transaction(() -> database.command("sql", "UPDATE Character SET house = 'Stark' WHERE 1=1").close());

    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS total FROM Character WHERE house = 'Stark'")) {
      assertThat(rs.next().<Number>getProperty("total").intValue()).isEqualTo(3);
    }
  }

  @Test
  void aDeleteWithAnAlwaysTrueFilterStillRemovesEveryRecord() {
    database.transaction(() -> database.command("sql", "DELETE FROM Character WHERE 1=1").close());

    assertThat(names("SELECT FROM Character")).isEmpty();
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

  private List<String> rids(final String sql) {
    final List<String> rids = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", sql)) {
      while (rs.hasNext())
        rids.add(rs.next().getIdentity().orElseThrow().toString());
    }
    return rids;
  }

  /** The records the query materialized. */
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

  private static void assertNoFilter(final ExecutionPlan plan) {
    assertThat(filterSteps(plan))
        .as("a filter that is true for every record must not survive into the plan: %s", plan.prettyPrint(0, 2))
        .isEmpty();
  }

  private static void assertFilter(final ExecutionPlan plan) {
    assertThat(filterSteps(plan))
        .as("a filter that can discard a record must still be evaluated: %s", plan.prettyPrint(0, 2))
        .isNotEmpty();
  }

  private static List<String> filterSteps(final ExecutionPlan plan) {
    final List<String> found = new ArrayList<>();
    for (final String name : stepNames(plan))
      if (name.contains("Filter"))
        found.add(name);
    return found;
  }

  private static List<String> fetchSteps(final ExecutionPlan plan) {
    final List<String> found = new ArrayList<>();
    for (final String name : stepNames(plan))
      if (name.startsWith("FetchFrom") || name.startsWith("CountFrom"))
        found.add(name);
    return found;
  }

  private static List<String> stepNames(final ExecutionPlan plan) {
    final List<String> found = new ArrayList<>();
    for (final ExecutionStep step : plan.getSteps())
      collectStepNames(step, found);
    return found;
  }

  private static void collectStepNames(final ExecutionStep step, final List<String> found) {
    found.add(step.getClass().getSimpleName());

    if (step.getSubSteps() != null)
      for (final ExecutionStep sub : step.getSubSteps())
        collectStepNames(sub, found);

    if (step instanceof ExecutionStepInternal internal && internal.getSubExecutionPlans() != null)
      for (final ExecutionPlan sub : internal.getSubExecutionPlans())
        for (final ExecutionStep subStep : sub.getSteps())
          collectStepNames(subStep, found);
  }
}
