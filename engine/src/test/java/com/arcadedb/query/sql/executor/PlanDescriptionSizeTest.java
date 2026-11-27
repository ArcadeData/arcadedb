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
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The structured form of a plan ({@code ExecutionPlan.toResult()}) carries one node per step, and each node describes
 * the step itself: a step that runs a sub-plan shows that sub-plan as the nodes under it, not a second time as text in
 * its own description. The size of the descriptions therefore follows the number of steps.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class PlanDescriptionSizeTest extends TestHelper {
  /** Well above the length of any one step's own description, and far below what a description that includes its sub-plan adds up to. */
  private static final int MAX_DESCRIPTION_CHARS_PER_NODE = 300;
  private static final int LEVELS                         = 60;

  @Override
  protected void beginTest() {
    database.getSchema().createDocumentType("Item");
    database.transaction(() -> database.newDocument("Item").set("idx", 1).save());
  }

  @Test
  void nestedFromSubqueriesDescribeEachStepOnce() {
    final StringBuilder sql = new StringBuilder();
    for (int i = 0; i < LEVELS; i++)
      sql.append("select from (");
    sql.append("select from Item");
    for (int i = 0; i < LEVELS; i++)
      sql.append(")");

    assertDescriptionsAreLinear(sql.toString());
  }

  @Test
  void nestedLetSubqueriesDescribeEachStepOnce() {
    final StringBuilder sql = new StringBuilder();
    for (int i = 0; i < LEVELS; i++)
      sql.append("select from Item let $a").append(i).append(" = (");
    sql.append("select from Item");
    for (int i = 0; i < LEVELS; i++)
      sql.append(")");

    assertDescriptionsAreLinear(sql.toString());
  }

  @Test
  void aSubPlanStillShowsUpAsNodesUnderItsStep() {
    final JSONObject plan = explainPlanOf("select from (select from (select from Item))");

    final JSONObject subQuery = findStep(plan.getJSONArray("steps"), "SubQueryStep");
    assertThat(subQuery).as("the plan must carry the step that runs the sub-query: %s", plan).isNotNull();
    assertThat(subQuery.getString("description", "")).contains("FETCH FROM SUBQUERY");

    final JSONArray under = subQuery.getJSONArray("subSteps");
    assertThat(under.length()).as("the steps of the sub-plan hang under the step that runs it").isGreaterThan(0);
    assertThat(findStep(under, "SubQueryStep")).as("the nested sub-query is a node of its own").isNotNull();
    assertThat(findStep(under, "FetchFromTypeExecutionStep")).as("the innermost scan is a node of its own").isNotNull();
  }

  @Test
  void aStepThatMergesBranchesDescribesItselfAndTheBranchesAreNodesUnderIt() {
    createSearchType();
    final String a = "SEARCH_INDEX('Doc[a]', 'common') = true";
    final String b = "SEARCH_INDEX('Doc[b]', 'common') = true";

    final JSONObject merge = findStep(explainPlanOf("select from Doc where " + a + " or " + b).getJSONArray("steps"),
        "MergeByScoreStep");
    assertThat(merge).as("two scored searches joined by OR are merged by score").isNotNull();
    assertThat(merge.getString("description", "")).startsWith("+ MERGE BY $score DESC").doesNotContain("BRANCH").doesNotContain("\n");
    assertThat(merge.getJSONArray("subSteps").length()).as("the steps of both branches hang under the step").isGreaterThanOrEqualTo(2);
  }

  @Test
  void aStepThatRunsBranchesInParallelDescribesItselfAndTheBranchesAreNodesUnderIt() {
    createSearchType();
    final String a = "SEARCH_INDEX('Doc[a]', 'common') = true";
    final String b = "SEARCH_INDEX('Doc[b]', 'common') = true";
    final String c = "SEARCH_INDEX('Doc[c]', 'common') = true";

    final JSONObject parallel = findStep(explainPlanOf("select from Doc where (" + a + " and " + c + ") or " + b).getJSONArray("steps"),
        "ParallelExecStep");
    assertThat(parallel).as("branches that are not merged by score run side by side").isNotNull();
    assertThat(parallel.getString("description", "")).isEqualTo("+ PARALLEL");
    assertThat(parallel.getJSONArray("subSteps").getJSONObject(0).getString("description", "")).isNotEmpty();
    assertThat(parallel.getJSONArray("subSteps").length()).as("the steps of both branches hang under the step").isGreaterThanOrEqualTo(2);
  }

  @Test
  void thePrettyPrintedPlanStillShowsTheWholeSubPlan() {
    try (final ResultSet resultSet = database.query("sql", "explain select from (select from (select from Item))")) {
      final String text = resultSet.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
      assertThat(text).contains("+ FETCH FROM SUBQUERY").contains("+ FETCH FROM TYPE Item");
      assertThat(text.split("FETCH FROM SUBQUERY", -1)).as("one header per nested level").hasSize(3);
    }
  }

  private void createSearchType() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.a STRING");
      database.command("sql", "CREATE PROPERTY Doc.b STRING");
      database.command("sql", "CREATE PROPERTY Doc.c STRING");
      for (final String field : new String[] { "a", "b", "c" })
        database.command("sql", "CREATE INDEX ON Doc (" + field + ") FULL_TEXT");
      database.newDocument("Doc").set("a", "common words").set("b", "common text").set("c", "common line").save();
    });
  }

  private void assertDescriptionsAreLinear(final String sql) {
    final JSONObject plan = explainPlanOf(sql);

    final long[] totals = new long[2];
    count(plan.getJSONArray("steps"), totals);
    final long nodes = totals[0];
    final long descriptionChars = totals[1];

    assertThat(nodes).as("the plan must have steps").isGreaterThanOrEqualTo(LEVELS);
    assertThat(descriptionChars)
        .as("%d nodes must not carry more than %d characters of description each, in total", nodes, MAX_DESCRIPTION_CHARS_PER_NODE)
        .isLessThanOrEqualTo(nodes * MAX_DESCRIPTION_CHARS_PER_NODE);
  }

  private JSONObject explainPlanOf(final String sql) {
    try (final ResultSet resultSet = database.query("sql", "explain " + sql)) {
      final ExecutionPlan plan = resultSet.getExecutionPlan().orElseThrow();
      while (resultSet.hasNext())
        resultSet.next();
      // the same call the server makes to answer an EXPLAIN
      return plan.toResult().toJSON();
    }
  }

  private static void count(final JSONArray steps, final long[] totals) {
    if (steps == null)
      return;
    for (int i = 0; i < steps.length(); i++) {
      final JSONObject step = steps.getJSONObject(i);
      totals[0]++;
      totals[1] += step.getString("description", "").length();
      count(step.getJSONArray("subSteps", null), totals);
    }
  }

  private static JSONObject findStep(final JSONArray steps, final String name) {
    if (steps == null)
      return null;
    for (int i = 0; i < steps.length(); i++) {
      final JSONObject step = steps.getJSONObject(i);
      if (name.equals(step.getString("name", "")))
        return step;
      final JSONObject inner = findStep(step.getJSONArray("subSteps", null), name);
      if (inner != null)
        return inner;
    }
    return null;
  }
}
