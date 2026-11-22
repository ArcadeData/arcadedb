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
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.function.IntPredicate;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A WHERE with an OR that some branch no index answers is one scan of the type with the whole WHERE. It used to be planned as a
 * union: one full scan per branch that has no index, an index search for every other branch, and a DISTINCT over the rows of all
 * of them, so {@code WHERE a = 1 OR b = 2} read the type twice (and lost the parallel filtered scan a single WHERE gets).
 * The counter-based assertions below count the type scans in the plan and the records the statement materialized.
 */
class OrBranchScanCountTest extends TestHelper {
  private static final int RECORDS = 300;

  @Override
  protected void beginTest() {
    database.getSchema().createDocumentType("Plain");
    database.getSchema().createDocumentType("Mixed");
    database.getSchema().getType("Mixed").createProperty("id", Type.INTEGER);
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "Mixed", "id");
    database.getSchema().getType("Mixed").createProperty("other", Type.INTEGER);
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "Mixed", "other");

    database.transaction(() -> {
      for (int i = 0; i < RECORDS; i++) {
        database.newDocument("Plain").set("a", i).set("b", i % 7).save();
        database.newDocument("Mixed").set("id", i).set("other", i % 10).set("b", i % 7).save();
      }
    });
  }

  @Test
  void anOrOverUnindexedPropertiesScansTheTypeOnce() {
    final String sql = "SELECT FROM Plain WHERE a = 1 OR a = 2 OR b = 3";

    final List<String> steps = stepNames(planOf(sql));
    assertThat(count(steps, "FetchFromType")).as("one scan of the type, plan: %s", steps).isEqualTo(1);
    assertThat(steps).as("one scan returns each record once: nothing to deduplicate").doesNotContain("DistinctExecutionStep");

    assertThat(recordsReadBy(sql)).as("records read by the one scan").isEqualTo(RECORDS);
    assertThat(values(sql, "a")).containsExactlyInAnyOrderElementsOf(matching(i -> i == 1 || i == 2 || i % 7 == 3));
  }

  @Test
  void aRecordMatchingTwoBranchesIsReturnedOnce() {
    assertThat(values("SELECT FROM Plain WHERE a < 5 OR b = 0 OR a = 3", "a")).doesNotHaveDuplicates().contains(0, 1, 2, 3, 4, 7, 14);
  }

  @Test
  void anIndexedBranchDoesNotKeepAnIndexSearchBesideTheScanTheOtherBranchNeeds() {
    final String sql = "SELECT FROM Mixed WHERE id = 5 OR b = 3";

    final List<String> steps = stepNames(planOf(sql));
    assertThat(count(steps, "FetchFromType")).as("one scan of the type, plan: %s", steps).isEqualTo(1);
    assertThat(count(steps, "FetchFromIndex")).as("the scan already covers the indexed branch, plan: %s", steps).isZero();

    assertThat(recordsReadBy(sql)).isEqualTo(RECORDS);
    assertThat(values(sql, "id")).containsExactlyInAnyOrderElementsOf(matching(i -> i == 5 || i % 7 == 3));
  }

  @Test
  void anOrWhoseBranchesAreAllIndexedStillSearchesTheIndexes() {
    final String sql = "SELECT FROM Mixed WHERE id = 5 OR other = 7";

    final List<String> steps = stepNames(planOf(sql));
    assertThat(count(steps, "FetchFromIndex")).as("an index search per branch, plan: %s", steps).isEqualTo(2);
    assertThat(count(steps, "FetchFromType")).as("no scan, plan: %s", steps).isZero();

    assertThat(recordsReadBy(sql)).as("only the records the two searches find").isLessThan(RECORDS);
    assertThat(values(sql, "id")).containsExactlyInAnyOrderElementsOf(matching(i -> i == 5 || i % 10 == 7));
  }

  @Test
  void anOrOverAnUnindexedBranchAggregatesAndFiltersLikeTheScanDoes() {
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM Plain WHERE a = 1 OR b = 3 OR a = 10")) {
      assertThat(rs.next().<Number>getProperty("c").intValue()).isEqualTo(matching(i -> i == 1 || i % 7 == 3 || i == 10).size());
    }
    try (final ResultSet rs = database.query("sql", "SELECT FROM Plain WHERE a = 1 OR b = 3 ORDER BY a DESC LIMIT 2")) {
      assertThat(rs.next().<Integer>getProperty("a")).isEqualTo(297);
      assertThat(rs.next().<Integer>getProperty("a")).isEqualTo(290);
    }
  }

  @Test
  void anOrderByOnAnIndexedPropertyKeepsItsOrderOverTheScanOfAnOrWithAnUnindexedBranch() {
    final List<Integer> expected = matching(i -> i == 5 || i % 7 == 3);
    assertThat(values("SELECT FROM Mixed WHERE id = 5 OR b = 3 ORDER BY id", "id")).containsExactlyElementsOf(expected);
    assertThat(values("SELECT FROM Mixed WHERE id = 5 OR b = 3 ORDER BY id DESC", "id")).containsExactlyElementsOf(expected.reversed());
    // the same statement run again: the plan is the same, the rows are too
    assertThat(values("SELECT FROM Mixed WHERE id = 5 OR b = 3 ORDER BY id", "id")).containsExactlyElementsOf(expected);
  }

  private static List<Integer> matching(final IntPredicate predicate) {
    final List<Integer> matches = new ArrayList<>();
    for (int i = 0; i < RECORDS; i++)
      if (predicate.test(i))
        matches.add(i);
    return matches;
  }

  private ExecutionPlan planOf(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      return rs.getExecutionPlan().orElseThrow();
    }
  }

  private List<Integer> values(final String sql, final String property) {
    final List<Integer> values = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", sql)) {
      while (rs.hasNext())
        values.add(rs.next().<Integer>getProperty(property));
    }
    return values;
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

  private static long count(final List<String> steps, final String prefix) {
    return steps.stream().filter(name -> name.startsWith(prefix)).count();
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
