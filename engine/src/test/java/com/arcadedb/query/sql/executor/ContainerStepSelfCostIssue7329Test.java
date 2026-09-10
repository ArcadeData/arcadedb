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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A step's {@code cost} is its SELF cost, so the costs of a plan partition its total instead of overlapping, and a
 * consumer that walks the tree adding them up - which is exactly what the server profiler's aggregated step table
 * does - cannot charge the same nanoseconds twice.
 * <p>
 * The three container steps ({@code FetchFromTypeExecutionStep}, {@code FetchFromTypeWithFilterStep},
 * {@code FetchFromClustersExecutionStep}) used to return their children's sum from {@code getCost()} while
 * {@code toResult()} emitted those same children, each with its own cost, in the same node. A plain type scan was
 * therefore reported twice over and the step costs added up to more than the Engine total the Studio caption says
 * contains them (issue #7329). The roll-up now travels separately as {@code totalCost}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class ContainerStepSelfCostIssue7329Test {
  private static final String DATABASE_DIR = "./target/databases/testcontainer-step-self-cost-7329";

  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory(DATABASE_DIR).create();
    database.getSchema().createDocumentType("Item");
    database.transaction(() -> {
      for (int i = 0; i < 200; i++)
        database.newDocument("Item").set("idx", i).save();
    });
  }

  @AfterEach
  void cleanup() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void aContainerStepReportsNoSelfCostAndTheChildrenCarryTheTime() {
    final JSONObject plan = profiledPlanOf("select from Item");

    final JSONObject container = findStep(plan, "FetchFromTypeExecutionStep");
    assertThat(container).as("the type scan container step must be in the plan").isNotNull();

    // -1 is the engine's "not calculated": this step dispatches to its bucket sub-steps and times nothing itself.
    assertThat(container.getLong("cost", 0)).isEqualTo(-1L);

    final List<JSONObject> buckets = collectSteps(container.getJSONArray("subSteps"));
    assertThat(buckets).as("the container must carry the bucket steps that do the timing").isNotEmpty();
    assertThat(buckets.stream().anyMatch(s -> s.getLong("cost", -1) >= 0))
        .as("at least one bucket step must have been timed").isTrue();
  }

  /**
   * The roll-up did not disappear, it moved: {@code totalCost} is the subtree total, and it is what the EXPLAIN tree
   * shows for a container. It is emitted under its own name precisely so an aggregator can tell it apart from a
   * measured self cost.
   */
  @Test
  void theContainerStillReportsTheSubtreeRollUpUnderTotalCost() {
    final JSONObject plan = profiledPlanOf("select from Item");
    final JSONObject container = findStep(plan, "FetchFromTypeExecutionStep");

    // Direct children only: the container's total is its own (absent) cost plus each child's total, so summing a
    // flattened tree here would count a grandchild inside its parent's total and again on its own.
    long childrenTotal = 0;
    final JSONArray buckets = container.getJSONArray("subSteps");
    for (int i = 0; i < buckets.length(); i++) {
      final long cost = buckets.getJSONObject(i).getLong("totalCost", -1);
      if (cost >= 0)
        childrenTotal += cost;
    }

    assertThat(container.getLong("totalCost", -1)).isEqualTo(childrenTotal);
  }

  /**
   * The invariant the Studio caption states, checked the way the server profiler computes it: walk the whole tree
   * adding every {@code cost}, and the answer must not exceed the plan's own subtree total. Before the fix the
   * container's children were counted twice - once inside its derived {@code cost}, once on their own nodes - so
   * the walk came out strictly larger.
   */
  @Test
  void theSelfCostsOfTheWholeTreeDoNotExceedTheSubtreeTotal() {
    final JSONObject plan = profiledPlanOf("select from Item");

    long selfCostSum = 0;
    long topLevelTotal = 0;
    final JSONArray steps = plan.getJSONArray("steps");
    for (int i = 0; i < steps.length(); i++) {
      selfCostSum += sumSelfCosts(steps.getJSONObject(i));
      final long total = steps.getJSONObject(i).getLong("totalCost", -1);
      if (total >= 0)
        topLevelTotal += total;
    }

    assertThat(topLevelTotal).as("the run must have been timed at all").isGreaterThan(0L);
    assertThat(selfCostSum).isEqualTo(topLevelTotal);
  }

  private long sumSelfCosts(final JSONObject step) {
    long total = 0;
    final long cost = step.getLong("cost", -1);
    if (cost >= 0)
      total += cost;

    if (step.has("subSteps") && step.get("subSteps") instanceof final JSONArray subSteps)
      for (int i = 0; i < subSteps.length(); i++)
        total += sumSelfCosts(subSteps.getJSONObject(i));

    return total;
  }

  private JSONObject profiledPlanOf(final String query) {
    try (final ResultSet rs = database.query("sql", query, Map.of("$profileExecution", true))) {
      while (rs.hasNext())
        rs.next();
      return rs.getExecutionPlan().orElseThrow().toResult().toJSON();
    }
  }

  private JSONObject findStep(final JSONObject plan, final String name) {
    for (final JSONObject step : collectSteps(plan.getJSONArray("steps")))
      if (name.equals(step.getString("name", "")))
        return step;
    return null;
  }

  /** Flattens a step array and everything nested under it, so a lookup does not depend on the plan's shape. */
  private List<JSONObject> collectSteps(final JSONArray steps) {
    final List<JSONObject> flattened = new ArrayList<>();
    if (steps == null)
      return flattened;

    for (int i = 0; i < steps.length(); i++) {
      final JSONObject step = steps.getJSONObject(i);
      flattened.add(step);
      if (step.has("subSteps") && step.get("subSteps") instanceof final JSONArray subSteps)
        flattened.addAll(collectSteps(subSteps));
    }
    return flattened;
  }
}
