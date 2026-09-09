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

import com.arcadedb.utility.ExcludeFromJacocoGeneratedReport;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by luigidellaquila on 20/07/16.
 */
@ExcludeFromJacocoGeneratedReport
public interface ExecutionStep {

  String getName();

  String getType();

  String getDescription();

  List<ExecutionStep> getSubSteps();

  /**
   * The <b>self</b> cost (in nanoseconds) of this step: the time spent inside this step's own work, with the time
   * its sub-steps spent inside theirs excluded. Every step in a plan reports its own, so the self costs of a plan
   * partition its total rather than overlapping - which is what lets a consumer sum them, and what identifies the
   * one step worth optimising.
   * <p>
   * A container step that only dispatches to its sub-steps has no work of its own to time and reports -1 here, the
   * same sentinel as a step that ran with profiling off. {@link #getTotalCost()} is the subtree roll-up, and the
   * number to display for such a container. Returning the roll-up from this method instead is what made the
   * profiler count a plain type scan twice, once on the container and once on the bucket step that actually timed
   * it (issue #7329).
   *
   * @return the self cost (in nanoseconds) of the execution of this step, -1 if not calculated
   */
  default long getCost() {
    return -1L;
  }

  /**
   * The total cost (in nanoseconds) of the subtree rooted at this step: this step's self cost plus, recursively,
   * that of every sub-step and of every step of every sub-execution-plan hanging off it. -1 when nothing in the
   * subtree was timed.
   * <p>
   * Never sum this across the steps of a plan - a parent's total already contains its children's. It is the number
   * to <i>display</i> for one node; {@link #getCost()} is the number to <i>aggregate</i>.
   * <p>
   * Re-walks the subtree on every call, so it is for the one node a caller is about to draw. A traversal that
   * already visits every node folds the totals up instead of calling this per node - see {@link #toResult()}.
   */
  default long getTotalCost() {
    long total = getCost();

    final List<ExecutionStep> subSteps = getSubSteps();
    if (subSteps != null)
      for (final ExecutionStep step : subSteps)
        total = addCost(total, step.getTotalCost());

    if (this instanceof final ExecutionStepInternal stepInternal) {
      final List<ExecutionPlan> subPlans = stepInternal.getSubExecutionPlans();
      if (subPlans != null)
        for (final ExecutionPlan plan : subPlans) {
          final List<ExecutionStep> planSteps = plan.getSteps();
          if (planSteps != null)
            for (final ExecutionStep step : planSteps)
              total = addCost(total, step.getTotalCost());
        }
    }

    return total;
  }

  /**
   * Adds two costs treating -1 ("not calculated") as absent rather than as a duration, so a subtree where only some
   * steps were timed reports the sum of the timed ones instead of a value pulled below zero by the sentinel.
   */
  private static long addCost(final long a, final long b) {
    if (a < 0)
      return b;
    if (b < 0)
      return a;
    return a + b;
  }

  default Result toResult() {
    final ResultInternal result = new ResultInternal();
    result.setProperty("name", getName());
    result.setProperty("type", getType());
    result.setProperty("targetNode", getType());
    result.setProperty(InternalExecutionPlan.JAVA_TYPE, getClass().getName());
    // Self cost and subtree roll-up under distinct names: an aggregator sums "cost" across the whole tree, a
    // display picks "totalCost" for the node it is drawing. Emitting the roll-up as "cost" while also emitting
    // each sub-step with its own "cost" in the same node double-counted every container (issue #7329).
    final long selfCost = getCost();
    result.setProperty("cost", selfCost);

    // Collect direct sub-steps
    List<Result> subStepResults = getSubSteps() == null ? null
        : getSubSteps().stream().map(ExecutionStep::toResult).collect(Collectors.toList());

    // Also include steps from sub-execution plans (e.g. SubQueryStep, GlobalLetQueryStep)
    if (this instanceof ExecutionStepInternal stepInternal) {
      final List<ExecutionPlan> subPlans = stepInternal.getSubExecutionPlans();
      if (subPlans != null && !subPlans.isEmpty()) {
        final List<Result> allSubSteps = subStepResults != null ? new ArrayList<>(subStepResults) : new ArrayList<>();
        for (final ExecutionPlan plan : subPlans) {
          final List<ExecutionStep> planSteps = plan.getSteps();
          if (planSteps != null)
            for (final ExecutionStep s : planSteps)
              allSubSteps.add(s.toResult());
        }
        subStepResults = allSubSteps;
      }
    }
    result.setProperty("subSteps", subStepResults);

    // The roll-up is folded up from the children's ALREADY-computed totals rather than read from
    // getTotalCost(), which re-walks the whole subtree on every call: one such call per node, inside a
    // traversal that already visits every node, makes serializing a plan quadratic in step count. Bottom-up
    // here it is linear, and every node still reports the same number getTotalCost() would.
    long totalCost = selfCost;
    if (subStepResults != null)
      for (final Result subStep : subStepResults) {
        final Long subTotal = subStep.getProperty("totalCost");
        // A step that overrides toResult() and omits the field contributes nothing rather than a wrong number.
        totalCost = addCost(totalCost, subTotal != null ? subTotal : -1L);
      }
    result.setProperty("totalCost", totalCost);

    result.setProperty("description", getDescription());
    return result;
  }

}
