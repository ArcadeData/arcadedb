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
package com.arcadedb.gremlin;

import com.arcadedb.query.sql.executor.ExecutionPlan;
import com.arcadedb.query.sql.executor.ExecutionStep;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.ProfileSideEffectStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversalMetrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.Metrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMetrics;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * The execution plan of a Gremlin statement run under {@code $profileExecution}, built from the SAME run the caller
 * consumes (issue #7408).
 * <p>
 * {@code $profileExecution} asks for the statement to be timed, not to be run differently - the rule #7330 settled for
 * OpenCypher. The plan used to come from a second execution of the statement with {@code .profile()} appended and
 * drained: every Gremlin read on a recording server cost double, the plan described a run the caller never got, and a
 * mutation could not be profiled at all without applying it twice (issue #7394). Instead, {@link #attach} appends
 * TinkerPop's side-effect form of the step, {@code profile(key)}, to the traversal the caller is about to iterate:
 * {@code ProfileStrategy} wraps every step with a timer, rows flow through unchanged, and the metrics are published
 * under the hidden side-effect key once the traversal is exhausted.
 * <p>
 * The plan is available once the traversal has started; before it is exhausted it is a snapshot of the part that ran.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class GremlinExecutionPlan implements ExecutionPlan {
  static final String METRICS_KEY = Graph.Hidden.hide("arcadedb.profile");

  private final Traversal.Admin<?, ?> traversal;

  private GremlinExecutionPlan(final Traversal.Admin<?, ?> traversal) {
    this.traversal = traversal;
  }

  /**
   * Instruments {@code statementResult} for profiling and returns its plan, or {@code null} when it cannot be profiled
   * without running it again: {@code eval()} answered a plain value (the statement ended in an eager terminal step such
   * as {@code .next()}, so it already ran), the traversal is already iterated, or the statement carries its own
   * {@code .profile()}, whose metrics ARE the rows it returns.
   */
  static GremlinExecutionPlan attach(final Object statementResult) {
    if (!(statementResult instanceof final GraphTraversal<?, ?> graphTraversal))
      return null;

    final Traversal.Admin<?, ?> admin = graphTraversal.asAdmin();
    if (admin.isLocked() || TraversalHelper.hasStepOfAssignableClassRecursively(ProfileSideEffectStep.class, admin))
      return null;

    graphTraversal.profile(METRICS_KEY);
    return new GremlinExecutionPlan(admin);
  }

  /** Whether the traversal ran far enough (strategies applied, iteration started) for its metrics to exist. */
  public boolean isAvailable() {
    return traversal.isLocked();
  }

  /**
   * The metrics of the run: the finalized ones once the traversal is exhausted, otherwise a snapshot of the steps that
   * ran so far. {@code null} before the iteration started.
   */
  public TraversalMetrics getMetrics() {
    if (!traversal.isLocked())
      return null;

    if (traversal.getSideEffects().exists(METRICS_KEY)) {
      final DefaultTraversalMetrics published = traversal.getSideEffects().get(METRICS_KEY);
      if (published.isFinalized())
        return published;
    }

    final DefaultTraversalMetrics snapshot = new DefaultTraversalMetrics();
    snapshot.setMetrics(traversal, false);
    return snapshot;
  }

  @Override
  public List<ExecutionStep> getSteps() {
    return Collections.emptyList();
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final TraversalMetrics metrics = getMetrics();
    return metrics != null ? metrics.toString() : "";
  }

  /**
   * The plan in the shape the SQL and OpenCypher plans share, so the server profiler and Studio read it without a
   * Gremlin-specific branch: {@code cost} is a step's SELF time in nanoseconds, {@code totalCost} includes its nested
   * traversals, which travel as {@code subSteps}.
   */
  @Override
  public Result toResult() {
    final TraversalMetrics metrics = getMetrics();
    final ResultInternal result = new ResultInternal();
    result.setProperty("type", "GremlinExecutionPlan");
    result.setProperty("javaType", getClass().getName());
    result.setProperty("cost", metrics != null ? metrics.getDuration(TimeUnit.NANOSECONDS) : -1L);
    result.setProperty("prettyPrint", metrics != null ? metrics.toString() : "");
    result.setProperty("steps", metrics != null ? toSteps(metrics.getMetrics()) : Collections.emptyList());
    return result;
  }

  private static List<Result> toSteps(final Collection<? extends Metrics> metrics) {
    final List<Result> steps = new ArrayList<>(metrics.size());
    for (final Metrics m : metrics) {
      final long total = m.getDuration(TimeUnit.NANOSECONDS);
      long nested = 0;
      for (final Metrics n : m.getNested())
        nested += n.getDuration(TimeUnit.NANOSECONDS);

      final ResultInternal step = new ResultInternal();
      step.setProperty("name", m.getName());
      step.setProperty("description", describe(m));
      step.setProperty("cost", Math.max(0L, total - nested));
      step.setProperty("totalCost", total);
      step.setProperty("elements", countOf(m, TraversalMetrics.ELEMENT_COUNT_ID));
      step.setProperty("traversers", countOf(m, TraversalMetrics.TRAVERSER_COUNT_ID));
      if (!m.getNested().isEmpty())
        step.setProperty("subSteps", toSteps(m.getNested()));
      steps.add(step);
    }
    return steps;
  }

  private static String describe(final Metrics m) {
    return m.getName() + " (elements=" + countOf(m, TraversalMetrics.ELEMENT_COUNT_ID) + ", traversers=" + countOf(m,
        TraversalMetrics.TRAVERSER_COUNT_ID) + ")";
  }

  private static long countOf(final Metrics m, final String key) {
    final Long count = m.getCount(key);
    return count != null ? count : 0L;
  }
}
