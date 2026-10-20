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

import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.TimeSeriesType;
import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.Contains;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.NotStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStepContract;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.ConnectiveP;
import org.apache.tinkerpop.gremlin.structure.T;

import java.util.Collection;
import java.util.List;

/**
 * Refuses a {@code hasLabel()} that asks for the elements of a TIMESERIES type (issue #10442, the Gremlin side of #10440).
 * <p>
 * A time-series type keeps its samples in its own engine, and they are neither vertices nor edges, so no element a traversal
 * reaches can carry the label. Before this check {@code g.V().hasLabel('Ev').count()} answered 0 over a series SQL counts the
 * samples of, which reads as a correct answer about the data. The check runs when the traversal is compiled, on every
 * {@code has()} step of it and on the edge labels of every {@code out()}/{@code in()}/{@code both()} hop and their
 * {@code E} forms: {@link ArcadeTraversalStrategy} is applied to each child traversal too, so a label inside
 * {@code where()}, {@code filter()}, {@code union()} and the like is refused as well, whether or not a traverser ever reaches it.
 * <p>
 * Only a label the predicate asks FOR is refused - an equality, a {@code within()}, and those inside {@code and()}/{@code or()}.
 * A negation ({@code neq}, {@code without}, {@code not(hasLabel(...))}) asks a question whose answer - no element carries the
 * label - is correct, so it is answered, as Cypher answers {@code WHERE NOT n:Ev}. A pattern predicate on the label
 * ({@code TextP.startingWith('E')}, a custom {@code BiPredicate}) is not refused either: it names no type, so it matches no
 * time-series type and answers nothing for it, a known limit of the check.
 */
final class TimeSeriesLabelGuard {
  private TimeSeriesLabelGuard() {
    // utility class
  }

  /**
   * @throws CommandSemanticException when a {@code has()} step or a hop of {@code traversal} asks for the label of a time-series type
   */
  static void check(final Schema schema, final Traversal.Admin<?, ?> traversal) {
    // inside the body of not(), however deep (not(and(hasLabel('Ev'), ...))): every label it names is negated
    if (isNegated(traversal))
      return;

    final List<Step> steps = traversal.getSteps();
    for (int i = 0; i < steps.size(); i++) {
      final Step<?, ?> step = steps.get(i);
      if (step instanceof HasContainerHolder<?, ?> holder) {
        final List<HasContainer> containers = holder.getHasContainers();
        for (int j = 0; j < containers.size(); j++) {
          final HasContainer container = containers.get(j);
          if (T.label.getAccessor().equals(container.getKey()))
            checkPredicate(schema, container.getPredicate());
        }
      } else if (step instanceof VertexStepContract<?> vertexStep) {
        // out('Ev'), outE('Ev'), ...: also what TinkerPop folds outE().hasLabel('Ev') into before this strategy runs
        final String[] edgeLabels = vertexStep.getEdgeLabels();
        if (edgeLabels != null)
          for (final String edgeLabel : edgeLabels)
            checkName(schema, edgeLabel);
      }
    }
  }

  /**
   * Whether {@code traversal} sits under an odd number of {@code not()} steps, walking its parents up to the root.
   */
  private static boolean isNegated(final Traversal.Admin<?, ?> traversal) {
    boolean negated = false;
    TraversalParent parent = traversal.getParent();
    while (parent != null && !(parent instanceof EmptyStep)) {
      if (parent instanceof NotStep)
        negated = !negated;
      parent = parent.asStep().getTraversal().getParent();
    }
    return negated;
  }

  private static void checkPredicate(final Schema schema, final P<?> predicate) {
    if (predicate == null)
      return;

    if (predicate instanceof ConnectiveP<?> connective) {
      // and() / or(): each operand is checked on its own, a negated one is skipped there
      for (final P<?> operand : connective.getPredicates())
        checkPredicate(schema, operand);
      return;
    }

    // any other predicate (neq, without, a TextP, a custom BiPredicate) names no label it asks for, so it is not refused
    final Object value = predicate.getValue();
    if (predicate.getBiPredicate() == Compare.eq)
      checkName(schema, value);
    else if (predicate.getBiPredicate() == Contains.within) {
      if (value instanceof Collection<?> names)
        for (final Object name : names)
          checkName(schema, name);
      else
        checkName(schema, value);
    }
  }

  private static void checkName(final Schema schema, final Object name) {
    if (name == null)
      return;
    // a lookup per label: a label naming no time-series type, which is every label of a graph traversal, costs only that
    // ('bucket:<name>' needs no case of its own: a time-series type has no bucket, its samples live in its own engine)
    final DocumentType type = schema.getTypeOrNull(name.toString());
    if (type instanceof TimeSeriesType)
      throw new CommandSemanticException("Gremlin cannot match the label '" + type.getName() + "', a TIMESERIES type: "
          + "time-series samples are neither vertices nor edges. Query it with SQL (SELECT FROM `" + type.getName()
          + "`) or PromQL instead");
  }
}
