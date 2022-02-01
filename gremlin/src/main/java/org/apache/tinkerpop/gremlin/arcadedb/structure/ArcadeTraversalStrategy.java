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
package org.apache.tinkerpop.gremlin.arcadedb.structure;

import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.serializer.BinaryComparator;
import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.InlineFilterStrategy;

import java.util.*;
import java.util.stream.*;

/**
 * Replaces default traversal steps to speedup execution. This is used only when the traversal has a GraphStep (vertices or edges) and HasStep (label eq(X)).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class ArcadeTraversalStrategy extends AbstractTraversalStrategy<TraversalStrategy.OptimizationStrategy>
    implements TraversalStrategy.OptimizationStrategy {

  private static final String LABEL_KEY = "~label";

  @Override
  public void apply(final Traversal.Admin<?, ?> traversal) {
    final List<Step> steps = traversal.getSteps();
    for (int i = 1; i < steps.size(); i++) {
      final Step step = steps.get(i);
      if (step instanceof HasStep) {
        final Step prevStep = steps.get(i - 1);
        if (prevStep instanceof GraphStep) {
          final GraphStep prevStepGraph = (GraphStep) prevStep;

          if (prevStepGraph.getIds().length != 0)
            continue;

          final List<HasContainer> hasContainers = new ArrayList<>(((HasStep) step).getHasContainers());

          String typeNameToMatch = null;

          int totalLabels = 0;
          for (HasContainer c : hasContainers) {
            final String key = c.getKey();
            if (BinaryComparator.equalsString(key, LABEL_KEY)) {
              ++totalLabels;

              if (totalLabels > 1)
                break;

              if (c.getBiPredicate().equals(Compare.eq) && c.getValue() != null)
                typeNameToMatch = c.getValue().toString();
            }
          }

          if (totalLabels == 1 && typeNameToMatch != null) {

            // LOOKING FOR INDEX LOOKUP
            final ArcadeGraph graph = (ArcadeGraph) traversal.getGraph().get();

            final List<IndexCursor> indexCursors = new ArrayList<>();

            for (HasContainer c : hasContainers) {
              final String key = c.getKey();
              if (!key.startsWith("~")) {
                final TypeIndex index = graph.database.getSchema().getType(typeNameToMatch).getPolymorphicIndexByProperties(key);
                if (index != null) {
                  if (c.getBiPredicate().equals(Compare.eq))
                    indexCursors.add(index.get(c.getValue().getClass().isArray() ? (Object[]) c.getValue() : new Object[] { c.getValue() }));
                  else if (c.getBiPredicate().equals(Compare.gt))
                    indexCursors.add(index.iterator(true, c.getValue().getClass().isArray() ? (Object[]) c.getValue() : new Object[] { c.getValue() }, false));
                  else if (c.getBiPredicate().equals(Compare.gte))
                    indexCursors.add(index.iterator(true, c.getValue().getClass().isArray() ? (Object[]) c.getValue() : new Object[] { c.getValue() }, true));
                  else if (c.getBiPredicate().equals(Compare.lt))
                    indexCursors.add(index.iterator(false, c.getValue().getClass().isArray() ? (Object[]) c.getValue() : new Object[] { c.getValue() }, false));
                  else if (c.getBiPredicate().equals(Compare.lte))
                    indexCursors.add(index.iterator(false, c.getValue().getClass().isArray() ? (Object[]) c.getValue() : new Object[] { c.getValue() }, true));
                }
              }
            }

            final Step replaceWith;
            if (indexCursors.isEmpty())
              replaceWith = new ArcadeFilterByTypeStep(prevStepGraph.getTraversal(), prevStepGraph.getReturnClass(), prevStepGraph.isStartStep(),
                  typeNameToMatch);
            else
              replaceWith = new ArcadeFilterByIndexStep(prevStepGraph.getTraversal(), prevStepGraph.getReturnClass(), prevStepGraph.isStartStep(),
                  indexCursors);

            //traversal.removeStep(i); // IF THE HAS-LABEL STEP IS REMOVED, FOR SOME REASON DOES NOT WORK
            traversal.removeStep(i - 1);
            traversal.addStep(i - 1, replaceWith);
            break;
          }
        }
      }
    }
  }

  @Override
  public Set<Class<? extends OptimizationStrategy>> applyPrior() {
    return Stream.of(
        //Inline must happen first as it sometimes removes the need for a TraversalFilterStep
        InlineFilterStrategy.class).collect(Collectors.toSet());
  }
}
