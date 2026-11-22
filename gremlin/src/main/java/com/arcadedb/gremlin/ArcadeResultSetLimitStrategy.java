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

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.query.OperationType;
import com.arcadedb.query.ProfiledLimitScope;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.ProfileSideEffectStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.finalization.ProfileStrategy;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;

/**
 * Applies the {@code resultSetLimit} of the current user's security groups to a Gremlin traversal as a cap on the rows it
 * answers (issue #13087), by ending it with an {@link ArcadeResultSetLimitStep}.
 * <p>
 * Gremlin never applied the quota: it was bounded only by the truncation of every bucket scan, which #12888 removed because it
 * changed the answer instead of its size. A strategy is the one place every entry point goes through: a statement of the
 * {@code gremlin} query engine, the eager terminal step of a script, the fluent API, and the script and bytecode requests of the
 * Gremlin Server, which build their traversal from the server's traversal source and never reach the query engine. It is
 * pinned in {@link ArcadeTraversalStrategies}, so no request can drop it with {@code withoutStrategies()}.
 * <p>
 * Only a root traversal is capped, and only one that is not run as a part of a statement the quota already caps
 * ({@link ProfiledLimitScope}): a child traversal ({@code union()}, {@code map()}, {@code where()}...) and a nested statement
 * read every row they match, and the root caps what it answers.
 * <p>
 * A finalization strategy: it runs once the optimizations have settled the steps, so none of them reads the cap as the end of
 * the traversal it rewrites. Before {@code ProfileStrategy}, so a profiled traversal times the cap as one of its steps, and the
 * cap goes before the {@code profile()} step, which TinkerPop requires at the end.
 *
 * @author robfrank (ro.franchini@gmail.com)
 */
public final class ArcadeResultSetLimitStrategy extends AbstractTraversalStrategy<TraversalStrategy.FinalizationStrategy>
    implements TraversalStrategy.FinalizationStrategy {

  private static final ArcadeResultSetLimitStrategy                             INSTANCE = new ArcadeResultSetLimitStrategy();
  private static final Set<Class<? extends TraversalStrategy.FinalizationStrategy>> POSTS    = Set.of(ProfileStrategy.class);

  private ArcadeResultSetLimitStrategy() {
  }

  public static ArcadeResultSetLimitStrategy instance() {
    return INSTANCE;
  }

  @Override
  public void apply(final Traversal.Admin<?, ?> traversal) {
    if (!traversal.isRoot() || ProfiledLimitScope.isActive())
      return;
    if (!(traversal.getGraph().orElse(null) instanceof ArcadeGraph graph) || !(graph.getDatabase() instanceof DatabaseInternal database))
      return;

    final long limit = database.getResultSetLimit();
    if (limit < 0)
      return;

    final List<Step> steps = traversal.getSteps();
    int position = steps.size();
    for (int i = 0; i < steps.size(); i++) {
      final Step<?, ?> step = steps.get(i);
      if (step instanceof ArcadeResultSetLimitStep)
        return;
      if (step instanceof ProfileSideEffectStep) {
        position = i;
        break;
      }
    }

    final boolean writes = GremlinStatementAnalyzer.collectWrites(traversal, EnumSet.noneOf(OperationType.class));
    traversal.addStep(position, new ArcadeResultSetLimitStep<>(traversal, limit, writes));
  }

  @Override
  public Set<Class<? extends TraversalStrategy.FinalizationStrategy>> applyPost() {
    return POSTS;
  }
}
