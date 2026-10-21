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
package com.arcadedb.query.opencypher.executor.operators;

import com.arcadedb.graph.GraphTraversalProvider;
import com.arcadedb.graph.GraphViewPins;
import com.arcadedb.query.opencypher.ast.RelationshipPattern;
import com.arcadedb.query.opencypher.traversal.GAVDistinctEndpointTraverser;
import com.arcadedb.query.sql.executor.CommandContext;

/**
 * {@link VarLengthExpand} in distinct-endpoint mode, walking a Graph Analytical View's adjacency (issue #10548): a depth-bounded
 * breadth-first search over the view's arrays with a bitmap visited set, instead of the edge lists of the records. The rows are
 * the same: one per end vertex the pattern reaches, each end vertex a lazily loaded {@link com.arcadedb.graph.GAVVertex}.
 * <p>
 * The optimizer picks it only when the hop already reports distinct end vertices (see
 * {@link com.arcadedb.query.opencypher.executor.VariableLengthDistinctEndpoints}), the relationship carries no inline property
 * map or {@code WHERE} (the view does not store what they test) and a ready view covers its types and every vertex type. A start
 * vertex the view does not map, and a relationship list an earlier clause bound, walk the records as {@link VarLengthExpand}
 * does.
 */
public class GAVVarLengthExpand extends VarLengthExpand {
  private final GraphTraversalProvider provider;

  public GAVVarLengthExpand(final PhysicalOperator child, final GraphTraversalProvider provider, final ExpansionVariables variables,
      final TraversalSpec traversal, final double estimatedCost, final long estimatedCardinality) {
    super(child, variables, traversal, estimatedCost, estimatedCardinality);
    this.provider = provider;
    setDistinctEndpoints(true);
  }

  /** The live view the plan holds: a cached plan asks it whether it is still ready before it runs again. */
  public GraphTraversalProvider getProvider() {
    return provider;
  }

  /** Whether the view can answer {@code pattern} alone: it stores no relationship property, so nothing may test one. */
  public static boolean isAnswerableByView(final RelationshipPattern pattern) {
    return (pattern.getProperties() == null || pattern.getProperties().isEmpty()) && pattern.getWhereExpression() == null;
  }

  @Override
  protected GAVDistinctEndpointTraverser createViewTraverser(final CommandContext context) {
    // The view as the statement reads it: the plan holds the live view, since plans are cached (issue #10415)
    final GraphTraversalProvider pinned = GraphViewPins.pin(context, provider);
    final RelationshipPattern pattern = getPattern();
    final String[] edgeTypes = pattern.hasTypes() ? pattern.getTypes().toArray(new String[0]) : new String[0];
    return new GAVDistinctEndpointTraverser(pinned, getDirection(), edgeTypes, pattern.getEffectiveMinHops(),
        pattern.getEffectiveMaxHops(), getPathMode(), context);
  }

  @Override
  protected String explainProvider() {
    return ", provider=" + provider.getName();
  }

  @Override
  public String getOperatorType() {
    return "GAVVarLengthExpand";
  }
}
