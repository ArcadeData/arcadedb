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

import com.arcadedb.query.opencypher.ast.BooleanExpression;
import com.arcadedb.query.opencypher.ast.Direction;
import com.arcadedb.query.opencypher.executor.EdgeIndexSeedPlanner;
import com.arcadedb.query.opencypher.executor.steps.MatchEdgeByTypeScanStep;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * The anchor of a cost-based plan that starts from a scan of the buckets of a relationship's types (issue #11874), under a
 * predicate that requires a property of the relationship, so no light edge can be an answer, or for a statement that only
 * aggregates properties of the relationship, so no light edge adds to the answer (see
 * {@link com.arcadedb.query.opencypher.executor.EdgeTypeScanPlanner}). It binds each edge that passes the predicate's conjuncts
 * on the relationship and both endpoints, checking the endpoints' labels. The plan's filter above still applies the whole
 * {@code WHERE} clause.
 * <p>
 * {@code MATCH (a:P)-[e:K]->(b:P) WHERE e.w = 3} anchored on a scan of every P and an expansion of every one of its edges; the
 * scan of K reads the edges once. It runs the step the step-by-step plan seeds the same pattern with, {@link MatchEdgeByTypeScanStep}.
 */
public class EdgeTypeScan extends AbstractPhysicalOperator implements RelationshipAnchor {
  private final List<String>                        edgeTypes;
  private final Direction                           direction;
  private final String                              sourceVariable;
  private final String                              relationshipVariable;
  private final String                              targetVariable;
  private final EdgeIndexSeedPlanner.EndpointFilter sourceFilter;
  private final EdgeIndexSeedPlanner.EndpointFilter targetFilter;
  private final BooleanExpression                   relationshipFilter;

  public EdgeTypeScan(final List<String> edgeTypes, final Direction direction, final String sourceVariable,
      final String relationshipVariable, final String targetVariable, final EdgeIndexSeedPlanner.EndpointFilter sourceFilter,
      final EdgeIndexSeedPlanner.EndpointFilter targetFilter, final BooleanExpression relationshipFilter, final double estimatedCost,
      final long estimatedCardinality) {
    super(estimatedCost, estimatedCardinality);
    this.edgeTypes = edgeTypes;
    this.direction = direction;
    this.sourceVariable = sourceVariable;
    this.relationshipVariable = relationshipVariable;
    this.targetVariable = targetVariable;
    this.sourceFilter = sourceFilter;
    this.targetFilter = targetFilter;
    this.relationshipFilter = relationshipFilter;
  }

  @Override
  public ResultSet execute(final CommandContext context, final int nRecords) {
    return new MatchEdgeByTypeScanStep(edgeTypes, direction, sourceVariable, relationshipVariable, targetVariable, sourceFilter,
        targetFilter, null, relationshipFilter, context).syncPull(context, nRecords > 0 ? nRecords : 100);
  }

  @Override
  public List<String> getBoundVariables() {
    final List<String> bound = new ArrayList<>(3);
    bound.add(sourceVariable);
    bound.add(targetVariable);
    if (relationshipVariable != null && !relationshipVariable.isEmpty())
      bound.add(relationshipVariable);
    return bound;
  }

  @Override
  public String getOperatorType() {
    return "EdgeTypeScan";
  }

  @Override
  public String explain(final int depth) {
    final StringBuilder sb = new StringBuilder();
    sb.append(getIndent(depth)).append("+ ").append(getOperatorType()).append(' ');
    sb.append(MatchEdgeByTypeScanStep.describe(edgeTypes, direction, sourceVariable, relationshipVariable, targetVariable,
        sourceFilter, targetFilter, relationshipFilter));
    sb.append(" [cost=").append(String.format(Locale.US, "%.2f", estimatedCost));
    sb.append(", rows=").append(estimatedCardinality);
    sb.append("]\n");
    return sb.toString();
  }
}
