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

import com.arcadedb.query.opencypher.ast.Direction;
import com.arcadedb.query.opencypher.executor.EdgeIndexSeedPlanner;
import com.arcadedb.query.opencypher.executor.steps.MatchEdgeByIdStep;
import com.arcadedb.query.opencypher.executor.steps.MatchEdgeByIndexStep;
import com.arcadedb.query.sql.executor.AbstractExecutionStep;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * The anchor of a cost-based plan that starts from a relationship instead of a node (issue #11481): it seeks the index of the
 * relationship's type for the edges its predicates pin, and binds the edge and both endpoints, checking the endpoints' labels on
 * the way. The expansion chain then goes on from both endpoints.
 * <p>
 * {@code MATCH (n:Entity)-[e:RELATES_TO]->(m:Entity) WHERE e.uuid = $u} used to anchor on a scan of every Entity and expand every
 * one of its edges to find the one the index holds; the step-by-step plan already answered the same lookup with anonymous endpoints
 * from the index, and this operator runs that same step, {@link MatchEdgeByIndexStep}, so both plans seek alike.
 * <p>
 * Built with ids instead of seeks ({@link #byIds}), it fetches the edges the {@code WHERE} clause pins by
 * {@code elementId(r)} / {@code id(r)} by their RID, running {@link MatchEdgeByIdStep} as the step-by-step plan does (issue #12134).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class EdgeIndexSeek extends AbstractPhysicalOperator {
  private final List<EdgeIndexSeedPlanner.TypeSeek> seeks;
  /** The ids of a seek by id, null for a seek of an index. */
  private final InListValues                        ids;
  /** The relationship types of a seek by id: the seeks name their own types. */
  private final List<String>                        edgeTypes;
  private final Direction                           direction;
  private final String                              sourceVariable;
  private final String                              relationshipVariable;
  private final String                              targetVariable;
  private final EdgeIndexSeedPlanner.EndpointFilter sourceFilter;
  private final EdgeIndexSeedPlanner.EndpointFilter targetFilter;
  private final Schema                              schema;

  public EdgeIndexSeek(final List<EdgeIndexSeedPlanner.TypeSeek> seeks, final Direction direction, final String sourceVariable,
      final String relationshipVariable, final String targetVariable, final EdgeIndexSeedPlanner.EndpointFilter sourceFilter,
      final EdgeIndexSeedPlanner.EndpointFilter targetFilter, final Schema schema, final double estimatedCost,
      final long estimatedCardinality) {
    this(seeks, null, List.of(), direction, sourceVariable, relationshipVariable, targetVariable, sourceFilter, targetFilter, schema,
        estimatedCost, estimatedCardinality);
  }

  private EdgeIndexSeek(final List<EdgeIndexSeedPlanner.TypeSeek> seeks, final InListValues ids, final List<String> edgeTypes,
      final Direction direction, final String sourceVariable, final String relationshipVariable, final String targetVariable,
      final EdgeIndexSeedPlanner.EndpointFilter sourceFilter, final EdgeIndexSeedPlanner.EndpointFilter targetFilter,
      final Schema schema, final double estimatedCost, final long estimatedCardinality) {
    super(estimatedCost, estimatedCardinality);
    this.seeks = seeks;
    this.ids = ids;
    this.edgeTypes = edgeTypes;
    this.direction = direction;
    this.sourceVariable = sourceVariable;
    this.relationshipVariable = relationshipVariable;
    this.targetVariable = targetVariable;
    this.sourceFilter = sourceFilter;
    this.targetFilter = targetFilter;
    this.schema = schema;
  }

  /** A seek of the edges of {@code edgeTypes} (any type when empty) the ids name, fetched by RID (issue #12134). */
  public static EdgeIndexSeek byIds(final InListValues ids, final List<String> edgeTypes, final Direction direction,
      final String sourceVariable, final String relationshipVariable, final String targetVariable,
      final EdgeIndexSeedPlanner.EndpointFilter sourceFilter, final EdgeIndexSeedPlanner.EndpointFilter targetFilter,
      final Schema schema, final double estimatedCost, final long estimatedCardinality) {
    return new EdgeIndexSeek(List.of(), ids, edgeTypes, direction, sourceVariable, relationshipVariable, targetVariable,
        sourceFilter, targetFilter, schema, estimatedCost, estimatedCardinality);
  }

  @Override
  public ResultSet execute(final CommandContext context, final int nRecords) {
    // The result set closes the step, and with it the index cursor it holds
    return newStep(context).syncPull(context, nRecords > 0 ? nRecords : 100);
  }

  private AbstractExecutionStep newStep(final CommandContext context) {
    if (ids != null)
      return new MatchEdgeByIdStep(ids, edgeTypes, direction, sourceVariable, relationshipVariable, targetVariable, sourceFilter,
          targetFilter, null, context);
    return new MatchEdgeByIndexStep(seeks, direction, sourceVariable, relationshipVariable, targetVariable, sourceFilter,
        targetFilter, null, context);
  }

  /** The variables a row of this operator binds: both endpoints and the relationship. */
  public List<String> getBoundVariables() {
    final List<String> bound = new ArrayList<>(3);
    bound.add(sourceVariable);
    bound.add(targetVariable);
    if (relationshipVariable != null && !relationshipVariable.isEmpty())
      bound.add(relationshipVariable);
    return bound;
  }

  public String getSourceVariable() {
    return sourceVariable;
  }

  public String getTargetVariable() {
    return targetVariable;
  }

  public String getRelationshipVariable() {
    return relationshipVariable;
  }

  @Override
  public String getOperatorType() {
    return ids != null ? "EdgeIdSeek" : "EdgeIndexSeek";
  }

  @Override
  public String explain(final int depth) {
    final StringBuilder sb = new StringBuilder();
    sb.append(getIndent(depth)).append("+ ").append(getOperatorType()).append(' ');
    sb.append(ids != null ?
        MatchEdgeByIdStep.describe(ids, edgeTypes, direction, sourceVariable, relationshipVariable, targetVariable, sourceFilter,
            targetFilter) :
        MatchEdgeByIndexStep.describe(schema, seeks, direction, sourceVariable, relationshipVariable, targetVariable, sourceFilter,
            targetFilter));
    sb.append(" [cost=").append(String.format(Locale.US, "%.2f", estimatedCost));
    sb.append(", rows=").append(estimatedCardinality);
    sb.append("]\n");
    return sb.toString();
  }
}
