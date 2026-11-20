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
package com.arcadedb.query.opencypher.executor.steps;

import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.graph.Edge;
import com.arcadedb.query.opencypher.ast.Direction;
import com.arcadedb.query.opencypher.executor.EdgeIndexSeedPlanner;
import com.arcadedb.query.opencypher.executor.IdSeekValues;
import com.arcadedb.query.opencypher.executor.operators.InListValues;
import com.arcadedb.query.sql.executor.AbstractExecutionStep;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.executor.WorkGuard;
import com.arcadedb.schema.DocumentType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Leading (seed) execution step that drives a single-hop relationship pattern from the ids its {@code WHERE} clause pins the
 * relationship to - {@code elementId(r) = $id}, {@code id(r) IN $ids} - instead of scanning every vertex and walking every edge list
 * (issue #12134). Each edge is fetched by its RID, checked against the pattern's types, and bound with both endpoints exactly as
 * {@link MatchEdgeByIndexStep} binds the edges an index finds.
 * <p>
 * Planned only over relationship types that hold no lightweight edge: a lightweight edge has no record, so no RID fetches it, and
 * a walk is the only way to find one. The {@code WHERE} clause is still evaluated above this step.
 */
public class MatchEdgeByIdStep extends AbstractExecutionStep {
  private final InListValues                        ids;
  /** The relationship types of the pattern, any of which an edge must be (or extend); empty for any type. */
  private final List<String>                        edgeTypes;
  private final Direction                           direction;
  private final String                              sourceVariable;
  private final String                              relationshipVariable; // may be null when the edge is anonymous
  private final String                              targetVariable;
  private final boolean                             selfLoopOnly;
  private final EdgeIndexSeedPlanner.EndpointFilter sourceFilter;
  private final EdgeIndexSeedPlanner.EndpointFilter targetFilter;
  private final Map<String, Object>                 relationshipProperties;

  public MatchEdgeByIdStep(final InListValues ids, final List<String> edgeTypes, final Direction direction,
      final String sourceVariable, final String relationshipVariable, final String targetVariable,
      final EdgeIndexSeedPlanner.EndpointFilter sourceFilter, final EdgeIndexSeedPlanner.EndpointFilter targetFilter,
      final Map<String, Object> relationshipProperties, final CommandContext context) {
    super(context);
    this.ids = ids;
    this.edgeTypes = edgeTypes == null ? List.of() : edgeTypes;
    this.direction = direction;
    this.sourceVariable = sourceVariable;
    this.relationshipVariable = relationshipVariable;
    this.targetVariable = targetVariable;
    this.selfLoopOnly = sourceVariable != null && sourceVariable.equals(targetVariable);
    this.sourceFilter = sourceFilter != null ? sourceFilter : EdgeIndexSeedPlanner.EndpointFilter.NONE;
    this.targetFilter = targetFilter != null ? targetFilter : EdgeIndexSeedPlanner.EndpointFilter.NONE;
    this.relationshipProperties = relationshipProperties == null || relationshipProperties.isEmpty() ? null : relationshipProperties;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    final WorkGuard guard = WorkGuard.forCommandDeadline(context);

    return new ResultSet() {
      private List<RID>          rids;
      private int                ridIndex    = 0;
      private final List<Result> buffer      = new ArrayList<>();
      private int                bufferIndex = 0;

      @Override
      public boolean hasNext() {
        if (bufferIndex < buffer.size())
          return true;
        if (rids == null)
          rids = IdSeekValues.resolve(ids, context);
        if (ridIndex >= rids.size())
          return false;
        fetchMore(nRecords > 0 ? nRecords : 100);
        return bufferIndex < buffer.size();
      }

      @Override
      public Result next() {
        if (!hasNext())
          throw new NoSuchElementException();
        return buffer.get(bufferIndex++);
      }

      private void fetchMore(final int n) {
        buffer.clear();
        bufferIndex = 0;
        while (buffer.size() < n && ridIndex < rids.size()) {
          guard.check();
          final long begin = context.isProfiling() ? System.nanoTime() : 0;
          try {
            if (context.isProfiling())
              rowCount++;

            final RID rid = rids.get(ridIndex++);
            final Record record;
            try {
              record = context.getDatabase().lookupByRID(rid, true);
            } catch (final RecordNotFoundException e) {
              // no record at that address: no relationship has that id
              continue;
            }
            if (!(record instanceof Edge edge) || !isOfPatternType(edge.getType()))
              continue;

            final ResultInternal result = MatchEdgeByIndexStep.bindEdge(edge, direction, sourceVariable, relationshipVariable,
                targetVariable, selfLoopOnly, relationshipProperties, sourceFilter, targetFilter, context);
            if (result != null)
              buffer.add(result);
          } finally {
            if (context.isProfiling())
              cost += System.nanoTime() - begin;
          }
        }
      }

      @Override
      public void close() {
        MatchEdgeByIdStep.this.close();
      }
    };
  }

  /** A relationship pattern matches its types and their subtypes; with no type, any relationship. */
  private boolean isOfPatternType(final DocumentType type) {
    if (edgeTypes.isEmpty())
      return true;
    for (final String edgeType : edgeTypes)
      if (type.instanceOf(edgeType))
        return true;
    return false;
  }

  /** {@code MATCH EDGE BY ID (a)-[r:T]->(b) [ids: ...]}. Shared with the cost-based plan's {@code EdgeIndexSeek}, which runs this step. */
  public static String describe(final InListValues ids, final List<String> edgeTypes, final Direction direction,
      final String sourceVariable, final String relationshipVariable, final String targetVariable,
      final EdgeIndexSeedPlanner.EndpointFilter sourceFilter, final EdgeIndexSeedPlanner.EndpointFilter targetFilter) {
    final StringBuilder builder = new StringBuilder("MATCH EDGE BY ID ");
    builder.append("(").append(sourceVariable).append(MatchEdgeByIndexStep.labelsOf(sourceFilter)).append(")");
    final String arrowRel = relationshipVariable != null && !relationshipVariable.isEmpty()
        && !relationshipVariable.startsWith("  ") ? relationshipVariable : "";
    final String types = edgeTypes.isEmpty() ? "" : ":" + String.join("|", edgeTypes);
    if (direction == Direction.IN)
      builder.append("<-[").append(arrowRel).append(types).append("]-");
    else
      builder.append("-[").append(arrowRel).append(types).append("]->");
    builder.append("(").append(targetVariable).append(MatchEdgeByIndexStep.labelsOf(targetFilter)).append(")");
    builder.append(" [ids: ").append(ids).append("]");
    return builder.toString();
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final StringBuilder builder = new StringBuilder();
    builder.append("  ".repeat(Math.max(0, depth * indent)));
    builder.append("+ ").append(describe(ids, edgeTypes, direction, sourceVariable, relationshipVariable, targetVariable,
        sourceFilter, targetFilter));
    if (context.isProfiling()) {
      builder.append(" (").append(getCostFormatted());
      if (rowCount > 0)
        builder.append(", ").append(getRowCountFormatted());
      builder.append(")");
    }
    return builder.toString();
  }
}
