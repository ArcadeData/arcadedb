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

import com.arcadedb.database.RID;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.GAVVertex;
import com.arcadedb.graph.GraphTraversalProvider;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.ast.Direction;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.NoSuchElementException;

/**
 * CSR-backed expand operator that uses a {@link GraphTraversalProvider} for O(1) neighbor lookups
 * instead of OLTP edge linked list traversal.
 * <p>
 * This operator is selected by the optimizer when:
 * <ul>
 *   <li>A ready {@link GraphTraversalProvider} covers the required edge types</li>
 *   <li>The edge variable is not captured (CSR doesn't store edge objects)</li>
 * </ul>
 * <p>
 * Performance: neighbor lookup is a direct array slice (CSR) vs O(degree) linked list scan (OLTP).
 * Target vertices are loaded by RID, which is a direct page access.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class GAVExpandAll extends AbstractPhysicalOperator {
  private final GraphTraversalProvider provider;
  private final String sourceVariable;
  private final String targetVariable;
  private final Direction direction;
  private final String[] edgeTypes;
  private String targetLabel;
  private boolean deferTargetLoad;

  public GAVExpandAll(final PhysicalOperator child, final GraphTraversalProvider provider,
                     final String sourceVariable, final String targetVariable,
                     final Direction direction, final String[] edgeTypes,
                     final double estimatedCost, final long estimatedCardinality) {
    super(child, estimatedCost, estimatedCardinality);
    this.provider = provider;
    this.sourceVariable = sourceVariable;
    this.targetVariable = targetVariable;
    this.direction = direction;
    this.edgeTypes = edgeTypes;
  }

  public void setTargetLabel(final String targetLabel) {
    this.targetLabel = targetLabel;
  }

  public String getTargetLabel() {
    return targetLabel;
  }

  /**
   * When true, target vertices are stored as {@link GAVVertex} instead of loading
   * from OLTP. This avoids expensive lookupByRID for intermediate hops where vertex
   * properties are not accessed.
   */
  public void setDeferTargetLoad(final boolean deferTargetLoad) {
    this.deferTargetLoad = deferTargetLoad;
  }

  @Override
  public ResultSet execute(final CommandContext context, final int nRecords) {
    final ResultSet inputResults = child.execute(context, nRecords);

    return new ResultSet() {
      private Result currentInputResult = null;
      private int[] neighborIds = null;
      private int neighborIdx = 0;
      // OLTP fallback iterator for vertices not present in the GAV mapping
      private Iterator<Edge> oltpFallbackEdges = null;
      private final List<Result> buffer = new ArrayList<>();
      private int bufferIndex = 0;
      private boolean finished = false;

      @Override
      public boolean hasNext() {
        if (bufferIndex < buffer.size())
          return true;
        if (finished)
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

        while (buffer.size() < n) {
          // OLTP fallback path: drain edges for vertices not in the GAV mapping
          if (oltpFallbackEdges != null) {
            if (oltpFallbackEdges.hasNext()) {
              final Edge edge = oltpFallbackEdges.next();
              final Vertex sourceVertex = currentInputResult.getProperty(sourceVariable);
              final Vertex targetVertex = getTargetVertex(edge, sourceVertex);
              if (targetLabel != null && !targetVertex.getType().instanceOf(targetLabel))
                continue;
              addResultWithTarget(targetVertex);
              continue;
            }
            oltpFallbackEdges = null;
          }

          // If we've exhausted neighbors for current input, get next input
          if (neighborIds == null || neighborIdx >= neighborIds.length) {
            if (!inputResults.hasNext()) {
              finished = true;
              break;
            }

            currentInputResult = inputResults.next();
            final Object sourceObj = currentInputResult.getProperty(sourceVariable);
            if (sourceObj == null) {
              neighborIds = null;
              continue;
            }

            // CSR lookup: accept both GAVVertex and Vertex as source
            final int nodeId;
            if (sourceObj instanceof GAVVertex)
              nodeId = ((GAVVertex) sourceObj).getNodeId();
            else if (sourceObj instanceof Vertex)
              nodeId = provider.getNodeId(((Vertex) sourceObj).getIdentity());
            else {
              neighborIds = null;
              continue;
            }

            if (nodeId < 0) {
              // Vertex not in GAV mapping (created after last build) — fall back to OLTP
              if (sourceObj instanceof Vertex) {
                final Vertex.DIRECTION arcadeDirection = direction.toArcadeDirection();
                oltpFallbackEdges = ((Vertex) sourceObj).getEdges(arcadeDirection, edgeTypes).iterator();
              }
              neighborIds = null;
              continue;
            }

            final Vertex.DIRECTION arcadeDirection = direction.toArcadeDirection();
            neighborIds = provider.getNeighborIds(nodeId, arcadeDirection, edgeTypes);
            neighborIdx = 0;
          }

          // Produce target vertex from neighbor ID
          if (neighborIdx < neighborIds.length) {
            final int targetNodeId = neighborIds[neighborIdx++];
            final RID targetRID = provider.getRID(targetNodeId);
            if (targetRID == null)
              continue; // stale node ID — vertex deleted since last CSR build

            if (deferTargetLoad) {
              // Deferred mode: store lightweight reference, skip OLTP load
              if (targetLabel != null) {
                final String typeName = context.getDatabase().getSchema().getTypeByBucketId(targetRID.getBucketId()).getName();
                if (!targetLabel.equals(typeName))
                  continue;
              }
              addResultWithReference(new GAVVertex(targetRID, targetNodeId, provider, context.getDatabase()));
            } else {
              // Full mode: load vertex from OLTP
              final Vertex targetVertex;
              try {
                final var record = context.getDatabase().lookupByRID(targetRID, true);
                if (!(record instanceof Vertex))
                  continue;
                targetVertex = (Vertex) record;
              } catch (final RecordNotFoundException e) {
                continue;
              }

              if (targetLabel != null && !targetVertex.getType().instanceOf(targetLabel))
                continue;

              addResultWithTarget(targetVertex);
            }
          }
        }
      }

      private void addResultWithTarget(final Vertex targetVertex) {
        final ResultInternal result = new ResultInternal();
        for (final String prop : currentInputResult.getPropertyNames())
          result.setProperty(prop, currentInputResult.getProperty(prop));
        if (targetVariable != null)
          result.setProperty(targetVariable, targetVertex);
        buffer.add(result);
      }

      private void addResultWithReference(final GAVVertex ref) {
        final ResultInternal result = new ResultInternal();
        for (final String prop : currentInputResult.getPropertyNames())
          result.setProperty(prop, currentInputResult.getProperty(prop));
        if (targetVariable != null)
          result.setProperty(targetVariable, ref);
        buffer.add(result);
      }

      private Vertex getTargetVertex(final Edge edge, final Vertex sourceVertex) {
        final Vertex out = edge.getOutVertex();
        final Vertex in = edge.getInVertex();
        if (direction == Direction.OUT)
          return in;
        if (direction == Direction.IN)
          return out;
        // BOTH — return the vertex that's not the source
        return out.getIdentity().equals(sourceVertex.getIdentity()) ? in : out;
      }

      @Override
      public void close() {
        inputResults.close();
      }
    };
  }

  @Override
  public String getOperatorType() {
    return "GAVExpandAll";
  }

  @Override
  public String explain(final int depth) {
    final StringBuilder sb = new StringBuilder();
    final String indent = getIndent(depth);

    sb.append(indent).append("+ GAVExpandAll");
    sb.append("(").append(sourceVariable).append(")-[");
    if (edgeTypes != null && edgeTypes.length > 0)
      sb.append(":").append(String.join("|", edgeTypes));
    sb.append("]-");
    sb.append(direction == Direction.OUT ? ">" : direction == Direction.IN ? "<" : "");
    sb.append("(").append(targetVariable);
    if (targetLabel != null)
      sb.append(":").append(targetLabel);
    sb.append(")");
    sb.append(" [provider=").append(provider.getName());
    sb.append(", cost=").append(String.format(Locale.US, "%.2f", estimatedCost));
    sb.append(", rows=").append(estimatedCardinality);
    sb.append("]\n");

    if (child != null)
      sb.append(child.explain(depth + 1));

    return sb.toString();
  }

  public GraphTraversalProvider getProvider() {
    return provider;
  }

  public String getSourceVariable() {
    return sourceVariable;
  }

  public String getTargetVariable() {
    return targetVariable;
  }

  public Direction getDirection() {
    return direction;
  }

  public String[] getEdgeTypes() {
    return edgeTypes;
  }
}
