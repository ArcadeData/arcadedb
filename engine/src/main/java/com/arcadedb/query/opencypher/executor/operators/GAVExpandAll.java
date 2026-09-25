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
import com.arcadedb.graph.GhostEdgeReporter;
import com.arcadedb.graph.GraphTraversalProvider;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.ast.Direction;
import com.arcadedb.query.opencypher.executor.SelfLoops;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.executor.WorkGuard;
import com.arcadedb.schema.DocumentType;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.NoSuchElementException;
import java.util.Set;

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
 * A hop whose relationship may collide with another one of its MATCH clause walks one adjacency slice per edge type and
 * orientation and binds a {@link GAVEdgeRef} for the relationship it took, so that neither it nor a later hop of the
 * clause binds that relationship again (issue #8394).
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
  // Relationship uniqueness (#8394): where this hop binds the label of the relationship it walked, and the variables
  // under which the preceding hops of the same MATCH clause bound theirs. Null when the hop cannot collide.
  private String edgeTrackingVar;
  private Set<String> sameClausePrecedingRelVars;

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

  /**
   * Makes this hop bind a {@link GAVEdgeRef} under {@code edgeTrackingVar} and refuse the relationships already bound
   * under {@code sameClausePrecedingRelVars}.
   */
  public void setEdgeTracking(final String edgeTrackingVar, final Set<String> sameClausePrecedingRelVars) {
    this.edgeTrackingVar = edgeTrackingVar;
    this.sameClausePrecedingRelVars = sameClausePrecedingRelVars;
  }

  public String getEdgeTrackingVar() {
    return edgeTrackingVar;
  }

  public Set<String> getSameClausePrecedingRelVars() {
    return sameClausePrecedingRelVars;
  }

  @Override
  public ResultSet execute(final CommandContext context, final int nRecords) {
    // Bounds this operator's row loop by the command deadline - see WorkGuard for why between-batches is
    // not enough (issue #6266).
    final WorkGuard guard = WorkGuard.forCommandDeadline(context);
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
      // Tracked mode (#8394): the current source's adjacency, one slice per (edge type, orientation)
      private final String[] trackedTypes = edgeTrackingVar != null ?
          GAVEdgeRef.trackedEdgeTypes(context.getDatabase(), edgeTypes) : null;
      private GAVEdgeRef[] boundRefs;
      private int sourceNodeId;
      private RID sourceRID;
      private int[][] slices;
      private String[] sliceTypes;
      private boolean[] sliceOutgoing;
      private int sliceCount;
      private int sliceIdx;
      private int entryIdx;
      private List<GAVEdgeRef> fallbackWalked;

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
        if (trackedTypes != null) {
          fetchMoreTracked(n);
          return;
        }

        while (buffer.size() < n) {
          guard.check();
          // OLTP fallback path: drain edges for vertices not in the GAV mapping
          if (oltpFallbackEdges != null) {
            if (oltpFallbackEdges.hasNext()) {
              final Edge edge = oltpFallbackEdges.next();
              final Vertex sourceVertex = currentInputResult.getProperty(sourceVariable);
              final Vertex targetVertex;
              try {
                targetVertex = getTargetVertex(edge, sourceVertex);
              } catch (final RecordNotFoundException e) {
                GhostEdgeReporter.reportSkipped(e);
                continue;
              }
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
                final Iterator<Edge> edges = ((Vertex) sourceObj).getEdges(arcadeDirection, edgeTypes).iterator();
                // A self-loop sits in both lists of an undirected walk: one row per relationship, as ExpandAll yields
                oltpFallbackEdges = direction == Direction.BOTH ? SelfLoops.deduplicatingEdges(edges) : edges;
              }
              neighborIds = null;
              continue;
            }

            final Vertex.DIRECTION arcadeDirection = direction.toArcadeDirection();
            neighborIds = provider.getNeighborIds(nodeId, arcadeDirection, edgeTypes);
            // An undirected hop yields each relationship once; a self-loop sits in both the outgoing
            // and the incoming list, so half of those entries are the same relationship seen twice.
            if (direction == Direction.BOTH)
              neighborIds = SelfLoops.deduplicate(neighborIds, nodeId);
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
                // Polymorphic, as the full-load branch below: a label matches its sub-types too (#8377)
                final DocumentType targetType = context.getDatabase().getSchema().getTypeByBucketId(targetRID.getBucketId());
                if (targetType == null || !targetType.instanceOf(targetLabel))
                  continue;
              }
              addResultWithReference(new GAVVertex(targetRID, targetNodeId, provider, context.getDatabase()));
            } else {
              // Full mode: use GAVVertex (lazy-loading proxy) instead of eager OLTP load
              final GAVVertex targetVertex = new GAVVertex(targetRID, targetNodeId, provider, context.getDatabase());

              if (targetLabel != null && !targetVertex.getType().instanceOf(targetLabel))
                continue;

              addResultWithTarget(targetVertex);
            }
          }
        }
      }

      /**
       * The tracked expansion: walks the source's adjacency one (edge type, orientation) slice at a time, so every
       * entry names one relationship whose label can be compared with the ones the row already binds.
       */
      private void fetchMoreTracked(final int n) {
        while (buffer.size() < n) {
          guard.check();
          if (oltpFallbackEdges != null) {
            if (oltpFallbackEdges.hasNext())
              walkFallbackEdge(oltpFallbackEdges.next());
            else
              oltpFallbackEdges = null;
            continue;
          }

          if (sliceIdx >= sliceCount) {
            if (!inputResults.hasNext()) {
              finished = true;
              break;
            }
            nextTrackedInput(inputResults.next());
            continue;
          }

          final int[] slice = slices[sliceIdx];
          if (entryIdx >= slice.length) {
            ++sliceIdx;
            entryIdx = 0;
            continue;
          }

          final int index = entryIdx++;
          final int targetNodeId = slice[index];
          final boolean outgoing = sliceOutgoing[sliceIdx];
          // Undirected: a self-loop sits in both lists of its vertex, and the outgoing one already yielded it
          if (!outgoing && direction == Direction.BOTH && targetNodeId == sourceNodeId)
            continue;

          final RID targetRID = provider.getRID(targetNodeId);
          if (targetRID == null)
            continue; // stale node ID — vertex deleted since last CSR build

          final String type = sliceTypes[sliceIdx];
          final RID out = outgoing ? sourceRID : targetRID;
          final RID in = outgoing ? targetRID : sourceRID;
          if (GAVEdgeRef.conflicts(boundRefs, type, out, in, slice, index))
            continue;

          emitNeighbor(targetNodeId, targetRID, GAVEdgeRef.inSlice(type, out, in, slice, index));
        }
      }

      private void nextTrackedInput(final Result input) {
        currentInputResult = input;
        sliceCount = 0;
        sliceIdx = 0;
        entryIdx = 0;

        final Object sourceObj = input.getProperty(sourceVariable);
        final int nodeId;
        if (sourceObj instanceof GAVVertex gavVertex)
          nodeId = gavVertex.getNodeId();
        else if (sourceObj instanceof Vertex vertex)
          nodeId = provider.getNodeId(vertex.getIdentity());
        else
          return;

        boundRefs = GAVEdgeRef.collect(input, sameClausePrecedingRelVars);

        if (nodeId < 0) {
          // Vertex not in GAV mapping (created after last build) — fall back to OLTP
          if (sourceObj instanceof Vertex vertex) {
            final Iterator<Edge> edges = vertex.getEdges(direction.toArcadeDirection(), edgeTypes).iterator();
            oltpFallbackEdges = direction == Direction.BOTH ? SelfLoops.deduplicatingEdges(edges) : edges;
            fallbackWalked = null;
          }
          return;
        }

        sourceNodeId = nodeId;
        sourceRID = ((Vertex) sourceObj).getIdentity();
        final int perType = direction == Direction.BOTH ? 2 : 1;
        if (slices == null || slices.length < trackedTypes.length * perType) {
          slices = new int[trackedTypes.length * perType][];
          sliceTypes = new String[slices.length];
          sliceOutgoing = new boolean[slices.length];
        }
        for (final String type : trackedTypes) {
          if (direction != Direction.IN)
            addSlice(type, true, provider.getNeighborIds(nodeId, Vertex.DIRECTION.OUT, type));
          if (direction != Direction.OUT)
            addSlice(type, false, provider.getNeighborIds(nodeId, Vertex.DIRECTION.IN, type));
        }
      }

      private void addSlice(final String type, final boolean outgoing, final int[] neighbors) {
        if (neighbors == null || neighbors.length == 0)
          return;
        slices[sliceCount] = neighbors;
        sliceTypes[sliceCount] = type;
        sliceOutgoing[sliceCount] = outgoing;
        ++sliceCount;
      }

      /**
       * A source the view does not map is expanded on its edge records. The relationship is still bound as a label,
       * ranked among the parallel ones this walk has met so far, since every other hop of the clause binds labels.
       */
      private void walkFallbackEdge(final Edge edge) {
        final Vertex sourceVertex = currentInputResult.getProperty(sourceVariable);
        final String type = edge.getTypeName();
        final RID out = edge.getOut();
        final RID in = edge.getIn();
        if (fallbackWalked == null)
          fallbackWalked = new ArrayList<>();
        int occurrence = 0;
        for (final GAVEdgeRef walked : fallbackWalked)
          if (walked.sameEndpoints(type, out, in))
            ++occurrence;
        final GAVEdgeRef ref = GAVEdgeRef.ranked(type, out, in, occurrence);
        fallbackWalked.add(ref);
        if (GAVEdgeRef.conflicts(boundRefs, type, out, in, occurrence))
          return;

        final Vertex targetVertex;
        try {
          targetVertex = getTargetVertex(edge, sourceVertex);
        } catch (final RecordNotFoundException e) {
          GhostEdgeReporter.reportSkipped(e);
          return;
        }
        if (targetLabel != null && !targetVertex.getType().instanceOf(targetLabel))
          return;
        addResult(targetVertex, ref);
      }

      private void emitNeighbor(final int targetNodeId, final RID targetRID, final GAVEdgeRef ref) {
        final GAVVertex targetVertex = new GAVVertex(targetRID, targetNodeId, provider, context.getDatabase());
        if (targetLabel != null) {
          if (deferTargetLoad) {
            // Polymorphic, as the full-load path: a label matches its sub-types too (#8377)
            final DocumentType targetType = context.getDatabase().getSchema().getTypeByBucketId(targetRID.getBucketId());
            if (targetType == null || !targetType.instanceOf(targetLabel))
              return;
          } else if (!targetVertex.getType().instanceOf(targetLabel))
            return;
        }
        addResult(targetVertex, ref);
      }

      private void addResultWithTarget(final Vertex targetVertex) {
        addResult(targetVertex, null);
      }

      private void addResultWithReference(final GAVVertex ref) {
        addResult(ref, null);
      }

      private void addResult(final Vertex targetVertex, final GAVEdgeRef edgeRef) {
        final ResultInternal result = new ResultInternal();
        for (final String prop : currentInputResult.getPropertyNames())
          result.setProperty(prop, currentInputResult.getProperty(prop));
        if (targetVariable != null)
          result.setProperty(targetVariable, targetVertex);
        if (edgeRef != null)
          result.setProperty(edgeTrackingVar, edgeRef);
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
    if (edgeTrackingVar != null)
      sb.append(", unique relationships");
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
