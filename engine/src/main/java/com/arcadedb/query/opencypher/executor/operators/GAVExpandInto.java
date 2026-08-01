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

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.GhostEdgeReporter;
import com.arcadedb.graph.GraphTraversalProvider;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.VertexInternal;
import com.arcadedb.query.opencypher.ast.Direction;
import com.arcadedb.query.opencypher.executor.SelfLoops;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * CSR-backed ExpandInto that uses binary search on sorted CSR arrays for O(log(degree))
 * lookups, without loading any edge objects or traversing linked lists.
 * <p>
 * A pattern relationship matches once per relationship, so the pair's row is repeated once per edge
 * joining it - the CSR holds one adjacency entry per edge, so the equal range around the search hit
 * gives that multiplicity without materialising anything (issue #5663).
 * <p>
 * Selected when both source and target are bound, no edge object is needed - neither captured by the
 * query nor required to enforce relationship uniqueness against another hop of the same MATCH clause,
 * which adjacency ids cannot answer - and a matching {@link GraphTraversalProvider} is available.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class GAVExpandInto extends AbstractPhysicalOperator {
  private final GraphTraversalProvider provider;
  private final String sourceVariable;
  private final String targetVariable;
  private final Direction direction;
  private final String[] edgeTypes;

  public GAVExpandInto(final PhysicalOperator child, final GraphTraversalProvider provider,
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

  @Override
  public ResultSet execute(final CommandContext context, final int nRecords) {
    final ResultSet inputResults = child.execute(context, nRecords);

    return new ResultSet() {
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

        while (buffer.size() < n && inputResults.hasNext()) {
          final Result inputResult = inputResults.next();

          final Vertex sourceVertex = inputResult.getProperty(sourceVariable);
          final Vertex targetVertex = inputResult.getProperty(targetVariable);
          if (sourceVertex == null || targetVertex == null)
            continue;

          // CSR multiplicity lookup: O(log(degree)) binary search plus the equal range
          final int srcId = provider.getNodeId(sourceVertex.getIdentity());
          final int tgtId = provider.getNodeId(targetVertex.getIdentity());

          // One or both vertices not in the GAV mapping, or a provider that cannot state the
          // multiplicity exactly (a negative answer) — fall back to the OLTP edge list
          long connectingEdges = srcId < 0 || tgtId < 0 ?
              -1 : provider.countEdgesBetween(srcId, tgtId, direction.toArcadeDirection(), edgeTypes);
          if (connectingEdges < 0)
            connectingEdges = countConnectingOLTP(sourceVertex, targetVertex);

          // One row per relationship joining the pair: a pattern hop is an expansion, not a
          // connectivity test, so parallel edges each contribute a walk. The row's property names
          // do not change between the copies, so the set is walked once however many rows it feeds.
          if (connectingEdges > 0) {
            final Set<String> properties = inputResult.getPropertyNames();
            for (long i = 0; i < connectingEdges; i++) {
              final ResultInternal result = new ResultInternal();
              for (final String prop : properties)
                result.setProperty(prop, inputResult.getProperty(prop));
              buffer.add(result);
            }
          }
        }

        if (!inputResults.hasNext())
          finished = true;
      }

      /**
       * OLTP fallback: counts the relationships joining the pair by iterating edges, for when one or
       * both vertices are not present in the GAV mapping (created after the last build), or when the
       * view cannot state the multiplicity exactly.
       * <p>
       * It counts every connecting edge without checking any against the relationship variables an
       * earlier hop of the same MATCH clause bound - which is correct only because
       * {@code CypherOptimizer.createExpandIntoOperator} selects this operator solely for a hop that
       * has no such variable to check against and no edge to track. That gate is what makes the
       * question moot here; widen it and this count has to start asking it, which it cannot do,
       * because the operator never binds the edges it counts.
       */
      private long countConnectingOLTP(final Vertex source, final Vertex target) {
        final Vertex.DIRECTION arcadeDirection = direction.toArcadeDirection();
        Iterator<Edge> edges = candidateEdges(source, target, arcadeDirection);
        if (arcadeDirection == Vertex.DIRECTION.BOTH)
          // both adjacency lists are walked, and a self-loop sits in each of them
          edges = SelfLoops.deduplicatingEdges(edges);

        long count = 0;
        while (edges.hasNext()) {
          final Edge edge = edges.next();
          try {
            if (arcadeDirection == Vertex.DIRECTION.BOTH) {
              // source can be either endpoint, so check both sides
              if (edge.getOutVertex().getIdentity().equals(target.getIdentity())
                  || edge.getInVertex().getIdentity().equals(target.getIdentity()))
                ++count;
            } else {
              final Vertex other = arcadeDirection == Vertex.DIRECTION.OUT ? edge.getInVertex() : edge.getOutVertex();
              if (other.getIdentity().equals(target.getIdentity()))
                ++count;
            }
          } catch (final RecordNotFoundException e) {
            GhostEdgeReporter.reportSkipped(e);
          }
        }
        return count;
      }

      /**
       * Narrows the fallback to the edges that actually reach the target, using the neighbour pointer
       * stored in the edge segment. The endpoint check below still runs and still reports ghosts, but
       * it now runs on the handful of candidate edges instead of on the source's whole edge list -
       * which is what keeps this fallback usable when the source is a super-node.
       * <p>
       * That narrows what the probe incidentally reports: a ghost edge reaching the target is still
       * loaded and reported, but one pointing elsewhere in the source's list is no longer visited, so
       * it goes unnoticed here. This is a connectivity probe, not a ghost scanner - CHECK DATABASE is
       * what sweeps a whole edge list.
       */
      private Iterator<Edge> candidateEdges(final Vertex source, final Vertex target, final Vertex.DIRECTION arcadeDirection) {
        if (source instanceof VertexInternal internalSource)
          return ((DatabaseInternal) source.getDatabase()).getGraphEngine()
              .getEdgesConnectedTo(internalSource, arcadeDirection, target.getIdentity(), edgeTypes);
        return source.getEdges(arcadeDirection, edgeTypes).iterator();
      }

      @Override
      public void close() {
        inputResults.close();
      }
    };
  }

  @Override
  public String getOperatorType() {
    return "GAVExpandInto";
  }

  @Override
  public String explain(final int depth) {
    final StringBuilder sb = new StringBuilder();
    final String indent = getIndent(depth);

    sb.append(indent).append("+ GAVExpandInto");
    sb.append("(").append(sourceVariable).append(")-[");
    if (edgeTypes != null && edgeTypes.length > 0)
      sb.append(":").append(String.join("|", edgeTypes));
    sb.append("]-");
    sb.append(direction == Direction.OUT ? ">" : direction == Direction.IN ? "<" : "");
    sb.append("(").append(targetVariable).append(")");
    sb.append(" [provider=").append(provider.getName());
    sb.append(", cost=").append(String.format(Locale.US, "%.2f", estimatedCost));
    sb.append(", rows=").append(estimatedCardinality);
    sb.append("]\n");

    if (child != null)
      sb.append(child.explain(depth + 1));

    return sb.toString();
  }
}
