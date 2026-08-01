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
import com.arcadedb.database.RID;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.VertexInternal;
import com.arcadedb.query.opencypher.ast.Direction;
import com.arcadedb.query.opencypher.executor.SelfLoops;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.RidHashSet;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Physical operator that walks the relationships between two vertices the plan has already bound.
 * <p>
 * KEY OPTIMIZATION: the edge list of the source is filtered on the neighbour pointer stored beside
 * each edge pointer in the segment ({@code GraphEngine.getEdgesConnectedTo}), so an edge that does
 * not reach the target is rejected by two primitive comparisons instead of a record load. On a hub
 * vertex that is the difference between answering a question about one edge and reading its whole
 * edge list.
 * <p>
 * Example query:
 * MATCH (a:Person {id: 1}), (b:Person {id: 2})
 * MATCH (a)-[r:KNOWS]->(b)
 * RETURN r
 * <p>
 * This is an expansion, not an existence check: a pattern relationship matches once per relationship,
 * so two parallel KNOWS edges between the pair yield two rows - whether or not the pattern names the
 * relationship. Answering it once per input row under-counts every cycle and every multigraph pattern
 * (issue #5663).
 * <p>
 * Cost: O(degree(source)) pointer comparisons plus one record load per emitted row
 * Cardinality: input rows * the number of relationships joining the pair
 */
public class ExpandInto extends AbstractPhysicalOperator {
  private final String    sourceVariable;
  private final String    targetVariable;
  private final String    edgeVariable;
  private final Direction direction;
  private final String[]  edgeTypes;
  // Same-MATCH-clause relationship variable names that were bound before this hop.
  // See ExpandAll for the rationale.
  private Set<String>     sameClausePrecedingRelVars;
  // Synthetic row property name under which to stash this hop's edge when edgeVariable is null but
  // the edge is still needed by later same-clause hops for the uniqueness check.
  private String          edgeTrackingVar;

  public ExpandInto(final PhysicalOperator child, final String sourceVariable,
                    final String targetVariable, final String edgeVariable,
                    final Direction direction, final String[] edgeTypes,
                    final double estimatedCost, final long estimatedCardinality) {
    super(child, estimatedCost, estimatedCardinality);
    this.sourceVariable = sourceVariable;
    this.targetVariable = targetVariable;
    this.edgeVariable = edgeVariable;
    this.direction = direction;
    this.edgeTypes = edgeTypes;
  }

  /**
   * Sets the relationship variables bound earlier in the same MATCH clause as this hop.
   * Used to enforce Cypher relationship uniqueness within the clause.
   */
  public void setSameClausePrecedingRelVars(final Set<String> sameClausePrecedingRelVars) {
    this.sameClausePrecedingRelVars = sameClausePrecedingRelVars;
  }

  public void setEdgeTrackingVar(final String edgeTrackingVar) {
    this.edgeTrackingVar = edgeTrackingVar;
  }

  @Override
  public ResultSet execute(final CommandContext context, final int nRecords) {
    final ResultSet inputResults = child.execute(context, nRecords);

    return new ResultSet() {
      private       Result         currentInputResult       = null;
      private       Iterator<Edge> edgeIterator             = null;
      // Edge RIDs already bound by same-clause preceding rel vars in the current input row.
      // Computed once per input row, queried per edge.
      private       RidHashSet     currentInputUsedEdgeRids = null;
      private final List<Result>   buffer                   = new ArrayList<>();
      private       int            bufferIndex              = 0;
      private       boolean        finished                 = false;

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
          // Move to the next input row once the current pair has no relationship left to yield
          if (edgeIterator == null || !edgeIterator.hasNext()) {
            if (!inputResults.hasNext()) {
              finished = true;
              break;
            }

            currentInputResult = inputResults.next();

            final Vertex sourceVertex = currentInputResult.getProperty(sourceVariable);
            final Vertex targetVertex = currentInputResult.getProperty(targetVariable);

            // Skip if either vertex is null (OPTIONAL MATCH case)
            if (sourceVertex == null || targetVertex == null) {
              edgeIterator = null;
              continue;
            }

            edgeIterator = connectingEdges(sourceVertex, targetVertex);
            currentInputUsedEdgeRids = collectUsedEdgeRids(currentInputResult);
            continue;
          }

          final Edge edge = edgeIterator.next();

          // Cypher path isomorphism: each relationship of a MATCH pattern must be a distinct edge.
          // The set is null when no same-clause rel var is bound, so the check is free.
          if (currentInputUsedEdgeRids != null && currentInputUsedEdgeRids.contains(edge.getIdentity()))
            continue;

          final ResultInternal result = new ResultInternal();

          // Copy all properties from input
          for (final String prop : currentInputResult.getPropertyNames())
            result.setProperty(prop, currentInputResult.getProperty(prop));

          if (edgeVariable != null)
            result.setProperty(edgeVariable, edge);
          else if (edgeTrackingVar != null)
            result.setProperty(edgeTrackingVar, edge);

          buffer.add(result);
        }
      }

      @Override
      public void close() {
        inputResults.close();
      }
    };
  }

  /**
   * Returns the relationships joining the two bound vertices, in the pattern's direction and of the
   * pattern's types, each one exactly once.
   * <p>
   * The engine walks the source's edge segments rejecting on the neighbour pointer, so only the edges
   * that actually reach the target are materialised.
   * <p>
   * The {@link Vertex#getEdges} branch is for a source handle that is not a {@link VertexInternal},
   * which the plan does not produce today - every vertex an operator binds comes from a scan, a seek
   * or an expansion, and all three yield engine vertices - so it exists to keep the operator total
   * against a handle arriving from elsewhere rather than to serve a known caller. It has to compare
   * the far endpoint of each edge, which is the work the neighbour-pointer filter avoids, so it
   * filters lazily: the operator's batching is what bounds how much of a super-node's edge list is
   * ever walked, and collecting into a list first would take that bound away.
   * <p>
   * An undirected hop walks both adjacency lists of the source, which reaches a self-loop twice - the
   * same relationship, not two of them - so the result is de-duplicated by edge identity.
   */
  private Iterator<Edge> connectingEdges(final Vertex source, final Vertex target) {
    final Vertex.DIRECTION arcadeDirection = direction.toArcadeDirection();
    final Iterator<Edge> connecting;

    if (source instanceof VertexInternal internalSource)
      connecting = ((DatabaseInternal) source.getDatabase()).getGraphEngine()
          .getEdgesConnectedTo(internalSource, arcadeDirection, target.getIdentity(), edgeTypes);
    else
      connecting = reachingTarget(source.getEdges(arcadeDirection, edgeTypes).iterator(),
          target.getIdentity(), arcadeDirection);

    return arcadeDirection == Vertex.DIRECTION.BOTH ? SelfLoops.deduplicatingEdges(connecting) : connecting;
  }

  /**
   * Lazily keeps the edges whose far endpoint is {@code target}, one at a time.
   */
  private static Iterator<Edge> reachingTarget(final Iterator<Edge> edges, final RID target,
      final Vertex.DIRECTION direction) {
    return new Iterator<>() {
      private Edge nextEdge = null;

      @Override
      public boolean hasNext() {
        if (nextEdge != null)
          return true;
        while (edges.hasNext()) {
          final Edge candidate = edges.next();
          final boolean reaches = direction == Vertex.DIRECTION.BOTH ?
              candidate.getOut().equals(target) || candidate.getIn().equals(target) :
              (direction == Vertex.DIRECTION.OUT ? candidate.getIn() : candidate.getOut()).equals(target);
          if (reaches) {
            nextEdge = candidate;
            return true;
          }
        }
        return false;
      }

      @Override
      public Edge next() {
        if (!hasNext())
          throw new NoSuchElementException();
        final Edge result = nextEdge;
        nextEdge = null;
        return result;
      }
    };
  }

  /**
   * Collects the RIDs of the edges already bound to same-clause preceding relationship variables in
   * the input row. Returns null when no relevant binding is present, so the per-edge check stays free
   * in the common single-hop case.
   */
  private RidHashSet collectUsedEdgeRids(final Result row) {
    if (sameClausePrecedingRelVars == null || sameClausePrecedingRelVars.isEmpty())
      return null;
    RidHashSet used = null;
    for (final String relVar : sameClausePrecedingRelVars) {
      final Object val = row.getProperty(relVar);
      if (val instanceof Edge edge) {
        if (used == null)
          used = new RidHashSet();
        used.add(edge.getIdentity());
      }
    }
    return used;
  }

  @Override
  public String getOperatorType() {
    return "ExpandInto";
  }

  @Override
  public String explain(final int depth) {
    final StringBuilder sb = new StringBuilder();
    final String indent = getIndent(depth);

    sb.append(indent).append("+ ExpandInto");
    sb.append("(").append(sourceVariable);
    sb.append(")-[");
    if (edgeVariable != null) {
      sb.append(edgeVariable);
    }
    if (edgeTypes != null && edgeTypes.length > 0) {
      sb.append(":").append(String.join("|", edgeTypes));
    }
    sb.append("]-");
    sb.append(direction == Direction.OUT ? ">" : direction == Direction.IN ? "<" : "");
    sb.append("(").append(targetVariable).append(")");
    sb.append(" [cost=").append(String.format(Locale.US, "%.2f", estimatedCost));
    sb.append(", rows=").append(estimatedCardinality);
    sb.append("] ⭐ BOUND-TARGET\n");

    if (child != null) {
      sb.append(child.explain(depth + 1));
    }

    return sb.toString();
  }

  public String getSourceVariable() {
    return sourceVariable;
  }

  public String getTargetVariable() {
    return targetVariable;
  }

  public String getEdgeVariable() {
    return edgeVariable;
  }

  public Direction getDirection() {
    return direction;
  }

  public String[] getEdgeTypes() {
    return edgeTypes;
  }
}
