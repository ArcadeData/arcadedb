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

import com.arcadedb.graph.Edge;
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
 * Physical operator that expands relationships from source vertices to target vertices.
 * This is the standard traversal operator used when the target vertex is not yet known.
 *
 * For each input vertex, this operator:
 * 1. Gets all edges matching the pattern (type, direction)
 * 2. Follows each edge to the target vertex
 * 3. Produces output rows with both edge and target vertex
 *
 * Cost: O(M) where M is the number of matching edges
 * Cardinality: input_rows * average_degree
 */
public class ExpandAll extends AbstractPhysicalOperator {
  private final String sourceVariable;
  private final String edgeVariable;
  private final String targetVariable;
  private final Direction direction;
  private final String[] edgeTypes;

  public ExpandAll(final PhysicalOperator child, final String sourceVariable,
                  final String edgeVariable, final String targetVariable,
                  final Direction direction, final String[] edgeTypes,
                  final double estimatedCost, final long estimatedCardinality) {
    super(child, estimatedCost, estimatedCardinality);
    this.sourceVariable = sourceVariable;
    this.edgeVariable = edgeVariable;
    this.targetVariable = targetVariable;
    this.direction = direction;
    this.edgeTypes = edgeTypes;
  }

  @Override
  public ResultSet execute(final CommandContext context, final int nRecords) {
    final ResultSet inputResults = child.execute(context, nRecords);

    return new ResultSet() {
      private Result currentInputResult = null;
      private Iterator<Edge> edgeIterator = null;
      private final List<Result> buffer = new ArrayList<>();
      private int bufferIndex = 0;
      private boolean finished = false;

      @Override
      public boolean hasNext() {
        if (bufferIndex < buffer.size()) {
          return true;
        }

        if (finished) {
          return false;
        }

        fetchMore(nRecords > 0 ? nRecords : 100);
        return bufferIndex < buffer.size();
      }

      @Override
      public Result next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        return buffer.get(bufferIndex++);
      }

      private void fetchMore(final int n) {
        buffer.clear();
        bufferIndex = 0;

        while (buffer.size() < n) {
          // If we've exhausted edges for current input, get next input
          if (edgeIterator == null || !edgeIterator.hasNext()) {
            if (!inputResults.hasNext()) {
              finished = true;
              break;
            }

            currentInputResult = inputResults.next();
            final Vertex sourceVertex = currentInputResult.getProperty(sourceVariable);

            if (sourceVertex == null) {
              continue; // Skip if source vertex is null (OPTIONAL MATCH)
            }

            // Get edges from source vertex
            final Vertex.DIRECTION arcadeDirection = direction.toArcadeDirection();
            edgeIterator = sourceVertex.getEdges(arcadeDirection, edgeTypes).iterator();
          }

          // Expand edges to target vertices
          if (edgeIterator.hasNext()) {
            final Edge edge = edgeIterator.next();
            final Vertex sourceVertex = currentInputResult.getProperty(sourceVariable);
            final Vertex targetVertex = getTargetVertex(edge, sourceVertex);

            // Copy input result and add edge and target vertex
            final ResultInternal result = new ResultInternal();
            for (final String prop : currentInputResult.getPropertyNames()) {
              result.setProperty(prop, currentInputResult.getProperty(prop));
            }

            if (edgeVariable != null) {
              result.setProperty(edgeVariable, edge);
            }
            if (targetVariable != null) {
              result.setProperty(targetVariable, targetVertex);
            }

            buffer.add(result);
          }
        }
      }

      @Override
      public void close() {
        inputResults.close();
      }
    };
  }

  @Override
  public String getOperatorType() {
    return "ExpandAll";
  }

  @Override
  public String explain(final int depth) {
    final StringBuilder sb = new StringBuilder();
    final String indent = getIndent(depth);

    sb.append(indent).append("+ ExpandAll");
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
    sb.append("]\n");

    if (child != null) {
      sb.append(child.explain(depth + 1));
    }

    return sb.toString();
  }

  public String getSourceVariable() {
    return sourceVariable;
  }

  public String getEdgeVariable() {
    return edgeVariable;
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

  /**
   * Gets the target vertex from an edge based on direction.
   */
  private Vertex getTargetVertex(final Edge edge, final Vertex sourceVertex) {
    final Vertex out = edge.getOutVertex();
    final Vertex in = edge.getInVertex();

    if (direction == Direction.OUT) {
      return in;
    } else if (direction == Direction.IN) {
      return out;
    } else {
      // BOTH direction - return the vertex that's not the source
      if (out.getIdentity().equals(sourceVertex.getIdentity())) {
        return in;
      } else {
        return out;
      }
    }
  }
}
