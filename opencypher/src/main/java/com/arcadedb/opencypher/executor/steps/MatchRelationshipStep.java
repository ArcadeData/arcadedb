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
package com.arcadedb.opencypher.executor.steps;

import com.arcadedb.exception.TimeoutException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.opencypher.ast.Direction;
import com.arcadedb.opencypher.ast.RelationshipPattern;
import com.arcadedb.query.sql.executor.AbstractExecutionStep;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Execution step for matching relationship patterns.
 * Expands from source vertices to target vertices following relationship patterns.
 * <p>
 * Example: (a)-[r:KNOWS]->(b)
 * - Takes vertices bound to 'a' from previous step
 * - Follows KNOWS relationships in OUT direction
 * - Binds edges to 'r' and target vertices to 'b'
 */
public class MatchRelationshipStep extends AbstractExecutionStep {
  private final String sourceVariable;
  private final String relationshipVariable;
  private final String targetVariable;
  private final RelationshipPattern pattern;

  /**
   * Creates a match relationship step.
   *
   * @param sourceVariable       variable name for source vertex
   * @param relationshipVariable variable name for relationship (can be null)
   * @param targetVariable       variable name for target vertex
   * @param pattern              relationship pattern to match
   * @param context              command context
   */
  public MatchRelationshipStep(final String sourceVariable, final String relationshipVariable, final String targetVariable,
      final RelationshipPattern pattern, final CommandContext context) {
    super(context);
    this.sourceVariable = sourceVariable;
    this.relationshipVariable = relationshipVariable;
    this.targetVariable = targetVariable;
    this.pattern = pattern;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    checkForPrevious("MatchRelationshipStep requires a previous step");

    return new ResultSet() {
      private Result lastResult = null;
      private Iterator<Edge> currentEdges = null;
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

        // Fetch more results
        fetchMore(nRecords);
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
          // Get edges from current vertex
          if (currentEdges != null && currentEdges.hasNext()) {
            final Edge edge = currentEdges.next();
            final Vertex targetVertex = getTargetVertex(edge, (Vertex) lastResult.getProperty(sourceVariable));

            // Filter by target type if specified
            if (pattern.hasTypes() && !matchesEdgeType(edge)) {
              continue;
            }

            // Create result with edge and target vertex
            final ResultInternal result = new ResultInternal();

            // Copy all properties from previous result
            for (final String prop : lastResult.getPropertyNames()) {
              result.setProperty(prop, lastResult.getProperty(prop));
            }

            // Add relationship binding if variable is specified
            if (relationshipVariable != null && !relationshipVariable.isEmpty()) {
              result.setProperty(relationshipVariable, edge);
            }

            // Add target vertex binding
            result.setProperty(targetVariable, targetVertex);

            buffer.add(result);
          } else {
            // Get next source vertex from previous step
            final ResultSet prevResults = prev.syncPull(context, 1);
            if (!prevResults.hasNext()) {
              finished = true;
              break;
            }

            lastResult = prevResults.next();
            final Object sourceObj = lastResult.getProperty(sourceVariable);

            if (sourceObj instanceof Vertex) {
              final Vertex sourceVertex = (Vertex) sourceObj;
              currentEdges = getEdges(sourceVertex);
            } else {
              // Source is not a vertex, skip
              currentEdges = null;
            }
          }
        }
      }

      @Override
      public void close() {
        MatchRelationshipStep.this.close();
      }
    };
  }

  /**
   * Gets edges from a vertex based on the relationship pattern.
   */
  private Iterator<Edge> getEdges(final Vertex vertex) {
    final Direction direction = pattern.getDirection();
    final String[] types = pattern.hasTypes() ?
        pattern.getTypes().toArray(new String[0]) :
        null;

    if (types == null || types.length == 0) {
      return vertex.getEdges(direction.toArcadeDirection()).iterator();
    } else {
      return vertex.getEdges(direction.toArcadeDirection(), types).iterator();
    }
  }

  /**
   * Gets the target vertex from an edge based on direction.
   */
  private Vertex getTargetVertex(final Edge edge, final Vertex sourceVertex) {
    final Vertex out = edge.getOutVertex();
    final Vertex in = edge.getInVertex();

    // Determine which vertex is the target based on direction
    if (pattern.getDirection() == Direction.OUT) {
      return in;
    } else if (pattern.getDirection() == Direction.IN) {
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

  /**
   * Checks if an edge matches the type filter.
   */
  private boolean matchesEdgeType(final Edge edge) {
    if (!pattern.hasTypes()) {
      return true;
    }

    final String edgeType = edge.getTypeName();
    for (final String type : pattern.getTypes()) {
      if (type.equals(edgeType)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final StringBuilder builder = new StringBuilder();
    final String ind = getIndent(depth, indent);
    builder.append(ind);
    builder.append("+ MATCH RELATIONSHIP ");
    builder.append("(").append(sourceVariable).append(")");
    builder.append(pattern);
    builder.append("(").append(targetVariable).append(")");
    if (context.isProfiling()) {
      builder.append(" (").append(getCostFormatted()).append(")");
    }
    return builder.toString();
  }

  private static String getIndent(final int depth, final int indent) {
    return "  ".repeat(Math.max(0, depth * indent));
  }
}
