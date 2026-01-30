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
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.ast.Direction;
import com.arcadedb.query.opencypher.ast.ShortestPathPattern;
import com.arcadedb.query.sql.executor.AbstractExecutionStep;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.function.graph.SQLFunctionShortestPath;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Execution step for shortestPath() and allShortestPaths() patterns in MATCH clauses.
 * <p>
 * Handles patterns like:
 * - MATCH p = shortestPath((a)-[:KNOWS*]-(b))
 * - MATCH p = allShortestPaths((a)-[:KNOWS*]-(b))
 * <p>
 * Uses the existing SQLFunctionShortestPath for path computation.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class ShortestPathStep extends AbstractExecutionStep {
  private final String sourceVariable;
  private final String targetVariable;
  private final String pathVariable;
  private final ShortestPathPattern pattern;

  /**
   * Creates a shortest path step.
   *
   * @param sourceVariable variable name for source vertex
   * @param targetVariable variable name for target vertex
   * @param pathVariable   variable name for the path result (can be null)
   * @param pattern        the shortest path pattern
   * @param context        command context
   */
  public ShortestPathStep(final String sourceVariable, final String targetVariable, final String pathVariable,
      final ShortestPathPattern pattern, final CommandContext context) {
    super(context);
    this.sourceVariable = sourceVariable;
    this.targetVariable = targetVariable;
    this.pathVariable = pathVariable;
    this.pattern = pattern;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    checkForPrevious("ShortestPathStep requires a previous step");

    return new ResultSet() {
      private ResultSet prevResults = null;
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
          if (prevResults == null) {
            prevResults = prev.syncPull(context, nRecords);
          }

          if (!prevResults.hasNext()) {
            finished = true;
            break;
          }

          final Result inputResult = prevResults.next();

          // Get source and target vertices from bound variables
          final Object sourceObj = inputResult.getProperty(sourceVariable);
          final Object targetObj = inputResult.getProperty(targetVariable);

          if (!(sourceObj instanceof Vertex) || !(targetObj instanceof Vertex)) {
            // If source or target is not a vertex, skip this result
            continue;
          }

          final Vertex sourceVertex = (Vertex) sourceObj;
          final Vertex targetVertex = (Vertex) targetObj;

          // Compute the shortest path
          final List<RID> path = computeShortestPath(sourceVertex, targetVertex, context);

          if (path != null && !path.isEmpty()) {
            // Create result with the path
            final ResultInternal result = new ResultInternal();

            // Copy all properties from previous result
            for (final String prop : inputResult.getPropertyNames()) {
              result.setProperty(prop, inputResult.getProperty(prop));
            }

            // Add path binding if path variable is specified
            if (pathVariable != null && !pathVariable.isEmpty()) {
              result.setProperty(pathVariable, path);
            }

            buffer.add(result);
          }
          // If no path found, skip this result (similar to a failed MATCH)
        }
      }

      @Override
      public void close() {
        ShortestPathStep.this.close();
      }
    };
  }

  /**
   * Computes the shortest path between source and target vertices.
   */
  private List<RID> computeShortestPath(final Vertex source, final Vertex target, final CommandContext context) {
    // Get edge type from pattern
    String edgeType = null;
    if (pattern.getRelationshipCount() > 0 && pattern.getRelationship(0).hasTypes()) {
      edgeType = pattern.getRelationship(0).getTypes().get(0);
    }

    // Get direction from pattern
    String direction = "BOTH";
    if (pattern.getRelationshipCount() > 0) {
      final Direction dir = pattern.getRelationship(0).getDirection();
      switch (dir) {
        case OUT:
          direction = "OUT";
          break;
        case IN:
          direction = "IN";
          break;
        default:
          direction = "BOTH";
      }
    }

    // Use SQLFunctionShortestPath to compute the path
    final SQLFunctionShortestPath shortestPathFunction = new SQLFunctionShortestPath();
    final Object[] params = edgeType != null ?
        new Object[] { source, target, direction, edgeType } :
        new Object[] { source, target, direction };

    return shortestPathFunction.execute(null, null, null, params, context);
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final StringBuilder builder = new StringBuilder();
    final String ind = getIndent(depth, indent);
    builder.append(ind);
    builder.append("+ SHORTEST PATH ");
    builder.append("(").append(sourceVariable).append(")");
    if (pattern.getRelationshipCount() > 0) {
      builder.append("-[");
      if (pattern.getRelationship(0).hasTypes()) {
        builder.append(":").append(String.join("|", pattern.getRelationship(0).getTypes()));
      }
      builder.append("*]-");
    }
    builder.append("(").append(targetVariable).append(")");
    if (pattern.isAllPaths()) {
      builder.append(" [ALL]");
    }
    if (context.isProfiling()) {
      builder.append(" (").append(getCostFormatted()).append(")");
    }
    return builder.toString();
  }

  private static String getIndent(final int depth, final int indent) {
    return "  ".repeat(Math.max(0, depth * indent));
  }
}
