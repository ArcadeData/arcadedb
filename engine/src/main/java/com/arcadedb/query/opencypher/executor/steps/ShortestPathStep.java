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

import com.arcadedb.database.Database;
import com.arcadedb.database.RID;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.graph.Edge;
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

          // Compute the shortest path and resolve RIDs to actual Vertex/Edge objects
          final List<Object> path = computeShortestPath(sourceVertex, targetVertex, context);

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
   * Returns a list of alternating Vertex and Edge objects representing the path.
   */
  private List<Object> computeShortestPath(final Vertex source, final Vertex target, final CommandContext context) {
    // Get edge type from pattern
    String edgeType = null;
    if (pattern.getRelationshipCount() > 0 && pattern.getRelationship(0).hasTypes()) {
      edgeType = pattern.getRelationship(0).getTypes().get(0);
    }

    // Get direction from pattern
    Vertex.DIRECTION vertexDirection = Vertex.DIRECTION.BOTH;
    String direction = "BOTH";
    if (pattern.getRelationshipCount() > 0) {
      final Direction dir = pattern.getRelationship(0).getDirection();
      switch (dir) {
        case OUT:
          direction = "OUT";
          vertexDirection = Vertex.DIRECTION.OUT;
          break;
        case IN:
          direction = "IN";
          vertexDirection = Vertex.DIRECTION.IN;
          break;
        default:
          direction = "BOTH";
      }
    }

    // Use SQLFunctionShortestPath to compute the path (returns vertex RIDs only)
    final SQLFunctionShortestPath shortestPathFunction = new SQLFunctionShortestPath();
    final Object[] params = edgeType != null ?
        new Object[] { source, target, direction, edgeType } :
        new Object[] { source, target, direction };

    final List<RID> pathRids = shortestPathFunction.execute(null, null, null, params, context);
    if (pathRids == null || pathRids.isEmpty())
      return null;

    // Build a proper path with alternating Vertex and Edge objects
    return resolvePathWithEdges(pathRids, vertexDirection, edgeType, context.getDatabase());
  }

  /**
   * Resolves a list of vertex RIDs into a proper path with alternating Vertex and Edge objects.
   */
  public static List<Object> resolvePathWithEdges(final List<RID> vertexRids, final Vertex.DIRECTION direction,
      final String edgeType, final Database database) {
    final List<Object> result = new ArrayList<>(vertexRids.size() * 2 - 1);

    Vertex prev = null;
    for (final RID rid : vertexRids) {
      final Vertex current = (Vertex) database.lookupByRID(rid, true);

      if (prev != null) {
        // Find the edge connecting prev to current
        final Edge edge = findConnectingEdge(prev, current, direction, edgeType);
        if (edge != null)
          result.add(edge);
      }

      result.add(current);
      prev = current;
    }

    return result;
  }

  /**
   * Finds the edge connecting two vertices.
   */
  private static Edge findConnectingEdge(final Vertex from, final Vertex to, final Vertex.DIRECTION direction,
      final String edgeType) {
    final Vertex.DIRECTION[] directions = direction == Vertex.DIRECTION.BOTH ?
        new Vertex.DIRECTION[] { Vertex.DIRECTION.OUT, Vertex.DIRECTION.IN } :
        new Vertex.DIRECTION[] { direction };

    for (final Vertex.DIRECTION dir : directions) {
      final Iterable<Edge> edges = edgeType != null ?
          from.getEdges(dir, edgeType) :
          from.getEdges(dir);

      for (final Edge edge : edges) {
        final RID connected = dir == Vertex.DIRECTION.OUT ? edge.getIn() : edge.getOut();
        if (connected.equals(to.getIdentity()))
          return edge;
      }
    }
    return null;
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
