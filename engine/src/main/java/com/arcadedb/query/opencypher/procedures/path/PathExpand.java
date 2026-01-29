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
package com.arcadedb.query.opencypher.procedures.path;

import com.arcadedb.database.RID;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Procedure: path.expand(startNode, relTypes, labelFilter, minDepth, maxDepth)
 * <p>
 * Expands paths from a starting node following relationship types and node labels.
 * </p>
 * <p>
 * Example Cypher usage:
 * <pre>
 * MATCH (a:Person {name: 'Alice'})
 * CALL path.expand(a, 'KNOWS|WORKS_WITH', '+Person', 1, 3)
 * YIELD path
 * </pre>
 * </p>
 *
 * @author ArcadeDB Team
 */
public class PathExpand extends AbstractPathProcedure {
  public static final String NAME = "path.expand";

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public int getMinArgs() {
    return 5;
  }

  @Override
  public int getMaxArgs() {
    return 5;
  }

  @Override
  public String getDescription() {
    return "Expand paths from a starting node following relationship types and node labels";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("path");
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    validateArgs(args);

    final Vertex startNode = extractVertex(args[0], "startNode");
    final String[] relTypes = extractRelTypes(args[1]);
    final String[] labelFilter = extractLabels(args[2]);
    final int minDepth = ((Number) args[3]).intValue();
    final int maxDepth = ((Number) args[4]).intValue();

    if (minDepth < 0) {
      throw new IllegalArgumentException(getName() + "(): minDepth must be non-negative");
    }
    if (maxDepth < minDepth) {
      throw new IllegalArgumentException(getName() + "(): maxDepth must be >= minDepth");
    }

    // Expand paths using BFS/DFS
    final List<List<Object>> allPaths = new ArrayList<>();
    final List<Object> currentPath = new ArrayList<>();
    final Set<RID> visited = new HashSet<>();

    currentPath.add(startNode);
    visited.add(startNode.getIdentity());

    expandPaths(startNode, relTypes, labelFilter, 0, minDepth, maxDepth, currentPath, visited, allPaths, context);

    // Convert paths to results
    return allPaths.stream().map(pathElements -> {
      final Map<String, Object> path = buildPath(pathElements);
      final ResultInternal result = new ResultInternal();
      result.setProperty("path", path);
      return (Result) result;
    });
  }

  private void expandPaths(final Vertex current, final String[] relTypes, final String[] labelFilter,
      final int currentDepth, final int minDepth, final int maxDepth,
      final List<Object> currentPath, final Set<RID> visited,
      final List<List<Object>> allPaths, final CommandContext context) {

    // If we're at or past minDepth, this is a valid path
    if (currentDepth >= minDepth) {
      allPaths.add(new ArrayList<>(currentPath));
    }

    // Stop if we've reached maxDepth
    if (currentDepth >= maxDepth) {
      return;
    }

    // Expand outgoing edges
    expandInDirection(current, relTypes, labelFilter, currentDepth, minDepth, maxDepth,
        currentPath, visited, allPaths, context, Vertex.DIRECTION.OUT);

    // Expand incoming edges
    expandInDirection(current, relTypes, labelFilter, currentDepth, minDepth, maxDepth,
        currentPath, visited, allPaths, context, Vertex.DIRECTION.IN);
  }

  private void expandInDirection(final Vertex current, final String[] relTypes, final String[] labelFilter,
      final int currentDepth, final int minDepth, final int maxDepth,
      final List<Object> currentPath, final Set<RID> visited,
      final List<List<Object>> allPaths, final CommandContext context, final Vertex.DIRECTION direction) {

    final Iterable<Edge> edges = relTypes != null && relTypes.length > 0
        ? current.getEdges(direction, relTypes)
        : current.getEdges(direction);

    for (final Edge edge : edges) {
      final Vertex neighbor = direction == Vertex.DIRECTION.OUT ? edge.getInVertex() : edge.getOutVertex();
      final RID neighborId = neighbor.getIdentity();

      if (!visited.contains(neighborId) && matchesLabels(neighbor, labelFilter)) {
        visited.add(neighborId);
        currentPath.add(edge);
        currentPath.add(neighbor);

        expandPaths(neighbor, relTypes, labelFilter, currentDepth + 1, minDepth, maxDepth,
            currentPath, visited, allPaths, context);

        // Backtrack
        currentPath.removeLast();
        currentPath.removeLast();
        visited.remove(neighborId);
      }
    }
  }
}
