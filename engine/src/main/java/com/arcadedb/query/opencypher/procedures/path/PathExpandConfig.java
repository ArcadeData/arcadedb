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
 * Procedure: path.expandConfig(startNode, config)
 * <p>
 * Expands paths from a starting node using a configuration map.
 * Config options: relationshipFilter, labelFilter, minLevel, maxLevel, bfs, uniqueness, limit
 * </p>
 * <p>
 * Example Cypher usage:
 * <pre>
 * MATCH (a:Person {name: 'Alice'})
 * CALL path.expandConfig(a, {
 *   relationshipFilter: 'KNOWS|WORKS_WITH',
 *   labelFilter: '+Person',
 *   minLevel: 1,
 *   maxLevel: 3,
 *   bfs: true
 * })
 * YIELD path
 * </pre>
 * </p>
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class PathExpandConfig extends AbstractPathProcedure {
  public static final String NAME = "path.expandconfig";

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public int getMinArgs() {
    return 2;
  }

  @Override
  public int getMaxArgs() {
    return 2;
  }

  @Override
  public String getDescription() {
    return "Expand paths from a starting node using a configuration map";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("path");
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    validateArgs(args);

    final Vertex startNode = extractVertex(args[0], "startNode");
    final Map<String, Object> config = extractConfig(args[1]);

    // Extract configuration options
    final String[] relTypes = extractRelTypes(config.get("relationshipFilter"));
    final String[] labelFilter = extractLabels(config.get("labelFilter"));
    final int minLevel = config.containsKey("minLevel") ? ((Number) config.get("minLevel")).intValue() : 0;
    final int maxLevel = config.containsKey("maxLevel") ? ((Number) config.get("maxLevel")).intValue() : Integer.MAX_VALUE;
    final boolean bfs = config.containsKey("bfs") ? (Boolean) config.get("bfs") : true;
    final int limit = config.containsKey("limit") ? ((Number) config.get("limit")).intValue() : Integer.MAX_VALUE;

    // Expand paths
    final List<List<Object>> allPaths = new ArrayList<>();
    final List<Object> currentPath = new ArrayList<>();
    final Set<RID> visited = new HashSet<>();

    currentPath.add(startNode);
    visited.add(startNode.getIdentity());

    if (bfs) {
      expandBFS(startNode, relTypes, labelFilter, minLevel, maxLevel, limit, allPaths, context);
    } else {
      expandDFS(startNode, relTypes, labelFilter, 0, minLevel, maxLevel, currentPath, visited, allPaths, context, limit);
    }

    // Convert paths to results (respect limit)
    return allPaths.stream().limit(limit).map(pathElements -> {
      final Map<String, Object> path = buildPath(pathElements);
      final ResultInternal result = new ResultInternal();
      result.setProperty("path", path);
      return (Result) result;
    });
  }

  private void expandBFS(final Vertex startNode, final String[] relTypes, final String[] labelFilter,
      final int minLevel, final int maxLevel, final int limit,
      final List<List<Object>> allPaths, final CommandContext context) {

    final List<List<Object>> frontier = new ArrayList<>();
    final Set<RID> visited = new HashSet<>();

    final List<Object> initialPath = new ArrayList<>();
    initialPath.add(startNode);
    frontier.add(initialPath);
    visited.add(startNode.getIdentity());

    int currentLevel = 0;

    while (!frontier.isEmpty() && currentLevel <= maxLevel && allPaths.size() < limit) {
      final List<List<Object>> nextFrontier = new ArrayList<>();

      for (final List<Object> path : frontier) {
        if (currentLevel >= minLevel && allPaths.size() < limit) {
          allPaths.add(new ArrayList<>(path));
        }

        if (currentLevel < maxLevel) {
          final Vertex lastNode = (Vertex) path.get(path.size() - 1);
          expandFromNode(lastNode, relTypes, labelFilter, path, visited, nextFrontier);
        }
      }

      frontier.clear();
      frontier.addAll(nextFrontier);
      currentLevel++;
    }
  }

  private void expandFromNode(final Vertex node, final String[] relTypes, final String[] labelFilter,
      final List<Object> currentPath, final Set<RID> visited, final List<List<Object>> nextFrontier) {

    // Expand in both directions
    for (final Vertex.DIRECTION direction : new Vertex.DIRECTION[] { Vertex.DIRECTION.OUT, Vertex.DIRECTION.IN }) {
      final Iterable<Edge> edges = relTypes != null && relTypes.length > 0
          ? node.getEdges(direction, relTypes)
          : node.getEdges(direction);

      for (final Edge edge : edges) {
        final Vertex neighbor = direction == Vertex.DIRECTION.OUT ? edge.getInVertex() : edge.getOutVertex();
        final RID neighborId = neighbor.getIdentity();

        if (!visited.contains(neighborId) && matchesLabels(neighbor, labelFilter)) {
          visited.add(neighborId);
          final List<Object> newPath = new ArrayList<>(currentPath);
          newPath.add(edge);
          newPath.add(neighbor);
          nextFrontier.add(newPath);
        }
      }
    }
  }

  private void expandDFS(final Vertex current, final String[] relTypes, final String[] labelFilter,
      final int currentDepth, final int minDepth, final int maxDepth,
      final List<Object> currentPath, final Set<RID> visited,
      final List<List<Object>> allPaths, final CommandContext context, final int limit) {

    if (allPaths.size() >= limit) {
      return;
    }

    if (currentDepth >= minDepth) {
      allPaths.add(new ArrayList<>(currentPath));
    }

    if (currentDepth >= maxDepth) {
      return;
    }

    for (final Vertex.DIRECTION direction : new Vertex.DIRECTION[] { Vertex.DIRECTION.OUT, Vertex.DIRECTION.IN }) {
      final Iterable<Edge> edges = relTypes != null && relTypes.length > 0
          ? current.getEdges(direction, relTypes)
          : current.getEdges(direction);

      for (final Edge edge : edges) {
        if (allPaths.size() >= limit) {
          return;
        }

        final Vertex neighbor = direction == Vertex.DIRECTION.OUT ? edge.getInVertex() : edge.getOutVertex();
        final RID neighborId = neighbor.getIdentity();

        if (!visited.contains(neighborId) && matchesLabels(neighbor, labelFilter)) {
          visited.add(neighborId);
          currentPath.add(edge);
          currentPath.add(neighbor);

          expandDFS(neighbor, relTypes, labelFilter, currentDepth + 1, minDepth, maxDepth,
              currentPath, visited, allPaths, context, limit);

          currentPath.remove(currentPath.size() - 1); // Remove neighbor
          currentPath.remove(currentPath.size() - 1); // Remove neighbor
          visited.remove(neighborId);
        }
      }
    }
  }
}
