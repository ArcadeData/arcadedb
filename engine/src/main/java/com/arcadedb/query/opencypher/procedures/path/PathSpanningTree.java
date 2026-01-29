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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Procedure: path.spanningTree(startNode, config)
 * <p>
 * Returns a spanning tree (paths from the start node to all reachable nodes)
 * using BFS traversal.
 * </p>
 * <p>
 * Example Cypher usage:
 * <pre>
 * MATCH (a:Person {name: 'Alice'})
 * CALL path.spanningTree(a, {
 *   relationshipFilter: 'KNOWS',
 *   maxLevel: 3
 * })
 * YIELD path
 * </pre>
 * </p>
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class PathSpanningTree extends AbstractPathProcedure {
  public static final String NAME = "path.spanningtree";

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
    return "Returns a spanning tree from the start node to all reachable nodes";
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

    final String[] relTypes = extractRelTypes(config.get("relationshipFilter"));
    final String[] labelFilter = extractLabels(config.get("labelFilter"));
    final int maxLevel = config.containsKey("maxLevel") ? ((Number) config.get("maxLevel")).intValue() : Integer.MAX_VALUE;

    // BFS to build spanning tree
    final List<List<Object>> allPaths = new ArrayList<>();
    final Set<RID> visited = new HashSet<>();
    final Queue<PathLevel> queue = new ArrayDeque<>();

    final List<Object> initialPath = new ArrayList<>();
    initialPath.add(startNode);
    queue.add(new PathLevel(initialPath, startNode, 0));
    visited.add(startNode.getIdentity());

    // Add root path
    allPaths.add(new ArrayList<>(initialPath));

    while (!queue.isEmpty()) {
      final PathLevel current = queue.poll();

      if (current.level >= maxLevel) {
        continue;
      }

      // Expand in both directions
      for (final Vertex.DIRECTION direction : new Vertex.DIRECTION[] { Vertex.DIRECTION.OUT, Vertex.DIRECTION.IN }) {
        final Iterable<Edge> edges = relTypes != null && relTypes.length > 0
            ? current.vertex.getEdges(direction, relTypes)
            : current.vertex.getEdges(direction);

        for (final Edge edge : edges) {
          final Vertex neighbor = direction == Vertex.DIRECTION.OUT ? edge.getInVertex() : edge.getOutVertex();
          final RID neighborId = neighbor.getIdentity();

          if (!visited.contains(neighborId) && matchesLabels(neighbor, labelFilter)) {
            visited.add(neighborId);

            final List<Object> newPath = new ArrayList<>(current.path);
            newPath.add(edge);
            newPath.add(neighbor);

            allPaths.add(new ArrayList<>(newPath));
            queue.add(new PathLevel(newPath, neighbor, current.level + 1));
          }
        }
      }
    }

    // Convert paths to results
    return allPaths.stream().map(pathElements -> {
      final Map<String, Object> path = buildPath(pathElements);
      final ResultInternal result = new ResultInternal();
      result.setProperty("path", path);
      return (Result) result;
    });
  }

  private static class PathLevel {
    final List<Object> path;
    final Vertex vertex;
    final int level;

    PathLevel(final List<Object> path, final Vertex vertex, final int level) {
      this.path = path;
      this.vertex = vertex;
      this.level = level;
    }
  }
}
