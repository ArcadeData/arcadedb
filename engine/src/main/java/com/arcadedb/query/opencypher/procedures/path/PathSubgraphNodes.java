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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Procedure: path.subgraphNodes(startNode, config)
 * <p>
 * Returns all nodes reachable from a starting node within the configured constraints.
 * </p>
 * <p>
 * Example Cypher usage:
 * <pre>
 * MATCH (a:Person {name: 'Alice'})
 * CALL path.subgraphNodes(a, {
 *   relationshipFilter: 'KNOWS',
 *   maxLevel: 3
 * })
 * YIELD node
 * </pre>
 * </p>
 *
 * @author ArcadeDB Team
 */
public class PathSubgraphNodes extends AbstractPathProcedure {
  public static final String NAME = "path.subgraphnodes";

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
    return "Returns all nodes reachable from a starting node within the configured constraints";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("node");
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    validateArgs(args);

    final Vertex startNode = extractVertex(args[0], "startNode");
    final Map<String, Object> config = extractConfig(args[1]);

    final String[] relTypes = extractRelTypes(config.get("relationshipFilter"));
    final String[] labelFilter = extractLabels(config.get("labelFilter"));
    final int maxLevel = config.containsKey("maxLevel") ? ((Number) config.get("maxLevel")).intValue() : Integer.MAX_VALUE;

    // BFS to find all reachable nodes
    final Set<Vertex> reachableNodes = new HashSet<>();
    final Set<RID> visited = new HashSet<>();
    final Queue<VertexLevel> queue = new ArrayDeque<>();

    queue.add(new VertexLevel(startNode, 0));
    visited.add(startNode.getIdentity());
    reachableNodes.add(startNode);

    while (!queue.isEmpty()) {
      final VertexLevel current = queue.poll();

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
            reachableNodes.add(neighbor);
            queue.add(new VertexLevel(neighbor, current.level + 1));
          }
        }
      }
    }

    return reachableNodes.stream().map(node -> {
      final ResultInternal result = new ResultInternal();
      result.setProperty("node", node);
      return (Result) result;
    });
  }

  private static class VertexLevel {
    final Vertex vertex;
    final int level;

    VertexLevel(final Vertex vertex, final int level) {
      this.vertex = vertex;
      this.level = level;
    }
  }
}
