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

import com.arcadedb.database.Database;
import com.arcadedb.database.RID;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.GhostEdgeReporter;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;

import com.arcadedb.utility.NumberUtils;
import com.arcadedb.utility.RidHashSet;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
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
    return 1;
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
    final Database database = startNode.getDatabase();
    final Map<String, Object> config = extractConfig(argOrDefault(args, 1, null));

    final int minLevel = extractComponentMinLevel(config);
    final int maxLevel = config.containsKey("maxLevel") ? NumberUtils.saturateToInt((Number) config.get("maxLevel")) : Integer.MAX_VALUE;
    final Filters filters = extractFilters(config, database, minLevel);
    final LabelFilter labels = filters.labels();

    // BFS to build spanning tree
    final List<List<Object>> allPaths = new ArrayList<>();
    final RidHashSet visited = new RidHashSet();
    final RidHashSet ghostNodes = new RidHashSet(16);
    final Queue<PathLevel> queue = new ArrayDeque<>();

    final LabelFilter.Verdict startVerdict = labels.evaluate(database, startNode.getIdentity(), 0);
    if (startVerdict != LabelFilter.Verdict.EXCLUDE_AND_PRUNE) {
      final List<Object> initialPath = new ArrayList<>();
      initialPath.add(startNode);
      visited.add(startNode.getIdentity());
      if (startVerdict.continues)
        queue.add(new PathLevel(initialPath, startNode, 0));

      // Add root path. A path already in allPaths is never appended to again - an expansion builds a new list from it -
      // so the defensive copies this used to make were copies of lists nobody could mutate (issue #7976)
      if (startVerdict.includes && minLevel == 0)
        allPaths.add(initialPath);
    }

    while (!queue.isEmpty()) {
      final PathLevel current = queue.poll();

      if (current.level >= maxLevel) {
        continue;
      }

      final int depth = current.level + 1;

      // Expand in both directions
      for (int side = 0; side < 2; side++) {
        final Vertex.DIRECTION direction = walkDirection(side);
        final Iterable<Edge> edges = hopEdges(context, current.vertex, direction, filters.relationships(), current.level);
        if (edges == null)
          continue;

        for (final Edge edge : edges) {
          try {
            // The neighbour is identified by its RID: a spanning tree rejects every edge that would close a cycle, so
            // most entries never need the vertex record at all (issue #7976)
            final RID neighborId = direction == Vertex.DIRECTION.OUT ? edge.getIn() : edge.getOut();
            if (visited.contains(neighborId))
              continue;

            final LabelFilter.Verdict verdict = labels.evaluate(database, neighborId, depth);
            if (verdict == LabelFilter.Verdict.EXCLUDE_AND_PRUNE)
              continue;

            // Resolved before the RID is marked visited: a RID marked visited by a load that then failed would make
            // every later edge to the same ghost short-circuit, so only the first one would ever be reported
            final Vertex neighbor = resolveNeighbor(neighborId, ghostNodes);
            if (neighbor == null)
              continue;

            visited.add(neighborId);

            final List<Object> newPath = new ArrayList<>(current.path.size() + 2);
            newPath.addAll(current.path);
            newPath.add(edge);
            newPath.add(neighbor);

            // A node the label filter walks through without reporting it still carries the paths that go past it
            if (verdict.includes)
              allPaths.add(newPath);
            if (verdict.continues)
              queue.add(new PathLevel(newPath, neighbor, depth));
          } catch (final RecordNotFoundException e) {
            GhostEdgeReporter.reportSkipped(e);
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
