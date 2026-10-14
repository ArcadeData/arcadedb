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
import com.arcadedb.query.opencypher.traversal.TraversalPath;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;

import com.arcadedb.utility.NumberUtils;
import com.arcadedb.utility.RidHashSet;

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
 * Config options: relationshipFilter, labelFilter, sequence, filterStartNode, beginSequenceAtStart, minLevel, maxLevel,
 * bfs, limit. The filters take APOC's syntax: see {@link RelationshipFilter} and {@link LabelFilter}.
 * </p>
 * <p>
 * Example Cypher usage:
 * <pre>
 * MATCH (a:Person {name: 'Alice'})
 * CALL path.expandConfig(a, {
 *   relationshipFilter: 'KNOWS>|&lt;WORKS_WITH',
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
    final Map<String, Object> config = optionalMapArg(args, 1, "config");

    // Extract configuration options
    final int minLevel = config.containsKey("minLevel") ? NumberUtils.saturateToInt((Number) config.get("minLevel")) : 0;
    final int maxLevel = config.containsKey("maxLevel") ? NumberUtils.saturateToInt((Number) config.get("maxLevel")) : Integer.MAX_VALUE;
    final boolean bfs = config.containsKey("bfs") ? (Boolean) config.get("bfs") : true;
    final int limit = config.containsKey("limit") ? NumberUtils.saturateToInt((Number) config.get("limit")) : Integer.MAX_VALUE;

    final Database database = startNode.getDatabase();
    final Filters filters = extractFilters(config, database, minLevel);

    // Expand paths
    final List<List<Object>> allPaths = new ArrayList<>();

    final LabelFilter.Verdict startVerdict = filters.labels().evaluate(database, startNode.getIdentity(), 0);
    if (startVerdict != LabelFilter.Verdict.EXCLUDE_AND_PRUNE) {
      final List<Object> initialPath = new ArrayList<>();
      initialPath.add(startNode);
      if (startVerdict.includes && minLevel <= 0 && limit > 0)
        allPaths.add(new ArrayList<>(initialPath));

      if (startVerdict.continues) {
        if (bfs) {
          expandBFS(initialPath, filters, minLevel, maxLevel, limit, allPaths, context);
        } else {
          // Only the depth-first walk carries this state, and bfs defaults to true
          final Set<RID> visited = new HashSet<>();
          final RidHashSet ghostNodes = new RidHashSet(16);
          visited.add(startNode.getIdentity());

          expandDFS(startNode, filters, 0, minLevel, maxLevel, initialPath, visited, ghostNodes, allPaths, context, limit);
        }
      }
    }

    // Convert paths to results (respect limit)
    return allPaths.stream().limit(limit).map(pathElements -> {
      final TraversalPath path = buildPath(pathElements);
      final ResultInternal result = new ResultInternal();
      result.setProperty("path", path);
      return (Result) result;
    });
  }

  /**
   * Breadth-first, each node reached once: a path is reported the moment its last node is reached, and extended on the
   * next wave if the label filter lets the walk go past that node.
   */
  private void expandBFS(final List<Object> initialPath, final Filters filters, final int minLevel, final int maxLevel,
      final int limit, final List<List<Object>> allPaths, final CommandContext context) {

    List<List<Object>> frontier = new ArrayList<>();
    final RidHashSet visited = new RidHashSet();
    final RidHashSet ghostNodes = new RidHashSet(16);

    frontier.add(initialPath);
    visited.add(((Vertex) initialPath.getFirst()).getIdentity());

    for (int level = 0; level < maxLevel && !frontier.isEmpty() && allPaths.size() < limit; ++level) {
      final List<List<Object>> nextFrontier = new ArrayList<>();

      for (final List<Object> path : frontier) {
        if (allPaths.size() >= limit)
          break;
        expandFromNode(path, filters, level, minLevel, limit, visited, ghostNodes, nextFrontier, allPaths, context);
      }

      frontier = nextFrontier;
    }
  }

  private void expandFromNode(final List<Object> currentPath, final Filters filters, final int level, final int minLevel,
      final int limit, final RidHashSet visited, final RidHashSet ghostNodes, final List<List<Object>> nextFrontier,
      final List<List<Object>> allPaths, final CommandContext context) {

    final Vertex node = (Vertex) currentPath.getLast();
    final int depth = level + 1;

    // Expand in both directions
    for (int side = 0; side < 2; side++) {
      final Vertex.DIRECTION direction = walkDirection(side);
      final Iterable<Edge> edges = hopEdges(context, node, direction, filters.relationships(), level);
      if (edges == null)
        continue;

      for (final Edge edge : edges) {
        try {
          // The neighbour is tested by its RID and loaded only once it is going into the path (issue #7976)
          final RID neighborId = direction == Vertex.DIRECTION.OUT ? edge.getIn() : edge.getOut();
          if (visited.contains(neighborId))
            continue;

          final LabelFilter.Verdict verdict = filters.labels().evaluate(node.getDatabase(), neighborId, depth);
          if (verdict == LabelFilter.Verdict.EXCLUDE_AND_PRUNE)
            continue;

          // Resolved before the RID is marked visited: a RID marked visited by a load that then failed would make
          // every later edge to the same ghost short-circuit, so only the first one would ever be reported
          final Vertex neighbor = resolveNeighbor(neighborId, ghostNodes);
          if (neighbor == null)
            continue;

          visited.add(neighborId);

          final List<Object> newPath = new ArrayList<>(currentPath.size() + 2);
          newPath.addAll(currentPath);
          newPath.add(edge);
          newPath.add(neighbor);

          if (verdict.includes && depth >= minLevel && allPaths.size() < limit)
            allPaths.add(newPath);
          if (verdict.continues)
            nextFrontier.add(newPath);
        } catch (final RecordNotFoundException e) {
          GhostEdgeReporter.reportSkipped(e);
        }
      }
    }
  }

  // `visited` here is a boxing Set<RID> rather than the primitive RidHashSet the breadth-first walks use, and has to
  // stay one: this is a depth-first path enumeration, so it UNMARKS a vertex on backtracking, and an open-addressed
  // set with linear probing cannot remove an entry without tombstones - which RidHashSet deliberately does not carry
  private void expandDFS(final Vertex current, final Filters filters, final int currentDepth, final int minDepth,
      final int maxDepth, final List<Object> currentPath, final Set<RID> visited, final RidHashSet ghostNodes,
      final List<List<Object>> allPaths, final CommandContext context, final int limit) {

    if (allPaths.size() >= limit || currentDepth >= maxDepth) {
      return;
    }

    final int depth = currentDepth + 1;
    for (int side = 0; side < 2; side++) {
      final Vertex.DIRECTION direction = walkDirection(side);
      final Iterable<Edge> edges = hopEdges(context, current, direction, filters.relationships(), currentDepth);
      if (edges == null)
        continue;

      for (final Edge edge : edges) {
        if (allPaths.size() >= limit) {
          return;
        }

        try {
          // The neighbour is tested by its RID and loaded only once it is going into the path (issue #7976)
          final RID neighborId = direction == Vertex.DIRECTION.OUT ? edge.getIn() : edge.getOut();
          if (visited.contains(neighborId))
            continue;

          final LabelFilter.Verdict verdict = filters.labels().evaluate(current.getDatabase(), neighborId, depth);
          if (verdict == LabelFilter.Verdict.EXCLUDE_AND_PRUNE)
            continue;

          final Vertex neighbor = resolveNeighbor(neighborId, ghostNodes);
          if (neighbor == null)
            continue;

          visited.add(neighborId);
          currentPath.add(edge);
          currentPath.add(neighbor);

          // try-finally so the path/visited bookkeeping is unwound even if the recursion throws.
          try {
            if (verdict.includes && depth >= minDepth)
              allPaths.add(new ArrayList<>(currentPath));
            if (verdict.continues)
              expandDFS(neighbor, filters, depth, minDepth, maxDepth, currentPath, visited, ghostNodes, allPaths, context, limit);
          } finally {
            currentPath.removeLast();
            currentPath.removeLast();
            visited.remove(neighborId);
          }
        } catch (final RecordNotFoundException e) {
          // Only edge.getIn()/getOut() (a ghost edge record, surfaced by its lazy load) can land here now; a ghost
          // VERTEX is caught and remembered inside resolveNeighbor. The recursion has its own per-edge catches.
          GhostEdgeReporter.reportSkipped(e);
        }
      }
    }
  }
}
