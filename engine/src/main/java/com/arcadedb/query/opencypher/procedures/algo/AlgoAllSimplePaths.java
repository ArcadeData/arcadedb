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
package com.arcadedb.query.opencypher.procedures.algo;

import com.arcadedb.database.RID;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Procedure: algo.allSimplePaths(startNode, endNode, relTypes, maxDepth)
 * <p>
 * Finds all simple paths (paths without repeated nodes) between two nodes.
 * </p>
 * <p>
 * Example Cypher usage:
 * <pre>
 * MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})
 * CALL algo.allSimplePaths(a, b, 'KNOWS', 5)
 * YIELD path
 * </pre>
 * </p>
 *
 * @author ArcadeDB Team
 */
public class AlgoAllSimplePaths extends AbstractAlgoProcedure {
  public static final String NAME = "algo.allsimplepaths";

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public int getMinArgs() {
    return 4;
  }

  @Override
  public int getMaxArgs() {
    return 4;
  }

  @Override
  public String getDescription() {
    return "Find all simple paths (without repeated nodes) between two nodes up to a maximum depth";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("path");
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    validateArgs(args);

    final Vertex startNode = extractVertex(args[0], "startNode");
    final Vertex endNode = extractVertex(args[1], "endNode");
    final String[] relTypes = extractRelTypes(args[2]);
    final int maxDepth = ((Number) args[3]).intValue();

    if (maxDepth < 1) {
      throw new IllegalArgumentException(getName() + "(): maxDepth must be at least 1");
    }

    // Find all simple paths using DFS
    final List<List<Object>> allPaths = new ArrayList<>();
    final List<Object> currentPath = new ArrayList<>();
    final Set<RID> visited = new HashSet<>();

    currentPath.add(startNode);
    visited.add(startNode.getIdentity());

    findPaths(startNode, endNode, relTypes, maxDepth, currentPath, visited, allPaths, context);

    // Convert paths to results
    return allPaths.stream().map(pathElements -> {
      final List<Object> nodes = new ArrayList<>();
      final List<Object> relationships = new ArrayList<>();

      for (final Object element : pathElements) {
        if (element instanceof Vertex) {
          nodes.add(element);
        } else if (element instanceof Edge) {
          relationships.add(element);
        }
      }

      final Map<String, Object> path = new HashMap<>();
      path.put("_type", "path");
      path.put("nodes", nodes);
      path.put("relationships", relationships);
      path.put("length", relationships.size());

      final ResultInternal result = new ResultInternal();
      result.setProperty("path", path);
      return (Result) result;
    });
  }

  private void findPaths(final Vertex current, final Vertex target, final String[] relTypes,
      final int remainingDepth, final List<Object> currentPath, final Set<RID> visited,
      final List<List<Object>> allPaths, final CommandContext context) {

    if (current.getIdentity().equals(target.getIdentity())) {
      // Found a path - copy it
      allPaths.add(new ArrayList<>(currentPath));
      return;
    }

    if (remainingDepth <= 0) {
      return;
    }

    // Explore neighbors
    final Iterable<Edge> edges = relTypes != null && relTypes.length > 0
        ? current.getEdges(Vertex.DIRECTION.OUT, relTypes)
        : current.getEdges(Vertex.DIRECTION.OUT);

    for (final Edge edge : edges) {
      final Vertex neighbor = edge.getInVertex();
      final RID neighborId = neighbor.getIdentity();

      if (!visited.contains(neighborId)) {
        visited.add(neighborId);
        currentPath.add(edge);
        currentPath.add(neighbor);

        findPaths(neighbor, target, relTypes, remainingDepth - 1, currentPath, visited, allPaths, context);

        // Backtrack
        currentPath.removeLast();
        currentPath.removeLast();
        visited.remove(neighborId);
      }
    }

    // Also check incoming edges for bidirectional traversal
    final Iterable<Edge> inEdges = relTypes != null && relTypes.length > 0
        ? current.getEdges(Vertex.DIRECTION.IN, relTypes)
        : current.getEdges(Vertex.DIRECTION.IN);

    for (final Edge edge : inEdges) {
      final Vertex neighbor = edge.getOutVertex();
      final RID neighborId = neighbor.getIdentity();

      if (!visited.contains(neighborId)) {
        visited.add(neighborId);
        currentPath.add(edge);
        currentPath.add(neighbor);

        findPaths(neighbor, target, relTypes, remainingDepth - 1, currentPath, visited, allPaths, context);

        // Backtrack
        currentPath.removeLast();
        currentPath.removeLast();
        visited.remove(neighborId);
      }
    }
  }
}
