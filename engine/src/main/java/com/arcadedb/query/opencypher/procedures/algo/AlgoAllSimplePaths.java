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
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.GhostEdgeReporter;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Procedure: algo.allSimplePaths(startNode, endNode, relTypes, maxDepth, [options])
 * <p>
 * Finds all simple paths (paths without repeated nodes) between two nodes.
 * </p>
 * <p>
 * The optional 5th argument is a configuration map. Supported keys:
 * <ul>
 *   <li>{@code skipRelTypes} - a relationship type (string) or list of types to exclude from traversal</li>
 *   <li>{@code skipVertexTypes} - a vertex (node) type (string) or list of types to treat as barriers; as
 *   soon as the traversal reaches a vertex of one of these types the branch is pruned immediately</li>
 * </ul>
 * </p>
 * <p>
 * Example Cypher usage:
 * <pre>
 * MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})
 * CALL algo.allSimplePaths(a, b, ['KNOWS','FRIEND'], 5, { skipRelTypes: ['FRIEND'], skipVertexTypes: ['Company'] })
 * YIELD path
 * </pre>
 * </p>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
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
    return 5;
  }

  @Override
  public String getDescription() {
    return """
        Find all simple paths (without repeated nodes) between two nodes up to a maximum depth, \
        optionally excluding relationship types via { skipRelTypes: [...] } and/or vertex types via \
        { skipVertexTypes: [...] }\
        """;
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
    final Map<String, Object> options = args.length > 4 ? extractMap(args[4], "options") : null;
    final Set<String> skipRelTypes = extractSkipTypes(options, "skipRelTypes");
    final Set<String> skipVertexTypes = extractSkipTypes(options, "skipVertexTypes");

    if (maxDepth < 1)
      throw new IllegalArgumentException(getName() + "(): maxDepth must be at least 1");

    final List<List<Object>> allPaths = new ArrayList<>();
    final List<Object> currentPath = new ArrayList<>();
    final Set<RID> visited = new HashSet<>();

    currentPath.add(startNode);
    visited.add(startNode.getIdentity());

    findPaths(startNode, endNode, relTypes, skipRelTypes, skipVertexTypes, maxDepth, currentPath, visited, allPaths, context);

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

      final Map<String, Object> path = new HashMap<>(Map.of(
          "_type", "path",
          "nodes", nodes,
          "relationships", relationships,
          "length", relationships.size()));

      final ResultInternal result = new ResultInternal();
      result.setProperty("path", path);
      return result;
    });
  }

  private Set<String> extractSkipTypes(final Map<String, Object> options, final String optionKey) {
    if (options == null || options.isEmpty())
      return Set.of();

    final Object value = options.get(optionKey);
    if (value == null)
      return Set.of();

    final String[] types = extractRelTypes(value);
    if (types == null || types.length == 0)
      return Set.of();

    final Set<String> result = new HashSet<>(types.length);
    Collections.addAll(result, types);
    return result;
  }

  private void findPaths(final Vertex current, final Vertex target, final String[] relTypes,
                         final Set<String> skipRelTypes, final Set<String> skipVertexTypes, final int remainingDepth,
                         final List<Object> currentPath, final Set<RID> visited,
                         final List<List<Object>> allPaths, final CommandContext context) {

    if (current.getIdentity().equals(target.getIdentity())) {
      allPaths.add(new ArrayList<>(currentPath));
      return;
    }

    if (remainingDepth <= 0)
      return;

    final Iterable<Edge> edges = relTypes != null && relTypes.length > 0
        ? current.getEdges(Vertex.DIRECTION.OUT, relTypes)
        : current.getEdges(Vertex.DIRECTION.OUT);

    for (final Edge edge : edges) {
      if (!skipRelTypes.isEmpty() && skipRelTypes.contains(edge.getTypeName()))
        continue;

      try {
        final Vertex neighbor = edge.getInVertex();

        // Early pruning: a barrier vertex type halts the expansion of this branch immediately
        if (!skipVertexTypes.isEmpty() && skipVertexTypes.contains(neighbor.getTypeName()))
          continue;

        final RID neighborId = neighbor.getIdentity();

        if (!visited.contains(neighborId)) {
          visited.add(neighborId);
          currentPath.add(edge);
          currentPath.add(neighbor);

          // try-finally so the path/visited bookkeeping is unwound even if the recursion throws,
          // leaving no dirty state for the rest of this branch.
          try {
            findPaths(neighbor, target, relTypes, skipRelTypes, skipVertexTypes, remainingDepth - 1, currentPath, visited, allPaths, context);
          } finally {
            currentPath.removeLast();
            currentPath.removeLast();
            visited.remove(neighborId);
          }
        }
      } catch (final RecordNotFoundException e) {
        // Only the outer edge.get*Vertex() above can land here; the recursion has its own per-edge catches.
        GhostEdgeReporter.reportSkipped(e);
      }
    }

    final Iterable<Edge> inEdges = relTypes != null && relTypes.length > 0
        ? current.getEdges(Vertex.DIRECTION.IN, relTypes)
        : current.getEdges(Vertex.DIRECTION.IN);

    for (final Edge edge : inEdges) {
      if (!skipRelTypes.isEmpty() && skipRelTypes.contains(edge.getTypeName()))
        continue;

      try {
        final Vertex neighbor = edge.getOutVertex();

        // Early pruning: a barrier vertex type halts the expansion of this branch immediately
        if (!skipVertexTypes.isEmpty() && skipVertexTypes.contains(neighbor.getTypeName()))
          continue;

        final RID neighborId = neighbor.getIdentity();

        if (!visited.contains(neighborId)) {
          visited.add(neighborId);
          currentPath.add(edge);
          currentPath.add(neighbor);

          // try-finally so the path/visited bookkeeping is unwound even if the recursion throws,
          // leaving no dirty state for the rest of this branch.
          try {
            findPaths(neighbor, target, relTypes, skipRelTypes, skipVertexTypes, remainingDepth - 1, currentPath, visited, allPaths, context);
          } finally {
            currentPath.removeLast();
            currentPath.removeLast();
            visited.remove(neighborId);
          }
        }
      } catch (final RecordNotFoundException e) {
        // Only the outer edge.get*Vertex() above can land here; the recursion has its own per-edge catches.
        GhostEdgeReporter.reportSkipped(e);
      }
    }
  }
}
