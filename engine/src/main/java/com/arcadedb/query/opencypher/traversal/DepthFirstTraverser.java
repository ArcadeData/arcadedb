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
package com.arcadedb.query.opencypher.traversal;

import com.arcadedb.database.RID;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.ast.Direction;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Depth-first graph traverser.
 * Explores paths deeply before backtracking, suitable for exhaustive path exploration.
 * Uses recursive DFS with iterative implementation to avoid stack overflow.
 */
public class DepthFirstTraverser extends GraphTraverser {

  public DepthFirstTraverser(final Direction direction, final String[] relationshipTypes, final int minHops,
      final int maxHops, final boolean trackPaths, final boolean detectCycles) {
    super(direction, relationshipTypes, minHops, maxHops, trackPaths, detectCycles);
  }

  public DepthFirstTraverser(final Direction direction, final String[] relationshipTypes,
      final Map<String, Object> edgePropertyFilters, final int minHops,
      final int maxHops, final boolean trackPaths, final boolean detectCycles) {
    super(direction, relationshipTypes, edgePropertyFilters, minHops, maxHops, trackPaths, detectCycles);
  }

  @Override
  public Iterator<Vertex> traverse(final Vertex startVertex) {
    return new DFSVertexIterator(startVertex);
  }

  @Override
  public Iterator<TraversalPath> traversePaths(final Vertex startVertex) {
    return new DFSPathIterator(startVertex);
  }

  /**
   * Iterator for DFS vertex traversal.
   */
  private class DFSVertexIterator implements Iterator<Vertex> {
    private final List<Vertex> results = new ArrayList<>();
    private int currentIndex = 0;

    DFSVertexIterator(final Vertex startVertex) {
      final Set<RID> visited = detectCycles ? createVisitedSet() : new HashSet<>();
      performDFS(startVertex, 0, visited);
    }

    private void performDFS(final Vertex vertex, final int depth, final Set<RID> visited) {
      // Skip if already visited
      if (detectCycles && isVisited(vertex, visited)) {
        return;
      }

      // Mark as visited
      if (detectCycles) {
        markVisited(vertex, visited);
      }

      // Add to results if depth is within bounds
      if (depth >= minHops && depth <= maxHops) {
        results.add(vertex);
      }

      // Stop if we've reached max depth
      if (depth >= maxHops) {
        return;
      }

      // Recursively explore neighbors using fast getNextVertices() (skips loading edge records)
      for (final Vertex nextVertex : getNextVertices(vertex)) {
        // Skip if already visited
        if (detectCycles && isVisited(nextVertex, visited)) {
          continue;
        }

        performDFS(nextVertex, depth + 1, visited);
      }
    }

    @Override
    public boolean hasNext() {
      return currentIndex < results.size();
    }

    @Override
    public Vertex next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      return results.get(currentIndex++);
    }
  }

  /**
   * Iterator for DFS path traversal.
   */
  private class DFSPathIterator implements Iterator<TraversalPath> {
    private final List<TraversalPath> results = new ArrayList<>();
    private int currentIndex = 0;

    DFSPathIterator(final Vertex startVertex) {
      final TraversalPath initialPath = new TraversalPath(startVertex);
      performDFS(initialPath, 0);
    }

    private void performDFS(final TraversalPath path, final int depth) {
      final Vertex vertex = path.getEndVertex();

      // Add to results if depth is within bounds
      if (depth >= minHops && depth <= maxHops)
        results.add(path);

      // Stop if we've reached max depth
      if (depth >= maxHops)
        return;

      // Recursively explore neighbors
      for (final Edge edge : getEdges(vertex)) {
        if (!matchesTypeFilter(edge))
          continue;

        if (!matchesPropertyFilter(edge))
          continue;

        // Cypher relationship uniqueness: skip edges already used in this path
        if (detectCycles && pathContainsEdge(path, edge))
          continue;

        final Vertex nextVertex = getOtherVertex(edge, vertex);
        final TraversalPath newPath = new TraversalPath(path, edge, nextVertex);
        performDFS(newPath, depth + 1);
      }
    }

    private boolean pathContainsEdge(final TraversalPath path, final Edge edge) {
      final RID edgeRid = edge.getIdentity();
      for (final Edge pathEdge : path.getEdges())
        if (pathEdge.getIdentity().equals(edgeRid))
          return true;
      return false;
    }

    @Override
    public boolean hasNext() {
      return currentIndex < results.size();
    }

    @Override
    public TraversalPath next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      return results.get(currentIndex++);
    }
  }
}
