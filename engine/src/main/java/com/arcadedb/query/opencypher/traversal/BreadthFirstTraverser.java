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

import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.ast.Direction;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Set;

/**
 * Breadth-first graph traverser.
 * Explores the graph level by level, finding shortest paths first.
 * Ideal for finding shortest paths and bounded-depth traversals.
 */
public class BreadthFirstTraverser extends GraphTraverser {

  public BreadthFirstTraverser(final Direction direction, final String[] relationshipTypes, final int minHops,
      final int maxHops, final boolean trackPaths, final boolean detectCycles) {
    super(direction, relationshipTypes, minHops, maxHops, trackPaths, detectCycles);
  }

  @Override
  public Iterator<Vertex> traverse(final Vertex startVertex) {
    return new BFSVertexIterator(startVertex);
  }

  @Override
  public Iterator<TraversalPath> traversePaths(final Vertex startVertex) {
    return new BFSPathIterator(startVertex);
  }

  /**
   * Iterator for BFS vertex traversal.
   */
  private class BFSVertexIterator implements Iterator<Vertex> {
    private final Queue<VertexWithDepth> queue = new LinkedList<>();
    private final Set<Vertex> visited;
    private final List<Vertex> results = new ArrayList<>();
    private int currentIndex = 0;

    BFSVertexIterator(final Vertex startVertex) {
      this.visited = detectCycles ? createVisitedSet() : new HashSet<>();
      queue.add(new VertexWithDepth(startVertex, 0));

      // Perform full BFS traversal
      performTraversal();
    }

    private void performTraversal() {
      while (!queue.isEmpty()) {
        final VertexWithDepth current = queue.poll();
        final Vertex vertex = current.vertex;
        final int depth = current.depth;

        // Skip if already visited (cycle detection)
        if (detectCycles && isVisited(vertex, visited)) {
          continue;
        }

        // Mark as visited
        if (detectCycles) {
          markVisited(vertex, visited);
        }

        // Add to results if depth is within bounds
        if (depth >= minHops && depth <= maxHops) {
          results.add(vertex);
        }

        // Stop expanding if we've reached max depth
        if (depth >= maxHops) {
          continue;
        }

        // Expand to neighbors
        for (final Edge edge : getEdges(vertex)) {
          if (!matchesTypeFilter(edge)) {
            continue;
          }

          final Vertex nextVertex = getOtherVertex(edge, vertex);

          // Skip if already visited
          if (detectCycles && isVisited(nextVertex, visited)) {
            continue;
          }

          queue.add(new VertexWithDepth(nextVertex, depth + 1));
        }
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
   * Iterator for BFS path traversal.
   */
  private class BFSPathIterator implements Iterator<TraversalPath> {
    private final Queue<PathWithDepth> queue = new LinkedList<>();
    private final Set<Vertex> visited;
    private final List<TraversalPath> results = new ArrayList<>();
    private int currentIndex = 0;

    BFSPathIterator(final Vertex startVertex) {
      this.visited = detectCycles ? createVisitedSet() : new HashSet<>();
      final TraversalPath initialPath = new TraversalPath(startVertex);
      queue.add(new PathWithDepth(initialPath, 0));

      // Perform full BFS traversal
      performTraversal();
    }

    private void performTraversal() {
      while (!queue.isEmpty()) {
        final PathWithDepth current = queue.poll();
        final TraversalPath path = current.path;
        final int depth = current.depth;
        final Vertex vertex = path.getEndVertex();

        // Skip if already visited (cycle detection)
        if (detectCycles && isVisited(vertex, visited)) {
          continue;
        }

        // Mark as visited
        if (detectCycles) {
          markVisited(vertex, visited);
        }

        // Add to results if depth is within bounds
        if (depth >= minHops && depth <= maxHops) {
          results.add(path);
        }

        // Stop expanding if we've reached max depth
        if (depth >= maxHops) {
          continue;
        }

        // Expand to neighbors
        for (final Edge edge : getEdges(vertex)) {
          if (!matchesTypeFilter(edge)) {
            continue;
          }

          final Vertex nextVertex = getOtherVertex(edge, vertex);

          // Skip if already visited or creates cycle
          if (detectCycles && (isVisited(nextVertex, visited) || path.containsVertex(nextVertex))) {
            continue;
          }

          final TraversalPath newPath = new TraversalPath(path, edge, nextVertex);
          queue.add(new PathWithDepth(newPath, depth + 1));
        }
      }
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

  /**
   * Helper class to track vertex with its depth.
   */
  private static class VertexWithDepth {
    final Vertex vertex;
    final int depth;

    VertexWithDepth(final Vertex vertex, final int depth) {
      this.vertex = vertex;
      this.depth = depth;
    }
  }

  /**
   * Helper class to track path with its depth.
   */
  private static class PathWithDepth {
    final TraversalPath path;
    final int depth;

    PathWithDepth(final TraversalPath path, final int depth) {
      this.path = path;
      this.depth = depth;
    }
  }
}
