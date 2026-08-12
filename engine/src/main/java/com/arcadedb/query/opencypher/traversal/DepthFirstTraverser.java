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
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.GhostEdgeReporter;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.ast.Direction;
import com.arcadedb.query.opencypher.ast.PathMode;
import com.arcadedb.utility.RidHashSet;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

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

  public DepthFirstTraverser(final Direction direction, final String[] relationshipTypes,
      final Map<String, Object> edgePropertyFilters, final int minHops,
      final int maxHops, final boolean trackPaths, final PathMode pathMode) {
    super(direction, relationshipTypes, edgePropertyFilters, minHops, maxHops, trackPaths, pathMode);
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
      final RidHashSet visited = createVisitedSet();
      performDFS(startVertex, 0, visited);
    }

    private void performDFS(final Vertex vertex, final int depth, final RidHashSet visited) {
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
   * <p>
   * Genuinely lazy: expands one branch at a time via an explicit stack instead of recursing to
   * completion and collecting every matching path into a list up front. A path is emitted the
   * moment it is discovered (pre-order), and only the current root-to-frontier chain - at most
   * {@code maxHops} frames - is ever on the stack, regardless of how many distinct paths the
   * pattern has in total. (Each frame's {@link TraversalPath} still copies its parent's vertex/edge
   * arrays, so per-frame size grows with depth: active memory is bounded by {@code maxHops} frames
   * of up to {@code maxHops} elements each - {@code O(maxHops^2)} in the worst case, not a flat
   * {@code O(maxHops)} - but that is no longer combinatorial in the branching factor, which is what
   * made eager enumeration unbounded.) Recursive eager enumeration used to hold every matching path
   * simultaneously, which grows combinatorially with the branching factor (see #6097).
   */
  private class DFSPathIterator implements Iterator<TraversalPath> {
    private final Deque<Frame> stack = new ArrayDeque<>();
    private TraversalPath nextResult;

    DFSPathIterator(final Vertex startVertex) {
      stack.push(new Frame(new TraversalPath(startVertex), 0));
      advance();
    }

    /**
     * Advances the stack until the next path in [minHops, maxHops] is found (pre-order), or the
     * stack is exhausted. Each frame is visited once for emission (when its edge iterator is
     * first created) and then revisited only to pull its next unexplored edge.
     */
    private void advance() {
      nextResult = null;
      while (!stack.isEmpty()) {
        final Frame frame = stack.peek();

        if (frame.edges == null) {
          frame.edges = frame.depth < maxHops ? getEdges(frame.path.getEndVertex()).iterator() : Collections.emptyIterator();
          if (frame.depth >= minHops && frame.depth <= maxHops) {
            nextResult = frame.path;
            return;
          }
        }

        if (frame.edges.hasNext()) {
          final Edge edge = frame.edges.next();
          try {
            if (!matchesTypeFilter(edge))
              continue;

            if (!matchesPropertyFilter(edge))
              continue;

            if (!matchesEdgePredicate(edge))
              continue;

            // Path mode: TRAIL/ACYCLIC = edge uniqueness, WALK = no restriction
            if (pathMode != PathMode.WALK && pathContainsEdge(frame.path, edge))
              continue;

            final Vertex nextVertex = getOtherVertex(edge, frame.path.getEndVertex());

            // ACYCLIC: also enforce vertex uniqueness
            if (pathMode == PathMode.ACYCLIC && frame.path.containsVertex(nextVertex))
              continue;

            stack.push(new Frame(new TraversalPath(frame.path, edge, nextVertex), frame.depth + 1));
          } catch (final RecordNotFoundException e) {
            GhostEdgeReporter.reportSkipped(e);
          }
        } else {
          stack.pop();
        }
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
      return nextResult != null;
    }

    @Override
    public TraversalPath next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      final TraversalPath result = nextResult;
      advance();
      return result;
    }
  }

  /**
   * One in-progress node in the DFS stack: the path reaching it, its depth, and a lazily-created
   * iterator over its outgoing edges that resumes exactly where the last visit left off.
   */
  private static final class Frame {
    final TraversalPath path;
    final int           depth;
    Iterator<Edge>       edges;

    Frame(final TraversalPath path, final int depth) {
      this.path = path;
      this.depth = depth;
    }
  }
}
