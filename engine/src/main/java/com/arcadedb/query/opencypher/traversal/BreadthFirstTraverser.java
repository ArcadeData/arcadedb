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

import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.GhostEdgeReporter;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.ast.Direction;
import com.arcadedb.query.opencypher.ast.PathMode;
import com.arcadedb.utility.RidHashSet;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;

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

  public BreadthFirstTraverser(final Direction direction, final String[] relationshipTypes,
      final Map<String, Object> edgePropertyFilters, final int minHops,
      final int maxHops, final boolean trackPaths, final boolean detectCycles) {
    super(direction, relationshipTypes, edgePropertyFilters, minHops, maxHops, trackPaths, detectCycles);
  }

  public BreadthFirstTraverser(final Direction direction, final String[] relationshipTypes,
      final Map<String, Object> edgePropertyFilters, final int minHops,
      final int maxHops, final boolean trackPaths, final PathMode pathMode) {
    super(direction, relationshipTypes, edgePropertyFilters, minHops, maxHops, trackPaths, pathMode);
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
   * Walks the pattern as a reachability search and answers each end vertex once, however many paths lead to it (issue #10062).
   * <p>
   * Only for a caller that needs the set of vertices some path of {@code minHops..maxHops} relationships reaches, and
   * only for {@code minHops <= 1}: then the shortest walk to a vertex other than the start is a simple path, which is a
   * trail and an acyclic path as well, so it exists exactly when the vertex is found at all, whatever the path mode. A vertex is
   * expanded once, from its shallowest depth, where it has the most hops left to spend. The traversal is lazy and level by
   * level, so a {@code LIMIT} stops it early and its memory is the widest level, not the number of paths.
   * <p>
   * The start vertex is the exception, because getting back to it means leaving along one relationship and returning along
   * another. With a lower bound of 0 it is the first end vertex. With a lower bound of 1 it is an end vertex when a cycle leads
   * back to it: any closed walk (WALK), never (ACYCLIC), and a shortest closed walk of a directed pattern is a simple cycle, which
   * is a trail (TRAIL). An undirected TRAIL pattern is the one case a visited set cannot answer (the shortest way back is the
   * relationship it left along, which a trail may not repeat), so the caller must not ask for it.
   */
  public Iterator<Vertex> traverseDistinctEndpoints(final Vertex startVertex) {
    if (minHops > 1)
      throw new IllegalStateException("A distinct-endpoint traversal needs a lower bound of 0 or 1, got " + minHops);
    if (minHops == 1 && direction == Direction.BOTH && pathMode == PathMode.TRAIL)
      throw new IllegalStateException("A distinct-endpoint traversal cannot decide an undirected trail pattern with a lower bound of 1");
    return new DistinctEndpointIterator(startVertex);
  }

  private final class DistinctEndpointIterator implements Iterator<Vertex> {
    private final RidHashSet seen = createVisitedSet();
    private final Vertex start;
    private final boolean readEdges = (edgePropertyFilters != null && !edgePropertyFilters.isEmpty()) || edgePredicate != null;
    private final boolean startIsEndOnReturn = minHops == 1 && pathMode != PathMode.ACYCLIC;
    private final ArrayDeque<Vertex> ready = new ArrayDeque<>();
    private List<Vertex> level = new ArrayList<>();
    private int depth;
    private boolean startReported;

    DistinctEndpointIterator(final Vertex start) {
      this.start = start;
      level.add(start);
      // The start is never found as a new vertex: it is reported at depth 0 (lower bound 0) or when a cycle leads back to it
      markVisited(start, seen);
      if (minHops == 0)
        ready.add(start);
    }

    @Override
    public boolean hasNext() {
      while (ready.isEmpty() && !level.isEmpty() && depth < maxHops)
        expandLevel();
      return !ready.isEmpty();
    }

    @Override
    public Vertex next() {
      if (!hasNext())
        throw new NoSuchElementException();
      return ready.poll();
    }

    private void expandLevel() {
      final List<Vertex> next = new ArrayList<>();
      for (final Vertex vertex : level) {
        if (readEdges) {
          for (final Edge edge : getEdges(vertex)) {
            try {
              if (matchesTypeFilter(edge) && matchesPropertyFilter(edge) && matchesEdgePredicate(edge))
                reach(getOtherVertex(edge, vertex), next);
            } catch (final RecordNotFoundException e) {
              GhostEdgeReporter.reportSkipped(e);
            }
          }
        } else
          for (final Vertex neighbour : getNextVertices(vertex))
            reach(neighbour, next);
      }
      level = next;
      depth++;
    }

    private void reach(final Vertex vertex, final List<Vertex> next) {
      if (isVisited(vertex, seen)) {
        // A cycle back to the start makes it an end vertex of a pattern whose lower bound is 1, once; its neighbours were all
        // explored from depth 0
        if (startIsEndOnReturn && !startReported && vertex.getIdentity().equals(start.getIdentity())) {
          startReported = true;
          ready.add(vertex);
        }
        return;
      }
      markVisited(vertex, seen);
      ready.add(vertex);
      next.add(vertex);
    }
  }

  /**
   * Iterator for BFS vertex traversal.
   */
  private class BFSVertexIterator implements Iterator<Vertex> {
    private final Queue<VertexWithDepth> queue = new LinkedList<>();
    private final RidHashSet visited;
    private final List<Vertex> results = new ArrayList<>();
    private int currentIndex = 0;

    BFSVertexIterator(final Vertex startVertex) {
      this.visited = createVisitedSet();
      queue.add(new VertexWithDepth(startVertex, 0));

      // Perform full BFS traversal
      performTraversal();
    }

    private void performTraversal() {
      // Mark start vertex as visited
      if (detectCycles) {
        markVisited(queue.peek().vertex, visited);
      }

      while (!queue.isEmpty()) {
        final VertexWithDepth current = queue.poll();
        final Vertex vertex = current.vertex;
        final int depth = current.depth;

        // Add to results if depth is within bounds
        if (depth >= minHops && depth <= maxHops) {
          results.add(vertex);
        }

        // Stop expanding if we've reached max depth
        if (depth >= maxHops) {
          continue;
        }

        // Expand to neighbors using fast getNextVertices() (skips loading edge records)
        for (final Vertex nextVertex : getNextVertices(vertex)) {
          // Skip if already visited - mark as visited when adding to queue
          if (detectCycles && isVisited(nextVertex, visited)) {
            continue;
          }

          // Mark as visited BEFORE adding to queue (prevents duplicates)
          if (detectCycles) {
            markVisited(nextVertex, visited);
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
   * <p>
   * Lazy: each call to {@code next()} advances the frontier queue only as far as needed to
   * produce one more result, instead of running the whole traversal to completion up front and
   * collecting every matching path into a list before the first row is available (#6097). Note
   * this still does not bound peak memory for a uniformly-branching pattern: level-order
   * expansion via a single FIFO queue necessarily enqueues an entire level's children before the
   * first child can be dequeued, so the queue itself can grow as large as the widest level. DFS
   * (see {@link DepthFirstTraverser}) is the traversal that gives a real, branching-independent
   * memory bound and is what {@code ExpandPathStep} now defaults to; this laziness mainly avoids
   * doubling up on storage (queue and results list) and lets a downstream LIMIT stop a
   * BFS-ordered (shortest-first) traversal early.
   */
  private class BFSPathIterator implements Iterator<TraversalPath> {
    private final Queue<PathWithDepth> queue = new LinkedList<>();
    private TraversalPath nextResult;

    BFSPathIterator(final Vertex startVertex) {
      queue.add(new PathWithDepth(new TraversalPath(startVertex), 0));
      advance();
    }

    private void advance() {
      nextResult = null;
      while (!queue.isEmpty()) {
        final PathWithDepth current = queue.poll();
        final TraversalPath path = current.path;
        final int depth = current.depth;
        final Vertex vertex = path.getEndVertex();

        // Expand to neighbors, unless we've reached max depth
        if (depth < maxHops) {
          for (final Edge edge : getEdges(vertex)) {
            try {
              if (!matchesTypeFilter(edge))
                continue;

              if (!matchesPropertyFilter(edge))
                continue;

              if (!matchesEdgePredicate(edge))
                continue;

              // Path mode: TRAIL/ACYCLIC = edge uniqueness, WALK = no restriction
              if (pathMode != PathMode.WALK && path.containsEdge(edge))
                continue;

              final Vertex nextVertex = getOtherVertex(edge, vertex);

              // ACYCLIC: also enforce vertex uniqueness
              if (pathMode == PathMode.ACYCLIC && path.containsVertex(nextVertex))
                continue;

              final TraversalPath newPath = new TraversalPath(path, edge, nextVertex);
              queue.add(new PathWithDepth(newPath, depth + 1));
            } catch (final RecordNotFoundException e) {
              GhostEdgeReporter.reportSkipped(e);
            }
          }
        }

        // Emit this path if its depth is within bounds
        if (depth >= minHops && depth <= maxHops) {
          nextResult = path;
          return;
        }
      }
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
