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

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * Base class for graph traversal implementations.
 * Provides common functionality for BFS, DFS, and variable-length path traversals.
 */
public abstract class GraphTraverser {
  protected final Direction direction;
  protected final String[] relationshipTypes;
  protected final int minHops;
  protected final int maxHops;
  protected final boolean trackPaths;
  protected final boolean detectCycles;

  /**
   * Creates a graph traverser with specified parameters.
   *
   * @param direction         direction to traverse (OUT, IN, BOTH)
   * @param relationshipTypes relationship types to follow (null = all types)
   * @param minHops           minimum number of hops
   * @param maxHops           maximum number of hops
   * @param trackPaths        whether to track full paths
   * @param detectCycles      whether to detect and avoid cycles
   */
  protected GraphTraverser(final Direction direction, final String[] relationshipTypes, final int minHops, final int maxHops,
      final boolean trackPaths, final boolean detectCycles) {
    this.direction = direction != null ? direction : Direction.BOTH;
    this.relationshipTypes = relationshipTypes;
    this.minHops = Math.max(0, minHops);
    this.maxHops = maxHops >= 0 ? maxHops : Integer.MAX_VALUE;
    this.trackPaths = trackPaths;
    this.detectCycles = detectCycles;

    // When minHops > maxHops, traversal naturally returns empty results
    // (no path can satisfy depth >= minHops AND depth <= maxHops)
  }

  /**
   * Traverses from a start vertex and returns matching end vertices.
   *
   * @param startVertex vertex to start traversal from
   * @return iterator of matching vertices
   */
  public abstract Iterator<Vertex> traverse(Vertex startVertex);

  /**
   * Traverses from a start vertex and returns matching paths.
   *
   * @param startVertex vertex to start traversal from
   * @return iterator of matching paths
   */
  public abstract Iterator<TraversalPath> traversePaths(Vertex startVertex);

  /**
   * Gets edges from a vertex based on direction and relationship types.
   * <p>
   * NOTE: Use getNextVertices() when you don't need the edge objects for better performance.
   *
   * @param vertex vertex to get edges from
   * @return iterable of matching edges
   */
  protected Iterable<Edge> getEdges(final Vertex vertex) {
    if (relationshipTypes == null || relationshipTypes.length == 0) {
      return vertex.getEdges(direction.toArcadeDirection());
    } else {
      return vertex.getEdges(direction.toArcadeDirection(), relationshipTypes);
    }
  }

  /**
   * Gets connected vertices directly without loading edge records.
   * <p>
   * PERFORMANCE: This is much faster than getEdges() + getOtherVertex()
   * because it skips loading the edge records entirely.
   *
   * @param vertex vertex to get connected vertices from
   * @return iterable of connected vertices
   */
  protected Iterable<Vertex> getNextVertices(final Vertex vertex) {
    if (relationshipTypes == null || relationshipTypes.length == 0) {
      return vertex.getVertices(direction.toArcadeDirection());
    } else {
      return vertex.getVertices(direction.toArcadeDirection(), relationshipTypes);
    }
  }

  /**
   * Gets the other vertex from an edge.
   *
   * @param edge   edge to traverse
   * @param from   starting vertex
   * @return other vertex
   */
  protected Vertex getOtherVertex(final Edge edge, final Vertex from) {
    final Vertex out = edge.getOutVertex();
    final Vertex in = edge.getInVertex();

    if (out.getIdentity().equals(from.getIdentity())) {
      return in;
    } else {
      return out;
    }
  }

  /**
   * Checks if a vertex matches the relationship type filter.
   *
   * @param edge edge to check
   * @return true if edge matches type filter
   */
  protected boolean matchesTypeFilter(final Edge edge) {
    if (relationshipTypes == null || relationshipTypes.length == 0) {
      return true;
    }

    final String edgeType = edge.getTypeName();
    for (final String type : relationshipTypes) {
      if (type.equals(edgeType)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Creates a visited set for cycle detection.
   * Uses RIDs instead of Vertex objects for efficient O(1) hash lookups.
   *
   * @return new hash set for tracking visited vertex RIDs
   */
  protected Set<RID> createVisitedSet() {
    return new HashSet<>();
  }

  /**
   * Checks if a vertex has been visited.
   * O(1) hash lookup using RID.
   * Creates a database-independent RID to ensure proper equality/hashCode.
   *
   * @param vertex  vertex to check
   * @param visited set of visited vertex RIDs
   * @return true if visited
   */
  protected boolean isVisited(final Vertex vertex, final Set<RID> visited) {
    final RID rid = vertex.getIdentity();
    // Create database-independent RID for consistent equals/hashCode
    return visited.contains(new RID(rid.getBucketId(), rid.getPosition()));
  }

  /**
   * Marks a vertex as visited.
   * Stores only the RID for memory efficiency and O(1) lookups.
   * Creates a database-independent RID to ensure proper equality/hashCode.
   *
   * @param vertex  vertex to mark
   * @param visited set of visited vertex RIDs
   */
  protected void markVisited(final Vertex vertex, final Set<RID> visited) {
    final RID rid = vertex.getIdentity();
    // Create database-independent RID for consistent equals/hashCode
    visited.add(new RID(rid.getBucketId(), rid.getPosition()));
  }
}
