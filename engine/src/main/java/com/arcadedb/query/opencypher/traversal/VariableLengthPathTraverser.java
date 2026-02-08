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

import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.ast.Direction;

import java.util.Iterator;
import java.util.Map;

/**
 * Specialized traverser for variable-length path patterns.
 * Handles patterns like -[*min..max]-> efficiently.
 * <p>
 * Uses BFS for shortest paths (default) or DFS for exhaustive exploration.
 */
public class VariableLengthPathTraverser extends GraphTraverser {
  private final boolean useBFS;

  /**
   * Creates a variable-length path traverser.
   *
   * @param direction         direction to traverse
   * @param relationshipTypes relationship types to follow
   * @param minHops           minimum number of hops
   * @param maxHops           maximum number of hops
   * @param trackPaths        whether to track full paths
   * @param useBFS            true for BFS (shortest paths), false for DFS
   */
  public VariableLengthPathTraverser(final Direction direction, final String[] relationshipTypes, final int minHops,
      final int maxHops, final boolean trackPaths, final boolean useBFS) {
    super(direction, relationshipTypes, minHops, maxHops, trackPaths, true); // Always detect cycles
    this.useBFS = useBFS;
  }

  public VariableLengthPathTraverser(final Direction direction, final String[] relationshipTypes,
      final Map<String, Object> edgePropertyFilters, final int minHops,
      final int maxHops, final boolean trackPaths, final boolean useBFS) {
    super(direction, relationshipTypes, edgePropertyFilters, minHops, maxHops, trackPaths, true);
    this.useBFS = useBFS;
  }

  /**
   * Creates a variable-length path traverser with BFS (default).
   *
   * @param direction         direction to traverse
   * @param relationshipTypes relationship types to follow
   * @param minHops           minimum number of hops
   * @param maxHops           maximum number of hops
   * @param trackPaths        whether to track full paths
   */
  public VariableLengthPathTraverser(final Direction direction, final String[] relationshipTypes, final int minHops,
      final int maxHops, final boolean trackPaths) {
    this(direction, relationshipTypes, minHops, maxHops, trackPaths, true);
  }

  @Override
  public Iterator<Vertex> traverse(final Vertex startVertex) {
    if (useBFS) {
      final BreadthFirstTraverser bfs = new BreadthFirstTraverser(direction, relationshipTypes, edgePropertyFilters,
          minHops, maxHops, trackPaths, detectCycles);
      return bfs.traverse(startVertex);
    } else {
      final DepthFirstTraverser dfs = new DepthFirstTraverser(direction, relationshipTypes, edgePropertyFilters,
          minHops, maxHops, trackPaths, detectCycles);
      return dfs.traverse(startVertex);
    }
  }

  @Override
  public Iterator<TraversalPath> traversePaths(final Vertex startVertex) {
    if (useBFS) {
      final BreadthFirstTraverser bfs = new BreadthFirstTraverser(direction, relationshipTypes, edgePropertyFilters,
          minHops, maxHops, trackPaths, detectCycles);
      return bfs.traversePaths(startVertex);
    } else {
      final DepthFirstTraverser dfs = new DepthFirstTraverser(direction, relationshipTypes, edgePropertyFilters,
          minHops, maxHops, trackPaths, detectCycles);
      return dfs.traversePaths(startVertex);
    }
  }

  /**
   * Returns true if using BFS strategy.
   *
   * @return true if BFS
   */
  public boolean isUsingBFS() {
    return useBFS;
  }
}
