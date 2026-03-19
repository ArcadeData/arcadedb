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
package com.arcadedb.graph;

import com.arcadedb.database.RID;

/**
 * SPI for accelerated graph traversal providers (e.g., Graph Analytical Views backed by CSR).
 * <p>
 * The query planner discovers registered providers via {@link GraphTraversalProviderRegistry}
 * and uses them for neighbor expansion when:
 * <ul>
 *   <li>The provider is ready ({@link #isReady()})</li>
 *   <li>The provider covers the required edge types ({@link #coversEdgeType(String)})</li>
 *   <li>The query does not need the edge object itself (no edge variable captured)</li>
 * </ul>
 * <p>
 * Providers map ArcadeDB RIDs to dense integer IDs for O(1) neighbor lookup
 * via CSR (Compressed Sparse Row) arrays, bypassing the OLTP edge linked lists.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public interface GraphTraversalProvider {

  /**
   * Returns the total number of nodes in this provider's CSR structure.
   * Dense node IDs range from 0 (inclusive) to getNodeCount() (exclusive).
   */
  int getNodeCount();

  /**
   * Returns true if this provider is ready to serve queries.
   * Providers that are still building should return false.
   */
  boolean isReady();

  /**
   * Returns the name of this provider.
   */
  String getName();

  /**
   * Returns true if this provider covers the given vertex type.
   * A null type name means "all types", which returns true only if the provider includes all vertex types.
   */
  boolean coversVertexType(String typeName);

  /**
   * Returns true if this provider covers the given edge type.
   * A null type name means "all types", which returns true only if the provider includes all edge types.
   */
  boolean coversEdgeType(String edgeTypeName);

  /**
   * Returns the dense node ID for a RID, or -1 if not mapped.
   */
  int getNodeId(RID rid);

  /**
   * Returns the RID for a dense node ID.
   */
  RID getRID(int nodeId);

  /**
   * Returns neighbor dense node IDs for a given node, direction, and edge types.
   * This is the primary acceleration method — O(1) array access vs O(n) linked list traversal.
   */
  int[] getNeighborIds(int nodeId, Vertex.DIRECTION direction, String... edgeTypes);

  /**
   * Returns the edge count for a node, direction, and edge types.
   * Avoids materializing the neighbor array when only the count is needed.
   */
  long countEdges(int nodeId, Vertex.DIRECTION direction, String... edgeTypes);

  /**
   * Checks if nodeA is connected to nodeB via the given direction and edge types.
   * Uses binary search on sorted CSR arrays — O(log(degree)).
   */
  boolean isConnectedTo(int nodeA, int nodeB, Vertex.DIRECTION direction, String... edgeTypes);

  /**
   * Returns a property value from columnar storage, or null if not materialized.
   */
  Object getProperty(int nodeId, String propertyName);

  /**
   * Returns true if this provider has edge property columns materialized.
   * When true, {@link #getEdgeProperty} can be used to retrieve edge properties from CSR storage.
   */
  default boolean hasEdgeProperties() {
    return false;
  }

  /**
   * Returns an edge property value from columnar storage, or null if not materialized.
   * <p>
   * The {@code neighborIndex} is the position within the node's adjacency list for the given direction
   * (0-based, matching the order returned by {@link #getNeighborIds}).
   *
   * @param nodeId         the source node's dense ID
   * @param neighborIndex  the index within the node's neighbor list for the given direction
   * @param direction      OUT or IN
   * @param edgeType       the edge type name
   * @param propertyName   the property to retrieve
   *
   * @return the property value, or null if not available
   */
  default Object getEdgeProperty(final int nodeId, final int neighborIndex,
      final Vertex.DIRECTION direction, final String edgeType, final String propertyName) {
    return null;
  }

  /**
   * Returns true if this provider's data is stale (not reflecting latest committed changes).
   * A provider may still be ready ({@link #isReady()}) while stale, if configured to serve stale data.
   */
  default boolean isStale() {
    return false;
  }

  /**
   * Returns a packed {@link NeighborView} for zero-allocation iteration over all nodes' neighbors,
   * or {@code null} if this provider does not support the optimization (e.g., when overlays are active).
   * <p>
   * When available, algorithms should prefer this over per-node {@link #getNeighborIds} calls
   * to avoid O(N) array allocations.
   */
  default NeighborView getNeighborView(final Vertex.DIRECTION direction, final String... edgeTypes) {
    return null;
  }

  /**
   * Bulk degree computation: fills {@code degrees[nodeId]} with the edge count for each node.
   * <p>
   * Default implementation calls {@link #countEdges} per node. CSR-backed providers override
   * this to compute degrees directly from offset arrays in a single pass, avoiding per-node
   * HashMap lookups and method dispatch overhead.
   * <p>
   * For star-join queries (Q4/Q7), this reduces 5M × 150ns/call = 750ms to a single
   * array scan at ~5M × 2ns = 10ms per edge type.
   */
  default void getDegrees(final int[] degrees, final Vertex.DIRECTION direction, final String edgeType) {
    for (int v = 0; v < degrees.length; v++)
      degrees[v] = (int) countEdges(v, direction, edgeType);
  }
}
