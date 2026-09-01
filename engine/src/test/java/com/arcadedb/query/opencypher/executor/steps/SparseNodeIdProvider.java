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
package com.arcadedb.query.opencypher.executor.steps;

import com.arcadedb.database.RID;
import com.arcadedb.graph.GraphTraversalProvider;
import com.arcadedb.graph.NeighborView;
import com.arcadedb.graph.Vertex;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Test provider whose node ID space contains one deleted slot and one live ID above the live-node count.
 * Per-node access fails on the deleted slot so a whole-ID-space scan must honor {@link #isNodeLive(int)}.
 */
final class SparseNodeIdProvider implements GraphTraversalProvider {
  static final int HOLE = 1;
  static final int HIGH_ID = 5;
  static final int UPPER_BOUND = 6;

  private final Map<String, int[][]> adjacency = new HashMap<>();
  private boolean exposeViews = true;

  SparseNodeIdProvider withEdges(final String edgeType, final Vertex.DIRECTION direction,
      final int nodeId, final int... neighbors) {
    adjacency.computeIfAbsent(key(direction, edgeType), ignored -> emptyAdjacency())[nodeId] = neighbors;
    return this;
  }

  SparseNodeIdProvider withoutViews() {
    exposeViews = false;
    return this;
  }

  @Override
  public int getNodeCount() {
    return UPPER_BOUND - 1;
  }

  @Override
  public int getNodeIdUpperBound() {
    return UPPER_BOUND;
  }

  @Override
  public boolean isNodeLive(final int nodeId) {
    return nodeId >= 0 && nodeId < UPPER_BOUND && nodeId != HOLE;
  }

  @Override
  public boolean isReady() {
    return true;
  }

  @Override
  public String getName() {
    return "sparse-node-id-test-provider";
  }

  @Override
  public boolean coversVertexType(final String typeName) {
    return true;
  }

  @Override
  public boolean coversEdgeType(final String edgeTypeName) {
    return true;
  }

  @Override
  public int getNodeId(final RID rid) {
    return (int) rid.getPosition();
  }

  @Override
  public RID getRID(final int nodeId) {
    assertLive(nodeId);
    return new RID(7, nodeId);
  }

  @Override
  public int[] getNeighborIds(final int nodeId, final Vertex.DIRECTION direction, final String... edgeTypes) {
    assertLive(nodeId);
    final int[][] rows = adjacency.get(key(direction, edgeTypes));
    return rows != null ? rows[nodeId] : new int[0];
  }

  @Override
  public long countEdges(final int nodeId, final Vertex.DIRECTION direction, final String... edgeTypes) {
    return getNeighborIds(nodeId, direction, edgeTypes).length;
  }

  @Override
  public void getDegrees(final int[] degrees, final Vertex.DIRECTION direction, final String edgeType) {
    Arrays.fill(degrees, 0);
    for (int nodeId = 0; nodeId < degrees.length; nodeId++)
      if (isNodeLive(nodeId))
        degrees[nodeId] = (int) countEdges(nodeId, direction, edgeType);
  }

  @Override
  public boolean isConnectedTo(final int nodeA, final int nodeB, final Vertex.DIRECTION direction,
      final String... edgeTypes) {
    return Arrays.binarySearch(getNeighborIds(nodeA, direction, edgeTypes), nodeB) >= 0;
  }

  @Override
  public Object getProperty(final int nodeId, final String propertyName) {
    return null;
  }

  @Override
  public NeighborView getNeighborView(final Vertex.DIRECTION direction, final String... edgeTypes) {
    if (!exposeViews)
      return null;
    final int[][] rows = adjacency.get(key(direction, edgeTypes));
    return rows != null ? toView(rows) : null;
  }

  private static String key(final Vertex.DIRECTION direction, final String... edgeTypes) {
    return direction + ":" + String.join(",", edgeTypes);
  }

  private void assertLive(final int nodeId) {
    if (!isNodeLive(nodeId))
      throw new AssertionError("attempted per-node access for deleted node ID " + nodeId);
  }

  private static int[][] emptyAdjacency() {
    final int[][] rows = new int[UPPER_BOUND][];
    Arrays.setAll(rows, ignored -> new int[0]);
    return rows;
  }

  private static NeighborView toView(final int[][] rows) {
    final int[] offsets = new int[UPPER_BOUND + 1];
    int edgeCount = 0;
    for (int nodeId = 0; nodeId < UPPER_BOUND; nodeId++) {
      offsets[nodeId] = edgeCount;
      edgeCount += rows[nodeId].length;
    }
    offsets[UPPER_BOUND] = edgeCount;
    final int[] neighbors = new int[edgeCount];
    int position = 0;
    for (final int[] row : rows) {
      System.arraycopy(row, 0, neighbors, position, row.length);
      position += row.length;
    }
    return new NeighborView(UPPER_BOUND, offsets, neighbors);
  }
}
