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
import com.arcadedb.query.sql.executor.WorkGuard;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #6943: {@link PartitionedTriangleOp#execute} answered 0 for the whole query whenever
 * the partition-chain {@link NeighborView} could not be handed out - which is the normal state of a
 * {@code GraphAnalyticalView} between commits (an active delta overlay makes {@code getNeighborView} return null
 * unconditionally), not an exotic condition.
 * <p>
 * {@code buildPartitionMapping} used to bail out to an all-{@code -1} mapping the moment any partition-chain view
 * was null; every consumer reads {@code -1} as "this node belongs to no partition", so the triangle count came back
 * 0 even though the triangle-edge view ({@code KNOWS}) and the underlying graph data were both perfectly fine.
 * <p>
 * The fix keeps the mapping on the CSR node-id path by falling back to {@link GraphTraversalProvider#getNeighborIds}
 * for the partition-chain walk - the same fallback {@code countTrianglesPerNode} already uses when the
 * triangle-edge view itself is missing - instead of silently producing invalid partition data.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class PartitionedTriangleOpMissingPartitionViewTest {

  private static final int NODE_COUNT = 12;

  /** Provider whose partition-chain view ("IN_CITY") is unavailable (simulating an active delta overlay),
   *  while the triangle-edge view ("KNOWS") and the per-node accessor are both fully functional. */
  private static final class OverlayActiveStubProvider implements GraphTraversalProvider {
    private final NeighborView knowsView;
    private final int[][] cityNeighbors;

    private OverlayActiveStubProvider(final NeighborView knowsView, final int[][] cityNeighbors) {
      this.knowsView = knowsView;
      this.cityNeighbors = cityNeighbors;
    }

    @Override
    public int getNodeCount() {
      return NODE_COUNT;
    }

    @Override
    public NeighborView getNeighborView(final Vertex.DIRECTION direction, final String... edgeTypes) {
      if (edgeTypes.length == 1 && "KNOWS".equals(edgeTypes[0]))
        return knowsView;
      // #6943: the partition-chain view is unavailable - this is what an active delta overlay looks like.
      return null;
    }

    @Override
    public boolean isReady() {
      return true;
    }

    @Override
    public String getName() {
      return "stub-overlay-active";
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
      return -1;
    }

    @Override
    public RID getRID(final int nodeId) {
      return null;
    }

    @Override
    public int[] getNeighborIds(final int nodeId, final Vertex.DIRECTION direction, final String... edgeTypes) {
      if (edgeTypes.length == 1 && "IN_CITY".equals(edgeTypes[0]))
        return cityNeighbors[nodeId];
      throw new UnsupportedOperationException();
    }

    @Override
    public long countEdges(final int nodeId, final Vertex.DIRECTION direction, final String... edgeTypes) {
      return getNeighborIds(nodeId, direction, edgeTypes).length;
    }

    @Override
    public boolean isConnectedTo(final int nodeA, final int nodeB, final Vertex.DIRECTION direction,
        final String... edgeTypes) {
      return false;
    }

    @Override
    public Object getProperty(final int nodeId, final String propertyName) {
      return null;
    }
  }

  @Test
  void missingPartitionViewFallsBackToPerNodeLookupInsteadOfReturningZero() {
    // Two disjoint triangles: (0,1,2) all in city 10, (3,4,5) all in city 11. Nodes 6..11 are isolated.
    final int[][] knowsAdjacency = new int[NODE_COUNT][];
    for (int i = 0; i < NODE_COUNT; i++)
      knowsAdjacency[i] = new int[0];
    knowsAdjacency[0] = new int[] { 1, 2 };
    knowsAdjacency[1] = new int[] { 0, 2 };
    knowsAdjacency[2] = new int[] { 0, 1 };
    knowsAdjacency[3] = new int[] { 4, 5 };
    knowsAdjacency[4] = new int[] { 3, 5 };
    knowsAdjacency[5] = new int[] { 3, 4 };

    int edgeCount = 0;
    for (final int[] adj : knowsAdjacency)
      edgeCount += adj.length;
    final int[] offsets = new int[NODE_COUNT + 1];
    final int[] neighbors = new int[edgeCount];
    int pos = 0;
    for (int i = 0; i < NODE_COUNT; i++) {
      offsets[i] = pos;
      for (final int n : knowsAdjacency[i])
        neighbors[pos++] = n;
    }
    offsets[NODE_COUNT] = pos;
    final NeighborView knowsView = new NeighborView(NODE_COUNT, offsets, neighbors);

    // IN_CITY chain: every node points to its own city node id (10 or 11) - only reachable via getNeighborIds
    // since getNeighborView("IN_CITY") returns null.
    final int[][] cityNeighbors = new int[NODE_COUNT][];
    for (int i = 0; i < NODE_COUNT; i++)
      cityNeighbors[i] = new int[0];
    cityNeighbors[0] = new int[] { 10 };
    cityNeighbors[1] = new int[] { 10 };
    cityNeighbors[2] = new int[] { 10 };
    cityNeighbors[3] = new int[] { 11 };
    cityNeighbors[4] = new int[] { 11 };
    cityNeighbors[5] = new int[] { 11 };

    final PartitionedTriangleOp op = new PartitionedTriangleOp(new String[] { "IN_CITY" },
        new Vertex.DIRECTION[] { Vertex.DIRECTION.OUT }, "KNOWS");

    final long count = op.execute(new OverlayActiveStubProvider(knowsView, cityNeighbors), null,
        WorkGuard.forCommandDeadline(null));

    // Each triangle is counted 6 times (each of its 3 nodes as u, each of its 2 neighbors as v) = 12 total,
    // NOT 0 - the bug returned 0 because the partition mapping degraded to all -1.
    assertThat(count).as("a missing partition-chain view must not silently zero out every partition membership")
        .isEqualTo(12L);
  }
}
