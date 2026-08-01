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
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link GraphAnalyticalView} overrides {@code countEdgesBetween}, so the SPI's default implementation is
 * the one a future provider inherits and the one nothing in the tree exercises. It carries a load-bearing
 * assumption - that {@link GraphTraversalProvider#getNeighborIds} returns the raw adjacency entries, so an
 * undirected self-loop appears once per list and has to be halved - and an assumption only a test can hold.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class GraphTraversalProviderCountEdgesBetweenTest {

  /**
   * The minimum provider the default needs: adjacency ids per direction, raw, exactly as the contract
   * spells them. Node 0 is joined to node 1 by three edges out and one back, and carries two self-loops.
   */
  private static final class RawAdjacencyProvider implements GraphTraversalProvider {
    @Override
    public int[] getNeighborIds(final int nodeId, final Vertex.DIRECTION direction, final String... edgeTypes) {
      if (nodeId != 0)
        return new int[0];
      // three 0->1 edges, plus two self-loops on 0, held once in each list as the engine stores them
      return switch (direction) {
        case OUT -> new int[] { 0, 0, 1, 1, 1 };
        case IN -> new int[] { 0, 0, 1 };
        case BOTH -> new int[] { 0, 0, 0, 0, 1, 1, 1, 1 };
      };
    }

    @Override
    public int getNodeCount() {
      return 2;
    }

    @Override
    public boolean isReady() {
      return true;
    }

    @Override
    public String getName() {
      return "raw-adjacency";
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
    public long countEdges(final int nodeId, final Vertex.DIRECTION direction, final String... edgeTypes) {
      return getNeighborIds(nodeId, direction, edgeTypes).length;
    }

    @Override
    public boolean isConnectedTo(final int nodeA, final int nodeB, final Vertex.DIRECTION direction,
        final String... edgeTypes) {
      for (final int neighbor : getNeighborIds(nodeA, direction, edgeTypes))
        if (neighbor == nodeB)
          return true;
      return false;
    }

    @Override
    public Object getProperty(final int nodeId, final String propertyName) {
      return null;
    }
  }

  @Test
  void theDefaultCountsEveryParallelEdgeAndEachSelfLoopOnce() {
    final GraphTraversalProvider provider = new RawAdjacencyProvider();

    // a pair joined several times contributes one per edge, in each direction and in both
    assertThat(provider.countEdgesBetween(0, 1, Vertex.DIRECTION.OUT)).isEqualTo(3);
    assertThat(provider.countEdgesBetween(0, 1, Vertex.DIRECTION.IN)).isEqualTo(1);
    assertThat(provider.countEdgesBetween(0, 1, Vertex.DIRECTION.BOTH)).isEqualTo(4);

    // a self-loop is held by both lists, so an undirected walk sees each of the two twice and must not
    // report four relationships where there are two
    assertThat(provider.countEdgesBetween(0, 0, Vertex.DIRECTION.OUT)).isEqualTo(2);
    assertThat(provider.countEdgesBetween(0, 0, Vertex.DIRECTION.IN)).isEqualTo(2);
    assertThat(provider.countEdgesBetween(0, 0, Vertex.DIRECTION.BOTH)).isEqualTo(2);

    // and a pair nothing joins is zero, never negative: the default always knows the answer
    assertThat(provider.countEdgesBetween(1, 0, Vertex.DIRECTION.OUT)).isZero();
  }
}
