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
package com.arcadedb.grapholap;

import com.arcadedb.database.Database;
import com.arcadedb.database.RID;
import com.arcadedb.graph.Vertex;

import java.util.Arrays;

/**
 * Graph Analytical View (GAV) — a synchronized, read-optimized CSR representation of the OLTP graph.
 * <p>
 * This is the main entry point for the Graph OLAP module. It materializes part or all of the graph
 * as a CSR adjacency index for high-performance analytical operations:
 * <ul>
 *   <li>Multi-hop traversals: O(degree) per hop via sequential array access</li>
 *   <li>Neighbor intersection: sorted CSR enables O(d1+d2) set intersection</li>
 *   <li>Vectorized scans: batches of 2048 neighbors per operator call</li>
 *   <li>Graph algorithms: PageRank, BFS, Connected Components on flat arrays</li>
 * </ul>
 * <p>
 * All internal arrays use {@code int[]} — Java arrays are capped at ~2.1B entries,
 * so {@code long[]} offsets would waste 50% memory with zero benefit.
 * <p>
 * Usage:
 * <pre>
 *   GraphAnalyticalView gav = new GraphAnalyticalView(database);
 *   gav.build(new String[]{"Person"}, new String[]{"FOLLOWS"});
 *
 *   // Get dense ID for a known vertex
 *   int nodeId = gav.getNodeId(vertexRID);
 *
 *   // Vectorized neighbor scan
 *   CSRScanOperator scan = gav.scanNeighbors(nodeId, Vertex.DIRECTION.OUT);
 *   DataVector batch = new DataVector(DataVector.Type.INT);
 *   while (scan.getNextBatch(batch)) {
 *     // process batch
 *   }
 *
 *   // Direct degree queries
 *   int outDegree = gav.outDegree(nodeId);
 *   int inDegree = gav.inDegree(nodeId);
 * </pre>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class GraphAnalyticalView {
  private final Database          database;
  private       CSRAdjacencyIndex csrIndex;
  private       NodeIdMapping     nodeMapping;
  private       long              buildTimestamp;

  public GraphAnalyticalView(final Database database) {
    this.database = database;
  }

  /**
   * Builds (or rebuilds) the analytical view from the OLTP graph.
   *
   * @param vertexTypes vertex type names to include (null = all)
   * @param edgeTypes   edge type names to include (null = all)
   */
  public void build(final String[] vertexTypes, final String[] edgeTypes) {
    final CSRBuilder builder = new CSRBuilder(database);
    final CSRBuilder.CSRResult result = builder.build(vertexTypes, edgeTypes);

    this.csrIndex = result.getIndex();
    this.nodeMapping = result.getMapping();
    this.buildTimestamp = System.currentTimeMillis();
  }

  /**
   * Returns the dense node ID for a given RID, or -1 if not in the view.
   */
  public int getNodeId(final RID rid) {
    checkBuilt();
    return nodeMapping.getId(rid);
  }

  /**
   * Returns the RID for a given dense node ID.
   */
  public RID getRID(final int nodeId) {
    checkBuilt();
    return nodeMapping.getRID(nodeId);
  }

  /**
   * Returns the outgoing degree of a node.
   */
  public int outDegree(final int nodeId) {
    checkBuilt();
    return csrIndex.outDegree(nodeId);
  }

  /**
   * Returns the incoming degree of a node.
   */
  public int inDegree(final int nodeId) {
    checkBuilt();
    return csrIndex.inDegree(nodeId);
  }

  /**
   * Creates a vectorized scan operator for the neighbors of a node.
   */
  public CSRScanOperator scanNeighbors(final int nodeId, final Vertex.DIRECTION direction) {
    checkBuilt();
    return new CSRScanOperator(csrIndex, nodeId, direction);
  }

  /**
   * Returns all outgoing neighbor IDs as an array (non-vectorized convenience method).
   */
  public int[] getOutNeighbors(final int nodeId) {
    checkBuilt();
    final int start = csrIndex.outOffset(nodeId);
    final int end = csrIndex.outOffsetEnd(nodeId);
    return Arrays.copyOfRange(csrIndex.getForwardNeighbors(), start, end);
  }

  /**
   * Returns all incoming neighbor IDs as an array (non-vectorized convenience method).
   */
  public int[] getInNeighbors(final int nodeId) {
    checkBuilt();
    final int start = csrIndex.inOffset(nodeId);
    final int end = csrIndex.inOffsetEnd(nodeId);
    return Arrays.copyOfRange(csrIndex.getBackwardNeighbors(), start, end);
  }

  /**
   * Counts common neighbors between two nodes using sorted set intersection.
   * This is the foundation for triangle counting and WCOJ-style joins.
   *
   * @return number of common neighbors in the given direction
   */
  public int countCommonNeighbors(final int nodeA, final int nodeB, final Vertex.DIRECTION direction) {
    checkBuilt();
    final int[] neighbors;
    final int[] offsets;

    if (direction == Vertex.DIRECTION.OUT) {
      neighbors = csrIndex.getForwardNeighbors();
      offsets = csrIndex.getForwardOffsets();
    } else {
      neighbors = csrIndex.getBackwardNeighbors();
      offsets = csrIndex.getBackwardOffsets();
    }

    int startA = offsets[nodeA];
    final int endA = offsets[nodeA + 1];
    int startB = offsets[nodeB];
    final int endB = offsets[nodeB + 1];

    // Sorted merge intersection — O(dA + dB) with no hash table overhead
    int count = 0;
    while (startA < endA && startB < endB) {
      final int a = neighbors[startA];
      final int b = neighbors[startB];
      if (a == b) {
        count++;
        startA++;
        startB++;
      } else if (a < b)
        startA++;
      else
        startB++;
    }
    return count;
  }

  /**
   * Checks if nodeA has an edge to nodeB using binary search on sorted CSR.
   * O(log(degree)) instead of O(degree) linear scan.
   */
  public boolean isConnected(final int nodeA, final int nodeB, final Vertex.DIRECTION direction) {
    checkBuilt();
    final int[] neighbors;
    final int[] offsets;

    if (direction == Vertex.DIRECTION.OUT) {
      neighbors = csrIndex.getForwardNeighbors();
      offsets = csrIndex.getForwardOffsets();
    } else {
      neighbors = csrIndex.getBackwardNeighbors();
      offsets = csrIndex.getBackwardOffsets();
    }

    final int start = offsets[nodeA];
    final int end = offsets[nodeA + 1];
    return Arrays.binarySearch(neighbors, start, end, nodeB) >= 0;
  }

  /**
   * Returns the underlying CSR index for direct access in algorithms.
   */
  public CSRAdjacencyIndex getCSRIndex() {
    checkBuilt();
    return csrIndex;
  }

  /**
   * Returns the node ID mapping for RID resolution.
   */
  public NodeIdMapping getNodeMapping() {
    checkBuilt();
    return nodeMapping;
  }

  public int getNodeCount() {
    checkBuilt();
    return csrIndex.getNodeCount();
  }

  public int getEdgeCount() {
    checkBuilt();
    return csrIndex.getEdgeCount();
  }

  /**
   * Returns the timestamp when the view was last built.
   */
  public long getBuildTimestamp() {
    return buildTimestamp;
  }

  /**
   * Returns true if the view has been built.
   */
  public boolean isBuilt() {
    return csrIndex != null;
  }

  /**
   * Returns approximate memory usage in bytes.
   */
  public long getMemoryUsageBytes() {
    if (csrIndex == null)
      return 0;
    // CSR arrays + mapping arrays (bucketIds[] + offsets[] + HashMap overhead)
    return csrIndex.getMemoryUsageBytes()
        + (long) nodeMapping.size() * (Integer.BYTES + Long.BYTES + 48); // ~48 bytes HashMap.Entry overhead
  }

  private void checkBuilt() {
    if (csrIndex == null)
      throw new IllegalStateException("GraphAnalyticalView has not been built yet. Call build() first.");
  }
}
