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
import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * Graph Analytical View (GAV) — a synchronized, read-optimized CSR representation of the OLTP graph.
 * <p>
 * Stores one {@link CSRAdjacencyIndex} per edge type, with a shared
 * {@link NodeIdMapping} across all vertex types. This enables:
 * <ul>
 *   <li>Type-filtered traversal: traverse only "FOLLOWS" edges, or only "LIKES" edges</li>
 *   <li>Type-filtered counting: count only edges of a specific type</li>
 *   <li>Cross-type queries: traverse all edge types by iterating per-type CSRs</li>
 *   <li>Vertex type awareness: check which type a node belongs to</li>
 * </ul>
 * <p>
 * Usage:
 * <pre>
 *   GraphAnalyticalView gav = new GraphAnalyticalView(database);
 *   gav.build(new String[]{"Person","Company"}, new String[]{"FOLLOWS","WORKS_AT"});
 *
 *   int nodeId = gav.getNodeId(vertexRID);
 *
 *   // Type-specific queries
 *   int followsOut = gav.outDegree(nodeId, "FOLLOWS");
 *   int worksAtOut = gav.outDegree(nodeId, "WORKS_AT");
 *
 *   // Cross-type query (all edge types)
 *   int totalOut = gav.outDegree(nodeId);
 *
 *   // Vertex type
 *   String type = gav.getNodeTypeName(nodeId); // "Person" or "Company"
 * </pre>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class GraphAnalyticalView {
  private final Database                       database;
  private       Map<String, CSRAdjacencyIndex> csrPerType;
  private       NodeIdMapping                  nodeMapping;
  private       long                           buildTimestamp;

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

    this.csrPerType = result.getCsrPerType();
    this.nodeMapping = result.getMapping();
    this.buildTimestamp = System.currentTimeMillis();
  }

  // --- Node ID / RID mapping ---

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

  // --- Node type queries ---

  /**
   * Returns the vertex type name for the given node.
   */
  public String getNodeTypeName(final int nodeId) {
    checkBuilt();
    return nodeMapping.getTypeName(nodeId);
  }

  /**
   * Returns the vertex type index for the given node.
   */
  public int getNodeTypeId(final int nodeId) {
    checkBuilt();
    return nodeMapping.getTypeId(nodeId);
  }

  // --- Degree queries ---

  /**
   * Returns the outgoing degree of a node for a specific edge type.
   */
  public int outDegree(final int nodeId, final String edgeType) {
    checkBuilt();
    final CSRAdjacencyIndex csr = csrPerType.get(edgeType);
    return csr != null ? csr.outDegree(nodeId) : 0;
  }

  /**
   * Returns the incoming degree of a node for a specific edge type.
   */
  public int inDegree(final int nodeId, final String edgeType) {
    checkBuilt();
    final CSRAdjacencyIndex csr = csrPerType.get(edgeType);
    return csr != null ? csr.inDegree(nodeId) : 0;
  }

  /**
   * Returns the outgoing degree of a node across ALL edge types.
   */
  public int outDegree(final int nodeId) {
    checkBuilt();
    int total = 0;
    for (final CSRAdjacencyIndex csr : csrPerType.values())
      total += csr.outDegree(nodeId);
    return total;
  }

  /**
   * Returns the incoming degree of a node across ALL edge types.
   */
  public int inDegree(final int nodeId) {
    checkBuilt();
    int total = 0;
    for (final CSRAdjacencyIndex csr : csrPerType.values())
      total += csr.inDegree(nodeId);
    return total;
  }

  // --- Neighbor access ---

  /**
   * Returns all outgoing neighbor IDs for a specific edge type.
   */
  public int[] getOutNeighbors(final int nodeId, final String edgeType) {
    checkBuilt();
    final CSRAdjacencyIndex csr = csrPerType.get(edgeType);
    if (csr == null)
      return new int[0];
    return Arrays.copyOfRange(csr.getForwardNeighbors(), csr.outOffset(nodeId), csr.outOffsetEnd(nodeId));
  }

  /**
   * Returns all incoming neighbor IDs for a specific edge type.
   */
  public int[] getInNeighbors(final int nodeId, final String edgeType) {
    checkBuilt();
    final CSRAdjacencyIndex csr = csrPerType.get(edgeType);
    if (csr == null)
      return new int[0];
    return Arrays.copyOfRange(csr.getBackwardNeighbors(), csr.inOffset(nodeId), csr.inOffsetEnd(nodeId));
  }

  /**
   * Returns all outgoing neighbor IDs across ALL edge types.
   */
  public int[] getOutNeighbors(final int nodeId) {
    checkBuilt();
    if (csrPerType.size() == 1)
      return getOutNeighbors(nodeId, csrPerType.keySet().iterator().next());

    final int totalDeg = outDegree(nodeId);
    if (totalDeg == 0)
      return new int[0];

    final int[] result = new int[totalDeg];
    int pos = 0;
    for (final CSRAdjacencyIndex csr : csrPerType.values()) {
      final int start = csr.outOffset(nodeId);
      final int end = csr.outOffsetEnd(nodeId);
      final int len = end - start;
      if (len > 0) {
        System.arraycopy(csr.getForwardNeighbors(), start, result, pos, len);
        pos += len;
      }
    }
    Arrays.sort(result);
    return result;
  }

  /**
   * Returns all incoming neighbor IDs across ALL edge types.
   */
  public int[] getInNeighbors(final int nodeId) {
    checkBuilt();
    if (csrPerType.size() == 1)
      return getInNeighbors(nodeId, csrPerType.keySet().iterator().next());

    final int totalDeg = inDegree(nodeId);
    if (totalDeg == 0)
      return new int[0];

    final int[] result = new int[totalDeg];
    int pos = 0;
    for (final CSRAdjacencyIndex csr : csrPerType.values()) {
      final int start = csr.inOffset(nodeId);
      final int end = csr.inOffsetEnd(nodeId);
      final int len = end - start;
      if (len > 0) {
        System.arraycopy(csr.getBackwardNeighbors(), start, result, pos, len);
        pos += len;
      }
    }
    Arrays.sort(result);
    return result;
  }

  // --- Vectorized scan ---

  /**
   * Creates a vectorized scan operator for the neighbors of a node, for a specific edge type.
   */
  public CSRScanOperator scanNeighbors(final int nodeId, final Vertex.DIRECTION direction, final String edgeType) {
    checkBuilt();
    final CSRAdjacencyIndex csr = csrPerType.get(edgeType);
    if (csr == null)
      throw new IllegalArgumentException("Edge type not in view: " + edgeType);
    return new CSRScanOperator(csr, nodeId, direction);
  }

  // --- Connectivity and intersection ---

  /**
   * Checks if nodeA has an edge to nodeB for a specific edge type.
   * O(log(degree)) using binary search on sorted CSR.
   */
  public boolean isConnected(final int nodeA, final int nodeB, final Vertex.DIRECTION direction,
      final String edgeType) {
    checkBuilt();
    final CSRAdjacencyIndex csr = csrPerType.get(edgeType);
    if (csr == null)
      return false;

    final int[] neighbors;
    final int[] offsets;
    if (direction == Vertex.DIRECTION.OUT) {
      neighbors = csr.getForwardNeighbors();
      offsets = csr.getForwardOffsets();
    } else {
      neighbors = csr.getBackwardNeighbors();
      offsets = csr.getBackwardOffsets();
    }

    return Arrays.binarySearch(neighbors, offsets[nodeA], offsets[nodeA + 1], nodeB) >= 0;
  }

  /**
   * Checks if nodeA has an edge to nodeB across ANY edge type.
   */
  public boolean isConnected(final int nodeA, final int nodeB, final Vertex.DIRECTION direction) {
    checkBuilt();
    for (final String edgeType : csrPerType.keySet())
      if (isConnected(nodeA, nodeB, direction, edgeType))
        return true;
    return false;
  }

  /**
   * Counts common neighbors between two nodes for a specific edge type.
   * Uses sorted merge intersection — O(dA + dB).
   */
  public int countCommonNeighbors(final int nodeA, final int nodeB, final Vertex.DIRECTION direction,
      final String edgeType) {
    checkBuilt();
    final CSRAdjacencyIndex csr = csrPerType.get(edgeType);
    if (csr == null)
      return 0;

    final int[] neighbors;
    final int[] offsets;
    if (direction == Vertex.DIRECTION.OUT) {
      neighbors = csr.getForwardNeighbors();
      offsets = csr.getForwardOffsets();
    } else {
      neighbors = csr.getBackwardNeighbors();
      offsets = csr.getBackwardOffsets();
    }

    return sortedIntersectionCount(neighbors, offsets[nodeA], offsets[nodeA + 1],
        neighbors, offsets[nodeB], offsets[nodeB + 1]);
  }

  /**
   * Counts common neighbors between two nodes across ALL edge types.
   */
  public int countCommonNeighbors(final int nodeA, final int nodeB, final Vertex.DIRECTION direction) {
    checkBuilt();
    int total = 0;
    for (final String edgeType : csrPerType.keySet())
      total += countCommonNeighbors(nodeA, nodeB, direction, edgeType);
    return total;
  }

  // --- Metadata ---

  /**
   * Returns the CSR index for a specific edge type, or null if not present.
   */
  public CSRAdjacencyIndex getCSRIndex(final String edgeType) {
    checkBuilt();
    return csrPerType.get(edgeType);
  }

  /**
   * Returns the set of edge type names in this view.
   */
  public Set<String> getEdgeTypes() {
    checkBuilt();
    return Collections.unmodifiableSet(csrPerType.keySet());
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
    return nodeMapping.size();
  }

  /**
   * Returns the total edge count across all edge types.
   */
  public int getEdgeCount() {
    checkBuilt();
    int total = 0;
    for (final CSRAdjacencyIndex csr : csrPerType.values())
      total += csr.getEdgeCount();
    return total;
  }

  /**
   * Returns the edge count for a specific edge type.
   */
  public int getEdgeCount(final String edgeType) {
    checkBuilt();
    final CSRAdjacencyIndex csr = csrPerType.get(edgeType);
    return csr != null ? csr.getEdgeCount() : 0;
  }

  public long getBuildTimestamp() {
    return buildTimestamp;
  }

  public boolean isBuilt() {
    return csrPerType != null;
  }

  /**
   * Returns approximate memory usage in bytes.
   */
  public long getMemoryUsageBytes() {
    if (csrPerType == null)
      return 0;
    long total = (long) nodeMapping.size() * (Integer.BYTES + Long.BYTES + Integer.BYTES + 48);
    for (final CSRAdjacencyIndex csr : csrPerType.values())
      total += csr.getMemoryUsageBytes();
    return total;
  }

  private void checkBuilt() {
    if (csrPerType == null)
      throw new IllegalStateException("GraphAnalyticalView has not been built yet. Call build() first.");
  }

  private static int sortedIntersectionCount(final int[] a, int startA, final int endA,
      final int[] b, int startB, final int endB) {
    int count = 0;
    while (startA < endA && startB < endB) {
      final int va = a[startA];
      final int vb = b[startB];
      if (va == vb) {
        count++;
        startA++;
        startB++;
      } else if (va < vb)
        startA++;
      else
        startB++;
    }
    return count;
  }
}
