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

import com.arcadedb.graph.Vertex;

/**
 * Vectorized scan operator over CSR adjacency data. Produces batches of neighbor node IDs
 * for a given source node, enabling downstream operators to process neighbors in bulk.
 * <p>
 * Usage pattern:
 * <pre>
 *   CSRScanOperator scan = new CSRScanOperator(csrIndex, nodeId, Vertex.DIRECTION.OUT);
 *   DataVector batch = new DataVector(DataVector.Type.INT);
 *   while (scan.getNextBatch(batch)) {
 *     // process batch.getSize() neighbors in batch.getIntData()
 *   }
 * </pre>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class CSRScanOperator {
  private final CSRAdjacencyIndex csr;
  private final int               nodeId;
  private final Vertex.DIRECTION  direction;
  private       int               cursor;
  private final int               end;

  public CSRScanOperator(final CSRAdjacencyIndex csr, final int nodeId, final Vertex.DIRECTION direction) {
    this.csr = csr;
    this.nodeId = nodeId;
    this.direction = direction;

    if (direction == Vertex.DIRECTION.OUT) {
      this.cursor = csr.outOffset(nodeId);
      this.end = csr.outOffsetEnd(nodeId);
    } else {
      this.cursor = csr.inOffset(nodeId);
      this.end = csr.inOffsetEnd(nodeId);
    }
  }

  /**
   * Fills the DataVector with the next batch of neighbor IDs.
   *
   * @return true if the batch has data, false if exhausted
   */
  public boolean getNextBatch(final DataVector output) {
    output.reset();

    if (cursor >= end)
      return false;

    final int[] neighbors = direction == Vertex.DIRECTION.OUT
        ? csr.getForwardNeighbors()
        : csr.getBackwardNeighbors();
    final int[] outData = output.getIntData();

    final int batchSize = Math.min(DataVector.VECTOR_SIZE, end - cursor);

    // Bulk copy — JVM will optimize this to a memcpy/SIMD operation
    System.arraycopy(neighbors, cursor, outData, 0, batchSize);

    output.setSize(batchSize);
    cursor += batchSize;
    return true;
  }

  /**
   * Returns the total number of neighbors for this scan.
   */
  public int totalNeighbors() {
    return end - (direction == Vertex.DIRECTION.OUT ? csr.outOffset(nodeId) : csr.inOffset(nodeId));
  }

  /**
   * Resets the scan cursor to the beginning.
   */
  public void reset() {
    if (direction == Vertex.DIRECTION.OUT)
      this.cursor = csr.outOffset(nodeId);
    else
      this.cursor = csr.inOffset(nodeId);
  }
}
