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

import com.arcadedb.grapholap.simd.GraphOlapVectorOps;
import com.arcadedb.grapholap.simd.GraphOlapVectorOpsProvider;

/**
 * Vectorized scan operator over a columnar property. Produces batches of values
 * from a {@link Column}, enabling downstream operators to process properties in bulk.
 * <p>
 * Supports scanning a full column (all nodes) or a subset defined by a selection
 * vector (array of node IDs to include).
 * <p>
 * Uses SIMD-accelerated gather operations (via {@link GraphOlapVectorOps}) for selective
 * scans when the Java Vector API is available, falling back to scalar loops otherwise.
 * <p>
 * Usage pattern:
 * <pre>
 *   ColumnScanOperator scan = new ColumnScanOperator(column);
 *   DataVector batch = new DataVector(DataVector.Type.INT);
 *   while (scan.getNextBatch(batch)) {
 *     // process batch.getSize() values in batch.getIntData()
 *   }
 * </pre>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class ColumnScanOperator {
  private static final GraphOlapVectorOps VECTOR_OPS = GraphOlapVectorOpsProvider.getInstance();

  private final Column column;
  private final int[]  selectionVector;  // null = full scan
  private       int    cursor;
  private final int    totalEntries;

  /**
   * Creates a full-scan operator over the entire column.
   */
  public ColumnScanOperator(final Column column) {
    this.column = column;
    this.selectionVector = null;
    this.cursor = 0;
    this.totalEntries = column.getNodeCount();
  }

  /**
   * Creates a selective scan operator over specific node IDs.
   */
  public ColumnScanOperator(final Column column, final int[] selectionVector) {
    this.column = column;
    this.selectionVector = selectionVector;
    this.cursor = 0;
    this.totalEntries = selectionVector.length;
  }

  /**
   * Fills the DataVector with the next batch of values.
   *
   * @return true if the batch has data, false if exhausted
   */
  public boolean getNextBatch(final DataVector output) {
    output.reset();

    if (cursor >= totalEntries)
      return false;

    final int batchSize = Math.min(DataVector.VECTOR_SIZE, totalEntries - cursor);

    if (selectionVector == null)
      fillFullScan(output, batchSize);
    else
      fillSelective(output, batchSize);

    output.setSize(batchSize);
    cursor += batchSize;
    return true;
  }

  /**
   * Resets the scan cursor to the beginning.
   */
  public void reset() {
    cursor = 0;
  }

  public int getTotalEntries() {
    return totalEntries;
  }

  private void fillFullScan(final DataVector output, final int batchSize) {
    final long[] nullBits = column.getNullBitset();

    switch (column.getType()) {
    case INT: {
      final int[] src = column.getIntData();
      final int[] dst = output.getIntData();
      System.arraycopy(src, cursor, dst, 0, batchSize);
      copyNulls(output, nullBits, cursor, batchSize);
      break;
    }
    case LONG: {
      final long[] src = column.getLongData();
      final long[] dst = output.getLongData();
      System.arraycopy(src, cursor, dst, 0, batchSize);
      copyNulls(output, nullBits, cursor, batchSize);
      break;
    }
    case DOUBLE: {
      final double[] src = column.getDoubleData();
      final double[] dst = output.getDoubleData();
      System.arraycopy(src, cursor, dst, 0, batchSize);
      copyNulls(output, nullBits, cursor, batchSize);
      break;
    }
    case STRING: {
      // For string columns, copy the dictionary codes as INT
      final int[] src = column.getStringCodes();
      final int[] dst = output.getIntData();
      System.arraycopy(src, cursor, dst, 0, batchSize);
      copyNulls(output, nullBits, cursor, batchSize);
      break;
    }
    }
  }

  private void fillSelective(final DataVector output, final int batchSize) {
    final long[] nullBits = column.getNullBitset();

    switch (column.getType()) {
    case INT:
      VECTOR_OPS.gatherInt(column.getIntData(), selectionVector, output.getIntData(), cursor, batchSize);
      break;
    case LONG:
      VECTOR_OPS.gatherLong(column.getLongData(), selectionVector, output.getLongData(), cursor, batchSize);
      break;
    case DOUBLE:
      VECTOR_OPS.gatherDouble(column.getDoubleData(), selectionVector, output.getDoubleData(), cursor, batchSize);
      break;
    case STRING:
      VECTOR_OPS.gatherInt(column.getStringCodes(), selectionVector, output.getIntData(), cursor, batchSize);
      break;
    }
    VECTOR_OPS.extractNullMaskGather(nullBits, selectionVector, output.getNullMask(), cursor, batchSize);
  }

  private static void copyNulls(final DataVector output, final long[] nullBits,
      final int startNodeId, final int batchSize) {
    VECTOR_OPS.extractNullMaskSequential(nullBits, startNodeId, output.getNullMask(), batchSize);
  }
}
