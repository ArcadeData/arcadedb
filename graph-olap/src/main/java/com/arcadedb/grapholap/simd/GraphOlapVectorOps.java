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
package com.arcadedb.grapholap.simd;

/**
 * Interface for vectorized operations on graph-OLAP columnar data.
 * Two implementations: scalar (pure Java loops) and SIMD (Java Vector API).
 * <p>
 * Operations are grouped into:
 * <ul>
 *   <li><b>Gather</b>: indexed reads from source arrays (for selective column scans)</li>
 *   <li><b>Aggregation</b>: sum/min/max reductions over primitive arrays</li>
 *   <li><b>Bitmask</b>: AND/OR/NOT/popcount on long[] null bitsets</li>
 * </ul>
 */
public interface GraphOlapVectorOps {

  // ── Gather operations (selective column scan) ──────────────────────────

  /**
   * Gathers int values: {@code dst[i] = src[indices[offset + i]]} for {@code i} in {@code [0, length)}.
   */
  void gatherInt(int[] src, int[] indices, int[] dst, int offset, int length);

  /**
   * Gathers long values: {@code dst[i] = src[indices[offset + i]]} for {@code i} in {@code [0, length)}.
   */
  void gatherLong(long[] src, int[] indices, long[] dst, int offset, int length);

  /**
   * Gathers double values: {@code dst[i] = src[indices[offset + i]]} for {@code i} in {@code [0, length)}.
   */
  void gatherDouble(double[] src, int[] indices, double[] dst, int offset, int length);

  // ── Aggregation operations ─────────────────────────────────────────────

  double sumDouble(double[] data, int offset, int length);

  double minDouble(double[] data, int offset, int length);

  double maxDouble(double[] data, int offset, int length);

  long sumInt(int[] data, int offset, int length);

  int minInt(int[] data, int offset, int length);

  int maxInt(int[] data, int offset, int length);

  long sumLong(long[] data, int offset, int length);

  long minLong(long[] data, int offset, int length);

  long maxLong(long[] data, int offset, int length);

  // ── Bitmask operations (null bitset processing) ────────────────────────

  /**
   * Bitwise AND of two long[] bitmasks: {@code out[i] = a[i] & b[i]}.
   */
  void bitmaskAnd(long[] a, long[] b, long[] out, int length);

  /**
   * Bitwise OR of two long[] bitmasks: {@code out[i] = a[i] | b[i]}.
   */
  void bitmaskOr(long[] a, long[] b, long[] out, int length);

  /**
   * Bitwise NOT of a long[] bitmask: {@code out[i] = ~a[i]}.
   */
  void bitmaskNot(long[] a, long[] out, int length);

  /**
   * Counts total set bits across all words in the bitmask.
   */
  int bitmaskPopcount(long[] bitmask, int length);

  // ── Null mask extraction ───────────────────────────────────────────────

  /**
   * Extracts null bits for sequential node IDs into a boolean mask.
   * {@code dst[i] = (nullBitset[(startNodeId+i) >>> 6] & (1L << ((startNodeId+i) & 63))) != 0}
   */
  void extractNullMaskSequential(long[] nullBitset, int startNodeId, boolean[] dst, int length);

  /**
   * Extracts null bits for arbitrary node IDs (from indices array) into a boolean mask.
   * {@code dst[i] = (nullBitset[indices[offset+i] >>> 6] & (1L << (indices[offset+i] & 63))) != 0}
   */
  void extractNullMaskGather(long[] nullBitset, int[] indices, boolean[] dst, int offset, int length);
}
