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
 * Pure Java scalar implementation of graph-OLAP vector operations. Always available as fallback.
 */
public final class ScalarGraphOlapVectorOps implements GraphOlapVectorOps {

  // ── Gather ─────────────────────────────────────────────────────────────

  @Override
  public void gatherInt(final int[] src, final int[] indices, final int[] dst, final int offset, final int length) {
    for (int i = 0; i < length; i++)
      dst[i] = src[indices[offset + i]];
  }

  @Override
  public void gatherLong(final long[] src, final int[] indices, final long[] dst, final int offset, final int length) {
    for (int i = 0; i < length; i++)
      dst[i] = src[indices[offset + i]];
  }

  @Override
  public void gatherDouble(final double[] src, final int[] indices, final double[] dst, final int offset, final int length) {
    for (int i = 0; i < length; i++)
      dst[i] = src[indices[offset + i]];
  }

  // ── Aggregation ────────────────────────────────────────────────────────

  @Override
  public double sumDouble(final double[] data, final int offset, final int length) {
    double s = 0;
    for (int i = offset, end = offset + length; i < end; i++)
      s += data[i];
    return s;
  }

  @Override
  public double minDouble(final double[] data, final int offset, final int length) {
    double m = Double.POSITIVE_INFINITY;
    for (int i = offset, end = offset + length; i < end; i++)
      if (data[i] < m)
        m = data[i];
    return m;
  }

  @Override
  public double maxDouble(final double[] data, final int offset, final int length) {
    double m = Double.NEGATIVE_INFINITY;
    for (int i = offset, end = offset + length; i < end; i++)
      if (data[i] > m)
        m = data[i];
    return m;
  }

  @Override
  public long sumInt(final int[] data, final int offset, final int length) {
    long s = 0;
    for (int i = offset, end = offset + length; i < end; i++)
      s += data[i];
    return s;
  }

  @Override
  public int minInt(final int[] data, final int offset, final int length) {
    int m = Integer.MAX_VALUE;
    for (int i = offset, end = offset + length; i < end; i++)
      if (data[i] < m)
        m = data[i];
    return m;
  }

  @Override
  public int maxInt(final int[] data, final int offset, final int length) {
    int m = Integer.MIN_VALUE;
    for (int i = offset, end = offset + length; i < end; i++)
      if (data[i] > m)
        m = data[i];
    return m;
  }

  @Override
  public long sumLong(final long[] data, final int offset, final int length) {
    long s = 0;
    for (int i = offset, end = offset + length; i < end; i++)
      s += data[i];
    return s;
  }

  @Override
  public long minLong(final long[] data, final int offset, final int length) {
    long m = Long.MAX_VALUE;
    for (int i = offset, end = offset + length; i < end; i++)
      if (data[i] < m)
        m = data[i];
    return m;
  }

  @Override
  public long maxLong(final long[] data, final int offset, final int length) {
    long m = Long.MIN_VALUE;
    for (int i = offset, end = offset + length; i < end; i++)
      if (data[i] > m)
        m = data[i];
    return m;
  }

  // ── Bitmask ────────────────────────────────────────────────────────────

  @Override
  public void bitmaskAnd(final long[] a, final long[] b, final long[] out, final int length) {
    for (int i = 0; i < length; i++)
      out[i] = a[i] & b[i];
  }

  @Override
  public void bitmaskOr(final long[] a, final long[] b, final long[] out, final int length) {
    for (int i = 0; i < length; i++)
      out[i] = a[i] | b[i];
  }

  @Override
  public void bitmaskNot(final long[] a, final long[] out, final int length) {
    for (int i = 0; i < length; i++)
      out[i] = ~a[i];
  }

  @Override
  public int bitmaskPopcount(final long[] bitmask, final int length) {
    int count = 0;
    for (int i = 0; i < length; i++)
      count += Long.bitCount(bitmask[i]);
    return count;
  }

  // ── Null mask extraction ───────────────────────────────────────────────

  @Override
  public void extractNullMaskSequential(final long[] nullBitset, final int startNodeId, final boolean[] dst, final int length) {
    for (int i = 0; i < length; i++) {
      final int nodeId = startNodeId + i;
      dst[i] = (nullBitset[nodeId >>> 6] & (1L << (nodeId & 63))) != 0;
    }
  }

  @Override
  public void extractNullMaskGather(final long[] nullBitset, final int[] indices, final boolean[] dst, final int offset,
      final int length) {
    for (int i = 0; i < length; i++) {
      final int nodeId = indices[offset + i];
      dst[i] = (nullBitset[nodeId >>> 6] & (1L << (nodeId & 63))) != 0;
    }
  }
}
