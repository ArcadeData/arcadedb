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
package com.arcadedb.graph.olap.simd;

import jdk.incubator.vector.DoubleVector;
import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;

/**
 * SIMD-accelerated implementation of graph-OLAP vector operations using the Java Vector API
 * ({@code jdk.incubator.vector}). Uses SPECIES_PREFERRED for automatic lane width selection.
 * <p>
 * Key SIMD-accelerated operations:
 * <ul>
 *   <li><b>Gather</b>: uses {@code IntVector/DoubleVector.fromArray} with index maps for SIMD scatter-gather</li>
 *   <li><b>Aggregation</b>: uses {@code reduceLanes()} for parallel sum/min/max</li>
 *   <li><b>Bitmask</b>: uses {@code LongVector} for bulk bitwise AND/OR/NOT</li>
 * </ul>
 * <p>
 * Null mask extraction delegates to scalar fallback since bit-level addressing
 * at arbitrary positions does not map efficiently to SIMD lanes.
 */
public final class SimdGraphOlapVectorOps implements GraphOlapVectorOps {

  private static final VectorSpecies<Integer> INT_SPECIES    = IntVector.SPECIES_PREFERRED;
  private static final VectorSpecies<Long>    LONG_SPECIES   = LongVector.SPECIES_PREFERRED;
  private static final VectorSpecies<Double>  DOUBLE_SPECIES = DoubleVector.SPECIES_PREFERRED;

  private static final ScalarGraphOlapVectorOps SCALAR = new ScalarGraphOlapVectorOps();

  // ── Gather operations (SIMD scatter-gather) ────────────────────────────

  @Override
  public void gatherInt(final int[] src, final int[] indices, final int[] dst, final int offset, final int length) {
    final int lanes = INT_SPECIES.length();
    int i = 0;
    for (; i + lanes <= length; i += lanes) {
      // SIMD gather: load values from src at positions given by indices
      final IntVector gathered = IntVector.fromArray(INT_SPECIES, src, 0, indices, offset + i);
      gathered.intoArray(dst, i);
    }
    // Scalar remainder
    for (; i < length; i++)
      dst[i] = src[indices[offset + i]];
  }

  @Override
  public void gatherLong(final long[] src, final int[] indices, final long[] dst, final int offset, final int length) {
    final int lanes = LONG_SPECIES.length();
    int i = 0;
    for (; i + lanes <= length; i += lanes) {
      final LongVector gathered = LongVector.fromArray(LONG_SPECIES, src, 0, indices, offset + i);
      gathered.intoArray(dst, i);
    }
    for (; i < length; i++)
      dst[i] = src[indices[offset + i]];
  }

  @Override
  public void gatherDouble(final double[] src, final int[] indices, final double[] dst, final int offset, final int length) {
    final int lanes = DOUBLE_SPECIES.length();
    int i = 0;
    for (; i + lanes <= length; i += lanes) {
      final DoubleVector gathered = DoubleVector.fromArray(DOUBLE_SPECIES, src, 0, indices, offset + i);
      gathered.intoArray(dst, i);
    }
    for (; i < length; i++)
      dst[i] = src[indices[offset + i]];
  }

  // ── Aggregation (SIMD reduction) ───────────────────────────────────────

  @Override
  public double sumDouble(final double[] data, final int offset, final int length) {
    final int lanes = DOUBLE_SPECIES.length();
    double s = 0;
    int i = 0;
    for (; i + lanes <= length; i += lanes) {
      final DoubleVector v = DoubleVector.fromArray(DOUBLE_SPECIES, data, offset + i);
      s += v.reduceLanes(VectorOperators.ADD);
    }
    for (; i < length; i++)
      s += data[offset + i];
    return s;
  }

  @Override
  public double minDouble(final double[] data, final int offset, final int length) {
    final int lanes = DOUBLE_SPECIES.length();
    double m = Double.POSITIVE_INFINITY;
    int i = 0;
    for (; i + lanes <= length; i += lanes) {
      final DoubleVector v = DoubleVector.fromArray(DOUBLE_SPECIES, data, offset + i);
      final double laneMin = v.reduceLanes(VectorOperators.MIN);
      if (laneMin < m)
        m = laneMin;
    }
    for (; i < length; i++)
      if (data[offset + i] < m)
        m = data[offset + i];
    return m;
  }

  @Override
  public double maxDouble(final double[] data, final int offset, final int length) {
    final int lanes = DOUBLE_SPECIES.length();
    double m = Double.NEGATIVE_INFINITY;
    int i = 0;
    for (; i + lanes <= length; i += lanes) {
      final DoubleVector v = DoubleVector.fromArray(DOUBLE_SPECIES, data, offset + i);
      final double laneMax = v.reduceLanes(VectorOperators.MAX);
      if (laneMax > m)
        m = laneMax;
    }
    for (; i < length; i++)
      if (data[offset + i] > m)
        m = data[offset + i];
    return m;
  }

  @Override
  public long sumInt(final int[] data, final int offset, final int length) {
    final int lanes = INT_SPECIES.length();
    long s = 0;
    int i = 0;
    final int longLanes = LONG_SPECIES.length();
    final int parts = (lanes + longLanes - 1) / longLanes;
    for (; i + lanes <= length; i += lanes) {
      final IntVector v = IntVector.fromArray(INT_SPECIES, data, offset + i);
      // Widen int lanes to long before reducing to avoid int overflow
      for (int p = 0; p < parts; p++)
        s += ((LongVector) v.convertShape(VectorOperators.I2L, LONG_SPECIES, p)).reduceLanes(VectorOperators.ADD);
    }
    for (; i < length; i++)
      s += data[offset + i];
    return s;
  }

  @Override
  public int minInt(final int[] data, final int offset, final int length) {
    final int lanes = INT_SPECIES.length();
    int m = Integer.MAX_VALUE;
    int i = 0;
    for (; i + lanes <= length; i += lanes) {
      final IntVector v = IntVector.fromArray(INT_SPECIES, data, offset + i);
      final int laneMin = v.reduceLanes(VectorOperators.MIN);
      if (laneMin < m)
        m = laneMin;
    }
    for (; i < length; i++)
      if (data[offset + i] < m)
        m = data[offset + i];
    return m;
  }

  @Override
  public int maxInt(final int[] data, final int offset, final int length) {
    final int lanes = INT_SPECIES.length();
    int m = Integer.MIN_VALUE;
    int i = 0;
    for (; i + lanes <= length; i += lanes) {
      final IntVector v = IntVector.fromArray(INT_SPECIES, data, offset + i);
      final int laneMax = v.reduceLanes(VectorOperators.MAX);
      if (laneMax > m)
        m = laneMax;
    }
    for (; i < length; i++)
      if (data[offset + i] > m)
        m = data[offset + i];
    return m;
  }

  @Override
  public long sumLong(final long[] data, final int offset, final int length) {
    final int lanes = LONG_SPECIES.length();
    long s = 0;
    int i = 0;
    for (; i + lanes <= length; i += lanes) {
      final LongVector v = LongVector.fromArray(LONG_SPECIES, data, offset + i);
      s += v.reduceLanes(VectorOperators.ADD);
    }
    for (; i < length; i++)
      s += data[offset + i];
    return s;
  }

  @Override
  public long minLong(final long[] data, final int offset, final int length) {
    final int lanes = LONG_SPECIES.length();
    long m = Long.MAX_VALUE;
    int i = 0;
    for (; i + lanes <= length; i += lanes) {
      final LongVector v = LongVector.fromArray(LONG_SPECIES, data, offset + i);
      final long laneMin = v.reduceLanes(VectorOperators.MIN);
      if (laneMin < m)
        m = laneMin;
    }
    for (; i < length; i++)
      if (data[offset + i] < m)
        m = data[offset + i];
    return m;
  }

  @Override
  public long maxLong(final long[] data, final int offset, final int length) {
    final int lanes = LONG_SPECIES.length();
    long m = Long.MIN_VALUE;
    int i = 0;
    for (; i + lanes <= length; i += lanes) {
      final LongVector v = LongVector.fromArray(LONG_SPECIES, data, offset + i);
      final long laneMax = v.reduceLanes(VectorOperators.MAX);
      if (laneMax > m)
        m = laneMax;
    }
    for (; i < length; i++)
      if (data[offset + i] > m)
        m = data[offset + i];
    return m;
  }

  // ── Bitmask operations (SIMD bulk bitwise) ─────────────────────────────

  @Override
  public void bitmaskAnd(final long[] a, final long[] b, final long[] out, final int length) {
    final int lanes = LONG_SPECIES.length();
    int i = 0;
    for (; i + lanes <= length; i += lanes) {
      final LongVector va = LongVector.fromArray(LONG_SPECIES, a, i);
      final LongVector vb = LongVector.fromArray(LONG_SPECIES, b, i);
      va.and(vb).intoArray(out, i);
    }
    for (; i < length; i++)
      out[i] = a[i] & b[i];
  }

  @Override
  public void bitmaskOr(final long[] a, final long[] b, final long[] out, final int length) {
    final int lanes = LONG_SPECIES.length();
    int i = 0;
    for (; i + lanes <= length; i += lanes) {
      final LongVector va = LongVector.fromArray(LONG_SPECIES, a, i);
      final LongVector vb = LongVector.fromArray(LONG_SPECIES, b, i);
      va.or(vb).intoArray(out, i);
    }
    for (; i < length; i++)
      out[i] = a[i] | b[i];
  }

  @Override
  public void bitmaskNot(final long[] a, final long[] out, final int length) {
    final int lanes = LONG_SPECIES.length();
    int i = 0;
    for (; i + lanes <= length; i += lanes) {
      final LongVector va = LongVector.fromArray(LONG_SPECIES, a, i);
      va.not().intoArray(out, i);
    }
    for (; i < length; i++)
      out[i] = ~a[i];
  }

  @Override
  public int bitmaskPopcount(final long[] bitmask, final int length) {
    // Long.bitCount() compiles to POPCNT on x86 — already optimal.
    // SIMD doesn't provide a lane-level popcount, so scalar is best here.
    int count = 0;
    for (int i = 0; i < length; i++)
      count += Long.bitCount(bitmask[i]);
    return count;
  }

  // ── Null mask extraction (scalar fallback) ─────────────────────────────
  // Bit-level addressing at arbitrary positions doesn't map to SIMD lanes.

  @Override
  public void extractNullMaskSequential(final long[] nullBitset, final int startNodeId, final boolean[] dst, final int length) {
    SCALAR.extractNullMaskSequential(nullBitset, startNodeId, dst, length);
  }

  @Override
  public void extractNullMaskGather(final long[] nullBitset, final int[] indices, final boolean[] dst, final int offset,
      final int length) {
    SCALAR.extractNullMaskGather(nullBitset, indices, dst, offset, length);
  }
}
