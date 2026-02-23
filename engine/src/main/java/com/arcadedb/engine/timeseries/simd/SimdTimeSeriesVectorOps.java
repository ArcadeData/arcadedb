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
package com.arcadedb.engine.timeseries.simd;

import jdk.incubator.vector.DoubleVector;
import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;

/**
 * SIMD-accelerated implementation using the Java Vector API (jdk.incubator.vector).
 * Uses SPECIES_PREFERRED for automatic lane width selection.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class SimdTimeSeriesVectorOps implements TimeSeriesVectorOps {

  private static final VectorSpecies<Double> DOUBLE_SPECIES = DoubleVector.SPECIES_PREFERRED;
  private static final VectorSpecies<Long>   LONG_SPECIES   = LongVector.SPECIES_PREFERRED;

  @Override
  public double sum(final double[] data, final int offset, final int length) {
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
  public double min(final double[] data, final int offset, final int length) {
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
  public double max(final double[] data, final int offset, final int length) {
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

  // The scalar fallback instance used for bitmask-based operations that do not benefit from SIMD
  // (bitmask layout with word/bit addressing doesn't map cleanly to SIMD masks)
  private static final ScalarTimeSeriesVectorOps SCALAR = new ScalarTimeSeriesVectorOps();

  @Override
  public double sumFiltered(final double[] data, final long[] bitmask, final int offset, final int length) {
    // Scalar fallback — bitmask operations are not SIMD-accelerated
    return SCALAR.sumFiltered(data, bitmask, offset, length);
  }

  @Override
  public int countFiltered(final long[] bitmask, final int offset, final int length) {
    // Scalar fallback — bitmask operations are not SIMD-accelerated
    return SCALAR.countFiltered(bitmask, offset, length);
  }

  @Override
  public void greaterThan(final double[] data, final double threshold, final long[] out, final int offset, final int length) {
    // Scalar fallback — bitmask output doesn't map cleanly to SIMD result masks
    SCALAR.greaterThan(data, threshold, out, offset, length);
  }

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
}
