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

/**
 * Pure Java scalar implementation of vector operations. Always available as fallback.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class ScalarTimeSeriesVectorOps implements TimeSeriesVectorOps {

  @Override
  public double sum(final double[] data, final int offset, final int length) {
    double s = 0;
    for (int i = offset; i < offset + length; i++)
      s += data[i];
    return s;
  }

  @Override
  public double min(final double[] data, final int offset, final int length) {
    double m = Double.POSITIVE_INFINITY;
    for (int i = offset; i < offset + length; i++)
      if (data[i] < m)
        m = data[i];
    return m;
  }

  @Override
  public double max(final double[] data, final int offset, final int length) {
    double m = Double.NEGATIVE_INFINITY;
    for (int i = offset; i < offset + length; i++)
      if (data[i] > m)
        m = data[i];
    return m;
  }

  @Override
  public long sumLong(final long[] data, final int offset, final int length) {
    long s = 0;
    for (int i = offset; i < offset + length; i++)
      s += data[i];
    return s;
  }

  @Override
  public long minLong(final long[] data, final int offset, final int length) {
    long m = Long.MAX_VALUE;
    for (int i = offset; i < offset + length; i++)
      if (data[i] < m)
        m = data[i];
    return m;
  }

  @Override
  public long maxLong(final long[] data, final int offset, final int length) {
    long m = Long.MIN_VALUE;
    for (int i = offset; i < offset + length; i++)
      if (data[i] > m)
        m = data[i];
    return m;
  }

  @Override
  public double sumFiltered(final double[] data, final long[] bitmask, final int offset, final int length) {
    double s = 0;
    for (int i = 0; i < length; i++) {
      final int maskWord = (offset + i) >> 6;
      final int maskBit = (offset + i) & 63;
      if ((bitmask[maskWord] & (1L << maskBit)) != 0)
        s += data[offset + i];
    }
    return s;
  }

  @Override
  public int countFiltered(final long[] bitmask, final int offset, final int length) {
    int count = 0;
    for (int i = 0; i < length; i++) {
      final int maskWord = (offset + i) >> 6;
      final int maskBit = (offset + i) & 63;
      if ((bitmask[maskWord] & (1L << maskBit)) != 0)
        count++;
    }
    return count;
  }

  @Override
  public void greaterThan(final double[] data, final double threshold, final long[] out, final int offset, final int length) {
    for (int i = 0; i < length; i++) {
      final int maskWord = (offset + i) >> 6;
      final int maskBit = (offset + i) & 63;
      if (data[offset + i] > threshold)
        out[maskWord] |= (1L << maskBit);
      else
        out[maskWord] &= ~(1L << maskBit);
    }
  }

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
}
