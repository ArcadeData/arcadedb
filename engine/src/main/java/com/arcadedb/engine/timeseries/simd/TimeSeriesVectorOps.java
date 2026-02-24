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
 * Interface for vectorized aggregation operations on primitive arrays.
 * Two implementations: scalar (pure Java loops) and SIMD (Java Vector API).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public interface TimeSeriesVectorOps {

  double sum(double[] data, int offset, int length);

  double min(double[] data, int offset, int length);

  double max(double[] data, int offset, int length);

  long sumLong(long[] data, int offset, int length);

  long minLong(long[] data, int offset, int length);

  long maxLong(long[] data, int offset, int length);

  /**
   * Sums only elements where the corresponding bitmask bit is set.
   * Bitmask is a long[] where each long covers 64 elements.
   */
  double sumFiltered(double[] data, long[] bitmask, int offset, int length);

  /**
   * Counts elements where the corresponding bitmask bit is set.
   */
  int countFiltered(long[] bitmask, int offset, int length);

  /**
   * Produces a bitmask where data[i] > threshold.
   */
  void greaterThan(double[] data, double threshold, long[] out, int offset, int length);

  /**
   * Bitwise AND of two bitmasks.
   */
  void bitmaskAnd(long[] a, long[] b, long[] out, int length);

  /**
   * Bitwise OR of two bitmasks.
   */
  void bitmaskOr(long[] a, long[] b, long[] out, int length);
}
