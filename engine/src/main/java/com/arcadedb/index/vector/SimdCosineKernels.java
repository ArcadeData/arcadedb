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
package com.arcadedb.index.vector;

import jdk.incubator.vector.FloatVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;

/**
 * The sums a cosine similarity is made of, over {@code float[]} components, as Vector API kernels (issue #13274).
 * <p>
 * jvector's {@code cosine} keeps one accumulator for each of the three sums, so every fused multiply-add waits for the one
 * before it (a dependency chain four cycles long) and the loop retires one vector per four cycles, however fast the loads
 * are, and it adds the squared norm of the query again for every candidate. These kernels keep four accumulators per sum,
 * which lets the loads, the multiplies and the adds of four vectors overlap, and take only the sums the caller cannot
 * have: the dot product, and the squared norm of the stored vector where it is not known yet. The squared norm of the query
 * is one number for the whole search and that of a cached vector one number for as long as it lives, so
 * {@link FlatVectorCache} computes each once.
 * <p>
 * Every kernel reduces its accumulators the same way (the four, then the lanes), so the squared norm {@link #squaredNorm}
 * returns for a vector and the one {@link #dotAndSquaredNorm} returns for it are the same number, and a vector scored where
 * it was just read from its page and one scored from the cache come out together. This class is the only one of the index
 * package that names the {@code jdk.incubator.vector} module: {@link FlatVectorCache} reaches it only after
 * {@link #selfTest} passed, so a JVM started without the module never links it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class SimdCosineKernels {
  private static final VectorSpecies<Float> SPECIES = FloatVector.SPECIES_PREFERRED;
  private static final int                  LANES   = SPECIES.length();
  /** Floats per iteration of the main loop: four vectors. */
  private static final int                  STEP    = 4 * LANES;

  private SimdCosineKernels() {
  }

  /**
   * Links the kernels and checks them on a vector no shorter than a main-loop step plus a tail.
   *
   * @return true when they return the sums of a plain loop
   * @throws LinkageError when the JVM runs without the {@code jdk.incubator.vector} module
   */
  static boolean selfTest() {
    final int length = STEP + LANES + 3;
    final float[] a = new float[length];
    final float[] b = new float[length];
    float dot = 0F;
    float norm = 0F;
    for (int i = 0; i < length; i++) {
      a[i] = (i % 7) - 3F;
      b[i] = (i % 5) - 2F;
      dot += a[i] * b[i];
      norm += b[i] * b[i];
    }
    // Small integers: every sum is exact whatever the order it is added in
    final long both = dotAndSquaredNorm(a, b, 0, length);
    return dot(a, b, 0, length) == dot && squaredNorm(b, 0, length) == norm
        && Float.intBitsToFloat((int) (both >>> 32)) == dot && Float.intBitsToFloat((int) both) == norm;
  }

  /** @return the dot product of {@code query} and the {@code length} components of {@code data} from {@code offset} */
  static float dot(final float[] query, final float[] data, final int offset, final int length) {
    FloatVector d0 = FloatVector.zero(SPECIES);
    FloatVector d1 = d0;
    FloatVector d2 = d0;
    FloatVector d3 = d0;
    int i = 0;
    final int wide = length & -STEP;
    for (; i < wide; i += STEP) {
      d0 = FloatVector.fromArray(SPECIES, query, i).fma(FloatVector.fromArray(SPECIES, data, offset + i), d0);
      d1 = FloatVector.fromArray(SPECIES, query, i + LANES).fma(FloatVector.fromArray(SPECIES, data, offset + i + LANES), d1);
      d2 = FloatVector.fromArray(SPECIES, query, i + 2 * LANES)
          .fma(FloatVector.fromArray(SPECIES, data, offset + i + 2 * LANES), d2);
      d3 = FloatVector.fromArray(SPECIES, query, i + 3 * LANES)
          .fma(FloatVector.fromArray(SPECIES, data, offset + i + 3 * LANES), d3);
    }
    final int narrow = length & -LANES;
    for (; i < narrow; i += LANES)
      d0 = FloatVector.fromArray(SPECIES, query, i).fma(FloatVector.fromArray(SPECIES, data, offset + i), d0);
    float sum = d0.add(d1).add(d2.add(d3)).reduceLanes(VectorOperators.ADD);
    for (; i < length; i++)
      sum += query[i] * data[offset + i];
    return sum;
  }

  /** @return the sum of the squares of the {@code length} components of {@code data} from {@code offset} */
  static float squaredNorm(final float[] data, final int offset, final int length) {
    FloatVector n0 = FloatVector.zero(SPECIES);
    FloatVector n1 = n0;
    FloatVector n2 = n0;
    FloatVector n3 = n0;
    int i = 0;
    final int wide = length & -STEP;
    for (; i < wide; i += STEP) {
      final FloatVector v0 = FloatVector.fromArray(SPECIES, data, offset + i);
      final FloatVector v1 = FloatVector.fromArray(SPECIES, data, offset + i + LANES);
      final FloatVector v2 = FloatVector.fromArray(SPECIES, data, offset + i + 2 * LANES);
      final FloatVector v3 = FloatVector.fromArray(SPECIES, data, offset + i + 3 * LANES);
      n0 = v0.fma(v0, n0);
      n1 = v1.fma(v1, n1);
      n2 = v2.fma(v2, n2);
      n3 = v3.fma(v3, n3);
    }
    final int narrow = length & -LANES;
    for (; i < narrow; i += LANES) {
      final FloatVector v = FloatVector.fromArray(SPECIES, data, offset + i);
      n0 = v.fma(v, n0);
    }
    float sum = n0.add(n1).add(n2.add(n3)).reduceLanes(VectorOperators.ADD);
    for (; i < length; i++)
      sum += data[offset + i] * data[offset + i];
    return sum;
  }

  /**
   * One pass for both sums a stored vector that is not cached yet needs.
   *
   * @return the dot product of {@code query} and the {@code length} components of {@code data} from {@code offset} in the
   * high 32 bits, as {@link Float#floatToRawIntBits}, and their squared norm in the low 32 bits: no object is allocated
   */
  static long dotAndSquaredNorm(final float[] query, final float[] data, final int offset, final int length) {
    FloatVector d0 = FloatVector.zero(SPECIES);
    FloatVector d1 = d0;
    FloatVector d2 = d0;
    FloatVector d3 = d0;
    FloatVector n0 = d0;
    FloatVector n1 = d0;
    FloatVector n2 = d0;
    FloatVector n3 = d0;
    int i = 0;
    final int wide = length & -STEP;
    for (; i < wide; i += STEP) {
      final FloatVector v0 = FloatVector.fromArray(SPECIES, data, offset + i);
      final FloatVector v1 = FloatVector.fromArray(SPECIES, data, offset + i + LANES);
      final FloatVector v2 = FloatVector.fromArray(SPECIES, data, offset + i + 2 * LANES);
      final FloatVector v3 = FloatVector.fromArray(SPECIES, data, offset + i + 3 * LANES);
      d0 = FloatVector.fromArray(SPECIES, query, i).fma(v0, d0);
      d1 = FloatVector.fromArray(SPECIES, query, i + LANES).fma(v1, d1);
      d2 = FloatVector.fromArray(SPECIES, query, i + 2 * LANES).fma(v2, d2);
      d3 = FloatVector.fromArray(SPECIES, query, i + 3 * LANES).fma(v3, d3);
      n0 = v0.fma(v0, n0);
      n1 = v1.fma(v1, n1);
      n2 = v2.fma(v2, n2);
      n3 = v3.fma(v3, n3);
    }
    final int narrow = length & -LANES;
    for (; i < narrow; i += LANES) {
      final FloatVector v = FloatVector.fromArray(SPECIES, data, offset + i);
      d0 = FloatVector.fromArray(SPECIES, query, i).fma(v, d0);
      n0 = v.fma(v, n0);
    }
    float dot = d0.add(d1).add(d2.add(d3)).reduceLanes(VectorOperators.ADD);
    float norm = n0.add(n1).add(n2.add(n3)).reduceLanes(VectorOperators.ADD);
    for (; i < length; i++) {
      final float component = data[offset + i];
      dot += query[i] * component;
      norm += component * component;
    }
    return ((long) Float.floatToRawIntBits(dot) << 32) | (Float.floatToRawIntBits(norm) & 0xFFFFFFFFL);
  }
}
