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
package com.arcadedb.bench.workload;

/**
 * Exact nearest neighbors by brute force and the recall of an approximate answer against them.
 */
public final class VectorRecall {
  private VectorRecall() {
  }

  /**
   * The indexes of the {@code k} vectors of {@code base} most similar to {@code query} by cosine, best first.
   */
  public static int[] exactTopK(final float[][] base, final float[] query, final int k) {
    return exactTopK(base, norms(base), query, k);
  }

  /** The Euclidean norm of every vector, to be computed once and passed to {@link #exactTopK(float[][], double[], float[], int)}. */
  public static double[] norms(final float[][] base) {
    final double[] norms = new double[base.length];
    for (int i = 0; i < base.length; i++)
      norms[i] = norm(base[i]);
    return norms;
  }

  public static int[] exactTopK(final float[][] base, final double[] baseNorms, final float[] query, final int k) {
    final int size = Math.min(k, base.length);
    if (size <= 0)
      return new int[0];
    final int[] ids = new int[size];
    final double[] scores = new double[size];
    int filled = 0;
    final double queryNorm = norm(query);
    for (int i = 0; i < base.length; i++) {
      final float[] v = base[i];
      double dot = 0;
      for (int j = 0; j < v.length; j++)
        dot += (double) v[j] * query[j];
      final double score = dot / (baseNorms[i] * queryNorm);
      if (filled == size && score <= scores[size - 1])
        continue;
      int pos = filled < size ? filled++ : size - 1;
      while (pos > 0 && scores[pos - 1] < score) {
        scores[pos] = scores[pos - 1];
        ids[pos] = ids[pos - 1];
        pos--;
      }
      scores[pos] = score;
      ids[pos] = i;
    }
    return ids;
  }

  /**
   * The fraction of {@code exact} that {@code approx} contains.
   */
  public static double recall(final int[] approx, final int[] exact) {
    if (exact.length == 0)
      return 1.0;
    int hits = 0;
    for (final int e : exact)
      for (final int a : approx)
        if (a == e) {
          hits++;
          break;
        }
    return (double) hits / exact.length;
  }

  private static double norm(final float[] v) {
    double sum = 0;
    for (final float x : v)
      sum += (double) x * x;
    final double n = Math.sqrt(sum);
    return n == 0 ? 1 : n;
  }
}
