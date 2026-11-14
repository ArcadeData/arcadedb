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

import java.util.Random;
import java.util.stream.IntStream;

/**
 * Unit-norm clustered vectors for the vector benchmarks, with the exact nearest neighbours of every query, so a
 * latency number is always reported next to the recall it was achieved at (#11711).
 * <p>
 * The data and the queries are drawn from ONE set of centroids: a query is a fresh sample of a centroid the data was
 * drawn from, never a data vector and never a sample of a centroid the data does not contain. Drawing the queries from
 * centroids of their own makes every data vector roughly equidistant from every query (random centroids are nearly
 * orthogonal in high dimensions), which no ANN index can search.
 * <p>
 * The per-dimension Gaussian noise defaults to {@link #defaultSpread(int)}, {@code 1 / sqrt(dims)}, so the noise vector
 * has about the norm of the unit centroid and two samples of one cluster have an expected cosine of about 0.5. A fixed
 * per-dimension spread grows the noise with the dimension count: 0.15 in 384 dimensions is a noise of norm 2.9 against
 * a centroid of norm 1, and the clusters all but disappear.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class ClusteredVectorDataset {
  final float[][] data;
  final float[][] queries;
  final int       numClusters;
  final float     spread;

  private ClusteredVectorDataset(final float[][] data, final float[][] queries, final int numClusters, final float spread) {
    this.data = data;
    this.queries = queries;
    this.numClusters = numClusters;
    this.spread = spread;
  }

  /**
   * The per-dimension noise that gives a noise vector of about unit norm in {@code dims} dimensions.
   */
  static float defaultSpread(final int dims) {
    return (float) (1.0 / Math.sqrt(dims));
  }

  /**
   * Generates {@code numData} data vectors (cluster {@code i % numClusters} for the i-th) and {@code numQueries} query
   * vectors, each a fresh sample of a randomly chosen centroid of the same set. Deterministic for a given seed.
   * <p>
   * More clusters than data vectors would leave centroids no data vector was drawn from, and a query drawn from one of
   * them would be out of distribution again: the cluster count is capped at {@code numData} for the data and the queries
   * alike, and {@link #numClusters} is the count used.
   */
  static ClusteredVectorDataset generate(final int numData, final int numQueries, final int dims, final int requestedClusters,
      final float spread, final long seed) {
    if (numData < 1 || numQueries < 1 || dims < 1 || requestedClusters < 1)
      throw new IllegalArgumentException(
          "numData, numQueries, dims and numClusters must be positive: " + numData + ", " + numQueries + ", " + dims + ", "
              + requestedClusters);

    final int numClusters = Math.min(requestedClusters, numData);
    final Random rng = new Random(seed);
    final float[][] centroids = new float[numClusters][];
    for (int c = 0; c < numClusters; c++) {
      final float[] centroid = new float[dims];
      for (int d = 0; d < dims; d++)
        centroid[d] = (float) rng.nextGaussian();
      centroids[c] = normalize(centroid);
    }

    final float[][] data = new float[numData][];
    for (int i = 0; i < numData; i++)
      data[i] = sample(centroids[i % numClusters], spread, rng);

    final float[][] queries = new float[numQueries][];
    for (int q = 0; q < numQueries; q++)
      queries[q] = sample(centroids[rng.nextInt(numClusters)], spread, rng);

    return new ClusteredVectorDataset(data, queries, numClusters, spread);
  }

  /**
   * The indexes (into {@link #data}) of the {@code k} data vectors with the highest cosine similarity to each query,
   * closest first, by brute force over the whole data set. Queries are processed in parallel: this is test code and
   * the scan is the only expensive part of a benchmark's setup.
   */
  int[][] exactNeighbors(final int k) {
    final int limit = Math.min(k, data.length);
    final int[][] result = new int[queries.length][];
    IntStream.range(0, queries.length).parallel().forEach(q -> result[q] = topK(queries[q], limit));
    return result;
  }

  /**
   * Fraction of {@code expected} found in {@code actual}: recall@K where K is {@code expected.length}. An empty
   * {@code actual} is a recall of 0, never a query to skip.
   */
  static double recall(final int[] expected, final int[] actual) {
    if (expected.length == 0)
      return 1.0;
    int hits = 0;
    for (final int e : expected)
      for (final int a : actual)
        if (a == e) {
          ++hits;
          break;
        }
    return (double) hits / expected.length;
  }

  static float dot(final float[] a, final float[] b) {
    float sum = 0;
    for (int i = 0; i < a.length; i++)
      sum += a[i] * b[i];
    return sum;
  }

  /**
   * Bounded min-heap over primitive arrays: the root is the weakest of the best {@code k} so far.
   */
  private int[] topK(final float[] query, final int k) {
    final float[] heapScore = new float[k];
    final int[] heapIndex = new int[k];
    int size = 0;
    for (int i = 0; i < data.length; i++) {
      // Every vector is unit norm, so the dot product is the cosine similarity
      final float score = dot(query, data[i]);
      if (size < k) {
        int child = size++;
        while (child > 0) {
          final int parent = (child - 1) >>> 1;
          if (heapScore[parent] <= score)
            break;
          heapScore[child] = heapScore[parent];
          heapIndex[child] = heapIndex[parent];
          child = parent;
        }
        heapScore[child] = score;
        heapIndex[child] = i;
      } else if (score > heapScore[0]) {
        int parent = 0;
        while (true) {
          int child = 2 * parent + 1;
          if (child >= k)
            break;
          if (child + 1 < k && heapScore[child + 1] < heapScore[child])
            ++child;
          if (heapScore[child] >= score)
            break;
          heapScore[parent] = heapScore[child];
          heapIndex[parent] = heapIndex[child];
          parent = child;
        }
        heapScore[parent] = score;
        heapIndex[parent] = i;
      }
    }

    // Pop the heap from the weakest, filling the result from the end so it reads closest first
    final int[] sorted = new int[size];
    for (int n = size - 1; n >= 0; n--) {
      sorted[n] = heapIndex[0];
      final float lastScore = heapScore[n];
      final int lastIndex = heapIndex[n];
      int parent = 0;
      while (true) {
        int child = 2 * parent + 1;
        if (child >= n)
          break;
        if (child + 1 < n && heapScore[child + 1] < heapScore[child])
          ++child;
        if (heapScore[child] >= lastScore)
          break;
        heapScore[parent] = heapScore[child];
        heapIndex[parent] = heapIndex[child];
        parent = child;
      }
      heapScore[parent] = lastScore;
      heapIndex[parent] = lastIndex;
    }
    return sorted;
  }

  private static float[] sample(final float[] centroid, final float spread, final Random rng) {
    final float[] v = new float[centroid.length];
    for (int d = 0; d < v.length; d++)
      v[d] = centroid[d] + (float) (rng.nextGaussian() * spread);
    return normalize(v);
  }

  private static float[] normalize(final float[] v) {
    final float norm = (float) Math.sqrt(dot(v, v));
    for (int d = 0; d < v.length; d++)
      v[d] /= norm;
    return v;
  }
}
