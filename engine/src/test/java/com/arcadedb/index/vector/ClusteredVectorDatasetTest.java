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

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Comparator;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for #11711: the vector benchmarks drew their queries from centroids the data did not contain, with a
 * per-dimension noise that swamped the clusters, so no ANN index could reach a meaningful recall on them and the
 * latency numbers described a degenerate workload. {@link ClusteredVectorDataset} is the generator they share now.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class ClusteredVectorDatasetTest {
  private static final int DIMS = 384;

  @Test
  void queriesComeFromTheCentroidsOfTheData() {
    final ClusteredVectorDataset ds = ClusteredVectorDataset.generate(2_000, 50, DIMS, 50,
        ClusteredVectorDataset.defaultSpread(DIMS), 42L);

    // A query is a sample of a centroid the data was drawn from: its nearest data vector is a sibling of the same
    // cluster (expected cosine about 0.5). With queries from centroids of their own it was about 0 for every vector.
    final int[][] nearest = ds.exactNeighbors(1);
    for (int q = 0; q < ds.queries.length; q++)
      assertThat(ClusteredVectorDataset.dot(ds.queries[q], ds.data[nearest[q][0]])).isGreaterThan(0.35f);
  }

  @Test
  void theDefaultSpreadKeepsTheClustersSeparable() {
    final ClusteredVectorDataset ds = ClusteredVectorDataset.generate(2_000, 1, DIMS, 50,
        ClusteredVectorDataset.defaultSpread(DIMS), 7L);

    // data[i] belongs to cluster i % 50: compare siblings with vectors of other clusters
    double sameCluster = 0;
    double otherCluster = 0;
    final int pairs = 500;
    for (int i = 0; i < pairs; i++) {
      sameCluster += ClusteredVectorDataset.dot(ds.data[i], ds.data[i + 50]);
      otherCluster += ClusteredVectorDataset.dot(ds.data[i], ds.data[i + 1]);
    }
    sameCluster /= pairs;
    otherCluster /= pairs;

    // Expected about 0.5 against about 0 (standard deviation 1/sqrt(384) ~ 0.05). The fixed spread of 0.15 the
    // benchmarks used gave about 0.10 for siblings: the clusters were noise.
    assertThat(sameCluster).isBetween(0.4, 0.6);
    assertThat(Math.abs(otherCluster)).isLessThan(0.05);
  }

  @Test
  void exactNeighborsAreTheBruteForceTopKClosestFirst() {
    final ClusteredVectorDataset ds = ClusteredVectorDataset.generate(3_000, 20, 32, 30,
        ClusteredVectorDataset.defaultSpread(32), 11L);
    final int k = 25;
    final int[][] neighbors = ds.exactNeighbors(k);

    for (int q = 0; q < ds.queries.length; q++) {
      final float[] query = ds.queries[q];
      final int[] expected = IntStream.range(0, ds.data.length).boxed()
          .sorted(Comparator.comparingDouble((Integer i) -> -ClusteredVectorDataset.dot(query, ds.data[i])))
          .limit(k).mapToInt(Integer::intValue).toArray();

      assertThat(neighbors[q]).hasSize(k);
      // Compare the scores, not the indexes: two data vectors at the same distance may come in either order
      for (int i = 0; i < k; i++)
        assertThat(ClusteredVectorDataset.dot(query, ds.data[neighbors[q][i]]))
            .isEqualTo(ClusteredVectorDataset.dot(query, ds.data[expected[i]]));
    }
  }

  @Test
  void exactNeighborsAreCappedByTheDataSize() {
    final ClusteredVectorDataset ds = ClusteredVectorDataset.generate(5, 3, 8, 2, 0.1f, 3L);
    final int[][] neighbors = ds.exactNeighbors(10);
    for (final int[] n : neighbors) {
      assertThat(n).hasSize(5);
      assertThat(Arrays.stream(n).sorted().toArray()).containsExactly(0, 1, 2, 3, 4);
    }
  }

  @Test
  void recallCountsTheExpectedNeighborsFound() {
    assertThat(ClusteredVectorDataset.recall(new int[] { 1, 2, 3, 4 }, new int[] { 4, 9, 1 })).isEqualTo(0.5);
    assertThat(ClusteredVectorDataset.recall(new int[] { 1, 2 }, new int[0])).isEqualTo(0.0);
    assertThat(ClusteredVectorDataset.recall(new int[] { 1, 2 }, new int[] { 2, 1 })).isEqualTo(1.0);
  }

  @Test
  void generationIsDeterministicForASeed() {
    final ClusteredVectorDataset a = ClusteredVectorDataset.generate(100, 10, 16, 5, 0.25f, 99L);
    final ClusteredVectorDataset b = ClusteredVectorDataset.generate(100, 10, 16, 5, 0.25f, 99L);
    assertThat(a.data).isDeepEqualTo(b.data);
    assertThat(a.queries).isDeepEqualTo(b.queries);
  }
}
