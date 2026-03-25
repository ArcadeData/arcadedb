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

import io.github.jbellis.jvector.graph.GraphIndexBuilder;
import io.github.jbellis.jvector.graph.GraphSearcher;
import io.github.jbellis.jvector.graph.ListRandomAccessVectorValues;
import io.github.jbellis.jvector.graph.RandomAccessVectorValues;
import io.github.jbellis.jvector.graph.SearchResult;
import io.github.jbellis.jvector.graph.similarity.BuildScoreProvider;
import io.github.jbellis.jvector.graph.similarity.SearchScoreProvider;
import io.github.jbellis.jvector.graph.similarity.DefaultSearchScoreProvider;
import io.github.jbellis.jvector.graph.similarity.ScoreFunction;
import io.github.jbellis.jvector.util.Bits;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that JVector's incremental build API (addGraphNode) works correctly for
 * the ArcadeDB integration use case: insert vectors one at a time, search immediately.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class JVectorIncrementalBuildTest {

  private static final VectorTypeSupport vts = VectorizationProvider.getInstance().getVectorTypeSupport();
  private static final int DIMENSIONS = 64;
  private static final VectorSimilarityFunction SIMILARITY = VectorSimilarityFunction.COSINE;

  @Test
  void incrementalBuildAndSearch() throws Exception {
    final int numVectors = 1000;
    final int k = 10;
    final Random rng = new Random(42);

    // Generate vectors
    final List<VectorFloat<?>> vectors = new ArrayList<>();
    for (int i = 0; i < numVectors; i++)
      vectors.add(randomNormalizedVector(rng));

    // Create a growable RandomAccessVectorValues backed by a ConcurrentHashMap
    final GrowableVectorValues growableVectors = new GrowableVectorValues(DIMENSIONS);

    // Create BuildScoreProvider from the growable vector values
    final BuildScoreProvider scoreProvider = BuildScoreProvider.randomAccessScoreProvider(growableVectors, SIMILARITY);

    // Create builder — this will live across all insertions
    try (final GraphIndexBuilder builder = new GraphIndexBuilder(
        scoreProvider,
        DIMENSIONS,
        16,    // M
        100,   // beamWidth (efConstruction)
        1.2f,  // neighborOverflow
        1.2f,  // alpha
        false, // hierarchy
        true   // concurrent
    )) {

      // Insert vectors one at a time
      for (int i = 0; i < numVectors; i++) {
        // 1. Add to our growable values (makes it available for scoring against existing nodes)
        growableVectors.addVector(i, vectors.get(i));

        // 2. Add to the graph
        builder.addGraphNode(i, vectors.get(i));

        // 3. Verify graph is searchable after every 100 inserts
        if ((i + 1) % 100 == 0) {
          final var graph = builder.getGraph();
          assertThat(graph.size()).isEqualTo(i + 1);

          // Search for a known vector — should find itself as nearest neighbor
          final VectorFloat<?> query = vectors.get(i);
          try (final GraphSearcher searcher = new GraphSearcher(graph)) {
            final ScoreFunction.ExactScoreFunction exactScore = (node) ->
                SIMILARITY.compare(query, growableVectors.getVector(node));
            final DefaultSearchScoreProvider ssp = new DefaultSearchScoreProvider(exactScore, exactScore);
            final SearchResult result = searcher.search(ssp, k, Bits.ALL);

            assertThat(result.getNodes().length).isGreaterThan(0);

            // The query vector itself should be the top result
            boolean foundSelf = false;
            for (final SearchResult.NodeScore ns : result.getNodes()) {
              if (ns.node == i) {
                foundSelf = true;
                break;
              }
            }
            assertThat(foundSelf).as("Vector %d should find itself in top-%d results", i, k).isTrue();
          }
        }
      }

      // Final verification: search with all 1000 vectors inserted
      final var graph = builder.getGraph();
      assertThat(graph.size()).isEqualTo(numVectors);

      // Compute recall by checking if nearest neighbors are correct
      int totalHits = 0;
      int totalQueries = 0;
      for (int q = 0; q < 50; q++) {
        final VectorFloat<?> query = vectors.get(q);

        // Brute force ground truth
        final float[] scores = new float[numVectors];
        for (int i = 0; i < numVectors; i++)
          scores[i] = SIMILARITY.compare(query, vectors.get(i));

        final Integer[] indices = new Integer[numVectors];
        for (int i = 0; i < numVectors; i++)
          indices[i] = i;
        Arrays.sort(indices, (a, b) -> Float.compare(scores[b], scores[a]));

        final Set<Integer> gtTopK = new HashSet<>();
        for (int i = 0; i < k; i++)
          gtTopK.add(indices[i]);

        // HNSW search
        try (final GraphSearcher searcher = new GraphSearcher(graph)) {
          final ScoreFunction.ExactScoreFunction exactScore = (node) ->
              SIMILARITY.compare(query, growableVectors.getVector(node));
          final DefaultSearchScoreProvider ssp = new DefaultSearchScoreProvider(exactScore, exactScore);
          final SearchResult result = searcher.search(ssp, k, 100, 0.0f, 0.0f, Bits.ALL);

          for (final SearchResult.NodeScore ns : result.getNodes()) {
            if (gtTopK.contains(ns.node))
              totalHits++;
          }
          totalQueries++;
        }
      }

      final double recall = (double) totalHits / (totalQueries * k);
      System.out.printf("Incremental build recall@%d: %.3f (%d queries on %d vectors)%n", k, recall, totalQueries, numVectors);
      assertThat(recall).as("Recall should be reasonable for incremental build").isGreaterThan(0.5);
    }
  }

  @Test
  void incrementalBuildWithDelete() throws Exception {
    final int numVectors = 500;
    final Random rng = new Random(42);

    final List<VectorFloat<?>> vectors = new ArrayList<>();
    for (int i = 0; i < numVectors; i++)
      vectors.add(randomNormalizedVector(rng));

    final GrowableVectorValues growableVectors = new GrowableVectorValues(DIMENSIONS);
    final BuildScoreProvider scoreProvider = BuildScoreProvider.randomAccessScoreProvider(growableVectors, SIMILARITY);

    try (final GraphIndexBuilder builder = new GraphIndexBuilder(
        scoreProvider, DIMENSIONS, 16, 100, 1.2f, 1.2f, false, true)) {

      // Insert all vectors
      for (int i = 0; i < numVectors; i++) {
        growableVectors.addVector(i, vectors.get(i));
        builder.addGraphNode(i, vectors.get(i));
      }

      assertThat(builder.getGraph().size()).isEqualTo(numVectors);

      // Delete every other vector
      for (int i = 0; i < numVectors; i += 2)
        builder.markNodeDeleted(i);

      // Remove deleted nodes
      builder.removeDeletedNodes();

      // Graph should have half the vectors
      final var graph = builder.getGraph();
      assertThat(graph.size()).isEqualTo(numVectors / 2);

      // Search should still work and only return non-deleted nodes
      try (final GraphSearcher searcher = new GraphSearcher(graph)) {
        final VectorFloat<?> query = vectors.get(1); // Use a non-deleted vector
        final ScoreFunction.ExactScoreFunction exactScore = (node) ->
            SIMILARITY.compare(query, growableVectors.getVector(node));
        final DefaultSearchScoreProvider ssp = new DefaultSearchScoreProvider(exactScore, exactScore);
        final SearchResult result = searcher.search(ssp, 10, Bits.ALL);

        assertThat(result.getNodes().length).isGreaterThan(0);
        // All returned nodes should be odd (non-deleted)
        for (final SearchResult.NodeScore ns : result.getNodes())
          assertThat(ns.node % 2).as("Node %d should be odd (non-deleted)", ns.node).isEqualTo(1);
      }
    }
  }

  private VectorFloat<?> randomNormalizedVector(final Random rng) {
    final float[] v = new float[DIMENSIONS];
    float norm = 0;
    for (int d = 0; d < DIMENSIONS; d++) {
      v[d] = (float) rng.nextGaussian();
      norm += v[d] * v[d];
    }
    norm = (float) Math.sqrt(norm);
    for (int d = 0; d < DIMENSIONS; d++)
      v[d] /= norm;
    return vts.createFloatVector(v);
  }

  /**
   * Growable RandomAccessVectorValues backed by ConcurrentHashMap.
   * Vectors can be added after construction — essential for incremental build.
   */
  static class GrowableVectorValues implements RandomAccessVectorValues {
    private final int dimensions;
    private final ConcurrentHashMap<Integer, VectorFloat<?>> vectors = new ConcurrentHashMap<>();

    GrowableVectorValues(final int dimensions) {
      this.dimensions = dimensions;
    }

    void addVector(final int ordinal, final VectorFloat<?> vector) {
      vectors.put(ordinal, vector);
    }

    @Override
    public int size() {
      return vectors.size();
    }

    @Override
    public int dimension() {
      return dimensions;
    }

    @Override
    public VectorFloat<?> getVector(final int ordinal) {
      return vectors.get(ordinal);
    }

    @Override
    public boolean isValueShared() {
      return false;
    }

    @Override
    public RandomAccessVectorValues copy() {
      return this; // Thread-safe via ConcurrentHashMap
    }
  }
}
