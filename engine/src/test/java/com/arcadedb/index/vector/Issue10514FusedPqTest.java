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

import com.arcadedb.TestHelper;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.utility.Pair;
import io.github.jbellis.jvector.graph.GraphSearcher;
import io.github.jbellis.jvector.graph.RandomAccessVectorValues;
import io.github.jbellis.jvector.graph.SearchResult;
import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndex;
import io.github.jbellis.jvector.graph.disk.feature.FeatureId;
import io.github.jbellis.jvector.graph.disk.feature.FusedPQ;
import io.github.jbellis.jvector.graph.similarity.DefaultSearchScoreProvider;
import io.github.jbellis.jvector.graph.similarity.ScoreFunction;
import io.github.jbellis.jvector.quantization.PQVectors;
import io.github.jbellis.jvector.util.Bits;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10514: a PRODUCT index with the {@code fusedPQ} option walks its graph on the PQ codes fused into the nodes of the
 * graph file (jvector's {@code FusedPQ}) and re-ranks what it finds on the stored vectors.
 * <p>
 * The reader of that feature was switched off in March 2026 because the search returned a single result. The fused score
 * function leaves the packed codes and the neighbour list of a node in the graph view it was given, and the walk reads the
 * neighbour list of the view of its own searcher; the reader passed it a fresh view, so the walk saw no neighbour of the entry
 * node. It has to be given the view of the searcher.
 * <p>
 * 20,000 vectors of 64 dimensions in 100 clusters, a fixed seed: the graph file spans many pages, and the nodes (a
 * neighbour list and the codes of up to 32 neighbours) are not a divisor of a page, so nodes cross page boundaries.
 */
@Tag("vector")
class Issue10514FusedPqTest extends TestHelper {
  private static final int DIMENSIONS = 64;
  private static final int VECTORS    = 20_000;
  private static final int CLUSTERS   = 100;
  private static final int QUERIES    = 100;
  private static final int K          = 10;
  private static final int EF_SEARCH  = 100;

  private static final VectorTypeSupport VTS = VectorizationProvider.getInstance().getVectorTypeSupport();

  @Test
  void aReopenedFusedPqGraphAnswersAsWellAsTheSameGraphWalkedOnTheNonFusedPqCodes() throws Exception {
    final float[][] base = clustered(new Random(10514L), VECTORS);
    final float[][] queries = clustered(new Random(105140L), QUERIES);
    load(base);
    indexOf().buildVectorGraphNow();

    // A fresh session: the index is searched through the graph file, not through the graph it was built as in memory
    database.close();
    database = factory.open();
    final LSMVectorIndex index = indexOf();
    final long readsBefore = index.metrics.getVectorFetchFromDocuments();
    assertThat(index.findNeighborsFromVector(queries[0], K, EF_SEARCH, null)).as("the first search of the session").hasSize(K);
    // The walk is on the PQ codes fused into the nodes: the stored vectors are read to re-rank the beam only (the cache is cold)
    assertThat(index.metrics.getVectorFetchFromDocuments() - readsBefore).as("stored vectors read by the first search of the session")
        .isLessThanOrEqualTo(2L * EF_SEARCH);
    assertThat(index.isPQSearchAvailable()).isTrue();

    try (final OnDiskGraphIndex graph = index.getGraphFile().loadGraph()) {
      assertThat(graph.getFeatureSet()).contains(FeatureId.FUSED_PQ);
      assertThat(index.getGraphFile().getTotalPages()).as("pages of the graph file").isGreaterThan(30);
      assertThat(graph.getIdUpperBound()).isEqualTo(VECTORS);
    }

    final PQVectors pqVectors = (PQVectors) field("pqVectors").get(index);
    assertThat(pqVectors).isNotNull();
    final VectorSimilarityFunction similarity = VectorSimilarityFunction.EUCLIDEAN;
    // The graph numbers its nodes by ordinal; the vid of an ordinal is found by the vector it holds
    final java.lang.reflect.Method searchVectorValues = LSMVectorIndex.class.getDeclaredMethod("searchVectorValues", int[].class);
    searchVectorValues.setAccessible(true);
    final int[] ordinalToVectorId = index.getOrdinalToVectorIdForTest();
    assertThat(ordinalToVectorId).hasSize(VECTORS);
    final RandomAccessVectorValues stored = (RandomAccessVectorValues) searchVectorValues.invoke(index, (Object) ordinalToVectorId);
    final java.util.Map<Integer, Integer> vidOfHash = new java.util.HashMap<>();
    for (int vid = 0; vid < VECTORS; vid++)
      vidOfHash.put(java.util.Arrays.hashCode(base[vid]), vid);
    final int[] vidOfOrdinal = new int[VECTORS];
    for (int ordinal = 0; ordinal < VECTORS; ordinal++) {
      final VectorFloat<?> vector = stored.getVector(ordinal);
      final float[] copy = new float[DIMENSIONS];
      for (int j = 0; j < DIMENSIONS; j++)
        copy[j] = vector.get(j);
      vidOfOrdinal[ordinal] = vidOfHash.get(java.util.Arrays.hashCode(copy));
    }

    double exact = 0, fused = 0, nonFused = 0;
    final long scansBefore = index.metrics.getBruteForceScans();
    try (final OnDiskGraphIndex graph = index.getGraphFile().loadGraph()) {
      for (final float[] query : queries) {
        final Set<Integer> truth = nearest(base, query, K);
        final List<Pair<RID, Float>> answer = index.findNeighborsFromVector(query, K, EF_SEARCH, null);
        assertThat(answer).as("a search of the fused graph returns k results, not one").hasSize(K);
        fused += recall(truth, answer);

        // The same graph walked on the PQ codes of the PQ file instead of the codes fused into its nodes
        final VectorFloat<?> q = VTS.createFloatVector(query);
        final ScoreFunction.ExactScoreFunction rerank = node -> similarity.compare(q, VTS.createFloatVector(base[vidOfOrdinal[node]]));
        try (final GraphSearcher searcher = new GraphSearcher(graph)) {
          final SearchResult result = searcher.search(new DefaultSearchScoreProvider(
              pqVectors.precomputedScoreFunctionFor(q, similarity), rerank), K, EF_SEARCH, 0.0f, 0.0f, Bits.ALL);
          nonFused += recall(truth, result, vidOfOrdinal);
        }
        try (final GraphSearcher searcher = new GraphSearcher(graph)) {
          final SearchResult result = searcher.search(new DefaultSearchScoreProvider(rerank, rerank), K, EF_SEARCH, 0.0f, 0.0f,
              Bits.ALL);
          exact += recall(truth, result, vidOfOrdinal);
        }
      }
    }
    // A walk that finds one result is topped up by a brute-force scan, which hides it behind a full and exact answer
    assertThat(index.metrics.getBruteForceScans() - scansBefore).as("brute-force scans that topped up a short walk").isZero();
    exact /= QUERIES;
    fused /= QUERIES;
    nonFused /= QUERIES;

    assertThat(exact).as("the exact walk of this graph").isGreaterThan(0.9);
    assertThat(fused).as("the fused-code walk (non-fused PQ walk %.4f, exact walk %.4f)", nonFused, exact)
        .isCloseTo(nonFused, org.assertj.core.data.Offset.offset(0.01));
    assertThat(fused).isGreaterThan(0.9 * exact);
  }

  @Test
  void theFusedScoreFunctionHasToBeGivenTheViewOfTheSearcher() throws Exception {
    final float[][] base = clustered(new Random(1L), 5000);
    final float[] query = clustered(new Random(2L), 1)[0];
    load(base);
    indexOf().buildVectorGraphNow();

    final VectorSimilarityFunction similarity = VectorSimilarityFunction.EUCLIDEAN;
    final VectorFloat<?> q = VTS.createFloatVector(query);
    final ScoreFunction.ExactScoreFunction exact = node -> similarity.compare(q, VTS.createFloatVector(base[node]));

    try (final OnDiskGraphIndex graph = indexOf().getGraphFile().loadGraph()) {
      final FusedPQ fusedPQ = (FusedPQ) graph.getFeatures().get(FeatureId.FUSED_PQ);

      // The view of the searcher: the walk reaches the neighbours of the entry node and finds k results
      try (final GraphSearcher searcher = new GraphSearcher(graph)) {
        final ScoreFunction.ApproximateScoreFunction fused = fusedPQ.approximateScoreFunctionFor(q, similarity,
            (OnDiskGraphIndex.View) searcher.getView(), exact);
        assertThat(searcher.search(new DefaultSearchScoreProvider(fused, exact), K, EF_SEARCH, 0.0f, 0.0f, Bits.ALL).getNodes())
            .hasSize(K);
      }

      // Any other view, as the reader switched off in March 2026 did: the codes and the neighbour list of a node are left in
      // the view given to the score function, and the walk looks for them in its own
      try (final GraphSearcher searcher = new GraphSearcher(graph)) {
        final ScoreFunction.ApproximateScoreFunction fused = fusedPQ.approximateScoreFunctionFor(q, similarity, graph.getView(),
            exact);
        assertThat(searcher.search(new DefaultSearchScoreProvider(fused, exact), K, EF_SEARCH, 0.0f, 0.0f, Bits.ALL).getNodes())
            .as("results of a walk scored through a view that is not its own").hasSizeLessThan(K);
      }
    }
  }

  private double recall(final Set<Integer> truth, final List<Pair<RID, Float>> answer) {
    int hits = 0;
    for (final Pair<RID, Float> neighbour : answer)
      if (truth.contains(database.lookupByRID(neighbour.getFirst(), true).asDocument().getInteger("vid")))
        hits++;
    return (double) hits / K;
  }

  private static double recall(final Set<Integer> truth, final SearchResult result, final int[] vidOfOrdinal) {
    int hits = 0;
    for (final SearchResult.NodeScore node : result.getNodes())
      if (truth.contains(vidOfOrdinal[node.node]))
        hits++;
    return (double) hits / K;
  }

  private LSMVectorIndex indexOf() {
    return (LSMVectorIndex) ((TypeIndex) database.getSchema().getIndexByName("Doc[embedding]")).getIndexesOnBuckets()[0];
  }

  private static Field field(final String name) throws Exception {
    final Field f = LSMVectorIndex.class.getDeclaredField(name);
    f.setAccessible(true);
    return f;
  }

  private void load(final float[][] base) {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.vid INTEGER");
      database.command("sql", "CREATE PROPERTY Doc.embedding ARRAY_OF_FLOATS");
      database.command("sql", "CREATE INDEX ON Doc (embedding) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
          + ", \"similarity\": \"EUCLIDEAN\", \"quantization\": \"PRODUCT\", \"fusedPQ\": true, \"storeVectorsInGraph\": false, "
          + "\"addHierarchy\": false }");
    });
    database.transaction(() -> {
      for (int i = 0; i < base.length; i++) {
        final MutableDocument doc = database.newDocument("Doc");
        doc.set("vid", i);
        doc.set("embedding", base[i]);
        doc.save();
      }
    });
  }

  /** {@code count} vectors around {@link #CLUSTERS} centres; the centres come from their own fixed seed, so base and queries share them. */
  private static float[][] clustered(final Random random, final int count) {
    final Random centreRandom = new Random(42L);
    final float[][] centres = new float[CLUSTERS][DIMENSIONS];
    for (final float[] centre : centres)
      for (int j = 0; j < DIMENSIONS; j++)
        centre[j] = (float) (centreRandom.nextGaussian() * 3.0);
    final float[][] vectors = new float[count][DIMENSIONS];
    for (int i = 0; i < count; i++) {
      final float[] centre = centres[random.nextInt(CLUSTERS)];
      for (int j = 0; j < DIMENSIONS; j++)
        vectors[i][j] = centre[j] + (float) random.nextGaussian();
    }
    return vectors;
  }

  private static Set<Integer> nearest(final float[][] base, final float[] query, final int k) {
    final double[] distance = new double[base.length];
    final Integer[] order = new Integer[base.length];
    for (int i = 0; i < base.length; i++) {
      distance[i] = squaredDistance(query, base[i]);
      order[i] = i;
    }
    java.util.Arrays.sort(order, java.util.Comparator.comparingDouble(i -> distance[i]));
    final Set<Integer> top = new HashSet<>();
    for (int i = 0; i < k; i++)
      top.add(order[i]);
    return top;
  }

  private static float squaredDistance(final float[] a, final float[] b) {
    double sum = 0;
    for (int i = 0; i < a.length; i++) {
      final double d = a[i] - b[i];
      sum += d * d;
    }
    return (float) sum;
  }
}
