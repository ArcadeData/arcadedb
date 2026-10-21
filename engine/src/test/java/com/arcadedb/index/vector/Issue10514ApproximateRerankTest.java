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
import com.arcadedb.query.select.SelectVectorResult;
import com.arcadedb.utility.Pair;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10514: the PQ search of a PRODUCT index returned the nearest of the candidates by their PQ scores, and PQ cannot
 * order neighbours that share most of their codes, so its recall stopped improving at any beam. With a re-rank the
 * candidates are scored again on the stored vectors, as the BINARY quantization does.
 * <p>
 * 20,000 vectors of 64 dimensions in 100 clusters, a fixed seed, and the exact top 10 of 100 queries computed here.
 */
@Tag("vector")
class Issue10514ApproximateRerankTest extends TestHelper {
  private static final int DIMENSIONS = 64;
  private static final int VECTORS    = 20_000;
  private static final int CLUSTERS   = 100;
  private static final int QUERIES    = 100;
  private static final int K          = 10;
  private static final int WIDE_OVERSAMPLE = 30;

  @Test
  void rerankingThePqCandidatesRecoversTheRecallOfTheExactSearchAndReturnsExactDistances() {
    final float[][] base = clustered(new Random(10514L), VECTORS);
    final float[][] queries = clustered(new Random(105140L), QUERIES);
    load(base);

    final LSMVectorIndex index = indexOf();
    index.buildVectorGraphNow();
    assertThat(index.isPQSearchAvailable()).as("the scenario needs a trained codebook").isTrue();

    float worstDistanceError = 0;
    double exact = 0, pqOnly = 0, reranked = 0, rerankedWide = 0, selectPqOnly = 0, selectReranked = 0;
    for (final float[] query : queries) {
      final Set<Integer> truth = nearest(base, query, K);

      exact += recall(truth, index.findNeighborsFromVector(query, K, 100, null));
      pqOnly += recall(truth, index.findNeighborsFromVectorApproximate(query, K));
      final List<Pair<RID, Float>> answer = index.findNeighborsFromVectorApproximate(query, K, -1,
          LSMVectorIndex.DEFAULT_APPROXIMATE_RERANK_OVERSAMPLE);
      reranked += recall(truth, answer);
      rerankedWide += recall(truth, index.findNeighborsFromVectorApproximate(query, K, -1, WIDE_OVERSAMPLE));

      // The distances are the exact ones: squared L2 against the stored vector, ascending
      assertThat(answer).hasSize(K);
      float previous = -1;
      for (final Pair<RID, Float> neighbour : answer) {
        final float expected = squaredDistance(query, base[id(neighbour.getFirst())]);
        worstDistanceError = Math.max(worstDistanceError, Math.abs(neighbour.getSecond() - expected) / Math.max(1f, expected));
        assertThat(neighbour.getSecond()).isGreaterThanOrEqualTo(previous);
        previous = neighbour.getSecond();
      }

      // The fluent select reaches the same two modes
      selectPqOnly += selectRecall(truth, query, 0);
      selectReranked += selectRecall(truth, query, LSMVectorIndex.DEFAULT_APPROXIMATE_RERANK_OVERSAMPLE);
    }
    exact /= QUERIES;
    pqOnly /= QUERIES;
    reranked /= QUERIES;
    rerankedWide /= QUERIES;
    selectPqOnly /= QUERIES;
    selectReranked /= QUERIES;

    assertThat(exact).as("the exact search on this data").isGreaterThan(0.9);
    assertThat(rerankedWide).as("re-ranking %d candidates per neighbour (exact %.4f, PQ scores only %.4f)", WIDE_OVERSAMPLE, exact,
        pqOnly).isGreaterThanOrEqualTo(exact - 0.02);
    assertThat(reranked).as("the default of %d candidates per neighbour is close behind", LSMVectorIndex.DEFAULT_APPROXIMATE_RERANK_OVERSAMPLE)
        .isGreaterThanOrEqualTo(exact - 0.08);
    assertThat(reranked).as("and clearly above the PQ scores alone").isGreaterThan(pqOnly + 0.30);
    assertThat(selectReranked).as("approximate(true).rerank() through the fluent select").isGreaterThan(selectPqOnly + 0.30);
    assertThat(selectReranked).isGreaterThanOrEqualTo(exact - 0.08);
    assertThat(worstDistanceError).as("the returned distances are the exact squared L2 distances (relative error)")
        .isLessThan(1e-4f);
  }

  @Test
  void withoutARerankTheAnswerIsTheOneOfBefore() {
    final float[][] base = clustered(new Random(1L), 3000);
    load(base);
    final LSMVectorIndex index = indexOf();
    index.buildVectorGraphNow();

    final float[] query = base[7];
    final List<Pair<RID, Float>> before = index.findNeighborsFromVectorApproximate(query, K);
    final List<Pair<RID, Float>> zero = index.findNeighborsFromVectorApproximate(query, K, -1, 0);
    assertThat(zero).isEqualTo(before);
  }

  @Test
  void aRerankHonoursTheAllowListAndStillReturnsExactDistances() {
    final float[][] base = clustered(new Random(2L), 3000);
    load(base);
    final LSMVectorIndex index = indexOf();
    index.buildVectorGraphNow();

    // The even ids: half of the index, so the walk is filtered and not answered by the pre-filter plan
    final Set<RID> allowed = new HashSet<>();
    database.iterateType("Doc", false).forEachRemaining(r -> {
      if (r.asDocument().getInteger("vid") % 2 == 0)
        allowed.add(r.getIdentity());
    });

    final float[] query = base[11];
    final List<Pair<RID, Float>> answer = index.findNeighborsFromVectorApproximate(query, K, -1,
        LSMVectorIndex.DEFAULT_APPROXIMATE_RERANK_OVERSAMPLE, allowed, com.arcadedb.query.sql.executor.WorkGuard.forCommandDeadline(null));
    assertThat(answer).hasSize(K);
    for (final Pair<RID, Float> neighbour : answer) {
      assertThat(allowed).contains(neighbour.getFirst());
      final float expected = squaredDistance(query, base[id(neighbour.getFirst())]);
      assertThat(neighbour.getSecond()).isCloseTo(expected, org.assertj.core.data.Offset.offset(Math.max(1e-3f, expected * 1e-4f)));
    }
  }

  @Test
  void aNegativeOversampleIsRefused() {
    final float[][] base = clustered(new Random(3L), 3000);
    load(base);
    final LSMVectorIndex index = indexOf();
    assertThatThrownBy(() -> index.findNeighborsFromVectorApproximate(base[0], K, -1, -1))
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("rerankOversample");
  }

  private LSMVectorIndex indexOf() {
    return (LSMVectorIndex) ((TypeIndex) database.getSchema().getIndexByName("Doc[embedding]")).getIndexesOnBuckets()[0];
  }

  private double selectRecall(final Set<Integer> truth, final float[] query, final int oversample) {
    final List<SelectVectorResult<com.arcadedb.database.Document>> rows = database.select().fromType("Doc")
        .nearestTo("embedding", query, K).approximate(true).rerank(oversample).documents();
    int hits = 0;
    for (final SelectVectorResult<com.arcadedb.database.Document> row : rows)
      if (truth.contains(row.getDocument().getInteger("vid")))
        hits++;
    return (double) hits / K;
  }

  private int id(final RID rid) {
    return database.lookupByRID(rid, true).asDocument().getInteger("vid");
  }

  private double recall(final Set<Integer> truth, final List<Pair<RID, Float>> answer) {
    int hits = 0;
    for (final Pair<RID, Float> neighbour : answer)
      if (truth.contains(id(neighbour.getFirst())))
        hits++;
    return (double) hits / K;
  }

  private void load(final float[][] base) {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.vid INTEGER");
      database.command("sql", "CREATE PROPERTY Doc.embedding ARRAY_OF_FLOATS");
      database.command("sql", "CREATE INDEX ON Doc (embedding) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
          + ", \"similarity\": \"EUCLIDEAN\", \"quantization\": \"PRODUCT\", \"storeVectorsInGraph\": false, "
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
