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
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10514: {@code vector.neighbors} on a PRODUCT index scored every node its walk visited on the stored vectors, as a
 * NONE index does, so the PQ codes were never used to search. It now walks on the PQ codes and re-ranks the best candidates
 * on the stored vectors, as BINARY does, and takes a {@code rerank} option for how many.
 * <p>
 * 20,000 vectors of 64 dimensions in 100 clusters, a fixed seed, and the exact top 10 of 100 queries computed here.
 */
@Tag("vector")
class Issue10514SqlRerankTest extends TestHelper {
  private static final int DIMENSIONS = 64;
  private static final int VECTORS    = 20_000;
  private static final int CLUSTERS   = 100;
  private static final int QUERIES    = 100;
  private static final int K          = 10;

  private static final String SQL = "SELECT vid, distance FROM (SELECT expand(`vector.neighbors`('Doc[embedding]', ?, ?, ?)))";

  @Test
  void aPrimaryIndexWalksOnThePqCodesAndReturnsTheExactDistances() {
    final float[][] base = clustered(new Random(10514L), VECTORS);
    final float[][] queries = clustered(new Random(105140L), QUERIES);
    load(base, "PRODUCT");
    final LSMVectorIndex index = indexOf();
    index.buildVectorGraphNow();
    assertThat(index.isPQSearchAvailable()).as("the scenario needs a trained codebook").isTrue();

    double exact = 0, sql = 0, sqlPqScores = 0;
    float worstDistanceError = 0;
    long readsOfTheDefault = 0;
    for (final float[] query : queries) {
      final Set<Integer> truth = nearest(base, query, K);
      exact += recall(truth, index.findNeighborsFromVector(query, K, 100, null).stream()
          .map(p -> vid(p.getFirst())).toList());

      final long before = index.metrics.getVectorFetchFromDocuments();
      final List<Result> rows = sql(query, Map.of("efSearch", 100));
      readsOfTheDefault += index.metrics.getVectorFetchFromDocuments() - before;
      assertThat(rows).hasSize(K);
      final List<Integer> ids = new ArrayList<>();
      float previous = -1;
      for (final Result row : rows) {
        final int id = row.getProperty("vid");
        ids.add(id);
        final float distance = row.getProperty("distance");
        final float expected = squaredDistance(query, base[id]);
        worstDistanceError = Math.max(worstDistanceError, Math.abs(distance - expected) / Math.max(1f, expected));
        assertThat(distance).isGreaterThanOrEqualTo(previous);
        previous = distance;
      }
      sql += recall(truth, ids);

      final List<Integer> pqIds = new ArrayList<>();
      for (final Result row : sql(query, Map.of("efSearch", 100, "rerank", 0)))
        pqIds.add(row.getProperty("vid"));
      sqlPqScores += recall(truth, pqIds);
    }
    exact /= QUERIES;
    sql /= QUERIES;
    sqlPqScores /= QUERIES;

    assertThat(exact).as("the exact search on this data").isGreaterThan(0.9);
    assertThat(sql).as("vector.neighbors with the default re-rank (exact %.4f, PQ scores only %.4f)", exact, sqlPqScores)
        .isGreaterThanOrEqualTo(0.9 * exact);
    assertThat(sql).as("and clearly above the PQ scores alone").isGreaterThan(sqlPqScores + 0.2);
    assertThat(worstDistanceError).as("the returned distances are the exact squared L2 distances (relative error)")
        .isLessThan(1e-4f);

    // The walk is on the PQ codes: the stored vectors are read for the re-rank only, not for every node visited. The exact
    // walk with the same beam reads a stored vector for each node it visits, which is several times the candidates re-ranked.
    final double readsPerQuery = (double) readsOfTheDefault / QUERIES;
    assertThat(readsPerQuery).as("stored-vector reads per query of the default re-rank, against %d candidates",
        K * LSMVectorIndex.DEFAULT_APPROXIMATE_RERANK_OVERSAMPLE).isLessThan(2.0 * K * LSMVectorIndex.DEFAULT_APPROXIMATE_RERANK_OVERSAMPLE);
  }

  @Test
  void theExactWalkReadsFarMoreStoredVectorsThanTheRerank() {
    final float[][] base = clustered(new Random(7L), VECTORS);
    load(base, "PRODUCT");
    final LSMVectorIndex index = indexOf();
    index.buildVectorGraphNow();

    final float[] query = clustered(new Random(8L), 1)[0];
    final long start = index.metrics.getVectorFetchFromDocuments();
    final List<Result> rows = sql(query, Map.of("efSearch", 100));
    final long rerankReads = index.metrics.getVectorFetchFromDocuments() - start;
    assertThat(rows).hasSize(K);

    // 100 candidates are re-ranked (the beam), against the several hundred nodes an exact walk of beam 100 visits
    assertThat(rerankReads).as("stored vectors read by the PQ walk with a re-rank").isLessThanOrEqualTo(200);
  }

  @Test
  void rerankZeroReturnsThePqScoresWithoutReadingTheStoredVectors() {
    final float[][] base = clustered(new Random(3L), 5000);
    load(base, "PRODUCT");
    final LSMVectorIndex index = indexOf();
    index.buildVectorGraphNow();

    final float[] query = base[17];
    final long start = index.metrics.getVectorFetchFromDocuments();
    final List<Result> pq = sql(query, Map.of("rerank", 0));
    assertThat(index.metrics.getVectorFetchFromDocuments() - start).as("no stored vector is read for PQ scores").isZero();

    final List<Pair> approximate = new ArrayList<>();
    for (final var neighbour : index.findNeighborsFromVectorApproximate(query, K))
      approximate.add(new Pair(vid(neighbour.getFirst()), neighbour.getSecond()));
    assertThat(pq).hasSameSizeAs(approximate);
    for (int i = 0; i < pq.size(); i++) {
      assertThat((int) pq.get(i).getProperty("vid")).isEqualTo(approximate.get(i).id);
      assertThat((float) pq.get(i).getProperty("distance")).isEqualTo(approximate.get(i).distance);
    }
  }

  @Test
  void aNegativeRerankIsRefused() {
    final float[][] base = clustered(new Random(4L), 500);
    load(base, "PRODUCT");
    assertThatThrownBy(() -> sql(base[0], Map.of("rerank", -1))).isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("rerank");
  }

  @Test
  void otherQuantizationsKeepTheirExactWalkAndIgnoreRerank() {
    final float[][] base = clustered(new Random(5L), 3000);
    load(base, "NONE");
    final LSMVectorIndex index = indexOf();
    index.buildVectorGraphNow();

    final float[] query = base[9];
    final List<Result> plain = sql(query, Map.of("efSearch", 50));
    final List<Result> withRerank = sql(query, Map.of("efSearch", 50, "rerank", 25));
    assertThat(withRerank).hasSameSizeAs(plain);
    for (int i = 0; i < plain.size(); i++) {
      assertThat((int) withRerank.get(i).getProperty("vid")).isEqualTo((int) plain.get(i).getProperty("vid"));
      assertThat((float) withRerank.get(i).getProperty("distance")).isEqualTo((float) plain.get(i).getProperty("distance"));
    }
  }

  private record Pair(int id, float distance) {
  }

  private List<Result> sql(final float[] query, final Map<String, Object> options) {
    final List<Result> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", SQL, query, K, options)) {
      while (rs.hasNext())
        rows.add(rs.next());
    }
    return rows;
  }

  private LSMVectorIndex indexOf() {
    return (LSMVectorIndex) ((TypeIndex) database.getSchema().getIndexByName("Doc[embedding]")).getIndexesOnBuckets()[0];
  }

  private int vid(final RID rid) {
    return database.lookupByRID(rid, true).asDocument().getInteger("vid");
  }

  private static double recall(final Set<Integer> truth, final List<Integer> answer) {
    int hits = 0;
    for (final int id : answer)
      if (truth.contains(id))
        hits++;
    return (double) hits / K;
  }

  private void load(final float[][] base, final String quantization) {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.vid INTEGER");
      database.command("sql", "CREATE PROPERTY Doc.embedding ARRAY_OF_FLOATS");
      database.command("sql", "CREATE INDEX ON Doc (embedding) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
          + ", \"similarity\": \"EUCLIDEAN\", \"quantization\": \"" + quantization + "\", \"storeVectorsInGraph\": false, "
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
