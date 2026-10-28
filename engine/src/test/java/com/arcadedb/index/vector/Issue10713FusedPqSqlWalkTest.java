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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.Pair;
import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndex;
import io.github.jbellis.jvector.graph.disk.feature.FeatureId;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10713: a PRODUCT index with {@code fusedPQ: true} stores the PQ codes of every node's neighbours in its graph file, and
 * the search that SQL {@code vector.neighbors}, the openCypher procedure {@code db.index.vector.queryNodes} and the Java
 * {@code findNeighbors} share (the PQ walk of {@code findNeighborsFromVectorApproximate}) never read them: it scored the walk
 * on the PQ codes held in memory, so the setting cost disk and bought nothing. Only the exact walk of
 * {@code findNeighborsFromVector} read the fused codes.
 * <p>
 * The PQ walk now scores the neighbours of the node it expands from the codes fused into that node when the graph file has
 * them, and the {@code fusedPQWalks} statistic counts the walks that did.
 */
@Tag("vector")
class Issue10713FusedPqSqlWalkTest extends TestHelper {
  private static final int DIMENSIONS = 64;
  private static final int VECTORS    = 6000;
  private static final int CLUSTERS   = 60;
  private static final int QUERIES    = 20;
  private static final int K          = 10;

  private static final String SQL         = "SELECT vid, distance FROM (SELECT expand(`vector.neighbors`('Doc[embedding]', ?, ?)))";
  private static final String SQL_OPTIONS = "SELECT vid, distance FROM (SELECT expand(`vector.neighbors`('Doc[embedding]', ?, ?, ?)))";
  private static final String CYPHER      = "CALL db.index.vector.queryNodes('Doc[embedding]', " + K
      + ", $v) YIELD node, score RETURN node.vid AS vid, score";

  @Test
  void sqlCypherAndJavaWalkTheFusedCodesOfAReopenedFusedPqIndex() {
    final float[][] base = clustered(new Random(10713L), VECTORS);
    final float[][] queries = clustered(new Random(107130L), QUERIES);
    load(base, true);
    indexOf().buildVectorGraphNow();

    // A fresh session: the index is searched through its graph file, not through the graph it was built as in memory
    database.close();
    database = factory.open();
    final LSMVectorIndex index = indexOf();
    assertThat(index.isPQSearchAvailable()).as("the scenario needs a trained codebook").isTrue();

    long walks = fusedWalks(index);
    double recall = 0;
    for (final float[] query : queries) {
      final List<Result> rows = sql(query);
      assertThat(rows).as("a SQL search of the fused graph returns k rows").hasSize(K);
      recall += recall(nearest(base, query), rows);
    }
    assertThat(fusedWalks(index) - walks).as("SQL vector.neighbors walks on the codes fused into the graph file").isEqualTo(QUERIES);
    assertThat(recall / QUERIES).as("recall@10 of SQL on the fused codes, re-ranked on the stored vectors").isGreaterThan(0.9);

    walks = fusedWalks(index);
    assertThat(cypher(queries[0])).hasSize(K);
    assertThat(fusedWalks(index) - walks).as("db.index.vector.queryNodes walks on the fused codes too").isEqualTo(1L);

    // The PQ scores alone: the walk on the fused codes, the answer scored on the PQ codes held in memory
    walks = fusedWalks(index);
    assertThat(sql(queries[1], Map.of("rerank", 0))).hasSize(K);
    final List<Pair<RID, Float>> pqOnly = index.findNeighborsFromVectorApproximate(queries[2], K);
    assertThat(pqOnly).hasSize(K);
    assertThat(fusedWalks(index) - walks).as("the PQ-scores-only walks of SQL and Java").isEqualTo(2L);
    for (int i = 1; i < pqOnly.size(); i++)
      assertThat(pqOnly.get(i).getSecond()).isGreaterThanOrEqualTo(pqOnly.get(i - 1).getSecond());

    // The exact walk read them before this fix and still does
    walks = fusedWalks(index);
    assertThat(index.findNeighborsFromVector(queries[3], K)).hasSize(K);
    assertThat(fusedWalks(index) - walks).as("the exact walk of findNeighborsFromVector").isEqualTo(1L);
  }

  @Test
  void aProductIndexWithoutFusedCodesIsWalkedOnItsInMemoryCodes() throws Exception {
    final float[][] base = clustered(new Random(1L), VECTORS);
    load(base, false);
    indexOf().buildVectorGraphNow();
    database.close();
    database = factory.open();
    final LSMVectorIndex index = indexOf();
    assertThat(index.isPQSearchAvailable()).isTrue();
    try (final OnDiskGraphIndex graph = index.getGraphFile().loadGraph()) {
      assertThat(graph.getFeatureSet()).doesNotContain(FeatureId.FUSED_PQ);
    }

    final long walks = fusedWalks(index);
    final float[] query = clustered(new Random(2L), 1)[0];
    assertThat(sql(query)).hasSize(K);
    assertThat(cypher(query)).hasSize(K);
    assertThat(index.findNeighborsFromVectorApproximate(query, K)).hasSize(K);
    assertThat(fusedWalks(index) - walks).as("walks on fused codes of a graph file that has none").isZero();
  }

  private static long fusedWalks(final LSMVectorIndex index) {
    return index.getStats().getOrDefault("fusedPQWalks", -1L);
  }

  private List<Result> sql(final float[] query) {
    final List<Result> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", SQL, query, K)) {
      rs.forEachRemaining(rows::add);
    }
    return rows;
  }

  private List<Result> sql(final float[] query, final Map<String, Object> options) {
    final List<Result> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", SQL_OPTIONS, query, K, options)) {
      rs.forEachRemaining(rows::add);
    }
    return rows;
  }

  private List<Result> cypher(final float[] query) {
    final List<Result> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", CYPHER, Map.of("v", query))) {
      rs.forEachRemaining(rows::add);
    }
    return rows;
  }

  private static double recall(final Set<Integer> truth, final List<Result> rows) {
    int hits = 0;
    for (final Result row : rows)
      if (truth.contains(row.<Integer>getProperty("vid")))
        hits++;
    return (double) hits / K;
  }

  private LSMVectorIndex indexOf() {
    return (LSMVectorIndex) ((TypeIndex) database.getSchema().getIndexByName("Doc[embedding]")).getIndexesOnBuckets()[0];
  }

  private void load(final float[][] base, final boolean fusedPQ) {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.vid INTEGER");
      database.command("sql", "CREATE PROPERTY Doc.embedding ARRAY_OF_FLOATS");
      database.command("sql", "CREATE INDEX ON Doc (embedding) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
          + ", \"similarity\": \"EUCLIDEAN\", \"quantization\": \"PRODUCT\", \"pqSubspaces\": 16, \"pqClusters\": 256, \"fusedPQ\": "
          + fusedPQ + " }");
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

  private static Set<Integer> nearest(final float[][] base, final float[] query) {
    final double[] distance = new double[base.length];
    final Integer[] order = new Integer[base.length];
    for (int i = 0; i < base.length; i++) {
      double sum = 0;
      for (int j = 0; j < DIMENSIONS; j++) {
        final double d = query[j] - base[i][j];
        sum += d * d;
      }
      distance[i] = sum;
      order[i] = i;
    }
    Arrays.sort(order, Comparator.comparingDouble(i -> distance[i]));
    final Set<Integer> top = new HashSet<>();
    for (int i = 0; i < K; i++)
      top.add(order[i]);
    return top;
  }
}
