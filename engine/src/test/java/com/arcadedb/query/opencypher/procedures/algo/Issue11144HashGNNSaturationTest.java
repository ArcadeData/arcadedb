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
package com.arcadedb.query.opencypher.procedures.algo;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.RID;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.offset;

/**
 * Regression tests for issue #11144: {@code algo.hashgnn} with its default settings answered an all-zero vector for
 * every node of a 200-node, 398-edge graph.
 * <p>
 * The propagation rounds only OR-combined the neighbours' feature sets and the MinHash reduction ran once, after the
 * last round, so nothing bounded the density: a few rounds over a neighbourhood of a few dozen nodes set every bit.
 * Nodes of one saturated region then shared one signature, and once every bit was set the minimum of a full-period
 * {@code (a * f + b) mod numFeatures} was 0 in every dimension.
 * <p>
 * Reducing inside each round, as the report suggests, removes the zeros but not the collapse: plain MinHash rounds
 * coalesce, and a dense 40-node graph still ended with one shared vector after a few more rounds. The samples are now
 * a consistent weighted sample of each node's neighbourhood, with the neighbours weighing {@code neighborInfluence}
 * against the node itself at every hop, so more rounds widen the neighbourhood without ever erasing the node.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11144HashGNNSaturationTest {
  private Database database;

  @AfterEach
  void teardown() {
    if (database != null) {
      if (database.isTransactionActive())
        database.rollback();
      database.drop();
      database = null;
    }
  }

  @Test
  void theReportedQueryAnswersNoAllZeroVector() {
    reporterGraph();

    final Result row = database.query("opencypher", """
        CALL algo.hashgnn() YIELD embedding
        WITH reduce(s = 0.0, x IN embedding | s + abs(x)) AS total
        RETURN count(*) AS nodes, sum(CASE WHEN total = 0 THEN 1 ELSE 0 END) AS all_zero""").next();

    assertThat(row.<Number>getProperty("nodes").intValue()).isEqualTo(200);
    assertThat(row.<Number>getProperty("all_zero").intValue()).isZero();
  }

  @Test
  void theEmbeddingsOfTheReportedGraphStayDistinct() {
    reporterGraph();

    for (final String config : List.of("{}", "{seed: 42}", "{iterations: 2, seed: 42}", "{iterations: 1, seed: 42}")) {
      final Map<RID, double[]> embeddings = embeddings(config);
      assertThat(embeddings).hasSize(200);
      assertThat(allZero(embeddings)).as("all-zero vectors with %s", config).isZero();
      // Before the fix {} and {seed: 42} collapsed to 1 and 3 distinct vectors.
      assertThat(distinct(embeddings)).as("distinct vectors with %s", config).isGreaterThanOrEqualTo(195);
    }
  }

  @Test
  void theRingTableOfTheReportStaysDistinctAtEveryRow() {
    // The rows of the report: every one with C > 0 and more than one round ended with ONE shared vector.
    record Row(int nodes, int extraEdges, int dimension, int iterations, int minDistinct) {
    }
    // Four rounds reach most of a 40-node graph, so a few neighbours may draw the same 16 samples. embeddingDimension
    // 32 leaves 4 samples per node, so more nodes of a 40-node graph with 8 edges each draw the same 4: that row is
    // about the zeros and the single vector, not about how fine 4 samples can be.
    final List<Row> rows = List.of(new Row(40, 0, 128, 4, 35), new Row(40, 1, 128, 4, 35), new Row(40, 3, 32, 3, 20),
        new Row(200, 1, 128, 4, 200), new Row(200, 1, 128, 1, 200));
    for (final Row r : rows) {
      ring(r.nodes(), r.extraEdges());
      final Map<RID, double[]> embeddings = embeddings(
          "{embeddingDimension: " + r.dimension() + ", iterations: " + r.iterations() + ", seed: 42}");
      assertThat(embeddings).hasSize(r.nodes());
      assertThat(allZero(embeddings)).as("all-zero vectors on %s", r).isZero();
      assertThat(distinct(embeddings)).as("distinct vectors on %s", r).isGreaterThanOrEqualTo(r.minDistinct());
      database.drop();
      database = null;
    }
  }

  @Test
  void moreRoundsNeverCollapseADenseGraph() {
    // Eight edges per node over 40 nodes: two rounds already reach the whole graph. Unweighted MinHash rounds (and
    // the code of the report) end with one vector here; the per-hop weight keeps every node ahead of the rest.
    ring(40, 3);
    for (final int iterations : new int[] { 2, 4, 8, 16 }) {
      final Map<RID, double[]> embeddings = embeddings("{iterations: " + iterations + ", seed: 42}");
      assertThat(allZero(embeddings)).as("all-zero vectors after %s rounds", iterations).isZero();
      assertThat(distinct(embeddings)).as("distinct vectors after %s rounds", iterations).isGreaterThanOrEqualTo(38);
    }
  }

  @Test
  void similarityFallsWithGraphDistance() {
    // The vectors have to carry the neighbourhood, not only differ: used as classifier features the saturated ones
    // changed no score. Before the fix every pair had cosine 1 (or 0 against the zero vector), at any distance.
    reporterGraph();
    final int[][] distance = reporterGraphDistances();
    final double[] byDistance = meanCosineByDistance(embeddingsByK("{seed: 42}"), distance);

    assertThat(byDistance[1]).as("distance 1 against 2").isGreaterThan(byDistance[2]);
    assertThat(byDistance[2]).as("distance 2 against 3").isGreaterThan(byDistance[3]);
    assertThat(byDistance[6]).as("distance 6 against 1").isLessThan(byDistance[1] / 2);
  }

  @Test
  void neighborInfluenceSetsHowFarTheEmbeddingReaches() {
    reporterGraph();
    final int[][] distance = reporterGraphDistances();

    final double local = meanCosineByDistance(embeddingsByK("{seed: 42, neighborInfluence: 0.5}"), distance)[1];
    final double standard = meanCosineByDistance(embeddingsByK("{seed: 42}"), distance)[1];
    final double wide = meanCosineByDistance(embeddingsByK("{seed: 42, neighborInfluence: 2.0}"), distance)[1];
    assertThat(local).isLessThan(standard);
    assertThat(standard).isLessThan(wide);

    // No influence at all: every node is its own only sample, one position of the vector.
    for (final double[] e : embeddings("{seed: 42, neighborInfluence: 0}").values())
      assertThat(Arrays.stream(e).filter(v -> v != 0.0).count()).isEqualTo(1);

    assertThatThrownBy(() -> embeddings("{neighborInfluence: -0.5}"))
        .hasStackTraceContaining("neighborInfluence must be a finite number of at least 0, got -0.5");
  }

  @Test
  void everyEmbeddingIsAUnitBinaryVectorWithAtMostEmbeddingDensityFeatures() {
    reporterGraph();

    for (final int density : new int[] { 1, 4, 16, 64 }) {
      final Map<RID, double[]> embeddings = embeddings("{embeddingDimension: 64, embeddingDensity: " + density
          + ", seed: 7}");
      for (final double[] e : embeddings.values()) {
        int set = 0;
        double norm = 0;
        for (final double v : e)
          if (v != 0.0) {
            set++;
            norm += v * v;
          }
        assertThat(set).isBetween(1, density);
        assertThat(norm).isCloseTo(1.0, offset(1e-9));
        for (final double v : e)
          if (v != 0.0)
            assertThat(v).isCloseTo(1.0 / Math.sqrt(set), offset(1e-12));
      }
    }
  }

  @Test
  void theSameSeedAnswersTheSameEmbeddings() {
    reporterGraph();
    final Map<RID, double[]> first = embeddings("{seed: 99}");
    final Map<RID, double[]> second = embeddings("{seed: 99}");
    for (final Map.Entry<RID, double[]> e : first.entrySet())
      assertThat(second.get(e.getKey())).containsExactly(e.getValue());
  }

  @Test
  void anEmbeddingDensityOutsideTheEmbeddingDimensionIsRejected() {
    reporterGraph();

    assertThatThrownBy(() -> embeddings("{embeddingDimension: 16, embeddingDensity: 0}"))
        .hasStackTraceContaining("embeddingDensity must be at least 1");
    assertThatThrownBy(() -> embeddings("{embeddingDimension: 16, embeddingDensity: 17}"))
        .hasStackTraceContaining("embeddingDensity must not exceed embeddingDimension (16), got 17");
  }

  // ── Fixtures ─────────────────────────────────────────────────────────────

  /** The graph of the report: a 200-node ring plus one chord per node, 398 distinct edges. */
  private void reporterGraph() {
    create("test-issue-11144-hashgnn");
    database.command("sql", "CREATE VERTEX TYPE N");
    database.command("sql", "CREATE EDGE TYPE E");
    database.transaction(() -> {
      database.command("opencypher", "UNWIND range(0, 199) AS k CREATE (:N {k: k})");
      database.command("opencypher", """
          MATCH (a:N), (b:N)
          WHERE b.k = (a.k + 1) % 200 OR b.k = (a.k * 7 + 3) % 200
          CREATE (a)-[:E]->(b)""");
    });
    assertThat(database.countType("E", false)).isEqualTo(398);
  }

  /** Hop distances of the reporter's graph, edges taken in both directions as the procedure's default does. */
  private static int[][] reporterGraphDistances() {
    final int n = 200;
    final List<List<Integer>> adjacency = new ArrayList<>();
    for (int k = 0; k < n; k++)
      adjacency.add(new ArrayList<>());
    for (int k = 0; k < n; k++)
      for (final int target : new int[] { (k + 1) % n, (k * 7 + 3) % n }) {
        adjacency.get(k).add(target);
        adjacency.get(target).add(k);
      }
    final int[][] distance = new int[n][n];
    for (int source = 0; source < n; source++) {
      Arrays.fill(distance[source], -1);
      distance[source][source] = 0;
      final ArrayDeque<Integer> queue = new ArrayDeque<>(List.of(source));
      while (!queue.isEmpty()) {
        final int u = queue.poll();
        for (final int v : adjacency.get(u))
          if (distance[source][v] < 0) {
            distance[source][v] = distance[source][u] + 1;
            queue.add(v);
          }
      }
    }
    return distance;
  }

  /** A ring of {@code nodes} nodes with {@code extraEdges} random extra edges per node, as in the report's table. */
  private void ring(final int nodes, final int extraEdges) {
    create("test-issue-11144-hashgnn-ring");
    database.command("sql", "CREATE VERTEX TYPE N");
    database.command("sql", "CREATE EDGE TYPE E");
    final Random random = new Random(nodes * 31L + extraEdges);
    database.transaction(() -> {
      final List<MutableVertex> vertices = new ArrayList<>(nodes);
      for (int i = 0; i < nodes; i++)
        vertices.add(database.newVertex("N").set("k", i).save());
      for (int i = 0; i < nodes; i++) {
        vertices.get(i).newEdge("E", vertices.get((i + 1) % nodes)).save();
        for (int c = 0; c < extraEdges; c++)
          vertices.get(i).newEdge("E", vertices.get(random.nextInt(nodes))).save();
      }
    });
  }

  private void create(final String name) {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/" + name);
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
  }

  private Map<RID, double[]> embeddings(final String config) {
    final Map<RID, double[]> embeddings = new HashMap<>();
    try (final ResultSet rs = database.query("opencypher",
        "CALL algo.hashgnn(" + config + ") YIELD node, embedding RETURN node, embedding")) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        final Object node = r.getProperty("node");
        embeddings.put(node instanceof Vertex v ? v.getIdentity() : (RID) node, toArray(r.getProperty("embedding")));
      }
    }
    return embeddings;
  }

  /** The embeddings of the reporter's graph indexed by the {@code k} property of their node. */
  private double[][] embeddingsByK(final String config) {
    final double[][] embeddings = new double[200][];
    try (final ResultSet rs = database.query("opencypher",
        "CALL algo.hashgnn(" + config + ") YIELD node, embedding RETURN node.k AS k, embedding")) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        embeddings[r.<Number>getProperty("k").intValue()] = toArray(r.getProperty("embedding"));
      }
    }
    return embeddings;
  }

  private static double[] toArray(final List<?> list) {
    final double[] e = new double[list.size()];
    for (int i = 0; i < e.length; i++)
      e[i] = ((Number) list.get(i)).doubleValue();
    return e;
  }

  private static double[] meanCosineByDistance(final double[][] embeddings, final int[][] distance) {
    final double[] sum = new double[embeddings.length];
    final int[] count = new int[embeddings.length];
    for (int i = 0; i < embeddings.length; i++)
      for (int j = i + 1; j < embeddings.length; j++) {
        sum[distance[i][j]] += cosine(embeddings[i], embeddings[j]);
        count[distance[i][j]]++;
      }
    for (int d = 0; d < sum.length; d++)
      if (count[d] > 0)
        sum[d] /= count[d];
    return sum;
  }

  private static int allZero(final Map<RID, double[]> embeddings) {
    int zero = 0;
    for (final double[] e : embeddings.values())
      if (Arrays.stream(e).allMatch(v -> v == 0.0))
        zero++;
    return zero;
  }

  private static int distinct(final Map<RID, double[]> embeddings) {
    final Set<List<Double>> distinct = new HashSet<>();
    for (final double[] e : embeddings.values())
      distinct.add(Arrays.stream(e).boxed().toList());
    return distinct.size();
  }

  private static double cosine(final double[] a, final double[] b) {
    double dot = 0, na = 0, nb = 0;
    for (int i = 0; i < a.length; i++) {
      dot += a[i] * b[i];
      na += a[i] * a[i];
      nb += b[i] * b[i];
    }
    return na == 0 || nb == 0 ? 0 : dot / Math.sqrt(na * nb);
  }
}
