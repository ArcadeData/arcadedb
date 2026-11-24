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

import com.arcadedb.TestHelper;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for #12934: the link-prediction scores are defined on the sets of distinct neighbours, and were computed on
 * adjacency rows that list a neighbour once per relationship, so a reciprocal or a parallel relationship inflated them
 * (a Jaccard of 2.0), and {@code algo.adamicAdar} with {@code 'OUT'} or {@code 'IN'} scored the successors of the neighbours of the
 * source instead of the nodes that share a neighbour with it.
 */
class Issue12934LinkPredictionSetsTest extends TestHelper {
  private static final String[] DIRECTIONS = { "BOTH", "OUT", "IN" };

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().createVertexType("P");
      database.getSchema().createEdgeType("K");
    });
  }

  private void graph(final int nodes, final int[][] edges) {
    database.transaction(() -> {
      final List<MutableVertex> vertices = new ArrayList<>();
      for (int i = 0; i < nodes; i++)
        vertices.add(database.newVertex("P").set("id", i).save());
      for (final int[] e : edges)
        vertices.get(e[0]).newEdge("K", vertices.get(e[1])).save();
    });
  }

  @Test
  void aParallelRelationshipIsOneNeighbour() {
    // a -> b, b -> c twice: N(a) = N(c) = {b}
    graph(3, new int[][] { { 0, 1 }, { 1, 2 }, { 1, 2 } });

    assertThat(scores("algo.jaccard", "similarity", 0, "BOTH")).containsOnlyKeys(2).containsEntry(2, 1.0);
    assertThat(scores("algo.commonNeighbors", "commonNeighbors", 0, "BOTH")).containsOnlyKeys(2).containsEntry(2, 1.0);
    assertThat(total(0, 2, "BOTH")).isEqualTo(1);
  }

  @Test
  void aReciprocalRelationshipIsOneNeighbour() {
    // a <-> b, b <-> c, a <-> d, d <-> c: N(a) = N(c) = {b, d} under BOTH, whose rows list each of them twice
    graph(4, new int[][] { { 0, 1 }, { 1, 0 }, { 1, 2 }, { 2, 1 }, { 0, 3 }, { 3, 0 }, { 3, 2 }, { 2, 3 } });

    assertThat(scores("algo.jaccard", "similarity", 0, "BOTH")).containsEntry(2, 1.0);
    assertThat(scores("algo.commonNeighbors", "commonNeighbors", 0, "BOTH")).containsEntry(2, 2.0);
    assertThat(total(0, 2, "BOTH")).isEqualTo(2);
  }

  @Test
  void adamicAdarWithADirectionScoresTheNodesSharingANeighbourInThatDirection() {
    // 0 -> 2 and 1 -> 2: nodes 0 and 1 share the out-neighbour 2, which has two out-neighbours of its own (3 and 4)
    graph(5, new int[][] { { 0, 2 }, { 1, 2 }, { 2, 3 }, { 2, 4 } });

    assertThat(scores("algo.commonNeighbors", "commonNeighbors", 0, "OUT")).containsOnlyKeys(1);
    assertThat(scores("algo.adamicAdar", "score", 0, "OUT")).containsOnlyKeys(1).containsEntry(1, Math.round(1e9 / Math.log(2)) / 1e9);
    // 3 and 4 share the in-neighbour 2, which has two in-neighbours (0 and 1)
    assertThat(scores("algo.adamicAdar", "score", 3, "IN")).containsOnlyKeys(4).containsEntry(4, Math.round(1e9 / Math.log(2)) / 1e9);
  }

  @Test
  void theScoresMatchTheSetDefinitionOnRandomMultigraphs() {
    final Random random = new Random(12934);
    for (int round = 0; round < 6; round++) {
      final int nodes = 6;
      final List<int[]> edges = new ArrayList<>();
      for (int i = 0, count = 8 + random.nextInt(10); i < count; i++) {
        final int[] edge = { random.nextInt(nodes), random.nextInt(nodes) };
        edges.add(edge);
        // reciprocal and parallel relationships are the point
        if (random.nextInt(3) == 0)
          edges.add(new int[] { edge[1], edge[0] });
        if (random.nextInt(4) == 0)
          edges.add(edge.clone());
      }
      database.transaction(() -> database.command("sql", "DELETE VERTEX FROM P").close());
      graph(nodes, edges.toArray(new int[0][]));
      verifyAgainstTheSets(nodes, edges);

      database.command("sql", "CREATE GRAPH ANALYTICAL VIEW gv VERTEX TYPES (P) EDGE TYPES (K) UPDATE MODE OFF").close();
      try {
        verifyAgainstTheSets(nodes, edges);
      } finally {
        database.command("sql", "DROP GRAPH ANALYTICAL VIEW gv").close();
      }
    }
  }

  private void verifyAgainstTheSets(final int nodes, final List<int[]> edges) {
    for (final String direction : DIRECTIONS)
      verifyKnn(nodes, edges, direction);
    for (final String direction : DIRECTIONS)
      for (int source = 0; source < nodes; source++) {
        final Set<Integer> sourceSet = neighbours(edges, source, direction);
        final Map<Integer, Double> jaccard = new HashMap<>();
        final Map<Integer, Double> common = new HashMap<>();
        final Map<Integer, Double> adamic = new HashMap<>();
        final Map<Integer, Double> resource = new HashMap<>();
        for (int v = 0; v < nodes; v++) {
          if (v == source)
            continue;
          final Set<Integer> shared = new HashSet<>(sourceSet);
          shared.retainAll(neighbours(edges, v, direction));
          if (shared.isEmpty())
            continue;
          final Set<Integer> union = new HashSet<>(sourceSet);
          union.addAll(neighbours(edges, v, direction));
          jaccard.put(v, (double) shared.size() / union.size());
          common.put(v, (double) shared.size());
          double adamicScore = 0.0;
          double resourceScore = 0.0;
          for (final int w : shared) {
            final int degree = neighbours(edges, w, direction).size();
            if (degree > 1)
              adamicScore += 1.0 / Math.log(degree);
            if (degree > 0)
              resourceScore += 1.0 / degree;
          }
          if (adamicScore > 0.0)
            adamic.put(v, adamicScore);
          if (resourceScore > 0.0)
            resource.put(v, resourceScore);
        }
        final String at = direction + " from " + source + " over " + edges.stream().map(e -> e[0] + ">" + e[1]).toList();
        assertThat(scores("algo.jaccard", "similarity", source, direction)).as("jaccard " + at).isEqualTo(approx(jaccard));
        assertThat(scores("algo.commonNeighbors", "commonNeighbors", source, direction)).as("commonNeighbors " + at).isEqualTo(approx(common));
        assertThat(scores("algo.adamicAdar", "score", source, direction)).as("adamicAdar " + at).isEqualTo(approx(adamic));
        assertThat(scores("algo.resourceAllocation", "score", source, direction)).as("resourceAllocation " + at).isEqualTo(approx(resource));
        for (int other = 0; other < nodes; other++)
          if (other != source) {
            final Set<Integer> union = new HashSet<>(sourceSet);
            union.addAll(neighbours(edges, other, direction));
            assertThat(total(source, other, direction)).as("totalNeighbors " + source + "," + other + " " + at).isEqualTo(union.size());
          }
      }
  }

  private void verifyKnn(final int nodes, final List<int[]> edges, final String direction) {
    final int k = 2;
    final Map<Integer, List<Double>> got = new HashMap<>();
    try (final ResultSet rs = database.query("opencypher",
        "CALL algo.knn(" + k + ", 'K', '" + direction + "') YIELD node1, node2, similarity RETURN node1.id AS a, node2.id AS b, similarity")) {
      while (rs.hasNext()) {
        final var row = rs.next();
        got.computeIfAbsent(row.<Number>getProperty("a").intValue(), x -> new ArrayList<>())
            .add(Math.round(row.<Number>getProperty("similarity").doubleValue() * 1e9) / 1e9);
      }
    }
    for (int u = 0; u < nodes; u++) {
      final List<Double> expected = new ArrayList<>();
      for (int v = 0; v < nodes; v++) {
        if (v == u)
          continue;
        final Set<Integer> shared = new HashSet<>(neighbours(edges, u, direction));
        shared.retainAll(neighbours(edges, v, direction));
        final Set<Integer> union = new HashSet<>(neighbours(edges, u, direction));
        union.addAll(neighbours(edges, v, direction));
        if (!shared.isEmpty())
          expected.add(Math.round((double) shared.size() / union.size() * 1e9) / 1e9);
      }
      expected.sort(Comparator.reverseOrder());
      final List<Double> actual = new ArrayList<>(got.getOrDefault(u, List.of()));
      actual.sort(Comparator.reverseOrder());
      assertThat(actual).as("knn " + direction + " of " + u + " over " + edges.stream().map(e -> e[0] + ">" + e[1]).toList())
          .isEqualTo(expected.subList(0, Math.min(k, expected.size())));
    }
  }

  /** The values rounded, so two maps of doubles compare as the scores are: to nine digits. */
  private static Map<Integer, Double> approx(final Map<Integer, Double> map) {
    final Map<Integer, Double> rounded = new HashMap<>();
    map.forEach((k, v) -> rounded.put(k, Math.round(v * 1e9) / 1e9));
    return rounded;
  }

  private static Set<Integer> neighbours(final List<int[]> edges, final int node, final String direction) {
    final Set<Integer> set = new HashSet<>();
    for (final int[] e : edges) {
      if (!direction.equals("IN") && e[0] == node)
        set.add(e[1]);
      if (!direction.equals("OUT") && e[1] == node)
        set.add(e[0]);
    }
    return set;
  }

  private Map<Integer, Double> scores(final String procedure, final String field, final int source, final String direction) {
    return scores(procedure, field, source, direction, null);
  }

  /** The id of each node the procedure lists for the source, with its score rounded to nine digits. */
  private Map<Integer, Double> scores(final String procedure, final String field, final int source, final String direction,
      final Double cutoff) {
    final Map<Integer, Double> result = new HashMap<>();
    final String cutoffArgument = cutoff == null ? "" : ", " + cutoff;
    try (final ResultSet rs = database.query("opencypher", "MATCH (s:P {id: " + source + "}) CALL " + procedure + "(s, 'K', '" + direction + "'"
        + cutoffArgument + ") YIELD node2, " + field + " RETURN node2.id AS id, " + field + " AS value")) {
      while (rs.hasNext()) {
        final var row = rs.next();
        final double value = row.<Number>getProperty("value").doubleValue();
        assertThat(result.put(row.<Number>getProperty("id").intValue(), Math.round(value * 1e9) / 1e9)).isNull();
      }
    }
    return result;
  }

  private long total(final int first, final int second, final String direction) {
    try (final ResultSet rs = database.query("opencypher", "MATCH (a:P {id: " + first + "}), (b:P {id: " + second
        + "}) CALL algo.totalNeighbors(a, b, 'K', '" + direction + "') YIELD coefficient RETURN coefficient")) {
      final long value = rs.next().<Number>getProperty("coefficient").longValue();
      assertThat(rs.hasNext()).isFalse();
      return value;
    }
  }
}
