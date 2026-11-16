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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Issue #12176: {@code algo.randomWalk}, {@code algo.totalNeighbors} and the source-anchored link-prediction procedures loaded
 * every vertex and every edge of the database when no Graph Analytical View covered the graph, to answer a question about one
 * or two vertices and the nodes one or two hops away. They now read those nodes only, and answer what the whole-graph
 * computation answered: the scores are checked against a reference over the edge list, parallel edges and a self-loop
 * included, without a view and with one.
 */
class AlgoLocalNeighborhoodTest extends TestHelper {
  // A SMALL COMPONENT THE CALLS START FROM, AND A LARGE ONE THEY NEVER REACH
  private static final int               SMALL     = 12;
  private static final int               LARGE     = 300;
  private static final Vertex.DIRECTION[] DIRECTIONS = { Vertex.DIRECTION.OUT, Vertex.DIRECTION.IN, Vertex.DIRECTION.BOTH };

  private final List<int[]> edges = new ArrayList<>();

  @Override
  protected void beginTest() {
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("E");
    database.transaction(() -> {
      final Map<Integer, MutableVertex> v = new HashMap<>();
      for (int i = 0; i < SMALL; i++)
        v.put(i, database.newVertex("Node").set("id", i).save());
      for (int i = 0; i < LARGE; i++)
        v.put(1000 + i, database.newVertex("Node").set("id", 1000 + i).save());

      for (int i = 0; i < SMALL; i++) {
        addEdge(v, i, (i * 5 + 1) % SMALL);
        addEdge(v, i, (i * 7 + 3) % SMALL);
      }
      // A parallel edge and a self-loop, which the rows count as many times as they are stored
      addEdge(v, 0, 1);
      addEdge(v, 2, 2);
      for (int i = 0; i + 1 < LARGE; i++)
        addEdge(v, 1000 + i, 1000 + i + 1);
    });
  }

  private void addEdge(final Map<Integer, MutableVertex> v, final int from, final int to) {
    v.get(from).newEdge("E", v.get(to)).save();
    edges.add(new int[] { from, to });
  }

  @Test
  void withoutAViewTheScoresAreTheWholeGraphScores() {
    assertScoresMatchTheReference();
  }

  @Test
  void withAViewTheScoresAreTheWholeGraphScores() {
    final GraphAnalyticalView view = GraphAnalyticalView.builder(database).withName("issue-12176").withVertexTypes("Node")
        .withEdgeTypes("E").build();
    try {
      assertScoresMatchTheReference();
    } finally {
      view.drop();
    }
  }

  /**
   * Room for a few dozen loaded vertices: loading the whole graph first, as every one of these calls used to, is refused, and
   * reading the nodes around the source is not.
   */
  @Test
  void withoutAViewACallLoadsOnlyTheNodesAroundItsSource() {
    database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY,
        AbstractAlgoProcedure.OLTP_VERTEX_BYTES * 40);
    try {
      for (final Vertex.DIRECTION dir : DIRECTIONS) {
        assertThat(jaccard(0, dir, 0.0)).as("jaccard %s", dir).isNotEmpty();
        assertThat(adamicAdar(0, dir)).as("adamicAdar %s", dir).isNotEmpty();
        assertThat(commonNeighbors(0, dir, 1)).as("commonNeighbors %s", dir).isNotEmpty();
        assertThat(resourceAllocation(0, dir)).as("resourceAllocation %s", dir).isNotEmpty();
        assertThat(totalNeighbors(0, 5, dir)).as("totalNeighbors %s", dir).isPositive();
        assertThat(randomWalk(0, 10, dir, 42L)).as("randomWalk %s", dir).isNotEmpty();
      }
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY,
          GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY.getDefValue());
    }
  }

  @Test
  void aRandomWalkStepsAlongTheEdgesAndRepeatsForASeed() {
    for (final Vertex.DIRECTION dir : DIRECTIONS) {
      final Map<Integer, List<Integer>> rows = rows(dir);
      final List<Integer> walk = randomWalk(0, 50, dir, 7L);
      assertThat(walk).as("%s", dir).hasSize(51).startsWith(0);
      for (int i = 0; i + 1 < walk.size(); i++)
        assertThat(rows.getOrDefault(walk.get(i), List.of())).as("%s step %d", dir, i).contains(walk.get(i + 1));
      assertThat(randomWalk(0, 50, dir, 7L)).as("%s, same seed", dir).isEqualTo(walk);
    }
  }

  private void assertScoresMatchTheReference() {
    for (final Vertex.DIRECTION dir : DIRECTIONS) {
      final Map<Integer, List<Integer>> rows = rows(dir);
      for (final int source : new int[] { 0, 5 }) {
        final String as = dir + " from " + source;
        assertClose(jaccard(source, dir, 0.0), referenceJaccard(rows, source, 0.0), as + " jaccard");
        assertClose(jaccard(source, dir, -1.0), referenceJaccard(rows, source, -1.0), as + " jaccard, negative cutoff");
        assertClose(adamicAdar(source, dir), referenceAdamicAdar(rows, source), as + " adamicAdar");
        assertClose(resourceAllocation(source, dir), referenceResourceAllocation(rows, source), as + " resourceAllocation");
        assertClose(commonNeighbors(source, dir, 1), referenceCommonNeighbors(rows, source, 1), as + " commonNeighbors");
        assertClose(commonNeighbors(source, dir, 0), referenceCommonNeighbors(rows, source, 0), as + " commonNeighbors 0");
      }
      final List<Integer> a = rows.getOrDefault(0, List.of());
      final List<Integer> b = rows.getOrDefault(5, List.of());
      final Set<Integer> aSet = new HashSet<>(a);
      assertThat(totalNeighbors(0, 5, dir)).as("%s totalNeighbors", dir)
          .isEqualTo(a.size() + b.size() - b.stream().filter(aSet::contains).count());
    }
  }

  private static void assertClose(final Map<Integer, Double> actual, final Map<Integer, Double> expected, final String as) {
    assertThat(actual.keySet()).as(as).isEqualTo(expected.keySet());
    for (final Map.Entry<Integer, Double> entry : expected.entrySet())
      assertThat(actual.get(entry.getKey())).as(as + " node " + entry.getKey()).isCloseTo(entry.getValue(), within(1e-12));
  }

  /** The row of every node in {@code dir}, as the whole-graph adjacency lists it: OUT then IN for BOTH, a self-loop twice. */
  private Map<Integer, List<Integer>> rows(final Vertex.DIRECTION dir) {
    final Map<Integer, List<Integer>> rows = new HashMap<>();
    for (final int[] e : edges)
      if (dir != Vertex.DIRECTION.IN)
        rows.computeIfAbsent(e[0], k -> new ArrayList<>()).add(e[1]);
    for (final int[] e : edges)
      if (dir != Vertex.DIRECTION.OUT)
        rows.computeIfAbsent(e[1], k -> new ArrayList<>()).add(e[0]);
    return rows;
  }

  private List<Integer> nodes() {
    final List<Integer> nodes = new ArrayList<>();
    for (int i = 0; i < SMALL; i++)
      nodes.add(i);
    for (int i = 0; i < LARGE; i++)
      nodes.add(1000 + i);
    return nodes;
  }

  private Map<Integer, Double> referenceJaccard(final Map<Integer, List<Integer>> rows, final int source, final double cutoff) {
    final List<Integer> src = rows.getOrDefault(source, List.of());
    final Set<Integer> srcSet = new HashSet<>(src);
    final Map<Integer, Double> scores = new HashMap<>();
    for (final int v : nodes()) {
      if (v == source)
        continue;
      final List<Integer> row = rows.getOrDefault(v, List.of());
      final long intersection = row.stream().filter(srcSet::contains).count();
      final long union = src.size() + row.size() - intersection;
      if (union > 0 && (double) intersection / union > cutoff)
        scores.put(v, (double) intersection / union);
    }
    return scores;
  }

  private Map<Integer, Double> referenceAdamicAdar(final Map<Integer, List<Integer>> rows, final int source) {
    final Map<Integer, Double> scores = new HashMap<>();
    for (final int w : rows.getOrDefault(source, List.of())) {
      final List<Integer> row = rows.getOrDefault(w, List.of());
      if (row.size() <= 1)
        continue;
      for (final int v : row)
        if (v != source)
          scores.merge(v, 1.0 / Math.log(row.size()), Double::sum);
    }
    scores.values().removeIf(score -> score <= 0.0);
    return scores;
  }

  private Map<Integer, Double> referenceResourceAllocation(final Map<Integer, List<Integer>> rows, final int source) {
    final Set<Integer> srcSet = new HashSet<>(rows.getOrDefault(source, List.of()));
    final Map<Integer, Double> scores = new HashMap<>();
    for (final int v : nodes()) {
      if (v == source)
        continue;
      double score = 0.0;
      for (final int w : rows.getOrDefault(v, List.of()))
        if (srcSet.contains(w) && !rows.getOrDefault(w, List.of()).isEmpty())
          score += 1.0 / rows.get(w).size();
      if (score > 0.0)
        scores.put(v, score);
    }
    return scores;
  }

  private Map<Integer, Double> referenceCommonNeighbors(final Map<Integer, List<Integer>> rows, final int source,
      final int cutoff) {
    final Set<Integer> srcSet = new HashSet<>(rows.getOrDefault(source, List.of()));
    final Map<Integer, Double> counts = new HashMap<>();
    for (final int v : nodes()) {
      if (v == source)
        continue;
      final long count = rows.getOrDefault(v, List.of()).stream().filter(srcSet::contains).count();
      if (count >= cutoff)
        counts.put(v, (double) count);
    }
    return counts;
  }

  private Map<Integer, Double> jaccard(final int source, final Vertex.DIRECTION dir, final double cutoff) {
    return scores("CALL algo.jaccard(s, 'E', '" + dir + "', " + cutoff + ") YIELD node2, similarity", "similarity", source);
  }

  private Map<Integer, Double> adamicAdar(final int source, final Vertex.DIRECTION dir) {
    return scores("CALL algo.adamicAdar(s, 'E', '" + dir + "') YIELD node2, score", "score", source);
  }

  private Map<Integer, Double> resourceAllocation(final int source, final Vertex.DIRECTION dir) {
    return scores("CALL algo.resourceAllocation(s, 'E', '" + dir + "') YIELD node2, score", "score", source);
  }

  private Map<Integer, Double> commonNeighbors(final int source, final Vertex.DIRECTION dir, final int cutoff) {
    return scores("CALL algo.commonNeighbors(s, 'E', '" + dir + "', " + cutoff + ") YIELD node2, commonNeighbors",
        "commonNeighbors", source);
  }

  private Map<Integer, Double> scores(final String call, final String field, final int source) {
    final Map<Integer, Double> scores = new HashMap<>();
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (s:Node {id: $source}) " + call + " RETURN node2.id AS id, " + field + " AS score", Map.of("source", source))) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        assertThat(scores.put(row.<Number>getProperty("id").intValue(), row.<Number>getProperty("score").doubleValue()))
            .as("a node reported twice").isNull();
      }
    }
    return scores;
  }

  private long totalNeighbors(final int a, final int b, final Vertex.DIRECTION dir) {
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (a:Node {id: $a}), (b:Node {id: $b}) CALL algo.totalNeighbors(a, b, 'E', '" + dir
            + "') YIELD coefficient RETURN coefficient", Map.of("a", a, "b", b))) {
      return rs.next().<Number>getProperty("coefficient").longValue();
    }
  }

  private List<Integer> randomWalk(final int start, final int steps, final Vertex.DIRECTION dir, final long seed) {
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (s:Node {id: $start}) CALL algo.randomWalk(s, $steps, 'E', '" + dir
            + "', $seed) YIELD path RETURN [n IN nodes(path) | n.id] AS ids", Map.of("start", start, "steps", steps, "seed", seed))) {
      final List<Integer> ids = new ArrayList<>();
      for (final Object id : rs.next().<List<?>>getProperty("ids"))
        ids.add(((Number) id).intValue());
      return ids;
    }
  }
}
