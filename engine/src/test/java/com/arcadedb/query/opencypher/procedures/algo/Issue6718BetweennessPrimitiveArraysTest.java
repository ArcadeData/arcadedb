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
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.offset;

/**
 * Issue #6718: {@code AlgoBetweenness.execute()}'s Brandes algorithm allocated {@code n} fresh
 * {@code ArrayList<Integer>} predecessor lists per source node, {@code n} times over - O(n^2) boxed-list
 * allocations. The fix replaces {@code predecessors}, and the equally boxed {@code stack}/{@code queue}, with
 * primitive {@code int[]} buffers allocated once and reused across every source, reset only over the nodes each
 * source's BFS actually touches.
 * <p>
 * Reusing per-source buffers across sources is exactly the kind of change that can leak stale state from one
 * source's round into the next if a reset is missed anywhere - a bug that would not show up on a small, fully
 * connected graph where every source touches every node anyway. So {@link #matchesNaiveReferenceOnAnIrregularGraph}
 * runs the algorithm on a graph deliberately shaped to touch very different, and sometimes disjoint, subsets of
 * nodes from one source to the next (a cycle, a path and a star bridged into one component, plus a node no other
 * source ever reaches) and compares every score against a naive reference Brandes implementation - the same
 * fresh-allocation-per-source shape the old code used, so it cannot share the bug being tested for - computed
 * independently in this test from the same edge list.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6718BetweennessPrimitiveArraysTest {
  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-issue-6718-betweenness-primitive-arrays");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("LINK");
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  /**
   * Two triangles sharing vertex C ("bowtie"): every cross-triangle pair (A-D, A-E, B-D, B-E) has a unique
   * shortest path of length 2 through C, while every same-triangle pair is a direct edge. Hand-computable exactly:
   * C sits on 4 unordered pairs' unique shortest path; running Brandes from every node as source over a
   * bidirectionally-represented (undirected) graph counts each such pair twice (once per direction), so the raw,
   * non-normalized score for C is 8 and 0 for every other node. Normalizing by {@code 2/((n-1)(n-2))} with n = 5
   * gives {@code 8 * 2/(4*3) = 4/3}.
   */
  @Test
  void bowtieGraphMatchesHandComputedScores() {
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("Node").set("name", "A").save();
      final MutableVertex b = database.newVertex("Node").set("name", "B").save();
      final MutableVertex c = database.newVertex("Node").set("name", "C").save();
      final MutableVertex d = database.newVertex("Node").set("name", "D").save();
      final MutableVertex e = database.newVertex("Node").set("name", "E").save();
      for (final MutableVertex[] edge : new MutableVertex[][] {
          { a, b }, { b, c }, { c, a }, { c, d }, { d, e }, { e, c } }) {
        edge[0].newEdge("LINK", edge[1]).save();
        edge[1].newEdge("LINK", edge[0]).save();
      }
    });

    final Map<String, Double> raw = scoresByName("CALL algo.betweenness({normalized: false}) YIELD node, score "
        + "RETURN node.name AS name, score");
    assertThat(raw.get("C")).isEqualTo(8.0);
    for (final String leaf : List.of("A", "B", "D", "E"))
      assertThat(raw.get(leaf)).isEqualTo(0.0);

    final Map<String, Double> normalized = scoresByName("CALL algo.betweenness({normalized: true}) YIELD node, score "
        + "RETURN node.name AS name, score");
    assertThat(normalized.get("C")).isCloseTo(4.0 / 3.0, offset(1e-9));
    for (final String leaf : List.of("A", "B", "D", "E"))
      assertThat(normalized.get(leaf)).isCloseTo(0.0, offset(1e-9));
  }

  /**
   * Cycle (0-5) bridged to a path (6-10) bridged to a star (center 11, leaves 12-16), plus node 17 which no edge
   * ever touches. 18 sources in total, with wildly different reachable sets and BFS-tree shapes between them -
   * exactly the condition under which a missed reset in the reused per-source buffers would surface as a wrong
   * score, not a crash.
   */
  @Test
  void matchesNaiveReferenceOnAnIrregularGraph() {
    final int n = 18;
    final List<int[]> edges = new ArrayList<>();
    // Cycle: 0-1-2-3-4-5-0
    for (int i = 0; i < 6; i++)
      edges.add(new int[] { i, (i + 1) % 6 });
    // Path: 6-7-8-9-10
    for (int i = 6; i < 10; i++)
      edges.add(new int[] { i, i + 1 });
    // Star: center 11, leaves 12-16
    for (int leaf = 12; leaf <= 16; leaf++)
      edges.add(new int[] { 11, leaf });
    // Bridges: cycle -> path, path -> star. Node 17 stays isolated.
    edges.add(new int[] { 3, 6 });
    edges.add(new int[] { 10, 11 });

    final MutableVertex[] vertices = new MutableVertex[n];
    database.transaction(() -> {
      for (int i = 0; i < n; i++)
        vertices[i] = database.newVertex("Node").set("name", "N" + i).save();
      for (final int[] edge : edges) {
        vertices[edge[0]].newEdge("LINK", vertices[edge[1]]).save();
        vertices[edge[1]].newEdge("LINK", vertices[edge[0]]).save();
      }
    });

    final int[][] adj = buildUndirectedAdjacency(n, edges);
    final double[] expected = naiveBrandes(n, adj);

    final Map<String, Double> actual = scoresByName("CALL algo.betweenness({normalized: false}) YIELD node, score "
        + "RETURN node.name AS name, score");
    assertThat(actual).hasSize(n);
    for (int i = 0; i < n; i++)
      assertThat(actual.get("N" + i)).as("score for N" + i).isCloseTo(expected[i], offset(1e-9));
  }

  private Map<String, Double> scoresByName(final String query) {
    final Map<String, Double> result = new HashMap<>();
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        result.put(row.<String>getProperty("name"), row.<Number>getProperty("score").doubleValue());
      }
    }
    return result;
  }

  private static int[][] buildUndirectedAdjacency(final int n, final List<int[]> edges) {
    final int[] degree = new int[n];
    for (final int[] edge : edges) {
      degree[edge[0]]++;
      degree[edge[1]]++;
    }
    final int[][] adj = new int[n][];
    for (int i = 0; i < n; i++)
      adj[i] = new int[degree[i]];
    final int[] cursor = new int[n];
    for (final int[] edge : edges) {
      adj[edge[0]][cursor[edge[0]]++] = edge[1];
      adj[edge[1]][cursor[edge[1]]++] = edge[0];
    }
    return adj;
  }

  /**
   * Textbook Brandes, unoptimized on purpose: fresh {@code ArrayList}/{@code ArrayDeque}/{@code LinkedList} per
   * source, exactly the shape issue #6718 replaced in the production code - so this reference cannot share a
   * buffer-reuse bug with the code it is checking.
   */
  private static double[] naiveBrandes(final int n, final int[][] adj) {
    final double[] betweenness = new double[n];
    for (int s = 0; s < n; s++) {
      final Deque<Integer> stack = new ArrayDeque<>();
      final List<List<Integer>> predecessors = new ArrayList<>(n);
      for (int i = 0; i < n; i++)
        predecessors.add(new ArrayList<>());
      final double[] sigma = new double[n];
      final int[] dist = new int[n];
      sigma[s] = 1.0;
      for (int i = 0; i < n; i++)
        dist[i] = -1;
      dist[s] = 0;

      final Queue<Integer> queue = new LinkedList<>();
      queue.add(s);
      while (!queue.isEmpty()) {
        final int v = queue.poll();
        stack.push(v);
        for (final int w : adj[v]) {
          if (dist[w] < 0) {
            queue.add(w);
            dist[w] = dist[v] + 1;
          }
          if (dist[w] == dist[v] + 1) {
            sigma[w] += sigma[v];
            predecessors.get(w).add(v);
          }
        }
      }

      final double[] delta = new double[n];
      while (!stack.isEmpty()) {
        final int w = stack.pop();
        for (final int v : predecessors.get(w))
          delta[v] += (sigma[v] / sigma[w]) * (1.0 + delta[w]);
        if (w != s)
          betweenness[w] += delta[w];
      }
    }
    return betweenness;
  }
}
