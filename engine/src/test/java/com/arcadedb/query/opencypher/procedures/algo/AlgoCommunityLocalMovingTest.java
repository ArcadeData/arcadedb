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
import com.arcadedb.query.sql.executor.WorkGuard;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
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
 * Modularity local moving: a node moves only to a community that beats the one it is in (the gain of staying is part of the
 * comparison), and {@code algo.leiden} answers the same communities whatever {@code maxIterations} is, because its passes stop when
 * nothing moves. Both used to move a node to any community with a positive gain: two 4-cliques joined by one edge came out of
 * {@code algo.louvain} as four communities, and {@code algo.leiden} merged them into one from its second pass on, never stopped for
 * {@code maxIterations = 2147483647}, and depended on that number for the answer.
 */
class AlgoCommunityLocalMovingTest {
  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-algo-community-local-moving");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("N");
    database.getSchema().createEdgeType("E");
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  /** Cliques of the given size, vertex ids consecutive from 0; clique c holds ids c*size .. c*size+size-1. */
  private void cliques(final int count, final int size, final int[][] links) {
    database.transaction(() -> {
      final List<MutableVertex> vertices = new ArrayList<>();
      for (int i = 0; i < count * size; i++)
        vertices.add(database.newVertex("N").set("id", i).save());
      for (int c = 0; c < count; c++)
        for (int i = 0; i < size; i++)
          for (int j = i + 1; j < size; j++)
            vertices.get(c * size + i).newEdge("E", vertices.get(c * size + j)).save();
      for (final int[] link : links)
        vertices.get(link[0]).newEdge("E", vertices.get(link[1])).save();
    });
  }

  private Map<Integer, Integer> communities(final String call, final String idColumn, final String communityColumn) {
    final Map<Integer, Integer> communities = new HashMap<>();
    try (final ResultSet rs = database.query("opencypher",
        "CALL " + call + " YIELD " + idColumn + ", " + communityColumn + " RETURN " + idColumn + ".id AS id, " + communityColumn + " AS community")) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        communities.put(((Number) row.getProperty("id")).intValue(), ((Number) row.getProperty("community")).intValue());
      }
    }
    return communities;
  }

  private static Set<Set<Integer>> groups(final Map<Integer, Integer> communities) {
    final Map<Integer, Set<Integer>> byCommunity = new HashMap<>();
    communities.forEach((node, community) -> byCommunity.computeIfAbsent(community, c -> new HashSet<>()).add(node));
    return new HashSet<>(byCommunity.values());
  }

  private static Set<Integer> range(final int from, final int to) {
    final Set<Integer> set = new HashSet<>();
    for (int i = from; i < to; i++)
      set.add(i);
    return set;
  }

  @Test
  void louvainSeparatesTwoCliquesJoinedByABridge() {
    cliques(2, 4, new int[][] { { 3, 4 } });

    assertThat(groups(communities("algo.louvain()", "node", "communityId"))).containsExactlyInAnyOrder(range(0, 4), range(4, 8));
  }

  @Test
  void louvainReportsTheModularityOfTheCliques() {
    cliques(2, 4, new int[][] { { 3, 4 } });

    // 13 edges, each clique holds 6 and has degree sum 13: Q = 2 * (6/13 - (13/26)^2)
    try (final ResultSet rs = database.query("opencypher", "CALL algo.louvain() YIELD modularity RETURN DISTINCT modularity AS q")) {
      assertThat(rs.next().<Number>getProperty("q").doubleValue()).isCloseTo(2 * (6.0 / 13 - 0.25), within(1e-9));
      assertThat(rs.hasNext()).isFalse();
    }
  }

  @Test
  void louvainFindsTheCliquesOfARing() {
    cliques(3, 5, new int[][] { { 4, 5 }, { 9, 10 }, { 14, 0 } });

    assertThat(groups(communities("algo.louvain()", "node", "communityId"))).containsExactlyInAnyOrder(range(0, 5), range(5, 10),
        range(10, 15));
  }

  @Test
  void leidenSeparatesTwoCliquesWhateverTheNumberOfPasses() {
    cliques(2, 4, new int[][] { { 3, 4 } });

    for (final String call : new String[] { "algo.leiden()", "algo.leiden('E', 2, 1.0)",
        "algo.leiden('E', 11, 1.0)", "algo.leiden('E', 100, 1.0)" })
      assertThat(groups(communities(call, "nodeId", "community"))).as(call).containsExactlyInAnyOrder(range(0, 4), range(4, 8));
  }

  @Test
  void leidenFindsTheCliquesOfARing() {
    cliques(3, 5, new int[][] { { 4, 5 }, { 9, 10 }, { 14, 0 } });

    assertThat(groups(communities("algo.leiden()", "nodeId", "community"))).containsExactlyInAnyOrder(range(0, 5), range(5, 10),
        range(10, 15));
  }

  @Test
  void leidenStopsWhenNothingMovesWhateverMaxIterationsIs() {
    cliques(2, 4, new int[][] { { 3, 4 } });

    assertThat(groups(communities("algo.leiden('E', 2147483647, 1.0)", "nodeId", "community"))).containsExactlyInAnyOrder(range(0, 4),
        range(4, 8));
  }

  @Test
  void leidenResolutionSetsTheGranularity() {
    // a star: centre 0, leaves 1..5. At resolution 0 the gain of a community is the weight towards it, so a lone leaf always
    // gains by joining the centre's community and the centre never leaves the community that holds its leaves, whatever
    // order the nodes are visited in: one community
    database.transaction(() -> {
      final MutableVertex centre = database.newVertex("N").set("id", 0).save();
      for (int i = 1; i <= 5; i++)
        centre.newEdge("E", database.newVertex("N").set("id", i).save()).save();
    });
    assertThat(groups(communities("algo.leiden('E', 10, 0.0)", "nodeId", "community"))).containsExactly(range(0, 6));
    // a huge resolution makes every join lose: singletons
    assertThat(groups(communities("algo.leiden('E', 10, 1000000.0)", "nodeId", "community"))).hasSize(6);
  }

  /**
   * The tolerance check measures the objective the moves optimise. A triangle 1-2-3 with a pendant 0 on 1, at resolution 0.25:
   * the first pass leaves {0} and {1, 2, 3}, which a second pass joins. That first pass changes the resolution-0.25 modularity
   * by more than 0.05 but the standard modularity (resolution 1) by less, so a check at resolution 1 stopped the passes there.
   */
  @Test
  void theToleranceIsMeasuredAtTheResolutionTheMovesOptimise() {
    final int[][] neighbors = { { 1 }, { 0, 2, 3 }, { 1, 3 }, { 1, 2 } };
    final double[][] weights = { { 1.0 }, { 1.0, 1.0, 1.0 }, { 1.0, 1.0 }, { 1.0, 1.0 } };
    final WorkGuard guard = WorkGuard.forDeadline("the test", Long.MAX_VALUE);

    // tolerance 0: the passes run until nothing moves
    assertThat(AlgoLouvain.detect(neighbors, weights, 100, 0.0, 0.25, guard).community()).containsExactly(0, 0, 0, 0);
    assertThat(AlgoLouvain.detect(neighbors, weights, 100, 0.05, 0.25, guard).community()).containsExactly(0, 0, 0, 0);
  }

  @Test
  void leidenKeepsIsolatedVerticesApart() {
    cliques(1, 3, new int[][] {});
    database.transaction(() -> database.newVertex("N").set("id", 3).save());

    assertThat(groups(communities("algo.leiden()", "nodeId", "community"))).containsExactlyInAnyOrder(range(0, 3), Set.of(3));
  }
}
