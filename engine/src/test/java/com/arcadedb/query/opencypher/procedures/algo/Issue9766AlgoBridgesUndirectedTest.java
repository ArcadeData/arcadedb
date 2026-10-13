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
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #9766: {@code algo.bridges} ran Tarjan's undirected bridge algorithm over the OUTGOING
 * adjacency only, so whether an edge was a bridge depended on which way it was stored and on the order the edges were
 * enumerated in. It now walks the undirected adjacency through the DFS {@code algo.articulationPoints} and
 * {@code algo.biconnectedComponents} share, which also skips the tree edge by occurrence, so a doubled edge is not a
 * bridge.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9766AlgoBridgesUndirectedTest {
  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-issue-9766-bridges");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("N").createProperty("k", Type.INTEGER);
    database.getSchema().createEdgeType("R");
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  @Test
  void barbellBridgeFoundWhicheverWayTheConnectorIsStored() {
    // A) connector stored 2->3
    load(6, 0, 1, 1, 2, 2, 0, 2, 3, 3, 4, 4, 5, 5, 3);
    assertThat(bridges()).containsExactly("2->3");
    assertThat(articulationPoints()).containsExactly(2, 3);
  }

  @Test
  void barbellBridgeFoundWhenTheConnectorIsStoredBackwards() {
    // B) the same undirected graph, connector stored 3->2: the bridge used to be lost
    load(6, 0, 1, 1, 2, 2, 0, 3, 2, 3, 4, 4, 5, 5, 3);
    assertThat(bridges()).containsExactly("3->2");
    assertThat(articulationPoints()).containsExactly(2, 3);
  }

  @Test
  void triangleHasNoBridgeWhateverItsOrientation() {
    // E) undirected triangle stored 0->1, 2->1, 2->0: used to report 0->1
    load(3, 0, 1, 2, 1, 2, 0);
    assertThat(bridges()).isEmpty();
    assertThat(articulationPoints()).isEmpty();
  }

  @Test
  void bothEdgesOfAPathAreBridges() {
    // F) path stored 0->1, 2->1: the second bridge used to be lost
    load(3, 0, 1, 2, 1);
    assertThat(bridges()).containsExactlyInAnyOrder("0->1", "2->1");
    assertThat(articulationPoints()).containsExactly(1);
    assertThat(biconnectedComponents()).containsExactlyInAnyOrder(Set.of(0, 1), Set.of(1, 2));
  }

  @Test
  void parallelEdgesAreNotBridges() {
    // 0 = 1 doubled (once each way, and once more the same way), 1 - 2 single: only 1 - 2 is a bridge
    load(3, 0, 1, 1, 0, 1, 2);
    assertThat(bridges()).containsExactly("1->2");

    load(3, 0, 1, 0, 1, 1, 2);
    assertThat(bridges()).containsExactly("1->2");
    assertThat(articulationPoints()).containsExactly(1);
    assertThat(biconnectedComponents()).containsExactlyInAnyOrder(Set.of(0, 1), Set.of(1, 2));
  }

  @Test
  void selfLoopIsNotABridgeAndDoesNotHideOne() {
    load(2, 0, 0, 0, 1);
    assertThat(bridges()).containsExactly("0->1");
  }

  @Test
  void relTypeFilterSelectsTheUndirectedSubgraph() {
    database.getSchema().createEdgeType("S");
    // Over R alone 0 - 1 - 2 is a path (two bridges); the S edge 2->0 closes it into a cycle only over both types
    load(3, 0, 1, 2, 1);
    database.transaction(() -> vertex(2).newEdge("S", vertex(0)).save());
    assertThat(bridges("'R'")).containsExactlyInAnyOrder("0->1", "2->1");
    assertThat(bridges("['R', 'S']")).isEmpty();
  }

  @Test
  void sameAnswerWithAndWithoutAGraphAnalyticalView() {
    // C) 2-edge-connected: the view's enumeration order used to make a bridge appear
    load(7, 0, 1, 1, 2, 2, 0, 1, 3, 3, 4, 4, 5, 5, 3, 0, 5, 2, 6, 6, 1, 3, 6);
    assertThat(bridges()).isEmpty();

    final GraphAnalyticalView view = GraphAnalyticalView.builder(database).withName("bridges9766").withVertexTypes("N")
        .withEdgeTypes("R").build();
    try {
      assertThat(bridges()).isEmpty();

      // And a bridge the view must report too: a pendant vertex hung off 4, stored towards it
      database.transaction(() -> {
        final MutableVertex v7 = database.newVertex("N").set("k", 7).save();
        v7.newEdge("R", vertex(4)).save();
      });
      assertThat(bridges()).containsExactly("7->4");
    } finally {
      view.drop();
    }
  }

  @Test
  void longPathDoesNotOverflowTheStack() {
    // Every edge of a path is a bridge and the DFS is as deep as the path: the walk is iterative, so no stack frame per
    // vertex. Edges alternate direction so the undirected walk is what reaches the far end.
    final int n = 20_000;
    final int[] pairs = new int[(n - 1) * 2];
    for (int i = 0; i < n - 1; i++) {
      pairs[i * 2] = i % 2 == 0 ? i : i + 1;
      pairs[i * 2 + 1] = i % 2 == 0 ? i + 1 : i;
    }
    load(n, pairs);
    assertThat(bridges()).hasSize(n - 1);
    assertThat(articulationPoints()).hasSize(n - 2);
  }

  @Test
  void biconnectedComponentsUnchangedOnTheIssueGraphs() {
    load(6, 0, 1, 1, 2, 2, 0, 3, 2, 3, 4, 4, 5, 5, 3);
    assertThat(biconnectedComponents()).containsExactlyInAnyOrder(Set.of(0, 1, 2), Set.of(2, 3), Set.of(3, 4, 5));
  }

  /** Recreates the graph: {@code n} vertices keyed 0..n-1 and one R edge per (from, to) pair. */
  private void load(final int n, final int... pairs) {
    database.transaction(() -> {
      database.command("sql", "DELETE FROM N");
      final List<MutableVertex> vertices = new ArrayList<>();
      for (int i = 0; i < n; i++)
        vertices.add(database.newVertex("N").set("k", i).save());
      for (int i = 0; i < pairs.length; i += 2)
        vertices.get(pairs[i]).newEdge("R", vertices.get(pairs[i + 1])).save();
    });
  }

  private MutableVertex vertex(final int k) {
    return database.query("sql", "SELECT FROM N WHERE k = ?", k).next().getVertex().orElseThrow().modify();
  }

  private List<String> bridges() {
    return bridges(null);
  }

  private List<String> bridges(final String relTypes) {
    final List<String> found = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher",
        "CALL algo.bridges(" + (relTypes != null ? relTypes : "") + ") YIELD source, target "
            + "RETURN source.k AS s, target.k AS t")) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        found.add(r.getProperty("s") + "->" + r.getProperty("t"));
      }
    }
    return found;
  }

  private Set<Integer> articulationPoints() {
    final Set<Integer> found = new TreeSet<>();
    try (final ResultSet rs = database.query("opencypher", "CALL algo.articulationPoints() YIELD node RETURN node.k AS k")) {
      while (rs.hasNext())
        found.add(rs.next().<Integer>getProperty("k"));
    }
    return found;
  }

  private List<Set<Integer>> biconnectedComponents() {
    final Map<Integer, Set<Integer>> components = new HashMap<>();
    try (final ResultSet rs = database.query("opencypher",
        "CALL algo.biconnectedComponents() YIELD node, componentId RETURN node.k AS k, componentId")) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        components.computeIfAbsent(r.<Integer>getProperty("componentId"), id -> new TreeSet<>()).add(r.<Integer>getProperty("k"));
      }
    }
    return new ArrayList<>(components.values());
  }
}
