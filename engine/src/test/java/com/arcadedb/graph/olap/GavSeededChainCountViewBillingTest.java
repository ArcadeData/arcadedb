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
package com.arcadedb.graph.olap;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.graph.GraphTraversalProviderRegistry;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for #12012: the chain count push-down walked from one vertex (a UNIQUE key, a correlated {@code COUNT {}})
 * asked a Graph Analytical View for the neighbour view of each hop it reads, and a view holding an overlay packs that view
 * for the whole graph once per snapshot, which every commit replaces. A point count over two edge types,
 * {@code MATCH (a:P {pid: $p})-[:K]->(b)-[:L]->(c) RETURN count(*)}, built the whole {@code K} adjacency after every write:
 * the cost #10471 removed from the fused chain. The walk now asks for a view only when the entries it visits pay for the
 * build, and walks per node otherwise.
 */
class GavSeededChainCountViewBillingTest extends TestHelper {
  private static final int      VERTICES = 3000;
  private static final int      DEGREE   = 8;
  private static final String[] POINTS   = { "MATCH (a:P {pid: $p})-[:K]->(b)-[:L]->(c) RETURN count(*) AS n",
      "MATCH (a:P {pid: $p})-[:K]->(b:P)-[:L]->(c:P) RETURN count(*) AS n" };

  private final List<RID> vertices = new ArrayList<>();
  private final Random    random   = new Random(10471);

  @Override
  protected void beginTest() {
    database.getSchema().createVertexType("P").createProperty("pid", Type.INTEGER);
    database.getSchema().createEdgeType("K");
    database.getSchema().createEdgeType("L");
    database.command("sql", "CREATE INDEX ON P (pid) UNIQUE").close();
    database.transaction(() -> {
      for (int i = 0; i < VERTICES; i++)
        vertices.add(database.newVertex("P").set("pid", i).save().getIdentity());
      for (int i = 0; i < VERTICES; i++)
        for (int k = 0; k < DEGREE; k++) {
          vertices.get(i).asVertex().modify().newEdge("K", vertices.get((i + 1 + random.nextInt(VERTICES - 1)) % VERTICES));
          vertices.get(i).asVertex().modify().newEdge("L", vertices.get((i + 1 + random.nextInt(VERTICES - 1)) % VERTICES));
        }
    });
  }

  @ParameterizedTest
  @ValueSource(strings = { "SYNCHRONOUS", "ASYNCHRONOUS" })
  void pointCountsDoNotBuildTheWholeGraphViewOfEachSnapshot(final String mode) {
    final GraphAnalyticalView view = createView(mode);
    for (final String query : POINTS)
      for (int round = 0; round < 4; round++) {
        commitAFewEdges();
        assertThat(view.awaitReady(60, TimeUnit.SECONDS)).isTrue();
        assertThat(view.hasActiveOverlay()).as("the overlay lives until the compaction threshold").isTrue();

        for (int i = 0; i < 20; i++) {
          final int pid = random.nextInt(VERTICES);
          assertThat(count(query, pid)).as("%s from %d", query, pid).isEqualTo(countWithoutTheView(view, query, pid));
        }

        assertThat(view.pinned().getNeighborViewBuildCost(Vertex.DIRECTION.OUT, "K")).as("round %d of %s", round, query)
            .isPositive();
        assertThat(view.pinned().getNeighborViewBuildCost(Vertex.DIRECTION.OUT, "L")).as("round %d of %s", round, query)
            .isPositive();
      }
  }

  /**
   * The visits are billed, not refused: a count from a hub reads the {@code L} adjacency of most of the graph, and a few of them
   * add up to the cost of the view, which is then built and read by the next one. Every answer is the records' answer.
   */
  @ParameterizedTest
  @ValueSource(strings = { "SYNCHRONOUS", "ASYNCHRONOUS" })
  void countsFromAHubPayForTheViewBetweenThem(final String mode) {
    final GraphAnalyticalView view = createView(mode);
    database.transaction(() -> {
      for (int i = 0; i < 6000; i++)
        vertices.get(0).asVertex().modify().newEdge("K", vertices.get(1 + random.nextInt(VERTICES - 1)));
      // an L edge in the overlay too: without one the L view is the base's own arrays, which cost nothing to read
      vertices.get(1).asVertex().modify().newEdge("L", vertices.get(2));
    });
    assertThat(view.awaitReady(60, TimeUnit.SECONDS)).isTrue();
    assertThat(view.pinned().getNeighborViewBuildCost(Vertex.DIRECTION.OUT, "L")).as("the L view has to be built").isPositive();

    final String query = POINTS[1];
    final long expected = countWithoutTheView(view, query, 0);
    int reads = 0;
    while (view.pinned().getNeighborViewBuildCost(Vertex.DIRECTION.OUT, "L") > 0 && reads < 5) {
      assertThat(count(query, 0)).isEqualTo(expected);
      ++reads;
    }
    assertThat(view.pinned().getNeighborViewBuildCost(Vertex.DIRECTION.OUT, "L")).as("built after %d reads from the hub", reads)
        .isZero();
    assertThat(reads).as("one read from the hub does not visit the whole L adjacency").isGreaterThan(1);
    assertThat(count(query, 0)).isEqualTo(expected);
  }

  private GraphAnalyticalView createView(final String mode) {
    database.command("sql", "CREATE GRAPH ANALYTICAL VIEW gav VERTEX TYPES (P) EDGE TYPES (K, L) UPDATE MODE " + mode).close();
    final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "gav");
    assertThat(view.awaitReady(60, TimeUnit.SECONDS)).isTrue();
    // warm the plans, so the first query of the test is not the one that plans
    for (final String query : POINTS)
      count(query, 0);
    return view;
  }

  private void commitAFewEdges() {
    database.transaction(() -> {
      for (int i = 0; i < 2; i++) {
        vertices.get(random.nextInt(VERTICES)).asVertex().modify().newEdge("K", vertices.get(random.nextInt(VERTICES)));
        vertices.get(random.nextInt(VERTICES)).asVertex().modify().newEdge("L", vertices.get(random.nextInt(VERTICES)));
      }
    });
  }

  private long count(final String query, final int pid) {
    try (final ResultSet rs = database.query("opencypher", query, Map.of("p", pid))) {
      return rs.next().<Number>getProperty("n").longValue();
    }
  }

  private long countWithoutTheView(final GraphAnalyticalView view, final String query, final int pid) {
    GraphTraversalProviderRegistry.unregister(database, view);
    try {
      return count(query + " /* records */", pid);
    } finally {
      GraphTraversalProviderRegistry.register(database, view);
    }
  }
}
