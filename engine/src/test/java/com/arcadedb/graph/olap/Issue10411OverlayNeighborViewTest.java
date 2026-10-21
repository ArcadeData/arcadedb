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
import com.arcadedb.graph.GraphTraversalProvider;
import com.arcadedb.graph.GraphTraversalProviderRegistry;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.NeighborView;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10411: a Graph Analytical View that held any overlay answered {@code getNeighborView()} with null, so every
 * whole-graph reader - the Cypher count operators, the algorithms - took the per-node path for as long as the overlay lived:
 * one committed edge made a 2-hop count about 27% slower until the overlay was compacted, and since #10212 an ASYNCHRONOUS
 * view merges every commit into an overlay. The view now serves the overlay applied to the base, built once per snapshot.
 * <p>
 * The view must answer exactly what the per-node reads answer: checked entry for entry against {@code getNeighborIds()} for
 * every node, every direction and every type combination, after random additions, deletions, new vertices, parallel edges and
 * self-loops, and through the Cypher counts against the records.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10411OverlayNeighborViewTest extends TestHelper {
  private static final String[][] TYPE_SETS = { { "K" }, { "J" }, { "K", "J" }, null };

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE P");
    database.command("sql", "CREATE EDGE TYPE K");
    database.command("sql", "CREATE EDGE TYPE J");
  }

  @ParameterizedTest
  @ValueSource(strings = { "SYNCHRONOUS", "ASYNCHRONOUS" })
  void theOverlayViewIsThePerNodeAnswer(final String mode) {
    final Random random = new Random(10411);
    final List<RID> vertices = new ArrayList<>();
    database.transaction(() -> {
      for (int i = 0; i < 60; i++)
        vertices.add(database.newVertex("P").set("id", i).save().getIdentity());
      for (int i = 0; i < 240; i++)
        connect(vertices, random);
    });

    database.command("sql", "CREATE GRAPH ANALYTICAL VIEW gav VERTEX TYPES (P) EDGE TYPES (K, J) UPDATE MODE " + mode).close();
    final GraphAnalyticalView view = awaitView();
    assertViewIsThePerNodeAnswer(view);

    for (int round = 0; round < 8; round++) {
      database.transaction(() -> {
        for (int i = 0; i < 6; i++)
          connect(vertices, random);
        // a vertex only the overlay holds, with edges both ways and a self-loop
        final MutableVertex added = database.newVertex("P").set("id", vertices.size()).save();
        added.newEdge("K", vertices.get(random.nextInt(vertices.size())));
        vertices.get(random.nextInt(vertices.size())).asVertex().modify().newEdge("J", added);
        added.newEdge("K", added);
        vertices.add(added.getIdentity());
        // an edge deleted, and a vertex deleted with all its edges
        final Vertex source = vertices.get(random.nextInt(vertices.size())).asVertex();
        source.getEdges(Vertex.DIRECTION.OUT).forEach(edge -> {
          if (random.nextBoolean())
            edge.delete();
        });
        vertices.remove(random.nextInt(vertices.size())).asVertex().delete();
      });
      awaitView();

      assertThat(view.hasActiveOverlay()).as("the overlay lives until the compaction threshold").isTrue();
      assertViewIsThePerNodeAnswer(view);
      assertCountsMatchTheRecords();
    }
  }

  private static void connect(final List<RID> vertices, final Random random) {
    final RID from = vertices.get(random.nextInt(vertices.size()));
    // a self-loop now and then, and parallel edges from the small id range
    final RID to = random.nextInt(10) == 0 ? from : vertices.get(random.nextInt(Math.min(vertices.size(), 20)));
    from.asVertex().modify().newEdge(random.nextBoolean() ? "K" : "J", to);
  }

  /** Every node's range of every view is, entry for entry, what {@code getNeighborIds()} answers for it. */
  private void assertViewIsThePerNodeAnswer(final GraphAnalyticalView view) {
    final GraphTraversalProvider pinned = view.pinned();
    final int bound = pinned.getNodeIdUpperBound();
    for (final Vertex.DIRECTION direction : Vertex.DIRECTION.values())
      for (final String[] types : TYPE_SETS) {
        final NeighborView neighbors = types == null ? pinned.getNeighborView(direction) : pinned.getNeighborView(direction, types);
        assertThat(neighbors).as("a view while the overlay is active, " + direction + " " + Arrays.toString(types)).isNotNull();
        assertThat(neighbors.nodeCount()).isEqualTo(bound);
        for (int node = 0; node < bound; node++) {
          final int[] expected = types == null ? pinned.getNeighborIds(node, direction) : pinned.getNeighborIds(node, direction, types);
          final int[] actual = Arrays.copyOfRange(neighbors.neighbors(), neighbors.offset(node), neighbors.offsetEnd(node));
          assertThat(actual).as("node " + node + " " + direction + " " + Arrays.toString(types)).containsExactly(expected);
        }
      }
    final int[] degrees = new int[bound];
    for (final String slice : new String[] { "K", "J" })
      for (final Vertex.DIRECTION direction : Vertex.DIRECTION.values()) {
        final NeighborView neighbors = pinned.getNeighborViewOfSlice(direction, slice);
        assertThat(neighbors).isNotNull();
        pinned.getDegrees(degrees, direction, slice);
        for (int node = 0; node < bound; node++) {
          final int[] expected = pinned.getNeighborIdsOfSlice(node, direction, slice);
          assertThat(Arrays.copyOfRange(neighbors.neighbors(), neighbors.offset(node), neighbors.offsetEnd(node)))
              .containsExactly(expected);
          if (pinned.isNodeLive(node))
            assertThat(degrees[node]).as("degree of node " + node + " " + direction + " " + slice).isEqualTo(expected.length);
        }
      }
  }

  /** The counts the view-served Cypher operators answer are the ones the records give. */
  private void assertCountsMatchTheRecords() {
    for (final String pattern : new String[] {
        "(a:P)-[:K]->(b:P)-[:K]->(c:P)",
        "(a:P)-[:K]->(b:P)-[:J]->(c:P)",
        "(a:P)-[:K|J]-(b:P)",
        "(a:P)-[:K]-(b:P)-[:K]-(c:P)",
        "(a:P)<-[:J]-(b:P)-[:K]->(c:P)" }) {
      final long viewed = count("MATCH " + pattern + " RETURN count(*) AS n");
      final long recorded = countWithoutTheView(pattern);
      assertThat(viewed).as(pattern).isEqualTo(recorded);
    }
  }

  /** The same count off the records: the view withheld from the planner, under a query text no cached plan holds. */
  private long countWithoutTheView(final String pattern) {
    final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "gav");
    GraphTraversalProviderRegistry.unregister(database, view);
    try {
      return count("MATCH " + pattern + " RETURN count(*) AS n /* records */");
    } finally {
      GraphTraversalProviderRegistry.register(database, view);
    }
  }

  private long count(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      return rs.next().<Number>getProperty("n").longValue();
    }
  }

  private GraphAnalyticalView awaitView() {
    final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "gav");
    assertThat(view.awaitReady(60, TimeUnit.SECONDS)).isTrue();
    return view;
  }
}
