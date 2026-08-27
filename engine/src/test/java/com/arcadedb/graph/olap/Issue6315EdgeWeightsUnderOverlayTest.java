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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.NodeEdgeWeights;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #6315 - the Graph Analytical View answered edge properties from the base CSR while
 * a delta overlay was active, so a caller reading them by neighbour position got a weight belonging to a
 * different edge.
 * <p>
 * The columns are aligned with the base CSR's forward edge slots. {@code getNeighborIds} serves the overlay's
 * view of the node instead: deleted edges dropped, added ones merged in, the whole list re-sorted. Delete the
 * first of three outgoing edges and every remaining neighbour shifts down one position, while the columns do
 * not - so position 1 answered with the deleted edge's weight, which is a wrong shortest path, a wrong MST and
 * a wrong Steiner tree, and never an exception. PR #6306 had closed that for the callers that existed by
 * narrowing {@code hasEdgeProperties()} to {@code false} whenever an overlay was active, which left the sharp
 * accessor in the SPI for the next caller to find.
 * <p>
 * The accessor is gone. {@link com.arcadedb.graph.GraphTraversalProvider#edgeWeightsForSlice} hands back the
 * neighbours and their weights together, paired where the overlay is applied, so there is no position to
 * address across and no rule for a caller to remember. Resolving the two rather than refusing them also keeps
 * the columnar path alive while a view is being updated, which is the state {@code UpdateMode.SYNCHRONOUS}
 * leaves it in after every single commit:
 * <ul>
 *   <li>a base edge the overlay has not deleted answers from its own column slot;</li>
 *   <li>an edge the overlay added answers from the value captured for it at commit time;</li>
 *   <li>an edge whose property was <em>updated</em> has neither - nothing maps a column slot back from an RID -
 *       so the view reports no edge properties until the rebuild that repairs the columns lands.</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6315EdgeWeightsUnderOverlayTest {
  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-issue-6315-edge-weights-overlay");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("N");
    database.getSchema().createEdgeType("ROAD").createProperty("w", Type.DOUBLE);
  }

  @AfterEach
  void teardown() {
    if (database != null) {
      if (database.isTransactionActive())
        database.rollback();
      database.drop();
      database = null;
    }
  }

  /**
   * The shape the issue is about: A has three outgoing edges of distinct weight, one of them is deleted in a
   * committed transaction, and the two that survive must still each answer with their own weight. Positionally
   * they no longer sit where they sat: dropping the first neighbour shifts the other two down one slot each.
   */
  @Test
  void aDeletedEdgeDoesNotShiftItsNeighboursOntoTheWrongWeights() {
    starGraph();

    final GraphAnalyticalView view = syncView("overlay-deleted-edge");
    try {
      final int a = view.getNodeId(vertex("A").getIdentity());
      assertThat(weightsByName(view, a)).containsExactlyInAnyOrderEntriesOf(Map.of("B", 10.0, "C", 20.0, "D", 30.0));

      deleteEdge("A", "B");

      assertThat(view.hasEdgeProperty("ROAD", "w"))
          .as("the overlay is resolved against the columns, not a reason to stop serving them")
          .isTrue();
      assertThat(weightsByName(view, a))
          .as("B is gone; C and D moved down a slot each and must keep their own weights, not B's and C's")
          .containsExactlyInAnyOrderEntriesOf(Map.of("C", 20.0, "D", 30.0));
    } finally {
      view.drop();
    }
  }

  /**
   * The same for the incoming direction, which reaches its columns through the backward-to-forward slot
   * mapping - a second addressing step the merge has to survive.
   */
  @Test
  void aDeletedEdgeDoesNotShiftIncomingNeighboursEither() {
    // B and C and D all point at A, so A's incoming slice is the one the deletion re-shapes.
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("N").set("name", "A").save();
      final MutableVertex b = database.newVertex("N").set("name", "B").save();
      final MutableVertex c = database.newVertex("N").set("name", "C").save();
      final MutableVertex d = database.newVertex("N").set("name", "D").save();
      b.newEdge("ROAD", a, true, new Object[] { "w", 10.0 }).save();
      c.newEdge("ROAD", a, true, new Object[] { "w", 20.0 }).save();
      d.newEdge("ROAD", a, true, new Object[] { "w", 30.0 }).save();
    });

    final GraphAnalyticalView view = syncView("overlay-deleted-incoming-edge");
    try {
      final int a = view.getNodeId(vertex("A").getIdentity());
      deleteEdge("B", "A");

      final Map<String, Double> incoming = new HashMap<>();
      final NodeEdgeWeights edges = view.edgeWeightsForSlice(a, Vertex.DIRECTION.IN, "ROAD", "w", -1.0, null);
      for (int i = 0; i < edges.neighbors().length; i++)
        incoming.put((String) view.getRID(edges.neighbors()[i]).asVertex().get("name"), edges.weights()[i]);

      assertThat(incoming).containsExactlyInAnyOrderEntriesOf(Map.of("C", 20.0, "D", 30.0));
    } finally {
      view.drop();
    }
  }

  /**
   * An edge the overlay added has no column slot at all - the columns were built with the base CSR - and used to
   * be the reason the whole node could not be answered for. It carries the value captured for it at commit time
   * instead, so the answer stays exact rather than defaulting the new edge to a unit weight.
   */
  @Test
  void anEdgeTheOverlayAddedAnswersWithItsOwnWeight() {
    starGraph();

    final GraphAnalyticalView view = syncView("overlay-added-edge");
    try {
      final int a = view.getNodeId(vertex("A").getIdentity());

      database.transaction(() -> {
        final MutableVertex e = database.newVertex("N").set("name", "E").save();
        vertex("A").newEdge("ROAD", e, true, new Object[] { "w", 99.0 }).save();
      });

      assertThat(weightsByName(view, a))
          .as("the added edge weighs what it was created with, not the default")
          .containsExactlyInAnyOrderEntriesOf(Map.of("B", 10.0, "C", 20.0, "D", 30.0, "E", 99.0));
    } finally {
      view.drop();
    }
  }

  /**
   * The limit of what the overlay can resolve, pinned so that it is refused rather than guessed. Deletions are
   * counted per pair - all the neighbour list needs, since dropping any one of a pair's parallel edges leaves
   * the same neighbours behind - but parallel edges need not weigh the same, and nothing says which of them
   * died. Answering would mean handing back a plausible wrong multiset of weights, which is the failure this
   * whole issue is about, so the view says it cannot serve the node and the caller reads the records.
   */
  @Test
  void aPartiallyDeletedRunOfParallelEdgesIsRefusedRatherThanGuessed() {
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("N").set("name", "A").save();
      final MutableVertex b = database.newVertex("N").set("name", "B").save();
      a.newEdge("ROAD", b, true, new Object[] { "w", 1.0 }).save();
      a.newEdge("ROAD", b, true, new Object[] { "w", 2.0 }).save();
      a.newEdge("ROAD", b, true, new Object[] { "w", 3.0 }).save();
    });

    final GraphAnalyticalView view = syncView("overlay-parallel-edges");
    try {
      final int a = view.getNodeId(vertex("A").getIdentity());

      // Delete the 2.0 edge and add a 4.0 one, both between the same pair.
      database.transaction(() -> {
        for (final Edge edge : vertex("A").getEdges(Vertex.DIRECTION.OUT, "ROAD"))
          if (Double.valueOf(2.0).equals(edge.get("w")))
            edge.delete();
        vertex("A").newEdge("ROAD", vertex("B"), true, new Object[] { "w", 4.0 }).save();
      });

      assertThat(view.edgeWeightsForSlice(a, Vertex.DIRECTION.OUT, "ROAD", "w", -1.0, null))
          .as("which of the three parallel edges was deleted is not recoverable from a per-pair count")
          .isNull();
      assertThat(view.edgeWeightsOf(a, Vertex.DIRECTION.OUT, "w", -1.0, "ROAD"))
          .as("and a node the provider cannot answer for makes the whole call fall back, not half of it")
          .isNull();
    } finally {
      view.drop();
    }
  }

  /**
   * The counterweight: parallel edges are only ambiguous while some of a pair's run survives a deletion.
   * Deleting the whole run leaves nothing to be ambiguous about, and adding to it identifies each new edge by
   * its own captured value, so both keep answering.
   */
  @Test
  void parallelEdgesStillAnswerWhenTheDeletionIsNotAmbiguous() {
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("N").set("name", "A").save();
      final MutableVertex b = database.newVertex("N").set("name", "B").save();
      final MutableVertex c = database.newVertex("N").set("name", "C").save();
      a.newEdge("ROAD", b, true, new Object[] { "w", 1.0 }).save();
      a.newEdge("ROAD", b, true, new Object[] { "w", 2.0 }).save();
      a.newEdge("ROAD", c, true, new Object[] { "w", 5.0 }).save();
    });

    final GraphAnalyticalView view = syncView("overlay-parallel-edges-unambiguous");
    try {
      final int a = view.getNodeId(vertex("A").getIdentity());

      // Both A->B edges go, and two more parallel ones are added between the same pair.
      database.transaction(() -> {
        for (final Edge edge : vertex("A").getEdges(Vertex.DIRECTION.OUT, "ROAD"))
          if ("B".equals(edge.getInVertex().get("name")))
            edge.delete();
        vertex("A").newEdge("ROAD", vertex("B"), true, new Object[] { "w", 7.0 }).save();
        vertex("A").newEdge("ROAD", vertex("B"), true, new Object[] { "w", 8.0 }).save();
      });

      final NodeEdgeWeights edges = view.edgeWeightsForSlice(a, Vertex.DIRECTION.OUT, "ROAD", "w", -1.0, null);
      assertThat(edges.neighbors()).isEqualTo(view.getNeighborIds(a, Vertex.DIRECTION.OUT, "ROAD"));
      assertThat(edges.weights()).containsExactlyInAnyOrder(5.0, 7.0, 8.0);
    } finally {
      view.drop();
    }
  }

  /**
   * The one thing the overlay cannot repair. An edge already in the base CSR is addressed by a column slot and
   * nothing maps that slot back from its RID, so a committed change to its weight leaves the columns holding a
   * value the database no longer has. Serving the added edges exactly while quietly serving this one from a
   * stale column would be the same silent wrong number the issue is about, reached from the other side, so the
   * view reports no edge properties until the rebuild it forces for the update lands.
   */
  @Test
  void anUpdatedEdgeWeightStopsTheColumnsFromBeingServedUntilTheRebuildLands() {
    starGraph();

    final GraphAnalyticalView view = syncView("overlay-updated-weight");
    try {
      final int a = view.getNodeId(vertex("A").getIdentity());

      database.transaction(() -> {
        for (final Edge edge : vertex("A").getEdges(Vertex.DIRECTION.OUT, "ROAD"))
          if ("B".equals(edge.getInVertex().get("name")))
            edge.modify().set("w", 77.0).save();
      });

      // Either the rebuild has not landed yet - and then the columns are refused rather than served stale - or
      // it has, and then they carry the new value. Never the old one.
      if (view.hasEdgeProperty("ROAD", "w"))
        assertThat(weightsByName(view, a).get("B")).isEqualTo(77.0);
      else
        assertThat(view.edgeWeightsForSlice(a, Vertex.DIRECTION.OUT, "ROAD", "w", -1.0, null)).isNull();

      // The forced rebuild makes it visible for good.
      assertThat(view.awaitReady(30, TimeUnit.SECONDS)).isTrue();
      waitUntilServed(view);
      assertThat(weightsByName(view, view.getNodeId(vertex("A").getIdentity())).get("B")).isEqualTo(77.0);
    } finally {
      view.drop();
    }
  }

  /**
   * The neighbours {@code edgeWeightsForSlice} pairs its weights with have to be the very ones
   * {@code getNeighborIds} reports, or a caller that mixes the two - and everything reading a Graph Analytical
   * View has both in hand - sees two different graphs.
   */
  @Test
  void theNeighboursAreTheOnesGetNeighborIdsReports() {
    starGraph();

    final GraphAnalyticalView view = syncView("overlay-neighbour-agreement");
    try {
      final int a = view.getNodeId(vertex("A").getIdentity());
      deleteEdge("A", "C");
      database.transaction(() -> {
        final MutableVertex e = database.newVertex("N").set("name", "E").save();
        vertex("A").newEdge("ROAD", e, true, new Object[] { "w", 99.0 }).save();
      });

      final NodeEdgeWeights edges = view.edgeWeightsForSlice(a, Vertex.DIRECTION.OUT, "ROAD", "w", -1.0, null);
      assertThat(edges.neighbors()).isEqualTo(view.getNeighborIds(a, Vertex.DIRECTION.OUT, "ROAD"));
      assertThat(edges.weights()).hasSize(edges.neighbors().length);
    } finally {
      view.drop();
    }
  }

  /**
   * A direction has an adjacency slice; {@code BOTH} does not. Answering it here would mean answering for
   * neither, so the slice accessor refuses it and {@code edgeWeightsOf} splits it into the two that exist -
   * which is the whole of the multi-slice composition, exercised end to end with an overlay in play.
   */
  @Test
  void bothDirectionsAreSplitIntoTheSlicesThatExist() {
    starGraph();
    // A -> B, C, D plus D -> A, so A's BOTH neighbourhood spans the outgoing and the incoming slice alike.
    database.transaction(() -> vertex("D").newEdge("ROAD", vertex("A"), true, new Object[] { "w", 40.0 }).save());

    final GraphAnalyticalView view = syncView("overlay-both-directions");
    try {
      final int a = view.getNodeId(vertex("A").getIdentity());
      deleteEdge("A", "B");

      assertThat(view.edgeWeightsForSlice(a, Vertex.DIRECTION.BOTH, "ROAD", "w", -1.0, null))
          .as("BOTH has no slice of its own to align with")
          .isNull();

      final NodeEdgeWeights both = view.edgeWeightsOf(a, Vertex.DIRECTION.BOTH, "w", -1.0, "ROAD");
      assertThat(both.weights()).containsExactlyInAnyOrder(20.0, 30.0, 40.0);
    } finally {
      view.drop();
    }
  }

  /**
   * The collateral of letting the view serve edge properties through an overlay: {@code algo.dijkstra.singleSource}
   * routes onto a kernel that reads the CSR offset and neighbour arrays directly, and those arrays are the graph
   * as it stood at the last build. It used to be kept off them by the view reporting no edge properties whenever
   * an overlay was active - a gate it never asked for and could not see. The kernel refuses for itself now, so
   * the answer follows the edges that exist rather than the ones the arrays remember.
   */
  @Test
  void dijkstraDoesNotAnswerFromTheStaleBaseCsrWhileAnOverlayIsActive() {
    // A -> B costs 10 directly, or 2 the way round through C.
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("N").set("name", "A").save();
      final MutableVertex b = database.newVertex("N").set("name", "B").save();
      final MutableVertex c = database.newVertex("N").set("name", "C").save();
      a.newEdge("ROAD", b, true, new Object[] { "w", 10.0 }).save();
      a.newEdge("ROAD", c, true, new Object[] { "w", 1.0 }).save();
      c.newEdge("ROAD", b, true, new Object[] { "w", 1.0 }).save();
    });

    final GraphAnalyticalView view = syncView("overlay-dijkstra-kernel");
    try {
      assertThat(shortestPathCostToB()).isEqualTo(2.0);

      deleteEdge("C", "B");

      assertThat(shortestPathCostToB())
          .as("the shortcut through C is gone; only the base CSR arrays still have it")
          .isEqualTo(10.0);
    } finally {
      view.drop();
    }
  }

  /**
   * The other half of a node the provider cannot answer for: the algorithm above it has to notice and read that
   * node's edges itself, not take the {@code null} for an empty neighbourhood or fall over it. One ambiguous
   * node in an otherwise columnar graph is exactly the shape that reaches
   * {@code AbstractAlgoProcedure.weightedAdjacencyFromColumns}'s per-node fallback.
   */
  @Test
  void anAlgorithmReadsTheRecordsForANodeTheViewCannotAnswerFor() {
    // A reaches B directly over one of two parallel edges (1.0 and 100.0), or for 6.0 the way round through C.
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("N").set("name", "A").save();
      final MutableVertex b = database.newVertex("N").set("name", "B").save();
      final MutableVertex c = database.newVertex("N").set("name", "C").save();
      a.newEdge("ROAD", b, true, new Object[] { "w", 1.0 }).save();
      a.newEdge("ROAD", b, true, new Object[] { "w", 100.0 }).save();
      a.newEdge("ROAD", c, true, new Object[] { "w", 5.0 }).save();
      c.newEdge("ROAD", b, true, new Object[] { "w", 1.0 }).save();
    });

    final GraphAnalyticalView view = syncView("overlay-ambiguous-node-fallback");
    try {
      // The cheap direct edge goes, leaving the 100.0 one - and leaving A's run of parallel edges partially
      // deleted, which is the pair the per-pair deletion count cannot tell apart.
      database.transaction(() -> {
        for (final Edge edge : vertex("A").getEdges(Vertex.DIRECTION.OUT, "ROAD"))
          if (Double.valueOf(1.0).equals(edge.get("w"))) {
            edge.delete();
            return;
          }
      });

      try (final ResultSet rows = database.command("opencypher",
          "MATCH (a:N {name: 'A'}), (b:N {name: 'B'}) CALL algo.bellmanford(a, b, 'ROAD', 'w') "
              + "YIELD weight RETURN weight")) {
        assertThat(rows.hasNext()).isTrue();
        assertThat(((Number) rows.next().getProperty("weight")).doubleValue())
            .as("A-C-B at 6.0: the surviving direct edge weighs 100.0, whichever slot it sits in")
            .isEqualTo(6.0);
      }
    } finally {
      view.drop();
    }
  }

  /**
   * An update to an edge the overlay itself holds is applied there, where the edge's values live, and leaves
   * the base columns alone - they never described that edge to begin with. This is also what keeps the plain
   * {@code newEdge(...).save()} from forcing a rebuild on every insert: an insert reports one create and one
   * update of the same edge.
   */
  @Test
  void updatingAnEdgeTheOverlayHoldsDoesNotPutTheColumnsOutOfDate() {
    starGraph();

    final GraphAnalyticalView view = syncView("overlay-updated-added-edge");
    try {
      final int a = view.getNodeId(vertex("A").getIdentity());

      database.transaction(() -> {
        final MutableVertex e = database.newVertex("N").set("name", "E").save();
        vertex("A").newEdge("ROAD", e, true, new Object[] { "w", 99.0 }).save();
      });
      // A separate transaction, so the update is resolved against the overlay left behind by the previous one
      // rather than against this delta's own additions.
      database.transaction(() -> {
        for (final Edge edge : vertex("A").getEdges(Vertex.DIRECTION.OUT, "ROAD"))
          if ("E".equals(edge.getInVertex().get("name")))
            edge.modify().set("w", 55.0).save();
      });

      assertThat(view.hasEdgeProperty("ROAD", "w"))
          .as("nothing about the base columns changed, so they are still exact")
          .isTrue();
      assertThat(weightsByName(view, a).get("E"))
          .as("the overlay carries the new value, so no rebuild is needed to see it")
          .isEqualTo(55.0);
    } finally {
      view.drop();
    }
  }

  /**
   * Past a cap, one transaction's edge property changes stop being tracked individually and the delta says only
   * that the columns are out of date - a bulk rewrite would otherwise hold one entry per edge where the old
   * boolean flag held nothing. The view must come back on its own afterwards: the flag has to reach the same
   * rebuild an individually-tracked update reaches, including the follow-up one scheduled for a delta buffered
   * during a rebuild already in flight, or a bulk rewrite that is the last write to the graph leaves the view
   * serving no edge properties for good.
   */
  @Test
  void aBulkEdgePropertyRewriteStopsTrackingIndividuallyAndStillRepairsItself() {
    final int edgeCount = 1100; // over DeltaCollector's 1024 cap, so the individual tracking is given up on
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("N").set("name", "A").save();
      for (int i = 0; i < edgeCount; i++) {
        final MutableVertex target = database.newVertex("N").set("name", "T" + i).save();
        a.newEdge("ROAD", target, true, new Object[] { "w", 1.0 }).save();
      }
    });

    final GraphAnalyticalView view = syncView("overlay-bulk-edge-property-rewrite");
    try {
      final int a = view.getNodeId(vertex("A").getIdentity());
      assertThat(weightsByName(view, a)).hasSize(edgeCount);

      database.transaction(() -> {
        for (final Edge edge : vertex("A").getEdges(Vertex.DIRECTION.OUT, "ROAD"))
          edge.modify().set("w", 2.0).save();
      });

      // Nothing is served from the columns while they hold the old weights, whether or not the rebuild that
      // repairs them has landed yet.
      if (!view.hasEdgeProperty("ROAD", "w"))
        assertThat(view.edgeWeightsForSlice(a, Vertex.DIRECTION.OUT, "ROAD", "w", -1.0, null)).isNull();

      // And it lands without any further commit to prompt it.
      waitUntilServed(view);
      assertThat(weightsByName(view, view.getNodeId(vertex("A").getIdentity())).values())
          .as("every edge weighs what the bulk rewrite set it to")
          .containsOnly(2.0);
    } finally {
      view.drop();
    }
  }

  /**
   * A view that materialises no edge property columns has nothing that can go out of date when an edge's
   * properties change - the base CSR holds the topology, which an update does not touch - so the full rebuild
   * #4513 forces for such an update has nothing to repair. It was forced anyway, on every edge update, because
   * the flag was raised before anyone asked whether there were columns at all. The overlay surviving is the
   * observable half of that: a rebuild would have compacted it away.
   */
  @Test
  void anEdgeUpdateOnAViewWithoutEdgeColumnsDoesNotForceARebuild() throws InterruptedException {
    starGraph();

    final GraphAnalyticalView view = GraphAnalyticalView.builder(database)
        .withName("no-edge-columns-update")
        .withVertexTypes("N")
        .withEdgeTypes("ROAD")
        .withUpdateMode(GraphAnalyticalView.UpdateMode.SYNCHRONOUS)
        .build();
    try {
      assertThat(view.hasEdgeProperties()).as("no edge property columns were asked for").isFalse();

      // An added edge, so there is an overlay for the rebuild to compact away if one is forced.
      database.transaction(() -> {
        final MutableVertex e = database.newVertex("N").set("name", "E").save();
        vertex("A").newEdge("ROAD", e, true, new Object[] { "w", 99.0 }).save();
      });
      assertThat(view.getStats().get("overlayActive")).isEqualTo(true);

      database.transaction(() -> {
        for (final Edge edge : vertex("A").getEdges(Vertex.DIRECTION.OUT, "ROAD"))
          if ("B".equals(edge.getInVertex().get("name")))
            edge.modify().set("w", 77.0).save();
      });

      // Long enough for a rebuild of a five-vertex graph to have finished several times over.
      final long deadline = System.currentTimeMillis() + 1_000;
      while (System.currentTimeMillis() < deadline) {
        assertThat(view.getStats().get("overlayActive"))
            .as("nothing was rebuilt, so the overlay is still the one the added edge went into")
            .isEqualTo(true);
        Thread.sleep(25);
      }
      assertThat(view.getNeighborIds(view.getNodeId(vertex("A").getIdentity()), Vertex.DIRECTION.OUT, "ROAD"))
          .as("and the topology still has all four outgoing edges")
          .hasSize(4);
    } finally {
      view.drop();
    }
  }

  // ── Helpers ──────────────────────────────────────────────────────────────

  /** The {@code algo.dijkstra.singleSource} cost from A to B over the {@code w} property. */
  private double shortestPathCostToB() {
    try (final ResultSet rows = database.command("opencypher",
        "MATCH (a:N {name: 'A'}) CALL algo.dijkstra.singleSource(a, 'ROAD', 'w', 'OUT') YIELD node, cost "
            + "WITH node, cost WHERE node.name = 'B' RETURN cost")) {
      assertThat(rows.hasNext()).isTrue();
      return ((Number) rows.next().getProperty("cost")).doubleValue();
    }
  }

  /** A -> B at 10.0, A -> C at 20.0, A -> D at 30.0. */
  private void starGraph() {
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("N").set("name", "A").save();
      final MutableVertex b = database.newVertex("N").set("name", "B").save();
      final MutableVertex c = database.newVertex("N").set("name", "C").save();
      final MutableVertex d = database.newVertex("N").set("name", "D").save();
      a.newEdge("ROAD", b, true, new Object[] { "w", 10.0 }).save();
      a.newEdge("ROAD", c, true, new Object[] { "w", 20.0 }).save();
      a.newEdge("ROAD", d, true, new Object[] { "w", 30.0 }).save();
    });
  }

  private GraphAnalyticalView syncView(final String name) {
    final GraphAnalyticalView view = GraphAnalyticalView.builder(database)
        .withName(name)
        .withVertexTypes("N")
        .withEdgeTypes("ROAD")
        .withEdgeProperties("w")
        .withUpdateMode(GraphAnalyticalView.UpdateMode.SYNCHRONOUS)
        .build();
    assertThat(view.hasEdgeProperty("ROAD", "w")).isTrue();
    return view;
  }

  /** The outgoing ROAD weights of a node, keyed by the neighbour's name. */
  private Map<String, Double> weightsByName(final GraphAnalyticalView view, final int nodeId) {
    final NodeEdgeWeights edges = view.edgeWeightsForSlice(nodeId, Vertex.DIRECTION.OUT, "ROAD", "w", -1.0, null);
    assertThat(edges).isNotNull();
    final Map<String, Double> byName = new HashMap<>();
    for (int i = 0; i < edges.neighbors().length; i++)
      byName.put((String) view.getRID(edges.neighbors()[i]).asVertex().get("name"), edges.weights()[i]);
    return byName;
  }

  private Vertex vertex(final String name) {
    return database.query("sql", "select from N where name = ?", name).next().getRecord().get().asVertex();
  }

  private void deleteEdge(final String from, final String to) {
    database.transaction(() -> {
      for (final Edge edge : vertex(from).getEdges(Vertex.DIRECTION.OUT, "ROAD"))
        if (to.equals(edge.getInVertex().get("name"))) {
          edge.delete();
          return;
        }
    });
  }

  /** Waits for the rebuild an edge-property update forces to have swapped its fresh columns in. */
  private void waitUntilServed(final GraphAnalyticalView view) {
    final long deadline = System.currentTimeMillis() + 30_000;
    while (!view.hasEdgeProperty("ROAD", "w") && System.currentTimeMillis() < deadline)
      Thread.yield();
    assertThat(view.hasEdgeProperty("ROAD", "w"))
        .as("the rebuild forced by an edge property update must restore the columns")
        .isTrue();
  }
}
