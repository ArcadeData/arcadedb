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
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.RID;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.graph.olap.GraphAnalyticalViewRegistry;
import com.arcadedb.query.opencypher.procedures.CypherProcedure;
import com.arcadedb.query.opencypher.procedures.CypherProcedureRegistry;
import com.arcadedb.query.opencypher.traversal.TraversalPath;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * The {@code algo.*} procedures over a Graph Analytical View: adoption of the view, its delta overlay and node-id space, and parity with the answer without a view.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class AlgoGraphAnalyticalViewTest {
  /**
   * Regression tests for issue #6316 - six {@code algo.*} procedures (kShortestPaths, mst, msa, maxFlow,
   * betweenness, degree) never resolved a {@link GraphTraversalProvider} at all: each hand-rolled vertex loading
   * and RID resolution with {@code getAllVertices}/{@code buildRidIndex}, and the two weighted ones re-derived the
   * neighbour/weight pairing {@code GraphData.weightedAdjacency} already owns (issue #6301 fixed that pairing bug
   * independently in each of them).
   * <p>
   * All six now route through {@code loadGraph}, so a Graph Analytical View accelerates them like every other
   * procedure in the package. Each test here pins the CSR-vs-OLTP equivalence the switch has to preserve: build
   * the same graph, run once without a view and once with one, and assert (a) the answer does not change and (b)
   * {@link CommandContext#CSR_ACCELERATED_VAR} was actually set - so a broken conversion that silently kept
   * pinning the OLTP path could not pass by comparing a result against itself.
   */
  @Nested
  class LoadGraphAdoption {
    private Database database;

    @BeforeEach
    void setup() {
      final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-issue-6316-loadgraph-adoption");
      if (factory.exists())
        factory.open().drop();
      database = factory.create();
      database.getSchema().createVertexType("N");
      // Declared property so a Graph Analytical View materialises a columnar weight (issue #6301's setup note):
      // without the column the view falls back to edge records, which would leave the columnar path untested.
      database.getSchema().createEdgeType("E").createProperty("w", Type.DOUBLE);
    }

    @AfterEach
    void teardown() {
      if (database != null) {
        if (database.isTransactionActive())
          database.rollback();
        database.drop();
      }
    }

    private Vertex node(final String name) {
      return database.newVertex("N").set("name", name).save();
    }

    private Vertex named(final String name) {
      return database.query("sql", "SELECT FROM N WHERE name = ?", name).next().getElement().orElseThrow().asVertex();
    }

    private GraphAnalyticalView buildView(final String name) {
      return GraphAnalyticalView.builder(database)
          .withName(name)
          .withVertexTypes("N")
          .withEdgeTypes("E")
          .withEdgeProperties("w")
          .build();
    }

    private BasicCommandContext newContext() {
      final BasicCommandContext context = new BasicCommandContext();
      context.setDatabase(database);
      return context;
    }

    private static List<Result> drain(final Stream<Result> rows) {
      final List<Result> results = new ArrayList<>();
      for (final Iterator<Result> it = rows.iterator(); it.hasNext(); )
        results.add(it.next());
      return results;
    }

    private void assertCSRAccelerated(final CommandContext context) {
      assertThat(context.getVariable(CommandContext.CSR_ACCELERATED_VAR))
          .as("the view must actually back the call, or this comparison pins the OLTP path against itself")
          .isEqualTo(true);
    }

    // ── algo.degree ──────────────────────────────────────────────────────────

    @Test
    void degreeCentralityMatchesBetweenOLTPAndCSR() {
      // Star with one extra edge, asymmetric on purpose: A: out=3 in=0; B: out=1 in=1; C: out=0 in=2; D: out=0 in=1.
      database.transaction(() -> {
        final Vertex a = node("A"), b = node("B"), c = node("C"), d = node("D");
        a.newEdge("E", b, true, new Object[] { "w", 1.0 }).save();
        a.newEdge("E", c, true, new Object[] { "w", 1.0 }).save();
        a.newEdge("E", d, true, new Object[] { "w", 1.0 }).save();
        b.newEdge("E", c, true, new Object[] { "w", 1.0 }).save();
      });

      final Map<RID, long[]> oltp = degreeMap(newContext());
      assertThat(oltp).hasSize(4);

      final GraphAnalyticalView view = buildView("degree-view");
      try {
        final BasicCommandContext csrContext = newContext();
        final Map<RID, long[]> csr = degreeMap(csrContext);
        assertCSRAccelerated(csrContext);
        assertThat(csr.keySet()).isEqualTo(oltp.keySet());
        for (final RID rid : oltp.keySet())
          assertThat(csr.get(rid)).as("in/out/total for " + rid).isEqualTo(oltp.get(rid));
      } finally {
        view.drop();
      }
    }

    private Map<RID, long[]> degreeMap(final CommandContext context) {
      final Map<RID, long[]> result = new HashMap<>();
      for (final Result row : drain(new AlgoDegreeCentrality().execute(new Object[0], null, context))) {
        final RID node = ((RID) row.getProperty("node"));
        result.put(node, new long[] {
            ((Number) row.getProperty("inDegree")).longValue(),
            ((Number) row.getProperty("outDegree")).longValue(),
            ((Number) row.getProperty("degree")).longValue() });
      }
      return result;
    }

    // ── algo.betweenness ────────────────────────────────────────────────────

    @Test
    void betweennessMatchesBetweenOLTPAndCSR() {
      // A path A-B-C-D-E with each hop inserted in both directions, so Brandes' OUT-only walk sees an
      // undirected path: B, C and D sit on shortest paths between other pairs, A and E do not.
      database.transaction(() -> {
        final Vertex a = node("A"), b = node("B"), c = node("C"), d = node("D"), e = node("E");
        for (final Vertex[] hop : new Vertex[][] { { a, b }, { b, c }, { c, d }, { d, e } }) {
          hop[0].newEdge("E", hop[1], true, new Object[] { "w", 1.0 }).save();
          hop[1].newEdge("E", hop[0], true, new Object[] { "w", 1.0 }).save();
        }
      });

      final Map<RID, Double> oltp = betweennessMap(newContext());
      assertThat(oltp).hasSize(5);
      // Sanity: the middle node is on strictly more shortest paths than either endpoint.
      assertThat(oltp.get(named("C").getIdentity())).isGreaterThan(oltp.get(named("A").getIdentity()));

      final GraphAnalyticalView view = buildView("betweenness-view");
      try {
        final BasicCommandContext csrContext = newContext();
        final Map<RID, Double> csr = betweennessMap(csrContext);
        assertCSRAccelerated(csrContext);
        assertThat(csr.keySet()).isEqualTo(oltp.keySet());
        for (final RID rid : oltp.keySet())
          assertThat(csr.get(rid)).as("score for " + rid).isEqualTo(oltp.get(rid));
      } finally {
        view.drop();
      }
    }

    private Map<RID, Double> betweennessMap(final CommandContext context) {
      final Map<RID, Double> result = new HashMap<>();
      for (final Result row : drain(new AlgoBetweenness().execute(new Object[0], null, context)))
        result.put((RID) row.getProperty("node"), ((Number) row.getProperty("score")).doubleValue());
      return result;
    }

    // ── algo.mst ─────────────────────────────────────────────────────────────

    @Test
    void mstMatchesBetweenOLTPAndCSR() {
      // A->B(1), B->C(2), A->C(3), C->D(1), B->D(4): the MST is {A-B, C-D, B-C} at total weight 4, and A-C/B-D
      // are the redundant, more expensive edges Kruskal's must reject.
      database.transaction(() -> {
        final Vertex a = node("A"), b = node("B"), c = node("C"), d = node("D");
        a.newEdge("E", b, true, new Object[] { "w", 1.0 }).save();
        b.newEdge("E", c, true, new Object[] { "w", 2.0 }).save();
        a.newEdge("E", c, true, new Object[] { "w", 3.0 }).save();
        c.newEdge("E", d, true, new Object[] { "w", 1.0 }).save();
        b.newEdge("E", d, true, new Object[] { "w", 4.0 }).save();
      });

      final Set<String> oltp = mstEdgeSet(newContext());
      assertThat(oltp).containsExactlyInAnyOrder("A-B:1.0", "B-C:2.0", "C-D:1.0");

      final GraphAnalyticalView view = buildView("mst-view");
      try {
        final BasicCommandContext csrContext = newContext();
        final Set<String> csr = mstEdgeSet(csrContext);
        assertCSRAccelerated(csrContext);
        assertThat(csr).isEqualTo(oltp);
      } finally {
        view.drop();
      }
    }

    private Set<String> mstEdgeSet(final CommandContext context) {
      final Set<String> result = new HashSet<>();
      for (final Result row : drain(new AlgoMST().execute(new Object[] { null, "w" }, null, context))) {
        final Vertex source = ((RID) row.getProperty("source")).asVertex();
        final Vertex target = ((RID) row.getProperty("target")).asVertex();
        result.add(source.getString("name") + "-" + target.getString("name") + ":" + row.<Number>getProperty("weight").doubleValue());
      }
      return result;
    }

    // ── algo.msa ─────────────────────────────────────────────────────────────

    @Test
    void msaMatchesBetweenOLTPAndCSRAndContractsACycle() {
      // Textbook Chu-Liu/Edmonds shape: R->A(10), R->B(8), A->B(1), B->A(1), B->C(9). Cheapest incoming edges
      // form a cycle A<->B (B->A(1) and A->B(1)); R must break in through the cheapest adjusted entry, R->B at
      // adjusted cost 8-1=7 versus R->A at 10-1=9. Expected MSA: R->B(8), B->A(1), B->C(9), total 18.
      database.transaction(() -> {
        final Vertex r = node("R"), a = node("A"), b = node("B"), c = node("C");
        r.newEdge("E", a, true, new Object[] { "w", 10.0 }).save();
        r.newEdge("E", b, true, new Object[] { "w", 8.0 }).save();
        a.newEdge("E", b, true, new Object[] { "w", 1.0 }).save();
        b.newEdge("E", a, true, new Object[] { "w", 1.0 }).save();
        b.newEdge("E", c, true, new Object[] { "w", 9.0 }).save();
      });

      final Set<String> oltp = msaEdgeSet(newContext());
      assertThat(oltp).containsExactlyInAnyOrder("R-B:8.0", "B-A:1.0", "B-C:9.0");

      final GraphAnalyticalView view = buildView("msa-view");
      try {
        final BasicCommandContext csrContext = newContext();
        final Set<String> csr = msaEdgeSet(csrContext);
        assertCSRAccelerated(csrContext);
        assertThat(csr).isEqualTo(oltp);
      } finally {
        view.drop();
      }
    }

    private Set<String> msaEdgeSet(final CommandContext context) {
      final Set<String> result = new HashSet<>();
      for (final Result row : drain(new AlgoMinSpanningArborescence().execute(new Object[] { named("R"), null, "w" }, null, context))) {
        final Vertex source = ((RID) row.getProperty("source")).asVertex();
        final Vertex target = ((RID) row.getProperty("target")).asVertex();
        result.add(source.getString("name") + "-" + target.getString("name") + ":" + row.<Number>getProperty("weight").doubleValue());
      }
      return result;
    }

    // ── algo.maxFlow ─────────────────────────────────────────────────────────

    @Test
    void maxFlowMatchesBetweenOLTPAndCSR() {
      // Classic small flow network: S->A(3), S->B(2), A->B(1), A->T(2), B->T(3).
      database.transaction(() -> {
        final Vertex s = node("S"), a = node("A"), b = node("B"), t = node("T");
        s.newEdge("E", a, true, new Object[] { "w", 3.0 }).save();
        s.newEdge("E", b, true, new Object[] { "w", 2.0 }).save();
        a.newEdge("E", b, true, new Object[] { "w", 1.0 }).save();
        a.newEdge("E", t, true, new Object[] { "w", 2.0 }).save();
        b.newEdge("E", t, true, new Object[] { "w", 3.0 }).save();
      });

      final double oltp = maxFlowValue(newContext());
      assertThat(oltp).isGreaterThan(0.0);

      final GraphAnalyticalView view = buildView("maxflow-view");
      try {
        final BasicCommandContext csrContext = newContext();
        final double csr = maxFlowValue(csrContext);
        assertCSRAccelerated(csrContext);
        assertThat(csr).isEqualTo(oltp);
      } finally {
        view.drop();
      }
    }

    private double maxFlowValue(final CommandContext context) {
      final List<Result> rows = drain(new AlgoMaxFlow().execute(new Object[] { named("S"), named("T"), null, "w" }, null, context));
      assertThat(rows).hasSize(1);
      return rows.getFirst().<Number>getProperty("maxFlow").doubleValue();
    }

    // ── algo.kShortestPaths ──────────────────────────────────────────────────

    @Test
    void kShortestPathsMatchesBetweenOLTPAndCSR() {
      // A->B(1), A->C(4), B->C(2), B->D(5), C->D(1). The two cheapest A-D paths are A-B-C-D (4) and A-C-D (5).
      database.transaction(() -> {
        final Vertex a = node("A"), b = node("B"), c = node("C"), d = node("D");
        a.newEdge("E", b, true, new Object[] { "w", 1.0 }).save();
        a.newEdge("E", c, true, new Object[] { "w", 4.0 }).save();
        b.newEdge("E", c, true, new Object[] { "w", 2.0 }).save();
        b.newEdge("E", d, true, new Object[] { "w", 5.0 }).save();
        c.newEdge("E", d, true, new Object[] { "w", 1.0 }).save();
      });

      final List<String> oltp = kShortestPathsSummary(newContext());
      assertThat(oltp).containsExactly("A,B,C,D:4.0", "A,C,D:5.0");

      final GraphAnalyticalView view = buildView("kshortest-view");
      try {
        final BasicCommandContext csrContext = newContext();
        final List<String> csr = kShortestPathsSummary(csrContext);
        assertCSRAccelerated(csrContext);
        assertThat(csr).isEqualTo(oltp);
      } finally {
        view.drop();
      }
    }

    @SuppressWarnings("unchecked")
    private List<String> kShortestPathsSummary(final CommandContext context) {
      final List<Result> rows = drain(new AlgoKShortestPaths().execute(
          new Object[] { named("A"), named("D"), 2, null, "w" }, null, context));
      return rows.stream()
          .map(row -> {
            final List<Vertex> nodes = ((TraversalPath) row.getProperty("path")).getVertices();
            final String names = nodes.stream().map(v -> v.getString("name")).collect(Collectors.joining(","));
            return names + ":" + row.<Number>getProperty("weight").doubleValue();
          })
          .sorted(Comparator.naturalOrder())
          .collect(Collectors.toList());
    }

    // ── parallel edges & self-loops ──────────────────────────────────────────

    /**
     * PR #6714 review round 13's second finding: every equivalence test above builds a graph with no parallel
     * edges or self-loops, so an OLTP/CSR mismatch specific to either shape - {@code GraphData.degrees} summing
     * a provider's per-type buffer versus {@code Vertex.countEdges}, or {@code weightedAdjacency}'s neighbour/
     * weight pairing - could pass every test above and still disagree once one occurred. These two tests do not
     * hand-derive the OLTP side's expected numbers the way the tests above do (self-loop counting conventions in
     * particular are not this test's concern, and the algorithms' own correctness is pinned elsewhere - e.g.
     * issue #6301 for weight pairing); they only assert OLTP and CSR agree with each other, which is exactly what
     * a shared-plumbing regression specific to either shape would break.
     */
    @Test
    void degreeCentralityMatchesBetweenOLTPAndCSRWithAParallelEdgeAndASelfLoop() {
      database.transaction(() -> {
        final Vertex a = node("A"), b = node("B"), c = node("C");
        a.newEdge("E", b, true, new Object[] { "w", 1.0 }).save();
        a.newEdge("E", b, true, new Object[] { "w", 1.0 }).save(); // parallel edge, same pair as above
        a.newEdge("E", a, true, new Object[] { "w", 1.0 }).save(); // self-loop
        b.newEdge("E", c, true, new Object[] { "w", 1.0 }).save();
      });

      final Map<RID, long[]> oltp = degreeMap(newContext());
      assertThat(oltp).hasSize(3);

      final GraphAnalyticalView view = buildView("degree-parallel-selfloop-view");
      try {
        final BasicCommandContext csrContext = newContext();
        final Map<RID, long[]> csr = degreeMap(csrContext);
        assertCSRAccelerated(csrContext);
        assertThat(csr.keySet()).isEqualTo(oltp.keySet());
        for (final RID rid : oltp.keySet())
          assertThat(csr.get(rid)).as("in/out/total for " + rid).isEqualTo(oltp.get(rid));
      } finally {
        view.drop();
      }
    }

    @Test
    void mstMatchesBetweenOLTPAndCSRWithAParallelEdgeAndASelfLoop() {
      // A-B twice at different weights (the cheaper one must win) plus a self-loop on A, which Kruskal's
      // union-find already excludes on both paths (a self-loop's two endpoints share a root from the start, so
      // it is never added, the same way #6300's own dedicated tests never needed to special-case one).
      database.transaction(() -> {
        final Vertex a = node("A"), b = node("B"), c = node("C");
        a.newEdge("E", b, true, new Object[] { "w", 5.0 }).save();
        a.newEdge("E", b, true, new Object[] { "w", 1.0 }).save(); // cheaper parallel edge, must be the one kept
        a.newEdge("E", a, true, new Object[] { "w", 0.5 }).save(); // self-loop, must not appear in the MST
        b.newEdge("E", c, true, new Object[] { "w", 2.0 }).save();
      });

      final Set<String> oltp = mstEdgeSet(newContext());
      assertThat(oltp).as("the self-loop must not appear and the cheaper parallel edge must win")
          .containsExactlyInAnyOrder("A-B:1.0", "B-C:2.0");

      final GraphAnalyticalView view = buildView("mst-parallel-selfloop-view");
      try {
        final BasicCommandContext csrContext = newContext();
        final Set<String> csr = mstEdgeSet(csrContext);
        assertCSRAccelerated(csrContext);
        assertThat(csr).isEqualTo(oltp);
      } finally {
        view.drop();
      }
    }
  }

  /**
   * Regression tests for issue #6791: {@code algo.dijkstra.singleSource} was the one weighted procedure that still
   * abandoned the CSR path and fell all the way back to the edge records on every commit against a
   * {@code SYNCHRONOUS} Graph Analytical View - the state such a view is in after every single commit - instead
   * of resolving the delta overlay the way {@code algo.mst}, {@code algo.steinerTree}, {@code astar()} and
   * {@code bellmanFord()} already do through {@link com.arcadedb.graph.GraphTraversalProvider#edgeWeightsOf}
   * (issue #6315).
   * <p>
   * {@link com.arcadedb.graph.olap.GraphAlgorithms#dijkstraSingleSource} now keeps popping nodes off its CSR-based
   * heap while an overlay is active, resolving each popped node's neighbours and weights through the provider
   * instead of indexing the raw arrays - and sizes its result against the overlay's own id-space upper bound
   * rather than the base node mapping, since an added vertex's dense id sits above it (issue #6792).
   */
  @Nested
  class DijkstraSingleSourceOverlay {
    private Database database;

    @BeforeEach
    void setup() {
      final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-issue-6791-dijkstra-overlay");
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
      }
    }

    @Test
    void staysCsrAcceleratedAcrossACommitAndAnswersForAddedAndDeletedEdges() {
      // A -1-> B -2-> C, plus a redundant direct A -10-> C that will be deleted.
      database.transaction(() -> {
        final MutableVertex a = database.newVertex("N").set("name", "A").save();
        final MutableVertex b = database.newVertex("N").set("name", "B").save();
        final MutableVertex c = database.newVertex("N").set("name", "C").save();
        a.newEdge("ROAD", b, true, new Object[] { "w", 1.0 }).save();
        b.newEdge("ROAD", c, true, new Object[] { "w", 2.0 }).save();
        a.newEdge("ROAD", c, true, new Object[] { "w", 10.0 }).save();
      });

      final GraphAnalyticalView view = syncView("dijkstra-overlay-basic");
      try {
        // Delete the redundant direct edge, add a new vertex E and an edge from C to it - both live only in the
        // overlay: the redundant edge's column slot still exists but must not be walked, and E's dense id sits
        // above the base mapping entirely.
        database.transaction(() -> {
          for (final Edge edge : vertex("A").getEdges(Vertex.DIRECTION.OUT, "ROAD"))
            if ("C".equals(edge.getInVertex().get("name")))
              edge.delete();
          final MutableVertex e = database.newVertex("N").set("name", "E").save();
          vertex("C").newEdge("ROAD", e, true, new Object[] { "w", 5.0 }).save();
        });
        assertThat(view.hasPendingChanges()).as("every commit leaves a SYNCHRONOUS view with an active overlay").isTrue();

        final BasicCommandContext context = new BasicCommandContext();
        context.setDatabase(database);
        final Map<String, Double> costs = new HashMap<>();
        for (final Result row : collect(new AlgoDijkstraSingleSource().execute(
            new Object[] { vertex("A"), "ROAD", "w", "OUT" }, null, context))) {
          final Vertex node = ((RID) row.getProperty("node")).asVertex();
          costs.put((String) node.get("name"), ((Number) row.getProperty("cost")).doubleValue());
        }

        assertThat(context.getVariable(CommandContext.CSR_ACCELERATED_VAR))
            .as("the overlay must not send this call back to the edge records")
            .isEqualTo(true);
        // B at 1, C via A-B-C at 3 (the direct 10-weight edge was deleted), E via A-B-C-E at 8.
        assertThat(costs).containsExactlyInAnyOrderEntriesOf(Map.of("B", 1.0, "C", 3.0, "E", 8.0));
      } finally {
        view.drop();
      }
    }

    @Test
    void fallsBackToOltpWhenTheOverlayCannotResolveAnAmbiguousParallelDeletion() {
      // Three parallel A->B edges of distinct weight.
      database.transaction(() -> {
        final MutableVertex a = database.newVertex("N").set("name", "A").save();
        final MutableVertex b = database.newVertex("N").set("name", "B").save();
        a.newEdge("ROAD", b, true, new Object[] { "w", 1.0 }).save();
        a.newEdge("ROAD", b, true, new Object[] { "w", 2.0 }).save();
        a.newEdge("ROAD", b, true, new Object[] { "w", 3.0 }).save();
      });

      final GraphAnalyticalView view = syncView("dijkstra-overlay-ambiguous");
      try {
        // Delete the 2.0 edge and add a 4.0 one between the same pair: which of the surviving column slots
        // belongs to which remaining edge is not recoverable from a per-pair deletion count (issue #6315).
        database.transaction(() -> {
          for (final Edge edge : vertex("A").getEdges(Vertex.DIRECTION.OUT, "ROAD"))
            if (Double.valueOf(2.0).equals(edge.get("w")))
              edge.delete();
          vertex("A").newEdge("ROAD", vertex("B"), true, new Object[] { "w", 4.0 }).save();
        });

        final BasicCommandContext context = new BasicCommandContext();
        context.setDatabase(database);
        final List<Result> rows = collect(new AlgoDijkstraSingleSource().execute(
            new Object[] { vertex("A"), "ROAD", "w", "OUT" }, null, context));

        assertThat(context.getVariable(CommandContext.CSR_ACCELERATED_VAR))
            .as("an unresolvable node must send the whole call back to the edge records, not half of it")
            .isNotEqualTo(true);
        assertThat(rows).hasSize(1);
        // The cheapest surviving edge (1.0) must win, not a plausible wrong weight from the ambiguous merge.
        assertThat(((Number) rows.getFirst().getProperty("cost")).doubleValue()).isEqualTo(1.0);
      } finally {
        view.drop();
      }
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

    private Vertex vertex(final String name) {
      return database.query("sql", "SELECT FROM N WHERE name = ?", name).next().getRecord().get().asVertex();
    }

    private static List<Result> collect(final Stream<Result> rows) {
      final List<Result> collected = new ArrayList<>();
      rows.forEach(collected::add);
      return collected;
    }
  }

  /**
   * The other half of issue #6792: a vertex added to a {@code SYNCHRONOUS} Graph Analytical View takes an ID above
   * the base node mapping, so the view's node count outgrows the arrays the {@code GraphAlgorithms} kernels size
   * from that mapping. Every CSR-accelerated procedure then threw {@code ArrayIndexOutOfBoundsException} on the
   * added vertex's ID - one added vertex was enough, no deletion involved.
   */
  @Nested
  class AddedVertexIdSpace {
    private Database            database;
    private GraphAnalyticalView view;
    private RID                 a;
    private RID                 b;
    private RID                 c;
    private RID                 fresh;

    @BeforeEach
    void setup() {
      final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-issue-6792-added-vertex");
      if (factory.exists())
        factory.open().drop();
      database = factory.create();
      database.getSchema().createVertexType("N");
      database.getSchema().createEdgeType("E");

      final MutableVertex[] created = new MutableVertex[3];
      database.transaction(() -> {
        created[0] = database.newVertex("N").set("name", "A").save();
        created[1] = database.newVertex("N").set("name", "B").save();
        created[2] = database.newVertex("N").set("name", "C").save();
        created[0].newEdge("E", created[1]).save();
      });
      a = created[0].getIdentity();
      b = created[1].getIdentity();
      c = created[2].getIdentity();

      view = GraphAnalyticalView.builder(database)
          .withName("issue-6792-added-vertex")
          .withVertexTypes("N")
          .withEdgeTypes("E")
          .withUpdateMode(GraphAnalyticalView.UpdateMode.SYNCHRONOUS)
          .withCompactionThreshold(Integer.MAX_VALUE)
          .build();

      final MutableVertex[] added = new MutableVertex[1];
      database.transaction(() -> {
        added[0] = database.newVertex("N").set("name", "FRESH").save();
        c.asVertex().modify().newEdge("E", added[0]).save();
      });
      fresh = added[0].getIdentity();
    }

    @AfterEach
    void teardown() {
      if (view != null)
        view.drop();
      if (database != null) {
        if (database.isTransactionActive())
          database.rollback();
        database.drop();
      }
    }

    @Test
    void theAddedVertexIsNumberedAboveTheBaseMapping() {
      assertThat(view.hasPendingChanges()).isTrue();
      assertThat(view.getNodeCount()).isEqualTo(4);
      assertThat(view.getNodeIdUpperBound()).isEqualTo(4);
      // No holes here, so the ID space is compact - and still one wider than the base mapping the CSR kernels
      // size their result arrays from.
      assertThat(view.getNodeId(fresh)).isEqualTo(view.getNodeMapping().size());
      assertThat(view.isNodeLive(view.getNodeId(fresh))).isTrue();
    }

    @Test
    void everyWholeGraphProcedureAnswersForTheAddedVertex() {
      final String[][] procedures = {
          { "algo.wcc()", "node" },
          { "algo.pagerank()", "node" },
          { "algo.articlerank()", "node" },
          { "algo.labelpropagation()", "node" },
          { "algo.localClusteringCoefficient()", "node" },
          { "algo.degree()", "node" },
          { "algo.closeness()", "node" },
          { "algo.harmonic(null, 'BOTH', true)", "node" },
          { "algo.eigenvector(null, 'BOTH')", "node" },
          { "algo.betweenness()", "node" },
          { "algo.kcore()", "node" },
          { "algo.scc()", "node" },
          { "algo.triangleCount()", "node" },
          { "algo.katz()", "nodeId" },
      };

      for (final String[] procedure : procedures) {
        final String query = "CALL " + procedure[0] + " YIELD " + procedure[1] + " RETURN " + procedure[1];
        final Set<RID> nodes = new HashSet<>();
        final ResultSet rs = database.query("opencypher", query);
        while (rs.hasNext())
          nodes.add((RID) rs.next().getProperty(procedure[1]));
        assertThat(nodes).as(query).containsExactlyInAnyOrder(a, b, c, fresh);
      }
    }

    @Test
    void wccJoinsTheAddedVertexToTheComponentItsEdgeReaches() {
      final ResultSet rs = database.query("opencypher", "CALL algo.wcc() YIELD node, componentId RETURN node, componentId");
      final Map<RID, Object> components = new HashMap<>();
      while (rs.hasNext()) {
        final Result row = rs.next();
        components.put(row.getProperty("node"), row.getProperty("componentId"));
      }
      assertThat(components).hasSize(4);
      assertThat(components.get(a)).isEqualTo(components.get(b));
      // The edge added to the overlay is what joins C to FRESH; a kernel reading only the base CSR would put them
      // in two components of their own.
      assertThat(components.get(c)).isEqualTo(components.get(fresh));
      assertThat(components.get(a)).isNotEqualTo(components.get(c));
    }

    @Test
    void pageRankInDirectionFollowsIncomingEdgesWhileTheViewHasPendingChanges() {
      assertThat(view.hasPendingChanges()).isTrue();

      final Map<RID, Double> scores = new HashMap<>();
      try (final ResultSet rs = database.query("opencypher",
          "CALL algo.pagerank({direction: 'IN', maxIterations: 20, tolerance: 0.0}) YIELD node, score RETURN node, score")) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          scores.put(row.getProperty("node"), ((Number) row.getProperty("score")).doubleValue());
        }
      }

      assertThat(scores).hasSize(4);
      // A -> B and C -> FRESH. Reversing the traversal must therefore rank A above B and C above FRESH.
      assertThat(scores.get(a)).isGreaterThan(scores.get(b));
      assertThat(scores.get(c)).isGreaterThan(scores.get(fresh));
    }
  }

  /**
   * Regression coverage for issue #6792: a Graph Analytical View serving a delta overlay reports a live node count
   * smaller than the exclusive bound of its own node ID space, because the overlay keeps the slot of every deleted
   * node and allocates the ID of every added one above the base mapping.
   * <p>
   * The fixture builds exactly that shape - one base vertex deleted, one vertex added above the base mapping, and
   * an edge reaching it - and asserts that no {@code algo.*} procedure either drops the added vertex or trips over
   * the ID that names it.
   */
  @Nested
  class GraphDataNodeIdBound {
    private Database           database;
    private GraphAnalyticalView view;
    private RID                a;
    private RID                b;
    private RID                fresh;
    private RID                deleted;

    @BeforeEach
    void setup() {
      final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-issue-6792-node-id-bound");
      if (factory.exists())
        factory.open().drop();
      database = factory.create();
      database.getSchema().createVertexType("N");
      database.getSchema().createEdgeType("E");

      // SPARE first so that it is the one the base mapping is most likely to number lowest; which vertex actually
      // gets which ID is read back below rather than assumed.
      final MutableVertex[] created = new MutableVertex[3];
      database.transaction(() -> {
        created[0] = database.newVertex("N").set("name", "SPARE").save();
        created[1] = database.newVertex("N").set("name", "A").save();
        created[2] = database.newVertex("N").set("name", "B").save();
        created[1].newEdge("E", created[2]).save();
      });

      view = GraphAnalyticalView.builder(database)
          .withName("issue-6792-node-id-bound")
          .withVertexTypes("N")
          .withEdgeTypes("E")
          .withUpdateMode(GraphAnalyticalView.UpdateMode.SYNCHRONOUS)
          .withCompactionThreshold(Integer.MAX_VALUE)
          .build();

      a = created[1].getIdentity();
      b = created[2].getIdentity();
      deleted = created[0].getIdentity();

      final MutableVertex[] added = new MutableVertex[1];
      database.transaction(() -> {
        deleted.asVertex().delete();
        added[0] = database.newVertex("N").set("name", "FRESH").save();
        b.asVertex().modify().newEdge("E", added[0]).save();
      });
      fresh = added[0].getIdentity();
    }

    @AfterEach
    void teardown() {
      if (view != null)
        view.drop();
      if (database != null) {
        if (database.isTransactionActive())
          database.rollback();
        database.drop();
      }
    }

    @Test
    void theViewSeparatesItsLiveNodeCountFromItsNodeIdSpace() {
      assertThat(view.hasPendingChanges()).isTrue();
      assertThat(view.getNodeCount()).isEqualTo(3);
      assertThat(view.getNodeIdUpperBound()).isEqualTo(4);

      final int freshId = view.getNodeId(fresh);
      // The whole of issue #6792 in one assertion: a live node whose ID is not smaller than the live count.
      assertThat(freshId).isGreaterThanOrEqualTo(view.getNodeCount());
      assertThat(freshId).isLessThan(view.getNodeIdUpperBound());

      assertThat(view.isNodeLive(freshId)).isTrue();
      assertThat(view.isNodeLive(view.getNodeId(a))).isTrue();
      assertThat(view.isNodeLive(view.getNodeId(b))).isTrue();
      // The deleted base node keeps its slot, and its RID is still in the base mapping - which is exactly why
      // liveness has to be asked of the view rather than inferred from getRID() answering non-null.
      assertThat(view.isNodeLive(deletedBaseId())).isFalse();

      assertThat(view.getNeighborIds(view.getNodeId(b), Vertex.DIRECTION.OUT, "E")).contains(freshId);
    }

    @Test
    void graphDataHandsTheProceduresACompactNodeIdSpace() {
      final BasicCommandContext context = new BasicCommandContext();
      context.setDatabase(database);
      final ProbeAPSP procedure = new ProbeAPSP();
      final AbstractAlgoProcedure.GraphData graph = procedure.loadGraph(database, context);

      // Every node-indexed array a procedure allocates is nodeCount long, so nodeCount has to be both the size of
      // the ID space and the number of live nodes in it. The renumbering is what makes those the same number.
      assertThat(graph.nodeCount).isEqualTo(view.getNodeCount());
      assertThat(graph.isCSRBacked()).isTrue();

      final Set<RID> resolved = new HashSet<>();
      for (int i = 0; i < graph.nodeCount; i++) {
        assertThat(graph.getRID(i)).isNotNull();
        resolved.add(graph.getRID(i));
      }
      assertThat(resolved).containsExactlyInAnyOrder(a, b, fresh);

      final int denseB = graph.indexOf(b);
      final int denseFresh = graph.indexOf(fresh);
      assertThat(denseB).isBetween(0, graph.nodeCount - 1);
      assertThat(denseFresh).isBetween(0, graph.nodeCount - 1);
      assertThat(graph.adjacency(Vertex.DIRECTION.OUT)[denseB]).contains(denseFresh);
      assertThat(graph.degrees(Vertex.DIRECTION.OUT, "E")[denseB]).isEqualTo(1);
    }

    @Test
    void apspReachesTheAddedVertexThroughTheSurvivingOne() {
      final BasicCommandContext context = new BasicCommandContext();
      context.setDatabase(database);
      final ProbeAPSP procedure = new ProbeAPSP();
      final List<Result> rows = drain(procedure.execute(new Object[0], null, context));

      assertThat(context.getVariable(CommandContext.CSR_ACCELERATED_VAR)).isEqualTo(true);
      assertThat(rows).anySatisfy(row -> {
        assertThat((RID) row.getProperty("source")).isEqualTo(b);
        assertThat((RID) row.getProperty("target")).isEqualTo(fresh);
        assertThat(((Number) row.getProperty("distance")).doubleValue()).isEqualTo(1.0);
      });
      assertThat(rows).anySatisfy(row -> {
        assertThat((RID) row.getProperty("source")).isEqualTo(a);
        assertThat((RID) row.getProperty("target")).isEqualTo(fresh);
        assertThat(((Number) row.getProperty("distance")).doubleValue()).isEqualTo(2.0);
      });
      assertThat(rows).noneSatisfy(row -> assertThat((RID) row.getProperty("source")).isEqualTo(deleted));
      assertThat(rows).noneSatisfy(row -> assertThat((RID) row.getProperty("target")).isEqualTo(deleted));
    }

    /**
     * The sweep issue #6792 asks for: every whole-graph procedure that emits one row per node has to emit one for
     * each of the three live vertices and none for the deleted one. Before the fix the CSR-accelerated ones threw
     * {@code ArrayIndexOutOfBoundsException} on the added vertex's ID, and the ones reading the graph through
     * {@code GraphData} silently left it out of the answer.
     */
    @Test
    void everyWholeGraphProcedureSeesExactlyTheLiveVertices() {
      final String[][] procedures = {
          { "algo.wcc()", "node" },
          { "algo.pagerank()", "node" },
          { "algo.articlerank()", "node" },
          { "algo.labelpropagation()", "node" },
          { "algo.localClusteringCoefficient()", "node" },
          { "algo.degree()", "node" },
          { "algo.closeness()", "node" },
          { "algo.harmonic(null, 'BOTH', true)", "node" },
          { "algo.eigenvector(null, 'BOTH')", "node" },
          { "algo.betweenness()", "node" },
          { "algo.kcore()", "node" },
          { "algo.scc()", "node" },
          { "algo.triangleCount()", "node" },
          { "algo.katz()", "nodeId" },
      };

      for (final String[] procedure : procedures) {
        final String query = "CALL " + procedure[0] + " YIELD " + procedure[1] + " RETURN " + procedure[1];
        final Set<RID> nodes = new HashSet<>();
        final ResultSet rs = database.query("opencypher", query);
        while (rs.hasNext())
          nodes.add((RID) rs.next().getProperty(procedure[1]));
        assertThat(nodes).as(query).containsExactlyInAnyOrder(a, b, fresh);
      }
    }

    @Test
    void traversalProceduresReachTheAddedVertex() {
      final ResultSet bfs = database.query("opencypher",
          "MATCH (s:N {name: 'A'}) CALL algo.bfs(s, 'E', 'OUT') YIELD node, depth RETURN node, depth");
      final List<Result> hops = new ArrayList<>();
      while (bfs.hasNext())
        hops.add(bfs.next());
      assertThat(hops).anySatisfy(row -> {
        assertThat((RID) row.getProperty("node")).isEqualTo(fresh);
        assertThat(((Number) row.getProperty("depth")).intValue()).isEqualTo(2);
      });

      final ResultSet dijkstra = database.query("opencypher",
          "MATCH (s:N {name: 'A'}) CALL algo.dijkstra.singleSource(s, 'E', 'w', 'OUT') YIELD node, cost RETURN node, cost");
      final Set<RID> reached = new HashSet<>();
      while (dijkstra.hasNext())
        reached.add((RID) dijkstra.next().getProperty("node"));
      assertThat(reached).contains(fresh);
      assertThat(reached).doesNotContain(deleted);
    }

    private int deletedBaseId() {
      // The deleted vertex's RID no longer resolves through the overlay, so its base ID is the one below the base
      // mapping's size that neither survivor holds.
      final Set<Integer> live = Set.of(view.getNodeId(a), view.getNodeId(b));
      for (int id = 0; id < view.getNodeMapping().size(); id++)
        if (!live.contains(id))
          return id;
      throw new IllegalStateException("no deleted base id");
    }

    private static List<Result> drain(final Stream<Result> rows) {
      final List<Result> results = new ArrayList<>();
      for (final Iterator<Result> it = rows.iterator(); it.hasNext(); )
        results.add(it.next());
      return results;
    }

    private static final class ProbeAPSP extends AlgoAPSP {
      private GraphData loadGraph(final Database database, final CommandContext context) {
        return loadGraph(database, null, null, context);
      }
    }
  }

  /**
   * Issue #9979 (#9829: #9767, #9791): the answer of an {@code algo.*} procedure is a function of its arguments and of the
   * data, never of whether a Graph Analytical View happens to cover the types. Each procedure that has both an accelerated
   * and an OLTP path is run with and without a view, for every value of every option it accepts, and the two answers must
   * agree. {@code algo.labelpropagation} dropped {@code direction} on the accelerated path, {@code algo.pagerank} dropped
   * {@code tolerance}.
   */
  @Nested
  class ViewParity extends TestHelper {
    private static final String   VIEW       = "parity";
    private static final String[] DIRECTIONS = { "OUT", "IN", "BOTH" };

    /**
     * The graph of #9767: 7 vertices, edges 0->1, 0->2, 0->3, 4->5, 6->4. Without a view the three directions answer three
     * different partitions, and the view must not turn OUT and IN into BOTH. Ties are broken on {@code k}: without a
     * tie-break property each path breaks them on its own internal node order, which the two need not share.
     */
    @Test
    void labelPropagationHonoursDirectionUnderAView() throws Exception {
      buildGraph(7, new int[][] { { 0, 1 }, { 0, 2 }, { 0, 3 }, { 4, 5 }, { 6, 4 } });

      final Map<String, Set<Set<Integer>>> expected = Map.of(//
          "BOTH", Set.of(Set.of(0), Set.of(1, 2, 3), Set.of(4), Set.of(5, 6)),//
          "OUT", Set.of(Set.of(0, 1), Set.of(2), Set.of(3), Set.of(4, 5, 6)),//
          "IN", Set.of(Set.of(0, 1, 2, 3), Set.of(4, 5, 6)));

      final Map<String, Set<Set<Integer>>> withoutView = new HashMap<>();
      for (final String direction : DIRECTIONS)
        withoutView.put(direction, partition(run("algo.labelpropagation", false, Map.of("direction", direction, "tieBreakProperty", "k"))));
      assertThat(withoutView).isEqualTo(expected);

      createView();
      for (final String direction : DIRECTIONS)
        assertThat(partition(run("algo.labelpropagation", true, Map.of("direction", direction, "tieBreakProperty", "k"))))
            .as("direction %s under a view", direction).isEqualTo(expected.get(direction));
    }

    /**
     * The graph of #9791: with {@code tolerance: 0.5} the OLTP path stops after one iteration, the accelerated path used to
     * run all of them whatever the tolerance.
     */
    @Test
    void pageRankHonoursToleranceUnderAView() throws Exception {
      buildGraph(8, new int[][] { { 0, 1 }, { 1, 2 }, { 2, 0 }, { 1, 3 }, { 3, 4 }, { 4, 5 }, { 5, 3 }, { 0, 5 }, { 2, 6 }, { 6, 1 }, { 3, 6 },
          { 6, 7 }, { 0, 7 } });

      final List<Map<String, Object>> configs = new ArrayList<>();
      for (final String direction : DIRECTIONS)
        for (final double tolerance : new double[] { 0.9, 0.5, 0.01, 0.0001, 0.0 })
          configs.add(Map.of("direction", direction, "tolerance", tolerance));
      configs.add(Map.of("direction", "OUT", "tolerance", 0.0, "maxIterations", 3));
      configs.add(Map.of("direction", "IN", "dampingFactor", 0.5, "tolerance", 0.001));
      configs.add(Map.of());

      final List<Map<Integer, Double>> withoutView = new ArrayList<>();
      for (final Map<String, Object> config : configs)
        withoutView.add(scores(run("algo.pagerank", false, config)));

      // the reporter's numbers: one iteration, three nodes tied for last
      final Map<Integer, Double> oneIteration = withoutView.getFirst();
      assertThat(oneIteration.get(0)).isCloseTo(0.08516, within(1e-5));
      assertThat(oneIteration.get(4)).isCloseTo(0.08516, within(1e-5));
      assertThat(oneIteration.get(3)).isCloseTo(0.19141, within(1e-5));

      createView();
      for (int i = 0; i < configs.size(); i++)
        assertScoresEqual(scores(run("algo.pagerank", true, configs.get(i))), withoutView.get(i), configs.get(i));
    }

    /** The in-package control of #9791: ArticleRank already forwarded its tolerance, and keeps doing so. */
    @Test
    void articleRankAgreesUnderAView() throws Exception {
      buildGraph(8, new int[][] { { 0, 1 }, { 1, 2 }, { 2, 0 }, { 1, 3 }, { 3, 4 }, { 4, 5 }, { 5, 3 }, { 0, 5 }, { 2, 6 }, { 6, 1 }, { 3, 6 },
          { 6, 7 }, { 0, 7 } });

      final List<Map<String, Object>> configs = List.of(Map.of("tolerance", 0.5), Map.of("tolerance", 0.0001),
          Map.of("tolerance", 0.0, "maxIterations", 4), Map.of());
      final List<Map<Integer, Double>> withoutView = new ArrayList<>();
      for (final Map<String, Object> config : configs)
        withoutView.add(scores(run("algo.articlerank", false, config)));

      createView();
      for (int i = 0; i < configs.size(); i++)
        assertScoresEqual(scores(run("algo.articlerank", true, configs.get(i))), withoutView.get(i), configs.get(i));
    }

    private static void assertScoresEqual(final Map<Integer, Double> actual, final Map<Integer, Double> expected, final Map<String, Object> config) {
      assertThat(actual.keySet()).as("%s", config).isEqualTo(expected.keySet());
      for (final Map.Entry<Integer, Double> entry : expected.entrySet())
        assertThat(actual.get(entry.getKey())).as("node %d, config %s", entry.getKey(), config).isCloseTo(entry.getValue(), within(1e-9));
    }

    private void buildGraph(final int vertices, final int[][] edges) {
      database.command("sql", "CREATE VERTEX TYPE N");
      database.command("sql", "CREATE PROPERTY N.k INTEGER");
      database.command("sql", "CREATE EDGE TYPE R");
      database.transaction(() -> {
        final MutableVertex[] v = new MutableVertex[vertices];
        for (int i = 0; i < vertices; i++)
          v[i] = database.newVertex("N").set("k", i).save();
        for (final int[] edge : edges)
          v[edge[0]].newEdge("R", v[edge[1]]);
      });
    }

    private void createView() throws Exception {
      database.command("sql", "CREATE GRAPH ANALYTICAL VIEW " + VIEW + " VERTEX TYPES (N) EDGE TYPES (R)");
      final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, VIEW);
      assertThat(view.awaitReady(60, TimeUnit.SECONDS)).isTrue();
    }

    /** Runs the procedure, asserting it took the path the test expects, and returns its rows keyed by the vertex's k. */
    private Map<Integer, Object> run(final String name, final boolean expectAccelerated, final Map<String, Object> config) {
      final CypherProcedure procedure = CypherProcedureRegistry.get(name);
      final BasicCommandContext context = new BasicCommandContext();
      context.setDatabase(database);
      final String valueField = procedure.getYieldFields().get(1);
      final Map<Integer, Object> byK = new TreeMap<>();
      try (final Stream<Result> rows = procedure.execute(new Object[] { config }, null, context)) {
        rows.forEach(row -> byK.put(database.lookupByRID(row.<RID>getProperty("node"), true).asVertex().getInteger("k"),
            row.getProperty(valueField)));
      }
      assertThat(Boolean.TRUE.equals(context.getVariable(CommandContext.CSR_ACCELERATED_VAR))).as("%s %s accelerated", name, config)
          .isEqualTo(expectAccelerated);
      return byK;
    }

    private static Map<Integer, Double> scores(final Map<Integer, Object> rows) {
      final Map<Integer, Double> scores = new TreeMap<>();
      rows.forEach((k, v) -> scores.put(k, ((Number) v).doubleValue()));
      return scores;
    }

    /** Community ids differ between the two paths (each numbers nodes its own way): the partition is what must agree. */
    private static Set<Set<Integer>> partition(final Map<Integer, Object> rows) {
      final Map<Object, Set<Integer>> communities = new HashMap<>();
      rows.forEach((k, community) -> communities.computeIfAbsent(community, c -> new TreeSet<>()).add(k));
      return communities.values().stream().map(Set::copyOf).collect(Collectors.toSet());
    }
  }
}
