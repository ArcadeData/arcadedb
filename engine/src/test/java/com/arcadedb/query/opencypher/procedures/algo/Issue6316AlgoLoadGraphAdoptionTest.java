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
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.schema.Type;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

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
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6316AlgoLoadGraphAdoptionTest {
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
    for (final Result row : drain(new AlgoMST().execute(new Object[] { "w" }, null, context))) {
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
          final Map<String, Object> path = (Map<String, Object>) row.getProperty("path");
          final List<Vertex> nodes = (List<Vertex>) path.get("nodes");
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
