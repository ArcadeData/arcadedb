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
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.RID;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.query.opencypher.traversal.TraversalPath;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.graph.Edge;
import com.arcadedb.query.sql.executor.Result;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for issues #12172, #12173, #12175 and #12176: the {@code algo.*} procedures that read the whole graph, or built
 * a {@code nodeCount x nodeCount} structure, to answer a question about a neighbourhood or a sparse graph.
 * <p>
 * Each rewrite is held to the answer of the algorithm it replaced, re-implemented here over the adjacency the vertex API lists (the
 * order and the repetitions the old {@code int[][]} adjacency had), on random multigraphs with parallel edges, reciprocal pairs,
 * self-loops and isolated vertices, with and without a Graph Analytical View. That it no longer scales with the database is asserted
 * through the working-memory budget rather than a clock: a budget that the old structure could not fit in serves the new one.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class SparseNeighbourhoodAlgorithmsTest {
  private static final String DB_PATH = "./target/databases/test-issue-12172-sparse-algorithms";

  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("N");
    database.getSchema().createEdgeType("R");
  }

  @AfterEach
  void teardown() {
    if (database != null) {
      database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY,
          GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY.getDefValue());
      if (database.isTransactionActive())
        database.rollback();
      database.drop();
      database = null;
    }
  }

  // ── Issue #12176: the source-anchored procedures read the neighbourhood, not the database ──────────────────────────

  @Nested
  class SourceAnchoredProcedures {
    @Test
    void randomWalkStepsExactlyAsTheWholeGraphAdjacencyWould() {
      final RID[] v = randomMultigraph(150, 450, 7L);
      for (final Vertex.DIRECTION dir : Vertex.DIRECTION.values())
        for (final int start : new int[] { 0, 3, 77, 149 }) {
          final List<RID> expected = oracleWalk(v[start], 25, dir, 42L);
          assertThat(walk(new BasicCommandContext(), v[start], 25, dir, 42L))
              .as("walk from %s over %s", v[start], dir).isEqualTo(expected);
        }
    }

    @Test
    void linkPredictionScoresMatchTheWholeGraphFormulas() {
      final RID[] v = randomMultigraph(150, 450, 11L);
      for (final Vertex.DIRECTION dir : Vertex.DIRECTION.values())
        for (final int source : new int[] { 0, 5, 64, 149 })
          assertScoresMatchOracle(newContext(), v, source, dir);
    }

    @Test
    void theAnswersAreTheSameWithAView() throws Exception {
      final RID[] v = randomMultigraph(150, 450, 13L);
      final GraphAnalyticalView view = buildView();
      try {
        for (final Vertex.DIRECTION dir : Vertex.DIRECTION.values())
          for (final int source : new int[] { 1, 42, 148 }) {
            final BasicCommandContext context = newContext();
            assertScoresMatchOracle(context, v, source, dir);
            assertThat(context.getVariable(CommandContext.CSR_ACCELERATED_VAR))
                .as("the view must back the call, or this compares the record path with itself").isEqualTo(true);
          }
        // The walk with a view lists the neighbours in the order the view stores them: it is a walk of the same graph
        final List<RID> walk = walk(newContext(), v[0], 30, Vertex.DIRECTION.BOTH, 5L);
        assertThat(walk).hasSizeGreaterThan(1);
        for (int i = 1; i < walk.size(); i++)
          assertThat(neighbours(walk.get(i - 1), Vertex.DIRECTION.BOTH)).contains(walk.get(i));
      } finally {
        view.drop();
      }
    }

    @Test
    void aSourceCreatedAfterTheViewIsAnsweredFromTheRecords() throws Exception {
      final RID[] v = randomMultigraph(40, 80, 17L);
      final GraphAnalyticalView view = buildView();
      try {
        final RID[] added = new RID[1];
        database.transaction(() -> {
          final MutableVertex fresh = database.newVertex("N").set("k", 1000).save();
          fresh.newEdge("R", v[0].asVertex());
          fresh.newEdge("R", v[1].asVertex());
          added[0] = fresh.getIdentity();
        });
        // The view was built before the vertex existed and cannot answer for it: the call used to answer nothing at all
        final List<Result> rows = drain(new AlgoCommonNeighbors().execute(new Object[] { added[0].asVertex(), "R", "BOTH" }, null,
            newContext()));
        final Map<RID, Number> expected = oracleCommonNeighbours(added[0], Vertex.DIRECTION.BOTH);
        assertThat(rows).hasSize(expected.size());
        for (final Result row : rows)
          assertThat(row.<Number>getProperty("commonNeighbors").intValue())
              .isEqualTo(expected.get(row.<RID>getProperty("node2")).intValue());
        assertThat(walk(newContext(), added[0], 1, Vertex.DIRECTION.OUT, 3L)).hasSize(2);
      } finally {
        view.drop();
      }
    }

    @Test
    void aBudgetFarBelowTheWholeGraphServesTheNeighbourhood() {
      // 4,000 vertices cost 384,000 bytes to load and their adjacency as much again: a 64 KB budget refused every one of these calls
      // before issue #12176, and serves a neighbourhood of a few dozen vertices now
      final RID[] v = chainOfTriangles(4_000);
      database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, 64 * 1024L);

      assertThat(walk(newContext(), v[10], 50, Vertex.DIRECTION.BOTH, 1L)).hasSize(51);
      assertThat(drain(new AlgoTotalNeighbors().execute(new Object[] { v[10].asVertex(), v[11].asVertex() }, null, newContext())))
          .hasSize(1);
      assertThat(drain(new AlgoJaccardSimilarity().execute(new Object[] { v[10].asVertex() }, null, newContext()))).isNotEmpty();
      assertThat(drain(new AlgoCommonNeighbors().execute(new Object[] { v[10].asVertex() }, null, newContext()))).isNotEmpty();
      assertThat(drain(new AlgoAdamicAdar().execute(new Object[] { v[10].asVertex() }, null, newContext()))).isNotEmpty();
      assertThat(drain(new AlgoResourceAllocation().execute(new Object[] { v[10].asVertex() }, null, newContext()))).isNotEmpty();

      // The control: the same budget still refuses a call that does read the whole graph
      assertThatThrownBy(() -> drain(new AlgoWCC().execute(new Object[0], null, newContext())))
          .hasStackTraceContaining(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY.getKey());
    }

    @Test
    void aCutoffThatAdmitsZeroStillListsEveryVertex() {
      final RID[] v = randomMultigraph(60, 90, 19L);
      // A negative cutoff admits the nodes that share nothing with the source: every vertex but the source, as before
      final List<Result> jaccard = drain(new AlgoJaccardSimilarity().execute(new Object[] { v[0].asVertex(), "R", "BOTH", -1.0 },
          null, newContext()));
      final int sourceDegree = neighbours(v[0], Vertex.DIRECTION.BOTH).size();
      int expected = 0;
      for (final RID rid : v)
        if (!rid.equals(v[0]) && (sourceDegree > 0 || !neighbours(rid, Vertex.DIRECTION.BOTH).isEmpty()))
          expected++;
      assertThat(jaccard).hasSize(expected);
      assertThat(drain(new AlgoCommonNeighbors().execute(new Object[] { v[0].asVertex(), "R", "BOTH", 0 }, null, newContext())))
          .hasSize(v.length - 1);
      assertThat(drain(new AlgoAdamicAdar().execute(new Object[] { v[0].asVertex(), "R", "BOTH", -0.5 }, null, newContext())))
          .hasSize(v.length - 1);
    }

    @Test
    void jaccardListsItsRowsInRidOrder() {
      final RID[] v = randomMultigraph(80, 240, 23L);
      final List<Result> rows = drain(new AlgoJaccardSimilarity().execute(new Object[] { v[2].asVertex() }, null, newContext()));
      final List<RID> listed = new ArrayList<>();
      for (final Result row : rows)
        listed.add(row.getProperty("node2"));
      assertThat(listed).isNotEmpty().isSortedAccordingTo(RID::compareTo);
    }

    @Test
    void theIncomingSideOfAUnidirectionalTypeIsRead() {
      database.getSchema().buildEdgeType().withName("U").withBidirectional(false).create();
      final RID[] v = new RID[3];
      database.transaction(() -> {
        final MutableVertex a = database.newVertex("N").set("k", 0).save();
        final MutableVertex b = database.newVertex("N").set("k", 1).save();
        final MutableVertex c = database.newVertex("N").set("k", 2).save();
        a.newEdge("U", b);
        c.newEdge("U", b);
        v[0] = a.getIdentity();
        v[1] = b.getIdentity();
        v[2] = c.getIdentity();
      });
      // b stores no trace of the edges that end in it: reaching a and c from b takes the incoming side (issue #8629)
      final List<Result> rows = drain(new AlgoCommonNeighbors().execute(new Object[] { v[0].asVertex(), "U", "BOTH" }, null,
          newContext()));
      assertThat(rows).hasSize(1);
      assertThat(rows.getFirst().<RID>getProperty("node2")).isEqualTo(v[2]);
      final List<Result> walk = drain(new AlgoRandomWalk().execute(new Object[] { v[1].asVertex(), 1, "U", "IN", 9L }, null,
          newContext()));
      assertThat(walk.getFirst().<Number>getProperty("steps").intValue()).isEqualTo(1);
    }

    private void assertScoresMatchOracle(final BasicCommandContext context, final RID[] v, final int source,
        final Vertex.DIRECTION dir) {
      final Vertex src = v[source].asVertex();
      final String d = dir.name();

      final Map<RID, Number> cn = oracleCommonNeighbours(v[source], dir);
      assertThat(scores(new AlgoCommonNeighbors().execute(new Object[] { src, "R", d }, null, context), "commonNeighbors"))
          .as("commonNeighbors from %s over %s", v[source], dir).isEqualTo(cn);

      final Map<RID, Number> jaccard = oracleJaccard(v[source], dir);
      assertThat(scores(new AlgoJaccardSimilarity().execute(new Object[] { src, "R", d }, null, context), "similarity"))
          .as("jaccard from %s over %s", v[source], dir).isEqualTo(jaccard);

      // Summed in the order the rows list the neighbours: a view may list them in another order than the records, and resource
      // allocation sums per neighbour rather than per candidate, so both are equal up to the rounding of the additions
      assertCloseTo(scores(new AlgoAdamicAdar().execute(new Object[] { src, "R", d }, null, context), "score"),
          oracleAdamicAdar(v[source], dir), "adamicAdar from " + v[source] + " over " + dir);
      assertCloseTo(scores(new AlgoResourceAllocation().execute(new Object[] { src, "R", d }, null, context), "score"),
          oracleResourceAllocation(v[source], dir), "resourceAllocation from " + v[source] + " over " + dir);

      final RID other = v[(source + 37) % v.length];
      final List<Result> total = drain(new AlgoTotalNeighbors().execute(new Object[] { src, other.asVertex(), "R", d }, null,
          context));
      assertThat(total.getFirst().<Number>getProperty("coefficient").longValue())
          .as("totalNeighbors of %s and %s over %s", v[source], other, dir).isEqualTo(oracleTotalNeighbours(v[source], other, dir));
    }

    private void assertCloseTo(final Map<RID, Number> actual, final Map<RID, Number> expected, final String what) {
      assertThat(actual.keySet()).as(what).isEqualTo(expected.keySet());
      for (final Map.Entry<RID, Number> e : expected.entrySet())
        assertThat(actual.get(e.getKey()).doubleValue()).as(what).isCloseTo(e.getValue().doubleValue(), Offset.offset(1e-12));
    }

    private List<RID> walk(final BasicCommandContext context, final RID start, final int steps, final Vertex.DIRECTION dir,
        final long seed) {
      context.setDatabase(database);
      final List<Result> rows = drain(new AlgoRandomWalk().execute(new Object[] { start.asVertex(), steps, "R", dir.name(), seed },
          null, context));
      final TraversalPath path = rows.getFirst().getProperty("path");
      final List<RID> rids = new ArrayList<>();
      for (final Vertex vertex : path.getVertices())
        rids.add(vertex.getIdentity());
      return rids;
    }
  }

  // ── Issue #12175: max flow over a residual graph of arcs ──────────────────────────────────────────────────────────

  @Nested
  class MaxFlow {
    @Test
    void theFlowIsTheDenseEdmondsKarpFlowToTheLastBit() {
      for (final long seed : new long[] { 1L, 2L, 3L }) {
        final RID[] v = randomMultigraph(90, 300, seed);
        for (final int[] pair : new int[][] { { 0, 89 }, { 5, 6 }, { 40, 12 } }) {
          final double expected = oracleMaxFlow(v, pair[0], pair[1]);
          final double actual = maxFlow(v[pair[0]], v[pair[1]]);
          assertThat(actual).as("flow %s -> %s, seed %d", v[pair[0]], v[pair[1]], seed).isEqualTo(expected);
        }
        database.transaction(() -> database.command("sql", "DELETE FROM N"));
      }
    }

    @Test
    void aGraphWhoseMatricesWouldNotFitTheBudgetIsServed() {
      // 3,000 vertices: the two dense matrices took 144 MB. The residual graph of a sparse graph of that size takes a few hundred KB
      final RID[] v = chainOfTriangles(3_000);
      database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, 4L * 1024 * 1024);
      assertThat(maxFlow(v[0], v[2_999])).isEqualTo(oracleMaxFlowOnChainOfTriangles());
    }

    @Test
    void aFlowFromAVertexToItselfIsRefused() {
      final RID[] v = randomMultigraph(10, 20, 4L);
      // It used to loop adding an unbounded path flow until the command timed out
      assertThatThrownBy(() -> maxFlow(v[3], v[3])).hasStackTraceContaining("sourceNode and sinkNode must be two different vertices");
    }

    private double maxFlow(final RID source, final RID sink) {
      final List<Result> rows = drain(new AlgoMaxFlow().execute(new Object[] { source.asVertex(), sink.asVertex(), "R", "w" }, null,
          newContext()));
      return rows.getFirst().<Number>getProperty("maxFlow").doubleValue();
    }

    /** Every link of the chain of triangles carries 1, and two disjoint paths run from end to end. */
    private double oracleMaxFlowOnChainOfTriangles() {
      return 2.0;
    }
  }

  // ── Issue #12173: clique and k-truss over sorted neighbour sets ──────────────────────────────────────────────────

  @Nested
  class CliqueAndTruss {
    @Test
    void theMaximalCliquesAreTheBruteForceOnes() {
      for (final long seed : new long[] { 5L, 6L }) {
        final RID[] v = randomMultigraph(70, 420, seed);
        for (final int minSize : new int[] { 1, 2, 3 }) {
          final Set<Set<RID>> expected = new HashSet<>();
          for (final Set<RID> clique : oracleMaximalCliques(v))
            if (clique.size() >= minSize)
              expected.add(clique);
          final List<Result> rows = drain(new AlgoClique().execute(new Object[] { "R", minSize }, null, newContext()));
          final Set<Set<RID>> actual = new HashSet<>();
          for (final Result row : rows) {
            final List<RID> members = row.getProperty("clique");
            assertThat(members).as("members listed once, in node order").doesNotHaveDuplicates();
            assertThat(row.<Integer>getProperty("size")).isEqualTo(members.size());
            actual.add(new HashSet<>(members));
          }
          assertThat(rows).as("each maximal clique listed once").hasSize(actual.size());
          assertThat(actual).as("seed %d, minSize %d", seed, minSize).isEqualTo(expected);
        }
        database.transaction(() -> database.command("sql", "DELETE FROM N"));
      }
    }

    @Test
    void theTrussNumbersAreThePeelingOnes() {
      for (final long seed : new long[] { 8L, 9L }) {
        final RID[] v = randomMultigraph(80, 500, seed);
        final Map<RID, Integer> expected = oracleTrussNumbers(v);
        final Map<RID, Integer> actual = new HashMap<>();
        for (final Result row : drain(new AlgoKTruss().execute(new Object[] { "R" }, null, newContext())))
          actual.put(row.getProperty("nodeId"), row.getProperty("trussNumber"));
        assertThat(actual).as("seed %d", seed).isEqualTo(expected);
        database.transaction(() -> database.command("sql", "DELETE FROM N"));
      }
    }

    @Test
    void aSelfLoopIsNoTriangleOfItsOwn() {
      // a - b with a self-loop on a: the old bit matrix counted a among the common neighbours of a and b and gave the edge a
      // phantom triangle (truss 3). The truss is defined on the simple graph, where a - b closes no triangle
      final RID[] v = new RID[2];
      database.transaction(() -> {
        final MutableVertex a = database.newVertex("N").set("k", 0).save();
        final MutableVertex b = database.newVertex("N").set("k", 1).save();
        a.newEdge("R", b);
        a.newEdge("R", a);
        v[0] = a.getIdentity();
        v[1] = b.getIdentity();
      });
      for (final Result row : drain(new AlgoKTruss().execute(new Object[] { "R" }, null, newContext())))
        assertThat(row.<Integer>getProperty("trussNumber")).isEqualTo(2);
    }

    @Test
    void aGraphWhoseBitMatrixWouldNotFitTheBudgetIsServed() {
      // 3,000 vertices: one bit matrix took 1.2 MB, kTruss built two. A 1 MB budget refused both procedures, and holds their sorted
      // neighbour sets with room to spare
      chainOfTriangles(3_000);
      database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, 1024L * 1024);

      final List<Result> cliques = drain(new AlgoClique().execute(new Object[] { "R", 3 }, null, newContext()));
      assertThat(cliques).hasSize(2_998);
      final List<Result> truss = drain(new AlgoKTruss().execute(new Object[] { "R" }, null, newContext()));
      assertThat(truss).hasSize(3_000).allSatisfy(row -> assertThat(row.<Integer>getProperty("trussNumber")).isEqualTo(3));
    }
  }

  // ── Issue #12172: k nearest neighbours from the two-hop neighbourhoods ────────────────────────────────────────────

  @Nested
  class KNearestNeighbours {
    @Test
    void theRowsAreTheAllPairsScanRowsInTheSameOrder() {
      for (final long seed : new long[] { 10L, 11L }) {
        final RID[] v = randomMultigraph(120, 360, seed);
        for (final Vertex.DIRECTION dir : Vertex.DIRECTION.values())
          for (final int k : new int[] { 1, 3, 10 }) {
            final List<String> expected = oracleKnn(v, k, dir);
            final List<String> actual = new ArrayList<>();
            for (final Result row : drain(new AlgoKNN().execute(new Object[] { k, "R", dir.name() }, null, newContext())))
              actual.add(row.getProperty("node1") + "->" + row.getProperty("node2") + "=" + row.getProperty("similarity"));
            assertThat(actual).as("seed %d, k %d, %s", seed, k, dir).isEqualTo(expected);
          }
        database.transaction(() -> database.command("sql", "DELETE FROM N"));
      }
    }

    @Test
    void zeroNeighboursListsNothing() {
      randomMultigraph(20, 40, 12L);
      // k = 0 left no top-k slot, and the first candidate indexed past the end of it
      assertThat(drain(new AlgoKNN().execute(new Object[] { 0 }, null, newContext()))).isEmpty();
    }

    @Test
    void aGraphWhoseBitMatrixWouldNotFitTheBudgetIsServed() {
      chainOfTriangles(3_000);
      database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, 1024L * 1024);
      assertThat(drain(new AlgoKNN().execute(new Object[] { 2, "R" }, null, newContext()))).isNotEmpty();
    }
  }

  // ── Graphs ─────────────────────────────────────────────────────────────────────────────────────────────────────

  /**
   * A random directed multigraph of {@code n} vertices: {@code edges} random edges, some of them parallel or reciprocal, a few
   * self-loops, and vertices left isolated. Each edge carries an integer weight {@code w} in [-1, 9], a few zero or negative.
   */
  private RID[] randomMultigraph(final int n, final int edges, final long seed) {
    final RID[] rids = new RID[n];
    database.transaction(() -> {
      final Random random = new Random(seed);
      final MutableVertex[] v = new MutableVertex[n];
      for (int i = 0; i < n; i++) {
        v[i] = database.newVertex("N").set("k", i).save();
        rids[i] = v[i].getIdentity();
      }
      // The last tenth stays isolated
      final int connected = Math.max(2, n - n / 10);
      for (int e = 0; e < edges; e++) {
        final int a = random.nextInt(connected);
        final int roll = random.nextInt(20);
        final int b = roll == 0 ? a : random.nextInt(connected);
        v[a].newEdge("R", v[b], "w", random.nextInt(11) - 1);
        if (roll == 1)
          v[b].newEdge("R", v[a], "w", random.nextInt(11) - 1);
        if (roll == 2)
          v[a].newEdge("R", v[b], "w", random.nextInt(11) - 1);
      }
    });
    return rids;
  }

  /** {@code n} vertices where each closes a triangle with the two before it: a sparse graph of maximal cliques of three. */
  private RID[] chainOfTriangles(final int n) {
    final RID[] rids = new RID[n];
    database.transaction(() -> {
      final MutableVertex[] v = new MutableVertex[n];
      for (int i = 0; i < n; i++) {
        v[i] = database.newVertex("N").set("k", i).save();
        rids[i] = v[i].getIdentity();
        if (i >= 1)
          v[i - 1].newEdge("R", v[i], "w", 1);
        if (i >= 2)
          v[i - 2].newEdge("R", v[i], "w", 1);
      }
    });
    return rids;
  }

  private GraphAnalyticalView buildView() throws Exception {
    final GraphAnalyticalView view = GraphAnalyticalView.builder(database).withName("sparse-view").withVertexTypes("N")
        .withEdgeTypes("R").build();
    assertThat(view.awaitReady(60, TimeUnit.SECONDS)).isTrue();
    return view;
  }

  // ── Oracles: the replaced algorithms, over the adjacency the vertex API lists ───────────────────────────────────

  /** The neighbours of {@code rid} in {@code dir}: one per edge, newest first, the OUT list before the IN list. */
  private List<RID> neighbours(final RID rid, final Vertex.DIRECTION dir) {
    final List<RID> result = new ArrayList<>();
    for (final Vertex n : rid.asVertex().getVertices(dir, "R"))
      result.add(n.getIdentity());
    return result;
  }

  private List<RID> oracleWalk(final RID start, final int steps, final Vertex.DIRECTION dir, final long seed) {
    final Random random = new Random(seed);
    final List<RID> walk = new ArrayList<>();
    walk.add(start);
    RID current = start;
    for (int s = 0; s < steps; s++) {
      final List<RID> row = neighbours(current, dir);
      if (row.isEmpty())
        break;
      current = row.get(random.nextInt(row.size()));
      walk.add(current);
    }
    return walk;
  }

  private Map<RID, Number> oracleCommonNeighbours(final RID source, final Vertex.DIRECTION dir) {
    final Set<RID> srcSet = new HashSet<>(neighbours(source, dir));
    final Map<RID, Number> result = new HashMap<>();
    for (final RID v : allVertices()) {
      if (v.equals(source))
        continue;
      int count = 0;
      for (final RID w : neighbours(v, dir))
        if (srcSet.contains(w))
          count++;
      if (count >= 1)
        result.put(v, count);
    }
    return result;
  }

  private Map<RID, Number> oracleJaccard(final RID source, final Vertex.DIRECTION dir) {
    final List<RID> srcRow = neighbours(source, dir);
    final Set<RID> srcSet = new HashSet<>(srcRow);
    final Map<RID, Number> result = new HashMap<>();
    for (final RID v : allVertices()) {
      if (v.equals(source))
        continue;
      final List<RID> row = neighbours(v, dir);
      int intersection = 0;
      for (final RID w : row)
        if (srcSet.contains(w))
          intersection++;
      final int union = srcRow.size() + row.size() - intersection;
      if (union == 0)
        continue;
      final double similarity = (double) intersection / union;
      if (similarity > 0.0)
        result.put(v, similarity);
    }
    return result;
  }

  private Map<RID, Number> oracleAdamicAdar(final RID source, final Vertex.DIRECTION dir) {
    final Map<RID, Double> scores = new LinkedHashMap<>();
    for (final RID w : neighbours(source, dir)) {
      final List<RID> row = neighbours(w, dir);
      if (row.size() <= 1)
        continue;
      final double contribution = 1.0 / Math.log(row.size());
      for (final RID v : row)
        if (!v.equals(source))
          scores.merge(v, contribution, Double::sum);
    }
    final Map<RID, Number> result = new HashMap<>();
    for (final Map.Entry<RID, Double> e : scores.entrySet())
      if (e.getValue() > 0.0)
        result.put(e.getKey(), e.getValue());
    return result;
  }

  private Map<RID, Number> oracleResourceAllocation(final RID source, final Vertex.DIRECTION dir) {
    final Set<RID> srcSet = new HashSet<>(neighbours(source, dir));
    final Map<RID, Number> result = new HashMap<>();
    for (final RID v : allVertices()) {
      if (v.equals(source))
        continue;
      double score = 0.0;
      for (final RID w : neighbours(v, dir)) {
        final int degree = neighbours(w, dir).size();
        if (srcSet.contains(w) && degree > 0)
          score += 1.0 / degree;
      }
      if (score > 0.0)
        result.put(v, score);
    }
    return result;
  }

  private long oracleTotalNeighbours(final RID a, final RID b, final Vertex.DIRECTION dir) {
    final List<RID> rowA = neighbours(a, dir);
    final Set<RID> setA = new HashSet<>(rowA);
    final List<RID> rowB = neighbours(b, dir);
    int common = 0;
    for (final RID w : rowB)
      if (setA.contains(w))
        common++;
    return (long) rowA.size() + rowB.size() - common;
  }

  /** The dense Edmonds-Karp the procedure used: capacity matrix summed edge by edge, residual BFS scanning every column. */
  private double oracleMaxFlow(final RID[] v, final int source, final int sink) {
    final int n = v.length;
    final Map<RID, Integer> index = indexOf(v);
    final double[][] residual = new double[n][n];
    for (int i = 0; i < n; i++)
      for (final Edge edge : v[i].asVertex().getEdges(Vertex.DIRECTION.OUT, "R")) {
        final int j = index.get(edge.getIn());
        final Object w = edge.get("w");
        final double cap = w instanceof Number num ? num.doubleValue() : 1.0;
        residual[i][j] += cap;
        residual[j][i] += cap;
      }
    final int[] parent = new int[n];
    final int[] queue = new int[n];
    double flow = 0.0;
    while (true) {
      Arrays.fill(parent, -1);
      parent[source] = source;
      int head = 0, tail = 0;
      queue[tail++] = source;
      bfs:
      while (head < tail) {
        final int u = queue[head++];
        for (int x = 0; x < n; x++)
          if (parent[x] == -1 && residual[u][x] > 0) {
            parent[x] = u;
            if (x == sink)
              break bfs;
            queue[tail++] = x;
          }
      }
      if (parent[sink] == -1)
        return flow;
      double pathFlow = Double.MAX_VALUE;
      for (int x = sink; x != source; x = parent[x])
        pathFlow = Math.min(pathFlow, residual[parent[x]][x]);
      for (int x = sink; x != source; x = parent[x]) {
        residual[parent[x]][x] -= pathFlow;
        residual[x][parent[x]] += pathFlow;
      }
      flow += pathFlow;
    }
  }

  /** Bron-Kerbosch without pivoting over the simple undirected graph: every maximal clique, isolated vertices included. */
  private List<Set<RID>> oracleMaximalCliques(final RID[] v) {
    final Map<RID, Set<RID>> adj = simpleUndirected(v);
    final List<Set<RID>> cliques = new ArrayList<>();
    bronKerbosch(new TreeSet<>(), new TreeSet<>(adj.keySet()), new TreeSet<>(), adj, cliques);
    return cliques;
  }

  private void bronKerbosch(final Set<RID> r, final Set<RID> p, final Set<RID> x, final Map<RID, Set<RID>> adj,
      final List<Set<RID>> out) {
    if (p.isEmpty() && x.isEmpty()) {
      out.add(new HashSet<>(r));
      return;
    }
    for (final RID node : new ArrayList<>(p)) {
      final Set<RID> r2 = new TreeSet<>(r);
      r2.add(node);
      final Set<RID> p2 = new TreeSet<>(p);
      p2.retainAll(adj.get(node));
      final Set<RID> x2 = new TreeSet<>(x);
      x2.retainAll(adj.get(node));
      bronKerbosch(r2, p2, x2, adj, out);
      p.remove(node);
      x.add(node);
    }
  }

  /** The round-by-round peeling: at level k, remove every edge in fewer than k - 2 triangles, giving it truss k - 1. */
  private Map<RID, Integer> oracleTrussNumbers(final RID[] v) {
    final Map<RID, Set<RID>> adj = simpleUndirected(v);
    final Map<RID, Integer> nodeTruss = new HashMap<>();
    for (final RID rid : v)
      nodeTruss.put(rid, 0);
    final Set<List<RID>> edges = new HashSet<>();
    for (final Map.Entry<RID, Set<RID>> e : adj.entrySet())
      for (final RID other : e.getValue())
        if (e.getKey().compareTo(other) < 0)
          edges.add(List.of(e.getKey(), other));
    int k = 2;
    while (!edges.isEmpty()) {
      boolean removed = true;
      while (removed) {
        removed = false;
        for (final Iterator<List<RID>> it = edges.iterator(); it.hasNext(); ) {
          final List<RID> edge = it.next();
          final Set<RID> common = new HashSet<>(adj.get(edge.get(0)));
          common.retainAll(adj.get(edge.get(1)));
          if (common.size() < k - 2) {
            it.remove();
            adj.get(edge.get(0)).remove(edge.get(1));
            adj.get(edge.get(1)).remove(edge.get(0));
            for (final RID end : edge)
              nodeTruss.merge(end, k - 1, Math::max);
            removed = true;
          }
        }
      }
      k++;
    }
    return nodeTruss;
  }

  /**
   * The all-pairs scan the procedure ran: for every node with neighbours, every other node with neighbours in node order, the
   * distinct-neighbour intersection over the union of the raw row lengths, kept in a top-k that replaces its minimum.
   */
  private List<String> oracleKnn(final RID[] v, final int k, final Vertex.DIRECTION dir) {
    final List<RID> order = allVertices();
    final Map<RID, Integer> index = new HashMap<>();
    for (int i = 0; i < order.size(); i++)
      index.put(order.get(i), i);
    final int n = order.size();
    final int[][] adj = new int[n][];
    final BitSet[] sets = new BitSet[n];
    for (int i = 0; i < n; i++) {
      final List<RID> row = neighbours(order.get(i), dir);
      adj[i] = new int[row.size()];
      sets[i] = new BitSet(n);
      for (int j = 0; j < row.size(); j++) {
        adj[i][j] = index.get(row.get(j));
        sets[i].set(adj[i][j]);
      }
    }
    final List<String> rows = new ArrayList<>();
    for (int u = 0; u < n; u++) {
      if (adj[u].length == 0)
        continue;
      final double[] topSim = new double[k];
      final int[] topIdx = new int[k];
      int topCount = 0;
      double minTop = 0.0;
      for (int x = 0; x < n; x++) {
        if (x == u || adj[x].length == 0)
          continue;
        final BitSet inter = (BitSet) sets[u].clone();
        inter.and(sets[x]);
        final int common = inter.cardinality();
        if (common == 0)
          continue;
        final double jaccard = (double) common / (adj[u].length + adj[x].length - common);
        if (topCount < k) {
          topSim[topCount] = jaccard;
          topIdx[topCount++] = x;
          if (topCount == k) {
            minTop = topSim[0];
            for (int y = 1; y < k; y++)
              minTop = Math.min(minTop, topSim[y]);
          }
        } else if (jaccard > minTop) {
          int minPos = 0;
          for (int y = 1; y < k; y++)
            if (topSim[y] < topSim[minPos])
              minPos = y;
          topSim[minPos] = jaccard;
          topIdx[minPos] = x;
          minTop = topSim[0];
          for (int y = 1; y < k; y++)
            minTop = Math.min(minTop, topSim[y]);
        }
      }
      for (int i = 0; i < topCount; i++)
        rows.add(order.get(u) + "->" + order.get(topIdx[i]) + "=" + topSim[i]);
    }
    return rows;
  }

  private Map<RID, Set<RID>> simpleUndirected(final RID[] v) {
    final Map<RID, Set<RID>> adj = new HashMap<>();
    for (final RID rid : v) {
      final Set<RID> set = new HashSet<>(neighbours(rid, Vertex.DIRECTION.BOTH));
      set.remove(rid);
      adj.put(rid, set);
    }
    return adj;
  }

  /** Every vertex, in the order the whole-graph load numbered them. */
  private List<RID> allVertices() {
    final List<RID> result = new ArrayList<>();
    for (final Iterator<Vertex> it = new AlgoWCC().getAllVertices(database, null); it.hasNext(); )
      result.add(it.next().getIdentity());
    return result;
  }

  private static Map<RID, Integer> indexOf(final RID[] v) {
    final Map<RID, Integer> index = new HashMap<>();
    for (int i = 0; i < v.length; i++)
      index.put(v[i], i);
    return index;
  }

  // ── Plumbing ───────────────────────────────────────────────────────────────────────────────────────────────────

  private BasicCommandContext newContext() {
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);
    return context;
  }

  private static Map<RID, Number> scores(final Stream<Result> rows, final String field) {
    final Map<RID, Number> result = new HashMap<>();
    for (final Result row : drain(rows))
      assertThat(result.put(row.getProperty("node2"), row.getProperty(field))).as("one row per node").isNull();
    return result;
  }

  private static List<Result> drain(final Stream<Result> rows) {
    final List<Result> results = new ArrayList<>();
    try (rows) {
      for (final Iterator<Result> it = rows.iterator(); it.hasNext(); )
        results.add(it.next());
    }
    return results;
  }
}
