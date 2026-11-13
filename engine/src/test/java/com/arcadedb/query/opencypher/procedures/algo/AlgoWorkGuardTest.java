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
import com.arcadedb.graph.GraphEngine;
import com.arcadedb.graph.GraphTraversalProvider;
import com.arcadedb.graph.GraphTraversalProviderRegistry;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.NodeEdgeWeights;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.StallAwareStopwatch;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntConsumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * The {@code WorkGuard} of the {@code algo.*} procedures: every phase that does work proportional to the graph checkpoints it, so a call can be aborted.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class AlgoWorkGuardTest {
  /**
   * Regression tests for issue #6302 - the {@code algo.*} procedures whose work is multiplied by the <em>graph</em>
   * rather than by a knob had no {@link com.arcadedb.query.sql.executor.WorkGuard} at all.
   * <p>
   * #6216 and #6264 established the rule that a long run should be abortable rather than forbidden, because time,
   * unlike memory, has no honest ceiling to pick. Both selected their procedures by asking "does it have an
   * iteration knob?", which is a proxy that misses every graph-driven loop - and the graph-driven ones are the
   * slowest in the package. {@code algo.apsp} makes the case on its own: the #6263 working-memory budget caps its
   * distance matrix at {@code arcadedb.cypher.algoMaxWorkingMemory}, whose 64 MB floor admits about 2890 nodes, and
   * 2890³ is ~2.4e10 iterations of the Floyd-Warshall triple loop. The budget's own answer to "is this graph
   * acceptable?" was yes, and nothing could then stop the run it had just accepted - not a thread interrupt, not
   * {@code arcadedb.command.timeout}, not the client hanging up.
   * <p>
   * The criterion applied here is the one the issue argues for: a guard belongs wherever the work is unbounded by
   * anything the caller controls, not only where a knob multiplies it. That covers the four procedures the issue
   * names and every other whose dominant loop is superlinear in the graph - all-pairs and per-source traversals,
   * peeling and contraction rounds, clique and simple-path enumeration. Procedures that make a single O(V + E)
   * pass are deliberately left alone: one pass costs what loading the graph and emitting the rows already cost, so
   * a checkpoint inside it would bound nothing the surrounding pipeline does not.
   */
  @Nested
  class GraphDrivenWorkGuard {
    private Database database;

    @BeforeEach
    void setup() {
      final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-issue-6302-algo-work-guard");
      if (factory.exists())
        factory.open().drop();
      database = factory.create();
      database.getSchema().createVertexType("Node");
      database.getSchema().createEdgeType("LINK");

      // Directed cycle A→B→C→D→A with a weight on every edge: every node has in- and out-edges, every ordered
      // pair is reachable, and the graph is bipartite - enough for every procedure below to reach its main loop.
      database.transaction(() -> {
        final MutableVertex a = database.newVertex("Node").set("name", "A").save();
        final MutableVertex b = database.newVertex("Node").set("name", "B").save();
        final MutableVertex c = database.newVertex("Node").set("name", "C").save();
        final MutableVertex d = database.newVertex("Node").set("name", "D").save();
        a.newEdge("LINK", b, true, new Object[] { "w", 1.0 }).save();
        b.newEdge("LINK", c, true, new Object[] { "w", 1.0 }).save();
        c.newEdge("LINK", d, true, new Object[] { "w", 1.0 }).save();
        d.newEdge("LINK", a, true, new Object[] { "w", 1.0 }).save();
      });
    }

    @AfterEach
    void teardown() {
      // A test that arms the interrupt flag must not leave it set for whatever runs next on this thread.
      Thread.interrupted();
      if (database != null) {
        if (database.isTransactionActive())
          database.rollback();
        database.drop();
      }
    }

    /**
     * The interrupt is the deterministic half of the guarantee: it is already pending when the call starts, so the
     * first checkpoint the procedure reaches must observe it, whatever the machine's speed. Nothing upstream of the
     * procedure consumes the flag, and the guard names the procedure in its message, so the assertion cannot pass
     * because something else aborted the query.
     * <p>
     * Each of these ran to completion before this change and returned a result.
     */
    @ParameterizedTest(name = "{0}")
    @CsvSource(delimiter = '|', value = {
        "algo.apsp                        | CALL algo.apsp() YIELD source RETURN source",
        "algo.mst                         | CALL algo.mst(null, 'w') YIELD source RETURN source",
        "algo.kShortestPaths              | MATCH (a:Node {name:'A'}), (c:Node {name:'C'}) CALL algo.kShortestPaths(a, c, 3) YIELD rank RETURN rank",
        "algo.steinerTree                 | MATCH (a:Node {name:'A'}), (c:Node {name:'C'}) CALL algo.steinerTree([a, c], 'LINK', 'w') YIELD weight RETURN weight",
        "algo.bellmanford                 | MATCH (a:Node {name:'A'}), (c:Node {name:'C'}) CALL algo.bellmanford(a, c, 'LINK', 'w') YIELD weight RETURN weight",
        "algo.betweenness                 | CALL algo.betweenness() YIELD node RETURN node",
        "algo.closeness                   | CALL algo.closeness() YIELD node RETURN node",
        "algo.harmonic                    | CALL algo.harmonic() YIELD node RETURN node",
        "algo.eccentricity                | CALL algo.eccentricity() YIELD node RETURN node",
        "algo.maxFlow                     | MATCH (a:Node {name:'A'}), (c:Node {name:'C'}) CALL algo.maxFlow(a, c, 'LINK', 'w') YIELD maxFlow RETURN maxFlow",
        "algo.msa                         | MATCH (a:Node {name:'A'}) CALL algo.msa(a, 'LINK', 'w') YIELD source RETURN source",
        "algo.knn                         | CALL algo.knn(2) YIELD node1 RETURN node1",
        "algo.hierarchicalClustering      | CALL algo.hierarchicalClustering('LINK', 2) YIELD node RETURN node",
        "algo.clique                      | CALL algo.clique('LINK', 2) YIELD nodes RETURN nodes",
        "algo.allsimplepaths              | MATCH (a:Node {name:'A'}), (c:Node {name:'C'}) CALL algo.allSimplePaths(a, c, 'LINK', 5) YIELD path RETURN path",
        "algo.kTruss                      | CALL algo.kTruss('LINK', 3) YIELD node RETURN node",
        "algo.triangleCount               | CALL algo.triangleCount() YIELD node RETURN node",
        "algo.localClusteringCoefficient  | CALL algo.localClusteringCoefficient() YIELD node RETURN node",
        "algo.densestSubgraph             | CALL algo.densestSubgraph() YIELD node RETURN node",
        "algo.bipartiteMatching           | CALL algo.bipartiteMatching() YIELD node1 RETURN node1",
        "algo.voteRank                    | CALL algo.voteRank() YIELD node RETURN node",
        "algo.richClub                    | CALL algo.richClub() YIELD degree RETURN degree" })
    @Timeout(120)
    void aGraphDrivenProcedureStopsOnAThreadInterrupt(final String procedure, final String query) {
      Thread.currentThread().interrupt();

      assertThatThrownBy(() -> drain(query))
          .as("%s has no knob to cap its work, so it must at least be abortable", procedure)
          .hasStackTraceContaining(procedure + "() has been interrupted");
    }

    /**
     * The counterweight: with nothing armed, every one of them still returns its result. A guard that aborted a
     * healthy call, or one placed where it changes the answer, would pass the test above and fail here.
     */
    @ParameterizedTest(name = "{0}")
    @CsvSource(delimiter = '|', value = {
        "algo.apsp                        | CALL algo.apsp() YIELD source RETURN source",
        "algo.mst                         | CALL algo.mst(null, 'w') YIELD source RETURN source",
        "algo.kShortestPaths              | MATCH (a:Node {name:'A'}), (c:Node {name:'C'}) CALL algo.kShortestPaths(a, c, 3) YIELD rank RETURN rank",
        "algo.steinerTree                 | MATCH (a:Node {name:'A'}), (c:Node {name:'C'}) CALL algo.steinerTree([a, c], 'LINK', 'w') YIELD weight RETURN weight",
        "algo.bellmanford                 | MATCH (a:Node {name:'A'}), (c:Node {name:'C'}) CALL algo.bellmanford(a, c, 'LINK', 'w') YIELD weight RETURN weight",
        "algo.betweenness                 | CALL algo.betweenness() YIELD node RETURN node",
        "algo.closeness                   | CALL algo.closeness() YIELD node RETURN node",
        "algo.harmonic                    | CALL algo.harmonic() YIELD node RETURN node",
        "algo.eccentricity                | CALL algo.eccentricity() YIELD node RETURN node",
        "algo.maxFlow                     | MATCH (a:Node {name:'A'}), (c:Node {name:'C'}) CALL algo.maxFlow(a, c, 'LINK', 'w') YIELD maxFlow RETURN maxFlow",
        "algo.msa                         | MATCH (a:Node {name:'A'}) CALL algo.msa(a, 'LINK', 'w') YIELD source RETURN source",
        "algo.knn                         | CALL algo.knn(2) YIELD node1 RETURN node1",
        "algo.hierarchicalClustering      | CALL algo.hierarchicalClustering('LINK', 2) YIELD node RETURN node",
        "algo.allsimplepaths              | MATCH (a:Node {name:'A'}), (c:Node {name:'C'}) CALL algo.allSimplePaths(a, c, 'LINK', 5) YIELD path RETURN path",
        "algo.triangleCount               | CALL algo.triangleCount() YIELD node RETURN node",
        "algo.localClusteringCoefficient  | CALL algo.localClusteringCoefficient() YIELD node RETURN node",
        "algo.densestSubgraph             | CALL algo.densestSubgraph() YIELD node RETURN node",
        "algo.bipartiteMatching           | CALL algo.bipartiteMatching() YIELD node1 RETURN node1",
        "algo.voteRank                    | CALL algo.voteRank() YIELD node RETURN node",
        "algo.richClub                    | CALL algo.richClub() YIELD degree RETURN degree" })
    @Timeout(120)
    void anUninterruptedCallStillReturnsItsResult(final String procedure, final String query) {
      assertThat(drain(query)).as("%s must still answer when nothing asked it to stop", procedure).isNotEmpty();
    }

    /**
     * {@code algo.msa} is the only procedure here whose guard is threaded through a <em>recursion</em>: Chu-Liu/
     * Edmonds contracts a cycle and re-runs itself on the smaller graph, so both the checkpoint's placement across
     * levels and the new leading parameter on the recursive self-call have to hold. The cycle fixture in
     * {@link #setup} never contracts anything - each non-root vertex's cheapest incoming edge already forms a path
     * from the root - so it exercises level 1 only.
     * <p>
     * Here R reaches A and B at cost 10 while A and B reach each other at cost 1, so the cheapest-incoming
     * selection is the 2-cycle A-B and the algorithm must contract it and recurse. The arborescence that comes back
     * is one 10 edge into the cycle plus one 1 edge inside it.
     */
    @Test
    @Timeout(120)
    void msaStaysAbortableThroughItsCycleContractionRecursion() {
      final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-issue-6302-msa-contraction");
      if (factory.exists())
        factory.open().drop();
      final Database contracted = factory.create();
      try {
        contracted.getSchema().createVertexType("Node");
        contracted.getSchema().createEdgeType("LINK");
        contracted.transaction(() -> {
          final MutableVertex r = contracted.newVertex("Node").set("name", "R").save();
          final MutableVertex a = contracted.newVertex("Node").set("name", "A").save();
          final MutableVertex b = contracted.newVertex("Node").set("name", "B").save();
          r.newEdge("LINK", a, true, new Object[] { "w", 10.0 }).save();
          r.newEdge("LINK", b, true, new Object[] { "w", 10.0 }).save();
          a.newEdge("LINK", b, true, new Object[] { "w", 1.0 }).save();
          b.newEdge("LINK", a, true, new Object[] { "w", 1.0 }).save();
        });

        final String query = "MATCH (r:Node {name:'R'}) CALL algo.msa(r, 'LINK', 'w') "
            + "YIELD source, target, weight RETURN source, target, weight";

        double total = 0.0;
        int rows = 0;
        final ResultSet rs = contracted.query("opencypher", query);
        while (rs.hasNext()) {
          total += ((Number) rs.next().getProperty("weight")).doubleValue();
          rows++;
        }
        assertThat(rows).as("an arborescence spans every non-root vertex").isEqualTo(2);
        assertThat(total).as("one 10 edge into the contracted cycle plus one 1 edge inside it").isEqualTo(11.0);

        Thread.currentThread().interrupt();
        assertThatThrownBy(() -> {
          final ResultSet aborted = contracted.query("opencypher", query);
          while (aborted.hasNext())
            aborted.next();
        }).as("the checkpoint has to survive being threaded through the contraction recursion")
            .hasStackTraceContaining("algo.msa() has been interrupted");
      } finally {
        Thread.interrupted();
        contracted.drop();
      }
    }

    /**
     * {@code algo.apsp} is the sharp case, and the one worth pinning against the clock rather than the interrupt
     * flag: Floyd-Warshall's triple loop is O(V³) on an input the memory budget explicitly admits, and the
     * deadline has to be observed <em>inside</em> it. There is no checkpoint after the loop, so a guard missing from
     * it lets the call complete normally and {@code assertThatThrownBy} fails - provided the loop is guaranteed to
     * outlast the deadline. That proviso is what the fixture is for.
     * <p>
     * It used to be a 1500-node ring, which looked like 3.4e9 relaxations but was not: the {@code dist[i][k] >= INF}
     * skip drops every row that cannot reach {@code k} through the nodes already processed, and on a ring that is
     * about half of them, so the real work was ~1.7e9 relaxations. A fast runner finished that inside the 1 s
     * deadline and the test failed with "Expecting code to raise a throwable" (issue #8653).
     * <p>
     * The hub topology removes the skip: node 0 is the first node loaded (one bucket, so scan order is insertion
     * order) and is linked both ways to every other node, so after {@code k = 0} every pair is reachable and every
     * later {@code k} relaxes all V² cells. With V = 4000 that is 6.4e10 relaxations - about 30 s of CPU on a
     * current workstation, thirty times the deadline - for a 128 MB matrix, which the budget is raised explicitly
     * to admit rather than left to the heap-scaled default.
     * <p>
     * The elapsed bound is measured with {@link StallAwareStopwatch} so a JVM-wide pause inside the window is
     * discounted rather than charged to the procedure. It is a tripwire between "aborted from inside the loop"
     * (about 1 s: the deadline plus one {@code i} row) and "ran the loop to the end" (tens of seconds), not a
     * latency budget: widen it rather than delete it if it ever flakes, but keep it under the full run.
     */
    @Test
    @Tag("slow")
    @Timeout(300)
    void apspObservesTheDeadlineInsideTheTripleLoop() {
      // The 128 MB matrix below needs real headroom: on a heap this small the call would die of an OutOfMemoryError
      // that says nothing about the deadline under test. The JVM default heap (a quarter of physical memory) clears
      // this on GitHub-hosted runners (7 GB and up) and the build sets no -Xmx, so only a starved run skips.
      assumeTrue(Runtime.getRuntime().maxMemory() >= 1024L * 1024 * 1024,
          "needs a heap of at least 1 GB for the 4000 x 4000 distance matrix");

      final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-issue-6302-apsp-deadline");
      if (factory.exists())
        factory.open().drop();
      final Database dense = factory.create();
      try {
        dense.getSchema().createVertexType("Node", 1);
        dense.getSchema().createEdgeType("LINK");

        final int nodeCount = 4000;
        dense.transaction(() -> {
          final MutableVertex hub = dense.newVertex("Node").set("idx", 0).save();
          for (int i = 1; i < nodeCount; i++) {
            final MutableVertex spoke = dense.newVertex("Node").set("idx", i).save();
            hub.newEdge("LINK", spoke, true, (Object[]) null).save();
            spoke.newEdge("LINK", hub, true, (Object[]) null).save();
          }
        });

        // The whole margin rests on the hub being node index 0: anywhere else, every k before it skips all rows but
        // one and the loop shrinks back to a size a fast runner can finish. algo.apsp numbers nodes in the order this
        // iterator returns them, so check it here rather than trust the bucket layout.
        assertThat(GraphEngine.getAllVertices(dense, null).next().getInteger("idx"))
            .as("the hub must be the first node algo.apsp loads, or the triple loop skips most of its work")
            .isEqualTo(0);

        // 4000 x 4000 doubles is 128 MB, over the 64 MB floor the default budget falls back to on a small heap.
        dense.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, 256L * 1024 * 1024);
        dense.getConfiguration().setValue(GlobalConfiguration.COMMAND_TIMEOUT, 1_000L);

        final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
        assertThatThrownBy(() -> {
          final ResultSet rs = dense.query("opencypher", "CALL algo.apsp() YIELD source RETURN source");
          while (rs.hasNext())
            rs.next();
        }).as("O(V³) with no knob is exactly the shape that has to be abortable")
            .hasStackTraceContaining(GlobalConfiguration.COMMAND_TIMEOUT.getKey());

        // Secondary to the assertion above, which already fails an unguarded loop outright (it returns, it does not
        // throw). This one catches a check that only fires after the loop: on a runner fast enough to bring the full
        // run under 15 s it can no longer tell the two apart, and only then does it stop adding anything.
        stopwatch.assertGaveUpWithin(15_000L, "a Floyd-Warshall pass aborted from inside, not run to the end");
      } finally {
        dense.getConfiguration().setValue(GlobalConfiguration.COMMAND_TIMEOUT,
            GlobalConfiguration.COMMAND_TIMEOUT.getDefValue());
        dense.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY,
            GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY.getDefValue());
        dense.drop();
      }
    }

    private List<Object> drain(final String query) {
      final List<Object> rows = new ArrayList<>();
      final ResultSet rs = database.query("opencypher", query);
      while (rs.hasNext())
        rows.add(rs.next());
      return rows;
    }
  }

  /**
   * Regression tests for issue #6295 - the phase an {@code algo.*} call actually spends its time in is not always
   * the loop its knob drives, and the phases <em>after</em> that loop are where #6216 and #6264 stopped looking.
   * <p>
   * Both of those issues hunted knobs whose value is unbounded. {@code algo.hashgnn}'s {@code embeddingDimension}
   * is not one - #6065 capped it at 4096 - which is exactly why the lens walked past it. The MinHash reduction runs
   * after the {@code iterations} loop and costs {@code embeddingDimension x numFeatures} = 4 x
   * {@code embeddingDimension²} operations <em>per node</em>, and that bounded per-node figure is then multiplied
   * by an unbounded node count. Measured on {@code main}: 112,988 ms against a 1000 ms
   * {@code arcadedb.command.timeout}, and the call returned a result rather than aborting.
   * <p>
   * The question that finds this shape is not "which knob has no ceiling" but <b>"which loop over the node count
   * has no checkpoint"</b>. Asking it across the package turned up {@code algo.slpa}'s post-processing (one boxed
   * map merge per remembered label, {@code nodeCount x (iterations + 1)} of them, after the propagation rounds),
   * {@code algo.slpa}'s {@code mostFrequent} (O(degree²) for one listener, between two per-node checkpoints), and
   * the pre-loop initialisation of {@code algo.hashgnn}, {@code algo.fastrp} and {@code algo.graphsage}.
   * <p>
   * <b>None of them is pinned as a binary outcome here any more.</b> {@code algo.hashgnn}'s reduction was, as the one
   * phase three orders of magnitude larger than the message-passing round beside it, until #11144 removed it: the
   * embedding is now built from {@code embeddingDensity} samples per node, with no {@code embeddingDimension²} step
   * left to isolate, and a deadline on {@code algo.hashgnn} is covered by the iteration-knob sweep of #6264. A test of
   * that kind needs a fixture where the unguarded phase outlasts a deadline that the guarded phase before it
   * comfortably survives. The rest are the same defect at a far smaller scale, and their cost sits within a small
   * factor of the guarded phase that precedes them - SLPA's
   * propagation and its post-processing are both O(nodeCount x iterations) and were measured at the same order, and
   * the initialisation phases of {@code algo.fastrp} and {@code algo.graphsage} are O(nodeCount x dimension) beside a
   * first layer that costs the same. Any threshold picked between two phases that close together would be measuring
   * the fixture and the runner, not the checkpoint. The issue's own numbers say as much: 113x over the deadline for
   * the reduction against 1086 ms over a 1000 ms deadline for SLPA. They are still fixed - a checkpoint that cannot
   * be isolated by a test is not a checkpoint that does nothing - and {@link #theGuardedPhasesStillProduceTheirResult}
   * pins that adding them changed no answer.
   */
  @Nested
  class UnguardedPhase {
    private Database database;

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
     * The counterweight: with no deadline configured, every procedure the sweep touched still produces its result at
     * an ordinary size. A checkpoint that aborted a healthy run, or one placed where it changes the answer, would fail
     * here - which is what covers the phases that cannot be isolated by a deadline.
     */
    @Test
    @Timeout(120)
    void theGuardedPhasesStillProduceTheirResult() {
      cycle("test-issue-6295-unguarded-phases-happy-path", 40);

      assertThat(drain("CALL algo.hashgnn({embeddingDimension: 16, iterations: 2, seed: 1}) YIELD node, embedding "
          + "RETURN node, embedding")).hasSize(40);
      assertThat(drain("CALL algo.slpa({iterations: 5, seed: 1}) YIELD node, communities RETURN node, communities"))
          .hasSize(40);
      assertThat(drain("CALL algo.fastrp({dimensions: 16, iterations: 2, seed: 1}) YIELD node RETURN node"))
          .hasSize(40);
      assertThat(drain("CALL algo.graphsage({embeddingDimension: 16, layers: 2, seed: 1}) YIELD node RETURN node"))
          .hasSize(40);
    }

    // ── Fixtures ─────────────────────────────────────────────────────────────

    /** A directed cycle of {@code nodeCount} nodes: every node has one out-edge and one in-edge. */
    private void cycle(final String name, final int nodeCount) {
      create(name);
      database.transaction(() -> {
        final List<MutableVertex> nodes = new ArrayList<>(nodeCount);
        for (int i = 0; i < nodeCount; i++)
          nodes.add(database.newVertex("Node").set("idx", i).save());
        for (int i = 0; i < nodeCount; i++)
          nodes.get(i).newEdge("LINK", nodes.get((i + 1) % nodeCount), true, (Object[]) null).save();
      });
    }

    private void create(final String name) {
      final DatabaseFactory factory = new DatabaseFactory("./target/databases/" + name);
      if (factory.exists())
        factory.open().drop();
      database = factory.create();
      database.getSchema().createVertexType("Node");
      database.getSchema().createEdgeType("LINK");
    }

    private List<Object> drain(final String query) {
      final List<Object> rows = new ArrayList<>();
      final ResultSet rs = database.query("opencypher", query);
      while (rs.hasNext())
        rows.add(rs.next());
      return rows;
    }
  }

  /**
   * Regression test for issue #6318 part 1 - {@code algo.localClusteringCoefficient} was abortable on its OLTP
   * path (a {@code guard.check()} per node, already covered by {@code Issue6302AlgoGraphDrivenWorkGuardTest}, whose
   * fixture never builds a {@link GraphAnalyticalView}) and not on its CSR path, which handed the whole computation
   * to {@code GraphAlgorithms#localClusteringCoefficient} with nothing threaded through to stop it.
   * <p>
   * Same deterministic technique as {@code Issue6302AlgoGraphDrivenWorkGuardTest}: the interrupt is armed before the
   * call starts, so the first checkpoint the procedure reaches must observe it regardless of the machine's speed.
   * Before this fix, the CSR path's checkpoint did not exist at all, so this call ran to completion and returned a
   * result despite the pending interrupt.
   */
  @Nested
  class LocalClusteringCoefficientCsrAbortability {
    private Database             database;
    private GraphAnalyticalView  view;

    @BeforeEach
    void setup() {
      final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-issue-6318-lcc-csr-abortability");
      if (factory.exists())
        factory.open().drop();
      database = factory.create();
      database.getSchema().createVertexType("Node");
      database.getSchema().createEdgeType("LINK");

      // A small triangle-bearing graph is enough: the point is whether the checkpoint is consulted at all, not
      // how much work the kernel does before finding it.
      database.transaction(() -> {
        final MutableVertex a = database.newVertex("Node").set("name", "A").save();
        final MutableVertex b = database.newVertex("Node").set("name", "B").save();
        final MutableVertex c = database.newVertex("Node").set("name", "C").save();
        final MutableVertex d = database.newVertex("Node").set("name", "D").save();
        a.newEdge("LINK", b, true, (Object[]) null).save();
        b.newEdge("LINK", c, true, (Object[]) null).save();
        c.newEdge("LINK", a, true, (Object[]) null).save();
        c.newEdge("LINK", d, true, (Object[]) null).save();
      });

      view = GraphAnalyticalView.builder(database)
          .withName("lcc-abortability-view")
          .withVertexTypes("Node")
          .withEdgeTypes("LINK")
          .build();
    }

    @AfterEach
    void teardown() {
      // A test that arms the interrupt flag must not leave it set for whatever runs next on this thread.
      Thread.interrupted();
      if (view != null)
        view.drop();
      if (database != null) {
        if (database.isTransactionActive())
          database.rollback();
        database.drop();
      }
    }

    @Test
    @Timeout(60)
    void theCSRPathStopsOnAThreadInterrupt() {
      Thread.currentThread().interrupt();

      assertThatThrownBy(() -> drain("CALL algo.localClusteringCoefficient() YIELD node RETURN node"))
          .as("a Graph Analytical View must not make algo.localClusteringCoefficient any less abortable than its "
              + "OLTP path")
          .hasStackTraceContaining("algo.localClusteringCoefficient() has been interrupted");
    }

    /**
     * The counterweight: with nothing armed, the CSR path still answers, and still through the view rather than
     * silently falling back to OLTP - a guard wired in wrong could break either half.
     */
    @Test
    void anUninterruptedCallStillReturnsItsResultThroughTheView() {
      assertThat(drain("CALL algo.localClusteringCoefficient() YIELD node RETURN node")).hasSize(4);
    }

    private List<Result> drain(final String query) {
      final List<Result> results = new ArrayList<>();
      final ResultSet rs = database.query("opencypher", query);
      while (rs.hasNext())
        results.add(rs.next());
      return results;
    }
  }

  /**
   * Issue #6444 (follow-up from #6417/#6317): {@code AbstractAlgoProcedure.GraphData.adjacency()}'s CSR fallback
   * branch - taken when a {@link GraphTraversalProvider} has no {@link com.arcadedb.graph.NeighborView} to size the
   * copy from - used to checkpoint the budget every 1024 <em>nodes</em>. A single supernode's neighbour list is
   * fully materialised by {@code getNeighborIds()} in one call, so a node-count interval gives the budget no chance
   * to refuse the call until up to 1023 more nodes have also been expanded, even though the supernode alone already
   * blew the budget.
   * <p>
   * The fix checkpoints on entries-seen-so-far as well as nodes-seen-so-far, so a single oversized row is priced
   * (and can be refused) the moment it lands, rather than only after the next node-count boundary.
   */
  @Nested
  class AdjacencyEntryCheckpoint {
    private Database database;

    @BeforeEach
    void setup() {
      final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-issue-6444-adjacency-entry-checkpoint");
      if (factory.exists())
        factory.open().drop();
      database = factory.create();
    }

    @AfterEach
    void teardown() {
      if (database != null) {
        database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY,
            GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY.getDefValue());
        GraphTraversalProviderRegistry.clearAll(database);
        database.drop();
        database = null;
      }
    }

    @Test
    void aSupernodeRowIsCheckpointedRightAwayRatherThanAfter1024Nodes() {
      // 5000 nodes so the old 1024-node interval has room to fire at least once before the loop ends, node 0 is a
      // supernode whose row alone costs far more than the budget, and every other node is empty - so an entries-
      // based checkpoint refuses right after node 0, while a nodes-based one only refuses at node 1023.
      final int nodeCount = 5000;
      final int supernodeDegree = 2_000_000;
      final SupernodeProvider provider = new SupernodeProvider(nodeCount, supernodeDegree);
      GraphTraversalProviderRegistry.register(database, provider);

      // Admits the upfront row-header reservation (5000 * 32 = 160,000 bytes) but not the supernode row on top of
      // it (2,000,000 entries * 4 bytes = 8,000,000 bytes).
      database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, 500_000L);

      assertThatThrownBy(() -> drain("CALL algo.wcc() YIELD node, componentId RETURN node"))
          .as("the supernode row alone must be enough to refuse the call")
          .hasStackTraceContaining("the adjacency list would need");

      assertThat(provider.calls.get())
          .as("a checkpoint keyed on entries-seen-so-far refuses right after the oversized row, "
              + "not after another 1023 nodes were expanded to reach the next node-count boundary")
          .isLessThan(10);
    }

    @Test
    void aBudgetThatFitsTheSupernodeLetsTheCallThrough() {
      // The counterweight: a budget generous enough for the supernode must not be refused by the new checkpoint.
      final int nodeCount = 50;
      final int supernodeDegree = 100;
      final SupernodeProvider provider = new SupernodeProvider(nodeCount, supernodeDegree);
      GraphTraversalProviderRegistry.register(database, provider);

      assertThat(drain("CALL algo.wcc() YIELD node, componentId RETURN node")).hasSize(nodeCount);
      assertThat(provider.calls.get()).isEqualTo(nodeCount);
    }

    private java.util.List<Object> drain(final String query) {
      final java.util.List<Object> rows = new java.util.ArrayList<>();
      try (final ResultSet resultSet = database.query("opencypher", query)) {
        while (resultSet.hasNext())
          rows.add(resultSet.next());
      }
      return rows;
    }

    /**
     * A minimal {@link GraphTraversalProvider} test double: no {@link com.arcadedb.graph.NeighborView}
     * (so {@code adjacency()} takes the fallback branch under test), node 0 returns an oversized neighbour
     * array and every other node returns an empty one.
     */
    private static final class SupernodeProvider implements GraphTraversalProvider {
      private final int nodeCount;
      private final int supernodeDegree;
      final AtomicInteger calls = new AtomicInteger();

      SupernodeProvider(final int nodeCount, final int supernodeDegree) {
        this.nodeCount = nodeCount;
        this.supernodeDegree = supernodeDegree;
      }

      @Override
      public int getNodeCount() {
        return nodeCount;
      }

      @Override
      public boolean isReady() {
        return true;
      }

      @Override
      public String getName() {
        return "test-supernode-provider";
      }

      @Override
      public boolean coversVertexType(final String typeName) {
        return true;
      }

      @Override
      public boolean coversEdgeType(final String edgeTypeName) {
        return true;
      }

      @Override
      public int getNodeId(final RID rid) {
        return -1;
      }

      @Override
      public RID getRID(final int nodeId) {
        return new RID(0, nodeId);
      }

      @Override
      public int[] getNeighborIds(final int nodeId, final Vertex.DIRECTION direction, final String... edgeTypes) {
        calls.incrementAndGet();
        return new int[nodeId == 0 ? supernodeDegree : 0];
      }

      @Override
      public long countEdges(final int nodeId, final Vertex.DIRECTION direction, final String... edgeTypes) {
        return nodeId == 0 ? supernodeDegree : 0;
      }

      @Override
      public boolean isConnectedTo(final int nodeA, final int nodeB, final Vertex.DIRECTION direction, final String... edgeTypes) {
        return false;
      }

      @Override
      public Object getProperty(final int nodeId, final String propertyName) {
        return null;
      }
    }
  }

  /**
   * Issue #6715: {@code AbstractAlgoProcedure.GraphData.weightedAdjacencyFromColumns} checkpointed its
   * {@link com.arcadedb.query.sql.executor.WorkGuard} once per <em>node</em>, but
   * {@link GraphTraversalProvider#edgeWeightsOf} - the one call each iteration makes to read a node's weighted
   * edges from a Graph Analytical View's columns - had no checkpoint of its own inside its O(degree) loop. A
   * supernode's row was therefore one unabortable unit of work: once {@code edgeWeightsOf} was entered for it,
   * nothing could stop the call until the whole row was built, however large the degree.
   * <p>
   * The fix threads the guard into {@code edgeWeightsOf} itself (a new overload taking a per-edge checkpoint
   * callback) so the walk can be interrupted mid-row, and gives the columnar build the same cumulative memory
   * reservation {@code adjacency()} already has, closing the second gap the issue's own follow-up comment named:
   * a supernode's arrays were never charged against {@code CYPHER_ALGO_MAX_WORKING_MEMORY} at all.
   */
  @Nested
  class EdgeWeightsOfCheckpoint {
    private Database database;

    @BeforeEach
    void setup() {
      final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-issue-6715-edgeweightsof-checkpoint");
      if (factory.exists())
        factory.open().drop();
      database = factory.create();
    }

    @AfterEach
    void teardown() {
      // A test that arms the interrupt flag must not leave it set for whatever runs next on this thread.
      Thread.interrupted();
      if (database != null) {
        database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY,
            GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY.getDefValue());
        GraphTraversalProviderRegistry.clearAll(database);
        database.drop();
        database = null;
      }
    }

    /**
     * The interrupt is armed from inside the provider itself, once the walk is a handful of edges into the
     * supernode's row - not before the call, which the outer per-node checkpoint would already catch regardless of
     * this fix. Before the fix, {@code edgeWeightsOf} has no checkpoint of its own, so the row is built to
     * completion (all {@code supernodeDegree} of the supernode's edges) before the interrupt is ever observed.
     * After the fix, the per-edge checkpoint restarts at zero for this call and fires at the next multiple of
     * 1024, so the walk stops within about one checkpoint stride of where the
     * interrupt was raised - orders of magnitude short of the full degree.
     */
    @Test
    void abortsMidSupernodeRatherThanBuildingTheWholeRow() {
      final int nodeCount = 10;
      final int supernodeIndex = 3;
      final int supernodeDegree = 5_000_000;
      final WeightedSupernodeProvider provider = new WeightedSupernodeProvider(nodeCount, supernodeIndex,
          supernodeDegree, "LINK", "w", 5);
      GraphTraversalProviderRegistry.register(database, provider);

      assertThatThrownBy(() -> drain("CALL algo.maxKCut(2, {weightProperty: 'w', direction: 'OUT'}) YIELD node RETURN node"))
          .as("a per-edge checkpoint inside edgeWeightsOf must observe the interrupt raised mid-row")
          .hasStackTraceContaining("algo.maxKCut() has been interrupted");

      assertThat(provider.supernodeEdgeCalls.get())
          .as("the walk must stop close to where the interrupt was raised, not after building the whole "
              + supernodeDegree + "-edge row")
          .isLessThan(2000);
    }

    /**
     * The counterweight: a healthy call - nothing armed - still reads every edge of a (much smaller) supernode and
     * returns its result, so the new checkpoint costs nothing when there is nothing to abort.
     */
    @Test
    void aHealthyCallStillReadsEveryEdgeOfTheSupernode() {
      final int nodeCount = 10;
      final int supernodeIndex = 3;
      final int supernodeDegree = 500;
      final WeightedSupernodeProvider provider = new WeightedSupernodeProvider(nodeCount, supernodeIndex,
          supernodeDegree, "LINK", "w", -1);
      GraphTraversalProviderRegistry.register(database, provider);

      assertThat(drain("CALL algo.maxKCut(2, {weightProperty: 'w', direction: 'OUT'}) YIELD node RETURN node")).isNotEmpty();
      assertThat(provider.supernodeEdgeCalls.get()).isEqualTo(supernodeDegree);
    }

    /**
     * The memory side of the same gap, named in the issue's own follow-up comment: before this fix, the columnar
     * weighted-adjacency build never reserved anything at all against {@code CYPHER_ALGO_MAX_WORKING_MEMORY}, so
     * many medium-sized rows could together exceed the budget with nothing to refuse the call. 3000 nodes of 2000
     * edges each is 6,000,000 entries; a tight 2,000,000-byte budget - which admits the row headers and only a
     * small fraction of the entries, since the checkpoint interval is capped by {@code MemoryBudget.capacityFor}
     * against what is actually left of it, not just the coarser {@code ADJACENCY_CHECKPOINT_ENTRIES / 3} - must
     * refuse within the first few dozen nodes, well short of building all 3000 rows.
     */
    @Test
    void manyMediumRowsTogetherExceedTheBudgetAndAreRefused() {
      final int nodeCount = 3000;
      final int degreePerNode = 2000;
      final UniformDegreeProvider provider = new UniformDegreeProvider(nodeCount, degreePerNode, "LINK", "w");
      GraphTraversalProviderRegistry.register(database, provider);

      // Admits the row-header reservation (3000 nodes * 2 arrays * 32 bytes = 192,000 bytes) but not the full
      // 6,000,000-entry payload (6,000,000 * (4 + 8) bytes = 72,000,000 bytes).
      database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, 2_000_000L);

      assertThatThrownBy(() -> drain("CALL algo.maxKCut(2, {weightProperty: 'w', direction: 'OUT'}) YIELD node RETURN node"))
          .as("the cumulative cost of many medium rows must be caught, not only a single oversized one")
          .hasStackTraceContaining("the weighted adjacency");

      assertThat(provider.edgePropertyCalls.get())
          .as("the call must be refused well before every node's row was built")
          .isLessThan(nodeCount * degreePerNode);
    }

    /**
     * The counterweight: a budget generous enough for the whole graph must not be refused by the new reservation.
     */
    @Test
    void aBudgetThatFitsTheWholeGraphLetsTheCallThrough() {
      final int nodeCount = 200;
      final int degreePerNode = 50;
      final UniformDegreeProvider provider = new UniformDegreeProvider(nodeCount, degreePerNode, "LINK", "w");
      GraphTraversalProviderRegistry.register(database, provider);

      assertThat(drain("CALL algo.maxKCut(2, {weightProperty: 'w', direction: 'OUT'}) YIELD node RETURN node")).isNotEmpty();
      assertThat(provider.edgePropertyCalls.get()).isEqualTo(nodeCount * degreePerNode);
    }

    private List<Object> drain(final String query) {
      final List<Object> rows = new ArrayList<>();
      try (final ResultSet resultSet = database.query("opencypher", query)) {
        while (resultSet.hasNext())
          rows.add(resultSet.next());
      }
      return rows;
    }

    /**
     * A minimal {@link GraphTraversalProvider} test double serving one weighted edge type/property: every node is
     * empty except {@code supernodeIndex}, whose row has {@code supernodeDegree} edges all reachable through
     * {@code edgeWeightsForSlice}. Self-interrupts the current thread once the supernode's slice build has
     * priced {@code selfInterruptAfterCalls} of its edges ({@code -1} disables this).
     */
    private static final class WeightedSupernodeProvider implements GraphTraversalProvider {
      private final int    nodeCount;
      private final int    supernodeIndex;
      private final int    supernodeDegree;
      private final String edgeType;
      private final String weightProperty;
      private final int    selfInterruptAfterCalls;
      final AtomicInteger supernodeEdgeCalls = new AtomicInteger();

      WeightedSupernodeProvider(final int nodeCount, final int supernodeIndex, final int supernodeDegree,
          final String edgeType, final String weightProperty, final int selfInterruptAfterCalls) {
        this.nodeCount = nodeCount;
        this.supernodeIndex = supernodeIndex;
        this.supernodeDegree = supernodeDegree;
        this.edgeType = edgeType;
        this.weightProperty = weightProperty;
        this.selfInterruptAfterCalls = selfInterruptAfterCalls;
      }

      @Override
      public int getNodeCount() {
        return nodeCount;
      }

      @Override
      public boolean isReady() {
        return true;
      }

      @Override
      public String getName() {
        return "test-weighted-supernode-provider";
      }

      @Override
      public boolean coversVertexType(final String typeName) {
        return true;
      }

      @Override
      public boolean coversEdgeType(final String edgeTypeName) {
        return true;
      }

      @Override
      public int getNodeId(final RID rid) {
        return -1;
      }

      @Override
      public RID getRID(final int nodeId) {
        return new RID(0, nodeId);
      }

      @Override
      public int[] getNeighborIds(final int nodeId, final Vertex.DIRECTION direction, final String... edgeTypes) {
        return new int[nodeId == supernodeIndex ? supernodeDegree : 0];
      }

      @Override
      public long countEdges(final int nodeId, final Vertex.DIRECTION direction, final String... edgeTypes) {
        return nodeId == supernodeIndex ? supernodeDegree : 0;
      }

      @Override
      public boolean isConnectedTo(final int nodeA, final int nodeB, final Vertex.DIRECTION direction, final String... edgeTypes) {
        return false;
      }

      @Override
      public Object getProperty(final int nodeId, final String propertyName) {
        return null;
      }

      @Override
      public String[] getMaterializedEdgeTypes() {
        return new String[] { edgeType };
      }

      @Override
      public boolean hasEdgeProperties() {
        return true;
      }

      @Override
      public boolean hasEdgeProperty(final String edgeType, final String propertyName) {
        return this.edgeType.equals(edgeType) && weightProperty.equals(propertyName);
      }

      @Override
      public NodeEdgeWeights edgeWeightsForSlice(final int nodeId, final Vertex.DIRECTION direction,
          final String edgeType, final String propertyName, final double defaultWeight,
          final IntConsumer edgeCheckpoint) {
        final int degree = nodeId == supernodeIndex ? supernodeDegree : 0;
        final int[] neighbors = new int[degree];
        final double[] weights = new double[degree];
        for (int i = 0; i < degree; i++) {
          if (edgeCheckpoint != null)
            edgeCheckpoint.accept(i);
          final int calls = supernodeEdgeCalls.incrementAndGet();
          if (selfInterruptAfterCalls >= 0 && calls == selfInterruptAfterCalls)
            Thread.currentThread().interrupt();
          weights[i] = 1.0;
        }
        return new NodeEdgeWeights(neighbors, weights);
      }
    }

    /**
     * A {@link GraphTraversalProvider} test double where every node has the same degree, so the cost that has to
     * be refused is the sum across many nodes rather than one oversized row.
     */
    private static final class UniformDegreeProvider implements GraphTraversalProvider {
      private final int    nodeCount;
      private final int    degreePerNode;
      private final String edgeType;
      private final String weightProperty;
      final AtomicInteger edgePropertyCalls = new AtomicInteger();

      UniformDegreeProvider(final int nodeCount, final int degreePerNode, final String edgeType, final String weightProperty) {
        this.nodeCount = nodeCount;
        this.degreePerNode = degreePerNode;
        this.edgeType = edgeType;
        this.weightProperty = weightProperty;
      }

      @Override
      public int getNodeCount() {
        return nodeCount;
      }

      @Override
      public boolean isReady() {
        return true;
      }

      @Override
      public String getName() {
        return "test-uniform-degree-provider";
      }

      @Override
      public boolean coversVertexType(final String typeName) {
        return true;
      }

      @Override
      public boolean coversEdgeType(final String edgeTypeName) {
        return true;
      }

      @Override
      public int getNodeId(final RID rid) {
        return -1;
      }

      @Override
      public RID getRID(final int nodeId) {
        return new RID(0, nodeId);
      }

      @Override
      public int[] getNeighborIds(final int nodeId, final Vertex.DIRECTION direction, final String... edgeTypes) {
        return new int[degreePerNode];
      }

      @Override
      public long countEdges(final int nodeId, final Vertex.DIRECTION direction, final String... edgeTypes) {
        return degreePerNode;
      }

      @Override
      public boolean isConnectedTo(final int nodeA, final int nodeB, final Vertex.DIRECTION direction, final String... edgeTypes) {
        return false;
      }

      @Override
      public Object getProperty(final int nodeId, final String propertyName) {
        return null;
      }

      @Override
      public String[] getMaterializedEdgeTypes() {
        return new String[] { edgeType };
      }

      @Override
      public boolean hasEdgeProperties() {
        return true;
      }

      @Override
      public boolean hasEdgeProperty(final String edgeType, final String propertyName) {
        return this.edgeType.equals(edgeType) && weightProperty.equals(propertyName);
      }

      @Override
      public NodeEdgeWeights edgeWeightsForSlice(final int nodeId, final Vertex.DIRECTION direction,
          final String edgeType, final String propertyName, final double defaultWeight,
          final IntConsumer edgeCheckpoint) {
        final int[] neighbors = new int[degreePerNode];
        final double[] weights = new double[degreePerNode];
        for (int i = 0; i < degreePerNode; i++) {
          if (edgeCheckpoint != null)
            edgeCheckpoint.accept(i);
          edgePropertyCalls.incrementAndGet();
          weights[i] = 1.0;
        }
        return new NodeEdgeWeights(neighbors, weights);
      }
    }
  }

  /**
   * Issue #6795 (follow-up on #6375/#6300): {@code weightedAdjacencyFromColumns} (the CSR path, see
   * {@link Issue6300AlgoMSTEdgeBudgetTest}) checkpoints its edge-entry reservation at
   * {@code min(ADJACENCY_CHECKPOINT_ENTRIES / 3, memory.capacityFor(INT_BYTES + DOUBLE_BYTES))}, recomputed
   * after every reservation so the checkpoint stride shrinks as the budget fills. {@code weightedAdjacencyFromRecords}
   * (the plain OLTP fallback, exercised here since no Graph Analytical View is built) used the flat
   * {@code ADJACENCY_CHECKPOINT_ENTRIES} constant with no {@code capacityFor} cap, so on a graph small enough that
   * neither that constant nor the every-1024-nodes boundary is ever reached mid-walk, the mid-loop checkpoint never
   * fired at all: the whole weighted adjacency list was built in memory before the single unconditional
   * post-loop reservation call finally refused it - the "up to ~12 MB overshoot before refusing" this issue
   * describes.
   * <p>
   * Pins the fix by budget: with a tight enough {@code arcadedb.cypher.algo.maxWorkingMemory}, the refusal must name
   * FEWER edge entries than the graph actually has - proof the checkpoint fired mid-walk instead of only after every
   * edge was already read into the adjacency arrays.
   */
  @Nested
  class WeightedAdjacencyRecordsCheckpoint {
    /** Below the every-1024-nodes checkpoint boundary and far below ADJACENCY_CHECKPOINT_ENTRIES (1_048_576). */
    private static final int  NODE_COUNT  = 200;
    private static final long GRAPH_BYTES = (NODE_COUNT + 1) * 96L; // OLTP_VERTEX_BYTES, +1 for the load's own headroom check
    private static final long ROW_HEADER_BYTES = NODE_COUNT * 64L; // 2 * MATRIX_ROW_OVERHEAD_BYTES per node

    private Database database;

    @BeforeEach
    void setup() {
      final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-issue-6795-weighted-adjacency-records");
      if (factory.exists())
        factory.open().drop();
      database = factory.create();
      database.getSchema().createVertexType("Node");
      // No edge property declared, and no Graph Analytical View built anywhere in this test: both keep the call on
      // weightedAdjacencyFromRecords rather than the CSR-columnar weightedAdjacencyFromColumns (see Issue6300's
      // setup note on the same distinction).
      database.getSchema().createEdgeType("LINK");

      database.transaction(() -> {
        final List<MutableVertex> nodes = new ArrayList<>(NODE_COUNT);
        for (int i = 0; i < NODE_COUNT; i++)
          nodes.add(database.newVertex("Node").set("idx", i).save());
        for (int i = 0; i < NODE_COUNT - 1; i++)
          nodes.get(i).newEdge("LINK", nodes.get(i + 1), true, new Object[] { "w", (double) (i + 1) }).save();
      });
    }

    @AfterEach
    void teardown() {
      if (database != null) {
        database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY,
            GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY.getDefValue());
        if (database.isTransactionActive())
          database.rollback();
        database.drop();
      }
    }

    @Test
    void theRecordPathChecksInMidWalkInsteadOfOnlyAfterMaterialisingEveryEdge() {
      // Enough to admit the graph load and the weighted-adjacency row headers, but only a handful of edge entries
      // on top - nowhere near the (NODE_COUNT - 1) = 199 edges the walk would fully materialise before a single
      // flat-threshold checkpoint ever fired.
      final long budget = GRAPH_BYTES + ROW_HEADER_BYTES + 200L;
      database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, budget);

      final Throwable failure = catchFailure(() -> drain("CALL algo.mst(null, 'w') YIELD source RETURN source"));

      assertThat(failure).as("a graph this far over budget must still be refused").isNotNull();
      assertThat(failure).hasStackTraceContaining("the weighted adjacency list would need");

      final Integer reportedEntries = reportedEdgeEntries(failure);
      assertThat(reportedEntries).as("the refusal message must name the edge-entry count it actually reached").isNotNull();
      // The unfixed code checkpoints only every 1024 nodes or ADJACENCY_CHECKPOINT_ENTRIES (1_048_576) entries -
      // neither ever reached by 199 total edges - so it reports the FULL edge count (199, measured) at the single
      // unconditional post-loop reservation: the whole adjacency list was already built by the time it refuses.
      // A checkpoint that also consults capacityFor(), sized against this test's tight budget, refuses within the
      // first handful of entries instead (1, measured) - comfortably under half of the full count either way.
      assertThat(reportedEntries)
          .as("a checkpoint that consults capacityFor must refuse mid-walk, well before every one of this "
              + "chain's " + (NODE_COUNT - 1) + " edge entries has been read into the adjacency arrays")
          .isLessThan((NODE_COUNT - 1) / 2);
    }

    private Throwable catchFailure(final Runnable body) {
      try {
        body.run();
        return null;
      } catch (final Throwable t) {
        return t;
      }
    }

    private static final Pattern EDGE_ENTRIES_PATTERN = Pattern.compile("(\\d+) edge entries");

    private static Integer reportedEdgeEntries(final Throwable failure) {
      // The refusal's own message may be wrapped by the query engine's own exception, so scan every cause's
      // message rather than only the outermost one - the same span assertj's hasStackTraceContaining covers.
      for (Throwable t = failure; t != null; t = t.getCause()) {
        final Matcher matcher = EDGE_ENTRIES_PATTERN.matcher(String.valueOf(t.getMessage()));
        if (matcher.find())
          return Integer.valueOf(matcher.group(1));
      }
      return null;
    }

    @Test
    void aBudgetThatFitsLetsTheCallThrough() {
      database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, -1L);
      assertThat(drain("CALL algo.mst(null, 'w') YIELD source RETURN source")).hasSize(NODE_COUNT - 1);
    }

    private List<Object> drain(final String query) {
      final List<Object> rows = new ArrayList<>();
      final ResultSet rs = database.query("opencypher", query);
      while (rs.hasNext())
        rows.add(rs.next());
      return rows;
    }
  }
}
