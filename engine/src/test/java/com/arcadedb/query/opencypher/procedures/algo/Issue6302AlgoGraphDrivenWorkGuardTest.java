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
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6302AlgoGraphDrivenWorkGuardTest {
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
      "algo.mst                         | CALL algo.mst('w') YIELD source RETURN source",
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
      "algo.mst                         | CALL algo.mst('w') YIELD source RETURN source",
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
   * deadline has to be observed <em>inside</em> it. 1500 nodes is 3.4e9 relaxations, several seconds of CPU, so
   * a 1 s deadline lands well inside the {@code k} loop - and the whole graph is only an 18 MB matrix, which the
   * 64 MB budget floor accepts without complaint. That is the issue's point in one fixture: the budget says yes,
   * and before this change nothing could then say stop.
   * <p>
   * The elapsed bound is measured with {@link StallAwareStopwatch} so a JVM-wide pause inside the window is
   * discounted rather than charged to the procedure. It is a tripwire between "aborted from inside the loop"
   * and "ran the loop to the end", not a latency budget: widen it rather than delete it if it ever flakes.
   */
  @Test
  @Tag("slow")
  @Timeout(300)
  void apspObservesTheDeadlineInsideTheTripleLoop() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-issue-6302-apsp-deadline");
    if (factory.exists())
      factory.open().drop();
    final Database dense = factory.create();
    try {
      dense.getSchema().createVertexType("Node");
      dense.getSchema().createEdgeType("LINK");

      final int nodeCount = 1500;
      dense.transaction(() -> {
        final List<MutableVertex> nodes = new ArrayList<>(nodeCount);
        for (int i = 0; i < nodeCount; i++)
          nodes.add(dense.newVertex("Node").set("idx", i).save());
        for (int i = 0; i < nodeCount; i++)
          nodes.get(i).newEdge("LINK", nodes.get((i + 1) % nodeCount), true, (Object[]) null).save();
      });

      dense.getConfiguration().setValue(GlobalConfiguration.COMMAND_TIMEOUT, 1_000L);

      final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
      assertThatThrownBy(() -> {
        final ResultSet rs = dense.query("opencypher", "CALL algo.apsp() YIELD source RETURN source");
        while (rs.hasNext())
          rs.next();
      }).as("O(V³) with no knob is exactly the shape that has to be abortable")
          .hasStackTraceContaining(GlobalConfiguration.COMMAND_TIMEOUT.getKey());

      stopwatch.assertGaveUpWithin(60_000L, "a Floyd-Warshall pass aborted from inside, not run to the end");
    } finally {
      dense.getConfiguration().setValue(GlobalConfiguration.COMMAND_TIMEOUT,
          GlobalConfiguration.COMMAND_TIMEOUT.getDefValue());
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
