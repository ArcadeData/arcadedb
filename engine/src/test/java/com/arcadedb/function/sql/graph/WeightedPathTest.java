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

package com.arcadedb.function.sql.graph;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.EdgeWeight;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.query.opencypher.traversal.TraversalPath;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

/**
 * Regression tests of the graph path functions, grouped by feature.
 */
class WeightedPathTest {

  /**
   * Regression for #10527: weighted path functions honour maxDepth.
   *
   * Issue #10527: {@code maxDepth} on {@code dijkstra()} and {@code astar()} bounded the nodes the search POLLED, not the edges of the
   * path. Dijkstra answered "no path" for a 2-hop path at any {@code maxDepth} from 2 to 9 (the goal was refused when it was polled at
   * the bound), and astar answered a path that never reached the destination.
   * <p>
   * The rule, shared with {@code shortestPath()} since #10439: {@code maxDepth: n} is the number of edges a returned path may have, and
   * a returned path always runs from the source to the destination. Beyond that, the answer is the CHEAPEST path of at most n edges,
   * which is not the cheapest path cut short: a dearer path with fewer hops can be the only one that fits.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class WeightedPathMaxDepth extends TestHelper {
    private static final String[] FUNCTIONS = { "dijkstra", "astar" };

    private RID source;
    private RID target;

    // ------------------------------------------------------------------------------------------------------------ the branching graph

    /** S -> a1 (50) -> T, and S -> a2..a8 (1) which are dead ends: the only path to T is S, a1, T. */
    private void buildBranching() {
      database.getSchema().createVertexType("V");
      database.getSchema().createEdgeType("E");
      database.transaction(() -> {
        database.command("sql", "CREATE VERTEX V SET nm = 'S'");
        database.command("sql", "CREATE VERTEX V SET nm = 'T'");
        for (int i = 1; i <= 8; i++)
          database.command("sql", "CREATE VERTEX V SET nm = ?", "a" + i);
        edge("S", "a1", 50.0);
        for (int i = 2; i <= 8; i++)
          edge("S", "a" + i, 1.0);
        edge("a1", "T", 1.0);
      });
      source = rid("S");
      target = rid("T");
    }

    @ParameterizedTest
    @ValueSource(strings = { "dijkstra", "astar" })
    void aTwoHopPathIsFoundWheneverTheBoundAllowsIt(final String function) {
      buildBranching();

      for (int maxDepth = 2; maxDepth <= 12; maxDepth++)
        assertThat(names(path(function, maxDepth, null))).as("%s maxDepth %d", function, maxDepth).containsExactly("S", "a1", "T");

      assertThat(path(function, 1, null)).as("%s maxDepth 1", function).isEmpty();
      assertThat(path(function, 0, null)).as("%s maxDepth 0", function).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(strings = { "dijkstra", "astar" })
    void aBoundTooSmallIsNeverAnsweredWithAPathThatMissesTheDestination(final String function) {
      buildBranching();

      for (final Boolean emptyIfMaxDepth : new Boolean[] { null, Boolean.TRUE, Boolean.FALSE })
        for (int maxDepth = 0; maxDepth < 2; maxDepth++)
          assertThat(path(function, maxDepth, emptyIfMaxDepth)).as("%s maxDepth %d emptyIfMaxDepth %s", function, maxDepth, emptyIfMaxDepth)
              .isEmpty();
    }

    // ------------------------------------------------------------------------------------------------------------------ the chain

    @ParameterizedTest
    @ValueSource(strings = { "dijkstra", "astar" })
    void maxDepthIsTheNumberOfEdgesOfThePath(final String function) {
      database.getSchema().createVertexType("V");
      database.getSchema().createEdgeType("E");
      database.transaction(() -> {
        for (int i = 0; i <= 5; i++)
          database.command("sql", "CREATE VERTEX V SET nm = ?", "v" + i);
        for (int i = 0; i < 5; i++)
          edge("v" + i, "v" + (i + 1), 1.0);
      });
      source = rid("v0");
      target = rid("v5");

      // five edges: the bound that answers is five, as for shortestPath(), and not six
      assertThat(path(function, 5, null)).as("%s maxDepth 5", function).hasSize(6);
      assertThat(path(function, 4, null)).as("%s maxDepth 4", function).isEmpty();
      assertThat(path(function, 6, null)).as("%s maxDepth 6", function).hasSize(6);
    }

    @ParameterizedTest
    @ValueSource(strings = { "dijkstra", "astar" })
    void aSourceThatIsTheDestinationIsAZeroEdgePath(final String function) {
      buildBranching();
      source = rid("S");
      target = rid("S");

      assertThat(names(path(function, 0, null))).containsExactly("S");
      assertThat(names(path(function, 3, null))).containsExactly("S");
    }

    // --------------------------------------------------------------------------------------- the cheapest path that fits, not the cut one

    /**
     * S -> T costs 10 in one hop; S -> b1 -> b2 -> b3 -> T costs 4 in four. The cheapest path of at most three edges is the direct
     * one, though the cheapest path overall is the long one.
     */
    @ParameterizedTest
    @ValueSource(strings = { "dijkstra", "astar" })
    void theCheapestPathThatFitsTheBoundWinsOverTheCheapestPathOverall(final String function) {
      database.getSchema().createVertexType("V");
      database.getSchema().createEdgeType("E");
      database.transaction(() -> {
        for (final String name : new String[] { "S", "T", "b1", "b2", "b3" })
          database.command("sql", "CREATE VERTEX V SET nm = ?", name);
        edge("S", "T", 10.0);
        edge("S", "b1", 1.0);
        edge("b1", "b2", 1.0);
        edge("b2", "b3", 1.0);
        edge("b3", "T", 1.0);
      });
      source = rid("S");
      target = rid("T");

      assertThat(names(path(function, null, null))).containsExactly("S", "b1", "b2", "b3", "T");
      assertThat(names(path(function, 4, null))).containsExactly("S", "b1", "b2", "b3", "T");
      assertThat(names(path(function, 3, null))).containsExactly("S", "T");
      assertThat(names(path(function, 1, null))).containsExactly("S", "T");
    }

    /**
     * The vertex M is reached cheaply in two hops (S, c, M) and dearly in one (S, M). The cheap arrival is settled first, and the dear
     * one has to stay a candidate: it is the only way to M, and on to T, within three edges.
     */
    @ParameterizedTest
    @ValueSource(strings = { "dijkstra", "astar" })
    void aVertexReachedCheaplyInManyHopsStaysReachableInFewer(final String function) {
      database.getSchema().createVertexType("V");
      database.getSchema().createEdgeType("E");
      database.transaction(() -> {
        for (final String name : new String[] { "S", "T", "c", "M", "x", "y" })
          database.command("sql", "CREATE VERTEX V SET nm = ?", name);
        edge("S", "c", 1.0);
        edge("c", "M", 1.0);
        edge("S", "M", 9.0);
        edge("M", "x", 1.0);
        edge("x", "y", 1.0);
        edge("y", "T", 1.0);
      });
      source = rid("S");
      target = rid("T");

      // S c M x y T is five edges
      assertThat(names(path(function, 5, null))).containsExactly("S", "c", "M", "x", "y", "T");
      // S M x y T is four
      assertThat(names(path(function, 4, null))).containsExactly("S", "M", "x", "y", "T");
      assertThat(path(function, 3, null)).isEmpty();
    }

    // ----------------------------------------------------------------------------------------------------------- against a reference

    /**
     * The property, with no function named: over random weighted digraphs and every bound, each function answers exactly when a path
     * of at most n edges exists, the answer runs from the source to the destination, has at most n edges, and costs what the best such
     * path costs - which a table of the cheapest cost per (vertex, number of edges) gives independently of any search.
     */
    @Test
    void theAnswerIsTheCheapestPathOfAtMostMaxDepthEdges() {
      database.getSchema().createVertexType("V");
      database.getSchema().createEdgeType("E");
      final Random random = new Random(10527L);

      for (int round = 0; round < 12; round++) {
        final int vertices = 6 + random.nextInt(7);
        final double[][] weight = new double[vertices][vertices];
        for (final double[] row : weight)
          Arrays.fill(row, Double.POSITIVE_INFINITY);

        final RID[] rids = new RID[vertices];
        database.transaction(() -> {
          for (int i = 0; i < vertices; i++)
            rids[i] = database.newVertex("V").set("nm", "r" + i).save().getIdentity();
        });
        final int edges = vertices * 2 + random.nextInt(vertices);
        database.transaction(() -> {
          for (int e = 0; e < edges; e++) {
            final int from = random.nextInt(vertices);
            final int to = random.nextInt(vertices);
            if (from == to || weight[from][to] != Double.POSITIVE_INFINITY)
              continue;
            // integral weights, so a total is exact and two paths of one cost are not told apart by rounding
            weight[from][to] = 1 + random.nextInt(9);
            rids[from].asVertex().newEdge("E", rids[to].asVertex()).set("w", weight[from][to]).save();
          }
        });

        for (int s = 0; s < vertices; s++)
          for (int t = 0; t < vertices; t++)
            for (final int maxDepth : new int[] { 0, 1, 2, 3, 5, vertices }) {
              final double expected = cheapestWithin(weight, s, t, maxDepth);
              for (final String function : FUNCTIONS) {
                source = rids[s];
                target = rids[t];
                final List<RID> answer = path(function, maxDepth, null);
                final String what = function + " " + s + "->" + t + " maxDepth " + maxDepth + " round " + round;
                if (expected == Double.POSITIVE_INFINITY) {
                  assertThat(answer).as(what).isEmpty();
                } else {
                  assertThat(answer).as(what).isNotEmpty();
                  assertThat(answer.getFirst()).as(what).isEqualTo(rids[s]);
                  assertThat(answer.getLast()).as(what).isEqualTo(rids[t]);
                  assertThat(answer.size() - 1).as(what + " edges").isLessThanOrEqualTo(maxDepth);
                  assertThat(cost(answer)).as(what + " cost").isEqualTo(expected);
                }
              }
            }

        database.transaction(() -> {
          database.command("sql", "DELETE FROM E");
          database.command("sql", "DELETE FROM V");
        });
      }
    }

    /**
     * A grid of 625 vertices with a road in each of the four directions and random weights, where the cheap routes wind and the short
     * ones are dear: the search under a bound that cuts the cheap routes keeps several labels per vertex. It must still finish, and
     * answer what the layered reference answers. The timeout is a hang detector, not a latency bound.
     */
    @Test
    @Timeout(120)
    void aBoundedSearchOverALargerGraphFinishesAndAnswersTheReference() {
      database.getSchema().createVertexType("V");
      database.getSchema().createEdgeType("E");
      final int side = 25;
      final int vertices = side * side;
      final double[][] weight = new double[vertices][vertices];
      for (final double[] row : weight)
        Arrays.fill(row, Double.POSITIVE_INFINITY);
      final Random random = new Random(1052725L);

      final RID[] rids = new RID[vertices];
      database.transaction(() -> {
        for (int i = 0; i < vertices; i++)
          rids[i] = database.newVertex("V").set("nm", "g" + i).save().getIdentity();
        for (int row = 0; row < side; row++)
          for (int column = 0; column < side; column++) {
            final int from = row * side + column;
            final int[] neighbours = { column + 1 < side ? from + 1 : -1, column > 0 ? from - 1 : -1, row + 1 < side ? from + side : -1,
                row > 0 ? from - side : -1 };
            for (final int to : neighbours)
              if (to >= 0) {
                weight[from][to] = 1 + random.nextInt(9);
                rids[from].asVertex().newEdge("E", rids[to].asVertex()).set("w", weight[from][to]).save();
              }
          }
      });

      final int s = 0;
      final int t = vertices - 1;
      // the corner to the opposite corner is 48 edges at the least: below that nothing fits, past it the bound starts to bind on cost
      for (final int maxDepth : new int[] { 47, 48, 50, 56, 70 }) {
        final double expected = cheapestWithin(weight, s, t, maxDepth);
        for (final String function : FUNCTIONS) {
          source = rids[s];
          target = rids[t];
          final List<RID> answer = path(function, maxDepth, null);
          final String what = function + " maxDepth " + maxDepth;
          if (expected == Double.POSITIVE_INFINITY) {
            assertThat(answer).as(what).isEmpty();
          } else {
            assertThat(answer).as(what).isNotEmpty();
            assertThat(answer.size() - 1).as(what + " edges").isLessThanOrEqualTo(maxDepth);
            assertThat(cost(answer)).as(what + " cost").isEqualTo(expected);
          }
        }
      }
    }

    /** Bellman-Ford by layers: the cheapest cost from {@code s} to {@code t} over at most {@code maxDepth} edges. */
    private static double cheapestWithin(final double[][] weight, final int s, final int t, final int maxDepth) {
      final int n = weight.length;
      double[] best = new double[n];
      Arrays.fill(best, Double.POSITIVE_INFINITY);
      best[s] = 0;
      for (int layer = 0; layer < maxDepth; layer++) {
        final double[] next = best.clone();
        for (int u = 0; u < n; u++)
          if (best[u] != Double.POSITIVE_INFINITY)
            for (int v = 0; v < n; v++)
              if (weight[u][v] != Double.POSITIVE_INFINITY && best[u] + weight[u][v] < next[v])
                next[v] = best[u] + weight[u][v];
        best = next;
      }
      return best[t];
    }

    /** The cost of a path of vertices: the cheapest edge between each consecutive pair, which is the one a search can use. */
    private double cost(final List<RID> path) {
      double total = 0;
      for (int i = 0; i + 1 < path.size(); i++) {
        double cheapest = Double.POSITIVE_INFINITY;
        final Vertex from = path.get(i).asVertex();
        for (final Edge edge : from.getEdges(Vertex.DIRECTION.OUT, "E"))
          if (edge.getIn().equals(path.get(i + 1)))
            cheapest = Math.min(cheapest, ((Number) edge.get("w")).doubleValue());
        total += cheapest;
      }
      return total;
    }

    // ---------------------------------------------------------------------------------------------------------------------- helpers

    private void edge(final String from, final String to, final double weight) {
      database.command("sql", "CREATE EDGE E FROM (SELECT FROM V WHERE nm = ?) TO (SELECT FROM V WHERE nm = ?) SET w = ?", from, to, weight);
    }

    private RID rid(final String name) {
      try (final ResultSet resultSet = database.query("sql", "SELECT FROM V WHERE nm = ?", name)) {
        return resultSet.next().getIdentity().orElseThrow();
      }
    }

    private List<String> names(final List<RID> rids) {
      final List<String> names = new ArrayList<>();
      for (final RID rid : rids)
        names.add(rid.asVertex().getString("nm"));
      return names;
    }

    private List<RID> path(final String function, final Integer maxDepth, final Boolean emptyIfMaxDepth) {
      final StringBuilder options = new StringBuilder("{direction: 'OUT', edgeTypeNames: ['E']");
      if (function.equals("astar"))
        options.append(", vertexAxisNames: []");
      if (maxDepth != null)
        options.append(", maxDepth: ").append(maxDepth);
      if (emptyIfMaxDepth != null)
        options.append(", emptyIfMaxDepth: ").append(emptyIfMaxDepth);
      options.append('}');

      try (final ResultSet resultSet = database.query("sql", "SELECT " + function + "(?, ?, 'w', " + options + ") AS p", source, target)) {
        final List<?> answer = resultSet.next().getProperty("p");
        final List<RID> rids = new ArrayList<>();
        if (answer != null)
          for (final Object element : answer)
            rids.add((RID) element);
        return rids;
      }
    }
  }

  /**
   * Regression for #9443: one weight rule across path functions.
   *
   * Regression test for issue #9443: every weighted shortest-path finder reads an edge's weight the same way. An edge
   * without the weight property weighs 1 (it used to weigh 0 in {@code dijkstra()}, {@code astar()}, {@code algo.dijkstra}
   * and {@code algo.astar}, which made every unweighted edge free), and an edge whose weight is negative or NaN is not
   * walked (it used to be walked by A*, Dijkstra, {@code duanSSSP()}, Yen's {@code algo.kShortestPaths} and
   * {@code algo.steinerTree}, which breaks their invariant and answers an arbitrary path).
   * <p>
   * Three fixtures, each a triangle with a direct edge A-C against a two-hop A-B-C, built so that only the right rule
   * answers the direct edge:
   * <ul>
   *   <li>{@code M}: A-B and B-C have no weight, A-C weighs 1.5. By the 0 rule A-B-C is free; by the 1 rule it costs 2;</li>
   *   <li>{@code N}: A-B weighs 1, B-C weighs -1, A-C weighs 3. Walking the negative edge makes A-B-C cost 0;</li>
   *   <li>{@code X}: A-B weighs 1, B-C weighs NaN, A-C weighs 3;</li>
   *   <li>{@code I}: A-B weighs 1, B-C weighs +Infinity, A-C weighs 3.</li>
   * </ul>
   * Every finder is asked twice: on the records, and with a Graph Analytical View materializing the weight, so the
   * columnar arms (A*'s CSR neighbourhood, the {@code algo.dijkstra.singleSource} kernel, the shortest path finder's view
   * search) are held to the same rule.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class PathWeightRule {
    private static final String[] FIXTURES = { "M", "N", "X", "I" };
    private static final double[] EXPECTED = { 1.5, 3.0, 3.0, 3.0 };

    private Database database;

    @BeforeEach
    void setup() {
      final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-issue-9443-path-weights");
      if (factory.exists())
        factory.open().drop();
      database = factory.create();
      database.getSchema().createVertexType("node");
      database.getSchema().createEdgeType("road").createProperty("weight", Type.DOUBLE);
      database.getSchema().createEdgeType("rail").createProperty("weight", Type.DOUBLE);

      database.transaction(() -> {
        triangle("M", null, null, 1.5);
        triangle("N", 1.0, -1.0, 3.0);
        triangle("X", 1.0, Double.NaN, 3.0);
        triangle("I", 1.0, Double.POSITIVE_INFINITY, 3.0);

        // algo.steinerTree over parallel edges, created in both orders so "the first one listed" is wrong for one of them
        // whichever way the adjacency happens to list them
        for (final String pair : new String[] { "P", "R" }) {
          final MutableVertex p = vertex(pair + "P");
          final MutableVertex q = vertex(pair + "Q");
          for (final double weight : pair.equals("P") ? new double[] { 5.0, 2.0 } : new double[] { 2.0, 5.0 })
            p.newEdge("road", q, true, new Object[] { "weight", weight }).save();
        }

        // duanSSSP edge type filter: X-M-Y over rail costs 2, the direct X-Y over road costs 5
        final MutableVertex x = vertex("TX");
        final MutableVertex m = vertex("TM");
        final MutableVertex y = vertex("TY");
        x.newEdge("rail", m, true, new Object[] { "weight", 1.0 }).save();
        m.newEdge("rail", y, true, new Object[] { "weight", 1.0 }).save();
        x.newEdge("road", y, true, new Object[] { "weight", 5.0 }).save();
      });
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
    void theRuleItself() {
      assertThat(EdgeWeight.of(null)).isEqualTo(EdgeWeight.MISSING);
      assertThat(EdgeWeight.of("12")).isEqualTo(EdgeWeight.MISSING);
      assertThat(EdgeWeight.of(0)).isEqualTo(0.0);
      assertThat(EdgeWeight.of(2.5f)).isEqualTo(2.5);
      assertThat(EdgeWeight.of(7L)).isEqualTo(7.0);
      assertThat(EdgeWeight.isWalkable(EdgeWeight.of(-0.5))).isFalse();
      assertThat(EdgeWeight.isWalkable(EdgeWeight.of(Double.NaN))).isFalse();
      assertThat(EdgeWeight.isWalkable(EdgeWeight.of(Double.POSITIVE_INFINITY))).isFalse();
      assertThat(EdgeWeight.isWalkable(EdgeWeight.of(Double.NEGATIVE_INFINITY))).isFalse();
      assertThat(EdgeWeight.isWalkable(EdgeWeight.MISSING)).isTrue();
    }

    @Test
    void sqlFunctionsOnRecords() {
      assertSqlFunctions("records");
    }

    @Test
    void sqlFunctionsWithAView() {
      final GraphAnalyticalView view = weightedView();
      try {
        assertSqlFunctions("view");
      } finally {
        view.drop();
      }
    }

    @Test
    void cypherProceduresOnRecords() {
      assertCypherProcedures("records");
    }

    @Test
    void cypherProceduresWithAView() {
      final GraphAnalyticalView view = weightedView();
      try {
        assertCypherProcedures("view");
      } finally {
        view.drop();
      }
    }

    @Test
    void duanSSSPAcceptsAnEdgeTypeFilter() {
      assertThat(sqlPath("duanSSSP", "TX", "TY", "")).as("every type").containsExactly("TX", "TM", "TY");
      assertThat(sqlPath("duanSSSP", "TX", "TY", ", {edgeTypeNames: ['road']}")).containsExactly("TX", "TY");
      assertThat(sqlPath("duanSSSP", "TX", "TY", ", {edgeTypeNames: ['rail']}")).containsExactly("TX", "TM", "TY");
      assertThat(sqlPath("duanSSSP", "TY", "TX", ", {direction: 'IN', edgeTypeNames: ['road']}"))
          .containsExactly("TY", "TX");
      assertThat(sqlPath("duanSSSP", "TY", "TX", ", 'IN'")).as("the positional direction still works")
          .containsExactly("TY", "TM", "TX");
      assertThat(sqlPath("duanSSSP", "TX", "TY", ", {direction: 'IN'}")).as("against the edges").isEmpty();
    }

    @Test
    void duanSSSPKeepsItsSameVertexAndDirectionContract() {
      assertThat(sqlPath("duanSSSP", "TX", "TX", "")).containsExactly("TX");
      assertThat(namesOf(sqlRids("SELECT duanSSSP(?, ?) AS p", rid("TX"), rid("TY")))).as("the weight property defaults to 'weight'")
          .containsExactly("TX", "TM", "TY");
      assertThat(namesOf(sqlRids("SELECT duanSSSP(?, ?, null, {edgeTypeNames: ['road']}) AS p", rid("TX"), rid("TY"))))
          .as("a null weight property defaults to 'weight' too").containsExactly("TX", "TY");
      assertThatThrownBy(() -> sqlPath("duanSSSP", "TX", "TY", ", 'SIDEWAYS'")).hasMessageContaining("Invalid direction 'SIDEWAYS'")
          .hasMessageContaining("duanSSSP()");
      assertThatThrownBy(() -> sqlPath("duanSSSP", "TX", "TY", ", {maxDepth: 3}")).as("an option it does not support is refused")
          .hasMessageContaining("maxDepth");
    }

    @Test
    void steinerTreeReportsTheCheapestOfParallelEdges() {
      for (final String pair : new String[] { "P", "R" })
        try (final ResultSet rs = database.query("opencypher", """
            MATCH (p:node {name: $p}), (q:node {name: $q}) CALL algo.steinerTree([p, q], 'road', 'weight')
            YIELD weight, totalWeight RETURN weight, totalWeight""", Map.of("p", pair + "P", "q", pair + "Q"))) {
          assertThat(rs.hasNext()).isTrue();
          final Result row = rs.next();
          assertThat(((Number) row.getProperty("weight")).doubleValue()).as("pair %s", pair).isEqualTo(2.0);
          assertThat(((Number) row.getProperty("totalWeight")).doubleValue()).as("pair %s", pair).isEqualTo(2.0);
          assertThat(rs.hasNext()).isFalse();
        }
    }

    @Test
    void dijkstraFallsBackToOneForAWeightPropertyNoEdgeHas() {
      // Every edge lacks 'cost', so every edge weighs 1 and the direct edge wins on hop count. By the 0 rule all paths
      // tied at 0 and the answer depended on which one the search happened to reach first.
      for (final String function : new String[] { "dijkstra", "astar", "duanSSSP", "cchShortestPath" })
        assertThat(namesOf(sqlRids("SELECT " + function + "(?, ?, 'cost') AS p", rid("NA"), rid("NC"))))
            .as(function).containsExactly("NA", "NC");
    }

    // ---------------------------------------------------------------------------------------------------------------

    private void assertSqlFunctions(final String mode) {
      // filtered on the type the view covers, so a view is found and its columnar arm answers in "view" mode
      for (final String function : new String[] { "dijkstra", "astar", "duanSSSP", "cchShortestPath" })
        for (final String fixture : FIXTURES)
          for (final String options : new String[] { "", ", {edgeTypeNames: ['road']}" })
            assertThat(sqlPath(function, fixture + "A", fixture + "C", options))
                .as("%s()%s on fixture %s (%s)", function, options, fixture, mode)
                .containsExactly(fixture + "A", fixture + "C");
    }

    private void assertCypherProcedures(final String mode) {
      for (int i = 0; i < FIXTURES.length; i++) {
        final String f = FIXTURES[i];
        final double expected = EXPECTED[i];

        for (final String call : new String[] {
            "algo.dijkstra(a, c, 'road', 'weight', 'OUT')",
            "algo.astar(a, c, 'road', 'weight')",
            "algo.cch.shortestPath(a, c, 'road', 'weight', 'OUT')",
            "algo.kShortestPaths(a, c, 1, 'road', 'weight')" }) {
          final Result row = single("""
              MATCH (a:node {name: $a}), (c:node {name: $c}) CALL %s YIELD path, weight RETURN path, weight"""
              .formatted(call), f);
          assertThat(pathNames(row.getProperty("path"))).as("%s on fixture %s (%s)", call, f, mode)
              .containsExactly(f + "A", f + "C");
          assertThat(((Number) row.getProperty("weight")).doubleValue()).as("%s weight on fixture %s (%s)", call, f, mode)
              .isCloseTo(expected, within(1e-9));
        }

        final Map<String, Double> costs = new HashMap<>();
        try (final ResultSet rs = database.query("opencypher", """
            MATCH (a:node {name: $a}) CALL algo.dijkstra.singleSource(a, 'road', 'weight') YIELD node, cost
            RETURN node.name AS name, cost""", Map.of("a", f + "A"))) {
          while (rs.hasNext()) {
            final Result row = rs.next();
            costs.put(row.getProperty("name"), ((Number) row.getProperty("cost")).doubleValue());
          }
        }
        assertThat(costs.get(f + "C")).as("algo.dijkstra.singleSource cost of C on fixture %s (%s)", f, mode)
            .isCloseTo(expected, within(1e-9));
        // B is reached over A-B alone: 1 for the edge without a weight, as for the weighted ones
        assertThat(costs.get(f + "B")).as("algo.dijkstra.singleSource cost of B on fixture %s (%s)", f, mode)
            .isCloseTo(1.0, within(1e-9));

        final Result steiner = single("""
            MATCH (a:node {name: $a}), (c:node {name: $c}) CALL algo.steinerTree([a, c], 'road', 'weight')
            YIELD totalWeight RETURN totalWeight""", f);
        assertThat(((Number) steiner.getProperty("totalWeight")).doubleValue())
            .as("algo.steinerTree total weight on fixture %s (%s)", f, mode).isCloseTo(expected, within(1e-9));
      }
    }

    private void triangle(final String prefix, final Double ab, final Double bc, final double ac) {
      final MutableVertex a = vertex(prefix + "A");
      final MutableVertex b = vertex(prefix + "B");
      final MutableVertex c = vertex(prefix + "C");
      edge(a, b, ab);
      edge(b, c, bc);
      edge(a, c, ac);
    }

    private MutableVertex vertex(final String name) {
      return database.newVertex("node").set("name", name).save();
    }

    private void edge(final MutableVertex from, final MutableVertex to, final Double weight) {
      if (weight == null)
        from.newEdge("road", to).save();
      else
        from.newEdge("road", to, true, new Object[] { "weight", weight }).save();
    }

    private GraphAnalyticalView weightedView() {
      return GraphAnalyticalView.builder(database)
          .withName("issue-9443-view")
          .withVertexTypes("node")
          .withEdgeTypes("road")
          .withEdgeProperties("weight")
          .build();
    }

    private List<String> sqlPath(final String function, final String from, final String to, final String options) {
      return namesOf(sqlRids("SELECT " + function + "(?, ?, 'weight'" + options + ") AS p", rid(from), rid(to)));
    }

    private List<?> sqlRids(final String sql, final Object... args) {
      try (final ResultSet rs = database.query("sql", sql, args)) {
        final List<?> path = rs.next().getProperty("p");
        return path == null ? List.of() : path;
      }
    }

    private Result single(final String cypher, final String fixture) {
      try (final ResultSet rs = database.query("opencypher", cypher, Map.of("a", fixture + "A", "c", fixture + "C"))) {
        assertThat(rs.hasNext()).as(cypher).isTrue();
        return rs.next();
      }
    }

    private RID rid(final String name) {
      try (final ResultSet rs = database.query("sql", "SELECT FROM node WHERE name = ?", name)) {
        return rs.next().getIdentity().get();
      }
    }

    private static List<String> pathNames(final Object path) {
      return namesOf(((TraversalPath) path).getVertices());
    }

    private static List<String> namesOf(final List<?> elements) {
      final List<String> names = new ArrayList<>(elements.size());
      for (final Object element : elements)
        names.add(((Vertex) ((Identifiable) element).getRecord()).getString("name"));
      return names;
    }
  }

  /**
   * Regression for #8031: cheapest parallel edge is used.
   *
   * Regression test for issue #8031.
   * <p>
   * ArcadeDB is a multigraph: several edges of the same type may join the same ordered pair of vertices, each with
   * its own weight. {@code SQLFunctionAstar.getNeighborWeightsCSR()} assembled the neighbourhood as a
   * {@code Map<Vertex, Double>} with one entry per NEIGHBOUR and filled it with {@code put()}, so parallel edges
   * overwrote each other and the neighbour ended up priced at whichever edge adjacency happened to list last.
   * {@code dijkstra()} delegates straight to it, so both functions answered a genuinely more expensive path and
   * called it the shortest one - and which path they answered depended on the order the edges had been created in.
   * <p>
   * The graph below is the smallest one that shows it: A-to-B costs 1 over one parallel edge and 100 over the other,
   * while the detour A-to-C-to-B costs 4. The two fixtures differ only in the order the two parallel edges are
   * created, which is exactly what used to decide the answer.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class ParallelEdgeShortestPath extends TestHelper {

    private RID a;
    private RID b;
    private RID c;

    @Test
    void dijkstraTakesTheCheapestParallelEdgeWhenItWasCreatedFirst() {
      buildGraph(true);
      assertThat(path("dijkstra")).containsExactly(a, b);
    }

    @Test
    void dijkstraTakesTheCheapestParallelEdgeWhenItWasCreatedSecond() {
      buildGraph(false);
      assertThat(path("dijkstra")).containsExactly(a, b);
    }

    @Test
    void astarTakesTheCheapestParallelEdgeWhenItWasCreatedFirst() {
      buildGraph(true);
      assertThat(path("astar")).containsExactly(a, b);
    }

    @Test
    void astarTakesTheCheapestParallelEdgeWhenItWasCreatedSecond() {
      buildGraph(false);
      assertThat(path("astar")).containsExactly(a, b);
    }

    /**
     * The mirror-image half of the same defect: {@code getDistance(node, parent, target)}, which prices the step the
     * heuristic takes, used to {@code break} on the FIRST edge it found to the target rather than on the cheapest.
     * With the expensive parallel edge created first, an A* run that consults it must still not be talked out of the
     * cheap edge.
     */
    @Test
    void theHeuristicStepCostAlsoTakesTheCheapestParallelEdge() {
      buildGraph(false);
      assertThat(path("astar", ", {direction:'OUT', heuristicFormula:'MANHATTAN'}")).containsExactly(a, b);
    }

    /**
     * The edge-type filter has to reach the heuristic step cost too (PR #8093 review).
     * <p>
     * With an {@code edgeTypeNames} option naming only E8031, a second edge type joining the same pair must not be
     * priced into the answer at all. The decoy below is a free (weight 0) F8031 edge straight from A to B: if the
     * heuristic weighed it, it would report the A-to-B step as costing nothing.
     */
    @Test
    void anEdgeOfAnotherTypeIsNotWeighedWhenEdgeTypeNamesExcludesIt() {
      buildGraph(true);
      database.transaction(() -> {
        database.command("sql", "CREATE EDGE TYPE F8031");
        database.command("sql", "CREATE PROPERTY F8031.weight DOUBLE");
        // A decoy of a type the search is told to ignore, cheaper than anything it is allowed to walk.
        database.command("sql", "CREATE EDGE F8031 FROM " + a + " TO " + c + " SET weight = 0.0");
      });

      // Restricted to E8031, the cheapest A-to-B route is still the direct parallel edge at cost 1.
      assertThat(path("astar", ", {direction:'OUT', edgeTypeNames:['E8031']}")).containsExactly(a, b);
      assertThat(path("dijkstra", ", {direction:'OUT', edgeTypeNames:['E8031']}")).containsExactly(a, b);
    }

    /**
     * @param cheapFirst whether the weight-1 edge is created before the weight-100 one. The only difference between
     *                   the two fixtures, and the whole of the non-determinism the issue reports.
     */
    private void buildGraph(final boolean cheapFirst) {
      database.transaction(() -> {
        database.command("sql", "CREATE VERTEX TYPE V8031");
        database.command("sql", "CREATE EDGE TYPE E8031");
        database.command("sql", "CREATE PROPERTY E8031.weight DOUBLE");

        final MutableVertex va = database.newVertex("V8031").set("name", "A").save();
        final MutableVertex vb = database.newVertex("V8031").set("name", "B").save();
        final MutableVertex vc = database.newVertex("V8031").set("name", "C").save();
        a = va.getIdentity();
        b = vb.getIdentity();
        c = vc.getIdentity();

        if (cheapFirst) {
          va.newEdge("E8031", vb).set("weight", 1.0).save();
          va.newEdge("E8031", vb).set("weight", 100.0).save();
        } else {
          va.newEdge("E8031", vb).set("weight", 100.0).save();
          va.newEdge("E8031", vb).set("weight", 1.0).save();
        }

        // The detour: cost 4 in total, so it wins only when the cheap parallel edge is lost.
        va.newEdge("E8031", vc).set("weight", 2.0).save();
        vc.newEdge("E8031", vb).set("weight", 2.0).save();
      });
    }

    private List<RID> path(final String function) {
      return path(function, "");
    }

    private List<RID> path(final String function, final String extraArgs) {
      final List<RID> rids = new ArrayList<>();
      try (final ResultSet rs = database.query("sql",
          "SELECT " + function + "(" + a + ", " + b + ", 'weight'" + extraArgs + ") AS p")) {
        final Result row = rs.next();
        for (final Object each : row.<List<Object>>getProperty("p"))
          rids.add(each instanceof RID rid ? rid : ((Identifiable) each).getIdentity());
      }
      return rids;
    }
  }

  /**
   * Regression for #6301: CSR keeps edge weights aligned.
   *
   * Regression tests for issue #6301 in the two SQL graph functions that read edge weights out of a Graph
   * Analytical View: {@code astar()} and {@code bellmanFord()}.
   * <p>
   * Both paired {@link com.arcadedb.graph.GraphTraversalProvider#getNeighborIds} with
   * {@link com.arcadedb.graph.GraphTraversalProvider#edgeWeightsForSlice} by hand, which was wrong in two independent
   * ways and silently so - a wrong weight produces a wrong path, never an exception:
   * <ul>
   *   <li><b>a multi-type neighbour list is merged and sorted across types</b>, while the property columns are per
   *       type, so position {@code j} in the list is not position {@code j} in any one column;</li>
   *   <li><b>{@code BOTH} has no column at all.</b> A provider resolves {@code OUT} and {@code IN} only, so a
   *       {@code BOTH} lookup answers {@code null} for every edge - and {@code bellmanFord}'s direction argument
   *       <em>defaults</em> to {@code BOTH}, which made the plain three-argument call unit-weight the whole graph
   *       the moment a view existed.</li>
   * </ul>
   * Both now go through {@code GraphTraversalProvider.edgeWeightsOf}, which walks one slice per (type, direction)
   * and is the same helper the {@code algo.*} procedures use, so the three cannot drift apart again.
   * <p>
   * The fixture is the discriminator: {@code A-B-C-D} at 1.0 per hop against a direct {@code A-C} at 100.0. By
   * weight the shortest path from A to D is the four-vertex {@code A-B-C-D}; by hop count - which is what unit
   * weights collapse to - it is the three-vertex {@code A-C-D}. So a test that gets the weights wrong does not
   * merely report a wrong number, it returns a different path.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class CsrEdgeWeightAlignment {
    private Database        database;
    private MutableVertex[] nodes;

    @BeforeEach
    void setup() {
      final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-issue-6301-csr-edge-weights");
      if (factory.exists())
        factory.open().drop();
      database = factory.create();
      database.getSchema().createVertexType("node");
      // Declared, so a view built over it actually materialises the edge property column - without the declaration
      // the columnar path is never taken and this test would silently pin the OLTP one twice.
      database.getSchema().createEdgeType("road").createProperty("weight", Type.DOUBLE);

      database.transaction(() -> {
        nodes = new MutableVertex[4];
        for (int i = 0; i < 4; i++)
          nodes[i] = database.newVertex("node").set("name", String.valueOf((char) ('A' + i))).save();
        nodes[0].newEdge("road", nodes[1], true, new Object[] { "weight", 1.0 }).save();
        nodes[1].newEdge("road", nodes[2], true, new Object[] { "weight", 1.0 }).save();
        nodes[2].newEdge("road", nodes[3], true, new Object[] { "weight", 1.0 }).save();
        nodes[0].newEdge("road", nodes[2], true, new Object[] { "weight", 100.0 }).save();
      });
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
    void bellmanFordKeepsItsWeightsOnACsrBackedGraph() {
      assertThat(bellmanFordPath()).as("OLTP: A-B-C-D costs 3.0, the direct A-C hop alone costs 100.0")
          .containsExactly(rid(0), rid(1), rid(2), rid(3));

      final GraphAnalyticalView view = weightedView("bellman-csr-view");
      try {
        assertThat(bellmanFordPath())
            .as("the same call with a view present: BOTH has no property column, so this used to unit-weight "
                + "every edge and return the two-hop A-C-D instead")
            .containsExactly(rid(0), rid(1), rid(2), rid(3));
      } finally {
        view.drop();
      }
    }

    @Test
    void astarKeepsItsWeightsOnACsrBackedGraph() {
      // Directions are exercised both ways round: OUT is the direction whose column exists, BOTH is the one that
      // has none and used to price every edge at MIN - which for astar is 0.0, i.e. free, so every path tied and
      // the first one found won.
      for (final String direction : new String[] { "OUT", "BOTH" }) {
        assertThat(astarPath(direction)).as("OLTP, direction=%s", direction)
            .containsExactly(rid(0), rid(1), rid(2), rid(3));
      }

      final GraphAnalyticalView view = weightedView("astar-csr-view");
      try {
        for (final String direction : new String[] { "OUT", "BOTH" }) {
          assertThat(astarPath(direction)).as("view present, direction=%s", direction)
              .containsExactly(rid(0), rid(1), rid(2), rid(3));
        }
      } finally {
        view.drop();
      }
    }

    @Test
    void aViewMaterialisingAnotherPropertyDoesNotUnweightTheGraph() {
      // The other half of the same question: the view holds `other`, the call asks for `weight`. Every
      // the view then has no value to answer with, which is indistinguishable from "this edge has no value", so the only
      // safe reading is that the provider cannot serve the request at all.
      database.getSchema().getType("road").createProperty("other", Type.DOUBLE);
      database.transaction(() -> {
        for (final var edge : nodes[0].getEdges(Vertex.DIRECTION.OUT))
          edge.asEdge().modify().set("other", 1.0).save();
      });

      final GraphAnalyticalView view = GraphAnalyticalView.builder(database)
          .withName("other-property-view")
          .withVertexTypes("node")
          .withEdgeTypes("road")
          .withEdgeProperties("other")
          .build();
      try {
        assertThat(bellmanFordPath()).as("`other` is materialised, `weight` is not, so the records answer")
            .containsExactly(rid(0), rid(1), rid(2), rid(3));
      } finally {
        view.drop();
      }
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    private GraphAnalyticalView weightedView(final String name) {
      return GraphAnalyticalView.builder(database)
          .withName(name)
          .withVertexTypes("node")
          .withEdgeTypes("road")
          .withEdgeProperties("weight")
          .build();
    }

    /** {@code bellmanFord(A, D, 'weight')} - three arguments, so the direction defaults to BOTH. */
    @SuppressWarnings("unchecked")
    private List<RID> bellmanFordPath() {
      database.begin();
      try {
        final BasicCommandContext context = new BasicCommandContext();
        context.setDatabase(database);
        return (List<RID>) new SQLFunctionBellmanFord().execute(null, null, null,
            new Object[] { nodes[0], nodes[3], "'weight'" }, context);
      } finally {
        database.rollback();
      }
    }

    private List<RID> astarPath(final String direction) {
      database.begin();
      try {
        final BasicCommandContext context = new BasicCommandContext();
        context.setDatabase(database);
        final Map<String, Object> options = new HashMap<>();
        options.put("direction", direction);
        options.put("edgeTypeNames", new String[] { "road" });
        return new SQLFunctionAstar().execute(null, null, null,
            new Object[] { nodes[0], nodes[3], "'weight'", options }, context);
      } finally {
        database.rollback();
      }
    }

    private RID rid(final int index) {
      return nodes[index].getIdentity();
    }
  }
}
