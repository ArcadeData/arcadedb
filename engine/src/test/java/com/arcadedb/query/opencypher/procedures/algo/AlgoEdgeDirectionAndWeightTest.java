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
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.graph.olap.GraphAnalyticalViewRegistry;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

/**
 * How the {@code algo.*} procedures read the edges of a graph: direction, parallel edges and self-loops, and which neighbour an edge weight belongs to.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class AlgoEdgeDirectionAndWeightTest {
  /**
   * Issue #10203: {@code algo.triangleCount} counted a self-loop, a parallel edge and an edge stored in both directions
   * as extra triangles, and {@code algo.localClusteringCoefficient}'s OLTP path did the same (its CSR path already
   * reduced the graph to a simple one, so the two paths disagreed). Triangles and clustering are defined on the simple
   * undirected graph, as NetworkX {@code triangles}/{@code clustering} compute them.
   */
  @Nested
  class TriangleCountMultigraph {
    private Database database;

    @BeforeEach
    void setup() {
      final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-issue-10203-triangles");
      if (factory.exists())
        factory.open().drop();
      database = factory.create();
      database.getSchema().createVertexType("V");
      database.getSchema().createEdgeType("E1");
    }

    @AfterEach
    void teardown() {
      if (database != null) {
        database.drop();
        database = null;
      }
    }

    @Test
    void selfLoopIsNoTriangle() {
      build(1, new int[][] { { 0, 0 } });
      assertThat(triangles(1)).containsExactly(0L);
    }

    @Test
    void simpleTriangleControl() {
      build(3, new int[][] { { 0, 1 }, { 1, 2 }, { 2, 0 } });
      assertThat(triangles(3)).containsExactly(1L, 1L, 1L);
    }

    @Test
    void parallelEdgeIsNotAnotherTriangle() {
      build(3, new int[][] { { 0, 1 }, { 0, 1 }, { 1, 2 }, { 2, 0 } });
      assertThat(triangles(3)).containsExactly(1L, 1L, 1L);
      assertThat(triangleCountCoefficients(3)).containsExactly(1.0, 1.0, 1.0);
    }

    @Test
    void reciprocalEdgeIsNotAnotherTriangle() {
      build(3, new int[][] { { 0, 1 }, { 1, 0 }, { 1, 2 }, { 2, 0 } });
      assertThat(triangles(3)).containsExactly(1L, 1L, 1L);
    }

    @Test
    void selfLoopOnATriangleVertexChangesNothing() {
      build(3, new int[][] { { 0, 1 }, { 1, 2 }, { 2, 0 }, { 0, 0 } });
      assertThat(triangles(3)).containsExactly(1L, 1L, 1L);
      assertThat(triangleCountCoefficients(3)).containsExactly(1.0, 1.0, 1.0);
      assertThat(localClusteringCoefficients(3)).containsExactly(1.0, 1.0, 1.0);
    }

    @Test
    void completeGraphOnFourVertices() {
      build(4, new int[][] { { 0, 1 }, { 0, 2 }, { 0, 3 }, { 1, 2 }, { 1, 3 }, { 2, 3 } });
      assertThat(triangles(4)).containsExactly(3L, 3L, 3L, 3L);
    }

    /**
     * A differential against a brute force over every vertex triple, on seeded random multigraphs with self-loops,
     * parallel and reciprocal edges: the shape of the report, kept as a guard for both procedures.
     */
    @Test
    void randomMultigraphsMatchBruteForce() {
      final Random random = new Random(10203);
      for (int round = 0; round < 6; round++) {
        final int n = 6 + random.nextInt(10);
        final int edgeCount = n * (2 + random.nextInt(3));
        final int[][] edges = new int[edgeCount][];
        for (int i = 0; i < edgeCount; i++)
          edges[i] = new int[] { random.nextInt(n), random.nextInt(n) };

        teardown();
        setup();
        build(n, edges);

        final boolean[][] adjacent = new boolean[n][n];
        for (final int[] e : edges)
          if (e[0] != e[1])
            adjacent[e[0]][e[1]] = adjacent[e[1]][e[0]] = true;

        final long[] expectedTriangles = new long[n];
        for (int a = 0; a < n; a++)
          for (int b = a + 1; b < n; b++)
            for (int c = b + 1; c < n; c++)
              if (adjacent[a][b] && adjacent[b][c] && adjacent[a][c]) {
                expectedTriangles[a]++;
                expectedTriangles[b]++;
                expectedTriangles[c]++;
              }

        final double[] expectedCoefficients = new double[n];
        for (int u = 0; u < n; u++) {
          int degree = 0;
          for (int v = 0; v < n; v++)
            if (adjacent[u][v])
              degree++;
          expectedCoefficients[u] = degree < 2 ? 0.0 : 2.0 * expectedTriangles[u] / (degree * (degree - 1.0));
        }

        assertThat(triangles(n)).as("round %d", round).containsExactly(expectedTriangles);
        final double[] fromTriangleCount = triangleCountCoefficients(n);
        final double[] fromLocalClustering = localClusteringCoefficients(n);
        for (int u = 0; u < n; u++) {
          assertThat(fromTriangleCount[u]).as("round %d, uid %d", round, u).isCloseTo(expectedCoefficients[u], within(1e-12));
          assertThat(fromLocalClustering[u]).as("round %d, uid %d", round, u).isCloseTo(expectedCoefficients[u], within(1e-12));
        }
      }
    }

    private void build(final int n, final int[][] edges) {
      database.transaction(() -> {
        final MutableVertex[] v = new MutableVertex[n];
        for (int i = 0; i < n; i++)
          v[i] = database.newVertex("V").set("uid", i).save();
        for (final int[] e : edges)
          v[e[0]].newEdge("E1", v[e[1]]);
      });
    }

    private long[] triangles(final int n) {
      final long[] out = new long[n];
      try (final ResultSet rs = database.query("opencypher",
          "CALL algo.triangleCount('E1') YIELD node, triangles RETURN node.uid AS u, triangles")) {
        while (rs.hasNext()) {
          final Result r = rs.next();
          out[r.<Integer>getProperty("u")] = r.<Number>getProperty("triangles").longValue();
        }
      }
      return out;
    }

    private double[] triangleCountCoefficients(final int n) {
      return doubles(n, "CALL algo.triangleCount('E1') YIELD node, clusteringCoefficient RETURN node.uid AS u, clusteringCoefficient AS c");
    }

    private double[] localClusteringCoefficients(final int n) {
      return doubles(n, "CALL algo.localClusteringCoefficient('E1') YIELD node, localClusteringCoefficient RETURN node.uid AS u, localClusteringCoefficient AS c");
    }

    private double[] doubles(final int n, final String query) {
      final double[] out = new double[n];
      try (final ResultSet rs = database.query("opencypher", query)) {
        while (rs.hasNext()) {
          final Result r = rs.next();
          out[r.<Integer>getProperty("u")] = r.<Number>getProperty("c").doubleValue();
        }
      }
      return out;
    }
  }

  /**
   * Regression tests for issue #9766: {@code algo.bridges} ran Tarjan's undirected bridge algorithm over the OUTGOING
   * adjacency only, so whether an edge was a bridge depended on which way it was stored and on the order the edges were
   * enumerated in. It now walks the undirected adjacency through the DFS {@code algo.articulationPoints} and
   * {@code algo.biconnectedComponents} share, which also skips the tree edge by occurrence, so a doubled edge is not a
   * bridge.
   */
  @Nested
  class BridgesUndirected {
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

  /**
   * Regression tests for #9914: {@code algo.kShortestPaths} only followed outgoing edges and took no direction, so on a
   * graph stored in one direction and walked in both (roads) its first path was not the one {@code algo.dijkstra} returns
   * for the same arguments, or there was none at all. It now takes {@code direction} with the same {@code BOTH} default.
   */
  @Nested
  class KShortestPathsDirection extends TestHelper {

    @Override
    protected void beginTest() {
      database.command("sql", "CREATE VERTEX TYPE City");
      database.command("sql", "CREATE EDGE TYPE ROAD");
    }

    @Test
    void walksBothDirectionsByDefaultLikeDijkstra() {
      reporterGraph();
      assertThat(dijkstraWeight("A", "C")).isEqualTo(3.0);

      final List<Result> paths = kShortest("A", "C", 3, "");
      assertThat(paths).hasSize(1);
      assertThat(paths.get(0).<Number>getProperty("weight").doubleValue()).isEqualTo(3.0);
      assertThat(paths.get(0).<Number>getProperty("rank").intValue()).isEqualTo(1);
    }

    @Test
    void theDirectionArgumentRestrictsTheWalk() {
      reporterGraph();
      assertThat(kShortest("A", "C", 3, ", 'OUT'")).isEmpty();
      assertThat(kShortest("A", "C", 3, ", 'IN'")).isEmpty();
      assertThat(kShortest("A", "B", 3, ", 'OUT'")).hasSize(1);
      assertThat(kShortest("B", "A", 3, ", 'IN'")).hasSize(1);
      assertThat(kShortest("A", "C", 3, ", 'BOTH'")).hasSize(1);
    }

    @Test
    void theYieldedPathCarriesItsRelationships() {
      reporterGraph();
      final TraversalPath path = kShortest("A", "C", 1, "").get(0).getProperty("path");
      final List<Object> names = new ArrayList<>();
      for (final Vertex node : path.getVertices())
        names.add(node.getString("name"));
      assertThat(names).containsExactly("A", "B", "C");
      assertThat(path.getEdges()).hasSize(2);
      assertThat(path.length()).isEqualTo(2);
    }

    /**
     * Rank 1 must be the path {@code algo.dijkstra} finds, in every direction, on a graph with edges stored both ways,
     * parallel edges and edges that are not walkable.
     */
    @Test
    void rankOneAlwaysEqualsDijkstra() {
      final Random random = new Random(9914);
      final int nodes = 30;
      database.transaction(() -> {
        final List<MutableVertex> cities = new ArrayList<>();
        for (int i = 0; i < nodes; i++)
          cities.add(database.newVertex("City").set("name", "N" + i).save());
        for (int i = 0; i < 90; i++) {
          final MutableVertex from = cities.get(random.nextInt(nodes));
          final MutableVertex to = cities.get(random.nextInt(nodes));
          from.newEdge("ROAD", to, "km", 1 + random.nextInt(20)).save();
        }
      });

      for (int i = 0; i < 20; i++) {
        final String from = "N" + random.nextInt(nodes);
        final String to = "N" + random.nextInt(nodes);
        if (from.equals(to))
          continue;
        for (final String direction : new String[] { "BOTH", "OUT", "IN" }) {
          final Double dijkstra = dijkstraWeight(from, to, direction);
          final List<Result> paths = kShortest(from, to, 4, ", '" + direction + "'");
          if (dijkstra == null) {
            assertThat(paths).as(from + "->" + to + " " + direction).isEmpty();
            continue;
          }
          assertThat(paths).as(from + "->" + to + " " + direction).isNotEmpty();
          assertThat(paths.get(0).<Number>getProperty("weight").doubleValue()).as(from + "->" + to + " " + direction)
              .isEqualTo(dijkstra);
          double previous = 0;
          for (final Result path : paths) {
            final double weight = path.<Number>getProperty("weight").doubleValue();
            assertThat(weight).isGreaterThanOrEqualTo(previous);
            previous = weight;
          }
        }
      }
    }

    @Test
    void edgeCases() {
      reporterGraph();
      // the start is the end: one path, no relationship, weight 0
      final List<Result> self = kShortest("A", "A", 3, "");
      assertThat(self).hasSize(1);
      assertThat(self.get(0).<Number>getProperty("weight").doubleValue()).isZero();
      // more paths asked than exist: as many as exist
      assertThat(kShortest("A", "C", 50, "")).hasSize(1);
      // a direction and a type filter together
      assertThat(kShortest("B", "A", 3, ", 'IN'")).hasSize(1);
      assertThat(kShortest("B", "C", 3, ", 'IN'")).hasSize(1);
      assertThat(kShortest("C", "B", 3, ", 'IN'")).isEmpty();
      // a direction that is none of the three is refused, not read as a default
      assertThatThrownBy(() -> kShortest("A", "C", 3, ", 'SIDEWAYS'")).hasStackTraceContaining("unknown direction 'SIDEWAYS'");
    }

    @Test
    @SuppressWarnings("unchecked")
    void lightweightEdgesAreWalkedAndReported() {
      database.transaction(() -> {
        final MutableVertex a = database.newVertex("City").set("name", "A").save();
        final MutableVertex b = database.newVertex("City").set("name", "B").save();
        final MutableVertex c = database.newVertex("City").set("name", "C").save();
        a.newLightEdge("ROAD", b);
        c.newLightEdge("ROAD", b);
      });
      final List<Result> paths = kShortest("A", "C", 2, "", "");
      assertThat(paths).hasSize(1);
      assertThat(paths.get(0).<Number>getProperty("weight").doubleValue()).isEqualTo(2.0);
      assertThat(paths.get(0).<TraversalPath>getProperty("path").getEdges()).hasSize(2);
      // the path algo.dijkstra yields used to fail building on a lightweight edge, which has no record to look up
      assertThat(dijkstraWeight("A", "C")).isEqualTo(2.0);
    }

    private void reporterGraph() {
      database.transaction(() -> {
        database.command("sql", "INSERT INTO City SET name = 'A'");
        database.command("sql", "INSERT INTO City SET name = 'B'");
        database.command("sql", "INSERT INTO City SET name = 'C'");
        database.command("sql",
            "CREATE EDGE ROAD FROM (SELECT FROM City WHERE name = 'A') TO (SELECT FROM City WHERE name = 'B') SET km = 1");
        database.command("sql",
            "CREATE EDGE ROAD FROM (SELECT FROM City WHERE name = 'C') TO (SELECT FROM City WHERE name = 'B') SET km = 2");
      });
    }

    private Double dijkstraWeight(final String from, final String to) {
      return dijkstraWeight(from, to, "BOTH");
    }

    private Double dijkstraWeight(final String from, final String to, final String direction) {
      try (final ResultSet rs = database.query("opencypher",
          "MATCH (a:City {name: $from}), (b:City {name: $to}) CALL algo.dijkstra(a, b, 'ROAD', 'km', $dir) YIELD weight RETURN weight",
          Map.of("from", from, "to", to, "dir", direction))) {
        return rs.hasNext() ? rs.next().<Number>getProperty("weight").doubleValue() : null;
      }
    }

    private List<Result> kShortest(final String from, final String to, final int k, final String direction) {
      return kShortest(from, to, k, ", 'km'", direction);
    }

    private List<Result> kShortest(final String from, final String to, final int k, final String weight, final String direction) {
      final List<Result> rows = new ArrayList<>();
      try (final ResultSet rs = database.query("opencypher",
          "MATCH (a:City {name: $from}), (b:City {name: $to}) CALL algo.kShortestPaths(a, b, " + k + ", 'ROAD'" + weight + direction
              + ") YIELD path, weight, rank RETURN path, weight, rank", Map.of("from", from, "to", to))) {
        while (rs.hasNext())
          rows.add(rs.next());
      }
      return rows;
    }
  }

  /**
   * Issue #11146: {@code algo.personalizedPageRank} walked OUT only and read no edge weight, so a vertex nothing points at yet
   * (every record just written, linking to existing ones) scored 0 from any source, and a similarity graph could not weigh its
   * edges. A config map as second argument now takes {@code direction} and {@code weightProperty}, under the names and values
   * of {@code algo.pagerank}, with OUT as default so the positional form answers as before.
   */
  @Nested
  class PersonalizedPageRankDirection extends TestHelper {

    @Test
    void theReportedVertexWithOnlyOutgoingEdgesIsReachedWithBoth() throws Exception {
      // in -> source -> out
      buildGraph(new String[] { "in", "source", "out" }, new String[][] { { "in", "source", "1" }, { "source", "out", "1" } });
      for (final boolean view : new boolean[] { false, true }) {
        if (view)
          createView();

        final Map<String, Double> outDefault = scores("source", null);
        assertThat(outDefault.get("in")).as("OUT, the default, as before (view=%s)", view).isEqualTo(0.0);
        assertThat(outDefault.get("out")).isGreaterThan(0.0);
        assertThat(scores("source", "{direction: 'OUT'}")).isEqualTo(outDefault);

        final Map<String, Double> both = scores("source", "{direction: 'BOTH'}");
        assertThat(both.get("in")).as("view=%s", view).isGreaterThan(0.0);
        assertThat(both.get("in")).isCloseTo(both.get("out"), within(1e-9));
        assertThat(sum(both)).isCloseTo(1.0, within(1e-6));

        final Map<String, Double> in = scores("source", "{direction: 'in'}");
        assertThat(in.get("in")).as("view=%s", view).isGreaterThan(0.0);
        assertThat(in.get("out")).isEqualTo(0.0);
        assertThat(in.get("in")).isCloseTo(outDefault.get("out"), within(1e-9));
      }
    }

    @Test
    void edgeWeightsSplitTheScoreInProportion() throws Exception {
      // source -> a (3), source -> b (1)
      buildGraph(new String[] { "source", "a", "b" }, new String[][] { { "source", "a", "3" }, { "source", "b", "1" } });
      for (final boolean view : new boolean[] { false, true }) {
        if (view)
          createView();
        final Map<String, Double> unweighted = scores("source", "{}");
        assertThat(unweighted.get("a")).isCloseTo(unweighted.get("b"), within(1e-9));

        final Map<String, Double> weighted = scores("source", "{weightProperty: 'w'}");
        assertThat(weighted.get("a")).as("view=%s", view).isCloseTo(3.0 * weighted.get("b"), within(1e-9));
        assertThat(weighted.get("source")).isCloseTo(unweighted.get("source"), within(1e-9));
        assertThat(sum(weighted)).isCloseTo(1.0, within(1e-6));
      }
    }

    @Test
    void weightsCombineWithDirection() {
      // a -> source (3), b -> source (1): only reached walking backwards
      buildGraph(new String[] { "source", "a", "b" }, new String[][] { { "a", "source", "3" }, { "b", "source", "1" } });
      final Map<String, Double> out = scores("source", "{weightProperty: 'w'}");
      assertThat(out.get("a")).isEqualTo(0.0);
      final Map<String, Double> in = scores("source", "{weightProperty: 'w', direction: 'IN'}");
      assertThat(in.get("a")).isCloseTo(3.0 * in.get("b"), within(1e-9));
    }

    @Test
    void configMapCarriesEveryKnobOfThePositionalForm() {
      buildGraph(new String[] { "a", "b", "c", "d" }, new String[][] { { "a", "b", "1" }, { "b", "c", "1" }, { "c", "d", "1" } });
      assertThat(scores("a", "{relTypes: 'E', dampingFactor: 0.5, maxIterations: 7, tolerance: 0.0000001}"))
          .isEqualTo(scoresOf("MATCH (s:N {k: 'a'}) CALL algo.personalizedPageRank(s, 'E', 0.5, 7, 0.0000001) YIELD nodeId, score "
              + "RETURN nodeId.k AS k, score"));
    }

    @Test
    void invalidConfigurationsAreRefused() {
      buildGraph(new String[] { "a", "b" }, new String[][] { { "a", "b", "-1" } });
      assertThatThrownBy(() -> scores("a", "{directon: 'BOTH'}")).hasMessageContaining("unknown config key 'directon'");
      assertThatThrownBy(() -> scores("a", "{direction: 'INCOMING'}")).hasMessageContaining("unknown direction 'INCOMING'");
      assertThatThrownBy(() -> scores("a", "{weightProperty: 'w'}")).hasMessageContaining("finite and non-negative");
      assertThatThrownBy(() -> scores("a", "{dampingFactor: 'high'}")).hasMessageContaining("dampingFactor must be a number");
      assertThatThrownBy(() -> scoresOf("MATCH (s:N {k: 'a'}) CALL algo.personalizedPageRank(s, {direction: 'BOTH'}, 0.85) "
          + "YIELD nodeId, score RETURN nodeId.k AS k, score")).hasMessageContaining("there is no third one");
    }

    private void buildGraph(final String[] names, final String[][] edges) {
      database.command("sql", "CREATE VERTEX TYPE N");
      database.command("sql", "CREATE EDGE TYPE E");
      database.transaction(() -> {
        final Map<String, MutableVertex> byName = new TreeMap<>();
        for (final String name : names)
          byName.put(name, database.newVertex("N").set("k", name).save());
        for (final String[] edge : edges)
          byName.get(edge[0]).newEdge("E", byName.get(edge[1]), "w", Double.parseDouble(edge[2]));
      });
    }

    private void createView() throws Exception {
      database.command("sql", "CREATE GRAPH ANALYTICAL VIEW ppr11146 VERTEX TYPES (N) EDGE TYPES (E)");
      final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "ppr11146");
      assertThat(view.awaitReady(60, TimeUnit.SECONDS)).isTrue();
    }

    private Map<String, Double> scores(final String source, final String config) {
      return scoresOf("MATCH (s:N {k: '" + source + "'}) CALL algo.personalizedPageRank(s" + (config != null ? ", " + config : "")
          + ") YIELD nodeId, score RETURN nodeId.k AS k, score");
    }

    private Map<String, Double> scoresOf(final String cypher) {
      final Map<String, Double> byK = new TreeMap<>();
      try (final ResultSet rs = database.query("opencypher", cypher)) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          byK.put(row.getProperty("k"), ((Number) row.getProperty("score")).doubleValue());
        }
      }
      return byK;
    }

    private static double sum(final Map<String, Double> scores) {
      return scores.values().stream().mapToDouble(Double::doubleValue).sum();
    }
  }

  /**
   * Regression tests for issue #6301 - {@code algo.steinerTree} paired an edge weight with the neighbour it was
   * <em>iterated at</em> rather than with the neighbour it belongs to.
   * <p>
   * The adjacency list was built with the {@code relTypes} filter applied and in the backing store's order; the
   * weight array beside it was filled positionally from an <em>unfiltered</em> {@code getEdges(BOTH)} walk in OLTP
   * order. Nothing reconciled the two, so {@code adjW[i][j]} was "the weight of the j-th edge I happened to see",
   * not "the weight of the edge to {@code adj[i][j]}". That array is what Dijkstra runs on in step 1, so the tree
   * itself moved, not merely the {@code weight} column of the result.
   * <p>
   * Two shapes make it visible, and both are pinned here:
   * <ul>
   *   <li>an edge type the caller excluded still lands its weight on an included edge;</li>
   *   <li>the same query answers differently depending on whether a {@link GraphAnalyticalView} happens to exist,
   *       because the CSR neighbour order need not match the OLTP one. A view is meant to be a transparent
   *       accelerator, so this is the worst failure mode it has.</li>
   * </ul>
   */
  @Nested
  class SteinerTreeWeightAlignment {
    private Database database;
    /** Whether the last {@link #apspDistance} call actually went through a provider rather than the OLTP records. */
    private boolean  csrAccelerated;

    @BeforeEach
    void setup() {
      final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-issue-6301-steiner-weights");
      if (factory.exists())
        factory.open().drop();
      database = factory.create();
      database.getSchema().createVertexType("N");
      // The weight is a DECLARED property on both edge types: a Graph Analytical View only materialises an edge
      // property column for a property the schema knows about, and without the column the view falls back to the
      // edge records - which would leave the columnar half of the fix untested.
      database.getSchema().createEdgeType("ROAD").createProperty("w", Type.DOUBLE);
      database.getSchema().createEdgeType("NOISE").createProperty("w", Type.DOUBLE);
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
    void anExcludedEdgeTypeDoesNotLendItsWeightToAnIncludedEdge() {
      // X -ROAD(1)- Y -ROAD(1)- Z, plus one NOISE(999) edge hanging off X. `relTypes='ROAD'` excludes NOISE
      // entirely, so the tree connecting X and Z is X-Y-Z and costs 2.0.
      //
      // Positionally, the NOISE edge was the first edge X's unfiltered walk produced, so its 999.0 landed on
      // slot 0 of X's ROAD-only adjacency - the X-Y edge - and the answer came back 500x too expensive.
      database.transaction(() -> {
        final MutableVertex x = database.newVertex("N").set("name", "X").save();
        final MutableVertex y = database.newVertex("N").set("name", "Y").save();
        final MutableVertex z = database.newVertex("N").set("name", "Z").save();
        final MutableVertex noise = database.newVertex("N").set("name", "NOISE_TARGET").save();
        x.newEdge("ROAD", y, true, new Object[] { "w", 1.0 }).save();
        y.newEdge("ROAD", z, true, new Object[] { "w", 1.0 }).save();
        x.newEdge("NOISE", noise, true, new Object[] { "w", 999.0 }).save();
      });

      final List<Result> rows = steinerTree("X", "Z", "ROAD", "w");

      assertThat(rows).as("the tree connecting X and Z is X-Y-Z, two edges").hasSize(2);
      for (final Result row : rows) {
        assertThat(((Number) row.getProperty("weight")).doubleValue())
            .as("every ROAD edge of the tree weighs 1.0; 999.0 belongs to the excluded NOISE edge")
            .isEqualTo(1.0);
        assertThat(((Number) row.getProperty("totalWeight")).doubleValue()).isEqualTo(2.0);
      }
    }

    @Test
    void theAnalyticalViewDoesNotChangeTheTree() {
      // One edge type, no filter subtlety at all: X has two ROAD neighbours (Y at 1.0 and Z at 50.0) and Y-Z
      // costs 1.0, so the cheapest tree joining X and Z is X-Y-Z at 2.0.
      //
      // The OLTP walk produced X's edges in creation order and the CSR produces them in dense-id order, so the
      // positional pairing swapped 1.0 and 50.0 exactly when a view existed - and Dijkstra then preferred the
      // direct 50.0 hop, returning a 51.0 tree for a query that had returned 2.0 a moment earlier.
      database.transaction(() -> {
        final MutableVertex x = database.newVertex("N").set("name", "X").save();
        final MutableVertex y = database.newVertex("N").set("name", "Y").save();
        final MutableVertex z = database.newVertex("N").set("name", "Z").save();
        x.newEdge("ROAD", y, true, new Object[] { "w", 1.0 }).save();
        x.newEdge("ROAD", z, true, new Object[] { "w", 50.0 }).save();
        y.newEdge("ROAD", z, true, new Object[] { "w", 1.0 }).save();
      });

      final List<Result> oltp = steinerTree("X", "Z", "ROAD", "w");
      assertThat(totalWeightOf(oltp)).as("OLTP: the cheapest tree is X-Y-Z").isEqualTo(2.0);
      assertThat(oltp).hasSize(2);

      final GraphAnalyticalView view = GraphAnalyticalView.builder(database)
          .withName("steiner-view")
          .withVertexTypes("N")
          .withEdgeTypes("ROAD")
          .withEdgeProperties("w")
          .build();
      try {
        final List<Result> csr = steinerTree("X", "Z", "ROAD", "w");
        assertThat(totalWeightOf(csr))
            .as("a Graph Analytical View is a transparent accelerator: same query, same tree")
            .isEqualTo(2.0);
        assertThat(csr).hasSize(2);
        for (final Result row : csr)
          assertThat(((Number) row.getProperty("weight")).doubleValue()).isEqualTo(1.0);
      } finally {
        view.drop();
      }
    }

    @Test
    void theViewIsActuallyExercised() {
      // The counterweight to the test above: if the view were silently ignored, both halves of it would be the
      // OLTP run and it would pin nothing. CSR_ACCELERATED_VAR is what the procedure sets when loadGraph()
      // resolves a provider, so asserting on it is what makes the comparison meaningful.
      database.transaction(() -> {
        final MutableVertex x = database.newVertex("N").set("name", "X").save();
        final MutableVertex y = database.newVertex("N").set("name", "Y").save();
        x.newEdge("ROAD", y, true, new Object[] { "w", 3.0 }).save();
      });

      final GraphAnalyticalView view = GraphAnalyticalView.builder(database)
          .withName("steiner-view-probe")
          .withVertexTypes("N")
          .withEdgeTypes("ROAD")
          .withEdgeProperties("w")
          .build();
      try {
        database.begin();
        try {
          final BasicCommandContext context = new BasicCommandContext();
          context.setDatabase(database);
          final List<Result> rows = collect(new AlgoSteinerTree().execute(
              new Object[] { List.of(vertexNamed("X"), vertexNamed("Y")), "ROAD", "w" }, null, context));

          assertThat(context.getVariable(CommandContext.CSR_ACCELERATED_VAR))
              .as("the view must actually back the call")
              .isEqualTo(true);
          assertThat(rows).hasSize(1);
          assertThat(((Number) rows.getFirst().getProperty("weight")).doubleValue()).isEqualTo(3.0);
        } finally {
          database.rollback();
        }
      } finally {
        view.drop();
      }
    }

    @Test
    void parallelEdgesOfTheSamePairKeepTheirOwnWeights() {
      // Two ROAD edges join X and Y, at 7.0 and at 2.0. Whichever adjacency slot each lands in, the pair of
      // weights present must be {2.0, 7.0} - so the shortest path from X to Y costs 2.0 and not 7.0. Pairing by
      // neighbour rather than by position has to survive a neighbour appearing more than once.
      database.transaction(() -> {
        final MutableVertex x = database.newVertex("N").set("name", "X").save();
        final MutableVertex y = database.newVertex("N").set("name", "Y").save();
        x.newEdge("ROAD", y, true, new Object[] { "w", 7.0 }).save();
        x.newEdge("ROAD", y, true, new Object[] { "w", 2.0 }).save();
      });

      final List<Result> rows = steinerTree("X", "Y", "ROAD", "w");
      assertThat(rows).hasSize(1);
      assertThat(totalWeightOf(rows)).isEqualTo(2.0);
    }

    @Test
    void weightsSurviveAViewThatDidNotMaterialiseThem() {
      // algo.bellmanford took the CSR path on the strength of the adjacency alone and then, finding no property
      // column for `w`, used a unit weight for EVERY edge - silently, and only when a view existed. X-Y-Z costs
      // 2.0 and the direct X-Z hop costs 50.0, so unit weights invert the answer: the direct hop wins at 1.0.
      //
      // The view here deliberately materialises no edge properties, which is the configuration that triggered it.
      database.transaction(() -> {
        final MutableVertex x = database.newVertex("N").set("name", "X").save();
        final MutableVertex y = database.newVertex("N").set("name", "Y").save();
        final MutableVertex z = database.newVertex("N").set("name", "Z").save();
        x.newEdge("ROAD", y, true, new Object[] { "w", 1.0 }).save();
        y.newEdge("ROAD", z, true, new Object[] { "w", 1.0 }).save();
        x.newEdge("ROAD", z, true, new Object[] { "w", 50.0 }).save();
      });

      assertThat(bellmanFordWeight()).as("OLTP: X-Y-Z at 2.0 beats the direct 50.0 hop").isEqualTo(2.0);

      final GraphAnalyticalView view = GraphAnalyticalView.builder(database)
          .withName("bellman-view-no-edge-properties")
          .withVertexTypes("N")
          .withEdgeTypes("ROAD")
          .build();
      try {
        assertThat(bellmanFordWeight())
            .as("a view without the property column must send the weights back to the edge records, not to 1.0")
            .isEqualTo(2.0);
      } finally {
        view.drop();
      }
    }

    @Test
    void everyEdgeTypeIsWeightedWhenNoFilterIsGiven() {
      // With no relTypes the CSR weight lookup had no type to address its property columns by, and produced an
      // empty weight row against a non-empty neighbour row - an ArrayIndexOutOfBoundsException out of algo.apsp
      // for a query that worked perfectly without the view. Two edge types make the "all types" path the one
      // under test rather than the single-type shortcut.
      database.transaction(() -> {
        final MutableVertex x = database.newVertex("N").set("name", "X").save();
        final MutableVertex y = database.newVertex("N").set("name", "Y").save();
        final MutableVertex z = database.newVertex("N").set("name", "Z").save();
        x.newEdge("ROAD", y, true, new Object[] { "w", 2.0 }).save();
        y.newEdge("NOISE", z, true, new Object[] { "w", 3.0 }).save();
      });

      assertThat(apspDistance("X", "Z")).as("OLTP: 2.0 + 3.0 across the two edge types").isEqualTo(5.0);

      final GraphAnalyticalView view = GraphAnalyticalView.builder(database)
          .withName("apsp-view-all-types")
          .withVertexTypes("N")
          .withEdgeTypes("ROAD", "NOISE")
          .withEdgeProperties("w")
          .build();
      try {
        final double distance = apspDistance("X", "Z");
        assertThat(csrAccelerated).as("the view must actually back the call, or this pins the OLTP path twice")
            .isTrue();
        assertThat(distance)
            .as("the same distance, whichever backing answered and however many edge types are in play")
            .isEqualTo(5.0);
      } finally {
        view.drop();
      }
    }

    @Test
    void aViewThatMaterialisedADifferentPropertyDoesNotMakeTheGraphUnweighted() {
      // The narrower half of the same defect, and the one a coarse "does this view have edge properties?" gate
      // walks straight into: the view materialises `other`, the query asks for `w`, so every weight lookup
      // returns null and every edge silently weighs 1.0 - which inverts the answer here, since X-Y-Z costs 2.0
      // and the direct X-Z hop costs 50.0. A null property value is also how "this edge has no value" is
      // reported, so the caller cannot tell the two apart and has to ask the sharper question up front.
      database.getSchema().getType("ROAD").createProperty("other", Type.DOUBLE);
      database.transaction(() -> {
        final MutableVertex x = database.newVertex("N").set("name", "X").save();
        final MutableVertex y = database.newVertex("N").set("name", "Y").save();
        final MutableVertex z = database.newVertex("N").set("name", "Z").save();
        x.newEdge("ROAD", y, true, new Object[] { "w", 1.0, "other", 1.0 }).save();
        y.newEdge("ROAD", z, true, new Object[] { "w", 1.0, "other", 1.0 }).save();
        x.newEdge("ROAD", z, true, new Object[] { "w", 50.0, "other", 1.0 }).save();
      });

      final GraphAnalyticalView view = GraphAnalyticalView.builder(database)
          .withName("steiner-view-other-property")
          .withVertexTypes("N")
          .withEdgeTypes("ROAD")
          .withEdgeProperties("other")
          .build();
      try {
        final List<Result> rows = steinerTree("X", "Z", "ROAD", "w");
        assertThat(totalWeightOf(rows))
            .as("the view holds `other`, not `w`, so the weights must come from the edge records")
            .isEqualTo(2.0);
      } finally {
        view.drop();
      }
    }

    @Test
    void anActiveOverlayDoesNotLendItsWeightsToTheWrongEdges() {
      // The overlay case, exercised through algo.bellmanford so the alignment is pinned for a caller outside the
      // procedures rewritten here. Edge property columns are aligned with the base CSR's forward slots, while
      // getNeighborIds serves the overlay's view of the node - deletions dropped, additions merged, the list
      // re-sorted - so the n-th neighbour is not the n-th edge of the column store. This test was written when
      // the answer to that was for the provider to report no edge properties at all while an overlay was active;
      // since issue #6315 it resolves the two against each other instead and keeps answering exactly, which is
      // what the weight assertion below has always been about.
      database.transaction(() -> {
        final MutableVertex x = database.newVertex("N").set("name", "X").save();
        final MutableVertex y = database.newVertex("N").set("name", "Y").save();
        final MutableVertex z = database.newVertex("N").set("name", "Z").save();
        x.newEdge("ROAD", y, true, new Object[] { "w", 1.0 }).save();
        y.newEdge("ROAD", z, true, new Object[] { "w", 1.0 }).save();
        x.newEdge("ROAD", z, true, new Object[] { "w", 50.0 }).save();
      });

      final GraphAnalyticalView view = GraphAnalyticalView.builder(database)
          .withName("overlay-view")
          .withVertexTypes("N")
          .withEdgeTypes("ROAD")
          .withEdgeProperties("w")
          .withUpdateMode(GraphAnalyticalView.UpdateMode.SYNCHRONOUS)
          .build();
      try {
        assertThat(view.hasEdgeProperties()).as("no overlay yet, so the columns are addressable").isTrue();

        // A committed change the synchronous view absorbs into its delta overlay rather than by rebuilding.
        database.transaction(() -> {
          final MutableVertex extra = database.newVertex("N").set("name", "W").save();
          vertexOf("X").newEdge("ROAD", extra, true, new Object[] { "w", 7.0 }).save();
        });

        assertThat(view.hasEdgeProperties())
            .as("an overlay is resolved against the columns rather than disqualifying them (issue #6315)")
            .isTrue();
        assertThat(view.hasEdgeProperty("ROAD", "w")).isTrue();
        assertThat(bellmanFordWeight()).as("X-Y-Z at 2.0, each weight on its own edge").isEqualTo(2.0);
      } finally {
        view.drop();
      }
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    /** The weight of the {@code algo.bellmanford} path from X to Z over the {@code w} property. */
    private double bellmanFordWeight() {
      database.begin();
      try {
        final BasicCommandContext context = new BasicCommandContext();
        context.setDatabase(database);
        final List<Result> rows = collect(new AlgoBellmanFord().execute(
            new Object[] { vertexNamed("X"), vertexNamed("Z"), "ROAD", "w" }, null, context));
        assertThat(rows).hasSize(1);
        return ((Number) rows.getFirst().getProperty("weight")).doubleValue();
      } finally {
        database.rollback();
      }
    }

    /** The {@code algo.apsp} distance between two named vertices over the {@code w} property, all edge types. */
    private double apspDistance(final String from, final String to) {
      database.begin();
      try {
        final BasicCommandContext context = new BasicCommandContext();
        context.setDatabase(database);
        final RID fromRid = ((Vertex) vertexNamed(from)).getIdentity();
        final RID toRid = ((Vertex) vertexNamed(to)).getIdentity();
        final List<Result> rows = collect(new AlgoAPSP().execute(new Object[] { null, "w" }, null, context));
        csrAccelerated = Boolean.TRUE.equals(context.getVariable(CommandContext.CSR_ACCELERATED_VAR));
        for (final Result row : rows)
          if (fromRid.equals(row.getProperty("source")) && toRid.equals(row.getProperty("target")))
            return ((Number) row.getProperty("distance")).doubleValue();
        throw new AssertionError(from + " -> " + to + " is not in the apsp result");
      } finally {
        database.rollback();
      }
    }

    /**
     * Calls the procedure directly rather than through Cypher: the two terminals are looked up by name and handed
     * over as vertices, which is what the CALL form does anyway, and it keeps the assertions about the procedure
     * rather than about the planner. Requires an active transaction, like every direct use of the graph API.
     */
    private List<Result> steinerTree(final String from, final String to, final String relTypes,
        final String weightProperty) {
      database.begin();
      try {
        final BasicCommandContext context = new BasicCommandContext();
        context.setDatabase(database);
        return collect(new AlgoSteinerTree().execute(
            new Object[] { List.of(vertexNamed(from), vertexNamed(to)), relTypes, weightProperty }, null, context));
      } finally {
        database.rollback();
      }
    }

    private Vertex vertexOf(final String name) {
      return (Vertex) vertexNamed(name);
    }

    private Object vertexNamed(final String name) {
      return database.query("sql", "SELECT FROM N WHERE name = ?", name).next().getElement().orElseThrow().asVertex();
    }

    private static List<Result> collect(final Stream<Result> rows) {
      final List<Result> collected = new ArrayList<>();
      rows.forEach(collected::add);
      return collected;
    }

    private static double totalWeightOf(final List<Result> rows) {
      assertThat(rows).isNotEmpty();
      double sum = 0.0;
      for (final Result row : rows)
        sum += ((Number) row.getProperty("weight")).doubleValue();
      // totalWeight repeats on every row and must agree with the edges actually returned.
      for (final Result row : rows)
        assertThat(((Number) row.getProperty("totalWeight")).doubleValue()).isEqualTo(sum);
      return sum;
    }

    @SuppressWarnings("unused")
    private static String nameOf(final Object rid) {
      return ((RID) rid).asVertex().getString("name");
    }
  }

  /**
   * Regression test for issue #6376 - {@code algo.maxKCut} paired an edge weight with the neighbour
   * it was <em>iterated at</em> rather than with the neighbour it belongs to, the same defect class
   * #6301 fixed for {@code algo.apsp}, {@code algo.bellmanford}, {@code algo.steinerTree} and the
   * kShortestPaths CSR path.
   * <p>
   * {@code buildWeightedAdj} filled a weight array positionally against
   * {@code graph.adjacency(dir, relTypes)} - CSR order (sorted by dense node id after the Graph
   * Analytical View's BFS/RCM cache-locality reordering) - by walking
   * {@code graph.getVertex(i).getEdges(dir, relTypes)}, which is always OLTP (edge-record) order.
   * When a view exists the two orders diverge - confirmed for the graph below by printing both
   * orders directly - so {@code adjW[i][j]} ends up being the weight of whatever edge OLTP
   * iteration happened to visit at position {@code j}, not the weight of the edge to
   * {@code adj[i][j]}.
   * <p>
   * H-A(1.0), H-B(10.0), H-C(100.0) plus one extra A-B(5.0) edge breaks the leaf symmetry a pure
   * star would have (under which a misassigned weight would just relabel an otherwise-identical
   * answer): the unique maximum 2-cut of this graph is 115.0, achieved by grouping {H, A} against
   * {B, C}. Empirically, the unfixed code returns a non-integer 110.5 through a Graph Analytical
   * View - itself a symptom of the bug: the cut sums each edge's weight from <em>both</em>
   * endpoints' adjacency rows and halves it, and misaligned rows disagree with each other - while
   * the OLTP path (no view, no misalignment) reliably returns the true 115.0, both independent of
   * seed.
   */
  @Nested
  class MaxKCutWeightAlignment {
    private Database database;

    @BeforeEach
    void setup() {
      final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-issue-6376-maxkcut-weights");
      if (factory.exists())
        factory.open().drop();
      database = factory.create();
      database.getSchema().createVertexType("N");
      // The weight is a DECLARED property: a Graph Analytical View only materialises an edge property
      // column for a property the schema knows about, and without the column the view falls back to
      // the edge records - which would leave the columnar half of the fix untested.
      database.getSchema().createEdgeType("ROAD").createProperty("w", Type.DOUBLE);

      // Star hub H with three unequal leaf weights, plus one extra A-B edge that makes the leaves
      // structurally distinguishable (without it, permuting which leaf gets the heaviest edge would
      // just relabel an equally-valued answer and could never expose a misalignment).
      database.transaction(() -> {
        final MutableVertex h = database.newVertex("N").set("name", "H").save();
        final MutableVertex a = database.newVertex("N").set("name", "A").save();
        final MutableVertex b = database.newVertex("N").set("name", "B").save();
        final MutableVertex c = database.newVertex("N").set("name", "C").save();
        h.newEdge("ROAD", c, true, new Object[] { "w", 100.0 }).save();
        h.newEdge("ROAD", b, true, new Object[] { "w", 10.0 }).save();
        h.newEdge("ROAD", a, true, new Object[] { "w", 1.0 }).save();
        a.newEdge("ROAD", b, true, new Object[] { "w", 5.0 }).save();
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
    void theAnalyticalViewDoesNotChangeTheMaxCut() {
      for (long seed = 0; seed < 10; seed++) {
        assertThat(maxKCutWeight(seed))
            .as("OLTP: the unique maximum 2-cut ({H,A} vs {B,C}) is 115.0, seed " + seed)
            .isEqualTo(115.0);
      }

      final GraphAnalyticalView view = GraphAnalyticalView.builder(database)
          .withName("maxkcut-view")
          .withVertexTypes("N")
          .withEdgeTypes("ROAD")
          .withEdgeProperties("w")
          .build();
      try {
        for (long seed = 0; seed < 10; seed++) {
          assertThat(maxKCutWeight(seed))
              .as("a Graph Analytical View is a transparent accelerator: same graph, same max-cut, seed " + seed)
              .isEqualTo(115.0);
        }
      } finally {
        view.drop();
      }
    }

    @Test
    void theViewIsActuallyExercised() {
      // The counterweight to the test above: if the view were silently ignored, both halves of it
      // would be the OLTP run and it would pin nothing. CSR_ACCELERATED_VAR is what loadGraph() sets
      // when it resolves a provider, so asserting on it is what makes the comparison meaningful.
      final GraphAnalyticalView view = GraphAnalyticalView.builder(database)
          .withName("maxkcut-view-probe")
          .withVertexTypes("N")
          .withEdgeTypes("ROAD")
          .withEdgeProperties("w")
          .build();
      try {
        database.begin();
        try {
          final BasicCommandContext context = new BasicCommandContext();
          context.setDatabase(database);
          final List<Result> rows = collect(new AlgoMaxKCut().execute(
              new Object[] { 2, Map.of("weightProperty", "w", "seed", 7L) }, null, context));

          assertThat(context.getVariable(CommandContext.CSR_ACCELERATED_VAR))
              .as("the view must actually back the call")
              .isEqualTo(true);
          assertThat(rows).hasSize(4);
          assertThat(((Number) rows.getFirst().getProperty("cutWeight")).doubleValue()).isEqualTo(115.0);
        } finally {
          database.rollback();
        }
      } finally {
        view.drop();
      }
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    /** The {@code cutWeight} of an {@code algo.maxKCut(2, ...)} run over the {@code w} property, for a fixed seed. */
    private double maxKCutWeight(final long seed) {
      database.begin();
      try {
        final BasicCommandContext context = new BasicCommandContext();
        context.setDatabase(database);
        final List<Result> rows = collect(new AlgoMaxKCut().execute(
            new Object[] { 2, Map.of("weightProperty", "w", "seed", seed) }, null, context));
        assertThat(rows).hasSize(4);
        return ((Number) rows.getFirst().getProperty("cutWeight")).doubleValue();
      } finally {
        database.rollback();
      }
    }

    private static List<Result> collect(final Stream<Result> rows) {
      final List<Result> collected = new ArrayList<>();
      rows.forEach(collected::add);
      return collected;
    }
  }
}
