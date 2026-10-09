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
package com.arcadedb.graph;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.graph.olap.GraphAnalyticalViewRegistry;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8629: an edge type declared UNIDIRECTIONAL stores only the outgoing pointer, and #8625 made the query
 * languages answer its incoming side. The graph algorithms, the path-finding functions and the path and refactor
 * procedures still read the incoming adjacency straight off the vertices, so over such a type they saw half of an
 * undirected graph, nothing at all when walking IN, and the refactor procedures dropped the incoming edges of the node
 * they absorbed or cloned.
 * <p>
 * An algorithm is a question about the graph, not about what a vertex stores, so the same graph must give the same
 * answer whichever way its edge type is stored. Every check runs the same statement against two databases holding the
 * same graph - built in the same order, so even the RIDs match - whose edge type is bidirectional in one and
 * unidirectional in the other, and compares the answers by the vertex names. A Graph Analytical View already answered
 * the incoming side (its CSR is built from the outgoing lists), so it must agree with both.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8629UnidirectionalGraphAlgorithmsTest extends TestHelper {
  private Database uni;

  @Override
  protected void beginTest() {
    buildGraph(database, true);
    uni = createDatabase(getDatabasePath() + "_uni");
    buildGraph(uni, false);
  }

  @Override
  protected void endTest() {
    if (uni != null) {
      uni.drop();
      uni = null;
    }
  }

  @Test
  void sqlPathFindingFunctions() {
    for (final String direction : new String[] { "IN", "BOTH" }) {
      assertSameFromTo("SELECT dijkstra(?, ?, 'w', '" + direction + "') AS p", "n5", "n0");
      assertSameFromTo("SELECT astar(?, ?, 'w', {direction: '" + direction + "'}) AS p", "n5", "n0");
      assertSameFromTo("SELECT bellmanFord(?, ?, 'w', '" + direction + "') AS p", "n5", "n0");
      assertSameFromTo("SELECT cchShortestPath(?, ?, 'w', '" + direction + "') AS p", "n5", "n0");
      assertSameFromTo("SELECT duanSSSP(?, ?, 'w', '" + direction + "') AS p", "n5", "n0");
    }
    // the backward half of a bidirectional search reads IN from the target even when the walk follows the edges forward
    assertSameFromTo("SELECT cchShortestPath(?, ?, 'w', 'OUT') AS p", "n0", "n5");
  }

  @Test
  void cypherPathFindingProcedures() {
    final String ends = "MATCH (a:N {name: 'n5'}), (b:N {name: 'n0'}) ";
    for (final String direction : new String[] { "IN", "BOTH" }) {
      assertSame("opencypher", ends + "CALL algo.dijkstra(a, b, 'R', 'w', '" + direction + "') YIELD path, weight RETURN path, weight",
          true);
      assertSame("opencypher", "MATCH (a:N {name: 'n5'}) CALL algo.dijkstra.singleSource(a, 'R', 'w', '" + direction
          + "') YIELD node, cost RETURN node, cost", true);
    }
    assertSame("opencypher", ends + "CALL algo.astar(a, b, 'R', 'w') YIELD path, weight RETURN path, weight", true);
    assertSame("opencypher", ends + "CALL algo.allSimplePaths(a, b, 'R', 6) YIELD path RETURN path", true);
  }

  @Test
  void wholeGraphAlgorithms() {
    assertSame("opencypher", "CALL algo.degree(null, 'BOTH') YIELD node, inDegree, outDegree, degree RETURN node, inDegree, outDegree, degree",
        true);
    assertSame("opencypher", "CALL algo.degree(null, 'IN') YIELD node, inDegree, degree RETURN node, inDegree, degree", true);
    assertSame("opencypher", "CALL algo.triangleCount('R') YIELD node, triangles RETURN node, triangles", true);
    assertSame("opencypher", "CALL algo.pageRank({direction: 'BOTH'}) YIELD node, score RETURN node, score", true);
    assertSame("opencypher", "CALL algo.pageRank({direction: 'IN'}) YIELD node, score RETURN node, score", true);
    assertSame("opencypher", "CALL algo.pageRank({direction: 'BOTH', weightProperty: 'w'}) YIELD node, score RETURN node, score", true);
    assertSame("opencypher", "CALL algo.kcore('R') YIELD node, coreNumber RETURN node, coreNumber", true);
    assertSame("opencypher", "CALL algo.closeness('R', 'BOTH') YIELD node, score RETURN node, score", true);

    assertSamePartition("CALL algo.wcc('R') YIELD node, componentId RETURN node.name AS name, componentId AS g", 2);
    assertSamePartition("CALL algo.scc('R') YIELD node, componentId RETURN node.name AS name, componentId AS g", 10);
    assertSamePartition("CALL algo.labelPropagation() YIELD node, communityId RETURN node.name AS name, communityId AS g", 0);
    assertSamePartition("CALL algo.louvain() YIELD node, communityId RETURN node.name AS name, communityId AS g", 0);
  }

  @Test
  void pathProcedures() {
    final String start = "MATCH (a:N {name: 'n5'}) ";
    assertSame("opencypher", start + "CALL path.expand(a, 'R', null, 1, 3) YIELD path RETURN path", true);
    assertSame("opencypher", start + "CALL path.expandConfig(a, {relationshipFilter: 'R', maxLevel: 3}) YIELD path RETURN path", true);
    assertSame("opencypher", start + "CALL path.expandConfig(a, {relationshipFilter: 'R', maxLevel: 3, bfs: false}) YIELD path RETURN path",
        true);
    assertSame("opencypher", start + "CALL path.subgraphNodes(a, {relationshipFilter: 'R'}) YIELD node RETURN node", true);
    assertSame("opencypher", start + "CALL path.subgraphAll(a, {relationshipFilter: 'R'}) YIELD nodes, relationships "
        + "RETURN size(nodes) AS nodes, size(relationships) AS relationships", true);
    // a spanning tree is any tree over the reachable vertices: which edges it takes depends on the order the edges are met
    assertSame("opencypher", start + "CALL path.spanningTree(a, {relationshipFilter: 'R'}) YIELD path "
        + "RETURN count(path) AS paths", true);
  }

  @Test
  void nodeFunctions() {
    assertSame("opencypher", "MATCH (n:N) RETURN n, node.degree(n, 'R', 'IN') AS d", true);
    assertSame("opencypher", "MATCH (n:N) RETURN n, node.degree(n, 'R') AS d", true);
    assertSame("opencypher", "MATCH (n:N) RETURN n, node.degree.in(n, 'R') AS d", true);
    assertSame("opencypher", "MATCH (n:N) RETURN n, node.relationship.exists(n, 'R', 'IN') AS d", true);
    assertSame("opencypher", "MATCH (n:N) RETURN n, node.relationship.types(n, 'IN') AS d", true);
  }

  @Test
  void refactorMergeNodesRewiresIncomingEdges() {
    final String query = "MATCH (a:N {name: 'n1'}), (b:N {name: 'n3'}) CALL refactor.mergeNodes([a, b]) YIELD node RETURN node";
    for (final Database db : new Database[] { database, uni })
      db.transaction(() -> db.command("opencypher", query).close());
    assertSameAfterWrite();
  }

  @Test
  void refactorCloneNodesKeepsIncomingEdges() {
    final String query = "MATCH (a:N {name: 'n3'}) CALL refactor.cloneNodesWithRelationships([a]) YIELD output "
        + "SET output.name = 'clone' RETURN output";
    for (final Database db : new Database[] { database, uni })
      db.transaction(() -> db.command("opencypher", query).close());
    assertSameAfterWrite();
    assertThat(rows(uni, "opencypher", "MATCH (x)-[:R]->(c {name: 'clone'}) RETURN count(*) AS n")).containsExactly("{n=3}");
  }

  @Test
  void labelChangeKeepsIncomingEdges() {
    final String query = "MATCH (a:N {name: 'n3'}) SET a:Extra RETURN a";
    for (final Database db : new Database[] { database, uni })
      db.transaction(() -> db.command("opencypher", query).close());
    assertSameAfterWrite();
    assertThat(rows(uni, "opencypher", "MATCH (x)-[:R]->(c {name: 'n3'}) RETURN count(*) AS n")).containsExactly("{n=3}");
  }

  /**
   * A view over the unidirectional graph agrees with the record path over both graphs. PageRank is left out: its view
   * kernel runs a fixed number of iterations while the record path stops on a tolerance, so the two differ on any graph.
   */
  @Test
  void aViewAgreesWithTheRecordPath() throws InterruptedException {
    final String[] queries = {
        "CALL algo.degree(null, 'BOTH') YIELD node, inDegree, outDegree, degree RETURN node, inDegree, outDegree, degree",
        "CALL algo.triangleCount('R') YIELD node, triangles RETURN node, triangles",
        "MATCH (a:N {name: 'n5'}) CALL algo.dijkstra.singleSource(a, 'R', 'w', 'BOTH') YIELD node, cost RETURN node, cost" };
    final List<List<String>> withoutView = new ArrayList<>();
    for (final String query : queries) {
      withoutView.add(rows(uni, "opencypher", query));
      assertThat(withoutView.getLast()).as(query).isEqualTo(rows(database, "opencypher", query));
    }

    uni.command("sql", "CREATE GRAPH ANALYTICAL VIEW algoView VERTEX TYPES (N) EDGE TYPES (R) PROPERTIES (w) UPDATE MODE OFF");
    final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(uni, "algoView");
    final long deadline = System.currentTimeMillis() + 60_000;
    while (!view.isReady() && System.currentTimeMillis() < deadline)
      Thread.sleep(20);
    assertThat(view.isReady()).isTrue();
    try {
      for (int i = 0; i < queries.length; i++)
        assertThat(rows(uni, "opencypher", queries[i])).as("with a view: %s", queries[i]).isEqualTo(withoutView.get(i));
    } finally {
      uni.command("sql", "DROP GRAPH ANALYTICAL VIEW algoView");
    }
  }

  /**
   * A bulk load that stored the incoming pointers of a unidirectional type: the incoming side is answered once, from the
   * edges that end in the vertex, and the stored pointers are not added on top of it.
   */
  @Test
  void incomingPointersStoredByABulkLoadAreNotCountedTwice() {
    final Database loaded = createDatabase(getDatabasePath() + "_batch");
    try {
      buildGraph(loaded, false, true);
      final String[] queries = {
          "CALL algo.degree(null, 'BOTH') YIELD node, inDegree, outDegree, degree RETURN node, inDegree, outDegree, degree",
          "CALL algo.triangleCount('R') YIELD node, triangles RETURN node, triangles",
          "CALL algo.kcore('R') YIELD node, coreNumber RETURN node, coreNumber",
          "MATCH (a:N {name: 'n5'}) CALL algo.dijkstra.singleSource(a, 'R', 'w', 'BOTH') YIELD node, cost RETURN node, cost",
          "MATCH (a:N {name: 'n5'}) CALL path.expand(a, 'R', null, 1, 2) YIELD path RETURN path",
          "MATCH (a:N {name: 'n5'}) CALL path.subgraphNodes(a, {relationshipFilter: 'R'}) YIELD node RETURN node",
          "MATCH (n:N) RETURN n, node.degree(n, 'R') AS d" };
      for (final String query : queries)
        assertThat(rows(loaded, "opencypher", query)).as(query).isEqualTo(rows(database, "opencypher", query));
    } finally {
      loaded.drop();
    }
  }

  private static void buildGraph(final Database db, final boolean bidirectional) {
    buildGraph(db, bidirectional, false);
  }

  /**
   * Two clusters of five vertices - the first with every edge pointing up, the second down, and one back edge closing a
   * cycle - joined by a bridge, plus an isolated pair. Every weight is a distinct power of two, so every path has a weight
   * of its own and a shortest path is never a tie.
   *
   * @param batchWithIncomingPointers load the edges with a bidirectional graph batch, which stores the incoming pointer of
   *                                  every edge whatever its type declares
   */
  private static void buildGraph(final Database db, final boolean bidirectional, final boolean batchWithIncomingPointers) {
    db.getSchema().createVertexType("N");
    db.getSchema().buildEdgeType().withName("R").withBidirectional(bidirectional).create();
    final List<RID> v = new ArrayList<>();
    db.transaction(() -> {
      for (int i = 0; i < 12; i++)
        v.add(db.newVertex("N").set("name", "n" + i).save().getIdentity());
    });
    final List<int[]> edges = new ArrayList<>();
    for (int i = 0; i < 5; i++)
      for (int j = i + 1; j < 5; j++)
        edges.add(new int[] { i, j });
    edges.add(new int[] { 2, 0 });
    for (int i = 5; i < 10; i++)
      for (int j = i + 1; j < 10; j++)
        edges.add(new int[] { j, i });
    edges.add(new int[] { 4, 5 });
    edges.add(new int[] { 10, 11 });

    if (batchWithIncomingPointers) {
      try (final GraphBatch batch = db.batch().withBidirectional(true).build()) {
        double weight = 1;
        for (final int[] edge : edges) {
          batch.newEdge(v.get(edge[0]), "R", v.get(edge[1]), "w", weight);
          weight *= 2;
        }
      }
      return;
    }
    db.transaction(() -> {
      double weight = 1;
      for (final int[] edge : edges) {
        v.get(edge[0]).asVertex().modify().newEdge("R", v.get(edge[1]), "w", weight);
        weight *= 2;
      }
    });
  }

  /** The statement answers the same on both graphs, and (when asked) something rather than nothing. */
  private void assertSame(final String language, final String query, final boolean notEmpty) {
    final List<String> expected = rows(database, language, query);
    if (notEmpty)
      assertThat(expected).as("bidirectional: %s", query).isNotEmpty().anyMatch(r -> !r.contains("=null") && !r.contains("=[]"));
    assertThat(rows(uni, language, query)).as(query).isEqualTo(expected);
  }

  /** A path function from one named vertex to another: the same path on both graphs, and a path rather than none. */
  private void assertSameFromTo(final String query, final String from, final String to) {
    final List<String> expected = rows(database, "sql", query, vertex(database, from), vertex(database, to));
    assertThat(expected).as("bidirectional: %s", query).hasSize(1).noneMatch(r -> r.contains("=null") || r.contains("=[]"));
    assertThat(rows(uni, "sql", query, vertex(uni, from), vertex(uni, to))).as(query).isEqualTo(expected);
  }

  private static RID vertex(final Database db, final String name) {
    try (final ResultSet rs = db.query("sql", "SELECT FROM N WHERE name = ?", name)) {
      return rs.next().getIdentity().get();
    }
  }

  /** The same grouping of the vertices, whatever ids the groups got; {@code groups} > 0 also checks how many there are. */
  private void assertSamePartition(final String query, final int groups) {
    final List<String> expected = partition(database, query);
    if (groups > 0)
      assertThat(expected).as("bidirectional: %s", query).hasSize(groups);
    assertThat(partition(uni, query)).as(query).isEqualTo(expected);
  }

  /** The whole graph, edges included, after a write. */
  private void assertSameAfterWrite() {
    final String edges = "MATCH (a)-[r:R]->(b) RETURN a.name AS a, b.name AS b, r.w AS w";
    assertThat(rows(uni, "opencypher", edges)).isEqualTo(rows(database, "opencypher", edges));
    final String perVertex = "MATCH (n) RETURN n.name AS n, labels(n) AS l, node.degree(n, 'R', 'IN') AS i, node.degree(n, 'R', 'OUT') AS o";
    assertThat(rows(uni, "opencypher", perVertex)).isEqualTo(rows(database, "opencypher", perVertex));
    final long stored = ((Number) uni.query("sql", "SELECT count(*) AS n FROM R").next().getProperty("n")).longValue();
    assertThat(rows(uni, "opencypher", "MATCH ()-[r:R]->() RETURN count(r) AS n")).containsExactly("{n=" + stored + "}");
  }

  private static List<String> partition(final Database db, final String query) {
    final Map<String, TreeSet<String>> groups = new HashMap<>();
    try (final ResultSet rs = db.query("opencypher", query)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        groups.computeIfAbsent(String.valueOf((Object) row.getProperty("g")), k -> new TreeSet<>()).add(row.getProperty("name"));
      }
    }
    final List<String> result = new ArrayList<>();
    for (final TreeSet<String> members : groups.values())
      result.add(members.toString());
    result.sort(null);
    return result;
  }

  /** The rows of a statement, each one canonical (vertices by name, edges by their ends, numbers rounded), sorted. */
  private static List<String> rows(final Database db, final String language, final String query, final Object... args) {
    final List<String> result = new ArrayList<>();
    try (final ResultSet rs = db.query(language, query, args)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        final Map<String, Object> canonical = new TreeMap<>();
        for (final String name : row.getPropertyNames())
          canonical.put(name, canonical(db, row.getProperty(name)));
        result.add(canonical.toString());
      }
    }
    result.sort(null);
    return result;
  }

  private static Object canonical(final Database db, final Object value) {
    if (value instanceof Edge edge)
      return "(" + name(db, edge.getOut()) + ")-[" + edge.getTypeName() + " " + edge.get("w") + "]->(" + name(db, edge.getIn()) + ")";
    if (value instanceof Document document && document.has("name"))
      return document.getString("name");
    if (value instanceof RID rid)
      return name(db, rid);
    if (value instanceof Double d)
      return String.format(Locale.ROOT, "%.6f", d);
    if (value instanceof Float f)
      return String.format(Locale.ROOT, "%.6f", f.doubleValue());
    if (value instanceof Map<?, ?> map) {
      final Map<String, Object> result = new TreeMap<>();
      for (final Map.Entry<?, ?> entry : map.entrySet())
        result.put(String.valueOf(entry.getKey()), canonical(db, entry.getValue()));
      return result;
    }
    if (value instanceof Collection<?> collection) {
      final List<Object> result = new ArrayList<>(collection.size());
      for (final Object element : collection)
        result.add(canonical(db, element));
      return result;
    }
    return value;
  }

  private static String name(final Database db, final Identifiable identifiable) {
    return db.lookupByRID(identifiable.getIdentity(), true).asDocument().getString("name");
  }
}
