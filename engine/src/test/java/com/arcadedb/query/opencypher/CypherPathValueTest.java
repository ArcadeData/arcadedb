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
package com.arcadedb.query.opencypher;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.traversal.TraversalPath;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Path values and path functions: nodes(), relationships(), length(), path combination and path procedures.
 * <p>
 * Each nested class holds the regression tests of the GitHub issue its name carries.
 */
class CypherPathValueTest {
  /**
   * Regression tests for <a href="https://github.com/ArcadeData/arcadedb/issues/4353">#4353</a>:
   * nodes(), relationships() and length() on variable-length path patterns such as [*1..3].
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue4353 extends TestHelper {
    @Override
    protected void beginTest() {
      database.getSchema().createVertexType("Worker");
      database.getSchema().createEdgeType("REPORTS_TO");

      // Chain of 4 workers: a -> b -> c -> d (3 hops)
      database.command("opencypher",
          """
          CREATE (a:Worker {name: 'A'}), \
          (b:Worker {name: 'B'}), \
          (c:Worker {name: 'C'}), \
          (d:Worker {name: 'D'}), \
          (a)-[:REPORTS_TO]->(b), \
          (b)-[:REPORTS_TO]->(c), \
          (c)-[:REPORTS_TO]->(d)""");
    }

    @Test
    void nodesOnVariableLengthPath() {
      final ResultSet result = database.command("opencypher",
          """
          MATCH p = (a:Worker {name: 'A'})-[:REPORTS_TO*1..3]->(b:Worker) \
          RETURN nodes(p) AS nodeList, b.name AS endName \
          ORDER BY endName""");

      final List<List<?>> nodes = new ArrayList<>();
      final List<String> endNames = new ArrayList<>();
      while (result.hasNext()) {
        final Result row = result.next();
        nodes.add((List<?>) row.getProperty("nodeList"));
        endNames.add(row.getProperty("endName"));
      }
      assertThat(endNames).containsExactly("B", "C", "D");
      assertThat(nodes.get(0)).hasSize(2);
      assertThat(nodes.get(1)).hasSize(3);
      assertThat(nodes.get(2)).hasSize(4);
      for (final List<?> n : nodes)
        for (final Object o : n)
          assertThat(o).isInstanceOf(Vertex.class);
    }

    @Test
    void relationshipsOnVariableLengthPath() {
      final ResultSet result = database.command("opencypher",
          """
          MATCH p = (a:Worker {name: 'A'})-[:REPORTS_TO*1..3]->(b:Worker) \
          RETURN relationships(p) AS relList, b.name AS endName \
          ORDER BY endName""");

      final List<List<?>> rels = new ArrayList<>();
      while (result.hasNext()) {
        final Result row = result.next();
        rels.add((List<?>) row.getProperty("relList"));
      }
      assertThat(rels.get(0)).hasSize(1);
      assertThat(rels.get(1)).hasSize(2);
      assertThat(rels.get(2)).hasSize(3);
      for (final List<?> r : rels)
        for (final Object o : r)
          assertThat(o).isInstanceOf(Edge.class);
    }

    @Test
    void lengthOnVariableLengthPath() {
      final ResultSet result = database.command("opencypher",
          """
          MATCH p = (a:Worker {name: 'A'})-[:REPORTS_TO*1..3]->(b:Worker) \
          RETURN length(p) AS pathLength, b.name AS endName \
          ORDER BY endName""");

      final List<Long> lengths = new ArrayList<>();
      while (result.hasNext())
        lengths.add(((Number) result.next().getProperty("pathLength")).longValue());

      assertThat(lengths).containsExactly(1L, 2L, 3L);
    }

    @Test
    void allPathFunctionsTogether() {
      final ResultSet result = database.command("opencypher",
          """
          MATCH p = (a:Worker {name: 'A'})-[:REPORTS_TO*1..3]->(b:Worker {name: 'D'}) \
          RETURN nodes(p) AS nodeList, relationships(p) AS relList, length(p) AS pathLength""");

      assertThat(result.hasNext()).isTrue();
      final Result row = result.next();
      final List<?> nodeList = row.getProperty("nodeList");
      final List<?> relList = row.getProperty("relList");
      assertThat(nodeList).hasSize(4);
      assertThat(relList).hasSize(3);
      assertThat(((Number) row.getProperty("pathLength")).longValue()).isEqualTo(3L);
    }

    @Test
    void whereLengthFilter() {
      // Only return paths of length 2 or more
      final ResultSet result = database.command("opencypher",
          """
          MATCH p = (a:Worker {name: 'A'})-[:REPORTS_TO*1..3]->(b:Worker) \
          WHERE length(p) >= 2 \
          RETURN b.name AS endName, length(p) AS len \
          ORDER BY endName""");

      final List<String> endNames = new ArrayList<>();
      final List<Long> lengths = new ArrayList<>();
      while (result.hasNext()) {
        final Result row = result.next();
        endNames.add(row.getProperty("endName"));
        lengths.add(((Number) row.getProperty("len")).longValue());
      }
      assertThat(endNames).containsExactly("C", "D");
      assertThat(lengths).containsExactly(2L, 3L);
    }

    @Test
    void unwindNodesOfPath() {
      // UNWIND nodes(p) AS n - exposes each node of every matching path
      final ResultSet result = database.command("opencypher",
          """
          MATCH p = (a:Worker {name: 'A'})-[:REPORTS_TO*1..3]->(b:Worker {name: 'D'}) \
          UNWIND nodes(p) AS n \
          RETURN n.name AS name \
          ORDER BY name""");

      final List<String> names = new ArrayList<>();
      while (result.hasNext())
        names.add(result.next().getProperty("name"));
      assertThat(names).containsExactly("A", "B", "C", "D");
    }

    @Test
    void variableLengthWithNoBounds() {
      // [*] (no bounds) defaults to *1..max - typically 1..Integer.MAX_VALUE in Neo4j
      final ResultSet result = database.command("opencypher",
          """
          MATCH p = (a:Worker {name: 'A'})-[:REPORTS_TO*]->(b:Worker {name: 'D'}) \
          RETURN length(p) AS len, nodes(p) AS ns, relationships(p) AS rs""");

      assertThat(result.hasNext()).isTrue();
      final Result row = result.next();
      assertThat(((Number) row.getProperty("len")).longValue()).isEqualTo(3L);
      assertThat((List<?>) row.getProperty("ns")).hasSize(4);
      assertThat((List<?>) row.getProperty("rs")).hasSize(3);
    }

    @Test
    void variableLengthWithUpperBoundOnly() {
      // [*..3] - up to 3 hops (default min=1)
      final ResultSet result = database.command("opencypher",
          """
          MATCH p = (a:Worker {name: 'A'})-[:REPORTS_TO*..3]->(b:Worker) \
          RETURN length(p) AS len, b.name AS endName \
          ORDER BY endName""");

      final List<Long> lengths = new ArrayList<>();
      while (result.hasNext())
        lengths.add(((Number) result.next().getProperty("len")).longValue());
      assertThat(lengths).containsExactly(1L, 2L, 3L);
    }

    @Test
    void variableLengthWithReverseDirection() {
      final ResultSet result = database.command("opencypher",
          """
          MATCH p = (d:Worker {name: 'D'})<-[:REPORTS_TO*1..3]-(b:Worker) \
          RETURN length(p) AS len, b.name AS sourceName \
          ORDER BY sourceName""");

      final List<String> names = new ArrayList<>();
      final List<Long> lengths = new ArrayList<>();
      while (result.hasNext()) {
        final Result row = result.next();
        names.add(row.getProperty("sourceName"));
        lengths.add(((Number) row.getProperty("len")).longValue());
      }
      assertThat(names).containsExactly("A", "B", "C");
      assertThat(lengths).containsExactly(3L, 2L, 1L);
    }

    @Test
    void chainedVariableLengthPaths() {
      // Path variable spans two consecutive variable-length segments
      final ResultSet result = database.command("opencypher",
          """
          MATCH p = (a:Worker {name: 'A'})-[:REPORTS_TO*1..2]->(b:Worker)-[:REPORTS_TO*1..2]->(c:Worker {name: 'D'}) \
          RETURN length(p) AS len, b.name AS midName \
          ORDER BY midName""");

      // From A to D, length must be 3 in every match (always 3 hops total)
      while (result.hasNext()) {
        final Result row = result.next();
        assertThat(((Number) row.getProperty("len")).longValue()).isEqualTo(3L);
      }
    }
  }

  /**
   * APOC declares the config of {@code path.subgraphNodes}, {@code path.subgraphAll} and {@code path.spanningTree} with
   * a default, so Cypher migrated from Neo4j calls them with the start node only (issue #8163).
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue8163PathOptionalConfig extends TestHelper {

    @Test
    void pathProceduresAcceptACallWithoutConfig() {
      database.getSchema().createVertexType("N");
      database.getSchema().createEdgeType("R");
      database.transaction(() -> {
        database.command("sql", "CREATE VERTEX N SET id = 1");
        database.command("sql", "CREATE VERTEX N SET id = 2");
        database.command("sql", "CREATE EDGE R FROM (SELECT FROM N WHERE id = 1) TO (SELECT FROM N WHERE id = 2)");
      });

      try (final ResultSet rs = database.query("opencypher", "MATCH (n:N {id: 1}) CALL path.subgraphNodes(n) YIELD node RETURN count(node) AS c")) {
        assertThat(((Number) rs.next().getProperty("c")).longValue()).isEqualTo(2L);
      }
      try (final ResultSet rs = database.query("opencypher", "MATCH (n:N {id: 1}) CALL path.subgraphAll(n) YIELD nodes, relationships RETURN size(nodes) AS c")) {
        assertThat(((Number) rs.next().getProperty("c")).longValue()).isEqualTo(2L);
      }
      try (final ResultSet rs = database.query("opencypher", "MATCH (n:N {id: 1}) CALL path.spanningTree(n) YIELD path RETURN count(path) AS c")) {
        assertThat(((Number) rs.next().getProperty("c")).longValue()).isEqualTo(2L);
      }
    }
  }

  /**
   * Issue #9913: the {@code path} the {@code algo.*} and {@code path.*} procedures yield was a {@code _type: "path"} map, so
   * {@code nodes()}, {@code relationships()} and {@code length()} answered {@code []}, {@code []} and {@code 0} on it without
   * an error. Every producer of a path now yields one path value - {@link TraversalPath} - whether it comes from a MATCH
   * pattern, {@code shortestPath()}, a procedure or an {@code apoc.path.*} function, and every path function reads it.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue9913PathValue extends TestHelper {
    private static final String ENDS = "MATCH (a:City {name: 'A'}), (c:City {name: 'C'}) ";
    private static final String PROJECT =
        " RETURN [n IN nodes(path) | n.name] AS names, size(relationships(path)) AS rels, length(path) AS len, path";

    @Override
    protected void beginTest() {
      database.command("sql", "CREATE VERTEX TYPE City");
      database.command("sql", "CREATE EDGE TYPE ROAD");
      database.transaction(() -> {
        database.command("sql", "INSERT INTO City SET name = 'A'");
        database.command("sql", "INSERT INTO City SET name = 'B'");
        database.command("sql", "INSERT INTO City SET name = 'C'");
        database.command("sql",
            "CREATE EDGE ROAD FROM (SELECT FROM City WHERE name = 'A') TO (SELECT FROM City WHERE name = 'B') SET km = 1");
        database.command("sql",
            "CREATE EDGE ROAD FROM (SELECT FROM City WHERE name = 'B') TO (SELECT FROM City WHERE name = 'C') SET km = 2");
      });
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "CALL algo.dijkstra(a, c, 'ROAD', 'km') YIELD path",
        "CALL algo.astar(a, c, 'ROAD', 'km') YIELD path",
        "CALL algo.cch.shortestPath(a, c, 'ROAD', 'km', 'OUT') YIELD path",
        "CALL algo.bellmanford(a, c, 'ROAD', 'km') YIELD path",
        "CALL algo.kShortestPaths(a, c, 1, 'ROAD', 'km') YIELD path",
        "CALL algo.allSimplePaths(a, c, 'ROAD', 5) YIELD path",
    })
    void procedurePathIsReadByThePathFunctions(final String call) {
      final Result row = single(ENDS + call + PROJECT);
      assertRoute(row);
    }

    @Test
    void pathProceduresYieldARealPath() {
      final Result row = single("MATCH (a:City {name: 'A'}) CALL path.expand(a, 'ROAD', null, 2, 2) YIELD path" + PROJECT);
      assertRoute(row);

      final Result config = single(
          "MATCH (a:City {name: 'A'}) CALL path.expandConfig(a, {relationshipFilter: 'ROAD', minLevel: 2, maxLevel: 2}) YIELD path"
              + PROJECT);
      assertRoute(config);

      final List<Result> tree = all("MATCH (a:City {name: 'A'}) CALL path.spanningTree(a, {relationshipFilter: 'ROAD'}) YIELD path"
          + " RETURN length(path) AS len, size(nodes(path)) AS n ORDER BY len");
      assertThat(tree).isNotEmpty();
      for (final Result r : tree)
        assertThat(r.<Long>getProperty("n")).isEqualTo(r.<Long>getProperty("len") + 1);
      assertThat(tree.getLast().<Long>getProperty("len")).isEqualTo(2L);
    }

    @Test
    void kShortestPathsAndRandomWalkCarryTheirRelationships() {
      // Both built their path from vertex RIDs alone, so the relationships were missing and the length was 0
      final Result k = single(ENDS + "CALL algo.kShortestPaths(a, c, 1, 'ROAD', 'km') YIELD path, weight" + PROJECT + ", weight");
      assertRoute(k);
      assertThat(k.<Number>getProperty("weight").doubleValue()).isEqualTo(3.0);

      final Result walk = single("MATCH (a:City {name: 'A'}) CALL algo.randomWalk(a, 2, 'ROAD', 'OUT', 42) YIELD path, steps"
          + PROJECT + ", steps");
      assertRoute(walk);
      assertThat(walk.<Number>getProperty("steps").intValue()).isEqualTo(2);
    }

    @Test
    void yieldedPathKeepsTheMapStyleReads() {
      // Queries written against the old map shape keep working: path.nodes, path.relationships, path.length, path['nodes']
      final Result row = single(ENDS + "CALL algo.dijkstra(a, c, 'ROAD', 'km') YIELD path, weight "
          + "RETURN size(path.nodes) AS pn, size(path.relationships) AS pr, path.length AS pl, size(path['nodes']) AS bn, "
          + "reduce(t = 0, r IN path.relationships | t + r.km) AS km, weight");
      assertThat(row.<Number>getProperty("pn").intValue()).isEqualTo(3);
      assertThat(row.<Number>getProperty("pr").intValue()).isEqualTo(2);
      assertThat(row.<Number>getProperty("pl").intValue()).isEqualTo(2);
      assertThat(row.<Number>getProperty("bn").intValue()).isEqualTo(3);
      assertThat(row.<Number>getProperty("km").intValue()).isEqualTo(3);
      assertThat(row.<Number>getProperty("weight").doubleValue()).isEqualTo(3.0);
    }

    @Test
    void procedurePathsAreValuesForDistinctAndCollect() {
      // the same route yielded twice is one DISTINCT value, and collect() keeps paths
      final Result row = single(ENDS + "UNWIND [1, 2] AS i CALL algo.dijkstra(a, c, 'ROAD', 'km') YIELD path "
          + "WITH collect(path) AS allPaths, collect(DISTINCT path) AS distinctPaths "
          + "RETURN size(allPaths) AS a, size(distinctPaths) AS d, [p IN allPaths | length(p)] AS lens");
      assertThat(row.<Number>getProperty("a").intValue()).isEqualTo(2);
      assertThat(row.<Number>getProperty("d").intValue()).isEqualTo(1);
      assertThat(row.<List<Long>>getProperty("lens")).containsExactly(2L, 2L);
    }

    @Test
    void shortestPathBindsAPath() {
      final Result row = single("MATCH path = shortestPath((a:City {name: 'A'})-[:ROAD*]->(c:City {name: 'C'}))" + PROJECT);
      assertRoute(row);

      final List<Result> all = all("MATCH path = allShortestPaths((a:City {name: 'A'})-[:ROAD*]->(c:City {name: 'C'}))" + PROJECT);
      assertThat(all).hasSize(1);
      assertRoute(all.getFirst());

      final Result expression = single(ENDS + "WITH shortestPath((a)-[:ROAD*]->(c)) AS path" + PROJECT);
      assertRoute(expression);

      final Result filtered = single(ENDS + "WITH shortestPath((a)-[:ROAD* {}]->(c)) AS path" + PROJECT);
      assertRoute(filtered);

      final Result allExpression = single(ENDS + "WITH allShortestPaths((a)-[:ROAD*]->(c)) AS paths UNWIND paths AS path" + PROJECT);
      assertRoute(allExpression);
    }

    @Test
    void apocPathFunctionsReadAndYieldAPath() {
      final String match = "MATCH p = (:City {name: 'A'})-[:ROAD*2]->(:City {name: 'C'}) ";

      // apoc.path.slice / elements answered null / [] on a MATCH path
      final Result slice = single(match + "WITH apoc.path.slice(p, 0, 1) AS path" + PROJECT);
      assertThat(slice.<List<String>>getProperty("names")).containsExactly("A", "B");
      assertThat(slice.<Long>getProperty("len")).isEqualTo(1L);
      assertThat(slice.<Object>getProperty("path")).isInstanceOf(TraversalPath.class);

      final Result elements = single(match + "RETURN size(apoc.path.elements(p)) AS n");
      assertThat(elements.<Number>getProperty("n").intValue()).isEqualTo(5);

      final Result combined = single(match + "WITH apoc.path.slice(p, 0, 1) AS first, apoc.path.slice(p, 1, 2) AS second "
          + "WITH apoc.path.combine([first, second]) AS path" + PROJECT);
      assertRoute(combined);

      // apoc.path.create walks the relationships from the start node: its nodes used to hold the start node alone
      final Result created = single(match + "WITH apoc.path.create(nodes(p)[0], relationships(p)) AS path" + PROJECT);
      assertRoute(created);

      final Result procedureSlice = single(ENDS + "CALL algo.dijkstra(a, c, 'ROAD', 'km') YIELD path "
          + "WITH apoc.path.slice(path, 1) AS path" + PROJECT);
      assertThat(procedureSlice.<List<String>>getProperty("names")).containsExactly("B", "C");
    }

    @Test
    void createRefusesARelationshipThatDoesNotContinueThePath() {
      assertThatThrownBy(() -> single("MATCH (c:City {name: 'C'}), ()-[r:ROAD {km: 1}]->() RETURN apoc.path.create(c, [r]) AS p"))
          .hasMessageContaining("does not continue the path");
    }

    @Test
    void pathTypeIsPath() {
      final Result row = single(ENDS + "CALL algo.dijkstra(a, c, 'ROAD', 'km') YIELD path "
          + "MATCH p = (a)-[:ROAD*]->(c) "
          + "RETURN valueType(path) AS t1, valueType(p) AS t2, path IS :: PATH AS i1, p IS :: PATH AS i2, [1, 2] IS :: PATH AS i3, "
          + "path = p AS same");
      assertThat(row.<String>getProperty("t1")).isEqualTo("PATH NOT NULL");
      assertThat(row.<String>getProperty("t2")).isEqualTo("PATH NOT NULL");
      assertThat(row.<Boolean>getProperty("i1")).isTrue();
      assertThat(row.<Boolean>getProperty("i2")).isTrue();
      assertThat(row.<Boolean>getProperty("i3")).isFalse();
      assertThat(row.<Boolean>getProperty("same")).isTrue();
    }

    @Test
    void pathFunctionsRefuseANonPath() {
      for (final String function : new String[] { "nodes", "relationships", "length" })
        assertThatThrownBy(() -> single("RETURN " + function + "({a: 1}) AS x"))
            .isInstanceOf(CommandSemanticException.class)
            .hasMessageContaining("PATH");

      // a list is not a path either, even a one-element list of a node, which used to read as a zero-length path
      assertThatThrownBy(() -> single("MATCH (n:City {name: 'A'}) RETURN length([n]) AS x"))
          .isInstanceOf(CommandSemanticException.class).hasMessageContaining("PATH");
      assertThatThrownBy(() -> single("MATCH (n:City {name: 'A'}) RETURN nodes([n]) AS x"))
          .isInstanceOf(CommandSemanticException.class).hasMessageContaining("PATH");
      assertThatThrownBy(() -> single("MATCH p = (:City {name: 'A'})-[:ROAD]->() RETURN apoc.path.slice(p, 'one') AS x"))
          .isInstanceOf(CommandSemanticException.class).hasMessageContaining("INTEGER");

      // a {_type: 'path'} map whose counts do not make a path is not one
      assertThatThrownBy(() -> single("MATCH (n:City {name: 'A'}) RETURN nodes({_type: 'path', nodes: [n, n], relationships: []}) AS x"))
          .isInstanceOf(CommandSemanticException.class).hasMessageContaining("PATH");

      // length() of a string keeps answering its length, as it always has
      assertThat(single("RETURN length('abc') AS l").<Long>getProperty("l")).isEqualTo(3L);

      final Result nulls = single("RETURN nodes(null) AS n, relationships(null) AS r, length(null) AS l");
      assertThat(nulls.<Object>getProperty("n")).isNull();
      assertThat(nulls.<Object>getProperty("r")).isNull();
      assertThat(nulls.<Object>getProperty("l")).isNull();
    }

    @Test
    void pathSerializesAsAnObject() {
      // A MATCH path used to serialize as its toString() "(#1:0)-[#4:0]->...", a procedure path as the map: both are now
      // the same object
      for (final String query : new String[] {
          ENDS + "CALL algo.dijkstra(a, c, 'ROAD', 'km') YIELD path RETURN path",
          "MATCH path = (:City {name: 'A'})-[:ROAD*]->(:City {name: 'C'}) RETURN path",
          "MATCH path = shortestPath((:City {name: 'A'})-[:ROAD*]->(:City {name: 'C'})) RETURN path" }) {
        final JSONObject json = single(query).toJSON().getJSONObject("path");
        assertThat(json.getString("_type")).isEqualTo("path");
        assertThat(json.getInt("length")).isEqualTo(2);
        final JSONArray nodes = json.getJSONArray("nodes");
        final JSONArray rels = json.getJSONArray("relationships");
        assertThat(nodes.length()).isEqualTo(3);
        assertThat(rels.length()).isEqualTo(2);
      }
    }

    @Test
    void ofReadsEveryPathShape() {
      final TraversalPath path = (TraversalPath) single(
          "MATCH path = (:City {name: 'A'})-[:ROAD*]->(:City {name: 'C'}) RETURN path").getProperty("path");

      assertThat(TraversalPath.of(path)).isSameAs(path);
      // a walk's alternating elements build the path; a list is never a path to of(), as IS :: PATH says
      assertThat(TraversalPath.ofWalk(path.toAlternatingList())).isEqualTo(path);
      assertThat(TraversalPath.of(path.toAlternatingList())).isNull();
      assertThat(TraversalPath.of(path.toMap())).isEqualTo(path);

      // the same elements as RIDs, as a map that went through a client and came back as a parameter
      final List<Object> nodeRids = new ArrayList<>();
      for (final Vertex v : path.getVertices())
        nodeRids.add(v.getIdentity());
      final List<Object> relRids = new ArrayList<>();
      for (final Edge e : path.getEdges())
        relRids.add(e.getIdentity());
      assertThat(TraversalPath.of(Map.of("_type", "path", "nodes", nodeRids, "relationships", relRids))).isEqualTo(path);

      // not paths: a map without the marker, a list that does not alternate, nodes that the relationships do not join
      assertThat(TraversalPath.of(Map.of("nodes", nodeRids, "relationships", relRids))).isNull();
      assertThat(TraversalPath.of(List.of(1, 2, 3))).isNull();
      assertThat(TraversalPath.of(List.of(path.getVertices().get(0), path.getVertices().get(1)))).isNull();
      assertThat(TraversalPath.of(Map.of("_type", "path", "nodes", nodeRids.subList(0, 2), "relationships", relRids))).isNull();
      assertThat(TraversalPath.of(null)).isNull();

      // a RID that points to no record is not a path element: null, not a RecordNotFoundException
      final List<Object> missing = new ArrayList<>(nodeRids);
      missing.set(1, new RID(path.getVertices().get(1).getIdentity().getBucketId(), 999_999L));
      assertThat(TraversalPath.of(Map.of("_type", "path", "nodes", missing, "relationships", relRids))).isNull();

      // the right counts, but A-[r0]->B listed between A and C: r0 does not join them
      final List<Object> swapped = List.of(nodeRids.get(0), nodeRids.get(2), nodeRids.get(1));
      assertThat(TraversalPath.of(Map.of("_type", "path", "nodes", swapped, "relationships", relRids))).isNull();
      assertThatThrownBy(() -> TraversalPath.ofWalk(List.of(path.getVertices().get(0), path.getEdges().get(1), path.getVertices().get(1))))
          .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void yieldedPathMemberReadsAndMatchPathPropertyAccess() {
      // a name the old map did not have answers null, as the map did
      final Result row = single(ENDS + "CALL algo.dijkstra(a, c, 'ROAD', 'km') YIELD path RETURN path.weight AS w, path['other'] AS o");
      assertThat(row.<Object>getProperty("w")).isNull();
      assertThat(row.<Object>getProperty("o")).isNull();

      // a path a MATCH pattern binds still refuses property access, as in Neo4j
      assertThatThrownBy(() -> single("MATCH p = (:City {name: 'A'})-[:ROAD]->() RETURN p.nodes AS n"))
          .hasMessageContaining("Property access on a path variable is not allowed");
    }

    @Test
    void anAlternatingListIsNotAPathAndAZeroLengthShortestPathIs() {
      final Result list = single("MATCH p = (a:City {name: 'A'})-[r:ROAD]->(b) RETURN [a, r, b] IS :: PATH AS isPath, p IS :: PATH AS p");
      assertThat(list.<Boolean>getProperty("isPath")).isFalse();
      assertThat(list.<Boolean>getProperty("p")).isTrue();

      final Result self = single("MATCH path = shortestPath((a:City {name: 'A'})-[:ROAD*0..]->(b:City {name: 'A'})) "
          + "RETURN length(path) AS len, size(nodes(path)) AS n, path");
      assertThat(self.<Long>getProperty("len")).isEqualTo(0L);
      assertThat(self.<Long>getProperty("n")).isEqualTo(1L);
      assertThat(self.<Object>getProperty("path")).isInstanceOf(TraversalPath.class);
    }

    private void assertRoute(final Result row) {
      assertThat(row.<List<String>>getProperty("names")).containsExactly("A", "B", "C");
      assertThat(row.<Number>getProperty("rels").intValue()).isEqualTo(2);
      assertThat(row.<Number>getProperty("len").longValue()).isEqualTo(2L);
      assertThat(row.<Object>getProperty("path")).isInstanceOf(TraversalPath.class);
    }

    private Result single(final String query) {
      final List<Result> rows = all(query);
      assertThat(rows).as(query).hasSize(1);
      return rows.getFirst();
    }

    private List<Result> all(final String query) {
      final List<Result> rows = new ArrayList<>();
      try (final ResultSet rs = database.query("opencypher", query)) {
        while (rs.hasNext())
          rows.add(rs.next());
      }
      return rows;
    }
  }

  /**
   * Regression tests for #10017: {@code path.combine} accepted one argument (a list of path maps) while its documented
   * signature, the APOC one, is {@code path.combine(path1, path2)}; and none of the {@code path.*} functions understood
   * the path a {@code MATCH} binds.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue10017PathCombine extends TestHelper {

    @Override
    protected void beginTest() {
      database.transaction(() -> database.command("opencypher", "CREATE (a:N {id: 1})-[:R]->(b:N {id: 2})-[:R]->(c:N {id: 3})"));
    }

    @Test
    void combinesTheTwoPathsOfTheIssue() {
      try (final ResultSet rs = database.query("opencypher", """
          MATCH p1 = (a:N {id: 1})-[:R]->(b:N {id: 2}),
                p2 = (b)-[:R]->(c:N {id: 3})
          RETURN path.combine(p1, p2) AS combined""")) {
        final Result row = rs.next();
        assertThat(rs.hasNext()).isFalse();
        final TraversalPath combined = row.getProperty("combined");
        assertThat(combined.length()).isEqualTo(2);
        final List<Object> ids = new ArrayList<>();
        combined.getVertices().forEach(v -> ids.add(v.get("id")));
        assertThat(ids).containsExactly(1, 2, 3);
        assertThat(combined.getEdges()).allMatch(e -> e.getTypeName().equals("R"));
      }
    }

    @Test
    void theCombinedPathIsAPathToTheOtherFunctions() {
      assertThat(single("""
          MATCH p1 = (:N {id: 1})-[:R]->(b:N {id: 2}), p2 = (b)-[:R]->(:N {id: 3})
          WITH path.combine(p1, p2) AS p
          RETURN [n IN nodes(p) | n.id] AS ids, length(p) AS len, size(relationships(p)) AS rels""")).satisfies(row -> {
        assertThat(row.<List<Object>>getProperty("ids")).containsExactly(1, 2, 3);
        assertThat(row.<Number>getProperty("len").intValue()).isEqualTo(2);
        assertThat(row.<Number>getProperty("rels").intValue()).isEqualTo(2);
      });
      // the APOC name resolves to the same function
      assertThat(single("""
          MATCH p1 = (:N {id: 1})-[:R]->(b:N {id: 2}), p2 = (b)-[:R]->(:N {id: 3})
          RETURN length(apoc.path.combine(p1, p2)) AS len""").<Number>getProperty("len").intValue()).isEqualTo(2);
    }

    @Test
    void aNullPathLeavesTheOther() {
      assertThat(single("MATCH p = (:N {id: 1})-[:R]->(:N {id: 2}) RETURN length(path.combine(p, null)) AS len")
          .<Number>getProperty("len").intValue()).isEqualTo(1);
      assertThat(single("MATCH p = (:N {id: 1})-[:R]->(:N {id: 2}) RETURN length(path.combine(null, p)) AS len")
          .<Number>getProperty("len").intValue()).isEqualTo(1);
      assertThat(single("RETURN path.combine(null, null) AS p").<Object>getProperty("p")).isNull();
    }

    @Test
    void pathsThatDoNotConnectAreRefused() {
      assertThatThrownBy(() -> single("""
          MATCH p1 = (:N {id: 1})-[:R]->(b:N {id: 2}), p2 = (b)-[:R]->(:N {id: 3})
          RETURN path.combine(p2, p1) AS p""")).isInstanceOf(CommandExecutionException.class)
          .hasMessageContaining("do not connect");
    }

    @Test
    void whatIsNotAPathIsRefused() {
      assertThatThrownBy(() -> single("MATCH p = (:N {id: 1})-[:R]->(:N {id: 2}) RETURN path.combine(p, 42) AS p"))
          .isInstanceOf(CommandExecutionException.class).hasMessageContaining("expects paths");
      assertThatThrownBy(() -> single("MATCH p = (:N {id: 1})-[:R]->(:N {id: 2}) RETURN path.combine([p, 'x']) AS p"))
          .isInstanceOf(CommandExecutionException.class).hasMessageContaining("expects paths");
    }

    @Test
    void theListFormJoinsEveryPathInOrder() {
      assertThat(single("""
          MATCH p1 = (:N {id: 1})-[:R]->(b:N {id: 2}), p2 = (b)-[:R]->(:N {id: 3})
          RETURN [n IN nodes(path.combine([p1, p2])) | n.id] AS ids""").<List<Object>>getProperty("ids")).containsExactly(1, 2, 3);
    }

    @Test
    void createDerivesTheNodesFromTheRelationships() {
      assertThat(single("""
          MATCH (a:N {id: 1})-[r1:R]->(:N {id: 2})-[r2:R]->(c:N {id: 3})
          WITH path.create(a, [r1, r2]) AS p
          RETURN [n IN nodes(p) | n.id] AS ids, length(p) AS len""")).satisfies(row -> {
        assertThat(row.<List<Object>>getProperty("ids")).containsExactly(1, 2, 3);
        assertThat(row.<Number>getProperty("len").intValue()).isEqualTo(2);
      });
      // walked against the stored direction, from the far end
      assertThat(single("""
          MATCH (a:N {id: 1})-[r1:R]->(:N {id: 2})-[r2:R]->(c:N {id: 3})
          RETURN [n IN nodes(path.create(c, [r2, r1])) | n.id] AS ids""").<List<Object>>getProperty("ids")).containsExactly(3, 2, 1);
      assertThatThrownBy(() -> single("""
          MATCH (a:N {id: 1})-[r1:R]->(:N {id: 2})-[r2:R]->(c:N {id: 3})
          RETURN path.create(a, [r2]) AS p""")).isInstanceOf(CommandExecutionException.class)
          .hasMessageContaining("does not continue the path");
    }

    @Test
    void sliceAndElementsReadAMatchedPath() {
      assertThat(single("""
          MATCH p = (:N {id: 1})-[:R]->(:N {id: 2})-[:R]->(:N {id: 3})
          RETURN [n IN nodes(path.slice(p, 1)) | n.id] AS ids, size(path.elements(p)) AS elements""")).satisfies(row -> {
        assertThat(row.<List<Object>>getProperty("ids")).containsExactly(2, 3);
        assertThat(row.<Number>getProperty("elements").intValue()).isEqualTo(5);
      });
    }

    @Test
    void combinesAProcedurePathWithAMatchedOne() {
      assertThat(single("""
          MATCH (a:N {id: 1}), (b:N {id: 2})
          CALL algo.dijkstra(a, b, 'R', 'weight') YIELD path
          MATCH p2 = (b)-[:R]->(:N {id: 3})
          RETURN [n IN nodes(path.combine(path, p2)) | n.id] AS ids""").<List<Object>>getProperty("ids")).containsExactly(1, 2, 3);
    }

    private Result single(final String query) {
      try (final ResultSet rs = database.query("opencypher", query)) {
        return rs.next();
      }
    }
  }

  /**
   * Regression tests for #10138: {@code path.elements(null)} answered an empty list, where every other path function
   * answers null for a null argument.
   */
  @Nested
  class Issue10138PathElementsNull extends TestHelper {

    @Test
    void nullArgumentAnswersNull() {
      final Result row = single("RETURN path.elements(null) AS r");
      assertThat(row.hasProperty("r")).isTrue();
      assertThat((Object) row.getProperty("r")).isNull();
    }

    @Test
    void agreesWithTheOtherPathFunctions() {
      final Result row = single("""
          RETURN path.elements(null) IS NULL AS elements, nodes(null) IS NULL AS nodes,
                 relationships(null) IS NULL AS rels, size(path.elements(null)) IS NULL AS size""");
      assertThat(row.<Boolean>getProperty("elements")).isTrue();
      assertThat(row.<Boolean>getProperty("nodes")).isTrue();
      assertThat(row.<Boolean>getProperty("rels")).isTrue();
      assertThat(row.<Boolean>getProperty("size")).isTrue();
    }

    @Test
    void aMatchedPathStillAnswersItsElements() {
      database.transaction(() -> database.command("opencypher", "CREATE (:N {id: 1})-[:R]->(:N {id: 2})"));
      final Result row = single("MATCH p = (:N {id: 1})-[:R]->(:N {id: 2}) RETURN size(path.elements(p)) AS n");
      assertThat(row.<Number>getProperty("n").intValue()).isEqualTo(3);
    }

    @Test
    void aNonPathArgumentIsStillRefused() {
      assertThatThrownBy(() -> single("RETURN path.elements(1) AS r"))
          .isInstanceOf(CommandSemanticException.class)
          .hasMessageContaining("PATH");
    }

    private Result single(final String query) {
      try (final ResultSet rs = database.query("opencypher", query)) {
        return rs.next();
      }
    }
  }
}
