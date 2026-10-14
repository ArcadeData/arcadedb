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
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.traversal.TraversalPath;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9913: the {@code path} the {@code algo.*} and {@code path.*} procedures yield was a {@code _type: "path"} map, so
 * {@code nodes()}, {@code relationships()} and {@code length()} answered {@code []}, {@code []} and {@code 0} on it without
 * an error. Every producer of a path now yields one path value - {@link TraversalPath} - whether it comes from a MATCH
 * pattern, {@code shortestPath()}, a procedure or an {@code apoc.path.*} function, and every path function reads it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9913PathValueTest extends TestHelper {
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
