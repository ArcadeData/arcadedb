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
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for https://github.com/ArcadeData/arcadedb/issues/8995
 * <p>
 * {@code allShortestPaths()} must return one path per distinct co-shortest path, and two parallel
 * relationships between the same pair of nodes make two distinct paths. The unconstrained layered BFS
 * recorded the parent VERTEX once per relationship and rebuilt each hop with the first relationship it
 * found between the two vertices, so the right number of rows came back but every row walked the same
 * relationship: {@code [11, 20]} twice instead of {@code [10, 20]} and {@code [11, 20]}.
 * <p>
 * Every evaluator of the pattern is exercised: the MATCH form directed, undirected, type-filtered, over a
 * unidirectional edge type and with an inline edge filter (the edge-aware BFS, which was already right and
 * stays a control), and the {@code RETURN allShortestPaths(...)} expression form, which used to answer
 * with a single path whatever the graph held.
 */
class CypherAllShortestPathsParallelEdgesIssue8995Test extends TestHelper {

  private void createIssueGraph() {
    database.transaction(() -> database.command("opencypher", """
        CREATE (a:N {id: 1}), (b:N {id: 2}), (c:N {id: 3}),
               (a)-[:R {eid: 10}]->(b), (a)-[:R {eid: 11}]->(b), (b)-[:R {eid: 20}]->(c)"""));
  }

  @Test
  void directedAllShortestPathsReturnsOnePathPerParallelRelationship() {
    createIssueGraph();
    assertThat(eidLists("MATCH p = allShortestPaths((a:N {id: 1})-[*]->(c:N {id: 3})) "
        + "RETURN [r IN relationships(p) | r.eid] AS rels"))
        .containsExactlyInAnyOrder(List.of(10, 20), List.of(11, 20));
  }

  @Test
  void undirectedAllShortestPathsReturnsOnePathPerParallelRelationship() {
    createIssueGraph();
    assertThat(eidLists("MATCH p = allShortestPaths((a:N {id: 1})-[*]-(c:N {id: 3})) "
        + "RETURN [r IN relationships(p) | r.eid] AS rels"))
        .containsExactlyInAnyOrder(List.of(10, 20), List.of(11, 20));
  }

  @Test
  void everyRowIsADistinctPath() {
    createIssueGraph();
    try (final ResultSet rs = database.query("opencypher",
        "MATCH p = allShortestPaths((a:N {id: 1})-[*]->(c:N {id: 3})) RETURN count(DISTINCT p) AS distinctPaths, count(*) AS rows")) {
      final Result row = rs.next();
      assertThat(row.<Number>getProperty("distinctPaths").longValue()).isEqualTo(2L);
      assertThat(row.<Number>getProperty("rows").longValue()).isEqualTo(2L);
    }
  }

  @Test
  void fixedLengthControlAgreesWithAllShortestPaths() {
    createIssueGraph();
    assertThat(eidLists("MATCH p = (a:N {id: 1})-[*2]->(c:N {id: 3}) RETURN [r IN relationships(p) | r.eid] AS rels"))
        .containsExactlyInAnyOrder(List.of(10, 20), List.of(11, 20));
  }

  @Test
  void parallelRelationshipsOnEveryHopMultiplyThePaths() {
    // Two parallel relationships on each of two hops: 2 x 2 = 4 distinct paths, including the hop into the target.
    database.transaction(() -> database.command("opencypher", """
        CREATE (a:N {id: 1}), (b:N {id: 2}), (c:N {id: 3}),
               (a)-[:R {eid: 10}]->(b), (a)-[:R {eid: 11}]->(b),
               (b)-[:R {eid: 20}]->(c), (b)-[:R {eid: 21}]->(c)"""));
    assertThat(eidLists("MATCH p = allShortestPaths((a:N {id: 1})-[*]->(c:N {id: 3})) "
        + "RETURN [r IN relationships(p) | r.eid] AS rels"))
        .containsExactlyInAnyOrder(List.of(10, 20), List.of(10, 21), List.of(11, 20), List.of(11, 21));
  }

  @Test
  void undirectedPatternCountsRelationshipsPointingEitherWay() {
    // a -> b, a -> b and b -> a: an undirected pattern walks all three, a directed one only the first two.
    database.transaction(() -> database.command("opencypher", """
        CREATE (a:N {id: 1}), (b:N {id: 2}), (c:N {id: 3}),
               (a)-[:R {eid: 10}]->(b), (a)-[:R {eid: 11}]->(b), (b)-[:R {eid: 12}]->(a),
               (b)-[:R {eid: 20}]->(c)"""));
    assertThat(eidLists("MATCH p = allShortestPaths((a:N {id: 1})-[*]-(c:N {id: 3})) "
        + "RETURN [r IN relationships(p) | r.eid] AS rels"))
        .containsExactlyInAnyOrder(List.of(10, 20), List.of(11, 20), List.of(12, 20));
    assertThat(eidLists("MATCH p = allShortestPaths((a:N {id: 1})-[*]->(c:N {id: 3})) "
        + "RETURN [r IN relationships(p) | r.eid] AS rels"))
        .containsExactlyInAnyOrder(List.of(10, 20), List.of(11, 20));
  }

  @Test
  void typeFilterKeepsOnlyParallelRelationshipsOfTheDeclaredTypes() {
    database.transaction(() -> database.command("opencypher", """
        CREATE (a:N {id: 1}), (b:N {id: 2}), (c:N {id: 3}),
               (a)-[:R {eid: 10}]->(b), (a)-[:S {eid: 15}]->(b), (b)-[:R {eid: 20}]->(c)"""));
    assertThat(eidLists("MATCH p = allShortestPaths((a:N {id: 1})-[:R*]->(c:N {id: 3})) "
        + "RETURN [r IN relationships(p) | r.eid] AS rels"))
        .containsExactly(List.of(10, 20));
    assertThat(eidLists("MATCH p = allShortestPaths((a:N {id: 1})-[:R|S*]->(c:N {id: 3})) "
        + "RETURN [r IN relationships(p) | r.eid] AS rels"))
        .containsExactlyInAnyOrder(List.of(10, 20), List.of(15, 20));
  }

  @Test
  void parallelRelationshipsOfAUnidirectionalTypeAreFoundFromTheTargetSide() {
    // A unidirectional edge type stores no incoming side, so walking (t)<-[:U*]-(q) reaches the edges
    // through the query's incoming-edge lookup (issue #8625); both parallel edges must still be told apart.
    database.getSchema().createVertexType("Q");
    database.getSchema().createVertexType("T");
    database.getSchema().buildEdgeType().withName("U").withBidirectional(false).create();
    database.transaction(() -> {
      final MutableVertex q = database.newVertex("Q").set("id", 1).save();
      final MutableVertex t = database.newVertex("T").set("id", 2).save();
      q.newEdge("U", t, "eid", 30);
      q.newEdge("U", t, "eid", 31);
    });
    assertThat(eidLists("MATCH (t:T {id: 2}), (q:Q {id: 1}) MATCH p = allShortestPaths((t)<-[:U*]-(q)) "
        + "RETURN [r IN relationships(p) | r.eid] AS rels"))
        .containsExactlyInAnyOrder(List.of(30), List.of(31));
    assertThat(eidLists("MATCH (t:T {id: 2}), (q:Q {id: 1}) MATCH p = allShortestPaths((q)-[:U*]->(t)) "
        + "RETURN [r IN relationships(p) | r.eid] AS rels"))
        .containsExactlyInAnyOrder(List.of(30), List.of(31));
    assertThat(expressionEidLists("MATCH (t:T {id: 2}), (q:Q {id: 1}) RETURN allShortestPaths((t)<-[:U*]-(q)) AS ps"))
        .containsExactlyInAnyOrder(List.of(30), List.of(31));
    assertThat(expressionEidLists("MATCH (t:T {id: 2}), (q:Q {id: 1}) RETURN allShortestPaths((t)-[:U*]-(q)) AS ps"))
        .containsExactlyInAnyOrder(List.of(30), List.of(31));
  }

  @Test
  void inlineEdgeFilterFormStillReturnsOnePathPerParallelRelationship() {
    // Control: the edge-aware BFS behind an inline WHERE always tracked the relationship it walked.
    createIssueGraph();
    assertThat(eidLists("MATCH p = allShortestPaths((a:N {id: 1})-[r:R* WHERE r.eid > 0]->(c:N {id: 3})) "
        + "RETURN [x IN relationships(p) | x.eid] AS rels"))
        .containsExactlyInAnyOrder(List.of(10, 20), List.of(11, 20));
  }

  @Test
  void expressionFormReturnsEveryCoShortestPath() {
    createIssueGraph();
    assertThat(expressionEidLists("MATCH (a:N {id: 1}), (c:N {id: 3}) RETURN allShortestPaths((a)-[*]->(c)) AS ps"))
        .containsExactlyInAnyOrder(List.of(10, 20), List.of(11, 20));
    assertThat(expressionEidLists("MATCH (a:N {id: 1}), (c:N {id: 3}) RETURN allShortestPaths((a)-[*]-(c)) AS ps"))
        .containsExactlyInAnyOrder(List.of(10, 20), List.of(11, 20));
  }

  @Test
  void expressionFormWithAnInlineEdgeFilterReturnsEveryCoShortestPath() {
    createIssueGraph();
    assertThat(expressionEidLists(
        "MATCH (a:N {id: 1}), (c:N {id: 3}) RETURN allShortestPaths((a)-[r:R* WHERE r.eid > 0]->(c)) AS ps"))
        .containsExactlyInAnyOrder(List.of(10, 20), List.of(11, 20));
  }

  @Test
  void expressionFormReturnsCoShortestPathsThroughDistinctVertices() {
    // Not only parallel relationships: two co-shortest paths through different middle vertices.
    database.transaction(() -> database.command("opencypher", """
        CREATE (a:N {id: 1}), (b1:N {id: 2}), (b2:N {id: 4}), (c:N {id: 3}),
               (a)-[:R {eid: 10}]->(b1), (b1)-[:R {eid: 20}]->(c),
               (a)-[:R {eid: 11}]->(b2), (b2)-[:R {eid: 21}]->(c)"""));
    assertThat(expressionEidLists("MATCH (a:N {id: 1}), (c:N {id: 3}) RETURN allShortestPaths((a)-[*]->(c)) AS ps"))
        .containsExactlyInAnyOrder(List.of(10, 20), List.of(11, 21));
  }

  private List<List<Object>> eidLists(final String query) {
    final List<List<Object>> out = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext())
        out.add(new ArrayList<>(rs.next().<List<?>>getProperty("rels")));
    }
    return out;
  }

  private List<List<Object>> expressionEidLists(final String query) {
    final List<List<Object>> out = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query)) {
      assertThat(rs.hasNext()).isTrue();
      final List<?> paths = rs.next().getProperty("ps");
      for (final Object path : paths) {
        final List<Object> eids = new ArrayList<>();
        for (final Object element : (List<?>) path)
          if (element instanceof Edge edge)
            eids.add(edge.get("eid"));
        out.add(eids);
      }
      assertThat(rs.hasNext()).isFalse();
    }
    return out;
  }
}
