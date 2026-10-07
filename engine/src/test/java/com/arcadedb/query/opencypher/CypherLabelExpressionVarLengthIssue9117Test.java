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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #9117: a label expression with {@code !}, {@code %} or a mix of {@code &} and {@code |}
 * on a variable-length relationship ({@code [:!R*1..3]}, {@code shortestPath((a)-[:!R*]-(b))}) was refused at parse
 * time. It is now evaluated against every relationship of the path, as Neo4j does.
 * <p>
 * Fixture: {@code (1:A)-[:R]->(2:B)}, {@code (1)-[:S]->(3:A:B)}, {@code (2)-[:T]->(4)}, {@code (3)-[:R]->(5:C)}.
 */
class CypherLabelExpressionVarLengthIssue9117Test {
  private Database database;

  @BeforeEach
  void setUp() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/cypher-label-expression-9117");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.transaction(() -> database.command("opencypher", """
        CREATE (a:A {id: 1}), (b:B {id: 2}), (c:A:B {id: 3}), (d {id: 4}), (e:C {id: 5}),
               (a)-[:R]->(b), (a)-[:S]->(c), (b)-[:T]->(d), (c)-[:R]->(e)"""));
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  // ---- the three queries of the issue --------------------------------------------------------------------------

  @Test
  void negatedTypeOnAVariableLengthRelationship() {
    // Every hop must not be R: from 1 only the S hop qualifies, and 3-[:R]->5 stops the walk there.
    assertThat(column("MATCH (a {id: 1})-[:!R*1..3]->(b) RETURN b.id AS id ORDER BY id", "id")).containsExactly(3);
    assertThat(pairs("MATCH (a)-[:!R*1..3]->(b) RETURN a.id AS a, b.id AS b ORDER BY a, b")).containsExactly("1-3", "2-4");
  }

  @Test
  void negatedTypeInsideShortestPath() {
    // Without R the only route from 1 to 4 (1-R-2-T-4) is cut, so there is no path at all.
    assertThat(column("MATCH p = shortestPath((a {id: 1})-[:!R*]-(b {id: 4})) RETURN length(p) AS l", "l")).isEmpty();
    // Without S the route over R and T stays.
    assertThat(column("MATCH p = shortestPath((a {id: 1})-[:!S*]-(b {id: 4})) RETURN length(p) AS l", "l"))
        .containsExactly(2L);
    // Every hop is checked, not only the first one: 1-S-3-R-5 is the only route from 1 to 5.
    assertThat(column("MATCH p = shortestPath((a {id: 1})-[:!R*]-(b {id: 5})) RETURN length(p) AS l", "l")).isEmpty();
    assertThat(column("MATCH p = shortestPath((a {id: 1})-[:!T*]-(b {id: 5})) RETURN length(p) AS l", "l"))
        .containsExactly(2L);
  }

  @Test
  void mixedConjunctionAndDisjunctionWithANegation() {
    // (R|S)&!T: 1-R->2, 1-S->3, 1-S->3-R->5, never 2-T->4.
    assertThat(column("MATCH (a {id: 1})-[:(R|S)&!T*]->(b) RETURN b.id AS id ORDER BY id", "id")).containsExactly(2, 3, 5);
  }

  // ---- the other operators and the other places a variable-length relationship can appear ----------------------

  @Test
  void wildcardMatchesEveryTypeAndItsNegationNone() {
    assertThat(column("MATCH (a {id: 1})-[:%*1..3]->(b) RETURN b.id AS id ORDER BY id", "id")).containsExactly(2, 3, 4, 5);
    assertThat(column("MATCH (a {id: 1})-[:!%*1..3]->(b) RETURN b.id AS id", "id")).isEmpty();
    // The zero-length path walks no relationship, so the expression has nothing to refuse.
    assertThat(column("MATCH (a {id: 1})-[:!%*0..3]->(b) RETURN b.id AS id", "id")).containsExactly(1);
  }

  @Test
  void conjunctionOfTypesMatchesNoHop() {
    // A relationship has exactly one type, so R&S can be no hop at all.
    assertThat(column("MATCH (a {id: 1})-[:R&S*1..3]->(b) RETURN b.id AS id", "id")).isEmpty();
    assertThat(column("MATCH (a {id: 1})-[:R&!S*1..3]->(b) RETURN b.id AS id", "id")).containsExactly(2);
  }

  @Test
  void undirectedAndReversedExpansionCheckEveryHop() {
    // 4-T-2 qualifies, 2-R-1 does not, so the walk from 4 stops at 2.
    assertThat(column("MATCH (a {id: 4})-[:!R*1..3]-(b) RETURN b.id AS id", "id")).containsExactly(2);
    assertThat(column("MATCH (a {id: 5})<-[:!S*1..3]-(b) RETURN b.id AS id", "id")).containsExactly(3);
  }

  @Test
  void namedVariableBindsTheListOfMatchingRelationships() {
    assertThat(column("MATCH (a {id: 1})-[r:!T*2]->(b) RETURN [x IN r | type(x)] AS t", "t"))
        .containsExactly(List.of("S", "R"));
  }

  @Test
  void combinesWithAnInlineWhere() {
    assertThat(column("MATCH (a {id: 1})-[r:!T* WHERE type(r) <> 'S']->(b) RETURN b.id AS id ORDER BY id", "id"))
        .containsExactly(2);
  }

  @Test
  void labelledEndpointsTakeTheSamePath() {
    // Labels on both ends make the pattern eligible for the cost-based optimizer's variable-length expansion.
    try (final ResultSet rs = database.query("opencypher", "EXPLAIN MATCH (a:A)-[:!R*1..3]->(b:B) RETURN a.id, b.id")) {
      assertThat(rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2)).contains("VarLengthExpand");
    }
    assertThat(column("MATCH (a:A)-[:!R*1..3]->(b:B) RETURN a.id + '-' + b.id AS p ORDER BY p", "p")).containsExactly("1-3");
    assertThat(column("MATCH (a:A)-[:!R*1..3]->(b:B) RETURN count(*) AS c", "c")).containsExactly(1L);
  }

  @Test
  void countIsNotPushedDownPastTheExpression() {
    assertThat(column("MATCH ()-[:!R*1..3]->() RETURN count(*) AS c", "c")).containsExactly(2L);
    assertThat(column("MATCH ()-[:%*1..3]->() RETURN count(*) AS c", "c")).containsExactly(6L);
  }

  @Test
  void allShortestPathsChecksEveryHop() {
    assertThat(column("MATCH p = allShortestPaths((a {id: 1})-[:!R*]-(b {id: 4})) RETURN length(p) AS l", "l")).isEmpty();
    assertThat(column("MATCH p = allShortestPaths((a {id: 1})-[:!S*]-(b {id: 4})) RETURN length(p) AS l", "l"))
        .containsExactly(2L);
  }

  @Test
  void shortestPathAsAnExpression() {
    assertThat(column("MATCH (a {id: 1}), (b {id: 4}) RETURN shortestPath((a)-[:!R*]-(b)) AS p", "p")).containsExactly((Object) null);
    final List<Object> paths = column("MATCH (a {id: 1}), (b {id: 4}) RETURN length(shortestPath((a)-[:!S*]-(b))) AS l", "l");
    assertThat(paths).containsExactly(2L);
  }

  @Test
  void insideAPatternPredicateAndExists() {
    assertThat(column("MATCH (n) WHERE (n)-[:!R*1..3]->({id: 4}) RETURN n.id AS id ORDER BY id", "id")).containsExactly(2);
    assertThat(column("MATCH (n) WHERE exists((n)-[:!S*2]->()) RETURN n.id AS id ORDER BY id", "id")).containsExactly(1);
    assertThat(column("MATCH (n) WHERE EXISTS { (n)-[:!T*2]->(:C) } RETURN n.id AS id", "id")).containsExactly(1);
  }

  @Test
  void insideAPatternComprehension() {
    assertThat(column("MATCH (n {id: 1}) RETURN [(n)-[:!R*1..3]->(m) | m.id] AS ids", "ids")).containsExactly(List.of(3));
    assertThat(column("MATCH (n {id: 1}) RETURN [(n)-[:(R|S)&!T*1..3]->(m) | m.id] AS ids", "ids"))
        .satisfiesExactly(ids -> assertThat((List<Object>) ids).containsExactlyInAnyOrder(2, 3, 5));
  }

  @Test
  void returnStarDoesNotExposeTheVariableTheAnonymousRelationshipWasGiven() {
    try (final ResultSet rs = database.query("opencypher", "MATCH (a {id: 1})-[:!R*1..3]->(b) RETURN *")) {
      final List<Result> rows = rs.stream().toList();
      assertThat(rows).hasSize(1);
      assertThat(rows.getFirst().getPropertyNames()).containsExactlyInAnyOrder("a", "b");
    }
  }

  private List<String> pairs(final String query) {
    final List<String> out = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query, Map.of())) {
      rs.stream().forEach(r -> out.add(r.getProperty("a") + "-" + r.getProperty("b")));
    }
    return out;
  }

  private List<Object> column(final String query, final String column) {
    final List<Object> out = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query, Map.of())) {
      rs.stream().forEach(r -> out.add(r.getProperty(column)));
    }
    return out;
  }
}
