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
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #5358: variable-length MATCH patterns are native physical operators rather than a reason
 * for the cost-based planner to decline the whole statement.
 */
class CypherVarLengthExpandIssue5358Test {
  private static final String DATABASE_PATH = "./target/databases/cypher-var-length-expand-5358";

  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory(DATABASE_PATH).create();
    database.getSchema().createVertexType("Node").createProperty("id", Type.STRING);
    database.getSchema().createEdgeType("LINK");

    database.transaction(() -> {
      final Map<String, MutableVertex> nodes = new HashMap<>();
      for (final String id : List.of("a", "b", "c", "d", "hub"))
        nodes.put(id, database.newVertex("Node").set("id", id).save());

      link(nodes, "a", "b", "a-b", "ok");
      link(nodes, "a", "c", "a-c", "bad");
      link(nodes, "b", "d", "b-d", "ok");
      link(nodes, "c", "d", "c-d", "ok");
      link(nodes, "d", "hub", "d-hub", "ok");
      link(nodes, "hub", "a", "hub-a", "ok");

      for (int i = 0; i < 64; i++)
        database.newVertex("Node").set("id", "decoy-" + i).save();
    });

    database.transaction(() -> database.getSchema().createTypeIndex(
        Schema.INDEX_TYPE.LSM_TREE, true, "Node", "id"));
  }

  @AfterEach
  void tearDown() {
    if (database != null)
      database.drop();
  }

  @Test
  void boundedUnboundedAndZeroHopPatternsUseThePhysicalOperator() {
    for (final String range : List.of("*1..3", "*1..", "*", "*0..0"))
      assertThat(planOf("MATCH (a:Node {id:'a'})-[:LINK" + range + "]->(b:Node) RETURN b.id AS id"))
          .contains("VarLengthExpand")
          .doesNotContain("Traditional Execution");
  }

  @Test
  void reversedIndexedAnchorPreservesNamedPathAndRelationshipOrder() {
    final String query = """
        MATCH p = (start:Node)-[relationships:LINK*1..3]->(target:Node {id:'hub'})
        RETURN start.id AS start,
               [n IN nodes(p) | n.id] AS nodeIds,
               [r IN relationships | r.step] AS steps
        ORDER BY start, nodeIds
        """;

    assertThat(planOf(query)).contains("VarLengthExpand").contains("NodeIndexSeek");
    assertThat(rows(query)).containsExactly(
        "a|[a, b, d, hub]|[a-b, b-d, d-hub]",
        "a|[a, c, d, hub]|[a-c, c-d, d-hub]",
        "b|[b, d, hub]|[b-d, d-hub]",
        "c|[c, d, hub]|[c-d, d-hub]",
        "d|[d, hub]|[d-hub]");
  }

  @Test
  void inlineRelationshipPredicatesRunInsideThePhysicalTraversal() {
    final String query = """
        MATCH (a:Node {id:'a'})-[relationships:LINK*1..3 WHERE relationships.tag = 'ok']->(b:Node)
        RETURN b.id AS id
        ORDER BY id
        """;

    assertThat(planOf(query)).contains("VarLengthExpand").doesNotContain("Traditional Execution");
    assertThat(singleColumn(query, "id")).containsExactly("b", "d", "hub");
  }

  @Test
  void inlineRelationshipPropertyMapsRunInsideThePhysicalTraversal() {
    final String query = """
        MATCH (a:Node {id:'a'})-[relationships:LINK*1..3 {tag:'ok'}]->(b:Node)
        RETURN b.id AS id
        ORDER BY id
        """;

    assertThat(planOf(query)).contains("VarLengthExpand").doesNotContain("Traditional Execution");
    assertThat(singleColumn(query, "id")).containsExactly("b", "d", "hub");
  }

  @Test
  void relationshipReuseAcrossMatchClausesRemainsValid() {
    final String query = """
        MATCH (a:Node {id:'a'})-[first:LINK*1..1]->(b:Node)
        MATCH (a)-[second:LINK]->(b)
        RETURN b.id AS id
        ORDER BY id
        """;

    assertThat(planOf(query)).contains("VarLengthExpand").contains("ExpandInto");
    assertThat(singleColumn(query, "id")).containsExactly("b", "c");
  }

  @Test
  void explicitPathModesMatchTheTraditionalTraversal() {
    for (final String mode : List.of("TRAIL", "ACYCLIC", "WALK")) {
      final String match = "MATCH " + mode
          + " (a:Node {id:'a'})-[relationships:LINK*1..6]->(b:Node)";
      final String optimized = match + " RETURN b.id AS id ORDER BY id";
      final String traditional = "CALL { " + match + " RETURN b.id AS id } RETURN id ORDER BY id";

      assertThat(planOf(optimized)).contains("VarLengthExpand").doesNotContain("Traditional Execution");
      assertThat(singleColumn(optimized, "id")).isEqualTo(singleColumn(traditional, "id"));
    }
  }

  @Test
  void disconnectedInlineRelationshipPredicatesRemainAnExplicitFallback() {
    assertThat(planOf("""
        MATCH (a:Node {id:'a'})-[r:LINK*1..2 WHERE r.tag = 'ok']->(b:Node),
              (p:Node {id:'a'})-[:LINK]->(q:Node)
        RETURN b.id AS id
        """))
        .contains("Traditional Execution")
        .doesNotContain("VarLengthExpand");
  }

  @Test
  void shortestPathRemainsAnExplicitTraditionalFallback() {
    assertThat(planOf("""
        MATCH p = shortestPath((a:Node {id:'a'})-[:LINK*1..3]->(b:Node {id:'hub'}))
        RETURN length(p) AS hops
        """))
        .contains("Traditional Execution")
        .doesNotContain("VarLengthExpand");
  }

  private void link(final Map<String, MutableVertex> nodes, final String from, final String to,
      final String step, final String tag) {
    nodes.get(from).newEdge("LINK", nodes.get(to), true, (Object[]) null)
        .set("step", step)
        .set("tag", tag)
        .save();
  }

  private String planOf(final String query) {
    try (final ResultSet resultSet = database.query("opencypher", "EXPLAIN " + query)) {
      assertThat(resultSet.hasNext()).isTrue();
      return String.valueOf(resultSet.next().<Object>getProperty("executionPlan"));
    }
  }

  private List<String> rows(final String query) {
    final List<String> rows = new ArrayList<>();
    try (final ResultSet resultSet = database.query("opencypher", query)) {
      while (resultSet.hasNext()) {
        final Result row = resultSet.next();
        rows.add(row.<Object>getProperty("start") + "|" + row.<Object>getProperty("nodeIds") + "|"
            + row.<Object>getProperty("steps"));
      }
    }
    return rows;
  }

  private List<String> singleColumn(final String query, final String column) {
    final List<String> values = new ArrayList<>();
    try (final ResultSet resultSet = database.query("opencypher", query)) {
      while (resultSet.hasNext())
        values.add(String.valueOf(resultSet.next().<Object>getProperty(column)));
    }
    return values;
  }
}
