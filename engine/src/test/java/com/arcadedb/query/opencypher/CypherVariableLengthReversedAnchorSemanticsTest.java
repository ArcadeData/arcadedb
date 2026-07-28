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
 * Semantic parity coverage for the indexed-anchor reversal bridge used by the traditional Cypher
 * executor (PRs #5357, #5387, #5393).
 * <p>
 * Every test runs the same query twice against the same graph: once anchored on the indexed
 * {@code id} property, which lets the bridge reverse the traversal, and once anchored on the
 * non-indexed {@code name} property, which keeps the original source-first traversal. Both runs must
 * be indistinguishable: same rows, same multiplicity, same written path order.
 * <p>
 * The graph is deliberately hostile to a naive reversal: a diamond gives two distinct paths between
 * the same pair of vertices, and a back edge closes a cycle so unbounded expansion has to rely on
 * relationship uniqueness to terminate.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherVariableLengthReversedAnchorSemanticsTest {
  private static final String DATABASE_PATH = "./target/databases/cypher-vlp-reversed-anchor-semantics";

  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory(DATABASE_PATH).create();
    database.getSchema().createVertexType("Node").createProperty("id", Type.STRING);
    database.getSchema().createEdgeType("LINK");

    database.transaction(() -> {
      final Map<String, MutableVertex> vertices = new HashMap<>();
      for (final String id : List.of("a", "b", "c", "d", "hub"))
        vertices.put(id, node(id));

      // diamond: two distinct 2-hop trails from "a" to "d"
      link(vertices, "a", "b");
      link(vertices, "a", "c");
      link(vertices, "b", "d");
      link(vertices, "c", "d");
      link(vertices, "d", "hub");
      // back edge closing a cycle so unbounded expansion must rely on relationship uniqueness
      link(vertices, "hub", "a");

      for (int i = 0; i < 128; i++)
        node("decoy-" + i);
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
  void unboundedReversalKeepsRowMultiplicity() {
    assertParity("""
        MATCH (n:Node)-[:LINK*]->(hub:Node)
        WHERE hub.%s = 'hub'
        RETURN n.id AS id
        ORDER BY id""");
  }

  @Test
  void openEndedMinimumHopsReversalKeepsRowMultiplicity() {
    assertParity("""
        MATCH (n:Node)-[:LINK*2..]->(hub:Node)
        WHERE hub.%s = 'hub'
        RETURN n.id AS id
        ORDER BY id""");
  }

  @Test
  void zeroLengthOpenEndedReversalKeepsRowMultiplicity() {
    assertParity("""
        MATCH (n:Node)-[:LINK*0..]->(hub:Node)
        WHERE hub.%s = 'hub'
        RETURN n.id AS id
        ORDER BY id""");
  }

  @Test
  void boundedReversalKeepsRowMultiplicity() {
    assertParity("""
        MATCH (n:Node)-[:LINK*1..3]->(hub:Node)
        WHERE hub.%s = 'hub'
        RETURN n.id AS id
        ORDER BY id""");
  }

  @Test
  void undirectedUnboundedReversalKeepsRowMultiplicity() {
    assertParity("""
        MATCH (n:Node)-[:LINK*1..2]-(hub:Node)
        WHERE hub.%s = 'hub'
        RETURN n.id AS id
        ORDER BY id""");
  }

  @Test
  void anonymousSourceUnboundedReversalKeepsPathCount() {
    assertParity("""
        MATCH (:Node)-[:LINK*]->(hub:Node)
        WHERE hub.%s = 'hub'
        RETURN count(*) AS paths""");
  }

  @Test
  void anonymousSourceReversalDoesNotRebindTheAnchor() {
    assertParity("""
        MATCH (:Node)-[:LINK*1..2]->(hub:Node)
        WHERE hub.%s = 'hub'
        RETURN hub.id AS anchor, count(*) AS paths
        ORDER BY anchor""");
  }

  @Test
  void unboundedReversalKeepsWrittenRelationshipOrder() {
    assertParity("""
        MATCH (n:Node)-[relationships:LINK*]->(hub:Node)
        WHERE hub.%s = 'hub'
        RETURN n.id AS id, [r IN relationships | r.step] AS steps
        ORDER BY id, steps""");
  }

  /**
   * An inline node property constraint used to keep the optimizer out entirely, and with it the
   * anchor the bridge needs. Now that the property map is planned as the equality predicate it
   * stands for, the bridge reverses this shape too, and the reversed expansion applies the written
   * source node's own property map as its target filter.
   */
  @Test
  void anInlinePropertyOnTheWrittenSourceIsAppliedByTheReversedExpansion() {
    final String inlineProperty = """
        MATCH (n:Node {name: 'a'})-[:LINK*]->(hub:Node)
        WHERE hub.id = 'hub'
        RETURN n.id AS id
        ORDER BY id""";

    assertThat(planStartsFromAnchor(inlineProperty)).isTrue();
    assertThat(rows(inlineProperty)).containsExactly("id=a;", "id=a;");
  }

  /**
   * A named path variable still keeps the optimizer out, so no physical anchor exists and the bridge
   * cannot engage. The query must return the correct rows through the plain source-first plan; this
   * pins that a future eligibility change cannot silently reroute it unnoticed.
   */
  @Test
  void aNamedPathStaysIneligibleForTheBridgeAndCorrect() {
    final String namedPath = """
        MATCH p = (n:Node)-[:LINK*]->(hub:Node)
        WHERE hub.id = 'hub'
        RETURN [x IN nodes(p) | x.id] AS ids
        ORDER BY ids""";

    assertThat(planStartsFromAnchor(namedPath)).isFalse();
    assertThat(rows(namedPath)).containsExactly(
        "ids=[a, b, d, hub];", "ids=[a, c, d, hub];", "ids=[b, d, hub];", "ids=[c, d, hub];",
        "ids=[d, hub];", "ids=[hub, a, b, d, hub];", "ids=[hub, a, c, d, hub];");
  }

  @Test
  void unboundedReversalKeepsSourcePredicateInWhere() {
    assertParity("""
        MATCH (n:Node)-[:LINK*]->(hub:Node)
        WHERE hub.%s = 'hub' AND n.id <> 'a'
        RETURN n.id AS id
        ORDER BY id""");
  }

  @Test
  void inListUnboundedReversalKeepsRowMultiplicity() {
    assertParity("""
        MATCH (n:Node)-[:LINK*]->(hub:Node)
        WHERE hub.%s IN ['hub', 'd']
        RETURN hub.id AS target, n.id AS id
        ORDER BY target, id""");
  }

  /**
   * Runs the query anchored on the indexed property (bridge reverses the traversal) and on the
   * non-indexed twin property (bridge stays out of the way), then compares rows one by one.
   */
  private void assertParity(final String queryTemplate) {
    final String indexed = queryTemplate.formatted("id");
    final String notIndexed = queryTemplate.formatted("name");

    assertThat(planStartsFromAnchor(indexed))
        .as("the indexed variant must start from the anchor: %s", indexed)
        .isTrue();
    assertThat(planStartsFromAnchor(notIndexed))
        .as("the non-indexed variant must keep the source-first plan: %s", notIndexed)
        .isFalse();

    assertThat(rows(indexed))
        .as("reversed and source-first execution must agree: %s", indexed)
        .isEqualTo(rows(notIndexed));
  }

  private boolean planStartsFromAnchor(final String query) {
    try (final ResultSet resultSet = database.query("opencypher", "PROFILE " + query)) {
      while (resultSet.hasNext())
        resultSet.next();
      final String firstStep = resultSet.getExecutionPlan().orElseThrow().getSteps().getFirst().getDescription();
      return firstStep.contains("(hub:Node)") && firstStep.contains("[index: Node[id]]");
    }
  }

  private List<String> rows(final String query) {
    final List<String> rows = new ArrayList<>();
    try (final ResultSet resultSet = database.query("opencypher", query)) {
      while (resultSet.hasNext()) {
        final Result row = resultSet.next();
        final StringBuilder buffer = new StringBuilder();
        for (final String property : row.getPropertyNames().stream().sorted().toList())
          buffer.append(property).append('=').append(String.valueOf(row.<Object>getProperty(property))).append(';');
        rows.add(buffer.toString());
      }
    }
    return rows;
  }

  private MutableVertex node(final String id) {
    return database.newVertex("Node").set("id", id).set("name", id).save();
  }

  private void link(final Map<String, MutableVertex> vertices, final String from, final String to) {
    vertices.get(from).newEdge("LINK", vertices.get(to), true, (Object[]) null).set("step", from + "-" + to).save();
  }
}
