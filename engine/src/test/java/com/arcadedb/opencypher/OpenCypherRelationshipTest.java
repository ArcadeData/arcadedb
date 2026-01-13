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
package com.arcadedb.opencypher;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.opencypher.ast.Direction;
import com.arcadedb.opencypher.ast.NodePattern;
import com.arcadedb.opencypher.ast.PathPattern;
import com.arcadedb.opencypher.ast.RelationshipPattern;
import com.arcadedb.opencypher.parser.PatternParser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for relationship pattern parsing and graph traversal.
 * Phase 2: Tests for relationship patterns and path matching.
 */
public class OpenCypherRelationshipTest {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/testopencypher-rel").create();
    setupTestGraph();
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  private void setupTestGraph() {
    // Create schema
    database.getSchema().createVertexType("Person");
    database.getSchema().createVertexType("Company");
    database.getSchema().createEdgeType("KNOWS");
    database.getSchema().createEdgeType("WORKS_FOR");

    // Create test data
    database.transaction(() -> {
      final MutableVertex alice = database.newVertex("Person").set("name", "Alice").set("age", 30).save();
      final MutableVertex bob = database.newVertex("Person").set("name", "Bob").set("age", 35).save();
      final MutableVertex charlie = database.newVertex("Person").set("name", "Charlie").set("age", 25).save();
      final MutableVertex acme = database.newVertex("Company").set("name", "Acme Corp").save();

      alice.newEdge("KNOWS", bob, true, new Object[]{"since", 2020}).save();
      bob.newEdge("KNOWS", charlie, true, new Object[]{"since", 2021}).save();
      alice.newEdge("WORKS_FOR", acme, true, (Object[]) null).save();
      bob.newEdge("WORKS_FOR", acme, true, (Object[]) null).save();
    });
  }

  @Test
  void testParseSimpleRelationshipPattern() {
    final List<PathPattern> patterns = PatternParser.parsePathPatterns("(a:Person)-[r:KNOWS]->(b:Person)");

    assertThat(patterns).hasSize(1);

    final PathPattern path = patterns.get(0);
    assertThat(path.getNodeCount()).isEqualTo(2);
    assertThat(path.getRelationshipCount()).isEqualTo(1);

    final NodePattern node1 = path.getNode(0);
    assertThat(node1.getVariable()).isEqualTo("a");
    assertThat(node1.getLabels()).containsExactly("Person");

    final RelationshipPattern rel = path.getRelationship(0);
    assertThat(rel.getVariable()).isEqualTo("r");
    assertThat(rel.getTypes()).containsExactly("KNOWS");
    assertThat(rel.getDirection()).isEqualTo(Direction.OUT);

    final NodePattern node2 = path.getNode(1);
    assertThat(node2.getVariable()).isEqualTo("b");
    assertThat(node2.getLabels()).containsExactly("Person");
  }

  @Test
  void testParseIncomingRelationshipPattern() {
    final List<PathPattern> patterns = PatternParser.parsePathPatterns("(a)<-[r:KNOWS]-(b)");

    assertThat(patterns).hasSize(1);

    final PathPattern path = patterns.get(0);
    final RelationshipPattern rel = path.getRelationship(0);

    assertThat(rel.getDirection()).isEqualTo(Direction.IN);
  }

  @Test
  void testParseBidirectionalRelationshipPattern() {
    final List<PathPattern> patterns = PatternParser.parsePathPatterns("(a)-[r:KNOWS]-(b)");

    assertThat(patterns).hasSize(1);

    final PathPattern path = patterns.get(0);
    final RelationshipPattern rel = path.getRelationship(0);

    assertThat(rel.getDirection()).isEqualTo(Direction.BOTH);
  }

  @Test
  void testParseVariableLengthPattern() {
    final List<PathPattern> patterns = PatternParser.parsePathPatterns("(a)-[r:KNOWS*1..3]->(b)");

    assertThat(patterns).hasSize(1);

    final PathPattern path = patterns.get(0);
    final RelationshipPattern rel = path.getRelationship(0);

    assertThat(rel.isVariableLength()).isTrue();
    assertThat(rel.getMinHops()).isEqualTo(1);
    assertThat(rel.getMaxHops()).isEqualTo(3);
    assertThat(rel.getTypes()).containsExactly("KNOWS");
  }

  @Test
  void testParseVariableLengthUnbounded() {
    final List<PathPattern> patterns = PatternParser.parsePathPatterns("(a)-[*1..]->(b)");

    assertThat(patterns).hasSize(1);

    final PathPattern path = patterns.get(0);
    final RelationshipPattern rel = path.getRelationship(0);

    assertThat(rel.isVariableLength()).isTrue();
    assertThat(rel.getMinHops()).isEqualTo(1);
    assertThat(rel.getMaxHops()).isNull();
    assertThat(rel.getEffectiveMaxHops()).isEqualTo(Integer.MAX_VALUE);
  }

  @Test
  void testParseAnonymousRelationship() {
    final List<PathPattern> patterns = PatternParser.parsePathPatterns("(a)-[]->(b)");

    assertThat(patterns).hasSize(1);

    final PathPattern path = patterns.get(0);
    final RelationshipPattern rel = path.getRelationship(0);

    assertThat(rel.getVariable()).isNull();
    assertThat(rel.hasTypes()).isFalse();
    assertThat(rel.getDirection()).isEqualTo(Direction.OUT);
  }

  @Test
  void testParseSingleNodePattern() {
    final List<PathPattern> patterns = PatternParser.parsePathPatterns("(n:Person)");

    assertThat(patterns).hasSize(1);

    final PathPattern path = patterns.get(0);
    assertThat(path.isSingleNode()).isTrue();
    assertThat(path.getNodeCount()).isEqualTo(1);
    assertThat(path.getRelationshipCount()).isEqualTo(0);

    final NodePattern node = path.getNode(0);
    assertThat(node.getVariable()).isEqualTo("n");
    assertThat(node.getLabels()).containsExactly("Person");
  }

  @Test
  void testParseNodePatternWithoutLabel() {
    final List<PathPattern> patterns = PatternParser.parsePathPatterns("(n)");

    assertThat(patterns).hasSize(1);

    final PathPattern path = patterns.get(0);
    final NodePattern node = path.getNode(0);

    assertThat(node.getVariable()).isEqualTo("n");
    assertThat(node.hasLabels()).isFalse();
  }

  @Test
  void testParseAnonymousNodePattern() {
    final List<PathPattern> patterns = PatternParser.parsePathPatterns("(:Person)-[:KNOWS]->(:Person)");

    assertThat(patterns).hasSize(1);

    final PathPattern path = patterns.get(0);
    assertThat(path.getNodeCount()).isEqualTo(2);

    final NodePattern node1 = path.getNode(0);
    assertThat(node1.getVariable()).isNull();
    assertThat(node1.getLabels()).containsExactly("Person");

    final NodePattern node2 = path.getNode(1);
    assertThat(node2.getVariable()).isNull();
    assertThat(node2.getLabels()).containsExactly("Person");
  }

  @Test
  void testRelationshipPatternEffectiveHops() {
    final RelationshipPattern rel1 = new RelationshipPattern("r", null, Direction.OUT, null, null, null);
    assertThat(rel1.getEffectiveMinHops()).isEqualTo(1);
    assertThat(rel1.getEffectiveMaxHops()).isEqualTo(1);
    assertThat(rel1.isVariableLength()).isFalse();

    final RelationshipPattern rel2 = new RelationshipPattern("r", null, Direction.OUT, null, 2, 5);
    assertThat(rel2.getEffectiveMinHops()).isEqualTo(2);
    assertThat(rel2.getEffectiveMaxHops()).isEqualTo(5);
    assertThat(rel2.isVariableLength()).isTrue();

    final RelationshipPattern rel3 = new RelationshipPattern("r", null, Direction.OUT, null, 0, null);
    assertThat(rel3.getEffectiveMinHops()).isEqualTo(1);
    assertThat(rel3.getEffectiveMaxHops()).isEqualTo(Integer.MAX_VALUE);
    assertThat(rel3.isVariableLength()).isTrue();
  }

  @Test
  void testPathPatternToString() {
    final NodePattern node1 = new NodePattern("a", List.of("Person"), null);
    final RelationshipPattern rel = new RelationshipPattern("r", List.of("KNOWS"), Direction.OUT, null, null, null);
    final NodePattern node2 = new NodePattern("b", List.of("Person"), null);

    final PathPattern path = new PathPattern(node1, rel, node2);

    final String pathStr = path.toString();
    assertThat(pathStr).contains("a");
    assertThat(pathStr).contains("Person");
    assertThat(pathStr).contains("KNOWS");
    assertThat(pathStr).contains("b");
  }
}
