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
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.database.RID;
import com.arcadedb.graph.MutableVertex;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Map;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

;

/**
 * Tests for MERGE clause in OpenCypher queries.
 */
public class OpenCypherMergeTest {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/testopencypher-merge").create();
    database.getSchema().createVertexType("Person");
    database.getSchema().createVertexType("Company");
    database.getSchema().createEdgeType("KNOWS");
    database.getSchema().createEdgeType("WORKS_AT");
    database.getSchema().createEdgeType("in");
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void mergeCreatesNodeWhenNotExists() {
    database.transaction(() -> {
      database.command("opencypher", "MERGE (n:Person {name: 'Alice'})");
    });

    final ResultSet verify = database.query("opencypher", "MATCH (n:Person {name: 'Alice'}) RETURN n");
    assertThat(verify.hasNext()).isTrue();
    final Vertex v = (Vertex) verify.next().toElement();
    assertThat((String) v.get("name")).isEqualTo("Alice");
  }

  @Test
  void mergeFindsNodeWhenExists() {
    // Create node
    database.transaction(() -> {
      database.command("opencypher", "CREATE (n:Person {name: 'Bob'})");
    });

    // MERGE should find it, not create duplicate
    database.transaction(() -> {
      database.command("opencypher", "MERGE (n:Person {name: 'Bob'})");
    });

    // Verify only one Bob exists
    final ResultSet verify = database.query("opencypher", "MATCH (n:Person {name: 'Bob'}) RETURN n");
    int count = 0;
    while (verify.hasNext()) {
      verify.next();
      count++;
    }
    assertThat(count).isEqualTo(1);
  }

  @Test
  void mergeWithReturn() {
    database.transaction(() -> {
      final ResultSet result = database.command("opencypher", "MERGE (n:Person {name: 'Charlie'}) RETURN n");
      assertThat(result.hasNext()).isTrue();
      final Vertex v = (Vertex) result.next().toElement();
      assertThat((String) v.get("name")).isEqualTo("Charlie");
    });
  }

  @Test
  void mergeMultipleTimes() {
    // First MERGE creates
    database.transaction(() -> {
      database.command("opencypher", "MERGE (n:Person {name: 'David', age: 30})");
    });

    // Second MERGE finds
    database.transaction(() -> {
      database.command("opencypher", "MERGE (n:Person {name: 'David', age: 30})");
    });

    // Third MERGE finds
    database.transaction(() -> {
      database.command("opencypher", "MERGE (n:Person {name: 'David', age: 30})");
    });

    // Verify only one David exists
    final ResultSet verify = database.query("opencypher", "MATCH (n:Person {name: 'David'}) RETURN n");
    int count = 0;
    while (verify.hasNext()) {
      verify.next();
      count++;
    }
    assertThat(count).isEqualTo(1);
  }

  @Test
  void mergeRelationship() {
    // Create nodes first
    database.transaction(() -> {
      database.command("opencypher", "CREATE (a:Person {name: 'Eve'})");
      database.command("opencypher", "CREATE (b:Person {name: 'Frank'})");
    });

    // MERGE relationship
    database.transaction(() -> {
      database.command("opencypher",
          "MERGE (a:Person {name: 'Eve'})-[r:KNOWS]->(b:Person {name: 'Frank'})");
    });

    // Verify relationship exists
    ResultSet verify = database.query("opencypher",
        "MATCH (a:Person {name: 'Eve'})-[r:KNOWS]->(b:Person {name: 'Frank'}) RETURN r");
    assertThat(verify.hasNext()).isTrue();

    // MERGE again - should find existing relationship
    database.transaction(() -> {
      database.command("opencypher",
          "MERGE (a:Person {name: 'Eve'})-[r:KNOWS]->(b:Person {name: 'Frank'})");
    });

    // Verify still only one relationship
    verify = database.query("opencypher",
        "MATCH (a:Person {name: 'Eve'})-[r:KNOWS]->(b:Person {name: 'Frank'}) RETURN r");
    int count = 0;
    while (verify.hasNext()) {
      verify.next();
      count++;
    }
    assertThat(count).isEqualTo(1);
  }

  /**
   * Test that MERGE with label only (no properties) finds existing node instead of creating duplicates.
   * This is the pattern: MERGE (n:PIPELINE_CONFIG) ON CREATE SET n.pipelines = ["miaou"] ON MATCH SET n.pipelines = ["miaou"]
   */
  @Test
  void mergeLabelOnlyFindsExistingNode() {
    database.getSchema().createVertexType("PIPELINE_CONFIG");

    // First MERGE should create the node
    database.transaction(() -> {
      final ResultSet result = database.command("opencypher",
          "MERGE (n:PIPELINE_CONFIG) ON CREATE SET n.pipelines = ['miaou'] ON MATCH SET n.pipelines = ['miaou'] RETURN n.pipelines as pipelines");
      assertThat(result.hasNext()).isTrue();
      result.next();
    });

    // Second MERGE should find the existing node, not create a duplicate
    database.transaction(() -> {
      final ResultSet result = database.command("opencypher",
          "MERGE (n:PIPELINE_CONFIG) ON CREATE SET n.pipelines = ['miaou'] ON MATCH SET n.pipelines = ['miaou'] RETURN n.pipelines as pipelines");
      assertThat(result.hasNext()).isTrue();
      result.next();
    });

    // Verify only one node exists
    final ResultSet verify = database.query("opencypher",
        "MATCH (n:PIPELINE_CONFIG) RETURN n");
    int count = 0;
    while (verify.hasNext()) {
      verify.next();
      count++;
    }
    assertThat(count).isEqualTo(1);
  }

  /**
   * Test that MERGE with label only (no properties) correctly triggers ON CREATE SET on first call
   * and ON MATCH SET on subsequent calls.
   */
  @Test
  void mergeLabelOnlyWithOnCreateAndOnMatchSet() {
    database.getSchema().createVertexType("SINGLETON");

    // First MERGE should create and apply ON CREATE SET
    ResultSet result = database.command("opencypher",
        "MERGE (n:SINGLETON) ON CREATE SET n.status = 'created', n.count = 1 ON MATCH SET n.status = 'matched', n.count = 2 RETURN n");
    assertThat(result.hasNext()).isTrue();
    Vertex v = (Vertex) result.next().toElement();
    assertThat(v.get("status")).isEqualTo("created");
    assertThat(((Number) v.get("count")).intValue()).isEqualTo(1);

    // Second MERGE should match and apply ON MATCH SET
    result = database.command("opencypher",
        "MERGE (n:SINGLETON) ON CREATE SET n.status = 'created', n.count = 1 ON MATCH SET n.status = 'matched', n.count = 2 RETURN n");
    assertThat(result.hasNext()).isTrue();
    v = (Vertex) result.next().toElement();
    assertThat(v.get("status")).isEqualTo("matched");
    assertThat(((Number) v.get("count")).intValue()).isEqualTo(2);
  }

  /**
   * Test for issue #3217: Backticks in relationship types should be treated as escape characters,
   * not included in the relationship type name.
   */
  @Test
  void mergeRelationshipWithBackticksInTypeName() {
    // Create nodes first
    database.transaction(() -> {
      database.command("opencypher", "CREATE (a:Person {name: 'Alice'})");
      database.command("opencypher", "CREATE (b:Company {name: 'TechCorp'})");
    });

    // MERGE relationship using backticks around the type name 'in' (which is a reserved keyword)
    database.transaction(() -> {
      database.command("opencypher",
          """
          MATCH (a:Person {name: 'Alice'}), (b:Company {name: 'TechCorp'}) \
          MERGE (a)-[r:`in`]->(b) RETURN a, b, r""");
    });

    // Verify the relationship type is "in" (without backticks)
    final ResultSet verify = database.query("opencypher",
        "MATCH (a:Person)-[r:`in`]->(b:Company) RETURN type(r) as relType");
    assertThat(verify.hasNext()).isTrue();
    final String relType = (String) verify.next().getProperty("relType");

    // The relationship type should be "in", NOT "`in`" (backticks should not be included)
    assertThat(relType).isEqualTo("in");
    assertThat(relType).doesNotContain("`");

    // MERGE again - should find the existing relationship (proves backticks are treated consistently)
    database.transaction(() -> {
      database.command("opencypher",
          """
          MATCH (a:Person {name: 'Alice'}), (b:Company {name: 'TechCorp'}) \
          MERGE (a)-[r2:`in`]->(b) RETURN r2""");
    });

    // Verify still only one relationship
    final ResultSet countVerify = database.query("opencypher",
        "MATCH (a:Person)-[r:`in`]->(b:Company) RETURN count(r) as cnt");
    assertThat(countVerify.hasNext()).isTrue();
    final Long count = (Long) countVerify.next().getProperty("cnt");
    assertThat(count).isEqualTo(1L);
  }

  /**
   * Test for issue #3217: Backticks in node labels should also be treated as escape characters.
   */
  @Test
  void createNodeWithBackticksInLabel() {
    // Create edge type for reserved keyword
    database.getSchema().createVertexType("select");

    // Create node using backticks around the label 'select' (which is a reserved keyword)
    database.transaction(() -> {
      database.command("opencypher", "CREATE (n:`select` {id: 1})");
    });

    // Verify the node label is "select" (without backticks)
    final ResultSet verify = database.query("opencypher",
        "MATCH (n:`select`) RETURN labels(n) as nodeLabels");
    assertThat(verify.hasNext()).isTrue();
    final Object labelsObj = verify.next().getProperty("nodeLabels");
    assertThat(labelsObj).isInstanceOf(List.class);

    @SuppressWarnings("unchecked")
    final List<String> labels = (List<String>) labelsObj;
    assertThat(labels).hasSize(1);
    assertThat(labels.get(0)).isEqualTo("select");
    assertThat(labels.get(0)).doesNotContain("`");
  }

  /** See issue #3131 */
  @Nested
  class MatchIdThenMergeRelationshipRegression {
    private Database database;
    private RID sourceRid;
    private RID targetRid;

    @BeforeEach
    void setUp() {
      database = new DatabaseFactory("./target/databases/issue-3131").create();

      // Create schema
      database.getSchema().createVertexType("Node");

      // Create test nodes
      database.transaction(() -> {
        final MutableVertex source = database.newVertex("Node");
        source.set("name", "Source");
        source.save();
        sourceRid = source.getIdentity();

        final MutableVertex target = database.newVertex("Node");
        target.set("name", "Target");
        target.save();
        targetRid = target.getIdentity();
      });
    }

    @AfterEach
    void tearDown() {
      if (database != null) {
        database.drop();
        database = null;
      }
    }

    @Test
    void matchWithIdThenMergeRelationship() {
      // This query should:
      // 1. MATCH two nodes by ID
      // 2. MERGE a relationship between them
      // 3. RETURN all three elements

      final ResultSet rs = database.command("opencypher",
          """
          MATCH (a), (b) WHERE ID(a) = $source_id AND ID(b) = $target_id \
          MERGE (a)-[r:in]->(b) \
          RETURN a, b, r""",
          Map.of("source_id", sourceRid.toString(), "target_id", targetRid.toString()));

      assertThat(rs.hasNext()).as("Should return one result").isTrue();

      final Result result = rs.next();

      // Verify all three elements are returned
      assertThat(result.getPropertyNames()).as("Should return a, b, and r")
          .containsExactlyInAnyOrder("a", "b", "r");

      // Verify the nodes
      assertThat((Object) result.getProperty("a")).as("a should be a vertex").isNotNull();
      assertThat((Object) result.getProperty("b")).as("b should be a vertex").isNotNull();
      assertThat((Object) result.getProperty("r")).as("r should be an edge").isNotNull();

//    System.out.println("Result: " + result.toJSON());

      // No more results
      assertThat(rs.hasNext()).as("Should only return one result").isFalse();
    }

    @Test
    void mergeRelationshipIdempotency() {
      // First execution - creates the relationship
      database.command("opencypher",
          """
          MATCH (a), (b) WHERE ID(a) = $source_id AND ID(b) = $target_id \
          MERGE (a)-[r:in]->(b) \
          RETURN a, b, r""",
          Map.of("source_id", sourceRid.toString(), "target_id", targetRid.toString()));

      // Second execution - should find the existing relationship
      final ResultSet rs = database.command("opencypher",
          """
          MATCH (a), (b) WHERE ID(a) = $source_id AND ID(b) = $target_id \
          MERGE (a)-[r:in]->(b) \
          RETURN a, b, r""",
          Map.of("source_id", sourceRid.toString(), "target_id", targetRid.toString()));

      assertThat(rs.hasNext()).as("Should return one result").isTrue();

      final Result result = rs.next();
      assertThat(result.getPropertyNames()).containsExactlyInAnyOrder("a", "b", "r");

      // Verify only one relationship exists
      final ResultSet countRs = database.query("opencypher",
          "MATCH (a)-[r:in]->(b) WHERE ID(a) = $source_id AND ID(b) = $target_id RETURN count(r) AS count",
          Map.of("source_id", sourceRid.toString(), "target_id", targetRid.toString()));

      final long count = ((Number) countRs.next().getProperty("count")).longValue();
      assertThat(count).as("Should have exactly one relationship").isEqualTo(1);
    }
  }
}
