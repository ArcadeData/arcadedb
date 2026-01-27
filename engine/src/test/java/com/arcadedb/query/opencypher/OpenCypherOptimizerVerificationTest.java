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
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

;

/**
 * Cypher Verification test for Cost-Based Query Optimizer.
 * Ensures that optimizations are correctly applied by checking EXPLAIN output.
 */
public class OpenCypherOptimizerVerificationTest {
  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./databases/test-optimizer-verification").create();

    // Create schema with properties
    database.transaction(() -> {
      final var personType = database.getSchema().createVertexType("Person");
      personType.createProperty("id", Integer.class);
      personType.createProperty("name", String.class);
      personType.createProperty("age", Integer.class);

      final var companyType = database.getSchema().createVertexType("Company");
      companyType.createProperty("id", Integer.class);
      companyType.createProperty("name", String.class);

      database.getSchema().createEdgeType("KNOWS");
      database.getSchema().createEdgeType("WORKS_AT");
    });

    // Populate test data
    database.transaction(() -> {
      // Create companies
      for (int i = 0; i < 10; i++) {
        database.command("opencypher",
          "CREATE (c:Company {name: 'Company" + i + "', id: " + i + "})");
      }

      // Create persons
      for (int i = 0; i < 100; i++) {
        database.command("opencypher",
          "CREATE (p:Person {name: 'Person" + i + "', id: " + i + ", age: " + (20 + (i % 50)) + "})");
      }

      // Create KNOWS relationships
      for (int i = 0; i < 50; i++) {
        final int targetId = (i + 5) % 100;
        database.command("opencypher",
          "MATCH (a:Person {id: " + i + "}), (b:Person {id: " + targetId + "}) " +
          "CREATE (a)-[:KNOWS]->(b)");
      }

      // Create WORKS_AT relationships
      for (int i = 0; i < 100; i++) {
        final int companyId = i % 10;
        database.command("opencypher",
          "MATCH (p:Person {id: " + i + "}), (c:Company {id: " + companyId + "}) " +
          "CREATE (p)-[:WORKS_AT]->(c)");
      }
    });

    // Create indexes AFTER data exists
    database.transaction(() -> {
      database.getSchema().getOrCreateVertexType("Person")
        .createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "id");
      database.getSchema().getOrCreateVertexType("Company")
        .createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "name");
    });
  }

  @AfterEach
  void teardown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void testIndexSeekOptimization() {
    // Execute query
    final String query = "MATCH (p:Person) WHERE p.id = 50 RETURN p";
    final ResultSet results = database.query("opencypher", query);

    int count = 0;
    while (results.hasNext()) {
      results.next();
      count++;
    }
    results.close();

    assertThat(count).isEqualTo(1);

    // Verify optimization with EXPLAIN
    final ResultSet explainResult = database.query("opencypher", "EXPLAIN " + query);
    final String plan = (String) explainResult.next().getProperty("plan");
    explainResult.close();

    // Assertions
    assertThat(plan).contains("Using Cost-Based Query Optimizer");
    assertThat(plan).contains("NodeIndexSeek(p:Person)");
    assertThat(plan).contains("index=Person[id]");
    assertThat(plan).contains("id=50");
    assertThat(plan).doesNotContain("NodeByLabelScan");
  }

  @Test
  void testFullScanWhenNoIndex() {
    // Execute query (no index on age)
    final String query = "MATCH (p:Person) WHERE p.age > 30 RETURN p";
    final ResultSet results = database.query("opencypher", query);

    int count = 0;
    while (results.hasNext()) {
      results.next();
      count++;
    }
    results.close();

    assertThat(count).isGreaterThan(0);

    // Verify uses scan (not index)
    final ResultSet explainResult = database.query("opencypher", "EXPLAIN " + query);
    final String plan = (String) explainResult.next().getProperty("plan");
    explainResult.close();

    assertThat(plan).contains("Using Cost-Based Query Optimizer");
    assertThat(plan).contains("NodeByLabelScan(p:Person)");
    assertThat(plan).doesNotContain("NodeIndexSeek");
  }

  @Test
  void testRelationshipTraversalWithIndexSeek() {
    // Execute query
    final String query = "MATCH (a:Person)-[r:KNOWS]->(b:Person) WHERE a.id = 10 RETURN b";
    final ResultSet results = database.query("opencypher", query);

    int count = 0;
    while (results.hasNext()) {
      results.next();
      count++;
    }
    results.close();

    assertThat(count).isEqualTo(1);

    // Verify optimization
    final ResultSet explainResult = database.query("opencypher", "EXPLAIN " + query);
    final String plan = (String) explainResult.next().getProperty("plan");
    explainResult.close();

    assertThat(plan).contains("Using Cost-Based Query Optimizer");
    assertThat(plan).contains("NodeIndexSeek(a:Person)");
    assertThat(plan).contains("index=Person[id]");
    assertThat(plan).contains("id=10");
    assertThat(plan).contains("ExpandAll(a)-[r:KNOWS]->(b)");
  }

  @Test
  void testMultiHopPattern() {
    // Execute query
    final String query = "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) WHERE a.id = 5 RETURN c";
    final ResultSet results = database.query("opencypher", query);

    int count = 0;
    while (results.hasNext()) {
      results.next();
      count++;
    }
    results.close();

    // Verify optimization
    final ResultSet explainResult = database.query("opencypher", "EXPLAIN " + query);
    final String plan = (String) explainResult.next().getProperty("plan");
    explainResult.close();

    assertThat(plan).contains("Using Cost-Based Query Optimizer");
    assertThat(plan).contains("NodeIndexSeek(a:Person)");
    assertThat(plan).contains("index=Person[id]");
    assertThat(plan).contains("id=5");
    // Should have 2 ExpandAll operators (2 hops)
    assertThat(plan).contains("ExpandAll");
  }

  @Test
  void testCrossTypeRelationship() {
    // Execute query
    final String query = "MATCH (p:Person)-[r:WORKS_AT]->(c:Company) WHERE p.id = 25 RETURN c";
    final ResultSet results = database.query("opencypher", query);

    int count = 0;
    while (results.hasNext()) {
      results.next();
      count++;
    }
    results.close();

    assertThat(count).isEqualTo(1);

    // Verify optimization
    final ResultSet explainResult = database.query("opencypher", "EXPLAIN " + query);
    final String plan = (String) explainResult.next().getProperty("plan");
    explainResult.close();

    assertThat(plan).contains("Using Cost-Based Query Optimizer");
    assertThat(plan).contains("NodeIndexSeek(p:Person)");
    assertThat(plan).contains("index=Person[id]");
    assertThat(plan).contains("id=25");
    assertThat(plan).contains("ExpandAll(p)-[r:WORKS_AT]->(c)");
  }

  @Test
  void testJoinOrderingOptimization() {
    // Execute query - should start from Company (fewer records)
    final String query = "MATCH (p:Person)-[:WORKS_AT]->(c:Company) WHERE c.name = 'Company5' RETURN p";
    final ResultSet results = database.query("opencypher", query);

    int count = 0;
    while (results.hasNext()) {
      results.next();
      count++;
    }
    results.close();

    assertThat(count).isGreaterThan(0);

    // Verify optimization - should start from Company, not Person
    final ResultSet explainResult = database.query("opencypher", "EXPLAIN " + query);
    final String plan = (String) explainResult.next().getProperty("plan");
    explainResult.close();

    assertThat(plan).contains("Using Cost-Based Query Optimizer");
    assertThat(plan).contains("NodeIndexSeek(c:Company)");
    assertThat(plan).contains("index=Company[name]");
    assertThat(plan).contains("name=Company5");
    // Should use reverse traversal from Company to Person
    assertThat(plan).contains("ExpandAll(c)-[:WORKS_AT]-<(p)");
  }

  @Test
  void testParameterizedQueryUsesIndex() {
    // Verify optimization detects indexed property with parameter
    final String query = "MATCH (p:Person) WHERE p.id = $personId RETURN p";

    // Check EXPLAIN output (parameters don't work yet in execution, but optimizer should detect index)
    final ResultSet explainResult = database.query("opencypher", "EXPLAIN " + query);
    final String plan = (String) explainResult.next().getProperty("plan");
    explainResult.close();

    assertThat(plan).contains("Using Cost-Based Query Optimizer");
    assertThat(plan).contains("NodeIndexSeek(p:Person)");
    assertThat(plan).contains("index=Person[id]");
    // Note: Parameter value shows as null in EXPLAIN, but index selection still works
  }

  @Test
  void testComplexWhereClauseWithIndex() {
    // Note: Complex WHERE clauses with AND/OR are not yet optimized
    // The optimizer currently only extracts equality predicates from simple comparisons
    // TODO: Future enhancement - extract predicates from LogicalExpression (AND/OR)
    final String query = "MATCH (p:Person) WHERE p.id = 30 AND p.age > 25 RETURN p";
    final ResultSet results = database.query("opencypher", query);

    int count = 0;
    while (results.hasNext()) {
      results.next();
      count++;
    }
    results.close();

    assertThat(count).isEqualTo(1);

    // Verify current behavior: uses NodeByLabelScan with Filter for AND expressions
    final ResultSet explainResult = database.query("opencypher", "EXPLAIN " + query);
    final String plan = (String) explainResult.next().getProperty("plan");
    explainResult.close();

    assertThat(plan).contains("Using Cost-Based Query Optimizer");
    // Current limitation: AND expressions not yet decomposed for index selection
    assertThat(plan).contains("NodeByLabelScan(p:Person)");
    assertThat(plan).contains("Filter");
  }

  @Test
  void testSimpleEqualityUsesIndex() {
    // Test that simple equality (without AND/OR) correctly uses index
    final String query = "MATCH (p:Person) WHERE p.id = 30 RETURN p";
    final ResultSet results = database.query("opencypher", query);

    int count = 0;
    while (results.hasNext()) {
      results.next();
      count++;
    }
    results.close();

    assertThat(count).isEqualTo(1);

    // Verify uses index for simple equality predicate
    final ResultSet explainResult = database.query("opencypher", "EXPLAIN " + query);
    final String plan = (String) explainResult.next().getProperty("plan");
    explainResult.close();

    assertThat(plan).contains("Using Cost-Based Query Optimizer");
    assertThat(plan).contains("NodeIndexSeek(p:Person)");
    assertThat(plan).contains("index=Person[id]");
    assertThat(plan).contains("id=30");
  }
}
