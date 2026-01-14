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
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for automatic transaction handling in OpenCypher write operations.
 * Verifies that CREATE, SET, DELETE, and MERGE operations:
 * 1. Automatically create transactions when none exist
 * 2. Reuse existing transactions when already active
 * 3. Properly commit/rollback based on transaction ownership
 */
public class OpenCypherTransactionTest {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/testopencypher-transaction").create();

    // Create schema
    database.getSchema().createVertexType("Person");
    database.getSchema().createVertexType("Company");
    database.getSchema().createEdgeType("WORKS_AT");
    database.getSchema().createEdgeType("KNOWS");
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void testCreateWithoutExplicitTransaction() {
    // CREATE should automatically handle transaction
    final ResultSet result = database.command("opencypher",
        "CREATE (n:Person {name: 'Alice', age: 30}) RETURN n");

    assertThat(result.hasNext()).isTrue();
    final Vertex person = (Vertex) result.next().getProperty("n");
    assertThat(person.get("name")).isEqualTo("Alice");
    assertThat(((Number) person.get("age")).intValue()).isEqualTo(30);

    // Verify persistence (transaction was committed)
    final ResultSet verify = database.query("opencypher", "MATCH (n:Person {name: 'Alice'}) RETURN n");
    assertThat(verify.hasNext()).isTrue();
  }

  @Test
  void testSetWithoutExplicitTransaction() {
    // Create a person first
    database.command("opencypher", "CREATE (n:Person {name: 'Bob', age: 25})");

    // SET should automatically handle transaction
    final ResultSet result = database.command("opencypher",
        "MATCH (n:Person {name: 'Bob'}) SET n.age = 26 RETURN n");

    assertThat(result.hasNext()).isTrue();
    final Vertex person = (Vertex) result.next().getProperty("n");
    assertThat(((Number) person.get("age")).intValue()).isEqualTo(26);

    // Verify persistence
    final ResultSet verify = database.query("opencypher", "MATCH (n:Person {name: 'Bob'}) RETURN n");
    assertThat(verify.hasNext()).isTrue();
    assertThat(((Number) ((Vertex) verify.next().getProperty("n")).get("age")).intValue()).isEqualTo(26);
  }

  @Test
  void testDeleteWithoutExplicitTransaction() {
    // Create a person first
    database.command("opencypher", "CREATE (n:Person {name: 'Charlie', age: 35})");

    // DELETE should automatically handle transaction
    database.command("opencypher", "MATCH (n:Person {name: 'Charlie'}) DELETE n");

    // Verify deletion
    final ResultSet verify = database.query("opencypher", "MATCH (n:Person {name: 'Charlie'}) RETURN n");
    assertThat(verify.hasNext()).isFalse();
  }

  @Test
  void testMergeWithoutExplicitTransaction() {
    // MERGE should automatically handle transaction
    final ResultSet result = database.command("opencypher",
        "MERGE (n:Person {name: 'David', age: 40}) RETURN n");

    assertThat(result.hasNext()).isTrue();
    final Vertex person = (Vertex) result.next().getProperty("n");
    assertThat(person.get("name")).isEqualTo("David");

    // Verify persistence
    final ResultSet verify = database.query("opencypher", "MATCH (n:Person {name: 'David'}) RETURN n");
    assertThat(verify.hasNext()).isTrue();
  }

  @Test
  void testCreateWithinExplicitTransaction() {
    // Operations within explicit transaction should use that transaction
    database.transaction(() -> {
      database.command("opencypher", "CREATE (n:Person {name: 'Eve', age: 28})");
      database.command("opencypher", "CREATE (n:Person {name: 'Frank', age: 32})");

      // Both should be visible within the same transaction
      final ResultSet result = database.query("opencypher", "MATCH (n:Person) WHERE n.name IN ['Eve', 'Frank'] RETURN count(n) as cnt");
      assertThat(result.next().<Long>getProperty("cnt")).isEqualTo(2L);
    });

    // Verify persistence after transaction commit
    final ResultSet verify = database.query("opencypher", "MATCH (n:Person) WHERE n.name IN ['Eve', 'Frank'] RETURN count(n) as cnt");
    assertThat(verify.next().<Long>getProperty("cnt")).isEqualTo(2L);
  }

  @Test
  void testMultipleOperationsInSingleTransaction() {
    // Multiple operations should share the same transaction
    database.transaction(() -> {
      // CREATE
      database.command("opencypher", "CREATE (n:Person {name: 'Grace', age: 29})");

      // SET
      database.command("opencypher", "MATCH (n:Person {name: 'Grace'}) SET n.age = 30");

      // Verify within transaction
      final ResultSet result = database.query("opencypher", "MATCH (n:Person {name: 'Grace'}) RETURN n");
      assertThat(result.hasNext()).isTrue();
      assertThat(((Number) ((Vertex) result.next().getProperty("n")).get("age")).intValue()).isEqualTo(30);
    });

    // Verify persistence
    final ResultSet verify = database.query("opencypher", "MATCH (n:Person {name: 'Grace'}) RETURN n");
    assertThat(verify.hasNext()).isTrue();
    assertThat(((Number) ((Vertex) verify.next().getProperty("n")).get("age")).intValue()).isEqualTo(30);
  }

  @Test
  void testTransactionRollbackOnError() {
    // Create initial data
    database.command("opencypher", "CREATE (n:Person {name: 'Henry', age: 45})");

    // Try to perform operations in a transaction that will fail
    assertThatThrownBy(() -> {
      database.transaction(() -> {
        // This should work
        database.command("opencypher", "MATCH (n:Person {name: 'Henry'}) SET n.age = 46");

        // Simulate an error by trying to delete a non-existent type
        // This will cause the transaction to rollback
        throw new RuntimeException("Simulated error");
      });
    }).isInstanceOf(RuntimeException.class);

    // Verify rollback - age should still be 45
    final ResultSet verify = database.query("opencypher", "MATCH (n:Person {name: 'Henry'}) RETURN n");
    assertThat(verify.hasNext()).isTrue();
    assertThat(((Number) ((Vertex) verify.next().getProperty("n")).get("age")).intValue()).isEqualTo(45);
  }

  @Test
  void testDetachDeleteWithTransaction() {
    // Create vertices with relationships
    database.transaction(() -> {
      database.command("opencypher", "CREATE (a:Person {name: 'Isaac'})-[:KNOWS]->(b:Person {name: 'Julia'})");
    });

    // DETACH DELETE should automatically handle transaction
    database.command("opencypher", "MATCH (n:Person {name: 'Isaac'}) DETACH DELETE n");

    // Verify deletion
    final ResultSet verify = database.query("opencypher", "MATCH (n:Person {name: 'Isaac'}) RETURN n");
    assertThat(verify.hasNext()).isFalse();

    // Julia should still exist
    final ResultSet verifyJulia = database.query("opencypher", "MATCH (n:Person {name: 'Julia'}) RETURN n");
    assertThat(verifyJulia.hasNext()).isTrue();
  }

  @Test
  void testMergeReusesTransaction() {
    // MERGE within transaction should reuse it
    database.transaction(() -> {
      // First MERGE creates
      database.command("opencypher", "MERGE (n:Person {name: 'Kate', age: 33})");

      // Second MERGE finds existing
      database.command("opencypher", "MERGE (n:Person {name: 'Kate', age: 33})");

      // Should only have one Kate
      final ResultSet result = database.query("opencypher", "MATCH (n:Person {name: 'Kate'}) RETURN count(n) as cnt");
      assertThat(result.next().<Long>getProperty("cnt")).isEqualTo(1L);
    });
  }
}
