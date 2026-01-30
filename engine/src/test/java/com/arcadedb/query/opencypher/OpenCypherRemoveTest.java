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
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for REMOVE clause in OpenCypher queries.
 * Issue #3257: REMOVE clause was not implemented in the new OpenCypher engine.
 */
public class OpenCypherRemoveTest {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/testopencypher-remove").create();
    database.getSchema().createVertexType("Person");
    database.getSchema().createEdgeType("KNOWS");
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  /**
   * Issue #3257: Creating and removing property on same query doesn't remove it on new engine.
   * This test reproduces the exact scenario from the issue.
   */
  @Test
  void testMergeWithOnCreateSetAndRemove() {
    // This is the exact query from issue #3257
    database.transaction(() -> {
      final ResultSet result = database.command("opencypher",
          "MERGE (n:Person {name: 'Charlie Sheen'}) " +
              "ON CREATE SET n._temp_created = true " +
              "ON MATCH SET n._temp_created = false " +
              "WITH n, n._temp_created AS created " +
              "REMOVE n._temp_created " +
              "RETURN ID(n) AS id, created");

      assertThat(result.hasNext()).isTrue();
      final Result row = result.next();

      // The created flag should be captured before removal
      final Boolean created = row.getProperty("created");
      assertThat(created).isEqualTo(true);

      // The _temp_created property should be removed
      final Object id = row.getProperty("id");
      assertThat(id).isNotNull();
    });

    // Verify the property is actually removed from the persisted node
    final ResultSet verify = database.query("opencypher",
        "MATCH (n:Person {name: 'Charlie Sheen'}) RETURN n");
    assertThat(verify.hasNext()).isTrue();
    final Vertex person = (Vertex) verify.next().toElement();
    assertThat(person.get("name")).isEqualTo("Charlie Sheen");
    // The _temp_created property should NOT exist
    assertThat(person.has("_temp_created")).isFalse();
  }

  @Test
  void testRemoveSingleProperty() {
    // Create a person with a property
    database.transaction(() -> {
      database.command("opencypher", "CREATE (n:Person {name: 'Alice', age: 30, temp: 'value'})");
    });

    // Remove the temp property
    database.transaction(() -> {
      final ResultSet result = database.command("opencypher",
          "MATCH (n:Person {name: 'Alice'}) REMOVE n.temp RETURN n");

      assertThat(result.hasNext()).isTrue();
      final Vertex person = (Vertex) result.next().toElement();
      assertThat(person.get("name")).isEqualTo("Alice");
      assertThat(person.get("age")).isEqualTo(30);
      assertThat(person.has("temp")).isFalse();
    });

    // Verify persistence
    final ResultSet verify = database.query("opencypher", "MATCH (n:Person {name: 'Alice'}) RETURN n");
    assertThat(verify.hasNext()).isTrue();
    final Vertex person = (Vertex) verify.next().toElement();
    assertThat(person.has("temp")).isFalse();
    assertThat(person.get("name")).isEqualTo("Alice");
  }

  @Test
  void testRemoveMultipleProperties() {
    // Create a person with multiple properties
    database.transaction(() -> {
      database.command("opencypher",
          "CREATE (n:Person {name: 'Bob', age: 25, temp1: 'a', temp2: 'b'})");
    });

    // Remove multiple properties
    database.transaction(() -> {
      final ResultSet result = database.command("opencypher",
          "MATCH (n:Person {name: 'Bob'}) REMOVE n.temp1, n.temp2 RETURN n");

      assertThat(result.hasNext()).isTrue();
      final Vertex person = (Vertex) result.next().toElement();
      assertThat(person.get("name")).isEqualTo("Bob");
      assertThat(person.get("age")).isEqualTo(25);
      assertThat(person.has("temp1")).isFalse();
      assertThat(person.has("temp2")).isFalse();
    });
  }

  @Test
  void testRemoveNonExistentProperty() {
    // Create a person without the property
    database.transaction(() -> {
      database.command("opencypher", "CREATE (n:Person {name: 'Charlie'})");
    });

    // Try to remove a non-existent property - should not fail
    database.transaction(() -> {
      final ResultSet result = database.command("opencypher",
          "MATCH (n:Person {name: 'Charlie'}) REMOVE n.nonexistent RETURN n");

      assertThat(result.hasNext()).isTrue();
      final Vertex person = (Vertex) result.next().toElement();
      assertThat(person.get("name")).isEqualTo("Charlie");
    });
  }

  @Test
  void testSetThenRemove() {
    // Create a person
    database.transaction(() -> {
      database.command("opencypher", "CREATE (n:Person {name: 'David'})");
    });

    // SET then REMOVE in the same query
    database.transaction(() -> {
      final ResultSet result = database.command("opencypher",
          "MATCH (n:Person {name: 'David'}) " +
              "SET n.temp = 'temporary' " +
              "WITH n " +
              "REMOVE n.temp " +
              "RETURN n");

      assertThat(result.hasNext()).isTrue();
      final Vertex person = (Vertex) result.next().toElement();
      assertThat(person.get("name")).isEqualTo("David");
      // Property should be removed even though it was set earlier in the same query
      assertThat(person.has("temp")).isFalse();
    });
  }

  @Test
  void testRemoveWithoutReturn() {
    // Create a person
    database.transaction(() -> {
      database.command("opencypher", "CREATE (n:Person {name: 'Eve', temp: 'remove_me'})");
    });

    // REMOVE without RETURN
    database.transaction(() -> {
      database.command("opencypher",
          "MATCH (n:Person {name: 'Eve'}) REMOVE n.temp");
    });

    // Verify property was removed
    final ResultSet verify = database.query("opencypher", "MATCH (n:Person {name: 'Eve'}) RETURN n");
    assertThat(verify.hasNext()).isTrue();
    final Vertex person = (Vertex) verify.next().toElement();
    assertThat(person.has("temp")).isFalse();
  }

  @Test
  void testRemoveOnMultipleNodes() {
    // Create multiple people with temp property
    database.transaction(() -> {
      database.command("opencypher", "CREATE (a:Person {name: 'A', temp: 1})");
      database.command("opencypher", "CREATE (b:Person {name: 'B', temp: 2})");
      database.command("opencypher", "CREATE (c:Person {name: 'C', temp: 3})");
    });

    // Remove temp from all
    database.transaction(() -> {
      final ResultSet result = database.command("opencypher",
          "MATCH (n:Person) REMOVE n.temp RETURN n");

      int count = 0;
      while (result.hasNext()) {
        final Vertex person = (Vertex) result.next().toElement();
        assertThat(person.has("temp")).isFalse();
        count++;
      }
      assertThat(count).isEqualTo(3);
    });

    // Verify all have temp removed
    final ResultSet verify = database.query("opencypher", "MATCH (n:Person) RETURN n");
    while (verify.hasNext()) {
      final Vertex person = (Vertex) verify.next().toElement();
      assertThat(person.has("temp")).isFalse();
    }
  }

  @Test
  void testRemovePropertyOnRelationship() {
    // Create relationship with property
    database.transaction(() -> {
      database.command("opencypher",
          "CREATE (a:Person {name: 'Alice'})-[r:KNOWS {since: 2020, temp: 'remove_me'}]->(b:Person {name: 'Bob'})");
    });

    // Remove property from relationship
    database.transaction(() -> {
      final ResultSet result = database.command("opencypher",
          "MATCH (a:Person)-[r:KNOWS]->(b:Person) REMOVE r.temp RETURN r");

      assertThat(result.hasNext()).isTrue();
      final Edge edge = (Edge) result.next().toElement();
      assertThat(((Number) edge.get("since")).intValue()).isEqualTo(2020);
      assertThat(edge.has("temp")).isFalse();
    });
  }

  @Test
  void testCreateWithRemove() {
    // CREATE and REMOVE in the same query
    database.transaction(() -> {
      final ResultSet result = database.command("opencypher",
          "CREATE (n:Person {name: 'Frank', temp: 'initial'}) " +
              "WITH n " +
              "REMOVE n.temp " +
              "RETURN n");

      assertThat(result.hasNext()).isTrue();
      final Vertex person = (Vertex) result.next().toElement();
      assertThat(person.get("name")).isEqualTo("Frank");
      assertThat(person.has("temp")).isFalse();
    });
  }
}
