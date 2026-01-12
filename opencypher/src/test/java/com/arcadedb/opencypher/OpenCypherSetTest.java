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

/**
 * Tests for SET clause in OpenCypher queries.
 */
public class OpenCypherSetTest {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/testopencypher-set").create();

    // Create schema
    database.getSchema().createVertexType("Person");
    database.getSchema().createVertexType("Company");
    database.getSchema().createEdgeType("WORKS_AT");
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void testSetSingleProperty() {
    // Create a person
    database.transaction(() -> {
      database.command("opencypher", "CREATE (n:Person {name: 'Alice', age: 30})");
    });

    // Update age
    database.transaction(() -> {
      final ResultSet result = database.command("opencypher",
          "MATCH (n:Person {name: 'Alice'}) SET n.age = 31 RETURN n");

      assertThat(result.hasNext()).isTrue();
      final Vertex v = (Vertex) result.next().getProperty("n");
      assertThat((Integer) v.get("age")).isEqualTo(31);
    });

    // Verify persistence
    final ResultSet verify = database.query("opencypher", "MATCH (n:Person {name: 'Alice'}) RETURN n");
    assertThat(verify.hasNext()).isTrue();
    final Vertex v = (Vertex) verify.next().getProperty("n");
    assertThat((Integer) v.get("age")).isEqualTo(31);
  }

  @Test
  void testSetMultipleProperties() {
    // Create a person
    database.transaction(() -> {
      database.command("opencypher", "CREATE (n:Person {name: 'Bob', age: 25})");
    });

    // Update multiple properties
    database.transaction(() -> {
      final ResultSet result = database.command("opencypher",
          "MATCH (n:Person {name: 'Bob'}) SET n.age = 26, n.city = 'NYC' RETURN n");

      assertThat(result.hasNext()).isTrue();
      final Vertex v = (Vertex) result.next().getProperty("n");
      assertThat((Integer) v.get("age")).isEqualTo(26);
      assertThat((String) v.get("city")).isEqualTo("NYC");
    });

    // Verify persistence
    final ResultSet verify = database.query("opencypher", "MATCH (n:Person {name: 'Bob'}) RETURN n");
    assertThat(verify.hasNext()).isTrue();
    final Vertex v = (Vertex) verify.next().getProperty("n");
    assertThat((Integer) v.get("age")).isEqualTo(26);
    assertThat((String) v.get("city")).isEqualTo("NYC");
  }

  @Test
  void testSetWithWhere() {
    // Create multiple people
    database.transaction(() -> {
      database.command("opencypher", "CREATE (a:Person {name: 'Alice', age: 30})");
      database.command("opencypher", "CREATE (b:Person {name: 'Bob', age: 25})");
      database.command("opencypher", "CREATE (c:Person {name: 'Charlie', age: 35})");
    });

    // Update all people over 30
    database.transaction(() -> {
      final ResultSet result = database.command("opencypher",
          "MATCH (n:Person) WHERE n.age > 30 SET n.senior = true RETURN n");

      int count = 0;
      while (result.hasNext()) {
        final Vertex v = (Vertex) result.next().getProperty("n");
        assertThat((Boolean) v.get("senior")).isTrue();
        count++;
      }
      assertThat(count).isEqualTo(1); // Only Charlie
    });

    // Verify only Charlie has senior flag
    final ResultSet verify = database.query("opencypher", "MATCH (n:Person) RETURN n ORDER BY n.age");
    int seniorCount = 0;
    while (verify.hasNext()) {
      final Vertex v = (Vertex) verify.next().getProperty("n");
      if (v.get("senior") != null && (Boolean) v.get("senior")) {
        seniorCount++;
      }
    }
    assertThat(seniorCount).isEqualTo(1);
  }

  @Test
  void testSetStringValue() {
    // Create a person
    database.transaction(() -> {
      database.command("opencypher", "CREATE (n:Person {name: 'Alice'})");
    });

    // Set string property
    database.transaction(() -> {
      final ResultSet result = database.command("opencypher",
          "MATCH (n:Person {name: 'Alice'}) SET n.email = 'alice@example.com' RETURN n");

      assertThat(result.hasNext()).isTrue();
      final Vertex v = (Vertex) result.next().getProperty("n");
      assertThat((String) v.get("email")).isEqualTo("alice@example.com");
    });
  }

  @Test
  void testSetNumericValue() {
    // Create a person
    database.transaction(() -> {
      database.command("opencypher", "CREATE (n:Person {name: 'Bob'})");
    });

    // Set numeric properties
    database.transaction(() -> {
      final ResultSet result = database.command("opencypher",
          "MATCH (n:Person {name: 'Bob'}) SET n.age = 42, n.salary = 75000.50 RETURN n");

      assertThat(result.hasNext()).isTrue();
      final Vertex v = (Vertex) result.next().getProperty("n");
      assertThat((Integer) v.get("age")).isEqualTo(42);
      assertThat((Double) v.get("salary")).isEqualTo(75000.50);
    });
  }

  @Test
  void testSetBooleanValue() {
    // Create a person
    database.transaction(() -> {
      database.command("opencypher", "CREATE (n:Person {name: 'Charlie'})");
    });

    // Set boolean property
    database.transaction(() -> {
      final ResultSet result = database.command("opencypher",
          "MATCH (n:Person {name: 'Charlie'}) SET n.active = true RETURN n");

      assertThat(result.hasNext()).isTrue();
      final Vertex v = (Vertex) result.next().getProperty("n");
      assertThat((Boolean) v.get("active")).isTrue();
    });

    // Update to false
    database.transaction(() -> {
      final ResultSet result = database.command("opencypher",
          "MATCH (n:Person {name: 'Charlie'}) SET n.active = false RETURN n");

      assertThat(result.hasNext()).isTrue();
      final Vertex v = (Vertex) result.next().getProperty("n");
      assertThat((Boolean) v.get("active")).isFalse();
    });
  }

  @Test
  void testSetNullValue() {
    // Create a person with age
    database.transaction(() -> {
      database.command("opencypher", "CREATE (n:Person {name: 'David', age: 30})");
    });

    // Set age to null (remove property)
    database.transaction(() -> {
      final ResultSet result = database.command("opencypher",
          "MATCH (n:Person {name: 'David'}) SET n.age = null RETURN n");

      assertThat(result.hasNext()).isTrue();
      final Vertex v = (Vertex) result.next().getProperty("n");
      assertThat(v.get("age")).isNull();
    });

    // Verify property is null
    final ResultSet verify = database.query("opencypher", "MATCH (n:Person {name: 'David'}) RETURN n");
    assertThat(verify.hasNext()).isTrue();
    final Vertex v = (Vertex) verify.next().getProperty("n");
    assertThat(v.get("age")).isNull();
  }

  @Test
  void testSetOnMultipleNodes() {
    // Create multiple people
    database.transaction(() -> {
      database.command("opencypher", "CREATE (a:Person {name: 'Alice', age: 30})");
      database.command("opencypher", "CREATE (b:Person {name: 'Bob', age: 25})");
      database.command("opencypher", "CREATE (c:Person {name: 'Charlie', age: 35})");
    });

    // Update all people
    database.transaction(() -> {
      final ResultSet result = database.command("opencypher",
          "MATCH (n:Person) SET n.verified = true RETURN n");

      int count = 0;
      while (result.hasNext()) {
        final Vertex v = (Vertex) result.next().getProperty("n");
        assertThat((Boolean) v.get("verified")).isTrue();
        count++;
      }
      assertThat(count).isEqualTo(3);
    });

    // Verify all have verified flag
    final ResultSet verify = database.query("opencypher", "MATCH (n:Person) RETURN n");
    int count = 0;
    while (verify.hasNext()) {
      final Vertex v = (Vertex) verify.next().getProperty("n");
      assertThat((Boolean) v.get("verified")).isTrue();
      count++;
    }
    assertThat(count).isEqualTo(3);
  }

  @Test
  void testSetWithoutReturn() {
    // Create a person
    database.transaction(() -> {
      database.command("opencypher", "CREATE (n:Person {name: 'Eve', age: 28})");
    });

    // Update without RETURN
    database.transaction(() -> {
      database.command("opencypher", "MATCH (n:Person {name: 'Eve'}) SET n.age = 29");
    });

    // Verify update persisted
    final ResultSet verify = database.query("opencypher", "MATCH (n:Person {name: 'Eve'}) RETURN n");
    assertThat(verify.hasNext()).isTrue();
    final Vertex v = (Vertex) verify.next().getProperty("n");
    assertThat((Integer) v.get("age")).isEqualTo(29);
  }

  @Test
  void testSetOnRelationship() {
    // Create two people and a relationship
    database.transaction(() -> {
      database.command("opencypher",
          "CREATE (a:Person {name: 'Alice'})-[r:WORKS_AT {since: 2020}]->(c:Company {name: 'ArcadeDB'})");
    });

    // Update relationship property
    database.transaction(() -> {
      final ResultSet result = database.command("opencypher",
          "MATCH (a:Person)-[r:WORKS_AT]->(c:Company) SET r.since = 2021, r.role = 'Engineer' RETURN r");

      assertThat(result.hasNext()).isTrue();
      final Result res = result.next();
      final com.arcadedb.graph.Edge edge = (com.arcadedb.graph.Edge) res.getProperty("r");
      assertThat((Integer) edge.get("since")).isEqualTo(2021);
      assertThat((String) edge.get("role")).isEqualTo("Engineer");
    });

    // Verify persistence
    final ResultSet verify = database.query("opencypher",
        "MATCH (a:Person)-[r:WORKS_AT]->(c:Company) RETURN r");
    assertThat(verify.hasNext()).isTrue();
    final com.arcadedb.graph.Edge edge = (com.arcadedb.graph.Edge) verify.next().getProperty("r");
    assertThat((Integer) edge.get("since")).isEqualTo(2021);
    assertThat((String) edge.get("role")).isEqualTo("Engineer");
  }

  @Test
  void testSetAfterCreate() {
    // CREATE and SET in same query
    database.transaction(() -> {
      final ResultSet result = database.command("opencypher",
          "CREATE (n:Person {name: 'Frank'}) SET n.age = 40 RETURN n");

      assertThat(result.hasNext()).isTrue();
      final Vertex v = (Vertex) result.next().getProperty("n");
      assertThat((String) v.get("name")).isEqualTo("Frank");
      assertThat((Integer) v.get("age")).isEqualTo(40);
    });

    // Verify both properties persisted
    final ResultSet verify = database.query("opencypher", "MATCH (n:Person {name: 'Frank'}) RETURN n");
    assertThat(verify.hasNext()).isTrue();
    final Vertex v = (Vertex) verify.next().getProperty("n");
    assertThat((Integer) v.get("age")).isEqualTo(40);
  }
}
