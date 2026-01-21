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
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for MERGE with ON CREATE SET and ON MATCH SET sub-clauses.
 */
public class OpenCypherMergeActionsTest {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/testopencypher-merge-actions").create();

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
  void testMergeOnCreateSet() {
    // MERGE with ON CREATE SET should set properties when creating new node
    final ResultSet result = database.command("opencypher",
        "MERGE (n:Person {name: 'Alice'}) ON CREATE SET n.created = true RETURN n");

    assertThat(result.hasNext()).isTrue();
    final Vertex person = (Vertex) result.next().getProperty("n");
    assertThat(person.get("name")).isEqualTo("Alice");
    assertThat((Boolean) person.get("created")).isTrue();

    // Verify persistence
    final ResultSet verify = database.query("opencypher", "MATCH (n:Person {name: 'Alice'}) RETURN n");
    assertThat(verify.hasNext()).isTrue();
    final Vertex verifyPerson = (Vertex) verify.next().getProperty("n");
    assertThat((Boolean) verifyPerson.get("created")).isTrue();
  }

  @Test
  void testMergeOnMatchSet() {
    // Create initial person
    database.command("opencypher", "CREATE (n:Person {name: 'Bob', visits: 1})");

    // MERGE with ON MATCH SET should set properties when matching existing node
    final ResultSet result = database.command("opencypher",
        "MERGE (n:Person {name: 'Bob'}) ON MATCH SET n.visits = 2 RETURN n");

    assertThat(result.hasNext()).isTrue();
    final Vertex person = (Vertex) result.next().getProperty("n");
    assertThat(person.get("name")).isEqualTo("Bob");
    assertThat(((Number) person.get("visits")).intValue()).isEqualTo(2);
  }

  @Test
  void testMergeOnCreateAndOnMatchSet() {
    // First MERGE creates, should set created property
    ResultSet result = database.command("opencypher",
        "MERGE (n:Person {name: 'Charlie'}) " +
            "ON CREATE SET n.created = true, n.count = 1 " +
            "ON MATCH SET n.count = 2 " +
            "RETURN n");

    assertThat(result.hasNext()).isTrue();
    Vertex person = (Vertex) result.next().getProperty("n");
    assertThat(person.get("name")).isEqualTo("Charlie");
    assertThat((Boolean) person.get("created")).isTrue();
    assertThat(((Number) person.get("count")).intValue()).isEqualTo(1);

    // Second MERGE matches, should set count property
    result = database.command("opencypher",
        "MERGE (n:Person {name: 'Charlie'}) " +
            "ON CREATE SET n.created = true, n.count = 1 " +
            "ON MATCH SET n.count = 2 " +
            "RETURN n");

    assertThat(result.hasNext()).isTrue();
    person = (Vertex) result.next().getProperty("n");
    assertThat(person.get("name")).isEqualTo("Charlie");
    assertThat((Boolean) person.get("created")).isTrue();
    assertThat(((Number) person.get("count")).intValue()).isEqualTo(2);
  }

  @Test
  void testMergeOnCreateSetMultipleProperties() {
    // MERGE with ON CREATE SET can set multiple properties
    final ResultSet result = database.command("opencypher",
        "MERGE (n:Person {name: 'David'}) " +
            "ON CREATE SET n.age = 30, n.city = 'NYC', n.active = true " +
            "RETURN n");

    assertThat(result.hasNext()).isTrue();
    final Vertex person = (Vertex) result.next().getProperty("n");
    assertThat(person.get("name")).isEqualTo("David");
    assertThat(((Number) person.get("age")).intValue()).isEqualTo(30);
    assertThat(person.get("city")).isEqualTo("NYC");
    assertThat((Boolean) person.get("active")).isTrue();
  }

  @Test
  void testMergeOnMatchSetUpdatesExisting() {
    // Create person with initial properties
    database.command("opencypher", "CREATE (n:Person {name: 'Eve', age: 25, visits: 0})");

    // MERGE with ON MATCH SET updates properties
    final ResultSet result = database.command("opencypher",
        "MERGE (n:Person {name: 'Eve'}) " +
            "ON MATCH SET n.age = 26, n.visits = 5 " +
            "RETURN n");

    assertThat(result.hasNext()).isTrue();
    final Vertex person = (Vertex) result.next().getProperty("n");
    assertThat(person.get("name")).isEqualTo("Eve");
    assertThat(((Number) person.get("age")).intValue()).isEqualTo(26);
    assertThat(((Number) person.get("visits")).intValue()).isEqualTo(5);
  }

  @Test
  void testMergeOnCreateSetWithLiterals() {
    // Test different literal types in ON CREATE SET
    final ResultSet result = database.command("opencypher",
        "MERGE (n:Person {name: 'Frank'}) " +
            "ON CREATE SET n.stringProp = 'hello', n.intProp = 42, n.floatProp = 3.14, n.boolProp = true, n.nullProp = null " +
            "RETURN n");

    assertThat(result.hasNext()).isTrue();
    final Vertex person = (Vertex) result.next().getProperty("n");
    assertThat(person.get("name")).isEqualTo("Frank");
    assertThat(person.get("stringProp")).isEqualTo("hello");
    assertThat(((Number) person.get("intProp")).intValue()).isEqualTo(42);
    assertThat(((Number) person.get("floatProp")).doubleValue()).isCloseTo(3.14, Offset.offset(0.01));
    assertThat((Boolean) person.get("boolProp")).isTrue();
    assertThat(person.get("nullProp")).isNull();
  }

  @Test
  void testMergeOnCreateSetWithPropertyReference() {
    // Create initial data
    database.command("opencypher", "CREATE (n:Person {name: 'Grace', age: 30})");

    // MERGE with ON CREATE SET can reference properties from matched pattern
    final ResultSet result = database.command("opencypher",
        "MATCH (existing:Person {name: 'Grace'}) " +
            "MERGE (n:Person {name: 'Henry'}) " +
            "ON CREATE SET n.age = existing.age " +
            "RETURN n, existing");

    assertThat(result.hasNext()).isTrue();
    final Result row = result.next();
    final Vertex newPerson = (Vertex) row.getProperty("n");
    final Vertex existingPerson = (Vertex) row.getProperty("existing");

    assertThat(newPerson.get("name")).isEqualTo("Henry");
    assertThat(((Number) newPerson.get("age")).intValue()).isEqualTo(30);
    assertThat(((Number) existingPerson.get("age")).intValue()).isEqualTo(30);
  }

  @Test
  void testMergeRelationshipOnCreateSet() {
    // Create vertices first
    database.command("opencypher", "CREATE (a:Person {name: 'Isaac'}), (b:Company {name: 'ArcadeDB'})");

    // MERGE relationship with ON CREATE SET
    final ResultSet result = database.command("opencypher",
        "MATCH (a:Person {name: 'Isaac'}), (b:Company {name: 'ArcadeDB'}) " +
            "MERGE (a)-[r:WORKS_AT]->(b) " +
            "ON CREATE SET r.since = 2020, r.role = 'Engineer' " +
            "RETURN r");

    assertThat(result.hasNext()).isTrue();
    final Edge edge = (Edge) result.next().getProperty("r");
    assertThat(((Number) edge.get("since")).intValue()).isEqualTo(2020);
    assertThat(edge.get("role")).isEqualTo("Engineer");
  }

  @Test
  void testMergeRelationshipOnMatchSet() {
    // Create vertices and relationship
    database.command("opencypher",
        "CREATE (a:Person {name: 'Julia'})-[r:WORKS_AT {since: 2020}]->(b:Company {name: 'ArcadeDB'})");

    // MERGE with ON MATCH SET on relationship
    final ResultSet result = database.command("opencypher",
        "MATCH (a:Person {name: 'Julia'}), (b:Company {name: 'ArcadeDB'}) " +
            "MERGE (a)-[r:WORKS_AT]->(b) " +
            "ON MATCH SET r.since = 2021, r.promoted = true " +
            "RETURN r");

    assertThat(result.hasNext()).isTrue();
    final Edge edge = (Edge) result.next().getProperty("r");
    assertThat(((Number) edge.get("since")).intValue()).isEqualTo(2021);
    assertThat((Boolean) edge.get("promoted")).isTrue();
  }
}
