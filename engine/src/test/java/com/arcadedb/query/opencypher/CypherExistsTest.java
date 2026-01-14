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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test EXISTS() expression in Cypher queries.
 */
public class CypherExistsTest {
  private Database database;

  @BeforeEach
  public void setup() {
    database = new DatabaseFactory("./target/databases/cypherexists").create();

    database.transaction(() -> {
      // Create schema
      database.getSchema().createVertexType("Person");
      database.getSchema().createVertexType("Company");
      database.getSchema().createEdgeType("WORKS_AT");
      database.getSchema().createEdgeType("KNOWS");

      // Create test data
      final MutableVertex alice = database.newVertex("Person").set("name", "Alice").set("age", 30).save();
      final MutableVertex bob = database.newVertex("Person").set("name", "Bob").set("age", 25).save();
      final MutableVertex charlie = database.newVertex("Person").set("name", "Charlie").set("age", 35).save();
      final MutableVertex company = database.newVertex("Company").set("name", "Acme Corp").save();

      // Create relationships
      alice.newEdge("WORKS_AT", company, new Object[0]).save();
      bob.newEdge("WORKS_AT", company, new Object[0]).save();
      alice.newEdge("KNOWS", bob, new Object[0]).save();
    });
  }

  @AfterEach
  public void teardown() {
    if (database != null) {
      database.drop();
    }
  }

  @Test
  public void testExistsWithSimplePattern() {
    // Find people who work at a company
    final ResultSet results = database.query("opencypher",
        "MATCH (p:Person) WHERE EXISTS { (p)-[:WORKS_AT]->(:Company) } RETURN p.name ORDER BY p.name");

    int count = 0;
    while (results.hasNext()) {
      final Result result = results.next();
      final String name = result.getProperty("p.name");
      assertThat(name).isIn("Alice", "Bob");
      count++;
    }
    assertThat(count).isEqualTo(2);
  }

  @Test
  public void testExistsWithNonMatchingPattern() {
    // Find people who don't have KNOWS relationships
    final ResultSet results = database.query("opencypher",
        "MATCH (p:Person) WHERE NOT EXISTS { (p)-[:KNOWS]->() } RETURN p.name ORDER BY p.name");

    int count = 0;
    while (results.hasNext()) {
      final Result result = results.next();
      final String name = result.getProperty("p.name");
      assertThat(name).isIn("Bob", "Charlie");
      count++;
    }
    assertThat(count).isEqualTo(2);
  }

  @Test
  public void testExistsInReturn() {
    // Return boolean indicating whether person works at a company
    final ResultSet results = database.query("opencypher",
        "MATCH (p:Person) RETURN p.name, EXISTS { (p)-[:WORKS_AT]->(:Company) } as hasJob ORDER BY p.name");

    int count = 0;
    while (results.hasNext()) {
      final Result result = results.next();
      final String name = result.getProperty("p.name");
      final Boolean hasJob = (Boolean) result.getProperty("hasJob");

      if (name.equals("Alice") || name.equals("Bob")) {
        assertThat(hasJob).isTrue();
      } else if (name.equals("Charlie")) {
        assertThat(hasJob).isFalse();
      }
      count++;
    }
    assertThat(count).isEqualTo(3);
  }

  @Test
  public void testExistsWithMatchInSubquery() {
    // EXISTS with full MATCH clause
    final ResultSet results = database.query("opencypher",
        "MATCH (p:Person) WHERE EXISTS { MATCH (p)-[:WORKS_AT]->(c:Company) WHERE c.name = 'Acme Corp' } RETURN p.name ORDER BY p.name");

    int count = 0;
    while (results.hasNext()) {
      final Result result = results.next();
      final String name = result.getProperty("p.name");
      assertThat(name).isIn("Alice", "Bob");
      count++;
    }
    assertThat(count).isEqualTo(2);
  }
}
