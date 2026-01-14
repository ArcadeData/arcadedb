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
package com.arcadedb.openopencypher;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for OPTIONAL MATCH clause functionality.
 */
public class OpenCypherOptionalMatchTest {
  private Database database;

  @BeforeEach
  public void setup() {
    database = new DatabaseFactory("./target/databases/testopenopencypher-optional").create();

    // Create schema
    database.getSchema().createVertexType("Person");
    database.getSchema().createEdgeType("KNOWS");

    database.transaction(() -> {
      // Create test data: Person nodes, some with KNOWS relationships
      database.command("opencypher", "CREATE (a:Person {name: 'Alice', age: 30})");
      database.command("opencypher", "CREATE (b:Person {name: 'Bob', age: 25})");
      database.command("opencypher", "CREATE (c:Person {name: 'Charlie', age: 35})");

      // Alice knows Bob
      database.command("opencypher", "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) " +
          "CREATE (a)-[:KNOWS]->(b)");

      // Charlie has no KNOWS relationships
    });
  }

  @AfterEach
  public void cleanup() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  public void testOptionalMatchWithExistingRelationship() {
    // Alice has a KNOWS relationship, should return Alice and Bob
    final ResultSet result = database.query("opencypher",
        "MATCH (a:Person {name: 'Alice'}) " +
            "OPTIONAL MATCH (a)-[r:KNOWS]->(b:Person) " +
            "RETURN a.name AS person, b.name AS knows");

    assertTrue(result.hasNext());
    final Result row = result.next();
    assertEquals("Alice", row.getProperty("person"));
    assertEquals("Bob", row.getProperty("knows"));
    assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testMatchCharlieAlone() {
    // First test that basic MATCH with property filter works
    final ResultSet result = database.query("opencypher",
        "MATCH (a:Person {name: 'Charlie'}) RETURN a.name AS person");

    final List<Result> allResults = new ArrayList<>();
    while (result.hasNext()) {
      final Result row = result.next();
      allResults.add(row);
      System.out.println("DEBUG testMatchCharlieAlone: person=" + row.getProperty("person"));
    }
    result.close();

    assertEquals(1, allResults.size(), "Expected exactly 1 result");
    assertEquals("Charlie", allResults.get(0).getProperty("person"));
  }

  @Test
  public void testOptionalMatchWithoutRelationship() {
    // Charlie has no KNOWS relationship, should return Charlie with NULL for b
    final ResultSet result = database.query("opencypher",
        "MATCH (a:Person {name: 'Charlie'}) " +
            "OPTIONAL MATCH (a)-[r:KNOWS]->(b:Person) " +
            "RETURN a.name AS person, b.name AS knows");

    // Debug: print all results
    final List<Result> allResults = new ArrayList<>();
    while (result.hasNext()) {
      final Result row = result.next();
      allResults.add(row);
      System.out.println("DEBUG testOptionalMatchWithoutRelationship: person=" + row.getProperty("person") + ", knows=" + row.getProperty("knows"));
    }
    result.close();

    assertEquals(1, allResults.size(), "Expected exactly 1 result");
    final Result row = allResults.get(0);
    assertEquals("Charlie", row.getProperty("person"));
    assertNull(row.getProperty("knows"), "Expected NULL for knows when no relationship exists");
  }

  @Test
  public void testOptionalMatchStandalone() {
    // OPTIONAL MATCH without preceding MATCH
    // Should return all Person nodes or NULL if no matches
    final ResultSet result = database.query("opencypher",
        "OPTIONAL MATCH (n:Person {name: 'NonExistent'}) " +
            "RETURN n.name AS name");

    assertTrue(result.hasNext());
    final Result row = result.next();
    assertNull(row.getProperty("name"), "Expected NULL when OPTIONAL MATCH finds nothing");
    assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testMultiplePeopleWithOptionalMatch() {
    // All people, with optional KNOWS relationships
    final ResultSet result = database.query("opencypher",
        "MATCH (a:Person) " +
            "OPTIONAL MATCH (a)-[:KNOWS]->(b:Person) " +
            "RETURN a.name AS person, b.name AS knows " +
            "ORDER BY a.name");

    final List<Result> results = new ArrayList<>();
    while (result.hasNext()) {
      results.add(result.next());
    }
    result.close();

    // Alice knows Bob
    // Bob knows nobody -> NULL
    // Charlie knows nobody -> NULL
    assertEquals(3, results.size(), "Expected 3 results (one per person)");

    // Alice -> Bob
    assertEquals("Alice", results.get(0).getProperty("person"));
    assertEquals("Bob", results.get(0).getProperty("knows"));

    // Bob -> NULL
    assertEquals("Bob", results.get(1).getProperty("person"));
    assertNull(results.get(1).getProperty("knows"));

    // Charlie -> NULL
    assertEquals("Charlie", results.get(2).getProperty("person"));
    assertNull(results.get(2).getProperty("knows"));
  }

  @Test
  public void testOptionalMatchWithWhere() {
    // WHERE clause is now correctly scoped to OPTIONAL MATCH
    // It filters the optional match results but keeps rows where the match failed

    // Query: MATCH all people, try to find their KNOWS relationships with WHERE filter
    // WHERE filters within OPTIONAL MATCH, so people without matches still appear
    final ResultSet result = database.query("opencypher",
        "MATCH (a:Person) " +
            "OPTIONAL MATCH (a)-[:KNOWS]->(b:Person) " +
            "WHERE b.age > 20 " +
            "RETURN a.name AS person, b.name AS knows " +
            "ORDER BY a.name");

    final List<Result> results = new ArrayList<>();
    while (result.hasNext()) {
      results.add(result.next());
    }
    result.close();

    // Correct behavior: WHERE filters within OPTIONAL MATCH
    // All people are returned, but only matches passing the filter are shown
    assertEquals(3, results.size(), "All people should be returned");

    // Alice -> Bob (matched and passed filter: age 25 > 20)
    assertEquals("Alice", results.get(0).getProperty("person"));
    assertEquals("Bob", results.get(0).getProperty("knows"));

    // Bob -> NULL (no outgoing relationships)
    assertEquals("Bob", results.get(1).getProperty("person"));
    assertNull(results.get(1).getProperty("knows"));

    // Charlie -> NULL (no outgoing relationships)
    assertEquals("Charlie", results.get(2).getProperty("person"));
    assertNull(results.get(2).getProperty("knows"));
  }
}
