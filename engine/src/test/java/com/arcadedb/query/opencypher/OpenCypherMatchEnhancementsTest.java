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
import com.arcadedb.query.opencypher.traversal.TraversalPath;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

;

/**
 * Tests for MATCH clause enhancements:
 * - Multiple MATCH clauses
 * - Patterns without labels
 * - Named paths
 */
public class OpenCypherMatchEnhancementsTest {
  private Database database;

  @BeforeEach
  public void setup() {
    database = new DatabaseFactory("./target/databases/testopencypher-match-enhancements").create();

    // Create schema
    database.getSchema().createVertexType("Person");
    database.getSchema().createVertexType("Company");
    database.getSchema().createEdgeType("WORKS_FOR");
    database.getSchema().createEdgeType("KNOWS");

    database.transaction(() -> {
      // Create test data
      database.command("opencypher", "CREATE (a:Person {name: 'Alice', age: 30})");
      database.command("opencypher", "CREATE (b:Person {name: 'Bob', age: 25})");
      database.command("opencypher", "CREATE (c:Company {name: 'TechCorp'})");

      // Alice works for TechCorp
      database.command("opencypher",
          "MATCH (a:Person {name: 'Alice'}), (c:Company {name: 'TechCorp'}) " +
              "CREATE (a)-[:WORKS_FOR]->(c)");

      // Alice knows Bob
      database.command("opencypher",
          "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) " +
              "CREATE (a)-[:KNOWS]->(b)");
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
  public void testMultipleMatchClauses() {
    // Two MATCH clauses: first matches Alice, second matches Bob
    // Result should be Cartesian product: Alice + Bob
    final ResultSet result = database.query("opencypher",
        "MATCH (a:Person {name: 'Alice'}) " +
            "MATCH (b:Person {name: 'Bob'}) " +
            "RETURN a.name AS person1, b.name AS person2");

    assertTrue(result.hasNext());
    final Result row = result.next();
    assertEquals("Alice", row.getProperty("person1"));
    assertEquals("Bob", row.getProperty("person2"));
    assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testMultipleMatchClausesCartesianProduct() {
    // MATCH all people twice - should get Cartesian product (2x2 = 4 results)
    final ResultSet result = database.query("opencypher",
        "MATCH (a:Person) " +
            "MATCH (b:Person) " +
            "RETURN a.name AS person1, b.name AS person2 " +
            "ORDER BY person1, person2");

    final List<Result> results = new ArrayList<>();
    while (result.hasNext()) {
      results.add(result.next());
    }
    result.close();

    assertEquals(4, results.size(), "Expected Cartesian product of 2 people");

    // Alice-Alice, Alice-Bob, Bob-Alice, Bob-Bob
    assertEquals("Alice", results.get(0).getProperty("person1"));
    assertEquals("Alice", results.get(0).getProperty("person2"));

    assertEquals("Alice", results.get(1).getProperty("person1"));
    assertEquals("Bob", results.get(1).getProperty("person2"));

    assertEquals("Bob", results.get(2).getProperty("person1"));
    assertEquals("Alice", results.get(2).getProperty("person2"));

    assertEquals("Bob", results.get(3).getProperty("person1"));
    assertEquals("Bob", results.get(3).getProperty("person2"));
  }

  @Test
  public void testPatternWithoutLabel() {
    // MATCH (n) without label should match ALL vertices (Person and Company)
    final ResultSet result = database.query("opencypher",
        "MATCH (n) RETURN n.name AS name ORDER BY name");

    final List<String> names = new ArrayList<>();
    while (result.hasNext()) {
      names.add((String) result.next().getProperty("name"));
    }
    result.close();

    assertEquals(3, names.size(), "Expected 3 vertices total");
    assertTrue(names.contains("Alice"));
    assertTrue(names.contains("Bob"));
    assertTrue(names.contains("TechCorp"));
  }

  @Test
  public void testPatternWithoutLabelFiltered() {
    // MATCH (n) with WHERE clause to filter
    final ResultSet result = database.query("opencypher",
        "MATCH (n) WHERE n.age > 20 RETURN n.name AS name ORDER BY name");

    final List<String> names = new ArrayList<>();
    while (result.hasNext()) {
      names.add((String) result.next().getProperty("name"));
    }
    result.close();

    // Should match Alice (age 30) and Bob (age 25), but not TechCorp (no age)
    assertEquals(2, names.size());
    assertTrue(names.contains("Alice"));
    assertTrue(names.contains("Bob"));
  }

  @Test
  public void testNamedPathSingleEdge() {
    // Named path: p = (a)-[r]->(b)
    final ResultSet result = database.query("opencypher",
        "MATCH p = (a:Person {name: 'Alice'})-[r:KNOWS]->(b:Person) " +
            "RETURN a.name AS person, b.name AS friend, p AS path");

    assertTrue(result.hasNext());
    final Result row = result.next();
    assertEquals("Alice", row.getProperty("person"));
    assertEquals("Bob", row.getProperty("friend"));

    // Check that path object is returned
    final Object pathObj = row.getProperty("path");
    assertNotNull(pathObj, "Path variable should not be null");
    assertTrue(pathObj instanceof TraversalPath, "Path should be a TraversalPath object");

    final TraversalPath path = (TraversalPath) pathObj;
    assertEquals(1, path.length(), "Path should have length 1 (one edge)");
    assertEquals(2, path.getVertices().size(), "Path should have 2 vertices");
    assertEquals(1, path.getEdges().size(), "Path should have 1 edge");

    // Check vertices in path
    assertEquals("Alice", path.getStartVertex().get("name"));
    assertEquals("Bob", path.getEndVertex().get("name"));

    assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testNamedPathMultiplePaths() {
    // Alice has two outgoing relationships: KNOWS to Bob, WORKS_FOR to TechCorp
    // Query all paths from Alice
    final ResultSet result = database.query("opencypher",
        "MATCH p = (a:Person {name: 'Alice'})-[r]->(b) " +
            "RETURN a.name AS person, b.name AS target, p AS path");

    final List<TraversalPath> paths = new ArrayList<>();
    while (result.hasNext()) {
      final Result row = result.next();
      paths.add((TraversalPath) row.getProperty("path"));
    }
    result.close();

    assertEquals(2, paths.size(), "Alice should have 2 outgoing paths");

    // Both paths should have length 1
    for (final TraversalPath path : paths) {
      assertEquals(1, path.length());
      assertEquals("Alice", path.getStartVertex().get("name"));
    }

    // Collect target names
    final Set<String> targets = new HashSet<>();
    for (final TraversalPath path : paths) {
      targets.add((String) path.getEndVertex().get("name"));
    }

    assertTrue(targets.contains("Bob"));
    assertTrue(targets.contains("TechCorp"));
  }

  @Test
  public void testCombinedFeatures() {
    // Combine multiple features: multiple MATCH + unlabeled pattern + named path
    final ResultSet result = database.query("opencypher",
        "MATCH (start) " +
            "WHERE start.name = 'Alice' " +
            "MATCH p = (start)-[r]->(target) " +
            "RETURN start.name AS startName, target.name AS targetName, p AS path");

    int pathCount = 0;
    while (result.hasNext()) {
      final Result row = result.next();
      assertEquals("Alice", row.getProperty("startName"));

      final TraversalPath path = (TraversalPath) row.getProperty("path");
      assertNotNull(path);
      assertEquals(1, path.length());
      pathCount++;
    }
    result.close();

    assertEquals(2, pathCount, "Alice should have 2 outgoing paths");
  }
}
