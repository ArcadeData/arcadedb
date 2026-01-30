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

import static org.assertj.core.api.Assertions.assertThat;

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
  void setup() {
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
  void cleanup() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void multipleMatchClauses() {
    // Two MATCH clauses: first matches Alice, second matches Bob
    // Result should be Cartesian product: Alice + Bob
    final ResultSet result = database.query("opencypher",
        "MATCH (a:Person {name: 'Alice'}) " +
            "MATCH (b:Person {name: 'Bob'}) " +
            "RETURN a.name AS person1, b.name AS person2");

    assertThat(result.hasNext()).isTrue();
    final Result row = result.next();
    assertThat(row.<String>getProperty("person1")).isEqualTo("Alice");
    assertThat(row.<String>getProperty("person2")).isEqualTo("Bob");
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void multipleMatchClausesCartesianProduct() {
    // MATCH all people twice - should get Cartesian product (2x2 = 4 results)
    final ResultSet result = database.query("opencypher",
        """
            MATCH (a:Person)
            MATCH (b:Person)
            RETURN a.name AS person1, b.name AS person2
            ORDER BY person1, person2""");

    final List<Result> results = new ArrayList<>();
    while (result.hasNext()) {
      results.add(result.next());
    }
    result.close();

    assertThat(results.size()).as("Expected Cartesian product of 2 people").isEqualTo(4);

    // Alice-Alice, Alice-Bob, Bob-Alice, Bob-Bob
    assertThat(results.get(0).<String>getProperty("person1")).isEqualTo("Alice");
    assertThat(results.get(0).<String>getProperty("person2")).isEqualTo("Alice");

    assertThat(results.get(1).<String>getProperty("person1")).isEqualTo("Alice");
    assertThat(results.get(1).<String>getProperty("person2")).isEqualTo("Bob");

    assertThat(results.get(2).<String>getProperty("person1")).isEqualTo("Bob");
    assertThat(results.get(2).<String>getProperty("person2")).isEqualTo("Alice");

    assertThat(results.get(3).<String>getProperty("person1")).isEqualTo("Bob");
    assertThat(results.get(3).<String>getProperty("person2")).isEqualTo("Bob");
  }

  @Test
  void patternWithoutLabel() {
    // MATCH (n) without label should match ALL vertices (Person and Company)
    final ResultSet result = database.query("opencypher",
        "MATCH (n) RETURN n.name AS name ORDER BY name");

    final List<String> names = new ArrayList<>();
    while (result.hasNext()) {
      names.add(result.next().getProperty("name"));
    }
    result.close();

    assertThat(names.size()).as("Expected 3 vertices total").isEqualTo(3);
    assertThat(names.contains("Alice")).isTrue();
    assertThat(names.contains("Bob")).isTrue();
    assertThat(names.contains("TechCorp")).isTrue();
  }

  @Test
  void patternWithoutLabelFiltered() {
    // MATCH (n) with WHERE clause to filter
    final ResultSet result = database.query("opencypher",
        "MATCH (n) WHERE n.age > 20 RETURN n.name AS name ORDER BY name");

    final List<String> names = new ArrayList<>();
    while (result.hasNext()) {
      names.add(result.next().getProperty("name"));
    }
    result.close();

    // Should match Alice (age 30) and Bob (age 25), but not TechCorp (no age)
    assertThat(names.size()).isEqualTo(2);
    assertThat(names.contains("Alice")).isTrue();
    assertThat(names.contains("Bob")).isTrue();
  }

  @Test
  void namedPathSingleEdge() {
    // Named path: p = (a)-[r]->(b)
    final ResultSet result = database.query("opencypher",
        """
            MATCH p = (a:Person {name: 'Alice'})-[r:KNOWS]->(b:Person)
            RETURN a.name AS person, b.name AS friend, p AS path""");

    assertThat(result.hasNext()).isTrue();
    final Result row = result.next();
    assertThat(row.<String>getProperty("person")).isEqualTo("Alice");
    assertThat(row.<String>getProperty("friend")).isEqualTo("Bob");

    // Check that path object is returned
    final Object pathObj = row.getProperty("path");
    assertThat(pathObj).as("Path variable should not be null").isNotNull();
    assertThat(pathObj).as("Path should be a TraversalPath object").isInstanceOf(TraversalPath.class);

    final TraversalPath path = (TraversalPath) pathObj;
    assertThat(path.length()).as("Path should have length 1 (one edge)").isEqualTo(1);
    assertThat(path.getVertices().size()).as("Path should have 2 vertices").isEqualTo(2);
    assertThat(path.getEdges().size()).as("Path should have 1 edge").isEqualTo(1);

    // Check vertices in path
    assertThat(path.getStartVertex().get("name")).isEqualTo("Alice");
    assertThat(path.getEndVertex().get("name")).isEqualTo("Bob");

    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void namedPathMultiplePaths() {
    // Alice has two outgoing relationships: KNOWS to Bob, WORKS_FOR to TechCorp
    // Query all paths from Alice
    final ResultSet result = database.query("opencypher",
        "MATCH p = (a:Person {name: 'Alice'})-[r]->(b) " +
            "RETURN a.name AS person, b.name AS target, p AS path");

    final List<TraversalPath> paths = new ArrayList<>();
    while (result.hasNext()) {
      final Result row = result.next();
      paths.add(row.getProperty("path"));
    }
    result.close();

    assertThat(paths.size()).as("Alice should have 2 outgoing paths").isEqualTo(2);

    // Both paths should have length 1
    for (final TraversalPath path : paths) {
      assertThat(path.length()).isEqualTo(1);
      assertThat(path.getStartVertex().get("name")).isEqualTo("Alice");
    }

    // Collect target names
    final Set<String> targets = new HashSet<>();
    for (final TraversalPath path : paths) {
      targets.add((String) path.getEndVertex().get("name"));
    }

    assertThat(targets.contains("Bob")).isTrue();
    assertThat(targets.contains("TechCorp")).isTrue();
  }

  @Test
  void combinedFeatures() {
    // Combine multiple features: multiple MATCH + unlabeled pattern + named path
    final ResultSet result = database.query("opencypher",
        "MATCH (start) " +
            "WHERE start.name = 'Alice' " +
            "MATCH p = (start)-[r]->(target) " +
            "RETURN start.name AS startName, target.name AS targetName, p AS path");

    int pathCount = 0;
    while (result.hasNext()) {
      final Result row = result.next();
      assertThat(row.<String>getProperty("startName")).isEqualTo("Alice");

      final TraversalPath path = row.getProperty("path");
      assertThat(path).isNotNull();
      assertThat(path.length()).isEqualTo(1);
      pathCount++;
    }
    result.close();

    assertThat(pathCount).as("Alice should have 2 outgoing paths").isEqualTo(2);
  }
}
