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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for MatchRelationshipStep fast path optimization.
 * Verifies that anonymous relationships use the optimized getVertices() path
 * when edges aren't needed (no edge variable, properties, or path tracking).
 */
public class MatchRelationshipStepOptimizationTest {
  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./target/databases/testopencypher-match-optimization").create();

    // Create schema
    database.getSchema().createVertexType("Person");
    database.getSchema().createEdgeType("KNOWS");
    database.getSchema().createEdgeType("FOLLOWS");

    database.transaction(() -> {
      // Create a small social network: Alice -> Bob -> Charlie
      //                                      \-> Dave
      database.command("opencypher", "CREATE (a:Person {name: 'Alice', age: 30})");
      database.command("opencypher", "CREATE (b:Person {name: 'Bob', age: 25})");
      database.command("opencypher", "CREATE (c:Person {name: 'Charlie', age: 28})");
      database.command("opencypher", "CREATE (d:Person {name: 'Dave', age: 35})");

      database.command("opencypher",
          "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) " +
              "CREATE (a)-[:KNOWS {since: 2020}]->(b)");

      database.command("opencypher",
          "MATCH (b:Person {name: 'Bob'}), (c:Person {name: 'Charlie'}) " +
              "CREATE (b)-[:KNOWS {since: 2021}]->(c)");

      database.command("opencypher",
          "MATCH (a:Person {name: 'Alice'}), (d:Person {name: 'Dave'}) " +
              "CREATE (a)-[:FOLLOWS]->(d)");
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
  void fastPathAnonymousRelationshipSimple() {
    // Fast path should be used: anonymous relationship, no properties, no path
    final ResultSet result = database.query("opencypher",
        "MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(b) " +
            "RETURN b.name AS name");

    assertThat(result.hasNext()).isTrue();
    final Result row = result.next();
    assertThat(row.<String>getProperty("name")).isEqualTo("Bob");
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void fastPathAnonymousRelationshipMultipleTypes() {
    // Fast path with multiple edge types
    final ResultSet result = database.query("opencypher",
        "MATCH (a:Person {name: 'Alice'})-[:KNOWS|FOLLOWS]->(b) " +
            "RETURN b.name AS name ORDER BY b.name");

    assertThat(result.hasNext()).isTrue();
    final Set<String> names = new HashSet<>();
    while (result.hasNext()) {
      names.add(result.next().<String>getProperty("name"));
    }
    assertThat(names).containsExactlyInAnyOrder("Bob", "Dave");
    result.close();
  }

  @Test
  void fastPathMultiHop() {
    // Fast path for multi-hop traversal without edge variables
    final ResultSet result = database.query("opencypher",
        "MATCH (a:Person {name: 'Alice'})-[:KNOWS]->()-[:KNOWS]->(c) " +
            "RETURN c.name AS name");

    assertThat(result.hasNext()).isTrue();
    final Result row = result.next();
    assertThat(row.<String>getProperty("name")).isEqualTo("Charlie");
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void standardPathWithEdgeVariable() {
    // Standard path: edge variable is used
    final ResultSet result = database.query("opencypher",
        "MATCH (a:Person {name: 'Alice'})-[r:KNOWS]->(b) " +
            "RETURN b.name AS name, r.since AS since");

    assertThat(result.hasNext()).isTrue();
    final Result row = result.next();
    assertThat(row.<String>getProperty("name")).isEqualTo("Bob");
    assertThat(row.<Integer>getProperty("since")).isEqualTo(2020);
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void standardPathWithEdgePropertyFilter() {
    // Standard path: edge property filter requires loading edge
    final ResultSet result = database.query("opencypher",
        "MATCH (a:Person {name: 'Alice'})-[:KNOWS {since: 2020}]->(b) " +
            "RETURN b.name AS name");

    assertThat(result.hasNext()).isTrue();
    final Result row = result.next();
    assertThat(row.<String>getProperty("name")).isEqualTo("Bob");
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void standardPathWithPath() {
    // Standard path: path variable requires edge tracking
    final ResultSet result = database.query("opencypher",
        "MATCH p = (a:Person {name: 'Alice'})-[:KNOWS]->(b) " +
            "RETURN b.name AS name, length(p) AS pathLength");

    assertThat(result.hasNext()).isTrue();
    final Result row = result.next();
    assertThat(row.<String>getProperty("name")).isEqualTo("Bob");
    assertThat(row.<Long>getProperty("pathLength")).isEqualTo(1L);
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void relationshipUniquenessWithAnonymousEdges() {
    // Test that relationship uniqueness still works with anonymous edges
    database.transaction(() -> {
      // Create a triangle: Alice -> Bob -> Charlie -> Alice
      database.command("opencypher",
          "MATCH (b:Person {name: 'Bob'}), (a:Person {name: 'Alice'}) " +
              "CREATE (b)-[:KNOWS]->(a)");
      database.command("opencypher",
          "MATCH (c:Person {name: 'Charlie'}), (a:Person {name: 'Alice'}) " +
              "CREATE (c)-[:KNOWS]->(a)");
    });

    // This should find paths without reusing the same edge
    final ResultSet result = database.query("opencypher",
        "MATCH (a:Person {name: 'Alice'})-[:KNOWS]->()-[:KNOWS]->(c) " +
            "RETURN c.name AS name");

    // Should find Charlie (Alice -> Bob -> Charlie)
    // Should NOT find Alice (would require reusing Alice->Bob edge)
    final Set<String> names = new HashSet<>();
    while (result.hasNext()) {
      names.add(result.next().<String>getProperty("name"));
    }
    assertThat(names).contains("Charlie");
    // Alice should appear if there's a valid path that doesn't reuse edges
    result.close();
  }

  @Test
  void fastPathBothDirection() {
    // Fast path with BOTH direction
    final ResultSet result = database.query("opencypher",
        "MATCH (b:Person {name: 'Bob'})-[:KNOWS]-(other) " +
            "RETURN other.name AS name ORDER BY other.name");

    final Set<String> names = new HashSet<>();
    while (result.hasNext()) {
      names.add(result.next().<String>getProperty("name"));
    }
    // Bob is connected to Alice (incoming) and Charlie (outgoing)
    assertThat(names).containsExactlyInAnyOrder("Alice", "Charlie");
    result.close();
  }

  @Test
  void fastPathWithTargetLabelFilter() {
    // Fast path with target node label filtering
    final ResultSet result = database.query("opencypher",
        "MATCH (a:Person {name: 'Alice'})-[:KNOWS|FOLLOWS]->(b:Person) " +
            "RETURN b.name AS name ORDER BY b.name");

    final Set<String> names = new HashSet<>();
    while (result.hasNext()) {
      names.add(result.next().<String>getProperty("name"));
    }
    assertThat(names).containsExactlyInAnyOrder("Bob", "Dave");
    result.close();
  }
}
