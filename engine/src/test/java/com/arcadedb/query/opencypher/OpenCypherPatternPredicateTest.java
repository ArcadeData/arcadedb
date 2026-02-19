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

;

/**
 * Tests for pattern predicates in WHERE clauses.
 * Pattern predicates test whether a pattern exists in the graph.
 * Examples: WHERE (n)-[:KNOWS]->(), WHERE NOT (a)-[:LIKES]->(b)
 */
class OpenCypherPatternPredicateTest {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./databases/test-pattern-predicate").create();
    database.getSchema().createVertexType("Person");
    database.getSchema().createEdgeType("KNOWS");
    database.getSchema().createEdgeType("LIKES");
    database.getSchema().createEdgeType("WORKS_WITH");

    // Create test data
    //   Alice KNOWS Bob
    //   Alice KNOWS Charlie
    //   Bob LIKES Alice
    //   David (isolated node)
    database.command("opencypher",
        """
        CREATE (alice:Person {name: 'Alice'}), \
        (bob:Person {name: 'Bob'}), \
        (charlie:Person {name: 'Charlie'}), \
        (david:Person {name: 'David'}), \
        (alice)-[:KNOWS]->(bob), \
        (alice)-[:KNOWS]->(charlie), \
        (bob)-[:LIKES]->(alice)""");
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
    }
  }

  @Test
  void patternPredicateWithOutgoingRelationship() {
    // Find persons who KNOW someone
    final ResultSet result = database.command("opencypher",
        """
        MATCH (n:Person) \
        WHERE (n)-[:KNOWS]->() \
        RETURN n.name AS name ORDER BY name""");

    assertThat(result.hasNext()).isTrue();
    assertThat((String) result.next().getProperty("name")).isEqualTo("Alice");
    assertThat(result.hasNext()).isFalse(); // Only Alice KNOWS someone
  }

  @Test
  void patternPredicateWithIncomingRelationship() {
    // Find persons who are KNOWN by someone
    final ResultSet result = database.command("opencypher",
        """
        MATCH (n:Person) \
        WHERE (n)<-[:KNOWS]-() \
        RETURN n.name AS name ORDER BY name""");

    // Bob and Charlie are known by Alice
    final Set<String> names = new HashSet<>();
    while (result.hasNext()) {
      names.add((String) result.next().getProperty("name"));
    }

    assertThat(names).containsExactlyInAnyOrder("Bob", "Charlie");
  }

  @Test
  void patternPredicateWithBidirectionalRelationship() {
    // Find persons who have any KNOWS relationship (either direction)
    final ResultSet result = database.command("opencypher",
        """
        MATCH (n:Person) \
        WHERE (n)-[:KNOWS]-() \
        RETURN n.name AS name ORDER BY name""");

    // Alice, Bob, and Charlie are all involved in KNOWS relationships
    final Set<String> names = new HashSet<>();
    while (result.hasNext()) {
      names.add((String) result.next().getProperty("name"));
    }

    assertThat(names).containsExactlyInAnyOrder("Alice", "Bob", "Charlie");
  }

  @Test
  void negatedPatternPredicate() {
    // Find persons who DON'T know anyone
    final ResultSet result = database.command("opencypher",
        """
        MATCH (n:Person) \
        WHERE NOT (n)-[:KNOWS]->() \
        RETURN n.name AS name ORDER BY name""");

    // Bob, Charlie, and David don't know anyone
    final Set<String> names = new HashSet<>();
    while (result.hasNext()) {
      names.add((String) result.next().getProperty("name"));
    }

    assertThat(names).containsExactlyInAnyOrder("Bob", "Charlie", "David");
  }

  @Test
  void patternPredicateWithSpecificEndNode() {
    // Find if Alice knows Bob specifically
    final ResultSet result = database.command("opencypher",
        """
        MATCH (alice:Person {name: 'Alice'}), (bob:Person {name: 'Bob'}) \
        WHERE (alice)-[:KNOWS]->(bob) \
        RETURN alice.name AS alice, bob.name AS bob""");

    assertThat(result.hasNext()).isTrue();
    final Result row = result.next();
    assertThat((String) row.getProperty("alice")).isEqualTo("Alice");
    assertThat((String) row.getProperty("bob")).isEqualTo("Bob");
    assertThat(result.hasNext()).isFalse();
  }

  @Test
  void patternPredicateWithSpecificEndNodeNotExist() {
    // Find if Bob knows Alice (should be false)
    final ResultSet result = database.command("opencypher",
        """
        MATCH (alice:Person {name: 'Alice'}), (bob:Person {name: 'Bob'}) \
        WHERE (bob)-[:KNOWS]->(alice) \
        RETURN alice.name AS alice, bob.name AS bob""");

    assertThat(result.hasNext()).isFalse(); // Bob doesn't know Alice
  }

  @Test
  void patternPredicateCombinedWithRegularConditions() {
    // Find persons whose name starts with 'A' and who know someone
    final ResultSet result = database.command("opencypher",
        """
        MATCH (n:Person) \
        WHERE n.name STARTS WITH 'A' AND (n)-[:KNOWS]->() \
        RETURN n.name AS name""");

    assertThat(result.hasNext()).isTrue();
    assertThat((String) result.next().getProperty("name")).isEqualTo("Alice");
    assertThat(result.hasNext()).isFalse();
  }

  @Test
  void patternPredicateWithMultipleRelationshipTypes() {
    // Find persons who have KNOWS or LIKES relationships
    final ResultSet result = database.command("opencypher",
        """
        MATCH (n:Person) \
        WHERE (n)-[:KNOWS|LIKES]->() \
        RETURN n.name AS name ORDER BY name""");

    // Alice has KNOWS, Bob has LIKES
    final Set<String> names = new HashSet<>();
    while (result.hasNext()) {
      names.add((String) result.next().getProperty("name"));
    }

    assertThat(names).containsExactlyInAnyOrder("Alice", "Bob");
  }

  @Test
  void patternPredicateOrCombination() {
    // Find persons who know someone OR are liked by someone
    final ResultSet result = database.command("opencypher",
        """
        MATCH (n:Person) \
        WHERE (n)-[:KNOWS]->() OR (n)<-[:LIKES]-() \
        RETURN n.name AS name ORDER BY name""");

    // Alice knows people and is liked by Bob
    assertThat(result.hasNext()).isTrue();
    assertThat((String) result.next().getProperty("name")).isEqualTo("Alice");
    assertThat(result.hasNext()).isFalse();
  }
}
