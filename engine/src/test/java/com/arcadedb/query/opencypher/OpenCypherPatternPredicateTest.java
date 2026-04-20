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
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;

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

  /** See issue #3331 */
  @Nested
  class PatternComprehensionReturnTypeRegression {
    private Database database;

    @BeforeEach
    void setUp() {
      database = new DatabaseFactory("./target/databases/test-issue3331").create();
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

    @Test
    void patternComprehensionFromIssue() {
      // Exact scenario from issue #3331
      database.transaction(() -> {
        database.command("opencypher",
            """
            CREATE (a:Person {name:'A'})-[:KNOWS]->(:Person {name:'B'}), \
            (a)-[:KNOWS]->(:Person {name:'C'})""");
      });

      try (final ResultSet rs = database.query("opencypher",
          """
          MATCH (a:Person {name: 'A'}) \
          RETURN [(a)-->(friend) WHERE friend.name <> 'B' | friend.name] AS result""")) {
        assertThat(rs.hasNext()).isTrue();
        final Result row = rs.next();
        final Object resultObj = row.getProperty("result");
        assertThat(resultObj).isInstanceOf(List.class);
        @SuppressWarnings("unchecked")
        final List<Object> resultList = (List<Object>) resultObj;
        assertThat(resultList).containsExactly("C");
      }
    }

    @Test
    void patternComprehensionNoFilter() {
      // Pattern comprehension without WHERE clause
      database.transaction(() -> {
        database.command("opencypher",
            """
            CREATE (a:Person {name:'A'})-[:KNOWS]->(:Person {name:'B'}), \
            (a)-[:KNOWS]->(:Person {name:'C'})""");
      });

      try (final ResultSet rs = database.query("opencypher",
          """
          MATCH (a:Person {name: 'A'}) \
          RETURN [(a)-->(friend) | friend.name] AS result""")) {
        assertThat(rs.hasNext()).isTrue();
        final Result row = rs.next();
        final Object resultObj = row.getProperty("result");
        assertThat(resultObj).isInstanceOf(List.class);
        @SuppressWarnings("unchecked")
        final List<Object> resultList = (List<Object>) resultObj;
        assertThat(resultList).containsExactlyInAnyOrder("B", "C");
      }
    }

    @Test
    void patternComprehensionWithRelType() {
      // Pattern comprehension with specific relationship type
      database.transaction(() -> {
        database.command("opencypher",
            """
            CREATE (a:Person {name:'A'})-[:KNOWS]->(:Person {name:'B'}), \
            (a)-[:LIKES]->(:Person {name:'C'})""");
      });

      try (final ResultSet rs = database.query("opencypher",
          """
          MATCH (a:Person {name: 'A'}) \
          RETURN [(a)-[:KNOWS]->(friend) | friend.name] AS result""")) {
        assertThat(rs.hasNext()).isTrue();
        final Result row = rs.next();
        final Object resultObj = row.getProperty("result");
        assertThat(resultObj).isInstanceOf(List.class);
        @SuppressWarnings("unchecked")
        final List<Object> resultList = (List<Object>) resultObj;
        assertThat(resultList).containsExactly("B");
      }
    }

    @Test
    void patternComprehensionEmptyResult() {
      // Pattern comprehension that matches nothing
      database.transaction(() -> {
        database.command("opencypher",
            "CREATE (:Person {name:'A'})");
      });

      try (final ResultSet rs = database.query("opencypher",
          """
          MATCH (a:Person {name: 'A'}) \
          RETURN [(a)-->(friend) | friend.name] AS result""")) {
        assertThat(rs.hasNext()).isTrue();
        final Result row = rs.next();
        final Object resultObj = row.getProperty("result");
        assertThat(resultObj).isInstanceOf(List.class);
        @SuppressWarnings("unchecked")
        final List<Object> resultList = (List<Object>) resultObj;
        assertThat(resultList).isEmpty();
      }
    }
  }

  /** See issue #3938: existential pattern predicate must filter by target-node properties too. */
  @Nested
  class ExistentialPatternPredicateTargetPropertiesRegression {
    private Database database;

    @BeforeEach
    void setUp() {
      database = new DatabaseFactory("./target/databases/test-issue3938").create();
      database.getSchema().createVertexType("Person");
      database.getSchema().createVertexType("Country");
      database.getSchema().createEdgeType("LIVING_IN");

      database.transaction(() -> {
        database.command("opencypher",
            """
            CREATE (:Country {name: 'Germany'}), \
            (:Country {name: 'United Kingdom'}), \
            (:Person {name: 'Alice'}), \
            (:Person {name: 'Bob'}), \
            (:Person {name: 'Charlie'})""");
        database.command("opencypher",
            """
            MATCH (p:Person {name: 'Alice'}), (c:Country {name: 'Germany'}) \
            CREATE (p)-[:LIVING_IN]->(c)""");
        database.command("opencypher",
            """
            MATCH (p:Person {name: 'Bob'}), (c:Country {name: 'United Kingdom'}) \
            CREATE (p)-[:LIVING_IN]->(c)""");
        database.command("opencypher",
            """
            MATCH (p:Person {name: 'Charlie'}), (c:Country {name: 'Germany'}) \
            CREATE (p)-[:LIVING_IN]->(c)""");
      });
    }

    @AfterEach
    void tearDown() {
      if (database != null) {
        database.drop();
        database = null;
      }
    }

    @Test
    void patternPredicateFiltersByTargetNodeProperties() {
      // Only people living in Germany must pass the predicate.
      try (final ResultSet rs = database.query("opencypher",
          """
          MATCH (p:Person) \
          WHERE (p)-[:LIVING_IN]->(:Country {name: 'Germany'}) \
          RETURN p.name AS name ORDER BY name""")) {
        final Set<String> names = new HashSet<>();
        while (rs.hasNext())
          names.add((String) rs.next().getProperty("name"));
        assertThat(names).containsExactlyInAnyOrder("Alice", "Charlie");
      }
    }

    @Test
    void patternPredicateFiltersByTargetNodePropertiesAnonymousEnd() {
      // Anonymous end node with property constraint only.
      try (final ResultSet rs = database.query("opencypher",
          """
          MATCH (p:Person) \
          WHERE (p)-[:LIVING_IN]->({name: 'Germany'}) \
          RETURN p.name AS name ORDER BY name""")) {
        final Set<String> names = new HashSet<>();
        while (rs.hasNext())
          names.add((String) rs.next().getProperty("name"));
        assertThat(names).containsExactlyInAnyOrder("Alice", "Charlie");
      }
    }

    @Test
    void patternPredicateFilteredRowDoesNotLeakIntoDownstreamMatch() {
      // The predicate must cut Bob off, so a later MATCH cannot reintroduce United Kingdom.
      try (final ResultSet rs = database.query("opencypher",
          """
          MATCH (p:Person) \
          WHERE (p)-[:LIVING_IN]->(:Country {name: 'Germany'}) \
          WITH p \
          MATCH (p)-[:LIVING_IN]->(c:Country) \
          RETURN c.name AS country, count(*) AS cnt \
          ORDER BY country""")) {
        assertThat(rs.hasNext()).isTrue();
        final Result row = rs.next();
        assertThat((String) row.getProperty("country")).isEqualTo("Germany");
        assertThat(((Number) row.getProperty("cnt")).longValue()).isEqualTo(2L);
        assertThat(rs.hasNext()).isFalse();
      }
    }

    @Test
    void negatedPatternPredicateHonorsTargetNodeProperties() {
      // Only people NOT living in Germany must pass.
      try (final ResultSet rs = database.query("opencypher",
          """
          MATCH (p:Person) \
          WHERE NOT (p)-[:LIVING_IN]->(:Country {name: 'Germany'}) \
          RETURN p.name AS name ORDER BY name""")) {
        final Set<String> names = new HashSet<>();
        while (rs.hasNext())
          names.add((String) rs.next().getProperty("name"));
        assertThat(names).containsExactlyInAnyOrder("Bob");
      }
    }
  }
}
