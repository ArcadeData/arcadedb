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

import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for pattern predicates in WHERE clauses.
 * Pattern predicates test whether a pattern exists in the graph.
 * Examples: WHERE (n)-[:KNOWS]->(), WHERE NOT (a)-[:LIKES]->(b)
 */
class OpenCypherPatternPredicateTest extends TestHelper {
  @Override
  protected void beginTest() {
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
    private Database db;

    @BeforeEach
    void setUp() {
      db = TestHelper.createDatabase("./target/databases/test-issue3331");
      db.getSchema().createVertexType("Person");
      db.getSchema().createEdgeType("KNOWS");
    }

    @AfterEach
    void tearDown() {
      if (db != null) {
        db.drop();
        db = null;
      }
    }

    @Test
    void patternComprehensionFromIssue() {
      // Exact scenario from issue #3331
      db.transaction(() ->
        db.command("opencypher",
            """
            CREATE (a:Person {name:'A'})-[:KNOWS]->(:Person {name:'B'}), \
            (a)-[:KNOWS]->(:Person {name:'C'})"""));

      try (final ResultSet rs = db.query("opencypher",
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
      db.transaction(() ->
        db.command("opencypher",
            """
            CREATE (a:Person {name:'A'})-[:KNOWS]->(:Person {name:'B'}), \
            (a)-[:KNOWS]->(:Person {name:'C'})"""));

      try (final ResultSet rs = db.query("opencypher",
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
      db.transaction(() ->
        db.command("opencypher",
            """
            CREATE (a:Person {name:'A'})-[:KNOWS]->(:Person {name:'B'}), \
            (a)-[:LIKES]->(:Person {name:'C'})"""));

      try (final ResultSet rs = db.query("opencypher",
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
      db.transaction(() ->
        db.command("opencypher",
            "CREATE (:Person {name:'A'})"));

      try (final ResultSet rs = db.query("opencypher",
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

    /**
     * See issue #5007: a 2-hop pattern comprehension with an anonymous middle node
     * {@code (:Person)} silently returned an empty list. The anonymous node must be
     * carried forward as the start of the following hop.
     */
    @Test
    void patternComprehensionTwoHopAnonymousMiddleNode() {
      db.transaction(() ->
          db.command("opencypher",
              """
              CREATE (p:Person {probe: true}), (m:Person {gid: 2}), (t:Person {gid: 3}), \
              (p)-[:KNOWS]->(m), (m)-[:KNOWS]->(t)"""));

      try (final ResultSet rs = db.query("opencypher",
          """
          MATCH (p:Person {probe: true}) \
          RETURN [(p)-[:KNOWS]->(:Person)-[:KNOWS]->(target:Person) | target.gid] AS targets""")) {
        assertThat(rs.hasNext()).isTrue();
        final Result row = rs.next();
        final Object resultObj = row.getProperty("targets");
        assertThat(resultObj).isInstanceOf(List.class);
        @SuppressWarnings("unchecked")
        final List<Object> resultList = (List<Object>) resultObj;
        assertThat(resultList).containsExactly(3);
      }
    }

    /** See issue #5007: a 2-hop pattern comprehension with a named middle node must keep working. */
    @Test
    void patternComprehensionTwoHopNamedMiddleNode() {
      db.transaction(() ->
          db.command("opencypher",
              """
              CREATE (p:Person {probe: true}), (m:Person {gid: 2}), (t:Person {gid: 3}), \
              (p)-[:KNOWS]->(m), (m)-[:KNOWS]->(t)"""));

      try (final ResultSet rs = db.query("opencypher",
          """
          MATCH (p:Person {probe: true}) \
          RETURN [(p)-[:KNOWS]->(mid:Person)-[:KNOWS]->(target:Person) | target.gid] AS targets""")) {
        assertThat(rs.hasNext()).isTrue();
        final Result row = rs.next();
        final Object resultObj = row.getProperty("targets");
        assertThat(resultObj).isInstanceOf(List.class);
        @SuppressWarnings("unchecked")
        final List<Object> resultList = (List<Object>) resultObj;
        assertThat(resultList).containsExactly(3);
      }
    }
  }

  /** See issue #3938: existential pattern predicate must filter by target-node properties too. */
  @Nested
  class ExistentialPatternPredicateTargetPropertiesRegression {
    private Database db;

    @BeforeEach
    void setUp() {
      db = TestHelper.createDatabase("./target/databases/test-issue3938");
      db.getSchema().createVertexType("Person");
      db.getSchema().createVertexType("Country");
      db.getSchema().createEdgeType("LIVING_IN");

      db.transaction(() -> {
        db.command("opencypher",
            """
            CREATE (:Country {name: 'Germany'}), \
            (:Country {name: 'United Kingdom'}), \
            (:Person {name: 'Alice'}), \
            (:Person {name: 'Bob'}), \
            (:Person {name: 'Charlie'})""");
        db.command("opencypher",
            """
            MATCH (p:Person {name: 'Alice'}), (c:Country {name: 'Germany'}) \
            CREATE (p)-[:LIVING_IN]->(c)""");
        db.command("opencypher",
            """
            MATCH (p:Person {name: 'Bob'}), (c:Country {name: 'United Kingdom'}) \
            CREATE (p)-[:LIVING_IN]->(c)""");
        db.command("opencypher",
            """
            MATCH (p:Person {name: 'Charlie'}), (c:Country {name: 'Germany'}) \
            CREATE (p)-[:LIVING_IN]->(c)""");
      });
    }

    @AfterEach
    void tearDown() {
      if (db != null) {
        db.drop();
        db = null;
      }
    }

    @Test
    void patternPredicateFiltersByTargetNodeProperties() {
      // Only people living in Germany must pass the predicate.
      try (final ResultSet rs = db.query("opencypher",
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
      try (final ResultSet rs = db.query("opencypher",
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
      try (final ResultSet rs = db.query("opencypher",
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
      try (final ResultSet rs = db.query("opencypher",
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

  @Nested
  class InlineRelationshipPredicate {
    private Database db;

    @BeforeEach
    void setUp() {
      db = TestHelper.createDatabase("./target/databases/test-inline-rel-predicate");
      db.getSchema().createVertexType("Person");
      db.getSchema().createEdgeType("KNOWS");
      db.command("opencypher",
          """
          CREATE (a:Person {name: 'Alice'}), \
          (b:Person {name: 'Bob'}), \
          (c:Person {name: 'Charlie'}), \
          (a)-[:KNOWS {since: 2018}]->(b), \
          (a)-[:KNOWS {since: 2020}]->(c)""");
    }

    @AfterEach
    void tearDown() {
      if (db != null) {
        db.drop();
        db = null;
      }
    }

    @Test
    void inlineWhereOnRelationshipIsApplied() {
      // Only the 2018 relationship must match in each direction
      try (final ResultSet rs = db.query("opencypher",
          """
          MATCH (a:Person)-[r:KNOWS WHERE r.since < 2019]-(b) \
          RETURN DISTINCT a.name AS person, b.name AS friend, r.since AS knowsSince \
          ORDER BY knowsSince, person, friend""")) {
        final List<String> rows = new ArrayList<>();
        while (rs.hasNext()) {
          final Result row = rs.next();
          rows.add(row.getProperty("person") + "->" + row.getProperty("friend") + ":" + row.getProperty("knowsSince"));
        }
        assertThat(rows).containsExactlyInAnyOrder("Alice->Bob:2018", "Bob->Alice:2018");
      }
    }

    @Test
    void inlineWhereOnRelationshipDirected() {
      // Directed pattern: only Alice->Bob (since=2018) must be returned
      try (final ResultSet rs = db.query("opencypher",
          """
          MATCH (a:Person)-[r:KNOWS WHERE r.since < 2019]->(b) \
          RETURN a.name AS person, b.name AS friend, r.since AS knowsSince""")) {
        assertThat(rs.hasNext()).isTrue();
        final Result row = rs.next();
        assertThat((String) row.getProperty("person")).isEqualTo("Alice");
        assertThat((String) row.getProperty("friend")).isEqualTo("Bob");
        assertThat(((Number) row.getProperty("knowsSince")).intValue()).isEqualTo(2018);
        assertThat(rs.hasNext()).isFalse();
      }
    }

    @Test
    void inlineWhereWithExternalWhereClause() {
      // Combined: inline relationship predicate AND an outer WHERE clause
      try (final ResultSet rs = db.query("opencypher",
          """
          MATCH (a:Person)-[r:KNOWS WHERE r.since < 2019]-(b) \
          WHERE NOT (b:NonexistentLabel) \
          RETURN DISTINCT a.name AS person, b.name AS friend, r.since AS knowsSince \
          ORDER BY knowsSince""")) {
        final List<String> rows = new ArrayList<>();
        while (rs.hasNext()) {
          final Result row = rs.next();
          rows.add(row.getProperty("person") + "->" + row.getProperty("friend") + ":" + row.getProperty("knowsSince"));
        }
        assertThat(rows).containsExactlyInAnyOrder("Alice->Bob:2018", "Bob->Alice:2018");
      }
    }
  }
}
