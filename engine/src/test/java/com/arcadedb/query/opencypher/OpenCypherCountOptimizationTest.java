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
import com.arcadedb.schema.VertexType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the TYPE COUNT optimization in OpenCypher queries.
 * Verifies that simple COUNT queries like "MATCH (a:Account) RETURN COUNT(a)" use O(1) optimization.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class OpenCypherCountOptimizationTest {
  /**
   * The marker {@code TypeCountStep.prettyPrint} emits. Asserting on it is what stops the push-down regressing
   * silently: a full type scan returns the same count, just slower, so the result assertions alone cannot tell the
   * two paths apart (issue #6280). The negative cases matter as much as the positive ones - they are what keeps the
   * push-down from firing where it is not valid.
   */
  private static final String TYPE_COUNT_PUSH_DOWN = "TYPE COUNT OPTIMIZATION ";

  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/testcyphercount").create();
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void simpleCountOptimization() {
    // Create test data
    database.transaction(() -> {
      database.getSchema().createVertexType("Account");
      for (int i = 0; i < 100; i++)
        database.newVertex("Account").save();
    });

    // Test the optimized count query
    final String query = "MATCH (a:Account) RETURN COUNT(a) as count";
    final ResultSet result = database.query("opencypher", query);

    assertThat(result.hasNext()).isTrue();
    final Result row = result.next();
    assertThat(row.<Long>getProperty("count")).isEqualTo(100L);
    assertThat(result.hasNext()).isFalse();
    result.close();

    assertThat(planOf(query)).contains(TYPE_COUNT_PUSH_DOWN + "(Account)");
  }

  @Test
  void countWithDifferentAliases() {
    // Create test data
    database.transaction(() -> {
      database.getSchema().createVertexType("Person");
      for (int i = 0; i < 50; i++)
        database.newVertex("Person").save();
    });

    // Test with different alias names
    final String query1 = "MATCH (p:Person) RETURN COUNT(p) as totalPeople";
    final ResultSet result1 = database.query("opencypher", query1);
    assertThat(result1.hasNext()).isTrue();
    assertThat(result1.next().<Long>getProperty("totalPeople")).isEqualTo(50L);
    result1.close();

    final String query2 = "MATCH (x:Person) RETURN COUNT(x) as cnt";
    final ResultSet result2 = database.query("opencypher", query2);
    assertThat(result2.hasNext()).isTrue();
    assertThat(result2.next().<Long>getProperty("cnt")).isEqualTo(50L);
    result2.close();

    // The alias is what the push-down has to carry through; both forms must still take it.
    assertThat(planOf(query1)).contains(TYPE_COUNT_PUSH_DOWN + "(Person)");
    assertThat(planOf(query2)).contains(TYPE_COUNT_PUSH_DOWN + "(Person)");
  }

  @Test
  void countWithEmptyType() {
    // Create empty type
    database.transaction(() ->
      database.getSchema().createVertexType("EmptyType"));

    // Test count on empty type
    final String query = "MATCH (e:EmptyType) RETURN COUNT(e) as count";
    final ResultSet result = database.query("opencypher", query);

    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<Long>getProperty("count")).isEqualTo(0L);
    result.close();

    assertThat(planOf(query)).contains(TYPE_COUNT_PUSH_DOWN + "(EmptyType)");
  }

  @Test
  void optimizationNotAppliedWithWhereClause() {
    // Create test data with properties
    database.transaction(() -> {
      final VertexType accountType = database.getSchema().createVertexType("BankAccount");
      accountType.createProperty("balance", Integer.class);

      for (int i = 0; i < 100; i++)
        database.newVertex("BankAccount").set("balance", i * 100).save();
    });

    // This query should NOT use the optimization due to WHERE clause
    final String query = "MATCH (a:BankAccount) WHERE a.balance > 5000 RETURN COUNT(a) as count";
    final ResultSet result = database.query("opencypher", query);

    assertThat(result.hasNext()).isTrue();
    final long count = result.next().<Long>getProperty("count");
    assertThat(count).isLessThan(100L); // Some records filtered out
    result.close();

    assertThat(planOf(query)).doesNotContain(TYPE_COUNT_PUSH_DOWN);
  }

  @Test
  void optimizationNotAppliedWithMultipleReturnItems() {
    // Create test data
    database.transaction(() -> {
      database.getSchema().createVertexType("Company");
      for (int i = 0; i < 25; i++)
        database.newVertex("Company").save();
    });

    // This query should NOT use the optimization due to multiple return items
    final String query = "MATCH (c:Company) RETURN COUNT(c) as count, 'test' as label";
    final ResultSet result = database.query("opencypher", query);

    assertThat(result.hasNext()).isTrue();
    result.close();

    assertThat(planOf(query)).doesNotContain(TYPE_COUNT_PUSH_DOWN);
  }

  @Test
  void countWithPolymorphicTypes() {
    // Create type hierarchy
    database.transaction(() -> {
      database.getSchema().createVertexType("Animal");
      final VertexType dog = database.getSchema().createVertexType("Dog");
      dog.addSuperType("Animal");
      final VertexType cat = database.getSchema().createVertexType("Cat");
      cat.addSuperType("Animal");

      for (int i = 0; i < 30; i++)
        database.newVertex("Dog").save();
      for (int i = 0; i < 20; i++)
        database.newVertex("Cat").save();
    });

    // Count base type (should include subtypes)
    final String query = "MATCH (a:Animal) RETURN COUNT(a) as count";
    final ResultSet result = database.query("opencypher", query);

    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<Long>getProperty("count")).isEqualTo(50L);
    result.close();

    // The push-down has to count the whole hierarchy, so it must be the polymorphic counter it delegates to and
    // not a per-bucket one: the 50 above is what says it counted right, this is what says it counted at all.
    assertThat(planOf(query)).contains(TYPE_COUNT_PUSH_DOWN + "(Animal)");
  }

  @Test
  void countEdgesReturnOptimization() {
    // MATCH (p:Person)-[:KNOWS]->(friend) RETURN p.name AS name, count(friend) AS cnt ORDER BY cnt DESC LIMIT 2
    database.transaction(() -> {
      database.getSchema().createVertexType("Person").createProperty("name", String.class);
      database.getSchema().createEdgeType("KNOWS");

      final var alice = database.newVertex("Person").set("name", "Alice").save();
      final var bob = database.newVertex("Person").set("name", "Bob").save();
      final var charlie = database.newVertex("Person").set("name", "Charlie").save();

      alice.newEdge("KNOWS", bob, new Object[0]).save();
      alice.newEdge("KNOWS", charlie, new Object[0]).save();
      bob.newEdge("KNOWS", charlie, new Object[0]).save();
    });

    // Alice has 2 friends, Bob has 1. Charlie has 0 (no outgoing KNOWS), should not appear.
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (p:Person)-[:KNOWS]->(friend) RETURN p.name AS name, count(friend) AS cnt ORDER BY cnt DESC")) {
      assertThat(rs.hasNext()).isTrue();
      final Result r1 = rs.next();
      assertThat(r1.<String>getProperty("name")).isEqualTo("Alice");
      assertThat(r1.<Long>getProperty("cnt")).isEqualTo(2L);

      assertThat(rs.hasNext()).isTrue();
      final Result r2 = rs.next();
      assertThat(r2.<String>getProperty("name")).isEqualTo("Bob");
      assertThat(r2.<Long>getProperty("cnt")).isEqualTo(1L);

      assertThat(rs.hasNext()).isFalse();
    }

    // Verify the optimization is used via PROFILE
    try (final ResultSet rs = database.query("opencypher",
        "PROFILE MATCH (p:Person)-[:KNOWS]->(friend) RETURN p.name AS name, count(friend) AS cnt ORDER BY cnt DESC")) {
      while (rs.hasNext())
        rs.next();
      assertThat(rs.getExecutionPlan().isPresent()).isTrue();
      final String plan = rs.getExecutionPlan().get().prettyPrint(0, 2);
      assertThat(plan).contains("COUNT EDGES RETURN");
    }
  }

  @Test
  void countEdgesReturnFiltersByTheTargetLabel() {
    // The push-down was once declined when the counted node carried a label, because it could not filter by
    // target type. CountEdgesReturnStep takes a targetLabel and applies it, so the push-down is taken and what
    // it counts is the edges that reach that label. This used to assert the step was ABSENT from the plan, and
    // that held only because the plan text did not print the steps following an optimized MATCH (issue #6323).
    database.transaction(() -> {
      database.getSchema().createVertexType("Person").createProperty("name", String.class);
      database.getSchema().createVertexType("Company").createProperty("name", String.class);
      database.getSchema().createEdgeType("KNOWS");

      final var alice = database.newVertex("Person").set("name", "Alice").save();
      final var bob = database.newVertex("Person").set("name", "Bob").save();
      final var acme = database.newVertex("Company").set("name", "Acme").save();

      alice.newEdge("KNOWS", bob, new Object[0]).save();
      alice.newEdge("KNOWS", acme, new Object[0]).save(); // edge to Company, not Person
    });

    // Count only Person targets — should be 1 (Bob), not 2
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (p:Person)-[:KNOWS]->(friend:Person) RETURN p.name AS name, count(friend) AS cnt")) {
      assertThat(rs.hasNext()).isTrue();
      final Result r1 = rs.next();
      assertThat(r1.<String>getProperty("name")).isEqualTo("Alice");
      assertThat(r1.<Long>getProperty("cnt")).isEqualTo(1L);
      assertThat(rs.hasNext()).isFalse();
    }

    // The plan names the step AND the label it filters on, which is what makes the count above readable as
    // "edges to a Person" rather than "edges".
    try (final ResultSet rs = database.query("opencypher",
        "PROFILE MATCH (p:Person)-[:KNOWS]->(friend:Person) RETURN p.name AS name, count(friend) AS cnt")) {
      while (rs.hasNext())
        rs.next();
      assertThat(rs.getExecutionPlan().isPresent()).isTrue();
      final String plan = rs.getExecutionPlan().get().prettyPrint(0, 2);
      assertThat(plan).contains("COUNT EDGES RETURN").contains("[target: Person]");
    }
  }

  @Test
  void countEdgesReturnCorrectnessWithDuplicateNames() {
    // Two persons with the same name — GROUP BY should merge their counts
    database.transaction(() -> {
      database.getSchema().createVertexType("Person").createProperty("name", String.class);
      database.getSchema().createEdgeType("LIKES");

      final var alice1 = database.newVertex("Person").set("name", "Alice").save();
      final var alice2 = database.newVertex("Person").set("name", "Alice").save();
      final var bob = database.newVertex("Person").set("name", "Bob").save();
      final var charlie = database.newVertex("Person").set("name", "Charlie").save();

      alice1.newEdge("LIKES", bob, new Object[0]).save();       // Alice(1) -> Bob
      alice2.newEdge("LIKES", charlie, new Object[0]).save();   // Alice(2) -> Charlie
      alice2.newEdge("LIKES", bob, new Object[0]).save();       // Alice(2) -> Bob
    });

    // Two Alices: Alice(1) has 1 edge, Alice(2) has 2 edges. Grouped by name: Alice = 3
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (p:Person)-[:LIKES]->(x) RETURN p.name AS name, count(x) AS cnt ORDER BY cnt DESC")) {
      assertThat(rs.hasNext()).isTrue();
      final Result r1 = rs.next();
      assertThat(r1.<String>getProperty("name")).isEqualTo("Alice");
      assertThat(r1.<Long>getProperty("cnt")).isEqualTo(3L);
      assertThat(rs.hasNext()).isFalse();
    }

    // Verify optimization IS used
    try (final ResultSet rs = database.query("opencypher",
        "PROFILE MATCH (p:Person)-[:LIKES]->(x) RETURN p.name AS name, count(x) AS cnt ORDER BY cnt DESC")) {
      while (rs.hasNext())
        rs.next();
      final String plan = rs.getExecutionPlan().get().prettyPrint(0, 2);
      assertThat(plan).contains("COUNT EDGES RETURN");
    }
  }

  /**
   * The plan openCypher would run for {@code query}, obtained with {@code EXPLAIN} rather than {@code PROFILE}
   * because it must not execute: the negative cases here describe the full scan the push-down was refused for, and
   * running one to find out it ran is the cost the assertion exists to detect.
   */
  private String planOf(final String query) {
    try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + query)) {
      assertThat(rs.getExecutionPlan()).as("EXPLAIN returned no plan for: %s", query).isPresent();
      return rs.getExecutionPlan().get().prettyPrint(0, 2);
    }
  }
}
