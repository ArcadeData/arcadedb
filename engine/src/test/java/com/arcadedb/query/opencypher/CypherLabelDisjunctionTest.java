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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for label disjunction patterns in Cypher MATCH clauses.
 * Tests that (n:A|B) returns nodes with label A OR label B.
 *
 * Reproduces GitHub issue #4211 where MATCH (n:A|B) returned 0 rows even when
 * nodes with both labels existed.
 */
class CypherLabelDisjunctionTest {
  private Database database;

  @BeforeEach
  void setUp() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/cypher-label-disjunction");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.transaction(() -> {
      database.command("opencypher",
          "CREATE (:A {id: 1}), (:B {id: 2}), (:C {id: 3})");
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
  void twoLabelDisjunctionReturnsBothNodes() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (n:A|B) RETURN n.id AS id ORDER BY id");
    final List<Integer> ids = collectIds(rs);
    assertThat(ids).containsExactly(1, 2);
  }

  @Test
  void twoLabelDisjunctionCountIsTwo() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (n:A|B) RETURN count(*) AS cnt");
    assertThat(rs.hasNext()).isTrue();
    assertThat(((Number) rs.next().getProperty("cnt")).intValue()).isEqualTo(2);
  }

  @Test
  void twoLabelDisjunctionAcReturnsACNodes() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (n:A|C) RETURN n.id AS id ORDER BY id");
    final List<Integer> ids = collectIds(rs);
    assertThat(ids).containsExactly(1, 3);
  }

  @Test
  void twoLabelDisjunctionBCReturnsBCNodes() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (n:B|C) RETURN n.id AS id ORDER BY id");
    final List<Integer> ids = collectIds(rs);
    assertThat(ids).containsExactly(2, 3);
  }

  @Test
  void threeLabelDisjunctionReturnsAllThreeNodes() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (n:A|B|C) RETURN n.id AS id ORDER BY id");
    final List<Integer> ids = collectIds(rs);
    assertThat(ids).containsExactly(1, 2, 3);
  }

  @Test
  void threeLabelDisjunctionCountIsThree() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (n:A|B|C) RETURN count(*) AS cnt");
    assertThat(rs.hasNext()).isTrue();
    assertThat(((Number) rs.next().getProperty("cnt")).intValue()).isEqualTo(3);
  }

  @Test
  void singleLabelMatchStillWorks() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (n:A) RETURN count(*) AS cnt");
    assertThat(rs.hasNext()).isTrue();
    assertThat(((Number) rs.next().getProperty("cnt")).intValue()).isEqualTo(1);
  }

  @Test
  void labelDisjunctionWithPropertyFilter() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (n:A|B) WHERE n.id > 1 RETURN n.id AS id");
    final List<Integer> ids = collectIds(rs);
    assertThat(ids).containsExactly(2);
  }

  @Test
  void labelDisjunctionAnchorWithRelationshipFallsBackToLegacy() {
    // Anchor-side disjunction with a relationship — planner falls back to MatchNodeStep
    // (ExpandAll cannot represent OR semantics on the target side). Must still return rows.
    database.transaction(() -> {
      database.command("opencypher",
          "MATCH (a:A {id: 1}), (b:B {id: 2}), (c:C {id: 3}) "
              + "CREATE (a)-[:REL]->(c), (b)-[:REL]->(c)");
    });

    final ResultSet rs = database.query("opencypher",
        "MATCH (n:A|B)-[:REL]->(m) RETURN n.id AS id ORDER BY id");
    final List<Integer> ids = collectIds(rs);
    assertThat(ids).containsExactly(1, 2);
  }

  @Test
  void labelDisjunctionMatchesSubtypeInstances() {
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:Animal:Dog {id: 10})");
      database.command("opencypher", "CREATE (:Pet {id: 20})");
    });

    final ResultSet rs = database.query("opencypher",
        "MATCH (n:Animal|Pet) RETURN n.id AS id ORDER BY id");
    final List<Integer> ids = collectIds(rs);
    assertThat(ids).containsExactly(10, 20);
  }

  @Test
  void labelDisjunctionMatchesSchemaSubtype() {
    // Real schema inheritance: Dog extends Animal as a parent type, not via multi-label.
    // The scan must include Dog records when querying MATCH (n:Animal|X) — verifies the
    // type.instanceOf(label) path that walks the schema parent chain.
    database.transaction(() -> {
      database.getSchema().createVertexType("Animal");
      database.getSchema().getOrCreateVertexType("Dog").addSuperType("Animal");
      database.command("opencypher", "CREATE (:Dog {id: 100})");
      database.command("opencypher", "CREATE (:Pet {id: 200})");
    });

    final ResultSet rs = database.query("opencypher",
        "MATCH (n:Animal|Pet) RETURN n.id AS id ORDER BY id");
    final List<Integer> ids = collectIds(rs);
    assertThat(ids).containsExactly(100, 200);
  }

  private List<Integer> collectIds(final ResultSet rs) {
    final List<Integer> ids = new ArrayList<>();
    while (rs.hasNext()) {
      final Result r = rs.next();
      final Object val = r.getProperty("id");
      ids.add(((Number) val).intValue());
    }
    return ids;
  }

  /** See issue #3923 */
  @Nested
  class DynamicLabelRegression {
    private Database database;

    @BeforeEach
    void setUp() {
      final DatabaseFactory factory = new DatabaseFactory("./target/databases/testopencypher-dynamic-label-3923");
      if (factory.exists())
        factory.open().drop();
      database = factory.create();
    }

    @AfterEach
    void tearDown() {
      if (database != null) {
        database.drop();
        database = null;
      }
    }

    // Issue #3923: dynamic label expression resolved from a WITH-bound variable matches nodes by that label
    @Test
    void dynamicLabelMatchingFromWithBinding() {
      database.command("opencypher", "CREATE (n:DynamicLabelTest {name: 'test'})");

      final ResultSet rs = database.query("opencypher",
          "WITH 'DynamicLabelTest' AS label MATCH (n:$(label)) RETURN labels(n) AS result");
      assertThat(rs.hasNext()).isTrue();
      final Result r = rs.next();
      final List<String> labels = r.getProperty("result");
      assertThat(labels).containsExactly("DynamicLabelTest");
      assertThat(rs.hasNext()).isFalse();
    }

    // Issue #3923: dynamic label expression resolved from a query parameter matches nodes by that label
    @Test
    void dynamicLabelMatchingFromParameter() {
      database.command("opencypher", "CREATE (n:DynamicLabelTest {name: 'test'})");

      final ResultSet rs = database.query("opencypher",
          "MATCH (n:$($label)) RETURN labels(n) AS result",
          Map.of("label", "DynamicLabelTest"));
      assertThat(rs.hasNext()).isTrue();
      final Result r = rs.next();
      final List<String> labels = r.getProperty("result");
      assertThat(labels).containsExactly("DynamicLabelTest");
      assertThat(rs.hasNext()).isFalse();
    }

    // Issue #3923: dynamic label that does not exist in the schema yields no rows
    @Test
    void dynamicLabelMatchingNonExistingLabelReturnsNothing() {
      database.command("opencypher", "CREATE (n:DynamicLabelTest {name: 'test'})");

      final ResultSet rs = database.query("opencypher",
          "WITH 'DoesNotExist' AS label MATCH (n:$(label)) RETURN n");
      assertThat(rs.hasNext()).isFalse();
    }

    // Issue #3923: static label combined with dynamic label requires both labels to match
    @Test
    void dynamicLabelMatchingCombinedWithStaticLabel() {
      database.getSchema().createVertexType("A");
      database.getSchema().createVertexType("B").addSuperType("A");
      database.command("opencypher", "CREATE (n:B {name: 'ab'})");
      database.command("opencypher", "CREATE (n:A {name: 'aOnly'})");

      // Requires ALL labels (static + dynamic) to match: only the B node should match
      final ResultSet rs = database.query("opencypher",
          "WITH 'B' AS label MATCH (n:A:$(label)) RETURN n.name AS name");
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<String>getProperty("name")).isEqualTo("ab");
      assertThat(rs.hasNext()).isFalse();
    }
  }

  /** See issue #4105 */
  @Nested
  class LabelUnionRegression {
    private Database database;

    @BeforeEach
    void setUp() {
      final DatabaseFactory factory = new DatabaseFactory("./target/databases/issue-4105-label-union");
      if (factory.exists())
        factory.open().drop();
      database = factory.create();
      database.transaction(() -> database.command("opencypher",
          "CREATE (:Person {name:'Alice'}), (:Company {name:'TechCorp'})"));
    }

    @AfterEach
    void tearDown() {
      if (database != null) {
        database.drop();
        database = null;
      }
    }

    // Issue #4105: MATCH (n:A|B) matches nodes with either label
    @Test
    void labelUnionMatchesEither() {
      final ResultSet rs = database.query("opencypher",
          "MATCH (n:Person|Company) RETURN n.name AS name ORDER BY name");
      final List<String> names = new ArrayList<>();
      while (rs.hasNext()) {
        final Result r = rs.next();
        names.add(r.<String>getProperty("name"));
      }
      assertThat(names).containsExactly("Alice", "TechCorp");
    }

    // Issue #4105: equivalent WHERE n:A OR n:B form also matches both labels
    @Test
    void labelUnionWhereControlAlsoWorks() {
      final ResultSet rs = database.query("opencypher",
          "MATCH (n) WHERE n:Person OR n:Company RETURN n.name AS name ORDER BY name");
      final List<String> names = new ArrayList<>();
      while (rs.hasNext()) {
        final Result r = rs.next();
        names.add(r.<String>getProperty("name"));
      }
      assertThat(names).containsExactly("Alice", "TechCorp");
    }
  }
}
