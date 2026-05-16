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
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test EXISTS() expression in Cypher queries.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherExistsTest {
  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/cypherexists");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();

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
  void teardown() {
    if (database != null) {
      database.drop();
    }
  }

  @Test
  void existsWithSimplePattern() {
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
  void existsWithNonMatchingPattern() {
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
  void existsInReturn() {
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
  void existsWithMatchInSubquery() {
    // EXISTS with full MATCH clause
    final ResultSet results = database.query("opencypher",
        """
        MATCH (p:Person) WHERE EXISTS { MATCH (p)-[:WORKS_AT]->(c:Company) WHERE c.name = 'Acme Corp' } RETURN p\
        .name ORDER BY p.name""");

    int count = 0;
    while (results.hasNext()) {
      final Result result = results.next();
      final String name = result.getProperty("p.name");
      assertThat(name).isIn("Alice", "Bob");
      count++;
    }
    assertThat(count).isEqualTo(2);
  }

  /** See issue #4097 */
  @Nested
  class ExistsOuterVarRegression {
    private Database database;

    @BeforeEach
    void setUp() {
      final DatabaseFactory factory = new DatabaseFactory("./target/databases/issue-4097-exists-outer-var");
      if (factory.exists())
        factory.open().drop();
      database = factory.create();
      database.transaction(() -> database.command("opencypher",
          "CREATE (:Node {id:1, name:'test'}), (:Node {id:2, name:'other'})"));
    }

    @AfterEach
    void tearDown() {
      if (database != null) {
        database.drop();
        database = null;
      }
    }

    // Issue #4097: EXISTS subquery returning an outer variable evaluates to true
    @Test
    void existsWithOuterVarReturnsTrue() {
      final ResultSet rs = database.query("opencypher",
          "MATCH (n) RETURN exists { RETURN n AS x } AS result ORDER BY result LIMIT 1");
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Boolean>getProperty("result")).isTrue();
    }

    // Issue #4097: EXISTS subquery returning a property of an outer variable evaluates to true
    @Test
    void existsWithOuterVarPropertyReturnsTrue() {
      final ResultSet rs = database.query("opencypher",
          "MATCH (n) RETURN exists { RETURN n.id AS x } AS result ORDER BY result LIMIT 1");
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Boolean>getProperty("result")).isTrue();
    }

    // Issue #4097: EXISTS subquery returning a literal evaluates to true
    @Test
    void existsWithConstantReturnsTrue() {
      final ResultSet rs = database.query("opencypher",
          "MATCH (n) RETURN exists { RETURN 1 AS x } AS result ORDER BY result LIMIT 1");
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Boolean>getProperty("result")).isTrue();
    }

    // Issue #4097: EXISTS subquery with an inner MATCH that produces rows evaluates to true
    @Test
    void existsWithInnerMatchReturnsTrue() {
      final ResultSet rs = database.query("opencypher",
          "MATCH (n) RETURN exists { MATCH (m:Node) RETURN m AS x } AS result ORDER BY result LIMIT 1");
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Boolean>getProperty("result")).isTrue();
    }

    // Issue #4097: EXISTS subquery with count(*) aggregation evaluates to true
    @Test
    void existsWithCountStarReturnsTrue() {
      final ResultSet rs = database.query("opencypher",
          "MATCH (n) RETURN exists { RETURN count(*) AS c } AS result ORDER BY result LIMIT 1");
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Boolean>getProperty("result")).isTrue();
    }
  }

  /** See issue #4126 */
  @Nested
  class ExistsAndNotExistsRegression {
    private Database database;

    @BeforeEach
    void setUp() {
      final DatabaseFactory factory = new DatabaseFactory("./target/databases/issue-4126-exists-and-not-exists");
      if (factory.exists())
        factory.open().drop();
      database = factory.create();
      database.transaction(() -> {
        database.command("sql", "CREATE VERTEX TYPE Owner");
        database.command("sql", "CREATE PROPERTY Owner.id STRING");
        database.command("sql", "CREATE VERTEX TYPE Item");
        database.command("sql", "CREATE PROPERTY Item.id STRING");
        database.command("sql", "CREATE PROPERTY Item.flag BOOLEAN");
        database.command("sql", "CREATE EDGE TYPE rel");
        database.command("sql", "CREATE EDGE TYPE excl");

        database.command("opencypher", "CREATE (:Owner {id: 'u'})");
        database.command("opencypher", "CREATE (:Item  {id: 'c', flag: true})");
      });
    }

    @AfterEach
    void tearDown() {
      if (database != null) {
        database.drop();
        database = null;
      }
    }

    // Issue #4126: control - two-MATCH cross product plus boolean predicate returns the row
    @Test
    void twoMatchBooleanOnly() {
      final List<Result> rows = collect(database.query("opencypher",
          "MATCH (u:Owner {id: 'u'}) MATCH (c:Item) WHERE c.flag = true RETURN c.id AS id"));
      assertThat(rows).hasSize(1);
      assertThat((String) rows.get(0).getProperty("id")).isEqualTo("c");
    }

    // Issue #4126: control - two-MATCH plus boolean OR EXISTS returns the row
    @Test
    void twoMatchBooleanOrExists() {
      final List<Result> rows = collect(database.query("opencypher",
          "MATCH (u:Owner {id: 'u'}) MATCH (c:Item) " +
              "WHERE c.flag = true OR EXISTS { MATCH (u)-[:rel]->(c) } " +
              "RETURN c.id AS id"));
      assertThat(rows).hasSize(1);
      assertThat((String) rows.get(0).getProperty("id")).isEqualTo("c");
    }

    // Issue #4126: two-MATCH plus (bool OR EXISTS) AND NOT EXISTS returns the row when no excluded edges exist
    @Test
    void twoMatchBooleanOrExistsAndNotExists() {
      final List<Result> rows = collect(database.query("opencypher",
          "MATCH (u:Owner {id: 'u'}) MATCH (c:Item) " +
              "WHERE (c.flag = true OR EXISTS { MATCH (u)-[:rel]->(c) }) " +
              "  AND NOT EXISTS { MATCH (u)-[:excl]->(c) } " +
              "RETURN c.id AS id"));
      assertThat(rows).hasSize(1);
      assertThat((String) rows.get(0).getProperty("id")).isEqualTo("c");
    }

    // Issue #4126: two-MATCH plus grouped exclusion AND NOT (EXISTS OR EXISTS) returns the row
    @Test
    void twoMatchBooleanOrExistsAndNotGroupedExclusion() {
      final List<Result> rows = collect(database.query("opencypher",
          "MATCH (u:Owner {id: 'u'}) MATCH (c:Item) " +
              "WHERE (c.flag = true OR EXISTS { MATCH (u)-[:rel]->(c) }) " +
              "  AND NOT (EXISTS { MATCH (u)-[:excl]->(c) } " +
              "        OR EXISTS { MATCH (u)-[:excl*1..3]->(c) }) " +
              "RETURN c.id AS id"));
      assertThat(rows).hasSize(1);
      assertThat((String) rows.get(0).getProperty("id")).isEqualTo("c");
    }

    // Issue #4126: same WHERE shape wrapped in a single outer MATCH with EXISTS body returns the row
    @Test
    void singleMatchWithExistsWrappingMatchAndWhere() {
      final List<Result> rows = collect(database.query("opencypher",
          "MATCH (c:Item) " +
              "WHERE EXISTS { " +
              "  MATCH (u:Owner {id: 'u'}) " +
              "  WHERE (c.flag = true OR EXISTS { MATCH (u)-[:rel]->(c) }) " +
              "    AND NOT (EXISTS { MATCH (u)-[:excl]->(c) } " +
              "          OR EXISTS { MATCH (u)-[:excl*1..3]->(c) }) " +
              "} " +
              "RETURN c.id AS id"));
      assertThat(rows).hasSize(1);
      assertThat((String) rows.get(0).getProperty("id")).isEqualTo("c");
    }

    private List<Result> collect(final ResultSet rs) {
      final List<Result> out = new ArrayList<>();
      while (rs.hasNext())
        out.add(rs.next());
      return out;
    }
  }
}
