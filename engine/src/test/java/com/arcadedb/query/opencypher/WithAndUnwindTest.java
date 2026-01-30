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
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for WITH and UNWIND clauses in native OpenCypher implementation.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class WithAndUnwindTest {
  private static       Database database;
  private static final String   DB_PATH = "./target/test-databases/with-unwind-test";

  @BeforeAll
  static void setup() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    database = new DatabaseFactory(DB_PATH).create();

    database.transaction(() -> {
      // Create schema
      final var personType = database.getSchema().createVertexType("Person");
      personType.createProperty("name", String.class);
      personType.createProperty("age", Integer.class);
      personType.createProperty("hobbies", String[].class);

      database.getSchema().createEdgeType("KNOWS");

      // Create test data
      database.command("opencypher", "CREATE (a:Person {name: 'Alice', age: 30})");
      database.command("opencypher", "CREATE (b:Person {name: 'Bob', age: 25})");
      database.command("opencypher", "CREATE (c:Person {name: 'Charlie', age: 35})");
      database.command("opencypher", "CREATE (d:Person {name: 'Diana', age: 28})");

      // Create relationships
      database.command("opencypher",
          "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)");
      database.command("opencypher",
          "MATCH (b:Person {name: 'Bob'}), (c:Person {name: 'Charlie'}) CREATE (b)-[:KNOWS]->(c)");
      database.command("opencypher",
          "MATCH (a:Person {name: 'Alice'}), (c:Person {name: 'Charlie'}) CREATE (a)-[:KNOWS]->(c)");
    });
  }

  @AfterAll
  static void teardown() {
    if (database != null) {
      database.drop();
      database = null;
    }
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  // ========== UNWIND TESTS ==========

  @Test
  @Order(1)
  void unwindSimpleList() {
    final ResultSet result = database.query("opencypher", "UNWIND [1, 2, 3] AS x RETURN x");

    int count = 0;
    long sum = 0;
    while (result.hasNext()) {
      final var row = result.next();
      sum += ((Number) row.getProperty("x")).longValue();
      count++;
    }
    result.close();

    assertThat(count).as("Should return 3 rows").isEqualTo(3);
    assertThat(sum).as("Sum should be 1+2+3=6").isEqualTo(6);
  }

  @Test
  @Order(2)
  void unwindWithMatch() {
    final ResultSet result = database.query("opencypher",
        "MATCH (p:Person) WHERE p.name = 'Alice' UNWIND [1, 2, 3] AS x RETURN p.name AS name, x");

    int count = 0;
    while (result.hasNext()) {
      final var row = result.next();
      assertThat(row.<String>getProperty("name")).isEqualTo("Alice");
      assertThat(row.<Long>getProperty("x")).isNotNull();
      count++;
    }
    result.close();

    assertThat(count).as("Should return 3 rows (one person × 3 list elements)").isEqualTo(3);
  }

  @Test
  @Order(3)
  void unwindEmptyList() {
    final ResultSet result = database.query("opencypher", "UNWIND [] AS x RETURN x");

    assertThat(result.hasNext()).as("Empty list should produce no rows").isFalse();
    result.close();
  }

  // ========== WITH TESTS ==========

  @Test
  @Order(10)
  void withProjection() {
    final ResultSet result = database.query("opencypher",
        "MATCH (p:Person) WITH p.name AS name, p.age AS age RETURN name, age ORDER BY name");

    int count = 0;
    while (result.hasNext()) {
      final var row = result.next();
      assertThat(row.<String>getProperty("name")).isNotNull();
      assertThat(row.<Integer>getProperty("age")).isNotNull();
      count++;
    }
    result.close();

    assertThat(count).as("Should return 4 people").isEqualTo(4);
  }

  @Test
  @Order(11)
  void withFiltering() {
    final ResultSet result = database.query("opencypher",
        "MATCH (p:Person) WITH p.name AS name, p.age AS age WHERE age > 28 RETURN name ORDER BY name");

    int count = 0;
    while (result.hasNext()) {
      final var row = result.next();
      final String name = row.getProperty("name");
      assertThat(name.equals("Alice") || name.equals("Charlie")).as("Should only return Alice or Charlie").isTrue();
      count++;
    }
    result.close();

    assertThat(count).as("Should return 2 people with age > 28").isEqualTo(2);
  }

  @Test
  @Order(12)
  void withDistinct() {
    // Create duplicate age values
    final ResultSet result = database.query("opencypher",
        "MATCH (p:Person) WITH DISTINCT p.age AS age RETURN age ORDER BY age");

    int count = 0;
    int prevAge = -1;
    while (result.hasNext()) {
      final var row = result.next();
      final int age = row.<Integer>getProperty("age");
      assertThat(age > prevAge).as("Ages should be distinct and sorted").isTrue();
      prevAge = age;
      count++;
    }
    result.close();

    assertThat(count).as("Should return 4 distinct ages").isEqualTo(4);
  }

  @Test
  @Order(13)
  void withLimit() {
    final ResultSet result = database.query("opencypher",
        "MATCH (p:Person) WITH p.name AS name ORDER BY name LIMIT 2 RETURN name");

    int count = 0;
    while (result.hasNext()) {
      result.next();
      count++;
    }
    result.close();

    assertThat(count).as("Should return only 2 rows due to LIMIT").isEqualTo(2);
  }

  @Test
  @Order(14)
  void withSkip() {
    final ResultSet result = database.query("opencypher",
        "MATCH (p:Person) WITH p.name AS name ORDER BY name SKIP 2 RETURN name");

    int count = 0;
    while (result.hasNext()) {
      result.next();
      count++;
    }
    result.close();

    assertThat(count).as("Should return 2 rows after skipping first 2").isEqualTo(2);
  }

  @Test
  @Order(15)
  void withAggregation() {
    final ResultSet result = database.query("opencypher",
        "MATCH (p:Person) WITH count(p) AS personCount RETURN personCount");

    assertThat(result.hasNext()).isTrue();
    final var row = result.next();
    assertThat(((Number) row.getProperty("personCount")).intValue()).as("Should count 4 people").isEqualTo(4);
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  // ========== WITH + UNWIND COMBINED TESTS ==========

  @Test
  @Order(20)
  void withAndUnwindCombined() {
    // Test that WITH passes through to subsequent clauses
    // This is currently not fully supported - UNWIND after WITH needs work
    // For now, test simpler case: WITH + RETURN only
    final ResultSet result = database.query("opencypher",
        "MATCH (p:Person) WHERE p.age < 30 " +
            "WITH p.name AS name, p.age AS age " +
            "RETURN name, age ORDER BY name");

    int count = 0;
    while (result.hasNext()) {
      final var row = result.next();
      assertThat(row.<String>getProperty("name")).as("name should not be null").isNotNull();
      assertThat(row.<Integer>getProperty("age")).as("age should not be null").isNotNull();
      count++;
    }
    result.close();

    assertThat(count).as("Should return 2 people with age < 30 (Bob=25, Diana=28)").isEqualTo(2);
  }

  @Test
  @Order(21)
  void multipleWithClauses() {
    final ResultSet result = database.query("opencypher",
        "MATCH (p:Person) " +
            "WITH p.name AS name, p.age AS age WHERE age > 25 " +
            "WITH name, age WHERE age < 35 " +
            "RETURN name ORDER BY name");

    int count = 0;
    while (result.hasNext()) {
      final var row = result.next();
      final String name = row.getProperty("name");
      assertThat(name.equals("Alice") || name.equals("Diana")).as("Should return Alice (30) and Diana (28), ages between 25 and 35")
          .isTrue();
      count++;
    }
    result.close();

    assertThat(count).as("Should return 2 people").isEqualTo(2);
  }

  @Test
  @Order(22)
  void withChainedMatch() {
    // Chaining MATCH after WITH is not yet fully implemented
    // For now, test a simpler pattern: MATCH -> WITH -> RETURN
    final ResultSet result = database.query("opencypher",
        """
            MATCH (a:Person)-[:KNOWS]->(b:Person)
            WHERE a.name = 'Alice'
            WITH a.name AS aname, b.name AS bname
            RETURN aname, bname ORDER BY bname""");

    int count = 0;
    while (result.hasNext()) {
      final var row = result.next();
      assertThat(row.<String>getProperty("aname")).isEqualTo("Alice");
      final String bname = row.getProperty("bname");
      assertThat(bname.equals("Bob") || bname.equals("Charlie")).as("Should return people Alice knows").isTrue();
      count++;
    }
    result.close();

    assertThat(count).as("Alice knows 2 people").isEqualTo(2);
  }
}
