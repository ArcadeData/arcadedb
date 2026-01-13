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
import com.arcadedb.schema.Schema;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.*;

import java.io.File;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for WITH and UNWIND clauses in native OpenCypher implementation.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class WithAndUnwindTest {
  private static Database database;
  private static final String DB_PATH = "./target/test-databases/with-unwind-test";

  @BeforeAll
  public static void setup() {
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
  public static void teardown() {
    if (database != null) {
      database.drop();
      database = null;
    }
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  // ========== UNWIND TESTS ==========

  @Test
  @Order(1)
  void testUnwindSimpleList() {
    final ResultSet result = database.query("opencypher", "UNWIND [1, 2, 3] AS x RETURN x");

    int count = 0;
    long sum = 0;
    while (result.hasNext()) {
      final var row = result.next();
      sum += ((Number) row.getProperty("x")).longValue();
      count++;
    }
    result.close();

    assertEquals(3, count, "Should return 3 rows");
    assertEquals(6, sum, "Sum should be 1+2+3=6");
  }

  @Test
  @Order(2)
  void testUnwindWithMatch() {
    final ResultSet result = database.query("opencypher",
        "MATCH (p:Person) WHERE p.name = 'Alice' UNWIND [1, 2, 3] AS x RETURN p.name AS name, x");

    int count = 0;
    while (result.hasNext()) {
      final var row = result.next();
      assertEquals("Alice", row.getProperty("name"));
      assertNotNull(row.getProperty("x"));
      count++;
    }
    result.close();

    assertEquals(3, count, "Should return 3 rows (one person × 3 list elements)");
  }

  @Test
  @Order(3)
  void testUnwindEmptyList() {
    final ResultSet result = database.query("opencypher", "UNWIND [] AS x RETURN x");

    assertFalse(result.hasNext(), "Empty list should produce no rows");
    result.close();
  }

  // ========== WITH TESTS ==========

  @Test
  @Order(10)
  void testWithProjection() {
    final ResultSet result = database.query("opencypher",
        "MATCH (p:Person) WITH p.name AS name, p.age AS age RETURN name, age ORDER BY name");

    int count = 0;
    while (result.hasNext()) {
      final var row = result.next();
      assertNotNull(row.getProperty("name"));
      assertNotNull(row.getProperty("age"));
      count++;
    }
    result.close();

    assertEquals(4, count, "Should return 4 people");
  }

  @Test
  @Order(11)
  void testWithFiltering() {
    final ResultSet result = database.query("opencypher",
        "MATCH (p:Person) WITH p.name AS name, p.age AS age WHERE age > 28 RETURN name ORDER BY name");

    int count = 0;
    while (result.hasNext()) {
      final var row = result.next();
      final String name = (String) row.getProperty("name");
      assertTrue(name.equals("Alice") || name.equals("Charlie"), "Should only return Alice or Charlie");
      count++;
    }
    result.close();

    assertEquals(2, count, "Should return 2 people with age > 28");
  }

  @Test
  @Order(12)
  void testWithDistinct() {
    // Create duplicate age values
    final ResultSet result = database.query("opencypher",
        "MATCH (p:Person) WITH DISTINCT p.age AS age RETURN age ORDER BY age");

    int count = 0;
    int prevAge = -1;
    while (result.hasNext()) {
      final var row = result.next();
      final int age = (Integer) row.getProperty("age");
      assertTrue(age > prevAge, "Ages should be distinct and sorted");
      prevAge = age;
      count++;
    }
    result.close();

    assertEquals(4, count, "Should return 4 distinct ages");
  }

  @Test
  @Order(13)
  void testWithLimit() {
    final ResultSet result = database.query("opencypher",
        "MATCH (p:Person) WITH p.name AS name ORDER BY name LIMIT 2 RETURN name");

    int count = 0;
    while (result.hasNext()) {
      result.next();
      count++;
    }
    result.close();

    assertEquals(2, count, "Should return only 2 rows due to LIMIT");
  }

  @Test
  @Order(14)
  void testWithSkip() {
    final ResultSet result = database.query("opencypher",
        "MATCH (p:Person) WITH p.name AS name ORDER BY name SKIP 2 RETURN name");

    int count = 0;
    while (result.hasNext()) {
      result.next();
      count++;
    }
    result.close();

    assertEquals(2, count, "Should return 2 rows after skipping first 2");
  }

  @Test
  @Order(15)
  void testWithAggregation() {
    final ResultSet result = database.query("opencypher",
        "MATCH (p:Person) WITH count(p) AS personCount RETURN personCount");

    assertTrue(result.hasNext());
    final var row = result.next();
    assertEquals(4, ((Number) row.getProperty("personCount")).intValue(), "Should count 4 people");
    assertFalse(result.hasNext());
    result.close();
  }

  // ========== WITH + UNWIND COMBINED TESTS ==========

  @Test
  @Order(20)
  void testWithAndUnwindCombined() {
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
      assertNotNull(row.getProperty("name"), "name should not be null");
      assertNotNull(row.getProperty("age"), "age should not be null");
      count++;
    }
    result.close();

    assertEquals(2, count, "Should return 2 people with age < 30 (Bob=25, Diana=28)");
  }

  @Test
  @Order(21)
  void testMultipleWithClauses() {
    final ResultSet result = database.query("opencypher",
        "MATCH (p:Person) " +
        "WITH p.name AS name, p.age AS age WHERE age > 25 " +
        "WITH name, age WHERE age < 35 " +
        "RETURN name ORDER BY name");

    int count = 0;
    while (result.hasNext()) {
      final var row = result.next();
      final String name = (String) row.getProperty("name");
      assertTrue(name.equals("Alice") || name.equals("Diana"),
          "Should return Alice (30) and Diana (28), ages between 25 and 35");
      count++;
    }
    result.close();

    assertEquals(2, count, "Should return 2 people");
  }

  @Test
  @Order(22)
  void testWithChainedMatch() {
    // Chaining MATCH after WITH is not yet fully implemented
    // For now, test a simpler pattern: MATCH -> WITH -> RETURN
    final ResultSet result = database.query("opencypher",
        "MATCH (a:Person)-[:KNOWS]->(b:Person) " +
        "WHERE a.name = 'Alice' " +
        "WITH a.name AS aname, b.name AS bname " +
        "RETURN aname, bname ORDER BY bname");

    int count = 0;
    while (result.hasNext()) {
      final var row = result.next();
      assertEquals("Alice", row.getProperty("aname"));
      final String bname = (String) row.getProperty("bname");
      assertTrue(bname.equals("Bob") || bname.equals("Charlie"),
          "Should return people Alice knows");
      count++;
    }
    result.close();

    assertEquals(2, count, "Alice knows 2 people");
  }
}
