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
package com.arcadedb.query.sql;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test CASE expressions in SQL queries (SELECT and MATCH).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class SQLCaseTest {
  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./target/databases/sqlcase").create();

    database.transaction(() -> {
      // Create schema
      database.getSchema().createVertexType("Person");
      database.getSchema().createVertexType("Product");

      // Create test data with different ages
      database.newVertex("Person").set("name", "Alice").set("age", 15).save();
      database.newVertex("Person").set("name", "Bob").set("age", 25).save();
      database.newVertex("Person").set("name", "Charlie").set("age", 45).save();
      database.newVertex("Person").set("name", "Dave").set("age", 70).save();

      // Create person with status
      database.newVertex("Person").set("name", "Eve").set("status", "active").save();
      database.newVertex("Person").set("name", "Frank").set("status", "inactive").save();

      // Create products with color codes (enum-like integer values)
      // Red=1, Blue=2, Green=3
      database.newVertex("Product").set("name", "Apple").set("color", 1).save();
      database.newVertex("Product").set("name", "Cherry").set("color", 1).save();
      database.newVertex("Product").set("name", "Blueberry").set("color", 2).save();
      database.newVertex("Product").set("name", "Grapes").set("color", 3).save();
    });
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  @Test
  void simpleCaseInSelectReturn() {
    // Simple CASE: CASE WHEN condition THEN result
    final ResultSet results = database.query("sql",
        "SELECT name, " +
            "CASE " +
            "  WHEN age < 18 THEN 'minor' " +
            "  WHEN age < 65 THEN 'adult' " +
            "  ELSE 'senior' " +
            "END as category " +
            "FROM Person WHERE age IS NOT NULL " +
            "ORDER BY name");

    int count = 0;
    while (results.hasNext()) {
      final Result result = results.next();
      final String name = result.getProperty("name");
      final String category = result.getProperty("category");

      switch (name) {
        case "Alice":
          assertThat(category).isEqualTo("minor");
          break;
        case "Bob":
        case "Charlie":
          assertThat(category).isEqualTo("adult");
          break;
        case "Dave":
          assertThat(category).isEqualTo("senior");
          break;
      }
      count++;
    }
    assertThat(count).isEqualTo(4);
  }

  @Test
  void simpleCaseWithoutElse() {
    // CASE without ELSE clause should return null for non-matching cases
    final ResultSet results = database.query("sql",
        "SELECT name, " +
            "CASE " +
            "  WHEN age < 10 THEN 'child' " +
            "  WHEN age < 13 THEN 'preteen' " +
            "END as category " +
            "FROM Person WHERE name = 'Alice'");

    assertThat(results.hasNext()).isTrue();
    final Result result = results.next();
    assertThat((Object) result.getProperty("category")).isNull(); // Alice is 15, doesn't match
  }

  @Test
  void extendedCaseInSelect() {
    // Extended CASE: CASE expression WHEN value THEN result
    final ResultSet results = database.query("sql",
        "SELECT name, " +
            "CASE status " +
            "  WHEN 'active' THEN 1 " +
            "  WHEN 'inactive' THEN 0 " +
            "  ELSE -1 " +
            "END as statusCode " +
            "FROM Person WHERE status IS NOT NULL " +
            "ORDER BY name");

    int count = 0;
    while (results.hasNext()) {
      final Result result = results.next();
      final String name = result.getProperty("name");
      final int statusCode = ((Number) result.getProperty("statusCode")).intValue();

      if (name.equals("Eve"))
        assertThat(statusCode).isEqualTo(1);
      else if (name.equals("Frank"))
        assertThat(statusCode).isEqualTo(0);

      count++;
    }
    assertThat(count).isEqualTo(2);
  }

  @Test
  void caseInWhereClause() {
    // Use CASE expression in WHERE clause
    // Note: People without age (Eve, Frank) will have age < 18 evaluate to NULL,
    // which is treated as false, so they go to ELSE 'adult' and are included
    final ResultSet results = database.query("sql",
        "SELECT name FROM Person " +
            "WHERE age IS NOT NULL AND CASE WHEN age < 18 THEN 'minor' ELSE 'adult' END = 'adult' " +
            "ORDER BY name");

    int count = 0;
    while (results.hasNext()) {
      final Result result = results.next();
      final String name = result.getProperty("name");
      // Should only return people with age >= 18
      assertThat(name).isIn("Bob", "Charlie", "Dave");
      count++;
    }
    assertThat(count).isEqualTo(3);
  }

  @Test
  void caseInMatchReturn() {
    // CASE in MATCH RETURN clause
    // First test CASE works in SELECT (not MATCH)
    final ResultSet selectResults = database.query("sql",
        "SELECT name, " +
            "CASE " +
            "  WHEN age < 18 THEN 'minor' " +
            "  WHEN age < 65 THEN 'adult' " +
            "  ELSE 'senior' " +
            "END as category " +
            "FROM Person WHERE age IS NOT NULL ORDER BY name");
    int selectCount = 0;
    while (selectResults.hasNext()) {
      final Result result = selectResults.next();
      final String name = result.getProperty("name");
      final String category = result.getProperty("category");
      assertThat(name).isNotNull();
      assertThat(category).isNotNull();
      selectCount++;
    }
    assertThat(selectCount).isEqualTo(4);

    // Now test MATCH without CASE to verify MATCH works
    final ResultSet simpleResults = database.query("sql",
        "MATCH {type: Person, as: p, where: (age IS NOT NULL)} RETURN p.name ORDER BY p.name");
    int simpleCount = 0;
    while (simpleResults.hasNext()) {
      simpleResults.next();
      simpleCount++;
    }
    assertThat(simpleCount).isEqualTo(4); // Alice, Bob, Charlie, Dave have ages

    // Test CASE in MATCH RETURN with proper alias alignment
    final ResultSet results = database.query("sql",
        "MATCH {type: Person, as: p, where: (age IS NOT NULL)} " +
            "RETURN p.name, " +
            "CASE " +
            "  WHEN p.age < 18 THEN 'minor' " +
            "  WHEN p.age < 65 THEN 'adult' " +
            "  ELSE 'senior' " +
            "END as category " +
            "ORDER BY p.name");

    int count = 0;
    while (results.hasNext()) {
      final Result result = results.next();
      final String name = result.getProperty("p.name");
      final String category = result.getProperty("category");

      switch (name) {
        case "Alice":
          assertThat(category).isEqualTo("minor");
          break;
        case "Bob":
        case "Charlie":
          assertThat(category).isEqualTo("adult");
          break;
        case "Dave":
          assertThat(category).isEqualTo("senior");
          break;
      }
      count++;
    }
    assertThat(count).isEqualTo(4);
  }

  @Test
  void caseInMatchWhere() {
    // This is the main use case from GitHub issue #3151
    // Use CASE in MATCH WHERE clause to convert enum values and filter with wildcards
    final ResultSet results = database.query("sql",
        "MATCH {type: Product, as: prod, " +
            "where: ((CASE WHEN color = 1 THEN 'red' WHEN color = 2 THEN 'blue' WHEN color = 3 THEN 'green' END) ILIKE '%ed%')} " +
            "RETURN prod.name ORDER BY prod.name");

    int count = 0;
    while (results.hasNext()) {
      final Result result = results.next();
      final String name = result.getProperty("prod.name");
      // Should match 'red' and 'green' (both contain 'e' followed by 'd')
      assertThat(name).isIn("Apple", "Cherry");
      count++;
    }
    assertThat(count).isEqualTo(2);
  }

  @Test
  void extendedCaseInMatchWhere() {
    // Extended CASE in MATCH WHERE
    final ResultSet results = database.query("sql",
        "MATCH {type: Product, as: prod, " +
            "where: (CASE color WHEN 1 THEN 'red' WHEN 2 THEN 'blue' ELSE 'other' END = 'red')} " +
            "RETURN prod.name ORDER BY prod.name");

    int count = 0;
    while (results.hasNext()) {
      final Result result = results.next();
      final String name = result.getProperty("prod.name");
      assertThat(name).isIn("Apple", "Cherry");
      count++;
    }
    assertThat(count).isEqualTo(2);
  }

  @Test
  void nestedCaseExpressions() {
    // Nested CASE expressions
    final ResultSet results = database.query("sql",
        "SELECT name, " +
            "CASE " +
            "  WHEN age < 18 THEN 'minor' " +
            "  ELSE CASE " +
            "    WHEN age < 30 THEN 'young adult' " +
            "    WHEN age < 65 THEN 'adult' " +
            "    ELSE 'senior' " +
            "  END " +
            "END as category " +
            "FROM Person WHERE name = 'Bob'");

    assertThat(results.hasNext()).isTrue();
    final Result result = results.next();
    assertThat((Object) result.getProperty("category")).isEqualTo("young adult"); // Bob is 25
  }

  @Test
  void caseWithNullHandling() {
    // Test CASE with null values
    database.transaction(() -> {
      database.newVertex("Person").set("name", "NoAge").save(); // No age property
    });

    final ResultSet results = database.query("sql",
        "SELECT name, " +
            "CASE " +
            "  WHEN age IS NULL THEN 'unknown' " +
            "  WHEN age < 18 THEN 'minor' " +
            "  ELSE 'adult' " +
            "END as category " +
            "FROM Person WHERE name = 'NoAge'");

    assertThat(results.hasNext()).isTrue();
    final Result result = results.next();
    assertThat((Object) result.getProperty("category")).isEqualTo("unknown");
  }
}
