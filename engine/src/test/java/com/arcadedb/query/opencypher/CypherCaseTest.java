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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test CASE expressions in Cypher queries.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherCaseTest {
  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./target/databases/cyphercase").create();

    database.transaction(() -> {
      // Create schema
      database.getSchema().createVertexType("Person");

      // Create test data with different ages
      database.newVertex("Person").set("name", "Alice").set("age", 15).save();
      database.newVertex("Person").set("name", "Bob").set("age", 25).save();
      database.newVertex("Person").set("name", "Charlie").set("age", 45).save();
      database.newVertex("Person").set("name", "Dave").set("age", 70).save();

      // Create person with status
      database.newVertex("Person").set("name", "Eve").set("status", "active").save();
      database.newVertex("Person").set("name", "Frank").set("status", "inactive").save();
    });
  }

  @AfterEach
  void teardown() {
    if (database != null) {
      database.drop();
    }
  }

  @Test
  void simpleCaseWithMultipleConditions() {
    // Simple CASE: CASE WHEN condition THEN result
    final ResultSet results = database.query("opencypher",
        """
        MATCH (p:Person) RETURN p.name, \
        CASE \
          WHEN p.age < 18 THEN 'minor' \
          WHEN p.age < 65 THEN 'adult' \
          ELSE 'senior' \
        END as category \
        ORDER BY p.name""");

    int count = 0;
    while (results.hasNext()) {
      final Result result = results.next();
      final String name = result.getProperty("p.name");
      final String category = (String) result.getProperty("category");

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
    assertThat((int) count).isGreaterThanOrEqualTo(4);
  }

  @Test
  void simpleCaseWithoutElse() {
    // CASE without ELSE clause should return null for non-matching cases
    final ResultSet results = database.query("opencypher",
        """
        MATCH (p:Person) WHERE p.name = 'Alice' RETURN p.name, \
        CASE \
          WHEN p.age < 10 THEN 'child' \
          WHEN p.age < 13 THEN 'preteen' \
        END as category""");

    assertThat((boolean) results.hasNext()).isTrue();
    final Result result = results.next();
    assertThat((Object) result.getProperty("category")).isNull(); // Alice is 15, doesn't match
  }

  @Test
  void extendedCaseExpression() {
    // Extended CASE: CASE expression WHEN value THEN result
    final ResultSet results = database.query("opencypher",
        """
        MATCH (p:Person) WHERE p.status IS NOT NULL RETURN p.name, \
        CASE p.status \
          WHEN 'active' THEN 1 \
          WHEN 'inactive' THEN 0 \
          ELSE -1 \
        END as statusCode \
        ORDER BY p.name""");

    int count = 0;
    while (results.hasNext()) {
      final Result result = results.next();
      final String name = result.getProperty("p.name");
      final Object statusCodeObj = result.getProperty("statusCode");
      final int statusCode = ((Number) statusCodeObj).intValue();

      if (name.equals("Eve")) {
        assertThat(statusCode).isEqualTo(1);
      } else if (name.equals("Frank")) {
        assertThat(statusCode).isEqualTo(0);
      }
      count++;
    }
    assertThat((int) count).isEqualTo(2);
  }

  @Test
  void caseInWhereClause() {
    // Use CASE expression in WHERE clause
    final ResultSet results = database.query("opencypher",
        """
        MATCH (p:Person) \
        WHERE CASE WHEN p.age < 18 THEN 'minor' ELSE 'adult' END = 'adult' \
        RETURN p.name ORDER BY p.name""");

    int count = 0;
    while (results.hasNext()) {
      final Result result = results.next();
      final String name = result.getProperty("p.name");
      // Should only return people with age >= 18
      assertThat(name).isIn("Bob", "Charlie", "Dave", "Eve", "Frank");
      count++;
    }
    assertThat((int) count).isGreaterThanOrEqualTo(3);
  }

  @Test
  void caseWithNullHandling() {
    // Test CASE with null values
    database.transaction(() -> {
      database.newVertex("Person").set("name", "NoAge").save(); // No age property
    });

    final ResultSet results = database.query("opencypher",
        """
        MATCH (p:Person) WHERE p.name = 'NoAge' RETURN p.name, \
        CASE \
          WHEN p.age IS NULL THEN 'unknown' \
          WHEN p.age < 18 THEN 'minor' \
          ELSE 'adult' \
        END as category""");

    assertThat((boolean) results.hasNext()).isTrue();
    final Result result = results.next();
    assertThat((Object) result.getProperty("category")).isEqualTo("unknown");
  }

  @Test
  void nestedCaseExpressions() {
    // Nested CASE expressions
    final ResultSet results = database.query("opencypher",
        """
        MATCH (p:Person) WHERE p.name = 'Bob' RETURN p.name, \
        CASE \
          WHEN p.age < 18 THEN 'minor' \
          ELSE CASE \
            WHEN p.age < 30 THEN 'young adult' \
            WHEN p.age < 65 THEN 'adult' \
            ELSE 'senior' \
          END \
        END as category""");

    assertThat((boolean) results.hasNext()).isTrue();
    final Result result = results.next();
    assertThat((Object) result.getProperty("category")).isEqualTo("young adult"); // Bob is 25
  }
}
