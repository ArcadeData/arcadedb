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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

;

/**
 * Tests for WHERE clause with logical operators, NULL checks, and regex matching.
 * Tests AND, OR, NOT operators, IS NULL/IS NOT NULL, and regex (=~) operator.
 */
public class OpenCypherWhereClauseTest {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/testopencypherWhere").create();
    database.getSchema().createVertexType("Person");

    // Create test data
    database.transaction(() -> {
      database.command("opencypher", "CREATE (p:Person {name: 'Alice', age: 30, email: 'alice@example.com'})");
      database.command("opencypher", "CREATE (p:Person {name: 'Bob', age: 25, email: 'bob@test.com'})");
      database.command("opencypher", "CREATE (p:Person {name: 'Charlie', age: 35})"); // No email
      database.command("opencypher", "CREATE (p:Person {name: 'David', age: 40, email: 'david@example.com'})");
      database.command("opencypher", "CREATE (p:Person {name: 'Eve', age: 28})"); // No email
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
  void testWhereWithAND() {
    // Test AND operator: age > 25 AND age < 35
    final ResultSet result = database.query("opencypher",
        "MATCH (p:Person) WHERE p.age > 25 AND p.age < 35 RETURN p.name ORDER BY p.name");

    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<String>getProperty("p.name")).isEqualTo("Alice");
    assertThat(result.next().<String>getProperty("p.name")).isEqualTo("Eve");
    assertThat(result.hasNext()).isFalse();
  }

  @Test
  void testWhereWithOR() {
    // Test OR operator: age < 26 OR age > 35
    final ResultSet result = database.query("opencypher",
        "MATCH (p:Person) WHERE p.age < 26 OR p.age > 35 RETURN p.name ORDER BY p.name");

    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<String>getProperty("p.name")).isEqualTo("Bob");
    assertThat(result.next().<String>getProperty("p.name")).isEqualTo("David");
    assertThat(result.hasNext()).isFalse();
  }

  @Test
  void testWhereWithNOT() {
    // Test NOT operator: NOT(age > 30)
    final ResultSet result = database.query("opencypher",
        "MATCH (p:Person) WHERE NOT p.age > 30 RETURN p.name ORDER BY p.name");

    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<String>getProperty("p.name")).isEqualTo("Alice");
    assertThat(result.next().<String>getProperty("p.name")).isEqualTo("Bob");
    assertThat(result.next().<String>getProperty("p.name")).isEqualTo("Eve");
    assertThat(result.hasNext()).isFalse();
  }

  @Test
  void testWhereWithComplexLogic() {
    // Test combined AND with NOT: age > 35 AND NOT name = 'David'
    // Note: Parenthesized OR expressions need additional parser work
    final ResultSet result = database.query("opencypher",
        "MATCH (p:Person) WHERE p.age > 35 AND NOT p.name = 'David' RETURN p.name ORDER BY p.name");

    assertThat(result.hasNext()).isFalse(); // No one matches: only David has age > 35
  }

  @Test
  void testWhereISNULL() {
    // Test IS NULL: email IS NULL
    final ResultSet result = database.query("opencypher",
        "MATCH (p:Person) WHERE p.email IS NULL RETURN p.name ORDER BY p.name");

    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<String>getProperty("p.name")).isEqualTo("Charlie");
    assertThat(result.next().<String>getProperty("p.name")).isEqualTo("Eve");
    assertThat(result.hasNext()).isFalse();
  }

  @Test
  void testWhereISNOTNULL() {
    // Test IS NOT NULL: email IS NOT NULL
    final ResultSet result = database.query("opencypher",
        "MATCH (p:Person) WHERE p.email IS NOT NULL RETURN p.name ORDER BY p.name");

    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<String>getProperty("p.name")).isEqualTo("Alice");
    assertThat(result.next().<String>getProperty("p.name")).isEqualTo("Bob");
    assertThat(result.next().<String>getProperty("p.name")).isEqualTo("David");
    assertThat(result.hasNext()).isFalse();
  }

  @Test
  void testWhereWithRegex() {
    // Test regex matching: name =~ 'Alice'
    // Start with exact match first
    final ResultSet result = database.query("opencypher",
        "MATCH (p:Person) WHERE p.name =~ 'Alice' RETURN p.name ORDER BY p.name");

    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<String>getProperty("p.name")).isEqualTo("Alice");
    assertThat(result.hasNext()).isFalse();
  }

  @Test
  void testWhereWithRegexPattern() {
    // Test regex with pattern: name =~ 'A.*'
    final ResultSet result = database.query("opencypher",
        "MATCH (p:Person) WHERE p.name =~ 'A.*' RETURN p.name ORDER BY p.name");

    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<String>getProperty("p.name")).isEqualTo("Alice");
    assertThat(result.hasNext()).isFalse();
  }

  @Test
  void testWhereWithRegexAndLogic() {
    // Test regex with AND: name =~ 'A.*' AND age = 30
    final ResultSet result = database.query("opencypher",
        "MATCH (p:Person) WHERE p.name =~ 'A.*' AND p.age = 30 RETURN p.name ORDER BY p.name");

    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<String>getProperty("p.name")).isEqualTo("Alice");
    assertThat(result.hasNext()).isFalse();
  }

  @Test
  void testWhereWithIN() {
    // Test IN operator: name IN ['Alice', 'Bob']
    final ResultSet result = database.query("opencypher",
        "MATCH (p:Person) WHERE p.name IN ['Alice', 'Bob'] RETURN p.name ORDER BY p.name");

    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<String>getProperty("p.name")).isEqualTo("Alice");
    assertThat(result.next().<String>getProperty("p.name")).isEqualTo("Bob");
    assertThat(result.hasNext()).isFalse();
  }

  @Test
  void testWhereWithINNumbers() {
    // Test IN operator with numbers: age IN [25, 30, 35]
    final ResultSet result = database.query("opencypher",
        "MATCH (p:Person) WHERE p.age IN [25, 30, 35] RETURN p.name ORDER BY p.name");

    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<String>getProperty("p.name")).isEqualTo("Alice");
    assertThat(result.next().<String>getProperty("p.name")).isEqualTo("Bob");
    assertThat(result.next().<String>getProperty("p.name")).isEqualTo("Charlie");
    assertThat(result.hasNext()).isFalse();
  }

  @Test
  void testWhereWithINAndLogic() {
    // Test IN with AND: name IN ['Alice', 'Bob', 'Charlie'] AND age > 28
    final ResultSet result = database.query("opencypher",
        "MATCH (p:Person) WHERE p.name IN ['Alice', 'Bob', 'Charlie'] AND p.age > 28 RETURN p.name ORDER BY p.name");

    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<String>getProperty("p.name")).isEqualTo("Alice");
    assertThat(result.next().<String>getProperty("p.name")).isEqualTo("Charlie");
    assertThat(result.hasNext()).isFalse();
  }

  @Test
  void testWhereWithNullCheckAndComparison() {
    // Test IS NOT NULL with comparison: email IS NOT NULL AND age >= 30
    final ResultSet result = database.query("opencypher",
        "MATCH (p:Person) WHERE p.email IS NOT NULL AND p.age >= 30 RETURN p.name ORDER BY p.name");

    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<String>getProperty("p.name")).isEqualTo("Alice");
    assertThat(result.next().<String>getProperty("p.name")).isEqualTo("David");
    assertThat(result.hasNext()).isFalse();
  }

  @Test
  void testWhereAllComparisonOperators() {
    // Test all comparison operators
    ResultSet result;

    // Equals
    result = database.query("opencypher", "MATCH (p:Person) WHERE p.age = 30 RETURN p.name");
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<String>getProperty("p.name")).isEqualTo("Alice");
    assertThat(result.hasNext()).isFalse();

    // Not equals
    result = database.query("opencypher", "MATCH (p:Person) WHERE p.age != 30 RETURN count(p) as cnt");
    assertThat(result.next().<Long>getProperty("cnt")).isEqualTo(4L);

    // Less than
    result = database.query("opencypher", "MATCH (p:Person) WHERE p.age < 30 RETURN count(p) as cnt");
    assertThat(result.next().<Long>getProperty("cnt")).isEqualTo(2L);

    // Greater than
    result = database.query("opencypher", "MATCH (p:Person) WHERE p.age > 30 RETURN count(p) as cnt");
    assertThat(result.next().<Long>getProperty("cnt")).isEqualTo(2L);

    // Less than or equal
    result = database.query("opencypher", "MATCH (p:Person) WHERE p.age <= 30 RETURN count(p) as cnt");
    assertThat(result.next().<Long>getProperty("cnt")).isEqualTo(3L);

    // Greater than or equal
    result = database.query("opencypher", "MATCH (p:Person) WHERE p.age >= 30 RETURN count(p) as cnt");
    assertThat(result.next().<Long>getProperty("cnt")).isEqualTo(3L);
  }

  @Test
  void testWhereComplexConditions() {
    // Test multiple AND conditions: age > 25 AND age < 35 AND email IS NOT NULL
    final ResultSet result = database.query("opencypher",
        "MATCH (p:Person) WHERE p.age > 25 AND p.age < 35 AND p.email IS NOT NULL RETURN p.name ORDER BY p.name");

    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<String>getProperty("p.name")).isEqualTo("Alice");
    assertThat(result.hasNext()).isFalse();
  }

  @Test
  void testWhereStartsWith() {
    // Test STARTS WITH operator
    final ResultSet result = database.query("opencypher",
        "MATCH (p:Person) WHERE p.name STARTS WITH 'A' RETURN p.name ORDER BY p.name");

    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<String>getProperty("p.name")).isEqualTo("Alice");
    assertThat(result.hasNext()).isFalse();
  }

  @Test
  void testWhereEndsWith() {
    // Test ENDS WITH operator
    final ResultSet result = database.query("opencypher",
        "MATCH (p:Person) WHERE p.name ENDS WITH 'e' RETURN p.name ORDER BY p.name");

    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<String>getProperty("p.name")).isEqualTo("Alice");
    assertThat(result.next().<String>getProperty("p.name")).isEqualTo("Charlie");
    assertThat(result.next().<String>getProperty("p.name")).isEqualTo("Eve");
    assertThat(result.hasNext()).isFalse();
  }

  @Test
  void testWhereContains() {
    // Test CONTAINS operator
    final ResultSet result = database.query("opencypher",
        "MATCH (p:Person) WHERE p.name CONTAINS 'li' RETURN p.name ORDER BY p.name");

    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<String>getProperty("p.name")).isEqualTo("Alice");
    assertThat(result.next().<String>getProperty("p.name")).isEqualTo("Charlie");
    assertThat(result.hasNext()).isFalse();
  }

  @Test
  void testWhereStartsWithEmailDomain() {
    // Test STARTS WITH on email property
    final ResultSet result = database.query("opencypher",
        "MATCH (p:Person) WHERE p.email STARTS WITH 'alice' RETURN p.name");

    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<String>getProperty("p.name")).isEqualTo("Alice");
    assertThat(result.hasNext()).isFalse();
  }

  @Test
  void testWhereEndsWithEmailDomain() {
    // Test ENDS WITH on email domain
    final ResultSet result = database.query("opencypher",
        "MATCH (p:Person) WHERE p.email ENDS WITH '@example.com' RETURN p.name ORDER BY p.name");

    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<String>getProperty("p.name")).isEqualTo("Alice");
    assertThat(result.next().<String>getProperty("p.name")).isEqualTo("David");
    assertThat(result.hasNext()).isFalse();
  }

  @Test
  void testWhereStringMatchWithAND() {
    // Test string matching combined with AND
    final ResultSet result = database.query("opencypher",
        "MATCH (p:Person) WHERE p.name STARTS WITH 'A' AND p.age > 28 RETURN p.name");

    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<String>getProperty("p.name")).isEqualTo("Alice");
    assertThat(result.hasNext()).isFalse();
  }

  @Test
  void testWhereParenthesizedOR() {
    // Test parenthesized OR with AND (operator precedence)
    // (age < 26 OR age > 35) AND email IS NOT NULL
    final ResultSet result = database.query("opencypher",
        "MATCH (p:Person) WHERE (p.age < 26 OR p.age > 35) AND p.email IS NOT NULL RETURN p.name ORDER BY p.name");

    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<String>getProperty("p.name")).isEqualTo("Bob");
    assertThat(result.next().<String>getProperty("p.name")).isEqualTo("David");
    assertThat(result.hasNext()).isFalse();
  }

  @Test
  void testWhereComplexParenthesizedExpressions() {
    // Test nested parentheses with multiple OR and AND
    // ((age < 28 OR age > 35) AND email IS NOT NULL) OR (name CONTAINS 'li' AND age = 35)
    final ResultSet result = database.query("opencypher",
        "MATCH (p:Person) WHERE ((p.age < 28 OR p.age > 35) AND p.email IS NOT NULL) OR (p.name CONTAINS 'li' AND p.age = 35) RETURN p.name ORDER BY p.name");

    // Should match:
    // - Bob (age 25 < 28, has email)
    // - David (age 40 > 35, has email)
    // - Charlie (name contains 'li', age 35, but no email - still matches)
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<String>getProperty("p.name")).isEqualTo("Bob");
    assertThat(result.next().<String>getProperty("p.name")).isEqualTo("Charlie");
    assertThat(result.next().<String>getProperty("p.name")).isEqualTo("David");
    assertThat(result.hasNext()).isFalse();
  }
}
