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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression matrix for issue #4141: ISO/IEC 39075 (GQL) three-valued (trinary) logic.
 * <p>
 * Validates that the {@code UNKNOWN} truth value (represented as {@code null} in a boolean context)
 * propagates correctly through {@code AND}/{@code OR}/{@code NOT}/{@code XOR}, comparisons against
 * {@code null}, and {@code IN} with {@code null} elements, and that {@code WHERE} treats
 * {@code UNKNOWN} as a non-match while {@code RETURN} preserves it as {@code null}.
 * <p>
 * GQL/Cypher truth tables under test:
 * <pre>
 * AND:  true AND null = null    false AND null = false   null AND null = null
 * OR:   true OR  null = true    false OR  null = null    null OR  null = null
 * NOT:  NOT null = null
 * XOR:  bool XOR null = null
 * </pre>
 */
class Issue4141TrinaryLogicTest {
  private Database database;

  @BeforeEach
  void setUp() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/testIssue4141TrinaryLogic");
    if (factory.exists())
      factory.open().drop(); // defend against a leftover db from a previously interrupted run
    database = factory.create();
    database.getSchema().createVertexType("Person");

    database.transaction(() -> {
      // Alice has an email; Charlie and Eve do not (email property is missing -> null).
      database.command("opencypher", "CREATE (p:Person {name: 'Alice', age: 30, email: 'alice@example.com'})");
      database.command("opencypher", "CREATE (p:Person {name: 'Charlie', age: 35})");
      database.command("opencypher", "CREATE (p:Person {name: 'Eve', age: 28})");
    });
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  // ----------------------------------------------------------------------------------------------
  // WHERE context: UNKNOWN must behave as a non-match (coerced to false at the top level).
  // ----------------------------------------------------------------------------------------------

  @Test
  void whereComparisonAgainstNullIsNonMatch() {
    // email = 'x' is UNKNOWN for Charlie/Eve (null email) -> excluded; false for Alice -> excluded.
    assertThat(matchedNames("MATCH (p:Person) WHERE p.email = 'nobody@example.com' RETURN p.name AS name")).isEmpty();
  }

  @Test
  void whereNotOfUnknownIsNonMatch() {
    // NOT (null) = null -> Charlie/Eve excluded. Alice: NOT(false) = true -> included.
    assertThat(matchedNames("MATCH (p:Person) WHERE NOT (p.email = 'nobody@example.com') RETURN p.name AS name"))
        .containsExactly("Alice");
  }

  @Test
  void whereTrueOrUnknownIsMatch() {
    // age > 30: Charlie true, Eve false, Alice false.
    // (age>30) OR (email=...): Charlie = true OR null = true (match); Eve = false OR null = null (no match);
    // Alice = false OR false = false (no match).
    assertThat(matchedNames(
        "MATCH (p:Person) WHERE p.age > 30 OR p.email = 'nobody@example.com' RETURN p.name AS name"))
        .containsExactly("Charlie");
  }

  @Test
  void whereTrueAndUnknownIsNonMatch() {
    // age < 40 is true for all; AND (email=...) is null for Charlie/Eve, false for Alice -> none match.
    assertThat(matchedNames(
        "MATCH (p:Person) WHERE p.age < 40 AND p.email = 'nobody@example.com' RETURN p.name AS name")).isEmpty();
  }

  // ----------------------------------------------------------------------------------------------
  // RETURN context: UNKNOWN must be preserved as null, distinct from false.
  // ----------------------------------------------------------------------------------------------

  @Test
  void returnFalseAndUnknownIsFalse() {
    // Charlie: (age > 100)=false  AND (email=...)=null  ->  false (false dominates).
    assertThat(scalarForCharlie("RETURN (p.age > 100) AND (p.email = 'x') AS v")).isEqualTo(false);
  }

  @Test
  void returnTrueAndUnknownIsNull() {
    // Charlie: (age > 0)=true  AND (email=...)=null  ->  null.
    assertThat(scalarForCharlie("RETURN (p.age > 0) AND (p.email = 'x') AS v")).isNull();
  }

  @Test
  void returnTrueOrUnknownIsTrue() {
    // Charlie: (age > 0)=true  OR (email=...)=null  ->  true (true dominates).
    assertThat(scalarForCharlie("RETURN (p.age > 0) OR (p.email = 'x') AS v")).isEqualTo(true);
  }

  @Test
  void returnFalseOrUnknownIsNull() {
    // Charlie: (age > 100)=false  OR (email=...)=null  ->  null.
    assertThat(scalarForCharlie("RETURN (p.age > 100) OR (p.email = 'x') AS v")).isNull();
  }

  @Test
  void returnNotUnknownIsNull() {
    // Charlie: NOT (email=...) = NOT null = null.
    assertThat(scalarForCharlie("RETURN NOT (p.email = 'x') AS v")).isNull();
  }

  @Test
  void returnXorWithUnknownIsNull() {
    // Charlie: (age > 0)=true  XOR (email=...)=null  ->  null.
    assertThat(scalarForCharlie("RETURN (p.age > 0) XOR (p.email = 'x') AS v")).isNull();
  }

  @Test
  void returnComparisonAgainstNullIsNull() {
    // Charlie has no email: email = 'x' -> null, email <> 'x' -> null.
    assertThat(scalarForCharlie("RETURN p.email = 'x' AS v")).isNull();
    assertThat(scalarForCharlie("RETURN p.email <> 'x' AS v")).isNull();
  }

  // ----------------------------------------------------------------------------------------------
  // IS NULL / IS NOT NULL must yield a concrete boolean, never UNKNOWN.
  // ----------------------------------------------------------------------------------------------

  @Test
  void isNullYieldsConcreteBoolean() {
    assertThat(scalarForCharlie("RETURN p.email IS NULL AS v")).isEqualTo(true);
    assertThat(scalarForCharlie("RETURN p.email IS NOT NULL AS v")).isEqualTo(false);
    assertThat(scalarForCharlie("RETURN p.age IS NULL AS v")).isEqualTo(false);
  }

  // ----------------------------------------------------------------------------------------------
  // IN with a null element follows GQL 3VL.
  // ----------------------------------------------------------------------------------------------

  @Test
  void inListWithNullElement() {
    // present element -> true even with a null in the list.
    assertThat(scalar("RETURN 1 IN [1, 2, null] AS v")).isEqualTo(true);
    // absent element with a null in the list -> UNKNOWN (null).
    assertThat(scalar("RETURN 3 IN [1, 2, null] AS v")).isNull();
    // null probe -> UNKNOWN.
    assertThat(scalar("RETURN null IN [1, 2] AS v")).isNull();
  }

  // ----------------------------------------------------------------------------------------------
  // Helpers
  // ----------------------------------------------------------------------------------------------

  // Returns the matched 'name' values sorted, so callers can use order-sensitive containsExactly assertions
  // without depending on row iteration order.
  private List<String> matchedNames(final String cypher) {
    final List<String> names = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", cypher)) {
      while (rs.hasNext())
        names.add(rs.next().getProperty("name"));
    }
    Collections.sort(names);
    return names;
  }

  private Object scalarForCharlie(final String returnClause) {
    return scalar("MATCH (p:Person {name: 'Charlie'}) " + returnClause);
  }

  private Object scalar(final String cypher) {
    try (final ResultSet rs = database.query("opencypher", cypher)) {
      assertThat(rs.hasNext()).as("query returned no rows: %s", cypher).isTrue();
      final Result r = rs.next();
      return r.getProperty("v");
    }
  }
}
