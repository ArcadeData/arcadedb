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
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for CALL subquery (CALL { ... }) support in OpenCypher.
 * Reproduces issue #3327: Cypher subquery call is broken.
 */
class OpenCypherSubqueryTest {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/testopencypher-subquery").create();
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  /**
   * Issue #3327: CALL subquery with UNWIND should compute expressions correctly.
   * UNWIND [1, 2, 3] AS x CALL { WITH x RETURN x * 10 AS y } RETURN x, y
   * Expected: x=1,y=10 / x=2,y=20 / x=3,y=30
   */
  @Test
  void callSubqueryWithUnwind() {
    final ResultSet result = database.query("opencypher",
        "UNWIND [1, 2, 3] AS x CALL { WITH x RETURN x * 10 AS y } RETURN x, y");

    final List<Result> rows = new ArrayList<>();
    while (result.hasNext())
      rows.add(result.next());

    assertThat(rows).hasSize(3);

    // Verify each row
    for (final Result row : rows) {
      final Object x = row.getProperty("x");
      final Object y = row.getProperty("y");
      assertThat(x).isNotNull();
      assertThat(y).isNotNull();
      assertThat(((Number) y).longValue()).isEqualTo(((Number) x).longValue() * 10);
    }
  }

  /**
   * CALL subquery with simple passthrough.
   * UNWIND [1, 2, 3] AS x CALL { WITH x RETURN x AS z } RETURN x, z
   */
  @Test
  void callSubqueryPassthrough() {
    final ResultSet result = database.query("opencypher",
        "UNWIND [1, 2, 3] AS x CALL { WITH x RETURN x AS z } RETURN x, z");

    final List<Result> rows = new ArrayList<>();
    while (result.hasNext())
      rows.add(result.next());

    assertThat(rows).hasSize(3);

    for (final Result row : rows) {
      final Object x = row.getProperty("x");
      final Object z = row.getProperty("z");
      assertThat(x).isNotNull();
      assertThat(z).isNotNull();
      assertThat(((Number) z).longValue()).isEqualTo(((Number) x).longValue());
    }
  }

  /**
   * CALL subquery with MATCH inside.
   */
  @Test
  void callSubqueryWithMatch() {
    database.getSchema().createVertexType("Item");

    database.transaction(() -> {
      database.command("opencypher", "CREATE (:Item {name: 'A', value: 10})");
      database.command("opencypher", "CREATE (:Item {name: 'B', value: 20})");
    });

    // First test: simpler version - just get doubled value from subquery
    final ResultSet result0 = database.query("opencypher",
        "MATCH (n:Item) CALL { WITH n RETURN n.value AS val } RETURN n.name AS name, val");
    final List<Result> rows0 = new ArrayList<>();
    while (result0.hasNext())
      rows0.add(result0.next());
    assertThat(rows0).hasSize(2);
    for (final Result row : rows0) {
      assertThat((Object) row.getProperty("name")).isNotNull();
      assertThat((Object) row.getProperty("val")).as("val should not be null, row props=" + row.getPropertyNames()).isNotNull();
    }

    // Full test: multiply inside subquery
    final ResultSet result = database.query("opencypher",
        "MATCH (n:Item) CALL { WITH n RETURN n.value * 2 AS doubled } RETURN n.name AS name, doubled");

    final List<Result> rows = new ArrayList<>();
    while (result.hasNext())
      rows.add(result.next());

    assertThat(rows).hasSize(2);

    for (final Result row : rows) {
      final String name = row.getProperty("name");
      final Number doubled = row.getProperty("doubled");
      assertThat(name).isNotNull();
      assertThat(doubled).as("doubled should not be null for name=" + name + ", props=" + row.getPropertyNames()).isNotNull();

      if ("A".equals(name))
        assertThat(doubled.longValue()).isEqualTo(20L);
      else if ("B".equals(name))
        assertThat(doubled.longValue()).isEqualTo(40L);
    }
  }

  /**
   * CALL subquery with map property access via UNWIND.
   */
  @Test
  void callSubqueryMapPropertyAccess() {
    final ResultSet result = database.query("opencypher",
        "UNWIND [{name: 'A', value: 10}] AS n CALL { WITH n RETURN n.value AS val } RETURN n.name AS name, val");

    final List<Result> rows = new ArrayList<>();
    while (result.hasNext())
      rows.add(result.next());

    assertThat(rows).hasSize(1);
    final Result row = rows.get(0);
    assertThat((Object) row.getProperty("name")).as("name should not be null, props=" + row.getPropertyNames()).isNotNull();
    assertThat((Object) row.getProperty("val")).as("val should not be null, props=" + row.getPropertyNames()).isNotNull();
    assertThat(((Number) row.getProperty("val")).longValue()).isEqualTo(10L);
  }

  /**
   * Issue #3769: WHERE clause inside a CALL subquery always returns false.
   * MERGE creates a node, coalesce produces an empty list, and the WHERE inside
   * the CALL subquery should let the row pass through because any() on an empty
   * list returns false, so NOT false = true.
   */
  @Test
  void callSubqueryWhereClauseNotAlwaysFalse() {
    database.getSchema().createVertexType("Thing1");

    database.transaction(() -> {
      final ResultSet result = database.command("opencypher",
          "MERGE (t1:Thing1 {username: \"bob\"}) WITH coalesce(t1.aList, []) AS aList " +
              "CALL { WITH aList WITH aList " +
              "WHERE NOT any(element IN aList WHERE element = \"92dc13ff-50d1-4879-b2fd-b0dedf7ec019\") " +
              "RETURN true AS success1 } RETURN success1");

      final List<Result> rows = new ArrayList<>();
      while (result.hasNext())
        rows.add(result.next());

      assertThat(rows).as("Expected one row with success1=true, got empty result set").hasSize(1);
      assertThat((Boolean) rows.get(0).getProperty("success1")).isTrue();
    });
  }

  /**
   * Simpler test for WHERE inside CALL subquery with an empty list.
   * Tests that the WHERE clause correctly evaluates to true for an empty list.
   */
  @Test
  void callSubqueryWhereWithEmptyList() {
    final ResultSet result = database.query("opencypher",
        "WITH [] AS aList " +
            "CALL { WITH aList WITH aList " +
            "WHERE NOT any(element IN aList WHERE element = \"x\") " +
            "RETURN true AS success1 } RETURN success1");

    final List<Result> rows = new ArrayList<>();
    while (result.hasNext())
      rows.add(result.next());

    assertThat(rows).as("WHERE on empty list should pass through").hasSize(1);
    assertThat((Boolean) rows.get(0).getProperty("success1")).isTrue();
  }

  /**
   * Test that WHERE inside CALL subquery correctly filters when condition is false.
   */
  @Test
  void callSubqueryWhereFiltersTrueCondition() {
    final ResultSet result = database.query("opencypher",
        "WITH [\"x\"] AS aList " +
            "CALL { WITH aList WITH aList " +
            "WHERE NOT any(element IN aList WHERE element = \"x\") " +
            "RETURN true AS success1 } RETURN success1");

    final List<Result> rows = new ArrayList<>();
    while (result.hasNext())
      rows.add(result.next());

    // any(element IN ["x"] WHERE element = "x") = true, NOT true = false, so no rows
    assertThat(rows).as("WHERE should filter out the row").hasSize(0);
  }

  /**
   * CALL subquery with multiple imported variables.
   */
  @Test
  void callSubqueryMultipleImports() {
    final ResultSet result = database.query("opencypher",
        "UNWIND [1, 2] AS a UNWIND [10, 20] AS b CALL { WITH a, b RETURN a + b AS sum } RETURN a, b, sum");

    final List<Result> rows = new ArrayList<>();
    while (result.hasNext())
      rows.add(result.next());

    assertThat(rows).hasSize(4);

    for (final Result row : rows) {
      final Number a = row.getProperty("a");
      final Number b = row.getProperty("b");
      final Number sum = row.getProperty("sum");
      assertThat(a).isNotNull();
      assertThat(b).isNotNull();
      assertThat(sum).isNotNull();
      assertThat(sum.longValue()).isEqualTo(a.longValue() + b.longValue());
    }
  }

  /**
   * Issue #3772: UNION inside CALL subquery should evaluate all branches.
   * When the first branch's WHERE filters out all rows, the second branch should still produce results.
   */
  @Test
  void callSubqueryWithUnionFallsThrough() {
    // First query: empty list, so SIZE > 0 is false, but SIZE = 0 is true -> should return success2 = 1
    final ResultSet result1 = database.query("opencypher",
        "WITH [] as toRemove " +
        "CALL { " +
        "  WITH toRemove WITH toRemove " +
        "  WHERE SIZE(toRemove) > 0 " +
        "  RETURN 2 AS success2 " +
        "  UNION " +
        "  WITH toRemove WITH toRemove " +
        "  WHERE SIZE(toRemove) = 0 " +
        "  RETURN 1 AS success2 " +
        "} " +
        "RETURN success2");

    assertThat(result1.hasNext()).isTrue();
    final Result row1 = result1.next();
    assertThat(((Number) row1.getProperty("success2")).intValue()).isEqualTo(1);
    assertThat(result1.hasNext()).isFalse();

    // Second query: non-empty list, so SIZE > 0 is true -> should return success2 = 2
    final ResultSet result2 = database.query("opencypher",
        "WITH ['a'] as toRemove " +
        "CALL { " +
        "  WITH toRemove WITH toRemove " +
        "  WHERE SIZE(toRemove) > 0 " +
        "  RETURN 2 AS success2 " +
        "  UNION " +
        "  WITH toRemove WITH toRemove " +
        "  WHERE SIZE(toRemove) = 0 " +
        "  RETURN 1 AS success2 " +
        "} " +
        "RETURN success2");

    assertThat(result2.hasNext()).isTrue();
    final Result row2 = result2.next();
    assertThat(((Number) row2.getProperty("success2")).intValue()).isEqualTo(2);
    assertThat(result2.hasNext()).isFalse();
  }
}
