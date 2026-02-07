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
package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for CartesianProductStep.
 * Note: CartesianProductStep is an internal execution step used in MATCH queries.
 * It is not directly accessible via SQL SELECT syntax (ArcadeDB SQL does not support
 * comma-separated FROM clauses, JOIN, or CROSS JOIN syntax).
 * <p>
 * These tests are disabled pending implementation of JOIN/CROSS JOIN SQL syntax.
 */
public class CartesianProductStepTest extends TestHelper {

  @Test
  @Disabled("SQL syntax not supported: comma-separated FROM clauses for Cartesian product")
  void shouldComputeCartesianProduct() {
    database.getSchema().createDocumentType("TableA");
    database.getSchema().createDocumentType("TableB");

    database.transaction(() -> {
      database.newDocument("TableA").set("id", 1).save();
      database.newDocument("TableA").set("id", 2).save();

      database.newDocument("TableB").set("value", "X").save();
      database.newDocument("TableB").set("value", "Y").save();
      database.newDocument("TableB").set("value", "Z").save();
    });

    // Cross product using multiple FROM clauses - NOT SUPPORTED
    final ResultSet result = database.query("sql", "SELECT $a.id as idA, $b.value as valB FROM (SELECT FROM TableA) AS $a, (SELECT FROM TableB) AS $b");

    int count = 0;
    while (result.hasNext()) {
      final Result item = result.next();
      final Object idA = item.getProperty("idA");
      assertThat(idA).isIn(1, 2);
      assertThat(item.<String>getProperty("valB")).isIn("X", "Y", "Z");
      count++;
    }

    assertThat(count).isEqualTo(6); // 2 * 3 = 6
    result.close();
  }

  @Test
  @Disabled("SQL syntax not supported: comma-separated FROM clauses for Cartesian product")
  void shouldHandleThreeWayProduct() {
    database.getSchema().createDocumentType("T1");
    database.getSchema().createDocumentType("T2");
    database.getSchema().createDocumentType("T3");

    database.transaction(() -> {
      database.newDocument("T1").set("v", "A").save();
      database.newDocument("T1").set("v", "B").save();

      database.newDocument("T2").set("v", 1).save();
      database.newDocument("T2").set("v", 2).save();

      database.newDocument("T3").set("v", "X").save();
      database.newDocument("T3").set("v", "Y").save();
    });

    final ResultSet result = database.query("sql", "SELECT $a.v as v1, $b.v as v2, $c.v as v3 FROM (SELECT FROM T1) AS $a, (SELECT FROM T2) AS $b, (SELECT FROM T3) AS $c");

    int count = 0;
    while (result.hasNext()) {
      final Result item = result.next();
      final String v1 = item.getProperty("v1");
      final Object v2 = item.getProperty("v2");
      final String v3 = item.getProperty("v3");
      assertThat(v1).isIn("A", "B");
      assertThat(v2).isIn(1, 2);
      assertThat(v3).isIn("X", "Y");
      count++;
    }

    assertThat(count).isEqualTo(8); // 2 * 2 * 2 = 8
    result.close();
  }

  @Test
  @Disabled("SQL syntax not supported: comma-separated FROM clauses for Cartesian product")
  void shouldHandleEmptySet() {
    database.getSchema().createDocumentType("Empty1");
    database.getSchema().createDocumentType("TableX");

    database.transaction(() -> {
      database.newDocument("TableX").set("v", 1).save();
      database.newDocument("TableX").set("v", 2).save();
    });

    final ResultSet result = database.query("sql", "SELECT $a.v as v1, $b.v as v2 FROM (SELECT FROM Empty1) AS $a, (SELECT FROM TableX) AS $b");

    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  @Disabled("SQL syntax not supported: comma-separated FROM clauses for Cartesian product")
  void shouldHandleOneElementSet() {
    database.getSchema().createDocumentType("Single");
    database.getSchema().createDocumentType("Multiple");

    database.transaction(() -> {
      database.newDocument("Single").set("v", "ONE").save();

      database.newDocument("Multiple").set("v", 1).save();
      database.newDocument("Multiple").set("v", 2).save();
      database.newDocument("Multiple").set("v", 3).save();
    });

    final ResultSet result = database.query("sql", "SELECT $a.v as v1, $b.v as v2 FROM (SELECT FROM Single) AS $a, (SELECT FROM Multiple) AS $b");

    int count = 0;
    while (result.hasNext()) {
      final Result item = result.next();
      final String v1 = item.getProperty("v1");
      final Object v2 = item.getProperty("v2");
      assertThat(v1).isEqualTo("ONE");
      assertThat(v2).isIn(1, 2, 3);
      count++;
    }

    assertThat(count).isEqualTo(3); // 1 * 3 = 3
    result.close();
  }

  @Test
  @Disabled("SQL syntax not supported: comma-separated FROM clauses for Cartesian product")
  void shouldWorkWithWhereClause() {
    database.getSchema().createDocumentType("Numbers");
    database.getSchema().createDocumentType("Letters");

    database.transaction(() -> {
      database.newDocument("Numbers").set("n", 1).save();
      database.newDocument("Numbers").set("n", 2).save();
      database.newDocument("Numbers").set("n", 3).save();

      database.newDocument("Letters").set("l", "A").save();
      database.newDocument("Letters").set("l", "B").save();
    });

    final ResultSet result = database.query("sql", "SELECT $a.n as num, $b.l as letter FROM (SELECT FROM Numbers) AS $a, (SELECT FROM Letters) AS $b WHERE $a.n > 1");

    int count = 0;
    while (result.hasNext()) {
      final Result item = result.next();
      final int num = item.getProperty("num");
      assertThat(num).isGreaterThan(1);
      assertThat(item.<String>getProperty("letter")).isIn("A", "B");
      count++;
    }

    assertThat(count).isEqualTo(4); // 2 numbers (2,3) * 2 letters = 4
    result.close();
  }
}
