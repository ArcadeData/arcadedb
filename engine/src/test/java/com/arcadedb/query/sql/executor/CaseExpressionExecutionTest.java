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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for SQL CASE expression execution.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class CaseExpressionExecutionTest extends TestHelper {

  @BeforeEach
  void setupTestData() {
    database.getSchema().createDocumentType("TestDoc");
    database.begin();
    database.command("sql", "INSERT INTO TestDoc SET value = 10, status = 'active', color = 1");
    database.command("sql", "INSERT INTO TestDoc SET value = -5, status = 'inactive', color = 2");
    database.command("sql", "INSERT INTO TestDoc SET value = 0, status = 'pending', color = 3");
    database.commit();
  }

  @Test
  void testSearchedCasePositive() {
    final ResultSet result = database.query("sql",
        "SELECT CASE WHEN value > 0 THEN 'positive' ELSE 'not positive' END as sign FROM TestDoc WHERE value = 10");
    assertThat(result.hasNext()).isTrue();
    final Result item = result.next();
    assertThat(item.<String>getProperty("sign")).isEqualTo("positive");
    result.close();
  }

  @Test
  void testSearchedCaseNegative() {
    final ResultSet result = database.query("sql",
        "SELECT CASE WHEN value > 0 THEN 'positive' ELSE 'not positive' END as sign FROM TestDoc WHERE value = -5");
    assertThat(result.hasNext()).isTrue();
    final Result item = result.next();
    assertThat(item.<String>getProperty("sign")).isEqualTo("not positive");
    result.close();
  }

  @Test
  void testSearchedCaseMultipleWhen() {
    final ResultSet result = database.query("sql",
        "SELECT CASE WHEN value < 0 THEN 'negative' WHEN value = 0 THEN 'zero' ELSE 'positive' END as category FROM TestDoc ORDER BY value");

    // First: value = -5 -> negative
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<String>getProperty("category")).isEqualTo("negative");

    // Second: value = 0 -> zero
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<String>getProperty("category")).isEqualTo("zero");

    // Third: value = 10 -> positive
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<String>getProperty("category")).isEqualTo("positive");

    result.close();
  }

  @Test
  void testSearchedCaseNoElseReturnsNull() {
    final ResultSet result = database.query("sql",
        "SELECT CASE WHEN value > 100 THEN 'very big' END as size FROM TestDoc WHERE value = 10");
    assertThat(result.hasNext()).isTrue();
    final Result item = result.next();
    assertThat((Object) item.getProperty("size")).isNull();
    result.close();
  }

  @Test
  void testSimpleCaseBasic() {
    final ResultSet result = database.query("sql",
        "SELECT CASE color WHEN 1 THEN 'red' WHEN 2 THEN 'blue' WHEN 3 THEN 'green' ELSE 'unknown' END as colorName FROM TestDoc ORDER BY value");

    // First: color = 2 -> blue
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<String>getProperty("colorName")).isEqualTo("blue");

    // Second: color = 3 -> green
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<String>getProperty("colorName")).isEqualTo("green");

    // Third: color = 1 -> red
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<String>getProperty("colorName")).isEqualTo("red");

    result.close();
  }

  @Test
  void testSimpleCaseWithString() {
    final ResultSet result = database.query("sql",
        "SELECT CASE status WHEN 'active' THEN 1 WHEN 'inactive' THEN 0 ELSE -1 END as statusCode FROM TestDoc ORDER BY value");

    // First: status = inactive -> 0
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<Integer>getProperty("statusCode")).isEqualTo(0);

    // Second: status = pending -> -1
    assertThat(result.hasNext()).isTrue();
    assertThat((int) result.next().<Integer>getProperty("statusCode")).isEqualTo(-1);

    // Third: status = active -> 1
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<Integer>getProperty("statusCode")).isEqualTo(1);

    result.close();
  }

  @Test
  void testNestedCase() {
    final ResultSet result = database.query("sql",
        "SELECT CASE WHEN value > 0 THEN CASE WHEN color = 1 THEN 'pos-red' ELSE 'pos-other' END ELSE 'not-positive' END as nested FROM TestDoc WHERE value = 10");
    assertThat(result.hasNext()).isTrue();
    final Result item = result.next();
    assertThat(item.<String>getProperty("nested")).isEqualTo("pos-red");
    result.close();
  }

  @Test
  void testCaseWithArithmetic() {
    final ResultSet result = database.query("sql",
        "SELECT CASE WHEN value > 0 THEN value + 1 ELSE value - 1 END as adjusted FROM TestDoc ORDER BY value");

    // First: value = -5 -> -6
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<Integer>getProperty("adjusted")).isEqualTo(-6);

    // Second: value = 0 -> -1
    assertThat(result.hasNext()).isTrue();
    assertThat((int) result.next().<Integer>getProperty("adjusted")).isEqualTo(-1);

    // Third: value = 10 -> 11
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<Integer>getProperty("adjusted")).isEqualTo(11);

    result.close();
  }

  @Test
  void testCaseWithAndOr() {
    database.begin();
    database.command("sql", "INSERT INTO TestDoc SET value = 5, status = 'active', color = 1, x = 1, y = 1");
    database.command("sql", "INSERT INTO TestDoc SET value = 6, status = 'active', color = 2, x = -1, y = 1");
    database.commit();

    final ResultSet result = database.query("sql",
        "SELECT CASE WHEN x > 0 AND y > 0 THEN 'q1' WHEN x < 0 OR y < 0 THEN 'other' ELSE 'origin' END as quadrant FROM TestDoc WHERE x is not null ORDER BY value");

    // value = 5: x=1, y=1 -> q1
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<String>getProperty("quadrant")).isEqualTo("q1");

    // value = 6: x=-1, y=1 -> other
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<String>getProperty("quadrant")).isEqualTo("other");

    result.close();
  }

  @Test
  void testCaseInLetClause() {
    final ResultSet result = database.query("sql",
        "SELECT $result FROM TestDoc LET $result = CASE WHEN value > 0 THEN 'positive' ELSE 'not positive' END WHERE value = 10");
    assertThat(result.hasNext()).isTrue();
    final Result item = result.next();
    assertThat(item.<String>getProperty("$result")).isEqualTo("positive");
    result.close();
  }

  @Test
  void testCaseNoTarget() {
    // CASE without FROM clause
    final ResultSet result = database.query("sql",
        "SELECT CASE WHEN 1 > 0 THEN 'yes' ELSE 'no' END as answer");
    assertThat(result.hasNext()).isTrue();
    final Result item = result.next();
    assertThat(item.<String>getProperty("answer")).isEqualTo("yes");
    result.close();
  }

  @Test
  void testSimpleCaseNoMatch() {
    final ResultSet result = database.query("sql",
        "SELECT CASE color WHEN 99 THEN 'found' END as found FROM TestDoc WHERE value = 10");
    assertThat(result.hasNext()).isTrue();
    final Result item = result.next();
    assertThat((Object) item.getProperty("found")).isNull();
    result.close();
  }
}
