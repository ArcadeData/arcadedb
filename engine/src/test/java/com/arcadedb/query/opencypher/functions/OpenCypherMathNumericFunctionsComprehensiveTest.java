/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.query.opencypher.functions;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import org.assertj.core.api.Assertions;
import static org.assertj.core.api.Assertions.within;

/**
 * Comprehensive tests for OpenCypher Mathematical Numeric functions based on Neo4j Cypher documentation.
 * Tests cover: abs(), ceil(), floor(), isNaN(), rand(), round(), sign()
 */
class OpenCypherMathNumericFunctionsComprehensiveTest {
  private Database database;

  @BeforeEach
  void setUp() {
    final DatabaseFactory factory = new DatabaseFactory("./databases/test-cypher-math-numeric");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
  }

  @AfterEach
  void tearDown() {
    if (database != null)
      database.drop();
  }

  // ==================== abs() Tests ====================

  @Test
  void absPositiveInteger() {
    final ResultSet result = database.command("opencypher", "RETURN abs(5) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat(((Number) result.next().getProperty("result")).intValue()).isEqualTo(5);
  }

  @Test
  void absNegativeInteger() {
    final ResultSet result = database.command("opencypher", "RETURN abs(-5) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat(((Number) result.next().getProperty("result")).intValue()).isEqualTo(5);
  }

  @Test
  void absPositiveFloat() {
    final ResultSet result = database.command("opencypher", "RETURN abs(3.14) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat(((Number) result.next().getProperty("result")).doubleValue()).isCloseTo(3.14, within(0.001));
  }

  @Test
  void absNegativeFloat() {
    final ResultSet result = database.command("opencypher", "RETURN abs(-3.14) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat(((Number) result.next().getProperty("result")).doubleValue()).isCloseTo(3.14, within(0.001));
  }

  @Test
  void absZero() {
    final ResultSet result = database.command("opencypher", "RETURN abs(0) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat(((Number) result.next().getProperty("result")).intValue()).isEqualTo(0);
  }

  @Test
  void absNull() {
    final ResultSet result = database.command("opencypher", "RETURN abs(null) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  // ==================== ceil() Tests ====================

  @Test
  void ceilPositive() {
    final ResultSet result = database.command("opencypher", "RETURN ceil(3.14) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat(((Number) result.next().getProperty("result")).doubleValue()).isCloseTo(4.0, within(0.001));
  }

  @Test
  void ceilNegative() {
    final ResultSet result = database.command("opencypher", "RETURN ceil(-3.14) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat(((Number) result.next().getProperty("result")).doubleValue()).isCloseTo(-3.0, within(0.001));
  }

  @Test
  void ceilWholeNumber() {
    final ResultSet result = database.command("opencypher", "RETURN ceil(5.0) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat(((Number) result.next().getProperty("result")).doubleValue()).isCloseTo(5.0, within(0.001));
  }

  @Test
  void ceilZero() {
    final ResultSet result = database.command("opencypher", "RETURN ceil(0.0) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat(((Number) result.next().getProperty("result")).doubleValue()).isCloseTo(0.0, within(0.001));
  }

  @Test
  void ceilNull() {
    final ResultSet result = database.command("opencypher", "RETURN ceil(null) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  // ==================== floor() Tests ====================

  @Test
  void floorPositive() {
    final ResultSet result = database.command("opencypher", "RETURN floor(3.14) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat(((Number) result.next().getProperty("result")).doubleValue()).isCloseTo(3.0, within(0.001));
  }

  @Test
  void floorNegative() {
    final ResultSet result = database.command("opencypher", "RETURN floor(-3.14) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat(((Number) result.next().getProperty("result")).doubleValue()).isCloseTo(-4.0, within(0.001));
  }

  @Test
  void floorWholeNumber() {
    final ResultSet result = database.command("opencypher", "RETURN floor(5.0) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat(((Number) result.next().getProperty("result")).doubleValue()).isCloseTo(5.0, within(0.001));
  }

  @Test
  void floorZero() {
    final ResultSet result = database.command("opencypher", "RETURN floor(0.0) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat(((Number) result.next().getProperty("result")).doubleValue()).isCloseTo(0.0, within(0.001));
  }

  @Test
  void floorNull() {
    final ResultSet result = database.command("opencypher", "RETURN floor(null) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  // ==================== isNaN() Tests ====================

  @Test
  void isNaNWithNaN() {
    final ResultSet result = database.command("opencypher", "RETURN isNaN(0.0 / 0.0) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat((Boolean) result.next().getProperty("result")).isTrue();
  }

  @Test
  void isNaNWithNumber() {
    final ResultSet result = database.command("opencypher", "RETURN isNaN(5.0) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat((Boolean) result.next().getProperty("result")).isFalse();
  }

  @Test
  void isNaNWithInfinity() {
    final ResultSet result = database.command("opencypher", "RETURN isNaN(1.0 / 0.0) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat((Boolean) result.next().getProperty("result")).isFalse();
  }

  @Test
  void isNaNWithInteger() {
    final ResultSet result = database.command("opencypher", "RETURN isNaN(42) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat((Boolean) result.next().getProperty("result")).isFalse();
  }

  @Test
  void isNaNNull() {
    final ResultSet result = database.command("opencypher", "RETURN isNaN(null) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  // ==================== rand() Tests ====================

  @Test
  void randBasic() {
    final ResultSet result = database.command("opencypher", "RETURN rand() AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final Double value = (Double) result.next().getProperty("result");
    assertThat(value).isGreaterThanOrEqualTo(0.0);
    assertThat(value).isLessThan(1.0);
  }

  @Test
  void randMultipleCalls() {
    final ResultSet result = database.command("opencypher",
        "RETURN rand() AS r1, rand() AS r2, rand() AS r3");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final var row = result.next();
    final Double r1 = (Double) row.getProperty("r1");
    final Double r2 = (Double) row.getProperty("r2");
    final Double r3 = (Double) row.getProperty("r3");
    // All should be in valid range
    assertThat(r1).isGreaterThanOrEqualTo(0.0).isLessThan(1.0);
    assertThat(r2).isGreaterThanOrEqualTo(0.0).isLessThan(1.0);
    assertThat(r3).isGreaterThanOrEqualTo(0.0).isLessThan(1.0);
  }

  // ==================== round() Tests ====================

  @Test
  void roundBasic() {
    final ResultSet result = database.command("opencypher", "RETURN round(3.14) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat(((Number) result.next().getProperty("result")).doubleValue()).isCloseTo(3.0, within(0.001));
  }

  @Test
  void roundHalfUp() {
    final ResultSet result = database.command("opencypher", "RETURN round(3.5) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat(((Number) result.next().getProperty("result")).doubleValue()).isCloseTo(4.0, within(0.001));
  }

  @Test
  void roundWithPrecision() {
    final ResultSet result = database.command("opencypher", "RETURN round(3.14159, 2) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat(((Number) result.next().getProperty("result")).doubleValue()).isCloseTo(3.14, within(0.001));
  }

  @Test
  void roundWithPrecisionAndMode() {
    // Test different rounding modes
    ResultSet result = database.command("opencypher", "RETURN round(3.145, 2, 'UP') AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat(((Number) result.next().getProperty("result")).doubleValue()).isCloseTo(3.15, within(0.001));

    result = database.command("opencypher", "RETURN round(3.145, 2, 'DOWN') AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat(((Number) result.next().getProperty("result")).doubleValue()).isCloseTo(3.14, within(0.001));

    result = database.command("opencypher", "RETURN round(3.145, 2, 'CEILING') AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat(((Number) result.next().getProperty("result")).doubleValue()).isCloseTo(3.15, within(0.001));

    result = database.command("opencypher", "RETURN round(3.145, 2, 'FLOOR') AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat(((Number) result.next().getProperty("result")).doubleValue()).isCloseTo(3.14, within(0.001));
  }

  @Test
  void roundWithHalfEvenMode() {
    // HALF_EVEN mode (banker's rounding)
    ResultSet result = database.command("opencypher", "RETURN round(2.5, 0, 'HALF_EVEN') AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat(((Number) result.next().getProperty("result")).doubleValue()).isCloseTo(2.0, within(0.001));

    result = database.command("opencypher", "RETURN round(3.5, 0, 'HALF_EVEN') AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat(((Number) result.next().getProperty("result")).doubleValue()).isCloseTo(4.0, within(0.001));
  }

  @Test
  void roundNegativeNumber() {
    final ResultSet result = database.command("opencypher", "RETURN round(-3.14) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat(((Number) result.next().getProperty("result")).doubleValue()).isCloseTo(-3.0, within(0.001));
  }

  @Test
  void roundZero() {
    final ResultSet result = database.command("opencypher", "RETURN round(0.0) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat(((Number) result.next().getProperty("result")).doubleValue()).isCloseTo(0.0, within(0.001));
  }

  @Test
  void roundNull() {
    final ResultSet result = database.command("opencypher", "RETURN round(null) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  // ==================== sign() Tests ====================

  @Test
  void signPositive() {
    final ResultSet result = database.command("opencypher", "RETURN sign(42) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat(((Number) result.next().getProperty("result")).intValue()).isEqualTo(1);
  }

  @Test
  void signNegative() {
    final ResultSet result = database.command("opencypher", "RETURN sign(-42) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat(((Number) result.next().getProperty("result")).intValue()).isEqualTo(-1);
  }

  @Test
  void signZero() {
    final ResultSet result = database.command("opencypher", "RETURN sign(0) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat(((Number) result.next().getProperty("result")).intValue()).isEqualTo(0);
  }

  @Test
  void signPositiveFloat() {
    final ResultSet result = database.command("opencypher", "RETURN sign(3.14) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat(((Number) result.next().getProperty("result")).intValue()).isEqualTo(1);
  }

  @Test
  void signNegativeFloat() {
    final ResultSet result = database.command("opencypher", "RETURN sign(-3.14) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat(((Number) result.next().getProperty("result")).intValue()).isEqualTo(-1);
  }

  @Test
  void signNull() {
    final ResultSet result = database.command("opencypher", "RETURN sign(null) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  // ==================== Combined/Integration Tests ====================

  @Test
  void mathFunctionsCombined() {
    final ResultSet result = database.command("opencypher",
        "RETURN abs(ceil(-3.14)) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat(((Number) result.next().getProperty("result")).doubleValue()).isCloseTo(3.0, within(0.001));
  }

  @Test
  void mathFunctionsChaining() {
    final ResultSet result = database.command("opencypher",
        "RETURN sign(floor(-3.7)) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat(((Number) result.next().getProperty("result")).intValue()).isEqualTo(-1);
  }

  @Test
  void mathFunctionsWithRound() {
    final ResultSet result = database.command("opencypher",
        "RETURN abs(round(-3.14159, 2)) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat(((Number) result.next().getProperty("result")).doubleValue()).isCloseTo(3.14, within(0.001));
  }
}
