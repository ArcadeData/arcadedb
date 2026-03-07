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
 * Comprehensive tests for OpenCypher Mathematical Logarithmic functions based on Neo4j Cypher documentation.
 * Tests cover: e(), exp(), log(), log10(), sqrt()
 */
class OpenCypherMathLogarithmicFunctionsComprehensiveTest {
  private Database database;

  @BeforeEach
  void setUp() {
    final DatabaseFactory factory = new DatabaseFactory("./databases/test-cypher-math-log");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
  }

  @AfterEach
  void tearDown() {
    if (database != null)
      database.drop();
  }

  // ==================== e() Tests ====================

  @Test
  void eBasic() {
    final ResultSet result = database.command("opencypher", "RETURN e() AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Number resultNum = (Number) result.next().getProperty("result");
    assertThat(resultNum.doubleValue()).isCloseTo(Math.E, within(0.0000001));
  }

  @Test
  void eConstant() {
    final ResultSet result = database.command("opencypher", "RETURN e() AS e1, e() AS e2");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final var row = result.next();
    assertThat((Double) row.getProperty("e1")).isEqualTo((Double) row.getProperty("e2"));
  }

  // ==================== exp() Tests ====================

  @Test
  void expZero() {
    final ResultSet result = database.command("opencypher", "RETURN exp(0.0) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Number resultNum = (Number) result.next().getProperty("result");
    assertThat(resultNum.doubleValue()).isCloseTo(1.0, within(0.0001));
  }

  @Test
  void expOne() {
    final ResultSet result = database.command("opencypher", "RETURN exp(1.0) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Number resultNum = (Number) result.next().getProperty("result");
    assertThat(resultNum.doubleValue()).isCloseTo(Math.E, within(0.0001));
  }

  @Test
  void expNegative() {
    final ResultSet result = database.command("opencypher", "RETURN exp(-1.0) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Number resultNum = (Number) result.next().getProperty("result");
    assertThat(resultNum.doubleValue()).isCloseTo(1.0 / Math.E, within(0.0001));
  }

  @Test
  void expLargeValue() {
    final ResultSet result = database.command("opencypher", "RETURN exp(10.0) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Number resultNum = (Number) result.next().getProperty("result");
    assertThat(resultNum.doubleValue()).isGreaterThan(20000.0);
  }

  @Test
  void expOverflowReturnsInfinity() {
    final ResultSet result = database.command("opencypher", "RETURN exp(1000.0) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final Double value = (Double) result.next().getProperty("result");
    assertThat(value.isInfinite()).isTrue();
  }

  @Test
  void expNull() {
    final ResultSet result = database.command("opencypher", "RETURN exp(null) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  // ==================== log() Tests ====================

  @Test
  void logOne() {
    final ResultSet result = database.command("opencypher", "RETURN log(1.0) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Number resultNum = (Number) result.next().getProperty("result");
    assertThat(resultNum.doubleValue()).isCloseTo(0.0, within(0.0001));
  }

  @Test
  void logE() {
    final ResultSet result = database.command("opencypher", "RETURN log(e()) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Number resultNum = (Number) result.next().getProperty("result");
    assertThat(resultNum.doubleValue()).isCloseTo(1.0, within(0.0001));
  }

  @Test
  void logPositive() {
    final ResultSet result = database.command("opencypher", "RETURN log(10.0) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Number resultNum = (Number) result.next().getProperty("result");
    assertThat(resultNum.doubleValue()).isCloseTo(Math.log(10.0), within(0.0001));
  }

  @Test
  void logZeroReturnsNegativeInfinity() {
    final ResultSet result = database.command("opencypher", "RETURN log(0.0) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final Double value = (Double) result.next().getProperty("result");
    assertThat(value).isEqualTo(Double.NEGATIVE_INFINITY);
  }

  @Test
  void logNegativeReturnsNaN() {
    final ResultSet result = database.command("opencypher", "RETURN log(-1.0) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Number resultNum = (Number) result.next().getProperty("result");
    assertThat(resultNum.doubleValue()).isNaN();
  }

  @Test
  void logNull() {
    final ResultSet result = database.command("opencypher", "RETURN log(null) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  // ==================== log10() Tests ====================

  @Test
  void log10One() {
    final ResultSet result = database.command("opencypher", "RETURN log10(1.0) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Number resultNum = (Number) result.next().getProperty("result");
    assertThat(resultNum.doubleValue()).isCloseTo(0.0, within(0.0001));
  }

  @Test
  void log10Ten() {
    final ResultSet result = database.command("opencypher", "RETURN log10(10.0) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Number resultNum = (Number) result.next().getProperty("result");
    assertThat(resultNum.doubleValue()).isCloseTo(1.0, within(0.0001));
  }

  @Test
  void log10Hundred() {
    final ResultSet result = database.command("opencypher", "RETURN log10(100.0) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Number resultNum = (Number) result.next().getProperty("result");
    assertThat(resultNum.doubleValue()).isCloseTo(2.0, within(0.0001));
  }

  @Test
  void log10Thousand() {
    final ResultSet result = database.command("opencypher", "RETURN log10(1000.0) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Number resultNum = (Number) result.next().getProperty("result");
    assertThat(resultNum.doubleValue()).isCloseTo(3.0, within(0.0001));
  }

  @Test
  void log10ZeroReturnsNegativeInfinity() {
    final ResultSet result = database.command("opencypher", "RETURN log10(0.0) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final Double value = (Double) result.next().getProperty("result");
    assertThat(value).isEqualTo(Double.NEGATIVE_INFINITY);
  }

  @Test
  void log10NegativeReturnsNaN() {
    final ResultSet result = database.command("opencypher", "RETURN log10(-1.0) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Number resultNum = (Number) result.next().getProperty("result");
    assertThat(resultNum.doubleValue()).isNaN();
  }

  @Test
  void log10Null() {
    final ResultSet result = database.command("opencypher", "RETURN log10(null) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  // ==================== sqrt() Tests ====================

  @Test
  void sqrtZero() {
    final ResultSet result = database.command("opencypher", "RETURN sqrt(0.0) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Number resultNum = (Number) result.next().getProperty("result");
    assertThat(resultNum.doubleValue()).isCloseTo(0.0, within(0.0001));
  }

  @Test
  void sqrtOne() {
    final ResultSet result = database.command("opencypher", "RETURN sqrt(1.0) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Number resultNum = (Number) result.next().getProperty("result");
    assertThat(resultNum.doubleValue()).isCloseTo(1.0, within(0.0001));
  }

  @Test
  void sqrtFour() {
    final ResultSet result = database.command("opencypher", "RETURN sqrt(4.0) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Number resultNum = (Number) result.next().getProperty("result");
    assertThat(resultNum.doubleValue()).isCloseTo(2.0, within(0.0001));
  }

  @Test
  void sqrtNine() {
    final ResultSet result = database.command("opencypher", "RETURN sqrt(9.0) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Number resultNum = (Number) result.next().getProperty("result");
    assertThat(resultNum.doubleValue()).isCloseTo(3.0, within(0.0001));
  }

  @Test
  void sqrtTwo() {
    final ResultSet result = database.command("opencypher", "RETURN sqrt(2.0) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Number resultNum = (Number) result.next().getProperty("result");
    assertThat(resultNum.doubleValue()).isCloseTo(Math.sqrt(2.0), within(0.0001));
  }

  @Test
  void sqrtNegativeReturnsNaN() {
    final ResultSet result = database.command("opencypher", "RETURN sqrt(-1.0) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Number resultNum = (Number) result.next().getProperty("result");
    assertThat(resultNum.doubleValue()).isNaN();
  }

  @Test
  void sqrtNull() {
    final ResultSet result = database.command("opencypher", "RETURN sqrt(null) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  // ==================== Combined/Integration Tests ====================

  @Test
  void logExpIdentity() {
    // log(exp(x)) = x
    final ResultSet result = database.command("opencypher",
        "RETURN log(exp(2.0)) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Number resultNum = (Number) result.next().getProperty("result");
    assertThat(resultNum.doubleValue()).isCloseTo(2.0, within(0.0001));
  }

  @Test
  void expLogIdentity() {
    // exp(log(x)) = x
    final ResultSet result = database.command("opencypher",
        "RETURN exp(log(5.0)) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Number resultNum = (Number) result.next().getProperty("result");
    assertThat(resultNum.doubleValue()).isCloseTo(5.0, within(0.0001));
  }

  @Test
  void sqrtSquareIdentity() {
    // sqrt(x²) = x
    final ResultSet result = database.command("opencypher",
        "WITH 7.0 AS x RETURN sqrt(x * x) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Number resultNum = (Number) result.next().getProperty("result");
    assertThat(resultNum.doubleValue()).isCloseTo(7.0, within(0.0001));
  }

  @Test
  void log10ConversionFromLog() {
    // log10(x) = log(x) / log(10)
    final ResultSet result = database.command("opencypher",
        "WITH 100.0 AS x RETURN log10(x) AS log10_val, log(x) / log(10.0) AS log_ratio");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final var row = result.next();
    final Number log10Val = (Number) row.getProperty("log10_val");
    final Number logRatio = (Number) row.getProperty("log_ratio");
    assertThat(log10Val.doubleValue()).isCloseTo(logRatio.doubleValue(), within(0.0001));
  }

  @Test
  void sqrtExp() {
    // sqrt(x) = exp(log(x) / 2)
    final ResultSet result = database.command("opencypher",
        "WITH 16.0 AS x RETURN sqrt(x) AS sqrt_val, exp(log(x) / 2.0) AS exp_val");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final var row = result.next();
    final Number sqrtVal = (Number) row.getProperty("sqrt_val");
    final Number expVal = (Number) row.getProperty("exp_val");
    assertThat(sqrtVal.doubleValue()).isCloseTo(expVal.doubleValue(), within(0.0001));
  }

  @Test
  void exponentialGrowth() {
    // Test exponential growth formula
    final ResultSet result = database.command("opencypher",
        "WITH 100.0 AS initial, 0.05 AS rate, 10.0 AS time " +
            "RETURN initial * exp(rate * time) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final Number growth = (Number) result.next().getProperty("result");
    assertThat(growth.doubleValue()).isGreaterThan(100.0);
    assertThat(growth.doubleValue()).isCloseTo(164.87, within(0.1));
  }
}
