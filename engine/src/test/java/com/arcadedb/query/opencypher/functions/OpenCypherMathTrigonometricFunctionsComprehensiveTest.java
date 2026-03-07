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
 * Comprehensive tests for OpenCypher Mathematical Trigonometric functions based on Neo4j Cypher documentation.
 * Tests cover: acos(), asin(), atan(), atan2(), cos(), cosh(), cot(), coth(), degrees(), haversin(), pi(), radians(), sin(), sinh(), tan(), tanh()
 */
class OpenCypherMathTrigonometricFunctionsComprehensiveTest {
  private Database database;

  @BeforeEach
  void setUp() {
    final DatabaseFactory factory = new DatabaseFactory("./databases/test-cypher-math-trig");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
  }

  @AfterEach
  void tearDown() {
    if (database != null)
      database.drop();
  }

  // ==================== acos() Tests ====================

  @Test
  void acosBasic() {
    final ResultSet result = database.command("opencypher", "RETURN acos(1.0) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Number numResult = (Number) result.next().getProperty("result");
    assertThat(numResult.doubleValue()).isCloseTo(0.0, within(0.0001));
  }

  @Test
  void acosZero() {
    final ResultSet result = database.command("opencypher", "RETURN acos(0.0) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Number numResult = (Number) result.next().getProperty("result");
    assertThat(numResult.doubleValue()).isCloseTo(Math.PI / 2, within(0.0001));
  }

  @Test
  void acosNegativeOne() {
    final ResultSet result = database.command("opencypher", "RETURN acos(-1.0) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Number numResult = (Number) result.next().getProperty("result");
    assertThat(numResult.doubleValue()).isCloseTo(Math.PI, within(0.0001));
  }

  @Test
  void acosOutOfRangeReturnsNaN() {
    ResultSet result = database.command("opencypher", "RETURN acos(2.0) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat((Double) result.next().getProperty("result")).isNaN();

    result = database.command("opencypher", "RETURN acos(-2.0) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat((Double) result.next().getProperty("result")).isNaN();
  }

  @Test
  void acosNull() {
    final ResultSet result = database.command("opencypher", "RETURN acos(null) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  // ==================== asin() Tests ====================

  @Test
  void asinBasic() {
    final ResultSet result = database.command("opencypher", "RETURN asin(1.0) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Number numResult = (Number) result.next().getProperty("result");
    assertThat(numResult.doubleValue()).isCloseTo(Math.PI / 2, within(0.0001));
  }

  @Test
  void asinZero() {
    final ResultSet result = database.command("opencypher", "RETURN asin(0.0) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Number numResult = (Number) result.next().getProperty("result");
    assertThat(numResult.doubleValue()).isCloseTo(0.0, within(0.0001));
  }

  @Test
  void asinNegativeOne() {
    final ResultSet result = database.command("opencypher", "RETURN asin(-1.0) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Number numResult = (Number) result.next().getProperty("result");
    assertThat(numResult.doubleValue()).isCloseTo(-Math.PI / 2, within(0.0001));
  }

  @Test
  void asinOutOfRangeReturnsNaN() {
    ResultSet result = database.command("opencypher", "RETURN asin(2.0) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat((Double) result.next().getProperty("result")).isNaN();

    result = database.command("opencypher", "RETURN asin(-2.0) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat((Double) result.next().getProperty("result")).isNaN();
  }

  @Test
  void asinNull() {
    final ResultSet result = database.command("opencypher", "RETURN asin(null) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  // ==================== atan() Tests ====================

  @Test
  void atanBasic() {
    final ResultSet result = database.command("opencypher", "RETURN atan(1.0) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Number numResult = (Number) result.next().getProperty("result");
    assertThat(numResult.doubleValue()).isCloseTo(Math.PI / 4, within(0.0001));
  }

  @Test
  void atanZero() {
    final ResultSet result = database.command("opencypher", "RETURN atan(0.0) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Number numResult = (Number) result.next().getProperty("result");
    assertThat(numResult.doubleValue()).isCloseTo(0.0, within(0.0001));
  }

  @Test
  void atanNegative() {
    final ResultSet result = database.command("opencypher", "RETURN atan(-1.0) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Number numResult = (Number) result.next().getProperty("result");
    assertThat(numResult.doubleValue()).isCloseTo(-Math.PI / 4, within(0.0001));
  }

  @Test
  void atanNull() {
    final ResultSet result = database.command("opencypher", "RETURN atan(null) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  // ==================== atan2() Tests ====================

  @Test
  void atan2Basic() {
    final ResultSet result = database.command("opencypher", "RETURN atan2(1.0, 1.0) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Number numResult = (Number) result.next().getProperty("result");
    assertThat(numResult.doubleValue()).isCloseTo(Math.PI / 4, within(0.0001));
  }

  @Test
  void atan2Quadrants() {
    ResultSet result = database.command("opencypher", "RETURN atan2(1.0, 1.0) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Number numResult = (Number) result.next().getProperty("result");
    assertThat(numResult.doubleValue()).isCloseTo(Math.PI / 4, within(0.0001));

    result = database.command("opencypher", "RETURN atan2(1.0, -1.0) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    numResult = (Number) result.next().getProperty("result");
    assertThat(numResult.doubleValue()).isCloseTo(3 * Math.PI / 4, within(0.0001));

    result = database.command("opencypher", "RETURN atan2(-1.0, -1.0) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    numResult = (Number) result.next().getProperty("result");
    assertThat(numResult.doubleValue()).isCloseTo(-3 * Math.PI / 4, within(0.0001));

    result = database.command("opencypher", "RETURN atan2(-1.0, 1.0) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    numResult = (Number) result.next().getProperty("result");
    assertThat(numResult.doubleValue()).isCloseTo(-Math.PI / 4, within(0.0001));
  }

  @Test
  void atan2Null() {
    ResultSet result = database.command("opencypher", "RETURN atan2(null, 1.0) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();

    result = database.command("opencypher", "RETURN atan2(1.0, null) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  // ==================== cos() Tests ====================

  @Test
  void cosBasic() {
    final ResultSet result = database.command("opencypher", "RETURN cos(0.0) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Number numResult = (Number) result.next().getProperty("result");
    assertThat(numResult.doubleValue()).isCloseTo(1.0, within(0.0001));
  }

  @Test
  void cosPi() {
    final ResultSet result = database.command("opencypher", "RETURN cos(pi()) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Number numResult = (Number) result.next().getProperty("result");
    assertThat(numResult.doubleValue()).isCloseTo(-1.0, within(0.0001));
  }

  @Test
  void cosPiOver2() {
    final ResultSet result = database.command("opencypher", "RETURN cos(pi() / 2) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Number numResult = (Number) result.next().getProperty("result");
    assertThat(numResult.doubleValue()).isCloseTo(0.0, within(0.0001));
  }

  @Test
  void cosNull() {
    final ResultSet result = database.command("opencypher", "RETURN cos(null) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  // ==================== cosh() Tests ====================

  @Test
  void coshZero() {
    final ResultSet result = database.command("opencypher", "RETURN cosh(0.0) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Number numResult = (Number) result.next().getProperty("result");
    assertThat(numResult.doubleValue()).isCloseTo(1.0, within(0.0001));
  }

  @Test
  void coshSymmetry() {
    final ResultSet result = database.command("opencypher", "RETURN cosh(2.0) AS x, cosh(-2.0) AS y");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final var row = result.next();
    final Double x = (Double) row.getProperty("x");
    final Double y = (Double) row.getProperty("y");
    assertThat(x).isCloseTo(y, within(0.0001));
  }

  @Test
  void coshNull() {
    final ResultSet result = database.command("opencypher", "RETURN cosh(null) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  // ==================== cot() Tests ====================

  @Test
  void cotBasic() {
    final ResultSet result = database.command("opencypher", "RETURN cot(pi() / 4) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Number numResult = (Number) result.next().getProperty("result");
    assertThat(numResult.doubleValue()).isCloseTo(1.0, within(0.001));
  }

  @Test
  void cotZeroReturnsInfinity() {
    final ResultSet result = database.command("opencypher", "RETURN cot(0.0) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final Double value = (Double) result.next().getProperty("result");
    assertThat(value.isInfinite()).isTrue();
  }

  @Test
  void cotNull() {
    final ResultSet result = database.command("opencypher", "RETURN cot(null) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  // ==================== coth() Tests ====================

  @Test
  void cothBasic() {
    final ResultSet result = database.command("opencypher", "RETURN coth(1.0) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat((Double) result.next().getProperty("result")).isNotNull();
  }

  @Test
  void cothZeroReturnsNaN() {
    final ResultSet result = database.command("opencypher", "RETURN coth(0.0) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    assertThat((Double) result.next().getProperty("result")).isNaN();
  }

  @Test
  void cothNull() {
    final ResultSet result = database.command("opencypher", "RETURN coth(null) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  // ==================== degrees() Tests ====================

  @Test
  void degreesFromPi() {
    final ResultSet result = database.command("opencypher", "RETURN degrees(pi()) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Number numResult = (Number) result.next().getProperty("result");
    assertThat(numResult.doubleValue()).isCloseTo(180.0, within(0.0001));
  }

  @Test
  void degreesFromZero() {
    final ResultSet result = database.command("opencypher", "RETURN degrees(0.0) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Number numResult = (Number) result.next().getProperty("result");
    assertThat(numResult.doubleValue()).isCloseTo(0.0, within(0.0001));
  }

  @Test
  void degreesFrom2Pi() {
    final ResultSet result = database.command("opencypher", "RETURN degrees(2 * pi()) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Number numResult = (Number) result.next().getProperty("result");
    assertThat(numResult.doubleValue()).isCloseTo(360.0, within(0.001));
  }

  @Test
  void degreesNull() {
    final ResultSet result = database.command("opencypher", "RETURN degrees(null) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  // ==================== haversin() Tests ====================

  @Test
  void haversinZero() {
    final ResultSet result = database.command("opencypher", "RETURN haversin(0.0) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Number numResult = (Number) result.next().getProperty("result");
    assertThat(numResult.doubleValue()).isCloseTo(0.0, within(0.0001));
  }

  @Test
  void haversinPi() {
    final ResultSet result = database.command("opencypher", "RETURN haversin(pi()) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Number numResult = (Number) result.next().getProperty("result");
    assertThat(numResult.doubleValue()).isCloseTo(1.0, within(0.0001));
  }

  @Test
  void haversinNull() {
    final ResultSet result = database.command("opencypher", "RETURN haversin(null) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  // ==================== pi() Tests ====================

  @Test
  void piBasic() {
    final ResultSet result = database.command("opencypher", "RETURN pi() AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Number numResult = (Number) result.next().getProperty("result");
    assertThat(numResult.doubleValue()).isCloseTo(Math.PI, within(0.0000001));
  }

  @Test
  void piConstant() {
    final ResultSet result = database.command("opencypher", "RETURN pi() AS p1, pi() AS p2");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final var row = result.next();
    assertThat((Double) row.getProperty("p1")).isEqualTo((Double) row.getProperty("p2"));
  }

  // ==================== radians() Tests ====================

  @Test
  void radiansFrom180() {
    final ResultSet result = database.command("opencypher", "RETURN radians(180.0) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Number numResult = (Number) result.next().getProperty("result");
    assertThat(numResult.doubleValue()).isCloseTo(Math.PI, within(0.0001));
  }

  @Test
  void radiansFromZero() {
    final ResultSet result = database.command("opencypher", "RETURN radians(0.0) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Number numResult = (Number) result.next().getProperty("result");
    assertThat(numResult.doubleValue()).isCloseTo(0.0, within(0.0001));
  }

  @Test
  void radiansFrom360() {
    final ResultSet result = database.command("opencypher", "RETURN radians(360.0) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Number numResult = (Number) result.next().getProperty("result");
    assertThat(numResult.doubleValue()).isCloseTo(2 * Math.PI, within(0.001));
  }

  @Test
  void radiansNull() {
    final ResultSet result = database.command("opencypher", "RETURN radians(null) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  // ==================== sin() Tests ====================

  @Test
  void sinZero() {
    final ResultSet result = database.command("opencypher", "RETURN sin(0.0) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Number numResult = (Number) result.next().getProperty("result");
    assertThat(numResult.doubleValue()).isCloseTo(0.0, within(0.0001));
  }

  @Test
  void sinPiOver2() {
    final ResultSet result = database.command("opencypher", "RETURN sin(pi() / 2) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Number numResult = (Number) result.next().getProperty("result");
    assertThat(numResult.doubleValue()).isCloseTo(1.0, within(0.0001));
  }

  @Test
  void sinPi() {
    final ResultSet result = database.command("opencypher", "RETURN sin(pi()) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Number numResult = (Number) result.next().getProperty("result");
    assertThat(numResult.doubleValue()).isCloseTo(0.0, within(0.0001));
  }

  @Test
  void sinNull() {
    final ResultSet result = database.command("opencypher", "RETURN sin(null) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  // ==================== sinh() Tests ====================

  @Test
  void sinhZero() {
    final ResultSet result = database.command("opencypher", "RETURN sinh(0.0) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Number numResult = (Number) result.next().getProperty("result");
    assertThat(numResult.doubleValue()).isCloseTo(0.0, within(0.0001));
  }

  @Test
  void sinhAntisymmetry() {
    final ResultSet result = database.command("opencypher", "RETURN sinh(2.0) AS x, sinh(-2.0) AS y");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final var row = result.next();
    final Double x = (Double) row.getProperty("x");
    final Double y = (Double) row.getProperty("y");
    assertThat(x).isCloseTo(-y, within(0.0001));
  }

  @Test
  void sinhNull() {
    final ResultSet result = database.command("opencypher", "RETURN sinh(null) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  // ==================== tan() Tests ====================

  @Test
  void tanZero() {
    final ResultSet result = database.command("opencypher", "RETURN tan(0.0) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Number numResult = (Number) result.next().getProperty("result");
    assertThat(numResult.doubleValue()).isCloseTo(0.0, within(0.0001));
  }

  @Test
  void tanPiOver4() {
    final ResultSet result = database.command("opencypher", "RETURN tan(pi() / 4) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Number numResult = (Number) result.next().getProperty("result");
    assertThat(numResult.doubleValue()).isCloseTo(1.0, within(0.001));
  }

  @Test
  void tanNull() {
    final ResultSet result = database.command("opencypher", "RETURN tan(null) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  // ==================== tanh() Tests ====================

  @Test
  void tanhZero() {
    final ResultSet result = database.command("opencypher", "RETURN tanh(0.0) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Number numResult = (Number) result.next().getProperty("result");
    assertThat(numResult.doubleValue()).isCloseTo(0.0, within(0.0001));
  }

  @Test
  void tanhLargePositive() {
    final ResultSet result = database.command("opencypher", "RETURN tanh(10.0) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Number numResult = (Number) result.next().getProperty("result");
    assertThat(numResult.doubleValue()).isCloseTo(1.0, within(0.001));
  }

  @Test
  void tanhLargeNegative() {
    final ResultSet result = database.command("opencypher", "RETURN tanh(-10.0) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Number numResult = (Number) result.next().getProperty("result");
    assertThat(numResult.doubleValue()).isCloseTo(-1.0, within(0.001));
  }

  @Test
  void tanhNull() {
    final ResultSet result = database.command("opencypher", "RETURN tanh(null) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Assertions.assertThat(result.next().getProperty("result") == null).isTrue();
  }

  // ==================== Combined/Integration Tests ====================

  @Test
  void trigIdentitySinCos() {
    // sin²(x) + cos²(x) = 1
    final ResultSet result = database.command("opencypher",
        "WITH pi() / 3 AS x RETURN sin(x) * sin(x) + cos(x) * cos(x) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Number numResult = (Number) result.next().getProperty("result");
    assertThat(numResult.doubleValue()).isCloseTo(1.0, within(0.0001));
  }

  @Test
  void degreesRadiansRoundtrip() {
    final ResultSet result = database.command("opencypher",
        "RETURN degrees(radians(90.0)) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Number numResult = (Number) result.next().getProperty("result");
    assertThat(numResult.doubleValue()).isCloseTo(90.0, within(0.001));
  }

  @Test
  void hyperbolicIdentity() {
    // cosh²(x) - sinh²(x) = 1
    final ResultSet result = database.command("opencypher",
        "WITH 1.5 AS x RETURN cosh(x) * cosh(x) - sinh(x) * sinh(x) AS result");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    Number numResult = (Number) result.next().getProperty("result");
    assertThat(numResult.doubleValue()).isCloseTo(1.0, within(0.0001));
  }

  @Test
  void tanhIdentity() {
    // tanh(x) = sinh(x) / cosh(x)
    final ResultSet result = database.command("opencypher",
        "WITH 1.5 AS x RETURN tanh(x) AS tanh_val, sinh(x) / cosh(x) AS ratio");
    Assertions.assertThat(result.hasNext() != false).isTrue();
    final var row = result.next();
    final Double tanhVal = (Double) row.getProperty("tanh_val");
    final Double ratio = (Double) row.getProperty("ratio");
    assertThat(tanhVal).isCloseTo(ratio, within(0.0001));
  }
}
