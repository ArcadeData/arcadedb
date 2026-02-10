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
import static org.assertj.core.api.Assertions.within;

/**
 * Tests for newly added Cypher functions: isNaN, cosh, sinh, tanh, cot, coth, isEmpty.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class OpenCypherNewFunctionsTest {
  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./databases/test-new-functions").create();
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  // ============ isNaN() tests ============

  @Test
  void testIsNaNWithNaN() {
    try (final ResultSet rs = database.command("opencypher", "RETURN isNaN(0.0 / 0.0) AS result")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat((Boolean) row.getProperty("result")).isTrue();
    }
  }

  @Test
  void testIsNaNWithRegularNumber() {
    try (final ResultSet rs = database.command("opencypher", "RETURN isNaN(42) AS result")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat((Boolean) row.getProperty("result")).isFalse();
    }
  }

  @Test
  void testIsNaNWithFloat() {
    try (final ResultSet rs = database.command("opencypher", "RETURN isNaN(3.14) AS result")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat((Boolean) row.getProperty("result")).isFalse();
    }
  }

  @Test
  void testIsNaNWithNull() {
    try (final ResultSet rs = database.command("opencypher", "RETURN isNaN(null) AS result")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat((Object) row.getProperty("result")).isNull();
    }
  }

  // ============ cosh() tests ============

  @Test
  void testCoshZero() {
    try (final ResultSet rs = database.command("opencypher", "RETURN cosh(0) AS result")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat(((Number) row.getProperty("result")).doubleValue()).isCloseTo(1.0, within(0.0001));
    }
  }

  @Test
  void testCoshSymmetry() {
    // cosh(x) = cosh(-x)
    try (final ResultSet rs = database.command("opencypher", "RETURN cosh(2.0) AS pos, cosh(-2.0) AS neg")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat(((Number) row.getProperty("pos")).doubleValue())
          .isCloseTo(((Number) row.getProperty("neg")).doubleValue(), within(0.0001));
    }
  }

  @Test
  void testCoshNull() {
    try (final ResultSet rs = database.command("opencypher", "RETURN cosh(null) AS result")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat((Object) rs.next().getProperty("result")).isNull();
    }
  }

  // ============ sinh() tests ============

  @Test
  void testSinhZero() {
    try (final ResultSet rs = database.command("opencypher", "RETURN sinh(0) AS result")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat(((Number) row.getProperty("result")).doubleValue()).isCloseTo(0.0, within(0.0001));
    }
  }

  @Test
  void testSinhAntisymmetry() {
    // sinh(-x) = -sinh(x)
    try (final ResultSet rs = database.command("opencypher", "RETURN sinh(2.0) AS pos, sinh(-2.0) AS neg")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat(((Number) row.getProperty("pos")).doubleValue())
          .isCloseTo(-((Number) row.getProperty("neg")).doubleValue(), within(0.0001));
    }
  }

  @Test
  void testSinhNull() {
    try (final ResultSet rs = database.command("opencypher", "RETURN sinh(null) AS result")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat((Object) rs.next().getProperty("result")).isNull();
    }
  }

  // ============ tanh() tests ============

  @Test
  void testTanhZero() {
    try (final ResultSet rs = database.command("opencypher", "RETURN tanh(0) AS result")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat(((Number) row.getProperty("result")).doubleValue()).isCloseTo(0.0, within(0.0001));
    }
  }

  @Test
  void testTanhApproachesOne() {
    try (final ResultSet rs = database.command("opencypher", "RETURN tanh(10) AS result")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat(((Number) row.getProperty("result")).doubleValue()).isCloseTo(1.0, within(0.001));
    }
  }

  @Test
  void testTanhNull() {
    try (final ResultSet rs = database.command("opencypher", "RETURN tanh(null) AS result")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat((Object) rs.next().getProperty("result")).isNull();
    }
  }

  // ============ cot() tests ============

  @Test
  void testCotPiOver4() {
    // cot(pi/4) = 1
    try (final ResultSet rs = database.command("opencypher", "RETURN cot(pi() / 4) AS result")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat(((Number) row.getProperty("result")).doubleValue()).isCloseTo(1.0, within(0.0001));
    }
  }

  @Test
  void testCotPiOver2() {
    // cot(pi/2) ≈ 0
    try (final ResultSet rs = database.command("opencypher", "RETURN cot(pi() / 2) AS result")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat(((Number) row.getProperty("result")).doubleValue()).isCloseTo(0.0, within(0.0001));
    }
  }

  @Test
  void testCotNull() {
    try (final ResultSet rs = database.command("opencypher", "RETURN cot(null) AS result")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat((Object) rs.next().getProperty("result")).isNull();
    }
  }

  // ============ coth() tests ============

  @Test
  void testCothValue() {
    // coth(x) = cosh(x) / sinh(x)
    try (final ResultSet rs = database.command("opencypher", "RETURN coth(2.0) AS result")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      final double expected = Math.cosh(2.0) / Math.sinh(2.0);
      assertThat(((Number) row.getProperty("result")).doubleValue()).isCloseTo(expected, within(0.0001));
    }
  }

  @Test
  void testCothNull() {
    try (final ResultSet rs = database.command("opencypher", "RETURN coth(null) AS result")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat((Object) rs.next().getProperty("result")).isNull();
    }
  }

  // ============ isEmpty() tests ============

  @Test
  void testIsEmptyWithEmptyList() {
    try (final ResultSet rs = database.command("opencypher", "RETURN isEmpty([]) AS result")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat((Boolean) row.getProperty("result")).isTrue();
    }
  }

  @Test
  void testIsEmptyWithNonEmptyList() {
    try (final ResultSet rs = database.command("opencypher", "RETURN isEmpty([1, 2, 3]) AS result")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat((Boolean) row.getProperty("result")).isFalse();
    }
  }

  @Test
  void testIsEmptyWithEmptyString() {
    try (final ResultSet rs = database.command("opencypher", "RETURN isEmpty('') AS result")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat((Boolean) row.getProperty("result")).isTrue();
    }
  }

  @Test
  void testIsEmptyWithNonEmptyString() {
    try (final ResultSet rs = database.command("opencypher", "RETURN isEmpty('hello') AS result")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat((Boolean) row.getProperty("result")).isFalse();
    }
  }

  @Test
  void testIsEmptyWithEmptyMap() {
    try (final ResultSet rs = database.command("opencypher", "RETURN isEmpty({}) AS result")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat((Boolean) row.getProperty("result")).isTrue();
    }
  }

  @Test
  void testIsEmptyWithNonEmptyMap() {
    try (final ResultSet rs = database.command("opencypher", "RETURN isEmpty({key: 'value'}) AS result")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat((Boolean) row.getProperty("result")).isFalse();
    }
  }

  @Test
  void testIsEmptyWithNull() {
    try (final ResultSet rs = database.command("opencypher", "RETURN isEmpty(null) AS result")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat((Object) rs.next().getProperty("result")).isNull();
    }
  }

  // ============ Hyperbolic identity tests ============

  @Test
  void testHyperbolicIdentity() {
    // cosh^2(x) - sinh^2(x) = 1
    try (final ResultSet rs = database.command("opencypher",
        "WITH 1.5 AS x RETURN cosh(x) * cosh(x) - sinh(x) * sinh(x) AS result")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat(((Number) row.getProperty("result")).doubleValue()).isCloseTo(1.0, within(0.0001));
    }
  }

  @Test
  void testTanhIdentity() {
    // tanh(x) = sinh(x) / cosh(x)
    try (final ResultSet rs = database.command("opencypher",
        "WITH 1.5 AS x RETURN tanh(x) AS t, sinh(x) / cosh(x) AS ratio")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat(((Number) row.getProperty("t")).doubleValue())
          .isCloseTo(((Number) row.getProperty("ratio")).doubleValue(), within(0.0001));
    }
  }
}
