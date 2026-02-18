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

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Tests for newly added OpenCypher functions: trigonometric, logarithmic, string, type conversion OrNull variants,
 * and scalar functions (nullIf, valueType).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class OpenCypherMissingFunctionsTest extends TestHelper {

  // ===================== TRIGONOMETRIC FUNCTIONS =====================

  @Test
  void sin() {
    try (final ResultSet rs = database.command("opencypher", "RETURN sin(0) AS val")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(((Number) rs.next().getProperty("val")).doubleValue()).isCloseTo(0.0, within(1e-10));
    }
  }

  @Test
  void cos() {
    try (final ResultSet rs = database.command("opencypher", "RETURN cos(0) AS val")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(((Number) rs.next().getProperty("val")).doubleValue()).isCloseTo(1.0, within(1e-10));
    }
  }

  @Test
  void tan() {
    try (final ResultSet rs = database.command("opencypher", "RETURN tan(0) AS val")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(((Number) rs.next().getProperty("val")).doubleValue()).isCloseTo(0.0, within(1e-10));
    }
  }

  @Test
  void asin() {
    try (final ResultSet rs = database.command("opencypher", "RETURN asin(1) AS val")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(((Number) rs.next().getProperty("val")).doubleValue()).isCloseTo(Math.PI / 2, within(1e-10));
    }
  }

  @Test
  void acos() {
    try (final ResultSet rs = database.command("opencypher", "RETURN acos(1) AS val")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(((Number) rs.next().getProperty("val")).doubleValue()).isCloseTo(0.0, within(1e-10));
    }
  }

  @Test
  void atan() {
    try (final ResultSet rs = database.command("opencypher", "RETURN atan(0) AS val")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(((Number) rs.next().getProperty("val")).doubleValue()).isCloseTo(0.0, within(1e-10));
    }
  }

  @Test
  void atan2() {
    try (final ResultSet rs = database.command("opencypher", "RETURN atan2(1, 1) AS val")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(((Number) rs.next().getProperty("val")).doubleValue()).isCloseTo(Math.PI / 4, within(1e-10));
    }
  }

  @Test
  void degrees() {
    try (final ResultSet rs = database.command("opencypher", "RETURN degrees(3.141592653589793) AS val")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(((Number) rs.next().getProperty("val")).doubleValue()).isCloseTo(180.0, within(1e-10));
    }
  }

  @Test
  void radians() {
    try (final ResultSet rs = database.command("opencypher", "RETURN radians(180) AS val")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(((Number) rs.next().getProperty("val")).doubleValue()).isCloseTo(Math.PI, within(1e-10));
    }
  }

  @Test
  void haversin() {
    // haversin(0) = (1 - cos(0)) / 2 = (1 - 1) / 2 = 0
    try (final ResultSet rs = database.command("opencypher", "RETURN haversin(0) AS val")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(((Number) rs.next().getProperty("val")).doubleValue()).isCloseTo(0.0, within(1e-10));
    }
  }

  @Test
  void haversinPi() {
    // haversin(pi) = (1 - cos(pi)) / 2 = (1 - (-1)) / 2 = 1
    try (final ResultSet rs = database.command("opencypher", "RETURN haversin(pi()) AS val")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(((Number) rs.next().getProperty("val")).doubleValue()).isCloseTo(1.0, within(1e-10));
    }
  }

  // ===================== LOGARITHMIC FUNCTIONS =====================

  @Test
  void exp() {
    try (final ResultSet rs = database.command("opencypher", "RETURN exp(0) AS val")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(((Number) rs.next().getProperty("val")).doubleValue()).isCloseTo(1.0, within(1e-10));
    }
  }

  @Test
  void expE() {
    try (final ResultSet rs = database.command("opencypher", "RETURN exp(1) AS val")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(((Number) rs.next().getProperty("val")).doubleValue()).isCloseTo(Math.E, within(1e-10));
    }
  }

  @Test
  void log() {
    try (final ResultSet rs = database.command("opencypher", "RETURN log(e()) AS val")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(((Number) rs.next().getProperty("val")).doubleValue()).isCloseTo(1.0, within(1e-10));
    }
  }

  @Test
  void log10() {
    try (final ResultSet rs = database.command("opencypher", "RETURN log10(100) AS val")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(((Number) rs.next().getProperty("val")).doubleValue()).isCloseTo(2.0, within(1e-10));
    }
  }

  // ===================== NULL HANDLING FOR MATH =====================

  @Test
  void trigFunctionsWithNull() {
    try (final ResultSet rs = database.command("opencypher", "RETURN sin(null) AS val")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat((Object) rs.next().getProperty("val")).isNull();
    }
  }

  // ===================== STRING FUNCTIONS =====================

  @Test
  void trim() {
    try (final ResultSet rs = database.command("opencypher", "RETURN trim('  hello  ') AS val")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<String>getProperty("val")).isEqualTo("hello");
    }
  }

  @Test
  void trimNull() {
    try (final ResultSet rs = database.command("opencypher", "RETURN trim(null) AS val")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat((Object) rs.next().getProperty("val")).isNull();
    }
  }

  @Test
  void trimLeading() {
    try (final ResultSet rs = database.command("opencypher", "RETURN trim(LEADING 'x' FROM 'xxhelloxx') AS val")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<String>getProperty("val")).isEqualTo("helloxx");
    }
  }

  @Test
  void trimTrailing() {
    try (final ResultSet rs = database.command("opencypher", "RETURN trim(TRAILING 'x' FROM 'xxhelloxx') AS val")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<String>getProperty("val")).isEqualTo("xxhello");
    }
  }

  @Test
  void trimBoth() {
    try (final ResultSet rs = database.command("opencypher", "RETURN trim(BOTH 'x' FROM 'xxhelloxx') AS val")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<String>getProperty("val")).isEqualTo("hello");
    }
  }

  @Test
  void trimAllThree() {
    try (final ResultSet rs = database.command("opencypher",
        "RETURN trim(LEADING 'x' FROM 'xxhelloxx') AS lead, trim(TRAILING 'x' FROM 'xxhelloxx') AS trail, trim(BOTH 'x' FROM 'xxhelloxx') AS both")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat(row.<String>getProperty("lead")).isEqualTo("helloxx");
      assertThat(row.<String>getProperty("trail")).isEqualTo("xxhello");
      assertThat(row.<String>getProperty("both")).isEqualTo("hello");
    }
  }

  @Test
  void trimDefaultFromSyntax() {
    // trim(FROM 'xxhelloxx') should trim whitespace (FROM without mode or character)
    try (final ResultSet rs = database.command("opencypher", "RETURN trim(BOTH FROM '  hello  ') AS val")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<String>getProperty("val")).isEqualTo("hello");
    }
  }

  @Test
  void replace() {
    try (final ResultSet rs = database.command("opencypher", "RETURN replace('hello world', 'world', 'cypher') AS val")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<String>getProperty("val")).isEqualTo("hello cypher");
    }
  }

  @Test
  void replaceMultipleOccurrences() {
    try (final ResultSet rs = database.command("opencypher", "RETURN replace('abcabc', 'a', 'x') AS val")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<String>getProperty("val")).isEqualTo("xbcxbc");
    }
  }

  @Test
  void charLength() {
    try (final ResultSet rs = database.command("opencypher", "RETURN char_length('hello') AS val")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(((Number) rs.next().getProperty("val")).longValue()).isEqualTo(5L);
    }
  }

  @Test
  void characterLength() {
    try (final ResultSet rs = database.command("opencypher", "RETURN character_length('hello') AS val")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(((Number) rs.next().getProperty("val")).longValue()).isEqualTo(5L);
    }
  }

  // ===================== TYPE CONVERSION *OrNull FUNCTIONS =====================

  @Test
  void toIntegerOrNullWithValidInt() {
    try (final ResultSet rs = database.command("opencypher", "RETURN toIntegerOrNull('42') AS val")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(((Number) rs.next().getProperty("val")).longValue()).isEqualTo(42L);
    }
  }

  @Test
  void toIntegerOrNullWithInvalidInput() {
    try (final ResultSet rs = database.command("opencypher", "RETURN toIntegerOrNull('abc') AS val")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat((Object) rs.next().getProperty("val")).isNull();
    }
  }

  @Test
  void toIntegerOrNullWithBoolean() {
    // In Neo4j, toIntegerOrNull(true) returns null because booleans are not valid for toInteger
    // However, ArcadeDB's toInteger supports booleans, so this should return 1
    try (final ResultSet rs = database.command("opencypher", "RETURN toIntegerOrNull(true) AS val")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(((Number) rs.next().getProperty("val")).longValue()).isEqualTo(1L);
    }
  }

  @Test
  void toFloatOrNullValid() {
    try (final ResultSet rs = database.command("opencypher", "RETURN toFloatOrNull('3.14') AS val")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(((Number) rs.next().getProperty("val")).doubleValue()).isCloseTo(3.14, within(0.001));
    }
  }

  @Test
  void toFloatOrNullInvalid() {
    try (final ResultSet rs = database.command("opencypher", "RETURN toFloatOrNull('not_a_number') AS val")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat((Object) rs.next().getProperty("val")).isNull();
    }
  }

  @Test
  void toBooleanOrNullValid() {
    try (final ResultSet rs = database.command("opencypher", "RETURN toBooleanOrNull('true') AS val")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Boolean>getProperty("val")).isTrue();
    }
  }

  @Test
  void toBooleanOrNullFromInteger() {
    // toBoolean() supports integers: 0 → false, non-zero → true (issue #3418)
    try (final ResultSet rs = database.command("opencypher", "RETURN toBooleanOrNull(42) AS val")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Boolean>getProperty("val")).isTrue();
    }
  }

  @Test
  void toBooleanOrNullInvalid() {
    // A list is not convertible to boolean, so toBooleanOrNull should return null
    try (final ResultSet rs = database.command("opencypher", "RETURN toBooleanOrNull([1,2,3]) AS val")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat((Object) rs.next().getProperty("val")).isNull();
    }
  }

  @Test
  void toStringOrNullValid() {
    try (final ResultSet rs = database.command("opencypher", "RETURN toStringOrNull(42) AS val")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<String>getProperty("val")).isEqualTo("42");
    }
  }

  @Test
  void toStringOrNullNull() {
    try (final ResultSet rs = database.command("opencypher", "RETURN toStringOrNull(null) AS val")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat((Object) rs.next().getProperty("val")).isNull();
    }
  }

  // ===================== SCALAR FUNCTIONS =====================

  @Test
  void nullIfEqual() {
    try (final ResultSet rs = database.command("opencypher", "RETURN nullIf(1, 1) AS val")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat((Object) rs.next().getProperty("val")).isNull();
    }
  }

  @Test
  void nullIfNotEqual() {
    try (final ResultSet rs = database.command("opencypher", "RETURN nullIf(1, 2) AS val")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(((Number) rs.next().getProperty("val")).intValue()).isEqualTo(1);
    }
  }

  @Test
  void valueTypeInteger() {
    try (final ResultSet rs = database.command("opencypher", "RETURN valueType(42) AS val")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<String>getProperty("val")).isEqualTo("INTEGER");
    }
  }

  @Test
  void valueTypeFloat() {
    try (final ResultSet rs = database.command("opencypher", "RETURN valueType(3.14) AS val")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<String>getProperty("val")).isEqualTo("FLOAT");
    }
  }

  @Test
  void valueTypeString() {
    try (final ResultSet rs = database.command("opencypher", "RETURN valueType('hello') AS val")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<String>getProperty("val")).isEqualTo("STRING");
    }
  }

  @Test
  void valueTypeBoolean() {
    try (final ResultSet rs = database.command("opencypher", "RETURN valueType(true) AS val")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<String>getProperty("val")).isEqualTo("BOOLEAN");
    }
  }

  @Test
  void valueTypeNull() {
    try (final ResultSet rs = database.command("opencypher", "RETURN valueType(null) AS val")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<String>getProperty("val")).isEqualTo("NULL");
    }
  }

  @Test
  void valueTypeList() {
    try (final ResultSet rs = database.command("opencypher", "RETURN valueType([1,2,3]) AS val")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<String>getProperty("val")).isEqualTo("LIST<ANY>");
    }
  }

  // ===================== COMBINED / INTEGRATION TESTS =====================

  @Test
  void degreesRadiansRoundTrip() {
    // degrees(radians(90)) should be 90
    try (final ResultSet rs = database.command("opencypher", "RETURN degrees(radians(90)) AS val")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(((Number) rs.next().getProperty("val")).doubleValue()).isCloseTo(90.0, within(1e-10));
    }
  }

  @Test
  void expLogRoundTrip() {
    // log(exp(5)) should be 5
    try (final ResultSet rs = database.command("opencypher", "RETURN log(exp(5)) AS val")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(((Number) rs.next().getProperty("val")).doubleValue()).isCloseTo(5.0, within(1e-10));
    }
  }

  @Test
  void sinCosPythagorean() {
    // sin^2(x) + cos^2(x) = 1
    try (final ResultSet rs = database.command("opencypher", "RETURN sin(1.5)*sin(1.5) + cos(1.5)*cos(1.5) AS val")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(((Number) rs.next().getProperty("val")).doubleValue()).isCloseTo(1.0, within(1e-10));
    }
  }
}
