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
package com.arcadedb.function.sql.math;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.ArithmeticErrorException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

/**
 * Issue #11047: {@code pow()} answered NULL for every negative base and narrowed its result back into the width of its
 * input ({@code pow(s, 3)} on a SHORT holding 300 answered -832, {@code pow(2, 31)} answered 2147483647), read its exponent
 * through a saturating int read ({@code pow(2, 2.5)} answered 4) and let a bare JDK {@code ArithmeticException} escape for a
 * DECIMAL base and a negative exponent. {@code sqrt()} narrowed the same way ({@code sqrt(2)} answered 1).
 * <p>
 * The guarantee: the answer of {@code pow()} and {@code sqrt()} is a function of the VALUES of their arguments, not of the Java
 * width the base happened to arrive in.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11047PowSqrtValueTest extends TestHelper {

  @Test
  void negativeBaseIsDefined() {
    assertThat(single("SELECT pow(-2, 2) AS v")).isEqualTo(4);
    assertThat(single("SELECT pow(-2, 3) AS v")).isEqualTo(-8);
    assertThat(single("SELECT pow(-2.0, 2) AS v")).isEqualTo(4.0);
    assertThat(single("SELECT pow(-2.0, 3) AS v")).isEqualTo(-8.0);
    // A negative base with a fractional exponent has no real result: NULL, as sqrt() of a negative number
    assertThat(single("SELECT pow(-8, 0.5) AS v")).isNull();
  }

  @Test
  void integralResultWidensInsteadOfSaturating() {
    assertThat(single("SELECT pow(2, 31) AS v")).isEqualTo(2147483648L);
    assertThat(single("SELECT pow(2, 30) AS v")).isEqualTo(1073741824);
    assertThat(single("SELECT pow(2, 63) AS v")).isEqualTo(new BigDecimal("9223372036854775808"));
    assertThat(single("SELECT pow(10, 30) AS v")).isEqualTo(new BigDecimal("1000000000000000000000000000000"));
    assertThat(single("SELECT pow(-2, 63) AS v")).isEqualTo(Long.MIN_VALUE);
    assertThat(single("SELECT pow(0, 0) AS v")).isEqualTo(1);
    assertThat(single("SELECT pow(1, 2000000000) AS v")).isEqualTo(1);
    assertThat(single("SELECT pow(-1, 2000000001) AS v")).isEqualTo(-1);
  }

  @Test
  void exponentIsReadAsTheValueTheCallerWrote() {
    assertThat((Double) single("SELECT pow(2, 2.5) AS v")).isCloseTo(Math.pow(2, 2.5), within(1e-12));
    assertThat(single("SELECT pow(2, -1) AS v")).isEqualTo(0.5);
    assertThat(single("SELECT pow(2.0, -1) AS v")).isEqualTo(0.5);
    assertThat(single("SELECT pow(4, 0.5) AS v")).isEqualTo(2.0);
    // A whole exponent written as a decimal is a whole exponent
    assertThat(single("SELECT pow(3, 2.0) AS v")).isEqualTo(9);
  }

  @Test
  void decimalBase() {
    assertThat((BigDecimal) single("SELECT pow(2.12345678901234567890, 2) AS v")).isEqualByComparingTo(
        new BigDecimal("2.12345678901234567890").pow(2));
    assertThat((BigDecimal) single("SELECT pow(2.12345678901234567890, -1) AS v")).isEqualByComparingTo(
        BigDecimal.ONE.divide(new BigDecimal("2.12345678901234567890"), MathContext.DECIMAL128));
    // No representable answer: the typed client error of the arithmetic family, not a bare JDK exception
    assertThatThrownBy(() -> new SQLFunctionPow().execute(null, null, null, new Object[] { BigDecimal.ZERO, -1 }, null))
        .isInstanceOf(ArithmeticErrorException.class);
  }

  @Test
  void zeroToANegativePowerIsRefused() {
    // the same answer whatever the base's type and the exponent's shape: no value, a typed error rather than infinity
    assertThatThrownBy(() -> single("SELECT pow(0, -1) AS v")).isInstanceOf(ArithmeticErrorException.class);
    assertThatThrownBy(() -> single("SELECT pow(0, -1.5) AS v")).isInstanceOf(ArithmeticErrorException.class);
  }

  @Test
  void hugeResultIsRefusedNotTruncated() {
    assertThatThrownBy(() -> single("SELECT pow(2, 3000000000) AS v")).isInstanceOf(ArithmeticErrorException.class);
    // An exact integral answer with more digits than an exact decimal holds is rounded like a decimal, never wrapped
    final BigDecimal big = (BigDecimal) single("SELECT pow(7, 5000) AS v");
    assertThat(big.precision()).isEqualTo(34);
    assertThat(big.signum()).isEqualTo(1);
  }

  @Test
  void nullArgumentAnswersNull() {
    assertThat(single("SELECT pow(null, 2) AS v")).isNull();
    assertThat(single("SELECT pow(2, null) AS v")).isNull();
  }

  @Test
  void sqrtDoesNotTruncate() {
    assertThat(single("SELECT sqrt(2) AS v")).isEqualTo(Math.sqrt(2));
    assertThat(single("SELECT sqrt(4) AS v")).isEqualTo(2.0);
    assertThat(single("SELECT sqrt(-4) AS v")).isNull();
  }

  /**
   * The same base and exponent stored as each numeric property type answer the same value: 300 cubed and 300 to the fifth
   * leave SHORT and INTEGER respectively.
   */
  @Test
  void answerDoesNotDependOnTheWidthOfTheBase() {
    database.command("sql", "CREATE DOCUMENT TYPE M");
    for (final String type : new String[] { "SHORT", "INTEGER", "LONG", "FLOAT", "DOUBLE", "DECIMAL" })
      database.command("sql", "CREATE PROPERTY M.p_" + type.toLowerCase() + " " + type);
    database.transaction(() -> database.command("sql",
        "INSERT INTO M SET p_short = 300, p_integer = 300, p_long = 300, p_float = 300, p_double = 300, p_decimal = 300"));

    for (final String type : new String[] { "short", "integer", "long", "float", "double", "decimal" }) {
      final Result row = database.query("sql",
          "SELECT pow(p_" + type + ", 3) AS cube, pow(p_" + type + ", 5) AS fifth, pow(-p_" + type + ", 3) AS negCube, sqrt(p_"
              + type + ") AS root FROM M").next();
      assertThat(new BigDecimal(row.<Number>getProperty("cube").toString())).as("pow(%s 300, 3)", type)
          .isEqualByComparingTo("27000000");
      assertThat(new BigDecimal(row.<Number>getProperty("fifth").toString())).as("pow(%s 300, 5)", type)
          .isEqualByComparingTo("2430000000000");
      assertThat(new BigDecimal(row.<Number>getProperty("negCube").toString())).as("pow(-%s 300, 3)", type)
          .isEqualByComparingTo("-27000000");
      assertThat(row.<Number>getProperty("root").doubleValue()).as("sqrt(%s 300)", type).isCloseTo(Math.sqrt(300), within(1e-6));
    }
  }

  @Test
  void bigIntegerBase() {
    final SQLFunctionPow pow = new SQLFunctionPow();
    assertThat(pow.execute(null, null, null, new Object[] { BigInteger.TEN, 20 }, null)).isEqualTo(BigInteger.TEN.pow(20));
    assertThat(pow.execute(null, null, null, new Object[] { BigInteger.valueOf(-3), 3 }, null)).isEqualTo(BigInteger.valueOf(-27));
    assertThat(pow.execute(null, null, null, new Object[] { (byte) -3, 3 }, null)).isEqualTo(-27);
  }

  private Object single(final String query) {
    try (final ResultSet rs = database.query("sql", query)) {
      return rs.next().getProperty("v");
    }
  }
}
