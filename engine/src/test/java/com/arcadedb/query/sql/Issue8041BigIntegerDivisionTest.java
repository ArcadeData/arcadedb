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
package com.arcadedb.query.sql;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #8041.
 * <p>
 * The #7917 fix gave {@code MathExpression.Operator.apply(Number, Operator, Number)} a {@code BigInteger} arm in
 * every position, and every one of them meets the two operands in {@code BigDecimal} via
 * {@code new BigDecimal(BigInteger)} - which has scale 0, always. {@code SLASH.apply(BigDecimal, BigDecimal)}
 * divided with {@code left.divide(right, RoundingMode.HALF_UP)}, an overload that answers AT THE SCALE OF THE LEFT
 * OPERAND, so every inexact quotient involving a {@code BigInteger} came back rounded to a whole number:
 * {@code 1 / 2} was {@code 1} and {@code 7 / 2} was {@code 4}. Loud failure turned into a silently wrong number.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue8041BigIntegerDivisionTest extends TestHelper {

  @Test
  void bigIntegerDivisionIsNotRoundedToAWholeNumber() {
    assertThat(divide(BigInteger.ONE, BigInteger.TWO)).isEqualByComparingTo("0.5");
    assertThat(divide(BigInteger.valueOf(7), BigInteger.TWO)).isEqualByComparingTo("3.5");
    assertThat(divide(BigInteger.TEN, BigInteger.valueOf(4))).isEqualByComparingTo("2.5");
  }

  /**
   * A BigInteger on EITHER side drags the other operand into a scale-0 BigDecimal, so a plain SQL literal was
   * affected too.
   */
  @Test
  void aBigIntegerOnEitherSideAloneIsEnough() {
    assertThat(divide(BigInteger.valueOf(7), 2)).isEqualByComparingTo("3.5");
    assertThat(divide(7, BigInteger.TWO)).isEqualByComparingTo("3.5");
    assertThat(divide(BigInteger.valueOf(7), 2L)).isEqualByComparingTo("3.5");
    assertThat(divide(BigInteger.valueOf(7), (short) 2)).isEqualByComparingTo("3.5");
    assertThat(divide(BigInteger.valueOf(7), (byte) 2)).isEqualByComparingTo("3.5");
  }

  /** An inline literal division, the reference the issue measures the BigInteger arms against. */
  @Test
  void aBigIntegerQuotientAgreesWithTheSameDivisionOnPlainIntegers() {
    try (final ResultSet rs = database.query("sql", "SELECT 10 / 4 AS r")) {
      assertThat(rs.next().<Number>getProperty("r").doubleValue()).isEqualTo(2.5d);
    }
    assertThat(divide(BigInteger.TEN, BigInteger.valueOf(4)).doubleValue()).isEqualTo(2.5d);
  }

  /** An exact quotient must stay exact, and must not gain the 34 digits of the fallback precision. */
  @Test
  void anExactQuotientStaysExact() {
    assertThat(divide(BigInteger.TEN, BigInteger.valueOf(5))).isEqualByComparingTo("2");
    assertThat(divide(new BigDecimal("6.0"), new BigDecimal("3"))).isEqualByComparingTo("2");
    assertThat(divide(new BigDecimal("1.0"), new BigDecimal("2"))).isEqualByComparingTo("0.5");
  }

  /**
   * The SCALE of every division that already answered correctly is unchanged, so this stays a bug fix rather
   * than a formatting change (PR #8093 review).
   * <p>
   * The old {@code divide(right, HALF_UP)} padded as well as rounded: {@code 10.00 / 2.00} came back
   * {@code 5.00}, not {@code 5}, and an application that renders money off {@code toString()} reads that
   * difference even though the numbers compare equal. {@code isEqualByComparingTo} cannot see it, so these
   * assert {@code toPlainString()}.
   */
  @Test
  void theScaleOfAnAlreadyCorrectDivisionIsPreserved() {
    assertThat(divide(new BigDecimal("10.00"), new BigDecimal("2.00")).toPlainString()).isEqualTo("5.00");
    assertThat(divide(new BigDecimal("6.0"), new BigDecimal("3")).toPlainString()).isEqualTo("2.0");
    assertThat(divide(new BigDecimal("1.000"), new BigDecimal("4")).toPlainString()).isEqualTo("0.250");
    // ...and the scale only ever WIDENS: where the old code had to round to reach the left scale, it no longer
    // does, which is the fix itself.
    assertThat(divide(new BigDecimal("1"), new BigDecimal("4")).toPlainString()).isEqualTo("0.25");
    assertThat(divide(BigInteger.ONE, BigInteger.TWO).toPlainString()).isEqualTo("0.5");
  }

  /**
   * The only case that HAS to round. It must round at a real precision rather than at the left operand's scale,
   * which for a BigInteger is zero - the whole of this issue.
   */
  @Test
  void aNonTerminatingQuotientRoundsAtFullPrecisionRatherThanAtTheLeftScale() {
    final BigDecimal third = divide(BigInteger.ONE, BigInteger.valueOf(3));
    assertThat(third.doubleValue()).isCloseTo(1d / 3d, org.assertj.core.data.Offset.offset(1e-15));
    assertThat(third.precision()).isGreaterThan(30);
  }

  /**
   * A negative operand on either side, or both. The exact divide and the scale floor are both sign-agnostic, but
   * the rounding mode is not - HALF_UP rounds away from zero - so the sign belongs in the test rather than in an
   * assumption (PR #8093 review).
   */
  @Test
  void signIsCarriedThroughTheQuotient() {
    assertThat(divide(BigInteger.valueOf(-7), BigInteger.TWO)).isEqualByComparingTo("-3.5");
    assertThat(divide(BigInteger.valueOf(7), BigInteger.valueOf(-2))).isEqualByComparingTo("-3.5");
    assertThat(divide(BigInteger.valueOf(-7), BigInteger.valueOf(-2))).isEqualByComparingTo("3.5");
    assertThat(divide(BigInteger.valueOf(-1), BigInteger.TWO)).isEqualByComparingTo("-0.5");
    assertThat(divide(new BigDecimal("-10.00"), new BigDecimal("2.00")).toPlainString()).isEqualTo("-5.00");
    // The non-terminating fallback, mirrored about zero: -1/3 must be the negation of 1/3 to the last digit.
    assertThat(divide(BigInteger.valueOf(-1), BigInteger.valueOf(3)))
        .isEqualByComparingTo(divide(BigInteger.ONE, BigInteger.valueOf(3)).negate());
  }

  /** The other operators were never affected, and must not become so. */
  @Test
  void theOtherOperatorsAreUnchanged() {
    assertThat(apply("+", BigInteger.valueOf(7), BigInteger.TWO)).isEqualByComparingTo("9");
    assertThat(apply("-", BigInteger.valueOf(7), BigInteger.TWO)).isEqualByComparingTo("5");
    assertThat(apply("*", BigInteger.valueOf(7), BigInteger.TWO)).isEqualByComparingTo("14");
    assertThat(apply("%", BigInteger.valueOf(7), BigInteger.TWO)).isEqualByComparingTo("1");
  }

  private BigDecimal divide(final Object a, final Object b) {
    return apply("/", a, b);
  }

  private BigDecimal apply(final String operator, final Object a, final Object b) {
    try (final ResultSet rs = database.query("sql", "SELECT :a " + operator + " :b AS r", Map.of("a", a, "b", b))) {
      final Number value = rs.next().getProperty("r");
      return value instanceof BigDecimal decimal ? decimal : new BigDecimal(value.toString());
    }
  }
}
