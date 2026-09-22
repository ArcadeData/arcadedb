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
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #8165, a regression introduced by the #8041 fix (PR #8093).
 * <p>
 * That fix bounded a non-terminating quotient at {@code MathContext.DECIMAL128} - 34 significant digits - and then
 * reinstated the left operand's scale as a floor with {@code setScale()}. For a left operand carrying MORE than 34
 * fractional digits, {@code setScale()} pads the rounded value with zeros, so the answer asserts precision that was
 * never computed and is LESS accurate than the single line the fix replaced
 * ({@code left.divide(right, RoundingMode.HALF_UP)}, which answers at the left scale, correctly rounded at every
 * digit). Nothing in the result distinguishes the padding from computed precision.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue8165WideScaleDivisionTest extends TestHelper {

  /** 34 significant digits: what {@code MathContext.DECIMAL128} gives, and the point past which padding began. */
  private static final int DECIMAL128_DIGITS = MathContext.DECIMAL128.getPrecision();

  /**
   * The defect. A left operand of scale 40 divided by a divisor whose quotient does not terminate must answer 40
   * COMPUTED fractional digits, not 34 computed ones followed by 6 zeros.
   */
  @Test
  void aQuotientWiderThanDecimal128IsComputedRatherThanPaddedWithZeros() {
    final BigDecimal left = new BigDecimal("1.0000000000000000000000000000000000000000"); // scale 40
    final BigDecimal right = new BigDecimal("3");

    final BigDecimal quotient = divide(left, right);

    assertThat(quotient.scale()).as("the left operand's scale is still the floor").isEqualTo(left.scale());
    assertThat(quotient).as("and every digit of it was computed, as the pre-#8093 code computed them")
        .isEqualByComparingTo(left.divide(right, left.scale(), RoundingMode.HALF_UP));
    assertThat(quotient.toPlainString()).isEqualTo("0.3333333333333333333333333333333333333333");
    assertThat(quotient.toPlainString()).as("no fabricated trailing zeros").doesNotEndWith("000000");
  }

  /**
   * The same shape one digit either side of the DECIMAL128 boundary, so the fix is pinned at the edge rather than
   * only well past it.
   */
  @Test
  void theBoundaryBetweenTheBoundedAndTheWidenedArmIsTheLeftScale() {
    for (int scale = DECIMAL128_DIGITS - 1; scale <= DECIMAL128_DIGITS + 2; scale++) {
      final BigDecimal left = BigDecimal.ONE.setScale(scale, RoundingMode.UNNECESSARY);
      final BigDecimal quotient = divide(left, new BigDecimal("7"));

      assertThat(quotient.scale()).as("scale %d: never narrower than the left operand", scale)
          .isGreaterThanOrEqualTo(scale);
      assertThat(quotient).as("scale %d: correctly rounded at the last digit it claims", scale)
          .isEqualByComparingTo(left.divide(new BigDecimal("7"), quotient.scale(), RoundingMode.HALF_UP));
    }
  }

  /**
   * The behaviour #8093 was protecting must be untouched: an EXACT quotient keeps the left operand's scale, zeros
   * and all, because there the padding is the exact value.
   */
  @Test
  void anExactQuotientStillKeepsTheLeftOperandScale() {
    assertThat(divide(new BigDecimal("10.00"), new BigDecimal("2.00")).toPlainString()).isEqualTo("5.00");
    assertThat(divide(new BigDecimal("1.000"), new BigDecimal("4")).toPlainString()).isEqualTo("0.250");
  }

  /**
   * And a left operand that asks for LESS than DECIMAL128 gives still answers at DECIMAL128: the fallback is a
   * floor on the precision, not a cap.
   */
  @Test
  void aNarrowLeftOperandStillAnswersAtFullFallbackPrecision() {
    final BigDecimal third = divide(BigDecimal.ONE, new BigDecimal("3"));
    assertThat(third.precision()).isEqualTo(DECIMAL128_DIGITS);
    assertThat(third.toPlainString()).isEqualTo("0.3333333333333333333333333333333333");
  }

  /** The widened arm is sign-agnostic: HALF_UP rounds away from zero on both sides of it. */
  @Test
  void theWidenedQuotientIsMirroredAboutZero() {
    final BigDecimal left = new BigDecimal("1.0000000000000000000000000000000000000000");
    assertThat(divide(left.negate(), new BigDecimal("3"))).isEqualByComparingTo(divide(left, new BigDecimal("3")).negate());
  }

  private BigDecimal divide(final Object a, final Object b) {
    try (final ResultSet rs = database.query("sql", "SELECT :a / :b AS r", Map.of("a", a, "b", b))) {
      final Number value = rs.next().getProperty("r");
      return value instanceof BigDecimal decimal ? decimal : new BigDecimal(value.toString());
    }
  }
}
