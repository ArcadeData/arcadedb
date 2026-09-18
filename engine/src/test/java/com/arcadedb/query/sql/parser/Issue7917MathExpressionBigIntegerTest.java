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
package com.arcadedb.query.sql.parser;

import com.arcadedb.query.sql.parser.MathExpression.Operator;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #7917: {@code MathExpression.Operator.apply(Number, Operator, Number)} - the SQL
 * ARITHMETIC promotion chain - had no {@code BigInteger} arm on either side and no {@code Byte} arm at all, so
 * every such operand fell off the end into {@code IllegalArgumentException("Cannot increment value ...")}, which
 * surfaced as HTTP 500 rather than as a 400 naming the caller's values. Comparing the identical pair has worked
 * since #7669 gave {@code Type.castComparableNumber} its full BigInteger branch; only the arithmetic path, a
 * separate chain in a separate file, was left behind.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7917MathExpressionBigIntegerTest {

  private static final BigInteger FIVE = BigInteger.valueOf(5);

  @Test
  void bigIntegerOnTheLeftAddsToEveryOtherNumericType() {
    assertThat(sum(FIVE, 1)).isEqualByComparingTo(new BigDecimal("6"));
    assertThat(sum(FIVE, 1L)).isEqualByComparingTo(new BigDecimal("6"));
    assertThat(sum(FIVE, (short) 1)).isEqualByComparingTo(new BigDecimal("6"));
    assertThat(sum(FIVE, (byte) 1)).isEqualByComparingTo(new BigDecimal("6"));
    assertThat(sum(FIVE, 1.5f)).isEqualByComparingTo(new BigDecimal("6.5"));
    assertThat(sum(FIVE, 1.5d)).isEqualByComparingTo(new BigDecimal("6.5"));
    assertThat(sum(FIVE, new BigDecimal("1.5"))).isEqualByComparingTo(new BigDecimal("6.5"));
    assertThat(sum(FIVE, BigInteger.ONE)).isEqualByComparingTo(new BigDecimal("6"));
  }

  @Test
  void bigIntegerOnTheRightAddsToEveryOtherNumericType() {
    assertThat(sum(1, FIVE)).isEqualByComparingTo(new BigDecimal("6"));
    assertThat(sum(1L, FIVE)).isEqualByComparingTo(new BigDecimal("6"));
    assertThat(sum((short) 1, FIVE)).isEqualByComparingTo(new BigDecimal("6"));
    assertThat(sum((byte) 1, FIVE)).isEqualByComparingTo(new BigDecimal("6"));
    assertThat(sum(1.5f, FIVE)).isEqualByComparingTo(new BigDecimal("6.5"));
    assertThat(sum(1.5d, FIVE)).isEqualByComparingTo(new BigDecimal("6.5"));
    assertThat(sum(new BigDecimal("1.5"), FIVE)).isEqualByComparingTo(new BigDecimal("6.5"));
  }

  /**
   * A BigInteger past 2^53 must not collapse onto a double on the way through: BigDecimal holds it exactly, which
   * is the whole reason the promotion goes there rather than to double.
   */
  @Test
  void aBigIntegerBeyondDoublesMantissaStaysExact() {
    final BigInteger huge = BigInteger.valueOf(2).pow(80).add(BigInteger.ONE);

    assertThat(sum(huge, 1)).isEqualByComparingTo(new BigDecimal(huge.add(BigInteger.ONE)));
    assertThat(sum(huge, BigInteger.ONE)).isEqualByComparingTo(new BigDecimal(huge.add(BigInteger.ONE)));
  }

  /**
   * NaN and the infinities have no BigDecimal form at all, so those pairs meet in double instead - the same guard
   * {@code Type.castComparableNumber}'s BigInteger arms carry.
   */
  @Test
  void nonFiniteFloatingPointMeetsABigIntegerInDouble() {
    assertThat(Operator.PLUS.apply(FIVE, Operator.PLUS, Double.POSITIVE_INFINITY).doubleValue())
        .isEqualTo(Double.POSITIVE_INFINITY);
    assertThat(Operator.PLUS.apply(Double.NaN, Operator.PLUS, FIVE).doubleValue()).isNaN();
    assertThat(Operator.PLUS.apply(FIVE, Operator.PLUS, Float.NEGATIVE_INFINITY).doubleValue())
        .isEqualTo(Double.NEGATIVE_INFINITY);
    assertThat(Operator.PLUS.apply(Float.NaN, Operator.PLUS, FIVE).doubleValue()).isNaN();
  }

  @Test
  void byteOperandsAreAccepted() {
    // Byte was missing from every arm in both positions, so even Byte + Integer threw.
    assertThat(sum((byte) 2, 1)).isEqualByComparingTo(new BigDecimal("3"));
    assertThat(sum(1, (byte) 2)).isEqualByComparingTo(new BigDecimal("3"));
    assertThat(sum((byte) 2, (byte) 3)).isEqualByComparingTo(new BigDecimal("5"));
    assertThat(sum((byte) 2, 1L)).isEqualByComparingTo(new BigDecimal("3"));
    assertThat(sum((byte) 2, 1.5f)).isEqualByComparingTo(new BigDecimal("3.5"));
    assertThat(sum((byte) 2, 1.5d)).isEqualByComparingTo(new BigDecimal("3.5"));
    assertThat(sum((byte) 2, new BigDecimal("1.5"))).isEqualByComparingTo(new BigDecimal("3.5"));
    assertThat(sum(new BigDecimal("1.5"), (byte) 2)).isEqualByComparingTo(new BigDecimal("3.5"));
  }

  /**
   * The first arm has always accepted a Short on the left, but reached it with {@code new BigDecimal((Integer) a)} -
   * a ClassCastException for exactly the type the arm was written to admit.
   */
  @Test
  void aShortMeetsABigDecimalWithoutAClassCastException() {
    assertThat(sum((short) 2, new BigDecimal("1.5"))).isEqualByComparingTo(new BigDecimal("3.5"));
  }

  @Test
  void theOtherArithmeticOperatorsTakeTheSameOperands() {
    assertThat(apply(Operator.MINUS, FIVE, 2)).isEqualByComparingTo(new BigDecimal("3"));
    assertThat(apply(Operator.STAR, FIVE, 3)).isEqualByComparingTo(new BigDecimal("15"));
    assertThat(apply(Operator.REM, FIVE, 3)).isEqualByComparingTo(new BigDecimal("2"));
    assertThat(apply(Operator.SLASH, BigInteger.TEN, 2)).isEqualByComparingTo(new BigDecimal("5"));
  }

  /**
   * The existing promotions must not move: the arms this fix widened also carry every pair that already worked.
   */
  @Test
  void theExistingPromotionsAreUnchanged() {
    assertThat(Operator.PLUS.apply(1, Operator.PLUS, 2)).isEqualTo(3);
    assertThat(Operator.PLUS.apply(1, Operator.PLUS, 2L)).isEqualTo(3L);
    assertThat(Operator.PLUS.apply(1, Operator.PLUS, 2.5f)).isEqualTo(3.5f);
    assertThat(Operator.PLUS.apply(1, Operator.PLUS, 2.5d)).isEqualTo(3.5d);
    assertThat(Operator.PLUS.apply((short) 1, Operator.PLUS, (short) 2)).isEqualTo(3);
    assertThat(Operator.PLUS.apply(1L, Operator.PLUS, 2L)).isEqualTo(3L);
    assertThat(Operator.PLUS.apply(1.5f, Operator.PLUS, 2.5f)).isEqualTo(4.0f);
    assertThat(Operator.PLUS.apply(1.5d, Operator.PLUS, 2.5d)).isEqualTo(4.0d);
  }

  private static BigDecimal sum(final Number left, final Number right) {
    return apply(Operator.PLUS, left, right);
  }

  private static BigDecimal apply(final Operator operator, final Number left, final Number right) {
    final Number result = operator.apply(left, operator, right);
    return result instanceof BigDecimal decimal ? decimal : new BigDecimal(result.toString());
  }
}
