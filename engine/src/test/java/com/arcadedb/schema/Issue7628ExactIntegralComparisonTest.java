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
package com.arcadedb.schema;

import com.arcadedb.serializer.BinaryComparator;
import com.arcadedb.serializer.BinaryTypes;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #7628: #7614 brought a {@code Long} and a {@code Float} to a common type by widening
 * both to {@code double}, which is exact only while the long fits in double's 53-bit mantissa. Past 2^53 the same
 * collapse #7614 removed at 2^24 returns one band higher - {@code 10_000_000_000_000_001L} and {@code 1.0E16f}
 * share a double - so {@link Type#castComparableNumber} reported them equal. The fix routes such a pair through
 * {@link BigDecimal} in both entry points, so {@link BinaryComparator}'s ordering and {@code castComparableNumber}'s
 * equality keep answering the same thing at every magnitude (the invariant of #7609/#7613/#7614).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7628ExactIntegralComparisonTest {
  /** One above 2^53's last exactly representable neighbourhood, and the float/double it used to collapse onto. */
  private static final long   BEYOND_MANTISSA = 10_000_000_000_000_001L;
  private static final float  AS_FLOAT        = 1.0E16f;
  private static final double AS_DOUBLE       = 1.0E16d;

  private final BinaryComparator comparator = new BinaryComparator();

  @Test
  void aLongBeyondTheDoubleMantissaIsNotEqualToTheFloatItRoundsTo() {
    // The premise: the two DO share a double, so a comparison performed there cannot tell them apart
    assertThat((double) BEYOND_MANTISSA).isEqualTo(Type.widenFloat(AS_FLOAT));

    final Number[] couple = Type.castComparableNumber(BEYOND_MANTISSA, AS_FLOAT);
    assertThat(couple[0]).isNotEqualTo(couple[1]);
    assertThat(BinaryComparator.equals(BEYOND_MANTISSA, AS_FLOAT)).isFalse();

    // ...and the identical value must still compare equal
    assertThat(BinaryComparator.equals(10_000_000_000_000_000L, AS_FLOAT)).isTrue();
  }

  @Test
  void aLongBeyondTheDoubleMantissaIsNotEqualToTheDoubleItRoundsTo() {
    final Number[] couple = Type.castComparableNumber(BEYOND_MANTISSA, AS_DOUBLE);
    assertThat(couple[0]).isNotEqualTo(couple[1]);
    assertThat(BinaryComparator.equals(BEYOND_MANTISSA, AS_DOUBLE)).isFalse();
    assertThat(BinaryComparator.equals(10_000_000_000_000_000L, AS_DOUBLE)).isTrue();
  }

  @Test
  void theSymmetricFloatAndDoubleBranchesAgreeWithTheLongOnes() {
    final Number[] floatFirst = Type.castComparableNumber(AS_FLOAT, BEYOND_MANTISSA);
    assertThat(floatFirst[0]).isNotEqualTo(floatFirst[1]);
    assertThat(((Comparable) floatFirst[0]).compareTo(floatFirst[1])).isNegative();

    final Number[] doubleFirst = Type.castComparableNumber(AS_DOUBLE, BEYOND_MANTISSA);
    assertThat(doubleFirst[0]).isNotEqualTo(doubleFirst[1]);
    assertThat(((Comparable) doubleFirst[0]).compareTo(doubleFirst[1])).isNegative();
  }

  @Test
  void castComparableNumberOrdersTheWholeBandStrictly() {
    // Every long in a run past 2^53 is strictly ordered against the float, rather than a whole band answering 0
    for (long delta = -3; delta <= 3; delta++) {
      final long value = 10_000_000_000_000_000L + delta;
      final Number[] couple = Type.castComparableNumber(value, AS_FLOAT);
      final int sign = ((Comparable) couple[0]).compareTo(couple[1]);
      assertThat(sign).as("long %d against %s", value, AS_FLOAT).isEqualTo(Long.signum(delta));
    }
  }

  @Test
  void theComparatorOrdersTheSameBandAndStaysAntisymmetric() {
    for (long delta = -3; delta <= 3; delta++) {
      final long value = 10_000_000_000_000_000L + delta;

      final int forward = comparator.compare(value, BinaryTypes.TYPE_LONG, AS_FLOAT, BinaryTypes.TYPE_FLOAT);
      final int backward = comparator.compare(AS_FLOAT, BinaryTypes.TYPE_FLOAT, value, BinaryTypes.TYPE_LONG);

      assertThat(Integer.signum(forward)).as("LONG %d vs FLOAT", value).isEqualTo((int) Long.signum(delta));
      assertThat(Integer.signum(backward)).as("FLOAT vs LONG %d", value).isEqualTo(-(int) Long.signum(delta));

      final int forwardD = comparator.compare(value, BinaryTypes.TYPE_LONG, AS_DOUBLE, BinaryTypes.TYPE_DOUBLE);
      final int backwardD = comparator.compare(AS_DOUBLE, BinaryTypes.TYPE_DOUBLE, value, BinaryTypes.TYPE_LONG);

      assertThat(Integer.signum(forwardD)).as("LONG %d vs DOUBLE", value).isEqualTo((int) Long.signum(delta));
      assertThat(Integer.signum(backwardD)).as("DOUBLE vs LONG %d", value).isEqualTo(-(int) Long.signum(delta));
    }
  }

  /**
   * The ordering entry point and the equality one have to answer the same for the identical pair: that is the
   * invariant #7609 established and the reason #7628 could not be fixed inside {@code castComparableNumber} alone.
   */
  @Test
  void orderingAndEqualityAgreeAcrossTheBoundary() {
    for (long delta = -3; delta <= 3; delta++) {
      final long value = 10_000_000_000_000_000L + delta;

      final boolean ordersEqual = comparator.compare(value, BinaryTypes.TYPE_LONG, AS_FLOAT, BinaryTypes.TYPE_FLOAT) == 0;
      assertThat(BinaryComparator.equals(value, AS_FLOAT)).as("long %d", value).isEqualTo(ordersEqual);
    }
  }

  /**
   * The comparator's own DECIMAL branch has always been exact, so the reverse direction going through {@code
   * double} made the two disagree on a decimal finer than a double resolves - {@code compare(a,b)} answered 0
   * while {@code compare(b,a)} answered non-zero.
   */
  @Test
  void integralAgainstDecimalIsExactInBothDirections() {
    final BigDecimal justAbove = new BigDecimal("5.0000000000000000001");

    assertThat(comparator.compare(5L, BinaryTypes.TYPE_LONG, justAbove, BinaryTypes.TYPE_DECIMAL)).isNegative();
    assertThat(comparator.compare(justAbove, BinaryTypes.TYPE_DECIMAL, 5L, BinaryTypes.TYPE_LONG)).isPositive();

    assertThat(comparator.compare(5, BinaryTypes.TYPE_INT, justAbove, BinaryTypes.TYPE_DECIMAL)).isNegative();
    assertThat(comparator.compare(justAbove, BinaryTypes.TYPE_DECIMAL, 5, BinaryTypes.TYPE_INT)).isPositive();
  }

  @Test
  void floatingAgainstDecimalIsExactInBothDirections() {
    final BigDecimal justAbove = new BigDecimal("0.5000000000000000001");

    assertThat(comparator.compare(0.5d, BinaryTypes.TYPE_DOUBLE, justAbove, BinaryTypes.TYPE_DECIMAL)).isNegative();
    assertThat(comparator.compare(justAbove, BinaryTypes.TYPE_DECIMAL, 0.5d, BinaryTypes.TYPE_DOUBLE)).isPositive();

    assertThat(comparator.compare(0.5f, BinaryTypes.TYPE_FLOAT, justAbove, BinaryTypes.TYPE_DECIMAL)).isNegative();
    assertThat(comparator.compare(justAbove, BinaryTypes.TYPE_DECIMAL, 0.5f, BinaryTypes.TYPE_FLOAT)).isPositive();
  }

  /**
   * NaN and the infinities have no decimal form, so the exact path must not be taken for them: they stay in
   * {@code double}, where {@link Double#compare} already orders them totally rather than throwing.
   */
  @Test
  void nonFiniteOperandsStillCompareWithoutThrowing() {
    assertThat(comparator.compare(BEYOND_MANTISSA, BinaryTypes.TYPE_LONG, Double.POSITIVE_INFINITY,
        BinaryTypes.TYPE_DOUBLE)).isNegative();
    assertThat(comparator.compare(BEYOND_MANTISSA, BinaryTypes.TYPE_LONG, Double.NEGATIVE_INFINITY,
        BinaryTypes.TYPE_DOUBLE)).isPositive();
    assertThat(comparator.compare(BEYOND_MANTISSA, BinaryTypes.TYPE_LONG, Float.NaN, BinaryTypes.TYPE_FLOAT)).isNegative();

    assertThat(Type.castComparableNumber(BEYOND_MANTISSA, Float.POSITIVE_INFINITY)[0]).isInstanceOf(Double.class);
    assertThat(Type.castComparableNumber(BEYOND_MANTISSA, Double.NaN)[0]).isInstanceOf(Double.class);
    assertThat(BinaryComparator.equals(BEYOND_MANTISSA, Double.NaN)).isFalse();
  }

  /**
   * Below the boundary nothing may change: the pair still meets at {@code double}, which is both exact and far
   * cheaper than a {@link BigDecimal} round trip on a hot comparison path.
   */
  @Test
  void aLongInsideTheMantissaStillMeetsAtDouble() {
    final Number[] couple = Type.castComparableNumber(16_777_217L, 1.0f);
    assertThat(couple[0]).isInstanceOf(Double.class);
    assertThat(couple[1]).isInstanceOf(Double.class);

    assertThat(Type.isExactAsDouble(1L << 53)).isTrue();
    assertThat(Type.isExactAsDouble(-(1L << 53))).isTrue();
    assertThat(Type.isExactAsDouble((1L << 53) + 1)).isFalse();
    assertThat(Type.isExactAsDouble(Long.MIN_VALUE)).isFalse();
  }
}
