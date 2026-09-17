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
import java.time.LocalDateTime;
import java.util.Date;

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
   * Found by the review of PR #7750, same family as #5900/#5947 and in the very switches this issue extends: the
   * three sub-millisecond timestamp types were missing from the widening lists that already carried {@code DATE}
   * and {@code DATETIME}, so an {@code INT}/{@code LONG}/{@code FLOAT}/{@code DOUBLE} against one of them fell
   * through to {@code default: return -1}. That is not merely imprecise, it is a hard antisymmetry violation:
   * {@code compare(10, INT, 5, DATETIME_NANOS)} and {@code compare(5, DATETIME_NANOS, 10, INT)} BOTH answered
   * "less than", and a {@code DATETIME_NANOS} column is routinely past 2^53 as epoch nanos, which is the same
   * band this issue is about.
   */
  @Test
  void theSubMillisecondTimestampTypesAreOrderedRatherThanAnsweringMinusOne() {
    final byte[] timestampTypes = { BinaryTypes.TYPE_DATETIME_SECOND, BinaryTypes.TYPE_DATETIME_MICROS,
        BinaryTypes.TYPE_DATETIME_NANOS, BinaryTypes.TYPE_DATETIME, BinaryTypes.TYPE_DATE };

    for (final byte timestampType : timestampTypes)
      for (final long timestamp : new long[] { 5L, 10L, 15L }) {
        assertOrdersAndIsAntisymmetric(10, BinaryTypes.TYPE_INT, timestamp, timestampType);
        assertOrdersAndIsAntisymmetric(10L, BinaryTypes.TYPE_LONG, timestamp, timestampType);
        assertOrdersAndIsAntisymmetric(10.0d, BinaryTypes.TYPE_DOUBLE, timestamp, timestampType);
        assertOrdersAndIsAntisymmetric(10.0f, BinaryTypes.TYPE_FLOAT, timestamp, timestampType);
      }
  }

  /** The same, at a magnitude only a {@code DATETIME_NANOS} reaches - past 2^53, where a double stops being exact. */
  @Test
  void aNanosecondTimestampPastTheDoubleMantissaIsStillOrdered() {
    final long nanos = 1_781_236_800_123_456_789L;
    assertThat(Type.isExactAsDouble(nanos)).as("the premise: epoch nanos are past 2^53").isFalse();

    assertOrdersAndIsAntisymmetric(nanos - 1, BinaryTypes.TYPE_LONG, nanos, BinaryTypes.TYPE_DATETIME_NANOS);
    assertOrdersAndIsAntisymmetric(nanos, BinaryTypes.TYPE_LONG, nanos, BinaryTypes.TYPE_DATETIME_NANOS);
    assertOrdersAndIsAntisymmetric(nanos + 1, BinaryTypes.TYPE_LONG, nanos, BinaryTypes.TYPE_DATETIME_NANOS);
  }

  /**
   * Asserts the pair orders by value AND that the reverse call is the exact negation.
   * <p>
   * The expected sign is computed from {@code longValue()}, not {@code doubleValue()}: every caller passes an
   * integral {@code value1}, and routing the expectation through a double would reintroduce the very 2^53
   * rounding this test exists to catch - the expectation would agree with a broken comparator.
   */
  private void assertOrdersAndIsAntisymmetric(final Number value1, final byte type1, final long timestamp,
      final byte timestampType) {
    final int forward = comparator.compare(value1, type1, timestamp, timestampType);
    final int backward = comparator.compare(timestamp, timestampType, value1, type1);
    final int expected = Long.compare(value1.longValue(), timestamp);

    assertThat(Integer.signum(forward)).as("%s(type %d) vs timestamp %d(type %d)", value1, type1, timestamp,
        timestampType).isEqualTo(Integer.signum(expected));
    assertThat(Integer.signum(backward)).as("the reverse of %s(type %d) vs timestamp %d(type %d)", value1, type1,
        timestamp, timestampType).isEqualTo(-Integer.signum(expected));
  }

  /**
   * Found by CodeRabbit's review of PR #7750. A temporal column is STORED as a long but MATERIALISED through the
   * configured {@code dateTimeImplementation}/{@code dateImplementation}, so a value reaching this comparator can
   * be a {@code LocalDateTime} or a {@code Date} rather than a {@code Number}. Casting it straight to
   * {@code Number} threw {@code ClassCastException} - pre-existing for DATE and DATETIME, and newly reachable for
   * the three sub-millisecond types once they stopped answering {@code -1}. All five normalise through
   * {@code DateUtils} now, so a numeric-first comparison answers instead of crashing.
   */
  @Test
  void aTemporalOperandThatIsNotANumberIsNormalisedRatherThanCastBlindly() {
    final LocalDateTime laterDateTime = LocalDateTime.of(2026, 6, 12, 15, 30);
    final Date laterDate = new Date(1781236800000L);

    for (final byte timestampType : new byte[] { BinaryTypes.TYPE_DATETIME, BinaryTypes.TYPE_DATE,
        BinaryTypes.TYPE_DATETIME_SECOND, BinaryTypes.TYPE_DATETIME_MICROS, BinaryTypes.TYPE_DATETIME_NANOS })
      for (final Object temporal : new Object[] { laterDateTime, laterDate }) {
        assertThat(comparator.compare(1L, BinaryTypes.TYPE_LONG, temporal, timestampType))
            .as("LONG vs %s as type %d", temporal.getClass().getSimpleName(), timestampType).isNegative();
        assertThat(comparator.compare(1, BinaryTypes.TYPE_INT, temporal, timestampType))
            .as("INT vs %s as type %d", temporal.getClass().getSimpleName(), timestampType).isNegative();
        assertThat(comparator.compare(1.0d, BinaryTypes.TYPE_DOUBLE, temporal, timestampType))
            .as("DOUBLE vs %s as type %d", temporal.getClass().getSimpleName(), timestampType).isNegative();
        assertThat(comparator.compare(1.0f, BinaryTypes.TYPE_FLOAT, temporal, timestampType))
            .as("FLOAT vs %s as type %d", temporal.getClass().getSimpleName(), timestampType).isNegative();

        assertThat(comparator.compare(temporal, timestampType, 1L, BinaryTypes.TYPE_LONG))
            .as("the reverse of LONG vs %s as type %d", temporal.getClass().getSimpleName(), timestampType)
            .isPositive();
      }
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
