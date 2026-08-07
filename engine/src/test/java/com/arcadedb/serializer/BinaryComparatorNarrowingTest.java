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
package com.arcadedb.serializer;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #5900. When the first operand's declared type is {@code INT}, {@code SHORT} or
 * {@code BYTE}, {@link BinaryComparator#compare(Object, byte, Object, byte)} narrowed the <em>other</em> operand
 * down to that same width instead of widening, which breaks the antisymmetry every {@code Comparator} must honour
 * ({@code sgn(compare(a,b)) == -sgn(compare(b,a))}): a {@code Long} outside {@code int} range truncated via
 * {@code intValue()} could land anywhere, including on the wrong side of a small {@code int}. The {@code SHORT}
 * and {@code BYTE} branches had the same defect, and more aggressively, since they also narrowed a same-or-wider
 * {@code INT} operand down to their own width.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class BinaryComparatorNarrowingTest {
  private final BinaryComparator comparator = new BinaryComparator();

  @Test
  void intVersusOutOfRangeLongIsAntisymmetric() {
    final int smallInt = -5;
    final long bigLong = 2147483648L; // Integer.MAX_VALUE + 1, truncates to Integer.MIN_VALUE via intValue()

    final int forward = comparator.compare(smallInt, BinaryTypes.TYPE_INT, bigLong, BinaryTypes.TYPE_LONG);
    final int backward = comparator.compare(bigLong, BinaryTypes.TYPE_LONG, smallInt, BinaryTypes.TYPE_INT);

    assertThat(Integer.signum(forward)).isEqualTo(-Integer.signum(backward));
    // -5 is unambiguously less than 2147483648
    assertThat(forward).isLessThan(0);
    assertThat(backward).isGreaterThan(0);
  }

  @Test
  void shortVersusOutOfRangeIntIsAntisymmetricAndNotTruncated() {
    final short smallShort = 1;
    final int bigInt = 100_000; // outside short range, would truncate to a small/negative short

    final int forward = comparator.compare(smallShort, BinaryTypes.TYPE_SHORT, bigInt, BinaryTypes.TYPE_INT);
    final int backward = comparator.compare(bigInt, BinaryTypes.TYPE_INT, smallShort, BinaryTypes.TYPE_SHORT);

    assertThat(Integer.signum(forward)).isEqualTo(-Integer.signum(backward));
    assertThat(forward).isLessThan(0);
    assertThat(backward).isGreaterThan(0);
  }

  @Test
  void byteVersusOutOfRangeIntIsAntisymmetricAndNotTruncated() {
    final byte smallByte = 1;
    final int bigInt = 1_000; // outside byte range

    final int forward = comparator.compare(smallByte, BinaryTypes.TYPE_BYTE, bigInt, BinaryTypes.TYPE_INT);
    final int backward = comparator.compare(bigInt, BinaryTypes.TYPE_INT, smallByte, BinaryTypes.TYPE_BYTE);

    assertThat(Integer.signum(forward)).isEqualTo(-Integer.signum(backward));
    assertThat(forward).isLessThan(0);
    assertThat(backward).isGreaterThan(0);
  }

  @Test
  void intVersusDoubleDoesNotTruncateTheDouble() {
    final int smallInt = 0;
    final double fractional = 0.5;

    // 0 < 0.5, but the old code truncated 0.5 to 0 via intValue(), making them appear equal
    assertThat(comparator.compare(smallInt, BinaryTypes.TYPE_INT, fractional, BinaryTypes.TYPE_DOUBLE)).isLessThan(0);
    assertThat(comparator.compare(fractional, BinaryTypes.TYPE_DOUBLE, smallInt, BinaryTypes.TYPE_INT)).isGreaterThan(0);
  }

  @Test
  void sameWidthAndBooleanComparisonsAreUnaffected() {
    assertThat(comparator.compare(5, BinaryTypes.TYPE_INT, 5, BinaryTypes.TYPE_INT)).isZero();
    assertThat(comparator.compare(5, BinaryTypes.TYPE_INT, 10, BinaryTypes.TYPE_INT)).isLessThan(0);
    assertThat(comparator.compare((short) 5, BinaryTypes.TYPE_SHORT, (short) 5, BinaryTypes.TYPE_SHORT)).isZero();
    assertThat(comparator.compare((byte) 5, BinaryTypes.TYPE_BYTE, (byte) 5, BinaryTypes.TYPE_BYTE)).isZero();
    assertThat(comparator.compare(1, BinaryTypes.TYPE_INT, true, BinaryTypes.TYPE_BOOLEAN)).isZero();
    assertThat(comparator.compare(0, BinaryTypes.TYPE_INT, true, BinaryTypes.TYPE_BOOLEAN)).isLessThan(0);
  }

  /**
   * The String branch of {@code compareNarrowIntegral} widens to {@code long} or {@code double} depending on
   * whether the string looks fractional, rather than always narrowing to {@code value1}'s own width.
   */
  @Test
  void intVersusNumericStringWidensRatherThanNarrows() {
    // Outside int range: the old Integer.parseInt() would have thrown; must parse and compare as a long instead.
    assertThat(comparator.compare(0, BinaryTypes.TYPE_INT, "5000000000", BinaryTypes.TYPE_STRING)).isLessThan(0);
    assertThat(comparator.compare(0, BinaryTypes.TYPE_INT, "-5000000000", BinaryTypes.TYPE_STRING)).isGreaterThan(0);

    // Fractional string: must parse and compare as a double, not truncate to an integral comparison.
    assertThat(comparator.compare(0, BinaryTypes.TYPE_INT, "0.5", BinaryTypes.TYPE_STRING)).isLessThan(0);
    assertThat(comparator.compare(1, BinaryTypes.TYPE_INT, "0.5", BinaryTypes.TYPE_STRING)).isGreaterThan(0);

    // Ordinary in-range integral string: unaffected by the widening change.
    assertThat(comparator.compare(10, BinaryTypes.TYPE_INT, "15", BinaryTypes.TYPE_STRING)).isLessThan(0);
    assertThat(comparator.compare(10, BinaryTypes.TYPE_INT, "10", BinaryTypes.TYPE_STRING)).isZero();
  }

  /**
   * The {@code TYPE_BOOLEAN} branch had the identical narrow-to-byte defect as the pre-fix INT/SHORT/BYTE
   * branches, just unnoticed because it is even less likely to be exercised. Fixed by routing through the same
   * {@code compareNarrowIntegral} helper.
   */
  @Test
  void booleanVersusOutOfByteRangeNumberIsAntisymmetricAndNotTruncated() {
    final boolean trueValue = true; // widens to 1
    final int bigInt = 1000; // outside byte range; would truncate to a small/negative byte

    final int forward = comparator.compare(trueValue, BinaryTypes.TYPE_BOOLEAN, bigInt, BinaryTypes.TYPE_INT);
    final int backward = comparator.compare(bigInt, BinaryTypes.TYPE_INT, trueValue, BinaryTypes.TYPE_BOOLEAN);

    assertThat(Integer.signum(forward)).isEqualTo(-Integer.signum(backward));
    assertThat(forward).isLessThan(0);
    assertThat(backward).isGreaterThan(0);
  }
}
