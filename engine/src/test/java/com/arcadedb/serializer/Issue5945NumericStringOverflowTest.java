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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #5945. {@link BinaryComparator}'s {@code compareAgainstNumericString} helper (added by
 * PR #5922 fixing #5900) widens a numeric {@code String} to {@code long} or {@code double} based on its shape, but
 * an integral string outside {@code long}'s range (e.g. 20+ digits) threw a raw, uncaught
 * {@code NumberFormatException} instead of comparing correctly. It also mis-routed {@code "NaN"} /
 * {@code "Infinity"} / {@code "-Infinity"} into the {@code Long.parseLong} branch, since none of those strings
 * contain {@code '.'}, {@code 'e'} or {@code 'E'}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5945NumericStringOverflowTest {
  private final BinaryComparator comparator = new BinaryComparator();

  @Test
  void intVersusOutOfLongRangeNumericStringDoesNotThrow() {
    // 20-digit integral string, far beyond Long.MAX_VALUE (19 digits): must compare, not throw.
    final String huge = "99999999999999999999";

    assertThat(comparator.compare(5, BinaryTypes.TYPE_INT, huge, BinaryTypes.TYPE_STRING)).isLessThan(0);
    assertThat(comparator.compare(huge, BinaryTypes.TYPE_STRING, 5, BinaryTypes.TYPE_INT)).isGreaterThan(0);
  }

  @Test
  void longVersusOutOfLongRangeNumericStringIsAntisymmetricAndExact() {
    final String huge = "99999999999999999999";
    final long five = 5L;

    final int forward = comparator.compare(five, BinaryTypes.TYPE_LONG, huge, BinaryTypes.TYPE_STRING);
    final int backward = comparator.compare(huge, BinaryTypes.TYPE_STRING, five, BinaryTypes.TYPE_LONG);

    assertThat(Integer.signum(forward)).isEqualTo(-Integer.signum(backward));
    assertThat(forward).isLessThan(0);
    assertThat(backward).isGreaterThan(0);
  }

  @Test
  void negativeOutOfLongRangeNumericStringComparesCorrectly() {
    // 20-digit negative integral string, beyond Long.MIN_VALUE's range.
    final String hugeNegative = "-99999999999999999999";

    assertThat(comparator.compare(5, BinaryTypes.TYPE_INT, hugeNegative, BinaryTypes.TYPE_STRING)).isGreaterThan(0);
    assertThat(comparator.compare(hugeNegative, BinaryTypes.TYPE_STRING, 5, BinaryTypes.TYPE_INT)).isLessThan(0);
  }

  @Test
  void outOfLongRangeNumericStringComparisonIsExactNotDoubleApproximated() {
    // Long.MAX_VALUE (19 digits, 9223372036854775807) versus a 20-digit value one order of magnitude larger.
    // A double round-trip of the huge string would lose precision but must still land unambiguously above
    // Long.MAX_VALUE, so this alone would not catch a double fallback; the real exactness check is that a
    // string differing from Long.MAX_VALUE by a small amount round-trips through BigDecimal, not through a
    // double that cannot represent 19-digit integers exactly.
    final long maxLong = Long.MAX_VALUE;
    // One more than Long.MAX_VALUE, expressed as its own out-of-range string.
    final String maxLongPlusOne = "9223372036854775808";

    assertThat(comparator.compare(maxLong, BinaryTypes.TYPE_LONG, maxLongPlusOne, BinaryTypes.TYPE_STRING)).isLessThan(0);
    assertThat(comparator.compare(maxLongPlusOne, BinaryTypes.TYPE_STRING, maxLong, BinaryTypes.TYPE_LONG)).isGreaterThan(0);
  }

  @Test
  void nanStringComparesAsDoubleNaNRatherThanThrowing() {
    // "NaN" contains none of '.', 'e', 'E', so it fell into the Long.parseLong branch and threw instead of
    // being routed to Double.parseDouble, which correctly parses it as Double.NaN.
    assertThat(comparator.compare(5, BinaryTypes.TYPE_INT, "NaN", BinaryTypes.TYPE_STRING))
        .isEqualTo(Double.compare(5, Double.NaN));

    // Double.parseDouble's grammar also accepts an explicit sign on NaN (Signopt NaN), even though it has no
    // effect on the parsed value: both must be routed the same way as the unsigned form, not fall through to
    // Long.parseLong.
    assertThat(comparator.compare(5, BinaryTypes.TYPE_INT, "+NaN", BinaryTypes.TYPE_STRING))
        .isEqualTo(Double.compare(5, Double.NaN));
    assertThat(comparator.compare(5, BinaryTypes.TYPE_INT, "-NaN", BinaryTypes.TYPE_STRING))
        .isEqualTo(Double.compare(5, Double.NaN));
  }

  @Test
  void infinityStringComparesAsDoubleInfinityRatherThanThrowing() {
    assertThat(comparator.compare(5, BinaryTypes.TYPE_INT, "Infinity", BinaryTypes.TYPE_STRING)).isLessThan(0);
    assertThat(comparator.compare(5, BinaryTypes.TYPE_INT, "+Infinity", BinaryTypes.TYPE_STRING)).isLessThan(0);
    assertThat(comparator.compare(5, BinaryTypes.TYPE_INT, "-Infinity", BinaryTypes.TYPE_STRING)).isGreaterThan(0);
    assertThat(comparator.compare(5L, BinaryTypes.TYPE_LONG, "Infinity", BinaryTypes.TYPE_STRING)).isLessThan(0);
    assertThat(comparator.compare(5L, BinaryTypes.TYPE_LONG, "-Infinity", BinaryTypes.TYPE_STRING)).isGreaterThan(0);
  }

  @Test
  void minLongMinusOneNumericStringComparesCorrectly() {
    // Long.MIN_VALUE (9223372036854775808, 19 digits after the sign) minus one, expressed as its own
    // out-of-range string: covers the low-end boundary, complementing the MAX_VALUE + 1 check above.
    final long minLong = Long.MIN_VALUE;
    final String minLongMinusOne = "-9223372036854775809";

    assertThat(comparator.compare(minLong, BinaryTypes.TYPE_LONG, minLongMinusOne, BinaryTypes.TYPE_STRING)).isGreaterThan(0);
    assertThat(comparator.compare(minLongMinusOne, BinaryTypes.TYPE_STRING, minLong, BinaryTypes.TYPE_LONG)).isLessThan(0);
  }

  @Test
  void nonNumericStringStillThrowsRatherThanSilentlyComparing() {
    // Regression guard: the overflow catch must not swallow a genuinely non-numeric string; BigDecimal's own
    // NumberFormatException should still propagate, same as before this fix.
    assertThatThrownBy(() -> comparator.compare(5, BinaryTypes.TYPE_INT, "abc", BinaryTypes.TYPE_STRING))
        .isInstanceOf(NumberFormatException.class);
  }

  @Test
  void ordinaryInRangeNumericStringsAreUnaffected() {
    assertThat(comparator.compare(10, BinaryTypes.TYPE_INT, "15", BinaryTypes.TYPE_STRING)).isLessThan(0);
    assertThat(comparator.compare(10, BinaryTypes.TYPE_INT, "10", BinaryTypes.TYPE_STRING)).isZero();
    assertThat(comparator.compare(5L, BinaryTypes.TYPE_LONG, "5.5", BinaryTypes.TYPE_STRING)).isLessThan(0);
  }
}
