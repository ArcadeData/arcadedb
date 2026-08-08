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

import com.arcadedb.utility.DateUtils;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

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

  /**
   * {@code TYPE_LONG} had the identical narrow-instead-of-widen defect as the pre-fix INT/SHORT/BYTE branches:
   * a fractional {@code Double} narrowed via {@code longValue()} silently drops its fraction, which can make an
   * unequal pair compare equal, or land on the wrong side (review follow-up on PR #5922).
   */
  @Test
  void longVersusFractionalDoubleIsAntisymmetricAndNotTruncated() {
    final long five = 5L;
    final double fiveAndAHalf = 5.5;

    // Before the fix: both directions truncated 5.5 to 5L and answered "equal".
    assertThat(comparator.compare(five, BinaryTypes.TYPE_LONG, fiveAndAHalf, BinaryTypes.TYPE_DOUBLE)).isLessThan(0);
    assertThat(comparator.compare(fiveAndAHalf, BinaryTypes.TYPE_DOUBLE, five, BinaryTypes.TYPE_LONG)).isGreaterThan(0);
  }

  /**
   * {@code LONG} vs a fractional numeric {@code String} must widen to {@code double} rather than hard-fail
   * {@code Long.parseLong} on the decimal point.
   */
  @Test
  void longVersusFractionalStringWidensRatherThanThrows() {
    assertThat(comparator.compare(5L, BinaryTypes.TYPE_LONG, "5.5", BinaryTypes.TYPE_STRING)).isLessThan(0);
    assertThat(comparator.compare(6L, BinaryTypes.TYPE_LONG, "5.5", BinaryTypes.TYPE_STRING)).isGreaterThan(0);
    // Out-of-int-range integral string must still compare as a long, not double-round-trip.
    assertThat(comparator.compare(5_000_000_001L, BinaryTypes.TYPE_LONG, "5000000000", BinaryTypes.TYPE_STRING)).isGreaterThan(0);
  }

  /**
   * {@code TYPE_FLOAT} narrowed a wider/more precise operand (a {@code Long} outside float's 24-bit mantissa, or
   * a {@code Double}) down to {@code float} instead of widening itself to {@code double} - the same bug shape,
   * lower severity (review follow-up on PR #5922). Fixed by merging FLOAT into DOUBLE's branch.
   */
  @Test
  void floatVersusPreciseDoubleAndLongIsNotTruncated() {
    // A double that differs from its nearest float only in the low mantissa bits: narrowing to float would
    // collapse the two values to the same float and answer "equal".
    final float asFloat = 16_777_217f; // rounds to 16777216f as a float (2^24 + 1 is not exactly representable)
    final double precise = 16_777_217.0; // exactly representable as a double

    assertThat(comparator.compare(asFloat, BinaryTypes.TYPE_FLOAT, precise, BinaryTypes.TYPE_DOUBLE)).isLessThan(0);
    assertThat(comparator.compare(precise, BinaryTypes.TYPE_DOUBLE, asFloat, BinaryTypes.TYPE_FLOAT)).isGreaterThan(0);
  }

  /**
   * {@code TYPE_STRING} as {@code type1} did an unconditional lexicographic {@code compareTo()} regardless of
   * {@code type2}, ignoring the numeric comparator every other branch in this class now uses - the same
   * antisymmetry bug this class was fixed for, just with the String on the other side (review follow-up on
   * PR #5922). {@code "2".compareTo("10")} is positive (lexicographic), while the numeric {@code 10 vs 2} is also
   * positive, so both directions of the same comparison used to agree "greater".
   */
  @Test
  void stringVersusNumericIsAntisymmetric() {
    final int forward = comparator.compare("2", BinaryTypes.TYPE_STRING, 10, BinaryTypes.TYPE_INT);
    final int backward = comparator.compare(10, BinaryTypes.TYPE_INT, "2", BinaryTypes.TYPE_STRING);

    assertThat(Integer.signum(forward)).isEqualTo(-Integer.signum(backward));
    // 2 is numerically less than 10, even though "2" > "10" lexicographically.
    assertThat(forward).isLessThan(0);
    assertThat(backward).isGreaterThan(0);
  }

  /**
   * Guard against over-fixing: a String compared against another String must still use lexicographic ordering,
   * not fall into the new numeric-delegation branch.
   */
  @Test
  void stringVersusStringStillLexicographic() {
    assertThat(comparator.compare("2", BinaryTypes.TYPE_STRING, "10", BinaryTypes.TYPE_STRING)).isGreaterThan(0);
    assertThat(comparator.compare("abc", BinaryTypes.TYPE_STRING, "abd", BinaryTypes.TYPE_STRING)).isLessThan(0);
  }

  /**
   * Guard against over-fixing: a numeric-valued String compared against a number must still resolve correctly in
   * both directions, and a non-numeric String against a number/boolean routes through the numeric side's
   * unguarded parse (consistent with every other branch in this class - #5900's SQL-level fix is what guards the
   * query engine against that, not this low-level comparator).
   */
  @Test
  void stringVersusBooleanIsAntisymmetric() {
    final int forward = comparator.compare("false", BinaryTypes.TYPE_STRING, true, BinaryTypes.TYPE_BOOLEAN);
    final int backward = comparator.compare(true, BinaryTypes.TYPE_BOOLEAN, "false", BinaryTypes.TYPE_STRING);

    assertThat(Integer.signum(forward)).isEqualTo(-Integer.signum(backward));
    assertThat(forward).isLessThan(0);
    assertThat(backward).isGreaterThan(0);
  }

  /**
   * {@code TYPE_STRING} as {@code type1} against a {@code TYPE_DATETIME}/{@code TYPE_DATE}/{@code type2} fell
   * through the same unconditional lexicographic {@code compareTo()} the numeric/boolean branches had before the
   * previous fix - issue #5947, filed as a follow-up to #5900/#5922 since it needed date parsing rather than a
   * mechanical widen-and-negate. An epoch-millis string makes the mismatch deterministic: every ISO date string
   * for a real-world year starts with {@code '1'} (years 1000-1999) or {@code '2'} (years 2000+), and every
   * epoch-millis string for a real-world moment also starts with {@code '1'} (millis since 1970 stay 13 digits
   * until the year 2286) - so a millis string compared against a post-2000 ISO date string always loses
   * lexicographically, regardless of which moment is actually later.
   */
  @Test
  void stringVersusDateTimeIsAntisymmetric() {
    final LocalDateTime earlierDate = LocalDateTime.of(2001, 1, 1, 0, 0, 0);
    // Chronologically AFTER earlierDate, despite its epoch-millis string starting with '1' - a lexicographically
    // smaller digit than the date's ISO string ("2001-01-01T00:00").
    final String laterAsEpochMillis =
        String.valueOf(DateUtils.dateTimeToTimestamp(LocalDateTime.of(2023, 11, 14, 0, 0, 0), ChronoUnit.MILLIS));

    final int forward = comparator.compare(laterAsEpochMillis, BinaryTypes.TYPE_STRING, earlierDate, BinaryTypes.TYPE_DATETIME);
    final int backward = comparator.compare(earlierDate, BinaryTypes.TYPE_DATETIME, laterAsEpochMillis, BinaryTypes.TYPE_STRING);

    assertThat(Integer.signum(forward)).isEqualTo(-Integer.signum(backward));
    // The 2023 moment is strictly later than the 2001 date.
    assertThat(forward).isGreaterThan(0);
    assertThat(backward).isLessThan(0);
  }

  /**
   * Same defect as {@link #stringVersusDateTimeIsAntisymmetric()} but for {@code TYPE_DATE}, and using a
   * parseable ISO datetime string (rather than an epoch-millis string) to exercise the date-parsing path end to
   * end - the shape a user's {@code WHERE dateColumn < '2001-06-01T00:00:00'} query would actually hit.
   */
  @Test
  void stringVersusDateIsAntisymmetric() {
    final LocalDate earlierDate = LocalDate.of(2001, 1, 1);
    final String laterIsoDateTime = "2001-06-01T00:00:00";

    final int forward = comparator.compare(laterIsoDateTime, BinaryTypes.TYPE_STRING, earlierDate, BinaryTypes.TYPE_DATE);
    final int backward = comparator.compare(earlierDate, BinaryTypes.TYPE_DATE, laterIsoDateTime, BinaryTypes.TYPE_STRING);

    assertThat(Integer.signum(forward)).isEqualTo(-Integer.signum(backward));
    assertThat(forward).isGreaterThan(0);
    assertThat(backward).isLessThan(0);
  }
}
