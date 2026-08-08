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
package com.arcadedb.utility;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class DateUtilsTest {

  /**
   * Regression for issue #4142: {@link DateUtils#dateTimeToTimestamp(Object, ChronoUnit)} is
   * the conversion landing path used by the GraphBatch edge serializer. Until #4142 it called
   * {@link LocalDateTime#parse(CharSequence)} directly, which rejects ISO-8601 strings ending
   * with {@code Z} or any {@code ±HH:mm} offset - the default JSON datetime serialization in
   * most languages.
   */
  @Test
  void parsesIsoDateTimeWithZSuffix() {
    final long expected = LocalDateTime.parse("2026-05-08T18:37:54").toEpochSecond(ZoneOffset.UTC);
    assertThat(DateUtils.dateTimeToTimestamp("2026-05-08T18:37:54Z", ChronoUnit.SECONDS)).isEqualTo(expected);
  }

  @Test
  void parsesIsoDateTimeWithMillisAndZSuffix() {
    final long expected = LocalDateTime.parse("2026-05-08T18:37:54").toEpochSecond(ZoneOffset.UTC) * 1_000L;
    assertThat(DateUtils.dateTimeToTimestamp("2026-05-08T18:37:54.000Z", ChronoUnit.MILLIS)).isEqualTo(expected);
  }

  @Test
  void parsesIsoDateTimeWithExplicitOffset() {
    final long expected = LocalDateTime.parse("2026-05-08T18:37:54").toEpochSecond(ZoneOffset.UTC);
    assertThat(DateUtils.dateTimeToTimestamp("2026-05-08T18:37:54+00:00", ChronoUnit.SECONDS)).isEqualTo(expected);
  }

  @Test
  void parsesIsoDateTimeWithoutZone() {
    final long expected = LocalDateTime.parse("2026-05-08T18:37:54").toEpochSecond(ZoneOffset.UTC);
    assertThat(DateUtils.dateTimeToTimestamp("2026-05-08T18:37:54", ChronoUnit.SECONDS)).isEqualTo(expected);
  }

  @Test
  void zoneSuffixYieldsSameTimestampAsLocalDateTime() {
    // Both inputs must collapse to the same UTC epoch value: the offset is a wall-clock hint
    // that the engine drops at ingest time so vertex and edge paths agree on storage.
    final Long withZ = DateUtils.dateTimeToTimestamp("2026-05-08T18:37:54Z", ChronoUnit.MILLIS);
    final Long withoutZone = DateUtils.dateTimeToTimestamp("2026-05-08T18:37:54", ChronoUnit.MILLIS);
    assertThat(withZ).isEqualTo(withoutZone);
  }

  /**
   * Regression for issue #5625: converting a far-future date - e.g. a "9999-12-31"/"2499-12-31" sentinel
   * commonly used to mean "no expiration" - to {@link ChronoUnit#NANOS} precision used to overflow a
   * {@code long} twice: once inside {@code TimeUnit.NANOSECONDS.convert(epochSeconds, SECONDS)} (which itself
   * saturates to {@link Long#MAX_VALUE}), and again when the sub-second nanosecond fraction was added on top of
   * that already-saturated value with no overflow guard, wrapping around to a large NEGATIVE number. That
   * silently inverted chronological order: the far-future sentinel compared as LESS than an ordinary
   * near-present date wherever the NANOS-precision timestamp was used for comparison (Gremlin's
   * {@code Compare.lt/lte/gt/gte} route through {@code GremlinValueComparator}, which always normalises
   * date/time operands to NANOS regardless of their actual precision).
   * <p>
   * The fix clamps the NANOS-precision conversion to {@link Long#MAX_VALUE} instead of wrapping, so ordering is
   * preserved for every representable {@link LocalDateTime}, no matter how far in the future.
   */
  @Test
  void nanosPrecisionOfFarFutureLocalDateTimeDoesNotOverflowAndPreservesOrdering() {
    final LocalDateTime nearPresent = LocalDateTime.of(2020, 1, 1, 0, 0, 0, 0);
    final LocalDateTime farFutureSentinel = LocalDateTime.of(2499, 12, 31, 23, 59, 59, 999_000_000);

    final long nearPresentNanos = DateUtils.dateTimeToTimestamp(nearPresent, ChronoUnit.NANOS);
    final long farFutureNanos = DateUtils.dateTimeToTimestamp(farFutureSentinel, ChronoUnit.NANOS);

    assertThat(nearPresentNanos).isPositive();
    assertThat(farFutureNanos)
        .as("the far-future sentinel must clamp to Long.MAX_VALUE instead of wrapping to a negative number")
        .isEqualTo(Long.MAX_VALUE);
    assertThat(farFutureNanos)
        .as("chronological order must be preserved: a far-future date must compare AFTER a near-present one")
        .isGreaterThan(nearPresentNanos);
  }

  @Test
  void nanosPrecisionOfFarFutureZonedDateTimeDoesNotOverflowAndPreservesOrdering() {
    final ZonedDateTime nearPresent = ZonedDateTime.of(2020, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);
    final ZonedDateTime farFutureSentinel = ZonedDateTime.of(2499, 12, 31, 23, 59, 59, 999_000_000, ZoneOffset.UTC);

    final long nearPresentNanos = DateUtils.dateTimeToTimestamp(nearPresent, ChronoUnit.NANOS);
    final long farFutureNanos = DateUtils.dateTimeToTimestamp(farFutureSentinel, ChronoUnit.NANOS);

    assertThat(farFutureNanos).isEqualTo(Long.MAX_VALUE);
    assertThat(farFutureNanos).isGreaterThan(nearPresentNanos);
  }

  @Test
  void nanosPrecisionOfFarFutureInstantDoesNotOverflowAndPreservesOrdering() {
    final Instant nearPresent = LocalDateTime.of(2020, 1, 1, 0, 0, 0, 0).toInstant(ZoneOffset.UTC);
    final Instant farFutureSentinel = LocalDateTime.of(2499, 12, 31, 23, 59, 59, 999_000_000).toInstant(ZoneOffset.UTC);

    final long nearPresentNanos = DateUtils.dateTimeToTimestamp(nearPresent, ChronoUnit.NANOS);
    final long farFutureNanos = DateUtils.dateTimeToTimestamp(farFutureSentinel, ChronoUnit.NANOS);

    assertThat(farFutureNanos).isEqualTo(Long.MAX_VALUE);
    assertThat(farFutureNanos).isGreaterThan(nearPresentNanos);
  }

  @Test
  void nanosPrecisionOfFarFutureOffsetDateTimeDoesNotOverflowAndPreservesOrdering() {
    final OffsetDateTime nearPresent = OffsetDateTime.of(2020, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);
    final OffsetDateTime farFutureSentinel = OffsetDateTime.of(2499, 12, 31, 23, 59, 59, 999_000_000, ZoneOffset.UTC);

    final long nearPresentNanos = DateUtils.dateTimeToTimestamp(nearPresent, ChronoUnit.NANOS);
    final long farFutureNanos = DateUtils.dateTimeToTimestamp(farFutureSentinel, ChronoUnit.NANOS);

    assertThat(farFutureNanos).isEqualTo(Long.MAX_VALUE);
    assertThat(farFutureNanos).isGreaterThan(nearPresentNanos);
  }

  /**
   * {@link LocalDate} took a different, multiplication-based overflow path (`millis * 1_000_000_000L`) that
   * wrapped even earlier than the addition-based date/time types above - any date whose epoch millis exceeded
   * roughly 9.2 million (year ~1970 + 107 days) would already overflow at MICROS or NANOS precision.
   */
  @Test
  void nanosPrecisionOfFarFutureLocalDateDoesNotOverflowAndPreservesOrdering() {
    final LocalDate nearPresent = LocalDate.of(2020, 1, 1);
    final LocalDate farFutureSentinel = LocalDate.of(2499, 12, 31);

    final long nearPresentNanos = DateUtils.dateTimeToTimestamp(nearPresent, ChronoUnit.NANOS);
    final long farFutureNanos = DateUtils.dateTimeToTimestamp(farFutureSentinel, ChronoUnit.NANOS);

    assertThat(farFutureNanos).isEqualTo(Long.MAX_VALUE);
    assertThat(farFutureNanos).isGreaterThan(nearPresentNanos);
  }

  /**
   * Regression for issue #5956: {@link DateUtils#dateTimeToTimestampInferringStringPrecision(Object, ChronoUnit)}
   * infers a bare numeric string's own precision from its digit count (present-day epoch values grow by ~3 digits
   * per finer step: ~10 for seconds, ~13 for millis, ~16 for micros, ~19 for nanos) and converts it to
   * {@code precisionToUse}, instead of assuming the raw digits already are at {@code precisionToUse} - a
   * nanos-epoch string asked for at {@code MILLIS} used to come back 6 orders of magnitude wrong.
   */
  @Test
  void numericStringInfersOwnPrecisionFromDigitCountInsteadOfAssumingPrecisionToUse() {
    // 10 digits: looks like epoch-SECONDS (2023-11-14T22:13:20Z).
    final String epochSecondsString = "1700000000";
    assertThat(DateUtils.dateTimeToTimestampInferringStringPrecision(epochSecondsString, ChronoUnit.SECONDS)).isEqualTo(1_700_000_000L);
    assertThat(DateUtils.dateTimeToTimestampInferringStringPrecision(epochSecondsString, ChronoUnit.MILLIS)).isEqualTo(1_700_000_000_000L);

    // 13 digits: looks like epoch-MILLIS.
    final String epochMillisString = "1700000000123";
    assertThat(DateUtils.dateTimeToTimestampInferringStringPrecision(epochMillisString, ChronoUnit.MILLIS)).isEqualTo(1_700_000_000_123L);
    assertThat(DateUtils.dateTimeToTimestampInferringStringPrecision(epochMillisString, ChronoUnit.SECONDS)).isEqualTo(1_700_000_000L);

    // 16 digits: looks like epoch-MICROS.
    final String epochMicrosString = "1700000000123456";
    assertThat(DateUtils.dateTimeToTimestampInferringStringPrecision(epochMicrosString, ChronoUnit.MICROS)).isEqualTo(1_700_000_000_123_456L);
    assertThat(DateUtils.dateTimeToTimestampInferringStringPrecision(epochMicrosString, ChronoUnit.MILLIS)).isEqualTo(1_700_000_000_123L);

    // 19 digits: looks like epoch-NANOS - the exact case raised in #5956.
    final String epochNanosString = "1700000000123456789";
    assertThat(DateUtils.dateTimeToTimestampInferringStringPrecision(epochNanosString, ChronoUnit.NANOS)).isEqualTo(1_700_000_000_123_456_789L);
    assertThat(DateUtils.dateTimeToTimestampInferringStringPrecision(epochNanosString, ChronoUnit.MILLIS)).isEqualTo(1_700_000_000_123L);
    assertThat(DateUtils.dateTimeToTimestampInferringStringPrecision(epochNanosString, ChronoUnit.SECONDS)).isEqualTo(1_700_000_000L);
  }

  /**
   * Off-boundary digit counts (11/12, 14/15, 17/18) must resolve to the same unit as their bucket's edges - this
   * locks in the exact thresholds documented on {@code DateUtils#inferEpochPrecision}, per code review follow-up
   * on PR #5960.
   */
  @Test
  void numericStringPrecisionInferenceCoversOffBoundaryDigitCounts() {
    // 11 and 12 digits: still MILLIS (bucket is 11-13 digits).
    assertThat(DateUtils.dateTimeToTimestampInferringStringPrecision("17000000000", ChronoUnit.MILLIS)).isEqualTo(17_000_000_000L);
    assertThat(DateUtils.dateTimeToTimestampInferringStringPrecision("170000000000", ChronoUnit.MILLIS)).isEqualTo(170_000_000_000L);

    // 14 and 15 digits: still MICROS (bucket is 14-16 digits).
    assertThat(DateUtils.dateTimeToTimestampInferringStringPrecision("17000000001234", ChronoUnit.MICROS)).isEqualTo(17_000_000_001_234L);
    assertThat(DateUtils.dateTimeToTimestampInferringStringPrecision("170000000012345", ChronoUnit.MICROS)).isEqualTo(170_000_000_012_345L);

    // 17 and 18 digits: still NANOS (bucket is 17+ digits).
    assertThat(DateUtils.dateTimeToTimestampInferringStringPrecision("17000000001234567", ChronoUnit.NANOS)).isEqualTo(17_000_000_001_234_567L);
    assertThat(DateUtils.dateTimeToTimestampInferringStringPrecision("170000000012345678", ChronoUnit.NANOS)).isEqualTo(170_000_000_012_345_678L);
  }

  /**
   * A small value matches the digit count of more than one precision bucket at once (e.g. {@code "5"} is a valid
   * 1-digit SECONDS value, but also a valid 1-digit MILLIS/MICROS/NANOS value within the first fraction of a
   * second after the epoch) - the coarser unit (SECONDS) must win every such tie, per the documented tie-break
   * rule on {@code DateUtils#inferEpochPrecision}.
   */
  @Test
  void numericStringPrecisionInferenceTieBreaksTowardCoarserUnit() {
    final String nearZero = "5";
    assertThat(DateUtils.dateTimeToTimestampInferringStringPrecision(nearZero, ChronoUnit.SECONDS)).isEqualTo(5L);
    // Interpreted as 5 SECONDS, converted (not left as raw "5") to every finer target precision.
    assertThat(DateUtils.dateTimeToTimestampInferringStringPrecision(nearZero, ChronoUnit.MILLIS)).isEqualTo(5_000L);
    assertThat(DateUtils.dateTimeToTimestampInferringStringPrecision(nearZero, ChronoUnit.MICROS)).isEqualTo(5_000_000L);
    assertThat(DateUtils.dateTimeToTimestampInferringStringPrecision(nearZero, ChronoUnit.NANOS)).isEqualTo(5_000_000_000L);
  }

  /**
   * Guard against over-fixing: the original {@link DateUtils#dateTimeToTimestamp(Object, ChronoUnit)} must keep its
   * pre-#5956 raw-digits behavior for numeric strings. It is shared by {@code MathExpression}'s date {@code +}/
   * {@code -} arithmetic, where a numeric string operand represents a raw duration/offset count to add at the
   * date's own precision (e.g. {@code date + '10'} meaning "10 units of the date's precision") - digit-count
   * inference would silently turn a small offset into a wildly different one there. Only
   * {@code dateTimeToTimestampInferringStringPrecision} (used by comparison-context callers, where the numeric
   * string represents an independent absolute moment) applies the inference.
   */
  @Test
  void plainDateTimeToTimestampKeepsRawDigitsBehaviorForMathExpressionCompatibility() {
    // "10" is a 2-digit string that would infer as SECONDS: the plain method must NOT convert it, unlike the
    // inferring one.
    assertThat(DateUtils.dateTimeToTimestamp("10", ChronoUnit.MILLIS)).isEqualTo(10L);
    assertThat(DateUtils.dateTimeToTimestampInferringStringPrecision("10", ChronoUnit.MILLIS)).isEqualTo(10_000L);
  }

  /**
   * Regression for a code-review follow-up on PR #5960: a 16-digit numeric string infers as {@code MICROS}, and
   * {@link DateUtils#convertTimestamp} widening it to {@code NANOS} used to be a raw {@code * 1_000} with no
   * overflow guard - a value above {@code Long.MAX_VALUE / 1000} silently wrapped to a large negative number
   * instead of saturating, which would have inverted a {@code BinaryComparator.compareTo()}-based comparison
   * (that entry point always widens to {@code NANOS}). {@code convertTimestamp} now delegates to
   * {@link java.util.concurrent.TimeUnit#convert}, which saturates to {@link Long#MAX_VALUE} on overflow instead.
   */
  @Test
  void numericStringPrecisionInferenceSaturatesInsteadOfOverflowingOnWideningToNanos() {
    // 16 digits: infers as MICROS. Value chosen above Long.MAX_VALUE / 1_000 (~9_223_372_036_854_775) so the
    // MICROS -> NANOS widening multiplication would overflow a signed 64-bit long.
    final String microsAboveNanosRange = "9999999999999999";
    assertThat(DateUtils.dateTimeToTimestampInferringStringPrecision(microsAboveNanosRange, ChronoUnit.NANOS))
        .as("must saturate to Long.MAX_VALUE instead of silently wrapping to a negative number")
        .isEqualTo(Long.MAX_VALUE);
  }
}
