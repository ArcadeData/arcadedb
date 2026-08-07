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
}
