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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link DateUtils#compareNumberAndTemporal}, the comparison of a number with a datetime that {@code =}, {@code <>}, {@code <},
 * {@code <=}, {@code >}, {@code >=} and {@code BETWEEN} share (issue #10560).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class DateUtilsCompareNumberAndTemporalTest {
  // 1970-01-01T00:00:02Z, 2000 epoch milliseconds
  private static final Instant TWO_SECONDS = Instant.ofEpochSecond(2);

  /** Every class the comparison reads, each holding 2000.5 milliseconds where it can hold a fraction of one. */
  private static Object[] datetimesAtTwoThousandAndAHalf() {
    final Instant instant = Instant.ofEpochSecond(2, 500_000);
    return new Object[] { LocalDateTime.ofEpochSecond(2, 500_000, ZoneOffset.UTC), instant.atZone(ZoneOffset.UTC),
        instant.atOffset(ZoneOffset.ofHours(5)), instant, instant.atZone(ZoneId.of("Asia/Tokyo")) };
  }

  /** Every class the comparison reads, at exactly 2000 milliseconds. */
  private static Object[] datetimesAtTwoThousand() {
    final Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("America/Los_Angeles"));
    calendar.setTimeInMillis(2000L);
    return new Object[] { LocalDateTime.ofEpochSecond(2, 0, ZoneOffset.UTC), TWO_SECONDS.atZone(ZoneOffset.UTC),
        TWO_SECONDS.atOffset(ZoneOffset.ofHours(-7)), TWO_SECONDS, new Date(2000L), calendar };
  }

  @Test
  void aWholeNumberIsComparedExactlyOnEitherSide() {
    for (final Object datetime : datetimesAtTwoThousand()) {
      final String name = datetime.getClass().getSimpleName();
      assertThat(DateUtils.compareNumberAndTemporal(2000L, datetime)).as(name).isZero();
      assertThat(DateUtils.compareNumberAndTemporal(datetime, 2000)).as(name).isZero();
      assertThat(DateUtils.compareNumberAndTemporal(1999L, datetime)).as(name).isEqualTo(-1);
      assertThat(DateUtils.compareNumberAndTemporal(datetime, 1999L)).as(name).isEqualTo(1);
      assertThat(DateUtils.compareNumberAndTemporal(2001, datetime)).as(name).isEqualTo(1);
      assertThat(DateUtils.compareNumberAndTemporal(datetime, 2001)).as(name).isEqualTo(-1);
    }
  }

  @Test
  void aFractionIsNeitherDroppedNorRoundedWhicheverClassHoldsTheNumber() {
    final Object[] numbers = { 2000.5d, 2000.5f, new BigDecimal("2000.5"), new BigDecimal("2000.50") };
    for (final Object datetime : datetimesAtTwoThousand())
      for (final Object number : numbers) {
        final String name = datetime.getClass().getSimpleName() + " " + number.getClass().getSimpleName();
        assertThat(DateUtils.compareNumberAndTemporal(number, datetime)).as(name).isEqualTo(1);
        assertThat(DateUtils.compareNumberAndTemporal(datetime, number)).as(name).isEqualTo(-1);
      }

    // the datetimes that hold the half millisecond are equal to it, and later than the whole millisecond below
    for (final Object datetime : datetimesAtTwoThousandAndAHalf())
      for (final Object number : numbers) {
        final String name = datetime.getClass().getSimpleName() + " " + number.getClass().getSimpleName();
        assertThat(DateUtils.compareNumberAndTemporal(number, datetime)).as(name).isZero();
        assertThat(DateUtils.compareNumberAndTemporal(datetime, number)).as(name).isZero();
        assertThat(DateUtils.compareNumberAndTemporal(2000L, datetime)).as(name).isEqualTo(-1);
        assertThat(DateUtils.compareNumberAndTemporal(2000.0d, datetime)).as(name).isEqualTo(-1);
        assertThat(DateUtils.compareNumberAndTemporal(2001L, datetime)).as(name).isEqualTo(1);
        assertThat(DateUtils.compareNumberAndTemporal(2000.25d, datetime)).as(name).isEqualTo(-1);
        assertThat(DateUtils.compareNumberAndTemporal(2000.75d, datetime)).as(name).isEqualTo(1);
      }
  }

  @Test
  void aNegativeFractionIsFlooredNotTruncated() {
    // -1.5 is inside millisecond -2: an instant before the epoch has its fraction counted upward too
    final LocalDateTime beforeTheEpoch = LocalDateTime.ofEpochSecond(-1, 998_500_000, ZoneOffset.UTC);
    assertThat(DateUtils.compareNumberAndTemporal(-1.5d, beforeTheEpoch)).isZero();
    assertThat(DateUtils.compareNumberAndTemporal(-1L, beforeTheEpoch)).isEqualTo(1);
    assertThat(DateUtils.compareNumberAndTemporal(-2L, beforeTheEpoch)).isEqualTo(-1);
  }

  @Test
  void aNumberBeyondTheLongIsAboveOrBelowEveryDatetime() {
    final Object datetime = LocalDateTime.ofEpochSecond(2, 0, ZoneOffset.UTC);
    final Object[] above = { 1e19d, Double.MAX_VALUE, Double.POSITIVE_INFINITY, Float.POSITIVE_INFINITY, new BigDecimal("9223372036854775808"),
        new BigInteger("9223372036854775808"), new BigDecimal("1e30") };
    final Object[] below = { -1e19d, -Double.MAX_VALUE, Double.NEGATIVE_INFINITY, Float.NEGATIVE_INFINITY, new BigDecimal("-9223372036854775809"),
        new BigInteger("-9223372036854775809"), new BigDecimal("-1e30") };
    for (final Object number : above) {
      assertThat(DateUtils.compareNumberAndTemporal(number, datetime)).as(number.toString()).isEqualTo(1);
      assertThat(DateUtils.compareNumberAndTemporal(datetime, number)).as(number.toString()).isEqualTo(-1);
    }
    for (final Object number : below) {
      assertThat(DateUtils.compareNumberAndTemporal(number, datetime)).as(number.toString()).isEqualTo(-1);
      assertThat(DateUtils.compareNumberAndTemporal(datetime, number)).as(number.toString()).isEqualTo(1);
    }
  }

  @Test
  void aNaNIsAboveEveryDatetime() {
    final Object datetime = LocalDateTime.ofEpochSecond(2, 0, ZoneOffset.UTC);
    assertThat(DateUtils.compareNumberAndTemporal(Double.NaN, datetime)).isEqualTo(1);
    assertThat(DateUtils.compareNumberAndTemporal(Float.NaN, datetime)).isEqualTo(1);
    assertThat(DateUtils.compareNumberAndTemporal(datetime, Double.NaN)).isEqualTo(-1);
  }

  @Test
  void aPairThatIsNotANumberAgainstADatetimeIsLeftToTheCaller() {
    assertThat(DateUtils.compareNumberAndTemporal(2000L, "2000")).isEqualTo(DateUtils.NOT_NUMBER_AND_TEMPORAL);
    assertThat(DateUtils.compareNumberAndTemporal(2000L, LocalDate.of(1970, 1, 2))).isEqualTo(DateUtils.NOT_NUMBER_AND_TEMPORAL);
    assertThat(DateUtils.compareNumberAndTemporal(2000L, 2000.5d)).isEqualTo(DateUtils.NOT_NUMBER_AND_TEMPORAL);
    assertThat(DateUtils.compareNumberAndTemporal(LocalDateTime.of(2000, 1, 1, 0, 0), Instant.EPOCH)).isEqualTo(DateUtils.NOT_NUMBER_AND_TEMPORAL);
    assertThat(DateUtils.compareNumberAndTemporal(null, Instant.EPOCH)).isEqualTo(DateUtils.NOT_NUMBER_AND_TEMPORAL);
    assertThat(DateUtils.compareNumberAndTemporal(2000L, null)).isEqualTo(DateUtils.NOT_NUMBER_AND_TEMPORAL);
    // a year no long of milliseconds reaches
    assertThat(DateUtils.compareNumberAndTemporal(2000L, LocalDateTime.MAX)).isEqualTo(DateUtils.NOT_NUMBER_AND_TEMPORAL);
  }

  /**
   * The push-down reads a date/time bound through {@link DateUtils#toEpochMillis}, the generic comparison through
   * {@link DateUtils#compareNumberAndTemporal}: they have to name the same millisecond for the same value, whichever time zone the
   * JVM runs in, or a bound would select other samples than the same comparison keeps.
   */
  @Test
  void theComparisonAndTheEpochMillisecondsOfADatetimeAgreeInAnyTimeZone() {
    final TimeZone previous = TimeZone.getDefault();
    try {
      for (final String zone : new String[] { "UTC", "Asia/Tokyo", "America/Los_Angeles", "Pacific/Chatham" }) {
        TimeZone.setDefault(TimeZone.getTimeZone(zone));
        final Object[] values = { LocalDateTime.of(2026, 10, 21, 20, 30, 15, 123_000_000),
            ZonedDateTime.of(2026, 10, 21, 20, 30, 15, 123_000_000, ZoneId.of("Europe/Rome")),
            OffsetDateTime.of(2026, 10, 21, 20, 30, 15, 123_000_000, ZoneOffset.ofHours(-3)), Instant.parse("2026-10-21T20:30:15.123Z"),
            new Date(Instant.parse("2026-10-21T20:30:15.123Z").toEpochMilli()) };
        for (final Object value : values) {
          final long millis = DateUtils.toEpochMillis(value);
          assertThat(DateUtils.compareNumberAndTemporal(millis, value)).as(zone + " " + value.getClass().getSimpleName()).isZero();
          assertThat(DateUtils.compareNumberAndTemporal(millis - 1, value)).as(zone + " " + value.getClass().getSimpleName()).isEqualTo(-1);
        }
      }
    } finally {
      TimeZone.setDefault(previous);
    }
  }

  /** {@link DateUtils#subMillisecondNanos} reads the same classes as the comparison, so it never names a fraction the comparison ignores. */
  @Test
  void theSubMillisecondPartAndTheComparisonReadTheSameClasses() {
    for (final Object datetime : datetimesAtTwoThousandAndAHalf()) {
      assertThat(DateUtils.subMillisecondNanos(datetime)).as(datetime.getClass().getSimpleName()).isEqualTo(500_000);
      // 2000.0 is below the datetime exactly when the fraction is read
      assertThat(DateUtils.compareNumberAndTemporal(2000L, datetime)).as(datetime.getClass().getSimpleName()).isEqualTo(-1);
    }
    for (final Object datetime : datetimesAtTwoThousand()) {
      assertThat(DateUtils.subMillisecondNanos(datetime)).as(datetime.getClass().getSimpleName()).isZero();
      assertThat(DateUtils.compareNumberAndTemporal(2000L, datetime)).as(datetime.getClass().getSimpleName()).isZero();
    }
    assertThat(DateUtils.subMillisecondNanos("2000")).isZero();
    assertThat(DateUtils.subMillisecondNanos(null)).isZero();
  }
}
