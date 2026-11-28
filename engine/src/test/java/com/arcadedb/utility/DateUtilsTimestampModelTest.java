/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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

import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.TimeZone;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Model test of the temporal to epoch-count conversions: {@link DateUtils#dateTimeToTimestamp} (the saturating one a comparison
 * uses) and {@link DateUtils#dateTimeToTimestampExact} (the one a write uses), against an independent reference that does the
 * arithmetic in {@link BigInteger}. Every class a DATETIME can be held as, every precision, instants either side of the epoch.
 */
class DateUtilsTimestampModelTest {
  private static final ChronoUnit[] PRECISIONS = { ChronoUnit.SECONDS, ChronoUnit.MILLIS, ChronoUnit.MICROS, ChronoUnit.NANOS };

  private static final BigInteger LONG_MAX = BigInteger.valueOf(Long.MAX_VALUE);
  private static final BigInteger LONG_MIN = BigInteger.valueOf(Long.MIN_VALUE);

  private static long unitsPerSecond(final ChronoUnit precision) {
    return switch (precision) {
      case SECONDS -> 1L;
      case MILLIS -> 1_000L;
      case MICROS -> 1_000_000L;
      default -> 1_000_000_000L;
    };
  }

  /** floor(second * unitsPerSecond + nano * unitsPerSecond / 1e9), the count of whole units since the epoch. */
  private static BigInteger reference(final long epochSecond, final int nano, final ChronoUnit precision) {
    final BigInteger units = BigInteger.valueOf(unitsPerSecond(precision));
    return BigInteger.valueOf(epochSecond).multiply(units).add(BigInteger.valueOf(nano).multiply(units).divide(BigInteger.valueOf(1_000_000_000L)));
  }

  private static boolean fits(final BigInteger value) {
    return value.compareTo(LONG_MIN) >= 0 && value.compareTo(LONG_MAX) <= 0;
  }

  private static List<long[]> instants() {
    final List<long[]> instants = new ArrayList<>();
    final long[] seconds = { 0L, 1L, -1L, 86_399L, -86_400L, 1_700_000_000L, -1_700_000_000L, 253_402_300_799L, -62_135_596_800L,
        9_223_372_035L, 9_223_372_036L, -9_223_372_036L, -9_223_372_037L, 9_223_372_036_854L, -9_223_372_036_854L, 9_223_372_036_855L,
        -9_223_372_036_855L };
    final int[] nanos = { 0, 1, 999, 1_000, 999_999, 1_000_000, 123_456_789, 854_775_807, 854_775_808, 999_999_999 };
    for (final long second : seconds)
      for (final int nano : nanos)
        instants.add(new long[] { second, nano });
    final Random random = new Random(20260622L);
    for (int i = 0; i < 400; i++)
      instants.add(new long[] { random.nextLong() % 9_223_372_037L, random.nextInt(1_000_000_000) });
    return instants;
  }

  private static List<Object> allClasses(final long epochSecond, final int nano) {
    final Instant instant = Instant.ofEpochSecond(epochSecond, nano);
    final List<Object> values = new ArrayList<>();
    values.add(instant);
    values.add(LocalDateTime.ofEpochSecond(epochSecond, nano, ZoneOffset.UTC));
    values.add(OffsetDateTime.ofInstant(instant, ZoneOffset.ofHoursMinutes(-5, -30)));
    values.add(ZonedDateTime.ofInstant(instant, ZoneId.of("Asia/Kolkata")));
    return values;
  }

  @Test
  void exactConversionOfEveryClassMatchesTheBigIntegerReferenceOrRefuses() {
    int compared = 0;
    int refused = 0;
    for (final long[] pair : instants())
      for (final ChronoUnit precision : PRECISIONS) {
        final BigInteger expected = reference(pair[0], (int) pair[1], precision);
        for (final Object value : allClasses(pair[0], (int) pair[1])) {
          final String what = value.getClass().getSimpleName() + " " + value + " at " + precision;
          if (fits(expected)) {
            assertThat(DateUtils.dateTimeToTimestampExact(null, value, precision)).as(what).isEqualTo(expected.longValueExact());
            compared++;
          } else {
            assertThatThrownBy(() -> DateUtils.dateTimeToTimestampExact(null, value, precision)).as(what)
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("outside the range");
            refused++;
          }
        }
      }
    assertThat(compared).as("instants inside the range of a long were compared").isGreaterThan(1000);
    assertThat(refused).as("instants beyond the range of a long were refused").isGreaterThan(100);
  }

  @Test
  void theSaturatingConversionAgreesWithTheExactOneWhereTheValueFitsAndClampsWhereItDoesNot() {
    for (final long[] pair : instants())
      for (final ChronoUnit precision : PRECISIONS) {
        final BigInteger expected = reference(pair[0], (int) pair[1], precision);
        final long clamped = fits(expected) ? expected.longValueExact() : expected.signum() > 0 ? Long.MAX_VALUE : Long.MIN_VALUE;
        for (final Object value : allClasses(pair[0], (int) pair[1]))
          assertThat(DateUtils.dateTimeToTimestamp(value, precision)).as(value.getClass().getSimpleName() + " " + value + " at " + precision)
              .isEqualTo(clamped);
      }
  }

  @Test
  void theLowestAndHighestCountOfEveryUnitReadBackAsTheCountItWasBuiltFrom() {
    for (final ChronoUnit precision : new ChronoUnit[] { ChronoUnit.MILLIS, ChronoUnit.MICROS, ChronoUnit.NANOS })
      for (final long count : new long[] { Long.MIN_VALUE, Long.MIN_VALUE + 1, Long.MAX_VALUE, Long.MAX_VALUE - 1, 0L, -1L }) {
        for (final Class<?> implementation : new Class<?>[] { Instant.class, LocalDateTime.class, ZonedDateTime.class }) {
          final Object value = DateUtils.dateTime(null, count, precision, implementation, precision);
          final String what = count + " as " + implementation.getSimpleName() + " at " + precision + " = " + value;
          assertThat(DateUtils.dateTimeToTimestampExact(null, value, precision)).as(what).isEqualTo(count);
          assertThat(DateUtils.dateTimeToTimestamp(value, precision)).as(what).isEqualTo(count);
        }
      }
  }

  @Test
  void aFarInstantSaturatesToTheEndOfTheRangeInsteadOfWrappingToTheOtherOne() {
    // past year 294,247 MICROS no longer hold the instant, past year 292,278,994 neither do MILLIS: the same sentinel-date case as NANOS
    final LocalDateTime farFuture = LocalDateTime.of(400_000, 1, 1, 0, 0, 0, 999_999_000);
    final LocalDateTime farPast = LocalDateTime.of(-400_000, 1, 1, 0, 0, 0, 999_999_000);
    assertThat(DateUtils.dateTimeToTimestamp(farFuture, ChronoUnit.MICROS)).isEqualTo(Long.MAX_VALUE);
    assertThat(DateUtils.dateTimeToTimestamp(farFuture.atZone(ZoneOffset.UTC), ChronoUnit.MICROS)).isEqualTo(Long.MAX_VALUE);
    assertThat(DateUtils.dateTimeToTimestamp(farFuture.atOffset(ZoneOffset.UTC), ChronoUnit.MICROS)).isEqualTo(Long.MAX_VALUE);
    assertThat(DateUtils.dateTimeToTimestamp(farFuture.toInstant(ZoneOffset.UTC), ChronoUnit.MICROS)).isEqualTo(Long.MAX_VALUE);
    assertThat(DateUtils.dateTimeToTimestamp(farPast, ChronoUnit.MICROS)).isEqualTo(Long.MIN_VALUE);
    assertThat(DateUtils.dateTimeToTimestamp(farPast, ChronoUnit.NANOS)).isEqualTo(Long.MIN_VALUE);

    final Instant beyondMillis = Instant.parse("+400000000-01-01T00:00:00Z");
    assertThat(DateUtils.dateTimeToTimestamp(beyondMillis, ChronoUnit.MILLIS)).isEqualTo(Long.MAX_VALUE);
    assertThat(DateUtils.dateTimeToTimestamp(beyondMillis.atZone(ZoneOffset.UTC), ChronoUnit.MILLIS)).isEqualTo(Long.MAX_VALUE);
    assertThat(DateUtils.dateTimeToTimestamp(LocalDateTime.of(400_000_000, 1, 1, 0, 0, 0, 999_000_000), ChronoUnit.MILLIS))
        .isEqualTo(Long.MAX_VALUE);
    assertThat(DateUtils.dateTimeToTimestamp(LocalDateTime.of(-400_000_000, 1, 1, 0, 0, 0, 999_000_000), ChronoUnit.MILLIS))
        .isEqualTo(Long.MIN_VALUE);
  }

  @Test
  void aLocalDateIsItsMidnightAtUtcInEveryPrecision() {
    final long[] days = { 0L, 1L, -1L, 19_000L, -719_528L, 106_751L, 106_752L, -106_751L, -106_752L, 106_751_991L, 365_241_780_471L,
        -365_243_219_162L };
    for (final long day : days)
      for (final ChronoUnit precision : PRECISIONS) {
        final BigInteger expected = reference(day * 86_400L, 0, precision);
        final LocalDate date = LocalDate.ofEpochDay(day);
        if (fits(expected)) {
          assertThat(DateUtils.dateTimeToTimestamp(date, precision)).as(date + " at " + precision).isEqualTo(expected.longValueExact());
          assertThat(DateUtils.dateTimeToTimestampExact(null, date, precision)).as(date + " exact at " + precision)
              .isEqualTo(expected.longValueExact());
        } else {
          assertThat(DateUtils.dateTimeToTimestamp(date, precision)).as(date + " at " + precision)
              .isEqualTo(expected.signum() > 0 ? Long.MAX_VALUE : Long.MIN_VALUE);
          assertThatThrownBy(() -> DateUtils.dateTimeToTimestampExact(null, date, precision)).isInstanceOf(IllegalArgumentException.class);
        }
      }
  }

  @Test
  void aMillisecondClassIsNarrowedTowardZeroToSecondsAndWidenedExactlyToTheFinerUnits() {
    final long[] millis = { 0L, 1L, -1L, 999L, 1_000L, 1_001L, -999L, -1_000L, -1_001L, -1_500L, 1_500L, 1_700_000_123_456L,
        -1_700_000_123_456L, Long.MAX_VALUE, Long.MIN_VALUE };
    for (final long ms : millis) {
      final Date date = new Date(ms);
      final Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
      calendar.setTimeInMillis(ms);
      for (final Object value : new Object[] { date, calendar })
        for (final ChronoUnit precision : PRECISIONS) {
          final BigInteger exact = BigInteger.valueOf(ms).multiply(BigInteger.valueOf(unitsPerSecond(precision))).divide(BigInteger.valueOf(1_000L));
          final String what = value.getClass().getSimpleName() + " " + ms + " at " + precision;
          if (fits(exact))
            assertThat(DateUtils.dateTimeToTimestampExact(null, value, precision)).as(what).isEqualTo(exact.longValueExact());
          else
            assertThatThrownBy(() -> DateUtils.dateTimeToTimestampExact(null, value, precision)).as(what)
                .isInstanceOf(IllegalArgumentException.class);
        }
    }
  }
}
