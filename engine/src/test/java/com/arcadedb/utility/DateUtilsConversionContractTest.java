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

import com.arcadedb.TestHelper;
import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Calendar;
import java.util.Date;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Contract of the conversions between a stored count and the class a DATETIME or a DATE is held as: {@link DateUtils#dateTime},
 * {@link DateUtils#date}, {@link DateUtils#convertTimestamp}, {@link DateUtils#dateNumberToEpochDays} and
 * {@link DateUtils#getDate}, against a BigInteger reference.
 */
class DateUtilsConversionContractTest extends TestHelper {
  private static final ChronoUnit[] UNITS = { ChronoUnit.SECONDS, ChronoUnit.MILLIS, ChronoUnit.MICROS, ChronoUnit.NANOS };

  private static BigInteger unitsPerSecond(final ChronoUnit unit) {
    return BigInteger.valueOf(switch (unit) {
      case SECONDS -> 1L;
      case MILLIS -> 1_000L;
      case MICROS -> 1_000_000L;
      default -> 1_000_000_000L;
    });
  }

  /** The count in {@code to} of a count in {@code from}: widened exactly, narrowed toward zero, as TimeUnit does. */
  private static BigInteger convert(final long count, final ChronoUnit from, final ChronoUnit to) {
    return BigInteger.valueOf(count).multiply(unitsPerSecond(to)).divide(unitsPerSecond(from));
  }

  private static boolean fits(final BigInteger value) {
    return value.bitLength() < 64;
  }

  private static Instant instantOf(final BigInteger count, final ChronoUnit unit) {
    final BigInteger[] parts = count.divideAndRemainder(unitsPerSecond(unit));
    long seconds = parts[0].longValueExact();
    long units = parts[1].longValueExact();
    if (units < 0) {
      seconds--;
      units += unitsPerSecond(unit).longValueExact();
    }
    return Instant.ofEpochSecond(seconds, units * (1_000_000_000L / unitsPerSecond(unit).longValueExact()));
  }

  @Test
  void aCountBecomesTheInstantItNamesInTheClassAskedFor() {
    final long[] counts = { 0L, 1L, -1L, 999L, -999L, 1_001L, -1_001L, 86_400L, 1_700_000_123L, -1_700_000_123L, 1_700_000_123_456L,
        -1_700_000_123_456L, 1_700_000_123_456_789L, -1_700_000_123_456_789L, 1_700_000_123_456_789_012L, -1_700_000_123_456_789_012L };
    int checked = 0;
    for (final long count : counts)
      for (final ChronoUnit source : UNITS)
        for (final ChronoUnit destination : UNITS) {
          final BigInteger converted = convert(count, source, destination);
          // a count a java.time.Instant cannot hold (past 31,556,889,864,403,199 seconds) has no instant to compare with
          if (!fits(converted) || converted.divide(unitsPerSecond(destination)).abs().compareTo(BigInteger.valueOf(31_556_889_864_403_199L)) > 0)
            continue;
          final Instant expected = instantOf(converted, destination);
          final String what = count + " " + source + " -> " + destination;
          assertThat(DateUtils.dateTime(database, count, source, Instant.class, destination)).as(what + " Instant").isEqualTo(expected);
          assertThat(DateUtils.dateTime(database, count, source, LocalDateTime.class, destination)).as(what + " LocalDateTime")
              .isEqualTo(LocalDateTime.ofInstant(expected, ZoneOffset.UTC));
          assertThat(DateUtils.dateTime(database, count, source, ZonedDateTime.class, destination)).as(what + " ZonedDateTime")
              .isEqualTo(ZonedDateTime.ofInstant(expected, ZoneOffset.UTC));

          if (destination == ChronoUnit.SECONDS || destination == ChronoUnit.MILLIS) {
            // a Date and a Calendar hold milliseconds whatever the precision the count came in
            final long millis = destination == ChronoUnit.SECONDS ? converted.longValueExact() * 1_000L : converted.longValueExact();
            assertThat(((Date) DateUtils.dateTime(database, count, source, Date.class, destination)).getTime()).as(what + " Date")
                .isEqualTo(millis);
            assertThat(((Calendar) DateUtils.dateTime(database, count, source, Calendar.class, destination)).getTimeInMillis())
                .as(what + " Calendar").isEqualTo(millis);
          } else {
            assertThatThrownBy(() -> DateUtils.dateTime(database, count, source, Date.class, destination)).as(what + " Date")
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("java.util.Date");
            assertThatThrownBy(() -> DateUtils.dateTime(database, count, source, Calendar.class, destination)).as(what + " Calendar")
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("java.util.Calendar");
          }
          checked++;
        }
    assertThat(checked).isGreaterThan(150);
  }

  @Test
  void aClassNoDateTimeIsHeldAsIsRefused() {
    assertThatThrownBy(() -> DateUtils.dateTime(database, 0L, ChronoUnit.MILLIS, String.class, ChronoUnit.MILLIS))
        .hasMessageContaining("java.lang.String");
    assertThatThrownBy(() -> DateUtils.date(database, 0L, String.class)).hasMessageContaining("java.lang.String");
  }

  @Test
  void aDayCountBecomesTheMidnightAtUtcOfThatDayInEveryClassADateIsHeldAs() {
    for (final long days : new long[] { 0L, 1L, -1L, 365L, -365L, 19_000L, -719_528L, 2_932_896L }) {
      final LocalDate day = LocalDate.ofEpochDay(days);
      assertThat(DateUtils.date(database, days, LocalDate.class)).isEqualTo(day);
      assertThat(DateUtils.date(database, days, LocalDateTime.class)).isEqualTo(day.atStartOfDay());
      assertThat(((Date) DateUtils.date(database, days, Date.class)).getTime()).isEqualTo(days * DateUtils.MS_IN_A_DAY);
      assertThat(((Calendar) DateUtils.date(database, days, Calendar.class)).getTimeInMillis()).isEqualTo(days * DateUtils.MS_IN_A_DAY);
    }
  }

  @Test
  void convertTimestampWidensExactlyAndNarrowsTowardZero() {
    final long[] counts = { 0L, 1L, -1L, 999L, -999L, 1_000L, 1_500L, -1_500L, 123_456_789L, -123_456_789L };
    for (final long count : counts)
      for (final ChronoUnit from : UNITS)
        for (final ChronoUnit to : UNITS)
          assertThat(DateUtils.convertTimestamp(count, from, to)).as(count + " " + from + " -> " + to)
              .isEqualTo(convert(count, from, to).longValueExact());
    // a count that does not fit the finer unit saturates, never wraps
    assertThat(DateUtils.convertTimestamp(Long.MAX_VALUE, ChronoUnit.SECONDS, ChronoUnit.NANOS)).isEqualTo(Long.MAX_VALUE);
    assertThat(DateUtils.convertTimestamp(Long.MIN_VALUE, ChronoUnit.SECONDS, ChronoUnit.NANOS)).isEqualTo(Long.MIN_VALUE);
    assertThat(DateUtils.convertTimestamp(7L, ChronoUnit.MILLIS, ChronoUnit.MILLIS)).isEqualTo(7L);
    assertThatThrownBy(() -> DateUtils.convertTimestamp(1L, ChronoUnit.DAYS, ChronoUnit.MILLIS)).isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Days");
    assertThatThrownBy(() -> DateUtils.convertTimestamp(1L, ChronoUnit.MILLIS, ChronoUnit.HOURS)).isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Hours");
  }

  @Test
  void aNumberWrittenIntoADateIsADayCountUnlessItIsTheMillisecondsOfAUtcMidnightOrBeyondEveryDayCount() {
    final long max = DateUtils.MAX_DATE_DAY_COUNT;
    assertThat(max).isEqualTo(Long.MAX_VALUE / DateUtils.MS_IN_A_DAY);

    // zero is the same day read either way
    assertThat(DateUtils.dateNumberToEpochDays(0L)).isZero();
    // the milliseconds of a UTC midnight are that day, whatever its sign
    assertThat(DateUtils.dateNumberToEpochDays(DateUtils.MS_IN_A_DAY)).isEqualTo(1L);
    assertThat(DateUtils.dateNumberToEpochDays(-DateUtils.MS_IN_A_DAY)).isEqualTo(-1L);
    assertThat(DateUtils.dateNumberToEpochDays(19_000L * DateUtils.MS_IN_A_DAY)).isEqualTo(19_000L);
    assertThat(DateUtils.dateNumberToEpochDays(-max * DateUtils.MS_IN_A_DAY)).isEqualTo(-max);
    // any other number up to MAX_DATE_DAY_COUNT either side of zero is a count of days
    assertThat(DateUtils.dateNumberToEpochDays(1L)).isEqualTo(1L);
    assertThat(DateUtils.dateNumberToEpochDays(-1L)).isEqualTo(-1L);
    assertThat(DateUtils.dateNumberToEpochDays(DateUtils.MS_IN_A_DAY + 1)).isEqualTo(DateUtils.MS_IN_A_DAY + 1);
    assertThat(DateUtils.dateNumberToEpochDays(max)).isEqualTo(max);
    assertThat(DateUtils.dateNumberToEpochDays(-max)).isEqualTo(-max);
    // past that it is the milliseconds it is, and stands for the UTC day they fall in, floored
    assertThat(DateUtils.dateNumberToEpochDays(max + 1)).isEqualTo(Math.floorDiv(max + 1, DateUtils.MS_IN_A_DAY));
    assertThat(DateUtils.dateNumberToEpochDays(-max - 1)).isEqualTo(Math.floorDiv(-max - 1, DateUtils.MS_IN_A_DAY));
    assertThat(DateUtils.dateNumberToEpochDays(19_675L * DateUtils.MS_IN_A_DAY + 1)).isEqualTo(19_675L);
    assertThat(DateUtils.dateNumberToEpochDays(19_676L * DateUtils.MS_IN_A_DAY - 1)).isEqualTo(19_675L);
    assertThat(DateUtils.dateNumberToEpochDays(-1_700_000_000_001L)).isEqualTo(-19_676L);
    assertThat(DateUtils.dateNumberToEpochDays(Long.MAX_VALUE)).isEqualTo(max);
    // the lowest day a java.util.Date can hold, and the milliseconds one past it
    final long lowestMidnight = -max * DateUtils.MS_IN_A_DAY;
    assertThat(DateUtils.dateNumberToEpochDays(lowestMidnight + 1)).isEqualTo(-max);
    assertThat(DateUtils.dateNumberToEpochDays(lowestMidnight)).isEqualTo(-max);
    assertThatThrownBy(() -> DateUtils.dateNumberToEpochDays(lowestMidnight - 1)).isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("outside the supported range");
    assertThatThrownBy(() -> DateUtils.dateNumberToEpochDays(Long.MIN_VALUE)).isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(String.valueOf(Long.MIN_VALUE));

    assertThat(DateUtils.isDateNumberMillis(max)).isFalse();
    assertThat(DateUtils.isDateNumberMillis(-max)).isFalse();
    assertThat(DateUtils.isDateNumberMillis(0L)).isFalse();
    assertThat(DateUtils.isDateNumberMillis(max + 1)).isTrue();
    assertThat(DateUtils.isDateNumberMillis(-max - 1)).isTrue();
    assertThat(DateUtils.isDateNumberMillis(Long.MAX_VALUE)).isTrue();
    assertThat(DateUtils.isDateNumberMillis(Long.MIN_VALUE)).isTrue();
  }

  @Test
  void getDateConvertsToTheClassAskedForAndKeepsAnInstanceOfIt() {
    final Instant instant = Instant.parse("1969-12-31T12:00:00.250Z");
    final LocalDateTime wall = LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
    assertThat(DateUtils.getDate(null, Date.class)).isNull();
    assertThat(DateUtils.getDate(wall, LocalDateTime.class)).isSameAs(wall);
    assertThat(((Date) DateUtils.getDate(wall, Date.class)).getTime()).isEqualTo(instant.toEpochMilli());
    assertThat(((Calendar) DateUtils.getDate(wall, Calendar.class)).getTimeInMillis()).isEqualTo(instant.toEpochMilli());
    // before the epoch a day is floored, not truncated toward zero, and a sub-second part is never negative
    assertThat(DateUtils.getDate(wall, LocalDate.class)).isEqualTo(LocalDate.of(1969, 12, 31));
    assertThat(DateUtils.getDate(new Date(-1L), LocalDateTime.class)).isEqualTo(LocalDateTime.of(1969, 12, 31, 23, 59, 59, 999_000_000));
    assertThat(DateUtils.getDate(new Date(-1L), LocalDate.class)).isEqualTo(LocalDate.of(1969, 12, 31));
    // a date is its midnight in a date-time, and refused by the millisecond classes when it is past what they hold
    assertThat(DateUtils.getDate(LocalDate.of(2024, 2, 29), LocalDateTime.class)).isEqualTo(LocalDateTime.of(2024, 2, 29, 0, 0));
    assertThat(((Date) DateUtils.getDate(LocalDate.of(2024, 2, 29), Date.class)).getTime())
        .isEqualTo(LocalDate.of(2024, 2, 29).toEpochDay() * DateUtils.MS_IN_A_DAY);
    assertThatThrownBy(() -> DateUtils.getDate(LocalDate.MAX, Date.class)).isInstanceOf(ArithmeticException.class);
    assertThatThrownBy(() -> DateUtils.getDate(LocalDate.MIN, Calendar.class)).isInstanceOf(ArithmeticException.class);
    // a class it does not know leaves the value as it is
    assertThat(DateUtils.getDate(wall, String.class)).isSameAs(wall);
  }

  @Test
  void aValueIsAnInstantOnlyWhenItNamesOne() {
    assertThat(DateUtils.isDate(null)).isFalse();
    assertThat(DateUtils.isDate("2024-01-01")).isFalse();
    assertThat(DateUtils.isDate(5L)).isFalse();
    for (final Object date : new Object[] { new Date(), Calendar.getInstance(), LocalDate.now(), LocalDateTime.now(), ZonedDateTime.now(),
        OffsetDateTime.now(), Instant.now() })
      assertThat(DateUtils.isDate(date)).as(date.getClass().getSimpleName()).isTrue();

    assertThat(DateUtils.toInstant(new Date(1_000L))).isEqualTo(Instant.ofEpochSecond(1));
    assertThat(DateUtils.toInstant(LocalDateTime.of(1970, 1, 1, 0, 0, 1))).isEqualTo(Instant.ofEpochSecond(1));
    assertThat(DateUtils.toInstant(LocalDate.of(1970, 1, 2))).isEqualTo(Instant.ofEpochSecond(86_400));
    assertThat(DateUtils.toInstant("x")).isNull();
    assertThat(DateUtils.toInstant(null)).isNull();

    assertThat(DateUtils.getPrecisionFromValue(new Date())).isEqualTo(ChronoUnit.MILLIS);
    assertThat(DateUtils.getPrecisionFromValue(Calendar.getInstance())).isEqualTo(ChronoUnit.MILLIS);
    assertThat(DateUtils.getPrecisionFromValue(Instant.ofEpochSecond(5, 1_000))).isEqualTo(ChronoUnit.MICROS);
    assertThat(DateUtils.getPrecisionFromValue(Instant.ofEpochSecond(5, 1))).isEqualTo(ChronoUnit.NANOS);
    assertThat(DateUtils.getPrecisionFromValue(Instant.ofEpochSecond(5, 5_000_000))).isEqualTo(ChronoUnit.MILLIS);
    assertThat(DateUtils.getPrecisionFromValue(Instant.ofEpochSecond(5))).isEqualTo(ChronoUnit.SECONDS);
    assertThat(DateUtils.getPrecisionFromValue(LocalDate.now())).isNull();
    assertThat(DateUtils.getPrecisionFromValue("x")).isNull();
    assertThat(DateUtils.getPrecision(1_000_000_000 - 1)).isEqualTo(ChronoUnit.NANOS);
    assertThat(DateUtils.getPrecision(999_000_000)).isEqualTo(ChronoUnit.MILLIS);
    assertThat(DateUtils.getPrecision(999_999_000)).isEqualTo(ChronoUnit.MICROS);
    assertThat(DateUtils.getPrecision(0)).isEqualTo(ChronoUnit.SECONDS);
  }
}
