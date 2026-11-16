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

import com.arcadedb.exception.ArithmeticErrorException;
import com.arcadedb.schema.Type;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.math.RoundingMode;
import java.sql.Timestamp;
import java.time.DateTimeException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.Date;
import java.util.function.Supplier;

/**
 * SQL arithmetic and ordering of a {@link Duration} - what subtracting two datetimes answers - and of a datetime shifted by an amount
 * of time (issue #12295).
 * <ul>
 *   <li>A number next to a duration is a count of SECONDS, the unit a duration is shown in (HTTP writes {@code sysdate() - ts} as its
 *   seconds): {@code diff < 3600}, {@code diff + 1}, {@code diff - 0.5} read the number so, and the answer of {@code +} and {@code -}
 *   is a duration again;</li>
 *   <li>a duration scales by a number ({@code diff * 2}, {@code diff / 3600}, {@code diff % 7}) into a duration, as an openCypher
 *   duration does, and {@code diff / duration(1, 'hour')} is the plain number of hours;</li>
 *   <li>a datetime plus or minus a duration is the datetime of the same class that much later or earlier;</li>
 *   <li>a datetime plus or minus a number moves it by that many MILLISECONDS, the unit a number has against a datetime everywhere else
 *   in SQL (a comparison, a number written into a DATETIME property, JSON).</li>
 * </ul>
 * Every operation is exact to the nanosecond and refuses an answer it cannot hold with an {@link ArithmeticErrorException}, which is the
 * caller's error (HTTP 400).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class DurationArithmetic {
  /** What the arithmetic methods answer for a pair of operands that is not theirs: the caller applies its own rule. */
  public static final Object NOT_APPLICABLE = new Object();

  private static final BigDecimal NANOS_PER_SECOND = BigDecimal.valueOf(1_000_000_000L);
  private static final BigDecimal MAX_NANOS        = toNanos(Duration.ofSeconds(Long.MAX_VALUE, 999_999_999));
  private static final BigDecimal MIN_NANOS        = toNanos(Duration.ofSeconds(Long.MIN_VALUE, 0));

  private DurationArithmetic() {
  }

  /** Whether the pair is one {@link #plus}, {@link #minus}, ... may answer: a duration or a datetime on either side. */
  public static boolean involves(final Object left, final Object right) {
    return left instanceof Duration || right instanceof Duration || DateUtils.isDate(left) || DateUtils.isDate(right);
  }

  public static Object plus(final Object left, final Object right) {
    if (left instanceof Duration duration) {
      if (right instanceof Duration other)
        return exact(() -> duration.plus(other));
      if (right instanceof Number number)
        return exact(() -> duration.plus(ofSeconds(number)));
      if (DateUtils.isDate(right))
        return shift(right, duration);
      return NOT_APPLICABLE;
    }
    if (right instanceof Duration duration) {
      if (left instanceof Number number)
        return exact(() -> ofSeconds(number).plus(duration));
      if (DateUtils.isDate(left))
        return shift(left, duration);
      return NOT_APPLICABLE;
    }
    if (DateUtils.isDate(left)) {
      if (right instanceof Number number)
        return shift(left, ofMillis(number));
      if (DateUtils.isDate(right))
        throw new ArithmeticErrorException(
            "Cannot add two datetimes ('" + left + "' + '" + right + "'): add a duration to a datetime, or subtract two datetimes");
      return NOT_APPLICABLE;
    }
    if (DateUtils.isDate(right) && left instanceof Number number)
      return shift(right, ofMillis(number));
    return NOT_APPLICABLE;
  }

  public static Object minus(final Object left, final Object right) {
    if (DateUtils.isDate(left) && DateUtils.isDate(right))
      // two date/time values: their exact difference, which no epoch count at nanosecond precision holds past a 292-year gap (#10206)
      return DateUtils.difference(left, right);
    if (left instanceof Duration duration) {
      if (right instanceof Duration other)
        return exact(() -> duration.minus(other));
      if (right instanceof Number number)
        return exact(() -> duration.minus(ofSeconds(number)));
      if (DateUtils.isDate(right))
        throw new ArithmeticErrorException("Cannot subtract a datetime from a duration ('" + left + "' - '" + right + "')");
      return NOT_APPLICABLE;
    }
    if (right instanceof Duration duration) {
      if (left instanceof Number number)
        return exact(() -> ofSeconds(number).minus(duration));
      if (DateUtils.isDate(left))
        return shift(left, exact(duration::negated));
      return NOT_APPLICABLE;
    }
    if (DateUtils.isDate(left) && right instanceof Number number)
      return shift(left, exact(() -> ofMillis(number).negated()));
    if (DateUtils.isDate(right) && left instanceof Number)
      throw new ArithmeticErrorException("Cannot subtract a datetime from a number ('" + left + "' - '" + right + "')");
    return NOT_APPLICABLE;
  }

  public static Object multiply(final Object left, final Object right) {
    if (left instanceof Duration duration && right instanceof Number number)
      return scale(duration, toDecimal(number, "multiply a duration by"), false);
    if (right instanceof Duration duration && left instanceof Number number)
      return scale(duration, toDecimal(number, "multiply a duration by"), false);
    if (left instanceof Duration && right instanceof Duration)
      throw new ArithmeticErrorException("Cannot multiply two durations ('" + left + "' * '" + right + "')");
    return NOT_APPLICABLE;
  }

  public static Object divide(final Object left, final Object right) {
    if (left instanceof Duration duration) {
      if (right instanceof Number number) {
        final BigDecimal divisor = toDecimal(number, "divide a duration by");
        if (divisor.signum() == 0)
          throw new ArithmeticErrorException("/ by zero");
        return scale(duration, divisor, true);
      }
      if (right instanceof Duration other) {
        // how many times one duration goes into the other: a plain number, whole when the division is exact
        if (other.isZero())
          throw new ArithmeticErrorException("/ by zero");
        final BigDecimal dividend = toNanos(duration);
        final BigDecimal divisor = toNanos(other);
        final BigDecimal[] division = dividend.divideAndRemainder(divisor);
        if (division[1].signum() == 0 && division[0].toBigInteger().bitLength() < 64)
          return division[0].longValueExact();
        return dividend.divide(divisor, MathContext.DECIMAL64).doubleValue();
      }
    }
    return NOT_APPLICABLE;
  }

  public static Object remainder(final Object left, final Object right) {
    if (left instanceof Duration duration) {
      final BigDecimal divisor;
      if (right instanceof Duration other)
        divisor = toNanos(other);
      else if (right instanceof Number number)
        divisor = toDecimal(number, "take the remainder of a duration by").multiply(NANOS_PER_SECOND);
      else
        return NOT_APPLICABLE;
      if (divisor.signum() == 0)
        throw new ArithmeticErrorException("% by zero");
      return ofNanos(toNanos(duration).remainder(divisor));
    }
    return NOT_APPLICABLE;
  }

  /**
   * The sign of {@code duration - seconds}, exactly, with the number read as a count of seconds. A NaN is above every duration, as it
   * is above every number everywhere else in the engine.
   */
  public static int compareToSeconds(final Duration duration, final Number seconds) {
    if (seconds instanceof Long || seconds instanceof Integer || seconds instanceof Short || seconds instanceof Byte) {
      final int order = Long.compare(duration.getSeconds(), seconds.longValue());
      return order != 0 ? order : duration.getNano() > 0 ? 1 : 0;
    }
    if (seconds instanceof Double || seconds instanceof Float) {
      final double value = seconds instanceof Float f ? Type.widenFloat(f) : seconds.doubleValue();
      if (Double.isNaN(value))
        return -1;
      if (Double.isInfinite(value))
        return value > 0 ? -1 : 1;
      return toSeconds(duration).compareTo(BigDecimal.valueOf(value));
    }
    if (seconds instanceof BigInteger bigInteger)
      return toSeconds(duration).compareTo(new BigDecimal(bigInteger));
    if (seconds instanceof BigDecimal bigDecimal)
      return toSeconds(duration).compareTo(bigDecimal);
    return toSeconds(duration).compareTo(BigDecimal.valueOf(seconds.doubleValue()));
  }

  /** The exact number of seconds of a duration, at the scale of a nanosecond: what HTTP shows it as. */
  public static BigDecimal toSeconds(final Duration duration) {
    return BigDecimal.valueOf(duration.getSeconds()).add(BigDecimal.valueOf(duration.getNano(), 9));
  }

  /** The duration of a number of seconds, to the nearest nanosecond. */
  public static Duration ofSeconds(final Number seconds) {
    return ofNanos(toDecimal(seconds, "read as seconds").multiply(NANOS_PER_SECOND));
  }

  /** The duration of a number of milliseconds, to the nearest nanosecond. */
  public static Duration ofMillis(final Number millis) {
    if (millis instanceof Long || millis instanceof Integer || millis instanceof Short || millis instanceof Byte)
      return Duration.ofMillis(millis.longValue());
    return ofNanos(toDecimal(millis, "read as milliseconds").movePointRight(6));
  }

  /**
   * The datetime {@code amount} after {@code dateTime}, of the same class. A DATE moved by whole days stays a DATE, and is otherwise
   * that day at midnight moved by the amount, so no part of the amount is dropped.
   */
  public static Object shift(final Object dateTime, final Duration amount) {
    try {
      return switch (dateTime) {
        case LocalDateTime localDateTime -> localDateTime.plus(amount);
        case ZonedDateTime zonedDateTime -> zonedDateTime.plus(amount);
        case OffsetDateTime offsetDateTime -> offsetDateTime.plus(amount);
        case Instant instant -> instant.plus(amount);
        case LocalDate localDate -> amount.getNano() == 0 && amount.getSeconds() % 86_400 == 0 ?
            localDate.plusDays(amount.getSeconds() / 86_400) :
            localDate.atStartOfDay().plus(amount);
        case Timestamp timestamp -> Timestamp.from(timestamp.toInstant().plus(amount));
        case Date date -> Date.from(date.toInstant().plus(amount));
        case Calendar calendar -> {
          final Calendar result = (Calendar) calendar.clone();
          result.setTimeInMillis(calendar.toInstant().plus(amount).toEpochMilli());
          yield result;
        }
        default -> throw new IllegalArgumentException("Not a datetime: " + dateTime);
      };
    } catch (final DateTimeException | ArithmeticException e) {
      throw new ArithmeticErrorException("Cannot move the datetime '" + dateTime + "' by " + amount + ": the result is out of range", e);
    }
  }

  private static Duration scale(final Duration duration, final BigDecimal factor, final boolean divide) {
    final BigDecimal nanos = toNanos(duration);
    final BigDecimal scaled = divide ? nanos.divide(factor, 0, RoundingMode.HALF_EVEN) : nanos.multiply(factor);
    return ofNanos(scaled);
  }

  private static BigDecimal toNanos(final Duration duration) {
    return BigDecimal.valueOf(duration.getSeconds()).multiply(NANOS_PER_SECOND).add(BigDecimal.valueOf(duration.getNano()));
  }

  private static Duration ofNanos(final BigDecimal nanos) {
    final BigDecimal whole = nanos.setScale(0, RoundingMode.HALF_EVEN);
    if (whole.compareTo(MAX_NANOS) > 0 || whole.compareTo(MIN_NANOS) < 0)
      throw new ArithmeticErrorException("The duration of " + nanos.movePointLeft(9).stripTrailingZeros().toPlainString()
          + " seconds is out of the range of a duration");
    final BigInteger[] secondsAndNanos = whole.toBigIntegerExact().divideAndRemainder(BigInteger.valueOf(1_000_000_000L));
    return Duration.ofSeconds(secondsAndNanos[0].longValueExact(), secondsAndNanos[1].longValueExact());
  }

  private static BigDecimal toDecimal(final Number number, final String what) {
    return switch (number) {
      case BigDecimal bigDecimal -> bigDecimal;
      case BigInteger bigInteger -> new BigDecimal(bigInteger);
      case Long l -> BigDecimal.valueOf(l);
      case Integer i -> BigDecimal.valueOf(i);
      case Short s -> BigDecimal.valueOf(s);
      case Byte b -> BigDecimal.valueOf(b);
      default -> {
        final double value = number instanceof Float f ? Type.widenFloat(f) : number.doubleValue();
        if (!Double.isFinite(value))
          throw new ArithmeticErrorException("Cannot " + what + " " + value);
        yield BigDecimal.valueOf(value);
      }
    };
  }

  /** A {@link Duration} operation that overflows the range of a duration is the caller's error, not an internal one. */
  private static Duration exact(final Supplier<Duration> operation) {
    try {
      return operation.get();
    } catch (final ArithmeticException e) {
      throw new ArithmeticErrorException("The duration is out of range: " + e.getMessage(), e);
    }
  }
}
