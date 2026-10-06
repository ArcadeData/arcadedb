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
package com.arcadedb.query.opencypher.temporal;

import com.arcadedb.exception.ArithmeticErrorException;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.arcadedb.query.opencypher.temporal.CypherDate.toDouble;

/**
 * OpenCypher Duration value. Combines calendar (months, days) and clock (seconds, nanoseconds) components.
 * Cypher durations are distinct from Java's Period and Duration — they track all four components.
 *
 * Components: months (includes years), days (includes weeks), seconds, nanosAdjustment.
 * Fractional values cascade: 1.5 years = 1 year + 6 months.
 */
public class CypherDuration implements CypherTemporalValue {
  private static final Pattern ISO_PATTERN = Pattern.compile(
      "P(?:([-\\d.]+)Y)?(?:([-\\d.]+)M)?(?:([-\\d.]+)W)?(?:([-\\d.]+)D)?(?:T(?:([-\\d.]+)H)?(?:([-\\d.]+)M)?(?:([-\\d.]+)S)?)?");
  // Alternative date-based format: P<years>-<months>-<days>T<hours>:<minutes>:<seconds>[.fraction]
  private static final Pattern DATE_BASED_PATTERN = Pattern.compile(
      "P(\\d+)-(\\d+)-(\\d+)T(\\d+):(\\d+):(\\d+(?:\\.\\d+)?)");

  private final long months;
  private final long days;
  private final long seconds;
  private final int nanosAdjustment; // 0..999_999_999

  /**
   * The nanosecond argument is a {@code long} and may hold any number of whole seconds, which are folded into
   * {@code seconds}: the {@code milliseconds}/{@code microseconds}/{@code nanoseconds} map fields are unbounded, so
   * {@code duration({nanoseconds: 3000000000})} is three seconds. It was an {@code int}, and every caller that carried
   * more than 2^31 ns wrapped silently, often to a negative duration (issue #8385).
   */
  public CypherDuration(final long months, final long days, final long seconds, final long nanosAdjustment) {
    this.months = months;
    this.days = days;
    try {
      this.seconds = Math.addExact(seconds, Math.floorDiv(nanosAdjustment, 1_000_000_000L));
    } catch (final ArithmeticException e) {
      // Reachable with client-supplied values (a Bolt duration struct): an overflow is the caller's mistake and must not
      // reach the wire layers as an unrecognised throwable (same as divide(), issue #5602)
      throw new ArithmeticErrorException("Duration overflow: " + seconds + " seconds + " + nanosAdjustment + " nanoseconds");
    }
    this.nanosAdjustment = (int) Math.floorMod(nanosAdjustment, 1_000_000_000L);
  }

  public static CypherDuration parse(final String str) {
    // Try alternative date-based format first: P<years>-<months>-<days>T<hours>:<minutes>:<seconds>
    final Matcher dm = DATE_BASED_PATTERN.matcher(str);
    if (dm.matches()) {
      final double years = Double.parseDouble(dm.group(1));
      final double months = Double.parseDouble(dm.group(2));
      final double daysVal = Double.parseDouble(dm.group(3));
      final double hours = Double.parseDouble(dm.group(4));
      final double minutes = Double.parseDouble(dm.group(5));
      final double secs = Double.parseDouble(dm.group(6));
      return fromComponents(years, months, 0, daysVal, hours, minutes, secs, 0);
    }

    final Matcher m = ISO_PATTERN.matcher(str);
    if (!m.matches())
      throw new IllegalArgumentException("Invalid duration string: " + str);

    if (isIntegral(m.group(1)) && isIntegral(m.group(2)) && isIntegral(m.group(3)) && isIntegral(m.group(4))
        && isIntegral(m.group(5)) && isIntegral(m.group(6)))
      return parseExact(m);

    final double years = m.group(1) != null ? Double.parseDouble(m.group(1)) : 0;
    final double months = m.group(2) != null ? Double.parseDouble(m.group(2)) : 0;
    final double weeks = m.group(3) != null ? Double.parseDouble(m.group(3)) : 0;
    final double daysVal = m.group(4) != null ? Double.parseDouble(m.group(4)) : 0;
    final double hours = m.group(5) != null ? Double.parseDouble(m.group(5)) : 0;
    final double minutes = m.group(6) != null ? Double.parseDouble(m.group(6)) : 0;
    final double secs = m.group(7) != null ? Double.parseDouble(m.group(7)) : 0;

    return fromComponents(years, months, weeks, daysVal, hours, minutes, secs, 0);
  }

  private static boolean isIntegral(final String component) {
    return component == null || component.indexOf('.') < 0;
  }

  /**
   * Parses an ISO text whose whole-unit components carry no fraction (the shape {@link #toString()} always writes) in
   * exact {@code long} arithmetic, reading the seconds fraction as a decimal rather than through a {@code double}, which
   * cannot hold a nanosecond past about 97 days (issue #9338).
   */
  private static CypherDuration parseExact(final Matcher m) {
    try {
      final long months = Math.addExact(Math.multiplyExact(longOf(m.group(1)), 12L), longOf(m.group(2)));
      final long days = Math.addExact(Math.multiplyExact(longOf(m.group(3)), 7L), longOf(m.group(4)));
      long seconds = Math.addExact(Math.multiplyExact(longOf(m.group(5)), 3600L), Math.multiplyExact(longOf(m.group(6)), 60L));
      long nanos = 0;
      if (m.group(7) != null) {
        final BigDecimal secs = new BigDecimal(m.group(7));
        final BigInteger whole = secs.toBigInteger();
        seconds = Math.addExact(seconds, whole.longValueExact());
        // Digits beyond the nanosecond round half away from zero
        nanos = secs.subtract(new BigDecimal(whole)).movePointRight(9).setScale(0, RoundingMode.HALF_UP).longValueExact();
      }
      return new CypherDuration(months, days, seconds, nanos);
    } catch (final ArithmeticException e) {
      throw new ArithmeticErrorException("Duration overflow: " + m.group());
    }
  }

  private static long longOf(final String component) {
    return component == null ? 0L : Long.parseLong(component);
  }

  public static CypherDuration fromMap(final Map<String, Object> map) {
    try {
      return fromMapInternal(map);
    } catch (final ArithmeticException e) {
      throw new ArithmeticErrorException("Duration fields overflow: " + map);
    }
  }

  private static CypherDuration fromMapInternal(final Map<String, Object> map) {
    // Fast path for single-field durations with integral values (common in bulk operations)
    if (map.size() == 1) {
      final Map.Entry<String, Object> entry = map.entrySet().iterator().next();
      final String key = entry.getKey();
      final Object value = entry.getValue();

      // Only use fast path for integral values; fractional values need cascading via general path
      if (value instanceof Number && !hasFraction((Number) value)) {
        // Common single-field cases
        switch (key) {
          case "seconds":
            return new CypherDuration(0, 0, ((Number) value).longValue(), 0);
          case "minutes":
            return new CypherDuration(0, 0, Math.multiplyExact(((Number) value).longValue(), 60L), 0);
          case "hours":
            return new CypherDuration(0, 0, Math.multiplyExact(((Number) value).longValue(), 3600L), 0);
          case "days":
            return new CypherDuration(0, ((Number) value).longValue(), 0, 0);
          case "weeks":
            return new CypherDuration(0, Math.multiplyExact(((Number) value).longValue(), 7L), 0, 0);
          case "months":
            return new CypherDuration(((Number) value).longValue(), 0, 0, 0);
          case "years":
            return new CypherDuration(Math.multiplyExact(((Number) value).longValue(), 12L), 0, 0, 0);
          case "milliseconds":
            return new CypherDuration(0, 0, ((Number) value).longValue() / 1000, ((Number) value).longValue() % 1000 * 1_000_000);
          case "microseconds":
            return new CypherDuration(0, 0, ((Number) value).longValue() / 1_000_000, ((Number) value).longValue() % 1_000_000 * 1_000);
          case "nanoseconds":
            return new CypherDuration(0, 0, 0, ((Number) value).longValue());
        }
      }
    }

    // General case: multiple fields or unrecognized field. Integral fields stay exact in long arithmetic (a double holds
    // whole numbers exactly only to 2^53, so the same value was exact or not depending on how many other keys the map had)
    if (allIntegral(map))
      return fromIntegralMap(map);

    // Fractional fields cascade into the smaller units through floating point
    final double years = map.containsKey("years") ? toDouble(map.get("years")) : 0;
    final double quarters = map.containsKey("quarters") ? toDouble(map.get("quarters")) : 0;
    final double months = map.containsKey("months") ? toDouble(map.get("months")) : 0;
    final double weeks = map.containsKey("weeks") ? toDouble(map.get("weeks")) : 0;
    final double days = map.containsKey("days") ? toDouble(map.get("days")) : 0;
    final double hours = map.containsKey("hours") ? toDouble(map.get("hours")) : 0;
    final double minutes = map.containsKey("minutes") ? toDouble(map.get("minutes")) : 0;
    final double seconds = map.containsKey("seconds") ? toDouble(map.get("seconds")) : 0;
    // Sub-second fields are summed in exact long arithmetic: a double sum loses nanoseconds past 2^53
    final long totalNanos;
    try {
      totalNanos = Math.addExact(Math.addExact(toNanos(map.get("milliseconds"), 1_000_000L),
          toNanos(map.get("microseconds"), 1_000L)), toNanos(map.get("nanoseconds"), 1L));
    } catch (final ArithmeticException e) {
      throw new ArithmeticErrorException("Duration sub-second fields overflow: " + map);
    }

    final double totalMonths = years * 12 + quarters * 3 + months;

    return fromComponents(0, totalMonths, weeks, days, hours, minutes, seconds, totalNanos);
  }

  private static boolean allIntegral(final Map<String, Object> map) {
    for (final String key : WHOLE_UNIT_KEYS) {
      final Object value = map.get(key);
      if (value != null && !(value instanceof Number number && !hasFraction(number)))
        return false;
    }
    return true;
  }

  private static final String[] WHOLE_UNIT_KEYS = { "years", "quarters", "months", "weeks", "days", "hours", "minutes", "seconds" };

  private static CypherDuration fromIntegralMap(final Map<String, Object> map) {
    try {
      final long totalMonths = Math.addExact(Math.addExact(Math.multiplyExact(wholeOf(map, "years"), 12L),
          Math.multiplyExact(wholeOf(map, "quarters"), 3L)), wholeOf(map, "months"));
      final long totalDays = Math.addExact(Math.multiplyExact(wholeOf(map, "weeks"), 7L), wholeOf(map, "days"));
      final long totalSeconds = Math.addExact(Math.addExact(Math.multiplyExact(wholeOf(map, "hours"), 3600L),
          Math.multiplyExact(wholeOf(map, "minutes"), 60L)), wholeOf(map, "seconds"));
      final long totalNanos = Math.addExact(Math.addExact(toNanos(map.get("milliseconds"), 1_000_000L),
          toNanos(map.get("microseconds"), 1_000L)), toNanos(map.get("nanoseconds"), 1L));
      return new CypherDuration(totalMonths, totalDays, totalSeconds, totalNanos);
    } catch (final ArithmeticException e) {
      throw new ArithmeticErrorException("Duration fields overflow: " + map);
    }
  }

  private static long wholeOf(final Map<String, Object> map, final String key) {
    final Object value = map.get(key);
    return value == null ? 0L : ((Number) value).longValue();
  }

  private static CypherDuration fromComponents(final double years, final double months, final double weeks,
      final double days, final double hours, final double minutes, final double secs, final long extraNanos) {
    // Fractional cascading: fractional years → months, fractional months → days, etc.
    double totalMonths = years * 12 + months;
    final long wholeMonths = (long) totalMonths;
    final double fracMonths = totalMonths - wholeMonths;

    double totalDays = weeks * 7 + days + fracMonths * (365.2425 / 12); // fractional months → average days per month
    final long wholeDays = (long) totalDays;
    final double fracDays = totalDays - wholeDays;

    double totalSeconds = hours * 3600 + minutes * 60 + secs + fracDays * 86400;
    final long wholeSeconds = (long) totalSeconds;
    final double fracSeconds = totalSeconds - wholeSeconds;

    final long nanos = Math.round(fracSeconds * 1_000_000_000) + extraNanos;

    return new CypherDuration(wholeMonths, wholeDays, wholeSeconds, nanos);
  }

  /**
   * Converts a sub-second map field to nanoseconds. Integral values stay exact in long arithmetic; a fractional value
   * ({@code milliseconds: 1.5}) is rounded to the nearest nanosecond.
   */
  private static long toNanos(final Object value, final long nanosPerUnit) {
    if (value == null)
      return 0L;
    if (value instanceof Number number && !hasFraction(number))
      return Math.multiplyExact(number.longValue(), nanosPerUnit);
    return Math.round(toDouble(value) * nanosPerUnit);
  }

  private static boolean hasFraction(final Number value) {
    final double d = value.doubleValue();
    return d != Math.floor(d);
  }

  public long getMonths() {
    return months;
  }

  public long getDays() {
    return days;
  }

  public long getSeconds() {
    return seconds;
  }

  public int getNanosAdjustment() {
    return nanosAdjustment;
  }

  public CypherDuration add(final CypherDuration other) {
    try {
      return new CypherDuration(Math.addExact(months, other.months), Math.addExact(days, other.days),
          Math.addExact(seconds, other.seconds), (long) nanosAdjustment + other.nanosAdjustment);
    } catch (final ArithmeticException e) {
      throw overflow("add", other);
    }
  }

  public CypherDuration subtract(final CypherDuration other) {
    try {
      return new CypherDuration(Math.subtractExact(months, other.months), Math.subtractExact(days, other.days),
          Math.subtractExact(seconds, other.seconds), (long) nanosAdjustment - other.nanosAdjustment);
    } catch (final ArithmeticException e) {
      throw overflow("subtract", other);
    }
  }

  private ArithmeticErrorException overflow(final String operation, final Object operand) {
    return new ArithmeticErrorException("Duration overflow: cannot " + operation + " " + operand + " and " + this);
  }

  public CypherDuration multiply(final double factor) {
    if (factor == Math.rint(factor) && Math.abs(factor) < 0x1p62) {
      // Integral factor: exact in long arithmetic, so "* 1" is the identity whatever the magnitude
      final long f = (long) factor;
      try {
        return new CypherDuration(Math.multiplyExact(months, f), Math.multiplyExact(days, f), Math.multiplyExact(seconds, f),
            Math.multiplyExact((long) nanosAdjustment, f));
      } catch (final ArithmeticException e) {
        throw overflow("multiply by " + factor, this);
      }
    }
    return scale(decimalOf(factor), BigDecimal.ONE);
  }

  public CypherDuration divide(final double divisor) {
    if (divisor == 0)
      // A raw java.lang.ArithmeticException reached the wire layers as an unrecognised throwable and became a 500;
      // dividing a duration by zero is the caller's mistake, same as 1/0 (issue #5602).
      throw new ArithmeticErrorException("Cannot divide duration by zero");
    if (divisor == 1)
      return this;
    return scale(BigDecimal.ONE, decimalOf(divisor));
  }

  private static BigDecimal decimalOf(final double value) {
    if (Double.isNaN(value) || Double.isInfinite(value))
      throw new ArithmeticErrorException("Cannot scale a duration by " + value);
    return BigDecimal.valueOf(value);
  }

  /**
   * Scales every component by {@code numerator / denominator} in exact decimal arithmetic. Fractional months carry to
   * days (1 month = 365.2425/12 days) and fractional days carry to seconds, as Neo4j does. The previous implementation
   * did it all in {@code double}, which saturated at {@code Long.MAX_VALUE} past the long range and lost nanoseconds
   * past 2^53 (issue #9338); an unrepresentable result now raises an arithmetic error.
   */
  private CypherDuration scale(final BigDecimal numerator, final BigDecimal denominator) {
    try {
      final BigDecimal newMonths = scaled(months, numerator, denominator);
      final BigInteger wholeMonths = newMonths.toBigInteger();
      final double monthRemainder = newMonths.subtract(new BigDecimal(wholeMonths)).doubleValue();

      final BigDecimal newDays = scaled(days, numerator, denominator).add(BigDecimal.valueOf(monthRemainder * (365.2425 / 12)));
      final BigInteger wholeDays = newDays.toBigInteger();
      final BigDecimal dayRemainder = newDays.subtract(new BigDecimal(wholeDays));

      final BigDecimal clockNanos = scaled(BigInteger.valueOf(seconds).multiply(BigInteger.valueOf(1_000_000_000L))
          .add(BigInteger.valueOf(nanosAdjustment)), numerator, denominator)
          .add(dayRemainder.multiply(BigDecimal.valueOf(86400_000_000_000L)));
      final BigInteger[] secondsAndNanos = clockNanos.toBigInteger().divideAndRemainder(BigInteger.valueOf(1_000_000_000L));

      return new CypherDuration(wholeMonths.longValueExact(), wholeDays.longValueExact(), secondsAndNanos[0].longValueExact(),
          secondsAndNanos[1].longValueExact());
    } catch (final ArithmeticException e) {
      throw overflow("scale by " + numerator + "/" + denominator, this);
    }
  }

  private static BigDecimal scaled(final long value, final BigDecimal numerator, final BigDecimal denominator) {
    return scaled(BigInteger.valueOf(value), numerator, denominator);
  }

  private static BigDecimal scaled(final BigInteger value, final BigDecimal numerator, final BigDecimal denominator) {
    final BigDecimal product = new BigDecimal(value).multiply(numerator);
    return BigDecimal.ONE.equals(denominator) ? product : product.divide(denominator, 40, RoundingMode.DOWN);
  }

  @Override
  public Object getTemporalProperty(final String name) {
    return switch (name) {
      case "years" -> months / 12;
      case "quarters" -> months / 3;
      case "months" -> months;
      case "weeks" -> days / 7;
      case "days" -> days;
      case "hours" -> seconds / 3600;
      case "minutes" -> seconds / 60;
      case "seconds" -> seconds;
      case "milliseconds" -> subSecondTotal(1_000L, nanosAdjustment / 1_000_000, name);
      case "microseconds" -> subSecondTotal(1_000_000L, nanosAdjustment / 1_000, name);
      case "nanoseconds" -> subSecondTotal(1_000_000_000L, nanosAdjustment, name);
      // "of" variants — remainder after extracting larger units
      case "monthsOfYear" -> months % 12;
      case "monthsOfQuarter" -> months % 3;
      case "quartersOfYear" -> (months / 3) % 4;
      case "daysOfWeek" -> days % 7;
      case "minutesOfHour" -> (seconds / 60) % 60;
      case "secondsOfMinute" -> seconds % 60;
      case "millisecondsOfSecond" -> nanosAdjustment / 1_000_000;
      case "microsecondsOfSecond" -> nanosAdjustment / 1_000;
      case "nanosecondsOfSecond" -> (long) nanosAdjustment;
      default -> null;
    };
  }

  private long subSecondTotal(final long unitsPerSecond, final long fraction, final String name) {
    try {
      return Math.addExact(Math.multiplyExact(seconds, unitsPerSecond), fraction);
    } catch (final ArithmeticException e) {
      throw new ArithmeticErrorException("Duration " + this + " does not fit the '" + name + "' accessor");
    }
  }

  @Override
  public int compareTo(final CypherTemporalValue other) {
    if (other instanceof CypherDuration d) {
      // Per Cypher spec, durations compare component-by-component (months, days, seconds, nanos).
      // They do NOT normalize across components (e.g. 24h != 1 day).
      int cmp = Long.compare(months, d.months);
      if (cmp != 0) return cmp;
      cmp = Long.compare(days, d.days);
      if (cmp != 0) return cmp;
      cmp = Long.compare(seconds, d.seconds);
      if (cmp != 0) return cmp;
      return Integer.compare(nanosAdjustment, d.nanosAdjustment);
    }
    throw new IllegalArgumentException("Cannot compare Duration with " + other.getClass().getSimpleName());
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("P");
    final long years = months / 12;
    final long remMonths = months % 12;

    if (years != 0)
      sb.append(years).append('Y');
    if (remMonths != 0)
      sb.append(remMonths).append('M');
    if (days != 0)
      sb.append(days).append('D');

    if (seconds != 0 || nanosAdjustment != 0) {
      sb.append('T');
      // When seconds < 0 and nanosAdjustment > 0, the effective time is (seconds + nanos/1e9),
      // which is less negative than seconds alone. We need to use the effective value for h/m extraction.
      long effectiveSecs = seconds;
      int effectiveNanos = nanosAdjustment;
      if (seconds < 0 && nanosAdjustment > 0) {
        effectiveSecs = seconds + 1;
        effectiveNanos = 1_000_000_000 - nanosAdjustment;
      }
      final long h = effectiveSecs / 3600;
      final long m = (effectiveSecs % 3600) / 60;
      final long s = effectiveSecs % 60;
      if (h != 0)
        sb.append(h).append('H');
      if (m != 0)
        sb.append(m).append('M');
      if (s != 0 || effectiveNanos != 0)
        appendSecondsWithFraction(sb, s, effectiveNanos, seconds < 0 && nanosAdjustment > 0);
    }

    // Empty duration
    if (sb.length() == 1)
      sb.append("T0S");

    return sb.toString();
  }

  /**
   * Append seconds with nanosecond fraction.
   * When negativeAdjusted is true, s and nanos represent the already-adjusted values
   * (s is negative, nanos is the positive fractional part of the negative number).
   */
  private static void appendSecondsWithFraction(final StringBuilder sb, final long s, final int nanos,
      final boolean negativeAdjusted) {
    if (nanos == 0) {
      sb.append(s).append('S');
      return;
    }
    if (s == 0 && negativeAdjusted)
      sb.append("-0.").append(formatNanos(nanos)).append('S');
    else
      sb.append(s).append('.').append(formatNanos(nanos)).append('S');
  }

  /**
   * Nine-digit, zero-padded fraction with the trailing zeros dropped. This string is the storage encoding
   * ({@link TemporalUtil#toCoreJavaType(Object)} writes {@code toString()}, {@link #parse(String)} reads it back), so the
   * digits must be ASCII whatever the JVM default locale: {@code String.format("%09d")} localized them, and under
   * {@code ar-SA} or {@code fa-IR} the stored value no longer parsed as a duration (issue #8387).
   */
  private static String formatNanos(final int nanos) {
    int value = Math.abs(nanos);
    int digits = 9;
    while (value % 10 == 0 && digits > 1) {
      value /= 10;
      digits--;
    }
    final char[] buffer = new char[digits];
    for (int i = digits - 1; i >= 0; i--) {
      buffer[i] = (char) ('0' + value % 10);
      value /= 10;
    }
    return new String(buffer);
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) return true;
    if (!(obj instanceof CypherDuration other)) return false;
    return months == other.months && days == other.days && seconds == other.seconds && nanosAdjustment == other.nanosAdjustment;
  }

  @Override
  public int hashCode() {
    return Objects.hash(months, days, seconds, nanosAdjustment);
  }
}
