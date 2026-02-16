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

  public CypherDuration(final long months, final long days, final long seconds, final int nanosAdjustment) {
    // Normalize nanoseconds into seconds
    long totalNanos = nanosAdjustment;
    long extraSeconds = totalNanos / 1_000_000_000L;
    int remainNanos = (int) (totalNanos % 1_000_000_000L);
    if (remainNanos < 0) {
      extraSeconds--;
      remainNanos += 1_000_000_000;
    }
    this.months = months;
    this.days = days;
    this.seconds = seconds + extraSeconds;
    this.nanosAdjustment = remainNanos;
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

    double years = m.group(1) != null ? Double.parseDouble(m.group(1)) : 0;
    double months = m.group(2) != null ? Double.parseDouble(m.group(2)) : 0;
    double weeks = m.group(3) != null ? Double.parseDouble(m.group(3)) : 0;
    double daysVal = m.group(4) != null ? Double.parseDouble(m.group(4)) : 0;
    double hours = m.group(5) != null ? Double.parseDouble(m.group(5)) : 0;
    double minutes = m.group(6) != null ? Double.parseDouble(m.group(6)) : 0;
    double secs = m.group(7) != null ? Double.parseDouble(m.group(7)) : 0;

    return fromComponents(years, months, weeks, daysVal, hours, minutes, secs, 0);
  }

  public static CypherDuration fromMap(final Map<String, Object> map) {
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
            return new CypherDuration(0, 0, ((Number) value).longValue() * 60, 0);
          case "hours":
            return new CypherDuration(0, 0, ((Number) value).longValue() * 3600, 0);
          case "days":
            return new CypherDuration(0, ((Number) value).longValue(), 0, 0);
          case "weeks":
            return new CypherDuration(0, ((Number) value).longValue() * 7, 0, 0);
          case "months":
            return new CypherDuration(((Number) value).longValue(), 0, 0, 0);
          case "years":
            return new CypherDuration(((Number) value).longValue() * 12, 0, 0, 0);
          case "milliseconds":
            return new CypherDuration(0, 0, ((Number) value).longValue() / 1000, (int) (((Number) value).longValue() % 1000 * 1_000_000));
          case "nanoseconds":
            return new CypherDuration(0, 0, 0, ((Number) value).intValue());
        }
      }
    }

    // General case: multiple fields or unrecognized field
    final double years = map.containsKey("years") ? toDouble(map.get("years")) : 0;
    final double quarters = map.containsKey("quarters") ? toDouble(map.get("quarters")) : 0;
    final double months = map.containsKey("months") ? toDouble(map.get("months")) : 0;
    final double weeks = map.containsKey("weeks") ? toDouble(map.get("weeks")) : 0;
    final double days = map.containsKey("days") ? toDouble(map.get("days")) : 0;
    final double hours = map.containsKey("hours") ? toDouble(map.get("hours")) : 0;
    final double minutes = map.containsKey("minutes") ? toDouble(map.get("minutes")) : 0;
    final double seconds = map.containsKey("seconds") ? toDouble(map.get("seconds")) : 0;
    final double milliseconds = map.containsKey("milliseconds") ? toDouble(map.get("milliseconds")) : 0;
    final double microseconds = map.containsKey("microseconds") ? toDouble(map.get("microseconds")) : 0;
    final long nanoseconds = map.containsKey("nanoseconds") ? ((Number) map.get("nanoseconds")).longValue() : 0;

    final double totalMonths = years * 12 + quarters * 3 + months;
    final double totalNanos = milliseconds * 1_000_000 + microseconds * 1_000 + nanoseconds;

    return fromComponents(0, totalMonths, weeks, days, hours, minutes, seconds, (long) totalNanos);
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

    return new CypherDuration(wholeMonths, wholeDays, wholeSeconds, (int) nanos);
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
    return new CypherDuration(months + other.months, days + other.days, seconds + other.seconds,
        nanosAdjustment + other.nanosAdjustment);
  }

  public CypherDuration subtract(final CypherDuration other) {
    return new CypherDuration(months - other.months, days - other.days, seconds - other.seconds,
        nanosAdjustment - other.nanosAdjustment);
  }

  public CypherDuration multiply(final double factor) {
    // Cascading remainder approach: fractional months carry to days, fractional days carry to seconds.
    // This matches Neo4j behavior where 1 month = 365.2425/12 days and 1 day = 86400 seconds.
    final double newMonths = months * factor;
    final long wholeMonths = (long) newMonths;
    final double monthRemainder = newMonths - wholeMonths;

    final double newDays = days * factor + monthRemainder * (365.2425 / 12);
    final long wholeDays = (long) newDays;
    final double dayRemainder = newDays - wholeDays;

    final double totalNanos = (seconds * 1_000_000_000.0 + nanosAdjustment) * factor + dayRemainder * 86400_000_000_000.0;
    final long wholeNanos = (long) totalNanos; // Truncate sub-nanosecond remainder
    final long newSeconds = wholeNanos / 1_000_000_000L;
    final int newNanosAdj = (int) (wholeNanos % 1_000_000_000L);

    return new CypherDuration(wholeMonths, wholeDays, newSeconds, newNanosAdj);
  }

  public CypherDuration divide(final double divisor) {
    if (divisor == 0)
      throw new ArithmeticException("Cannot divide duration by zero");
    return multiply(1.0 / divisor);
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
      case "milliseconds" -> seconds * 1000 + nanosAdjustment / 1_000_000;
      case "microseconds" -> seconds * 1_000_000 + nanosAdjustment / 1_000;
      case "nanoseconds" -> seconds * 1_000_000_000L + nanosAdjustment;
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

  private static String formatNanos(final int nanos) {
    final String nanoStr = String.format("%09d", Math.abs(nanos));
    return nanoStr.replaceAll("0+$", "");
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
