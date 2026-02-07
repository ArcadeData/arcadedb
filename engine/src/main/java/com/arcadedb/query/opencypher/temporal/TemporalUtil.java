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

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.time.temporal.IsoFields;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalAdjusters;
import java.time.temporal.WeekFields;

/**
 * Utility methods for temporal parsing and operations.
 */
public final class TemporalUtil {

  private static final java.util.regex.Pattern COMPACT_OFFSET = java.util.regex.Pattern.compile(
      "([+-])(\\d{2})(\\d{2})(?!:)");

  private TemporalUtil() {
  }

  /**
   * Normalize compact timezone offsets in a datetime string: +0200 → +02:00
   * Also handles the case where the offset appears before a timezone name in brackets.
   */
  public static String normalizeOffsetInString(final String str) {
    // Find timezone offset pattern: +HHMM or -HHMM (not followed by colon, not preceded by colon)
    final java.util.regex.Matcher m = COMPACT_OFFSET.matcher(str);
    if (m.find()) {
      final StringBuffer sb = new StringBuffer();
      m.appendReplacement(sb, "$1$2:$3");
      m.appendTail(sb);
      return sb.toString();
    }
    return str;
  }

  /**
   * Normalize a time string for OffsetTime.parse():
   * - Add :00 seconds if only HH:MM
   * - Normalize compact timezone offsets
   */
  public static String normalizeTimeString(final String str) {
    String result = str;
    // Check if this is HH:MM format without seconds (followed by offset or end)
    // Pattern: HH:MM followed by + or - or Z or end
    if (result.matches("^\\d{2}:\\d{2}[+\\-Z].*$"))
      result = result.substring(0, 5) + ":00" + result.substring(5);
    else if (result.matches("^\\d{2}:\\d{2}$"))
      result = result + ":00";

    return normalizeOffsetInString(result);
  }

  /**
   * Normalize a local time string:
   * - Add :00 seconds if only HH:MM
   */
  public static String normalizeLocalTimeString(final String str) {
    // If it's exactly HH:MM (5 chars, no offset), add seconds
    if (str.matches("^\\d{2}:\\d{2}$"))
      return str + ":00";
    return str;
  }

  /**
   * Parse a timezone offset string like "+01:00", "+0100", "Z".
   */
  public static ZoneOffset parseOffset(final String str) {
    if ("Z".equalsIgnoreCase(str))
      return ZoneOffset.UTC;
    return ZoneOffset.of(str);
  }

  /**
   * Parse a timezone string which may be a named zone ("Europe/Stockholm") or an offset ("+01:00").
   */
  public static ZoneId parseZone(final String str) {
    if ("Z".equalsIgnoreCase(str))
      return ZoneOffset.UTC;
    try {
      return ZoneId.of(str);
    } catch (final Exception e) {
      return ZoneOffset.of(str);
    }
  }

  /**
   * Truncate a date to the given unit.
   */
  public static LocalDate truncateDate(final LocalDate date, final String unit) {
    return switch (unit.toLowerCase()) {
      case "millennium" -> LocalDate.of((date.getYear() / 1000) * 1000, 1, 1);
      case "century" -> LocalDate.of((date.getYear() / 100) * 100, 1, 1);
      case "decade" -> LocalDate.of((date.getYear() / 10) * 10, 1, 1);
      case "year" -> LocalDate.of(date.getYear(), 1, 1);
      case "weekyear" -> {
        final int weekYear = date.get(WeekFields.ISO.weekBasedYear());
        LocalDate d = LocalDate.of(weekYear, 1, 4);
        yield d.with(WeekFields.ISO.weekOfWeekBasedYear(), 1).with(WeekFields.ISO.dayOfWeek(), 1);
      }
      case "quarter" -> {
        final int quarter = date.get(IsoFields.QUARTER_OF_YEAR);
        yield LocalDate.of(date.getYear(), (quarter - 1) * 3 + 1, 1);
      }
      case "month" -> LocalDate.of(date.getYear(), date.getMonthValue(), 1);
      case "week" -> date.with(WeekFields.ISO.dayOfWeek(), 1);
      case "day" -> date;
      default -> throw new IllegalArgumentException("Unknown truncation unit: " + unit);
    };
  }

  /**
   * Truncate a datetime to the given unit.
   */
  public static LocalDateTime truncateLocalDateTime(final LocalDateTime dateTime, final String unit) {
    // Time-level truncation: date stays the same, only time component changes
    return switch (unit.toLowerCase()) {
      case "hour" -> LocalDateTime.of(dateTime.toLocalDate(), LocalTime.of(dateTime.getHour(), 0));
      case "minute" -> LocalDateTime.of(dateTime.toLocalDate(), LocalTime.of(dateTime.getHour(), dateTime.getMinute()));
      case "second" -> LocalDateTime.of(dateTime.toLocalDate(), LocalTime.of(dateTime.getHour(), dateTime.getMinute(), dateTime.getSecond()));
      case "millisecond" -> {
        final int millis = dateTime.getNano() / 1_000_000;
        yield LocalDateTime.of(dateTime.toLocalDate(),
            LocalTime.of(dateTime.getHour(), dateTime.getMinute(), dateTime.getSecond(), millis * 1_000_000));
      }
      case "microsecond" -> {
        final int micros = dateTime.getNano() / 1_000;
        yield LocalDateTime.of(dateTime.toLocalDate(),
            LocalTime.of(dateTime.getHour(), dateTime.getMinute(), dateTime.getSecond(), micros * 1_000));
      }
      default -> {
        // Date-level truncation: truncate date and set time to midnight
        final LocalDate truncatedDate = truncateDate(dateTime.toLocalDate(), unit);
        yield LocalDateTime.of(truncatedDate, LocalTime.MIDNIGHT);
      }
    };
  }

  /**
   * Truncate a local time to the given unit.
   */
  public static LocalTime truncateLocalTime(final LocalTime time, final String unit) {
    return switch (unit.toLowerCase()) {
      case "day" -> LocalTime.MIDNIGHT;
      case "hour" -> LocalTime.of(time.getHour(), 0);
      case "minute" -> LocalTime.of(time.getHour(), time.getMinute());
      case "second" -> LocalTime.of(time.getHour(), time.getMinute(), time.getSecond());
      case "millisecond" -> {
        final int millis = time.getNano() / 1_000_000;
        yield LocalTime.of(time.getHour(), time.getMinute(), time.getSecond(), millis * 1_000_000);
      }
      case "microsecond" -> {
        final int micros = time.getNano() / 1_000;
        yield LocalTime.of(time.getHour(), time.getMinute(), time.getSecond(), micros * 1_000);
      }
      default -> throw new IllegalArgumentException("Unknown truncation unit for time: " + unit);
    };
  }

  /**
   * Compute a duration between two temporal values, returning only months.
   * Returns P<Y>Y<M>M format (no days/time components).
   */
  public static CypherDuration durationInMonths(final CypherTemporalValue from, final CypherTemporalValue to) {
    // Time-only types have no date component → return PT0S
    if (isTimeOnly(from) || isTimeOnly(to))
      return new CypherDuration(0, 0, 0, 0);
    final LocalDateTime fromDT = resolveDateTime(from, to);
    final LocalDateTime toDT = resolveDateTime(to, from);
    long totalMonths = fromDT.toLocalDate().until(toDT.toLocalDate()).toTotalMonths();
    // Adjust for time-of-day: if we overestimate months, the remainder would have wrong sign
    if (totalMonths != 0) {
      final LocalDateTime afterMonths = fromDT.plusMonths(totalMonths);
      final java.time.Duration remainder = java.time.Duration.between(afterMonths, toDT);
      if (totalMonths > 0 && remainder.isNegative())
        totalMonths--;
      else if (totalMonths < 0 && !remainder.isNegative() && !remainder.isZero())
        totalMonths++;
    }
    return new CypherDuration(totalMonths, 0, 0, 0);
  }

  /**
   * Compute a duration between two temporal values, returning total days only.
   * Returns P<totalDays>D format (no months/time components).
   * Months are converted to approximate days and added to total.
   */
  public static CypherDuration durationInDays(final CypherTemporalValue from, final CypherTemporalValue to) {
    // Time-only types have no date component → return PT0S
    if (isTimeOnly(from) || isTimeOnly(to))
      return new CypherDuration(0, 0, 0, 0);
    final LocalDateTime fromDT = resolveDateTime(from, to);
    final LocalDateTime toDT = resolveDateTime(to, from);
    long totalDays = ChronoUnit.DAYS.between(fromDT.toLocalDate(), toDT.toLocalDate());
    // Adjust for time-of-day: if we overestimate days, the remainder would have wrong sign
    if (totalDays != 0) {
      final LocalDateTime afterDays = fromDT.plusDays(totalDays);
      final java.time.Duration remainder = java.time.Duration.between(afterDays, toDT);
      if (totalDays > 0 && remainder.isNegative())
        totalDays--;
      else if (totalDays < 0 && !remainder.isNegative() && !remainder.isZero())
        totalDays++;
    }
    return new CypherDuration(0, totalDays, 0, 0);
  }

  /**
   * Compute a duration between two temporal values, returning total seconds.
   * Returns PT<totalHours>H<min>M<sec>S format (no date components).
   */
  public static CypherDuration durationInSeconds(final CypherTemporalValue from, final CypherTemporalValue to) {
    // If either is time-only, only compare time portions
    if (isTimeOnly(from) || isTimeOnly(to)) {
      // Two CypherTime values: compare by instant (UTC-normalized)
      if (from instanceof CypherTime && to instanceof CypherTime) {
        final java.time.Duration duration = java.time.Duration.between(
            ((CypherTime) from).getValue(), ((CypherTime) to).getValue());
        return new CypherDuration(0, 0, duration.getSeconds(), duration.getNano());
      }
      // When mixed with a zoned datetime, use the zoned datetime's timezone
      if (from instanceof CypherDateTime || to instanceof CypherDateTime) {
        final CypherDateTime zoned = (from instanceof CypherDateTime) ? (CypherDateTime) from : (CypherDateTime) to;
        final ZoneId zone = zoned.getValue().getZone();
        final LocalDate refDate = getReferenceDate(zoned);
        final ZonedDateTime fromZDT = toZonedDateTime(from, zone, refDate);
        final ZonedDateTime toZDT = toZonedDateTime(to, zone, refDate);
        final java.time.Duration duration = java.time.Duration.between(fromZDT, toZDT);
        return new CypherDuration(0, 0, duration.getSeconds(), duration.getNano());
      }
      // Otherwise compare local times
      final LocalTime fromTime = extractTime(from);
      final LocalTime toTime = extractTime(to);
      final java.time.Duration duration = java.time.Duration.between(fromTime, toTime);
      return new CypherDuration(0, 0, duration.getSeconds(), duration.getNano());
    }
    // Both have date components — handle DST-aware computation
    if (from instanceof CypherDateTime || to instanceof CypherDateTime) {
      final CypherDateTime zoned = (from instanceof CypherDateTime) ? (CypherDateTime) from : (CypherDateTime) to;
      final ZoneId zone = zoned.getValue().getZone();
      final LocalDate refDate = getReferenceDate(zoned);
      final ZonedDateTime fromZDT = toZonedDateTime(from, zone, refDate);
      final ZonedDateTime toZDT = toZonedDateTime(to, zone, refDate);
      final java.time.Duration duration = java.time.Duration.between(fromZDT, toZDT);
      return new CypherDuration(0, 0, duration.getSeconds(), duration.getNano());
    }
    final LocalDateTime fromDT = extractDateTime(from);
    final LocalDateTime toDT = extractDateTime(to);
    final java.time.Duration duration = java.time.Duration.between(fromDT, toDT);
    return new CypherDuration(0, 0, duration.getSeconds(), duration.getNano());
  }

  /**
   * Compute the full duration between two temporal values (months, days, seconds, nanos).
   * Returns smart mixed format: P<Y>Y<M>M<D>DT<H>H<M>M<S>S
   */
  public static CypherDuration durationBetween(final CypherTemporalValue from, final CypherTemporalValue to) {
    // If either is time-only, only compare time portions
    if (isTimeOnly(from) || isTimeOnly(to)) {
      // Two CypherTime values: compare by instant (UTC-normalized)
      if (from instanceof CypherTime && to instanceof CypherTime) {
        final java.time.Duration timeDur = java.time.Duration.between(
            ((CypherTime) from).getValue(), ((CypherTime) to).getValue());
        return new CypherDuration(0, 0, timeDur.getSeconds(), timeDur.getNano());
      }
      // When mixed with a zoned datetime, use the zoned datetime's timezone
      if (from instanceof CypherDateTime || to instanceof CypherDateTime) {
        final CypherDateTime zoned = (from instanceof CypherDateTime) ? (CypherDateTime) from : (CypherDateTime) to;
        final ZoneId zone = zoned.getValue().getZone();
        final LocalDate refDate = getReferenceDate(zoned);
        final ZonedDateTime fromZDT = toZonedDateTime(from, zone, refDate);
        final ZonedDateTime toZDT = toZonedDateTime(to, zone, refDate);
        final java.time.Duration timeDur = java.time.Duration.between(fromZDT, toZDT);
        return new CypherDuration(0, 0, timeDur.getSeconds(), timeDur.getNano());
      }
      // Otherwise compare local times
      final LocalTime fromTime = extractTime(from);
      final LocalTime toTime = extractTime(to);
      final java.time.Duration timeDur = java.time.Duration.between(fromTime, toTime);
      return new CypherDuration(0, 0, timeDur.getSeconds(), timeDur.getNano());
    }

    // Both have date components: compute full duration
    final LocalDateTime fromDT = resolveDateTime(from, to);
    final LocalDateTime toDT = resolveDateTime(to, from);

    // Calendar component: months and days
    final java.time.Period period = fromDT.toLocalDate().until(toDT.toLocalDate());
    long months = period.toTotalMonths();
    long days = period.getDays();

    // Clock component: seconds between same-day times
    final LocalDateTime afterCalendar = fromDT.plusMonths(months).plusDays(days);
    final java.time.Duration clockDuration = java.time.Duration.between(afterCalendar, toDT);
    long seconds = clockDuration.getSeconds();
    int nanos = clockDuration.getNano();

    // Normalize: ensure days and seconds have the same sign
    if (days < 0 && (seconds > 0 || (seconds == 0 && nanos > 0))) {
      // Negative days with positive clock: roll seconds back into days
      days++;
      seconds -= 86400;
    } else if ((seconds < 0 || (seconds == 0 && nanos < 0)) && days > 0) {
      // Positive days with negative clock: borrow from days
      days--;
      seconds += 86400;
    } else if (months < 0 && days > 0) {
      // Negative months with positive days: borrow from months
      months++;
      days -= 30;
    } else if (months > 0 && days < 0) {
      // Positive months with negative days: borrow from months
      months--;
      days += 30;
    }

    return new CypherDuration(months, days, seconds, nanos);
  }

  /**
   * Compute the total nanoseconds from millisecond, microsecond, and nanosecond map fields.
   * Per Cypher spec, these are additive: total = millisecond*1_000_000 + microsecond*1_000 + nanosecond.
   * If none are present, returns the defaultNanos value.
   */
  public static int computeNanos(final java.util.Map<String, Object> map, final int defaultNanos) {
    final boolean hasMs = map.containsKey("millisecond");
    final boolean hasUs = map.containsKey("microsecond");
    final boolean hasNs = map.containsKey("nanosecond");
    if (!hasMs && !hasUs && !hasNs)
      return defaultNanos;
    // Preserve unspecified higher-order portions from defaultNanos.
    // E.g. truncate('millisecond', t, {nanosecond: 2}) should keep the millisecond portion.
    int nanos = 0;
    if (hasMs)
      nanos += ((Number) map.get("millisecond")).intValue() * 1_000_000;
    else
      nanos += (defaultNanos / 1_000_000) * 1_000_000;
    if (hasUs)
      nanos += ((Number) map.get("microsecond")).intValue() * 1_000;
    else
      nanos += ((defaultNanos % 1_000_000) / 1_000) * 1_000;
    if (hasNs)
      nanos += ((Number) map.get("nanosecond")).intValue();
    else
      nanos += defaultNanos % 1_000;
    return nanos;
  }

  private static boolean isTimeOnly(final CypherTemporalValue val) {
    return val instanceof CypherLocalTime || val instanceof CypherTime;
  }

  private static LocalTime extractTime(final CypherTemporalValue val) {
    if (val instanceof CypherLocalTime)
      return ((CypherLocalTime) val).getValue();
    if (val instanceof CypherTime)
      return ((CypherTime) val).getValue().toLocalTime();
    if (val instanceof CypherLocalDateTime)
      return ((CypherLocalDateTime) val).getValue().toLocalTime();
    if (val instanceof CypherDateTime)
      return ((CypherDateTime) val).getValue().toLocalTime();
    if (val instanceof CypherDate)
      return LocalTime.MIDNIGHT;
    throw new IllegalArgumentException("Cannot extract time from: " + val.getClass().getSimpleName());
  }

  private static LocalDate extractDate(final CypherTemporalValue val) {
    if (val instanceof CypherDate)
      return ((CypherDate) val).getValue();
    if (val instanceof CypherLocalDateTime)
      return ((CypherLocalDateTime) val).getValue().toLocalDate();
    if (val instanceof CypherDateTime)
      return ((CypherDateTime) val).getValue().toLocalDate();
    throw new IllegalArgumentException("Cannot extract date from: " + val.getClass().getSimpleName());
  }

  private static LocalDateTime extractDateTime(final CypherTemporalValue val) {
    if (val instanceof CypherDate)
      return ((CypherDate) val).getValue().atStartOfDay();
    if (val instanceof CypherLocalDateTime)
      return ((CypherLocalDateTime) val).getValue();
    if (val instanceof CypherDateTime)
      return ((CypherDateTime) val).getValue().toLocalDateTime();
    if (val instanceof CypherLocalTime)
      return LocalDateTime.of(LocalDate.of(0, 1, 1), ((CypherLocalTime) val).getValue());
    if (val instanceof CypherTime)
      return LocalDateTime.of(LocalDate.of(0, 1, 1), ((CypherTime) val).getValue().toLocalTime());
    throw new IllegalArgumentException("Cannot extract datetime from: " + val.getClass().getSimpleName());
  }

  /**
   * Resolve a temporal value to a LocalDateTime, using the other value's timezone if needed.
   * When one arg is a CypherDateTime (zoned) and the other is not, the non-zoned value
   * is interpreted in the zoned value's timezone, then both are converted to UTC.
   */
  private static LocalDateTime resolveDateTime(final CypherTemporalValue val, final CypherTemporalValue other) {
    if (val instanceof CypherDateTime dt) {
      // If both are zoned, convert to UTC for accurate comparison
      if (other instanceof CypherDateTime)
        return dt.getValue().withZoneSameInstant(ZoneOffset.UTC).toLocalDateTime();
      // If only this one is zoned, convert to UTC
      return dt.getValue().withZoneSameInstant(ZoneOffset.UTC).toLocalDateTime();
    }
    if (other instanceof CypherDateTime otherDT) {
      // Non-zoned value paired with a zoned value: interpret in that timezone, then convert to UTC
      final ZoneId zone = otherDT.getValue().getZone();
      final LocalDateTime localDT = extractDateTime(val);
      return localDT.atZone(zone).withZoneSameInstant(ZoneOffset.UTC).toLocalDateTime();
    }
    return extractDateTime(val);
  }

  /**
   * Convert a temporal value to a ZonedDateTime in the given timezone.
   * Used for DST-aware duration calculations when mixing zoned and non-zoned types.
   * The referenceDate is used for time-only types to determine which date to place them on.
   */
  private static ZonedDateTime toZonedDateTime(final CypherTemporalValue val, final ZoneId zone,
      final LocalDate referenceDate) {
    if (val instanceof CypherDateTime dt)
      return dt.getValue();
    if (val instanceof CypherDate d)
      return d.getValue().atStartOfDay(zone);
    if (val instanceof CypherLocalDateTime ldt)
      return ldt.getValue().atZone(zone);
    if (val instanceof CypherLocalTime lt)
      return lt.getValue().atDate(referenceDate).atZone(zone);
    if (val instanceof CypherTime t)
      return t.getValue().atDate(referenceDate).toZonedDateTime();
    throw new IllegalArgumentException("Cannot convert to ZonedDateTime: " + val.getClass().getSimpleName());
  }

  /**
   * Get the reference date from a temporal value (for placing time-only values on a date).
   */
  private static LocalDate getReferenceDate(final CypherTemporalValue val) {
    if (val instanceof CypherDateTime dt)
      return dt.getValue().toLocalDate();
    if (val instanceof CypherDate d)
      return d.getValue();
    if (val instanceof CypherLocalDateTime ldt)
      return ldt.getValue().toLocalDate();
    return LocalDate.of(0, 1, 1);
  }

  private static java.time.Instant toInstant(final CypherTemporalValue val) {
    if (val instanceof CypherDateTime)
      return ((CypherDateTime) val).getValue().toInstant();
    if (val instanceof CypherDate)
      return ((CypherDate) val).getValue().atStartOfDay(java.time.ZoneOffset.UTC).toInstant();
    if (val instanceof CypherLocalDateTime)
      return ((CypherLocalDateTime) val).getValue().atZone(java.time.ZoneOffset.UTC).toInstant();
    if (val instanceof CypherTime)
      return ((CypherTime) val).getValue().atDate(LocalDate.of(0, 1, 1)).toInstant();
    if (val instanceof CypherLocalTime)
      return LocalDateTime.of(LocalDate.of(0, 1, 1), ((CypherLocalTime) val).getValue()).atZone(java.time.ZoneOffset.UTC).toInstant();
    throw new IllegalArgumentException("Cannot convert to Instant: " + val.getClass().getSimpleName());
  }
}
