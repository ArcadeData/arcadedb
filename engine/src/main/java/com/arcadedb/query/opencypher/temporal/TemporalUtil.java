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

import com.arcadedb.database.Database;
import com.arcadedb.database.Document;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.DateUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.*;
import java.time.temporal.ChronoUnit;
import java.time.temporal.IsoFields;
import java.time.temporal.Temporal;
import java.time.temporal.WeekFields;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility methods for temporal parsing and operations.
 */
public final class TemporalUtil {

  // An offset follows the digits of a time and is not followed by more digits: a signed year of a date ('-0001-12-31',
  // '+10000-01-01') is not one, and used to be rewritten into '-00:01-12-31', which no parser read (issue #10199)
  private static final Pattern COMPACT_OFFSET = Pattern.compile(
      "(?<=\\d)([+-])(\\d{2})(\\d{2})(?![:\\d])");

  private TemporalUtil() {
  }

  /**
   * Normalize compact timezone offsets in a datetime string: +0200 → +02:00
   * Also handles the case where the offset appears before a timezone name in brackets.
   */
  public static String normalizeOffsetInString(final String str) {
    // Find timezone offset pattern: +HHMM or -HHMM (not followed by colon, not preceded by colon)
    final Matcher m = COMPACT_OFFSET.matcher(str);
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
   * Parse the timezone of a TIME: an offset like "+01:00", "+0100" or "Z", or a named zone ("UTC", "Asia/Tokyo"), which
   * stands for the offset the zone has now, as Neo4j reads it: a time has no date to resolve a daylight saving rule
   * against. A named zone used to be refused with a {@code DateTimeException} (issue #10199).
   */
  public static ZoneOffset parseOffset(final String str) {
    if ("Z".equalsIgnoreCase(str))
      return ZoneOffset.UTC;
    try {
      return ZoneOffset.of(str);
    } catch (final DateTimeException e) {
      final ZoneId zone;
      try {
        zone = ZoneId.of(str);
      } catch (final DateTimeException notAZone) {
        throw e;
      }
      return zone.getRules().getOffset(Instant.now());
    }
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
    return switch (unit.toLowerCase(Locale.ROOT)) {
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
    return switch (unit.toLowerCase(Locale.ROOT)) {
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
    return switch (unit.toLowerCase(Locale.ROOT)) {
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
    final Temporal[] pair = dateBearingPair(from, to);
    return new CypherDuration(pair[0].until(pair[1], ChronoUnit.MONTHS), 0, 0, 0);
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
    final Temporal[] pair = dateBearingPair(from, to);
    return new CypherDuration(0, pair[0].until(pair[1], ChronoUnit.DAYS), 0, 0);
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
        final Duration duration = Duration.between(
            ((CypherTime) from).getValue(), ((CypherTime) to).getValue());
        return new CypherDuration(0, 0, duration.getSeconds(), duration.getNano());
      }
      // When mixed with a zoned datetime, use the zoned datetime's timezone
      if (from instanceof CypherDateTime || to instanceof CypherDateTime) {
        final CypherDateTime zoned = from instanceof CypherDateTime ? (CypherDateTime) from : (CypherDateTime) to;
        final ZoneId zone = zoned.getValue().getZone();
        final LocalDate refDate = getReferenceDate(zoned);
        final ZonedDateTime fromZDT = toZonedDateTime(from, zone, refDate);
        final ZonedDateTime toZDT = toZonedDateTime(to, zone, refDate);
        final Duration duration = Duration.between(fromZDT, toZDT);
        return new CypherDuration(0, 0, duration.getSeconds(), duration.getNano());
      }
      // Otherwise compare local times
      final LocalTime fromTime = extractTime(from);
      final LocalTime toTime = extractTime(to);
      final Duration duration = Duration.between(fromTime, toTime);
      return new CypherDuration(0, 0, duration.getSeconds(), duration.getNano());
    }
    // Both have date components — handle DST-aware computation
    if (from instanceof CypherDateTime || to instanceof CypherDateTime) {
      final CypherDateTime zoned = from instanceof CypherDateTime ? (CypherDateTime) from : (CypherDateTime) to;
      final ZoneId zone = zoned.getValue().getZone();
      final LocalDate refDate = getReferenceDate(zoned);
      final ZonedDateTime fromZDT = toZonedDateTime(from, zone, refDate);
      final ZonedDateTime toZDT = toZonedDateTime(to, zone, refDate);
      final Duration duration = Duration.between(fromZDT, toZDT);
      return new CypherDuration(0, 0, duration.getSeconds(), duration.getNano());
    }
    final LocalDateTime fromDT = extractDateTime(from);
    final LocalDateTime toDT = extractDateTime(to);
    final Duration duration = Duration.between(fromDT, toDT);
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
        final Duration timeDur = Duration.between(
            ((CypherTime) from).getValue(), ((CypherTime) to).getValue());
        return new CypherDuration(0, 0, timeDur.getSeconds(), timeDur.getNano());
      }
      // When mixed with a zoned datetime, use the zoned datetime's timezone
      if (from instanceof CypherDateTime || to instanceof CypherDateTime) {
        final CypherDateTime zoned = from instanceof CypherDateTime ? (CypherDateTime) from : (CypherDateTime) to;
        final ZoneId zone = zoned.getValue().getZone();
        final LocalDate refDate = getReferenceDate(zoned);
        final ZonedDateTime fromZDT = toZonedDateTime(from, zone, refDate);
        final ZonedDateTime toZDT = toZonedDateTime(to, zone, refDate);
        final Duration timeDur = Duration.between(fromZDT, toZDT);
        return new CypherDuration(0, 0, timeDur.getSeconds(), timeDur.getNano());
      }
      // Otherwise compare local times
      final LocalTime fromTime = extractTime(from);
      final LocalTime toTime = extractTime(to);
      final Duration timeDur = Duration.between(fromTime, toTime);
      return new CypherDuration(0, 0, timeDur.getSeconds(), timeDur.getNano());
    }

    // Both have date components: compute full duration. Split exactly as Neo4j does: whole months first, then whole days
    // after them, then the clock remainder, each unit truncated towards zero on the full date-time. Every component
    // therefore carries the same sign, and from.plusMonths(m).plusDays(d).plusSeconds(s) - the order ArithmeticExpression
    // applies a duration in - lands exactly on `to`. Borrowing from a Period with a fixed month length did not, for any
    // month that was not 30 days long (issue #8386). On zoned operands the months and days are the local calendar's
    // (see dateBearingPair, issue #10198)
    final Temporal[] pair = dateBearingPair(from, to);
    final Temporal toDT = pair[1];
    final long months = pair[0].until(toDT, ChronoUnit.MONTHS);
    final Temporal afterMonths = pair[0].plus(months, ChronoUnit.MONTHS);
    final long days = afterMonths.until(toDT, ChronoUnit.DAYS);
    final Duration clockDuration = Duration.ofNanos(afterMonths.plus(days, ChronoUnit.DAYS).until(toDT, ChronoUnit.NANOS));

    return new CypherDuration(months, days, clockDuration.getSeconds(), clockDuration.getNano());
  }

  /**
   * {@code base} with the date and time fields {@code map} gives in place of its own, all resolved together: applied one at
   * a time, {@code withYear(2023)} on February 29 clamped the day to the 28th before {@code withMonth(3)} saw it, so
   * {@code {year: 2023, month: 3}} answered March 28 instead of March 29. A millisecond, microsecond or nanosecond
   * replaces the whole fraction of second (see {@link #computeNanos(Map, int)}).
   */
  public static LocalDateTime withFieldOverrides(final LocalDateTime base, final Map<String, Object> map) {
    final Object year = map.get("year");
    final Object month = map.get("month");
    final Object day = map.get("day");
    final Object hour = map.get("hour");
    final Object minute = map.get("minute");
    final Object second = map.get("second");
    final LocalDate date = year == null && month == null && day == null ?
        base.toLocalDate() :
        LocalDate.of(year != null ? toIntField("year", year) : base.getYear(), month != null ? toIntField("month", month) : base.getMonthValue(),
            day != null ? toIntField("day", day) : base.getDayOfMonth());
    final LocalTime time = LocalTime.of(hour != null ? toIntField("hour", hour) : base.getHour(),
        minute != null ? toIntField("minute", minute) : base.getMinute(), second != null ? toIntField("second", second) : base.getSecond(),
        computeNanos(map, base.getNano()));
    return LocalDateTime.of(date, time);
  }

  /**
   * The fraction of second of a temporal CONSTRUCTED from a map, which may name a base value ({@code time:},
   * {@code datetime:}) to take the fields it does not give from: the map's millisecond, microsecond and nanosecond
   * fields, when it has any, are the whole fraction, and the base's is {@code defaultNanos}, kept only when it has none.
   * {@code localtime({time: localtime('12:30:45.123456789'), nanosecond: 5})} is {@code 12:30:45.000000005}, as in Neo4j:
   * the base's milliseconds and microseconds used to survive a nanosecond given on its own (issue #10199).
   *
   * @see #computeNanos(Map, int, boolean)
   */
  public static int computeNanos(final Map<String, Object> map, final int defaultNanos) {
    return computeNanos(map, defaultNanos, false);
  }

  /**
   * Compute the total nanoseconds from millisecond, microsecond, and nanosecond map fields.
   * Per Cypher spec, these are additive: total = millisecond*1_000_000 + microsecond*1_000 + nanosecond.
   * If none are present, returns the defaultNanos value.
   * <p>
   * Each component is range-checked the way Neo4j does, BEFORE it is combined: millisecond in 0..999, microsecond in
   * 0..999 when millisecond is given (else 0..999_999), nanosecond in 0..999 when a coarser component is given (else up
   * to 999_999 or 999_999_999). A component is read as a long, so a value past the int range is refused rather than
   * wrapped into a plausible fraction of a second (issue #8571).
   *
   * @param keepCoarserUnits true for the adjustment map of a {@code truncate()}, which keeps the portions of
   *                         {@code defaultNanos} coarser than the fields it gives: {@code truncate('millisecond', t,
   *                         {nanosecond: 2})} keeps the milliseconds the truncation left. False for a constructor, see
   *                         {@link #computeNanos(Map, int)}
   */
  public static int computeNanos(final Map<String, Object> map, final int defaultNanos, final boolean keepCoarserUnits) {
    final Object ms = map.get("millisecond");
    final Object us = map.get("microsecond");
    final Object ns = map.get("nanosecond");
    if (ms == null && us == null && ns == null)
      return defaultNanos;

    final int keptNanos = keepCoarserUnits ? defaultNanos : 0;
    long nanos = 0;
    if (ms != null)
      nanos += subSecondField("Millisecond", ms, 1_000L) * 1_000_000L;
    else
      nanos += (keptNanos / 1_000_000) * 1_000_000L;
    if (us != null)
      nanos += subSecondField("Microsecond", us, ms != null ? 1_000L : 1_000_000L) * 1_000L;
    else
      nanos += ((keptNanos % 1_000_000) / 1_000) * 1_000L;
    if (ns != null)
      nanos += subSecondField("Nanosecond", ns, us != null ? 1_000L : ms != null ? 1_000_000L : 1_000_000_000L);
    else
      nanos += keptNanos % 1_000;
    if (nanos >= 1_000_000_000L)
      // Only reachable when a portion preserved from defaultNanos is combined with a finer field whose own range is
      // wider because the coarser one was not given, e.g. a preserved millisecond plus a bare microsecond: 999999
      throw new IllegalArgumentException("Invalid value for NanoOfSecond: " + nanos + " (valid values 0 - 999999999)");
    return (int) nanos;
  }

  private static long subSecondField(final String name, final Object value, final long limit) {
    final long v = toIntegralLong(name, value);
    if (v < 0 || v >= limit)
      throw new IllegalArgumentException("Invalid value for " + name + ": " + value + " (valid values 0 - " + (limit - 1) + ")");
    return v;
  }

  /**
   * Reads a temporal component as an int, refusing a value the int range cannot represent instead of letting
   * {@link Number#intValue()} wrap it into a different, in-range value (issue #8571: {@code hour: 4294967308} must not
   * become 12).
   */
  public static int toIntField(final String name, final Object value) {
    final long v = toIntegralLong(name, value);
    if (v < Integer.MIN_VALUE || v > Integer.MAX_VALUE)
      throw new IllegalArgumentException("Invalid value for " + name + ": " + value);
    return (int) v;
  }

  private static long toIntegralLong(final String name, final Object value) {
    if (value instanceof Long || value instanceof Integer || value instanceof Short || value instanceof Byte)
      return ((Number) value).longValue();
    // Exact for the arbitrary-precision types: a double would round 1.0000000000000001 to an integral 1.0
    if (value instanceof BigDecimal || value instanceof BigInteger) {
      try {
        return (value instanceof BigDecimal d ? d : new BigDecimal((BigInteger) value)).longValueExact();
      } catch (final ArithmeticException e) {
        throw new IllegalArgumentException("Invalid value for " + name + ": " + value);
      }
    }
    if (value instanceof Number n) {
      final double d = n.doubleValue();
      if (d != Math.rint(d) || d < Long.MIN_VALUE || d >= 0x1p63)
        throw new IllegalArgumentException("Invalid value for " + name + ": " + value);
      return (long) d;
    }
    try {
      return Long.parseLong(value.toString());
    } catch (final NumberFormatException e) {
      throw new IllegalArgumentException("Invalid value for " + name + ": " + value);
    }
  }

  /**
   * Convert a Cypher temporal value (or a collection/array containing temporal values) to a form
   * that ArcadeDB can serialize.
   *
   * CypherDateTime is kept as its ISO-8601 string representation (not unwrapped to ZonedDateTime)
   * so that: (a) untyped properties store it as TYPE_STRING, preserving timezone info for
   * later component access via {@link #convertFromStorage(Object)}; and (b) for
   * schema-typed DATETIME properties, Type.convert() parses the string into the target Java type
   * (timezone is dropped on a LocalDateTime target, matching the SQL sysdate() semantics).
   *
   * Non-temporal scalars and collections of non-temporal scalars are returned unchanged.
   */
  public static Object toCoreJavaType(final Object value) {
    if (value == null || value instanceof Number || value instanceof String || value instanceof Boolean)
      return value;

    // Every Cypher temporal is persisted as a native value that names its own type (issue #8572), so a read never has to
    // guess a type from the text. A property declared STRING gets the text form from Type.convert
    if (value instanceof CypherDateTime dt)
      return dt.getValue();
    if (value instanceof CypherDate d)
      return d.getValue();
    if (value instanceof CypherLocalDateTime ldt)
      return ldt.getValue();
    if (value instanceof CypherLocalTime lt)
      return lt.getValue();
    if (value instanceof CypherTime t)
      return t.getValue();
    if (value instanceof CypherDuration dur)
      return dur;

    // Recurse into collections - skip when first element is a non-temporal scalar (vector embeddings, etc.)
    if (value instanceof Collection<?> collection) {
      if (collection.isEmpty())
        return value;
      final Object first = collection.iterator().next();
      if (first instanceof Number || first instanceof String || first instanceof Boolean)
        return value;
      final List<Object> converted = new ArrayList<>(collection.size());
      for (final Object item : collection)
        converted.add(toCoreJavaType(item));
      return converted;
    }
    if (value instanceof Object[] array) {
      if (array.length == 0)
        return value;
      if (array[0] instanceof Number || array[0] instanceof String || array[0] instanceof Boolean)
        return value;
      final Object[] converted = new Object[array.length];
      for (int i = 0; i < array.length; i++)
        converted[i] = toCoreJavaType(array[i]);
      return converted;
    }

    return value;
  }

  /**
   * The text a Cypher temporal is stored as in a property declared STRING: what {@link #toCoreJavaType(Object)} answered
   * before the temporals had native types (issue #8572).
   */
  public static String toStorageText(final Object value) {
    if (value instanceof CypherDateTime || value instanceof CypherDuration)
      return value.toString();
    return String.valueOf(toCoreJavaType(value));
  }

  /**
   * The value to hand to an index lookup for a Cypher temporal operand: the {@code java.time} value the wrapper holds,
   * which the index key conversion understands (a zoned datetime stays a {@link ZonedDateTime}, where
   * {@link #toCoreJavaType(Object)} would turn it into text). Any other value is returned unchanged (issue #8921).
   */
  public static Object toIndexKey(final Object value) {
    if (value instanceof CypherDateTime dt)
      return dt.getValue();
    if (value instanceof CypherTemporalValue)
      return toCoreJavaType(value);
    return value;
  }

  /**
   * Inverse of {@link #toCoreJavaType(Object)}: wrap a native {@code java.time} / {@code java.util.Date}
   * value into its Cypher temporal type so it participates in temporal comparison and component access.
   * <p>
   * Used on the read side (property fetch) and at comparison time to normalize temporal query
   * parameters (e.g. a datetime sent over Bolt, which arrives as a {@code java.time} value) against
   * stored temporals. Values that are already {@link CypherTemporalValue} are returned unchanged, and
   * non-temporal values are passed through untouched, so this is safe to call on any operand.
   */
  public static Object fromCoreJavaType(final Object value) {
    if (value == null || value instanceof CypherTemporalValue || value instanceof Number || value instanceof String
        || value instanceof Boolean)
      return value;

    if (value instanceof LocalDate d)
      return new CypherDate(d);
    if (value instanceof LocalTime t)
      return new CypherLocalTime(t);
    if (value instanceof OffsetTime t)
      return new CypherTime(t);
    if (value instanceof LocalDateTime ldt)
      return new CypherLocalDateTime(ldt);
    if (value instanceof ZonedDateTime zdt)
      return new CypherDateTime(zdt);
    if (value instanceof OffsetDateTime odt)
      return new CypherDateTime(odt.toZonedDateTime());
    if (value instanceof Instant i)
      return new CypherDateTime(i.atZone(ZoneOffset.UTC));
    if (value instanceof Date date)
      return new CypherDateTime(date.toInstant().atZone(ZoneOffset.UTC));
    // A supported arcadedb.dateTimeImplementation / dateImplementation: an instant like java.util.Date (issue #9647)
    if (value instanceof Calendar calendar)
      return new CypherDateTime(calendar.toInstant().atZone(ZoneOffset.UTC));
    // A duration a driver or the Java API binds as a parameter
    if (value instanceof Duration duration)
      return new CypherDuration(0, 0, duration.getSeconds(), duration.getNano());
    if (value instanceof Period period)
      return new CypherDuration(period.toTotalMonths(), period.getDays(), 0, 0);

    return value;
  }

  /**
   * Reads {@code propertyName} from {@code document} as a Cypher value: a native temporal is wrapped into its Cypher
   * type so component access ({@code dur.seconds}, {@code t.hour}) and temporal comparison work. A String is not
   * interpreted: it is a String unless the schema declares the property with a temporal type, in which case the
   * deserializer has already converted it (issue #8572). The one exception is a database written by a release that
   * stored these temporals as text, see {@link #restoreLegacyText(Document, String, String)} (issue #9636).
   */
  public static Object convertFromStorage(final Document document, final String propertyName) {
    final Object value = document.get(propertyName);
    // A ZonedDateTime on an undeclared (or ZONED_DATETIME) property is the native type that kept its zone, not a
    // DATETIME read back in the configured Java class, so it is a real zoned value (issue #8572)
    if (value instanceof ZonedDateTime zoned && keepsZone(document, propertyName))
      return new CypherDateTime(zoned);
    if (value instanceof String text)
      return restoreLegacyText(document, propertyName, text);
    // A DATE is a Cypher Date whatever Java class arcadedb.dateImplementation reads it into (issue #9647)
    return convertFromStorage(asDeclaredDate(document, propertyName, value));
  }

  /**
   * The value of {@code propertyName} as the Cypher type its schema declares: a property declared DATE that
   * {@code arcadedb.dateImplementation} read back as a {@code java.util.Date}, a {@code java.util.Calendar} or a
   * {@code LocalDateTime} is the {@link CypherDate} of that day, so it compares with a {@code date()} literal under every
   * setting as it does under the default {@code LocalDate} (issue #9647). Any other value is returned unchanged.
   */
  public static Object asDeclaredDate(final Document document, final String propertyName, final Object value) {
    if (isDateReadAsInstant(value) && isDeclaredDate(document, propertyName))
      return new CypherDate(LocalDate.ofEpochDay(DateUtils.dateToEpochDays(value)));
    return value;
  }

  /**
   * Whether {@code value} is a native temporal {@link #fromCoreJavaType(Object)} wraps: a {@code java.time} value, a
   * {@code java.util.Date} or a {@code java.util.Calendar}. The one list of those classes, so a newly supported
   * {@code arcadedb.dateTimeImplementation} cannot be missed on one path (issue #9647).
   */
  public static boolean isNativeTemporal(final Object value) {
    return value instanceof Temporal || value instanceof Date || value instanceof Calendar;
  }

  /**
   * Whether {@code value} is a temporal {@link #distinctKey(Object)} keys: a Cypher temporal, a native one, or a
   * {@code java.time} duration or period.
   */
  public static boolean isTemporalValue(final Object value) {
    return value instanceof CypherTemporalValue || isNativeTemporal(value) || value instanceof Duration || value instanceof Period;
  }

  /**
   * The value a temporal stands for in duplicate elimination ({@code DISTINCT}, {@code GROUP BY}, {@code UNION}), so that two
   * temporals which {@code =} reports equal share a key whatever Java class holds them (issue #10526): {@code LocalDateTime.equals}
   * is false against a {@code ZonedDateTime} for every pair, however equal the instants. The rule is the one of
   * {@code ComparisonExpression}:
   * <ul>
   *   <li>an instant that has no zone of its own - a stored DATETIME, which is read as a local datetime or as a zone-less
   *   datetime; a raw {@code LocalDateTime}; an {@code Instant}, a {@code java.util.Date} or a {@code Calendar} - is that instant in
   *   UTC, which is where {@code =} puts it against a zoned datetime in UTC</li>
   *   <li>a zoned datetime keeps its zone: the same instant in two zones is two values to {@code =}</li>
   *   <li>a {@code localdatetime()} built by a query is a local datetime, never equal to a datetime, and keeps its own key</li>
   *   <li>every other temporal is the {@code java.time} value its Cypher wrapper holds; a native duration or period is read as the
   *   Cypher duration {@code =} reads it as</li>
   * </ul>
   * {@code =} is not transitive across the first two kinds (a stored value equals the datetime in every zone at its instant, and
   * those datetimes are not equal to each other), so the zone-less value joins the one it matches in UTC and no other.
   *
   * @return the key, or {@code null} when {@code value} is no temporal this class keys
   */
  public static Object distinctKey(final Object value) {
    return switch (value) {
      case CypherDateTime dateTime ->
          dateTime.isZoneless() ? dateTime.getValue().withZoneSameInstant(ZoneOffset.UTC) : dateTime.getValue();
      case CypherLocalDateTime local -> local.isStored() ? local.getValue().atZone(ZoneOffset.UTC) : local.getValue();
      case CypherDate date -> date.getValue();
      case CypherLocalTime time -> time.getValue();
      case CypherTime time -> time.getValue();
      // the one wrapper left, the duration: its equals / hashCode compare its months, days, seconds and nanoseconds, as = does
      case CypherTemporalValue other -> other;
      case ZonedDateTime zoned -> zoned;
      case OffsetDateTime offset -> offset.toZonedDateTime();
      case LocalDateTime local -> local.atZone(ZoneOffset.UTC);
      case Instant instant -> instant.atZone(ZoneOffset.UTC);
      // getTime(), not toInstant(): java.sql.Date refuses the latter
      case Date date -> Instant.ofEpochMilli(date.getTime()).atZone(ZoneOffset.UTC);
      case Calendar calendar -> Instant.ofEpochMilli(calendar.getTimeInMillis()).atZone(ZoneOffset.UTC);
      case LocalDate date -> date;
      case LocalTime time -> time;
      case OffsetTime time -> time;
      case Duration duration -> fromCoreJavaType(duration);
      case Period period -> fromCoreJavaType(period);
      case null, default -> null;
    };
  }

  // The classes a non-default arcadedb.dateImplementation reads a DATE into: only these need the schema lookup, so the
  // default LocalDate and every non-temporal value pay one instanceof chain and nothing more
  private static boolean isDateReadAsInstant(final Object value) {
    return value instanceof Date || value instanceof Calendar || value instanceof LocalDateTime;
  }

  private static boolean isDeclaredDate(final Document document, final String propertyName) {
    final DocumentType type = document.getType();
    final Property property = type != null ? type.getPolymorphicPropertyIfExists(propertyName) : null;
    return property != null && property.getType() == Type.DATE;
  }

  /**
   * The value an openCypher read gives {@code text}, stored under {@code propertyName} of {@code document}: the text
   * itself, unless the database predates the native temporal types (issue #9636). A release before them persisted a
   * {@code time()}, {@code localtime()}, zoned {@code datetime()} or {@code duration()} on a property the schema does
   * not declare - every property openCypher writes - as its ISO text, and restored it on read by its shape. A database
   * written by such a release keeps that reading until {@code CALL db.migrateLegacyTemporals()} rewrites the text in
   * the native types; without it every duration and zoned datetime it holds would read back as a String. A declared
   * property is never restored: a STRING holds text by contract (issue #8384), and a temporal one has already been
   * converted by the deserializer.
   */
  public static Object restoreLegacyText(final Document document, final String propertyName, final String text) {
    // The character test first: ordinary text pays nothing for the schema lookups
    if (!mayBeTemporalString(text) || !isLegacyTemporalText(document))
      return text;
    final DocumentType type = document.getType();
    if (type != null && type.getPolymorphicPropertyIfExists(propertyName) != null)
      return text;
    return parseLegacyText(text);
  }

  private static boolean isLegacyTemporalText(final Document document) {
    final Database database = document.getDatabase();
    if (database == null)
      return false;
    final LocalSchema schema = database.getSchema().getEmbedded();
    return schema != null && schema.isLegacyTemporalText();
  }

  /**
   * The cheap shape of an ISO duration: {@code P} followed by a digit, a sign or {@code T}, ending in a digit or a unit
   * letter. It keeps ordinary text that merely starts with {@code P} ("Paris", "Peter") away from the parse and its
   * exception (issue #9338).
   */
  private static boolean looksLikeIsoDuration(final String str) {
    final char second = str.charAt(1);
    if (!(second >= '0' && second <= '9') && second != 'T' && second != '-' && second != '+')
      return false;
    final char last = str.charAt(str.length() - 1);
    return last >= '0' && last <= '9' || last == 'Y' || last == 'M' || last == 'W' || last == 'D' || last == 'H' || last == 'S';
  }

  /**
   * Whether {@code str} has the shape of a temporal text a release before the native temporal types wrote: the
   * character test that keeps ordinary strings away from {@link #parseLegacyText(String)}.
   */
  public static boolean mayBeTemporalString(final String str) {
    final int length = str.length();
    if (length == 0)
      return false;
    final char first = str.charAt(0);
    // An ISO duration is recognised by its leading 'P' and is as short as "P1D": the length floor must not apply to it
    // (issue #9338). Every other temporal text is at least 5 characters long.
    if (first == 'P')
      return length >= 3 && looksLikeIsoDuration(str);
    return length >= 5 && Character.isDigit(first);
  }

  /**
   * Parses a temporal text the way releases before the native temporal types read their stored text back (issue
   * #9636): a duration, a datetime, a time with an offset or a local time, recognised by shape. Returns the text itself
   * when it is none of them. Callers decide whether the text may be legacy at all; this only reads it.
   */
  public static Object parseLegacyText(final String str) {
    if (!mayBeTemporalString(str))
      return str;

    // Duration strings start with P (ISO-8601)
    if (str.charAt(0) == 'P') {
      try {
        return CypherDuration.parse(str);
      } catch (final Exception ignored) {
        // Not a valid duration string
      }
    }

    // DateTime strings: contain 'T' with date part before it and timezone/offset
    // e.g., 1912-01-01T00:00Z, 1984-10-11T12:31:14+01:00[Europe/Stockholm]
    final int tIdx = str.indexOf('T');
    if (tIdx >= 4 && tIdx < str.length() - 1 && Character.isDigit(str.charAt(0))) {
      try {
        return CypherDateTime.parse(str);
      } catch (final Exception ignored) {
        // Not a valid datetime string
      }
    }

    // Time strings: HH:MM:SS[.nanos][+/-offset] or HH:MM[+/-offset] or HH:MM:SS[.nanos]Z
    // Handles both full (10:35:00-08:00) and short (10:35-08:00) time formats
    if (str.length() >= 5 && str.charAt(2) == ':' && Character.isDigit(str.charAt(0)) && Character.isDigit(str.charAt(3))) {
      final boolean hasSeconds = str.length() >= 8 && str.charAt(5) == ':';
      // Check if it has a timezone offset (+ or - after the time part, or trailing Z)
      final int searchFrom = hasSeconds ? 8 : 5;
      boolean hasOffset = str.endsWith("Z");
      if (!hasOffset) {
        for (int i = searchFrom; i < str.length(); i++) {
          final char c = str.charAt(i);
          if (c == '+' || c == '-') {
            hasOffset = true;
            break;
          }
        }
      }
      try {
        return hasOffset ? CypherTime.parse(str) : CypherLocalTime.parse(str);
      } catch (final Exception ignored) {
        // Not a valid time string
      }
    }
    return str;
  }

  private static boolean keepsZone(final Document document, final String propertyName) {
    final DocumentType type = document.getType();
    final Property property = type != null ? type.getPolymorphicPropertyIfExists(propertyName) : null;
    return property == null || property.getType() == Type.ZONED_DATETIME;
  }

  /**
   * Wraps a native value into its Cypher temporal type, and returns anything else (a String included) unchanged.
   */
  public static Object convertFromStorage(final Object value) {
    // Fast path: common non-temporal types don't need conversion
    if (value == null || value instanceof Number || value instanceof Boolean)
      return value;

    // Handle single values - check temporal types before collections. Native java.time / java.util.Date
    // temporals (incl. java.util.Date, the default DATETIME storage type, and ZonedDateTime) are wrapped
    // into Cypher temporal values so a stored native datetime reads back as a comparable temporal.
    if (value instanceof CypherDuration)
      return value;
    if (isNativeTemporal(value)) {
      // A stored datetime keeps no zone: mark it so a comparison lets it adopt the other operand's zone (issue #9325).
      // Built directly, not through fromCoreJavaType(), so a scan allocates one wrapper per value
      if (value instanceof ZonedDateTime zdt)
        return CypherDateTime.ofStored(zdt);
      if (value instanceof OffsetDateTime odt)
        return CypherDateTime.ofStored(odt.toZonedDateTime());
      if (value instanceof Instant instant)
        return CypherDateTime.ofStored(instant.atZone(ZoneOffset.UTC));
      if (value instanceof Date date)
        return CypherDateTime.ofStored(date.toInstant().atZone(ZoneOffset.UTC));
      // Under arcadedb.dateTimeImplementation=java.util.Calendar: the zone it carries is the schema's, not a chosen one
      if (value instanceof Calendar calendar)
        return CypherDateTime.ofStored(calendar.toInstant().atZone(ZoneOffset.UTC));
      // A DATETIME column holds an instant: against a zoned datetime it compares as one (issue #10200)
      if (value instanceof LocalDateTime ldt)
        return CypherLocalDateTime.ofStored(ldt);
      final Object coerced = fromCoreJavaType(value);
      if (coerced instanceof CypherTemporalValue)
        return coerced;
    }

    return value;
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
   * The two date-bearing operands of {@code duration.between()}, {@code inMonths()} and {@code inDays()} as the
   * {@code java.time} values whose {@code until()} answers as Neo4j does: when either operand carries a zone, both are
   * {@link ZonedDateTime}s, a zone-less one read in that zone (the first operand's, when both have one). A zoned
   * {@code until()} counts months and days on the local calendar of the first operand's zone and the rest in elapsed time,
   * so the day across a daylight saving change is {@code P1D}, not {@code PT23H}. Both used to be converted to UTC first,
   * which counted every unit in elapsed time (issue #10198). Without a zone, both are {@link LocalDateTime}s.
   */
  private static Temporal[] dateBearingPair(final CypherTemporalValue from, final CypherTemporalValue to) {
    final ZoneId zone = from instanceof CypherDateTime f ? f.getValue().getZone() :
        to instanceof CypherDateTime t ? t.getValue().getZone() : null;
    if (zone == null)
      return new Temporal[] { extractDateTime(from), extractDateTime(to) };
    return new Temporal[] { toZonedDateTime(from, zone, null), toZonedDateTime(to, zone, null) };
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

  private static Instant toInstant(final CypherTemporalValue val) {
    if (val instanceof CypherDateTime)
      return ((CypherDateTime) val).getValue().toInstant();
    if (val instanceof CypherDate)
      return ((CypherDate) val).getValue().atStartOfDay(ZoneOffset.UTC).toInstant();
    if (val instanceof CypherLocalDateTime)
      return ((CypherLocalDateTime) val).getValue().atZone(ZoneOffset.UTC).toInstant();
    if (val instanceof CypherTime)
      return ((CypherTime) val).getValue().atDate(LocalDate.of(0, 1, 1)).toInstant();
    if (val instanceof CypherLocalTime)
      return LocalDateTime.of(LocalDate.of(0, 1, 1), ((CypherLocalTime) val).getValue()).atZone(ZoneOffset.UTC).toInstant();
    throw new IllegalArgumentException("Cannot convert to Instant: " + val.getClass().getSimpleName());
  }
}
