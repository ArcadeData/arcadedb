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

import com.arcadedb.database.Document;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Type;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.*;
import java.time.temporal.ChronoUnit;
import java.time.temporal.IsoFields;
import java.time.temporal.Temporal;
import java.time.temporal.WeekFields;
import java.util.ArrayList;
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

  private static final Pattern COMPACT_OFFSET = Pattern.compile(
      "([+-])(\\d{2})(\\d{2})(?!:)");

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
    final LocalDateTime fromDT = resolveDateTime(from, to);
    final LocalDateTime toDT = resolveDateTime(to, from);
    long totalMonths = fromDT.toLocalDate().until(toDT.toLocalDate()).toTotalMonths();
    // Adjust for time-of-day: if we overestimate months, the remainder would have wrong sign
    if (totalMonths != 0) {
      final LocalDateTime afterMonths = fromDT.plusMonths(totalMonths);
      final Duration remainder = Duration.between(afterMonths, toDT);
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
      final Duration remainder = Duration.between(afterDays, toDT);
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

    // Both have date components: compute full duration
    final LocalDateTime fromDT = resolveDateTime(from, to);
    final LocalDateTime toDT = resolveDateTime(to, from);

    // Split exactly as Neo4j does: whole months first, then whole days after them, then the clock remainder, each unit
    // truncated towards zero on the full date-time. Every component therefore carries the same sign, and
    // from.plusMonths(m).plusDays(d).plusSeconds(s) - the order ArithmeticExpression applies a duration in - lands
    // exactly on `to`. Borrowing from a Period with a fixed month length did not, for any month that was not 30 days
    // long (issue #8386).
    final long months = ChronoUnit.MONTHS.between(fromDT, toDT);
    final LocalDateTime afterMonths = fromDT.plusMonths(months);
    final long days = ChronoUnit.DAYS.between(afterMonths, toDT);
    final Duration clockDuration = Duration.between(afterMonths.plusDays(days), toDT);

    return new CypherDuration(months, days, clockDuration.getSeconds(), clockDuration.getNano());
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
   */
  public static int computeNanos(final Map<String, Object> map, final int defaultNanos) {
    final Object ms = map.get("millisecond");
    final Object us = map.get("microsecond");
    final Object ns = map.get("nanosecond");
    if (ms == null && us == null && ns == null)
      return defaultNanos;

    // Preserve unspecified higher-order portions from defaultNanos.
    // E.g. truncate('millisecond', t, {nanosecond: 2}) should keep the millisecond portion.
    long nanos = 0;
    if (ms != null)
      nanos += subSecondField("Millisecond", ms, 1_000L) * 1_000_000L;
    else
      nanos += (defaultNanos / 1_000_000) * 1_000_000L;
    if (us != null)
      nanos += subSecondField("Microsecond", us, ms != null ? 1_000L : 1_000_000L) * 1_000L;
    else
      nanos += ((defaultNanos % 1_000_000) / 1_000) * 1_000L;
    if (ns != null)
      nanos += subSecondField("Nanosecond", ns, us != null ? 1_000L : ms != null ? 1_000_000L : 1_000_000_000L);
    else
      nanos += defaultNanos % 1_000;
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

    return value;
  }

  /**
   * Reads {@code propertyName} from {@code document} as a Cypher value: a native temporal is wrapped into its Cypher
   * type so component access ({@code dur.seconds}, {@code t.hour}) and temporal comparison work. A String is never
   * interpreted: it is a String unless the schema declares the property with a temporal type, in which case the
   * deserializer has already converted it (issue #8572).
   */
  public static Object convertFromStorage(final Document document, final String propertyName) {
    final Object value = document.get(propertyName);
    // A ZonedDateTime on an undeclared (or ZONED_DATETIME) property is the native type that kept its zone, not a
    // DATETIME read back in the configured Java class, so it is a real zoned value (issue #8572)
    if (value instanceof ZonedDateTime zoned && keepsZone(document, propertyName))
      return new CypherDateTime(zoned);
    return convertFromStorage(value);
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
    if (value instanceof Temporal || value instanceof Date) {
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
