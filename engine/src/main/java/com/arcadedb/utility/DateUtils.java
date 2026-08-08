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

import com.arcadedb.database.Database;
import com.arcadedb.exception.SerializationException;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.BinaryTypes;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAccessor;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class DateUtils {
  public static final  String                                       DATE_TIME_ISO_8601_FORMAT = "yyyy-MM-dd'T'HH:mm:ssZ";
  public static final  long                                         MS_IN_A_DAY               = 24 * 60 * 60 * 1000L; // 86_400_000
  private static final ZoneId                                       UTC_ZONE_ID               = ZoneId.of("UTC");
  private static       ConcurrentHashMap<String, DateTimeFormatter> CACHED_FORMATTERS         = new ConcurrentHashMap<>();

  public static Object dateTime(final Database database, final long timestamp, final ChronoUnit sourcePrecision,
      final Class dateTimeImplementation, final ChronoUnit destinationPrecision) {
    final long convertedTimestamp = convertTimestamp(timestamp, sourcePrecision, destinationPrecision);

    final Object value;
    if (dateTimeImplementation.equals(Date.class)) {
      if (destinationPrecision == ChronoUnit.MICROS || destinationPrecision == ChronoUnit.NANOS)
        throw new IllegalArgumentException(
            "java.util.Date implementation cannot handle datetime with precision " + destinationPrecision);
      value = new Date(convertedTimestamp);
    } else if (dateTimeImplementation.equals(Calendar.class)) {
      if (destinationPrecision == ChronoUnit.MICROS || destinationPrecision == ChronoUnit.NANOS)
        throw new IllegalArgumentException(
            "java.util.Calendar implementation cannot handle datetime with precision " + destinationPrecision);
      value = Calendar.getInstance(database.getSchema().getTimeZone());
      ((Calendar) value).setTimeInMillis(convertedTimestamp);
    } else if (dateTimeImplementation.equals(LocalDateTime.class)) {
      if (destinationPrecision.equals(ChronoUnit.SECONDS))
        value = LocalDateTime.ofInstant(Instant.ofEpochSecond(convertedTimestamp), UTC_ZONE_ID);
      else if (destinationPrecision.equals(ChronoUnit.MILLIS))
        value = LocalDateTime.ofInstant(Instant.ofEpochMilli(convertedTimestamp), UTC_ZONE_ID);
      else if (destinationPrecision.equals(ChronoUnit.MICROS))
        value = LocalDateTime.ofInstant(Instant.ofEpochSecond(TimeUnit.MICROSECONDS.toSeconds(convertedTimestamp),
            TimeUnit.MICROSECONDS.toNanos(Math.floorMod(convertedTimestamp, TimeUnit.SECONDS.toMicros(1)))), UTC_ZONE_ID);
      else if (destinationPrecision.equals(ChronoUnit.NANOS))
        value = LocalDateTime.ofInstant(Instant.ofEpochSecond(0L, convertedTimestamp), UTC_ZONE_ID);
      else
        value = 0;
    } else if (dateTimeImplementation.equals(ZonedDateTime.class)) {
      if (destinationPrecision.equals(ChronoUnit.SECONDS))
        value = ZonedDateTime.ofInstant(Instant.ofEpochSecond(convertedTimestamp), UTC_ZONE_ID);
      else if (destinationPrecision.equals(ChronoUnit.MILLIS))
        value = ZonedDateTime.ofInstant(Instant.ofEpochMilli(convertedTimestamp), UTC_ZONE_ID);
      else if (destinationPrecision.equals(ChronoUnit.MICROS))
        value = ZonedDateTime.ofInstant(Instant.ofEpochSecond(TimeUnit.MICROSECONDS.toSeconds(convertedTimestamp),
            TimeUnit.MICROSECONDS.toNanos(Math.floorMod(convertedTimestamp, TimeUnit.SECONDS.toMicros(1)))), UTC_ZONE_ID);
      else if (destinationPrecision.equals(ChronoUnit.NANOS))
        value = ZonedDateTime.ofInstant(Instant.ofEpochSecond(0L, convertedTimestamp), UTC_ZONE_ID);
      else
        value = 0;
    } else if (dateTimeImplementation.equals(Instant.class)) {
      if (destinationPrecision.equals(ChronoUnit.SECONDS))
        value = Instant.ofEpochSecond(convertedTimestamp);
      else if (destinationPrecision.equals(ChronoUnit.MILLIS))
        value = Instant.ofEpochMilli(convertedTimestamp);
      else if (destinationPrecision.equals(ChronoUnit.MICROS))
        value = Instant.ofEpochSecond(TimeUnit.MICROSECONDS.toSeconds(convertedTimestamp),
            TimeUnit.MICROSECONDS.toNanos(Math.floorMod(convertedTimestamp, TimeUnit.SECONDS.toMicros(1))));
      else if (destinationPrecision.equals(ChronoUnit.NANOS))
        value = Instant.ofEpochSecond(0L, convertedTimestamp);
      else
        value = 0;
    } else
      throw new SerializationException(
          "Error on deserialize datetime. Configured class '" + dateTimeImplementation + "' is not supported");
    return value;
  }

  public static Object date(final Database database, final long timestamp, final Class dateImplementation) {
    final Object value;
    if (dateImplementation.equals(Date.class))
      value = new Date(timestamp * MS_IN_A_DAY);
    else if (dateImplementation.equals(Calendar.class)) {
      value = Calendar.getInstance(database.getSchema().getTimeZone());
      ((Calendar) value).setTimeInMillis(timestamp * MS_IN_A_DAY);
    } else if (dateImplementation.equals(LocalDate.class)) {
      value = LocalDate.ofEpochDay(timestamp);
    } else if (dateImplementation.equals(LocalDateTime.class)) {
      value = LocalDateTime.ofEpochSecond(timestamp / 1_000, (int) ((timestamp % 1_000) * 1_000_000), ZoneOffset.UTC);
    } else
      throw new SerializationException("Error on deserialize date. Configured class '" + dateImplementation + "' is not supported");
    return value;
  }

  /**
   * Converts a temporal value to the number of days since the epoch, the canonical encoding for the
   * {@link com.arcadedb.schema.Type#DATE} type both on disk and on the remote JSON wire. Mirrors the
   * {@code TYPE_DATE} branch of the binary serializer so the remote path and the embedded binary path
   * agree: a {@code java.util.Date} written to a DATE property over the remote client used to be
   * serialized as epoch milliseconds, which the server then decoded as epoch days, silently losing the
   * value (issue #4601).
   */
  public static Long dateToEpochDays(final Object value) {
    if (value == null)
      return null;
    else if (value instanceof LocalDate localDate)
      return localDate.toEpochDay();
    else if (value instanceof LocalDateTime localDateTime)
      return localDateTime.toLocalDate().toEpochDay();
    else if (value instanceof Date date)
      return date.getTime() / MS_IN_A_DAY;
    else if (value instanceof Calendar calendar)
      return calendar.getTimeInMillis() / MS_IN_A_DAY;
    else if (value instanceof Instant instant)
      return instant.atZone(UTC_ZONE_ID).toLocalDate().toEpochDay();
    else if (value instanceof ZonedDateTime zonedDateTime)
      return zonedDateTime.toLocalDate().toEpochDay();
    else if (value instanceof Number number)
      return number.longValue();
    else
      throw new IllegalArgumentException("Cannot convert value of type '" + value.getClass() + "' to epoch days for a DATE value");
  }

  public static Long dateTimeToTimestamp(final Object value, final ChronoUnit precisionToUse) {
    return dateTimeToTimestamp(null, value, precisionToUse);
  }

  /**
   * Database-aware overload: when {@code value} is a {@link String}, the schema-configured
   * date/time formats are tried as fallbacks after ISO-8601, mirroring the vertex path in
   * {@code Type.convert}. Used by the binary serializer (and thus by GraphBatch's edge bulk
   * path) so that vertex and edge ingestion accept the same set of inputs - issue #4142.
   */
  public static Long dateTimeToTimestamp(final Database database, final Object value, final ChronoUnit precisionToUse) {
    if (value == null)
      return null;

    final long timestamp;
    if (value instanceof Date date) {
      // WRITE MILLISECONDS
      timestamp = convertTimestamp(date.getTime(), ChronoUnit.MILLIS, precisionToUse);
    } else if (value instanceof Calendar calendar)
      // WRITE MILLISECONDS
      timestamp = convertTimestamp(calendar.getTimeInMillis(), ChronoUnit.MILLIS, precisionToUse);
    else if (value instanceof LocalDateTime localDateTime) {
      if (precisionToUse.equals(ChronoUnit.SECONDS))
        timestamp = localDateTime.toInstant(ZoneOffset.UTC).getEpochSecond();
      else if (precisionToUse.equals(ChronoUnit.MILLIS))
        timestamp =
            TimeUnit.MILLISECONDS.convert(localDateTime.toEpochSecond(ZoneOffset.UTC), TimeUnit.SECONDS) + localDateTime.getLong(
                ChronoField.MILLI_OF_SECOND);
      else if (precisionToUse.equals(ChronoUnit.MICROS))
        timestamp =
            TimeUnit.MICROSECONDS.convert(localDateTime.toEpochSecond(ZoneOffset.UTC), TimeUnit.SECONDS) + (localDateTime.getNano()
                / 1000);
      else if (precisionToUse.equals(ChronoUnit.NANOS))
        timestamp = addNanosClampingOverflow(TimeUnit.NANOSECONDS.convert(localDateTime.toEpochSecond(ZoneOffset.UTC), TimeUnit.SECONDS),
            localDateTime.getNano());
      else
        // NOT SUPPORTED
        timestamp = 0;
    } else if (value instanceof LocalDate localDate) {
      if (precisionToUse.equals(ChronoUnit.SECONDS))
        timestamp = localDate.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli() / 1_000L;
      else if (precisionToUse.equals(ChronoUnit.MILLIS))
        timestamp = localDate.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
      else if (precisionToUse.equals(ChronoUnit.MICROS))
        // TimeUnit.convert() SATURATES to Long.MAX_VALUE/MIN_VALUE on overflow instead of silently wrapping,
        // unlike a raw `* 1_000_000L` multiplication (issue #5625).
        timestamp = TimeUnit.MICROSECONDS.convert(localDate.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli(), TimeUnit.MILLISECONDS);
      else if (precisionToUse.equals(ChronoUnit.NANOS))
        timestamp = TimeUnit.NANOSECONDS.convert(localDate.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli(), TimeUnit.MILLISECONDS);
      else
        // NOT SUPPORTED
        timestamp = 0;
    } else if (value instanceof ZonedDateTime zonedDateTime) {
      if (precisionToUse.equals(ChronoUnit.SECONDS))
        timestamp = zonedDateTime.toInstant().getEpochSecond();
      else if (precisionToUse.equals(ChronoUnit.MILLIS))
        timestamp = zonedDateTime.toInstant().toEpochMilli();
      else if (precisionToUse.equals(ChronoUnit.MICROS))
        timestamp =
            TimeUnit.MICROSECONDS.convert(zonedDateTime.toEpochSecond(), TimeUnit.SECONDS) + (zonedDateTime.getNano() / 1000);
      else if (precisionToUse.equals(ChronoUnit.NANOS))
        timestamp = addNanosClampingOverflow(TimeUnit.NANOSECONDS.convert(zonedDateTime.toEpochSecond(), TimeUnit.SECONDS),
            zonedDateTime.getNano());
      else
        // NOT SUPPORTED
        timestamp = 0;
    } else if (value instanceof OffsetDateTime offsetDateTime) {
      if (precisionToUse.equals(ChronoUnit.SECONDS))
        timestamp = offsetDateTime.toInstant().getEpochSecond();
      else if (precisionToUse.equals(ChronoUnit.MILLIS))
        timestamp = offsetDateTime.toInstant().toEpochMilli();
      else if (precisionToUse.equals(ChronoUnit.MICROS))
        timestamp =
            TimeUnit.MICROSECONDS.convert(offsetDateTime.toEpochSecond(), TimeUnit.SECONDS) + (offsetDateTime.getNano() / 1000);
      else if (precisionToUse.equals(ChronoUnit.NANOS))
        timestamp = addNanosClampingOverflow(TimeUnit.NANOSECONDS.convert(offsetDateTime.toEpochSecond(), TimeUnit.SECONDS),
            offsetDateTime.getNano());
      else
        // NOT SUPPORTED
        timestamp = 0;
    } else if (value instanceof Instant instant) {
      if (precisionToUse.equals(ChronoUnit.SECONDS))
        timestamp = instant.getEpochSecond();
      else if (precisionToUse.equals(ChronoUnit.MILLIS))
        timestamp = instant.toEpochMilli();
      else if (precisionToUse.equals(ChronoUnit.MICROS))
        timestamp = TimeUnit.MICROSECONDS.convert(instant.getEpochSecond(), TimeUnit.SECONDS) + (instant.getNano() / 1000);
      else if (precisionToUse.equals(ChronoUnit.NANOS))
        timestamp = addNanosClampingOverflow(TimeUnit.NANOSECONDS.convert(instant.getEpochSecond(), TimeUnit.SECONDS), instant.getNano());
      else
        // NOT SUPPORTED
        timestamp = 0;
    } else if (value instanceof Number number)
      timestamp = number.longValue();
    else if (value instanceof String string) {
      if (FileUtils.isLong(string))
        timestamp = Long.parseLong(string);
      else
        return dateTimeToTimestamp(database, parseIsoDateTime(database, string), precisionToUse);
    } else
      // UNSUPPORTED
      return null;

    return timestamp;
  }

  /**
   * Like {@link #dateTimeToTimestamp(Object, ChronoUnit)}, except a bare numeric {@link String} has its own epoch
   * precision inferred from its digit count and converted to {@code precisionToUse}, instead of its raw digits
   * being assumed to already be at {@code precisionToUse}.
   * <p>
   * This distinction matters because {@code dateTimeToTimestamp}'s numeric-string handling is shared by two
   * semantically different callers: {@link com.arcadedb.serializer.BinaryComparator}'s {@code TYPE_DATE}/
   * {@code TYPE_DATETIME*} branch, where the string represents an independent absolute moment being compared
   * against another date/time value - use this method there - and {@code MathExpression}'s date {@code +}/{@code -}
   * arithmetic, where a numeric operand represents a raw duration/offset count to add at the date's own precision
   * (e.g. {@code date + '10'} meaning "10 units of the date's precision"), for which digit count carries no
   * meaning and {@code dateTimeToTimestamp} must keep its original raw-digits behavior.
   * <p>
   * Without this, a numeric string holding a <em>different</em> precision than whatever the comparison settled on
   * (e.g. a nanos-epoch string compared against a {@link Date}/{@link Calendar} operand, which forces
   * {@code MILLIS}) was misinterpreted by orders of magnitude instead of being converted (issue #5956).
   */
  public static Long dateTimeToTimestampInferringStringPrecision(final Object value, final ChronoUnit precisionToUse) {
    if (value instanceof String string && FileUtils.isLong(string)) {
      final long rawValue = Long.parseLong(string);
      return convertTimestamp(rawValue, inferEpochPrecision(rawValue), precisionToUse);
    }
    return dateTimeToTimestamp(value, precisionToUse);
  }

  /**
   * Infers the epoch precision a bare numeric string most likely represents, from its digit count: for a
   * present-day moment, an epoch value grows by roughly 3 digits per finer precision step (~10 digits for
   * seconds, ~13 for millis, ~16 for micros, ~19 for nanos).
   * <p>
   * This cannot distinguish a genuinely small value from the coarser unit it also matches digit-for-digit at each
   * boundary - e.g. a millis timestamp within the first ~3 months of 1970, or a micros/nanos timestamp within the
   * first ~10 seconds of 1970 - the coarser unit wins every such tie, since a near-zero epoch value is far more
   * common as a duration/offset than as an actual date that close to the epoch.
   */
  private static ChronoUnit inferEpochPrecision(final long epochValue) {
    final int digits = digitCount(epochValue);
    if (digits <= 10)
      return ChronoUnit.SECONDS;
    else if (digits <= 13)
      return ChronoUnit.MILLIS;
    else if (digits <= 16)
      return ChronoUnit.MICROS;
    return ChronoUnit.NANOS;
  }

  /**
   * Counts the decimal digits of a non-negative value without the {@code String} allocation a
   * {@code Long.toString(value).length()} round trip would cost - {@link #inferEpochPrecision(long)} runs on the
   * {@code BinaryComparator} hot path for every date/numeric-string comparison. {@code value} is always
   * non-negative here: its only caller receives it from {@code Long.parseLong()} on a string that already passed
   * {@code FileUtils.isLong()}, which accepts only the digits {@code 0-9} (no sign).
   */
  private static int digitCount(long value) {
    int digits = 1;
    while (value >= 10) {
      value /= 10;
      digits++;
    }
    return digits;
  }

  /**
   * Adds a non-negative sub-second nanosecond fraction (0..999,999,999, as returned by {@code getNano()}) to an
   * already-computed NANOS-precision epoch value without silently wrapping past {@link Long#MAX_VALUE}.
   * <p>
   * {@code epochSecondsAsNanos} is normally {@code TimeUnit.NANOSECONDS.convert(epochSeconds, SECONDS)}, which
   * itself saturates to {@link Long#MAX_VALUE} for any date far enough in the future that its NANOS-precision
   * epoch timestamp does not fit a signed 64-bit long (roughly beyond the year 2262). Without this guard, adding
   * the nanosecond fraction on top of that already-saturated value overflows a second time and wraps around to a
   * large NEGATIVE number, silently inverting chronological order for such dates - e.g. the far-future sentinel
   * date pattern (`9999-12-31`, `2499-12-31 23:59:59.999`, ...) commonly used to mean "no expiration" then
   * compares as LESS than an ordinary near-present date. This broke every {@code P.lt/lte/gt/gte} Gremlin
   * predicate and {@code order()} step touching such a value, because {@code Compare}'s biPredicates route
   * through {@code org.apache.tinkerpop.gremlin.util.GremlinValueComparator}, which always normalises date/time
   * operands to {@link ChronoUnit#NANOS} regardless of their actual precision (issue #5625).
   * <p>
   * There is no matching underflow risk: the nanosecond fraction is always non-negative, so adding it to an
   * already Long.MIN_VALUE-saturated {@code epochSecondsAsNanos} (from a very ancient date) only moves the
   * result toward zero.
   */
  private static long addNanosClampingOverflow(final long epochSecondsAsNanos, final int nanoFraction) {
    return epochSecondsAsNanos >= 0 && Long.MAX_VALUE - epochSecondsAsNanos < nanoFraction ?
        Long.MAX_VALUE : epochSecondsAsNanos + nanoFraction;
  }

  /**
   * Mirrors the vertex string-to-{@link LocalDateTime} fallback chain in {@code Type.convert}
   * so the GraphBatch edge bulk path (which bypasses {@code Type.convert}) accepts the same
   * set of inputs - issue #4142. Tries, in order:
   * <ol>
   *   <li>{@link LocalDateTime#parse(CharSequence)} (ISO without zone);</li>
   *   <li>{@link ZonedDateTime#parse(CharSequence)} for ISO inputs ending in {@code Z} or
   *   with a {@code ±HH:mm} offset - the resulting instant is rebased onto the database's
   *   configured zone (default {@link ZoneId#systemDefault()}) before stripping the offset,
   *   so the stored wall-clock follows the database's locale rather than the input's;</li>
   *   <li>the schema's {@code dateTimeFormat};</li>
   *   <li>the schema's {@code dateFormat}.</li>
   * </ol>
   * When {@code database} is {@code null} only the ISO formats are tried and offset-bearing
   * inputs keep their wall-clock without rebasing, matching legacy parsing behavior in
   * scopes without a schema.
   */
  private static LocalDateTime parseIsoDateTime(final Database database, final String string) {
    try {
      return LocalDateTime.parse(string);
    } catch (final DateTimeParseException e) {
      try {
        final ZonedDateTime parsed = ZonedDateTime.parse(string);
        if (database != null) {
          final ZoneId zoneId = database.getSchema().getZoneId();
          if (zoneId != null)
            return parsed.withZoneSameInstant(zoneId).toLocalDateTime();
        }
        return parsed.toLocalDateTime();
      } catch (final DateTimeParseException e2) {
        if (database != null) {
          try {
            return LocalDateTime.parse(string,
                DateTimeFormatter.ofPattern(database.getSchema().getDateTimeFormat()));
          } catch (final DateTimeParseException ignore) {
            return LocalDateTime.parse(string,
                DateTimeFormatter.ofPattern(database.getSchema().getDateFormat()));
          }
        }
        throw e2;
      }
    }
  }

  public static ChronoUnit parsePrecision(final String precision) {
    return switch (precision.toLowerCase(Locale.ENGLISH)) {
      case "year", "years" -> ChronoUnit.YEARS;
      case "month", "months" -> ChronoUnit.MONTHS;
      case "week", "weeks" -> ChronoUnit.WEEKS;
      case "day", "days" -> ChronoUnit.DAYS;
      case "hour", "hours" -> ChronoUnit.HOURS;
      case "minute", "minutes" -> ChronoUnit.MINUTES;
      case "second", "seconds" -> ChronoUnit.SECONDS;
      case "millisecond", "milliseconds", "millis" -> ChronoUnit.MILLIS;
      case "microsecond", "microseconds", "micros" -> ChronoUnit.MICROS;
      case "nanosecond", "nanoseconds", "nanos" -> ChronoUnit.NANOS;
      default -> throw new SerializationException("Unsupported datetime precision '" + precision + "'");
    };
  }

  public static ChronoUnit getPrecision(final int nanos) {
    if (nanos % 1_000_000_000 == 0)
      return ChronoUnit.SECONDS;
    if (nanos % 1_000_000 == 0)
      return ChronoUnit.MILLIS;
    if (nanos % 1_000 == 0)
      return ChronoUnit.MICROS;
    else
      return ChronoUnit.NANOS;
  }

  /**
   * Widening conversions (e.g. SECONDS to NANOS) delegate to {@link TimeUnit#convert}, which saturates to
   * {@link Long#MAX_VALUE}/{@link Long#MIN_VALUE} on overflow instead of silently wrapping the way a raw
   * multiplication would - the same reasoning already applied to {@code LocalDate}'s conversion a few lines up
   * in {@link #dateTimeToTimestamp(Database, Object, ChronoUnit)} (issue #5625). This matters more directly since
   * {@link #dateTimeToTimestampInferringStringPrecision} started routing bare numeric strings through a widening
   * conversion here (issue #5956 review follow-up): a MICROS-bucketed 16-digit string above roughly
   * {@code Long.MAX_VALUE / 1000} widened to NANOS by {@code BinaryComparator.compareTo} used to wrap to a large
   * negative number and silently invert the comparison.
   */
  public static long convertTimestamp(final long timestamp, final ChronoUnit from, final ChronoUnit to) {
    if (from == to)
      return timestamp;
    return toTimeUnit(to).convert(timestamp, toTimeUnit(from));
  }

  private static TimeUnit toTimeUnit(final ChronoUnit unit) {
    return switch (unit) {
      case SECONDS -> TimeUnit.SECONDS;
      case MILLIS -> TimeUnit.MILLISECONDS;
      case MICROS -> TimeUnit.MICROSECONDS;
      case NANOS -> TimeUnit.NANOSECONDS;
      default -> throw new IllegalArgumentException("Not supported conversion unit '" + unit + "'");
    };
  }

  public static byte getBestBinaryTypeForPrecision(final ChronoUnit precision) {
    if (precision == ChronoUnit.SECONDS)
      return BinaryTypes.TYPE_DATETIME_SECOND;
    else if (precision == ChronoUnit.MILLIS)
      return BinaryTypes.TYPE_DATETIME;
    else if (precision == ChronoUnit.MICROS)
      return BinaryTypes.TYPE_DATETIME_MICROS;
    else if (precision == ChronoUnit.NANOS)
      return BinaryTypes.TYPE_DATETIME_NANOS;
    throw new IllegalArgumentException("Not supported precision '" + precision + "'");
  }

  public static final ChronoUnit getPrecisionFromType(final Type type) {
    switch (type) {
    case DATETIME_SECOND:
      return ChronoUnit.SECONDS;
    case DATETIME:
      return ChronoUnit.MILLIS;
    case DATETIME_MICROS:
      return ChronoUnit.MICROS;
    case DATETIME_NANOS:
      return ChronoUnit.NANOS;
    default:
      throw new IllegalArgumentException("Illegal date type from type " + type);
    }
  }

  public static final ChronoUnit getPrecisionFromBinaryType(final byte type) {
    switch (type) {
    case BinaryTypes.TYPE_DATETIME_SECOND:
      return ChronoUnit.SECONDS;
    case BinaryTypes.TYPE_DATETIME:
      return ChronoUnit.MILLIS;
    case BinaryTypes.TYPE_DATETIME_MICROS:
      return ChronoUnit.MICROS;
    case BinaryTypes.TYPE_DATETIME_NANOS:
      return ChronoUnit.NANOS;
    default:
      throw new IllegalArgumentException("Illegal date type from binary type " + type);
    }
  }

  public static int getNanos(final Object obj) {
    if (obj == null)
      throw new IllegalArgumentException("Object is null");
    else if (obj instanceof LocalDateTime time)
      return time.getNano();
    else if (obj instanceof ZonedDateTime time)
      return time.getNano();
    else if (obj instanceof Instant instant)
      return instant.getNano();
    throw new IllegalArgumentException("Object of class '" + obj.getClass() + "' is not supported");
  }

  public static boolean isDate(final Object obj) {
    if (obj == null)
      return false;
    return obj instanceof Date || obj instanceof Calendar || obj instanceof LocalDate || obj instanceof LocalDateTime
        || obj instanceof ZonedDateTime || obj instanceof OffsetDateTime || obj instanceof Instant;
  }

  public static ChronoUnit getHigherPrecision(final Object... objs) {
    if (objs == null || objs.length == 0)
      return null;

    ChronoUnit highestPrecision = ChronoUnit.MILLIS;
    for (int i = 0; i < objs.length; i++) {
      final Object obj = objs[i];
      final ChronoUnit precision;
      if (obj instanceof Date || obj instanceof Calendar)
        precision = ChronoUnit.MILLIS;
      else if (obj instanceof LocalDateTime || obj instanceof ZonedDateTime || obj instanceof Instant)
        precision = getPrecision(getNanos(obj));
      else
        continue;

      if (precision.compareTo(highestPrecision) < 0)
        highestPrecision = precision;
    }
    return highestPrecision;
  }

  public static LocalDateTime millisToLocalDateTime(final long millis, final String timeZone) {
    if (timeZone == null)
      return Instant.ofEpochMilli(millis).atZone(ZoneId.systemDefault()).toLocalDateTime();
    return Instant.ofEpochMilli(millis).atZone(ZoneId.of(timeZone)).toLocalDateTime();
  }

  public static LocalDate millisToLocalDate(final long millis) {
    return LocalDate.ofInstant(Instant.ofEpochMilli(millis), ZoneId.systemDefault());
  }

  public static String format(final Object obj, final String format) {
    return format(obj, format, null);
  }

  public static String format(final Object obj, final String format, final String timeZone) {
    if (obj instanceof Number number)
      return getFormatter(format).format(millisToLocalDateTime(number.longValue(), timeZone));
    else if (obj instanceof Date date)
      return getFormatter(format).format(millisToLocalDateTime(date.getTime(), timeZone));
    else if (obj instanceof Calendar calendar)
      return getFormatter(format).format(millisToLocalDateTime(calendar.getTimeInMillis(), timeZone));
    else if (obj instanceof LocalDateTime time) {
      if (timeZone != null)
        return time.atZone(ZoneId.of(timeZone)).format(getFormatter(format));
      else
        return getFormatter(format).format(time);
    } else if (obj instanceof Instant instant)
      // An Instant carries no date/time fields on its own, so a pattern like `yyyy-MM-dd HH:mm:ss`
      // throws UnsupportedTemporalTypeException unless it is first anchored to a zone. UTC is the
      // anchor used everywhere else in this class (see dateTime() and dateTimeToTimestamp()), so
      // `arcadedb.dateTimeImplementation=java.time.Instant` renders exactly like LocalDateTime.
      return getFormatter(format).format(LocalDateTime.ofInstant(instant, timeZone != null ? ZoneId.of(timeZone) : UTC_ZONE_ID));
    else if (obj instanceof TemporalAccessor accessor)
      return getFormatter(format).format(accessor);
    return null;
  }

  public static Object parse(final String text, final String format) {
    return LocalDateTime.parse(text, getFormatter(format));
  }

  public static DateTimeFormatter getFormatter(final String format) {
    return CACHED_FORMATTERS.computeIfAbsent(format,
        f -> new DateTimeFormatterBuilder().appendPattern(f).parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
            .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0).parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0).toFormatter());
  }

  public static Object getDate(final Object date, final Class dateImplementation) {
    if (date == null)
      return null;

    if (date.getClass().equals(dateImplementation))
      return date;

    final long timestamp = DateUtils.dateTimeToTimestamp(date, ChronoUnit.MILLIS);

    if (dateImplementation.equals(Date.class))
      return new Date(timestamp);
    else if (dateImplementation.equals(Calendar.class)) {
      final Calendar cal = Calendar.getInstance();
      cal.setTimeInMillis(timestamp);
      return cal;
    } else if (dateImplementation.equals(LocalDate.class))
      return LocalDate.ofEpochDay(timestamp / DateUtils.MS_IN_A_DAY);
    else if (dateImplementation.equals(LocalDateTime.class))
      return LocalDateTime.ofEpochSecond(timestamp / 1_000, (int) ((timestamp % 1_000) * 1_000_000), ZoneOffset.UTC);
    else
      return date;
  }

  public static String formatElapsed(final long ms) {
    if (ms < 1000)
      return ms + " ms";

    final long seconds = ms / 1000;
    if (seconds < 60)
      return seconds + " seconds";

    final float minutes = seconds / 60F;
    if (minutes < 60F)
      return "%.1f minutes".formatted(minutes);

    final float hours = minutes / 60F;
    if (hours < 24F)
      return "%.1f hours".formatted(hours);

    final float days = hours / 24F;
    if (days < 30F)
      return "%.1f days".formatted(days);

    final float months = days / 30F;
    if (months < 12F)
      return "%.1f months".formatted(months);

    return "%.1f years".formatted(months / 12F);
  }

  public static boolean areSameDay(final Date d1, final Date d2) {
    final Calendar c1 = Calendar.getInstance();
    c1.setTime(d1);
    final Calendar c2 = Calendar.getInstance();
    c2.setTime(d2);
    return c1.get(Calendar.YEAR) == c2.get(Calendar.YEAR) && c1.get(Calendar.DAY_OF_YEAR) == c2.get(Calendar.DAY_OF_YEAR);
  }
}
