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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.exception.SerializationException;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.BinaryTypes;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.text.ParsePosition;
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
import java.time.temporal.Temporal;
import java.time.temporal.TemporalAccessor;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class DateUtils {
  public static final  String                                       DATE_TIME_ISO_8601_FORMAT = "yyyy-MM-dd'T'HH:mm:ssZ";
  public static final  long                                         MS_IN_A_DAY               = 24 * 60 * 60 * 1000L; // 86_400_000
  /**
   * The timestamp in every generated file name (backup and export archives). Pinned to {@link Locale#ROOT} so the name
   * is Gregorian with ASCII digits whatever the JVM's default locale: the server's backup retention parses these names
   * back and orders them, and a locale-formatted name was either misdated (th-TH's Buddhist year) or unreadable
   * (ar-EG's digits) to it (issue #8301). One shared constant so no generator can drift from the others.
   */
  public static final  DateTimeFormatter                            FILE_NAME_TIMESTAMP         = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmssSSS", Locale.ROOT);
  /**
   * {@link #FILE_NAME_TIMESTAMP} at second precision, for log and profiler files rotated by name order.
   */
  public static final  DateTimeFormatter                            FILE_NAME_TIMESTAMP_SECONDS = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss", Locale.ROOT);
  private static final ZoneId                                       UTC_ZONE_ID               = ZoneId.of("UTC");
  private static final ConcurrentHashMap<String, DateTimeFormatter> CACHED_FORMATTERS         = new ConcurrentHashMap<>();
  /**
   * Ceiling on {@link #CACHED_FORMATTERS}. The cache exists for the handful of patterns a schema and the built-in
   * formats produce, which is why an unbounded map was fine while every key came from inside. The SQL {@code date()}
   * function passes the CALLER's format straight through, so a client sending distinct patterns could grow it without
   * bound (issue #6388). Past the ceiling a formatter is still built and returned - it is simply not remembered.
   */
  private static final int                                          MAX_CACHED_FORMATTERS     = 1_000;

  /**
   * The built-in patterns, so a schema still on them can skip a walk whose answer {@link #SPACE_SEPARATED_DATE_TIME}
   * already gives. Read from {@link GlobalConfiguration} rather than restated, so the two cannot drift apart.
   */
  private static final String                                       DEFAULT_DATE_TIME_FORMAT  = (String) GlobalConfiguration.DATE_TIME_FORMAT.getDefValue();
  private static final String                                       DEFAULT_DATE_FORMAT       = (String) GlobalConfiguration.DATE_FORMAT.getDefValue();

  /**
   * Last-resort parser for the SQL-timestamp spelling that none of the strict ISO formats accept: an ISO date, a
   * <em>space</em> separator, a time, and - the part that made issue #8090 a silent data loss - an arbitrary
   * fractional-second field. That is exactly what {@code psqlodbc} renders a bound timestamp in and what PostgreSQL
   * itself prints, so every PostgreSQL-wire client binding a timestamp against a sub-second-precision property used
   * to write nothing at all: no strict format matched, the schema's default {@code yyyy-MM-dd HH:mm:ss} left the
   * {@code .123456} unparsed, and the resulting exception degraded to {@code null}.
   * <p>
   * The whole time part, the seconds, the fraction and a trailing offset are each optional, and the offset is
   * accepted in all three widths PostgreSQL emits ({@code +01}, {@code +0100}, {@code +01:00}, plus {@code Z}), so
   * one formatter covers a bare {@code date}, a {@code timestamp} and a {@code timestamptz} rendering - which is
   * also why it subsumes the string-length guessing the no-database paths used to do. It is only ever consulted
   * after the strict ISO formats and after the schema's own patterns, so it can widen what is accepted but never
   * reinterpret a string that already parsed.
   * <p>
   * The fraction is nested INSIDE the optional seconds section, and the offset inside the optional time, rather than
   * beside them, so the grammar is exactly what the sentence above says: a fractional second only where there is a
   * second to qualify, and an offset only where there is a time for it to offset. A stray
   * {@code '2024-02-29 13:45.123456'} is refused rather than quietly read as 13:45:00.123456, and a bare
   * {@code '2024-02-29+01:00'} rather than as midnight in that offset.
   */
  private static final DateTimeFormatter                            SPACE_SEPARATED_DATE_TIME = new DateTimeFormatterBuilder()//
      .append(DateTimeFormatter.ISO_LOCAL_DATE)//
      .optionalStart()//
      .appendLiteral(' ')//
      .appendValue(ChronoField.HOUR_OF_DAY, 2)//
      .appendLiteral(':')//
      .appendValue(ChronoField.MINUTE_OF_HOUR, 2)//
      .optionalStart().appendLiteral(':').appendValue(ChronoField.SECOND_OF_MINUTE, 2)//
      .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true).optionalEnd()//
      .appendPattern("[XXX][XX][X]")// already optional: the brackets ARE the optional sections
      .optionalEnd()//
      .parseDefaulting(ChronoField.HOUR_OF_DAY, 0)//
      .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)//
      .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0)//
      .toFormatter(Locale.ENGLISH);

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
      // floorDiv/floorMod for the same reason as getDate() below: '%' keeps the dividend's sign, so a pre-epoch
      // value yields a NEGATIVE nanoOfSecond and LocalDateTime.ofEpochSecond rejects it outright (found in
      // review). NOTE, separately and deliberately left alone: every other arm here reads `timestamp` as a count
      // of DAYS, which is what a DATE stores, while this one divides it as if it were millis. That is a
      // pre-existing unit mismatch in this branch, not arithmetic, and changing it would change what a DATE
      // column configured with LocalDateTime answers - a decision, not a fix.
      value = LocalDateTime.ofEpochSecond(Math.floorDiv(timestamp, 1_000L),
          (int) (Math.floorMod(timestamp, 1_000L) * 1_000_000L), ZoneOffset.UTC);
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
    return switch (value) {
      case null -> null;
      case LocalDate localDate -> localDate.toEpochDay();
      case LocalDateTime localDateTime -> localDateTime.toLocalDate().toEpochDay();
      // floorDiv, NOT '/': integer division truncates TOWARDS ZERO, so any pre-epoch instant that is not exactly
      // midnight lands on the day AFTER the one it belongs to - 1969-12-31T12:00Z became day 0, 1970-01-01. Every
      // other arm here floors (LocalDate.toEpochDay and friends), and so does the DATE branch of BinarySerializer
      // since #7638, so these two were the ones left disagreeing with the rest (found in review).
      case Date date -> Math.floorDiv(date.getTime(), MS_IN_A_DAY);
      case Calendar calendar -> Math.floorDiv(calendar.getTimeInMillis(), MS_IN_A_DAY);
      case Instant instant -> instant.atZone(UTC_ZONE_ID).toLocalDate().toEpochDay();
      case ZonedDateTime zonedDateTime -> zonedDateTime.toLocalDate().toEpochDay();
      case Number number -> numberToEpochUnits(number);
      default ->
          throw new IllegalArgumentException("Cannot convert value of type '" + value.getClass() + "' to epoch days for a DATE value");
    };
  }

  /**
   * Reads a {@link Number} holding a count of epoch units (days, seconds, millis, micros or nanos - whatever the caller
   * reads it as) as a {@code long}, the one place every date/time conversion in this class turns a number into an
   * instant (issue #8216).
   * <p>
   * {@link Number#longValue()} is wrong for this in three ways, each of them silent:
   * <ul>
   * <li>a fractional value is truncated TOWARD ZERO, which for a pre-epoch instant is the LATER unit: {@code -1.5}
   * millis is an instant inside millisecond {@code -2}, not {@code -1}. The value is FLOORED instead - the unit that
   * contains the instant - which is also what {@link Instant#toEpochMilli()} and {@code Math.floorDiv} (#6824) do. A
   * fractional value is kept rather than refused because it is a real instant, and a common one: a client computing
   * millis as {@code time.time() * 1000} (Python) or {@code performance.timeOrigin + performance.now()} (JavaScript)
   * sends one;</li>
   * <li>{@code NaN} becomes {@code 0} - the epoch, a real instant that means something else (the #8152 trap), and
   * an infinity saturates to {@link Long#MAX_VALUE}/{@link Long#MIN_VALUE}: all three are refused;</li>
   * <li>a {@link BigInteger}/{@link BigDecimal} outside the {@code long} range WRAPS to an unrelated instant, and a
   * {@code double} outside it saturates: both are refused.</li>
   * </ul>
   *
   * @throws IllegalArgumentException when the number is not finite or outside the {@code long} range
   */
  public static long numberToEpochUnits(final Number number) {
    return switch (number) {
      // INTEGRAL: longValue() IS EXACT
      case Long l -> l;
      case Integer i -> i;
      case Short s -> s;
      case Byte b -> b;
      case AtomicLong a -> a.get();
      case AtomicInteger a -> a.get();
      case BigInteger bigInteger -> {
        if (bigInteger.bitLength() > 63)
          throw new IllegalArgumentException("Timestamp value " + bigInteger + " is outside the supported range");
        yield bigInteger.longValue();
      }
      case BigDecimal bigDecimal -> {
        try {
          yield bigDecimal.setScale(0, RoundingMode.FLOOR).longValueExact();
        } catch (final ArithmeticException e) {
          throw new IllegalArgumentException("Timestamp value " + bigDecimal + " is outside the supported range");
        }
      }
      // Double, Float and any other Number: read through the double value, floored, a non-finite one refused
      default -> floorToLong(number.doubleValue());
    };
  }

  private static long floorToLong(final double value) {
    // -2^63 is exactly representable as a double and 2^63 is the first double past Long.MAX_VALUE, so this range check
    // is exact: anything that passes floors to a value (long) holds without saturating
    if (Double.isNaN(value) || Double.isInfinite(value))
      throw new IllegalArgumentException("Timestamp value " + value + " is not a finite number");
    final double floored = Math.floor(value);
    if (floored < -0x1p63 || floored >= 0x1p63)
      throw new IllegalArgumentException("Timestamp value " + value + " is outside the supported range");
    return (long) floored;
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
      timestamp = numberToEpochUnits(number);
    else if (value instanceof String string) {
      if (FileUtils.isLong(string))
        timestamp = Long.parseLong(string);
      else
        return dateTimeToTimestamp(database, parseDateTime(database, string), precisionToUse);
    } else
      // UNSUPPORTED
      return null;

    return timestamp;
  }

  /**
   * THE one converter from any of the engine's date/time representations to epoch milliseconds. Accepts everything
   * {@link #dateTimeToTimestamp(Object, ChronoUnit)} accepts - {@link Date}, {@link Calendar}, {@link LocalDateTime},
   * {@link LocalDate}, {@link ZonedDateTime}, {@link OffsetDateTime}, {@link Instant}, {@link Number} and a
   * {@link String} in any format the engine parses - reading a zone-less value at UTC, which is how the engine's own
   * DATETIME representation is anchored.
   * <p>
   * #8152: three near-copies of this conversion lived in three packages, each covering a different subset of the
   * types, and a {@code return 0} / {@code return Long.MIN_VALUE} fall-through for the rest. When
   * {@code ts.timeBucket()} changed its return type from {@link Date} to {@link LocalDateTime} (#7610, #4385) the
   * change reached only some of them, and the continuous-aggregate refresher silently read every bucket as the epoch
   * - a value indistinguishable from "no watermark yet" - so its watermark never advanced and every refresh appended
   * a second copy of the whole aggregate. A SILENT SENTINEL IS WHAT TURNED A TYPE CHANGE INTO A DATA DEFECT, so this
   * one throws on a type it does not know rather than answering with a number that means something else.
   * <p>
   * A bare numeric {@link String} has its own epoch precision inferred from its digit count
   * ({@link #dateTimeToTimestampInferringStringPrecision}), because every caller here is asking for an ABSOLUTE
   * MOMENT - a bucket boundary, a time-range bound - which is the reading {@code BinaryComparator} already uses for
   * the same string (#5956). Taking the raw digits as milliseconds instead would make a pushed-down range bound
   * disagree with the generic filter evaluating the very same predicate.
   *
   * @throws IllegalArgumentException when {@code value} is {@code null}, or of a type that carries no date/time
   */
  public static long toEpochMillis(final Object value) {
    if (value == null)
      throw new IllegalArgumentException("Cannot convert a null value to a timestamp");
    final Long millis = value instanceof String
        ? dateTimeToTimestampInferringStringPrecision(value, ChronoUnit.MILLIS)
        : dateTimeToTimestamp(null, value, ChronoUnit.MILLIS);
    if (millis == null)
      throw new IllegalArgumentException(
          "Cannot convert value of type '" + value.getClass().getName() + "' to a timestamp in milliseconds");
    return millis;
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
   * The single string-to-{@link LocalDateTime} entry point for the whole engine. {@code Type.convert} used to carry
   * its own copy of this chain and the two drifted (issue #4142 added this one so the GraphBatch bulk path, which
   * bypasses {@code Type.convert}, would accept the same inputs); both now call here, so a format accepted by one
   * write path is accepted by every write path. Tries, in order:
   * <ol>
   *   <li>{@link LocalDateTime#parse(CharSequence)} (ISO without zone);</li>
   *   <li>{@link ZonedDateTime#parse(CharSequence)} for ISO inputs ending in {@code Z} or
   *   with a {@code ±HH:mm} offset - the resulting instant is rebased onto the database's
   *   configured zone (default {@link ZoneId#systemDefault()}) before stripping the offset,
   *   so the stored wall-clock follows the database's locale rather than the input's;</li>
   *   <li>the schema's {@code dateTimeFormat};</li>
   *   <li>the schema's {@code dateFormat};</li>
   *   <li>{@link #SPACE_SEPARATED_DATE_TIME}, the SQL-timestamp spelling with an optional fraction and offset
   *   (issue #8090). Last so that it can only widen what is accepted, never reinterpret a string one of the
   *   schema's own patterns already claimed.</li>
   * </ol>
   * When {@code database} is {@code null} the schema patterns are skipped - there is no schema to read them from -
   * but the ISO and SQL-timestamp formats are still tried, and offset-bearing inputs keep their wall-clock without
   * rebasing, matching legacy parsing behavior in scopes without a schema.
   * <p>
   * A step whose format provably cannot match the input is not attempted at all - the order is unchanged, only the
   * wasted work is gone. See {@link #parseDateTime(Database, String, boolean)}.
   *
   * @throws DateTimeParseException when no format matches. Never answers {@code null}: a datetime that cannot be
   *                                parsed has to fail the write rather than empty the column.
   */
  public static LocalDateTime parseDateTime(final Database database, final String string) {
    return parseDateTime(database, string, true);
  }

  /**
   * {@link #parseDateTime} with the offset of an offset-bearing input DROPPED rather than rebased: the wall-clock
   * written is the wall-clock read.
   * <p>
   * The two callers of this chain disagree about that, and each disagreement is pinned by its own tests. The bulk
   * path rebases, so a client in another zone stores the instant it meant. {@code Type.convert} strips, because
   * Cypher's {@code datetime('2026-01-01T00:00:00')} renders itself as {@code 2026-01-01T00:00Z} and
   * {@code SET n.t = datetime(...)} has to read back the wall-clock the query named (issue #4125) rather than one
   * shifted by the server's zone. Reconciling the two changes what existing databases store and is its own issue;
   * issue #8090 unified only WHICH FORMATS are accepted, which is what was losing data.
   */
  public static LocalDateTime parseDateTimeKeepingWallClock(final Database database, final String string) {
    return parseDateTime(database, string, false);
  }

  private static LocalDateTime parseDateTime(final Database database, final String string, final boolean rebaseOffset) {
    // The order below is the contract; what varies is only whether a step is ATTEMPTED, and a step is skipped only
    // when it provably cannot match. This matters because the chain is now on the hot path: before issue #8090 the
    // literal it exists for never parsed at all, so no client could have been writing at volume through here, and
    // every failed attempt costs a JDK exception with its stack trace. The target literal now reaches its formatter
    // without a single throw.
    final boolean spaceSeparated = hasSpaceDateTimeSeparator(string);

    DateTimeParseException isoFailure = null;
    if (!spaceSeparated) {
      // ISO demands a 'T'. With a space at the separator both of these are guaranteed to fail, so they are not run.
      try {
        return LocalDateTime.parse(string);
      } catch (final DateTimeParseException e) {
        try {
          return dropZone(database, ZonedDateTime.parse(string), rebaseOffset);
        } catch (final DateTimeParseException e2) {
          isoFailure = e2;
        }
      }
    }

    final Temporal parsed = parseSchemaOrSqlTimestamp(database, string, isoFailure);
    return parsed instanceof OffsetDateTime offset ?
        dropZone(database, offset.toZonedDateTime(), rebaseOffset) :
        (LocalDateTime) parsed;
  }

  /**
   * The tail of the chain both entry points share: the schema's patterns, then {@link #SPACE_SEPARATED_DATE_TIME}.
   * Answers an {@link OffsetDateTime} when whichever format matched captured an offset and a {@link LocalDateTime}
   * otherwise, leaving each caller to apply its own offset policy.
   * <p>
   * Shared rather than reached by {@code parseZonedDateTime} calling {@code parseDateTime}, which made it parse the
   * same string twice: once itself to look for an offset, then again inside the nested call that re-derived the
   * schema attempt and re-ran the same formatter. Bounded duplicate work, but on a path every {@code DATE} and
   * {@code Instant} string write in the SQL-timestamp form now reaches.
   *
   * @param isoFailure the ISO failure to report in preference to this step's own, or {@code null} when the ISO
   *                   formats were skipped because the input could not have matched them
   */
  private static Temporal parseSchemaOrSqlTimestamp(final Database database, final String string,
      final DateTimeParseException isoFailure) {
    if (database != null) {
      // getFormatter(), not DateTimeFormatter.ofPattern(): the latter binds the JVM default locale, so a schema
      // pattern with a textual field parsed here but not through format()/parse() (issue #7144)
      final Temporal fromSchema = parseWithSchemaPatterns(database, string);
      if (fromSchema != null)
        return fromSchema;
    }

    // Unconditional, not gated on the separator: the time part is optional, so this is also what reads a bare date.
    try {
      return (Temporal) SPACE_SEPARATED_DATE_TIME.parseBest(string, OffsetDateTime::from, LocalDateTime::from);
    } catch (final DateTimeParseException e) {
      throw isoFailure != null ? isoFailure : e;
    }
  }

  /**
   * True when the value is space-separated rather than ISO {@code 'T'}-separated, which is what lets
   * {@link #parseDateTime} skip the steps that cannot match: no ISO format admits a space anywhere, and
   * {@link #SPACE_SEPARATED_DATE_TIME} requires one.
   * <p>
   * The test is "contains a space at all" rather than "the character at index 10", because the latter assumes the
   * date part is exactly {@code yyyy-MM-dd} and mis-reads ISO's extended year forms, where the separator does not
   * sit at 10. Mis-reading only ever cost a guaranteed-to-fail attempt rather than a wrong answer, but this costs
   * no more and cannot be wrong.
   */
  private static boolean hasSpaceDateTimeSeparator(final String string) {
    return string.indexOf(' ') >= 0;
  }

  /**
   * Applies a schema pattern, answering {@code null} instead of throwing when it does not match.
   * <p>
   * {@code parseUnresolved} is the only entry point in {@link DateTimeFormatter} that reports a non-match by
   * returning {@code null} rather than by constructing a {@link DateTimeParseException}, so it is used as a
   * predicate: it says whether the pattern consumed the WHOLE string, and only then is the real parse run to get
   * the resolved value. Resolution can still fail on a structurally-matching string (a pattern admitting month 13,
   * say), which is why the real parse keeps its own guard.
   * <p>
   * The string is walked twice on a match - {@code parseUnresolved} to ask whether the pattern claimed all of it,
   * then the real parse for the value - because {@code parseUnresolved} deliberately stops before RESOLUTION: it
   * applies no {@code parseDefaulting}, runs no chronology, and would leave this method reimplementing what the
   * formatter already does. The second pass is the cost of not doing that, and it is paid only where the pattern
   * matched; a non-match, which is the common case for the formats that do not apply, still costs one pass and
   * no exception.
   * <p>
   * Answers an {@link OffsetDateTime} when the PATTERN captured an offset ({@code XXX} and friends) and a
   * {@link LocalDateTime} otherwise, so the offset a schema pattern read out of the value survives to the caller
   * and each policy can decide what to do with it. Resolving with {@code LocalDateTime.parse} here instead would
   * discard it before anyone could: a caller wanting the instant would then anchor a wall-clock that had already
   * lost its offset, and land on a different moment than the value named.
   */
  private static Temporal parseWithPattern(final String string, final String pattern) {
    final DateTimeFormatter formatter = getFormatter(pattern);

    final ParsePosition position = new ParsePosition(0);
    if (formatter.parseUnresolved(string, position) == null || position.getIndex() != string.length())
      return null;

    try {
      return (Temporal) formatter.parseBest(string, OffsetDateTime::from, LocalDateTime::from);
    } catch (final DateTimeParseException ignore) {
      return null;
    }
  }

  /**
   * The schema's two patterns, in order, as a {@link Temporal} that still carries any offset they captured.
   */
  private static Temporal parseWithSchemaPatterns(final Database database, final String string) {
    final String dateTimeFormat = database.getSchema().getDateTimeFormat();
    final String dateFormat = database.getSchema().getDateFormat();

    // Nothing to try when neither has been changed: SPACE_SEPARATED_DATE_TIME already accepts everything the two
    // built-in patterns do - a bare date, and a date with a space and a time - and resolves it identically, the same
    // defaulted hour/minute/second included. Walking them first would cost the commonest write on the commonest
    // configuration TWO extra formatter passes to reach the answer the shared shape gives anyway.
    if (DEFAULT_DATE_TIME_FORMAT.equals(dateTimeFormat) && DEFAULT_DATE_FORMAT.equals(dateFormat))
      return null;

    final Temporal fromDateTimeFormat = parseWithPattern(string, dateTimeFormat);
    return fromDateTimeFormat != null ? fromDateTimeFormat : parseWithPattern(string, dateFormat);
  }

  /**
   * Parses into a {@link ZonedDateTime}, PRESERVING an offset the input carries instead of dropping it.
   * <p>
   * The offset-dropping policy {@link #parseDateTimeKeepingWallClock} implements exists for a {@code LocalDateTime}
   * target, where there is no zone to keep and Cypher's {@code datetime()} round-trip depends on the wall-clock
   * surviving (issue #4125). For a {@code ZonedDateTime} target the zone IS the type, so discarding a real offset
   * would change the instant the client sent. It also made the answer depend on the SEPARATOR:
   * {@code '...T13:45:10+01:00'} kept its offset because {@link ZonedDateTime#parse} claimed it first, while the same
   * moment written {@code '... 13:45:10+01:00'} fell through to the wall-clock chain and lost it. Both spellings now
   * denote the same instant.
   * <p>
   * An input with no offset has nothing to preserve and is anchored to the database's zone, as before.
   */
  public static ZonedDateTime parseZonedDateTime(final Database database, final String string) {
    return parseZonedDateTime(database, string, true);
  }

  /**
   * As {@link #parseZonedDateTime(Database, String)}, but a caller that has ALREADY tried the schema's patterns with
   * its own parser passes {@code false} for {@code trySchemaPatterns}.
   * <p>
   * {@code Type.convertToDate} is that caller: a {@code java.util.Date} target reads the schema's patterns through
   * {@code SimpleDateFormat}, deliberately, for its lenient resolution and default-time-zone anchoring that
   * {@code java.time} does not reproduce. Walking them again here is not just repeated work on the commonest
   * DATETIME target - it is a SECOND interpretation of the same pattern, which could answer differently from the one
   * that target is defined by. Only the built-in shapes below are its business.
   * <p>
   * The database is still needed for the zone an offset-less value is attached to, which is why it is not simply
   * passed as {@code null}.
   */
  public static ZonedDateTime parseZonedDateTime(final Database database, final String string,
      final boolean trySchemaPatterns) {
    DateTimeParseException isoFailure = null;
    if (!hasSpaceDateTimeSeparator(string)) {
      // ISO demands a 'T', so both of these are guaranteed to fail on a space-separated value and are not run.
      //
      // Offset-LESS first, though this method exists to keep offsets: that is the commoner ISO shape, and whichever
      // is tried second pays a thrown exception on every value of the other kind. An offset-bearing value still
      // reaches ZonedDateTime.parse with its offset intact, one attempt later.
      try {
        return LocalDateTime.parse(string).atZone(zoneOf(database));
      } catch (final DateTimeParseException e) {
        isoFailure = e;
      }
      try {
        return ZonedDateTime.parse(string);
      } catch (final DateTimeParseException ignore) {
        // Keep the offset-less failure: with no offset in the value, that is the format it most resembled.
      }
    }

    // A schema pattern can capture an offset too ('yyyy-MM-dd HH:mm:ss XXX'), and it is the value's own offset just
    // as much as an ISO one is, so it is kept rather than replaced by the database's zone.
    final Temporal parsed = parseSchemaOrSqlTimestamp(trySchemaPatterns ? database : null, string, isoFailure);
    return parsed instanceof OffsetDateTime offset ?
        offset.toZonedDateTime() :
        ((LocalDateTime) parsed).atZone(zoneOf(database));
  }

  /**
   * The database's configured zone, falling back to the JVM's when there is no database in scope.
   */
  private static ZoneId zoneOf(final Database database) {
    if (database != null) {
      final ZoneId zoneId = database.getSchema().getZoneId();
      if (zoneId != null)
        return zoneId;
    }
    return ZoneId.systemDefault();
  }

  /**
   * Turns an offset-bearing value into the local datetime that gets stored. With {@code rebaseOffset} the instant is
   * moved onto the database's configured zone first, so the stored wall-clock denotes the same moment the client
   * sent; without it the wall-clock is kept exactly as written. Without a database there is no zone to consult and
   * the wall-clock is kept either way.
   */
  private static LocalDateTime dropZone(final Database database, final ZonedDateTime parsed, final boolean rebaseOffset) {
    if (rebaseOffset && database != null) {
      final ZoneId zoneId = database.getSchema().getZoneId();
      if (zoneId != null)
        return parsed.withZoneSameInstant(zoneId).toLocalDateTime();
    }
    return parsed.toLocalDateTime();
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
    return switch (precision) {
      case SECONDS -> BinaryTypes.TYPE_DATETIME_SECOND;
      case MILLIS -> BinaryTypes.TYPE_DATETIME;
      case MICROS -> BinaryTypes.TYPE_DATETIME_MICROS;
      case NANOS -> BinaryTypes.TYPE_DATETIME_NANOS;
      case null, default -> throw new IllegalArgumentException("Not supported precision '" + precision + "'");
    };
  }

  public static final ChronoUnit getPrecisionFromType(final Type type) {
    return switch (type) {
      case DATETIME_SECOND -> ChronoUnit.SECONDS;
      case DATETIME -> ChronoUnit.MILLIS;
      case DATETIME_MICROS -> ChronoUnit.MICROS;
      case DATETIME_NANOS -> ChronoUnit.NANOS;
      default -> throw new IllegalArgumentException("Illegal date type from type " + type);
    };
  }

  public static final ChronoUnit getPrecisionFromBinaryType(final byte type) {
    return switch (type) {
      case BinaryTypes.TYPE_DATETIME_SECOND -> ChronoUnit.SECONDS;
      case BinaryTypes.TYPE_DATETIME -> ChronoUnit.MILLIS;
      case BinaryTypes.TYPE_DATETIME_MICROS -> ChronoUnit.MICROS;
      case BinaryTypes.TYPE_DATETIME_NANOS -> ChronoUnit.NANOS;
      default -> throw new IllegalArgumentException("Illegal date type from binary type " + type);
    };
  }

  public static int getNanos(final Object obj) {
    return switch (obj) {
      case null -> throw new IllegalArgumentException("Object is null");
      case LocalDateTime time -> time.getNano();
      case ZonedDateTime time -> time.getNano();
      case OffsetDateTime time -> time.getNano();
      case Instant instant -> instant.getNano();
      default -> throw new IllegalArgumentException("Object of class '" + obj.getClass() + "' is not supported");
    };
  }

  /**
   * Returns the sub-second precision actually carried by a temporal value, or {@code null} when the object is not a
   * temporal ArcadeDB stores with a sub-second precision (a {@code LocalDate}, a number, a string, anything else).
   * {@code Date} and {@code Calendar} cannot hold anything finer than a millisecond, so they always report
   * {@link ChronoUnit#MILLIS}.
   *
   * @param obj value to inspect
   *
   * @return the value's precision, or {@code null} if it is not a sub-second-capable temporal
   */
  public static ChronoUnit getPrecisionFromValue(final Object obj) {
    if (obj instanceof Date || obj instanceof Calendar)
      return ChronoUnit.MILLIS;
    if (obj instanceof LocalDateTime || obj instanceof ZonedDateTime || obj instanceof OffsetDateTime
        || obj instanceof Instant)
      return getPrecision(getNanos(obj));
    return null;
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
      final ChronoUnit precision = getPrecisionFromValue(objs[i]);
      if (precision == null)
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
      return getFormatter(format).format(millisToLocalDateTime(numberToEpochUnits(number), timeZone));
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
    final DateTimeFormatter cached = CACHED_FORMATTERS.get(format);
    if (cached != null)
      return cached;

    // A storage format must render and parse the same bytes on every server: the locale is pinned so a textual field
    // (MMM, EEE) never follows the JVM default, which the cache would otherwise freeze at the first caller's (issue #7112).
    final DateTimeFormatter formatter = new DateTimeFormatterBuilder().appendPattern(format)
        .parseDefaulting(ChronoField.HOUR_OF_DAY, 0).parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
        .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0).toFormatter(Locale.ENGLISH);

    // Bounded rather than evicting: the working set is a handful of schema formats that all fit, so an LRU would only
    // add a lock on a hot path to reorder entries nothing ever evicts. A slight overshoot when several threads race
    // past the check is harmless - the map is capped by intent, not by contract.
    if (CACHED_FORMATTERS.size() < MAX_CACHED_FORMATTERS)
      CACHED_FORMATTERS.putIfAbsent(format, formatter);

    return formatter;
  }

  /**
   * Number of patterns currently remembered by {@link #getFormatter(String)}. Test-only visibility into the bound
   * introduced by issue #6388.
   */
  public static int getCachedFormatterCount() {
    return CACHED_FORMATTERS.size();
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
      // floorDiv, not '/': the fourth site of the truncating division #7638 fixed elsewhere, and reachable under
      // the DEFAULT dateImplementation through asDate()/date()/sysdate(). 1969-12-31T12:00Z truncated to day 0
      // and came back as 1970-01-01 (found in review).
      return LocalDate.ofEpochDay(Math.floorDiv(timestamp, DateUtils.MS_IN_A_DAY));
    else if (dateImplementation.equals(LocalDateTime.class))
      // floorDiv AND floorMod, for a worse version of the same bug: Java's '%' keeps the DIVIDEND's sign, so a
      // pre-epoch timestamp with a sub-second component produced a NEGATIVE nanoOfSecond, which
      // LocalDateTime.ofEpochSecond validates and rejects - asDateTime() did not merely misreport such an
      // instant, it threw DateTimeException. floorMod pairs with floorDiv so second and nanosecond stay
      // consistent: the seconds floor down and the remainder is the non-negative distance above that second.
      return LocalDateTime.ofEpochSecond(Math.floorDiv(timestamp, 1_000L),
          (int) (Math.floorMod(timestamp, 1_000L) * 1_000_000L), ZoneOffset.UTC);
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
