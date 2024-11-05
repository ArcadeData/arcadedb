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
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
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
      value = LocalDateTime.ofEpochSecond(timestamp / 1000, (int) ((timestamp % 1000) * 1000), ZoneOffset.UTC);
    } else
      throw new SerializationException("Error on deserialize date. Configured class '" + dateImplementation + "' is not supported");
    return value;
  }

  public static Long dateTimeToTimestamp(final Object value, final ChronoUnit precisionToUse) {
    final long timestamp;
    if (value instanceof Date) {
      // WRITE MILLISECONDS
      timestamp = convertTimestamp(((Date) value).getTime(), ChronoUnit.MILLIS, precisionToUse);
    } else if (value instanceof Calendar)
      // WRITE MILLISECONDS
      timestamp = convertTimestamp(((Calendar) value).getTimeInMillis(), ChronoUnit.MILLIS, precisionToUse);
    else if (value instanceof LocalDateTime) {
      final LocalDateTime localDateTime = (LocalDateTime) value;
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
        timestamp =
            TimeUnit.NANOSECONDS.convert(localDateTime.toEpochSecond(ZoneOffset.UTC), TimeUnit.SECONDS) + localDateTime.getNano();
      else
        // NOT SUPPORTED
        timestamp = 0;
    } else if (value instanceof LocalDate) {
      final LocalDate localDate = (LocalDate) value;
      if (precisionToUse.equals(ChronoUnit.SECONDS))
        timestamp = localDate.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli() / 1_000L;
      else if (precisionToUse.equals(ChronoUnit.MILLIS))
        timestamp = localDate.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
      else if (precisionToUse.equals(ChronoUnit.MICROS))
        timestamp = localDate.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli() * 1_000_000L;
      else if (precisionToUse.equals(ChronoUnit.NANOS))
        timestamp = localDate.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli() * 1_000_000_000L;
      else
        // NOT SUPPORTED
        timestamp = 0;
    } else if (value instanceof ZonedDateTime) {
      final ZonedDateTime zonedDateTime = (ZonedDateTime) value;
      if (precisionToUse.equals(ChronoUnit.SECONDS))
        timestamp = zonedDateTime.toInstant().getEpochSecond();
      else if (precisionToUse.equals(ChronoUnit.MILLIS))
        timestamp = zonedDateTime.toInstant().toEpochMilli();
      else if (precisionToUse.equals(ChronoUnit.MICROS))
        timestamp =
            TimeUnit.MICROSECONDS.convert(zonedDateTime.toEpochSecond(), TimeUnit.SECONDS) + (zonedDateTime.getNano() / 1000);
      else if (precisionToUse.equals(ChronoUnit.NANOS))
        timestamp = TimeUnit.NANOSECONDS.convert(zonedDateTime.toEpochSecond(), TimeUnit.SECONDS) + zonedDateTime.getNano();
      else
        // NOT SUPPORTED
        timestamp = 0;
    } else if (value instanceof Instant) {
      final Instant instant = (Instant) value;
      if (precisionToUse.equals(ChronoUnit.SECONDS))
        timestamp = instant.getEpochSecond();
      else if (precisionToUse.equals(ChronoUnit.MILLIS))
        timestamp = instant.toEpochMilli();
      else if (precisionToUse.equals(ChronoUnit.MICROS))
        timestamp = TimeUnit.MICROSECONDS.convert(instant.getEpochSecond(), TimeUnit.SECONDS) + (instant.getNano() / 1000);
      else if (precisionToUse.equals(ChronoUnit.NANOS))
        timestamp = TimeUnit.NANOSECONDS.convert(instant.getEpochSecond(), TimeUnit.SECONDS) + instant.getNano();
      else
        // NOT SUPPORTED
        timestamp = 0;
    } else if (value instanceof Number)
      timestamp = ((Number) value).longValue();
    else if (value instanceof String) {
      if (FileUtils.isLong((String) value))
        timestamp = Long.parseLong(value.toString());
      else
        return dateTimeToTimestamp(LocalDateTime.parse((String) value), precisionToUse);
    } else
      // UNSUPPORTED
      return null;

    return timestamp;
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

  public static long convertTimestamp(final long timestamp, final ChronoUnit from, final ChronoUnit to) {
    if (from == to)
      return timestamp;

    if (from == ChronoUnit.SECONDS) {
      if (to == ChronoUnit.MILLIS)
        return timestamp * 1_000;
      else if (to == ChronoUnit.MICROS)
        return timestamp * 1_000_000;
      else if (to == ChronoUnit.NANOS)
        return timestamp * 1_000_000_000;

    } else if (from == ChronoUnit.MILLIS) {
      if (to == ChronoUnit.SECONDS)
        return timestamp / 1_000;
      else if (to == ChronoUnit.MICROS)
        return timestamp * 1_000;
      else if (to == ChronoUnit.NANOS)
        return timestamp * 1_000_000;

    } else if (from == ChronoUnit.MICROS) {
      if (to == ChronoUnit.SECONDS)
        return timestamp / 1_000_000;
      else if (to == ChronoUnit.MILLIS)
        return timestamp / 1_000;
      else if (to == ChronoUnit.NANOS)
        return timestamp * 1_000;

    } else if (from == ChronoUnit.NANOS) {
      if (to == ChronoUnit.SECONDS)
        return timestamp / 1_000_000_000;
      else if (to == ChronoUnit.MILLIS)
        return timestamp / 1_000_000;
      else if (to == ChronoUnit.MICROS)
        return timestamp / 1_000;
    }
    throw new IllegalArgumentException("Not supported conversion from '" + from + "' to '" + to + "'");
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
    else if (obj instanceof LocalDateTime)
      return ((LocalDateTime) obj).getNano();
    else if (obj instanceof ZonedDateTime)
      return ((ZonedDateTime) obj).getNano();
    else if (obj instanceof Instant)
      return ((Instant) obj).getNano();
    throw new IllegalArgumentException("Object of class '" + obj.getClass() + "' is not supported");
  }

  public static boolean isDate(final Object obj) {
    if (obj == null)
      return false;
    return obj instanceof Date || obj instanceof Calendar || obj instanceof LocalDate || obj instanceof LocalDateTime
        || obj instanceof ZonedDateTime || obj instanceof Instant;
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
    if (obj instanceof Number)
      return getFormatter(format).format(millisToLocalDateTime(((Number) obj).longValue(), timeZone));
    else if (obj instanceof Date)
      return getFormatter(format).format(millisToLocalDateTime(((Date) obj).getTime(), timeZone));
    else if (obj instanceof Calendar)
      return getFormatter(format).format(millisToLocalDateTime(((Calendar) obj).getTimeInMillis(), timeZone));
    else if (obj instanceof LocalDateTime) {
      if (timeZone != null)
        return ((LocalDateTime) obj).atZone(ZoneId.of(timeZone)).format(getFormatter(format));
      else
        return getFormatter(format).format(((LocalDateTime) obj));
    } else if (obj instanceof TemporalAccessor)
      return getFormatter(format).format((TemporalAccessor) obj);
    return null;
  }

  public static Object parse(final String text, final String format) {
    return LocalDateTime.parse(text, getFormatter(format));
  }

  public static DateTimeFormatter getFormatter(final String format) {
    return CACHED_FORMATTERS.computeIfAbsent(format,
        (f) -> new DateTimeFormatterBuilder().appendPattern(f).parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
            .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0).parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0).toFormatter());
  }

  public static Object getDate(final Object date, final Class impl) {
    if (impl.equals(Date.class))
      return new Date(DateUtils.dateTimeToTimestamp(date, ChronoUnit.MILLIS));
    else if (impl.equals(Calendar.class)) {
      final Calendar cal = Calendar.getInstance();
      cal.setTimeInMillis(DateUtils.dateTimeToTimestamp(date, ChronoUnit.MILLIS));
      return cal;
    }
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
      return String.format("%.1f minutes", minutes);

    final float hours = minutes / 60F;
    if (hours < 24F)
      return String.format("%.1f hours", hours);

    final float days = hours / 24F;
    if (days < 30F)
      return String.format("%.1f days", days);

    final float months = days / 30F;
    if (months < 12F)
      return String.format("%.1f months", months);

    return String.format("%.1f years", months / 12F);
  }

  public static boolean areSameDay(final Date d1, final Date d2) {
    final Calendar c1 = Calendar.getInstance();
    c1.setTime(d1);
    final Calendar c2 = Calendar.getInstance();
    c2.setTime(d2);
    return c1.get(Calendar.YEAR) == c2.get(Calendar.YEAR) && c1.get(Calendar.DAY_OF_YEAR) == c2.get(Calendar.DAY_OF_YEAR);
  }
}
