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

import java.time.*;
import java.time.temporal.*;
import java.util.*;
import java.util.concurrent.*;

public class DateUtils {
  public static final  long   MS_IN_A_DAY = 24 * 60 * 60 * 1000L; // 86_400_000
  private static final ZoneId UTC_ZONE_ID = ZoneId.of("UTC");

  public static Object dateTime(final Database database, final long timestamp, final ChronoUnit sourcePrecision, final Class dateTimeImplementation,
      final ChronoUnit destinationPrecision) {
    final long convertedTimestamp = convertTimestamp(timestamp, sourcePrecision, destinationPrecision);

    final Object value;
    if (dateTimeImplementation.equals(Date.class))
      value = new Date(convertedTimestamp);
    else if (dateTimeImplementation.equals(Calendar.class)) {
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
      throw new SerializationException("Error on deserialize datetime. Configured class '" + dateTimeImplementation + "' is not supported");
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
    } else
      throw new SerializationException("Error on deserialize date. Configured class '" + dateImplementation + "' is not supported");
    return value;
  }

  public static ChronoUnit parsePrecision(final String precision) {
    switch (precision) {
    case "second":
      return ChronoUnit.SECONDS;
    case "millisecond":
      return ChronoUnit.MILLIS;
    case "microsecond":
      return ChronoUnit.MICROS;
    case "nanosecond":
      return ChronoUnit.NANOS;
    default:
      throw new SerializationException("Unsupported datetime precision '" + precision + "'");
    }
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
      throw new IllegalArgumentException("Object i snull");
    else if (obj instanceof LocalDateTime)
      return ((LocalDateTime) obj).getNano();
    else if (obj instanceof ZonedDateTime)
      return ((ZonedDateTime) obj).getNano();
    else if (obj instanceof Instant)
      return ((Instant) obj).getNano();
    throw new IllegalArgumentException("Object of class '" + obj.getClass() + "' is not supported");
  }
}
