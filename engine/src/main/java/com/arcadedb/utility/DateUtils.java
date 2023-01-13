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

import java.time.*;
import java.time.temporal.*;
import java.util.*;
import java.util.concurrent.*;

public class DateUtils {
  public static final  long   MS_IN_A_DAY = 24 * 60 * 60 * 1000L; // 86_400_000
  private static final ZoneId UTC_ZONE_ID = ZoneId.of("UTC");

  public static Object dateTime(final Database database, final long timestamp, final Class dateTimeImplementation,
      final ChronoUnit dateTimePrecision) {
    final Object value;
    if (dateTimeImplementation.equals(Date.class))
      value = new Date(timestamp);
    else if (dateTimeImplementation.equals(Calendar.class)) {
      value = Calendar.getInstance(database.getSchema().getTimeZone());
      ((Calendar) value).setTimeInMillis(timestamp);
    } else if (dateTimeImplementation.equals(LocalDateTime.class)) {
      if (dateTimePrecision.equals(ChronoUnit.MILLIS))
        value = LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), UTC_ZONE_ID);
      else if (dateTimePrecision.equals(ChronoUnit.MICROS))
        value = LocalDateTime.ofInstant(Instant.ofEpochSecond(TimeUnit.MICROSECONDS.toSeconds(timestamp),
            TimeUnit.MICROSECONDS.toNanos(Math.floorMod(timestamp, TimeUnit.SECONDS.toMicros(1)))), UTC_ZONE_ID);
      else if (dateTimePrecision.equals(ChronoUnit.NANOS))
        value = LocalDateTime.ofInstant(Instant.ofEpochSecond(0L, timestamp), UTC_ZONE_ID);
      else
        value = 0;
    } else if (dateTimeImplementation.equals(ZonedDateTime.class)) {
      if (dateTimePrecision.equals(ChronoUnit.MILLIS))
        value = ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestamp), UTC_ZONE_ID);
      else if (dateTimePrecision.equals(ChronoUnit.MICROS))
        value = ZonedDateTime.ofInstant(Instant.ofEpochSecond(TimeUnit.MICROSECONDS.toSeconds(timestamp),
            TimeUnit.MICROSECONDS.toNanos(Math.floorMod(timestamp, TimeUnit.SECONDS.toMicros(1)))), UTC_ZONE_ID);
      else if (dateTimePrecision.equals(ChronoUnit.NANOS))
        value = ZonedDateTime.ofInstant(Instant.ofEpochSecond(0L, timestamp), UTC_ZONE_ID);
      else
        value = 0;
    } else if (dateTimeImplementation.equals(Instant.class)) {
      if (dateTimePrecision.equals(ChronoUnit.MILLIS))
        value = Instant.ofEpochMilli(timestamp);
      else if (dateTimePrecision.equals(ChronoUnit.MICROS))
        value = Instant.ofEpochSecond(TimeUnit.MICROSECONDS.toSeconds(timestamp),
            TimeUnit.MICROSECONDS.toNanos(Math.floorMod(timestamp, TimeUnit.SECONDS.toMicros(1))));
      else if (dateTimePrecision.equals(ChronoUnit.NANOS))
        value = Instant.ofEpochSecond(0L, timestamp);
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
}
