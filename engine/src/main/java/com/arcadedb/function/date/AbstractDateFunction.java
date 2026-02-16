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
package com.arcadedb.function.date;

import com.arcadedb.function.StatelessFunction;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;

/**
 * Abstract base class for date functions.
 * All date functions share the "date." namespace prefix.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public abstract class AbstractDateFunction implements StatelessFunction {
  protected static final String NAMESPACE = "date";

  // Common time units
  public static final String UNIT_MS = "ms";
  public static final String UNIT_S = "s";
  public static final String UNIT_M = "m";
  public static final String UNIT_H = "h";
  public static final String UNIT_D = "d";

  /**
   * Returns the simple name without namespace prefix.
   */
  protected abstract String getSimpleName();

  @Override
  public String getName() {
    return NAMESPACE + "." + getSimpleName();
  }

  /**
   * Converts a timestamp value to milliseconds.
   * Handles various input types (Long, Date, Instant, etc.)
   */
  protected long toMillis(final Object timestamp) {
    if (timestamp == null) {
      return System.currentTimeMillis();
    }
    if (timestamp instanceof Long) {
      return (Long) timestamp;
    }
    if (timestamp instanceof Number) {
      return ((Number) timestamp).longValue();
    }
    if (timestamp instanceof Date) {
      return ((Date) timestamp).getTime();
    }
    if (timestamp instanceof Instant) {
      return ((Instant) timestamp).toEpochMilli();
    }
    if (timestamp instanceof ZonedDateTime) {
      return ((ZonedDateTime) timestamp).toInstant().toEpochMilli();
    }
    if (timestamp instanceof LocalDateTime) {
      return ((LocalDateTime) timestamp).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
    }
    // Try parsing as string
    try {
      return Long.parseLong(timestamp.toString());
    } catch (final NumberFormatException e) {
      throw new IllegalArgumentException("Cannot convert to timestamp: " + timestamp);
    }
  }

  /**
   * Converts a unit string to milliseconds multiplier.
   */
  protected long unitToMillis(final String unit) {
    if (unit == null)
      return 1; // default to milliseconds

    return switch (unit.toLowerCase()) {
      case UNIT_MS, "millis", "milliseconds" -> 1L;
      case UNIT_S, "sec", "seconds" -> 1000L;
      case UNIT_M, "min", "minutes" -> 60_000L;
      case UNIT_H, "hour", "hours" -> 3_600_000L;
      case UNIT_D, "day", "days" -> 86_400_000L;
      default -> throw new IllegalArgumentException("Unknown time unit: " + unit);
    };
  }

  /**
   * Gets a DateTimeFormatter for the given pattern, with default if null.
   */
  protected DateTimeFormatter getFormatter(final String format) {
    if (format == null || format.isEmpty()) {
      return DateTimeFormatter.ISO_LOCAL_DATE_TIME;
    }
    return DateTimeFormatter.ofPattern(format);
  }
}
