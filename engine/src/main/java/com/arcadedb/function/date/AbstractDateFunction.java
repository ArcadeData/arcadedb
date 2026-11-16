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
import com.arcadedb.function.cypher.CypherFunctionHelper;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

/**
 * Abstract base class for date functions.
 * All date functions share the "date." namespace prefix.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public abstract class AbstractDateFunction implements StatelessFunction {
  protected static final String NAMESPACE = "date";

  /** The pattern APOC's date.format/date.parse/date.fields use when none is given (issue #12106). */
  public static final String DEFAULT_FORMAT = "yyyy-MM-dd HH:mm:ss";

  protected static final String UTC = "UTC";

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
      // Issue #10850: a whole number of milliseconds - longValue() read 1.9 as 1
      return CypherFunctionHelper.requireWholeNumberArgument(timestamp, getName(), "timestamp");
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
   * The time unit an APOC unit name denotes (issue #12106): {@code ms}/{@code milli}/{@code millis}/{@code milliseconds},
   * {@code s}/{@code second}/{@code seconds}, {@code m}/{@code minute}/{@code minutes}, {@code h}/{@code hour}/{@code hours},
   * {@code d}/{@code day}/{@code days}, case-insensitive, plus ArcadeDB's {@code sec} and {@code min}. A null unit is
   * milliseconds, as in APOC. Conversions go through {@link TimeUnit}, which saturates at {@link Long#MAX_VALUE} and
   * {@link Long#MIN_VALUE} exactly like APOC does.
   */
  protected static TimeUnit timeUnit(final Object unit) {
    if (unit == null)
      return TimeUnit.MILLISECONDS;

    return switch (unit.toString().toLowerCase(Locale.ROOT)) {
      case "ms", "milli", "millis", "milliseconds" -> TimeUnit.MILLISECONDS;
      case "s", "sec", "second", "seconds" -> TimeUnit.SECONDS;
      case "m", "min", "minute", "minutes" -> TimeUnit.MINUTES;
      case "h", "hour", "hours" -> TimeUnit.HOURS;
      case "d", "day", "days" -> TimeUnit.DAYS;
      default -> throw new IllegalArgumentException("The unit: " + unit + " is not correct");
    };
  }

  /**
   * The pattern argument at {@code index}, {@link #DEFAULT_FORMAT} when it is absent, null or empty, as in APOC.
   */
  protected static String patternArgument(final Object[] args, final int index) {
    final Object pattern = args.length > index ? args[index] : null;
    return pattern == null || pattern.toString().isEmpty() ? DEFAULT_FORMAT : pattern.toString();
  }

  /**
   * A new {@link SimpleDateFormat}, the formatter APOC's date.format/date.parse/date.toISO8601 use (issue #12106): its
   * pattern letters, its lenient parsing (a prefix of the text is enough, a month 13 rolls over) and its Julian calendar
   * before 1582. The time zone is {@code timezone} read by {@link TimeZone#getTimeZone(String)} (an unknown id is GMT),
   * UTC when it is null or empty. The locale is pinned, so a textual field renders the same on every server (issue #7144).
   * A new instance per call: the class is not thread-safe.
   */
  protected static SimpleDateFormat simpleDateFormat(final String pattern, final Object timezone) {
    final SimpleDateFormat format;
    try {
      format = new SimpleDateFormat(pattern, Locale.ENGLISH);
    } catch (final IllegalArgumentException e) {
      throw new IllegalArgumentException("The pattern: " + pattern + " is not correct", e);
    }
    format.setTimeZone(TimeZone.getTimeZone(timezone == null || timezone.toString().isEmpty() ? UTC : timezone.toString()));
    return format;
  }
}
