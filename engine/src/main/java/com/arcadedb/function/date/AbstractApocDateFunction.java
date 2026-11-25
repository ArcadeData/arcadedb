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

import com.arcadedb.exception.InvalidArgumentException;

import java.text.SimpleDateFormat;
import java.time.DateTimeException;
import java.time.ZoneId;
import java.util.Locale;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

/**
 * Base class of the {@code apoc.date.*} functions that answer as APOC 5.26's do (issue #12106). They are registered under
 * their {@code apoc.} name, which the function registries resolve before stripping the prefix, so {@code apoc.date.format}
 * reaches the APOC semantics while {@code date.format} keeps ArcadeDB's own (the {@code Date*} classes). The
 * {@code apoc.date.*} functions APOC and ArcadeDB answer alike ({@code currentTimestamp}, {@code fromISO8601},
 * {@code systemTimezone}) have no class here: their {@code apoc.} name still resolves to the {@code date.} one.
 * <p>
 * An argument the function is not defined for (an unknown unit, time zone or pattern) is an {@link InvalidArgumentException},
 * a client error on every protocol (issue #11589).
 */
public abstract class AbstractApocDateFunction extends AbstractDateFunction {
  /** The pattern APOC's apoc.date.format/parse/fields use when none is given. */
  public static final String DEFAULT_FORMAT = "yyyy-MM-dd HH:mm:ss";

  protected static final String UTC = "UTC";

  /**
   * The formatters of the calling thread: {@link SimpleDateFormat} is not thread-safe, and building one (with its calendar and
   * its number format) for every row of a query was the cost of a call. A thread keeps at most
   * {@link SimpleDateFormatCache#SLOTS} formatters for its lifetime (pooled server threads included): bounded, not a leak.
   */
  private static final ThreadLocal<SimpleDateFormatCache> FORMATS = ThreadLocal.withInitial(SimpleDateFormatCache::new);

  @Override
  public String getName() {
    return "apoc." + super.getName();
  }

  /**
   * The time unit an APOC unit name denotes: {@code ms}/{@code milli}/{@code millis}/{@code milliseconds},
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
      default -> throw new InvalidArgumentException("The unit: " + InvalidArgumentException.quote(unit) + " is not correct");
    };
  }

  /**
   * The time argument in milliseconds: a count (a number or its text) is in {@code unit}, a point in time is already one and the
   * unit, still validated, does not scale it.
   */
  protected long timeInMillis(final Object time, final Object unit) {
    final TimeUnit timeUnit = timeUnit(unit);
    final long value = toMillis(time);
    return isPointInTime(time) ? value : timeUnit.toMillis(value);
  }

  /**
   * The time argument in {@code unit}: a count (a number or its text) already is, a point in time is converted from its
   * milliseconds, truncating toward zero as {@link TimeUnit#convert(long, TimeUnit)} does.
   */
  protected long timeInUnit(final Object time, final TimeUnit unit) {
    final long value = toMillis(time);
    return isPointInTime(time) ? unit.convert(value, TimeUnit.MILLISECONDS) : value;
  }

  /**
   * The pattern argument at {@code index}, {@link #DEFAULT_FORMAT} when it is absent, null or empty, as in APOC.
   */
  protected static String patternArgument(final Object[] args, final int index) {
    final Object pattern = args.length > index ? args[index] : null;
    return pattern == null || pattern.toString().isEmpty() ? DEFAULT_FORMAT : pattern.toString();
  }

  /**
   * The time zone argument at {@code index} as the id of a {@link ZoneId}, UTC when it is absent. A null or an unknown id is an
   * error, as in APOC's apoc.date.field ({@code ZoneId.of}).
   */
  protected static ZoneId zoneIdArgument(final Object[] args, final int index) {
    final Object timezone = args.length > index ? args[index] : UTC;
    if (timezone == null)
      throw new InvalidArgumentException("The timezone: null is not correct");
    try {
      return ZoneId.of(timezone.toString());
    } catch (final DateTimeException e) {
      throw new InvalidArgumentException("The timezone: " + InvalidArgumentException.quote(timezone) + " is not correct", e);
    }
  }

  /**
   * The {@link SimpleDateFormat} APOC's apoc.date.format/parse/toISO8601 use: its pattern letters, its lenient parsing (a
   * prefix of the text is enough, a month 13 rolls over) and its Julian calendar before 1582. Its time zone is
   * {@code timezone}, UTC when it is null or empty. The locale is pinned, so a textual field renders the same on every server
   * (issue #7144).
   * <p>
   * APOC reads the zone with {@link TimeZone#getTimeZone(String)}, which answers GMT for an id it does not know, so a typo
   * renders or reads every row in GMT without a word. Here an id neither {@link TimeZone} nor {@link ZoneId} knows is an
   * error, as it is in apoc.date.field/fields.
   * <p>
   * The instance belongs to the calling thread and is reused by its next call with the same pattern and zone: use it at once
   * and do not hand it on.
   */
  protected static SimpleDateFormat simpleDateFormat(final String pattern, final Object timezone) {
    return FORMATS.get().get(pattern, timezone == null || timezone.toString().isEmpty() ? UTC : timezone.toString());
  }

  private static TimeZone timeZone(final String id) {
    final TimeZone zone = TimeZone.getTimeZone(id);
    if (!"GMT".equals(zone.getID()) || "GMT".equals(id))
      return zone;
    // GMT is also what TimeZone answers for an id it does not know: one ZoneId reads (an offset such as +02:00, Z) is a zone
    try {
      return TimeZone.getTimeZone(ZoneId.of(id, ZoneId.SHORT_IDS));
    } catch (final DateTimeException e) {
      throw new InvalidArgumentException("The timezone: " + InvalidArgumentException.quote(id) + " is not correct", e);
    }
  }

  /**
   * A few formatters of one thread, by pattern and zone, replaced in turn once all slots are taken. A hit allocates nothing.
   */
  private static final class SimpleDateFormatCache {
    private static final int                SLOTS     = 8;
    private final        String[]           patterns  = new String[SLOTS];
    private final        String[]           zoneIds   = new String[SLOTS];
    private final        TimeZone[]         timeZones = new TimeZone[SLOTS];
    private final        SimpleDateFormat[] formats   = new SimpleDateFormat[SLOTS];
    private              int                next;

    private SimpleDateFormat get(final String pattern, final String zoneId) {
      for (int i = 0; i < SLOTS; i++)
        if (formats[i] != null && patterns[i].equals(pattern) && zoneIds[i].equals(zoneId)) {
          // parsing a text that names a zone ('z') sets that zone on the formatter, so the zone asked for is set back each time
          formats[i].setTimeZone(timeZones[i]);
          return formats[i];
        }

      final TimeZone timeZone = timeZone(zoneId);
      final SimpleDateFormat format;
      try {
        format = new SimpleDateFormat(pattern, Locale.ENGLISH);
      } catch (final IllegalArgumentException e) {
        throw new InvalidArgumentException("The pattern: " + InvalidArgumentException.quote(pattern) + " is not correct", e);
      }
      format.setTimeZone(timeZone);

      final int slot = next;
      next = (next + 1) % SLOTS;
      patterns[slot] = pattern;
      zoneIds[slot] = zoneId;
      timeZones[slot] = timeZone;
      formats[slot] = format;
      return format;
    }
  }
}
