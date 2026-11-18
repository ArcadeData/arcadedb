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

import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.YearMonth;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.IsoFields;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses the ISO 8601 text of the openCypher temporal constructors ({@code date('2015-07-21')}, {@code datetime(...)}...) in
 * the forms Neo4j accepts, and refuses the ones it refuses (issue #12298).
 * <ul>
 *   <li>A date is a calendar date ({@code 2015-07-21}, {@code 2015-7-1}, {@code 20150721}), a year and month
 *   ({@code 2015-07}), a week date ({@code 2015-W30-2}, {@code 2015-W1}, {@code 2015W302}), a quarter date
 *   ({@code 2015-Q3-05}, {@code 2015Q305}), an ordinal date ({@code 2015-202}) or a year. The year is 4 digits, or signed
 *   with up to 9 ({@code +2015-07-21}, {@code -0001-12-31}); the forms without separators take the 4-digit year only.</li>
 *   <li>A time is {@code hh[:mm[:ss[.fff]]]} with one or two digits per field, or {@code hh[mm[ss[.fff]]]}, the fraction up to
 *   9 digits after a dot or a comma, optionally behind a {@code T}.</li>
 *   <li>An offset is {@code Z}, {@code +hh}, {@code +hhmm} or {@code +hh:mm}; a zone is a region name in brackets, matched
 *   whatever its case ({@code [europe/london]}). An offset and a zone together must name the same instant: the text
 *   {@code 21:40-04:00[Europe/London]} is refused, where it used to be read as {@code 21:40+01:00}, five hours away.</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class TemporalParser {
  private static final String YEAR = "(?<year>[0-9]{4}|[+-][0-9]{1,9})";

  private static final Pattern CALENDAR      = Pattern.compile(YEAR + "-(?<month>[0-9]{1,2})-(?<day>[0-9]{1,2})");
  private static final Pattern YEAR_MONTH    = Pattern.compile(YEAR + "-(?<month>[0-9]{2})");
  private static final Pattern WEEK          = Pattern.compile(YEAR + "-W(?<week>[0-9]{1,2})(?:-(?<dow>[0-9]))?");
  private static final Pattern QUARTER       = Pattern.compile(YEAR + "-Q(?<quarter>[0-9])(?:-(?<doq>[0-9]{1,2}))?");
  private static final Pattern ORDINAL       = Pattern.compile(YEAR + "-(?<ordinal>[0-9]{3})");
  private static final Pattern YEAR_ONLY     = Pattern.compile(YEAR);
  private static final Pattern BASIC_DATE    = Pattern.compile("(?<year>[0-9]{4})(?<month>[0-9]{2})(?<day>[0-9]{2})?");
  private static final Pattern BASIC_WEEK    = Pattern.compile("(?<year>[0-9]{4})W(?<week>[0-9]{2}|[0-9](?![0-9]))(?<dow>[0-9])?");
  private static final Pattern BASIC_QUARTER = Pattern.compile("(?<year>[0-9]{4})Q(?<quarter>[0-9])(?<doq>[0-9]{2})?");
  private static final Pattern BASIC_ORDINAL = Pattern.compile("(?<year>[0-9]{4})(?<ordinal>[0-9]{3})");

  private static final Pattern TIME_EXTENDED = Pattern.compile(
      "(?<hour>[0-9]{1,2})(?::(?<minute>[0-9]{1,2})(?::(?<second>[0-9]{1,2})(?:[.,](?<fraction>[0-9]{1,9}))?)?)?");
  private static final Pattern TIME_BASIC    = Pattern.compile(
      "(?<hour>[0-9]{2})(?:(?<minute>[0-9]{2})(?:(?<second>[0-9]{2})(?:[.,](?<fraction>[0-9]{1,9}))?)?)?");
  // A time followed by its offset: the time part holds no letter and no sign, so the first of them starts the offset
  private static final Pattern TIME_AND_OFFSET = Pattern.compile("(?<time>[0-9:.,]+)(?<offset>[Zz]|[+-][0-9]{2}(?::?[0-9]{2})?)?");

  private TemporalParser() {
  }

  public static LocalDate parseDate(final String text) {
    final LocalDate date = parseDateOrNull(text);
    if (date == null)
      throw new DateTimeException("Text cannot be parsed to a Date: " + text);
    return date;
  }

  public static LocalTime parseLocalTime(final String text) {
    final LocalTime time = parseTimeOrNull(text.startsWith("T") ? text.substring(1) : text);
    if (time == null)
      throw new DateTimeException("Text cannot be parsed to a LocalTime: " + text);
    return time;
  }

  /**
   * A time with its offset, UTC when the text gives none. A zone name is refused: a time has no date to resolve its rules on.
   */
  public static OffsetTime parseTime(final String text) {
    if (text.indexOf('[') >= 0)
      throw new TemporalUtil.InvalidTemporalTextException(
          "Using a named time zone e.g. [UTC] is not valid for a time without a date. Instead, use a specific time zone string e.g. +00:00.");
    final Matcher m = TIME_AND_OFFSET.matcher(text.startsWith("T") ? text.substring(1) : text);
    final LocalTime time = m.matches() ? parseTimeOrNull(m.group("time")) : null;
    if (time == null)
      throw new DateTimeException("Text cannot be parsed to a Time: " + text);
    return OffsetTime.of(time, m.group("offset") != null ? offset(m.group("offset")) : ZoneOffset.UTC);
  }

  public static LocalDateTime parseLocalDateTime(final String text) {
    final int t = timeSeparator(text);
    final LocalDate date = parseDateOrNull(t < 0 ? text : text.substring(0, t));
    final LocalTime time = t < 0 ? LocalTime.MIDNIGHT : parseTimeOrNull(text.substring(t + 1));
    if (date == null || time == null)
      throw new DateTimeException("Text cannot be parsed to a LocalDateTime: " + text);
    return LocalDateTime.of(date, time);
  }

  /**
   * A datetime: its date, then optionally its time, offset and zone. Without an offset or a zone it is in UTC; with a zone and no
   * offset, a local time the zone skips is moved forward by the gap, as Neo4j does.
   */
  public static ZonedDateTime parseDateTime(final String text) {
    final int t = timeSeparator(text);
    if (t < 0) {
      final LocalDate date = parseDateOrNull(text);
      if (date == null)
        throw new DateTimeException("Text cannot be parsed to a DateTime: " + text);
      return date.atStartOfDay(ZoneOffset.UTC);
    }

    String rest = text.substring(t + 1);
    ZoneId zone = null;
    final int bracket = rest.indexOf('[');
    if (bracket >= 0) {
      if (!rest.endsWith("]"))
        throw new DateTimeException("Text cannot be parsed to a DateTime: " + text);
      zone = zoneOrNull(rest.substring(bracket + 1, rest.length() - 1));
      if (zone == null)
        throw new DateTimeException("Text cannot be parsed to a DateTime: " + text);
      rest = rest.substring(0, bracket);
    }

    final LocalDate date = parseDateOrNull(text.substring(0, t));
    final Matcher m = TIME_AND_OFFSET.matcher(rest);
    final LocalTime time = m.matches() ? parseTimeOrNull(m.group("time")) : null;
    if (date == null || time == null)
      throw new DateTimeException("Text cannot be parsed to a DateTime: " + text);

    final LocalDateTime local = LocalDateTime.of(date, time);
    final ZoneOffset offset = m.group("offset") != null ? offset(m.group("offset")) : null;
    if (zone == null)
      return ZonedDateTime.of(local, offset != null ? offset : ZoneOffset.UTC);
    if (offset == null)
      return ZonedDateTime.of(local, zone);
    // The offset is the one the text states: the zone must have it at that local time, or the two name different instants.
    // ZonedDateTime.parse() takes the offset as a mere preference, which replaced it by the zone's (issue #12298)
    final ZonedDateTime zoned = ZonedDateTime.ofLocal(local, zone, offset);
    if (!zoned.getOffset().equals(offset) || !zoned.toLocalDateTime().equals(local))
      throw new TemporalUtil.InvalidTemporalTextException("Timezone and offset do not match: " + text);
    return zoned;
  }

  private static LocalDate parseDateOrNull(final String text) {
    Matcher m;
    if ((m = CALENDAR.matcher(text)).matches())
      return LocalDate.of(year(m), Integer.parseInt(m.group("month")), Integer.parseInt(m.group("day")));
    if ((m = YEAR_MONTH.matcher(text)).matches())
      return YearMonth.of(year(m), Integer.parseInt(m.group("month"))).atDay(1);
    if ((m = WEEK.matcher(text)).matches() || (m = BASIC_WEEK.matcher(text)).matches())
      return CypherDate.isoWeekDate(year(m), Integer.parseInt(m.group("week")), m.group("dow") != null ? Integer.parseInt(m.group("dow")) : 1);
    if ((m = QUARTER.matcher(text)).matches() || (m = BASIC_QUARTER.matcher(text)).matches()) {
      final int quarter = Integer.parseInt(m.group("quarter"));
      IsoFields.QUARTER_OF_YEAR.range().checkValidValue(quarter, IsoFields.QUARTER_OF_YEAR);
      return TemporalMapFields.withDayOfQuarter(LocalDate.of(year(m), (quarter - 1) * 3 + 1, 1),
          m.group("doq") != null ? Integer.parseInt(m.group("doq")) : 1);
    }
    if ((m = ORDINAL.matcher(text)).matches() || (m = BASIC_ORDINAL.matcher(text)).matches())
      return LocalDate.ofYearDay(year(m), Integer.parseInt(m.group("ordinal")));
    if ((m = YEAR_ONLY.matcher(text)).matches())
      return LocalDate.of(year(m), 1, 1);
    if ((m = BASIC_DATE.matcher(text)).matches())
      return LocalDate.of(year(m), Integer.parseInt(m.group("month")), m.group("day") != null ? Integer.parseInt(m.group("day")) : 1);
    return null;
  }

  private static LocalTime parseTimeOrNull(final String text) {
    Matcher m = TIME_EXTENDED.matcher(text);
    if (!m.matches()) {
      m = TIME_BASIC.matcher(text);
      if (!m.matches())
        return null;
    }
    final String fraction = m.group("fraction");
    int nanos = 0;
    if (fraction != null) {
      nanos = Integer.parseInt(fraction);
      for (int i = fraction.length(); i < 9; i++)
        nanos *= 10;
    }
    return LocalTime.of(Integer.parseInt(m.group("hour")), m.group("minute") != null ? Integer.parseInt(m.group("minute")) : 0,
        m.group("second") != null ? Integer.parseInt(m.group("second")) : 0, nanos);
  }

  /** The index of the separator between the date and the time of a datetime, -1 when there is no time. */
  private static int timeSeparator(final String text) {
    for (int i = 0; i < text.length(); i++) {
      final char c = text.charAt(i);
      if (c == 'T' || c == 't')
        return i;
    }
    return -1;
  }

  private static int year(final Matcher m) {
    final String year = m.group("year");
    return Integer.parseInt(year.charAt(0) == '+' ? year.substring(1) : year);
  }

  private static ZoneOffset offset(final String text) {
    if (text.length() == 1)
      return ZoneOffset.UTC;
    return ZoneOffset.of(text.length() == 5 ? text.substring(0, 3) + ":" + text.substring(3) : text);
  }

  /** A region in brackets, whatever its case, or {@code Z}: an offset is no zone there, so {@code [+01:00]} is refused. */
  private static ZoneId zoneOrNull(final String name) {
    if ("Z".equals(name))
      return ZoneOffset.UTC;
    final String id = ZoneNames.BY_LOWER_CASE.get(name.toLowerCase(Locale.ROOT));
    return id != null ? ZoneId.of(id) : null;
  }

  /** The region ids by their lower-case spelling, built on first use. */
  private static final class ZoneNames {
    private static final Map<String, String> BY_LOWER_CASE = new HashMap<>();

    static {
      // The available ids are the region ids of the zone rules: an offset id such as "UTC+01:00" is not among them
      for (final String id : ZoneId.getAvailableZoneIds())
        BY_LOWER_CASE.put(id.toLowerCase(Locale.ROOT), id);
    }
  }
}
