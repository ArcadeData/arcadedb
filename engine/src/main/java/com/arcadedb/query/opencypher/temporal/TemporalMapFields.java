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
import java.time.temporal.IsoFields;
import java.time.temporal.WeekFields;
import java.util.Map;
import java.util.Set;

/**
 * The fields of an openCypher temporal map - the argument of {@code date()}, {@code localtime()}, {@code time()},
 * {@code localdatetime()}, {@code datetime()} and the adjustment map of their {@code truncate()} - resolved as Neo4j resolves
 * them (issue #12297).
 * <p>
 * The date fields form four kinds of date, and a map names one of them: a calendar date ({@code month}, {@code day}), a
 * week date ({@code week}, {@code dayOfWeek}), a quarter date ({@code quarter}, {@code dayOfQuarter}) or an ordinal date
 * ({@code ordinalDay}), each with an optional {@code year}. A field of another kind is refused rather than dropped.
 * <ul>
 *   <li>Without a base value the map has to be a complete prefix of its kind: {@code year} first, then the fields in order
 *   ({@code day} needs {@code month}, {@code dayOfWeek} needs {@code week}, {@code second} needs {@code minute}). A missing
 *   year used to default to the current year, so the answer changed every January 1.</li>
 *   <li>With a base value ({@code date:}, {@code datetime:}, {@code time:}, or the truncated value) each field the map gives
 *   replaces the base's, applied in order, as {@code java.time}'s {@code with()} applies it: a day past the end of the new
 *   month is clamped ({@code {date: date('2020-02-29'), year: 2021}} is {@code 2021-02-28}). The week, quarter and ordinal
 *   fields used to be dropped on this path.</li>
 *   <li>A field given as {@code null} is given: it reads as the field's smallest value ({@code year} 0, {@code month} 1,
 *   {@code hour} 0...), as in Neo4j. A field value has to be an integer.</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class TemporalMapFields {
  public static final Set<String> DATE_FIELDS = Set.of("year", "month", "day", "week", "dayOfWeek", "quarter", "dayOfQuarter",
      "ordinalDay");
  public static final Set<String> TIME_FIELDS = Set.of("hour", "minute", "second", "millisecond", "microsecond", "nanosecond");
  // Every key a temporal map can hold: one of them in the wrong constructor is "not supported", any other key is unknown
  private static final Set<String> SELECTION_KEYS = Set.of("date", "time", "datetime", "timezone", "epochSeconds", "epochMillis");
  private static final Set<String> ADJUSTMENT_SELECTIONS       = Set.of("date", "time", "datetime");
  private static final Set<String> ZONED_ADJUSTMENT_SELECTIONS = Set.of("date", "time", "datetime", "timezone");

  // A field the map does not give. No field reads Integer.MIN_VALUE: a year is limited to +/-999,999,999
  static final int ABSENT = Integer.MIN_VALUE;

  private static final int CALENDAR = 0;
  private static final int WEEK     = 1;
  private static final int QUARTER  = 2;
  private static final int ORDINAL  = 3;

  private static final String[] KIND_NAMES = { "calendar", "week", "quarter", "ordinal" };
  // Each kind's fields after the year, in order
  private static final String[] DATE_FIELD_NAMES = { "month", "day", "week", "dayOfWeek", "quarter", "dayOfQuarter", "ordinalDay" };
  private static final int[]    DATE_FIELD_KINDS = { CALENDAR, CALENDAR, WEEK, WEEK, QUARTER, QUARTER, ORDINAL };

  private TemporalMapFields() {
  }

  /**
   * Refuses an empty map, and a key the constructor does not take: one of the other constructors' keys is "not supported"
   * ({@code date({year: 2020, hour: 3})}), any other key is unknown ({@code date({year: 2020, foo: 3})}).
   *
   * @param allowedSelections the base and zone keys this constructor takes besides its fields, e.g. {@code date} for
   *                          {@code date()}
   */
  public static void checkKeys(final Map<String, Object> map, final boolean dateFields, final boolean timeFields,
      final Set<String> allowedSelections) {
    if (map.isEmpty())
      throw new IllegalArgumentException("At least one temporal unit must be specified.");
    for (final String key : map.keySet()) {
      if (dateFields && DATE_FIELDS.contains(key) || timeFields && TIME_FIELDS.contains(key) || allowedSelections.contains(key))
        continue;
      if (DATE_FIELDS.contains(key) || TIME_FIELDS.contains(key) || SELECTION_KEYS.contains(key))
        throw new IllegalArgumentException("Not supported: " + key);
      throw new IllegalArgumentException("No such field: " + key);
    }
  }

  /**
   * Refuses a key the adjustment map of a {@code truncate()} cannot hold: a field of the other half ({@code date.truncate(...,
   * {hour: 2})}), an unknown key, or a {@code timezone} for a value that has none. The selection keys are accepted and
   * ignored, as Neo4j does.
   *
   * @param zoned whether the truncated value has a zone, which the map's {@code timezone} then replaces
   */
  public static void checkAdjustmentKeys(final Map<String, Object> map, final boolean dateFields, final boolean timeFields,
      final boolean zoned) {
    if (!zoned && map.containsKey("timezone"))
      throw new IllegalArgumentException("Cannot assign time zone if also assigning other fields.");
    checkKeys(map, dateFields, timeFields, zoned ? ZONED_ADJUSTMENT_SELECTIONS : ADJUSTMENT_SELECTIONS);
  }

  /**
   * {@code time} with the time fields of a truncate adjustment map in place of its own. Unlike a selection, a sub-second field
   * keeps the coarser sub-second units the truncation left ({@link TemporalUtil#computeNanos(Map, int, boolean)}).
   */
  public static LocalTime adjustTruncatedTime(final LocalTime time, final Map<String, Object> map) {
    final int hour = field(map, "hour", 0);
    final int minute = field(map, "minute", 0);
    final int second = field(map, "second", 0);
    return LocalTime.of(hour != ABSENT ? hour : time.getHour(), minute != ABSENT ? minute : time.getMinute(),
        second != ABSENT ? second : time.getSecond(), TemporalUtil.computeNanos(map, time.getNano(), true));
  }

  /**
   * Whether the map gives any date field.
   */
  public static boolean hasDateField(final Map<String, Object> map) {
    for (final String key : DATE_FIELDS)
      if (map.containsKey(key))
        return true;
    return false;
  }

  /**
   * Whether the map gives any time field.
   */
  public static boolean hasTimeField(final Map<String, Object> map) {
    for (final String key : TIME_FIELDS)
      if (map.containsKey(key))
        return true;
    return false;
  }

  /**
   * The date the map's date fields select.
   *
   * @param base         the date the fields adjust, null when the map has none
   * @param withTimeOrZone true when the map also builds a time (a time field, a {@code time:} base): without a base the date
   *                     must then be complete, as Neo4j requires ({@code localdatetime({year: 2020, hour: 3})} is refused,
   *                     "month must be specified"), where alone it would be the first day of its year
   *
   * @return the date, or {@code base} when the map gives no date field (null when there is no base either)
   */
  public static LocalDate resolveDate(final Map<String, Object> map, final LocalDate base, final boolean withTimeOrZone) {
    final int year = field(map, "year", 0);
    final int[] values = new int[DATE_FIELD_NAMES.length];
    boolean any = year != ABSENT;
    for (int i = 0; i < values.length; i++) {
      values[i] = field(map, DATE_FIELD_NAMES[i], 1);
      any |= values[i] != ABSENT;
    }
    if (!any)
      return base;

    final int kind = values[2] != ABSENT || values[3] != ABSENT ? WEEK :
        values[0] != ABSENT || values[1] != ABSENT ? CALENDAR :
            values[4] != ABSENT || values[5] != ABSENT ? QUARTER :
                values[6] != ABSENT ? ORDINAL : CALENDAR;
    for (int i = 0; i < values.length; i++)
      if (values[i] != ABSENT && DATE_FIELD_KINDS[i] != kind)
        throw new IllegalArgumentException("Cannot assign " + DATE_FIELD_NAMES[i] + " to " + KIND_NAMES[kind] + " date.");

    final int month = values[0], day = values[1], week = values[2], dayOfWeek = values[3], quarter = values[4], dayOfQuarter =
        values[5], ordinalDay = values[6];

    if (base == null) {
      if (year == ABSENT)
        throw new IllegalArgumentException("year must be specified");
      return switch (kind) {
        case WEEK -> {
          requireBefore(dayOfWeek, "dayOfWeek", week, "week");
          if (withTimeOrZone)
            requireGiven(dayOfWeek, "dayOfWeek");
          yield CypherDate.isoWeekDate(year, week != ABSENT ? week : 1, dayOfWeek != ABSENT ? dayOfWeek : 1);
        }
        case QUARTER -> {
          requireBefore(dayOfQuarter, "dayOfQuarter", quarter, "quarter");
          if (withTimeOrZone)
            requireGiven(dayOfQuarter, "dayOfQuarter");
          yield withQuarter(LocalDate.of(year, 1, 1), quarter, dayOfQuarter != ABSENT ? dayOfQuarter : 1);
        }
        case ORDINAL -> LocalDate.ofYearDay(year, ordinalDay);
        default -> {
          requireBefore(day, "day", month, "month");
          if (withTimeOrZone) {
            requireGiven(month, "month");
            requireGiven(day, "day");
          }
          yield LocalDate.of(year, month != ABSENT ? month : 1, day != ABSENT ? day : 1);
        }
      };
    }

    return switch (kind) {
      case WEEK -> {
        final int weekYear = year != ABSENT ? year : base.get(WeekFields.ISO.weekBasedYear());
        final int selectedWeek = week != ABSENT ? week : Math.min(base.get(WeekFields.ISO.weekOfWeekBasedYear()), weeksIn(weekYear));
        yield CypherDate.isoWeekDate(weekYear, selectedWeek, dayOfWeek != ABSENT ? dayOfWeek : base.getDayOfWeek().getValue());
      }
      case QUARTER -> {
        LocalDate date = year != ABSENT ? base.withYear(year) : base;
        if (quarter != ABSENT) {
          IsoFields.QUARTER_OF_YEAR.range().checkValidValue(quarter, IsoFields.QUARTER_OF_YEAR);
          date = date.with(IsoFields.QUARTER_OF_YEAR, quarter);
        }
        yield dayOfQuarter != ABSENT ? withDayOfQuarter(date, dayOfQuarter) : date;
      }
      case ORDINAL -> (year != ABSENT ? base.withYear(year) : base).withDayOfYear(ordinalDay);
      default -> {
        LocalDate date = year != ABSENT ? base.withYear(year) : base;
        if (month != ABSENT)
          date = date.withMonth(month);
        yield day != ABSENT ? date.withDayOfMonth(day) : date;
      }
    };
  }

  /**
   * The time the map's time fields select.
   *
   * @param base the time the fields adjust, null when the map has none
   *
   * @return the time, or {@code base} when the map gives no time field (null when there is no base either)
   */
  public static LocalTime resolveTime(final Map<String, Object> map, final LocalTime base) {
    final int hour = field(map, "hour", 0);
    final int minute = field(map, "minute", 0);
    final int second = field(map, "second", 0);
    final boolean subSecond = map.containsKey("millisecond") || map.containsKey("microsecond") || map.containsKey("nanosecond");
    if (hour == ABSENT && minute == ABSENT && second == ABSENT && !subSecond)
      return base;

    if (base == null) {
      if (hour == ABSENT)
        throw new IllegalArgumentException("hour must be specified");
      requireBefore(second, "second", minute, "minute");
      if (subSecond) {
        if (minute == ABSENT)
          throw new IllegalArgumentException("subsecond cannot be specified without minute");
        if (second == ABSENT)
          throw new IllegalArgumentException("subsecond cannot be specified without second");
      }
      return LocalTime.of(hour, minute != ABSENT ? minute : 0, second != ABSENT ? second : 0, TemporalUtil.computeNanos(map, 0));
    }
    return LocalTime.of(hour != ABSENT ? hour : base.getHour(), minute != ABSENT ? minute : base.getMinute(),
        second != ABSENT ? second : base.getSecond(), TemporalUtil.computeNanos(map, base.getNano()));
  }

  /**
   * The date of a temporal a map names as its {@code date:} or {@code datetime:} base: a date, a local datetime or a datetime.
   * Anything else is refused, as Neo4j refuses it; a String, a time, a duration or {@code null} used to leave the map without a
   * base, which then defaulted to January 1 of the current year.
   */
  public static LocalDate dateOf(final Object value) {
    return switch (value) {
      case CypherDate d -> d.getValue();
      case CypherLocalDateTime ldt -> ldt.getValue().toLocalDate();
      case CypherDateTime dt -> dt.getValue().toLocalDate();
      case CypherLocalTime ignored -> throw new IllegalArgumentException("Cannot get the date of: " + value);
      case CypherTime ignored -> throw new IllegalArgumentException("Cannot get the date of: " + value);
      case null, default -> throw new IllegalArgumentException("Cannot construct date from: " + describe(value));
    };
  }

  /**
   * The time of a temporal a map names as its {@code time:} or {@code datetime:} base: a time, a local time, a local datetime
   * or a datetime. Anything else is refused.
   */
  public static LocalTime timeOf(final Object value) {
    return switch (value) {
      case CypherLocalTime lt -> lt.getValue();
      case CypherTime t -> t.getValue().toLocalTime();
      case CypherLocalDateTime ldt -> ldt.getValue().toLocalTime();
      case CypherDateTime dt -> dt.getValue().toLocalTime();
      case CypherDate ignored -> throw new IllegalArgumentException("Cannot get the time of: " + value);
      case null, default -> throw new IllegalArgumentException("Cannot construct time from: " + describe(value));
    };
  }

  /**
   * The local date and time of a temporal a map names as its {@code datetime:} base: a local datetime or a datetime, the two
   * that have both. Anything else is refused.
   */
  public static LocalDateTime dateTimeOf(final Object value) {
    return switch (value) {
      case CypherLocalDateTime ldt -> ldt.getValue();
      case CypherDateTime dt -> dt.getValue().toLocalDateTime();
      case CypherDate ignored -> throw new IllegalArgumentException("Cannot get the time of: " + value);
      case CypherLocalTime ignored -> throw new IllegalArgumentException("Cannot get the date of: " + value);
      case CypherTime ignored -> throw new IllegalArgumentException("Cannot get the date of: " + value);
      case null, default -> throw new IllegalArgumentException("Cannot construct date time from: " + describe(value));
    };
  }

  /**
   * The value of an epoch field: a whole number. A fraction used to be truncated ({@code epochSeconds: 1.5} was 1 second), a
   * String parsed.
   */
  public static long epochValue(final String name, final Object value) {
    if (!(value instanceof Number))
      throw new IllegalArgumentException("Cannot construct date time from: " + describe(value));
    return TemporalUtil.toLongField(name, value);
  }

  /**
   * The text of the map's {@code timezone}. A null timezone names no zone and is refused, as Neo4j refuses it.
   */
  public static String timezoneText(final Map<String, Object> map) {
    final Object timezone = map.get("timezone");
    if (timezone == null)
      throw new IllegalArgumentException("Cannot convert to ZoneId: null");
    return timezone.toString();
  }

  static String describe(final Object value) {
    if (value == null)
      return "null";
    if (value instanceof String s)
      return "String(\"" + s + "\")";
    if (value instanceof CypherTemporalValue)
      return value.toString();
    return value.getClass().getSimpleName() + "(" + value + ")";
  }

  /**
   * {@code date} moved to the given day of its quarter, checked against the days that quarter has (90 to 92).
   */
  static LocalDate withDayOfQuarter(final LocalDate date, final int dayOfQuarter) {
    try {
      IsoFields.DAY_OF_QUARTER.rangeRefinedBy(date).checkValidValue(dayOfQuarter, IsoFields.DAY_OF_QUARTER);
    } catch (final DateTimeException e) {
      throw new DateTimeException(
          "Quarter " + date.get(IsoFields.QUARTER_OF_YEAR) + " of " + date.getYear() + " only has " + IsoFields.DAY_OF_QUARTER.rangeRefinedBy(
              date).getMaximum() + " days.");
    }
    return date.with(IsoFields.DAY_OF_QUARTER, dayOfQuarter);
  }

  private static LocalDate withQuarter(final LocalDate firstOfYear, final int quarter, final int dayOfQuarter) {
    IsoFields.QUARTER_OF_YEAR.range().checkValidValue(quarter, IsoFields.QUARTER_OF_YEAR);
    return withDayOfQuarter(firstOfYear.withMonth((quarter - 1) * 3 + 1), dayOfQuarter);
  }

  private static int weeksIn(final int weekBasedYear) {
    return (int) WeekFields.ISO.weekOfWeekBasedYear().rangeRefinedBy(LocalDate.of(weekBasedYear, 1, 4)).getMaximum();
  }

  private static void requireBefore(final int value, final String name, final int previous, final String previousName) {
    if (value != ABSENT && previous == ABSENT)
      throw new IllegalArgumentException(name + " cannot be specified without " + previousName);
  }

  private static void requireGiven(final int value, final String name) {
    if (value == ABSENT)
      throw new IllegalArgumentException(name + " must be specified");
  }

  /**
   * The value of a field, {@link #ABSENT} when the map does not give it, {@code whenNull} when it gives null.
   */
  static int field(final Map<String, Object> map, final String name, final int whenNull) {
    final Object value = map.get(name);
    if (value == null)
      return map.containsKey(name) ? whenNull : ABSENT;
    final int field = TemporalUtil.toIntField(name, value);
    // No field has this value in its range: given explicitly, it must not read as a field the map omits
    if (field == ABSENT)
      throw new IllegalArgumentException("Invalid value for " + name + ": " + value);
    return field;
  }
}
