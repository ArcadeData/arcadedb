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

import java.time.LocalDate;
import java.time.temporal.IsoFields;
import java.time.temporal.WeekFields;
import java.util.Map;
import java.util.Objects;

/**
 * OpenCypher Date value wrapping java.time.LocalDate.
 * Supports construction from maps and ISO 8601 strings.
 */
public class CypherDate implements CypherTemporalValue {
  private final LocalDate value;

  public CypherDate(final LocalDate value) {
    this.value = value;
  }

  public static CypherDate now() {
    return new CypherDate(LocalDate.now());
  }

  public static CypherDate parse(final String str) {
    return new CypherDate(parseLocalDate(str));
  }

  /**
   * Parse a date string supporting extended ISO 8601 formats:
   * - Standard: 2015-07-21
   * - Ordinal: 2015-202, 2015202
   * - Week: 2015-W30, 2015W30, 2015-W30-2, 2015W302
   */
  static LocalDate parseLocalDate(final String str) {
    // Week date: 2015-W30, 2015W30, 2015-W30-2, 2015W302
    if (str.contains("W")) {
      final java.util.regex.Matcher m = java.util.regex.Pattern.compile(
          "([-]?\\d{4})-?W(\\d{2})(?:-?(\\d))?").matcher(str);
      if (m.matches()) {
        final int year = Integer.parseInt(m.group(1));
        final int week = Integer.parseInt(m.group(2));
        final int dow = m.group(3) != null ? Integer.parseInt(m.group(3)) : 1;
        LocalDate d = LocalDate.of(year, 1, 4); // Jan 4 always in week 1
        d = d.with(WeekFields.ISO.weekOfWeekBasedYear(), week);
        d = d.with(WeekFields.ISO.dayOfWeek(), dow);
        return d;
      }
    }
    // Ordinal date: 2015-202 or 2015202
    if (str.matches("[-]?\\d{4}-?\\d{3}") && !str.matches("[-]?\\d{4}-\\d{2}-\\d{2}")) {
      final boolean hasHyphen = str.contains("-") && str.lastIndexOf('-') > 0;
      final String cleaned = str.replace("-", "");
      final int sign = cleaned.startsWith("-") ? -1 : 1;
      final String unsigned = cleaned.startsWith("-") ? cleaned.substring(1) : cleaned;
      final int year = sign * Integer.parseInt(unsigned.substring(0, 4));
      final int ordinal = Integer.parseInt(unsigned.substring(4));
      return LocalDate.ofYearDay(year, ordinal);
    }
    // Compact date: 20150721
    if (str.matches("[-]?\\d{8}"))
      return LocalDate.parse(str, java.time.format.DateTimeFormatter.BASIC_ISO_DATE);
    // Year-month: 2015-07
    if (str.matches("[-]?\\d{4}-\\d{2}"))
      return java.time.YearMonth.parse(str).atDay(1);
    // Compact year-month: 201507
    if (str.matches("[-]?\\d{6}"))
      return java.time.YearMonth.of(Integer.parseInt(str.substring(0, 4)), Integer.parseInt(str.substring(4, 6))).atDay(1);
    // Year only: 2015
    if (str.matches("[-]?\\d{4}"))
      return LocalDate.of(Integer.parseInt(str), 1, 1);
    // Standard ISO
    return LocalDate.parse(str);
  }

  public static CypherDate fromMap(final Map<String, Object> map) {
    // Check for base date
    LocalDate base = null;
    if (map.containsKey("date")) {
      final Object dateVal = map.get("date");
      if (dateVal instanceof CypherDate)
        base = ((CypherDate) dateVal).value;
      else if (dateVal instanceof CypherLocalDateTime)
        base = ((CypherLocalDateTime) dateVal).getValue().toLocalDate();
      else if (dateVal instanceof CypherDateTime)
        base = ((CypherDateTime) dateVal).getValue().toLocalDate();
    }

    // Week-based date construction
    if (map.containsKey("week")) {
      final int week = toInt(map.get("week"));
      int year = map.containsKey("year") ? toInt(map.get("year")) : (base != null ? base.get(WeekFields.ISO.weekBasedYear()) : LocalDate.now().getYear());
      final int dayOfWeek = map.containsKey("dayOfWeek") ? toInt(map.get("dayOfWeek")) : (base != null ? base.getDayOfWeek().getValue() : 1);

      LocalDate result = LocalDate.of(year, 1, 4); // Jan 4 is always in week 1 by ISO
      result = result.with(WeekFields.ISO.weekOfWeekBasedYear(), week);
      result = result.with(WeekFields.ISO.dayOfWeek(), dayOfWeek);
      return new CypherDate(result);
    }

    // Quarter-based date construction
    if (map.containsKey("quarter")) {
      final int quarter = toInt(map.get("quarter"));
      final int year = map.containsKey("year") ? toInt(map.get("year")) : (base != null ? base.getYear() : LocalDate.now().getYear());
      final int quarterStartMonth = (quarter - 1) * 3 + 1;
      if (map.containsKey("dayOfQuarter")) {
        final int dayOfQuarter = toInt(map.get("dayOfQuarter"));
        return new CypherDate(LocalDate.of(year, quarterStartMonth, 1).plusDays(dayOfQuarter - 1));
      }
      if (base != null && !map.containsKey("month") && !map.containsKey("day")) {
        // Preserve relative position within quarter from base
        final int baseQuarterStart = ((base.getMonthValue() - 1) / 3) * 3 + 1;
        final int monthOffset = base.getMonthValue() - baseQuarterStart;
        final int targetMonth = quarterStartMonth + monthOffset;
        return new CypherDate(LocalDate.of(year, targetMonth, base.getDayOfMonth()));
      }
      final int day = map.containsKey("day") ? toInt(map.get("day")) : 1;
      return new CypherDate(LocalDate.of(year, quarterStartMonth, day));
    }

    // Ordinal date construction
    if (map.containsKey("ordinalDay")) {
      final int ordinalDay = toInt(map.get("ordinalDay"));
      final int year = map.containsKey("year") ? toInt(map.get("year")) : (base != null ? base.getYear() : LocalDate.now().getYear());
      return new CypherDate(LocalDate.ofYearDay(year, ordinalDay));
    }

    // Standard year-month-day construction
    final int year = map.containsKey("year") ? toInt(map.get("year")) : (base != null ? base.getYear() : LocalDate.now().getYear());
    final int month = map.containsKey("month") ? toInt(map.get("month")) : (base != null ? base.getMonthValue() : 1);
    final int day = map.containsKey("day") ? toInt(map.get("day")) : (base != null ? base.getDayOfMonth() : 1);
    return new CypherDate(LocalDate.of(year, month, day));
  }

  public LocalDate getValue() {
    return value;
  }

  @Override
  public Object getTemporalProperty(final String name) {
    return switch (name) {
      case "year" -> (long) value.getYear();
      case "month" -> (long) value.getMonthValue();
      case "day" -> (long) value.getDayOfMonth();
      case "quarter" -> (long) value.get(IsoFields.QUARTER_OF_YEAR);
      case "week" -> (long) value.get(WeekFields.ISO.weekOfWeekBasedYear());
      case "weekYear" -> (long) value.get(WeekFields.ISO.weekBasedYear());
      case "dayOfWeek", "weekDay" -> (long) value.getDayOfWeek().getValue();
      case "ordinalDay" -> (long) value.getDayOfYear();
      case "dayOfQuarter" -> (long) value.get(IsoFields.DAY_OF_QUARTER);
      default -> null;
    };
  }

  @Override
  public int compareTo(final CypherTemporalValue other) {
    if (other instanceof CypherDate)
      return value.compareTo(((CypherDate) other).value);
    throw new IllegalArgumentException("Cannot compare Date with " + other.getClass().getSimpleName());
  }

  @Override
  public String toString() {
    return value.toString();
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) return true;
    if (!(obj instanceof CypherDate other)) return false;
    return value.equals(other.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(value);
  }

  static int toInt(final Object val) {
    if (val instanceof Number)
      return ((Number) val).intValue();
    return Integer.parseInt(val.toString());
  }

  static long toLong(final Object val) {
    if (val instanceof Number)
      return ((Number) val).longValue();
    return Long.parseLong(val.toString());
  }

  static double toDouble(final Object val) {
    if (val instanceof Number)
      return ((Number) val).doubleValue();
    return Double.parseDouble(val.toString());
  }
}
