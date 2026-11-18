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
import java.util.Set;

/**
 * OpenCypher Date value wrapping java.time.LocalDate.
 * Supports construction from maps and ISO 8601 strings.
 */
public class CypherDate implements CypherTemporalValue {
  private static final Set<String> DATE_SELECTIONS = Set.of("date");

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
   * Parses a date in the ISO 8601 forms Neo4j accepts, see {@link TemporalParser} (issue #12298).
   */
  static LocalDate parseLocalDate(final String str) {
    return TemporalParser.parseDate(str);
  }

  /**
   * A date from a map of fields, optionally adjusting the {@code date:} base the map names; see {@link TemporalMapFields} for
   * how the fields resolve (issue #12297). A map holding nothing but a {@code timezone} is the current date in that zone,
   * which needs the statement clock and is answered by the {@code date()} function.
   */
  public static CypherDate fromMap(final Map<String, Object> map) {
    if (map.containsKey("timezone"))
      throw new IllegalArgumentException("Cannot assign time zone if also assigning other fields.");
    TemporalMapFields.checkKeys(map, true, false, DATE_SELECTIONS);
    final LocalDate base = map.containsKey("date") ? TemporalMapFields.dateOf(map.get("date")) : null;
    return new CypherDate(TemporalMapFields.resolveDate(map, base, false));
  }

  /**
   * The day of an ISO week date. The week is checked against the weeks its own year has: with() checks only the 1-53 every
   * week-based year may have, so week 53 of a year with 52 rolled over into the next year, where Neo4j refuses it.
   */
  static LocalDate isoWeekDate(final int year, final int week, final int dayOfWeek) {
    final LocalDate january4 = LocalDate.of(year, 1, 4); // always in week 1
    WeekFields.ISO.weekOfWeekBasedYear().rangeRefinedBy(january4).checkValidValue(week, WeekFields.ISO.weekOfWeekBasedYear());
    WeekFields.ISO.dayOfWeek().range().checkValidValue(dayOfWeek, WeekFields.ISO.dayOfWeek());
    return january4.with(WeekFields.ISO.weekOfWeekBasedYear(), week).with(WeekFields.ISO.dayOfWeek(), dayOfWeek);
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
      case "dayOfQuarter", "quarterDay" -> (long) value.get(IsoFields.DAY_OF_QUARTER);
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

  static double toDouble(final Object val) {
    if (val instanceof Number)
      return ((Number) val).doubleValue();
    return Double.parseDouble(val.toString());
  }
}
