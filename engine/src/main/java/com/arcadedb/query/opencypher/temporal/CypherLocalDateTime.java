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
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.temporal.IsoFields;
import java.time.temporal.WeekFields;
import java.util.Map;
import java.util.Objects;

import static com.arcadedb.query.opencypher.temporal.CypherDate.toInt;

/**
 * OpenCypher LocalDateTime value wrapping java.time.LocalDateTime.
 */
public class CypherLocalDateTime implements CypherTemporalValue {
  private final LocalDateTime value;

  public CypherLocalDateTime(final LocalDateTime value) {
    this.value = value;
  }

  public static CypherLocalDateTime now() {
    return new CypherLocalDateTime(LocalDateTime.now());
  }

  public static CypherLocalDateTime parse(final String str) {
    return new CypherLocalDateTime(parseLocalDateTime(str));
  }

  /**
   * Parse a local datetime string, supporting compact ISO 8601 formats.
   * Splits on 'T' to handle date and time parts separately.
   */
  static LocalDateTime parseLocalDateTime(final String str) {
    final int tIdx = str.indexOf('T');
    if (tIdx < 0)
      // Date only → at start of day
      return CypherDate.parseLocalDate(str).atStartOfDay();

    final String datePart = str.substring(0, tIdx);
    final String timePart = str.substring(tIdx + 1);

    final LocalDate date = CypherDate.parseLocalDate(datePart);
    final LocalTime time = CypherLocalTime.parseLocalTime(timePart);
    return LocalDateTime.of(date, time);
  }

  public static CypherLocalDateTime fromMap(final Map<String, Object> map) {
    // Check for base localdatetime/datetime
    if (map.containsKey("datetime") || map.containsKey("localdatetime")) {
      final Object dtVal = map.containsKey("datetime") ? map.get("datetime") : map.get("localdatetime");
      LocalDateTime base = null;
      if (dtVal instanceof CypherLocalDateTime)
        base = ((CypherLocalDateTime) dtVal).getValue();
      else if (dtVal instanceof CypherDateTime)
        base = ((CypherDateTime) dtVal).getValue().toLocalDateTime();
      if (base != null) {
        LocalDateTime result = base;
        if (map.containsKey("year"))
          result = result.withYear(toInt(map.get("year")));
        if (map.containsKey("month"))
          result = result.withMonth(toInt(map.get("month")));
        if (map.containsKey("day"))
          result = result.withDayOfMonth(toInt(map.get("day")));
        if (map.containsKey("hour"))
          result = result.withHour(toInt(map.get("hour")));
        if (map.containsKey("minute"))
          result = result.withMinute(toInt(map.get("minute")));
        if (map.containsKey("second"))
          result = result.withSecond(toInt(map.get("second")));
        result = result.withNano(TemporalUtil.computeNanos(map, result.getNano()));
        return new CypherLocalDateTime(result);
      }
    }

    // Date part
    final LocalDate datePart = CypherDate.fromMap(map).getValue();

    // Time part — check if there's a base temporal with time
    int hour = 0, minute = 0, second = 0;
    int nanos = 0;
    if (map.containsKey("time")) {
      final Object timeVal = map.get("time");
      if (timeVal instanceof CypherLocalTime clt) {
        hour = clt.getValue().getHour();
        minute = clt.getValue().getMinute();
        second = clt.getValue().getSecond();
        nanos = clt.getValue().getNano();
      } else if (timeVal instanceof CypherTime ct) {
        hour = ct.getValue().getHour();
        minute = ct.getValue().getMinute();
        second = ct.getValue().getSecond();
        nanos = ct.getValue().getNano();
      } else if (timeVal instanceof CypherLocalDateTime cldt) {
        hour = cldt.getValue().getHour();
        minute = cldt.getValue().getMinute();
        second = cldt.getValue().getSecond();
        nanos = cldt.getValue().getNano();
      } else if (timeVal instanceof CypherDateTime cdt) {
        hour = cdt.getValue().getHour();
        minute = cdt.getValue().getMinute();
        second = cdt.getValue().getSecond();
        nanos = cdt.getValue().getNano();
      }
    }
    if (map.containsKey("hour"))
      hour = toInt(map.get("hour"));
    if (map.containsKey("minute"))
      minute = toInt(map.get("minute"));
    if (map.containsKey("second"))
      second = toInt(map.get("second"));
    nanos = TemporalUtil.computeNanos(map, nanos);

    return new CypherLocalDateTime(LocalDateTime.of(datePart, LocalTime.of(hour, minute, second, nanos)));
  }

  public LocalDateTime getValue() {
    return value;
  }

  @Override
  public Object getTemporalProperty(final String name) {
    // Date properties
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
      // Time properties
      case "hour" -> (long) value.getHour();
      case "minute" -> (long) value.getMinute();
      case "second" -> (long) value.getSecond();
      case "millisecond" -> (long) (value.getNano() / 1_000_000);
      case "microsecond" -> (long) (value.getNano() / 1_000);
      case "nanosecond" -> (long) value.getNano();
      default -> null;
    };
  }

  @Override
  public int compareTo(final CypherTemporalValue other) {
    if (other instanceof CypherLocalDateTime)
      return value.compareTo(((CypherLocalDateTime) other).value);
    throw new IllegalArgumentException("Cannot compare LocalDateTime with " + other.getClass().getSimpleName());
  }

  @Override
  public String toString() {
    // Cypher format uses 'T' separator
    return value.toString();
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) return true;
    if (!(obj instanceof CypherLocalDateTime other)) return false;
    return value.equals(other.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(value);
  }
}
