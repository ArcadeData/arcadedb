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
import java.time.ZoneOffset;
import java.time.temporal.IsoFields;
import java.time.temporal.WeekFields;
import java.util.Map;
import java.util.Objects;
import java.util.Set;


/**
 * OpenCypher LocalDateTime value wrapping java.time.LocalDateTime.
 */
public class CypherLocalDateTime implements CypherTemporalValue {
  private static final Set<String> DATETIME_SELECTIONS = Set.of("date", "time", "datetime");

  private final LocalDateTime value;
  // True for a value read out of a DATETIME column, which ArcadeDB stores as an instant (epoch based) and materializes as a
  // LocalDateTime in UTC: against a zoned datetime it still compares by instant, so a filter like
  // n.createdAt > datetime('2024-01-01T00:00Z') keeps working. A localdatetime() the query builds is a Cypher local
  // datetime, which is never equal to a zoned one and not comparable with it (issue #10200). Not part of the value's
  // identity: equals/hashCode ignore it. A value derived from it (truncated, plus a duration) is an ordinary one.
  private final boolean       stored;

  public CypherLocalDateTime(final LocalDateTime value) {
    this(value, false);
  }

  private CypherLocalDateTime(final LocalDateTime value, final boolean stored) {
    this.value = value;
    this.stored = stored;
  }

  /**
   * Wraps a datetime read from storage, where it is kept as an instant: see {@link #isStored()}.
   */
  public static CypherLocalDateTime ofStored(final LocalDateTime value) {
    return new CypherLocalDateTime(value, true);
  }

  public boolean isStored() {
    return stored;
  }

  public static CypherLocalDateTime now() {
    return new CypherLocalDateTime(LocalDateTime.now());
  }

  public static CypherLocalDateTime parse(final String str) {
    return new CypherLocalDateTime(parseLocalDateTime(str));
  }

  /**
   * Parses a local datetime in the ISO 8601 forms Neo4j accepts, see {@link TemporalParser} (issue #12298).
   */
  static LocalDateTime parseLocalDateTime(final String str) {
    return TemporalParser.parseLocalDateTime(str);
  }

  /**
   * A local datetime from a map of fields, optionally adjusting a {@code datetime:} base, or a {@code date:} and a
   * {@code time:} base; see {@link TemporalMapFields} (issue #12297).
   */
  public static CypherLocalDateTime fromMap(final Map<String, Object> map) {
    if (map.containsKey("timezone"))
      throw new IllegalArgumentException("Cannot assign time zone if also assigning other fields.");
    TemporalMapFields.checkKeys(map, true, true, DATETIME_SELECTIONS);
    if (map.containsKey("datetime")) {
      if (map.containsKey("date") || map.containsKey("time"))
        throw new IllegalArgumentException("datetime cannot be selected together with date or time.");
      return new CypherLocalDateTime(TemporalUtil.withFieldOverrides(TemporalMapFields.dateTimeOf(map.get("datetime")), map));
    }

    final LocalDate baseDate = map.containsKey("date") ? TemporalMapFields.dateOf(map.get("date")) : null;
    final LocalTime baseTime = map.containsKey("time") ? TemporalMapFields.timeOf(map.get("time")) : null;
    final LocalDate date = TemporalMapFields.resolveDate(map, baseDate, baseTime != null || TemporalMapFields.hasTimeField(map));
    if (date == null)
      throw new IllegalArgumentException("year must be specified");
    final LocalTime time = TemporalMapFields.resolveTime(map, baseTime);
    return new CypherLocalDateTime(LocalDateTime.of(date, time != null ? time : LocalTime.MIDNIGHT));
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
      case "dayOfQuarter", "quarterDay" -> (long) value.get(IsoFields.DAY_OF_QUARTER);
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
    if (other instanceof CypherLocalDateTime cld)
      return value.compareTo(cld.value);
    // Cross-type with zoned DateTime: treat LocalDateTime as UTC, mirroring datetime(localDatetimeValue), and at the
    // same instant sort the LocalDateTime first: the mirror of CypherDateTime.compareTo, so the order stays total.
    if (other instanceof CypherDateTime cdt) {
      final int cmp = value.toInstant(ZoneOffset.UTC).compareTo(cdt.getValue().toInstant());
      return cmp != 0 ? cmp : -1;
    }
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
