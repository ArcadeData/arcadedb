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

import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.Objects;
import java.util.Set;


/**
 * OpenCypher Time value wrapping java.time.OffsetTime.
 * Represents a time with timezone offset.
 */
public class CypherTime implements CypherTemporalValue {
  private static final Set<String> TIME_SELECTIONS = Set.of("time", "timezone");

  private final OffsetTime value;

  public CypherTime(final OffsetTime value) {
    this.value = value;
  }

  public static CypherTime now() {
    return new CypherTime(OffsetTime.now(ZoneOffset.UTC));
  }

  /**
   * Parses a time in the ISO 8601 forms Neo4j accepts, UTC without an offset, see {@link TemporalParser} (issue #12298).
   */
  public static CypherTime parse(final String str) {
    return new CypherTime(TemporalParser.parseTime(str));
  }



  /**
   * A time from a map of fields, optionally adjusting the {@code time:} base the map names, in the map's {@code timezone};
   * see {@link TemporalMapFields} (issue #12297). A map holding nothing but a {@code timezone} is the current time in that
   * zone, which needs the statement clock and is answered by the {@code time()} function.
   */
  public static CypherTime fromMap(final Map<String, Object> map) {
    TemporalMapFields.checkKeys(map, false, true, TIME_SELECTIONS);
    OffsetTime base = null;
    boolean baseHasTimezone = false;
    if (map.containsKey("time")) {
      final Object timeVal = map.get("time");
      if (timeVal instanceof CypherTime time) {
        base = time.value;
        baseHasTimezone = true;
      } else if (timeVal instanceof CypherDateTime dateTime) {
        base = dateTime.getValue().toOffsetDateTime().toOffsetTime();
        baseHasTimezone = true;
      } else
        base = TemporalMapFields.timeOf(timeVal).atOffset(ZoneOffset.UTC);
    }

    if (map.containsKey("timezone")) {
      final ZoneOffset newOffset = TemporalUtil.parseOffset(map.get("timezone").toString());
      // The fields apply in the base's own offset, then a base that has one moves to the new offset at the same instant, as
      // Neo4j does: {time: time('10:00+02:00'), timezone: '+01:00', hour: 3} is 02:00+01:00 (issue #12297)
      if (base != null && baseHasTimezone)
        return new CypherTime(
            OffsetTime.of(requireTime(TemporalMapFields.resolveTime(map, base.toLocalTime())), base.getOffset()).withOffsetSameInstant(newOffset));
      return new CypherTime(OffsetTime.of(requireTime(TemporalMapFields.resolveTime(map, base != null ? base.toLocalTime() : null)), newOffset));
    }
    return new CypherTime(OffsetTime.of(requireTime(TemporalMapFields.resolveTime(map, base != null ? base.toLocalTime() : null)),
        base != null ? base.getOffset() : ZoneOffset.UTC));
  }

  private static LocalTime requireTime(final LocalTime time) {
    if (time == null)
      throw new IllegalArgumentException("hour must be specified");
    return time;
  }

  public OffsetTime getValue() {
    return value;
  }

  @Override
  public Object getTemporalProperty(final String name) {
    return switch (name) {
      case "hour" -> (long) value.getHour();
      case "minute" -> (long) value.getMinute();
      case "second" -> (long) value.getSecond();
      case "millisecond" -> (long) (value.getNano() / 1_000_000);
      case "microsecond" -> (long) (value.getNano() / 1_000);
      case "nanosecond" -> (long) value.getNano();
      case "timezone" -> value.getOffset().toString();
      case "offset" -> value.getOffset().toString();
      case "offsetMinutes" -> (long) (value.getOffset().getTotalSeconds() / 60);
      case "offsetSeconds" -> (long) value.getOffset().getTotalSeconds();
      default -> null;
    };
  }

  @Override
  public int compareTo(final CypherTemporalValue other) {
    if (other instanceof CypherTime)
      return value.compareTo(((CypherTime) other).value);
    throw new IllegalArgumentException("Cannot compare Time with " + other.getClass().getSimpleName());
  }

  @Override
  public String toString() {
    return value.toString();
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) return true;
    if (!(obj instanceof CypherTime other)) return false;
    return value.equals(other.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(value);
  }
}
