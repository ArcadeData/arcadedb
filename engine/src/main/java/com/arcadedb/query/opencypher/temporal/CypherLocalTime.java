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
import java.util.Map;
import java.util.Objects;
import java.util.Set;


/**
 * OpenCypher LocalTime value wrapping java.time.LocalTime.
 */
public class CypherLocalTime implements CypherTemporalValue {
  private static final Set<String> TIME_SELECTIONS = Set.of("time");

  private final LocalTime value;

  public CypherLocalTime(final LocalTime value) {
    this.value = value;
  }

  public static CypherLocalTime now() {
    return new CypherLocalTime(LocalTime.now());
  }

  public static CypherLocalTime parse(final String str) {
    return new CypherLocalTime(parseLocalTime(str));
  }

  /**
   * Parses a local time in the ISO 8601 forms Neo4j accepts, see {@link TemporalParser} (issue #12298).
   */
  static LocalTime parseLocalTime(final String str) {
    return TemporalParser.parseLocalTime(str);
  }

  /**
   * A local time from a map of fields, optionally adjusting the {@code time:} base the map names; see
   * {@link TemporalMapFields} (issue #12297).
   */
  public static CypherLocalTime fromMap(final Map<String, Object> map) {
    if (map.containsKey("timezone"))
      throw new IllegalArgumentException("Cannot assign time zone if also assigning other fields.");
    TemporalMapFields.checkKeys(map, false, true, TIME_SELECTIONS);
    final LocalTime base = map.containsKey("time") ? TemporalMapFields.timeOf(map.get("time")) : null;
    return new CypherLocalTime(TemporalMapFields.resolveTime(map, base));
  }

  public LocalTime getValue() {
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
      default -> null;
    };
  }

  @Override
  public int compareTo(final CypherTemporalValue other) {
    if (other instanceof CypherLocalTime)
      return value.compareTo(((CypherLocalTime) other).value);
    throw new IllegalArgumentException("Cannot compare LocalTime with " + other.getClass().getSimpleName());
  }

  @Override
  public String toString() {
    // Java's LocalTime.toString() already does proper formatting:
    // HH:MM if seconds and nanos are 0, HH:MM:SS if nanos are 0, etc.
    return value.toString();
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) return true;
    if (!(obj instanceof CypherLocalTime other)) return false;
    return value.equals(other.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(value);
  }
}
