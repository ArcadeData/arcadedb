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

import static com.arcadedb.query.opencypher.temporal.CypherDate.toInt;
import static com.arcadedb.query.opencypher.temporal.CypherDate.toLong;

/**
 * OpenCypher LocalTime value wrapping java.time.LocalTime.
 */
public class CypherLocalTime implements CypherTemporalValue {
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
   * Parse a local time string, handling compact ISO 8601 formats:
   * - Standard: 12:31:14, 12:31
   * - Compact: 123114, 1231, 12
   * - With fractional seconds: 12:31:14.645
   */
  static LocalTime parseLocalTime(final String str) {
    // Compact format without colons: 214032, 2140, 21
    if (str.matches("^\\d{6}(\\.\\d+)?$")) {
      // HHMMSS[.frac]
      final String time = str.substring(0, 2) + ":" + str.substring(2, 4) + ":" + str.substring(4);
      return LocalTime.parse(time);
    }
    if (str.matches("^\\d{4}$")) {
      // HHMM
      return LocalTime.of(Integer.parseInt(str.substring(0, 2)), Integer.parseInt(str.substring(2, 4)));
    }
    if (str.matches("^\\d{2}$")) {
      // HH
      return LocalTime.of(Integer.parseInt(str), 0);
    }
    // Standard with missing seconds
    final String normalized = TemporalUtil.normalizeLocalTimeString(str);
    return LocalTime.parse(normalized);
  }

  public static CypherLocalTime fromMap(final Map<String, Object> map) {
    // Check for base time
    LocalTime base = null;
    if (map.containsKey("time")) {
      final Object timeVal = map.get("time");
      if (timeVal instanceof CypherLocalTime)
        base = ((CypherLocalTime) timeVal).value;
      else if (timeVal instanceof CypherTime)
        base = ((CypherTime) timeVal).getValue().toLocalTime();
      else if (timeVal instanceof CypherLocalDateTime)
        base = ((CypherLocalDateTime) timeVal).getValue().toLocalTime();
      else if (timeVal instanceof CypherDateTime)
        base = ((CypherDateTime) timeVal).getValue().toLocalTime();
    }

    final int hour = map.containsKey("hour") ? toInt(map.get("hour")) : (base != null ? base.getHour() : 0);
    final int minute = map.containsKey("minute") ? toInt(map.get("minute")) : (base != null ? base.getMinute() : 0);
    final int second = map.containsKey("second") ? toInt(map.get("second")) : (base != null ? base.getSecond() : 0);
    int nanos = base != null ? base.getNano() : 0;
    if (map.containsKey("nanosecond"))
      nanos = toInt(map.get("nanosecond"));
    else if (map.containsKey("microsecond"))
      nanos = toInt(map.get("microsecond")) * 1000;
    else if (map.containsKey("millisecond"))
      nanos = toInt(map.get("millisecond")) * 1_000_000;

    return new CypherLocalTime(LocalTime.of(hour, minute, second, nanos));
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
