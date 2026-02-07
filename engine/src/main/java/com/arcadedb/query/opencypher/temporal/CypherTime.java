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

import static com.arcadedb.query.opencypher.temporal.CypherDate.toInt;

/**
 * OpenCypher Time value wrapping java.time.OffsetTime.
 * Represents a time with timezone offset.
 */
public class CypherTime implements CypherTemporalValue {
  private final OffsetTime value;

  public CypherTime(final OffsetTime value) {
    this.value = value;
  }

  public static CypherTime now() {
    return new CypherTime(OffsetTime.now(ZoneOffset.UTC));
  }

  public static CypherTime parse(final String str) {
    // Split time part from offset
    final int offsetIdx = findOffsetIndex(str);
    if (offsetIdx < 0) {
      // No timezone offset — default to UTC for deterministic behavior
      final LocalTime localTime = CypherLocalTime.parseLocalTime(str);
      return new CypherTime(OffsetTime.of(localTime, ZoneOffset.UTC));
    }

    final String timePart = str.substring(0, offsetIdx);
    final String offsetPart = TemporalUtil.normalizeOffsetInString(str.substring(offsetIdx));

    final LocalTime localTime = CypherLocalTime.parseLocalTime(timePart);
    final ZoneOffset offset = "Z".equalsIgnoreCase(offsetPart) ? ZoneOffset.UTC : ZoneOffset.of(offsetPart);
    return new CypherTime(OffsetTime.of(localTime, offset));
  }

  private static int findOffsetIndex(final String str) {
    // Search from end for offset indicator
    for (int i = str.length() - 1; i >= 0; i--) {
      final char c = str.charAt(i);
      if (c == 'Z' || c == 'z')
        return i;
      if ((c == '+' || c == '-') && i > 0)
        return i;
    }
    return -1;
  }

  public static CypherTime fromMap(final Map<String, Object> map) {
    // Check for base time
    OffsetTime base = null;
    if (map.containsKey("time")) {
      final Object timeVal = map.get("time");
      if (timeVal instanceof CypherTime)
        base = ((CypherTime) timeVal).value;
      else if (timeVal instanceof CypherLocalTime)
        base = ((CypherLocalTime) timeVal).getValue().atOffset(ZoneOffset.UTC);
      else if (timeVal instanceof CypherDateTime)
        base = ((CypherDateTime) timeVal).getValue().toOffsetDateTime().toOffsetTime();
      else if (timeVal instanceof CypherLocalDateTime)
        base = ((CypherLocalDateTime) timeVal).getValue().toLocalTime().atOffset(ZoneOffset.UTC);
    }

    final int hour = map.containsKey("hour") ? toInt(map.get("hour")) : (base != null ? base.getHour() : 0);
    final int minute = map.containsKey("minute") ? toInt(map.get("minute")) : (base != null ? base.getMinute() : 0);
    final int second = map.containsKey("second") ? toInt(map.get("second")) : (base != null ? base.getSecond() : 0);
    final int nanos = TemporalUtil.computeNanos(map, base != null ? base.getNano() : 0);

    ZoneOffset offset = base != null ? base.getOffset() : ZoneOffset.UTC;
    if (map.containsKey("timezone")) {
      final ZoneOffset newOffset = TemporalUtil.parseOffset(map.get("timezone").toString());
      if (base != null && !map.containsKey("hour") && !map.containsKey("minute") && !map.containsKey("second")) {
        // Convert time to new timezone (same instant, different representation)
        final OffsetTime converted = base.withOffsetSameInstant(newOffset);
        return new CypherTime(converted);
      }
      offset = newOffset;
    }

    return new CypherTime(OffsetTime.of(hour, minute, second, nanos, offset));
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
