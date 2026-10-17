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

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.IsoFields;
import java.time.temporal.WeekFields;
import java.util.Map;
import java.util.Objects;

import static com.arcadedb.query.opencypher.temporal.CypherDate.toInt;
import static com.arcadedb.query.opencypher.temporal.CypherDate.toLong;

/**
 * OpenCypher DateTime value wrapping java.time.ZonedDateTime.
 * Represents a date and time with timezone.
 */
public class CypherDateTime implements CypherTemporalValue {
  private final ZonedDateTime value;
  // True for a value read out of a column that stores no zone: the zone it carries was materialized, not chosen, so in
  // a comparison it adopts the other operand's zone, as a java.util.Date parameter does (issue #9325). Not part of the
  // value's identity: equals/hashCode ignore it. Set only on a stored read; a value derived from it (truncated, plus a
  // duration) is a new, ordinary datetime.
  private final boolean       zoneless;

  public CypherDateTime(final ZonedDateTime value) {
    this(value, false);
  }

  private CypherDateTime(final ZonedDateTime value, final boolean zoneless) {
    this.value = value;
    this.zoneless = zoneless;
  }

  /**
   * Wraps a datetime read from storage, where no zone is kept and the one on {@code value} is a materialized default.
   */
  public static CypherDateTime ofStored(final ZonedDateTime value) {
    return new CypherDateTime(value, true);
  }

  public boolean isZoneless() {
    return zoneless;
  }

  public static CypherDateTime now() {
    return new CypherDateTime(ZonedDateTime.now());
  }

  public static CypherDateTime parse(final String str) {
    // Extract named timezone in brackets if present: 2015-07-21T21:40:32[Europe/London]
    String input = str;
    ZoneId namedZone = null;
    final int bracketIdx = input.indexOf('[');
    if (bracketIdx >= 0) {
      final int endBracket = input.indexOf(']', bracketIdx);
      if (endBracket > bracketIdx) {
        namedZone = TemporalUtil.parseZone(input.substring(bracketIdx + 1, endBracket));
        input = input.substring(0, bracketIdx);
      }
    }

    // Normalize compact timezone offsets: +0200 → +02:00
    input = TemporalUtil.normalizeOffsetInString(input);

    // Find offset marker
    final int tIdx = input.indexOf('T');
    if (tIdx < 0) {
      // Date only with possible offset
      final LocalDate date = CypherDate.parseLocalDate(input);
      final ZoneId zone = namedZone != null ? namedZone : ZoneOffset.UTC;
      return new CypherDateTime(date.atStartOfDay(zone));
    }

    // Check if there's an offset in the time part
    final String afterT = input.substring(tIdx + 1);
    final int offsetIdx = findOffsetIndex(afterT);

    if (offsetIdx >= 0) {
      // Has offset: parse as standard datetime with offset
      try {
        ZonedDateTime parsed = ZonedDateTime.parse(input);
        if (namedZone != null)
          parsed = parsed.withZoneSameLocal(namedZone);
        return new CypherDateTime(parsed);
      } catch (final Exception e) {
        // Fall through to manual parsing
      }
      // Manual parsing for compact formats
      final String datePart = input.substring(0, tIdx);
      final String timePart = afterT.substring(0, offsetIdx);
      final String offsetPart = afterT.substring(offsetIdx);

      final LocalDate date = CypherDate.parseLocalDate(datePart);
      final LocalTime time = CypherLocalTime.parseLocalTime(timePart);
      final ZoneOffset offset = "Z".equalsIgnoreCase(offsetPart) ? ZoneOffset.UTC : ZoneOffset.of(offsetPart);
      final ZoneId zone = namedZone != null ? namedZone : offset;
      return new CypherDateTime(ZonedDateTime.of(date, time, zone));
    }

    // No offset in the time part
    final String datePart = input.substring(0, tIdx);
    final String timePart = afterT;
    final LocalDate date = CypherDate.parseLocalDate(datePart);
    final LocalTime time = CypherLocalTime.parseLocalTime(timePart);
    final ZoneId zone = namedZone != null ? namedZone : ZoneOffset.UTC;
    return new CypherDateTime(ZonedDateTime.of(date, time, zone));
  }

  private static int findOffsetIndex(final String timeStr) {
    // Look for Z, +, or - that indicates timezone offset
    // Must skip the initial characters (could be time digits)
    for (int i = timeStr.length() - 1; i >= 1; i--) {
      final char c = timeStr.charAt(i);
      if (c == 'Z' || c == 'z')
        return i;
      // + or - after at least one digit
      if ((c == '+' || c == '-') && i >= 2 && Character.isDigit(timeStr.charAt(i - 1)))
        return i;
    }
    return -1;
  }

  public static CypherDateTime fromEpoch(final long seconds, final long nanosAdjustment) {
    final Instant instant = Instant.ofEpochSecond(seconds, nanosAdjustment);
    return new CypherDateTime(ZonedDateTime.ofInstant(instant, ZoneOffset.UTC));
  }

  public static CypherDateTime fromEpochMillis(final long millis) {
    final Instant instant = Instant.ofEpochMilli(millis);
    return new CypherDateTime(ZonedDateTime.ofInstant(instant, ZoneOffset.UTC));
  }

  public static CypherDateTime fromMap(final Map<String, Object> map) {
    // Epoch-based construction: the instant, read in the map's timezone (UTC without one), then every field the map also
    // gives, as Neo4j applies them. The timezone used to be ignored, so datetime({epochSeconds: 0, timezone: '+05:30'}) was
    // midnight UTC instead of 05:30 at +05:30 (issue #10199)
    final boolean epochMillis = map.containsKey("epochMillis");
    if (epochMillis || map.containsKey("epochSeconds")) {
      if (epochMillis && map.containsKey("epochSeconds"))
        throw new IllegalArgumentException("epochMillis cannot be selected together with epochSeconds");
      final Instant instant = epochMillis ?
          Instant.ofEpochMilli(toLong(map.get("epochMillis"))) :
          Instant.ofEpochSecond(toLong(map.get("epochSeconds")));
      final ZoneId zone = map.containsKey("timezone") ? TemporalUtil.parseZone(map.get("timezone").toString()) : ZoneOffset.UTC;
      return new CypherDateTime(withFieldOverrides(instant.atZone(zone), map));
    }

    // Check for base datetime
    if (map.containsKey("datetime")) {
      final Object dtVal = map.get("datetime");
      ZonedDateTime base = null;
      boolean baseHasTimezone = false;
      if (dtVal instanceof CypherDateTime) {
        base = ((CypherDateTime) dtVal).getValue();
        baseHasTimezone = true;
      } else if (dtVal instanceof CypherLocalDateTime)
        base = ((CypherLocalDateTime) dtVal).getValue().atZone(ZoneOffset.UTC);
      if (base != null) {
        // Apply timezone conversion FIRST if specified
        if (map.containsKey("timezone")) {
          final ZoneId newZone = TemporalUtil.parseZone(map.get("timezone").toString());
          if (baseHasTimezone)
            base = base.withZoneSameInstant(newZone);
          else
            base = base.withZoneSameLocal(newZone);
        }
        // Individual field overrides apply on the (possibly timezone-converted) base
        return new CypherDateTime(withFieldOverrides(base, map));
      }
    }

    // Date part
    final LocalDate datePart = CypherDate.fromMap(map).getValue();

    // Time part — check if there's a base temporal with time
    int hour = 0, minute = 0, second = 0;
    int nanos = 0;
    ZoneId timeZone = null; // timezone from time source (null = no timezone)
    boolean timeHasTimezone = false;
    if (map.containsKey("time")) {
      final Object timeVal = map.get("time");
      if (timeVal instanceof CypherTime ct) {
        hour = ct.getValue().getHour();
        minute = ct.getValue().getMinute();
        second = ct.getValue().getSecond();
        nanos = ct.getValue().getNano();
        timeZone = ct.getValue().getOffset();
        timeHasTimezone = true;
      } else if (timeVal instanceof CypherLocalTime clt) {
        hour = clt.getValue().getHour();
        minute = clt.getValue().getMinute();
        second = clt.getValue().getSecond();
        nanos = clt.getValue().getNano();
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
        timeZone = cdt.getValue().getZone();
        timeHasTimezone = true;
      }
    }
    if (map.containsKey("hour"))
      hour = toInt(map.get("hour"));
    if (map.containsKey("minute"))
      minute = toInt(map.get("minute"));
    if (map.containsKey("second"))
      second = toInt(map.get("second"));
    nanos = TemporalUtil.computeNanos(map, nanos);

    // Determine timezone: time source timezone → explicit timezone override
    final ZoneId initialZone = timeZone != null ? timeZone : ZoneOffset.UTC;
    ZonedDateTime result = ZonedDateTime.of(datePart, LocalTime.of(hour, minute, second, nanos), initialZone);

    if (map.containsKey("timezone")) {
      final ZoneId newZone = TemporalUtil.parseZone(map.get("timezone").toString());
      if (timeHasTimezone)
        result = result.withZoneSameInstant(newZone);
      else
        result = result.withZoneSameLocal(newZone);
    }

    return new CypherDateTime(result);
  }

  /**
   * {@code base} with the fields {@code map} gives in place of its own, resolved together (see
   * {@link TemporalUtil#withFieldOverrides(LocalDateTime, Map)}), in its zone and keeping its offset where the local time
   * allows two.
   */
  private static ZonedDateTime withFieldOverrides(final ZonedDateTime base, final Map<String, Object> map) {
    return ZonedDateTime.ofLocal(TemporalUtil.withFieldOverrides(base.toLocalDateTime(), map), base.getZone(), base.getOffset());
  }

  public ZonedDateTime getValue() {
    return value;
  }

  @Override
  public Object getTemporalProperty(final String name) {
    return switch (name) {
      // Date properties
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
      // Timezone properties
      case "timezone" -> value.getZone().toString();
      case "offset" -> value.getOffset().toString();
      case "offsetMinutes" -> (long) (value.getOffset().getTotalSeconds() / 60);
      case "offsetSeconds" -> (long) value.getOffset().getTotalSeconds();
      // Epoch properties
      case "epochSeconds" -> value.toEpochSecond();
      case "epochMillis" -> value.toInstant().toEpochMilli();
      default -> null;
    };
  }

  @Override
  public int compareTo(final CypherTemporalValue other) {
    if (other instanceof CypherDateTime cdt) {
      // openCypher order: by instant, then by offset (west to east), then by zone name. Stopping at the instant made
      // two values = called equal hash and group apart, because equals/hashCode compare the zone (issue #8300).
      final int cmp = value.toInstant().compareTo(cdt.value.toInstant());
      if (cmp != 0)
        return cmp;
      final int offsetCmp = Integer.compare(value.getOffset().getTotalSeconds(), cdt.value.getOffset().getTotalSeconds());
      return offsetCmp != 0 ? offsetCmp : value.getZone().getId().compareTo(cdt.value.getZone().getId());
    }
    // Cross-type with naive LocalDateTime: treat it as UTC, mirroring datetime(localDatetimeValue), and at the same
    // instant sort the LocalDateTime first. This is the total order ORDER BY needs: answering 0 made a local value
    // "equal" to two zoned values that are not equal to each other. The = and < operators keep comparing the two
    // types by instant only, in ComparisonExpression.
    if (other instanceof CypherLocalDateTime cld) {
      final int cmp = value.toInstant().compareTo(cld.getValue().toInstant(ZoneOffset.UTC));
      return cmp != 0 ? cmp : 1;
    }
    throw new IllegalArgumentException("Cannot compare DateTime with " + other.getClass().getSimpleName());
  }

  @Override
  public String toString() {
    // Format: 1984-10-11T12:31:14.645876123+01:00[Europe/Stockholm]
    // Use compact form: omit seconds when zero, like Java's LocalDateTime.toString()
    final String datePart = value.toLocalDate().toString();
    final String timePart = value.toLocalTime().toString(); // Compact: omits :00 seconds if zero
    final String offsetPart = value.getOffset().toString();
    final String base = datePart + "T" + timePart + ("Z".equals(offsetPart) ? "Z" : offsetPart);
    final ZoneId zone = value.getZone();
    if (zone instanceof ZoneOffset)
      return base;
    // Named timezone: include both offset and zone
    return base + "[" + zone.getId() + "]";
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) return true;
    if (!(obj instanceof CypherDateTime other)) return false;
    return value.equals(other.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(value);
  }
}
