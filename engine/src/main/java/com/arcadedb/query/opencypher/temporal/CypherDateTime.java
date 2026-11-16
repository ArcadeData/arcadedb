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
import java.util.Set;


/**
 * OpenCypher DateTime value wrapping java.time.ZonedDateTime.
 * Represents a date and time with timezone.
 */
public class CypherDateTime implements CypherTemporalValue {
  private static final Set<String> DATETIME_SELECTIONS = Set.of("date", "time", "datetime", "timezone", "epochSeconds", "epochMillis");

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

  /**
   * Parses a datetime in the ISO 8601 forms Neo4j accepts, see {@link TemporalParser}. An offset that contradicts the zone the
   * text also names is refused (issue #12298).
   */
  public static CypherDateTime parse(final String str) {
    return new CypherDateTime(TemporalParser.parseDateTime(str));
  }



  public static CypherDateTime fromEpoch(final long seconds, final long nanosAdjustment) {
    final Instant instant = Instant.ofEpochSecond(seconds, nanosAdjustment);
    return new CypherDateTime(ZonedDateTime.ofInstant(instant, ZoneOffset.UTC));
  }

  public static CypherDateTime fromEpochMillis(final long millis) {
    final Instant instant = Instant.ofEpochMilli(millis);
    return new CypherDateTime(ZonedDateTime.ofInstant(instant, ZoneOffset.UTC));
  }

  /**
   * A datetime from a map of fields: an epoch instant, or a {@code datetime:} base, or a {@code date:} and a {@code time:}
   * base, each adjusted by the fields the map gives (see {@link TemporalMapFields}, issue #12297), in the map's
   * {@code timezone}. A map holding nothing but a {@code timezone} is the current datetime in that zone, which needs the
   * statement clock and is answered by the {@code datetime()} function.
   */
  public static CypherDateTime fromMap(final Map<String, Object> map) {
    TemporalMapFields.checkKeys(map, true, true, DATETIME_SELECTIONS);

    // Epoch-based construction: the instant, read in the map's timezone (UTC without one), then every field the map also
    // gives, as Neo4j applies them. The timezone used to be ignored, so datetime({epochSeconds: 0, timezone: '+05:30'}) was
    // midnight UTC instead of 05:30 at +05:30 (issue #10199)
    final boolean epochMillis = map.containsKey("epochMillis");
    if (epochMillis || map.containsKey("epochSeconds")) {
      final String epochKey = epochMillis ? "epochMillis" : "epochSeconds";
      if (epochMillis && map.containsKey("epochSeconds"))
        throw new IllegalArgumentException("epochMillis cannot be selected together with epochSeconds.");
      if (map.containsKey("datetime"))
        throw new IllegalArgumentException(epochKey + " cannot be selected together with datetime.");
      if (map.containsKey("date") || map.containsKey("time"))
        throw new IllegalArgumentException(epochKey + " cannot be selected together with date or time.");
      final long epoch = TemporalMapFields.epochValue(epochKey, map.get(epochKey));
      final Instant instant = epochMillis ? Instant.ofEpochMilli(epoch) : Instant.ofEpochSecond(epoch);
      final ZoneId zone = map.containsKey("timezone") ? TemporalUtil.parseZone(TemporalMapFields.timezoneText(map)) : ZoneOffset.UTC;
      return new CypherDateTime(withFieldOverrides(instant.atZone(zone), map));
    }

    if (map.containsKey("datetime")) {
      if (map.containsKey("date") || map.containsKey("time"))
        throw new IllegalArgumentException("datetime cannot be selected together with date or time.");
      final Object dtVal = map.get("datetime");
      final boolean baseHasTimezone = dtVal instanceof CypherDateTime;
      // A local datetime has no zone: it is read in UTC, or in the map's timezone below
      final ZonedDateTime base = baseHasTimezone ?
          ((CypherDateTime) dtVal).getValue() :
          TemporalMapFields.dateTimeOf(dtVal).atZone(ZoneOffset.UTC);
      // The fields apply in the base's own zone, then a zoned base moves to the map's timezone at the same instant, as Neo4j
      // does: {datetime: datetime('2021-05-15T10:00+02:00'), timezone: '+01:00', hour: 3} is 02:00+01:00. Converting first
      // answered 03:00+01:00, another instant (issue #12297). A local base has no instant to keep: its overridden local
      // date-time is read in the timezone, so a gap of the zone is resolved on the final value, not on the base
      final ZonedDateTime overridden = withFieldOverrides(base, map);
      if (map.containsKey("timezone")) {
        final ZoneId newZone = TemporalUtil.parseZone(TemporalMapFields.timezoneText(map));
        return new CypherDateTime(baseHasTimezone ? overridden.withZoneSameInstant(newZone) : overridden.withZoneSameLocal(newZone));
      }
      return new CypherDateTime(overridden);
    }

    final LocalDate baseDate = map.containsKey("date") ? TemporalMapFields.dateOf(map.get("date")) : null;

    // Time part: a base with a zone gives it its zone
    LocalTime baseTime = null;
    ZoneId timeZone = null;
    boolean timeHasTimezone = false;
    if (map.containsKey("time")) {
      final Object timeVal = map.get("time");
      baseTime = TemporalMapFields.timeOf(timeVal);
      if (timeVal instanceof CypherTime ct) {
        timeZone = ct.getValue().getOffset();
        timeHasTimezone = true;
      } else if (timeVal instanceof CypherDateTime cdt) {
        timeZone = cdt.getValue().getZone();
        timeHasTimezone = true;
      }
    }

    final LocalDate datePart = TemporalMapFields.resolveDate(map, baseDate, baseTime != null || TemporalMapFields.hasTimeField(map));
    if (datePart == null)
      throw new IllegalArgumentException("year must be specified");
    final LocalTime timePart = TemporalMapFields.resolveTime(map, baseTime);

    // Determine timezone: time source timezone → explicit timezone override
    final ZoneId initialZone = timeZone != null ? timeZone : ZoneOffset.UTC;
    ZonedDateTime result = ZonedDateTime.of(datePart, timePart != null ? timePart : LocalTime.MIDNIGHT, initialZone);

    if (map.containsKey("timezone")) {
      final ZoneId newZone = TemporalUtil.parseZone(TemporalMapFields.timezoneText(map));
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
      case "dayOfQuarter", "quarterDay" -> (long) value.get(IsoFields.DAY_OF_QUARTER);
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
