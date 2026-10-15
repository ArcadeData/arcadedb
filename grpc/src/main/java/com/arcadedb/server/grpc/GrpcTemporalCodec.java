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
package com.arcadedb.server.grpc;

import com.arcadedb.exception.ArithmeticErrorException;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.opencypher.temporal.CypherDuration;
import com.google.protobuf.Timestamp;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Date;
import java.util.logging.Level;

/**
 * The one encoder of {@code java.time} values and durations onto a {@link GrpcValue}, shared by the server's two encoders
 * ({@code GrpcTypeConverter}, {@code ArcadeDbGrpcService}) and the client's ({@code ProtoUtils}), so that a temporal type is
 * added once and the two ends of the wire cannot drift apart (issue #9969). It lives in the module that owns the proto, the
 * same arrangement {@link GraphBatchProtocol} and {@link TransactionProtocol} make.
 * <p>
 * Two shapes travel:
 * <ul>
 *   <li>An instant - {@link Date}, {@link Instant}, {@link LocalDateTime} (UTC-anchored) and {@link LocalDate} (start of day
 *   at UTC) - as a proto {@link Timestamp} tagged {@value #DATETIME} or {@value #DATE} (issues #4149, #5045).</li>
 *   <li>A value that holds more than an instant - a {@link ZonedDateTime}, an {@link OffsetTime}, a {@link LocalTime} - as
 *   its ISO text tagged {@value #ZONED_DATETIME}, {@value #OFFSET_TIME} or {@value #LOCAL_TIME}. A {@code ZONED_DATETIME}
 *   compares by instant, then offset, then zone id, so a bare instant cannot be written back as a predicate that matches the
 *   stored value; the text is the shape the JSON and Bolt surfaces already carry.</li>
 *   <li>A {@link CypherDuration} - months, days, seconds and nanoseconds, with no epoch form - as its ISO text tagged
 *   {@value #DURATION} (issue #10034). The text is the storage encoding ({@code toString()} / {@code parse()}), which keeps
 *   all four components apart and round-trips every value, including a negative number of seconds with a positive
 *   nanosecond adjustment.</li>
 * </ul>
 * This is a wire change for {@link ZonedDateTime}: before issue #9969 it travelled as a {@code timestamp_value}, so a peer that
 * decodes it by kind now sees a {@code string_value}. A peer that ignores {@code logical_type} reads the ISO text as a plain
 * string. For {@link OffsetTime} and {@link LocalTime} the change is additive: they were already the same untagged text, and so
 * was a {@link CypherDuration}, which a peer that ignores {@code logical_type} still reads as its ISO text.
 * Tags are matched case-insensitively, as {@code ProtoUtils} matches {@value #DATE} and {@value #DATETIME}.
 */
public final class GrpcTemporalCodec {

  public static final String DATE           = "date";
  public static final String DATETIME       = "datetime";
  public static final String ZONED_DATETIME = "zoned_datetime";
  public static final String OFFSET_TIME    = "offset_time";
  public static final String LOCAL_TIME     = "local_time";
  public static final String DURATION       = "duration";

  private GrpcTemporalCodec() {
    // Utility class
  }

  /**
   * Encodes a temporal value, or returns {@code null} when {@code o} is not one of the temporal types the wire carries, so
   * the caller goes on with its own dispatch.
   */
  public static GrpcValue encode(final Object o) {
    if (o instanceof Date v)
      return timestamp(Math.floorDiv(v.getTime(), 1000L), (int) (Math.floorMod(v.getTime(), 1000L) * 1_000_000L), DATETIME);
    if (o instanceof LocalDate ld)
      return timestamp(ld.toEpochDay() * 86_400L, 0, DATE);
    if (o instanceof LocalDateTime ldt)
      return timestamp(ldt.toEpochSecond(ZoneOffset.UTC), ldt.getNano(), DATETIME);
    if (o instanceof Instant instant)
      return timestamp(instant.getEpochSecond(), instant.getNano(), DATETIME);
    if (o instanceof ZonedDateTime zdt)
      return text(DateTimeFormatter.ISO_ZONED_DATE_TIME.format(zdt), ZONED_DATETIME);
    if (o instanceof OffsetTime ot)
      return text(DateTimeFormatter.ISO_OFFSET_TIME.format(ot), OFFSET_TIME);
    if (o instanceof LocalTime lt)
      return text(DateTimeFormatter.ISO_LOCAL_TIME.format(lt), LOCAL_TIME);
    if (o instanceof CypherDuration duration)
      return text(duration.toString(), DURATION);
    return null;
  }

  /**
   * Decodes a {@code string_value}: the temporal its {@code logical_type} names, or the text itself when it names none. Text
   * that does not parse as the type it claims is returned as is rather than failing the whole row, so a hand-built message
   * from a third-party client degrades to the string it would have been before the tag existed.
   */
  public static Object decodeString(final String text, final String logicalType) {
    if (logicalType == null || logicalType.isEmpty())
      return text;
    try {
      if (ZONED_DATETIME.equalsIgnoreCase(logicalType))
        return ZonedDateTime.parse(text, DateTimeFormatter.ISO_ZONED_DATE_TIME);
      if (OFFSET_TIME.equalsIgnoreCase(logicalType))
        return OffsetTime.parse(text, DateTimeFormatter.ISO_OFFSET_TIME);
      if (LOCAL_TIME.equalsIgnoreCase(logicalType))
        return LocalTime.parse(text, DateTimeFormatter.ISO_LOCAL_TIME);
      if (DURATION.equalsIgnoreCase(logicalType))
        return CypherDuration.parse(text);
      return text;
    } catch (final DateTimeParseException | IllegalArgumentException | ArithmeticErrorException e) {
      // Logged so that a typed column rejecting the string later can be traced back to the tag that did not parse
      LogManager.instance().log(GrpcTemporalCodec.class, Level.FINE, "gRPC string_value tagged '%s' does not parse as one, kept as text: '%s'",
          logicalType, text.length() > 64 ? text.substring(0, 64) + "..." : text);
      return text;
    }
  }

  private static GrpcValue timestamp(final long seconds, final int nanos, final String logicalType) {
    return GrpcValue.newBuilder().setTimestampValue(Timestamp.newBuilder().setSeconds(seconds).setNanos(nanos).build())
        .setLogicalType(logicalType).build();
  }

  private static GrpcValue text(final String text, final String logicalType) {
    return GrpcValue.newBuilder().setStringValue(text).setLogicalType(logicalType).build();
  }
}
