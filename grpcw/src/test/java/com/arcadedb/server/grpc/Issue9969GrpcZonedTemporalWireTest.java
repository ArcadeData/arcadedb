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

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9969: a {@code ZONED_DATETIME} compares by instant, then offset, then zone id, so the gRPC wire has to carry the zone
 * rather than a bare instant; an {@code OFFSET_TIME} and a {@code LOCAL_TIME} have to arrive typed rather than as an untagged
 * string. These drive the server's {@link GrpcTypeConverter}, which every record and parameter path in the module shares
 * (vector search, time series, query parameters), and the {@link GrpcTemporalCodec} it delegates to.
 */
class Issue9969GrpcZonedTemporalWireTest {

  private static final ZonedDateTime NAMED_ZONE  = ZonedDateTime.of(2026, 1, 1, 0, 30, 0, 123_456_789, ZoneId.of("Europe/Rome"));
  private static final ZonedDateTime OFFSET_ZONE = ZonedDateTime.of(2026, 1, 1, 0, 30, 0, 0, ZoneOffset.ofHours(2));
  private static final OffsetTime    OFFSET_TIME = OffsetTime.of(0, 30, 15, 500_000_000, ZoneOffset.ofHours(2));
  private static final LocalTime     LOCAL_TIME  = LocalTime.of(23, 59, 59, 999_999_999);

  @Test
  void zonedDateTimeWithNamedZoneKeepsTheZoneId() {
    final GrpcValue encoded = GrpcTypeConverter.toGrpcValue(NAMED_ZONE);

    assertThat(encoded.getKindCase()).isEqualTo(GrpcValue.KindCase.STRING_VALUE);
    assertThat(encoded.getLogicalType()).isEqualTo(GrpcTemporalCodec.ZONED_DATETIME);
    assertThat(encoded.getStringValue()).isEqualTo("2026-01-01T00:30:00.123456789+01:00[Europe/Rome]");

    final Object decoded = GrpcTypeConverter.fromGrpcValue(encoded);
    assertThat(decoded).isInstanceOf(ZonedDateTime.class);
    // equals() on ZonedDateTime compares the zone id too: an instant-only round trip fails here
    assertThat(decoded).isEqualTo(NAMED_ZONE);
  }

  @Test
  void zonedDateTimeWithOffsetZoneKeepsTheOffset() {
    final GrpcValue encoded = GrpcTypeConverter.toGrpcValue(OFFSET_ZONE);

    assertThat(encoded.getLogicalType()).isEqualTo(GrpcTemporalCodec.ZONED_DATETIME);
    assertThat(encoded.getStringValue()).isEqualTo("2026-01-01T00:30:00+02:00");
    assertThat(GrpcTypeConverter.fromGrpcValue(encoded)).isEqualTo(OFFSET_ZONE);
  }

  @Test
  void offsetTimeArrivesTyped() {
    final GrpcValue encoded = GrpcTypeConverter.toGrpcValue(OFFSET_TIME);

    assertThat(encoded.getKindCase()).isEqualTo(GrpcValue.KindCase.STRING_VALUE);
    assertThat(encoded.getLogicalType()).isEqualTo(GrpcTemporalCodec.OFFSET_TIME);
    assertThat(GrpcTypeConverter.fromGrpcValue(encoded)).isEqualTo(OFFSET_TIME);
  }

  @Test
  void offsetTimeAtUtcAndAtANegativeOffsetRoundTrip() {
    final OffsetTime utc = OffsetTime.of(12, 0, 0, 0, ZoneOffset.UTC);
    final GrpcValue encodedUtc = GrpcTypeConverter.toGrpcValue(utc);
    assertThat(encodedUtc.getStringValue()).isEqualTo("12:00:00Z");
    assertThat(GrpcTypeConverter.fromGrpcValue(encodedUtc)).isEqualTo(utc);

    final OffsetTime negative = OffsetTime.of(23, 59, 0, 1, ZoneOffset.ofHoursMinutes(-9, -30));
    final GrpcValue encodedNegative = GrpcTypeConverter.toGrpcValue(negative);
    assertThat(encodedNegative.getStringValue()).isEqualTo("23:59:00.000000001-09:30");
    assertThat(GrpcTypeConverter.fromGrpcValue(encodedNegative)).isEqualTo(negative);
  }

  @Test
  void localTimeArrivesTyped() {
    final GrpcValue encoded = GrpcTypeConverter.toGrpcValue(LOCAL_TIME);

    assertThat(encoded.getKindCase()).isEqualTo(GrpcValue.KindCase.STRING_VALUE);
    assertThat(encoded.getLogicalType()).isEqualTo(GrpcTemporalCodec.LOCAL_TIME);
    assertThat(GrpcTypeConverter.fromGrpcValue(encoded)).isEqualTo(LOCAL_TIME);
  }

  @Test
  void temporalsInsideCollectionsAreEncodedTheSameWay() {
    final GrpcValue list = GrpcTypeConverter.toGrpcValue(List.of(NAMED_ZONE, LOCAL_TIME));
    assertThat(GrpcTypeConverter.fromGrpcValue(list)).isEqualTo(List.of(NAMED_ZONE, LOCAL_TIME));

    final GrpcValue map = GrpcTypeConverter.toGrpcValue(Map.of("t", OFFSET_TIME));
    assertThat(GrpcTypeConverter.fromGrpcValue(map)).isEqualTo(Map.of("t", OFFSET_TIME));
  }

  @Test
  void untaggedStringStaysAString() {
    final GrpcValue plain = GrpcValue.newBuilder().setStringValue("2026-01-01T00:30+02:00").build();
    assertThat(GrpcTypeConverter.fromGrpcValue(plain)).isEqualTo("2026-01-01T00:30+02:00");
  }

  @Test
  void timesWithZeroSecondsRoundTrip() {
    final LocalTime local = LocalTime.of(0, 30);
    assertThat(GrpcTypeConverter.fromGrpcValue(GrpcTypeConverter.toGrpcValue(local))).isEqualTo(local);

    final OffsetTime offset = OffsetTime.of(0, 30, 0, 0, ZoneOffset.ofHours(5));
    assertThat(GrpcTypeConverter.fromGrpcValue(GrpcTypeConverter.toGrpcValue(offset))).isEqualTo(offset);
  }

  @Test
  void tagsMatchCaseInsensitively() {
    final GrpcValue upper = GrpcValue.newBuilder().setStringValue("00:30:00+02:00").setLogicalType("OFFSET_TIME").build();
    assertThat(GrpcTypeConverter.fromGrpcValue(upper)).isEqualTo(OffsetTime.of(0, 30, 0, 0, ZoneOffset.ofHours(2)));
  }

  @Test
  void malformedTaggedTextDegradesToTheString() {
    // A hand-built message from a third-party client must not fail the whole row
    final GrpcValue bad = GrpcValue.newBuilder().setStringValue("not a time").setLogicalType(GrpcTemporalCodec.LOCAL_TIME).build();
    assertThat(GrpcTypeConverter.fromGrpcValue(bad)).isEqualTo("not a time");
  }

  @Test
  void instantShapedTemporalsKeepTheirTimestampEncoding() {
    // The shared encoder must not change what the instant-shaped types already sent (issues #4149, #5045)
    final LocalDateTime ldt = LocalDateTime.of(2026, 5, 9, 12, 34, 56, 123_456_789);
    final GrpcValue dt = GrpcTypeConverter.toGrpcValue(ldt);
    assertThat(dt.getLogicalType()).isEqualTo(GrpcTemporalCodec.DATETIME);
    assertThat(dt.getTimestampValue().getSeconds()).isEqualTo(ldt.toEpochSecond(ZoneOffset.UTC));
    assertThat(dt.getTimestampValue().getNanos()).isEqualTo(123_456_789);

    final GrpcValue d = GrpcTypeConverter.toGrpcValue(LocalDate.of(1969, 12, 31));
    assertThat(d.getLogicalType()).isEqualTo(GrpcTemporalCodec.DATE);
    assertThat(d.getTimestampValue().getSeconds()).isEqualTo(-86_400L);

    final GrpcValue date = GrpcTypeConverter.toGrpcValue(new Date(-1L));
    assertThat(date.getLogicalType()).isEqualTo(GrpcTemporalCodec.DATETIME);
    assertThat(date.getTimestampValue().getSeconds()).isEqualTo(-1L);
    assertThat(date.getTimestampValue().getNanos()).isEqualTo(999_000_000);

    final Instant instant = Instant.ofEpochSecond(1_000_000_000L, 7);
    assertThat(GrpcTypeConverter.fromGrpcValue(GrpcTypeConverter.toGrpcValue(instant))).isEqualTo(instant);
  }
}
