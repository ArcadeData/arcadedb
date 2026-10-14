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
package com.arcadedb.remote.grpc.utils;

import com.arcadedb.server.grpc.GrpcTemporalCodec;
import com.arcadedb.server.grpc.GrpcValue;
import org.junit.jupiter.api.Test;

import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9969, client side: {@link ProtoUtils} encodes a {@link ZonedDateTime} with its zone and an {@link OffsetTime} /
 * {@link LocalTime} typed, and decodes the three logical types the server now sends back to the matching {@code java.time}
 * value.
 */
class Issue9969ProtoUtilsZonedTemporalTest {

  @Test
  void zonedDateTimeRoundTripsWithItsZoneId() {
    final ZonedDateTime zdt = ZonedDateTime.of(2026, 1, 1, 0, 30, 0, 1_000, ZoneId.of("America/New_York"));

    final GrpcValue encoded = ProtoUtils.toGrpcValue(zdt);

    assertThat(encoded.getKindCase()).isEqualTo(GrpcValue.KindCase.STRING_VALUE);
    assertThat(encoded.getLogicalType()).isEqualTo(GrpcTemporalCodec.ZONED_DATETIME);
    assertThat(ProtoUtils.fromGrpcValue(encoded)).isEqualTo(zdt);
  }

  @Test
  void offsetTimeRoundTripsTyped() {
    final OffsetTime t = OffsetTime.of(0, 30, 0, 0, ZoneOffset.ofHours(2));

    final GrpcValue encoded = ProtoUtils.toGrpcValue(t);

    assertThat(encoded.getLogicalType()).isEqualTo(GrpcTemporalCodec.OFFSET_TIME);
    assertThat(ProtoUtils.fromGrpcValue(encoded)).isEqualTo(t);
  }

  @Test
  void localTimeRoundTripsTyped() {
    final LocalTime t = LocalTime.of(7, 5, 3, 120_000_000);

    final GrpcValue encoded = ProtoUtils.toGrpcValue(t);

    assertThat(encoded.getLogicalType()).isEqualTo(GrpcTemporalCodec.LOCAL_TIME);
    assertThat(ProtoUtils.fromGrpcValue(encoded)).isEqualTo(t);
  }

  @Test
  void serverShapedValuesDecode() {
    // Exactly the text the server's encoder emits for a ZONED_DATETIME in a named zone
    final GrpcValue fromServer = GrpcValue.newBuilder().setStringValue("2026-01-01T00:30:00+01:00[Europe/Rome]")
        .setLogicalType(GrpcTemporalCodec.ZONED_DATETIME).build();
    assertThat(ProtoUtils.fromGrpcValue(fromServer)).isEqualTo(ZonedDateTime.of(2026, 1, 1, 0, 30, 0, 0, ZoneId.of("Europe/Rome")));

    // Untagged text is still a plain string
    assertThat(ProtoUtils.fromGrpcValue(GrpcValue.newBuilder().setStringValue("00:30").build())).isEqualTo("00:30");
  }
}
