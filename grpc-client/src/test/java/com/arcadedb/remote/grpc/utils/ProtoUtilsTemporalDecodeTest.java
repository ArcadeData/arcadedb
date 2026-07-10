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

import com.arcadedb.server.grpc.GrpcValue;
import com.google.protobuf.Timestamp;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #5045 (COR-5 / COR-6): the client must reconstruct the correct temporal type from the
 * {@code logical_type} tag the server sets on a proto Timestamp, preserving sub-millisecond
 * precision and reading the value back at the same UTC anchor used on encode - instead of
 * collapsing every Timestamp to a bare {@code Long} epoch-millis (losing micros/nanos and the
 * temporal type identity).
 */
class ProtoUtilsTemporalDecodeTest {

  @Test
  void dateLogicalTypeDecodesToLocalDate() {
    final LocalDate expected = LocalDate.of(2026, 7, 6);
    // Server encodes a LocalDate as midnight-UTC: epochDay * 86400 seconds, nanos 0, logical_type "date".
    final long seconds = expected.toEpochDay() * 86_400L;
    final GrpcValue v = GrpcValue.newBuilder()
        .setTimestampValue(Timestamp.newBuilder().setSeconds(seconds).setNanos(0).build())
        .setLogicalType("date")
        .build();

    final Object decoded = ProtoUtils.fromGrpcValue(v);
    assertThat(decoded).isInstanceOf(LocalDate.class);
    assertThat((LocalDate) decoded).isEqualTo(expected);
  }

  @Test
  void datetimeLogicalTypeDecodesToLocalDateTimePreservingMillis() {
    final LocalDateTime expected = LocalDateTime.of(2026, 1, 1, 0, 0, 0, 123_000_000);
    final GrpcValue v = GrpcValue.newBuilder()
        .setTimestampValue(Timestamp.newBuilder()
            .setSeconds(expected.toEpochSecond(ZoneOffset.UTC))
            .setNanos(expected.getNano())
            .build())
        .setLogicalType("datetime")
        .build();

    final Object decoded = ProtoUtils.fromGrpcValue(v);
    assertThat(decoded).isInstanceOf(LocalDateTime.class);
    assertThat((LocalDateTime) decoded).isEqualTo(expected);
  }

  @Test
  void datetimeLogicalTypePreservesMicros() {
    final LocalDateTime expected = LocalDateTime.of(2026, 1, 1, 0, 0, 0, 123_456_000);
    final GrpcValue v = GrpcValue.newBuilder()
        .setTimestampValue(Timestamp.newBuilder()
            .setSeconds(expected.toEpochSecond(ZoneOffset.UTC))
            .setNanos(expected.getNano())
            .build())
        .setLogicalType("datetime")
        .build();

    final Object decoded = ProtoUtils.fromGrpcValue(v);
    assertThat(decoded).isInstanceOf(LocalDateTime.class);
    assertThat(((LocalDateTime) decoded).getNano()).isEqualTo(123_456_000);
    assertThat((LocalDateTime) decoded).isEqualTo(expected);
  }

  @Test
  void datetimeLogicalTypePreservesNanos() {
    final LocalDateTime expected = LocalDateTime.of(2026, 5, 9, 12, 34, 56, 123_456_789);
    final GrpcValue v = GrpcValue.newBuilder()
        .setTimestampValue(Timestamp.newBuilder()
            .setSeconds(expected.toEpochSecond(ZoneOffset.UTC))
            .setNanos(expected.getNano())
            .build())
        .setLogicalType("datetime")
        .build();

    final Object decoded = ProtoUtils.fromGrpcValue(v);
    assertThat(decoded).isInstanceOf(LocalDateTime.class);
    assertThat(((LocalDateTime) decoded).getNano()).isEqualTo(123_456_789);
    assertThat((LocalDateTime) decoded).isEqualTo(expected);
  }

  @Test
  void bareTimestampWithoutLogicalTypeStillDecodesToLongForBackwardCompatibility() {
    // A Timestamp with no logical_type keeps the legacy epoch-millis Long behavior so existing
    // consumers (and ProtoUtilsTest.fromGrpcValueTimestamp) are unaffected.
    final GrpcValue v = GrpcValue.newBuilder()
        .setTimestampValue(Timestamp.newBuilder().setSeconds(1_234_567_890L).setNanos(123_456_789).build())
        .build();

    final Object decoded = ProtoUtils.fromGrpcValue(v);
    assertThat(decoded).isInstanceOf(Long.class);
    assertThat((Long) decoded).isEqualTo(1_234_567_890_123L);
  }
}
