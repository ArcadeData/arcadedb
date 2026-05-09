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

import com.google.protobuf.Timestamp;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #4149: gRPC parameter binding and result encoding must preserve
 * sub-millisecond precision for datetime values.
 * <p>
 * Inbound: a proto {@link Timestamp} carrying nanosecond precision (e.g.
 * {@code seconds + 123456789 nanos}) was being converted to {@link java.util.Date} via a
 * {@code millis} round trip, dropping the bottom 6 decimal digits. Now we map to
 * {@link Instant} so {@code Type.convert} can truncate to the column's declared precision
 * via {@code DateUtils.getPrecisionFromType}.
 * <p>
 * Outbound: ArcadeDB stores datetimes as {@link LocalDateTime}/{@link ZonedDateTime}/
 * {@link Instant}; the converter previously had no branch for these types and fell through
 * to {@code String.valueOf(o)} - emitting a {@code string_value} rather than the natural
 * {@code timestamp_value}, and at {@code LocalDateTime#toString} precision rather than the
 * column's declared precision.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue4149GrpcTypeConverterTest {

  @Test
  void fromGrpcValueTimestampPreservesNanos() {
    // 2026-05-09T12:34:56.123456789Z
    final Instant expected = LocalDateTime.of(2026, 5, 9, 12, 34, 56, 123_456_789).toInstant(ZoneOffset.UTC);
    final Timestamp ts = Timestamp.newBuilder()
        .setSeconds(expected.getEpochSecond())
        .setNanos(expected.getNano())
        .build();
    final GrpcValue v = GrpcValue.newBuilder().setTimestampValue(ts).build();

    final Object result = GrpcTypeConverter.fromGrpcValue(v);

    assertThat(result).as("TIMESTAMP_VALUE must produce a temporal carrying full nanosecond precision")
        .isInstanceOf(Instant.class);
    assertThat(((Instant) result).getEpochSecond()).isEqualTo(expected.getEpochSecond());
    assertThat(((Instant) result).getNano()).isEqualTo(123_456_789);
  }

  @Test
  void toGrpcValueLocalDateTimeProducesTimestampWithNanos() {
    final LocalDateTime ldt = LocalDateTime.of(2026, 5, 9, 12, 34, 56, 123_456_789);

    final GrpcValue result = GrpcTypeConverter.toGrpcValue(ldt);

    assertThat(result.hasTimestampValue()).as("LocalDateTime must serialize to proto Timestamp, not string fallback").isTrue();
    assertThat(result.getTimestampValue().getNanos()).isEqualTo(123_456_789);
  }

  @Test
  void toGrpcValueInstantProducesTimestampWithNanos() {
    final Instant instant = LocalDateTime.of(2026, 5, 9, 12, 34, 56, 987_654_321).toInstant(ZoneOffset.UTC);

    final GrpcValue result = GrpcTypeConverter.toGrpcValue(instant);

    assertThat(result.hasTimestampValue()).isTrue();
    assertThat(result.getTimestampValue().getSeconds()).isEqualTo(instant.getEpochSecond());
    assertThat(result.getTimestampValue().getNanos()).isEqualTo(987_654_321);
  }

  @Test
  void toGrpcValueZonedDateTimeProducesTimestampWithNanos() {
    final ZonedDateTime zdt = LocalDateTime.of(2026, 5, 9, 12, 34, 56, 111_222_333).atZone(ZoneOffset.UTC);

    final GrpcValue result = GrpcTypeConverter.toGrpcValue(zdt);

    assertThat(result.hasTimestampValue()).isTrue();
    assertThat(result.getTimestampValue().getNanos()).isEqualTo(111_222_333);
  }

  @Test
  void timestampRoundTripPreservesNanos() {
    final Instant original = LocalDateTime.of(2026, 5, 9, 12, 34, 56, 123_456_789).toInstant(ZoneOffset.UTC);

    final GrpcValue encoded = GrpcTypeConverter.toGrpcValue(original);
    final Object decoded = GrpcTypeConverter.fromGrpcValue(encoded);

    assertThat(decoded).isInstanceOf(Instant.class);
    assertThat((Instant) decoded).isEqualTo(original);
  }
}
