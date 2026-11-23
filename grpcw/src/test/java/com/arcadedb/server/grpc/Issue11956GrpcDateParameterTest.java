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
import java.time.LocalDate;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11956: a {@code timestamp_value} tagged {@code date}, the encoding the server itself writes for a DATE, was decoded
 * as an {@link Instant}, so a date parameter was a datetime to the engine. It decodes to the {@link LocalDate} it names.
 */
class Issue11956GrpcDateParameterTest {
  private static final LocalDate DAY = LocalDate.of(2024, 2, 29);

  @Test
  void aTimestampTaggedDateDecodesToALocalDate() {
    final GrpcValue encoded = GrpcTypeConverter.toGrpcValue(DAY);
    assertThat(encoded.getLogicalType()).isEqualTo(GrpcTemporalCodec.DATE);

    assertThat(GrpcTypeConverter.fromGrpcValue(encoded)).isEqualTo(DAY);
  }

  @Test
  void aDateBeforeTheEpochRoundTrips() {
    final LocalDate before = LocalDate.of(1500, 1, 1);
    assertThat(GrpcTypeConverter.fromGrpcValue(GrpcTypeConverter.toGrpcValue(before))).isEqualTo(before);
  }

  @Test
  void datesInsideParametersCollectionsAndMapsDecodeToLocalDates() {
    final GrpcValue date = GrpcTypeConverter.toGrpcValue(DAY);
    assertThat(GrpcTypeConverter.convertParameters(Map.of("p", date))).containsEntry("p", DAY);
    assertThat(GrpcTypeConverter.fromGrpcValue(GrpcTypeConverter.toGrpcValue(List.of(DAY)))).isEqualTo(List.of(DAY));
    assertThat(GrpcTypeConverter.fromGrpcValue(GrpcTypeConverter.toGrpcValue(Map.of("d", DAY)))).isEqualTo(Map.of("d", DAY));
  }

  @Test
  void aTimestampTaggedDatetimeOrUntaggedStaysAnInstant() {
    final Timestamp ts = Timestamp.newBuilder().setSeconds(1709212455L).setNanos(123_456_789).build();
    final Instant instant = Instant.ofEpochSecond(1709212455L, 123_456_789);

    assertThat(GrpcTypeConverter.fromGrpcValue(GrpcValue.newBuilder().setTimestampValue(ts).build())).isEqualTo(instant);
    assertThat(GrpcTypeConverter.fromGrpcValue(
        GrpcValue.newBuilder().setTimestampValue(ts).setLogicalType(GrpcTemporalCodec.DATETIME).build())).isEqualTo(instant);
  }
}
