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

import com.arcadedb.query.opencypher.temporal.CypherDuration;
import com.arcadedb.server.grpc.GrpcTemporalCodec;
import com.arcadedb.server.grpc.GrpcValue;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10034, client side: {@link ProtoUtils} encodes a {@link CypherDuration} as ISO text tagged {@code duration} and
 * decodes the same shape back to a {@link CypherDuration}, instead of handing the application an untagged {@code String}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10034ProtoUtilsDurationTest {

  @Test
  void durationRoundTripsTyped() {
    final CypherDuration duration = new CypherDuration(14, 3, 3723, 4_500_000);

    final GrpcValue encoded = ProtoUtils.toGrpcValue(duration);

    assertThat(encoded.getKindCase()).isEqualTo(GrpcValue.KindCase.STRING_VALUE);
    assertThat(encoded.getLogicalType()).isEqualTo(GrpcTemporalCodec.DURATION);
    assertThat(ProtoUtils.fromGrpcValue(encoded)).isInstanceOf(CypherDuration.class).isEqualTo(duration);
  }

  @Test
  void negativeSecondsWithPositiveNanosRoundTrip() {
    for (final CypherDuration duration : List.of(new CypherDuration(0, 0, -1, 500_000_000), new CypherDuration(0, 0, -61, 1),
        new CypherDuration(-14, -3, -3601, 999_999_999), new CypherDuration(0, 0, 0, 0)))
      assertThat(ProtoUtils.fromGrpcValue(ProtoUtils.toGrpcValue(duration))).as("round trip of %s", duration).isEqualTo(duration);
  }

  @Test
  void serverShapedValuesDecode() {
    // Exactly the text the server's encoder emits for a duration
    final GrpcValue fromServer = GrpcValue.newBuilder().setStringValue("P1Y2M3DT4H5M6.789S")
        .setLogicalType(GrpcTemporalCodec.DURATION).build();
    assertThat(ProtoUtils.fromGrpcValue(fromServer)).isEqualTo(new CypherDuration(14, 3, 14706, 789_000_000));

    // Untagged text is still a plain string, and text that is not a duration degrades to itself instead of failing the row
    assertThat(ProtoUtils.fromGrpcValue(GrpcValue.newBuilder().setStringValue("P1Y").build())).isEqualTo("P1Y");
    assertThat(ProtoUtils.fromGrpcValue(
        GrpcValue.newBuilder().setStringValue("not a duration").setLogicalType(GrpcTemporalCodec.DURATION).build())).isEqualTo(
        "not a duration");
  }

  @Test
  void durationsInsideCollectionsAreEncodedTheSameWay() {
    final CypherDuration duration = new CypherDuration(0, 7, 0, 0);

    assertThat(ProtoUtils.fromGrpcValue(ProtoUtils.toGrpcValue(List.of(duration)))).isEqualTo(List.of(duration));
    assertThat(ProtoUtils.fromGrpcValue(ProtoUtils.toGrpcValue(Map.of("d", duration)))).isEqualTo(Map.of("d", duration));
  }
}
