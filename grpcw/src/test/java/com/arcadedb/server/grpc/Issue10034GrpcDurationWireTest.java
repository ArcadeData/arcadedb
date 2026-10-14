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

import com.arcadedb.query.opencypher.temporal.CypherDuration;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10034: a {@code DURATION} ({@link CypherDuration}) travels as its ISO text tagged {@code duration}, like the other
 * values #9969 moved onto {@link GrpcTemporalCodec}, instead of falling through to the untagged {@code String.valueOf}
 * fallback. The text is the storage encoding ({@code toString()} / {@code parse()}), so every component combination has to
 * round-trip exactly, including the negative-seconds-with-positive-nanos case {@code toString()} special-cases.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10034GrpcDurationWireTest {

  private static final List<CypherDuration> EDGE_CASES = List.of(//
      new CypherDuration(0, 0, 0, 0),//
      new CypherDuration(1, 0, 0, 0),//
      new CypherDuration(14, 0, 0, 0),//
      new CypherDuration(-5, 0, 0, 0),//
      new CypherDuration(-13, 0, 0, 0),//
      new CypherDuration(0, 10, 0, 0),//
      new CypherDuration(0, -10, 0, 0),//
      new CypherDuration(0, 0, 1, 0),//
      new CypherDuration(0, 0, 3723, 0),//
      new CypherDuration(0, 0, 0, 1),//
      new CypherDuration(0, 0, 0, 999_999_999),//
      new CypherDuration(0, 0, 1, 500_000_000),//
      // MONTHS + DAYS + TIME
      new CypherDuration(14, 3, 3723, 4_500_000),//
      new CypherDuration(-14, -3, -3723, 0),//
      // NEGATIVE SECONDS WITH POSITIVE NANOS: toString() WRITES THE EFFECTIVE (LESS NEGATIVE) VALUE
      new CypherDuration(0, 0, -1, 500_000_000),//
      new CypherDuration(0, 0, -1, 1),//
      new CypherDuration(0, 0, -1, 999_999_999),//
      new CypherDuration(0, 0, -61, 500_000_000),//
      new CypherDuration(0, 0, -3601, 1),//
      new CypherDuration(0, 0, -4, 500_000_000),//
      new CypherDuration(3, -2, -7, 250_000_000),//
      // THE LIMITS OF EVERY COMPONENT
      new CypherDuration(Long.MAX_VALUE, 0, 0, 0),//
      new CypherDuration(Long.MIN_VALUE, 0, 0, 0),//
      new CypherDuration(0, Long.MAX_VALUE, 0, 0),//
      new CypherDuration(0, Long.MIN_VALUE, 0, 0),//
      new CypherDuration(0, 0, Long.MAX_VALUE, 999_999_999),//
      new CypherDuration(0, 0, Long.MIN_VALUE, 0),//
      new CypherDuration(0, 0, Long.MIN_VALUE, 1),//
      new CypherDuration(Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE, 999_999_999),//
      new CypherDuration(Long.MIN_VALUE, Long.MIN_VALUE, Long.MIN_VALUE, 1));

  @Test
  void aDurationTravelsAsTaggedIsoText() {
    final CypherDuration duration = new CypherDuration(14, 3, 3723, 4_500_000);

    final GrpcValue encoded = GrpcTypeConverter.toGrpcValue(duration);

    assertThat(encoded.getKindCase()).isEqualTo(GrpcValue.KindCase.STRING_VALUE);
    assertThat(encoded.getLogicalType()).isEqualTo(GrpcTemporalCodec.DURATION);
    assertThat(encoded.getStringValue()).isEqualTo("P1Y2M3DT1H2M3.0045S");

    final Object decoded = GrpcTypeConverter.fromGrpcValue(encoded);
    assertThat(decoded).isInstanceOf(CypherDuration.class);
    assertThat(decoded).isEqualTo(duration);
  }

  @Test
  void everyEdgeCaseRoundTripsExactly() {
    for (final CypherDuration duration : EDGE_CASES) {
      final GrpcValue encoded = GrpcTypeConverter.toGrpcValue(duration);

      assertThat(encoded.getLogicalType()).as("tag of %s", duration).isEqualTo(GrpcTemporalCodec.DURATION);
      assertThat(GrpcTypeConverter.fromGrpcValue(encoded)).as("round trip of %s", duration).isEqualTo(duration);
    }
  }

  @Test
  void randomDurationsRoundTripExactly() {
    // Seeded, so a failure names a value that can be replayed
    final Random random = new Random(10034L);
    for (int i = 0; i < 20_000; i++) {
      final CypherDuration duration = new CypherDuration(component(random), component(random), component(random),
          random.nextInt(1_000_000_000));

      final Object decoded = GrpcTypeConverter.fromGrpcValue(GrpcTypeConverter.toGrpcValue(duration));

      assertThat(decoded).as("round trip of %s (months=%d days=%d seconds=%d nanos=%d)", duration, duration.getMonths(),
          duration.getDays(), duration.getSeconds(), duration.getNanosAdjustment()).isEqualTo(duration);
    }
  }

  @Test
  void durationsInsideCollectionsAreEncodedTheSameWay() {
    final CypherDuration duration = new CypherDuration(0, 0, -1, 500_000_000);

    final GrpcValue list = GrpcTypeConverter.toGrpcValue(List.of(duration));
    assertThat(GrpcTypeConverter.fromGrpcValue(list)).isEqualTo(List.of(duration));

    final GrpcValue map = GrpcTypeConverter.toGrpcValue(Map.of("d", duration));
    assertThat(GrpcTypeConverter.fromGrpcValue(map)).isEqualTo(Map.of("d", duration));
  }

  @Test
  void untaggedTextStaysAString() {
    final GrpcValue plain = GrpcValue.newBuilder().setStringValue("P1Y").build();
    assertThat(GrpcTypeConverter.fromGrpcValue(plain)).isEqualTo("P1Y");
  }

  @Test
  void theTagMatchesCaseInsensitively() {
    final GrpcValue upper = GrpcValue.newBuilder().setStringValue("P1Y2M").setLogicalType("DURATION").build();
    assertThat(GrpcTypeConverter.fromGrpcValue(upper)).isEqualTo(new CypherDuration(14, 0, 0, 0));
  }

  @Test
  void textThatIsNotADurationDegradesToTheString() {
    // A hand-built message from a third-party client must not fail the whole row
    for (final String text : List.of("not a duration", "", "1Y", "PT1H1", "P1.2.3Y", "P99999999999999999999Y")) {
      final GrpcValue bad = GrpcValue.newBuilder().setStringValue(text).setLogicalType(GrpcTemporalCodec.DURATION).build();
      assertThat(GrpcTypeConverter.fromGrpcValue(bad)).as("text '%s'", text).isEqualTo(text);
    }
  }

  /** Mostly small magnitudes, where the carries between hours, minutes and seconds live, with the odd huge one. */
  private static long component(final Random random) {
    return switch (random.nextInt(4)) {
      case 0 -> random.nextInt(200) - 100;
      case 1 -> random.nextInt(2_000_000) - 1_000_000;
      case 2 -> random.nextLong() / 3;
      default -> random.nextLong() >> random.nextInt(63);
    };
  }
}
