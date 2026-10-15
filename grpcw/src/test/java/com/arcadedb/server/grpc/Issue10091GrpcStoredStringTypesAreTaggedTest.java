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
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.EnumMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10091: the rule "a value that holds more than an instant travels as its own typed text" was written down twice
 * in the engine ({@code Type.isConvertedFromStoredString()} and {@code JsonSerializer.isNativeCypherTemporal()}) and a
 * third time, by hand, in {@link GrpcTemporalCodec}, which named three of the four types: a {@code DURATION} fell past it
 * into the untagged {@code String.valueOf} fallback and reached the client as a {@code String}. The arm itself was added by
 * issue #10034.
 * <p>
 * What is left of #10091 is the guarantee it asks for: the codec must not be a list that has to be kept in step by
 * memory. This walks {@code Type.values()}, takes every type the schema authority says is read back from stored text, and
 * requires that a representative value of it crosses the wire tagged, comes back as the same type and equal. A type added
 * to the authority fails here, by name, instead of silently becoming a {@code String} on the wire.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10091GrpcStoredStringTypesAreTaggedTest {

  /** One value per type the schema authority converts from stored text. A new member of the set must be given one here. */
  private static final Map<Type, Object> REPRESENTATIVES = new EnumMap<>(Type.class);

  static {
    REPRESENTATIVES.put(Type.OFFSET_TIME, OffsetTime.of(13, 45, 30, 123_456_789, ZoneOffset.ofHoursMinutes(5, 30)));
    REPRESENTATIVES.put(Type.LOCAL_TIME, LocalTime.of(13, 45, 30, 123_456_789));
    REPRESENTATIVES.put(Type.ZONED_DATETIME, ZonedDateTime.of(2026, 10, 15, 13, 45, 30, 123_456_789, ZoneId.of("Europe/Rome")));
    REPRESENTATIVES.put(Type.DURATION, new CypherDuration(14, 3, 3723, 4_500_000));
  }

  @Test
  void everyTypeTheSchemaConvertsFromStoredTextTravelsTaggedAndComesBackEqual() {
    for (final Type type : Type.values()) {
      if (!type.isConvertedFromStoredString())
        continue;

      final Object value = REPRESENTATIVES.get(type);
      assertThat(value).as("Type.%s is read back from stored text, so GrpcTemporalCodec must carry it as its own tagged text: "
          + "give it a representative value here and an arm in the codec", type).isNotNull();
      assertThat(Type.getTypeByValue(value)).as("the representative of %s", type).isEqualTo(type);

      final GrpcValue encoded = GrpcTemporalCodec.encode(value);
      assertThat(encoded).as("GrpcTemporalCodec.encode(%s) for %s", value, type).isNotNull();
      assertThat(encoded.getKindCase()).as("wire kind of %s", type).isEqualTo(GrpcValue.KindCase.STRING_VALUE);
      assertThat(encoded.getLogicalType()).as("an empty logical_type is what makes the client hand back a String (%s)", type)
          .isNotEmpty();

      final Object decoded = GrpcTemporalCodec.decodeString(encoded.getStringValue(), encoded.getLogicalType());
      assertThat(decoded).as("decoded %s", type).isEqualTo(value);
      assertThat(Type.getTypeByValue(decoded)).as("the type identity of %s survives the wire", type).isEqualTo(type);
    }
  }

  @Test
  void theServerConverterAgreesWithTheCodecForEveryOfThem() {
    for (final Map.Entry<Type, Object> entry : REPRESENTATIVES.entrySet()) {
      final GrpcValue encoded = GrpcTypeConverter.toGrpcValue(entry.getValue());

      assertThat(encoded.getLogicalType()).as("tag of %s", entry.getKey()).isNotEmpty();
      assertThat(GrpcTypeConverter.fromGrpcValue(encoded)).as("round trip of %s", entry.getKey()).isEqualTo(entry.getValue());
    }
  }

  @Test
  void theRepresentativesAreExactlyTheTypesTheSchemaConverts() {
    // The other direction: a representative whose type left the authority's set is a stale row, not a guarantee
    for (final Type type : REPRESENTATIVES.keySet())
      assertThat(type.isConvertedFromStoredString()).as("Type.%s is no longer converted from stored text", type).isTrue();
  }
}
