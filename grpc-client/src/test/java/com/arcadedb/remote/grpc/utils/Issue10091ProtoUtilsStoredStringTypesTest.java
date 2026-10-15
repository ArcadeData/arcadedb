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
import com.arcadedb.schema.Type;
import com.arcadedb.server.grpc.GrpcValue;
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
 * Issue #10091, client side: every type {@code Type.isConvertedFromStoredString()} names must cross {@link ProtoUtils}
 * tagged and come back as the same type. The server-side twin is {@code Issue10091GrpcStoredStringTypesAreTaggedTest}; the
 * two ends share {@code GrpcTemporalCodec}, and this keeps a type added to the schema authority from being a {@code String}
 * on the client's side of the wire only.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10091ProtoUtilsStoredStringTypesTest {

  @Test
  void everyTypeTheSchemaConvertsFromStoredTextRoundTripsTyped() {
    final Map<Type, Object> representatives = new EnumMap<>(Type.class);
    representatives.put(Type.OFFSET_TIME, OffsetTime.of(13, 45, 30, 123_456_789, ZoneOffset.ofHoursMinutes(5, 30)));
    representatives.put(Type.LOCAL_TIME, LocalTime.of(13, 45, 30, 123_456_789));
    representatives.put(Type.ZONED_DATETIME, ZonedDateTime.of(2026, 10, 15, 13, 45, 30, 123_456_789, ZoneId.of("Europe/Rome")));
    representatives.put(Type.DURATION, new CypherDuration(14, 3, 3723, 4_500_000));

    for (final Type type : Type.values()) {
      if (!type.isConvertedFromStoredString())
        continue;

      final Object value = representatives.get(type);
      assertThat(value).as("Type.%s is read back from stored text: give it a representative value here and an arm in "
          + "GrpcTemporalCodec", type).isNotNull();

      final GrpcValue encoded = ProtoUtils.toGrpcValue(value);
      assertThat(encoded.getLogicalType()).as("tag of %s", type).isNotEmpty();

      final Object decoded = ProtoUtils.fromGrpcValue(encoded);
      assertThat(decoded).as("round trip of %s", type).isEqualTo(value);
      assertThat(Type.getTypeByValue(decoded)).as("type identity of %s", type).isEqualTo(type);
    }
  }
}
