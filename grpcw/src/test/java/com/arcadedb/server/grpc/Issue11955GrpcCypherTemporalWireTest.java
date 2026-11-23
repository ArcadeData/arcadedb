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

import com.arcadedb.query.opencypher.temporal.CypherDate;
import com.arcadedb.query.opencypher.temporal.CypherDateTime;
import com.arcadedb.query.opencypher.temporal.CypherDuration;
import com.arcadedb.query.opencypher.temporal.CypherLocalDateTime;
import com.arcadedb.query.opencypher.temporal.CypherLocalTime;
import com.arcadedb.query.opencypher.temporal.CypherTime;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11955: a Cypher date, time or datetime reached the client as an untagged {@code string_value}, where SQL sends the
 * same property as a tagged value. Each wrapper is now encoded as the {@code java.time} value it wraps, on the one converter
 * every record and parameter path of the module shares, alone and inside a list or a map.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class Issue11955GrpcCypherTemporalWireTest {

  private static final LocalDate     LOCAL_DATE      = LocalDate.of(2024, 2, 29);
  private static final LocalDateTime LOCAL_DATETIME  = LocalDateTime.of(2024, 2, 29, 13, 14, 15, 123_000_000);
  private static final ZonedDateTime ZONED_DATETIME  = ZonedDateTime.of(LOCAL_DATETIME, ZoneOffset.UTC);
  private static final LocalTime     LOCAL_TIME      = LocalTime.of(10, 0);
  private static final OffsetTime    OFFSET_TIME     = OffsetTime.of(LOCAL_TIME, ZoneOffset.ofHours(1));

  private static final Object[][] PAIRS = { { new CypherDate(LOCAL_DATE), LOCAL_DATE },
      { new CypherLocalDateTime(LOCAL_DATETIME), LOCAL_DATETIME }, { new CypherDateTime(ZONED_DATETIME), ZONED_DATETIME },
      { new CypherLocalTime(LOCAL_TIME), LOCAL_TIME }, { new CypherTime(OFFSET_TIME), OFFSET_TIME } };

  @Test
  void aWrapperIsEncodedAsTheValueItWraps() {
    for (final Object[] pair : PAIRS) {
      final GrpcValue encoded = GrpcTypeConverter.toGrpcValue(pair[0]);
      assertThat(encoded).as(pair[0].getClass().getSimpleName()).isEqualTo(GrpcTypeConverter.toGrpcValue(pair[1]));
      assertThat(encoded.getLogicalType()).as("the tag of %s", pair[0].getClass().getSimpleName()).isNotEmpty();
    }
  }

  @Test
  void theTagsAreTheOnesSqlGetsForTheSameProperty() {
    final GrpcValue date = GrpcTypeConverter.toGrpcValue(new CypherDate(LOCAL_DATE));
    assertThat(date.getKindCase()).isEqualTo(GrpcValue.KindCase.TIMESTAMP_VALUE);
    assertThat(date.getLogicalType()).isEqualTo(GrpcTemporalCodec.DATE);

    final GrpcValue dateTime = GrpcTypeConverter.toGrpcValue(new CypherLocalDateTime(LOCAL_DATETIME));
    assertThat(dateTime.getKindCase()).isEqualTo(GrpcValue.KindCase.TIMESTAMP_VALUE);
    assertThat(dateTime.getLogicalType()).isEqualTo(GrpcTemporalCodec.DATETIME);

    assertThat(GrpcTypeConverter.toGrpcValue(new CypherDateTime(ZONED_DATETIME)).getLogicalType()).isEqualTo(
        GrpcTemporalCodec.ZONED_DATETIME);
    assertThat(GrpcTypeConverter.toGrpcValue(new CypherLocalTime(LOCAL_TIME)).getLogicalType()).isEqualTo(GrpcTemporalCodec.LOCAL_TIME);

    final GrpcValue time = GrpcTypeConverter.toGrpcValue(new CypherTime(OFFSET_TIME));
    assertThat(time.getLogicalType()).isEqualTo(GrpcTemporalCodec.OFFSET_TIME);
    assertThat(time.getStringValue()).isEqualTo("10:00:00+01:00");
  }

  @Test
  void aWrapperComesBackAsTheJavaTimeValue() {
    for (final Object[] pair : PAIRS)
      assertThat(GrpcTypeConverter.fromGrpcValue(GrpcTypeConverter.toGrpcValue(pair[0]))).as(pair[0].getClass().getSimpleName())
          .isEqualTo(roundTripped(pair[1]));
  }

  @Test
  void aWrapperInsideAListOrAMapIsEncodedTheSameWay() {
    for (final Object[] pair : PAIRS) {
      assertThat(GrpcTypeConverter.toGrpcValue(List.of(pair[0]))).as("list of %s", pair[0].getClass().getSimpleName())
          .isEqualTo(GrpcTypeConverter.toGrpcValue(List.of(pair[1])));
      assertThat(GrpcTypeConverter.toGrpcValue(Map.of("t", pair[0]))).as("map of %s", pair[0].getClass().getSimpleName())
          .isEqualTo(GrpcTypeConverter.toGrpcValue(Map.of("t", pair[1])));
    }
  }

  @Test
  void aDurationKeepsItsOwnTag() {
    final GrpcValue encoded = GrpcTypeConverter.toGrpcValue(new CypherDuration(14, 3, 3723, 4_500_000));
    assertThat(encoded.getKindCase()).isEqualTo(GrpcValue.KindCase.STRING_VALUE);
    assertThat(encoded.getLogicalType()).isEqualTo(GrpcTemporalCodec.DURATION);
  }

  /** What the client reads back for a java.time value: the encoder of a date or a datetime carries an instant, not the type. */
  private static Object roundTripped(final Object javaTime) {
    return GrpcTypeConverter.fromGrpcValue(GrpcTypeConverter.toGrpcValue(javaTime));
  }
}
