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
package com.arcadedb.query.opencypher;

import com.arcadedb.query.opencypher.temporal.CypherDate;
import com.arcadedb.query.opencypher.temporal.CypherDateTime;
import com.arcadedb.query.opencypher.temporal.CypherDuration;
import com.arcadedb.query.opencypher.temporal.CypherLocalDateTime;
import com.arcadedb.query.opencypher.temporal.CypherLocalTime;
import com.arcadedb.query.opencypher.temporal.CypherTemporalValue;
import com.arcadedb.query.opencypher.temporal.CypherTime;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11955: {@link CypherTemporalValue#toJavaTime} is the one conversion the wire protocols (Bolt, gRPC, Postgres) apply before
 * they encode a value, so a Cypher date, time or datetime is sent as the same type as the {@code java.time} value SQL reads for the
 * same property.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class CypherTemporalValueToJavaTimeTest {

  @Test
  void eachWrapperIsUnwrappedToItsJavaTimeValue() {
    final LocalDate date = LocalDate.of(2024, 2, 29);
    final LocalDateTime localDateTime = LocalDateTime.of(2024, 2, 29, 13, 14, 15, 123_000_000);
    final ZonedDateTime dateTime = ZonedDateTime.of(localDateTime, ZoneOffset.UTC);
    final LocalTime localTime = LocalTime.of(10, 0);
    final OffsetTime time = OffsetTime.of(localTime, ZoneOffset.ofHours(1));

    assertThat(CypherTemporalValue.toJavaTime(new CypherDate(date))).isEqualTo(date);
    assertThat(CypherTemporalValue.toJavaTime(new CypherLocalDateTime(localDateTime))).isEqualTo(localDateTime);
    assertThat(CypherTemporalValue.toJavaTime(new CypherDateTime(dateTime))).isEqualTo(dateTime);
    assertThat(CypherTemporalValue.toJavaTime(new CypherLocalTime(localTime))).isEqualTo(localTime);
    assertThat(CypherTemporalValue.toJavaTime(new CypherTime(time))).isEqualTo(time);
  }

  @Test
  void aDurationIsLeftAsItIs() {
    // no single java.time type holds months, days and seconds: every protocol has its own encoding for it
    final CypherDuration duration = new CypherDuration(14, 3, 4, 5);
    assertThat(CypherTemporalValue.toJavaTime(duration)).isSameAs(duration);
  }

  @Test
  void anythingElseIsReturnedAsItIs() {
    final LocalDate plain = LocalDate.of(2024, 2, 29);
    assertThat(CypherTemporalValue.toJavaTime(plain)).isSameAs(plain);
    assertThat(CypherTemporalValue.toJavaTime("2024-02-29")).isEqualTo("2024-02-29");
    assertThat(CypherTemporalValue.toJavaTime(42)).isEqualTo(42);
    assertThat(CypherTemporalValue.toJavaTime(null)).isNull();
  }
}
