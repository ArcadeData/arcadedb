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

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.TimeZone;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11912: toString() of localtime, time, localdatetime and datetime prints as Neo4j 5 does, always with seconds and
 * with no trailing zeros in the fraction of a second.
 */
class CypherTemporalToStringTest extends TestHelper {

  private Object cypher(final String expression, final Object... params) {
    try (final ResultSet rs = database.query("opencypher", "RETURN " + expression + " AS x", params)) {
      return rs.next().getProperty("x");
    }
  }

  @Test
  void zeroSecondsArePrinted() {
    assertThat(cypher("toString(localdatetime('2020-01-01T10:00'))")).isEqualTo("2020-01-01T10:00:00");
    assertThat(cypher("toString(datetime('2020-01-01T10:00Z'))")).isEqualTo("2020-01-01T10:00:00Z");
    assertThat(cypher("toString(localtime('10:00'))")).isEqualTo("10:00:00");
    assertThat(cypher("toString(time('10:00Z'))")).isEqualTo("10:00:00Z");
    assertThat(cypher("toString(time('10:00+01:30'))")).isEqualTo("10:00:00+01:30");
    assertThat(cypher("toString(datetime.truncate('month', datetime('2019-08-17T17:19:10[Europe/Berlin]')))"))
        .isEqualTo("2019-08-01T00:00:00+02:00[Europe/Berlin]");
  }

  @Test
  void trailingZerosOfTheFractionAreRemoved() {
    assertThat(cypher("toString(datetime('2020-01-01T10:00:00.500Z'))")).isEqualTo("2020-01-01T10:00:00.5Z");
    assertThat(cypher("toString(localdatetime('2020-01-01T10:00:00.120'))")).isEqualTo("2020-01-01T10:00:00.12");
    assertThat(cypher("toString(localtime('10:00:00.000000001'))")).isEqualTo("10:00:00.000000001");
    assertThat(cypher("toString(localtime('10:00:00.0001'))")).isEqualTo("10:00:00.0001");
    assertThat(cypher("toString(localtime('10:00:00.123456789'))")).isEqualTo("10:00:00.123456789");
    assertThat(cypher("toString(time('10:00:00.000+01:00'))")).isEqualTo("10:00:00+01:00");
    assertThat(cypher("toString(datetime('2020-01-01T10:00:00.120[Europe/Berlin]'))"))
        .isEqualTo("2020-01-01T10:00:00.12+01:00[Europe/Berlin]");
  }

  @Test
  void relatedConversionsAndParametersUseTheSameText() {
    assertThat(cypher("toStringOrNull(localtime('10:00'))")).isEqualTo("10:00:00");
    assertThat(cypher("toStringList([localtime('10:00'), datetime('2020-01-01T10:00:00.500Z')])"))
        .isEqualTo(List.of("10:00:00", "2020-01-01T10:00:00.5Z"));
    assertThat(cypher("toString($t)", "t", LocalTime.of(10, 0))).isEqualTo("10:00:00");
    // Dates and durations are unchanged
    assertThat(cypher("toString(date('2020-01-01'))")).isEqualTo("2020-01-01");
    assertThat(cypher("toString(duration('PT1.5S'))")).isEqualTo("PT1.5S");
  }

  /** The APOC-namespaced entry points delegate to toString(), so they print the same text. */
  @Test
  void convertToStringPrintsTheSameText() {
    assertThat(cypher("convert.toString(localtime('10:00'))")).isEqualTo("10:00:00");
    assertThat(cypher("apoc.convert.toString(datetime('2020-01-01T10:00:00.500Z'))")).isEqualTo("2020-01-01T10:00:00.5Z");
    assertThat(cypher("apoc.convert.toString(time('10:00:00.120+01:00'))")).isEqualTo("10:00:00.12+01:00");
  }

  /** A native temporal handed over as a parameter (Bolt, the Java API) prints as the Cypher temporal it stands for. */
  @Test
  void nativeTemporalParametersPrintAsTheirCypherTemporal() {
    assertThat(cypher("toString($t)", "t", OffsetTime.of(10, 0, 0, 0, ZoneOffset.ofHoursMinutes(1, 30)))).isEqualTo("10:00:00+01:30");
    assertThat(cypher("toString($t)", "t", LocalDateTime.of(2020, 1, 1, 10, 0))).isEqualTo("2020-01-01T10:00:00");
    assertThat(cypher("toString($t)", "t", LocalDate.of(2020, 1, 1))).isEqualTo("2020-01-01");
    assertThat(cypher("toString($t)", "t", ZonedDateTime.of(2020, 1, 1, 10, 0, 0, 120_000_000, ZoneId.of("Europe/Berlin"))))
        .isEqualTo("2020-01-01T10:00:00.12+01:00[Europe/Berlin]");
    assertThat(cypher("toString($t)", "t", OffsetDateTime.of(2020, 1, 1, 10, 0, 0, 0, ZoneOffset.ofHours(2))))
        .isEqualTo("2020-01-01T10:00:00+02:00");
    assertThat(cypher("toString($t)", "t", Instant.parse("2020-01-01T10:00:00.500Z"))).isEqualTo("2020-01-01T10:00:00.5Z");
  }

  /** java.util.Date and java.util.Calendar are instants the engine reads as a datetime, not as Java's own text. */
  @Test
  void dateAndCalendarParametersPrintAsADatetime() {
    final long millis = Instant.parse("2020-01-01T10:00:00.500Z").toEpochMilli();
    assertThat(cypher("toString($t)", "t", new Date(millis))).isEqualTo("2020-01-01T10:00:00.5Z");
    final Calendar calendar = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
    calendar.setTimeInMillis(millis);
    assertThat(cypher("toString($t)", "t", calendar)).isEqualTo("2020-01-01T10:00:00.5Z");
  }

  @Test
  void returnedValuesKeepTheirTextOutsideToString() {
    // Only the Cypher string conversion changes: a temporal returned as a value keeps its own text.
    assertThat(cypher("localtime('10:00')").toString()).isEqualTo("10:00");
  }
}
