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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #13569: a temporal sent as a query parameter (a Bolt driver's DateTime arrives as a {@code java.time} value) was refused by
 * temporal arithmetic and by the {@code date()}, {@code datetime()}, {@code localdatetime()}, {@code time()}, {@code localtime()}
 * constructors and the {@code truncate()} functions, which only knew the Cypher temporal classes, while the literal of the same value
 * worked. The expected values are Neo4j 5.26's for the same statement and parameter, checked on a live server through the Python
 * driver; the one exception is {@link #aTimeWithAnOffsetParameter()}, because that driver cannot send a time with an offset.
 */
class Issue13569TemporalParameterFunctionsTest extends TestHelper {

  private static final ZonedDateTime UTC      = ZonedDateTime.of(2012, 6, 1, 8, 5, 0, 0, ZoneOffset.UTC);
  private static final ZonedDateTime PLUS_TWO = ZonedDateTime.of(2012, 6, 1, 8, 5, 0, 0, ZoneOffset.ofHours(2));

  @ParameterizedTest
  @CsvSource(delimiter = '|', value = {
      "RETURN $d + duration({days: 100})                | 2012-09-09T08:05Z",
      "RETURN $d - duration({hours: 1})                 | 2012-06-01T07:05Z",
      "RETURN duration({days: 1}) + $d                  | 2012-06-02T08:05Z",
      "RETURN date($d)                                  | 2012-06-01",
      "RETURN datetime($d)                              | 2012-06-01T08:05Z",
      "RETURN localdatetime($d)                         | 2012-06-01T08:05",
      "RETURN time($d)                                  | 08:05Z",
      "RETURN localtime($d)                             | 08:05",
      "RETURN date.truncate('month', $d)                | 2012-06-01",
      "RETURN datetime.truncate('day', $d)              | 2012-06-01T00:00Z",
      "RETURN localdatetime.truncate('hour', $d)        | 2012-06-01T08:00",
      "RETURN time.truncate('hour', $d)                 | 08:00Z",
      "RETURN localtime.truncate('hour', $d)            | 08:00" })
  void aDateTimeParameterIsTheCypherDateTimeItsLiteralIs(final String statement, final String expected) {
    assertThat(single(statement + " AS x", Map.of("d", UTC))).isEqualTo(expected);
  }

  @Test
  void theOffsetOfADateTimeParameterIsKept() {
    assertThat(single("RETURN $d + duration({days: 1}) AS x", Map.of("d", PLUS_TWO))).isEqualTo("2012-06-02T08:05+02:00");
  }

  @Test
  void aDateTimeParameterWindowOverStoredValues() {
    database.transaction(() -> database.command("opencypher", "CREATE (:M {creationDate: $e})", Map.of("e", UTC.plusDays(30))));
    // the LDBC SNB BI 2 shape: a window of 100 days from a parameter
    assertThat(single("MATCH (m:M) WHERE $d <= m.creationDate AND m.creationDate < $d + duration({days: 100}) RETURN count(m) AS x",
        Map.of("d", UTC))).isEqualTo("1");
    // the LDBC SNB BI 16 shape: date() of a parameter against date() of a stored value
    assertThat(single("MATCH (m:M) WHERE date(m.creationDate) = date($e) RETURN count(m) AS x", Map.of("e", UTC.plusDays(30))))
        .isEqualTo("1");
  }

  @Test
  void aLocalTimeParameter() {
    assertThat(single("RETURN $t + duration({hours: 1}) AS x", Map.of("t", LocalTime.of(8, 5, 3)))).isEqualTo("09:05:03");
    assertThat(single("RETURN localtime($t) AS x", Map.of("t", LocalTime.of(8, 5, 3)))).isEqualTo("08:05:03");
    assertThat(single("RETURN localtime.truncate('hour', $t) AS x", Map.of("t", LocalTime.of(8, 5, 3)))).isEqualTo("08:00");
  }

  @Test
  void aTimeWithAnOffsetParameter() {
    assertThat(single("RETURN $t + duration({hours: 1}) AS x", Map.of("t", OffsetTime.of(8, 5, 0, 0, ZoneOffset.UTC))))
        .isEqualTo("09:05Z");
  }

  @Test
  void aTemporalParameterIsStoredAsTheNativeTypeItWas() {
    database.transaction(() -> database.command("opencypher", "CREATE (:S {z: $z, l: $l, d: $d, t: $t})",
        Map.of("z", UTC, "l", LocalDateTime.of(2012, 6, 1, 8, 5), "d", LocalDate.of(2012, 6, 1), "t", LocalTime.of(8, 5))));
    final var vertex = database.query("sql", "SELECT FROM S").next().getVertex().get();
    assertThat(vertex.get("z")).isInstanceOf(ZonedDateTime.class);
    assertThat(vertex.get("l")).isInstanceOf(LocalDateTime.class);
    assertThat(vertex.get("d")).isInstanceOf(LocalDate.class);
    assertThat(vertex.get("t")).isInstanceOf(LocalTime.class);
  }

  /**
   * The fix is in the consumers (the arithmetic, the constructors, the truncate and duration functions, the map bases), so every
   * native temporal is taken, not only the three classes a Bolt driver decodes to, and so is one that never went through the
   * parameter expression: an element of a list parameter, an entry of a map parameter.
   */
  @Test
  void everyNativeTemporalIsTakenWhereverItComesFrom() {
    final OffsetDateTime offsetDateTime = UTC.toOffsetDateTime();
    final Instant instant = UTC.toInstant();
    assertThat(single("RETURN $d + duration({days: 1}) AS x", Map.of("d", offsetDateTime))).isEqualTo("2012-06-02T08:05Z");
    assertThat(single("RETURN $d + duration({hours: 1}) AS x", Map.of("d", instant))).isEqualTo("2012-06-01T09:05Z");
    assertThat(single("RETURN date($d) AS x", Map.of("d", Date.from(instant)))).isEqualTo("2012-06-01");
    assertThat(single("RETURN datetime.truncate('day', $d) AS x", Map.of("d", instant))).isEqualTo("2012-06-01T00:00Z");
    assertThat(single("RETURN $d + $p AS x", Map.of("d", UTC, "p", Duration.ofHours(1)))).isEqualTo("2012-06-01T09:05Z");

    assertThat(single("UNWIND $ds AS d RETURN d + duration({days: 1}) AS x", Map.of("ds", List.of(UTC)))).isEqualTo("2012-06-02T08:05Z");
    assertThat(single("UNWIND $ds AS d RETURN date(d) AS x", Map.of("ds", List.of(UTC)))).isEqualTo("2012-06-01");
    assertThat(single("RETURN date.truncate('month', $m.d) AS x", Map.of("m", Map.of("d", UTC)))).isEqualTo("2012-06-01");
  }

  @Test
  void aNativeTemporalAsTheBaseOfATemporalMap() {
    assertThat(single("RETURN datetime({datetime: $d, timezone: '+02:00'}) AS x", Map.of("d", UTC))).isEqualTo("2012-06-01T10:05+02:00");
    assertThat(single("RETURN date({date: $d, day: 3}) AS x", Map.of("d", UTC))).isEqualTo("2012-06-03");
    assertThat(single("RETURN localtime({time: $t, minute: 0}) AS x", Map.of("t", LocalTime.of(8, 5)))).isEqualTo("08:00");
    assertThat(single("RETURN time({time: $t}) AS x", Map.of("t", OffsetTime.of(8, 5, 0, 0, ZoneOffset.ofHours(2)))))
        .isEqualTo("08:05+02:00");
  }

  @Test
  void durationFunctionsTakeANativeTime() {
    assertThat(single("RETURN duration.between($a, $b) AS x", Map.of("a", LocalTime.of(8, 5), "b", LocalTime.of(9, 5)))).isEqualTo("PT1H");
    assertThat(single("RETURN duration.inSeconds($a, $b) AS x",
        Map.of("a", OffsetTime.of(8, 5, 0, 0, ZoneOffset.UTC), "b", OffsetTime.of(9, 5, 0, 0, ZoneOffset.UTC)))).isEqualTo("PT1H");
  }

  @Test
  void aReturnedParameterKeepsItsJavaType() {
    try (final ResultSet resultSet = database.query("opencypher", "RETURN $d AS x", Map.of("d", UTC))) {
      assertThat((Object) resultSet.next().getProperty("x")).isEqualTo(UTC);
    }
  }

  @Test
  void aTemporalParameterSetOnAPropertyIsStoredAsTheNativeTypeItWas() {
    database.transaction(() -> database.command("opencypher", "CREATE (n:S2) SET n.z = $z, n.t = $t",
        Map.of("z", UTC, "t", LocalTime.of(8, 5))));
    final var vertex = database.query("sql", "SELECT FROM S2").next().getVertex().get();
    assertThat(vertex.get("z")).isInstanceOf(ZonedDateTime.class);
    assertThat(vertex.get("t")).isInstanceOf(LocalTime.class);
  }

  private String single(final String statement, final Map<String, Object> parameters) {
    try (final ResultSet resultSet = database.query("opencypher", statement, parameters)) {
      final Result row = resultSet.next();
      assertThat(resultSet.hasNext()).isFalse();
      return String.valueOf((Object) row.getProperty("x"));
    }
  }
}
