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
import com.arcadedb.query.opencypher.temporal.CypherDuration;
import com.arcadedb.query.opencypher.temporal.CypherTime;
import com.arcadedb.query.opencypher.temporal.TemporalUtil;
import com.arcadedb.schema.Type;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #8572: an undeclared property holding a String that merely looks temporal
 * ({@code "09:00-17:00"}, {@code "P100D"}) was re-read by openCypher as a temporal, so an equality against the literal
 * it was written with matched nothing. Cypher temporals are now persisted with self-describing types, so a String
 * stays a String and a temporal stays a temporal, with no guess on the read side.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8572UndeclaredTemporalSniffTest extends TestHelper {

  @ParameterizedTest
  @ValueSource(strings = { "09:00-17:00", "09:00-19:00", "P100D", "10:35:00-08:00", "2024-01-01T10:00:00+02:00[Europe/Rome]" })
  void undeclaredStringThatLooksTemporalStaysAString(final String text) {
    database.getSchema().createVertexType("Loose");
    database.command("opencypher", "CREATE (:Loose {name: 'a', v: $v})", Map.of("v", text));

    try (final ResultSet rs = database.query("opencypher", "MATCH (l:Loose) RETURN l.v AS v")) {
      assertThat((Object) rs.next().getProperty("v")).isEqualTo(text);
    }
    try (final ResultSet rs = database.query("opencypher", "MATCH (l:Loose) WHERE l.v = $v RETURN l.name AS n", Map.of("v", text))) {
      assertThat(rs.hasNext()).isTrue();
      assertThat((Object) rs.next().getProperty("n")).isEqualTo("a");
    }
  }

  @Test
  void undeclaredTemporalsStillRoundTripAsTemporals() {
    database.getSchema().createVertexType("Loose");
    database.command("opencypher",
        "CREATE (:Loose {t: time('09:00-17:00'), lt: localtime('10:15:30'), dt: datetime('2024-01-01T10:00:00+02:00[Europe/Rome]'),"
            + " d: duration('P1M2DT3S')})");

    try (final ResultSet rs = database.query("opencypher",
        "MATCH (l:Loose) RETURN l.t.hour AS th, l.lt.minute AS ltm, l.dt.timezone AS tz, l.d.months AS dm, l.d.seconds AS ds")) {
      final var r = rs.next();
      assertThat(((Number) r.getProperty("th")).intValue()).isEqualTo(9);
      assertThat(((Number) r.getProperty("ltm")).intValue()).isEqualTo(15);
      assertThat((Object) r.getProperty("tz")).isEqualTo("Europe/Rome");
      assertThat(((Number) r.getProperty("dm")).intValue()).isEqualTo(1);
      assertThat(((Number) r.getProperty("ds")).intValue()).isEqualTo(3);
    }
    try (final ResultSet rs = database.query("opencypher", "MATCH (l:Loose) WHERE l.t = time('09:00-17:00') RETURN count(*) AS c")) {
      assertThat(((Number) rs.next().getProperty("c")).intValue()).isEqualTo(1);
    }
  }

  @Test
  void declaredTemporalPropertyKeepsItsTypeAndSqlSeesNativeValues() {
    database.getSchema().createVertexType("Typed").createProperty("t", Type.OFFSET_TIME);
    database.command("opencypher", "CREATE (:Typed {t: time('09:00-17:00')})");
    try (final ResultSet rs = database.query("sql", "SELECT t FROM Typed")) {
      assertThat(rs.next().<Object>getProperty("t")).isEqualTo(OffsetTime.of(9, 0, 0, 0, ZoneOffset.ofHours(-17)));
    }
  }

  @Test
  void temporalStoredWithoutSchemaIsNotAStringForSql() {
    database.getSchema().createVertexType("Loose");
    database.command("opencypher", "CREATE (:Loose {t: time('09:00-17:00')})");
    try (final ResultSet rs = database.query("sql", "SELECT t FROM Loose")) {
      assertThat(rs.next().<Object>getProperty("t")).isInstanceOf(OffsetTime.class);
    }
  }

  @Test
  void nativeTemporalsSerializeToJsonAndSurviveReopen() {
    database.getSchema().createVertexType("Loose");
    database.command("opencypher",
        "CREATE (:Loose {t: time('09:00-17:00'), lt: localtime('10:15:30'), dt: datetime('2024-01-01T10:00:00+02:00[Europe/Rome]'),"
            + " d: duration('P1M2DT3S')})");
    try (final ResultSet rs = database.query("sql", "SELECT FROM Loose")) {
      final String json = rs.next().toJSON().toString();
      assertThat(json).contains("17:00").contains("10:15:30").contains("Europe/Rome").contains("P1M2DT3S");
    }
    reopenDatabase();
    try (final ResultSet rs = database.query("opencypher", "MATCH (l:Loose) RETURN l.t AS t, l.dt AS dt, l.d AS d")) {
      final var r = rs.next();
      assertThat(r.<Object>getProperty("t")).isEqualTo(new CypherTime(OffsetTime.of(9, 0, 0, 0, ZoneOffset.ofHours(-17))));
      assertThat(r.<Object>getProperty("d")).isEqualTo(new CypherDuration(1, 2, 3, 0));
    }
  }

  @Test
  void edgeValuesRoundTripThroughTheNativeTypes() {
    database.getSchema().createVertexType("Edge8572");
    database.command("opencypher",
        "CREATE (:Edge8572 {dt: datetime('1969-12-31T23:59:59.123456789-03:30'), lt: localtime('23:59:59.999999999'),"
            + " t: time('00:00:00+18:00'), d: duration({months: -14, days: 3, seconds: -90, nanoseconds: 5})})");
    reopenDatabase();
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (n:Edge8572) RETURN toString(n.dt) AS dt, toString(n.lt) AS lt, toString(n.t) AS t, toString(n.d) AS d")) {
      final var r = rs.next();
      assertThat(r.<String>getProperty("dt")).isEqualTo("1969-12-31T23:59:59.123456789-03:30");
      assertThat(r.<String>getProperty("lt")).isEqualTo("23:59:59.999999999");
      assertThat(r.<String>getProperty("t")).isEqualTo("00:00+18:00");
      assertThat(r.<String>getProperty("d")).isEqualTo(
          TemporalUtil.toStorageText(new CypherDuration(-14, 3, -90, 5)));
    }
  }
}
