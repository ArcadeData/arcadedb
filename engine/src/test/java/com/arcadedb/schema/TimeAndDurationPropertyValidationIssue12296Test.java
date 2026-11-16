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
package com.arcadedb.schema;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Document;
import com.arcadedb.query.opencypher.temporal.CypherDuration;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.Period;
import java.time.ZoneOffset;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #12296: a property declared OFFSET_TIME, LOCAL_TIME or DURATION did not check what was written to it. An unparseable
 * String was stored as null, a number, a boolean, a list or a date was stored as it was, and any other temporal kind failed
 * with a ClassCastException at serialization. A value is now converted to the declared kind or the write is refused with an
 * error naming the property, as DATE, DATETIME and ZONED_DATETIME do. Separately, a localdatetime() with a nanosecond digit
 * outside the range a nanosecond count can hold could not be stored on an undeclared property at all.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class TimeAndDurationPropertyValidationIssue12296Test extends TestHelper {

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE MX");
    database.command("sql", "CREATE PROPERTY MX.ot OFFSET_TIME");
    database.command("sql", "CREATE PROPERTY MX.lt LOCAL_TIME");
    database.command("sql", "CREATE PROPERTY MX.du DURATION");
    database.command("sql", "CREATE VERTEX TYPE NN");
    database.command("sql", "CREATE PROPERTY NN.ot OFFSET_TIME (notnull)");
    database.command("sql", "CREATE VERTEX TYPE LD");
  }

  @Nested
  class Refused {
    @Test
    void anUnparseableStringIsRefusedNotStoredAsNull() {
      assertRefusedSql("ot", "'garbage'");
      assertRefusedSql("lt", "'garbage'");
      assertRefusedSql("du", "'garbage'");
      assertRefusedSql("lt", "'24:00+00:00'");
      assertRefusedSql("ot", "'24:00+00:00'");
    }

    @Test
    void aNotNullPropertyNamesTheConversionNotTheNull() {
      assertThatThrownBy(() -> database.transaction(() -> database.command("sql", "INSERT INTO NN SET ot = 'garbage'")))
          .hasMessageContaining("ot").hasMessageNotContaining("cannot be null");
    }

    @Test
    void aNumberABooleanAListAndADateAreRefused() {
      for (final String property : new String[] { "ot", "lt", "du" }) {
        assertRefusedCypher(property, "42");
        assertRefusedCypher(property, "3.5");
        assertRefusedCypher(property, "true");
        assertRefusedCypher(property, "[1]");
        assertRefusedCypher(property, "date('2020-01-02')");
        assertRefusedSql(property, "42");
        assertRefusedSql(property, "true");
      }
    }

    @Test
    void aDurationIntoATimeAndATimeIntoADurationAreRefusedNotAClassCastException() {
      assertRefusedCypher("ot", "duration('P1D')");
      assertRefusedCypher("lt", "duration('P1D')");
      for (final String temporal : new String[] { "localtime('10:00')", "time('10:00+01:00')", "localdatetime('2020-01-02T10:00')",
          "datetime('2020-01-02T10:00+01:00')", "date('2020-01-02')" })
        assertRefusedCypher("du", temporal);
    }

    @Test
    void nothingIsStoredByARefusedWrite() {
      assertRefusedCypher("ot", "42");
      assertThat(count()).isZero();
    }
  }

  @Nested
  class Converted {
    @Test
    void stringsParse() {
      assertThat(storeSql("ot", "'10:00+01:00'")).isEqualTo(OffsetTime.of(10, 0, 0, 0, ZoneOffset.ofHours(1)));
      assertThat(storeSql("ot", "'10:00'")).isEqualTo(OffsetTime.of(10, 0, 0, 0, ZoneOffset.UTC));
      assertThat(storeSql("lt", "'10:00'")).isEqualTo(LocalTime.of(10, 0));
      // an offset-bearing time read as the UTC wall clock, as a DATETIME reads an offset-bearing datetime (issue #10489)
      assertThat(storeSql("lt", "'10:00+01:00'")).isEqualTo(LocalTime.of(9, 0));
      assertThat(storeSql("du", "'P1DT2H'")).isEqualTo(CypherDuration.parse("P1DT2H"));
    }

    @Test
    void theTemporalOfTheDeclaredKindIsKept() {
      assertThat(storeCypher("ot", "time('10:00+01:00')")).isEqualTo(OffsetTime.of(10, 0, 0, 0, ZoneOffset.ofHours(1)));
      assertThat(storeCypher("lt", "localtime('10:00')")).isEqualTo(LocalTime.of(10, 0));
      assertThat(storeCypher("du", "duration('P1D')")).isEqualTo(CypherDuration.parse("P1D"));
    }

    @Test
    void anotherTimeOfDayIsConverted() {
      // a local time is at UTC, as the same text without an offset is
      assertThat(storeCypher("ot", "localtime('10:00')")).isEqualTo(OffsetTime.of(10, 0, 0, 0, ZoneOffset.UTC));
      assertThat(storeCypher("lt", "time('10:00+01:00')")).isEqualTo(LocalTime.of(9, 0));
    }

    @Test
    void aDatetimeKeepsItsTimeOfDay() {
      assertThat(storeCypher("ot", "datetime('2020-01-02T10:00+01:00')")).isEqualTo(OffsetTime.of(10, 0, 0, 0, ZoneOffset.ofHours(1)));
      assertThat(storeCypher("ot", "localdatetime('2020-01-02T10:00')")).isEqualTo(OffsetTime.of(10, 0, 0, 0, ZoneOffset.UTC));
      assertThat(storeCypher("lt", "localdatetime('2020-01-02T10:00:00.123456789')")).isEqualTo(LocalTime.of(10, 0, 0, 123456789));
      assertThat(storeCypher("lt", "datetime('2020-01-02T10:00+01:00')")).isEqualTo(LocalTime.of(9, 0));
    }

    @Test
    void javaDurationAndPeriodAreDurations() {
      database.transaction(() -> {
        final Document doc = database.newVertex("MX").set("du", Duration.ofSeconds(90, 5)).save();
        assertThat(doc.get("du")).isEqualTo(new CypherDuration(0, 0, 90, 5));
        final Document period = database.newVertex("MX").set("du", Period.of(1, 2, 3)).save();
        assertThat(period.get("du")).isEqualTo(new CypherDuration(14, 3, 0, 0));
      });
    }

    @Test
    void theStoredValueReadsBackAfterAReload() {
      final Object stored = storeCypher("ot", "datetime('2020-01-02T10:00+01:00')");
      reopenDatabase();
      try (final ResultSet rs = database.query("sql", "SELECT ot FROM MX")) {
        assertThat((Object) rs.next().getProperty("ot")).isEqualTo(stored);
      }
    }
  }

  @Nested
  class UndeclaredLocalDateTime {
    @Test
    void aNanosecondValueOutsideTheNanosRangeIsStoredExactly() {
      for (final String text : new String[] { "2300-01-01T00:00:00.000000001", "9999-12-31T23:59:59.999999999",
          "1600-01-01T00:00:00.000000001" }) {
        database.transaction(() -> database.command("opencypher", "MATCH (n:LD) DELETE n"));
        database.transaction(() -> database.command("opencypher", "CREATE (n:LD {v: localdatetime('" + text + "')})"));
        assertThat(readUndeclared()).isEqualTo(LocalDateTime.parse(text));
      }
    }

    @Test
    void itReadsBackAfterAReloadAndKeepsItsKind() {
      database.transaction(() -> database.command("opencypher", "CREATE (n:LD {v: localdatetime('9999-12-31T23:59:59.999999999')})"));
      reopenDatabase();
      assertThat(readUndeclared()).isEqualTo(LocalDateTime.parse("9999-12-31T23:59:59.999999999"));
      try (final ResultSet rs = database.query("opencypher", "MATCH (n:LD) RETURN n.v < localdatetime('9999-12-31T23:59:59.999999999') AS lt, "
          + "n.v = localdatetime('9999-12-31T23:59:59.999999999') AS eq")) {
        final Result row = rs.next();
        assertThat((Boolean) row.getProperty("lt")).isFalse();
        assertThat((Boolean) row.getProperty("eq")).isTrue();
      }
    }

    @Test
    void itComparesWithTheOtherDatetimeKinds() {
      database.transaction(() -> {
        database.command("opencypher", "CREATE (n:LD {v: localdatetime('9999-12-31T23:59:59.999999999')})");
        database.command("opencypher", "CREATE (n:LD {v: localdatetime('2020-01-01T00:00:00.000000001')})");
      });
      try (final ResultSet rs = database.query("opencypher",
          "MATCH (n:LD) WHERE n.v > localdatetime('2262-04-12T00:00:00') RETURN count(n) AS c")) {
        assertThat(rs.next().<Number>getProperty("c").longValue()).isEqualTo(1L);
      }
      try (final ResultSet rs = database.query("opencypher", "MATCH (n:LD) RETURN n.v AS v ORDER BY n.v DESC")) {
        assertThat(rs.next().getProperty("v").toString()).isEqualTo("9999-12-31T23:59:59.999999999");
      }
      try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM LD WHERE v > '2262-04-12T00:00:00'")) {
        assertThat(rs.next().<Number>getProperty("c").longValue()).isEqualTo(1L);
      }
      try (final ResultSet rs = database.query("sql", "SELECT v FROM LD ORDER BY v DESC")) {
        assertThat((Object) rs.next().getProperty("v")).isEqualTo(LocalDateTime.parse("9999-12-31T23:59:59.999999999"));
      }
    }

    @Test
    void aValueInsideTheNanosRangeIsStoredAsBefore() {
      database.transaction(() -> database.command("opencypher", "CREATE (n:LD {v: localdatetime('2020-01-01T00:00:00.000000001')})"));
      assertThat(readUndeclared()).isEqualTo(LocalDateTime.parse("2020-01-01T00:00:00.000000001"));
    }

    private Object readUndeclared() {
      try (final ResultSet rs = database.query("sql", "SELECT v FROM LD")) {
        final Object v = rs.next().getProperty("v");
        return v instanceof LocalDateTime ? v : v == null ? null : LocalDateTime.parse(v.toString());
      }
    }
  }

  private void assertRefusedSql(final String property, final String value) {
    assertThatThrownBy(() -> database.transaction(() -> database.command("sql", "INSERT INTO MX SET " + property + " = " + value)))
        .as("%s = %s", property, value).isInstanceOf(IllegalArgumentException.class).hasMessageContaining(property);
  }

  private void assertRefusedCypher(final String property, final String value) {
    assertThatThrownBy(() -> database.transaction(() -> database.command("opencypher", "CREATE (n:MX {" + property + ": " + value + "})")))
        .as("%s: %s", property, value).hasMessageContaining(property);
  }

  private Object storeSql(final String property, final String value) {
    database.transaction(() -> database.command("sql", "DELETE FROM MX"));
    database.transaction(() -> database.command("sql", "INSERT INTO MX SET " + property + " = " + value));
    return read(property);
  }

  private Object storeCypher(final String property, final String value) {
    database.transaction(() -> database.command("sql", "DELETE FROM MX"));
    database.transaction(() -> database.command("opencypher", "CREATE (n:MX {" + property + ": " + value + "})"));
    return read(property);
  }

  private Object read(final String property) {
    try (final ResultSet rs = database.query("sql", "SELECT " + property + " FROM MX")) {
      return rs.next().getProperty(property);
    }
  }

  private long count() {
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM MX")) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }
}
