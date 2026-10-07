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
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.query.opencypher.temporal.CypherDuration;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #8572: the native Cypher temporal types (OFFSET_TIME, LOCAL_TIME, ZONED_DATETIME, DURATION) can be indexed by both
 * index families, survive a reopen, convert a String stored before they were declared, and are migrated by the script the
 * documentation gives.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8572NativeTemporalIndexTest extends TestHelper {

  // type, index algorithm
  private static final String CASES = """
      OFFSET_TIME,LSM_TREE
      OFFSET_TIME,HASH
      LOCAL_TIME,LSM_TREE
      LOCAL_TIME,HASH
      ZONED_DATETIME,LSM_TREE
      ZONED_DATETIME,HASH
      DURATION,LSM_TREE
      DURATION,HASH
      """;

  /** Three strictly increasing values per type, as Cypher literals. */
  private static String[] literals(final Type type) {
    return switch (type) {
      case OFFSET_TIME -> new String[] { "time('08:00:00+01:00')", "time('09:30:00-05:00')", "time('23:15:00+00:00')" };
      case LOCAL_TIME -> new String[] { "localtime('01:00:00')", "localtime('12:30:15.5')", "localtime('23:59:59')" };
      case ZONED_DATETIME ->
          new String[] { "datetime('2020-01-01T10:00:00+02:00')", "datetime('2021-06-01T10:00:00[Europe/Rome]')", "datetime('2030-01-01T00:00:00Z')" };
      default -> new String[] { "duration('PT1S')", "duration('P1DT1S')", "duration('P1M')" };
    };
  }

  @ParameterizedTest
  @CsvSource(textBlock = CASES)
  void indexedLookupAndRangeAreCorrectAndSurviveReopen(final String typeName, final String algorithm) {
    final Type type = Type.valueOf(typeName);
    final VertexType vertexType = database.getSchema().createVertexType("T");
    vertexType.createProperty("p", type);
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.valueOf(algorithm), false, "T", "p");

    final String[] values = literals(type);
    for (int i = 0; i < values.length; i++)
      database.command("opencypher", "CREATE (:T {id: " + i + ", p: " + values[i] + "})");

    assertIndexedResults(values);
    reopenDatabase();
    assertIndexedResults(values);
  }

  private void assertIndexedResults(final String[] values) {
    for (int i = 0; i < values.length; i++) {
      final List<Integer> ids = ids("MATCH (n:T) WHERE n.p = " + values[i] + " RETURN n.id AS id");
      assertThat(ids).as("equality " + values[i]).containsExactly(i);
    }
    // the SQL side compares the native value too, whatever the algorithm
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM T")) {
      assertThat(rs.next().<Long>getProperty("c")).isEqualTo(3L);
    }
  }

  @ParameterizedTest
  @CsvSource(textBlock = """
      OFFSET_TIME
      ZONED_DATETIME
      LOCAL_TIME
      DURATION
      """)
  void rangeScanOnLsmIsOrdered(final String typeName) {
    final Type type = Type.valueOf(typeName);
    database.getSchema().createVertexType("T").createProperty("p", type);
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "T", "p");
    final String[] values = literals(type);
    // inserted out of order on purpose
    final int[] order = { 2, 0, 1 };
    for (final int i : order)
      database.command("opencypher", "CREATE (:T {id: " + i + ", p: " + values[i] + "})");

    assertThat(ids("MATCH (n:T) WHERE n.p > " + values[0] + " RETURN n.id AS id ORDER BY n.p")).containsExactly(1, 2);
    assertThat(ids("MATCH (n:T) WHERE n.p <= " + values[1] + " RETURN n.id AS id ORDER BY n.p")).containsExactly(0, 1);
    assertThat(ids("MATCH (n:T) RETURN n.id AS id ORDER BY n.p")).containsExactly(0, 1, 2);
  }

  @ParameterizedTest
  @CsvSource(textBlock = """
      LSM_TREE
      HASH
      """)
  void uniqueIndexRefusesTheSameInstantInTwoOffsets(final String algorithm) {
    database.getSchema().createVertexType("U").createProperty("p", Type.OFFSET_TIME);
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.valueOf(algorithm), true, "U", "p");
    database.transaction(() -> database.newVertex("U").set("p", OffsetTime.of(10, 0, 0, 0, ZoneOffset.ofHours(2))).save());
    assertThatThrownBy(
        () -> database.transaction(() -> database.newVertex("U").set("p", OffsetTime.of(8, 0, 0, 0, ZoneOffset.UTC)).save()))
        .isInstanceOf(DuplicatedKeyException.class);

    database.getSchema().createVertexType("Z").createProperty("p", Type.ZONED_DATETIME);
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.valueOf(algorithm), true, "Z", "p");
    database.transaction(
        () -> database.newVertex("Z").set("p", ZonedDateTime.parse("2024-01-01T12:00:00+01:00")).save());
    assertThatThrownBy(() -> database.transaction(
        () -> database.newVertex("Z").set("p", ZonedDateTime.parse("2024-01-01T11:00:00Z")).save()))
        .isInstanceOf(DuplicatedKeyException.class);
  }

  @Test
  void stringStoredBeforeTheDeclarationIsConvertedOnReadAndSavedNatively() {
    database.getSchema().createVertexType("Old");
    database.transaction(() -> {
      database.newVertex("Old").set("t", "09:00-17:00").set("lt", "10:15:30").set("dt", "2024-01-01T10:00:00+02:00[Europe/Rome]")
          .set("d", "P1M2DT3S").save();
    });
    // undeclared: a String is a String, with no interpretation
    try (final ResultSet rs = database.query("sql", "SELECT t FROM Old")) {
      assertThat(rs.next().<Object>getProperty("t")).isEqualTo("09:00-17:00");
    }

    database.command("sql", "CREATE PROPERTY Old.t OFFSET_TIME");
    database.command("sql", "CREATE PROPERTY Old.lt LOCAL_TIME");
    database.command("sql", "CREATE PROPERTY Old.dt ZONED_DATETIME");
    database.command("sql", "CREATE PROPERTY Old.d DURATION");

    try (final ResultSet rs = database.query("sql", "SELECT t, lt, dt, d FROM Old")) {
      final var r = rs.next();
      assertThat(r.<Object>getProperty("t")).isEqualTo(OffsetTime.of(9, 0, 0, 0, ZoneOffset.ofHours(-17)));
      assertThat(r.<Object>getProperty("lt")).isEqualTo(LocalTime.of(10, 15, 30));
      assertThat(r.<Object>getProperty("dt")).isInstanceOf(ZonedDateTime.class);
      assertThat(r.<Object>getProperty("d")).isEqualTo(new CypherDuration(1, 2, 3, 0));
    }
    // and Cypher reads them as temporals
    try (final ResultSet rs = database.query("opencypher", "MATCH (o:Old) RETURN o.t.hour AS h, o.d.months AS m")) {
      final var r = rs.next();
      assertThat(((Number) r.getProperty("h")).intValue()).isEqualTo(9);
      assertThat(((Number) r.getProperty("m")).intValue()).isEqualTo(1);
    }
  }

  @Test
  void migrationScriptRewritesStringsAsNativeValues() {
    database.getSchema().createVertexType("Place");
    database.transaction(() -> {
      database.newVertex("Place").set("hours", "09:00-17:00").set("every", "P1M2DT3S").save();
      database.newVertex("Place").set("hours", "08:30+01:00").set("every", "PT90S").save();
      database.newVertex("Place").set("every", "PT1S").save();
    });

    // The migration documented for #8572: park each String in a scratch property, declare the property with its native
    // type, and copy the String back into it, which stores the native value. (UPDATE SET p = p is not it: with p
    // declared, the read has already converted the String, so the value looks unchanged and the write is skipped.)
    database.command("sqlscript", """
        BEGIN;
        UPDATE Place SET hours_old = hours, every_old = every;
        UPDATE Place REMOVE hours, every;
        COMMIT;
        CREATE PROPERTY Place.hours OFFSET_TIME;
        CREATE PROPERTY Place.every DURATION;
        BEGIN;
        UPDATE Place SET hours = hours_old WHERE hours_old IS NOT NULL;
        UPDATE Place SET every = every_old WHERE every_old IS NOT NULL;
        UPDATE Place REMOVE hours_old, every_old;
        COMMIT;
        """);

    reopenDatabase();
    // the records hold the native value: undeclare the type and read what is on disk
    database.command("sql", "DROP PROPERTY Place.hours");
    database.command("sql", "DROP PROPERTY Place.every");
    try (final ResultSet rs = database.query("sql", "SELECT hours, every FROM Place ORDER BY every")) {
      int nulls = 0;
      while (rs.hasNext()) {
        final var r = rs.next();
        final Object hours = r.getProperty("hours");
        if (hours == null)
          ++nulls;
        else
          assertThat(hours).isInstanceOf(OffsetTime.class);
        assertThat(r.<Object>getProperty("every")).isInstanceOf(CypherDuration.class);
      }
      assertThat(nulls).isEqualTo(1);
    }
  }

  private List<Integer> ids(final String cypher) {
    final List<Integer> ids = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", cypher)) {
      while (rs.hasNext())
        ids.add(((Number) rs.next().getProperty("id")).intValue());
    }
    return ids;
  }
}
