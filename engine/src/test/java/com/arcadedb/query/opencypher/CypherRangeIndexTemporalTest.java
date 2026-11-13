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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.Test;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import com.arcadedb.TestHelper;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.query.opencypher.temporal.CypherDuration;
import com.arcadedb.query.opencypher.temporal.CypherTime;
import com.arcadedb.query.opencypher.temporal.TemporalUtil;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for issue #5008: when a range index exists on the compared temporal property, the
 * query planner picks {@code NodeIndexRangeScan}, whose {@code compareValues()} compared the stored
 * value against the resolved predicate bound with a raw {@link Comparable#compareTo} - bypassing the
 * temporal normalization added in #4906. A native {@code java.time} parameter (e.g. a datetime sent
 * over Bolt) therefore threw {@code ClassCastException} instead of comparing against the stored temporal.
 * <p>
 * Same scenario as {@link OpenCypherTemporalParameterTest} but with an LSM index on the property so the
 * index-scan path (not the general {@code ComparisonExpression} path) is exercised.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherRangeIndexTemporalTest {

  private static final ZonedDateTime BEFORE    = ZonedDateTime.of(2026, 7, 1, 0, 0, 0, 0, ZoneOffset.UTC);
  private static final ZonedDateTime REFERENCE = ZonedDateTime.of(2026, 7, 2, 0, 0, 0, 0, ZoneOffset.UTC);
  private static final ZonedDateTime AFTER     = ZonedDateTime.of(2026, 7, 3, 0, 0, 0, 0, ZoneOffset.UTC);

  @Test
  void upperBoundNativeDatetimeParameterUsesIndex() {
    withDatabase(database -> {
      createEpisodes(database);
      // WHERE valid_at <= $ref : upper-bound-only path (full iterator + compareValues filter).
      assertThat(query(database, "MATCH (e:Episodic) WHERE e.valid_at <= $ref RETURN e.uuid AS uuid", REFERENCE))
          .containsExactlyInAnyOrder("before", "reference");
    });
  }

  @Test
  void lowerBoundNativeDatetimeParameterUsesIndex() {
    withDatabase(database -> {
      createEpisodes(database);
      // WHERE valid_at >= $ref : lower-bound-only path (iterator from key).
      assertThat(query(database, "MATCH (e:Episodic) WHERE e.valid_at >= $ref RETURN e.uuid AS uuid", REFERENCE))
          .containsExactlyInAnyOrder("reference", "after");
    });
  }

  @Test
  void bothBoundsNativeDatetimeParametersUseIndex() {
    withDatabase(database -> {
      createEpisodes(database);
      // WHERE $lo <= valid_at <= $hi : both-bounds range() path.
      assertThat(query(database,
          "MATCH (e:Episodic) WHERE e.valid_at >= $lo AND e.valid_at <= $hi RETURN e.uuid AS uuid",
          Map.of("lo", BEFORE, "hi", REFERENCE)))
          .containsExactlyInAnyOrder("before", "reference");
    });
  }

  @Test
  void acceptsDifferentNativeParameterTypesOnIndexedProperty() {
    withDatabase(database -> {
      createEpisodes(database);
      // java.util.Date, OffsetDateTime and Instant params against the indexed datetime property.
      assertThat(query(database, "MATCH (e:Episodic) WHERE e.valid_at <= $ref RETURN e.uuid AS uuid",
          Date.from(REFERENCE.toInstant())))
          .containsExactlyInAnyOrder("before", "reference");
      assertThat(query(database, "MATCH (e:Episodic) WHERE e.valid_at <= $ref RETURN e.uuid AS uuid",
          REFERENCE.toOffsetDateTime()))
          .containsExactlyInAnyOrder("before", "reference");
      assertThat(query(database, "MATCH (e:Episodic) WHERE e.valid_at <= $ref RETURN e.uuid AS uuid",
          REFERENCE.toInstant()))
          .containsExactlyInAnyOrder("before", "reference");
    });
  }

  private static void createEpisodes(final Database database) {
    database.transaction(() -> {
      createEpisode(database, "before", BEFORE);
      createEpisode(database, "reference", REFERENCE);
      createEpisode(database, "after", AFTER);
    });
  }

  private static void createEpisode(final Database database, final String uuid, final Object validAt) {
    database.command("opencypher", "CREATE (e:Episodic {uuid: $uuid, valid_at: $valid_at})",
        Map.of("uuid", uuid, "valid_at", validAt));
  }

  private static Set<String> query(final Database database, final String cypher, final Object ref) {
    return query(database, cypher, Map.of("ref", ref));
  }

  private static Set<String> query(final Database database, final String cypher, final Map<String, Object> params) {
    final Set<String> uuids = new HashSet<>();
    final ResultSet rs = database.query("opencypher", cypher, params);
    while (rs.hasNext()) {
      final Result row = rs.next();
      uuids.add(row.getProperty("uuid"));
    }
    return uuids;
  }

  private static void withDatabase(final Consumer<Database> test) {
    final String path = "./target/testrangeindextemporal_" + System.nanoTime();
    final Database database = new DatabaseFactory(path).create();
    try {
      database.transaction(() -> {
        final Schema schema = database.getSchema();
        final VertexType type = schema.createVertexType("Episodic");
        // DATETIME_MICROS stores as LocalDateTime, matching the ClassCastException in the issue's stack trace.
        type.createProperty("valid_at", Type.DATETIME_MICROS);
        type.createProperty("uuid", Type.STRING);
        schema.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "Episodic", "valid_at");
      });
      test.accept(database);
    } finally {
      database.drop();
    }
  }

  /**
   * Issue #8572: the native Cypher temporal types (OFFSET_TIME, LOCAL_TIME, ZONED_DATETIME, DURATION) can be indexed by both
   * index families, survive a reopen, convert a String stored before they were declared, and are migrated by the script the
   * documentation gives.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue8572NativeTemporalIndex extends TestHelper {

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

      if (type == Type.DURATION) {
        // Durations are not comparable (issue #10200): a range keeps no row but the equal one, through the index as by scan
        assertThat(ids("MATCH (n:T) WHERE n.p > " + values[0] + " RETURN n.id AS id ORDER BY n.p")).isEmpty();
        assertThat(ids("MATCH (n:T) WHERE n.p <= " + values[1] + " RETURN n.id AS id ORDER BY n.p")).containsExactly(1);
      } else {
        assertThat(ids("MATCH (n:T) WHERE n.p > " + values[0] + " RETURN n.id AS id ORDER BY n.p")).containsExactly(1, 2);
        assertThat(ids("MATCH (n:T) WHERE n.p <= " + values[1] + " RETURN n.id AS id ORDER BY n.p")).containsExactly(0, 1);
      }
      assertThat(ids("MATCH (n:T) RETURN n.id AS id ORDER BY n.p")).containsExactly(0, 1, 2);
    }

    @ParameterizedTest
    @CsvSource(textBlock = """
        LSM_TREE
        HASH
        """)
    void uniqueIndexTellsTheSameInstantInTwoOffsetsApart(final String algorithm) {
      // The same instant in two offsets is two values in openCypher, and to the row filter (issue #9648): the unique index
      // takes both, and refuses only the same value written twice
      database.getSchema().createVertexType("U").createProperty("p", Type.OFFSET_TIME);
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.valueOf(algorithm), true, "U", "p");
      database.transaction(() -> database.newVertex("U").set("p", OffsetTime.of(10, 0, 0, 0, ZoneOffset.ofHours(2))).save());
      database.transaction(() -> database.newVertex("U").set("p", OffsetTime.of(8, 0, 0, 0, ZoneOffset.UTC)).save());
      assertThatThrownBy(() -> database.transaction(
          () -> database.newVertex("U").set("p", OffsetTime.of(10, 0, 0, 0, ZoneOffset.ofHours(2))).save()))
          .isInstanceOf(DuplicatedKeyException.class);

      database.getSchema().createVertexType("Z").createProperty("p", Type.ZONED_DATETIME);
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.valueOf(algorithm), true, "Z", "p");
      database.transaction(
          () -> database.newVertex("Z").set("p", ZonedDateTime.parse("2024-01-01T12:00:00+01:00")).save());
      database.transaction(() -> database.newVertex("Z").set("p", ZonedDateTime.parse("2024-01-01T11:00:00Z")).save());
      assertThatThrownBy(() -> database.transaction(
          () -> database.newVertex("Z").set("p", ZonedDateTime.parse("2024-01-01T12:00:00+01:00")).save()))
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

  /**
   * Regression test for issue #8572: an undeclared property holding a String that merely looks temporal
   * ({@code "09:00-17:00"}, {@code "P100D"}) was re-read by openCypher as a temporal, so an equality against the literal
   * it was written with matched nothing. Cypher temporals are now persisted with self-describing types, so a String
   * stays a String and a temporal stays a temporal, with no guess on the read side.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue8572UndeclaredTemporalSniff extends TestHelper {

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

  /**
   * Issue #9348: ArcadeDB has no native duration type, a duration is stored as its ISO-8601 text, and an index handed that
   * text. An index-backed range predicate on a duration property therefore ordered lexically ({@code P10D} before
   * {@code P2D}) where the same predicate without the index compares durations component by component.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue9348DurationRangeIndex {
    private Database database;

    @BeforeEach
    void setup() {
      final DatabaseFactory factory = new DatabaseFactory("./target/databases/issue9348");
      if (factory.exists())
        factory.open().drop();
      database = factory.create();
    }

    @AfterEach
    void teardown() {
      if (database != null && database.isOpen())
        database.drop();
    }

    @Test
    void rangeOnAnIndexedDurationAnswersLikeTheUnindexedScan() {
      // The index comes first: over existing text values the property would be declared STRING (issue #8384), which is text
      // by contract, so a duration property is one the index was created on while it was still undeclared
      database.command("cypher", "CREATE INDEX FOR (s:S) ON (s.d)");
      insertDurations();

      // Durations are not comparable (issue #10200, as in Neo4j): < and > answer null and keep no row, and >= / <= keep only
      // the equal one. The index must answer exactly as the scan does
      assertThat(names("MATCH (s:S) WHERE s.d > duration({days: 2}) RETURN s.n AS r")).isEmpty();
      assertThat(names("MATCH (s:S) WHERE s.d >= duration({days: 3}) RETURN s.n AS r")).containsExactly("3d");
      assertThat(names("MATCH (s:S) WHERE s.d < duration({days: 10}) RETURN s.n AS r")).isEmpty();
      assertThat(names("MATCH (s:S) WHERE s.d <= duration({days: 3}) AND s.d > duration({days: 1}) RETURN s.n AS r")).isEmpty();
      for (final String predicate : new String[] { "s.d > duration({days: 2})", "s.d >= duration({days: 3})", "s.d <= duration({days: 3})" })
        assertThat(names("MATCH (s:S) WHERE " + predicate + " RETURN s.n AS r"))
            .as(predicate).containsExactlyInAnyOrderElementsOf(names("MATCH (s:S) WITH s, s.d AS d WHERE "
                + predicate.replace("s.d", "d") + " RETURN s.n AS r"));
    }

    @Test
    void rangeOnAnIndexedDurationWithParameters() {
      database.command("cypher", "CREATE INDEX FOR (s:S) ON (s.d)");
      insertDurations();

      // Not comparable (issue #10200): the parameter answers as the literal does
      assertThat(names("MATCH (s:S) WHERE s.d > $lo RETURN s.n AS r", Map.of("lo", CypherDuration.parse("P2D")))).isEmpty();
      assertThat(names("MATCH (s:S) WHERE s.d >= $lo RETURN s.n AS r", Map.of("lo", CypherDuration.parse("P3D"))))
          .containsExactly("3d");
      assertThat(names("MATCH (s:S) WHERE s.d < $hi RETURN s.n AS r", Map.of("hi", CypherDuration.parse("P10D")))).isEmpty();
      assertThat(names("MATCH (s:S) WHERE s.d > $lo AND s.d <= $hi RETURN s.n AS r",
          Map.of("lo", CypherDuration.parse("P2D"), "hi", CypherDuration.parse("P10D")))).isEmpty();
      assertThat(names("MATCH (s:S) WHERE s.d <= $hi RETURN s.n AS r", Map.of("hi", CypherDuration.parse("P10D"))))
          .containsExactly("10d");
    }

    @Test
    void orderByOnAnIndexedDurationSortsAsDurations() {
      database.command("cypher", "CREATE INDEX FOR (s:S) ON (s.d)");
      insertDurations();

      assertThat(names("MATCH (s:S) RETURN s.n AS r ORDER BY s.d")).containsExactly("2d", "3d", "10d");
      assertThat(names("MATCH (s:S) RETURN s.n AS r ORDER BY s.d DESC")).containsExactly("10d", "3d", "2d");
    }

    @Test
    void equalityOnAnIndexedDurationStillFindsTheRecord() {
      database.command("cypher", "CREATE INDEX FOR (s:S) ON (s.d)");
      insertDurations();

      assertThat(names("MATCH (s:S) WHERE s.d = duration({days: 10}) RETURN s.n AS r")).containsExactly("10d");
    }

    private void insertDurations() {
      database.transaction(() -> {
        database.command("cypher", "CREATE (:S {n: '2d', d: duration({days: 2})})");
        database.command("cypher", "CREATE (:S {n: '10d', d: duration({days: 10})})");
        database.command("cypher", "CREATE (:S {n: '3d', d: duration({days: 3})})");
      });
    }

    private List<String> names(final String query) {
      return names(query, Map.of());
    }

    private List<String> names(final String query, final Map<String, Object> parameters) {
      final List<String> result = new ArrayList<>();
      try (final ResultSet rs = database.query("cypher", query, parameters)) {
        while (rs.hasNext())
          result.add(rs.next().getProperty("r"));
      }
      return result;
    }
  }
}
