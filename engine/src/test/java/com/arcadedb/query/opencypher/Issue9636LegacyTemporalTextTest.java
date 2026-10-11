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
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.opencypher.procedures.db.DbMigrateLegacyTemporals;
import com.arcadedb.query.opencypher.temporal.CypherDuration;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * Regression test for issue #9636: a release before the native temporal types (#8572) stored an openCypher
 * {@code duration()}, {@code time()}, {@code localtime()} or zoned {@code datetime()} on an undeclared property as its ISO
 * text and restored it on read by its shape. After the upgrade those values read back as plain Strings.
 * <p>
 * A legacy database is reproduced the way a release before #8572 left it: the temporal is a String in the record (what
 * {@code toCoreJavaType} answered then), and the schema file carries no {@code temporalStorage} setting.
 */
class Issue9636LegacyTemporalTextTest extends TestHelper {

  @Test
  void newDatabaseIsNativeAndStaysSoAfterReopen() {
    database.getSchema().createVertexType("T");
    assertThat(database.getSchema().getEmbedded().isLegacyTemporalText()).isFalse();
    database.transaction(() -> database.newVertex("T").set("d", "P1D").save());

    reopenDatabase();

    assertThat(database.getSchema().getEmbedded().isLegacyTemporalText()).isFalse();
    // #8572 unchanged: on a native database a String that looks temporal is a String
    try (final ResultSet rs = database.query("opencypher", "MATCH (n:T) RETURN n.d AS d")) {
      assertThat(rs.next().<Object>getProperty("d")).isEqualTo("P1D");
    }
  }

  @Test
  void legacyDatabaseReadsUndeclaredTemporalTextAsTemporal() throws IOException {
    writeLegacyRecords();
    makeLegacy();

    assertThat(database.getSchema().getEmbedded().isLegacyTemporalText()).isTrue();

    // Component access, the repro of the issue
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (n:T {name: 'a'}) RETURN n.d.days AS days, n.t.hour AS th, n.lt.minute AS ltm, n.dt.timezone AS tz")) {
      final Result r = rs.next();
      assertThat(((Number) r.getProperty("days")).intValue()).isEqualTo(1);
      assertThat(((Number) r.getProperty("th")).intValue()).isEqualTo(9);
      assertThat(((Number) r.getProperty("ltm")).intValue()).isEqualTo(15);
      assertThat(r.<Object>getProperty("tz")).isEqualTo("Europe/Rome");
    }

    // Equality against a temporal
    try (final ResultSet rs = database.query("opencypher", "MATCH (n:T) WHERE n.d = duration({days: 1}) RETURN n.name AS name")) {
      assertThat(rs.next().<Object>getProperty("name")).isEqualTo("a");
      assertThat(rs.hasNext()).isFalse();
    }

    // ORDER BY sorts durations as durations: as text "P10D" sorts before "P2D"
    final List<Object> names = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", "MATCH (n:T) RETURN n.name AS name ORDER BY n.d")) {
      while (rs.hasNext())
        names.add(rs.next().getProperty("name"));
    }
    assertThat(names).containsExactly("a", "b", "c");
  }

  @Test
  void legacyDatabaseNeverRestoresADeclaredStringOrPlainText() throws IOException {
    database.getSchema().createVertexType("S").createProperty("code", Type.STRING);
    database.transaction(() -> database.newVertex("S").set("code", "P100D").set("free", "Paris").save());
    makeLegacy();

    try (final ResultSet rs = database.query("opencypher", "MATCH (n:S) RETURN n.code AS code, n.free AS free")) {
      final Result r = rs.next();
      assertThat(r.<Object>getProperty("code")).isEqualTo("P100D");
      assertThat(r.<Object>getProperty("free")).isEqualTo("Paris");
    }

    // MERGE finds the declared STRING by the text the temporal would be stored as (issue #9695), not by a legacy restore:
    // the text is still text, so no duplicate is created and the property reads back as the String it holds
    database.command("opencypher", "MERGE (n:S {code: duration('P100D')})");
    try (final ResultSet rs = database.query("opencypher", "MATCH (n:S) RETURN count(n) AS c, n.code AS code")) {
      final Result r = rs.next();
      assertThat(((Number) r.getProperty("c")).intValue()).isEqualTo(1);
      assertThat(r.<Object>getProperty("code")).isEqualTo("P100D");
    }
  }

  @Test
  void aMigrationThatFailsLeavesTheDatabaseLegacyAndARerunCompletesIt() throws IOException {
    writeLegacyRecords();
    // A record the migration cannot save: it predates a mandatory property
    database.getSchema().getType("T").createProperty("required", Type.STRING).setMandatory(true);
    makeLegacy();

    try (final ResultSet rs = database.command("opencypher", "CALL db.migrateLegacyTemporals()")) {
      rs.next();
      fail("the migration must fail on a record it cannot save");
    } catch (final RuntimeException expected) {
      // the record validation refused the save
    }

    assertThat(database.getSchema().getEmbedded().isLegacyTemporalText()).isTrue();
    try (final ResultSet rs = database.query("sql", "SELECT d FROM T WHERE name = 'a'")) {
      assertThat(rs.next().<Object>getProperty("d")).isEqualTo("P1D");
    }
    // Still read as a temporal by openCypher, as before the attempt
    try (final ResultSet rs = database.query("opencypher", "MATCH (n:T {name: 'a'}) RETURN n.d.days AS days")) {
      assertThat(((Number) rs.next().getProperty("days")).intValue()).isEqualTo(1);
    }

    database.getSchema().getType("T").getProperty("required").setMandatory(false);
    try (final ResultSet rs = database.command("opencypher", "CALL db.migrateLegacyTemporals()")) {
      assertThat(((Number) rs.next().getProperty("records")).longValue()).isEqualTo(3L);
    }
    assertThat(database.getSchema().getEmbedded().isLegacyTemporalText()).isFalse();
    try (final ResultSet rs = database.query("sql", "SELECT d FROM T WHERE name = 'a'")) {
      assertThat(rs.next().<Object>getProperty("d")).isEqualTo(new CypherDuration(0, 1, 0, 0));
    }
  }

  @Test
  void mergeOnLegacyDatabaseMatchesTheTemporalText() throws IOException {
    writeLegacyRecords();
    makeLegacy();

    database.command("opencypher", "MERGE (n:T {d: duration({days: 1})})");

    try (final ResultSet rs = database.query("opencypher", "MATCH (n:T) RETURN count(n) AS c")) {
      assertThat(((Number) rs.next().getProperty("c")).intValue()).isEqualTo(3);
    }
  }

  @Test
  void migrationRewritesTheTextNativelyAndEndsTheLegacyReading() throws IOException {
    writeLegacyRecords();
    database.getSchema().createVertexType("S").createProperty("code", Type.STRING);
    database.transaction(() -> database.newVertex("S").set("code", "P100D").set("free", "Paris").save());
    makeLegacy();

    try (final ResultSet rs = database.command("opencypher", "CALL db.migrateLegacyTemporals() YIELD records, properties RETURN records, properties")) {
      final Result r = rs.next();
      // a: d, t, lt, dt. b and c: d
      assertThat(((Number) r.getProperty("records")).longValue()).isEqualTo(3L);
      assertThat(((Number) r.getProperty("properties")).longValue()).isEqualTo(6L);
    }
    assertThat(database.getSchema().getEmbedded().isLegacyTemporalText()).isFalse();

    // SQL sees the native values, not the text
    try (final ResultSet rs = database.query("sql", "SELECT d, t, lt, dt FROM T WHERE name = 'a'")) {
      final Result r = rs.next();
      assertThat(r.<Object>getProperty("d")).isEqualTo(new CypherDuration(0, 1, 0, 0));
      assertThat(r.<Object>getProperty("t")).isInstanceOf(OffsetTime.class);
      assertThat(r.<Object>getProperty("lt")).isInstanceOf(LocalTime.class);
      assertThat(r.<Object>getProperty("dt")).isInstanceOf(ZonedDateTime.class);
    }
    // A declared STRING and plain text are left alone
    try (final ResultSet rs = database.query("sql", "SELECT code, free FROM S")) {
      final Result r = rs.next();
      assertThat(r.<Object>getProperty("code")).isEqualTo("P100D");
      assertThat(r.<Object>getProperty("free")).isEqualTo("Paris");
    }

    reopenDatabase();
    assertThat(database.getSchema().getEmbedded().isLegacyTemporalText()).isFalse();
    try (final ResultSet rs = database.query("opencypher", "MATCH (n:T {name: 'a'}) RETURN n.d.days AS days, n.dt.timezone AS tz")) {
      final Result r = rs.next();
      assertThat(((Number) r.getProperty("days")).intValue()).isEqualTo(1);
      assertThat(r.<Object>getProperty("tz")).isEqualTo("Europe/Rome");
    }
    // A String written from now on is a String
    database.command("opencypher", "CREATE (:T {name: 'z', d: 'P3D'})");
    try (final ResultSet rs = database.query("opencypher", "MATCH (n:T {name: 'z'}) RETURN n.d AS d")) {
      assertThat(rs.next().<Object>getProperty("d")).isEqualTo("P3D");
    }
  }

  @Test
  void migrationConvertsEdgesLeavesListsAndRunsSeveralBatches() throws IOException {
    final int count = DbMigrateLegacyTemporals.BATCH_SIZE * 2 + 7;
    database.getSchema().createVertexType("V");
    database.getSchema().createEdgeType("E");
    database.transaction(() -> {
      final MutableVertex first = database.newVertex("V").set("d", "P1D").set("list", List.of("P2D", "P3D")).save();
      for (int i = 1; i < count; i++)
        database.newVertex("V").set("d", "P" + i + "D").save();
      first.newEdge("E", first).set("since", "2024-01-01T10:00+01:00[Europe/Rome]").save();
    });
    makeLegacy();

    try (final ResultSet rs = database.command("opencypher", "CALL db.migrateLegacyTemporals()")) {
      final Result r = rs.next();
      assertThat(((Number) r.getProperty("records")).longValue()).isEqualTo(count + 1L);
      assertThat(((Number) r.getProperty("properties")).longValue()).isEqualTo(count + 1L);
    }

    final int[] durations = new int[1];
    database.iterateType("V", false).forEachRemaining(record -> {
      assertThat(record.asDocument().get("d")).isInstanceOf(CypherDuration.class);
      ++durations[0];
    });
    assertThat(durations[0]).isEqualTo(count);
    try (final ResultSet rs = database.query("sql", "SELECT since FROM E")) {
      assertThat(rs.next().<Object>getProperty("since")).isInstanceOf(ZonedDateTime.class);
    }
    // The elements of a list were text on a property read before the native types too: left as they are
    try (final ResultSet rs = database.query("sql", "SELECT list FROM V WHERE list IS NOT NULL")) {
      assertThat(rs.next().<List<Object>>getProperty("list")).containsExactly("P2D", "P3D");
    }
  }

  @Test
  void migrationSurvivesARollbackOfTheCallersTransaction() throws IOException {
    writeLegacyRecords();
    makeLegacy();

    // The HTTP handlers wrap every command in a transaction: the batches commit on their own, nested inside it
    database.begin();
    try (final ResultSet rs = database.command("opencypher", "CALL db.migrateLegacyTemporals()")) {
      assertThat(((Number) rs.next().getProperty("records")).longValue()).isEqualTo(3L);
    }
    database.rollback();

    try (final ResultSet rs = database.query("sql", "SELECT d FROM T WHERE name = 'b'")) {
      assertThat(rs.next().<Object>getProperty("d")).isEqualTo(new CypherDuration(0, 2, 0, 0));
    }
    assertThat(database.getSchema().getEmbedded().isLegacyTemporalText()).isFalse();
    reopenDatabase();
    assertThat(database.getSchema().getEmbedded().isLegacyTemporalText()).isFalse();
    try (final ResultSet rs = database.query("sql", "SELECT d FROM T WHERE name = 'b'")) {
      assertThat(rs.next().<Object>getProperty("d")).isEqualTo(new CypherDuration(0, 2, 0, 0));
    }
  }

  @Test
  void migrationOnANativeDatabaseConvertsNothing() {
    database.getSchema().createVertexType("T");
    database.transaction(() -> database.newVertex("T").set("d", "P1D").save());

    try (final ResultSet rs = database.command("opencypher", "CALL db.migrateLegacyTemporals()")) {
      final Result r = rs.next();
      assertThat(((Number) r.getProperty("records")).longValue()).isZero();
      assertThat(((Number) r.getProperty("properties")).longValue()).isZero();
    }
    try (final ResultSet rs = database.query("sql", "SELECT d FROM T")) {
      assertThat(rs.next().<Object>getProperty("d")).isEqualTo("P1D");
    }
  }

  /**
   * The records a release before #8572 wrote for {@code CREATE (:T {d: duration(...), ...})}: the temporals as text.
   */
  private void writeLegacyRecords() {
    database.getSchema().createVertexType("T");
    database.transaction(() -> {
      database.newVertex("T").set("name", "a").set("d", "P1D").set("t", "09:00-17:00").set("lt", "10:15:30")
          .set("dt", "2024-01-01T10:00+01:00[Europe/Rome]").save();
      database.newVertex("T").set("name", "b").set("d", "P2D").save();
      database.newVertex("T").set("name", "c").set("d", "P10D").save();
    });
  }

  /**
   * Closes the database and removes the temporal storage setting from its schema file, which is what a schema written by
   * a release before #8572 looks like, then opens it again.
   */
  private void makeLegacy() throws IOException {
    database.close();
    final File schemaFile = new File(getDatabasePath(), "schema.json");
    final JSONObject root = new JSONObject(Files.readString(schemaFile.toPath(), StandardCharsets.UTF_8));
    assertThat(root.getJSONObject("settings").has("temporalStorage")).isTrue();
    root.getJSONObject("settings").remove("temporalStorage");
    Files.writeString(schemaFile.toPath(), root.toString(), StandardCharsets.UTF_8);
    database = factory.open();
    assertThat(DatabaseFactory.getActiveDatabaseInstance(database.getDatabasePath())).isEqualTo(database);
  }
}
