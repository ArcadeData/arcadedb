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
package com.arcadedb.server.ha.raft;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.query.opencypher.temporal.CypherDuration;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.ZonedDateTime;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9712, the HA side of #9636: {@code CALL db.migrateLegacyTemporals()} run on the leader of a Raft cluster must
 * leave every node native, not only the leader. The batches replicate as transactions; the {@code temporalStorage}
 * setting replicates as part of the schema, and each follower has to re-read it so that its own
 * {@code isLegacyTemporalText()} flips too. A follower still legacy while the leader is native would answer the same
 * openCypher read differently: a String with the shape of a temporal, written after the migration, would come back as
 * a temporal on that follower and as a String on the leader.
 * <p>
 * The cluster starts from a legacy database the way a release before the native temporal types left it: temporals
 * stored as ISO text on undeclared properties, and a schema file with no {@code temporalStorage} setting, on every
 * node.
 */
class Issue9712MigrateLegacyTemporalsHAIT extends BaseRaftHATest {

  private static final String TEMPORAL_STORAGE = "temporalStorage";

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Override
  protected boolean persistentRaftStorage() {
    // A follower is restarted below and has to rejoin the same group
    return true;
  }

  /**
   * Writes the records a release before #8572 wrote for {@code CREATE (:T {d: duration(...), ...})}, then strips the
   * temporal storage setting from the schema file before {@code prepareDatabase()} copies the database to every node.
   */
  @Override
  protected void populateDatabase() {
    final Database database = getDatabases()[0];
    database.getSchema().createVertexType("T");
    database.transaction(() -> {
      database.newVertex("T").set("name", "a").set("d", "P1D").set("t", "09:00-17:00").set("lt", "10:15:30")
          .set("dt", "2024-01-01T10:00+01:00[Europe/Rome]").save();
      database.newVertex("T").set("name", "b").set("d", "P2D").save();
      database.newVertex("T").set("name", "c").set("d", "P10D").save();
    });
    database.close();

    try {
      final File schemaFile = new File(getDatabasePath(0), "schema.json");
      final JSONObject root = new JSONObject(Files.readString(schemaFile.toPath(), StandardCharsets.UTF_8));
      assertThat(root.getJSONObject("settings").has(TEMPORAL_STORAGE)).isTrue();
      root.getJSONObject("settings").remove(TEMPORAL_STORAGE);
      Files.writeString(schemaFile.toPath(), root.toString(), StandardCharsets.UTF_8);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }

    // prepareDatabase() closes databases[0] before the copy: hand it an open instance of the legacy database
    getDatabases()[0] = new DatabaseFactory(getDatabasePath(0)).open();
    assertThat(getDatabases()[0].getSchema().getEmbedded().isLegacyTemporalText()).isTrue();
  }

  @Test
  void migrationOnTheLeaderMakesEveryNodeNative() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);

    assertEveryNodeIsLegacy();

    final Database leader = getServerDatabase(leaderIndex, getDatabaseName());
    try (final ResultSet rs = leader.command("opencypher", "CALL db.migrateLegacyTemporals() YIELD records, properties RETURN records, properties")) {
      final Result r = rs.next();
      // a: d, t, lt, dt. b and c: d
      assertThat(((Number) r.getProperty("records")).longValue()).isEqualTo(3L);
      assertThat(((Number) r.getProperty("properties")).longValue()).isEqualTo(6L);
    }
    assertThat(leader.getSchema().getEmbedded().isLegacyTemporalText()).isFalse();

    assertClusterConsistency();
    assertEveryNodeIsNativeWithTheRewrittenRecords();

    // A String written from now on is a String, on every node
    leader.command("opencypher", "CREATE (:T {name: 'z', d: 'P3D'})");
    assertClusterConsistency();
    assertEveryNodeReadsTheNewTextAsAString();

    // A follower restarted after the migration reads the setting back from its own schema file
    final int followerIndex = (leaderIndex + 1) % getServerCount();
    restartServer(followerIndex);
    assertClusterConsistency();
    assertEveryNodeIsNativeWithTheRewrittenRecords();
    assertEveryNodeReadsTheNewTextAsAString();
  }

  /**
   * The path a user takes: the procedure sent over HTTP to a FOLLOWER, which forwards the write to the leader, where the
   * HTTP handler wraps the command in a transaction of its own. The batches commit nested inside it, and the schema is
   * saved, and shipped, at the end of that transaction.
   */
  @Test
  void migrationSentOverHttpToAFollowerMakesEveryNodeNative() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);
    final int followerIndex = (leaderIndex + 1) % getServerCount();

    assertEveryNodeIsLegacy();

    final JSONObject response = executeCommand(followerIndex, "opencypher", "CALL db.migrateLegacyTemporals()");
    assertThat(response).as("the command must succeed on the follower").isNotNull();
    assertThat(getServerDatabase(leaderIndex, getDatabaseName()).getSchema().getEmbedded().isLegacyTemporalText()).isFalse();

    assertClusterConsistency();
    assertEveryNodeIsNativeWithTheRewrittenRecords();

    executeCommand(followerIndex, "opencypher", "CREATE (:T {name: 'z', d: 'P3D'})");
    assertClusterConsistency();
    assertEveryNodeReadsTheNewTextAsAString();
  }

  private void assertEveryNodeIsLegacy() throws IOException {
    for (int i = 0; i < getServerCount(); i++) {
      final Database database = getServerDatabase(i, getDatabaseName());
      assertThat(database.getSchema().getEmbedded().isLegacyTemporalText()).as("server %d starts legacy", i).isTrue();
      assertThat(schemaSettings(database).has(TEMPORAL_STORAGE) ? schemaSettings(database).getString(TEMPORAL_STORAGE) : "text")
          .as("server %d schema file", i).isEqualTo("text");
      // The legacy reading: the text of an undeclared property comes back as a temporal
      try (final ResultSet rs = database.query("opencypher", "MATCH (n:T {name: 'a'}) RETURN n.d.days AS days")) {
        assertThat(((Number) rs.next().getProperty("days")).intValue()).as("server %d", i).isEqualTo(1);
      }
      try (final ResultSet rs = database.query("sql", "SELECT d FROM T WHERE name = 'a'")) {
        assertThat(rs.next().<Object>getProperty("d")).as("server %d stores text", i).isEqualTo("P1D");
      }
    }
  }

  private void assertEveryNodeIsNativeWithTheRewrittenRecords() throws IOException {
    for (int i = 0; i < getServerCount(); i++) {
      final Database database = getServerDatabase(i, getDatabaseName());
      assertThat(database.getSchema().getEmbedded().isLegacyTemporalText()).as("server %d is native", i).isFalse();
      assertThat(schemaSettings(database).getString(TEMPORAL_STORAGE, null)).as("server %d schema file", i).isEqualTo("native");

      // The records are rewritten in their native types on every node, as SQL sees them
      try (final ResultSet rs = database.query("sql", "SELECT d, dt FROM T WHERE name = 'a'")) {
        final Result r = rs.next();
        assertThat(r.<Object>getProperty("d")).as("server %d", i).isEqualTo(new CypherDuration(0, 1, 0, 0));
        assertThat(r.<Object>getProperty("dt")).as("server %d", i).isInstanceOf(ZonedDateTime.class);
      }
      try (final ResultSet rs = database.query("sql", "SELECT d FROM T WHERE name = 'c'")) {
        assertThat(rs.next().<Object>getProperty("d")).as("server %d", i).isEqualTo(new CypherDuration(0, 10, 0, 0));
      }
      // And openCypher reads them as temporals, natively now
      try (final ResultSet rs = database.query("opencypher", "MATCH (n:T {name: 'a'}) RETURN n.d.days AS days, n.dt.timezone AS tz")) {
        final Result r = rs.next();
        assertThat(((Number) r.getProperty("days")).intValue()).as("server %d", i).isEqualTo(1);
        assertThat(r.<Object>getProperty("tz")).as("server %d", i).isEqualTo("Europe/Rome");
      }
    }
  }

  private void assertEveryNodeReadsTheNewTextAsAString() {
    for (int i = 0; i < getServerCount(); i++) {
      final Database database = getServerDatabase(i, getDatabaseName());
      try (final ResultSet rs = database.query("opencypher", "MATCH (n:T {name: 'z'}) RETURN n.d AS d")) {
        assertThat(rs.next().<Object>getProperty("d")).as("server %d reads a String written after the migration", i)
            .isEqualTo("P3D");
      }
      try (final ResultSet rs = database.query("opencypher", "MATCH (n:T) WHERE n.d = duration('P3D') RETURN count(n) AS c")) {
        assertThat(((Number) rs.next().getProperty("c")).longValue()).as("server %d must not restore the String", i).isZero();
      }
    }
  }

  private static JSONObject schemaSettings(final Database database) throws IOException {
    final File schemaFile = new File(database.getDatabasePath(), "schema.json");
    return new JSONObject(Files.readString(schemaFile.toPath(), StandardCharsets.UTF_8)).getJSONObject("settings");
  }
}
