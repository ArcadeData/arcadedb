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
package com.arcadedb.server.ws;

import com.arcadedb.database.Database;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The conflict half of the gRPC {@code InsertOptions} on a {@code /ws} insert session (issue #7404):
 * {@code conflictMode}, {@code keyColumns}, {@code updateColumnsOnConflict} and {@code validateOnly}, with the
 * semantics {@code ArcadeDbGrpcService.insertRows} gives them.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class WebSocketInsertSessionConflictIT extends BaseGraphServerTest {
  private static final String TYPE      = "Keyed7404";
  private static final String EDGE_TYPE = "KeyedEdge7404";

  private Database database;

  @BeforeEach
  void createKeyedType() {
    database = getServerDatabase(0, getDatabaseName());
    database.command("sql", "CREATE DOCUMENT TYPE " + TYPE);
    database.command("sql", "CREATE PROPERTY " + TYPE + ".name STRING");
    database.command("sql", "CREATE INDEX ON " + TYPE + " (name) UNIQUE");
    database.command("sql", "CREATE EDGE TYPE " + EDGE_TYPE);
    database.command("sql", "CREATE PROPERTY " + EDGE_TYPE + ".name STRING");
    database.command("sql", "CREATE INDEX ON " + EDGE_TYPE + " (name) UNIQUE");
    database.transaction(() -> {
      database.newDocument(TYPE).set("name", "a").set("role", "old-a").save();
      database.newDocument(TYPE).set("name", "b").set("role", "old-b").save();
    });
  }

  @AfterEach
  void dropKeyedType() {
    database.command("sql", "DROP TYPE " + EDGE_TYPE + " IF EXISTS UNSAFE");
    database.command("sql", "DROP TYPE " + TYPE + " IF EXISTS UNSAFE");
  }

  /** Verification 1: the duplicate chunk under {@code update} is acknowledged as updated, and nothing new is created. */
  @Test
  void updateMatchesOnTheKeyColumnsAndRewritesTheExistingRecords() throws Throwable {
    try (final var client = newClient()) {
      final JSONObject started = new JSONObject(client.send(start(options("update", "name"))));
      assertThat(started.getString("action", "")).isEqualTo("started");
      assertThat(started.getString("conflictMode", "")).isEqualTo("update");
      final String sessionId = started.getString("sessionId");

      final JSONObject ack = new JSONObject(client.send(chunk(sessionId, 1,
          record("a", "new-a").put("extra", 1), record("b", "new-b"), record("c", "new-c"))));
      assertThat(ack.getString("action", "")).isEqualTo("batchAck");
      assertThat(ack.getLong("received", -1)).isEqualTo(3);
      assertThat(ack.getLong("updated", -1)).isEqualTo(2);
      assertThat(ack.getLong("inserted", -1)).isEqualTo(1);
      assertThat(ack.getLong("failed", -1)).isZero();
      assertThat(ack.has("errors")).isFalse();

      final JSONObject committed = new JSONObject(client.send(control("commit", sessionId)));
      assertThat(committed.getJSONObject("summary").getLong("updated", -1)).isEqualTo(2);
    }

    assertThat(database.countType(TYPE, false)).isEqualTo(3);
    assertThat(roleOf("a")).isEqualTo("new-a");
    assertThat(roleOf("b")).isEqualTo("new-b");
    assertThat(propertyOf("a", "extra")).isEqualTo(1);
  }

  /** {@code updateColumnsOnConflict} narrows the merge to the columns it names; the others keep their values. */
  @Test
  void updateColumnsOnConflictLimitsWhatAnUpdateOverwrites() throws Throwable {
    try (final var client = newClient()) {
      final JSONObject options = options("update", "name").put("updateColumnsOnConflict", new JSONArray().put("extra"));
      final String sessionId = new JSONObject(client.send(start(options))).getString("sessionId");

      final JSONObject ack = new JSONObject(client.send(chunk(sessionId, 1, record("a", "should-not-land").put("extra", 7))));
      assertThat(ack.getLong("updated", -1)).isEqualTo(1);
      new JSONObject(client.send(control("commit", sessionId)));
    }

    assertThat(roleOf("a")).isEqualTo("old-a");
    assertThat(propertyOf("a", "extra")).isEqualTo(7);
  }

  /** Verification 2: the same chunk under {@code ignore} is acknowledged as ignored. */
  @Test
  void ignoreDropsTheRowsWhoseKeyIsTaken() throws Throwable {
    try (final var client = newClient()) {
      final String sessionId = new JSONObject(client.send(start(options("ignore", "name")))).getString("sessionId");

      final JSONObject ack = new JSONObject(client.send(chunk(sessionId, 1, record("a", "x"), record("b", "x"), record("c", "x"))));
      assertThat(ack.getLong("ignored", -1)).isEqualTo(2);
      assertThat(ack.getLong("inserted", -1)).isEqualTo(1);
      assertThat(ack.getLong("failed", -1)).isZero();
      new JSONObject(client.send(control("commit", sessionId)));
    }

    assertThat(database.countType(TYPE, false)).isEqualTo(3);
    assertThat(roleOf("a")).isEqualTo("old-a");
  }

  /** Verification 3: under the default, each duplicate is a failed row with a {@code CONFLICT}-coded error. */
  @Test
  void theDefaultReportsEachDuplicateAsAConflictAndStillAppliesTheRest() throws Throwable {
    try (final var client = newClient()) {
      final String sessionId = new JSONObject(client.send(start(options(null, "name")))).getString("sessionId");

      final JSONObject ack = new JSONObject(client.send(chunk(sessionId, 1, record("a", "x"), record("c", "x"), record("b", "x"))));
      assertThat(ack.getLong("failed", -1)).isEqualTo(2);
      assertThat(ack.getLong("inserted", -1)).isEqualTo(1);
      final JSONArray errors = ack.getJSONArray("errors");
      assertThat(errors.length()).isEqualTo(2);
      assertThat(errors.getJSONObject(0).getInt("rowIndex", -1)).isZero();
      assertThat(errors.getJSONObject(0).getString("code", "")).isEqualTo("CONFLICT");
      assertThat(errors.getJSONObject(1).getInt("rowIndex", -1)).isEqualTo(2);
      assertThat(errors.getJSONObject(1).getString("code", "")).isEqualTo("CONFLICT");
      new JSONObject(client.send(control("commit", sessionId)));
    }

    assertThat(database.countType(TYPE, false)).isEqualTo(3);
  }

  /** {@code abort} is accepted for parity with gRPC and answered exactly as {@code error} is there. */
  @Test
  void abortIsAnsweredLikeError() throws Throwable {
    try (final var client = newClient()) {
      final String sessionId = new JSONObject(client.send(start(options("abort", "name")))).getString("sessionId");
      final JSONObject ack = new JSONObject(client.send(chunk(sessionId, 1, record("a", "x"))));
      assertThat(ack.getLong("failed", -1)).isEqualTo(1);
      assertThat(ack.getJSONArray("errors").getJSONObject(0).getString("code", "")).isEqualTo("CONFLICT");
      new JSONObject(client.send(control("rollback", sessionId)));
    }
  }

  /** Verification 4: a dry run acknowledges every row as received while the database stays as it was. */
  @Test
  void validateOnlyReceivesEveryRowAndWritesNothing() throws Throwable {
    try (final var client = newClient()) {
      final JSONObject options = new JSONObject().put("targetType", TYPE).put("validateOnly", true);
      final String sessionId = new JSONObject(client.send(start(options))).getString("sessionId");

      final JSONObject ack = new JSONObject(client.send(chunk(sessionId, 1, record("a", "x"), record("z", "x"))));
      assertThat(ack.getLong("received", -1)).isEqualTo(2);
      assertThat(ack.getLong("inserted", -1)).isZero();
      assertThat(ack.getLong("failed", -1)).isZero();

      // Validation still validates: a row with no type is reported even though nothing would be written.
      final JSONArray untyped = new JSONArray().put(new JSONObject().put("@class", "NoSuchType7404").put("name", "q"));
      final JSONObject bad = new JSONObject(client.send(chunkOf(sessionId, 2, untyped)));
      assertThat(bad.getLong("failed", -1)).isEqualTo(1);

      final JSONObject committed = new JSONObject(client.send(control("commit", sessionId)));
      assertThat(committed.getJSONObject("summary").getLong("received", -1)).isEqualTo(3);
      assertThat(committed.getJSONObject("summary").getLong("inserted", -1)).isZero();
    }

    assertThat(database.countType(TYPE, false)).isEqualTo(2);
  }

  /** {@code update} with nothing to match on can never update anything, so it is refused at {@code start}. */
  @Test
  void updateWithoutKeyColumnsIsRefusedAtStart() throws Throwable {
    try (final var client = newClient()) {
      final JSONObject refused = new JSONObject(client.send(start(new JSONObject().put("conflictMode", "update"))));
      assertThat(refused.getString("result", "")).isEqualTo("error");
      assertThat(refused.getString("detail", "")).contains("keyColumns");
    }
    assertThat(getServer(0).getHttpServer().getInsertSessionManager().getOpenSessionCount()).isZero();
  }

  /**
   * With no key columns the duplicate is the engine's to find, and it finds it where the transaction commits:
   * under {@code per_row} that is the row itself, so the mode still gets to answer per row.
   */
  @Test
  void withoutKeyColumnsPerRowStillAnswersPerRow() throws Throwable {
    try (final var client = newClient()) {
      final JSONObject ignoring = new JSONObject().put("targetType", TYPE).put("transactionMode", "per_row")
          .put("conflictMode", "ignore");
      final String ignoringSession = new JSONObject(client.send(start(ignoring))).getString("sessionId");
      final JSONObject ignoredAck = new JSONObject(client.send(chunk(ignoringSession, 1, record("a", "x"), record("d", "x"))));
      assertThat(ignoredAck.getLong("ignored", -1)).isEqualTo(1);
      assertThat(ignoredAck.getLong("inserted", -1)).isEqualTo(1);
      assertThat(ignoredAck.getLong("failed", -1)).isZero();
      new JSONObject(client.send(control("commit", ignoringSession)));

      final JSONObject erroring = new JSONObject().put("targetType", TYPE).put("transactionMode", "per_row");
      final String erroringSession = new JSONObject(client.send(start(erroring))).getString("sessionId");
      final JSONObject failedAck = new JSONObject(client.send(chunk(erroringSession, 1, record("b", "x"), record("e", "x"))));
      assertThat(failedAck.getLong("failed", -1)).isEqualTo(1);
      assertThat(failedAck.getLong("inserted", -1)).isEqualTo(1);
      assertThat(failedAck.getJSONArray("errors").getJSONObject(0).getInt("rowIndex", -1)).isZero();
      assertThat(failedAck.getJSONArray("errors").getJSONObject(0).getString("code", "")).isEqualTo("CONFLICT");
      new JSONObject(client.send(control("commit", erroringSession)));
    }

    assertThat(database.countType(TYPE, false)).isEqualTo(4);
    assertThat(roleOf("a")).isEqualTo("old-a");
  }

  /**
   * And under {@code per_stream} the engine finds it at the session's commit: the {@code commit} frame is
   * answered with a client error naming the duplicate, the session is closed, and nothing of it is durable.
   */
  @Test
  void withoutKeyColumnsPerStreamRefusesTheCommit() throws Throwable {
    try (final var client = newClient()) {
      final String sessionId = new JSONObject(client.send(start(new JSONObject().put("targetType", TYPE)))).getString("sessionId");
      final JSONObject ack = new JSONObject(client.send(chunk(sessionId, 1, record("a", "x"), record("f", "x"))));
      assertThat(ack.getLong("inserted", -1)).isEqualTo(2);

      final JSONObject refused = new JSONObject(client.send(control("commit", sessionId)));
      assertThat(refused.getString("result", "")).isEqualTo("error");
      assertThat(refused.getString("error", "")).isEqualTo("Insert session error");
      assertThat(refused.getString("exception", "")).endsWith("DuplicatedKeyException");

      final JSONObject gone = new JSONObject(client.send(chunk(sessionId, 2, record("g", "x"))));
      assertThat(gone.getString("result", "")).isEqualTo("error");
      assertThat(gone.getString("detail", "")).contains("not found or expired");
    }

    assertThat(database.countType(TYPE, false)).isEqualTo(2);
    assertThat(getServer(0).getHttpServer().getInsertSessionManager().getOpenSessionCount()).isZero();
  }

  /** An edge upsert rewrites the edge's own properties and never its endpoints, even when asked to. */
  @Test
  void anEdgeUpsertKeepsItsEndpoints() throws Throwable {
    final String fromRid = ridOfFirst("select from " + VERTEX1_TYPE_NAME + " limit 1");
    final String toRid = ridOfFirst("select from " + VERTEX2_TYPE_NAME + " limit 1");
    final String[] otherRid = new String[1];
    database.transaction(() -> {
      otherRid[0] = database.newVertex(VERTEX1_TYPE_NAME).set("id", 7404).set("name", "other").save().getIdentity().toString();
      database.lookupByRID(database.newRID(fromRid), false).asVertex()
          .newEdge(EDGE_TYPE, database.newRID(toRid)).set("name", "e1").set("weight", 1).save();
    });

    try (final var client = newClient()) {
      final JSONObject options = new JSONObject().put("targetType", EDGE_TYPE).put("conflictMode", "update")
          .put("keyColumns", new JSONArray().put("name"))
          .put("updateColumnsOnConflict", new JSONArray().put("weight").put("out"));
      final String sessionId = new JSONObject(client.send(start(options))).getString("sessionId");

      final JSONArray records = new JSONArray().put(new JSONObject().put("name", "e1").put("weight", 2)
          .put("@from", otherRid[0]).put("@to", toRid));
      final JSONObject ack = new JSONObject(client.send(chunkOf(sessionId, 1, records)));
      assertThat(ack.getLong("updated", -1)).isEqualTo(1);
      assertThat(ack.getLong("inserted", -1)).isZero();
      new JSONObject(client.send(control("commit", sessionId)));
    }

    try (final ResultSet rs = database.query("sql", "select from " + EDGE_TYPE + " where name = 'e1'")) {
      final var edge = rs.next().getEdge().orElseThrow();
      assertThat(edge.<Integer>get("weight")).isEqualTo(2);
      assertThat(edge.getOut().toString()).isEqualTo(fromRid);
      assertThat(rs.hasNext()).isFalse();
    }
  }

  private WebSocketClientHelper newClient() throws Exception {
    return new WebSocketClientHelper("ws://localhost:" + getServer(0).getHttpServer().getPort() + "/ws", "root",
        BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
  }

  private static JSONObject options(final String conflictMode, final String... keyColumns) {
    final JSONObject options = new JSONObject().put("targetType", TYPE);
    if (conflictMode != null)
      options.put("conflictMode", conflictMode);
    options.put("keyColumns", new JSONArray(keyColumns));
    return options;
  }

  private String start(final JSONObject options) {
    final JSONObject message = new JSONObject();
    message.put("action", "start");
    message.put("database", getDatabaseName());
    message.put("options", options);
    return message.toString();
  }

  private static JSONObject record(final String name, final String role) {
    return new JSONObject().put("name", name).put("role", role);
  }

  private static String chunk(final String sessionId, final long chunkSeq, final JSONObject... records) {
    return chunkOf(sessionId, chunkSeq, new JSONArray(records));
  }

  private static String chunkOf(final String sessionId, final long chunkSeq, final JSONArray records) {
    final JSONObject message = new JSONObject();
    message.put("action", "chunk");
    message.put("sessionId", sessionId);
    message.put("chunkSeq", chunkSeq);
    message.put("records", records);
    return message.toString();
  }

  private static String control(final String action, final String sessionId) {
    final JSONObject message = new JSONObject();
    message.put("action", action);
    message.put("sessionId", sessionId);
    return message.toString();
  }

  private String roleOf(final String name) {
    return (String) propertyOf(name, "role");
  }

  private Object propertyOf(final String name, final String property) {
    try (final ResultSet rs = database.query("sql", "select from " + TYPE + " where name = ?", name)) {
      return rs.next().getProperty(property);
    }
  }

  private String ridOfFirst(final String query) {
    try (final ResultSet rs = database.query("sql", query)) {
      return rs.next().getIdentity().orElseThrow().toString();
    }
  }
}
