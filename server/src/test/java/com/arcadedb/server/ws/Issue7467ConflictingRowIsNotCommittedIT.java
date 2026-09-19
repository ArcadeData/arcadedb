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
 * Issue #7467, end to end on the route it was reported against: a {@code /ws} insert row acknowledged as
 * {@code CONFLICT} must not be in the chunk the server then commits.
 * <p>
 * The shape matters. With {@code keyColumns} the session finds the conflict itself, with a {@code SELECT}
 * BEFORE it writes anything, which was never the defect. Without them the duplicate is the engine's to find,
 * and it finds it inline at {@code save()} when the twin was written by this same transaction - by which point
 * the record body, its identity and the bucket's record delta were already in the transaction. The row was
 * tallied {@code CONFLICT}, the chunk carried on to its commit, and the body went with it: a record in the
 * bucket, counted by {@code count(*)}, absent from the unique index that was supposed to forbid it, and
 * acknowledged to the client as not written.
 * <p>
 * The retraction is the engine's ({@code Issue7467FailedCreateLeavesNothingInTheTransactionTest} pins it
 * there). This pins what the client sees: the tally, the commit, and the database afterwards agreeing.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7467ConflictingRowIsNotCommittedIT extends BaseGraphServerTest {
  private static final String TYPE = "Keyed7467";

  private Database database;

  @BeforeEach
  void createKeyedType() {
    database = getServerDatabase(0, getDatabaseName());
    database.command("sql", "CREATE DOCUMENT TYPE " + TYPE);
    database.command("sql", "CREATE PROPERTY " + TYPE + ".name STRING");
    database.command("sql", "CREATE INDEX ON " + TYPE + " (name) UNIQUE");
  }

  @AfterEach
  void dropKeyedType() {
    database.command("sql", "DROP TYPE " + TYPE + " IF EXISTS UNSAFE");
  }

  /**
   * {@code per_stream}: one transaction for the whole session, so the twin of row 0 is refused at row 1's own
   * {@code save()} and the session's {@code commit} frame writes what is left.
   */
  @Test
  void aConflictingRowInOneChunkIsNotCommittedUnderPerStream() throws Throwable {
    try (final var client = newClient()) {
      final String sessionId = new JSONObject(client.send(start("per_stream"))).getString("sessionId");

      final JSONObject ack = new JSONObject(client.send(chunk(sessionId, 1,
          record("dup", 1), record("dup", 2), record("other", 3))));
      assertThat(ack.getString("action", "")).isEqualTo("batchAck");
      assertThat(ack.getLong("inserted", -1)).isEqualTo(2);
      assertThat(ack.getLong("failed", -1)).isEqualTo(1);
      assertThat(ack.getJSONArray("errors").getJSONObject(0).getInt("rowIndex", -1)).isEqualTo(1);
      assertThat(ack.getJSONArray("errors").getJSONObject(0).getString("code", "")).isEqualTo("CONFLICT");

      final JSONObject committed = new JSONObject(client.send(control("commit", sessionId)));
      assertThat(committed.getString("action", "")).isEqualTo("committed");
      assertThat(committed.getJSONObject("summary").getLong("inserted", -1)).isEqualTo(2);
    }

    assertTheAcknowledgementAndTheDatabaseAgree();
  }

  /** {@code per_batch}: the chunk is one transaction, and the refused row must not ride its commit either. */
  @Test
  void aConflictingRowInOneChunkIsNotCommittedUnderPerBatch() throws Throwable {
    try (final var client = newClient()) {
      final String sessionId = new JSONObject(client.send(start("per_batch"))).getString("sessionId");

      final JSONObject ack = new JSONObject(client.send(chunk(sessionId, 1,
          record("dup", 1), record("dup", 2), record("other", 3))));
      assertThat(ack.getLong("inserted", -1)).isEqualTo(2);
      assertThat(ack.getLong("failed", -1)).isEqualTo(1);

      new JSONObject(client.send(control("commit", sessionId)));
    }

    assertTheAcknowledgementAndTheDatabaseAgree();
  }

  /**
   * {@code per_row}: the row IS the transaction, so a refused row takes its own transaction down rather than
   * being tallied inside it and committed anyway.
   */
  @Test
  void aConflictingRowIsNotCommittedUnderPerRow() throws Throwable {
    try (final var client = newClient()) {
      final String sessionId = new JSONObject(client.send(start("per_row"))).getString("sessionId");

      final JSONObject ack = new JSONObject(client.send(chunk(sessionId, 1,
          record("dup", 1), record("dup", 2), record("other", 3))));
      assertThat(ack.getLong("inserted", -1)).isEqualTo(2);
      assertThat(ack.getLong("failed", -1)).isEqualTo(1);
      assertThat(ack.getJSONArray("errors").getJSONObject(0).getInt("rowIndex", -1)).isEqualTo(1);
      assertThat(ack.getJSONArray("errors").getJSONObject(0).getString("code", "")).isEqualTo("CONFLICT");

      new JSONObject(client.send(control("commit", sessionId)));
    }

    assertTheAcknowledgementAndTheDatabaseAgree();
  }

  /**
   * The three views the defect made disagree: what the client was told, what {@code count(*)} says, and what the
   * unique index holds.
   */
  private void assertTheAcknowledgementAndTheDatabaseAgree() {
    assertThat(database.countType(TYPE, false)).as("count(*) must match what the session acknowledged").isEqualTo(2);

    try (final ResultSet rs = database.query("sql", "SELECT FROM " + TYPE + " WHERE name = 'dup'")) {
      assertThat(rs.stream().count()).as("the unique index must answer for the one row that took the key").isEqualTo(1);
    }

    final long scanned;
    try (final ResultSet rs = database.query("sql", "SELECT FROM " + TYPE)) {
      scanned = rs.stream().count();
    }
    assertThat(scanned).as("a full scan must not find a record no lookup by key can reach").isEqualTo(2);

    try (final ResultSet rs = database.query("sql", "SELECT row FROM " + TYPE + " WHERE name = 'dup'")) {
      assertThat(rs.next().<Integer>getProperty("row")).as("the row that took the key is the one that kept it")
          .isEqualTo(1);
    }
  }

  private WebSocketClientHelper newClient() throws Exception {
    return new WebSocketClientHelper("ws://localhost:" + getServer(0).getHttpServer().getPort() + "/ws", "root",
        BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
  }

  /** No {@code keyColumns}: the duplicate has to be the engine's to find, which is the whole point. */
  private String start(final String transactionMode) {
    final JSONObject message = new JSONObject();
    message.put("action", "start");
    message.put("database", getDatabaseName());
    message.put("options", new JSONObject().put("targetType", TYPE).put("transactionMode", transactionMode));
    return message.toString();
  }

  private static JSONObject record(final String name, final int row) {
    return new JSONObject().put("name", name).put("row", row);
  }

  private static String chunk(final String sessionId, final long chunkSeq, final JSONObject... records) {
    final JSONObject message = new JSONObject();
    message.put("action", "chunk");
    message.put("sessionId", sessionId);
    message.put("chunkSeq", chunkSeq);
    message.put("records", new JSONArray(records));
    return message.toString();
  }

  private static String control(final String action, final String sessionId) {
    return new JSONObject().put("action", action).put("sessionId", sessionId).toString();
  }
}
