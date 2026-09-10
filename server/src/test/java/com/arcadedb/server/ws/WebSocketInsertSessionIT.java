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
import com.arcadedb.server.security.ServerSecurity;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The duplex insert session on {@code /ws} (issue #7382): the {@code start} / {@code chunk} /
 * {@code commit} / {@code rollback} control frames that the gRPC {@code InsertBidirectional} RPC has and that
 * {@code POST /api/v1/batch} - even with the per-chunk acknowledgements of issue #7311 - structurally cannot
 * offer, because they need the client to react to what the server said and change what it sends next inside the
 * same session.
 */
class WebSocketInsertSessionIT extends BaseGraphServerTest {

  /**
   * The verification issue #7382 asks for first: a session that sends two chunks and, AFTER seeing the
   * acknowledgement of the first, rolls the session back - the records of neither chunk are in the database.
   * That "after seeing the ack" ordering is the whole point: over {@code /batch} the commit policy is fixed by
   * query parameters before the load starts.
   */
  @Test
  void rollbackAfterSeeingAnAckDiscardsEveryChunk() throws Throwable {
    final Database database = getServerDatabase(0, getDatabaseName());
    assertThat(database.countType("Person", false)).isZero();

    try (final var client = newClient()) {
      final JSONObject started = new JSONObject(client.send(start(null, "Person", null)));
      assertThat(started.getString("action", "")).isEqualTo("started");
      assertThat(started.getString("transactionMode", "")).isEqualTo("per_stream");
      final String sessionId = started.getString("sessionId");
      assertThat(sessionId).isNotBlank();

      final JSONObject firstAck = new JSONObject(client.send(chunk(sessionId, 1, "a", "b")));
      assertThat(firstAck.getString("action", "")).isEqualTo("batchAck");
      assertThat(firstAck.getLong("chunkSeq", -1)).isEqualTo(1);
      assertThat(firstAck.getLong("inserted", -1)).isEqualTo(2);
      assertThat(firstAck.getLong("failed", -1)).isZero();

      // Only now, having seen the first acknowledgement, does the client send the second chunk.
      final JSONObject secondAck = new JSONObject(client.send(chunk(sessionId, 2, "c", "d")));
      assertThat(secondAck.getLong("inserted", -1)).isEqualTo(2);

      final JSONObject committed = new JSONObject(client.send(control("rollback", sessionId)));
      assertThat(committed.getString("action", "")).isEqualTo("committed");
      assertThat(committed.getString("outcome", "")).isEqualTo("rollback");

      final JSONObject summary = committed.getJSONObject("summary");
      assertThat(summary.getLong("received", -1)).isEqualTo(4);
      assertThat(summary.getLong("inserted", -1)).isEqualTo(4);
      assertThat(summary.getBoolean("partialCommit", true)).isFalse();
    }

    assertThat(database.countType("Person", false)).isZero();
  }

  /** The companion verification: a session that commits after the last acknowledgement keeps every record. */
  @Test
  void commitAfterTheLastAckPersistsEveryRecord() throws Throwable {
    final Database database = getServerDatabase(0, getDatabaseName());

    try (final var client = newClient()) {
      final JSONObject started = new JSONObject(client.send(start(null, "Person", null)));
      final String sessionId = started.getString("sessionId");

      assertThat(new JSONObject(client.send(chunk(sessionId, 1, "a", "b"))).getLong("inserted", -1)).isEqualTo(2);
      assertThat(new JSONObject(client.send(chunk(sessionId, 2, "c", "d"))).getLong("inserted", -1)).isEqualTo(2);

      final JSONObject committed = new JSONObject(client.send(control("commit", sessionId)));
      assertThat(committed.getString("outcome", "")).isEqualTo("commit");
      assertThat(committed.getJSONObject("summary").getLong("inserted", -1)).isEqualTo(4);
    }

    assertThat(database.countType("Person", false)).isEqualTo(4);
  }

  /**
   * The third verification: the same session id is rejected by a second concurrent {@code start} - both when the
   * second one arrives on a different connection and when it arrives on the one that already owns a session.
   */
  @Test
  void aSecondStartWithTheSameSessionIdIsRejected() throws Throwable {
    try (final var first = newClient(); final var second = newClient()) {
      final JSONObject started = new JSONObject(first.send(start("shared-session-id", "Person", null)));
      assertThat(started.getString("action", "")).isEqualTo("started");
      assertThat(started.getString("sessionId", "")).isEqualTo("shared-session-id");

      final JSONObject fromAnotherConnection = new JSONObject(second.send(start("shared-session-id", "Person", null)));
      assertThat(fromAnotherConnection.getString("result", "")).isEqualTo("error");
      assertThat(fromAnotherConnection.getString("detail", "")).contains("already exists");

      final JSONObject onTheSameConnection = new JSONObject(first.send(start("shared-session-id", "Person", null)));
      assertThat(onTheSameConnection.getString("result", "")).isEqualTo("error");
      assertThat(onTheSameConnection.getString("detail", "")).contains("already has insert session");

      new JSONObject(first.send(control("rollback", "shared-session-id")));
    }
  }

  /** A frame naming a session the server does not know is refused, never served against a fresh transaction. */
  @Test
  void aChunkForAnUnknownSessionIsRefused() throws Throwable {
    try (final var client = newClient()) {
      final JSONObject error = new JSONObject(client.send(chunk("no-such-session", 1, "a")));
      assertThat(error.getString("result", "")).isEqualTo("error");
      assertThat(error.getString("detail", "")).contains("not found or expired");
    }

    assertThat(getServerDatabase(0, getDatabaseName()).countType("Person", false)).isZero();
  }

  /** A session is reachable only from the connection that opened it, even by the very same principal. */
  @Test
  void anotherConnectionCannotDriveTheSession() throws Throwable {
    try (final var owner = newClient(); final var intruder = newClient()) {
      final String sessionId = new JSONObject(owner.send(start(null, "Person", null))).getString("sessionId");

      final JSONObject error = new JSONObject(intruder.send(chunk(sessionId, 1, "stolen")));
      assertThat(error.getString("result", "")).isEqualTo("error");
      assertThat(error.getString("detail", "")).contains("another connection");

      new JSONObject(owner.send(control("rollback", sessionId)));
    }

    assertThat(getServerDatabase(0, getDatabaseName()).countType("Person", false)).isZero();
  }

  /** And only by the principal that opened it: a second user's connection cannot adopt someone else's session. */
  @Test
  void anotherUserCannotDriveTheSession() throws Throwable {
    final ServerSecurity security = getServer(0).getSecurity();
    final String otherUser = "mallory";
    final String password = "mallorypassword";
    security.createUser(new JSONObject().put("name", otherUser).put("password", security.encodePassword(password))
        .put("databases", new JSONObject().put(getDatabaseName(), new JSONArray(new String[] { "admin" }))));
    try {
      try (final var owner = newClient();
          final var other = new WebSocketClientHelper(wsUrl(), otherUser, password)) {
        final String sessionId = new JSONObject(owner.send(start(null, "Person", null))).getString("sessionId");

        // Reaches the ownership check only because the intruder is on its own connection; the connection check
        // fires first, so this asserts the pair of them rather than the user check alone.
        final JSONObject error = new JSONObject(other.send(chunk(sessionId, 1, "stolen")));
        assertThat(error.getString("result", "")).isEqualTo("error");
        assertThat(error.getString("detail", "")).contains("another connection");

        new JSONObject(owner.send(control("rollback", sessionId)));
      }
    } finally {
      security.dropUser(otherUser);
    }
  }

  /** A user with no rights on the database cannot open a session on it at all. */
  @Test
  void startOnAnUnauthorizedDatabaseIsRejected() throws Throwable {
    final ServerSecurity security = getServer(0).getSecurity();
    final String otherUser = "eve7382";
    final String password = "evepassword";
    security.createUser(new JSONObject().put("name", otherUser).put("password", security.encodePassword(password))
        .put("databases", new JSONObject().put("otherdb", new JSONArray(new String[] { "admin" }))));
    try {
      try (final var client = new WebSocketClientHelper(wsUrl(), otherUser, password)) {
        final JSONObject error = new JSONObject(client.send(start(null, "Person", null)));
        assertThat(error.getString("result", "")).isEqualTo("error");
        assertThat(error.getString("error", "")).contains("Security");
      }
    } finally {
      security.dropUser(otherUser);
    }

    assertThat(getServer(0).getHttpServer().getInsertSessionManager().getOpenSessionCount()).isZero();
  }

  /**
   * {@code per_batch} commits each chunk before acknowledging it, so a later {@code rollback} frame cannot take
   * one back. The answer says so with {@code partialCommit} rather than letting {@code outcome: rollback} imply
   * an undo that did not happen.
   */
  @Test
  void perBatchCommitsEachChunkSoARollbackCannotTakeItBack() throws Throwable {
    final Database database = getServerDatabase(0, getDatabaseName());

    try (final var client = newClient()) {
      final JSONObject started = new JSONObject(client.send(start(null, "Person", "per_batch")));
      assertThat(started.getString("transactionMode", "")).isEqualTo("per_batch");
      final String sessionId = started.getString("sessionId");

      assertThat(new JSONObject(client.send(chunk(sessionId, 1, "a", "b"))).getLong("inserted", -1)).isEqualTo(2);

      // Durable already, before the client has said anything about committing.
      assertThat(database.countType("Person", false)).isEqualTo(2);

      final JSONObject committed = new JSONObject(client.send(control("rollback", sessionId)));
      assertThat(committed.getString("outcome", "")).isEqualTo("rollback");
      assertThat(committed.getJSONObject("summary").getBoolean("partialCommit", false)).isTrue();
    }

    assertThat(database.countType("Person", false)).isEqualTo(2);
  }

  /** {@code per_row} commits every row on its own, which the same {@code partialCommit} flag reports. */
  @Test
  void perRowCommitsEveryRowOnItsOwn() throws Throwable {
    final Database database = getServerDatabase(0, getDatabaseName());

    try (final var client = newClient()) {
      final String sessionId = new JSONObject(client.send(start(null, "Person", "per_row"))).getString("sessionId");
      assertThat(new JSONObject(client.send(chunk(sessionId, 1, "a", "b", "c"))).getLong("inserted", -1)).isEqualTo(3);
      assertThat(database.countType("Person", false)).isEqualTo(3);

      final JSONObject committed = new JSONObject(client.send(control("commit", sessionId)));
      assertThat(committed.getJSONObject("summary").getBoolean("partialCommit", false)).isTrue();
    }

    assertThat(database.countType("Person", false)).isEqualTo(3);
  }

  /** {@code per_request} is gRPC's name for the same thing a {@code /ws} session calls {@code per_stream}. */
  @Test
  void perRequestIsAnAliasOfPerStream() throws Throwable {
    try (final var client = newClient()) {
      final JSONObject started = new JSONObject(client.send(start(null, "Person", "per_request")));
      assertThat(started.getString("transactionMode", "")).isEqualTo("per_stream");
      new JSONObject(client.send(control("rollback", started.getString("sessionId"))));
    }
  }

  /**
   * The options a {@code /ws} session does not implement are refused at {@code start}, not ignored: a loader
   * ported from {@code InsertBidirectional} must not silently get plain inserts where it asked for upserts
   * (issues #7403, #7404).
   */
  @Test
  void optionsThisServerDoesNotImplementAreRefusedRatherThanIgnored() throws Throwable {
    try (final var client = newClient()) {
      final JSONObject options = new JSONObject().put("targetType", "Person").put("conflictMode", "update");
      final JSONObject refused = new JSONObject(client.send(startWithOptions(null, options)));
      assertThat(refused.getString("result", "")).isEqualTo("error");
      assertThat(refused.getString("detail", "")).contains("conflictMode").contains("#7404");

      final JSONObject noneMode = new JSONObject(
          client.send(startWithOptions(null, new JSONObject().put("targetType", "Person").put("transactionMode", "none"))));
      assertThat(noneMode.getString("result", "")).isEqualTo("error");
      assertThat(noneMode.getString("detail", "")).contains("#7403");

      final JSONObject unknownMode = new JSONObject(
          client.send(startWithOptions(null, new JSONObject().put("targetType", "Person").put("transactionMode", "whenever"))));
      assertThat(unknownMode.getString("result", "")).isEqualTo("error");
      assertThat(unknownMode.getString("detail", "")).contains("Unknown transactionMode");
    }

    assertThat(getServer(0).getHttpServer().getInsertSessionManager().getOpenSessionCount()).isZero();
  }

  /**
   * A row that cannot be applied is counted and described, and the rest of the chunk still goes in. The client
   * gets to decide what to do about a partial chunk - which is what having a duplex session is for.
   */
  @Test
  void aRowThatFailsIsReportedWhileTheRestOfTheChunkStillGoesIn() throws Throwable {
    final Database database = getServerDatabase(0, getDatabaseName());

    try (final var client = newClient()) {
      final String sessionId = new JSONObject(client.send(start(null, "Person", null))).getString("sessionId");

      final JSONArray records = new JSONArray();
      records.put(new JSONObject().put("name", "good-1"));
      records.put(new JSONObject().put("@class", "NoSuchTypeAnywhere").put("name", "bad"));
      records.put(new JSONObject().put("name", "good-2"));

      final JSONObject ack = new JSONObject(client.send(chunkOf(sessionId, 1, records)));
      assertThat(ack.getLong("received", -1)).isEqualTo(3);
      assertThat(ack.getLong("inserted", -1)).isEqualTo(2);
      assertThat(ack.getLong("failed", -1)).isEqualTo(1);
      assertThat(ack.getJSONArray("errors").length()).isEqualTo(1);
      assertThat(ack.getJSONArray("errors").getJSONObject(0).getInt("rowIndex", -1)).isEqualTo(1);

      new JSONObject(client.send(control("commit", sessionId)));
    }

    assertThat(database.countType("Person", false)).isEqualTo(2);
  }

  /** A chunk sequence at or below the watermark is acknowledged as a replay, not applied a second time. */
  @Test
  void aReplayedChunkIsAcknowledgedWithoutBeingAppliedAgain() throws Throwable {
    final Database database = getServerDatabase(0, getDatabaseName());

    try (final var client = newClient()) {
      final String sessionId = new JSONObject(client.send(start(null, "Person", null))).getString("sessionId");

      assertThat(new JSONObject(client.send(chunk(sessionId, 1, "a", "b"))).getLong("inserted", -1)).isEqualTo(2);

      final JSONObject replay = new JSONObject(client.send(chunk(sessionId, 1, "a", "b")));
      assertThat(replay.getBoolean("replay", false)).isTrue();
      assertThat(replay.getLong("inserted", -1)).isZero();

      new JSONObject(client.send(control("commit", sessionId)));
    }

    assertThat(database.countType("Person", false)).isEqualTo(2);
  }

  /**
   * A chunk that skips ahead of the next sequence due is refused. Letting the watermark follow whatever arrived
   * would make the skipped chunk, when it finally lands, look like a replay of something that never happened -
   * every row in it dropped, under a successful-looking answer.
   */
  @Test
  void aChunkThatSkipsAheadIsRefusedAndTheSkippedOneStillLands() throws Throwable {
    final Database database = getServerDatabase(0, getDatabaseName());

    try (final var client = newClient()) {
      final String sessionId = new JSONObject(client.send(start(null, "Person", null))).getString("sessionId");

      assertThat(new JSONObject(client.send(chunk(sessionId, 1, "one"))).getLong("inserted", -1)).isEqualTo(1);

      final JSONObject skipped = new JSONObject(client.send(chunk(sessionId, 5, "five")));
      assertThat(skipped.getString("result", "")).isEqualTo("error");
      assertThat(skipped.getString("detail", "")).contains("skips ahead").contains("expects 2");

      // The refusal left the watermark alone, so the chunk that was actually next still goes in rather than
      // being acknowledged as a replay of the jump.
      final JSONObject second = new JSONObject(client.send(chunk(sessionId, 2, "two")));
      assertThat(second.getBoolean("replay", false)).isFalse();
      assertThat(second.getLong("inserted", -1)).isEqualTo(1);

      new JSONObject(client.send(control("commit", sessionId)));
    }

    assertThat(database.countType("Person", false)).isEqualTo(2);
  }

  /** Vertices and edges travel through the same session; an edge names its endpoints with {@code @from}/{@code @to}. */
  @Test
  void verticesAndEdgesGoInThroughTheSameSession() throws Throwable {
    final Database database = getServerDatabase(0, getDatabaseName());
    final String fromRid = ridOfFirst(database, "select from " + VERTEX1_TYPE_NAME + " limit 1");
    final String toRid = ridOfFirst(database, "select from " + VERTEX2_TYPE_NAME + " limit 1");
    final long edgesBefore = database.countType(EDGE1_TYPE_NAME, false);

    try (final var client = newClient()) {
      final String sessionId = new JSONObject(client.send(start(null, null, null))).getString("sessionId");

      final JSONArray records = new JSONArray();
      records.put(new JSONObject().put("@class", VERTEX2_TYPE_NAME).put("name", "ws-vertex"));
      records.put(new JSONObject().put("@class", EDGE1_TYPE_NAME).put("@from", fromRid).put("@to", toRid)
          .put("name", "ws-edge"));

      final JSONObject ack = new JSONObject(client.send(chunkOf(sessionId, 1, records)));
      assertThat(ack.getLong("failed", -1)).isZero();
      assertThat(ack.getLong("inserted", -1)).isEqualTo(2);

      new JSONObject(client.send(control("commit", sessionId)));
    }

    assertThat(database.countType(EDGE1_TYPE_NAME, false)).isEqualTo(edgesBefore + 1);
    assertThat(database.query("sql", "select from " + EDGE1_TYPE_NAME + " where name = 'ws-edge'").hasNext()).isTrue();
  }

  /** gRPC spells an edge's endpoints {@code out}/{@code in}; a loader ported from it keeps working. */
  @Test
  void anEdgeCanNameItsEndpointsTheWayGrpcDoes() throws Throwable {
    final Database database = getServerDatabase(0, getDatabaseName());
    final String fromRid = ridOfFirst(database, "select from " + VERTEX1_TYPE_NAME + " limit 1");
    final String toRid = ridOfFirst(database, "select from " + VERTEX2_TYPE_NAME + " limit 1");
    final long edgesBefore = database.countType(EDGE1_TYPE_NAME, false);

    try (final var client = newClient()) {
      final String sessionId = new JSONObject(client.send(start(null, EDGE1_TYPE_NAME, null))).getString("sessionId");

      final JSONArray records = new JSONArray();
      records.put(new JSONObject().put("out", fromRid).put("in", toRid).put("name", "grpc-shaped-edge"));

      assertThat(new JSONObject(client.send(chunkOf(sessionId, 1, records))).getLong("inserted", -1)).isEqualTo(1);
      new JSONObject(client.send(control("commit", sessionId)));
    }

    assertThat(database.countType(EDGE1_TYPE_NAME, false)).isEqualTo(edgesBefore + 1);
  }

  /** An edge record with no endpoints is a client error, reported per row rather than aborting the chunk. */
  @Test
  void anEdgeWithoutEndpointsIsReportedPerRow() throws Throwable {
    try (final var client = newClient()) {
      final String sessionId = new JSONObject(client.send(start(null, EDGE1_TYPE_NAME, null))).getString("sessionId");

      final JSONArray records = new JSONArray();
      records.put(new JSONObject().put("name", "dangling"));

      final JSONObject ack = new JSONObject(client.send(chunkOf(sessionId, 1, records)));
      assertThat(ack.getLong("failed", -1)).isEqualTo(1);
      assertThat(ack.getJSONArray("errors").getJSONObject(0).getString("message", "")).contains("@from");

      new JSONObject(client.send(control("rollback", sessionId)));
    }
  }

  /** A record with neither {@code @class} nor a session {@code targetType} has no type to go into. */
  @Test
  void aRecordWithNoTypeIsReported() throws Throwable {
    try (final var client = newClient()) {
      final String sessionId = new JSONObject(client.send(start(null, null, null))).getString("sessionId");

      final JSONArray records = new JSONArray();
      records.put(new JSONObject().put("name", "homeless"));

      final JSONObject ack = new JSONObject(client.send(chunkOf(sessionId, 1, records)));
      assertThat(ack.getLong("failed", -1)).isEqualTo(1);
      assertThat(ack.getJSONArray("errors").getJSONObject(0).getString("message", "")).contains("targetType");

      new JSONObject(client.send(control("rollback", sessionId)));
    }
  }

  /**
   * A connection that goes away with an open session rolls it back. Nothing else would: the client is gone, so
   * it is never going to say what it wanted, and the transaction would otherwise be held until the idle sweep.
   */
  @Test
  void closingTheConnectionRollsTheSessionBack() throws Throwable {
    final Database database = getServerDatabase(0, getDatabaseName());

    final var client = newClient();
    final String sessionId = new JSONObject(client.send(start(null, "Person", null))).getString("sessionId");
    assertThat(new JSONObject(client.send(chunk(sessionId, 1, "a", "b"))).getLong("inserted", -1)).isEqualTo(2);
    assertThat(getServer(0).getHttpServer().getInsertSessionManager().getOpenSessionCount()).isEqualTo(1);

    client.breakConnection();

    Awaitility.await().atMost(10, TimeUnit.SECONDS).pollInterval(100, TimeUnit.MILLISECONDS)
        .until(() -> getServer(0).getHttpServer().getInsertSessionManager().getOpenSessionCount() == 0);

    assertThat(database.countType("Person", false)).isZero();
  }

  /**
   * A chunk with no sequence is refused, not defaulted. Defaulting it to 0 would land on the initial watermark
   * and be acknowledged as a duplicate, which silently drops every record the frame carried - the worst possible
   * answer for a load.
   */
  @Test
  void aChunkWithNoSequenceIsRefusedRatherThanTreatedAsAReplay() throws Throwable {
    try (final var client = newClient()) {
      final String sessionId = new JSONObject(client.send(start(null, "Person", null))).getString("sessionId");

      final JSONObject message = new JSONObject();
      message.put("action", "chunk");
      message.put("sessionId", sessionId);
      message.put("records", new JSONArray().put(new JSONObject().put("name", "unsequenced")));

      final JSONObject error = new JSONObject(client.send(message.toString()));
      assertThat(error.getString("result", "")).isEqualTo("error");
      assertThat(error.getString("detail", "")).contains("chunkSeq");

      new JSONObject(client.send(control("rollback", sessionId)));
    }

    assertThat(getServerDatabase(0, getDatabaseName()).countType("Person", false)).isZero();
  }

  /**
   * A frame whose fields carry the wrong JSON TYPE is a client mistake, and is answered as one. It used to reach
   * the catch-all and come back as "Internal error", telling a client the server had broken when it had not.
   */
  @Test
  void aFrameWithAWronglyTypedFieldIsAClientErrorNotAnInternalOne() throws Throwable {
    try (final var client = newClient()) {
      final JSONObject badOptions = new JSONObject();
      badOptions.put("action", "start");
      badOptions.put("database", getDatabaseName());
      badOptions.put("options", "not-an-object");

      final JSONObject refused = new JSONObject(client.send(badOptions.toString()));
      assertThat(refused.getString("result", "")).isEqualTo("error");
      assertThat(refused.getString("error", "")).isEqualTo("Insert session error");

      final String sessionId = new JSONObject(client.send(start(null, "Person", null))).getString("sessionId");

      final JSONObject badRecords = new JSONObject();
      badRecords.put("action", "chunk");
      badRecords.put("sessionId", sessionId);
      badRecords.put("chunkSeq", 1);
      badRecords.put("records", "not-an-array");

      final JSONObject refusedChunk = new JSONObject(client.send(badRecords.toString()));
      assertThat(refusedChunk.getString("result", "")).isEqualTo("error");
      assertThat(refusedChunk.getString("error", "")).isEqualTo("Insert session error");

      new JSONObject(client.send(control("rollback", sessionId)));
    }

    assertThat(getServer(0).getHttpServer().getInsertSessionManager().getOpenSessionCount()).isZero();
  }

  /** A finished session releases its connection, which can then open another one. */
  @Test
  void aConnectionCanOpenANewSessionOnceTheFirstIsFinished() throws Throwable {
    final Database database = getServerDatabase(0, getDatabaseName());

    try (final var client = newClient()) {
      final String first = new JSONObject(client.send(start(null, "Person", null))).getString("sessionId");
      assertThat(new JSONObject(client.send(chunk(first, 1, "first"))).getLong("inserted", -1)).isEqualTo(1);
      new JSONObject(client.send(control("commit", first)));

      final JSONObject startedAgain = new JSONObject(client.send(start(null, "Person", null)));
      assertThat(startedAgain.getString("action", "")).isEqualTo("started");
      final String second = startedAgain.getString("sessionId");
      assertThat(second).isNotEqualTo(first);

      assertThat(new JSONObject(client.send(chunk(second, 1, "second"))).getLong("inserted", -1)).isEqualTo(1);
      new JSONObject(client.send(control("commit", second)));
    }

    assertThat(database.countType("Person", false)).isEqualTo(2);
    assertThat(getServer(0).getHttpServer().getInsertSessionManager().getOpenSessionCount()).isZero();
  }

  private WebSocketClientHelper newClient() throws Exception {
    return new WebSocketClientHelper(wsUrl(), "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
  }

  private String wsUrl() {
    return "ws://localhost:" + getServer(0).getHttpServer().getPort() + "/ws";
  }

  private String start(final String sessionId, final String targetType, final String transactionMode) {
    final JSONObject options = new JSONObject();
    if (targetType != null)
      options.put("targetType", targetType);
    if (transactionMode != null)
      options.put("transactionMode", transactionMode);
    return startWithOptions(sessionId, options);
  }

  private String startWithOptions(final String sessionId, final JSONObject options) {
    final JSONObject message = new JSONObject();
    message.put("action", "start");
    message.put("database", getDatabaseName());
    if (sessionId != null)
      message.put("sessionId", sessionId);
    message.put("options", options);
    return message.toString();
  }

  private static String chunk(final String sessionId, final long chunkSeq, final String... names) {
    final JSONArray records = new JSONArray();
    for (final String name : names)
      records.put(new JSONObject().put("name", name));
    return chunkOf(sessionId, chunkSeq, records);
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

  private static String ridOfFirst(final Database database, final String query) {
    try (final ResultSet rs = database.query("sql", query)) {
      return rs.next().getIdentity().orElseThrow().toString();
    }
  }
}
