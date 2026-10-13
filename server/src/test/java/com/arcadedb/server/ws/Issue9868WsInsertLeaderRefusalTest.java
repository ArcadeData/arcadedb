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

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.event.BeforeRecordCreateListener;
import com.arcadedb.network.binary.ServerIsNotTheLeaderException;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9868: a {@code /ws} insert refused because the node is not the leader reported {@code DB_ERROR} - the code a
 * genuine server fault gets - and dropped the leader's address, which only {@code toString()} of the exception carries. It
 * now reports {@code NOT_LEADER} with the address in a {@code leaderAddress} field, outside the (concealable) message, on
 * every path such a refusal reaches the client by: a row, a row's own commit ({@code per_row}), a chunk's own commit
 * ({@code per_batch}), and the {@code commit} frame of a {@code per_stream} session.
 * <p>
 * The refusal is raised where Raft raises it - by the database's commit - through the same {@code setWrappedDatabaseInstance}
 * hook {@code RaftReplicatedDatabase} installs itself with: a JDK proxy that rolls the transaction back and refuses while
 * armed, and delegates otherwise.
 *
 * @author Arcade Data Ltd
 */
// A hang detector, not a latency bound
@Timeout(value = 5, unit = TimeUnit.MINUTES)
class Issue9868WsInsertLeaderRefusalTest extends BaseGraphServerTest {
  private static final String LEADER = "leader-host:2480";

  private final AtomicBoolean refuseCommits = new AtomicBoolean();
  private final AtomicBoolean refuseRows    = new AtomicBoolean();

  private LocalDatabase              local;
  private BeforeRecordCreateListener refuseRow;

  @BeforeEach
  void installRefusingWrapper() {
    local = (LocalDatabase) ((DatabaseInternal) getServerDatabase(0, getDatabaseName())).getEmbedded();
    final DatabaseInternal real = local;
    final DatabaseInternal wrapper = (DatabaseInternal) Proxy.newProxyInstance(DatabaseInternal.class.getClassLoader(),
        new Class<?>[] { DatabaseInternal.class }, (proxy, method, args) -> {
          if (refuseCommits.get() && "commit".equals(method.getName()) && method.getParameterCount() == 0) {
            // What a follower does with a write it cannot replicate: nothing of the transaction survives the refusal
            real.rollback();
            throw new ServerIsNotTheLeaderException("This server is not the leader", LEADER);
          }
          try {
            return method.invoke(real, args);
          } catch (final InvocationTargetException e) {
            throw e.getCause();
          }
        });
    local.setWrappedDatabaseInstance(wrapper);

    refuseRow = record -> {
      if (refuseRows.get())
        throw new ServerIsNotTheLeaderException("This server is not the leader", LEADER);
      return true;
    };
    local.getSchema().getType("Person").getEvents().registerListener(refuseRow);
  }

  @AfterEach
  void removeRefusingWrapper() {
    refuseCommits.set(false);
    refuseRows.set(false);
    if (local != null && local.isOpen()) {
      local.getSchema().getType("Person").getEvents().unregisterListener(refuseRow);
      // A database with no HA wrapper is its own wrapped instance: this restores the state before the test
      local.setWrappedDatabaseInstance(local);
    }
  }

  @Test
  void aRefusedRowIsReportedAsNotLeaderWithTheLeaderAddress() throws Exception {
    try (final WebSocketClientHelper client = connect()) {
      final String sessionId = start(client, "per_stream");
      refuseRows.set(true);
      final JSONObject ack = chunk(client, sessionId, 1, 2);

      assertThat(ack.getString("action", "")).isEqualTo("batchAck");
      assertThat(ack.getLong("failed", -1)).isEqualTo(2);
      final JSONArray errors = ack.getJSONArray("errors");
      assertThat(errors.length()).isEqualTo(2);
      for (int i = 0; i < errors.length(); i++)
        assertNotLeader(errors.getJSONObject(i), i);

      refuseRows.set(false);
      client.send(new JSONObject().put("action", "rollback").put("sessionId", sessionId).toString());
    }
  }

  @Test
  void aRowWhoseOwnCommitIsRefusedIsReportedAsNotLeader() throws Exception {
    try (final WebSocketClientHelper client = connect()) {
      final String sessionId = start(client, "per_row");
      refuseCommits.set(true);
      final JSONObject ack = chunk(client, sessionId, 1, 2);
      refuseCommits.set(false);

      assertThat(ack.getLong("failed", -1)).isEqualTo(2);
      final JSONArray errors = ack.getJSONArray("errors");
      assertThat(errors.length()).isEqualTo(2);
      for (int i = 0; i < errors.length(); i++)
        assertNotLeader(errors.getJSONObject(i), i);

      client.send(new JSONObject().put("action", "rollback").put("sessionId", sessionId).toString());
    }
    assertThat(personCount()).as("no refused row was written").isZero();
  }

  @Test
  void aChunkWhoseCommitIsRefusedIsReportedAsNotLeader() throws Exception {
    try (final WebSocketClientHelper client = connect()) {
      final String sessionId = start(client, "per_batch");
      refuseCommits.set(true);
      final JSONObject ack = chunk(client, sessionId, 1, 3);
      refuseCommits.set(false);

      assertThat(ack.getLong("failed", -1)).isEqualTo(3);
      final JSONArray errors = ack.getJSONArray("errors");
      assertThat(errors.length()).as("one whole-chunk entry").isEqualTo(1);
      assertNotLeader(errors.getJSONObject(0), -1);

      client.send(new JSONObject().put("action", "rollback").put("sessionId", sessionId).toString());
    }
    assertThat(personCount()).as("the refused chunk was not written").isZero();
  }

  @Test
  void aRefusedSessionCommitIsAnsweredAsNotLeaderWithTheLeaderAddress() throws Exception {
    try (final WebSocketClientHelper client = connect()) {
      final String sessionId = start(client, "per_stream");
      final JSONObject ack = chunk(client, sessionId, 1, 2);
      assertThat(ack.getLong("inserted", -1)).isEqualTo(2);

      refuseCommits.set(true);
      final JSONObject answer = new JSONObject(
          client.send(new JSONObject().put("action", "commit").put("sessionId", sessionId).toString()));
      refuseCommits.set(false);

      assertThat(answer.getString("result", "")).isEqualTo("error");
      assertThat(answer.getString("error", "")).as("not reported as a server fault").isEqualTo("Not the leader");
      assertThat(answer.getString("leaderAddress", null)).isEqualTo(LEADER);
      assertThat(answer.getString("exception", "")).isEqualTo(ServerIsNotTheLeaderException.class.getName());
      assertThat(answer.getString("sessionId", "")).isEqualTo(sessionId);
    }
    assertThat(personCount()).as("the refused session wrote nothing").isZero();
  }

  @Test
  void anOrdinaryRowFailureKeepsDbErrorAndNamesNoLeader() throws Exception {
    try (final WebSocketClientHelper client = connect()) {
      final String sessionId = start(client, "per_stream");
      // A record of a type that does not exist: a row failure that is no leader refusal
      final JSONObject ack = new JSONObject(client.send(new JSONObject().put("action", "chunk").put("sessionId", sessionId)
          .put("chunkSeq", 1).put("records", new JSONArray().put(new JSONObject().put("@class", "NoSuchType"))).toString()));
      final JSONObject error = ack.getJSONArray("errors").getJSONObject(0);
      assertThat(error.getString("code", "")).isEqualTo("DB_ERROR");
      assertThat(error.has("leaderAddress")).isFalse();
      client.send(new JSONObject().put("action", "rollback").put("sessionId", sessionId).toString());
    }
  }

  private static void assertNotLeader(final JSONObject error, final int rowIndex) {
    assertThat(error.getInt("rowIndex", -2)).isEqualTo(rowIndex);
    assertThat(error.getString("code", "")).isEqualTo("NOT_LEADER");
    assertThat(error.getString("leaderAddress", null)).isEqualTo(LEADER);
    assertThat(error.getString("exception", "")).isEqualTo(ServerIsNotTheLeaderException.class.getName());
  }

  private WebSocketClientHelper connect() throws Exception {
    return new WebSocketClientHelper("ws://localhost:" + getServerHttpPort(0) + "/ws", "root", DEFAULT_PASSWORD_FOR_TESTS);
  }

  private String start(final WebSocketClientHelper client, final String mode) throws Exception {
    final JSONObject started = new JSONObject(client.send(new JSONObject().put("action", "start").put("database", getDatabaseName())
        .put("options", new JSONObject().put("targetType", "Person").put("transactionMode", mode)).toString()));
    assertThat(started.getString("action", "")).isEqualTo("started");
    return started.getString("sessionId");
  }

  private static JSONObject chunk(final WebSocketClientHelper client, final String sessionId, final int chunkSeq, final int rows)
      throws Exception {
    final JSONArray records = new JSONArray();
    for (int i = 0; i < rows; i++)
      records.put(new JSONObject().put("name", "n" + i));
    return new JSONObject(client.send(new JSONObject().put("action", "chunk").put("sessionId", sessionId).put("chunkSeq", chunkSeq)
        .put("records", records).toString()));
  }

  private long personCount() {
    return getServerDatabase(0, getDatabaseName()).countType("Person", false);
  }
}
