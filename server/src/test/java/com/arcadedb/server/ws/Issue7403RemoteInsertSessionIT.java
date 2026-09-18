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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.remote.RemoteException;
import com.arcadedb.remote.RemoteInsertSession;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Part 3 of issue #7403: the Java client for the {@code /ws} insert session.
 * <p>
 * Before this, {@code RemoteDatabase} had no method that opened one, so the only way to use the feature from
 * Java was to bring a WebSocket client and hand-write the frames - which is what
 * {@link WebSocketInsertSessionIT} does. The gRPC side has had the counterpart since
 * {@code RemoteGrpcDatabase.insertBidirectional*}.
 */
class Issue7403RemoteInsertSessionIT extends BaseGraphServerTest {
  /** Low enough that a frame-level refusal is reachable from the client without sending 100,000 rows. */
  private static final int MAX_ROWS = 4;

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    config.setValue(GlobalConfiguration.SERVER_WS_MAX_INSERT_CHUNK_ROWS, MAX_ROWS);
  }

  /**
   * The point of the control frames, from Java: the caller sees each acknowledgement and only THEN decides
   * whether to commit. {@code RemoteGraphBatch}' progress callback structurally cannot offer that - the commit
   * policy of {@code POST /api/v1/batch} is fixed by query parameters before the load starts.
   */
  @Test
  void theCallerCommitsAfterSeeingTheAcknowledgements() {
    final Database database = getServerDatabase(0, getDatabaseName());
    final List<Long> seenAcks = new ArrayList<>();

    try (final RemoteDatabase remote = newRemote()) {
      try (final RemoteInsertSession session = remote.newInsertSession()
          .targetType("Person")
          .onBatchAck(ack -> seenAcks.add(ack.getLong("chunkSeq", -1)))
          .open()) {

        assertThat(session.getSessionId()).isNotBlank();
        assertThat(session.getTransactionMode()).isEqualTo("per_stream");

        assertThat(session.sendChunk(people("a", "b")).getLong("inserted", -1)).isEqualTo(2);
        assertThat(session.sendChunk(people("c")).getLong("inserted", -1)).isEqualTo(1);
        assertThat(seenAcks).containsExactly(1L, 2L);

        // Nothing is durable until the client says so.
        assertThat(database.countType("Person", false)).isZero();

        final JSONObject committed = session.commit();
        assertThat(committed.getString("outcome", "")).isEqualTo("commit");
        assertThat(committed.getJSONObject("summary").getLong("inserted", -1)).isEqualTo(3);
      }
    }

    assertThat(database.countType("Person", false)).isEqualTo(3);
  }

  /** The other half of the same decision: a rollback taken after the acknowledgements discards every chunk. */
  @Test
  void theCallerCanRollBackAfterSeeingTheAcknowledgements() {
    final Database database = getServerDatabase(0, getDatabaseName());

    try (final RemoteDatabase remote = newRemote()) {
      try (final RemoteInsertSession session = remote.newInsertSession().targetType("Person").open()) {
        assertThat(session.sendChunk(people("a", "b")).getLong("inserted", -1)).isEqualTo(2);
        assertThat(session.rollback().getString("outcome", "")).isEqualTo("rollback");
      }
    }

    assertThat(database.countType("Person", false)).isZero();
  }

  /** A session left open by the try-with-resources block is rolled back, never left holding a transaction. */
  @Test
  void closingWithoutCommittingRollsTheSessionBack() {
    final Database database = getServerDatabase(0, getDatabaseName());

    try (final RemoteDatabase remote = newRemote()) {
      try (final RemoteInsertSession session = remote.newInsertSession().targetType("Person").open()) {
        assertThat(session.sendChunk(people("a")).getLong("inserted", -1)).isEqualTo(1);
      }
    }

    assertThat(database.countType("Person", false)).isZero();
    assertThat(getServer(0).getHttpServer().getInsertSessionManager().getOpenSessionCount()).isZero();
  }

  /**
   * The two halves of this issue meeting: the session joins the transaction the {@link RemoteDatabase} already
   * began, and that database's own {@code commit()} is what makes the rows durable.
   */
  @Test
  void aSessionCanJoinTheDatabasesOwnTransaction() {
    final Database database = getServerDatabase(0, getDatabaseName());

    try (final RemoteDatabase remote = newRemote()) {
      remote.begin();

      try (final RemoteInsertSession session = remote.newInsertSession()
          .targetType("Person")
          .joinCurrentTransaction()
          .open()) {

        assertThat(session.getTransactionMode()).isEqualTo("none");
        assertThat(session.getExternalTransactionId()).isNotBlank();
        assertThat(session.sendChunk(people("a", "b")).getLong("inserted", -1)).isEqualTo(2);

        final JSONObject committed = session.commit();
        assertThat(committed.getString("outcome", "")).isEqualTo(RemoteInsertSession.OUTCOME_DETACHED);
        assertThat(committed.getJSONObject("summary").getBoolean("externalTransaction", false)).isTrue();
      }

      // Still nothing: the session committed nothing, it only stopped writing.
      assertThat(database.countType("Person", false)).isZero();
      remote.commit();
    }

    assertThat(database.countType("Person", false)).isEqualTo(2);
  }

  /** And the caller's rollback undoes the rows the session was acknowledged for. */
  @Test
  void theDatabasesRollbackUndoesWhatTheJoinedSessionWrote() {
    final Database database = getServerDatabase(0, getDatabaseName());

    try (final RemoteDatabase remote = newRemote()) {
      remote.begin();
      try (final RemoteInsertSession session = remote.newInsertSession()
          .targetType("Person").joinCurrentTransaction().open()) {
        assertThat(session.sendChunk(people("a", "b")).getLong("inserted", -1)).isEqualTo(2);
        session.commit();
      }
      remote.rollback();
    }

    assertThat(database.countType("Person", false)).isZero();
  }

  /** Joining needs a transaction to join, and says so rather than quietly opening a server-managed session. */
  @Test
  void joiningWithoutAnOpenTransactionIsRefused() {
    try (final RemoteDatabase remote = newRemote()) {
      assertThatThrownBy(() -> remote.newInsertSession().joinCurrentTransaction())
          .isInstanceOf(RemoteException.class)
          .hasMessageContaining("no open transaction");
    }
  }

  /**
   * A chunk the server refuses as a whole - here for carrying more rows than
   * {@code arcadedb.server.wsMaxInsertChunkRows} allows - surfaces as an exception and leaves the session
   * usable: the watermark never moved, so the client splits the batch and the next chunk takes the SAME
   * sequence number.
   * <p>
   * Note the contrast with a row the server cannot apply, which is not a refusal at all: it is counted in
   * {@code failed}, described in {@code errors}, and the rest of the chunk still goes in - see
   * {@code WebSocketInsertSessionIT.aRowThatFailsIsReportedWhileTheRestOfTheChunkStillGoesIn}.
   */
  @Test
  void aRefusedChunkIsReportedAndTheSessionCarriesOn() {
    final Database database = getServerDatabase(0, getDatabaseName());

    try (final RemoteDatabase remote = newRemote()) {
      try (final RemoteInsertSession session = remote.newInsertSession().targetType("Person").open()) {
        assertThatThrownBy(() -> session.sendChunk(people("a", "b", "c", "d", "e")))
            .isInstanceOf(RemoteException.class)
            .hasMessageContaining("more than the " + MAX_ROWS);

        assertThat(session.getLastChunkSeq()).as("a refused chunk must not consume a sequence number").isZero();

        // A row the server cannot apply, by contrast, is tallied rather than refused.
        final JSONObject ack = session.sendChunk(List.of(
            new JSONObject().put("name", "a"), new JSONObject().put("@class", "NoSuchType")));
        assertThat(ack.getLong("chunkSeq", -1)).isEqualTo(1);
        assertThat(ack.getLong("inserted", -1)).isEqualTo(1);
        assertThat(ack.getLong("failed", -1)).isEqualTo(1);

        session.commit();
      }
    }

    assertThat(database.countType("Person", false)).isEqualTo(1);
  }

  // ---------------------------------------------------------------------------------------------------------

  private RemoteDatabase newRemote() {
    return new RemoteDatabase("127.0.0.1", getServer(0).getHttpServer().getPort(), getDatabaseName(), "root",
        DEFAULT_PASSWORD_FOR_TESTS);
  }

  private static List<JSONObject> people(final String... names) {
    final List<JSONObject> records = new ArrayList<>(names.length);
    for (final String name : names)
      records.add(new JSONObject().put("name", name));
    return records;
  }
}
