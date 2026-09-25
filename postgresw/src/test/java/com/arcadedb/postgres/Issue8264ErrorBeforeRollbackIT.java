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
package com.arcadedb.postgres;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.arcadedb.postgres.PostgresWireMessages.WireMessage;
import static com.arcadedb.postgres.PostgresWireMessages.errorFields;
import static com.arcadedb.postgres.PostgresWireMessages.messageTypesOf;
import static com.arcadedb.postgres.PostgresWireMessages.readUntilReadyForQuery;
import static com.arcadedb.postgres.PostgresWireMessages.readWireMessage;
import static com.arcadedb.postgres.PostgresWireMessages.readyForQueryStatusOf;
import static com.arcadedb.postgres.PostgresWireMessages.sendBind;
import static com.arcadedb.postgres.PostgresWireMessages.sendExecute;
import static com.arcadedb.postgres.PostgresWireMessages.sendParse;
import static com.arcadedb.postgres.PostgresWireMessages.sendSimpleQuery;
import static com.arcadedb.postgres.PostgresWireMessages.sendSync;
import static com.arcadedb.postgres.PostgresWireMessages.sqlStateOf;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
 * Regression tests for issue #8264: every failure arm of {@code queryCommand()} rolled back FIRST and sent the
 * ErrorResponse second, so a rollback that threw skipped the ErrorResponse and the client saw its query end with a bare
 * ReadyForQuery, the cause only in the server log; {@code syncCommand()}'s discard branch had the same unguarded
 * rollback. Also covers the correlated defect found while fixing it: a commit that fails at Sync - which is where
 * ArcadeDB checks unique keys - escaped {@code syncCommand()} and the client never got an answer at all.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8264ErrorBeforeRollbackIT extends PostgresWireProtocolTestBase {

  private static final String TYPE        = "Issue8264Probe";
  private static final String UNIQUE_TYPE = "Issue8264Unique";

  @AfterEach
  void clearHook() {
    PostgresNetworkExecutor.TEST_ROLLBACK_HOOK = null;
  }

  @Test
  @DisplayName("[#8264] a failed simple query whose rollback throws still reports the ORIGINAL error first, then FATAL and closes")
  void simpleQueryReportsTheOriginalErrorBeforeAFailingRollback() throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        createType(out, in, TYPE);

        // an extended-protocol write with no Sync yet leaves an implicit block open, which the failing 'Q' must abort
        sendParse(out, "", "INSERT INTO " + TYPE + " SET tag = 'pending'");
        sendBind(out, "", "");
        sendExecute(out, "");
        failNextRollback();

        sendSimpleQuery(out, "SELECT * FROM NoSuchType8264");
        final List<WireMessage> response = readUntilClosed(in);
        final List<WireMessage> errors = response.stream().filter(m -> m.type() == 'E').toList();
        assertThat(errors).as("the original error, then the FATAL for the failed rollback: " + messageTypesOf(response)).hasSize(2);
        assertThat(errorFields(errors.get(0)).get('C')).as("the original failure comes first").isEqualTo("42P01");
        assertThat(errorFields(errors.get(1)).get('S')).isEqualTo("FATAL");
        assertThat(messageTypesOf(response)).as("nothing after a FATAL, ReadyForQuery included").doesNotContain('Z');
      });
    }
    assertThat(getServerDatabase(0, getDatabaseName()).countType(TYPE, true)).as("the pending write is discarded").isZero();
  }

  @Test
  @DisplayName("[#8264] a Sync whose discard rollback throws answers FATAL and closes instead of escaping unanswered")
  void syncWithAFailingRollbackClosesWithFatal() throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        createType(out, in, TYPE);

        sendParse(out, "", "INSERT INTO " + TYPE + " SET tag = 'pipelined'");
        sendBind(out, "", "");
        sendExecute(out, "");
        sendParse(out, "", "SELECT * FROM NoSuchType8264");
        sendBind(out, "", "");
        sendExecute(out, "");
        failNextRollback();
        sendSync(out);

        final List<WireMessage> response = readUntilClosed(in);
        final List<WireMessage> errors = response.stream().filter(m -> m.type() == 'E').toList();
        assertThat(errors).as(messageTypesOf(response).toString()).hasSize(2);
        assertThat(errorFields(errors.get(0)).get('C')).isEqualTo("42P01");
        assertThat(errorFields(errors.get(1)).get('S')).isEqualTo("FATAL");
        assertThat(messageTypesOf(response)).doesNotContain('Z');
      });
    }
    assertThat(getServerDatabase(0, getDatabaseName()).countType(TYPE, true)).as("the pipelined write is discarded").isZero();
  }

  @Test
  @DisplayName("[#8264] a commit that fails at Sync (duplicate key) is answered ErrorResponse + ReadyForQuery, and the connection stays usable")
  void commitFailingAtSyncIsAnswered() throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        createUniqueType(out, in);

        sendParse(out, "", "INSERT INTO " + UNIQUE_TYPE + " SET k = 'b'");
        sendBind(out, "", "");
        sendExecute(out, "");
        sendParse(out, "", "UPDATE " + UNIQUE_TYPE + " SET k = 'a' WHERE k = 'b'");
        sendBind(out, "", "");
        sendExecute(out, "");
        sendSync(out);

        // THE BUG: nothing came back, and the client waited for this ReadyForQuery until it timed out
        final List<WireMessage> response = readUntilReadyForQuery(in);
        assertThat(sqlStateOf(response)).isEqualTo("23505");
        assertThat(readyForQueryStatusOf(response)).isEqualTo('I');

        sendSimpleQuery(out, "SELECT count(*) AS c FROM " + UNIQUE_TYPE);
        final List<WireMessage> after = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(after)).doesNotContain('E');
      });
    }
    assertThat(getServerDatabase(0, getDatabaseName()).countType(UNIQUE_TYPE, true)).as("the failed block is discarded").isEqualTo(1);
  }

  @Test
  @DisplayName("[#8264] a COMMIT that fails ends the block: the session is idle, not aborted")
  void failedCommitEndsTheBlock() throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        createUniqueType(out, in);

        sendSimpleQuery(out, "BEGIN");
        assertThat(readyForQueryStatusOf(readUntilReadyForQuery(in))).isEqualTo('T');
        sendSimpleQuery(out, "INSERT INTO " + UNIQUE_TYPE + " SET k = 'a'");
        assertThat(readyForQueryStatusOf(readUntilReadyForQuery(in))).isEqualTo('T');

        sendSimpleQuery(out, "COMMIT");
        final List<WireMessage> commit = readUntilReadyForQuery(in);
        assertThat(sqlStateOf(commit)).isEqualTo("23505");
        assertThat(readyForQueryStatusOf(commit)).as("PostgreSQL leaves the session idle after a failed COMMIT").isEqualTo('I');

        sendSimpleQuery(out, "SELECT count(*) AS c FROM " + UNIQUE_TYPE);
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).as("the next statement is not refused with 25P02").doesNotContain('E');
      });
    }
    assertThat(getServerDatabase(0, getDatabaseName()).countType(UNIQUE_TYPE, true)).isEqualTo(1);
  }

  private static void failNextRollback() {
    final AtomicBoolean fired = new AtomicBoolean();
    PostgresNetworkExecutor.TEST_ROLLBACK_HOOK = () -> {
      if (fired.compareAndSet(false, true))
        throw new IllegalStateException("simulated rollback failure (issue #8264)");
    };
  }

  private static void createType(final DataOutputStream out, final DataInputStream in, final String type) throws Exception {
    sendSimpleQuery(out, "CREATE DOCUMENT TYPE " + type + " IF NOT EXISTS");
    assertThat(messageTypesOf(readUntilReadyForQuery(in))).doesNotContain('E');
  }

  private static void createUniqueType(final DataOutputStream out, final DataInputStream in) throws Exception {
    createType(out, in, UNIQUE_TYPE);
    for (final String statement : new String[] { "CREATE PROPERTY " + UNIQUE_TYPE + ".k IF NOT EXISTS STRING",
        "CREATE INDEX IF NOT EXISTS ON " + UNIQUE_TYPE + " (k) UNIQUE", "DELETE FROM " + UNIQUE_TYPE,
        "INSERT INTO " + UNIQUE_TYPE + " SET k = 'a'" }) {
      sendSimpleQuery(out, statement);
      assertThat(messageTypesOf(readUntilReadyForQuery(in))).as(statement).doesNotContain('E');
    }
  }

  /**
   * Every message the server sends until it closes the connection.
   */
  private static List<WireMessage> readUntilClosed(final DataInputStream in) throws Exception {
    final List<WireMessage> messages = new ArrayList<>();
    try {
      while (true)
        messages.add(readWireMessage(in));
    } catch (final EOFException e) {
      return messages;
    } catch (final IOException e) {
      // a reset rather than an orderly close ends the stream just the same
      return messages;
    }
  }

  private Socket connect() throws Exception {
    final Socket socket = new Socket();
    socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
    return socket;
  }

  private void authenticate(final DataOutputStream out, final DataInputStream in) throws Exception {
    sendStartupMessage(out, "root", getDatabaseName());
    readMessage(in);
    sendPasswordMessage(out, DEFAULT_PASSWORD_FOR_TESTS);
    readMessageOfType(in, 'Z');
  }
}
