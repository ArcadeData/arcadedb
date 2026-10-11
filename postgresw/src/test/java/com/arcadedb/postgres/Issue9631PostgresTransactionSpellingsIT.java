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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;

import static com.arcadedb.postgres.PostgresWireMessages.WireMessage;
import static com.arcadedb.postgres.PostgresWireMessages.messageTypesOf;
import static com.arcadedb.postgres.PostgresWireMessages.readUntilReadyForQuery;
import static com.arcadedb.postgres.PostgresWireMessages.readyForQueryStatusOf;
import static com.arcadedb.postgres.PostgresWireMessages.sendBind;
import static com.arcadedb.postgres.PostgresWireMessages.sendDescribe;
import static com.arcadedb.postgres.PostgresWireMessages.sendExecute;
import static com.arcadedb.postgres.PostgresWireMessages.sendParse;
import static com.arcadedb.postgres.PostgresWireMessages.sendSimpleQuery;
import static com.arcadedb.postgres.PostgresWireMessages.sendSync;
import static com.arcadedb.postgres.PostgresWireMessages.show;
import static com.arcadedb.postgres.PostgresWireMessages.sqlStateOf;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
 * Regression tests for issue #9631: PostgreSQL's own spellings of the transaction-control statements - {@code START
 * TRANSACTION}, {@code ABORT}, {@code BEGIN <transaction_mode>} (what Go's {@code database/sql} {@code BeginTx} sends
 * when given options) and {@code COMMIT/ROLLBACK AND [NO] CHAIN} - reached the SQL grammar, which refused them with
 * 42601, over both protocols.
 */
class Issue9631PostgresTransactionSpellingsIT extends PostgresWireProtocolTestBase {

  private static final String TYPE = "Issue9631Probe";

  // { statement sent, tag expected, ReadyForQuery status expected after it }
  private static final String[][] STEPS = {
      { "START TRANSACTION", "START TRANSACTION", "T" }, { "ABORT", "ROLLBACK", "I" },
      { "START TRANSACTION ISOLATION LEVEL READ COMMITTED", "START TRANSACTION", "T" }, { "ABORT WORK", "ROLLBACK", "I" },
      { "START TRANSACTION READ WRITE", "START TRANSACTION", "T" }, { "ABORT TRANSACTION", "ROLLBACK", "I" },
      { "BEGIN ISOLATION LEVEL READ COMMITTED", "BEGIN", "T" }, { "COMMIT AND NO CHAIN", "COMMIT", "I" },
      { "begin isolation level repeatable read", "BEGIN", "T" }, { "ROLLBACK AND NO CHAIN", "ROLLBACK", "I" },
      { "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ WRITE", "BEGIN", "T" }, { "END AND NO CHAIN", "COMMIT", "I" },
      { "begin read write", "BEGIN", "T" }, { "abort;", "ROLLBACK", "I" },
      { "BEGIN NOT DEFERRABLE", "BEGIN", "T" }, { "COMMIT WORK AND NO CHAIN", "COMMIT", "I" },
      // the spellings that worked before must still work, ArcadeDB's own grammar spelling included
      { "BEGIN", "BEGIN", "T" }, { "COMMIT", "COMMIT", "I" },
      { "BEGIN WORK", "BEGIN", "T" }, { "ROLLBACK TRANSACTION", "ROLLBACK", "I" },
      { "BEGIN ISOLATION REPEATABLE_READ", "BEGIN", "T" }, { "END", "COMMIT", "I" } };

  // { statement, SQLSTATE expected }: PostgreSQL accepts each one, this server cannot honor it
  private static final String[][] REFUSED = {
      { "BEGIN READ ONLY", "0A000" }, { "BEGIN ISOLATION LEVEL SERIALIZABLE", "0A000" },
      { "START TRANSACTION READ ONLY", "0A000" }, { "START TRANSACTION ISOLATION LEVEL SERIALIZABLE", "0A000" } };

  @Test
  @DisplayName("[#9631] simple protocol: every PostgreSQL spelling is answered with its tag and moves the status byte")
  void simpleProtocolRecognizesPostgresSpellings() throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        for (final String[] step : STEPS) {
          sendSimpleQuery(out, step[0]);
          final List<WireMessage> response = readUntilReadyForQuery(in);
          assertThat(messageTypesOf(response)).as(step[0] + ": no error, no RowDescription").containsExactly('C', 'Z');
          assertThat(commandTagOf(response)).as(step[0] + " command tag").isEqualTo(step[1]);
          assertThat(readyForQueryStatusOf(response)).as(step[0] + " ReadyForQuery status").isEqualTo(step[2].charAt(0));
        }
      });
    }
  }

  @Test
  @DisplayName("[#9631] extended protocol: every PostgreSQL spelling is NoData + its tag and moves the status byte")
  void extendedProtocolRecognizesPostgresSpellings() throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        for (final String[] step : STEPS) {
          final List<WireMessage> response = roundTrip(out, in, "", step[0]);
          assertThat(messageTypesOf(response)).as(step[0]).containsExactly('1', '2', 'n', 'C', 'Z');
          assertThat(commandTagOf(response)).as(step[0] + " command tag").isEqualTo(step[1]);
          assertThat(readyForQueryStatusOf(response)).as(step[0] + " ReadyForQuery status").isEqualTo(step[2].charAt(0));
        }
      });
    }
  }

  @Test
  @DisplayName("[#9631] a cached named START TRANSACTION / ABORT is applied again on every Bind of the statement")
  void namedPreparedStatementsAreReusable() throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        sendParse(out, "start9631", "START TRANSACTION");
        sendParse(out, "abort9631", "ABORT");
        sendSync(out);
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).containsExactly('1', '1', 'Z');

        for (int i = 0; i < 2; i++) {
          final List<WireMessage> begin = bindExecute(out, in, "start9631");
          assertThat(commandTagOf(begin)).isEqualTo("START TRANSACTION");
          assertThat(readyForQueryStatusOf(begin)).as("reuse " + i + " opens the block").isEqualTo('T');
          final List<WireMessage> abort = bindExecute(out, in, "abort9631");
          assertThat(commandTagOf(abort)).isEqualTo("ROLLBACK");
          assertThat(readyForQueryStatusOf(abort)).as("reuse " + i + " closes the block").isEqualTo('I');
        }
      });
    }
  }

  @Test
  @DisplayName("[#9631] READ ONLY, SERIALIZABLE and AND CHAIN are refused with 0A000, not as syntax errors, and open nothing")
  void unsupportedModesAreRefusedForTheRightReason() throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        for (final String[] refused : REFUSED) {
          sendSimpleQuery(out, refused[0]);
          final List<WireMessage> simple = readUntilReadyForQuery(in);
          assertThat(sqlStateOf(simple)).as(refused[0] + " (simple)").isEqualTo(refused[1]);
          assertThat(readyForQueryStatusOf(simple)).as(refused[0] + " opens no block (simple)").isEqualTo('I');

          final List<WireMessage> extended = roundTrip(out, in, "", refused[0]);
          assertThat(sqlStateOf(extended)).as(refused[0] + " (extended)").isEqualTo(refused[1]);
          assertThat(messageTypesOf(extended)).as(refused[0] + " is refused at Parse").startsWith('E');
          assertThat(readyForQueryStatusOf(extended)).as(refused[0] + " opens no block (extended)").isEqualTo('I');
        }

        // AND CHAIN inside a block: refused, the block is aborted by the refusal, and a ROLLBACK still ends it
        for (final String chain : new String[] { "COMMIT AND CHAIN", "ROLLBACK AND CHAIN", "ABORT AND CHAIN", "END AND CHAIN" }) {
          sendSimpleQuery(out, "BEGIN");
          assertThat(readyForQueryStatusOf(readUntilReadyForQuery(in))).isEqualTo('T');
          sendSimpleQuery(out, chain);
          final List<WireMessage> refusedChain = readUntilReadyForQuery(in);
          assertThat(sqlStateOf(refusedChain)).as(chain).isEqualTo("0A000");
          assertThat(readyForQueryStatusOf(refusedChain)).as(chain + " aborts the block like any failed statement").isEqualTo('E');
          sendSimpleQuery(out, "ABORT");
          final List<WireMessage> abort = readUntilReadyForQuery(in);
          assertThat(commandTagOf(abort)).isEqualTo("ROLLBACK");
          assertThat(readyForQueryStatusOf(abort)).isEqualTo('I');
        }
      });
    }
  }

  @Test
  @DisplayName("[#9631] ABORT and COMMIT AND NO CHAIN end an aborted block, over both protocols")
  void postgresSpellingsEndAnAbortedBlock() throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        for (final String end : new String[] { "ABORT", "ABORT TRANSACTION", "COMMIT AND NO CHAIN", "ROLLBACK WORK AND NO CHAIN" }) {
          sendSimpleQuery(out, "START TRANSACTION");
          assertThat(readyForQueryStatusOf(readUntilReadyForQuery(in))).isEqualTo('T');
          sendSimpleQuery(out, "SELECT * FROM NoSuchType9631");
          assertThat(readyForQueryStatusOf(readUntilReadyForQuery(in))).isEqualTo('E');
          sendSimpleQuery(out, end);
          final List<WireMessage> simple = readUntilReadyForQuery(in);
          assertThat(messageTypesOf(simple)).as(end + " (simple)").containsExactly('C', 'Z');
          assertThat(commandTagOf(simple)).as("ending an aborted block is a ROLLBACK").isEqualTo("ROLLBACK");
          assertThat(readyForQueryStatusOf(simple)).isEqualTo('I');

          assertThat(readyForQueryStatusOf(roundTrip(out, in, "", "START TRANSACTION"))).isEqualTo('T');
          assertThat(readyForQueryStatusOf(roundTrip(out, in, "", "SELECT * FROM NoSuchType9631"))).isEqualTo('E');
          final List<WireMessage> extended = roundTrip(out, in, "", end);
          assertThat(messageTypesOf(extended)).as(end + " (extended)").doesNotContain('E');
          assertThat(commandTagOf(extended)).isEqualTo("ROLLBACK");
          assertThat(readyForQueryStatusOf(extended)).isEqualTo('I');
        }

        // AND CHAIN in an aborted block cannot be honored, so it is refused like any other statement there, the block
        // stays aborted, and a plain ROLLBACK still recovers the session
        sendSimpleQuery(out, "BEGIN");
        readUntilReadyForQuery(in);
        sendSimpleQuery(out, "SELECT * FROM NoSuchType9631");
        assertThat(readyForQueryStatusOf(readUntilReadyForQuery(in))).isEqualTo('E');
        sendSimpleQuery(out, "COMMIT AND CHAIN");
        final List<WireMessage> chain = readUntilReadyForQuery(in);
        assertThat(sqlStateOf(chain)).isEqualTo("25P02");
        assertThat(readyForQueryStatusOf(chain)).isEqualTo('E');
        sendSimpleQuery(out, "ROLLBACK");
        final List<WireMessage> recovered = readUntilReadyForQuery(in);
        assertThat(commandTagOf(recovered)).isEqualTo("ROLLBACK");
        assertThat(readyForQueryStatusOf(recovered)).isEqualTo('I');
        sendSimpleQuery(out, "SELECT 1");
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).doesNotContain('E');
      });
    }
  }

  @Test
  @DisplayName("[#9631] START TRANSACTION holds writes until COMMIT AND NO CHAIN; ABORT discards them")
  void postgresSpellingsDriveTheEngineTransaction() throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        sendSimpleQuery(out, "CREATE DOCUMENT TYPE " + TYPE + " IF NOT EXISTS");
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).doesNotContain('E');

        sendSimpleQuery(out, "START TRANSACTION ISOLATION LEVEL READ COMMITTED");
        readUntilReadyForQuery(in);
        sendSimpleQuery(out, "INSERT INTO " + TYPE + " SET tag = 'discarded'");
        assertThat(readyForQueryStatusOf(readUntilReadyForQuery(in))).isEqualTo('T');
        sendSimpleQuery(out, "ABORT");
        assertThat(readyForQueryStatusOf(readUntilReadyForQuery(in))).isEqualTo('I');

        sendSimpleQuery(out, "BEGIN ISOLATION LEVEL READ COMMITTED READ WRITE");
        readUntilReadyForQuery(in);
        sendSimpleQuery(out, "INSERT INTO " + TYPE + " SET tag = 'kept'");
        assertThat(readyForQueryStatusOf(readUntilReadyForQuery(in))).isEqualTo('T');
        sendSimpleQuery(out, "COMMIT AND NO CHAIN");
        assertThat(readyForQueryStatusOf(readUntilReadyForQuery(in))).isEqualTo('I');
      });
    }

    assertThat(getServerDatabase(0, getDatabaseName()).countType(TYPE, true)).as("only the committed write is there").isEqualTo(1);
  }

  @Test
  @DisplayName("[#9631] BEGIN ISOLATION LEVEL REPEATABLE READ opens a repeatable-read transaction")
  void beginIsolationLevelIsHonored() throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        sendSimpleQuery(out, "BEGIN ISOLATION LEVEL REPEATABLE READ");
        assertThat(readyForQueryStatusOf(readUntilReadyForQuery(in))).isEqualTo('T');
        assertThat(show(out, in, "transaction_isolation")).isEqualTo("repeatable read");
        sendSimpleQuery(out, "ABORT");
        readUntilReadyForQuery(in);

        assertThat(readyForQueryStatusOf(roundTrip(out, in, "", "START TRANSACTION ISOLATION LEVEL REPEATABLE READ"))).isEqualTo('T');
        assertThat(show(out, in, "transaction_isolation")).isEqualTo("repeatable read");
        sendSimpleQuery(out, "ROLLBACK");
        readUntilReadyForQuery(in);

        sendSimpleQuery(out, "START TRANSACTION ISOLATION LEVEL READ COMMITTED");
        readUntilReadyForQuery(in);
        assertThat(show(out, in, "transaction_isolation")).isEqualTo("read committed");
        sendSimpleQuery(out, "ROLLBACK");
        readUntilReadyForQuery(in);
      });
    }
  }

  private Socket connect() throws Exception {
    final Socket socket = new Socket();
    socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
    return socket;
  }

  /**
   * One Parse/Bind/Describe('P')/Execute/Sync round trip on the unnamed portal.
   */
  private static List<WireMessage> roundTrip(final DataOutputStream out, final DataInputStream in, final String statement,
      final String query) throws Exception {
    sendParse(out, statement, query);
    sendBind(out, "", statement);
    sendDescribe(out, 'P', "");
    sendExecute(out, "");
    sendSync(out);
    return readUntilReadyForQuery(in);
  }

  private static List<WireMessage> bindExecute(final DataOutputStream out, final DataInputStream in, final String statement)
      throws Exception {
    sendBind(out, "", statement);
    sendExecute(out, "");
    sendSync(out);
    return readUntilReadyForQuery(in);
  }

  private static String commandTagOf(final List<WireMessage> messages) {
    for (final WireMessage message : messages) {
      if (message.type() == 'C') {
        final byte[] body = message.body();
        final int end = body.length > 0 && body[body.length - 1] == 0 ? body.length - 1 : body.length;
        return new String(body, 0, end, StandardCharsets.UTF_8);
      }
    }
    throw new AssertionError("No CommandComplete message found in " + messageTypesOf(messages));
  }

  private void authenticate(final DataOutputStream out, final DataInputStream in) throws Exception {
    sendStartupMessage(out, "root", getDatabaseName());
    readMessage(in);
    sendPasswordMessage(out, DEFAULT_PASSWORD_FOR_TESTS);
    readMessageOfType(in, 'Z');
  }
}
