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

import com.arcadedb.GlobalConfiguration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
 * A transaction-control statement on the simple query protocol returns no rows, so PostgreSQL answers it with the
 * bare {@code CommandComplete} tag. A {@code RowDescription} in front of the tag, even with zero fields, makes libpq
 * classify the result as {@code PGRES_TUPLES_OK} instead of {@code PGRES_COMMAND_OK}: {@code psql} prints an empty
 * table after {@code BEGIN}, and a client that checks the status when it opens a transaction (the Arrow PostgreSQL
 * ADBC driver, whose DB-API connections start with autocommit off) fails with
 * {@code [libpq] Failed to begin transaction}.
 * <p>
 * {@code ROLLBACK TO <savepoint>} is the one exception (issue #7846): unlike {@code BEGIN}/{@code COMMIT}/
 * {@code ROLLBACK}/{@code SAVEPOINT}/{@code RELEASE}, which this server can honor and so may answer with a
 * bare {@code CommandComplete}, it has no savepoint checkpoint to roll back to - answering success there
 * would silently keep writes the client asked to discard, so it is refused instead.
 */
class TransactionControlNoRowDescriptionIT extends PostgresWireProtocolTestBase {

  @Test
  @DisplayName("BEGIN, COMMIT and ROLLBACK on the simple query protocol are answered with CommandComplete only")
  void transactionControlIsAnsweredWithTheBareCommandTag() throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", GlobalConfiguration.POSTGRES_PORT.getValueAsInteger()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
        for (final String[] step : new String[][] { { "BEGIN", "BEGIN", "T" }, { "COMMIT", "COMMIT", "I" },
            { "BEGIN", "BEGIN", "T" }, { "ROLLBACK", "ROLLBACK", "I" } }) {
          sendSimpleQuery(out, step[0]);
          final List<WireMessage> response = readUntilReadyForQuery(in);
          assertThat(messageTypesOf(response)).as(step[0] + " returns no rows, so no RowDescription").doesNotContain('T');
          assertThat(messageTypesOf(response)).as(step[0] + " returns no rows, so no DataRow").doesNotContain('D');
          assertThat(messageTypesOf(response)).as(step[0] + " is acknowledged by its command tag").contains('C');
          assertThat(commandTagOf(response)).isEqualTo(step[1]);
          assertThat(readyForQueryStatusOf(response)).isEqualTo(step[2].charAt(0));
        }

        // A statement that does return rows keeps its RowDescription, inside and outside a transaction.
        sendSimpleQuery(out, "BEGIN");
        readUntilReadyForQuery(in);
        sendSimpleQuery(out, "SELECT 1 AS one");
        final List<WireMessage> select = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(select)).contains('T', 'D', 'C');
        sendSimpleQuery(out, "COMMIT");
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).doesNotContain('T');
      });
    }
  }

  @Test
  @DisplayName("SAVEPOINT and RELEASE are answered with CommandComplete only")
  void savepointStatementsAreAnsweredWithTheBareCommandTag() throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", GlobalConfiguration.POSTGRES_PORT.getValueAsInteger()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
        sendSimpleQuery(out, "BEGIN");
        readUntilReadyForQuery(in);
        for (final String[] step : new String[][] { { "SAVEPOINT sp1", "SAVEPOINT" }, { "RELEASE sp1", "RELEASE" } }) {
          sendSimpleQuery(out, step[0]);
          final List<WireMessage> response = readUntilReadyForQuery(in);
          assertThat(messageTypesOf(response)).as(step[0] + " returns no rows, so no RowDescription").doesNotContain('T');
          assertThat(messageTypesOf(response)).as(step[0] + " is acknowledged by its command tag").contains('C');
          assertThat(commandTagOf(response)).isEqualTo(step[1]);
          assertThat(readyForQueryStatusOf(response)).as(step[0] + " leaves the transaction open").isEqualTo('T');
        }
        sendSimpleQuery(out, "COMMIT");
        readUntilReadyForQuery(in);
      });
    }
  }

  @Test
  @DisplayName("[#7846] ROLLBACK TO is refused, not answered with CommandComplete, and aborts the transaction")
  void rollbackToIsRefusedRatherThanAcceptedAsANoOp() throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", GlobalConfiguration.POSTGRES_PORT.getValueAsInteger()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
        sendSimpleQuery(out, "BEGIN");
        readUntilReadyForQuery(in);
        sendSimpleQuery(out, "SAVEPOINT sp1");
        readUntilReadyForQuery(in);

        // This server has no savepoint checkpoint to roll back to: a CommandComplete here would tell the
        // client its rollback succeeded while every write made since the savepoint is still pending.
        sendSimpleQuery(out, "ROLLBACK TO sp1");
        final List<WireMessage> response = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(response)).as("ROLLBACK TO is refused, not silently accepted").contains('E');
        assertThat(messageTypesOf(response)).as("a refused statement is not acknowledged by a command tag").doesNotContain('C');
        assertThat(readyForQueryStatusOf(response)).as("the transaction is left aborted").isEqualTo('E');

        // Once aborted, every statement but COMMIT/ROLLBACK/END is refused (issue #6457) - including a
        // client that ignores the ErrorResponse and tries to keep using the transaction.
        sendSimpleQuery(out, "SELECT 1");
        assertThat(readyForQueryStatusOf(readUntilReadyForQuery(in))).isEqualTo('E');

        sendSimpleQuery(out, "ROLLBACK");
        readUntilReadyForQuery(in);
      });
    }
  }

  @Test
  @DisplayName("[#7846] ROLLBACK TO outside an explicit transaction is refused without wedging the session")
  void rollbackToOutsideAnExplicitTransactionIsRefusedButLeavesTheSessionIdle() throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", GlobalConfiguration.POSTGRES_PORT.getValueAsInteger()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
        // No BEGIN: setErrorInTx() is a no-op without an explicit transaction (explicitTransactionStarted ==
        // false), matching every other refused statement in autocommit mode - there is nothing pending to
        // lose, so the session must not be left wedged in an aborted state the client can never end.
        sendSimpleQuery(out, "ROLLBACK TO sp1");
        final List<WireMessage> response = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(response)).as("ROLLBACK TO is refused even in autocommit mode").contains('E');
        assertThat(readyForQueryStatusOf(response)).as("autocommit mode is left idle, not aborted").isEqualTo('I');

        sendSimpleQuery(out, "SELECT 1");
        final List<WireMessage> select = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(select)).as("the session still accepts statements").contains('T', 'D', 'C');
      });
    }
  }

  @Test
  @DisplayName("A language-prefixed {sql}BEGIN still gets the bare BEGIN command tag")
  void languagePrefixedTransactionControlGetsTheRightCommandTag() throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", GlobalConfiguration.POSTGRES_PORT.getValueAsInteger()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
        sendSimpleQuery(out, "{sql}BEGIN");
        final List<WireMessage> begin = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(begin)).doesNotContain('T');
        assertThat(commandTagOf(begin)).isEqualTo("BEGIN");
        sendSimpleQuery(out, "{sql}COMMIT");
        final List<WireMessage> commit = readUntilReadyForQuery(in);
        assertThat(commandTagOf(commit)).isEqualTo("COMMIT");
      });
    }
  }

  /**
   * Issue #7905: the fix above covered only the simple ('Q') protocol. The extended one - Parse/Bind/Describe/
   * Execute, which is what libpq's {@code PQexecParams}/{@code PQexecPrepared} send, and with them psycopg3,
   * asyncpg and the Arrow ADBC driver this whole class is named after - still answered BEGIN/COMMIT/ROLLBACK
   * with a zero-field {@code RowDescription}, because Parse gave those three an empty materialized result set
   * whose (empty, but non-null) column map {@code describeCommand()} then announced. That is the same byte, and
   * so the same libpq status defect, reached from the protocol those clients actually use.
   * <p>
   * SAVEPOINT/RELEASE/SET were already answered {@code NoData} on this path, so the file used to answer the same
   * class of statement two different ways depending on the keyword.
   */
  @Test
  @DisplayName("[#7905] BEGIN, COMMIT and ROLLBACK on the EXTENDED protocol answer NoData, not a zero-field RowDescription")
  void transactionControlOnTheExtendedProtocolAnswersNoData() throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", GlobalConfiguration.POSTGRES_PORT.getValueAsInteger()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
        for (final String[] step : new String[][] { { "BEGIN", "BEGIN", "T" }, { "SAVEPOINT sp1", "SAVEPOINT", "T" },
            { "COMMIT", "COMMIT", "I" }, { "BEGIN", "BEGIN", "T" }, { "ROLLBACK", "ROLLBACK", "I" } }) {
          sendParse(out, step[0]);
          sendBind(out);
          sendDescribePortal(out);
          sendExecute(out);
          sendSync(out);

          final List<WireMessage> response = readUntilReadyForQuery(in);
          assertThat(messageTypesOf(response)).as(step[0] + " is parsed and bound").contains('1', '2');
          assertThat(messageTypesOf(response))
              .as("%s returns no result set, so Describe('P') owes NoData - a 'T' of zero fields makes libpq "
                  + "report PGRES_TUPLES_OK", step[0])
              .doesNotContain('T');
          assertThat(messageTypesOf(response)).as(step[0] + " answers exactly one NoData").containsOnlyOnce('n');
          assertThat(messageTypesOf(response)).as(step[0] + " returns no rows, so no DataRow").doesNotContain('D');
          assertThat(commandTagOf(response)).as("the command tag is unchanged by the Describe fix").isEqualTo(step[1]);
          assertThat(readyForQueryStatusOf(response)).as(step[0] + " moves the transaction state as before")
              .isEqualTo(step[2].charAt(0));
        }

        // A statement that DOES return rows still gets its RowDescription on the same connection and the same
        // protocol, so the fix narrowed nothing but the three keywords.
        sendParse(out, "SELECT 1 AS one");
        sendBind(out);
        sendDescribePortal(out);
        sendExecute(out);
        sendSync(out);
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).contains('T', 'D', 'C');
      });
    }
  }

  /**
   * The recovery portal an aborted block builds for a COMMIT/END/ROLLBACK sent through Parse (issue #6548) is a
   * transaction-control statement too, and took the same empty-result-set path, so it answered the same
   * zero-field RowDescription (issue #7905).
   */
  @Test
  @DisplayName("[#7905] the ROLLBACK that recovers an aborted block over the extended protocol answers NoData too")
  void theAbortedBlockRecoveryPortalAnswersNoDataAsWell() throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", GlobalConfiguration.POSTGRES_PORT.getValueAsInteger()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
        sendSimpleQuery(out, "BEGIN");
        readUntilReadyForQuery(in);
        // Aborts the block: every statement but COMMIT/ROLLBACK/END is refused from here on.
        sendSimpleQuery(out, "SELEKT bogus");
        assertThat(readyForQueryStatusOf(readUntilReadyForQuery(in))).isEqualTo('E');

        sendParse(out, "COMMIT");
        sendBind(out);
        sendDescribePortal(out);
        sendExecute(out);
        sendSync(out);

        final List<WireMessage> response = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(response)).as("the recovery statement is accepted").contains('1');
        assertThat(messageTypesOf(response)).as("Describe('P') answers NoData, and exactly once").containsOnlyOnce('n');
        assertThat(messageTypesOf(response)).doesNotContain('T');
        assertThat(commandTagOf(response)).as("a COMMIT of an aborted block is tagged ROLLBACK").isEqualTo("ROLLBACK");
        assertThat(readyForQueryStatusOf(response)).as("the block is over").isEqualTo('I');
      });
    }
  }

  private static void sendParse(final DataOutputStream out, final String query) throws Exception {
    final ByteArrayOutputStream body = new ByteArrayOutputStream();
    writeCString(body, ""); // unnamed statement
    writeCString(body, query);
    body.write(0);
    body.write(0); // int16 numParamDataTypes = 0

    final byte[] bodyBytes = body.toByteArray();
    out.writeByte('P');
    out.writeInt(4 + bodyBytes.length);
    out.write(bodyBytes);
    out.flush();
  }

  private static void sendBind(final DataOutputStream out) throws Exception {
    final ByteArrayOutputStream body = new ByteArrayOutputStream();
    writeCString(body, ""); // portal name
    writeCString(body, ""); // statement name
    body.write(0);
    body.write(0); // int16 numParamFormatCodes = 0
    body.write(0);
    body.write(0); // int16 numParamValues = 0
    body.write(0);
    body.write(0); // int16 numResultFormatCodes = 0

    final byte[] bodyBytes = body.toByteArray();
    out.writeByte('B');
    out.writeInt(4 + bodyBytes.length);
    out.write(bodyBytes);
    out.flush();
  }

  private static void sendDescribePortal(final DataOutputStream out) throws Exception {
    final ByteArrayOutputStream body = new ByteArrayOutputStream();
    body.write('P'); // describe a portal, not a prepared statement
    writeCString(body, ""); // unnamed portal

    final byte[] bodyBytes = body.toByteArray();
    out.writeByte('D');
    out.writeInt(4 + bodyBytes.length);
    out.write(bodyBytes);
    out.flush();
  }

  private static void sendExecute(final DataOutputStream out) throws Exception {
    final ByteArrayOutputStream body = new ByteArrayOutputStream();
    writeCString(body, ""); // portal name
    body.write(0);
    body.write(0);
    body.write(0);
    body.write(0); // int32 limit = 0 (no limit)

    final byte[] bodyBytes = body.toByteArray();
    out.writeByte('E');
    out.writeInt(4 + bodyBytes.length);
    out.write(bodyBytes);
    out.flush();
  }

  private static void sendSync(final DataOutputStream out) throws Exception {
    out.writeByte('S');
    out.writeInt(4);
    out.flush();
  }

  private static String commandTagOf(final List<WireMessage> messages) {
    final WireMessage complete = messages.stream().filter(m -> m.type() == 'C').findFirst()
        .orElseThrow(() -> new AssertionError("expected a CommandComplete"));
    final byte[] body = complete.body();
    return new String(body, 0, body.length - 1, StandardCharsets.UTF_8);
  }

  private void authenticate(final DataOutputStream out, final DataInputStream in) throws Exception {
    sendStartupMessage(out, "root", getDatabaseName());
    readMessage(in); // AuthenticationCleartextPassword
    sendPasswordMessage(out, DEFAULT_PASSWORD_FOR_TESTS);
    readMessageOfType(in, 'Z'); // drain AuthenticationOk/BackendKeyData/ParameterStatus.../ReadyForQuery
  }

  private static void sendSimpleQuery(final DataOutputStream out, final String query) throws Exception {
    final byte[] queryBytes = query.getBytes(StandardCharsets.UTF_8);
    out.writeByte('Q');
    out.writeInt(4 + queryBytes.length + 1);
    out.write(queryBytes);
    out.writeByte(0);
    out.flush();
  }

  private record WireMessage(char type, byte[] body) {
  }

  private static WireMessage readWireMessage(final DataInputStream in) throws Exception {
    final int type = in.readUnsignedByte();
    final int length = in.readInt();
    final byte[] body = new byte[length - 4];
    in.readFully(body);
    return new WireMessage((char) type, body);
  }

  /**
   * Reads messages until (and including) the next {@code ReadyForQuery}, so a single simple-query request/
   * response round trip can be inspected in full - every {@code ErrorResponse}, {@code CommandComplete}, row,
   * etc. the server sent back for it, not just the final status byte.
   */
  private static List<WireMessage> readUntilReadyForQuery(final DataInputStream in) throws Exception {
    final List<WireMessage> messages = new ArrayList<>();
    WireMessage message;
    do {
      message = readWireMessage(in);
      messages.add(message);
    } while (message.type() != 'Z');
    return messages;
  }

  private static char readyForQueryStatusOf(final List<WireMessage> messages) {
    final WireMessage last = messages.get(messages.size() - 1);
    assertThat(last.type()).isEqualTo('Z');
    return (char) last.body()[0];
  }

  private static List<Character> messageTypesOf(final List<WireMessage> messages) {
    final List<Character> types = new ArrayList<>(messages.size());
    for (final WireMessage message : messages)
      types.add(message.type());
    return types;
  }
}
