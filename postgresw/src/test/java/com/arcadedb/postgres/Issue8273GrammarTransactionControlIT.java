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
import static com.arcadedb.postgres.PostgresWireMessages.sqlStateOf;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
 * Regression tests for issue #8273: transaction-control text that the exact matchers of {@code PostgresNetworkExecutor}
 * miss but the SQL grammar accepts - a leading or trailing comment, a doubled {@code ;}, {@code BEGIN ISOLATION},
 * {@code COMMIT RETRY} - fell through to the engine, which began/committed/rolled back behind the protocol's back:
 * the ReadyForQuery status byte stayed {@code 'I'}, a failure inside the block did not abort it, the statement was
 * answered with a RowDescription and an empty command tag, and in an aborted block a {@code COMMIT; -- done} was
 * refused with 25P02 like any other statement, wedging the connection.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8273GrammarTransactionControlIT extends PostgresWireProtocolTestBase {

  private static final String TYPE = "Issue8273Probe";

  // { statement sent, tag expected, ReadyForQuery status expected after it }
  private static final String[][] STEPS = {
      { "BEGIN; -- open", "BEGIN", "T" }, { "COMMIT; -- close", "COMMIT", "I" },
      { "/* hi */ BEGIN", "BEGIN", "T" }, { "ROLLBACK; -- x", "ROLLBACK", "I" },
      { "BEGIN -- go", "BEGIN", "T" }, { "COMMIT /*traceparent='00-abc-def-01'*/", "COMMIT", "I" },
      { "BEGIN;;", "BEGIN", "T" }, { "ROLLBACK;;", "ROLLBACK", "I" },
      { "BEGIN ISOLATION REPEATABLE_READ", "BEGIN", "T" }, { "COMMIT;;", "COMMIT", "I" },
      { "BEGIN", "BEGIN", "T" }, { "COMMIT RETRY 3", "COMMIT", "I" } };

  @Test
  @DisplayName("[#8273] simple protocol: every grammar-only spelling is answered with its bare tag and moves the status byte")
  void simpleProtocolRecognizesGrammarSpellings() throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        for (final String[] step : STEPS) {
          sendSimpleQuery(out, step[0]);
          final List<WireMessage> response = readUntilReadyForQuery(in);
          assertThat(messageTypesOf(response)).as(step[0] + ": no RowDescription/DataRow").containsExactly('C', 'Z');
          assertThat(commandTagOf(response)).as(step[0] + " command tag").isEqualTo(step[1]);
          assertThat(readyForQueryStatusOf(response)).as(step[0] + " ReadyForQuery status").isEqualTo(step[2].charAt(0));
        }
      });
    }
  }

  @Test
  @DisplayName("[#8273] extended protocol: every grammar-only spelling is NoData + its bare tag and moves the status byte")
  void extendedProtocolRecognizesGrammarSpellings() throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        for (final String[] step : STEPS) {
          final List<WireMessage> response = roundTrip(out, in, step[0]);
          assertThat(messageTypesOf(response)).as(step[0]).containsExactly('1', '2', 'n', 'C', 'Z');
          assertThat(commandTagOf(response)).as(step[0] + " command tag").isEqualTo(step[1]);
          assertThat(readyForQueryStatusOf(response)).as(step[0] + " ReadyForQuery status").isEqualTo(step[2].charAt(0));
        }
      });
    }
  }

  @Test
  @DisplayName("[#8273] a failure inside a block opened with a commented BEGIN aborts it, and a commented COMMIT ends it")
  void failureInsideACommentedBlockAbortsIt() throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        sendSimpleQuery(out, "CREATE DOCUMENT TYPE " + TYPE + " IF NOT EXISTS");
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).doesNotContain('E');

        sendSimpleQuery(out, "BEGIN; -- open");
        assertThat(readyForQueryStatusOf(readUntilReadyForQuery(in))).isEqualTo('T');

        sendSimpleQuery(out, "SELECT * FROM NoSuchType8273");
        final List<WireMessage> failure = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(failure)).contains('E');
        assertThat(readyForQueryStatusOf(failure)).as("the failure aborts the block").isEqualTo('E');

        sendSimpleQuery(out, "INSERT INTO " + TYPE + " SET tag = 'after-failure-comment'");
        final List<WireMessage> afterFailure = readUntilReadyForQuery(in);
        assertThat(sqlStateOf(afterFailure)).isEqualTo("25P02");

        // THE WEDGE: in an aborted block the commented COMMIT used to be refused with 25P02 like any other statement
        sendSimpleQuery(out, "COMMIT; -- close");
        final List<WireMessage> commit = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(commit)).containsExactly('C', 'Z');
        assertThat(commandTagOf(commit)).as("COMMIT of an aborted block is a ROLLBACK").isEqualTo("ROLLBACK");
        assertThat(readyForQueryStatusOf(commit)).isEqualTo('I');

        // same over the extended protocol
        assertThat(readyForQueryStatusOf(roundTrip(out, in, "/* x */ BEGIN"))).isEqualTo('T');
        assertThat(readyForQueryStatusOf(roundTrip(out, in, "SELECT * FROM NoSuchType8273"))).isEqualTo('E');
        final List<WireMessage> rollback = roundTrip(out, in, "ROLLBACK; -- x");
        assertThat(commandTagOf(rollback)).isEqualTo("ROLLBACK");
        assertThat(readyForQueryStatusOf(rollback)).isEqualTo('I');
      });
    }

    assertThat(getServerDatabase(0, getDatabaseName()).countType(TYPE, true))
        .as("nothing written after the failure inside the block may be committed").isZero();
  }

  @Test
  @DisplayName("[#8273] a write inside a block opened with a grammar-only BEGIN is committed only by the COMMIT")
  void grammarBeginHoldsTheWriteUntilCommit() throws Exception {
    final String type = TYPE + "Commit";
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        sendSimpleQuery(out, "CREATE DOCUMENT TYPE " + type + " IF NOT EXISTS");
        readUntilReadyForQuery(in);

        sendSimpleQuery(out, "BEGIN ISOLATION READ_COMMITTED -- tx");
        assertThat(readyForQueryStatusOf(readUntilReadyForQuery(in))).isEqualTo('T');
        sendSimpleQuery(out, "INSERT INTO " + type + " SET tag = 'inside'");
        assertThat(readyForQueryStatusOf(readUntilReadyForQuery(in))).isEqualTo('T');
        sendSimpleQuery(out, "ROLLBACK;;");
        assertThat(readyForQueryStatusOf(readUntilReadyForQuery(in))).isEqualTo('I');
      });
    }
    assertThat(getServerDatabase(0, getDatabaseName()).countType(type, true)).as("the ROLLBACK;; discarded the write").isZero();
  }

  @Test
  @DisplayName("[#8273] BEGIN ISOLATION with an unknown level is refused and opens nothing")
  void unknownIsolationLevelIsRefused() throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        sendSimpleQuery(out, "BEGIN ISOLATION NO_SUCH_LEVEL");
        final List<WireMessage> simple = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(simple)).contains('E');
        assertThat(readyForQueryStatusOf(simple)).isEqualTo('I');

        final List<WireMessage> extended = roundTrip(out, in, "BEGIN ISOLATION NO_SUCH_LEVEL");
        assertThat(messageTypesOf(extended)).contains('E');
        assertThat(readyForQueryStatusOf(extended)).isEqualTo('I');
      });
    }
  }

  private Socket connect() throws Exception {
    final Socket socket = new Socket();
    socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
    return socket;
  }

  /**
   * One Parse/Bind/Describe('P')/Execute/Sync round trip on the unnamed statement and portal.
   */
  private static List<WireMessage> roundTrip(final DataOutputStream out, final DataInputStream in, final String query)
      throws Exception {
    sendParse(out, "", query);
    sendBind(out, "", "");
    sendDescribe(out, 'P', "");
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
