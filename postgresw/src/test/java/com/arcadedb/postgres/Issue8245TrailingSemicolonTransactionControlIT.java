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
 * Regression tests for issue #8245: over the extended query protocol, {@code parseCommand()} recognized
 * BEGIN/COMMIT/ROLLBACK by exact string equality against the RAW wire text, so {@code "BEGIN;"},
 * {@code "COMMIT;"}, {@code "\nCOMMIT"} or {@code "COMMIT "} fell through to the SQL engine, whose grammar accepts
 * them. The engine then began/committed/rolled back at Execute while the protocol's own block state never moved:
 * ReadyForQuery kept reporting {@code 'I'} inside the block, a failure inside it did not abort it (so a statement
 * pipelined after the failure still ran and was committed), and the statement was answered with a RowDescription
 * and a data row under an empty command tag.
 * <p>
 * Each round trip here is Parse/Bind/Describe('P')/Execute/Sync, the shape libpq's {@code PQexecParams} (psycopg3,
 * the Arrow ADBC driver) sends. pgjdbc strips the trailing ';' itself, which is why the JDBC suites never saw this.
 */
class Issue8245TrailingSemicolonTransactionControlIT extends PostgresWireProtocolTestBase {

  private static final String ABORT_TYPE = "Issue8245Abort";

  @Test
  @DisplayName("[#8245] every spelling of BEGIN/COMMIT/ROLLBACK/END is answered NoData + its tag and moves the status byte")
  void everySpellingIsTransactionControl() throws Exception {
    // { statement sent, tag expected, ReadyForQuery status expected after it }
    final String[][] steps = {
        { "BEGIN;", "BEGIN", "T" }, { "COMMIT;", "COMMIT", "I" },
        { "\nBEGIN", "BEGIN", "T" }, { "ROLLBACK;", "ROLLBACK", "I" },
        { "BEGIN ", "BEGIN", "T" }, { "END;", "COMMIT", "I" },
        { "BEGIN TRANSACTION;", "BEGIN", "T" }, { "COMMIT TRANSACTION;", "COMMIT", "I" },
        { "BEGIN WORK;", "BEGIN", "T" }, { "ROLLBACK WORK;", "ROLLBACK", "I" },
        { " begin ; ", "BEGIN", "T" }, { "\nCOMMIT", "COMMIT", "I" },
        { "BEGIN\n", "BEGIN", "T" }, { "COMMIT ", "COMMIT", "I" },
        { "BEGIN", "BEGIN", "T" }, { "\tROLLBACK TRANSACTION ;\n", "ROLLBACK", "I" },
        { "BEGIN", "BEGIN", "T" }, { "END WORK;", "COMMIT", "I" },
        { "BEGIN", "BEGIN", "T" }, { "{sql} COMMIT;", "COMMIT", "I" } };

    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        for (final String[] step : steps) {
          final List<WireMessage> response = roundTrip(out, in, step[0]);
          final String label = "'" + step[0].replace("\n", "\\n").replace("\t", "\\t") + "'";
          assertThat(messageTypesOf(response))
              .as(label + " is ParseComplete, BindComplete, NoData, CommandComplete, ReadyForQuery - no RowDescription or DataRow")
              .containsExactly('1', '2', 'n', 'C', 'Z');
          assertThat(commandTagOf(response)).as(label + " command tag").isEqualTo(step[1]);
          assertThat(readyForQueryStatusOf(response)).as(label + " ReadyForQuery status").isEqualTo(step[2].charAt(0));
        }
      });
    }
  }

  @Test
  @DisplayName("[#8245] a failure inside a block opened with BEGIN; aborts it: the next statement is refused and COMMIT; rolls back")
  void failureInsideASemicolonBlockAbortsIt() throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        sendSimpleQuery(out, "CREATE DOCUMENT TYPE " + ABORT_TYPE + " IF NOT EXISTS");
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).doesNotContain('E');

        assertThat(readyForQueryStatusOf(roundTrip(out, in, "BEGIN;"))).as("BEGIN; opens the block").isEqualTo('T');

        final List<WireMessage> failure = roundTrip(out, in, "SELECT * FROM NoSuchType8245");
        assertThat(messageTypesOf(failure)).contains('E');
        assertThat(readyForQueryStatusOf(failure)).as("the failure aborts the block").isEqualTo('E');

        // THE BUG: this INSERT ran, was acknowledged INSERT 0 1 and was committed by the COMMIT; that followed.
        final List<WireMessage> afterFailure = roundTrip(out, in, "INSERT INTO " + ABORT_TYPE + " SET tag = 'after-failure'");
        assertThat(messageTypesOf(afterFailure)).as("a statement inside an aborted block is refused").contains('E');
        assertThat(sqlStateOf(afterFailure)).isEqualTo("25P02");
        assertThat(readyForQueryStatusOf(afterFailure)).isEqualTo('E');

        final List<WireMessage> commit = roundTrip(out, in, "COMMIT;");
        assertThat(commandTagOf(commit)).as("COMMIT of an aborted block is a ROLLBACK").isEqualTo("ROLLBACK");
        assertThat(readyForQueryStatusOf(commit)).isEqualTo('I');
      });
    }

    assertThat(getServerDatabase(0, getDatabaseName()).countType(ABORT_TYPE, true))
        .as("nothing written after the failure inside the block may be committed").isZero();
  }

  @Test
  @DisplayName("[#8245] COMMIT; inside a block opened with the bare BEGIN ends the block")
  void semicolonCommitEndsABareBlock() throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        assertThat(readyForQueryStatusOf(roundTrip(out, in, "BEGIN"))).isEqualTo('T');
        final List<WireMessage> commit = roundTrip(out, in, "COMMIT;");
        assertThat(commandTagOf(commit)).isEqualTo("COMMIT");
        assertThat(readyForQueryStatusOf(commit)).as("still 'T' after the client's COMMIT; before the fix").isEqualTo('I');
      });
    }
  }

  @Test
  @DisplayName("[#8245] the simple query protocol also accepts whitespace between the keyword and its ';'")
  void simpleQueryAcceptsWhitespaceBeforeTheSemicolon() throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        for (final String[] step : new String[][] { { "BEGIN ;", "BEGIN", "T" }, { "COMMIT ;", "COMMIT", "I" } }) {
          sendSimpleQuery(out, step[0]);
          final List<WireMessage> response = readUntilReadyForQuery(in);
          assertThat(messageTypesOf(response)).as(step[0]).containsExactly('C', 'Z');
          assertThat(commandTagOf(response)).as(step[0]).isEqualTo(step[1]);
          assertThat(readyForQueryStatusOf(response)).as(step[0]).isEqualTo(step[2].charAt(0));
        }
      });
    }
  }

  @Test
  @DisplayName("[#8245] normalizeStatementText trims and strips exactly one trailing ';'")
  void normalizeStatementText() {
    assertThat(PostgresNetworkExecutor.normalizeStatementText("COMMIT")).isEqualTo("COMMIT");
    assertThat(PostgresNetworkExecutor.normalizeStatementText("COMMIT;")).isEqualTo("COMMIT");
    assertThat(PostgresNetworkExecutor.normalizeStatementText("\n COMMIT ; \n")).isEqualTo("COMMIT");
    assertThat(PostgresNetworkExecutor.normalizeStatementText("COMMIT;;")).isEqualTo("COMMIT;");
    assertThat(PostgresNetworkExecutor.normalizeStatementText(" ; ")).isEmpty();
    assertThat(PostgresNetworkExecutor.normalizeStatementText("SELECT 'a;'")).isEqualTo("SELECT 'a;'");
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
