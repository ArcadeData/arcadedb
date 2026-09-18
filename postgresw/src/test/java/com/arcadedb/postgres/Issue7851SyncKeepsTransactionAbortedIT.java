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
import com.arcadedb.database.Database;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.Duration;
import java.util.List;

import static com.arcadedb.postgres.PostgresWireMessages.WireMessage;
import static com.arcadedb.postgres.PostgresWireMessages.errorFields;
import static com.arcadedb.postgres.PostgresWireMessages.messageTypesOf;
import static com.arcadedb.postgres.PostgresWireMessages.readUntilReadyForQuery;
import static com.arcadedb.postgres.PostgresWireMessages.readWireMessage;
import static com.arcadedb.postgres.PostgresWireMessages.readyForQueryStatusOf;
import static com.arcadedb.postgres.PostgresWireMessages.sendBind;
import static com.arcadedb.postgres.PostgresWireMessages.sendClose;
import static com.arcadedb.postgres.PostgresWireMessages.sendDescribe;
import static com.arcadedb.postgres.PostgresWireMessages.sendExecute;
import static com.arcadedb.postgres.PostgresWireMessages.sendParse;
import static com.arcadedb.postgres.PostgresWireMessages.sendSimpleQuery;
import static com.arcadedb.postgres.PostgresWireMessages.sendSync;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
 * Regression test for issue #7851: {@code syncCommand()}'s error-recovery branch rolled the engine transaction
 * back and cleared {@code errorInTransaction}, but left {@code explicitTransactionStarted} set. The client was
 * then told {@code 'T'} - still inside a healthy transaction - instead of {@code 'E'}, and the SQLSTATE
 * {@code 25P02} guards that key off {@code errorInTransaction} were bypassed, so each further statement ran and
 * auto-committed on its own. A client grouping several statements into one transaction, and expecting to be
 * able to roll the whole group back, got some of them silently persisted.
 * <p>
 * Real PostgreSQL keeps the session in state {@code E} until the client explicitly ends the transaction block
 * with {@code COMMIT}/{@code ROLLBACK}/{@code END}; Sync itself never ends an aborted transaction. This is the
 * explicit-transaction counterpart of {@link Issue7775PipelineSkipUntilSyncIT}, which covers the autocommit case.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7851SyncKeepsTransactionAbortedIT extends PostgresWireProtocolTestBase {

  private void authenticate(final DataOutputStream out, final DataInputStream in) throws Exception {
    sendStartupMessage(out, "root", getDatabaseName());
    readMessage(in); // AuthenticationCleartextPassword
    sendPasswordMessage(out, DEFAULT_PASSWORD_FOR_TESTS);
    readMessageOfType(in, 'Z'); // drain AuthenticationOk/BackendKeyData/ParameterStatus.../ReadyForQuery
  }

  private static final String TYPE_NAME  = "Issue7851Aborted";
  private static final String BLOCK_TYPE = "Issue7851Block";

  @Test
  @DisplayName("[#7851] Sync after an error inside an explicit transaction reports 'E' and refuses further statements")
  void syncDoesNotEndTheAbortedTransactionBlock() throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", GlobalConfiguration.POSTGRES_PORT.getValueAsInteger()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        sendSimpleQuery(out, "CREATE DOCUMENT TYPE " + TYPE_NAME + " IF NOT EXISTS");
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).doesNotContain('E');

        sendSimpleQuery(out, "BEGIN");
        assertThat(readyForQueryStatusOf(readUntilReadyForQuery(in))).isEqualTo('T');

        // Row A, inside the explicit transaction: still pending, nothing committed yet.
        runExtendedStatement(out, "a", "INSERT INTO " + TYPE_NAME + " SET id = 1");
        assertThat(readyForQueryStatusOf(readUntilReadyForQuery(in)))
            .as("a healthy statement leaves the transaction open").isEqualTo('T');

        // A statement that fails inside the block. Nothing else is sent before the Sync.
        sendParse(out, "bad", "SELEC 1");
        assertThat(readWireMessage(in).type()).as("ErrorResponse for the malformed statement").isEqualTo('E');

        // THE BUG: this Sync discarded the engine transaction (row A is gone) but told the client 'T'.
        sendSync(out);
        assertThat(readyForQueryStatusOf(readUntilReadyForQuery(in)))
            .as("Sync does not end a transaction block: the session stays aborted until the client ends it")
            .isEqualTo('E');

        // Row B: a client that believed the 'T' would send this next, and before the fix it ran as its own
        // implicit transaction and committed. It must be refused with 25P02 instead.
        sendParse(out, "b", "INSERT INTO " + TYPE_NAME + " SET id = 2");
        sendBind(out, "b", "b");
        final WireMessage refusal = readWireMessage(in);
        assertThat(refusal.type()).as("an ErrorResponse, not BindComplete").isEqualTo('E');
        assertThat(errorFields(refusal).get('C')).as("SQLSTATE 25P02 in_failed_sql_transaction").isEqualTo("25P02");

        sendExecute(out, "b");
        sendSync(out);
        assertThat(readyForQueryStatusOf(readUntilReadyForQuery(in)))
            .as("still aborted after a second Sync").isEqualTo('E');

        // Only the client's own ROLLBACK ends the block.
        sendSimpleQuery(out, "ROLLBACK");
        assertThat(readyForQueryStatusOf(readUntilReadyForQuery(in))).isEqualTo('I');

        final List<WireMessage> afterRecovery = runAndRead(out, in, "c", "SELECT 1");
        assertThat(messageTypesOf(afterRecovery)).as("the session is fully usable again").doesNotContain('E');
        assertThat(readyForQueryStatusOf(afterRecovery)).isEqualTo('I');
      });
    }

    final Database database = getServerDatabase(0, getDatabaseName());
    assertThat(database.countType(TYPE_NAME, true))
        .as("neither the row written before the error nor the one refused after it may reach the database")
        .isZero();
  }

  @Test
  @DisplayName("[#7851] after Sync, an Execute in the aborted block is refused with 25P02 and a Close still completes")
  void executeIsRefusedAndCloseStillCompletesAfterSync() throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", GlobalConfiguration.POSTGRES_PORT.getValueAsInteger()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        sendSimpleQuery(out, "BEGIN");
        assertThat(readyForQueryStatusOf(readUntilReadyForQuery(in))).isEqualTo('T');

        // A portal prepared and bound while the block was still healthy, so it is still registered when the
        // block aborts below - the case where a silent discard would leave the client waiting forever.
        sendParse(out, "live", "SELECT 1");
        sendBind(out, "live", "live");
        assertThat(messageTypesOf(List.of(readWireMessage(in), readWireMessage(in))))
            .as("ParseComplete then BindComplete while the block is healthy").containsExactly('1', '2');

        sendParse(out, "bad", "SELEC 1");
        assertThat(readWireMessage(in).type()).isEqualTo('E');
        sendSync(out);
        assertThat(readyForQueryStatusOf(readUntilReadyForQuery(in))).isEqualTo('E');

        // Execute of that still-registered portal: refused, not run and not swallowed.
        sendExecute(out, "live");
        final WireMessage refusal = readWireMessage(in);
        assertThat(refusal.type()).as("an ErrorResponse, not silence and not a result").isEqualTo('E');
        assertThat(errorFields(refusal).get('C')).isEqualTo("25P02");

        sendSync(out);
        assertThat(readyForQueryStatusOf(readUntilReadyForQuery(in))).isEqualTo('E');

        // Close ends no transaction command and touches no data, so PostgreSQL runs it inside an aborted block
        // like any other: the client cleaning up its portals gets the CloseComplete it waits for.
        sendClose(out, 'P', "live");
        assertThat(readWireMessage(in).type()).as("CloseComplete").isEqualTo('3');

        sendSimpleQuery(out, "ROLLBACK");
        assertThat(readyForQueryStatusOf(readUntilReadyForQuery(in))).isEqualTo('I');
      });
    }
  }

  @Test
  @DisplayName("[#7851] a BEGIN sent over the extended protocol opens a real transaction that ROLLBACK discards")
  void anExtendedProtocolBeginIsARealTransactionBlock() throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", GlobalConfiguration.POSTGRES_PORT.getValueAsInteger()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        sendSimpleQuery(out, "CREATE DOCUMENT TYPE " + BLOCK_TYPE + " IF NOT EXISTS");
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).doesNotContain('E');

        // BEGIN through Parse/Bind/Execute rather than a simple Query - what pgx in extended mode, and any
        // pipelining client, sends. parseCommand()'s BEGIN branch only raises explicitTransactionStarted, so
        // without an engine transaction behind it every write below committed itself and the ROLLBACK found
        // nothing to undo, all while ReadyForQuery reported 'T'.
        assertThat(readyForQueryStatusOf(runAndRead(out, in, "b1", "BEGIN"))).isEqualTo('T');
        assertThat(readyForQueryStatusOf(runAndRead(out, in, "i1", "INSERT INTO " + BLOCK_TYPE + " SET id = 1")))
            .isEqualTo('T');
        assertThat(readyForQueryStatusOf(runAndRead(out, in, "r1", "ROLLBACK"))).isEqualTo('I');

        assertThat(readyForQueryStatusOf(runAndRead(out, in, "b2", "BEGIN"))).isEqualTo('T');
        assertThat(readyForQueryStatusOf(runAndRead(out, in, "i2", "INSERT INTO " + BLOCK_TYPE + " SET id = 2")))
            .isEqualTo('T');
        assertThat(readyForQueryStatusOf(runAndRead(out, in, "c2", "COMMIT"))).isEqualTo('I');
      });
    }

    final Database database = getServerDatabase(0, getDatabaseName());
    assertThat(database.countType(BLOCK_TYPE, true))
        .as("the rolled-back block leaves nothing behind; only the committed one persists")
        .isEqualTo(1);
    assertThat(database.query("sql", "SELECT id FROM " + BLOCK_TYPE).next().<Integer>getProperty("id"))
        .as("the surviving row is the one the client committed").isEqualTo(2);
  }

  private static void runExtendedStatement(final DataOutputStream out, final String name, final String query) throws Exception {
    sendParse(out, name, query);
    sendBind(out, name, name);
    sendExecute(out, name);
    sendSync(out);
  }

  private static List<WireMessage> runAndRead(final DataOutputStream out, final DataInputStream in, final String name,
      final String query) throws Exception {
    runExtendedStatement(out, name, query);
    return readUntilReadyForQuery(in);
  }
}
