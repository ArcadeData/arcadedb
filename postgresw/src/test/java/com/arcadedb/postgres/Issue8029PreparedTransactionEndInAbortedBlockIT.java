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
import static com.arcadedb.postgres.PostgresWireMessages.errorFields;
import static com.arcadedb.postgres.PostgresWireMessages.messageTypesOf;
import static com.arcadedb.postgres.PostgresWireMessages.readUntilReadyForQuery;
import static com.arcadedb.postgres.PostgresWireMessages.readyForQueryStatusOf;
import static com.arcadedb.postgres.PostgresWireMessages.sendBind;
import static com.arcadedb.postgres.PostgresWireMessages.sendDescribe;
import static com.arcadedb.postgres.PostgresWireMessages.sendExecute;
import static com.arcadedb.postgres.PostgresWireMessages.sendParse;
import static com.arcadedb.postgres.PostgresWireMessages.sendSimpleQuery;
import static com.arcadedb.postgres.PostgresWireMessages.sendSync;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
 * Regression tests for issue #8029: while the transaction block is aborted, {@code bindCommand()},
 * {@code describeCommand()} and {@code executeCommand()} refused every message with SQLSTATE {@code 25P02},
 * including the COMMIT/ROLLBACK/END that is supposed to END the block. Only {@code parseCommand()} carried the
 * recovery branch (issue #6548), so a client that ends its transactions through a NAMED prepared statement -
 * pgjdbc once {@code prepareThreshold} promotes {@code ROLLBACK}, psycopg3/libpq through
 * {@code PQexecPrepared} - sent its rollback as Bind(+Describe)+Execute with no Parse and stayed wedged in
 * {@code 'E'} forever.
 * <p>
 * Every test here prepares the transaction-end statement while the session is healthy, aborts a block, then
 * reuses the statement WITHOUT a new Parse - the shape the reporter reproduced.
 */
class Issue8029PreparedTransactionEndInAbortedBlockIT extends PostgresWireProtocolTestBase {

  private static final String ROLLBACK_TYPE = "Issue8029Rollback";
  private static final String COMMIT_TYPE   = "Issue8029Commit";

  @Test
  @DisplayName("[#8029] a prepared ROLLBACK bound and executed without a Parse ends an aborted block")
  void cachedRollbackEndsTheAbortedBlock() throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        sendSimpleQuery(out, "CREATE DOCUMENT TYPE " + ROLLBACK_TYPE + " IF NOT EXISTS");
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).doesNotContain('E');

        // The statement pgjdbc promotes to a named server-side statement, prepared while healthy.
        assertThat(readyForQueryStatusOf(runAndRead(out, in, "rb", "ROLLBACK"))).isEqualTo('I');

        abortAnExplicitTransactionWithAWrite(out, in, ROLLBACK_TYPE, 1);

        // THE BUG: Bind+Execute+Sync of the cached statement, no Parse. Refused with 25P02 before the fix.
        final List<WireMessage> rollback = bindExecuteSync(out, in, "rb");
        assertThat(messageTypesOf(rollback)).as("the cached ROLLBACK must not be refused").doesNotContain('E');
        assertThat(commandCompleteTagOf(rollback)).isEqualTo("ROLLBACK");
        assertThat(readyForQueryStatusOf(rollback)).as("the aborted block is over").isEqualTo('I');

        // The session is usable again - the reporter's "after" step.
        sendSimpleQuery(out, "SELECT 1");
        final List<WireMessage> after = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(after)).doesNotContain('E');
        assertThat(readyForQueryStatusOf(after)).isEqualTo('I');
      });
    }

    assertThat(getServerDatabase(0, getDatabaseName()).countType(ROLLBACK_TYPE, true))
        .as("the aborted block's write must not survive").isZero();
  }

  @Test
  @DisplayName("[#8029] a prepared COMMIT ends an aborted block as a ROLLBACK, and still commits once the session is healthy")
  void cachedCommitEndsTheAbortedBlockAsARollback() throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        sendSimpleQuery(out, "CREATE DOCUMENT TYPE " + COMMIT_TYPE + " IF NOT EXISTS");
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).doesNotContain('E');

        assertThat(readyForQueryStatusOf(runAndRead(out, in, "cm", "COMMIT"))).isEqualTo('I');

        abortAnExplicitTransactionWithAWrite(out, in, COMMIT_TYPE, 1);

        final List<WireMessage> commit = bindExecuteSync(out, in, "cm");
        assertThat(messageTypesOf(commit)).as("the cached COMMIT must not be refused").doesNotContain('E');
        assertThat(commandCompleteTagOf(commit))
            .as("a COMMIT of an aborted block is a ROLLBACK, and PostgreSQL tags it so").isEqualTo("ROLLBACK");
        assertThat(readyForQueryStatusOf(commit)).isEqualTo('I');

        // The same cached COMMIT in a healthy block must still COMMIT: the aborted-block handling must not
        // have rewritten the prepared statement it was bound from.
        assertThat(readyForQueryStatusOf(runAndRead(out, in, "b2", "BEGIN"))).isEqualTo('T');
        assertThat(readyForQueryStatusOf(runAndRead(out, in, "w2", "INSERT INTO " + COMMIT_TYPE + " SET id = 2"))).isEqualTo('T');
        final List<WireMessage> healthyCommit = bindExecuteSync(out, in, "cm");
        assertThat(messageTypesOf(healthyCommit)).doesNotContain('E');
        assertThat(commandCompleteTagOf(healthyCommit)).isEqualTo("COMMIT");
        assertThat(readyForQueryStatusOf(healthyCommit)).isEqualTo('I');
      });
    }

    assertThat(getServerDatabase(0, getDatabaseName()).countType(COMMIT_TYPE, true))
        .as("only the healthy block's write is committed, not the aborted one's").isEqualTo(1);
  }

  @Test
  @DisplayName("[#8029] libpq's PQdescribePrepared/PQexecPrepared shape: Describe of a prepared ROLLBACK is answered, not refused")
  void describedCachedRollbackEndsTheAbortedBlock() throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        sendSimpleQuery(out, "CREATE DOCUMENT TYPE " + ROLLBACK_TYPE + " IF NOT EXISTS");
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).doesNotContain('E');

        assertThat(readyForQueryStatusOf(runAndRead(out, in, "rbd", "ROLLBACK"))).isEqualTo('I');

        abortAnExplicitTransactionWithAWrite(out, in, ROLLBACK_TYPE, 10);

        // Describe('S') of the statement: PostgreSQL refuses only a statement that returns rows while aborted.
        sendDescribe(out, 'S', "rbd");
        sendSync(out);
        final List<WireMessage> describeStatement = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(describeStatement)).as("ParameterDescription, NoData").containsExactly('t', 'n', 'Z');
        assertThat(readyForQueryStatusOf(describeStatement)).as("describing ends nothing").isEqualTo('E');

        // PQexecPrepared: Bind, Describe('P'), Execute, Sync.
        sendBind(out, "rbdp", "rbd");
        sendDescribe(out, 'P', "rbdp");
        sendExecute(out, "rbdp");
        sendSync(out);
        final List<WireMessage> rollback = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(rollback)).as("BindComplete, NoData, CommandComplete, ReadyForQuery")
            .containsExactly('2', 'n', 'C', 'Z');
        assertThat(commandCompleteTagOf(rollback)).isEqualTo("ROLLBACK");
        assertThat(readyForQueryStatusOf(rollback)).isEqualTo('I');
      });
    }
  }

  @Test
  @DisplayName("[#8029] a prepared ROLLBACK also ends a block aborted on the simple query protocol")
  void cachedRollbackEndsABlockAbortedBySimpleQuery() throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        assertThat(readyForQueryStatusOf(runAndRead(out, in, "rbs", "ROLLBACK"))).isEqualTo('I');

        sendSimpleQuery(out, "BEGIN");
        assertThat(readyForQueryStatusOf(readUntilReadyForQuery(in))).isEqualTo('T');
        sendSimpleQuery(out, "SELEC 1");
        assertThat(readyForQueryStatusOf(readUntilReadyForQuery(in))).isEqualTo('E');

        final List<WireMessage> rollback = bindExecuteSync(out, in, "rbs");
        assertThat(messageTypesOf(rollback)).doesNotContain('E');
        assertThat(commandCompleteTagOf(rollback)).isEqualTo("ROLLBACK");
        assertThat(readyForQueryStatusOf(rollback)).isEqualTo('I');
      });
    }
  }

  @Test
  @DisplayName("[#8029] a prepared BEGIN or ordinary statement is still refused in an aborted block")
  void nonTransactionEndStatementsAreStillRefused() throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        assertThat(readyForQueryStatusOf(runAndRead(out, in, "bg", "BEGIN"))).isEqualTo('T');
        assertThat(readyForQueryStatusOf(runAndRead(out, in, "sel", "SELECT 1"))).isEqualTo('T');
        assertThat(readyForQueryStatusOf(runAndRead(out, in, "rbn", "ROLLBACK"))).isEqualTo('I');

        assertThat(readyForQueryStatusOf(runAndRead(out, in, "bg2", "BEGIN"))).isEqualTo('T');
        assertThat(readyForQueryStatusOf(runAndRead(out, in, "bad", "SELEC 1"))).isEqualTo('E');

        // PostgreSQL accepts only a transaction-EXIT statement in an aborted block: BEGIN is not one.
        for (final String statement : new String[] { "bg", "sel" }) {
          final List<WireMessage> refused = bindExecuteSync(out, in, statement);
          assertThat(messageTypesOf(refused)).as(statement + " is refused").containsExactly('E', 'Z');
          assertThat(errorFields(refused.getFirst()).get('C')).isEqualTo("25P02");
          assertThat(readyForQueryStatusOf(refused)).as(statement + " leaves the block aborted").isEqualTo('E');

          sendDescribe(out, 'S', statement);
          sendSync(out);
          final List<WireMessage> describeRefused = readUntilReadyForQuery(in);
          assertThat(messageTypesOf(describeRefused)).as("Describe of " + statement + " is refused").containsExactly('E', 'Z');
        }

        assertThat(readyForQueryStatusOf(bindExecuteSync(out, in, "rbn"))).isEqualTo('I');
      });
    }
  }

  @Test
  @DisplayName("[#8029] a prepared ROLLBACK pipelined behind the failing message is still discarded until Sync")
  void pipelinedRollbackBehindTheFailureIsStillDiscarded() throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        assertThat(readyForQueryStatusOf(runAndRead(out, in, "rbp", "ROLLBACK"))).isEqualTo('I');
        assertThat(readyForQueryStatusOf(runAndRead(out, in, "bgp", "BEGIN"))).isEqualTo('T');

        // The failure and the cached ROLLBACK in ONE pipeline: skip-until-Sync still wins, in PostgreSQL too.
        sendParse(out, "badp", "SELEC 1");
        sendBind(out, "rbpp", "rbp");
        sendExecute(out, "rbpp");
        sendSync(out);
        final List<WireMessage> pipeline = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(pipeline)).as("one ErrorResponse, nothing for the discarded messages").containsExactly('E', 'Z');
        assertThat(readyForQueryStatusOf(pipeline)).isEqualTo('E');

        // Sent again after the Sync, the same cached ROLLBACK ends the block.
        assertThat(readyForQueryStatusOf(bindExecuteSync(out, in, "rbp"))).isEqualTo('I');
      });
    }
  }

  private Socket connect() throws Exception {
    final Socket socket = new Socket();
    socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
    return socket;
  }

  /**
   * Opens an explicit block, writes one document into it, then fails a statement and Syncs, leaving the session
   * in the aborted state ({@code ReadyForQuery 'E'}) the reporter's "cached ROLLBACK" arrives in.
   */
  private static void abortAnExplicitTransactionWithAWrite(final DataOutputStream out, final DataInputStream in, final String type,
      final int id) throws Exception {
    assertThat(readyForQueryStatusOf(runAndRead(out, in, "beg" + id, "BEGIN"))).isEqualTo('T');
    assertThat(readyForQueryStatusOf(runAndRead(out, in, "ins" + id, "INSERT INTO " + type + " SET id = " + id))).isEqualTo('T');
    final List<WireMessage> failure = runAndRead(out, in, "bad" + id, "SELEC 1");
    assertThat(messageTypesOf(failure)).contains('E');
    assertThat(readyForQueryStatusOf(failure)).isEqualTo('E');
  }

  /**
   * Bind+Execute+Sync of an already-prepared statement with NO Parse: what a client statement cache sends.
   */
  private static List<WireMessage> bindExecuteSync(final DataOutputStream out, final DataInputStream in, final String statement)
      throws Exception {
    sendBind(out, statement + "_p", statement);
    sendExecute(out, statement + "_p");
    sendSync(out);
    return readUntilReadyForQuery(in);
  }

  private static List<WireMessage> runAndRead(final DataOutputStream out, final DataInputStream in, final String name,
      final String query) throws Exception {
    sendParse(out, name, query);
    sendBind(out, name, name);
    sendExecute(out, name);
    sendSync(out);
    return readUntilReadyForQuery(in);
  }

  private static String commandCompleteTagOf(final List<WireMessage> messages) {
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
    readMessage(in); // AuthenticationCleartextPassword
    sendPasswordMessage(out, DEFAULT_PASSWORD_FOR_TESTS);
    readMessageOfType(in, 'Z');
  }
}
