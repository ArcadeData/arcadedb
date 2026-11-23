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

import com.arcadedb.database.Database;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

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
import static com.arcadedb.postgres.PostgresWireMessages.sqlStateOf;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
 * A transaction block aborted by a failed statement, over the PostgreSQL wire protocol: every later statement is refused
 * with SQLSTATE 25P02 until the block ends, on the simple and the extended query protocol, a Sync does not end it, and
 * the {@code ROLLBACK} (or {@code COMMIT}, reported as {@code ROLLBACK}) that ends it is accepted however it arrives -
 * including when the rollback itself fails.
 */
class PostgresAbortedTransactionIT extends PostgresWireProtocolTestBase {

  /*
   * Regression tests for issue #6457: over the Postgres <b>simple-query</b> protocol, once a statement erred
   * inside an explicit {@code BEGIN} block the session became permanently wedged.
   * <p>
   * Three defects, all in {@link PostgresNetworkExecutor}:
   * <ul>
   *   <li>{@code writeReadyForQueryMessage()} never reported the aborted status {@code 'E'} - a client had no
   *   wire-level signal the transaction had failed.</li>
   *   <li>{@code queryCommand()} recognised only {@code BEGIN}, not {@code COMMIT}/{@code ROLLBACK}/{@code END}
   *   - over the simple protocol there was no statement that could ever end an aborted block, and even without
   *   a prior error a plain {@code COMMIT}/{@code ROLLBACK} left {@code explicitTransactionStarted} stuck.</li>
   *   <li>while aborted, {@code queryCommand()} silently returned for every statement - no {@code ErrorResponse},
   *   no {@code CommandComplete} - so a client had no way to tell its statement never ran.</li>
   * </ul>
   * These tests speak the wire protocol directly over a raw socket (rather than through the JDBC driver) so the
   * exact byte-level behaviour - the {@code ReadyForQuery} status byte, whether an {@code ErrorResponse} was
   * actually sent - can be asserted on.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  // Regression: #6457
  @Test
  @DisplayName("[#6457] an aborted explicit transaction reports status 'E', refuses further statements, and ROLLBACK ends it")
  void abortedTransactionRejectsFurtherStatementsUntilRollback() throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
        // 1. BEGIN starts an explicit transaction: status must go to 'T'.
        sendSimpleQuery(out, "BEGIN");
        assertThat(readyForQueryStatusOf(readUntilReadyForQuery(in))).isEqualTo('T');

        // 2. A malformed statement errors: real Postgres reports the aborted status 'E', not 'T'.
        sendSimpleQuery(out, "SELEC 1");
        final List<WireMessage> afterBadStatement = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(afterBadStatement)).as("an ErrorResponse for the malformed statement").contains('E');
        assertThat(readyForQueryStatusOf(afterBadStatement))
            .as("ReadyForQuery must report the aborted transaction status").isEqualTo('E');

        // 3. Any further statement - even a harmless one - must be refused with an ErrorResponse, not
        // silently swallowed (the core of issue #6457): before the fix this response held only the
        // ReadyForQuery message and nothing else.
        sendSimpleQuery(out, "SELECT 1");
        final List<WireMessage> whileAborted = readUntilReadyForQuery(in);
        assertThat(whileAborted).as("the aborted statement must not be silently swallowed").hasSizeGreaterThan(1);
        final WireMessage error = whileAborted.stream().filter(m -> m.type() == 'E').findFirst()
            .orElseThrow(() -> new AssertionError("expected an ErrorResponse while the transaction is aborted"));
        final Map<Character, String> fields = errorFields(error);
        assertThat(fields.get('C')).as("SQLSTATE 25P02 in_failed_sql_transaction").isEqualTo("25P02");
        assertThat(fields.get('M')).contains("current transaction is aborted");
        assertThat(readyForQueryStatusOf(whileAborted)).as("still aborted").isEqualTo('E');

        // 4. ROLLBACK, unrecognized before the fix, must now end the aborted block cleanly.
        sendSimpleQuery(out, "ROLLBACK");
        final List<WireMessage> afterRollback = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(afterRollback)).as("no ErrorResponse for ROLLBACK itself").doesNotContain('E');
        assertThat(readyForQueryStatusOf(afterRollback)).as("back to idle, outside any transaction").isEqualTo('I');

        // 5. The session is fully usable again.
        sendSimpleQuery(out, "SELECT 1");
        final List<WireMessage> afterRecovery = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(afterRecovery)).as("a normal query answers normally again").doesNotContain('E');
        assertThat(readyForQueryStatusOf(afterRecovery)).isEqualTo('I');
      });
    }
  }

  // Regression: #6457
  @Test
  @DisplayName("[#6457] COMMIT of an aborted transaction acts as ROLLBACK and discards the writes made before the error")
  void abortedTransactionCommitActsAsRollbackAndDiscardsPriorWrites() throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
        sendSimpleQuery(out, "CREATE DOCUMENT TYPE Issue6457Commit IF NOT EXISTS");
        readUntilReadyForQuery(in);

        sendSimpleQuery(out, "BEGIN");
        readUntilReadyForQuery(in);

        sendSimpleQuery(out, "INSERT INTO Issue6457Commit SET id = 1");
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).doesNotContain('E');

        sendSimpleQuery(out, "SELEC 1"); // aborts the transaction
        assertThat(readyForQueryStatusOf(readUntilReadyForQuery(in))).isEqualTo('E');

        // COMMIT while aborted: Postgres treats it exactly like ROLLBACK (with a warning) rather than
        // trying to persist writes that already failed.
        sendSimpleQuery(out, "COMMIT");
        final List<WireMessage> afterCommit = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(afterCommit)).doesNotContain('E');
        assertThat(readyForQueryStatusOf(afterCommit)).isEqualTo('I');
      });
    }

    final Database database = getServerDatabase(0, getDatabaseName());
    assertThat(database.countType("Issue6457Commit", true))
        .as("a COMMIT of an aborted transaction must not persist the writes made before the error")
        .isZero();
  }

  // Regression: #6457
  @Test
  @DisplayName("[#6457] a plain COMMIT with no prior error is recognized over the simple-query protocol and persists the writes")
  void plainCommitOverSimpleQueryProtocolEndsTheTransactionAndPersists() throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
        sendSimpleQuery(out, "CREATE DOCUMENT TYPE Issue6457PlainCommit IF NOT EXISTS");
        readUntilReadyForQuery(in);

        sendSimpleQuery(out, "BEGIN");
        assertThat(readyForQueryStatusOf(readUntilReadyForQuery(in))).isEqualTo('T');

        sendSimpleQuery(out, "INSERT INTO Issue6457PlainCommit SET id = 1");
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).doesNotContain('E');

        // Before the fix, COMMIT was not recognized at all here: it fell through to the SQL engine as an
        // ordinary (failing) statement and explicitTransactionStarted was never cleared, so status stayed 'T'.
        sendSimpleQuery(out, "COMMIT");
        final List<WireMessage> afterCommit = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(afterCommit)).doesNotContain('E');
        assertThat(readyForQueryStatusOf(afterCommit)).as("COMMIT must end the explicit transaction").isEqualTo('I');
      });
    }

    final Database database = getServerDatabase(0, getDatabaseName());
    assertThat(database.countType("Issue6457PlainCommit", true))
        .as("a plain COMMIT must persist the writes made in the transaction")
        .isEqualTo(1);
  }

  /*
   * Regression test for issue #6545: over the Postgres <b>extended-query</b> protocol, once a statement erred
   * inside an explicit {@code BEGIN} block, {@link PostgresNetworkExecutor#bindCommand()} silently accepted a
   * further {@code Bind} message instead of refusing it - the guard at the top of the aborted-transaction check
   * simply returned, writing nothing back to the client at all (not even a malformed response), so a client
   * waiting for a reply to that {@code Bind} would hang.
   * <p>
   * This is the extended-protocol counterpart of #6457/#6542, which fixed the same silent-swallow pattern on
   * the simple-query path. This test speaks the wire protocol directly over a raw socket (Parse/Bind messages
   * built by hand) so the exact byte-level response - whether an {@code ErrorResponse} was actually sent instead
   * of nothing - can be asserted on.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  // Regression: #6545
  @Test
  @DisplayName("[#6545] a Bind sent while the transaction is aborted is refused with an ErrorResponse, not silently dropped")
  void bindWhileAbortedIsRefusedNotSilentlyDropped() throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
        // 1. BEGIN starts an explicit transaction: status must go to 'T'.
        sendSimpleQuery(out, "BEGIN");
        assertThat(readyForQueryStatusOf(readUntilReadyForQuery(in))).isEqualTo('T');

        // 2. Parse a statement while the transaction is still healthy: this must succeed normally and store
        // the prepared statement, so a later Bind against it has a portal to look up.
        sendParse(out, "stmt1", "SELECT 1");
        assertThat(readWireMessage(in).type()).as("ParseComplete for a healthy Parse").isEqualTo('1');

        // 3. A malformed statement aborts the transaction: real Postgres reports status 'E'.
        sendSimpleQuery(out, "SELEC 1");
        assertThat(readyForQueryStatusOf(readUntilReadyForQuery(in))).isEqualTo('E');

        // 4. Bind against the already-prepared statement while aborted: this must be refused with an
        // ErrorResponse (SQLSTATE 25P02), not silently accepted. Before the fix, bindCommand() wrote nothing
        // at all for this message - the read below would block until the timeout wrapping this lambda fired.
        sendBind(out, "portal1", "stmt1");
        final WireMessage response = readWireMessage(in);
        assertThat(response.type()).as("an ErrorResponse, not BindComplete ('2') nor silence").isEqualTo('E');
        final Map<Character, String> fields = errorFields(response);
        assertThat(fields.get('C')).as("SQLSTATE 25P02 in_failed_sql_transaction").isEqualTo("25P02");
        assertThat(fields.get('M')).contains("current transaction is aborted");

        // 5. The session must still be aborted: Sync rolls the doomed transaction back but does not end the
        // block, so the status it reports is 'E' until the client's own COMMIT/ROLLBACK/END (issue #7851).
        sendSync(out);
        assertThat(readyForQueryStatusOf(readUntilReadyForQuery(in)))
            .as("the connection is still aborted after Sync").isEqualTo('E');

        // 6. The session is fully usable again after an explicit ROLLBACK.
        sendSimpleQuery(out, "ROLLBACK");
        readUntilReadyForQuery(in);
        sendSimpleQuery(out, "SELECT 1");
        final List<WireMessage> afterRecovery = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(afterRecovery)).as("a normal query answers normally again").doesNotContain('E');
        assertThat(readyForQueryStatusOf(afterRecovery)).isEqualTo('I');
      });
    }
  }

  /*
   * Regression tests for issue #6548, a follow-up from the review on PR #6546 (fix for #6543):
   * {@link PostgresNetworkExecutor#parseCommand()}, {@code bindCommand()}, and {@code executeCommand()} (the
   * extended query protocol - Parse/Bind/Execute) all started with an unconditional
   * <pre>{@code
   * if (errorInTransaction)
   *   return;
   * }</pre>
   * before the query text was ever inspected. Once {@code errorInTransaction} was set - a statement erred
   * inside an explicit {@code BEGIN} block - a client sending an explicit {@code ROLLBACK} (or {@code COMMIT}/
   * {@code END}, which real Postgres treats identically while aborted) over the extended protocol to recover
   * never reached the BEGIN/COMMIT/ROLLBACK dispatch #6546 added to the simple-query path: {@code Parse} just
   * silently dropped the message.
   * <p>
   * When {@code Sync} eventually arrived, its {@code errorInTransaction} branch called {@code
   * database.rollback()} and cleared {@code errorInTransaction}, but never touched {@code
   * explicitTransactionStarted}. Since {@code writeReadyForQueryMessage()} checks {@code errorInTransaction}
   * first and {@code explicitTransactionStarted} second, the client ended up back at {@code ReadyForQuery}
   * status {@code 'T'} instead of {@code 'I'} - the same "wedged forever" symptom #6543 fixed, just reached via
   * the aborted-transaction path instead of a plain {@code ROLLBACK} with no prior error.
   * <p>
   * These tests speak the wire protocol directly over a raw socket so the exact sequence - a malformed
   * statement that aborts an explicit transaction, followed by a recovery statement sent as its own
   * Parse/Bind/Execute/Sync round trip - can be driven and the resulting {@code ReadyForQuery} status byte
   * asserted on.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  // Regression: #6548
  @Test
  @DisplayName("[#6548] ROLLBACK sent as its own Parse/Bind/Execute while aborted clears both errorInTransaction and explicitTransactionStarted")
  void rollbackRecoversFromAbortedTransactionOverExtendedProtocol() throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
        // 1. BEGIN over the extended query protocol: status must go to 'T'.
        assertThat(readyForQueryStatusOf(runExtendedQuery(out, in, "BEGIN"))).isEqualTo('T');

        // 2. A malformed statement, sent as its own Parse, aborts the transaction without ever reaching
        // Sync - errorInTransaction is set synchronously inside parseCommand()'s own catch block.
        sendParse(out, "", "SELEC 1");
        final WireMessage parseError = readWireMessage(in);
        assertThat(parseError.type()).as("ErrorResponse for the malformed statement").isEqualTo('E');

        // 3. ROLLBACK sent as its own Parse/Bind/Execute/Sync must recover the session: no ErrorResponse
        // anywhere in the round trip, and status must be 'I' - not 'T' (explicitTransactionStarted must be
        // cleared, not just errorInTransaction) and not 'E' (the transaction must actually be resolved).
        final List<WireMessage> afterRollback = runExtendedQuery(out, in, "ROLLBACK");
        assertThat(messageTypesOf(afterRollback)).as("no ErrorResponse for the recovering ROLLBACK").doesNotContain('E');
        assertThat(readyForQueryStatusOf(afterRollback))
            .as("ROLLBACK must clear both errorInTransaction and explicitTransactionStarted").isEqualTo('I');

        // 4. The session is fully usable again, not wedged in a permanent aborted/explicit-transaction state.
        final List<WireMessage> afterRecovery = runExtendedQuery(out, in, "SELECT 1");
        assertThat(messageTypesOf(afterRecovery)).doesNotContain('E');
        assertThat(readyForQueryStatusOf(afterRecovery)).isEqualTo('I');
      });
    }
  }

  // Regression: #6548
  @Test
  @DisplayName("[#6548] a trailing ';' or surrounding whitespace on ROLLBACK does not defeat the aborted-transaction recovery match")
  void rollbackWithTrailingSemicolonOrWhitespaceRecoversFromAbortedTransaction() throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
        // isCommitStatement()/isRollbackStatement() match by exact string equality, so the aborted-branch
        // dispatch must trim and strip a trailing ';' before comparing - the same normalization
        // queryCommand() applies to its own queryText - or a driver that appends the statement terminator
        // (many do) would fall through to the silent return and reproduce this issue's "wedged forever"
        // symptom via a trailing semicolon instead of via the missing dispatch.
        for (final String rollbackVariant : new String[] {"ROLLBACK;", " ROLLBACK ", " ROLLBACK; "}) {
          assertThat(readyForQueryStatusOf(runExtendedQuery(out, in, "BEGIN"))).isEqualTo('T');

          sendParse(out, "", "SELEC 1");
          assertThat(readWireMessage(in).type()).isEqualTo('E');

          final List<WireMessage> afterRollback = runExtendedQuery(out, in, rollbackVariant);
          assertThat(messageTypesOf(afterRollback))
              .as("'" + rollbackVariant + "' must not itself error").doesNotContain('E');
          assertThat(readyForQueryStatusOf(afterRollback))
              .as("'" + rollbackVariant + "' must clear both errorInTransaction and explicitTransactionStarted")
              .isEqualTo('I');
        }
      });
    }
  }

  // Regression: #6548
  @Test
  @DisplayName("[#6548] COMMIT/END sent while aborted over the extended query protocol recover the session and report the ROLLBACK tag")
  void commitAndEndRecoverFromAbortedTransactionOverExtendedProtocol() throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
        for (final String recoveryStatement : new String[] {"COMMIT", "END"}) {
          assertThat(readyForQueryStatusOf(runExtendedQuery(out, in, "BEGIN"))).isEqualTo('T');

          sendParse(out, "", "SELEC 1");
          assertThat(readWireMessage(in).type()).isEqualTo('E');

          // Real Postgres treats a COMMIT/END of an aborted transaction the same as ROLLBACK - there is
          // nothing left to commit - so the command tag must read ROLLBACK regardless of which keyword the
          // client sent, exactly like queryCommand()'s simple-query dispatch already does.
          final List<WireMessage> afterRecoveryAttempt = runExtendedQuery(out, in, recoveryStatement);
          assertThat(messageTypesOf(afterRecoveryAttempt))
              .as(recoveryStatement + " must not itself error").doesNotContain('E');
          assertThat(readyForQueryStatusOf(afterRecoveryAttempt))
              .as(recoveryStatement + " must clear both errorInTransaction and explicitTransactionStarted")
              .isEqualTo('I');
          assertThat(commandCompleteTagOf(afterRecoveryAttempt))
              .as(recoveryStatement + " while aborted must report the ROLLBACK tag, not " + recoveryStatement)
              .isEqualTo("ROLLBACK");
        }
      });
    }
  }

  /**
   * Runs one statement as its own Parse+Bind+Execute+Sync round trip over the extended query protocol (the
   * unnamed statement/portal, no parameters), then reads every message the server sends back up to and
   * including the {@code ReadyForQuery} the Sync produces.
   */
  private static List<WireMessage> runExtendedQuery(final DataOutputStream out, final DataInputStream in, final String query)
      throws Exception {
    sendParse(out, "", query);
    sendBind(out, "", "");
    sendExecute(out, "");
    sendSync(out);
    return readUntilReadyForQuery(in);
  }

  /*
   * Regression test for issue #7851: {@code syncCommand()}'s error-recovery branch rolled the engine transaction
   * back and cleared {@code errorInTransaction}, but left {@code explicitTransactionStarted} set. The client was
   * then told {@code 'T'} - still inside a healthy transaction - instead of {@code 'E'}, and the SQLSTATE
   * {@code 25P02} guards that key off {@code errorInTransaction} were bypassed, so each further statement ran and
   * auto-committed on its own. A client grouping several statements into one transaction, and expecting to be
   * able to roll the whole group back, got some of them silently persisted.
   * <p>
   * Real PostgreSQL keeps the session in state {@code E} until the client explicitly ends the transaction block
   * with {@code COMMIT}/{@code ROLLBACK}/{@code END}; Sync itself never ends an aborted transaction. This is the
   * explicit-transaction counterpart of the #7775 tests in {@link PostgresPipelineIT}, which covers the autocommit case.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  private static final String TYPE_NAME  = "Issue7851Aborted";
  private static final String BLOCK_TYPE  = "Issue7851Block";
  private static final String PARSED_TYPE = "Issue7851Parsed";
  private static final String ACKED_TYPE  = "Issue7851Acked";
  private static final String REPLAY_TYPE = "Issue7851Replay";

  // Regression: #7851
  @Test
  @DisplayName("[#7851] Sync after an error inside an explicit transaction reports 'E' and refuses further statements")
  void syncDoesNotEndTheAbortedTransactionBlock() throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
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

  // Regression: #7851
  @Test
  @DisplayName("[#7851] after Sync, an Execute in the aborted block is refused with 25P02 and a Close still completes")
  void executeIsRefusedAndCloseStillCompletesAfterSync() throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
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

  // Regression: #7851
  @Test
  @DisplayName("[#7851] a BEGIN sent over the extended protocol opens a real transaction that ROLLBACK discards")
  void anExtendedProtocolBeginIsARealTransactionBlock() throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
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

  // Regression: #7851
  @Test
  @DisplayName("[#7851] a COMMIT that was only parsed, never executed, does not make the next Sync commit")
  void transactionControlTakesEffectAtExecuteNotAtParse() throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        sendSimpleQuery(out, "CREATE DOCUMENT TYPE " + PARSED_TYPE + " IF NOT EXISTS");
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).doesNotContain('E');

        sendSimpleQuery(out, "BEGIN");
        assertThat(readyForQueryStatusOf(readUntilReadyForQuery(in))).isEqualTo('T');

        runExtendedStatement(out, "ins", "INSERT INTO " + PARSED_TYPE + " SET id = 1");
        assertThat(readyForQueryStatusOf(readUntilReadyForQuery(in))).isEqualTo('T');

        // Parse alone, with no Bind and no Execute, then Sync - which is how pgjdbc and psycopg3 prepare a
        // statement ahead of using it. Parse prepares; it does not run. Applying the keyword there made this
        // Sync take its implicit-commit branch and persist the row above, for a COMMIT the client never ran.
        sendParse(out, "pendingCommit", "COMMIT");
        assertThat(readWireMessage(in).type()).as("ParseComplete for the prepared COMMIT").isEqualTo('1');
        sendSync(out);
        assertThat(readyForQueryStatusOf(readUntilReadyForQuery(in)))
            .as("a COMMIT that was only prepared leaves the transaction open").isEqualTo('T');

        sendSimpleQuery(out, "ROLLBACK");
        assertThat(readyForQueryStatusOf(readUntilReadyForQuery(in))).isEqualTo('I');
      });
    }

    assertThat(getServerDatabase(0, getDatabaseName()).countType(PARSED_TYPE, true))
        .as("the row was written inside a block the client rolled back, so nothing may remain")
        .isZero();
  }

  // Regression: #7851
  @Test
  @DisplayName("[#7851] a COMMIT acknowledged at Execute survives a later failure in the same pipeline")
  void anAcknowledgedCommitCannotBeTakenBackByALaterFailure() throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        sendSimpleQuery(out, "CREATE DOCUMENT TYPE " + ACKED_TYPE + " IF NOT EXISTS");
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).doesNotContain('E');

        // One pipeline: a whole explicit block, and then a statement that fails - all before the Sync. Execute
        // answers CommandComplete("COMMIT"), so the client is told the block is durable; leaving the actual
        // commit to the Sync meant the failure below made that Sync roll it back instead, losing data the
        // client had already been told was safe.
        sendParse(out, "b", "BEGIN");
        sendBind(out, "b", "b");
        sendExecute(out, "b");
        sendParse(out, "i", "INSERT INTO " + ACKED_TYPE + " SET id = 1");
        sendBind(out, "i", "i");
        sendExecute(out, "i");
        sendParse(out, "c", "COMMIT");
        sendBind(out, "c", "c");
        sendExecute(out, "c");
        sendParse(out, "bad", "SELEC 1");
        sendSync(out);

        final List<WireMessage> answers = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(answers)).as("the failing statement is still reported").contains('E');
        assertThat(readyForQueryStatusOf(answers))
            .as("the block was ended by the client's own COMMIT, so the failure leaves the session idle")
            .isEqualTo('I');
      });
    }

    assertThat(getServerDatabase(0, getDatabaseName()).countType(ACKED_TYPE, true))
        .as("a COMMIT the client was told succeeded cannot be taken back by what came after it")
        .isEqualTo(1);
  }

  // Regression: #7851
  @Test
  @DisplayName("[#7851] a ROLLBACK portal cannot be replayed into a later transaction")
  void aTransactionControlPortalCannotBeReplayedIntoALaterBlock() throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        sendSimpleQuery(out, "CREATE DOCUMENT TYPE " + REPLAY_TYPE + " IF NOT EXISTS");
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).doesNotContain('E');

        // First block: opened, written to, and rolled back through a NAMED portal, which outlives its Execute -
        // a portal is kept so a limit-hit Execute can be resumed through it (issue #6458) - but not the block it
        // was bound in (issue #8212).
        runExtendedStatement(out, "b1", "BEGIN");
        readUntilReadyForQuery(in);
        runExtendedStatement(out, "w1", "INSERT INTO " + REPLAY_TYPE + " SET id = 1");
        readUntilReadyForQuery(in);
        sendParse(out, "rb", "ROLLBACK");
        sendBind(out, "rb", "rb");
        sendExecute(out, "rb");
        sendSync(out);
        assertThat(readyForQueryStatusOf(readUntilReadyForQuery(in))).isEqualTo('I');

        // Second block, then an Execute of that SAME portal name with no new Bind. The ROLLBACK it carried must
        // never be re-applied to a block the client never asked to discard: the portal ended with the block its
        // own ROLLBACK closed, so the Execute is refused with 34000 - which, as in PostgreSQL, aborts the block it
        // arrived in.
        runExtendedStatement(out, "b2", "BEGIN");
        readUntilReadyForQuery(in);
        runExtendedStatement(out, "w2", "INSERT INTO " + REPLAY_TYPE + " SET id = 2");
        readUntilReadyForQuery(in);
        sendExecute(out, "rb");
        sendSync(out);
        final List<WireMessage> replay = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(replay)).containsExactly('E', 'Z');
        assertThat(errorFields(replay.getFirst()).get('C')).as("portal does not exist").isEqualTo("34000");
        assertThat(readyForQueryStatusOf(replay)).as("the refusal aborts the second block").isEqualTo('E');

        sendSimpleQuery(out, "ROLLBACK");
        assertThat(readyForQueryStatusOf(readUntilReadyForQuery(in))).isEqualTo('I');
      });
    }

    assertThat(getServerDatabase(0, getDatabaseName()).countType(REPLAY_TYPE, true))
        .as("both blocks were rolled back, and the first one's portal ran only once").isZero();
  }

  // Regression: #7851
  @Test
  @DisplayName("[#7851] a statement that runs nothing opens no transaction for Sync to commit empty")
  void aPortalThatRunsNothingOpensNoTransaction() throws Exception {
    final Database database = getServerDatabase(0, getDatabaseName());
    // A transaction that changed nothing is counted here when it commits, so a stray empty block opened for a
    // statement that executes nothing shows up as this counter moving - the only way the cost is observable from
    // outside, since the transaction itself lives on the connection's own thread.
    final long emptyTransactionsBefore = (Long) database.getStats().get("readTx");

    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        // A bare COMMIT and a bare ROLLBACK with no block open, and a SHOW whose answer Parse computed in full:
        // three portals whose Execute runs nothing at all.
        for (final String query : new String[] { "COMMIT", "ROLLBACK", "SHOW datestyle" }) {
          final List<WireMessage> answers = runAndRead(out, in, "n" + query.hashCode(), query);
          assertThat(messageTypesOf(answers)).as(query + " answers normally").doesNotContain('E');
          assertThat(readyForQueryStatusOf(answers)).isEqualTo('I');
        }
      });
    }

    assertThat((Long) database.getStats().get("readTx") - emptyTransactionsBefore)
        .as("none of the three opened a transaction: there was nothing for one to hold")
        .isZero();
  }

  private static void runExtendedStatement(final DataOutputStream out, final String name, final String query) throws Exception {
    sendParse(out, name, query);
    sendBind(out, name, name);
    sendExecute(out, name);
    sendSync(out);
  }

  /*
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
  private static final String ROLLBACK_TYPE = "Issue8029Rollback";
  private static final String COMMIT_TYPE   = "Issue8029Commit";

  // Regression: #8029
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

  // Regression: #8029
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

  // Regression: #8029
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

  // Regression: #8029
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

  // Regression: #8029
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

  // Regression: #8029
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

  /*
   * Regression tests for issue #8030: {@code parseCommand()}'s aborted-block recovery branch (issue #6548)
   * registered the client's {@code COMMIT}/{@code END}/{@code ROLLBACK} as a prepared statement so the Bind and
   * Execute pipelined behind the Parse had something to bind to, and marked it {@code ignoreExecution} so its
   * {@code Describe('P')} would answer {@code NoData} (issue #7905) - but never set
   * {@code portal.transactionControl}, which the healthy Parse branch sets for the same three keywords.
   * <p>
   * The recovery itself worked, because that branch rolls the engine transaction back on the spot. The
   * PREPARED STATEMENT it left behind did not: every later Bind of it produced a portal carrying no
   * transaction-control marker, so {@code applyTransactionControl()} returned false, {@code executeCommand()}'s
   * {@code ignoreExecution} arm answered {@code CommandComplete ROLLBACK} anyway, and the open transaction was
   * left open with {@code explicitTransactionStarted} still set - {@code ReadyForQuery} even kept reporting
   * {@code 'T'} after the client had been told its ROLLBACK succeeded, and whatever COMMIT came next persisted
   * the writes the client believed it had discarded.
   * <p>
   * Reusing a named prepared statement without re-parsing it is the normal shape for pgjdbc and psycopg3 once a
   * statement is promoted into their caches, which is why these tests speak the wire protocol directly: they
   * Bind and Execute the SAME statement name a second time with no Parse in between.
   * <p>
   * Note the deliberate boundary with the #7851 test
   * {@code aRetainedTransactionControlPortalAppliesOnlyOnce}: re-Executing an already-bound PORTAL must stay a
   * no-op ({@code applyTransactionControl()} clears the marker on the portal it acted on), while a new Bind of
   * the prepared STATEMENT copies the marker afresh out of the template and applies it again. The last test
   * here pins that boundary for the recovery-registered statement specifically.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  private static final String REUSE_TYPE  = "Issue8030Reuse";
  private static final String REBIND_TYPE = "Issue8030Rebind";
  private static final String RECOVERY_COMMIT_TYPE = "Issue8030Commit";
  private static final String RECOVERY_REPLAY_TYPE = "Issue8030Replay";

  // Regression: #8030
  @Test
  @DisplayName("[#8030] a ROLLBACK registered by the aborted-block recovery really rolls back when it is bound and executed again")
  void reusedRecoveryRollbackRollsBackTheNextBlock() throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        sendSimpleQuery(out, "CREATE DOCUMENT TYPE " + REUSE_TYPE + " IF NOT EXISTS");
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).doesNotContain('E');

        abortAnExplicitTransaction(out, in);

        // The recovery round trip, exactly as the #6548 tests drive it but through a NAMED statement: the
        // Parse recovers the block on the spot and registers "rb" for the Bind pipelined behind it.
        sendParse(out, "rb", "ROLLBACK");
        sendBind(out, "rbp", "rb");
        sendExecute(out, "rbp");
        sendSync(out);
        final List<WireMessage> recovery = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(recovery)).as("the recovering ROLLBACK must not error").doesNotContain('E');
        assertThat(readyForQueryStatusOf(recovery)).as("the aborted block is over").isEqualTo('I');

        // A fresh explicit transaction with one pending write in it.
        runAndRead(out, in, "b2", "BEGIN");
        runAndRead(out, in, "w2", "INSERT INTO " + REUSE_TYPE + " SET id = 100");

        // THE BUG: the client's statement cache re-Binds "rb" rather than re-Parsing it. Before the fix the
        // portal carried no transaction-control marker, so this answered CommandComplete ROLLBACK, rolled
        // nothing back, and left ReadyForQuery reporting 'T'.
        sendBind(out, "rbp", "rb");
        sendExecute(out, "rbp");
        sendSync(out);
        final List<WireMessage> reuse = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(reuse)).as("the reused ROLLBACK must not error").doesNotContain('E');
        assertThat(commandCompleteTagOf(reuse)).as("the tag the client is given").isEqualTo("ROLLBACK");
        assertThat(readyForQueryStatusOf(reuse))
            .as("an acknowledged ROLLBACK must end the block, not report 'T'").isEqualTo('I');

        // Whatever the client sends next must not be able to persist the discarded write.
        assertThat(readyForQueryStatusOf(runAndRead(out, in, "c2", "COMMIT"))).isEqualTo('I');
      });
    }

    final Database database = getServerDatabase(0, getDatabaseName());
    assertThat(database.countType(REUSE_TYPE, true))
        .as("the write the client rolled back must not be committed by the COMMIT that followed").isZero();
  }

  // Regression: #8030
  @Test
  @DisplayName("[#8030] the recovery-registered statement rolls back when bound under a different portal name too")
  void recoveryRollbackReboundUnderAnotherPortalNameStillRollsBack() throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        sendSimpleQuery(out, "CREATE DOCUMENT TYPE " + REBIND_TYPE + " IF NOT EXISTS");
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).doesNotContain('E');

        abortAnExplicitTransaction(out, in);

        sendParse(out, "rb", "ROLLBACK");
        sendBind(out, "first", "rb");
        sendExecute(out, "first");
        sendSync(out);
        assertThat(readyForQueryStatusOf(readUntilReadyForQuery(in))).isEqualTo('I');

        runAndRead(out, in, "b2", "BEGIN");
        runAndRead(out, in, "w2", "INSERT INTO " + REBIND_TYPE + " SET id = 200");

        // PostgresPortal.bindFrom() copies the marker out of the template, so a SECOND portal name bound from
        // the same statement is an independent portal that carries the same ROLLBACK.
        sendBind(out, "second", "rb");
        sendExecute(out, "second");
        sendSync(out);
        final List<WireMessage> reuse = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(reuse)).doesNotContain('E');
        assertThat(readyForQueryStatusOf(reuse))
            .as("a second portal bound from the recovery statement must roll back too").isEqualTo('I');

        assertThat(readyForQueryStatusOf(runAndRead(out, in, "c2", "COMMIT"))).isEqualTo('I');
      });
    }

    assertThat(getServerDatabase(0, getDatabaseName()).countType(REBIND_TYPE, true))
        .as("the write the second portal rolled back must not be committed").isZero();
  }

  // Regression: #8030
  @Test
  @DisplayName("[#8030] a COMMIT or END that recovered an aborted block rolls back on reuse, matching the ROLLBACK tag it answers")
  void recoveryViaCommitOrEndAlsoRollsBackOnReuse() throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(60), () -> {
        sendSimpleQuery(out, "CREATE DOCUMENT TYPE " + RECOVERY_COMMIT_TYPE + " IF NOT EXISTS");
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).doesNotContain('E');

        int id = 300;
        // Both end keywords, and a trailing ';' on one of them: the recovery branch rewrites portal.query to
        // "ROLLBACK" for all of them, so the marker they carry has to be ROLLBACK too - a reuse that committed
        // instead would contradict the very tag the client is handed.
        for (final String recoveryStatement : new String[] { "COMMIT", "END", "COMMIT;" }) {
          final String statementName = "rec" + id;
          abortAnExplicitTransaction(out, in);

          sendParse(out, statementName, recoveryStatement);
          sendBind(out, statementName + "p", statementName);
          sendExecute(out, statementName + "p");
          sendSync(out);
          final List<WireMessage> recovery = readUntilReadyForQuery(in);
          assertThat(messageTypesOf(recovery)).as(recoveryStatement + " must not error").doesNotContain('E');
          assertThat(readyForQueryStatusOf(recovery)).isEqualTo('I');

          runAndRead(out, in, "b" + id, "BEGIN");
          runAndRead(out, in, "w" + id, "INSERT INTO " + RECOVERY_COMMIT_TYPE + " SET id = " + id);

          sendBind(out, statementName + "p", statementName);
          sendExecute(out, statementName + "p");
          sendSync(out);
          final List<WireMessage> reuse = readUntilReadyForQuery(in);
          assertThat(messageTypesOf(reuse)).as(recoveryStatement + " reused must not error").doesNotContain('E');
          assertThat(commandCompleteTagOf(reuse))
              .as(recoveryStatement + " recovered while aborted always answers the ROLLBACK tag").isEqualTo("ROLLBACK");
          assertThat(readyForQueryStatusOf(reuse))
              .as(recoveryStatement + " reused must end the block it says it ended").isEqualTo('I');

          assertThat(readyForQueryStatusOf(runAndRead(out, in, "c" + id, "COMMIT"))).isEqualTo('I');
          ++id;
        }
      });
    }

    assertThat(getServerDatabase(0, getDatabaseName()).countType(RECOVERY_COMMIT_TYPE, true))
        .as("none of the three reuses may leave its write behind to be committed").isZero();
  }

  // Regression: #8030
  @Test
  @DisplayName("[#8030] re-executing the already-bound recovery portal after its block ended is refused, never replayed")
  void replayingTheBoundRecoveryPortalAfterItsBlockEndedIsRefused() throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        sendSimpleQuery(out, "CREATE DOCUMENT TYPE " + RECOVERY_REPLAY_TYPE + " IF NOT EXISTS");
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).doesNotContain('E');

        abortAnExplicitTransaction(out, in);

        sendParse(out, "rb", "ROLLBACK");
        sendBind(out, "rbp", "rb");
        sendExecute(out, "rbp");
        sendSync(out);
        assertThat(readyForQueryStatusOf(readUntilReadyForQuery(in))).isEqualTo('I');

        runAndRead(out, in, "b2", "BEGIN");
        runAndRead(out, in, "w2", "INSERT INTO " + RECOVERY_REPLAY_TYPE + " SET id = 400");

        // No Bind this time: the portal "rbp" belonged to the block its own ROLLBACK ended, and a portal ends with
        // its transaction (issue #8212), so it cannot be replayed into the second block at all - the strongest form
        // of the boundary issue #7851 drew for a healthy ROLLBACK portal. As in PostgreSQL, the refusal aborts
        // the block it arrived in.
        sendExecute(out, "rbp");
        sendSync(out);
        final List<WireMessage> replay = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(replay)).containsExactly('E', 'Z');
        assertThat(errorFields(replay.getFirst()).get('C')).as("portal does not exist").isEqualTo("34000");
        assertThat(readyForQueryStatusOf(replay)).isEqualTo('E');

        final List<WireMessage> commit = runAndRead(out, in, "c2", "COMMIT");
        assertThat(commandCompleteTagOf(commit)).as("COMMIT of an aborted block rolls it back").isEqualTo("ROLLBACK");
        assertThat(readyForQueryStatusOf(commit)).isEqualTo('I');
      });
    }

    assertThat(getServerDatabase(0, getDatabaseName()).countType(RECOVERY_REPLAY_TYPE, true))
        .as("the block the refused replay aborted is rolled back, not committed").isZero();
  }

  /**
   * Opens an explicit transaction and aborts it with a statement the grammar refuses, leaving the session in
   * the aborted-block state {@code parseCommand()}'s recovery branch is reached from.
   */
  private static void abortAnExplicitTransaction(final DataOutputStream out, final DataInputStream in) throws Exception {
    assertThat(readyForQueryStatusOf(runAndRead(out, in, "beg" + System.nanoTime(), "BEGIN"))).isEqualTo('T');
    sendParse(out, "bad", "SELEC 1");
    assertThat(readWireMessage(in).type()).as("ErrorResponse for the malformed statement").isEqualTo('E');
  }

  /*
   * Regression tests for issue #8264: every failure arm of {@code queryCommand()} rolled back FIRST and sent the
   * ErrorResponse second, so a rollback that threw skipped the ErrorResponse and the client saw its query end with a bare
   * ReadyForQuery, the cause only in the server log; {@code syncCommand()}'s discard branch had the same unguarded
   * rollback. Also covers the correlated defect found while fixing it: a commit that fails at Sync - which is where
   * ArcadeDB checks unique keys - escaped {@code syncCommand()} and the client never got an answer at all.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  private static final String TYPE        = "Issue8264Probe";
  private static final String UNIQUE_TYPE = "Issue8264Unique";

  @AfterEach
  void clearHook() {
    PostgresNetworkExecutor.TEST_ROLLBACK_HOOK = null;
  }

  // Regression: #8264
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

  // Regression: #8264
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

  // Regression: #8264
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

  // Regression: #8264
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

  private void authenticate(final DataOutputStream out, final DataInputStream in) throws Exception {
    sendStartupMessage(out, "root", getDatabaseName());
    readMessage(in); // AuthenticationCleartextPassword
    sendPasswordMessage(out, DEFAULT_PASSWORD_FOR_TESTS);
    readMessageOfType(in, 'Z'); // drain AuthenticationOk/BackendKeyData/ParameterStatus.../ReadyForQuery
  }

  private Socket connect() throws Exception {
    final Socket socket = new Socket();
    socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
    return socket;
  }

  private static List<WireMessage> runAndRead(final DataOutputStream out, final DataInputStream in, final String name,
      final String query) throws Exception {
    sendParse(out, name, query);
    sendBind(out, name, name);
    sendExecute(out, name);
    sendSync(out);
    return readUntilReadyForQuery(in);
  }

  /**
   * Extracts the tag from the round trip's {@code CommandComplete} ('C') message, e.g. {@code "ROLLBACK"}.
   */
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
}
