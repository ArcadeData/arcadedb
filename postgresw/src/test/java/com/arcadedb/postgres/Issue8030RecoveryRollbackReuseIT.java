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
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;

import static com.arcadedb.postgres.PostgresWireMessages.WireMessage;
import static com.arcadedb.postgres.PostgresWireMessages.messageTypesOf;
import static com.arcadedb.postgres.PostgresWireMessages.readUntilReadyForQuery;
import static com.arcadedb.postgres.PostgresWireMessages.readWireMessage;
import static com.arcadedb.postgres.PostgresWireMessages.readyForQueryStatusOf;
import static com.arcadedb.postgres.PostgresWireMessages.sendBind;
import static com.arcadedb.postgres.PostgresWireMessages.sendExecute;
import static com.arcadedb.postgres.PostgresWireMessages.sendParse;
import static com.arcadedb.postgres.PostgresWireMessages.sendSimpleQuery;
import static com.arcadedb.postgres.PostgresWireMessages.sendSync;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
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
 * Note the deliberate boundary with {@link Issue7851SyncKeepsTransactionAbortedIT}'s
 * {@code aRetainedTransactionControlPortalAppliesOnlyOnce}: re-Executing an already-bound PORTAL must stay a
 * no-op ({@code applyTransactionControl()} clears the marker on the portal it acted on), while a new Bind of
 * the prepared STATEMENT copies the marker afresh out of the template and applies it again. The last test
 * here pins that boundary for the recovery-registered statement specifically.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8030RecoveryRollbackReuseIT extends PostgresWireProtocolTestBase {

  private static final String REUSE_TYPE  = "Issue8030Reuse";
  private static final String REBIND_TYPE = "Issue8030Rebind";
  private static final String COMMIT_TYPE = "Issue8030Commit";
  private static final String REPLAY_TYPE = "Issue8030Replay";

  @Test
  @DisplayName("[#8030] a ROLLBACK registered by the aborted-block recovery really rolls back when it is bound and executed again")
  void reusedRecoveryRollbackRollsBackTheNextBlock() throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", GlobalConfiguration.POSTGRES_PORT.getValueAsInteger()), 2000);
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

  @Test
  @DisplayName("[#8030] the recovery-registered statement rolls back when bound under a different portal name too")
  void recoveryRollbackReboundUnderAnotherPortalNameStillRollsBack() throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", GlobalConfiguration.POSTGRES_PORT.getValueAsInteger()), 2000);
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

  @Test
  @DisplayName("[#8030] a COMMIT or END that recovered an aborted block rolls back on reuse, matching the ROLLBACK tag it answers")
  void recoveryViaCommitOrEndAlsoRollsBackOnReuse() throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", GlobalConfiguration.POSTGRES_PORT.getValueAsInteger()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(60), () -> {
        sendSimpleQuery(out, "CREATE DOCUMENT TYPE " + COMMIT_TYPE + " IF NOT EXISTS");
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
          runAndRead(out, in, "w" + id, "INSERT INTO " + COMMIT_TYPE + " SET id = " + id);

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

    assertThat(getServerDatabase(0, getDatabaseName()).countType(COMMIT_TYPE, true))
        .as("none of the three reuses may leave its write behind to be committed").isZero();
  }

  @Test
  @DisplayName("[#8030] re-executing the already-bound recovery portal without a new Bind still applies only once")
  void replayingTheBoundRecoveryPortalAppliesOnlyOnce() throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", GlobalConfiguration.POSTGRES_PORT.getValueAsInteger()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        sendSimpleQuery(out, "CREATE DOCUMENT TYPE " + REPLAY_TYPE + " IF NOT EXISTS");
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).doesNotContain('E');

        abortAnExplicitTransaction(out, in);

        sendParse(out, "rb", "ROLLBACK");
        sendBind(out, "rbp", "rb");
        sendExecute(out, "rbp");
        sendSync(out);
        assertThat(readyForQueryStatusOf(readUntilReadyForQuery(in))).isEqualTo('I');

        runAndRead(out, in, "b2", "BEGIN");
        runAndRead(out, in, "w2", "INSERT INTO " + REPLAY_TYPE + " SET id = 400");

        // No Bind this time: the portal "rbp" is the one the recovery Execute already consumed the marker
        // from, and re-running it must discard nothing - the same boundary issue #7851 drew for a healthy
        // ROLLBACK portal.
        sendExecute(out, "rbp");
        sendSync(out);
        final List<WireMessage> replay = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(replay)).doesNotContain('E');
        assertThat(readyForQueryStatusOf(replay))
            .as("a replayed portal applies its transaction control once, so the second block is untouched")
            .isEqualTo('T');

        assertThat(readyForQueryStatusOf(runAndRead(out, in, "c2", "COMMIT"))).isEqualTo('I');
      });
    }

    assertThat(getServerDatabase(0, getDatabaseName()).countType(REPLAY_TYPE, true))
        .as("the replayed portal discarded nothing, so the block the client committed is committed").isEqualTo(1);
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

  private void authenticate(final DataOutputStream out, final DataInputStream in) throws Exception {
    sendStartupMessage(out, "root", getDatabaseName());
    readMessage(in); // AuthenticationCleartextPassword
    sendPasswordMessage(out, DEFAULT_PASSWORD_FOR_TESTS);
    readMessageOfType(in, 'Z'); // drain AuthenticationOk/BackendKeyData/ParameterStatus.../ReadyForQuery
  }
}
