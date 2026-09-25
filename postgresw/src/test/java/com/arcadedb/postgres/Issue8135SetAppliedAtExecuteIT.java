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
import java.time.Duration;
import java.util.List;

import static com.arcadedb.postgres.PostgresWireMessages.WireMessage;
import static com.arcadedb.postgres.PostgresWireMessages.messageTypesOf;
import static com.arcadedb.postgres.PostgresWireMessages.readUntilReadyForQuery;
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
 * Regression tests for issue #8135: on the extended query protocol, {@code SET} ran {@code setConfiguration()}
 * at Parse and only marked the portal {@code ignoreExecution}, so the side effect happened when the statement was
 * PREPARED rather than when it was EXECUTED - the rule {@code BEGIN}/{@code COMMIT}/{@code ROLLBACK} were moved to
 * by #6543/#7905. Two consequences:
 * <ul>
 *   <li>a {@code SET} that is only prepared (Parse, or Parse+Bind, then Sync) still changed the session;</li>
 *   <li>a cached {@code SET} bound and executed again with no Parse in between - what a pgjdbc/psycopg3 statement
 *   cache sends once the statement is promoted - answered {@code CommandComplete SET} and re-applied nothing.</li>
 * </ul>
 * A {@code SET} is session-scoped (issue #8217), so every test observes it through {@code SHOW} on the connection
 * that ran it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8135SetAppliedAtExecuteIT extends PostgresWireProtocolTestBase {

  private static final String PARAMETER   = "application_name";
  private static final String VALUE       = "issue8135";
  private static final String SET_VALUE   = "SET " + PARAMETER + " = '" + VALUE + "'";
  private static final String OTHER       = "other";
  private static final String REFUSED_SET = "SET server_version = '1'";

  @Test
  @DisplayName("[#8135] a SET that is only prepared - Parse, then Parse+Bind, each followed by Sync - does not change the session")
  void preparedButNotExecutedSetHasNoEffect() throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        // Parse and Sync: what a driver sends to prepare a statement ahead of using it.
        sendParse(out, "s", SET_VALUE);
        sendSync(out);
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).as("the Parse is accepted").containsExactly('1', 'Z');
        assertThat(show(out, in, PARAMETER)).as("preparing a SET must not apply it").isNotEqualTo(VALUE);

        // An explicit block keeps portal "p" alive across the Syncs below: in autocommit a Sync ends the implicit
        // transaction and the portal with it, as in PostgreSQL (issue #8212).
        sendSimpleQuery(out, "BEGIN");
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).containsExactly('C', 'Z');

        // Bound and described but still never executed.
        sendBind(out, "p", "s");
        sendDescribe(out, 'P', "p");
        sendSync(out);
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).doesNotContain('E');
        assertThat(show(out, in, PARAMETER)).as("binding and describing a SET must not apply it either").isNotEqualTo(VALUE);

        // The Execute is what applies it: this is what proves the two assertions above could have failed.
        sendExecute(out, "p");
        sendSync(out);
        final List<WireMessage> executed = readUntilReadyForQuery(in);
        // The ParameterStatus announces the application_name the SET changed (issue #8241)
        assertThat(messageTypesOf(executed)).containsExactly('C', 'S', 'Z');
        assertThat(show(out, in, PARAMETER)).as("executing the SET applies it").isEqualTo(VALUE);

        sendSimpleQuery(out, "COMMIT");
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).containsExactly('C', 'Z');
      });
    }
  }

  @Test
  @DisplayName("[#8135] a cached SET bound and executed again without a new Parse re-applies the setting")
  void reExecutedCachedSetIsReapplied() throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        sendParse(out, "s", SET_VALUE);
        sendBind(out, "p", "s");
        sendExecute(out, "p");
        sendSync(out);
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).doesNotContain('E');
        assertThat(show(out, in, PARAMETER)).isEqualTo(VALUE);

        // Something else changes the value the SET established.
        setOther(out, in);

        // The statement cache re-runs the prepared SET to restore it: Bind+Execute of the same name, no Parse.
        sendBind(out, "p", "s");
        sendExecute(out, "p");
        sendSync(out);
        final List<WireMessage> reuse = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(reuse)).as("the re-executed SET answers CommandComplete, and reports the application_name it restored")
            .containsExactly('2', 'C', 'S', 'Z');
        assertThat(show(out, in, PARAMETER)).as("a re-executed cached SET must apply the setting again, not only claim to")
            .isEqualTo(VALUE);
      });
    }
  }

  @Test
  @DisplayName("[#8135] re-running the same already-executed SET portal without a new Bind applies it only once")
  void replayingTheBoundSetPortalAppliesOnlyOnce() throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        // An explicit block keeps portal "p" alive across the Sync and the simple queries: in autocommit a Sync or a
        // simple Query ends the implicit transaction and the portal with it, as in PostgreSQL (issue #8212).
        sendSimpleQuery(out, "BEGIN");
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).containsExactly('C', 'Z');

        sendParse(out, "s", SET_VALUE);
        sendBind(out, "p", "s");
        sendExecute(out, "p");
        sendSync(out);
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).doesNotContain('E');
        assertThat(show(out, in, PARAMETER)).isEqualTo(VALUE);

        setOther(out, in);

        // No Bind: the same boundary applyTransactionControl() draws (#7851) - the marker belongs to the bound
        // portal and is consumed by its first Execute; only a new Bind of the statement carries it again.
        sendExecute(out, "p");
        sendSync(out);
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).containsExactly('C', 'Z');
        assertThat(show(out, in, PARAMETER)).as("a replayed portal applies its SET once").isEqualTo(OTHER);

        sendSimpleQuery(out, "COMMIT");
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).containsExactly('C', 'Z');
      });
    }
  }

  @Test
  @DisplayName("[#8135] a SET whose Execute is discarded by an earlier error in the same pipeline does not take effect")
  void setDiscardedBySkipUntilSyncHasNoEffect() throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        // The SET is parsed BEFORE the failing statement, so its Parse succeeds; its Bind and Execute come after
        // the error and are discarded up to the Sync, as PostgreSQL discards them.
        sendParse(out, "s", SET_VALUE);
        sendParse(out, "bad", "SELEC 1");
        sendBind(out, "p", "s");
        sendExecute(out, "p");
        sendSync(out);
        final List<WireMessage> pipeline = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(pipeline)).as("the malformed statement fails the pipeline").contains('E');
        assertThat(messageTypesOf(pipeline)).as("the discarded Execute answers nothing").doesNotContain('C');
        assertThat(show(out, in, PARAMETER)).as("a SET the client never got to execute must not have been applied")
            .isNotEqualTo(VALUE);
      });
    }
  }

  /**
   * Issue #8261: this test used to assert the retry's SQLSTATE was {@code 55P02} (refused again, same reason),
   * which was meant to prove {@code applyPendingSetting()}'s marker-clearing rule - cleared only AFTER the SET
   * applies, so a refused attempt leaves the marker standing and a replay is refused again rather than answered
   * {@code CommandComplete SET}. That specific ordering has no wire-observable form any more: in autocommit (this
   * test's shape) the Sync after the refused Execute ends the implicit transaction and drops the portal with it
   * (#8212), so the retry is answered {@code 34000} (portal missing) before it ever reaches the marker, and inside
   * an explicit block the refusal aborts the block with no way to recover it and retry the same portal, since
   * {@code ROLLBACK TO SAVEPOINT} is refused at Parse (#7846). The test still passes whether the marker is cleared
   * before or after the apply - only the weaker assertion below (an error either way, never a false success) is
   * still real and wire-observable, so that is what it now asserts and documents. The marker-clearing order itself
   * is pinned only by the javadoc on {@code applyPendingSetting()}, which explains why it cannot regress silently.
   */
  @Test
  @DisplayName("[#8135] a SET refused at Execute, then replayed, is never answered as CommandComplete")
  void refusedSetIsRefusedAgainOnReplay() throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        // server_version is read-only, in PostgreSQL as here (issue #8217): the SET parses, and is refused at Execute.
        sendParse(out, "s", REFUSED_SET);
        sendBind(out, "p", "s");
        sendExecute(out, "p");
        sendSync(out);
        final List<WireMessage> refused = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(refused)).as("the SET is refused at Execute").contains('E').doesNotContain('C');
        assertThat(sqlStateOf(refused)).as("cant_change_runtime_param, as PostgreSQL answers it").isEqualTo("55P02");

        // Recovered by the Sync, the client retries the SAME bound portal - no new Bind. Since #8212 the Sync that
        // ended the implicit transaction also dropped the portal, so this is answered 34000 (portal missing), not
        // 55P02 - see this method's javadoc. Either way it must not answer "CommandComplete SET" having applied
        // nothing.
        sendExecute(out, "p");
        sendSync(out);
        final List<WireMessage> retried = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(retried)).as("the replay is answered as an error, not reported as applied")
            .contains('E').doesNotContain('C');

        assertThat(show(out, in, "server_version")).as("a refused SET changes nothing")
            .isEqualTo(PostgresNetworkExecutor.PG_SERVER_VERSION);
      });
    }
  }

  private static void setOther(final DataOutputStream out, final DataInputStream in) throws Exception {
    sendSimpleQuery(out, "SET " + PARAMETER + " = '" + OTHER + "'");
    assertThat(messageTypesOf(readUntilReadyForQuery(in))).doesNotContain('E');
    assertThat(show(out, in, PARAMETER)).isEqualTo(OTHER);
  }

  private Socket connect() throws Exception {
    final Socket socket = new Socket();
    socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
    return socket;
  }

  private void authenticate(final DataOutputStream out, final DataInputStream in) throws Exception {
    sendStartupMessage(out, "root", getDatabaseName());
    readMessage(in); // AuthenticationCleartextPassword
    sendPasswordMessage(out, DEFAULT_PASSWORD_FOR_TESTS);
    readMessageOfType(in, 'Z'); // drain AuthenticationOk/BackendKeyData/ParameterStatus.../ReadyForQuery
  }
}
