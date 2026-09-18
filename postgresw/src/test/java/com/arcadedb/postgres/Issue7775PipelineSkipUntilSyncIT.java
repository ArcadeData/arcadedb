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
 * Regression test for issue #7775: in autocommit - the mode every JDBC / psycopg / asyncpg client is in unless
 * it explicitly opened a transaction - an ErrorResponse in the middle of an extended-protocol pipeline left the
 * connection fully live. The single {@code errorInTransaction} flag that drives both "discard messages until
 * Sync" and Sync's rollback-vs-commit decision was raised only inside an explicit {@code BEGIN} block, so:
 * <ol>
 *   <li>the Parse/Bind/Describe/Execute messages that followed the failing one were executed and answered,
 *       instead of being discarded until Sync as the protocol requires, and</li>
 *   <li>Sync took its implicit-commit arm and <b>persisted</b> the whole run - the classic JDBC
 *       {@code executeBatch()} shape, where callers assume a failed batch left nothing behind.</li>
 * </ol>
 * PostgreSQL treats a run of extended-protocol messages terminated by one Sync as an implicit transaction
 * block and rolls the WHOLE block back when anything in it fails.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7775PipelineSkipUntilSyncIT extends PostgresWireProtocolTestBase {

  private void authenticate(final DataOutputStream out, final DataInputStream in) throws Exception {
    sendStartupMessage(out, "root", getDatabaseName());
    readMessage(in); // AuthenticationCleartextPassword
    sendPasswordMessage(out, DEFAULT_PASSWORD_FOR_TESTS);
    readMessageOfType(in, 'Z'); // drain AuthenticationOk/BackendKeyData/ParameterStatus.../ReadyForQuery
  }

  private static final String TYPE_NAME   = "Issue7775Pipe";
  private static final String SINGLE_TYPE = "Issue7775Single";

  @Test
  @DisplayName("[#7775] a failed autocommit pipeline discards the messages after the error and persists nothing")
  void failedAutocommitPipelineDiscardsTheRestAndRollsTheImplicitBlockBack() throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", GlobalConfiguration.POSTGRES_PORT.getValueAsInteger()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        sendSimpleQuery(out, "CREATE DOCUMENT TYPE " + TYPE_NAME + " IF NOT EXISTS");
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).doesNotContain('E');

        // One pipeline, no explicit BEGIN, one Sync at the end: the implicit transaction block.
        parseBindDescribeExecute(out, "s1", "INSERT INTO " + TYPE_NAME + " SET id = 1");
        parseBindDescribeExecute(out, "s2", "SELECT FROM Issue7775NoSuchType");
        parseBindDescribeExecute(out, "s3", "INSERT INTO " + TYPE_NAME + " SET id = 3");
        sendSync(out);

        final List<WireMessage> answers = readUntilReadyForQuery(in);
        final List<Character> types = messageTypesOf(answers);

        // Exactly one ErrorResponse, and nothing but the ReadyForQuery after it: everything s3 asked for was
        // discarded. Before the fix the trace continued "1 2 T D C" - s3 was parsed, bound, described, returned
        // a row and completed.
        assertThat(types).as("the failing statement is answered with exactly one ErrorResponse")
            .containsOnlyOnce('E');
        assertThat(types.subList(types.indexOf('E') + 1, types.size()))
            .as("every message after the ErrorResponse is discarded until Sync")
            .containsExactly('Z');

        assertThat(errorFields(answers.get(types.indexOf('E'))).get('C'))
            .as("SQLSTATE 42P01 undefined_table").isEqualTo("42P01");

        // Autocommit, so the session is idle again once the Sync has discarded the block - not aborted: only an
        // explicit BEGIN puts the session in the 'E' state that lasts until COMMIT/ROLLBACK/END.
        assertThat(readyForQueryStatusOf(answers)).isEqualTo('I');

        // And the connection really is usable again: the next pipeline runs and commits normally.
        parseBindDescribeExecute(out, "s4", "INSERT INTO " + TYPE_NAME + " SET id = 4");
        sendSync(out);
        final List<WireMessage> afterRecovery = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(afterRecovery)).as("skip-until-Sync is cleared by the Sync").doesNotContain('E');
        assertThat(readyForQueryStatusOf(afterRecovery)).isEqualTo('I');
      });
    }

    final Database database = getServerDatabase(0, getDatabaseName());
    assertThat(database.countType(TYPE_NAME, true))
        .as("the failed pipeline persists nothing; only the pipeline that succeeded afterwards does")
        .isEqualTo(1);
    assertThat(database.query("sql", "SELECT id FROM " + TYPE_NAME).next().<Integer>getProperty("id"))
        .as("the row that survived is the one inserted by the pipeline that did not fail")
        .isEqualTo(4);
  }

  @Test
  @DisplayName("[#7775] one statement per Sync - the ordinary JDBC shape - is unaffected by the implicit block")
  void aSingleStatementPerSyncStillCommitsAndReads() throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", GlobalConfiguration.POSTGRES_PORT.getValueAsInteger()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        sendSimpleQuery(out, "CREATE DOCUMENT TYPE " + SINGLE_TYPE + " IF NOT EXISTS");
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).doesNotContain('E');

        // A write with its own Sync: the block opens and closes around this one statement, and it commits -
        // pgjdbc's non-batch path, which is the overwhelming majority of extended-protocol write traffic.
        parseBindDescribeExecute(out, "w", "INSERT INTO " + SINGLE_TYPE + " SET id = 7");
        sendSync(out);
        final List<WireMessage> afterWrite = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(afterWrite)).doesNotContain('E');
        assertThat(readyForQueryStatusOf(afterWrite)).isEqualTo('I');

        // A read with its own Sync: it opens no block at all - a read has nothing to roll back - and still sees
        // what the write above committed.
        parseBindDescribeExecute(out, "r", "SELECT id FROM " + SINGLE_TYPE);
        sendSync(out);
        final List<WireMessage> afterRead = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(afterRead)).as("a plain autocommit SELECT answers rows, not an error")
            .doesNotContain('E').contains('D');
        assertThat(readyForQueryStatusOf(afterRead)).isEqualTo('I');
      });
    }

    assertThat(getServerDatabase(0, getDatabaseName()).countType(SINGLE_TYPE, true))
        .as("a statement that has its own Sync commits, exactly as it did before the implicit block existed")
        .isEqualTo(1);
  }

  /**
   * The four messages a driver sends per statement when it pipelines a batch: one Sync terminates the run, not
   * each statement.
   */
  private static void parseBindDescribeExecute(final DataOutputStream out, final String name, final String query)
      throws Exception {
    sendParse(out, name, query);
    sendBind(out, name, name);
    sendDescribe(out, 'P', name);
    sendExecute(out, name);
  }
}
