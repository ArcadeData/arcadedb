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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
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
 * Extended-query pipelines (several Parse/Bind/Execute messages ahead of one Sync) over the PostgreSQL wire protocol:
 * after an error the server skips every message up to the Sync, a simple query that arrives during that skip is not
 * answered out of step, and a simple query that arrives while a pipeline is open commits what the pipeline wrote.
 */
class PostgresPipelineIT extends PostgresWireProtocolTestBase {

  /*
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
  private void authenticate(final DataOutputStream out, final DataInputStream in) throws Exception {
    sendStartupMessage(out, "root", getDatabaseName());
    readMessage(in); // AuthenticationCleartextPassword
    sendPasswordMessage(out, DEFAULT_PASSWORD_FOR_TESTS);
    readMessageOfType(in, 'Z'); // drain AuthenticationOk/BackendKeyData/ParameterStatus.../ReadyForQuery
  }

  private static final String TYPE_NAME   = "Issue7775Pipe";
  private static final String SINGLE_TYPE = "Issue7775Single";

  // Regression: #7775
  @Test
  @DisplayName("[#7775] a failed autocommit pipeline discards the messages after the error and persists nothing")
  void failedAutocommitPipelineDiscardsTheRestAndRollsTheImplicitBlockBack() throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
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

  // Regression: #7775
  @Test
  @DisplayName("[#7775] one statement per Sync - the ordinary JDBC shape - is unaffected by the implicit block")
  void aSingleStatementPerSyncStillCommitsAndReads() throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
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

  /*
   * Regression test for issue #8175: a failure inside an extended-protocol pipeline puts the session in the
   * "discard every message until Sync" state ({@code skipUntilSync}), but {@code queryCommand()} - the simple query
   * protocol's 'Q' handler - never looked at that state. A 'Q' message a client interleaved into the failed but not
   * yet synced pipeline therefore ran: a {@code COMMIT} persisted the implicit block PostgreSQL would have discarded
   * (since #8144 made that COMMIT really commit), and an ordinary statement executed and was acknowledged although
   * the block it ran in was about to be thrown away.
   * <p>
   * PostgreSQL's backend loop drops EVERY message but Sync (and Terminate) while {@code ignore_till_sync} is set,
   * 'Q' included: the message is read and discarded without any reply, so the client sees nothing for it and the
   * Sync that follows rolls the whole block back. Every test here therefore asserts that the interleaved 'Q' gets
   * no answer of its own, that the first ReadyForQuery the client reads is the Sync's, and that the stream is still
   * in step afterwards.
   *
   * @author Roberto Franchini (r.franchini@arcadedata.com)
   */
  private static final String COMMIT_TYPE    = "Issue8175Commit";
  private static final String ROLLBACK_TYPE  = "Issue8175Rollback";
  private static final String STATEMENT_TYPE = "Issue8175Statement";
  private static final String EXPLICIT_TYPE  = "Issue8175Explicit";

  // Regression: #8175
  @Test
  @DisplayName("[#8175] a simple-query COMMIT interleaved into a failed, unsynced pipeline is discarded, not committed")
  void simpleQueryCommitDuringSkipUntilSyncIsDiscarded() throws Exception {
    withConnection((out, in) -> {
      createType(out, in, COMMIT_TYPE);

      // The write opens the implicit block, the bogus Parse fails and raises skipUntilSync, and the 'Q' COMMIT
      // arrives before the client's Sync. Before the fix the COMMIT persisted the block and was answered at once.
      parseBindExecute(out, "w1", "INSERT INTO " + COMMIT_TYPE + " SET id = 100");
      sendParse(out, "bad", "SELEKT bogus syntax");
      sendSimpleQuery(out, "COMMIT");
      sendSync(out);

      final List<WireMessage> answers = readUntilReadyForQuery(in);
      assertThat(messageTypesOf(answers)).as("the failed Parse is answered with exactly one ErrorResponse").containsOnlyOnce('E');
      assertThat(commandTagsOf(answers)).as("the discarded COMMIT gets no CommandComplete of its own")
          .doesNotContain("COMMIT", "ROLLBACK");
      assertThat(readyForQueryStatusOf(answers)).as("the Sync ends the implicit block, so the session is idle").isEqualTo('I');

      assertStreamStillInStep(out, in);
    });

    assertThat(idsOf(COMMIT_TYPE)).as("the Sync discards the failed block; the interleaved COMMIT did not persist it").isEmpty();
  }

  // Regression: #8175
  @Test
  @DisplayName("[#8175] a simple-query ROLLBACK interleaved into a failed, unsynced pipeline is discarded too")
  void simpleQueryRollbackDuringSkipUntilSyncIsDiscarded() throws Exception {
    withConnection((out, in) -> {
      createType(out, in, ROLLBACK_TYPE);

      parseBindExecute(out, "w1", "INSERT INTO " + ROLLBACK_TYPE + " SET id = 200");
      sendParse(out, "bad", "SELEKT bogus syntax");
      sendSimpleQuery(out, "ROLLBACK");
      sendSync(out);

      final List<WireMessage> answers = readUntilReadyForQuery(in);
      assertThat(messageTypesOf(answers)).containsOnlyOnce('E');
      assertThat(commandTagsOf(answers)).as("the discarded ROLLBACK gets no CommandComplete of its own")
          .doesNotContain("ROLLBACK");
      assertThat(readyForQueryStatusOf(answers)).isEqualTo('I');

      assertStreamStillInStep(out, in);
    });

    assertThat(idsOf(ROLLBACK_TYPE)).isEmpty();
  }

  // Regression: #8175
  @Test
  @DisplayName("[#8175] an ordinary simple-query statement interleaved into a failed, unsynced pipeline never runs")
  void simpleQueryStatementDuringSkipUntilSyncNeverRuns() throws Exception {
    withConnection((out, in) -> {
      createType(out, in, STATEMENT_TYPE);

      // Before the fix this INSERT ran inside the doomed block and was acknowledged with "INSERT 0 1".
      parseBindExecute(out, "w1", "INSERT INTO " + STATEMENT_TYPE + " SET id = 300");
      sendParse(out, "bad", "SELEKT bogus syntax");
      sendSimpleQuery(out, "INSERT INTO " + STATEMENT_TYPE + " SET id = 301");
      sendSync(out);

      final List<WireMessage> answers = readUntilReadyForQuery(in);
      assertThat(messageTypesOf(answers)).containsOnlyOnce('E');
      assertThat(commandTagsOf(answers)).as("only the pipeline's own INSERT was executed and acknowledged")
          .containsOnlyOnce("INSERT 0 1");
      assertThat(readyForQueryStatusOf(answers)).isEqualTo('I');

      assertStreamStillInStep(out, in);
    });

    assertThat(idsOf(STATEMENT_TYPE)).as("neither the pipeline's write nor the discarded 'Q' write survives").isEmpty();
  }

  // Regression: #8175
  @Test
  @DisplayName("[#8175] inside an explicit block the interleaved 'Q' COMMIT is discarded and the block stays aborted past the Sync")
  void simpleQueryCommitDuringSkipUntilSyncInsideAnExplicitBlock() throws Exception {
    withConnection((out, in) -> {
      createType(out, in, EXPLICIT_TYPE);

      sendSimpleQuery(out, "BEGIN");
      assertThat(readyForQueryStatusOf(readUntilReadyForQuery(in))).isEqualTo('T');

      // Before the fix queryCommand()'s aborted branch answered this COMMIT with "ROLLBACK" and ended the block
      // before the Sync; PostgreSQL never sees the message, and the explicit block stays aborted after the Sync
      // until the client itself ends it (issue #7851).
      parseBindExecute(out, "w1", "INSERT INTO " + EXPLICIT_TYPE + " SET id = 400");
      sendParse(out, "bad", "SELEKT bogus syntax");
      sendSimpleQuery(out, "COMMIT");
      sendSync(out);

      final List<WireMessage> answers = readUntilReadyForQuery(in);
      assertThat(messageTypesOf(answers)).containsOnlyOnce('E');
      assertThat(commandTagsOf(answers)).as("the discarded COMMIT gets no CommandComplete of its own")
          .doesNotContain("COMMIT", "ROLLBACK");
      assertThat(readyForQueryStatusOf(answers)).as("the explicit block is still aborted after the Sync").isEqualTo('E');

      // Now outside skipUntilSync, the aborted block's own rules apply: the client's ROLLBACK ends it.
      sendSimpleQuery(out, "ROLLBACK");
      final List<WireMessage> afterRollback = readUntilReadyForQuery(in);
      assertThat(commandTagsOf(afterRollback)).containsExactly("ROLLBACK");
      assertThat(readyForQueryStatusOf(afterRollback)).isEqualTo('I');

      assertStreamStillInStep(out, in);
    });

    assertThat(idsOf(EXPLICIT_TYPE)).isEmpty();
  }

  /**
   * A 'Q' answered despite the skip would leave its own ReadyForQuery - or the Sync's - unread in the stream, and the
   * next round trip would read it instead of its own reply. A fresh simple query must get exactly its own answer.
   */
  private static void assertStreamStillInStep(final DataOutputStream out, final DataInputStream in) throws Exception {
    sendSimpleQuery(out, "SELECT 1 AS probe");
    final List<WireMessage> probe = readUntilReadyForQuery(in);
    assertThat(messageTypesOf(probe)).as("the probe gets its own RowDescription, DataRow and CommandComplete")
        .containsSequence('T', 'D', 'C', 'Z');
    assertThat(readyForQueryStatusOf(probe)).isEqualTo('I');
    assertThat(in.available()).as("nothing is left unread behind the probe's ReadyForQuery").isZero();
  }

  /*
   * Regression test for issue #8214: a simple query ('Q') interleaved into an extended-protocol pipeline that is still
   * open - no failure, no Sync yet - joined the pipeline's implicit transaction and was acknowledged without being
   * committed. A later failure in the same pipeline made the Sync discard the block, taking back a write the client had
   * already been told had completed.
   * <p>
   * PostgreSQL's {@code exec_simple_query()} ends with {@code finish_xact_command()}: outside an explicit BEGIN block a
   * 'Q' always ends the transaction it ran in, committing it when the statement succeeded - the pipeline's pending
   * writes included - and aborting it when the statement failed. The simple query protocol never raises
   * {@code ignore_till_sync}, so the rest of the pipeline then runs normally in a fresh transaction. Inside an explicit
   * BEGIN block the 'Q' joins the block and commits nothing.
   *
   * @author Roberto Franchini (r.franchini@arcadedata.com)
   */
  // Regression: #8214
  @Test
  @DisplayName("[#8214] a simple-query write interleaved into an open pipeline commits it, so a later failure cannot take it back")
  void simpleQueryWriteCommitsTheOpenPipeline() throws Exception {
    final String type = "Issue8214Write";
    withConnection((out, in) -> {
      createType(out, in, type);

      // The sequence of the issue: the pipeline's INSERT opens the implicit block, the 'Q' INSERT is acknowledged,
      // then a bogus Parse fails and the Sync takes its discard branch. Before the fix both rows were rolled back.
      parseBindExecute(out, "w1", "INSERT INTO " + type + " SET id = 1");
      sendSimpleQuery(out, "INSERT INTO " + type + " SET id = 2");
      final List<WireMessage> answers = readUntilReadyForQuery(in);
      assertThat(commandTagsOf(answers)).as("the pipeline's INSERT and the 'Q' INSERT").containsExactly("INSERT 0 1", "INSERT 0 1");
      assertThat(readyForQueryStatusOf(answers)).isEqualTo('I');

      failPipelineAndSync(out, in, 'I');
      assertProbeStillInStep(out, in);
    });

    assertThat(idsOf(type)).as("the 'Q' committed both writes before the pipeline failed").containsExactly(1, 2);
  }

  // Regression: #8214
  @Test
  @DisplayName("[#8214] a read-only simple query interleaved into an open pipeline commits it too")
  void simpleQueryReadCommitsTheOpenPipeline() throws Exception {
    final String type = "Issue8214Read";
    withConnection((out, in) -> {
      createType(out, in, type);

      parseBindExecute(out, "w1", "INSERT INTO " + type + " SET id = 10");
      sendSimpleQuery(out, "SELECT count(*) AS total FROM " + type);
      final List<WireMessage> answers = readUntilReadyForQuery(in);
      assertThat(messageTypesOf(answers)).doesNotContain('E');
      assertThat(readyForQueryStatusOf(answers)).isEqualTo('I');

      failPipelineAndSync(out, in, 'I');
      assertProbeStillInStep(out, in);
    });

    assertThat(idsOf(type)).containsExactly(10);
  }

  // Regression: #8214
  @Test
  @DisplayName("[#8214] an empty simple query interleaved into an open pipeline commits it too")
  void emptySimpleQueryCommitsTheOpenPipeline() throws Exception {
    final String type = "Issue8214Empty";
    withConnection((out, in) -> {
      createType(out, in, type);

      parseBindExecute(out, "w1", "INSERT INTO " + type + " SET id = 20");
      sendSimpleQuery(out, "");
      final List<WireMessage> answers = readUntilReadyForQuery(in);
      assertThat(messageTypesOf(answers)).as("the EmptyQueryResponse").contains('I').doesNotContain('E');
      assertThat(readyForQueryStatusOf(answers)).isEqualTo('I');

      failPipelineAndSync(out, in, 'I');
      assertProbeStillInStep(out, in);
    });

    assertThat(idsOf(type)).containsExactly(20);
  }

  // Regression: #8214
  @Test
  @DisplayName("[#8214] a simple-query COPY TO STDOUT interleaved into an open pipeline commits it too")
  void simpleQueryCopyCommitsTheOpenPipeline() throws Exception {
    final String type = "Issue8214Copy";
    withConnection((out, in) -> {
      createType(out, in, type);

      parseBindExecute(out, "w1", "INSERT INTO " + type + " SET id = 30");
      sendSimpleQuery(out, "COPY (SELECT id FROM " + type + ") TO STDOUT");
      final List<WireMessage> answers = readUntilReadyForQuery(in);
      assertThat(messageTypesOf(answers)).as("CopyOutResponse, CopyData, CopyDone").contains('H', 'd', 'c').doesNotContain('E');
      assertThat(commandTagsOf(answers)).contains("COPY 1");
      assertThat(readyForQueryStatusOf(answers)).isEqualTo('I');

      failPipelineAndSync(out, in, 'I');
      assertProbeStillInStep(out, in);
    });

    assertThat(idsOf(type)).containsExactly(30);
  }

  // Regression: #8214
  @Test
  @DisplayName("[#8214] a failed simple query interleaved into an open pipeline aborts it, and the rest of the pipeline runs in a new block")
  void failedSimpleQueryAbortsTheOpenPipeline() throws Exception {
    final String type = "Issue8214Failed";
    withConnection((out, in) -> {
      createType(out, in, type);

      // PostgreSQL aborts the transaction the failed 'Q' ran in - the pipeline's pending INSERT with it - and does not
      // enter ignore_till_sync, so the INSERT sent after it runs in a fresh block that the Sync commits.
      parseBindExecute(out, "w1", "INSERT INTO " + type + " SET id = 40");
      sendSimpleQuery(out, "SELEKT bogus syntax");
      final List<WireMessage> answers = readUntilReadyForQuery(in);
      assertThat(messageTypesOf(answers)).containsOnlyOnce('E');
      assertThat(readyForQueryStatusOf(answers)).isEqualTo('I');

      parseBindExecute(out, "w2", "INSERT INTO " + type + " SET id = 41");
      sendSync(out);
      final List<WireMessage> synced = readUntilReadyForQuery(in);
      assertThat(messageTypesOf(synced)).doesNotContain('E');
      assertThat(commandTagsOf(synced)).containsExactly("INSERT 0 1");
      assertThat(readyForQueryStatusOf(synced)).isEqualTo('I');

      assertProbeStillInStep(out, in);
    });

    assertThat(idsOf(type)).as("the failed 'Q' discarded the write before it, not the one after it").containsExactly(41);
  }

  // Regression: #8214
  @Test
  @DisplayName("[#8214] a refused ROLLBACK TO interleaved into an open pipeline aborts it")
  void refusedRollbackToAbortsTheOpenPipeline() throws Exception {
    final String type = "Issue8214RollbackTo";
    withConnection((out, in) -> {
      createType(out, in, type);

      parseBindExecute(out, "w1", "INSERT INTO " + type + " SET id = 50");
      sendSimpleQuery(out, "ROLLBACK TO sp1");
      final List<WireMessage> answers = readUntilReadyForQuery(in);
      assertThat(messageTypesOf(answers)).containsOnlyOnce('E');
      assertThat(readyForQueryStatusOf(answers)).isEqualTo('I');

      sendSync(out);
      assertThat(readyForQueryStatusOf(readUntilReadyForQuery(in))).isEqualTo('I');
      assertProbeStillInStep(out, in);
    });

    assertThat(idsOf(type)).as("the refused statement aborted the block its pending write was in").isEmpty();
  }

  // Regression: #8214
  @Test
  @DisplayName("[#8214] inside an explicit block a simple query commits nothing: the block still ends only on COMMIT/ROLLBACK")
  void simpleQueryInsideAnExplicitBlockCommitsNothing() throws Exception {
    final String type = "Issue8214Explicit";
    withConnection((out, in) -> {
      createType(out, in, type);

      sendSimpleQuery(out, "BEGIN");
      assertThat(readyForQueryStatusOf(readUntilReadyForQuery(in))).isEqualTo('T');

      parseBindExecute(out, "w1", "INSERT INTO " + type + " SET id = 60");
      sendSimpleQuery(out, "INSERT INTO " + type + " SET id = 61");
      final List<WireMessage> answers = readUntilReadyForQuery(in);
      assertThat(commandTagsOf(answers)).containsExactly("INSERT 0 1", "INSERT 0 1");
      assertThat(readyForQueryStatusOf(answers)).isEqualTo('T');

      failPipelineAndSync(out, in, 'E');

      sendSimpleQuery(out, "ROLLBACK");
      assertThat(readyForQueryStatusOf(readUntilReadyForQuery(in))).isEqualTo('I');
      assertProbeStillInStep(out, in);
    });

    assertThat(idsOf(type)).as("the explicit block was rolled back as a whole").isEmpty();
  }

  // Regression: #8214
  @Test
  @DisplayName("[#8214] inside an explicit block opened through the extended protocol a simple query commits nothing either")
  void simpleQueryInsideAnExtendedProtocolExplicitBlockCommitsNothing() throws Exception {
    final String type = "Issue8214ExtendedBegin";
    withConnection((out, in) -> {
      createType(out, in, type);

      // BEGIN through Parse/Bind/Execute takes applyTransactionControl()'s arm, not queryCommand()'s; the 'Q' that
      // follows must still see the explicit block and leave it open.
      parseBindExecute(out, "b1", "BEGIN");
      parseBindExecute(out, "w1", "INSERT INTO " + type + " SET id = 70");
      sendSimpleQuery(out, "INSERT INTO " + type + " SET id = 71");
      final List<WireMessage> answers = readUntilReadyForQuery(in);
      assertThat(commandTagsOf(answers)).containsExactly("BEGIN", "INSERT 0 1", "INSERT 0 1");
      assertThat(readyForQueryStatusOf(answers)).as("the explicit block is still open").isEqualTo('T');

      sendSimpleQuery(out, "ROLLBACK");
      assertThat(readyForQueryStatusOf(readUntilReadyForQuery(in))).isEqualTo('I');
      assertProbeStillInStep(out, in);
    });

    assertThat(idsOf(type)).as("the explicit block was rolled back as a whole").isEmpty();
  }

  /**
   * A bogus Parse puts the session in skip-until-Sync, and the Sync then takes its discard branch.
   */
  private static void failPipelineAndSync(final DataOutputStream out, final DataInputStream in, final char expectedStatus)
      throws Exception {
    sendParse(out, "bad", "SELEKT bogus syntax");
    sendSync(out);
    final List<WireMessage> answers = readUntilReadyForQuery(in);
    assertThat(messageTypesOf(answers)).as("the failed Parse is answered with exactly one ErrorResponse").containsOnlyOnce('E');
    assertThat(readyForQueryStatusOf(answers)).isEqualTo(expectedStatus);
  }

  private static void assertProbeStillInStep(final DataOutputStream out, final DataInputStream in) throws Exception {
    sendSimpleQuery(out, "SELECT 1 AS probe");
    final List<WireMessage> probe = readUntilReadyForQuery(in);
    assertThat(messageTypesOf(probe)).containsSequence('T', 'D', 'C', 'Z');
    assertThat(in.available()).as("nothing is left unread behind the probe's ReadyForQuery").isZero();
  }

  private interface WireExchange {
    void run(DataOutputStream out, DataInputStream in) throws Exception;
  }

  private void withConnection(final WireExchange exchange) throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());

      sendStartupMessage(out, "root", getDatabaseName());
      readMessage(in); // AuthenticationCleartextPassword
      sendPasswordMessage(out, DEFAULT_PASSWORD_FOR_TESTS);
      readMessageOfType(in, 'Z');

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> exchange.run(out, in));
    }
  }

  private void createType(final DataOutputStream out, final DataInputStream in, final String typeName) throws Exception {
    sendSimpleQuery(out, "CREATE DOCUMENT TYPE " + typeName + " IF NOT EXISTS");
    assertThat(messageTypesOf(readUntilReadyForQuery(in))).doesNotContain('E');
  }

  private static void parseBindExecute(final DataOutputStream out, final String name, final String query) throws Exception {
    sendParse(out, name, query);
    sendBind(out, name, name);
    sendExecute(out, name);
  }

  /**
   * The command tags of every CommandComplete ('C') in a round trip. The body is one null-terminated string.
   */
  private static List<String> commandTagsOf(final List<WireMessage> messages) {
    final List<String> tags = new ArrayList<>();
    for (final WireMessage message : messages) {
      if (message.type() != 'C')
        continue;
      final byte[] body = message.body();
      int end = 0;
      while (end < body.length && body[end] != 0)
        end++;
      tags.add(new String(body, 0, end, StandardCharsets.UTF_8));
    }
    return tags;
  }

  private List<Integer> idsOf(final String typeName) {
    final Database database = getServerDatabase(0, getDatabaseName());
    final List<Integer> ids = new ArrayList<>();
    database.query("sql", "SELECT id FROM " + typeName + " ORDER BY id")
        .forEachRemaining(row -> ids.add(row.getProperty("id")));
    return ids;
  }
}
