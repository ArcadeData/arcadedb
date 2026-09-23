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
import static com.arcadedb.postgres.PostgresWireMessages.messageTypesOf;
import static com.arcadedb.postgres.PostgresWireMessages.readUntilReadyForQuery;
import static com.arcadedb.postgres.PostgresWireMessages.readyForQueryStatusOf;
import static com.arcadedb.postgres.PostgresWireMessages.sendBind;
import static com.arcadedb.postgres.PostgresWireMessages.sendExecute;
import static com.arcadedb.postgres.PostgresWireMessages.sendParse;
import static com.arcadedb.postgres.PostgresWireMessages.sendSimpleQuery;
import static com.arcadedb.postgres.PostgresWireMessages.sendSync;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
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
class Issue8214SimpleQueryCommitsOpenPipelineIT extends PostgresWireProtocolTestBase {

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
      assertStreamStillInStep(out, in);
    });

    assertThat(idsOf(type)).as("the 'Q' committed both writes before the pipeline failed").containsExactly(1, 2);
  }

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
      assertStreamStillInStep(out, in);
    });

    assertThat(idsOf(type)).containsExactly(10);
  }

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
      assertStreamStillInStep(out, in);
    });

    assertThat(idsOf(type)).containsExactly(20);
  }

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
      assertStreamStillInStep(out, in);
    });

    assertThat(idsOf(type)).containsExactly(30);
  }

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

      assertStreamStillInStep(out, in);
    });

    assertThat(idsOf(type)).as("the failed 'Q' discarded the write before it, not the one after it").containsExactly(41);
  }

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
      assertStreamStillInStep(out, in);
    });

    assertThat(idsOf(type)).as("the refused statement aborted the block its pending write was in").isEmpty();
  }

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
      assertStreamStillInStep(out, in);
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

  private static void assertStreamStillInStep(final DataOutputStream out, final DataInputStream in) throws Exception {
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
