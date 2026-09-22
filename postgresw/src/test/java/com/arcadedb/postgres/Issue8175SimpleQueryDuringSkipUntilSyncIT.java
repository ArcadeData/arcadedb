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
class Issue8175SimpleQueryDuringSkipUntilSyncIT extends PostgresWireProtocolTestBase {

  private static final String COMMIT_TYPE    = "Issue8175Commit";
  private static final String ROLLBACK_TYPE  = "Issue8175Rollback";
  private static final String STATEMENT_TYPE = "Issue8175Statement";
  private static final String EXPLICIT_TYPE  = "Issue8175Explicit";

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

  private interface WireExchange {
    void run(DataOutputStream out, DataInputStream in) throws Exception;
  }

  private void withConnection(final WireExchange exchange) throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", GlobalConfiguration.POSTGRES_PORT.getValueAsInteger()), 2000);
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
