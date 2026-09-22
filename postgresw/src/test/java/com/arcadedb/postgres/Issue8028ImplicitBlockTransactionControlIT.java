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
 * Regression test for issue #8028: {@code applyTransactionControl()} guarded both its COMMIT and its ROLLBACK
 * arm on {@code explicitTransactionStarted}, the flag only a BEGIN raises. Issue #7775 had since given a
 * pipeline sent without BEGIN an implicit transaction block of its own, inside which that flag is false, so
 * both arms answered the client and did nothing:
 * <ul>
 *   <li>ROLLBACK left the block open for the next Sync to COMMIT - the client was told "ROLLBACK" and its
 *       writes were kept. PostgreSQL aborts the implicit block on ROLLBACK
 *       ({@code UserAbortTransactionBlock} on {@code TBLOCK_IMPLICIT_INPROGRESS}).</li>
 *   <li>COMMIT did not commit; the block was left for Sync, so a later failure in the same pipeline took
 *       Sync's discard branch and rolled back statements the client had already been told were committed -
 *       the loss {@code applyTransactionControl()}'s own javadoc says it exists to prevent.</li>
 * </ul>
 * The same guard sat on {@code queryCommand()}'s simple-protocol COMMIT/ROLLBACK, where a 'Q' message that
 * arrives while an implicit block opened by an earlier extended-protocol Execute is still open hits exactly
 * the same two outcomes.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue8028ImplicitBlockTransactionControlIT extends PostgresWireProtocolTestBase {

  private static final String ROLLBACK_TYPE        = "Issue8028Rollback";
  private static final String COMMIT_TYPE          = "Issue8028Commit";
  private static final String SIMPLE_ROLLBACK_TYPE = "Issue8028SimpleRollback";
  private static final String SIMPLE_COMMIT_TYPE   = "Issue8028SimpleCommit";
  private static final String EXPLICIT_TYPE        = "Issue8028Explicit";
  private static final String NO_BLOCK_TYPE        = "Issue8028NoBlock";

  @Test
  @DisplayName("[#8028] ROLLBACK inside an implicit block discards its writes instead of leaving them for Sync to commit")
  void rollbackInsideAnImplicitBlockDiscardsItsWrites() throws Exception {
    withConnection((out, in) -> {
      createType(out, in, ROLLBACK_TYPE);

      // One pipeline, no BEGIN anywhere, one Sync: the implicit transaction block. The client rolls it back
      // itself, mid-pipeline, and then writes again - PostgreSQL starts a fresh implicit block for the second
      // write and that one IS committed by the Sync.
      parseBindExecute(out, "w1", "INSERT INTO " + ROLLBACK_TYPE + " SET id = 200");
      parseBindExecute(out, "tc", "ROLLBACK");
      parseBindExecute(out, "w2", "INSERT INTO " + ROLLBACK_TYPE + " SET id = 201");
      sendSync(out);

      final List<WireMessage> answers = readUntilReadyForQuery(in);
      assertThat(messageTypesOf(answers)).as("nothing in this pipeline fails").doesNotContain('E');
      assertThat(commandTagsOf(answers)).as("the client is acknowledged the ROLLBACK it sent").contains("ROLLBACK");
      assertThat(readyForQueryStatusOf(answers)).as("no explicit block was ever opened, so the session is idle")
          .isEqualTo('I');
    });

    assertThat(idsOf(ROLLBACK_TYPE))
        .as("the acknowledged ROLLBACK discards the write that preceded it, and only the write that followed it survives")
        .containsExactly(201);
  }

  @Test
  @DisplayName("[#8028] COMMIT inside an implicit block persists at once, so a later failure in the same pipeline cannot take it back")
  void commitInsideAnImplicitBlockSurvivesALaterFailureInTheSamePipeline() throws Exception {
    withConnection((out, in) -> {
      createType(out, in, COMMIT_TYPE);

      // The acknowledged COMMIT has to be final by the time its CommandComplete goes out: the statement that
      // fails behind it in the same pipeline sends Sync down its discard branch, which rolls back whatever is
      // still open. Before the fix that discard took the "committed" row with it.
      parseBindExecute(out, "w1", "INSERT INTO " + COMMIT_TYPE + " SET id = 300");
      parseBindExecute(out, "tc", "COMMIT");
      parseBindExecute(out, "bad", "INSERT INTO Issue8028NoSuchType SET id = 301");
      sendSync(out);

      final List<WireMessage> answers = readUntilReadyForQuery(in);
      assertThat(messageTypesOf(answers)).as("the bogus statement is answered with an ErrorResponse")
          .containsOnlyOnce('E');
      assertThat(commandTagsOf(answers)).as("the client is acknowledged the COMMIT it sent").contains("COMMIT");
      assertThat(readyForQueryStatusOf(answers)).isEqualTo('I');
    });

    assertThat(idsOf(COMMIT_TYPE))
        .as("an acknowledged COMMIT is not taken back by a later failure in the same pipeline")
        .containsExactly(300);
  }

  @Test
  @DisplayName("[#8028] a simple-query ROLLBACK ends an implicit block the extended protocol left open")
  void simpleQueryRollbackEndsAnImplicitBlockOpenedByTheExtendedProtocol() throws Exception {
    withConnection((out, in) -> {
      createType(out, in, SIMPLE_ROLLBACK_TYPE);

      // The write opens the implicit block and is NOT followed by a Sync, so the block is still open when the
      // 'Q' message arrives. queryCommand()'s ROLLBACK arm carried the same explicitTransactionStarted guard,
      // so it answered "ROLLBACK" and left the block for the Sync below to commit.
      parseBindExecute(out, "w1", "INSERT INTO " + SIMPLE_ROLLBACK_TYPE + " SET id = 400");
      sendSimpleQuery(out, "ROLLBACK");

      final List<WireMessage> afterRollback = readUntilReadyForQuery(in);
      assertThat(messageTypesOf(afterRollback)).doesNotContain('E');
      assertThat(readyForQueryStatusOf(afterRollback)).isEqualTo('I');

      sendSync(out);
      assertThat(readyForQueryStatusOf(readUntilReadyForQuery(in))).isEqualTo('I');
    });

    assertThat(idsOf(SIMPLE_ROLLBACK_TYPE))
        .as("the acknowledged ROLLBACK discards the block the extended protocol had opened").isEmpty();
  }

  @Test
  @DisplayName("[#8028] a simple-query COMMIT persists an implicit block the extended protocol left open")
  void simpleQueryCommitInsideAnImplicitBlockSurvivesALaterFailure() throws Exception {
    withConnection((out, in) -> {
      createType(out, in, SIMPLE_COMMIT_TYPE);

      parseBindExecute(out, "w1", "INSERT INTO " + SIMPLE_COMMIT_TYPE + " SET id = 500");
      sendSimpleQuery(out, "COMMIT");
      final List<WireMessage> afterCommit = readUntilReadyForQuery(in);
      assertThat(messageTypesOf(afterCommit)).doesNotContain('E');
      assertThat(readyForQueryStatusOf(afterCommit)).isEqualTo('I');

      // A failure after the acknowledged COMMIT sends the following Sync down its discard branch.
      parseBindExecute(out, "bad", "INSERT INTO Issue8028NoSuchType SET id = 501");
      sendSync(out);
      final List<WireMessage> afterFailure = readUntilReadyForQuery(in);
      assertThat(messageTypesOf(afterFailure)).containsOnlyOnce('E');
      assertThat(readyForQueryStatusOf(afterFailure)).isEqualTo('I');
    });

    assertThat(idsOf(SIMPLE_COMMIT_TYPE))
        .as("the simple-query COMMIT persisted the block at once, so the later discard has nothing of it left to take")
        .containsExactly(500);
  }

  @Test
  @DisplayName("[#8028] an explicit BEGIN block still commits and rolls back exactly as before")
  void anExplicitBlockStillCommitsAndRollsBackAsBefore() throws Exception {
    withConnection((out, in) -> {
      createType(out, in, EXPLICIT_TYPE);

      // BEGIN/COMMIT over the extended protocol: the path the guard was written for, unchanged by the fix.
      parseBindExecute(out, "b1", "BEGIN");
      parseBindExecute(out, "w1", "INSERT INTO " + EXPLICIT_TYPE + " SET id = 600");
      parseBindExecute(out, "c1", "COMMIT");
      sendSync(out);
      final List<WireMessage> afterCommit = readUntilReadyForQuery(in);
      assertThat(messageTypesOf(afterCommit)).doesNotContain('E');
      assertThat(readyForQueryStatusOf(afterCommit)).as("the block the client ended leaves the session idle")
          .isEqualTo('I');

      // BEGIN/ROLLBACK, same protocol: the write is discarded.
      parseBindExecute(out, "b2", "BEGIN");
      parseBindExecute(out, "w2", "INSERT INTO " + EXPLICIT_TYPE + " SET id = 601");
      parseBindExecute(out, "r2", "ROLLBACK");
      sendSync(out);
      final List<WireMessage> afterRollback = readUntilReadyForQuery(in);
      assertThat(messageTypesOf(afterRollback)).doesNotContain('E');
      assertThat(readyForQueryStatusOf(afterRollback)).isEqualTo('I');

      // And the same pair on the simple query protocol.
      sendSimpleQuery(out, "BEGIN");
      assertThat(readyForQueryStatusOf(readUntilReadyForQuery(in))).as("an open explicit block reports 'T'")
          .isEqualTo('T');
      sendSimpleQuery(out, "INSERT INTO " + EXPLICIT_TYPE + " SET id = 602");
      assertThat(messageTypesOf(readUntilReadyForQuery(in))).doesNotContain('E');
      sendSimpleQuery(out, "ROLLBACK");
      assertThat(readyForQueryStatusOf(readUntilReadyForQuery(in))).isEqualTo('I');
    });

    assertThat(idsOf(EXPLICIT_TYPE)).as("only the explicitly committed row survives").containsExactly(600);
  }

  @Test
  @DisplayName("[#8028] COMMIT and ROLLBACK with no transaction open are acknowledged and leave the session usable")
  void transactionControlWithNothingOpenIsAcknowledgedAndHarmless() throws Exception {
    withConnection((out, in) -> {
      createType(out, in, NO_BLOCK_TYPE);

      // Nothing is open on either protocol here, which is the other side of the isTransactionActive() guard
      // the fix now relies on: before it, the flag short-circuited and the database was never even asked.
      // PostgreSQL answers a COMMIT/ROLLBACK outside a block with a warning and the command tag, not an
      // error, and every driver that ends a unit of work unconditionally sends one.
      sendSimpleQuery(out, "ROLLBACK");
      final List<WireMessage> simpleRollback = readUntilReadyForQuery(in);
      assertThat(messageTypesOf(simpleRollback)).as("a stray ROLLBACK is not an error").doesNotContain('E');
      assertThat(readyForQueryStatusOf(simpleRollback)).isEqualTo('I');

      sendSimpleQuery(out, "COMMIT");
      final List<WireMessage> simpleCommit = readUntilReadyForQuery(in);
      assertThat(messageTypesOf(simpleCommit)).as("a stray COMMIT is not an error").doesNotContain('E');
      assertThat(readyForQueryStatusOf(simpleCommit)).isEqualTo('I');

      // The same pair over the extended protocol, each as its own pipeline so no write precedes them.
      parseBindExecute(out, "r0", "ROLLBACK");
      sendSync(out);
      final List<WireMessage> extRollback = readUntilReadyForQuery(in);
      assertThat(messageTypesOf(extRollback)).doesNotContain('E');
      assertThat(commandTagsOf(extRollback)).contains("ROLLBACK");
      assertThat(readyForQueryStatusOf(extRollback)).isEqualTo('I');

      parseBindExecute(out, "c0", "COMMIT");
      sendSync(out);
      final List<WireMessage> extCommit = readUntilReadyForQuery(in);
      assertThat(messageTypesOf(extCommit)).doesNotContain('E');
      assertThat(commandTagsOf(extCommit)).contains("COMMIT");
      assertThat(readyForQueryStatusOf(extCommit)).isEqualTo('I');

      // And none of the four left the connection in a state that swallows the next write.
      parseBindExecute(out, "w", "INSERT INTO " + NO_BLOCK_TYPE + " SET id = 700");
      sendSync(out);
      final List<WireMessage> afterWrite = readUntilReadyForQuery(in);
      assertThat(messageTypesOf(afterWrite)).doesNotContain('E');
      assertThat(readyForQueryStatusOf(afterWrite)).isEqualTo('I');
    });

    assertThat(idsOf(NO_BLOCK_TYPE)).as("the write that followed four no-op transaction-control statements commits")
        .containsExactly(700);
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

  /**
   * Parse/Bind/Execute without a Describe: a transaction-control portal answers Describe('P') with NoData, and
   * what these tests assert on is the CommandComplete tag and what the database ends up holding.
   */
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
