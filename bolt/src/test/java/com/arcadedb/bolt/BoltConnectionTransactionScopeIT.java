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
package com.arcadedb.bolt;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.bolt.message.BoltMessage;
import com.arcadedb.database.Database;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.security.ServerSecurity;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The transaction of a BOLT connection is the one the BEGIN, COMMIT and ROLLBACK messages of the protocol manage. A statement
 * of the same name sent as the text of a RUN is not a way to open or end one: the connection tracks no such transaction, so
 * neither the RESET, the LOGOFF, a re-authentication nor the close of the connection would end it, and the next statement of
 * the connection, whoever it runs as by then, would join it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class BoltConnectionTransactionScopeIT extends BaseBoltServerTest {
  private static final String SECOND_USER     = "boltScopeSecondUser";
  private static final String SECOND_PASSWORD = "boltScopeSecondPassword";
  private static final String PROBE_TYPE      = "ScopeProbe";

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("Bolt:com.arcadedb.bolt.BoltProtocolPlugin");
  }

  @AfterEach
  @Override
  public void endTest() {
    final ArcadeDBServer server = getServer(0);
    if (server != null && server.isStarted()) {
      final ServerSecurity security = server.getSecurity();
      if (security.getUser(SECOND_USER) != null)
        security.dropUser(SECOND_USER);
    }
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    super.endTest();
  }

  /** Every spelling of a transaction control statement is answered with a client failure, and the connection recovers by RESET. */
  @Test
  void statementsThatBeginOrEndATransactionAreRefused() throws Exception {
    try (final BoltWireConnection bolt = new BoltWireConnection(getServerBoltPort(), getDatabaseName())) {
      for (final String statement : List.of("START TRANSACTION", "start transaction", "COMMIT", "ROLLBACK",
          "/* note */ START TRANSACTION", "// note\nCOMMIT", "PROFILE START TRANSACTION")) {
        bolt.run(statement);
        final BoltWireConnection.Summary refused = bolt.readSummary();
        assertThat(refused.signature()).as(statement).isEqualTo(BoltMessage.FAILURE);
        assertThat(refused.code()).as(statement).isEqualTo(BoltErrorCodes.SEMANTIC_ERROR);

        bolt.sendNoFields(BoltMessage.RESET);
        assertThat(bolt.readSummary().signature()).as(statement).isEqualTo(BoltMessage.SUCCESS);
      }

      // The connection is as usable as before
      assertThat(runAndPull(bolt, "RETURN 1 AS one").signature()).isEqualTo(BoltMessage.SUCCESS);
    }
  }

  /** Inside the transaction of a BEGIN message the same statements are refused, and what the transaction wrote is still its own. */
  @Test
  void statementsThatEndATransactionAreRefusedInsideAnOpenTransaction() throws Exception {
    try (final BoltWireConnection bolt = new BoltWireConnection(getServerBoltPort(), getDatabaseName())) {
      bolt.begin(getDatabaseName());
      assertThat(runAndPull(bolt, "CREATE (:" + PROBE_TYPE + " {owner: 'inside'})").signature()).isEqualTo(BoltMessage.SUCCESS);

      for (final String statement : List.of("COMMIT", "ROLLBACK", "START TRANSACTION")) {
        bolt.run(statement);
        final BoltWireConnection.Summary refused = bolt.readSummary();
        assertThat(refused.signature()).as(statement).isEqualTo(BoltMessage.FAILURE);
        assertThat(refused.code()).as(statement).isEqualTo(BoltErrorCodes.SEMANTIC_ERROR);

        bolt.sendNoFields(BoltMessage.RESET);
        assertThat(bolt.readSummary().signature()).as(statement).isEqualTo(BoltMessage.SUCCESS);

        // The RESET rolled the transaction back, and the statement ended nothing on its own before that
        assertThat(committedProbes()).as(statement).isZero();

        bolt.begin(getDatabaseName());
        assertThat(runAndPull(bolt, "CREATE (:" + PROBE_TYPE + " {owner: 'inside'})").signature()).isEqualTo(BoltMessage.SUCCESS);
      }

      bolt.sendNoFields(BoltMessage.COMMIT);
      assertThat(bolt.readSummary().signature()).isEqualTo(BoltMessage.SUCCESS);
      assertThat(committedProbes()).isEqualTo(1);
    }
  }

  /**
   * A connection that changes user goes on with a transaction of its own: what the next user writes with an auto-commit statement
   * is committed, and what the previous user left uncommitted is neither seen nor committed by it.
   */
  @Test
  void aUserThatTakesOverAConnectionRunsInTransactionsOfItsOwn() throws Exception {
    createSecondUser();

    try (final BoltWireConnection bolt = new BoltWireConnection(getServerBoltPort(), getDatabaseName())) {
      // Whatever this answers, what is written next must not depend on it: the statement either opens nothing or is refused
      bolt.run("START TRANSACTION");
      final BoltWireConnection.Summary first = bolt.readSummary();
      if (first.signature() == BoltMessage.SUCCESS) {
        bolt.pull(-1, -1);
        assertThat(bolt.readSummary().signature()).isEqualTo(BoltMessage.SUCCESS);
      } else {
        bolt.sendNoFields(BoltMessage.RESET);
        assertThat(bolt.readSummary().signature()).isEqualTo(BoltMessage.SUCCESS);
      }

      assertThat(runAndPull(bolt, "CREATE (:" + PROBE_TYPE + " {owner: 'first'})").signature()).isEqualTo(BoltMessage.SUCCESS);

      bolt.sendNoFields(BoltMessage.LOGOFF);
      assertThat(bolt.readSummary().signature()).isEqualTo(BoltMessage.SUCCESS);
      bolt.logon(SECOND_USER, SECOND_PASSWORD);

      assertThat(runAndPull(bolt, "CREATE (:" + PROBE_TYPE + " {owner: 'second'})").signature()).isEqualTo(BoltMessage.SUCCESS);

      // Both statements of the connection committed on their own, as an auto-commit statement does: a reader outside the
      // connection finds them
      assertThat(committedProbes()).as("probes committed by the connection").isEqualTo(2);
    }

    // And they are still there once the connection is gone
    assertThat(committedProbes()).isEqualTo(2);
  }

  /** The same, for a LOGON of another user on a connection that was not logged off first. */
  @Test
  void aReauthenticationWithoutLogoffRunsInTransactionsOfItsOwn() throws Exception {
    createSecondUser();

    try (final BoltWireConnection bolt = new BoltWireConnection(getServerBoltPort(), getDatabaseName())) {
      bolt.run("START TRANSACTION");
      if (bolt.readSummary().signature() == BoltMessage.SUCCESS) {
        bolt.pull(-1, -1);
        assertThat(bolt.readSummary().signature()).isEqualTo(BoltMessage.SUCCESS);
      } else {
        bolt.sendNoFields(BoltMessage.RESET);
        assertThat(bolt.readSummary().signature()).isEqualTo(BoltMessage.SUCCESS);
      }
      assertThat(runAndPull(bolt, "CREATE (:" + PROBE_TYPE + " {owner: 'first'})").signature()).isEqualTo(BoltMessage.SUCCESS);

      bolt.logon(SECOND_USER, SECOND_PASSWORD);
      assertThat(runAndPull(bolt, "CREATE (:" + PROBE_TYPE + " {owner: 'second'})").signature()).isEqualTo(BoltMessage.SUCCESS);

      assertThat(committedProbes()).as("probes committed by the connection").isEqualTo(2);
    }
  }

  /**
   * A transaction a nested statement of the connection opens is not one the connection tracks, and it ends with the identity the
   * connection began it under: after a LOGOFF and a LOGON the next user neither sees what was written in it nor writes into it.
   */
  @Test
  void aTransactionLeftOpenByANestedStatementEndsWithTheLogoff() throws Exception {
    createSecondUser();

    try (final BoltWireConnection bolt = new BoltWireConnection(getServerBoltPort(), getDatabaseName())) {
      openTransactionFromANestedStatement(bolt);
      assertThat(runAndPull(bolt, "CREATE (:" + PROBE_TYPE + " {owner: 'first'})").signature()).isEqualTo(BoltMessage.SUCCESS);

      bolt.sendNoFields(BoltMessage.LOGOFF);
      assertThat(bolt.readSummary().signature()).isEqualTo(BoltMessage.SUCCESS);
      bolt.logon(SECOND_USER, SECOND_PASSWORD);

      final BoltWireConnection.Summary seen = runAndPull(bolt, "MATCH (n:" + PROBE_TYPE + ") RETURN count(n) AS c");
      assertThat(seen.signature()).isEqualTo(BoltMessage.SUCCESS);
      assertThat(seen.records()).as("what the second user reads").isEqualTo(List.of(List.of(0L)));

      assertThat(runAndPull(bolt, "CREATE (:" + PROBE_TYPE + " {owner: 'second'})").signature()).isEqualTo(BoltMessage.SUCCESS);
      assertThat(committedProbes("second")).as("probes of the second user, committed").isEqualTo(1);
    }
  }

  /** A LOGON that re-authenticates a connection in READY ends it the same way. */
  @Test
  void aTransactionLeftOpenByANestedStatementEndsWithTheReauthentication() throws Exception {
    createSecondUser();

    try (final BoltWireConnection bolt = new BoltWireConnection(getServerBoltPort(), getDatabaseName())) {
      openTransactionFromANestedStatement(bolt);
      assertThat(runAndPull(bolt, "CREATE (:" + PROBE_TYPE + " {owner: 'first'})").signature()).isEqualTo(BoltMessage.SUCCESS);

      bolt.logon(SECOND_USER, SECOND_PASSWORD);

      assertThat(runAndPull(bolt, "CREATE (:" + PROBE_TYPE + " {owner: 'second'})").signature()).isEqualTo(BoltMessage.SUCCESS);
      assertThat(committedProbes("second")).as("probes of the second user, committed").isEqualTo(1);
    }
  }

  /** And so does a RESET, the message a connection pool sends before it hands the connection to its next borrower. */
  @Test
  void aTransactionLeftOpenByANestedStatementEndsWithTheReset() throws Exception {
    try (final BoltWireConnection bolt = new BoltWireConnection(getServerBoltPort(), getDatabaseName())) {
      openTransactionFromANestedStatement(bolt);
      assertThat(runAndPull(bolt, "CREATE (:" + PROBE_TYPE + " {owner: 'first'})").signature()).isEqualTo(BoltMessage.SUCCESS);

      bolt.sendNoFields(BoltMessage.RESET);
      assertThat(bolt.readSummary().signature()).isEqualTo(BoltMessage.SUCCESS);

      assertThat(runAndPull(bolt, "CREATE (:" + PROBE_TYPE + " {owner: 'second'})").signature()).isEqualTo(BoltMessage.SUCCESS);
      assertThat(committedProbes("second")).as("probes written after the RESET, committed").isEqualTo(1);
    }
  }

  /**
   * Opens an engine transaction from inside another statement and checks it was opened, so the tests above cannot pass for want of
   * one: the statement the nested text runs is not the connection's own.
   */
  private static void openTransactionFromANestedStatement(final BoltWireConnection bolt) throws IOException {
    final BoltWireConnection.Summary nested = runAndPull(bolt,
        "CALL apoc.do.when(true, 'START TRANSACTION', 'RETURN 1', {}) YIELD value RETURN value");
    assertThat(nested.signature()).isEqualTo(BoltMessage.SUCCESS);
    assertThat(nested.records().toString()).contains("begin");
  }

  /** What a reader that is not part of any transaction finds: the probes the server has committed. */
  private int committedProbes() {
    final Database database = getServerDatabase(0, getDatabaseName());
    return database.getSchema().existsType(PROBE_TYPE) ? (int) database.countType(PROBE_TYPE, true) : 0;
  }

  /** The probes of {@code owner} that the server has committed. */
  private int committedProbes(final String owner) {
    final Database database = getServerDatabase(0, getDatabaseName());
    if (!database.getSchema().existsType(PROBE_TYPE))
      return 0;
    int count = 0;
    try (final ResultSet resultSet = database.query("sql", "SELECT FROM " + PROBE_TYPE + " WHERE owner = ?", owner)) {
      while (resultSet.hasNext()) {
        resultSet.next();
        ++count;
      }
    }
    return count;
  }

  /** RUN, then PULL of everything, returning the summary of the PULL (or the one of the RUN when it failed). */
  private static BoltWireConnection.Summary runAndPull(final BoltWireConnection bolt, final String query) throws IOException {
    bolt.run(query);
    final BoltWireConnection.Summary run = bolt.readSummary();
    if (run.signature() != BoltMessage.SUCCESS)
      return run;
    bolt.pull(-1, -1);
    return bolt.readSummary();
  }

  private void createSecondUser() {
    final ServerSecurity security = getServer(0).getSecurity();
    security.createUser(new JSONObject().put("name", SECOND_USER).put("password", security.encodePassword(SECOND_PASSWORD))
        .put("databases", new JSONObject().put(getDatabaseName(), new JSONArray(new String[] { "admin" }))));
  }
}
