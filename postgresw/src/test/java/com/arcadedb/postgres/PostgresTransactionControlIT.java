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
import org.postgresql.util.PSQLException;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

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
import static com.arcadedb.postgres.PostgresWireMessages.show;
import static com.arcadedb.postgres.PostgresWireMessages.sqlStateOf;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
 * Transaction-control statements ({@code BEGIN}, {@code COMMIT}, {@code ROLLBACK}, {@code END}, {@code ABORT},
 * {@code SAVEPOINT}, {@code RELEASE}, {@code ROLLBACK TO} and their spellings) over the PostgreSQL wire protocol, on
 * the simple and the extended query protocol: what each one is answered with, what it does to the transaction status
 * a client reads from {@code ReadyForQuery}, and which writes end up committed, including autocommit and
 * {@code CALL ... IN TRANSACTIONS} through the JDBC driver.
 */
class PostgresTransactionControlIT extends PostgresWireProtocolTestBase {

  /*
   * Regression tests for issue #6543: over the Postgres <b>extended query</b> protocol (Parse/Bind/Execute), the
   * Parse-time dispatch in {@link PostgresNetworkExecutor#parseCommand()} that tracks explicit-transaction state
   * recognized only {@code BEGIN}/{@code BEGIN TRANSACTION} and {@code COMMIT}. There was no {@code ROLLBACK}
   * branch, and worse, {@code sqlEngine.parse()} was called <i>unconditionally</i> before that check ever ran: a
   * bare {@code ROLLBACK} sent as its own Parse/Bind/Execute (rather than folded into a simple-query {@code 'Q'}
   * message, the wire path already covered by issue #6457/#6542) parsed successfully as ArcadeDB SQL's own
   * {@code RollbackStatement} - which does roll the underlying transaction back - but left
   * {@code explicitTransactionStarted} set. Since {@code writeReadyForQueryMessage()} reports status {@code 'T'}
   * whenever that flag is set, and {@code syncCommand()}'s implicit-commit branch only runs when the flag is
   * clear, the wire-level session stayed reported as "inside an explicit transaction" forever after the
   * ROLLBACK, even though the underlying transaction had already been discarded.
   * <p>
   * The same unconditional parse also meant a {@code ROLLBACK WORK}/{@code ROLLBACK TRANSACTION}/
   * {@code COMMIT WORK} could never be recognized by merely widening the {@code isCommitStatement}/
   * {@code isRollbackStatement} helpers (the follow-up issue #6543 explicitly asked to fold in): ArcadeDB SQL's
   * {@code rollbackStatement}/{@code commitStatement} grammar productions accept only the bare keyword (plus
   * COMMIT's own {@code RETRY} clause), so parsing any of those forms as SQL throws a syntax error before the
   * widened check is ever reached. The fix checks BEGIN/COMMIT/ROLLBACK - in every recognized form - before
   * calling {@code sqlEngine.parse()} at all, the same ordering {@code queryCommand()}'s simple-query dispatch
   * already used for exactly this reason.
   * <p>
   * These tests speak the wire protocol directly over a raw socket, framing each statement as its own
   * Parse+Bind+Execute+Sync round trip, so the extended-query dispatch path is actually exercised (a JDBC/
   * psycopg2 client normally sends transaction-control statements over the simple-query protocol, which would
   * not reach this bug at all).
   * <p>
   * <b>Not covered here:</b> whether ROLLBACK actually discards a write made after an extended-query BEGIN.
   * Unlike {@code queryCommand()}'s BEGIN handling (which calls {@code database.begin()} directly),
   * {@code parseCommand()}'s BEGIN branch only sets {@code explicitTransactionStarted} and never starts a real
   * underlying transaction - a pre-existing gap, independent of this issue, that means a write made after an
   * extended-query BEGIN auto-commits on its own before any following ROLLBACK/COMMIT is reached. Asserting
   * data-discarding here would therefore pass or fail on that unrelated gap rather than on the fix this class
   * targets.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  // Regression: #6543
  @Test
  @DisplayName("[#6543] a bare ROLLBACK over the extended query protocol ends the explicit transaction cleanly")
  void rollbackOverExtendedProtocolEndsTransaction() throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
        // BEGIN over the extended query protocol: status must go to 'T'.
        assertThat(readyForQueryStatusOf(runExtendedQuery(out, in, "BEGIN"))).isEqualTo('T');

        // ROLLBACK, unrecognized in the Parse dispatch before the fix, must not error and must end the
        // transaction, reporting idle status - the core of issue #6543.
        final List<WireMessage> afterRollback = runExtendedQuery(out, in, "ROLLBACK");
        assertThat(messageTypesOf(afterRollback)).as("no ErrorResponse for ROLLBACK itself").doesNotContain('E');
        assertThat(readyForQueryStatusOf(afterRollback))
            .as("ROLLBACK must clear explicitTransactionStarted so status goes back to idle").isEqualTo('I');

        // The session is fully usable again, not wedged in a permanent 'T' state.
        final List<WireMessage> afterRecovery = runExtendedQuery(out, in, "SELECT 1");
        assertThat(messageTypesOf(afterRecovery)).doesNotContain('E');
        assertThat(readyForQueryStatusOf(afterRecovery)).isEqualTo('I');
      });
    }
  }

  // Regression: #6543
  @Test
  @DisplayName("[#6543] ROLLBACK WORK / ROLLBACK TRANSACTION are recognized as aliases for ROLLBACK over the extended query protocol")
  void rollbackAliasesAreRecognizedOverExtendedProtocol() throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
        assertThat(readyForQueryStatusOf(runExtendedQuery(out, in, "BEGIN"))).isEqualTo('T');
        assertThat(readyForQueryStatusOf(runExtendedQuery(out, in, "ROLLBACK WORK")))
            .as("ROLLBACK WORK must be recognized just like a bare ROLLBACK").isEqualTo('I');

        assertThat(readyForQueryStatusOf(runExtendedQuery(out, in, "BEGIN TRANSACTION"))).isEqualTo('T');
        assertThat(readyForQueryStatusOf(runExtendedQuery(out, in, "ROLLBACK TRANSACTION")))
            .as("ROLLBACK TRANSACTION must be recognized just like a bare ROLLBACK").isEqualTo('I');

        // BEGIN WORK: PostgreSQL's own grammar is BEGIN [ WORK | TRANSACTION ], added per review feedback on
        // this PR for symmetry with the COMMIT/ROLLBACK WORK forms above.
        assertThat(readyForQueryStatusOf(runExtendedQuery(out, in, "BEGIN WORK")))
            .as("BEGIN WORK must be recognized just like a bare BEGIN").isEqualTo('T');
        assertThat(readyForQueryStatusOf(runExtendedQuery(out, in, "ROLLBACK"))).isEqualTo('I');
      });
    }
  }

  // Regression: #6543
  @Test
  @DisplayName("[#6543] COMMIT WORK / END are recognized as aliases for COMMIT over the extended query protocol")
  void commitAliasesAreRecognizedOverExtendedProtocol() throws Exception {
    // Companion to the ROLLBACK-alias test above: isCommitStatement() was widened to COMMIT WORK/END/END
    // TRANSACTION/END WORK alongside the ROLLBACK fix (the extended-protocol COMMIT branch previously matched
    // only the exact literal "COMMIT"), and that widening only works because BEGIN/COMMIT/ROLLBACK are now
    // checked before sqlEngine.parse() is ever called - so this exercises both halves of the fix together.
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
        assertThat(readyForQueryStatusOf(runExtendedQuery(out, in, "BEGIN"))).isEqualTo('T');
        assertThat(readyForQueryStatusOf(runExtendedQuery(out, in, "COMMIT WORK")))
            .as("COMMIT WORK must be recognized just like a bare COMMIT").isEqualTo('I');

        assertThat(readyForQueryStatusOf(runExtendedQuery(out, in, "BEGIN"))).isEqualTo('T');
        assertThat(readyForQueryStatusOf(runExtendedQuery(out, in, "COMMIT TRANSACTION")))
            .as("COMMIT TRANSACTION must be recognized just like a bare COMMIT").isEqualTo('I');

        assertThat(readyForQueryStatusOf(runExtendedQuery(out, in, "BEGIN"))).isEqualTo('T');
        assertThat(readyForQueryStatusOf(runExtendedQuery(out, in, "END")))
            .as("END must be recognized as an alias for COMMIT").isEqualTo('I');

        assertThat(readyForQueryStatusOf(runExtendedQuery(out, in, "BEGIN"))).isEqualTo('T');
        assertThat(readyForQueryStatusOf(runExtendedQuery(out, in, "END TRANSACTION")))
            .as("END TRANSACTION must be recognized as an alias for COMMIT").isEqualTo('I');

        assertThat(readyForQueryStatusOf(runExtendedQuery(out, in, "BEGIN"))).isEqualTo('T');
        assertThat(readyForQueryStatusOf(runExtendedQuery(out, in, "END WORK")))
            .as("END WORK must be recognized as an alias for COMMIT").isEqualTo('I');
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
   * Regression tests for issue #7846: {@code ROLLBACK TO <savepoint>} used to be accepted as a silent no-op,
   * the same way {@code SAVEPOINT} and {@code RELEASE} harmlessly are, even though this server has no
   * savepoint checkpoint to roll back to. That let a client believe a rollback-to-savepoint had discarded
   * some of its pending writes when every one of them was still going to be persisted by the next
   * {@code COMMIT}. {@code ROLLBACK TO} must now fail instead, aborting the transaction so nothing commits
   * silently.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  private Connection getConnection() throws SQLException, ClassNotFoundException {
    Class.forName("org.postgresql.Driver");
    final var url = getServerPostgresJdbcUrl();
    final var props = new Properties();
    props.setProperty("user", "root");
    props.setProperty("password", DEFAULT_PASSWORD_FOR_TESTS);
    return DriverManager.getConnection(url, props);
  }

  // Regression: #7846
  @Test
  void rollbackToSavepointFailsAndAbortsTheTransaction() throws Exception {
    try (var conn = getConnection()) {
      try (var st = conn.createStatement()) {
        st.execute("CREATE VERTEX TYPE Issue7846Test IF NOT EXISTS");
      }

      conn.setAutoCommit(false);
      try (var st = conn.createStatement()) {
        st.execute("INSERT INTO Issue7846Test SET name = 'beforeSavepoint'");
        final Savepoint sp = conn.setSavepoint("issue7846sp");
        st.execute("INSERT INTO Issue7846Test SET name = 'afterSavepoint'");

        // A real PostgreSQL server would discard only 'afterSavepoint' here. This server cannot, so it must
        // refuse the rollback rather than pretend it happened: pgjdbc's Savepoint API runs over the extended
        // query protocol, where refusing a statement aborts the whole pipelined request and the transaction
        // it belongs to (mirrored by the simple-query-protocol case below) - so neither write survives, which
        // is the safe outcome given this server cannot undo only the one the client actually asked to discard.
        assertThatThrownBy(() -> conn.rollback(sp)).isInstanceOf(PSQLException.class);
      } catch (final SQLException ignored) {
        // pgjdbc surfaces the aborted transaction to further statements on this Connection as an exception
        // too, depending on how much of the Execute pipeline it had already queued - either way is fine here.
      }

      try {
        conn.rollback();
      } catch (final SQLException ignored) {
        // Already rolled back server-side by the failed statement above; a client-side ROLLBACK on top of
        // that is a no-op it's fine for pgjdbc to reject.
      }
      conn.setAutoCommit(true);

      try (var st = conn.createStatement(); ResultSet rs = st.executeQuery("SELECT FROM Issue7846Test")) {
        assertThat(rs.next()).as("neither the pre- nor the post-savepoint write may survive a rollback this server could not honor")
            .isFalse();
      }
    }
  }

  // Regression: #7846
  @Test
  void savepointAndReleaseStillSucceedAsHarmlessNoOps() throws Exception {
    try (var conn = getConnection()) {
      try (var st = conn.createStatement()) {
        st.execute("CREATE VERTEX TYPE Issue7846ReleaseTest IF NOT EXISTS");
      }

      conn.setAutoCommit(false);
      try (var st = conn.createStatement()) {
        st.execute("INSERT INTO Issue7846ReleaseTest SET name = 'row'");
        final Savepoint sp = conn.setSavepoint("issue7846releaseSp");
        conn.releaseSavepoint(sp);
        conn.commit();
      }

      try (var st = conn.createStatement(); ResultSet rs = st.executeQuery("SELECT FROM Issue7846ReleaseTest")) {
        assertThat(rs.next()).isTrue();
      }
    }
  }

  // Regression: #7846
  @Test
  void rollbackToOnTheSimpleQueryProtocolReportsTheFeatureNotSupportedSqlState() throws Exception {
    try (var conn = getConnection()) {
      conn.setAutoCommit(false);
      try (var st = conn.createStatement()) {
        st.execute("BEGIN");
        st.execute("SAVEPOINT plainsp");
        assertThatThrownBy(() -> st.execute("ROLLBACK TO plainsp"))
            .isInstanceOf(PSQLException.class)
            .satisfies(e -> assertThat(((PSQLException) e).getSQLState()).isEqualTo("0A000"));
        st.execute("ROLLBACK");
      }
    }
  }

  /*
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
  private static final String ROLLBACK_TYPE        = "Issue8028Rollback";
  private static final String COMMIT_TYPE          = "Issue8028Commit";
  private static final String SIMPLE_ROLLBACK_TYPE = "Issue8028SimpleRollback";
  private static final String SIMPLE_COMMIT_TYPE   = "Issue8028SimpleCommit";
  private static final String EXPLICIT_TYPE        = "Issue8028Explicit";
  private static final String NO_BLOCK_TYPE        = "Issue8028NoBlock";

  // Regression: #8028
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

  // Regression: #8028
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

  // Regression: #8028
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

  // Regression: #8028
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

  // Regression: #8028
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

  // Regression: #8028
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

  private void createType(final DataOutputStream out, final DataInputStream in, final String typeName) throws Exception {
    sendSimpleQuery(out, "CREATE DOCUMENT TYPE " + typeName + " IF NOT EXISTS");
    assertThat(messageTypesOf(readUntilReadyForQuery(in))).doesNotContain('E');
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

  /*
   * Regression tests for issue #8245: over the extended query protocol, {@code parseCommand()} recognized
   * BEGIN/COMMIT/ROLLBACK by exact string equality against the RAW wire text, so {@code "BEGIN;"},
   * {@code "COMMIT;"}, {@code "\nCOMMIT"} or {@code "COMMIT "} fell through to the SQL engine, whose grammar accepts
   * them. The engine then began/committed/rolled back at Execute while the protocol's own block state never moved:
   * ReadyForQuery kept reporting {@code 'I'} inside the block, a failure inside it did not abort it (so a statement
   * pipelined after the failure still ran and was committed), and the statement was answered with a RowDescription
   * and a data row under an empty command tag.
   * <p>
   * Each round trip here is Parse/Bind/Describe('P')/Execute/Sync, the shape libpq's {@code PQexecParams} (psycopg3,
   * the Arrow ADBC driver) sends. pgjdbc strips the trailing ';' itself, which is why the JDBC suites never saw this.
   */
  private static final String ABORT_TYPE = "Issue8245Abort";

  // Regression: #8245
  @Test
  @DisplayName("[#8245] every spelling of BEGIN/COMMIT/ROLLBACK/END is answered NoData + its tag and moves the status byte")
  void everySpellingIsTransactionControl() throws Exception {
    // { statement sent, tag expected, ReadyForQuery status expected after it }
    final String[][] steps = {
        { "BEGIN;", "BEGIN", "T" }, { "COMMIT;", "COMMIT", "I" },
        { "\nBEGIN", "BEGIN", "T" }, { "ROLLBACK;", "ROLLBACK", "I" },
        { "BEGIN ", "BEGIN", "T" }, { "END;", "COMMIT", "I" },
        { "BEGIN TRANSACTION;", "BEGIN", "T" }, { "COMMIT TRANSACTION;", "COMMIT", "I" },
        { "BEGIN WORK;", "BEGIN", "T" }, { "ROLLBACK WORK;", "ROLLBACK", "I" },
        { " begin ; ", "BEGIN", "T" }, { "\nCOMMIT", "COMMIT", "I" },
        { "BEGIN\n", "BEGIN", "T" }, { "COMMIT ", "COMMIT", "I" },
        { "BEGIN", "BEGIN", "T" }, { "\tROLLBACK TRANSACTION ;\n", "ROLLBACK", "I" },
        { "BEGIN", "BEGIN", "T" }, { "END WORK;", "COMMIT", "I" },
        { "BEGIN", "BEGIN", "T" }, { "{sql} COMMIT;", "COMMIT", "I" } };

    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        for (final String[] step : steps) {
          final List<WireMessage> response = roundTrip(out, in, step[0]);
          final String label = "'" + step[0].replace("\n", "\\n").replace("\t", "\\t") + "'";
          assertThat(messageTypesOf(response))
              .as(label + " is ParseComplete, BindComplete, NoData, CommandComplete, ReadyForQuery - no RowDescription or DataRow")
              .containsExactly('1', '2', 'n', 'C', 'Z');
          assertThat(commandTagOf(response)).as(label + " command tag").isEqualTo(step[1]);
          assertThat(readyForQueryStatusOf(response)).as(label + " ReadyForQuery status").isEqualTo(step[2].charAt(0));
        }
      });
    }
  }

  // Regression: #8245
  @Test
  @DisplayName("[#8245] a failure inside a block opened with BEGIN; aborts it: the next statement is refused and COMMIT; rolls back")
  void failureInsideASemicolonBlockAbortsIt() throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        sendSimpleQuery(out, "CREATE DOCUMENT TYPE " + ABORT_TYPE + " IF NOT EXISTS");
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).doesNotContain('E');

        assertThat(readyForQueryStatusOf(roundTrip(out, in, "BEGIN;"))).as("BEGIN; opens the block").isEqualTo('T');

        final List<WireMessage> failure = roundTrip(out, in, "SELECT * FROM NoSuchType8245");
        assertThat(messageTypesOf(failure)).contains('E');
        assertThat(readyForQueryStatusOf(failure)).as("the failure aborts the block").isEqualTo('E');

        // THE BUG: this INSERT ran, was acknowledged INSERT 0 1 and was committed by the COMMIT; that followed.
        final List<WireMessage> afterFailure = roundTrip(out, in, "INSERT INTO " + ABORT_TYPE + " SET tag = 'after-failure'");
        assertThat(messageTypesOf(afterFailure)).as("a statement inside an aborted block is refused").contains('E');
        assertThat(sqlStateOf(afterFailure)).isEqualTo("25P02");
        assertThat(readyForQueryStatusOf(afterFailure)).isEqualTo('E');

        final List<WireMessage> commit = roundTrip(out, in, "COMMIT;");
        assertThat(commandTagOf(commit)).as("COMMIT of an aborted block is a ROLLBACK").isEqualTo("ROLLBACK");
        assertThat(readyForQueryStatusOf(commit)).isEqualTo('I');
      });
    }

    assertThat(getServerDatabase(0, getDatabaseName()).countType(ABORT_TYPE, true))
        .as("nothing written after the failure inside the block may be committed").isZero();
  }

  // Regression: #8245
  @Test
  @DisplayName("[#8245] COMMIT; inside a block opened with the bare BEGIN ends the block")
  void semicolonCommitEndsABareBlock() throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        assertThat(readyForQueryStatusOf(roundTrip(out, in, "BEGIN"))).isEqualTo('T');
        final List<WireMessage> commit = roundTrip(out, in, "COMMIT;");
        assertThat(commandTagOf(commit)).isEqualTo("COMMIT");
        assertThat(readyForQueryStatusOf(commit)).as("still 'T' after the client's COMMIT; before the fix").isEqualTo('I');
      });
    }
  }

  // Regression: #8245
  @Test
  @DisplayName("[#8245] the simple query protocol also accepts whitespace between the keyword and its ';'")
  void simpleQueryAcceptsWhitespaceBeforeTheSemicolon() throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        for (final String[] step : new String[][] { { "BEGIN ;", "BEGIN", "T" }, { "COMMIT ;", "COMMIT", "I" } }) {
          sendSimpleQuery(out, step[0]);
          final List<WireMessage> response = readUntilReadyForQuery(in);
          assertThat(messageTypesOf(response)).as(step[0]).containsExactly('C', 'Z');
          assertThat(commandTagOf(response)).as(step[0]).isEqualTo(step[1]);
          assertThat(readyForQueryStatusOf(response)).as(step[0]).isEqualTo(step[2].charAt(0));
        }
      });
    }
  }

  // Regression: #8245
  @Test
  @DisplayName("[#8245] normalizeStatementText trims and strips exactly one trailing ';'")
  void normalizeStatementText() {
    assertThat(PostgresNetworkExecutor.normalizeStatementText("COMMIT")).isEqualTo("COMMIT");
    assertThat(PostgresNetworkExecutor.normalizeStatementText("COMMIT;")).isEqualTo("COMMIT");
    assertThat(PostgresNetworkExecutor.normalizeStatementText("\n COMMIT ; \n")).isEqualTo("COMMIT");
    assertThat(PostgresNetworkExecutor.normalizeStatementText("COMMIT;;")).isEqualTo("COMMIT;");
    assertThat(PostgresNetworkExecutor.normalizeStatementText(" ; ")).isEmpty();
    assertThat(PostgresNetworkExecutor.normalizeStatementText("SELECT 'a;'")).isEqualTo("SELECT 'a;'");
  }

  /*
   * Regression tests for issue #8273: transaction-control text that the exact matchers of {@code PostgresNetworkExecutor}
   * miss but the SQL grammar accepts - a leading or trailing comment, a doubled {@code ;}, {@code BEGIN ISOLATION},
   * {@code COMMIT RETRY} - fell through to the engine, which began/committed/rolled back behind the protocol's back:
   * the ReadyForQuery status byte stayed {@code 'I'}, a failure inside the block did not abort it, the statement was
   * answered with a RowDescription and an empty command tag, and in an aborted block a {@code COMMIT; -- done} was
   * refused with 25P02 like any other statement, wedging the connection.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  private static final String TYPE = "Issue8273Probe";

  // { statement sent, tag expected, ReadyForQuery status expected after it }
  private static final String[][] STEPS = {
      { "BEGIN; -- open", "BEGIN", "T" }, { "COMMIT; -- close", "COMMIT", "I" },
      { "/* hi */ BEGIN", "BEGIN", "T" }, { "ROLLBACK; -- x", "ROLLBACK", "I" },
      { "BEGIN -- go", "BEGIN", "T" }, { "COMMIT /*traceparent='00-abc-def-01'*/", "COMMIT", "I" },
      { "BEGIN;;", "BEGIN", "T" }, { "ROLLBACK;;", "ROLLBACK", "I" },
      { "BEGIN ISOLATION REPEATABLE_READ", "BEGIN", "T" }, { "COMMIT;;", "COMMIT", "I" },
      { "BEGIN", "BEGIN", "T" }, { "COMMIT RETRY 3", "COMMIT", "I" } };

  // Regression: #8273
  @Test
  @DisplayName("[#8273] simple protocol: every grammar-only spelling is answered with its bare tag and moves the status byte")
  void simpleProtocolRecognizesGrammarSpellings() throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        for (final String[] step : STEPS) {
          sendSimpleQuery(out, step[0]);
          final List<WireMessage> response = readUntilReadyForQuery(in);
          assertThat(messageTypesOf(response)).as(step[0] + ": no RowDescription/DataRow").containsExactly('C', 'Z');
          assertThat(commandTagOf(response)).as(step[0] + " command tag").isEqualTo(step[1]);
          assertThat(readyForQueryStatusOf(response)).as(step[0] + " ReadyForQuery status").isEqualTo(step[2].charAt(0));
        }
      });
    }
  }

  // Regression: #8273
  @Test
  @DisplayName("[#8273] extended protocol: every grammar-only spelling is NoData + its bare tag and moves the status byte")
  void extendedProtocolRecognizesGrammarSpellings() throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        for (final String[] step : STEPS) {
          final List<WireMessage> response = roundTrip(out, in, step[0]);
          assertThat(messageTypesOf(response)).as(step[0]).containsExactly('1', '2', 'n', 'C', 'Z');
          assertThat(commandTagOf(response)).as(step[0] + " command tag").isEqualTo(step[1]);
          assertThat(readyForQueryStatusOf(response)).as(step[0] + " ReadyForQuery status").isEqualTo(step[2].charAt(0));
        }
      });
    }
  }

  // Regression: #8273
  @Test
  @DisplayName("[#8273] a failure inside a block opened with a commented BEGIN aborts it, and a commented COMMIT ends it")
  void failureInsideACommentedBlockAbortsIt() throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        sendSimpleQuery(out, "CREATE DOCUMENT TYPE " + TYPE + " IF NOT EXISTS");
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).doesNotContain('E');

        sendSimpleQuery(out, "BEGIN; -- open");
        assertThat(readyForQueryStatusOf(readUntilReadyForQuery(in))).isEqualTo('T');

        sendSimpleQuery(out, "SELECT * FROM NoSuchType8273");
        final List<WireMessage> failure = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(failure)).contains('E');
        assertThat(readyForQueryStatusOf(failure)).as("the failure aborts the block").isEqualTo('E');

        sendSimpleQuery(out, "INSERT INTO " + TYPE + " SET tag = 'after-failure-comment'");
        final List<WireMessage> afterFailure = readUntilReadyForQuery(in);
        assertThat(sqlStateOf(afterFailure)).isEqualTo("25P02");

        // THE WEDGE: in an aborted block the commented COMMIT used to be refused with 25P02 like any other statement
        sendSimpleQuery(out, "COMMIT; -- close");
        final List<WireMessage> commit = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(commit)).containsExactly('C', 'Z');
        assertThat(commandTagOf(commit)).as("COMMIT of an aborted block is a ROLLBACK").isEqualTo("ROLLBACK");
        assertThat(readyForQueryStatusOf(commit)).isEqualTo('I');

        // same over the extended protocol
        assertThat(readyForQueryStatusOf(roundTrip(out, in, "/* x */ BEGIN"))).isEqualTo('T');
        assertThat(readyForQueryStatusOf(roundTrip(out, in, "SELECT * FROM NoSuchType8273"))).isEqualTo('E');
        final List<WireMessage> rollback = roundTrip(out, in, "ROLLBACK; -- x");
        assertThat(commandTagOf(rollback)).isEqualTo("ROLLBACK");
        assertThat(readyForQueryStatusOf(rollback)).isEqualTo('I');
      });
    }

    assertThat(getServerDatabase(0, getDatabaseName()).countType(TYPE, true))
        .as("nothing written after the failure inside the block may be committed").isZero();
  }

  // Regression: #8273
  @Test
  @DisplayName("[#8273] a write inside a block opened with a grammar-only BEGIN is committed only by the COMMIT")
  void grammarBeginHoldsTheWriteUntilCommit() throws Exception {
    final String type = TYPE + "Commit";
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        sendSimpleQuery(out, "CREATE DOCUMENT TYPE " + type + " IF NOT EXISTS");
        readUntilReadyForQuery(in);

        sendSimpleQuery(out, "BEGIN ISOLATION READ_COMMITTED -- tx");
        assertThat(readyForQueryStatusOf(readUntilReadyForQuery(in))).isEqualTo('T');
        sendSimpleQuery(out, "INSERT INTO " + type + " SET tag = 'inside'");
        assertThat(readyForQueryStatusOf(readUntilReadyForQuery(in))).isEqualTo('T');
        sendSimpleQuery(out, "ROLLBACK;;");
        assertThat(readyForQueryStatusOf(readUntilReadyForQuery(in))).isEqualTo('I');
      });
    }
    assertThat(getServerDatabase(0, getDatabaseName()).countType(type, true)).as("the ROLLBACK;; discarded the write").isZero();
  }

  // Regression: #8273
  @Test
  @DisplayName("[#8273] BEGIN ISOLATION with an unknown level is refused and opens nothing")
  void unknownIsolationLevelIsRefused() throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        sendSimpleQuery(out, "BEGIN ISOLATION NO_SUCH_LEVEL");
        final List<WireMessage> simple = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(simple)).contains('E');
        assertThat(readyForQueryStatusOf(simple)).isEqualTo('I');

        final List<WireMessage> extended = roundTrip(out, in, "BEGIN ISOLATION NO_SUCH_LEVEL");
        assertThat(messageTypesOf(extended)).contains('E');
        assertThat(readyForQueryStatusOf(extended)).isEqualTo('I');
      });
    }
  }

  /*
   * Regression tests for issue #9631: PostgreSQL's own spellings of the transaction-control statements - {@code START
   * TRANSACTION}, {@code ABORT}, {@code BEGIN <transaction_mode>} (what Go's {@code database/sql} {@code BeginTx} sends
   * when given options) and {@code COMMIT/ROLLBACK AND [NO] CHAIN} - reached the SQL grammar, which refused them with
   * 42601, over both protocols.
   */
  private static final String SPELLING_TYPE = "Issue9631Probe";

  // { statement sent, tag expected, ReadyForQuery status expected after it }
  private static final String[][] SPELLING_STEPS = {
      { "START TRANSACTION", "START TRANSACTION", "T" }, { "ABORT", "ROLLBACK", "I" },
      { "START TRANSACTION ISOLATION LEVEL READ COMMITTED", "START TRANSACTION", "T" }, { "ABORT WORK", "ROLLBACK", "I" },
      { "START TRANSACTION READ WRITE", "START TRANSACTION", "T" }, { "ABORT TRANSACTION", "ROLLBACK", "I" },
      { "BEGIN ISOLATION LEVEL READ COMMITTED", "BEGIN", "T" }, { "COMMIT AND NO CHAIN", "COMMIT", "I" },
      { "begin isolation level repeatable read", "BEGIN", "T" }, { "ROLLBACK AND NO CHAIN", "ROLLBACK", "I" },
      { "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ WRITE", "BEGIN", "T" }, { "END AND NO CHAIN", "COMMIT", "I" },
      { "begin read write", "BEGIN", "T" }, { "abort;", "ROLLBACK", "I" },
      { "BEGIN NOT DEFERRABLE", "BEGIN", "T" }, { "COMMIT WORK AND NO CHAIN", "COMMIT", "I" },
      // the spellings that worked before must still work, ArcadeDB's own grammar spelling included
      { "BEGIN", "BEGIN", "T" }, { "COMMIT", "COMMIT", "I" },
      { "BEGIN WORK", "BEGIN", "T" }, { "ROLLBACK TRANSACTION", "ROLLBACK", "I" },
      { "BEGIN ISOLATION REPEATABLE_READ", "BEGIN", "T" }, { "END", "COMMIT", "I" } };

  // { statement, SQLSTATE expected }: PostgreSQL accepts each one, this server cannot honor it
  private static final String[][] REFUSED = {
      { "BEGIN READ ONLY", "0A000" }, { "BEGIN ISOLATION LEVEL SERIALIZABLE", "0A000" },
      { "START TRANSACTION READ ONLY", "0A000" }, { "START TRANSACTION ISOLATION LEVEL SERIALIZABLE", "0A000" } };

  // Regression: #9631
  @Test
  @DisplayName("[#9631] simple protocol: every PostgreSQL spelling is answered with its tag and moves the status byte")
  void simpleProtocolRecognizesPostgresSpellings() throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        for (final String[] step : SPELLING_STEPS) {
          sendSimpleQuery(out, step[0]);
          final List<WireMessage> response = readUntilReadyForQuery(in);
          assertThat(messageTypesOf(response)).as(step[0] + ": no error, no RowDescription").containsExactly('C', 'Z');
          assertThat(commandTagOf(response)).as(step[0] + " command tag").isEqualTo(step[1]);
          assertThat(readyForQueryStatusOf(response)).as(step[0] + " ReadyForQuery status").isEqualTo(step[2].charAt(0));
        }
      });
    }
  }

  // Regression: #9631
  @Test
  @DisplayName("[#9631] extended protocol: every PostgreSQL spelling is NoData + its tag and moves the status byte")
  void extendedProtocolRecognizesPostgresSpellings() throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        for (final String[] step : SPELLING_STEPS) {
          final List<WireMessage> response = roundTrip(out, in, "", step[0]);
          assertThat(messageTypesOf(response)).as(step[0]).containsExactly('1', '2', 'n', 'C', 'Z');
          assertThat(commandTagOf(response)).as(step[0] + " command tag").isEqualTo(step[1]);
          assertThat(readyForQueryStatusOf(response)).as(step[0] + " ReadyForQuery status").isEqualTo(step[2].charAt(0));
        }
      });
    }
  }

  // Regression: #9631
  @Test
  @DisplayName("[#9631] a cached named START TRANSACTION / ABORT is applied again on every Bind of the statement")
  void namedPreparedStatementsAreReusable() throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        sendParse(out, "start9631", "START TRANSACTION");
        sendParse(out, "abort9631", "ABORT");
        sendSync(out);
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).containsExactly('1', '1', 'Z');

        for (int i = 0; i < 2; i++) {
          final List<WireMessage> begin = bindExecute(out, in, "start9631");
          assertThat(commandTagOf(begin)).isEqualTo("START TRANSACTION");
          assertThat(readyForQueryStatusOf(begin)).as("reuse " + i + " opens the block").isEqualTo('T');
          final List<WireMessage> abort = bindExecute(out, in, "abort9631");
          assertThat(commandTagOf(abort)).isEqualTo("ROLLBACK");
          assertThat(readyForQueryStatusOf(abort)).as("reuse " + i + " closes the block").isEqualTo('I');
        }
      });
    }
  }

  // Regression: #9631
  @Test
  @DisplayName("[#9631] READ ONLY, SERIALIZABLE and AND CHAIN are refused with 0A000, not as syntax errors, and open nothing")
  void unsupportedModesAreRefusedForTheRightReason() throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        for (final String[] refused : REFUSED) {
          sendSimpleQuery(out, refused[0]);
          final List<WireMessage> simple = readUntilReadyForQuery(in);
          assertThat(sqlStateOf(simple)).as(refused[0] + " (simple)").isEqualTo(refused[1]);
          assertThat(readyForQueryStatusOf(simple)).as(refused[0] + " opens no block (simple)").isEqualTo('I');

          final List<WireMessage> extended = roundTrip(out, in, "", refused[0]);
          assertThat(sqlStateOf(extended)).as(refused[0] + " (extended)").isEqualTo(refused[1]);
          assertThat(messageTypesOf(extended)).as(refused[0] + " is refused at Parse").startsWith('E');
          assertThat(readyForQueryStatusOf(extended)).as(refused[0] + " opens no block (extended)").isEqualTo('I');
        }

        // AND CHAIN inside a block: refused, the block is aborted by the refusal, and a ROLLBACK still ends it
        for (final String chain : new String[] { "COMMIT AND CHAIN", "ROLLBACK AND CHAIN", "ABORT AND CHAIN", "END AND CHAIN" }) {
          sendSimpleQuery(out, "BEGIN");
          assertThat(readyForQueryStatusOf(readUntilReadyForQuery(in))).isEqualTo('T');
          sendSimpleQuery(out, chain);
          final List<WireMessage> refusedChain = readUntilReadyForQuery(in);
          assertThat(sqlStateOf(refusedChain)).as(chain).isEqualTo("0A000");
          assertThat(readyForQueryStatusOf(refusedChain)).as(chain + " aborts the block like any failed statement").isEqualTo('E');
          sendSimpleQuery(out, "ABORT");
          final List<WireMessage> abort = readUntilReadyForQuery(in);
          assertThat(commandTagOf(abort)).isEqualTo("ROLLBACK");
          assertThat(readyForQueryStatusOf(abort)).isEqualTo('I');
        }
      });
    }
  }

  // Regression: #9631
  @Test
  @DisplayName("[#9631] ABORT and COMMIT AND NO CHAIN end an aborted block, over both protocols")
  void postgresSpellingsEndAnAbortedBlock() throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        for (final String end : new String[] { "ABORT", "ABORT TRANSACTION", "COMMIT AND NO CHAIN", "ROLLBACK WORK AND NO CHAIN" }) {
          sendSimpleQuery(out, "START TRANSACTION");
          assertThat(readyForQueryStatusOf(readUntilReadyForQuery(in))).isEqualTo('T');
          sendSimpleQuery(out, "SELECT * FROM NoSuchType9631");
          assertThat(readyForQueryStatusOf(readUntilReadyForQuery(in))).isEqualTo('E');
          sendSimpleQuery(out, end);
          final List<WireMessage> simple = readUntilReadyForQuery(in);
          assertThat(messageTypesOf(simple)).as(end + " (simple)").containsExactly('C', 'Z');
          assertThat(commandTagOf(simple)).as("ending an aborted block is a ROLLBACK").isEqualTo("ROLLBACK");
          assertThat(readyForQueryStatusOf(simple)).isEqualTo('I');

          assertThat(readyForQueryStatusOf(roundTrip(out, in, "", "START TRANSACTION"))).isEqualTo('T');
          assertThat(readyForQueryStatusOf(roundTrip(out, in, "", "SELECT * FROM NoSuchType9631"))).isEqualTo('E');
          final List<WireMessage> extended = roundTrip(out, in, "", end);
          assertThat(messageTypesOf(extended)).as(end + " (extended)").doesNotContain('E');
          assertThat(commandTagOf(extended)).isEqualTo("ROLLBACK");
          assertThat(readyForQueryStatusOf(extended)).isEqualTo('I');
        }

        // AND CHAIN in an aborted block cannot be honored, so it is refused like any other statement there, the block
        // stays aborted, and a plain ROLLBACK still recovers the session
        sendSimpleQuery(out, "BEGIN");
        readUntilReadyForQuery(in);
        sendSimpleQuery(out, "SELECT * FROM NoSuchType9631");
        assertThat(readyForQueryStatusOf(readUntilReadyForQuery(in))).isEqualTo('E');
        sendSimpleQuery(out, "COMMIT AND CHAIN");
        final List<WireMessage> chain = readUntilReadyForQuery(in);
        assertThat(sqlStateOf(chain)).isEqualTo("25P02");
        assertThat(readyForQueryStatusOf(chain)).isEqualTo('E');
        sendSimpleQuery(out, "ROLLBACK");
        final List<WireMessage> recovered = readUntilReadyForQuery(in);
        assertThat(commandTagOf(recovered)).isEqualTo("ROLLBACK");
        assertThat(readyForQueryStatusOf(recovered)).isEqualTo('I');
        sendSimpleQuery(out, "SELECT 1");
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).doesNotContain('E');
      });
    }
  }

  // Regression: #9631
  @Test
  @DisplayName("[#9631] START TRANSACTION holds writes until COMMIT AND NO CHAIN; ABORT discards them")
  void postgresSpellingsDriveTheEngineTransaction() throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        sendSimpleQuery(out, "CREATE DOCUMENT TYPE " + SPELLING_TYPE + " IF NOT EXISTS");
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).doesNotContain('E');

        sendSimpleQuery(out, "START TRANSACTION ISOLATION LEVEL READ COMMITTED");
        readUntilReadyForQuery(in);
        sendSimpleQuery(out, "INSERT INTO " + SPELLING_TYPE + " SET tag = 'discarded'");
        assertThat(readyForQueryStatusOf(readUntilReadyForQuery(in))).isEqualTo('T');
        sendSimpleQuery(out, "ABORT");
        assertThat(readyForQueryStatusOf(readUntilReadyForQuery(in))).isEqualTo('I');

        sendSimpleQuery(out, "BEGIN ISOLATION LEVEL READ COMMITTED READ WRITE");
        readUntilReadyForQuery(in);
        sendSimpleQuery(out, "INSERT INTO " + SPELLING_TYPE + " SET tag = 'kept'");
        assertThat(readyForQueryStatusOf(readUntilReadyForQuery(in))).isEqualTo('T');
        sendSimpleQuery(out, "COMMIT AND NO CHAIN");
        assertThat(readyForQueryStatusOf(readUntilReadyForQuery(in))).isEqualTo('I');
      });
    }

    assertThat(getServerDatabase(0, getDatabaseName()).countType(SPELLING_TYPE, true)).as("only the committed write is there").isEqualTo(1);
  }

  // Regression: #9631
  @Test
  @DisplayName("[#9631] BEGIN ISOLATION LEVEL REPEATABLE READ opens a repeatable-read transaction")
  void beginIsolationLevelIsHonored() throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        sendSimpleQuery(out, "BEGIN ISOLATION LEVEL REPEATABLE READ");
        assertThat(readyForQueryStatusOf(readUntilReadyForQuery(in))).isEqualTo('T');
        assertThat(show(out, in, "transaction_isolation")).isEqualTo("repeatable read");
        sendSimpleQuery(out, "ABORT");
        readUntilReadyForQuery(in);

        assertThat(readyForQueryStatusOf(roundTrip(out, in, "", "START TRANSACTION ISOLATION LEVEL REPEATABLE READ"))).isEqualTo('T');
        assertThat(show(out, in, "transaction_isolation")).isEqualTo("repeatable read");
        sendSimpleQuery(out, "ROLLBACK");
        readUntilReadyForQuery(in);

        sendSimpleQuery(out, "START TRANSACTION ISOLATION LEVEL READ COMMITTED");
        readUntilReadyForQuery(in);
        assertThat(show(out, in, "transaction_isolation")).isEqualTo("read committed");
        sendSimpleQuery(out, "ROLLBACK");
        readUntilReadyForQuery(in);
      });
    }
  }

  /**
   * One Parse/Bind/Describe('P')/Execute/Sync round trip on the unnamed portal.
   */
  private static List<WireMessage> roundTrip(final DataOutputStream out, final DataInputStream in, final String statement,
      final String query) throws Exception {
    sendParse(out, statement, query);
    sendBind(out, "", statement);
    sendDescribe(out, 'P', "");
    sendExecute(out, "");
    sendSync(out);
    return readUntilReadyForQuery(in);
  }

  private static List<WireMessage> bindExecute(final DataOutputStream out, final DataInputStream in, final String statement)
      throws Exception {
    sendBind(out, "", statement);
    sendExecute(out, "");
    sendSync(out);
    return readUntilReadyForQuery(in);
  }

  /*
   * Issue #11588 over the Postgres extended protocol: CALL { ... } IN TRANSACTIONS keeps running in batches in the implicit
   * transaction block {@code beginImplicitTransactionBlock} opens around a pipeline, and is refused once a client BEGIN has
   * joined that block and made it the client's, whose ROLLBACK could not undo the batches.
   */
  private static final String STATEMENT = "{cypher} UNWIND range(1, 25) AS i CALL { WITH i CREATE (:%s {i: i}) } IN TRANSACTIONS OF 10 ROWS";

  // Regression: #11588
  @Test
  void runsInBatchesInTheImplicitBlockOfAPipeline() throws Exception {
    final String label = "PgImplicit11588";
    withConnection((out, in) -> {
      parseBindExecute(out, "c", STATEMENT.formatted(label));
      sendSync(out);

      final List<WireMessage> answers = readUntilReadyForQuery(in);
      assertThat(messageTypesOf(answers)).as(errorMessage(answers)).doesNotContain('E');
      assertThat(readyForQueryStatusOf(answers)).isEqualTo('I');
    });

    assertThat(count(label)).isEqualTo(25);
  }

  // Regression: #11588
  @Test
  void refusedOnceAClientBeginJoinsTheImplicitBlock() throws Exception {
    final String label = "PgJoined11588";
    final String type = "PgJoinedDoc11588";
    withConnection((out, in) -> {
      sendSimpleQuery(out, "CREATE DOCUMENT TYPE " + type + " IF NOT EXISTS");
      assertThat(messageTypesOf(readUntilReadyForQuery(in))).doesNotContain('E');

      // The write opens the implicit block, the BEGIN joins it and declares it the client's
      parseBindExecute(out, "w", "INSERT INTO " + type + " SET id = 1");
      parseBindExecute(out, "b", "BEGIN");
      parseBindExecute(out, "c", STATEMENT.formatted(label));
      sendSync(out);

      final List<WireMessage> answers = readUntilReadyForQuery(in);
      assertThat(messageTypesOf(answers)).containsOnlyOnce('E');
      assertThat(errorMessage(answers)).contains("can only be executed in an implicit transaction");
      // the client's transaction block is the cause: active_sql_transaction, a client error (issue #12161)
      assertThat(errorField(answers, 'C')).isEqualTo("25001");

      sendSimpleQuery(out, "ROLLBACK");
      assertThat(readyForQueryStatusOf(readUntilReadyForQuery(in))).isEqualTo('I');
    });

    assertThat(count(label)).isZero();
    assertThat(getServerDatabase(0, getDatabaseName()).countType(type, false)).isZero();
  }

  private static String errorMessage(final List<WireMessage> answers) {
    return errorField(answers, 'M');
  }

  private static String errorField(final List<WireMessage> answers, final char field) {
    for (final WireMessage message : answers)
      if (message.type() == 'E')
        return errorFields(message).get(field);
    return "";
  }

  private long count(final String label) {
    return getServerDatabase(0, getDatabaseName()).query("opencypher", "MATCH (x:" + label + ") RETURN count(x) AS c").next()
        .<Number>getProperty("c").longValue();
  }

  /*
   * Regression test for issue #7096: on the Postgres wire port an {@code UPDATE} against a vertex type failed with
   * "Transaction not active" unless the client had opened an explicit transaction, while the same statement against a
   * document or an edge type, and every {@code INSERT}/{@code CREATE VERTEX}/{@code DELETE}, ran fine in autocommit.
   * Autocommit is the JDBC default and what Spark's Postgres connector uses for one-shot DML, so the asymmetry made
   * vertex updates unusable from those clients.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  private Connection getConnectionWithoutSsl() throws SQLException, ClassNotFoundException {
    Class.forName("org.postgresql.Driver");
    final Properties props = new Properties();
    props.setProperty("user", "root");
    props.setProperty("password", DEFAULT_PASSWORD_FOR_TESTS);
    props.setProperty("ssl", "false");
    return DriverManager.getConnection(getServerPostgresJdbcUrl(), props);
  }

  // Regression: #7096
  @Test
  @DisplayName("[#7096] a scalar vertex UPDATE runs in autocommit, exactly like a document UPDATE")
  void scalarVertexUpdateInAutocommit() throws Exception {
    try (final Connection conn = getConnectionWithoutSsl(); final Statement st = conn.createStatement()) {
      assertThat(conn.getAutoCommit()).isTrue();
      st.execute("CREATE VERTEX TYPE Character7096 IF NOT EXISTS");
      st.execute("CREATE VERTEX Character7096 SET name = 'Napoleon'");

      st.execute("UPDATE Character7096 SET name = 'Bonaparte' WHERE name = 'Napoleon'");

      try (final ResultSet rs = st.executeQuery("SELECT name FROM Character7096")) {
        assertThat(rs.next()).isTrue();
        assertThat(rs.getString("name")).isEqualTo("Bonaparte");
        assertThat(rs.next()).isFalse();
      }
    }
  }

  // Regression: #7096
  @Test
  @DisplayName("[#7096] a vertex UPDATE assigning LIST and MAP literals runs in autocommit")
  void containerVertexUpdateInAutocommit() throws Exception {
    try (final Connection conn = getConnectionWithoutSsl(); final Statement st = conn.createStatement()) {
      st.execute("CREATE VERTEX TYPE Character7096 IF NOT EXISTS");
      st.execute("CREATE VERTEX Character7096 SET name = 'Napoleon'");

      st.execute("UPDATE Character7096 SET aliases = ['Emperor','Bonaparte'], attrs = {'title':'Emperor','nation':'France'} "
          + "WHERE name = 'Napoleon'");

      try (final ResultSet rs = st.executeQuery("SELECT aliases, attrs FROM Character7096 WHERE name = 'Napoleon'")) {
        assertThat(rs.next()).isTrue();
        assertThat(rs.getString("aliases")).contains("Emperor").contains("Bonaparte");
        assertThat(rs.getString("attrs")).contains("France");
      }
    }
  }

  // Regression: #7096
  @Test
  @DisplayName("[#7096] the same vertex UPDATE through the extended protocol (prepared statement) in autocommit")
  void preparedVertexUpdateInAutocommit() throws Exception {
    try (final Connection conn = getConnectionWithoutSsl(); final Statement st = conn.createStatement()) {
      st.execute("CREATE VERTEX TYPE Character7096 IF NOT EXISTS");
      st.execute("CREATE VERTEX Character7096 SET name = 'Napoleon'");

      try (final PreparedStatement ps = conn.prepareStatement("UPDATE Character7096 SET name = ? WHERE name = ?")) {
        ps.setString(1, "Bonaparte");
        ps.setString(2, "Napoleon");
        assertThat(ps.executeUpdate()).isEqualTo(1);
      }

      try (final ResultSet rs = st.executeQuery("SELECT name FROM Character7096")) {
        assertThat(rs.next()).isTrue();
        assertThat(rs.getString("name")).isEqualTo("Bonaparte");
      }
    }
  }

  // Regression: #7096
  @Test
  @DisplayName("[#7096] a vertex UPDATE followed by an edge UPDATE and a DELETE, all in autocommit on one connection")
  void mixedDmlInAutocommit() throws Exception {
    try (final Connection conn = getConnectionWithoutSsl(); final Statement st = conn.createStatement()) {
      st.execute("CREATE VERTEX TYPE Character7096 IF NOT EXISTS");
      st.execute("CREATE EDGE TYPE Appearance7096 IF NOT EXISTS");
      st.execute("CREATE VERTEX Character7096 SET name = 'Napoleon'");
      st.execute("CREATE VERTEX Character7096 SET name = 'Myriel'");
      st.execute("CREATE EDGE Appearance7096 FROM (SELECT FROM Character7096 WHERE name = 'Napoleon') "
          + "TO (SELECT FROM Character7096 WHERE name = 'Myriel') SET weight = 1");

      st.execute("UPDATE Character7096 SET rank = 1 WHERE name = 'Napoleon'");
      st.execute("UPDATE Appearance7096 SET weight = 2");
      st.execute("UPDATE Character7096 SET rank = 2");
      st.execute("DELETE FROM Character7096 WHERE name = 'Myriel'");

      try (final ResultSet rs = st.executeQuery("SELECT name, rank FROM Character7096")) {
        assertThat(rs.next()).isTrue();
        assertThat(rs.getString("name")).isEqualTo("Napoleon");
        assertThat(rs.getInt("rank")).isEqualTo(2);
        assertThat(rs.next()).isFalse();
      }
    }
  }

  /*
   * A transaction-control statement on the simple query protocol returns no rows, so PostgreSQL answers it with the
   * bare {@code CommandComplete} tag. A {@code RowDescription} in front of the tag, even with zero fields, makes libpq
   * classify the result as {@code PGRES_TUPLES_OK} instead of {@code PGRES_COMMAND_OK}: {@code psql} prints an empty
   * table after {@code BEGIN}, and a client that checks the status when it opens a transaction (the Arrow PostgreSQL
   * ADBC driver, whose DB-API connections start with autocommit off) fails with
   * {@code [libpq] Failed to begin transaction}.
   * <p>
   * {@code ROLLBACK TO <savepoint>} is the one exception (issue #7846): unlike {@code BEGIN}/{@code COMMIT}/
   * {@code ROLLBACK}/{@code SAVEPOINT}/{@code RELEASE}, which this server can honor and so may answer with a
   * bare {@code CommandComplete}, it has no savepoint checkpoint to roll back to - answering success there
   * would silently keep writes the client asked to discard, so it is refused instead.
   */
  @Test
  @DisplayName("BEGIN, COMMIT and ROLLBACK on the simple query protocol are answered with CommandComplete only")
  void transactionControlIsAnsweredWithTheBareCommandTag() throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
        for (final String[] step : new String[][] { { "BEGIN", "BEGIN", "T" }, { "COMMIT", "COMMIT", "I" },
            { "BEGIN", "BEGIN", "T" }, { "ROLLBACK", "ROLLBACK", "I" } }) {
          sendSimpleQuery(out, step[0]);
          final List<WireMessage> response = readUntilReadyForQuery(in);
          assertThat(messageTypesOf(response)).as(step[0] + " returns no rows, so no RowDescription").doesNotContain('T');
          assertThat(messageTypesOf(response)).as(step[0] + " returns no rows, so no DataRow").doesNotContain('D');
          assertThat(messageTypesOf(response)).as(step[0] + " is acknowledged by its command tag").contains('C');
          assertThat(bareCommandTagOf(response)).isEqualTo(step[1]);
          assertThat(readyForQueryStatusOf(response)).isEqualTo(step[2].charAt(0));
        }

        // A statement that does return rows keeps its RowDescription, inside and outside a transaction.
        sendSimpleQuery(out, "BEGIN");
        readUntilReadyForQuery(in);
        sendSimpleQuery(out, "SELECT 1 AS one");
        final List<WireMessage> select = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(select)).contains('T', 'D', 'C');
        sendSimpleQuery(out, "COMMIT");
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).doesNotContain('T');
      });
    }
  }

  @Test
  @DisplayName("SAVEPOINT and RELEASE are answered with CommandComplete only")
  void savepointStatementsAreAnsweredWithTheBareCommandTag() throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
        sendSimpleQuery(out, "BEGIN");
        readUntilReadyForQuery(in);
        for (final String[] step : new String[][] { { "SAVEPOINT sp1", "SAVEPOINT" }, { "RELEASE sp1", "RELEASE" } }) {
          sendSimpleQuery(out, step[0]);
          final List<WireMessage> response = readUntilReadyForQuery(in);
          assertThat(messageTypesOf(response)).as(step[0] + " returns no rows, so no RowDescription").doesNotContain('T');
          assertThat(messageTypesOf(response)).as(step[0] + " is acknowledged by its command tag").contains('C');
          assertThat(bareCommandTagOf(response)).isEqualTo(step[1]);
          assertThat(readyForQueryStatusOf(response)).as(step[0] + " leaves the transaction open").isEqualTo('T');
        }
        sendSimpleQuery(out, "COMMIT");
        readUntilReadyForQuery(in);
      });
    }
  }

  @Test
  @DisplayName("[#7846] ROLLBACK TO is refused, not answered with CommandComplete, and aborts the transaction")
  void rollbackToIsRefusedRatherThanAcceptedAsANoOp() throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
        sendSimpleQuery(out, "BEGIN");
        readUntilReadyForQuery(in);
        sendSimpleQuery(out, "SAVEPOINT sp1");
        readUntilReadyForQuery(in);

        // This server has no savepoint checkpoint to roll back to: a CommandComplete here would tell the
        // client its rollback succeeded while every write made since the savepoint is still pending.
        sendSimpleQuery(out, "ROLLBACK TO sp1");
        final List<WireMessage> response = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(response)).as("ROLLBACK TO is refused, not silently accepted").contains('E');
        assertThat(messageTypesOf(response)).as("a refused statement is not acknowledged by a command tag").doesNotContain('C');
        assertThat(readyForQueryStatusOf(response)).as("the transaction is left aborted").isEqualTo('E');

        // Once aborted, every statement but COMMIT/ROLLBACK/END is refused (issue #6457) - including a
        // client that ignores the ErrorResponse and tries to keep using the transaction.
        sendSimpleQuery(out, "SELECT 1");
        assertThat(readyForQueryStatusOf(readUntilReadyForQuery(in))).isEqualTo('E');

        sendSimpleQuery(out, "ROLLBACK");
        readUntilReadyForQuery(in);
      });
    }
  }

  @Test
  @DisplayName("[#7846] ROLLBACK TO outside an explicit transaction is refused without wedging the session")
  void rollbackToOutsideAnExplicitTransactionIsRefusedButLeavesTheSessionIdle() throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
        // No BEGIN: setErrorInTx() is a no-op without an explicit transaction (explicitTransactionStarted ==
        // false), matching every other refused statement in autocommit mode - there is nothing pending to
        // lose, so the session must not be left wedged in an aborted state the client can never end.
        sendSimpleQuery(out, "ROLLBACK TO sp1");
        final List<WireMessage> response = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(response)).as("ROLLBACK TO is refused even in autocommit mode").contains('E');
        assertThat(readyForQueryStatusOf(response)).as("autocommit mode is left idle, not aborted").isEqualTo('I');

        sendSimpleQuery(out, "SELECT 1");
        final List<WireMessage> select = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(select)).as("the session still accepts statements").contains('T', 'D', 'C');
      });
    }
  }

  @Test
  @DisplayName("A language-prefixed {sql}BEGIN still gets the bare BEGIN command tag")
  void languagePrefixedTransactionControlGetsTheRightCommandTag() throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
        sendSimpleQuery(out, "{sql}BEGIN");
        final List<WireMessage> begin = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(begin)).doesNotContain('T');
        assertThat(bareCommandTagOf(begin)).isEqualTo("BEGIN");
        sendSimpleQuery(out, "{sql}COMMIT");
        final List<WireMessage> commit = readUntilReadyForQuery(in);
        assertThat(bareCommandTagOf(commit)).isEqualTo("COMMIT");
      });
    }
  }

  /**
   * Issue #7905: the fix above covered only the simple ('Q') protocol. The extended one - Parse/Bind/Describe/
   * Execute, which is what libpq's {@code PQexecParams}/{@code PQexecPrepared} send, and with them psycopg3,
   * asyncpg and the Arrow ADBC driver this whole class is named after - still answered BEGIN/COMMIT/ROLLBACK
   * with a zero-field {@code RowDescription}, because Parse gave those three an empty materialized result set
   * whose (empty, but non-null) column map {@code describeCommand()} then announced. That is the same byte, and
   * so the same libpq status defect, reached from the protocol those clients actually use.
   * <p>
   * SAVEPOINT/RELEASE/SET were already answered {@code NoData} on this path, so the file used to answer the same
   * class of statement two different ways depending on the keyword.
   */
  @Test
  @DisplayName("[#7905] BEGIN, COMMIT and ROLLBACK on the EXTENDED protocol answer NoData, not a zero-field RowDescription")
  void transactionControlOnTheExtendedProtocolAnswersNoData() throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
        for (final String[] step : new String[][] { { "BEGIN", "BEGIN", "T" }, { "SAVEPOINT sp1", "SAVEPOINT", "T" },
            { "COMMIT", "COMMIT", "I" }, { "BEGIN", "BEGIN", "T" }, { "ROLLBACK", "ROLLBACK", "I" } }) {
          sendParse(out, "", step[0]);
          sendBind(out, "", "");
          sendDescribe(out, 'P', "");
          sendExecute(out, "");
          sendSync(out);

          final List<WireMessage> response = readUntilReadyForQuery(in);
          assertThat(messageTypesOf(response)).as(step[0] + " is parsed and bound").contains('1', '2');
          assertThat(messageTypesOf(response))
              .as("%s returns no result set, so Describe('P') owes NoData - a 'T' of zero fields makes libpq "
                  + "report PGRES_TUPLES_OK", step[0])
              .doesNotContain('T');
          assertThat(messageTypesOf(response)).as(step[0] + " answers exactly one NoData").containsOnlyOnce('n');
          assertThat(messageTypesOf(response)).as(step[0] + " returns no rows, so no DataRow").doesNotContain('D');
          assertThat(bareCommandTagOf(response)).as("the command tag is unchanged by the Describe fix").isEqualTo(step[1]);
          assertThat(readyForQueryStatusOf(response)).as(step[0] + " moves the transaction state as before")
              .isEqualTo(step[2].charAt(0));
        }

        // A statement that DOES return rows still gets its RowDescription on the same connection and the same
        // protocol, so the fix narrowed nothing but the three keywords.
        sendParse(out, "", "SELECT 1 AS one");
        sendBind(out, "", "");
        sendDescribe(out, 'P', "");
        sendExecute(out, "");
        sendSync(out);
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).contains('T', 'D', 'C');
      });
    }
  }

  /**
   * The recovery portal an aborted block builds for a COMMIT/END/ROLLBACK sent through Parse (issue #6548) is a
   * transaction-control statement too, and took the same empty-result-set path, so it answered the same
   * zero-field RowDescription (issue #7905).
   */
  @Test
  @DisplayName("[#7905] the ROLLBACK that recovers an aborted block over the extended protocol answers NoData too")
  void theAbortedBlockRecoveryPortalAnswersNoDataAsWell() throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
        sendSimpleQuery(out, "BEGIN");
        readUntilReadyForQuery(in);
        // Aborts the block: every statement but COMMIT/ROLLBACK/END is refused from here on.
        sendSimpleQuery(out, "SELEKT bogus");
        assertThat(readyForQueryStatusOf(readUntilReadyForQuery(in))).isEqualTo('E');

        sendParse(out, "", "COMMIT");
        sendBind(out, "", "");
        sendDescribe(out, 'P', "");
        sendExecute(out, "");
        sendSync(out);

        final List<WireMessage> response = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(response)).as("the recovery statement is accepted").contains('1');
        assertThat(messageTypesOf(response)).as("Describe('P') answers NoData, and exactly once").containsOnlyOnce('n');
        assertThat(messageTypesOf(response)).doesNotContain('T');
        assertThat(bareCommandTagOf(response)).as("a COMMIT of an aborted block is tagged ROLLBACK").isEqualTo("ROLLBACK");
        assertThat(readyForQueryStatusOf(response)).as("the block is over").isEqualTo('I');
      });
    }
  }

  private static String bareCommandTagOf(final List<WireMessage> messages) {
    final WireMessage complete = messages.stream().filter(m -> m.type() == 'C').findFirst()
        .orElseThrow(() -> new AssertionError("expected a CommandComplete"));
    final byte[] body = complete.body();
    return new String(body, 0, body.length - 1, StandardCharsets.UTF_8);
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

  /**
   * One Parse/Bind/Describe('P')/Execute/Sync round trip on the unnamed statement and portal.
   */
  private static List<WireMessage> roundTrip(final DataOutputStream out, final DataInputStream in, final String query)
      throws Exception {
    sendParse(out, "", query);
    sendBind(out, "", "");
    sendDescribe(out, 'P', "");
    sendExecute(out, "");
    sendSync(out);
    return readUntilReadyForQuery(in);
  }

  private static String commandTagOf(final List<WireMessage> messages) {
    for (final WireMessage message : messages) {
      if (message.type() == 'C') {
        final byte[] body = message.body();
        final int end = body.length > 0 && body[body.length - 1] == 0 ? body.length - 1 : body.length;
        return new String(body, 0, end, StandardCharsets.UTF_8);
      }
    }
    throw new AssertionError("No CommandComplete message found in " + messageTypesOf(messages));
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

  /**
   * Parse/Bind/Execute without a Describe: a transaction-control portal answers Describe('P') with NoData, and
   * what these tests assert on is the CommandComplete tag and what the database ends up holding.
   */
  private static void parseBindExecute(final DataOutputStream out, final String name, final String query) throws Exception {
    sendParse(out, name, query);
    sendBind(out, name, name);
    sendExecute(out, name);
  }
}
