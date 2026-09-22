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
package com.arcadedb.remote;

import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression for issue #8062, the follow-up to #7916: {@code RemoteDatabase.transaction(txBlock, join, attempts, ...)}
 * is the THIRD retry loop of that shape and was the one left without the partial-commit guard.
 * <p>
 * A block containing a statement with an explicit batch boundary - {@code UPDATE/DELETE/MOVE VERTEX ... BATCH n} -
 * commits part of its work mid-execution and re-begins straight afterwards. The rollback the loop performs before the
 * next attempt cannot take that part back, and nothing the remote client holds can see that it happened: its whole
 * view of the transaction is the session id, which does not change. The second attempt used to apply the already
 * durable half a SECOND time and then report clean success.
 * <p>
 * The two embedded loops detect this locally with {@code TransactionContext.isPartiallyCommitted(...)}, which the
 * remote client cannot call because it holds no {@code TransactionContext}. The server evaluates that same predicate
 * on the session's transaction and reports it in the {@link RemoteDatabase#ARCADEDB_SESSION_PARTIAL_COMMIT} response
 * header, which is what these tests drive end to end over a real HTTP server.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8062RemoteRetryDoesNotReplayCommittedHalfIT extends BaseGraphServerTest {
  private static final String DATABASE_NAME = "remote-database-8062";
  private static final String ROWS          = "Issue8062Row";
  private static final int    SIZE          = 25;
  private static final int    ATTEMPTS      = 3;

  @Override
  protected boolean isCreateDatabases() {
    return false;
  }

  /**
   * The defect itself: with a BATCH boundary crossed before the conflict, the retry used to double-apply the rows
   * already published. Now the block is not re-run at all, so every row is incremented exactly once, and the conflict
   * reaches the caller instead of being swallowed into a clean return.
   */
  @Test
  void aRemoteBlockThatCommittedPartOfItsWorkIsNotReplayed() {
    try (final RemoteDatabase db = remoteDatabase()) {
      populate(db);

      final AtomicInteger attempts = new AtomicInteger();
      final AtomicInteger okCalls = new AtomicInteger();

      assertThatThrownBy(() -> db.transaction(() -> {
        attempts.incrementAndGet();
        // Commits every 10 of the 25 rows: 20 are durable by the time the statement ends.
        db.command("sql", "UPDATE " + ROWS + " SET seq = seq + 1 BATCH 10");
        // ... and then the block fails, the way a genuine MVCC conflict on a later statement would.
        throw new ConcurrentModificationException("simulated conflict after the batch boundary");
      }, true, ATTEMPTS, okCalls::incrementAndGet, null)).isInstanceOf(NeedRetryException.class);

      assertThat(attempts.get()).as("the block must be run ONCE: its durable half cannot be replayed").isEqualTo(1);
      assertThat(okCalls.get()).as("and a block that failed must not report success").isZero();

      assertThat(countRowsWithSeq(db, 1)).as("the 20 rows the batch published are incremented exactly once").isEqualTo(20);
      assertThat(countRowsWithSeq(db, 0)).as("the remaining 5 were rolled back").isEqualTo(5);
      assertThat(countRowsWithSeq(db, 2)).as("no row may be incremented twice").isZero();
    }
  }

  /**
   * {@code DELETE ... BATCH n} goes through the same {@code BatchStep}, so it earns the same refusal. Driven through
   * its own entry point rather than assumed from the UPDATE above.
   */
  @Test
  void aRemoteBatchedDeleteThatCommittedPartOfItsWorkIsNotReplayed() {
    try (final RemoteDatabase db = remoteDatabase()) {
      populate(db);

      final AtomicInteger attempts = new AtomicInteger();

      assertThatThrownBy(() -> db.transaction(() -> {
        attempts.incrementAndGet();
        db.command("sql", "DELETE FROM " + ROWS + " BATCH 10");
        throw new ConcurrentModificationException("simulated conflict after the batch boundary");
      }, true, ATTEMPTS, null, null)).isInstanceOf(NeedRetryException.class);

      assertThat(attempts.get()).isEqualTo(1);
      assertThat(countRowsWithSeq(db, 0)).as("20 rows are durably gone, the last 5 deletes were rolled back").isEqualTo(5);
    }
  }

  /**
   * The guard must not disturb the ordinary case it sits next to: a block that publishes NOTHING before failing is
   * still retried the full number of attempts, because the rollback really did take everything back. This is the test
   * that fails if the header is latched on the wrong request or never cleared by {@code begin()}.
   */
  @Test
  void aRemoteBlockThatCommittedNothingIsStillRetried() {
    try (final RemoteDatabase db = remoteDatabase()) {
      populate(db);

      final AtomicInteger attempts = new AtomicInteger();

      assertThatThrownBy(() -> db.transaction(() -> {
        attempts.incrementAndGet();
        db.command("sql", "UPDATE " + ROWS + " SET seq = seq + 1");
        throw new ConcurrentModificationException("simulated conflict with nothing published");
      }, true, ATTEMPTS, null, null)).isInstanceOf(NeedRetryException.class);

      assertThat(attempts.get()).as("nothing was durable, so all the attempts are still available").isEqualTo(ATTEMPTS);
      assertThat(countRowsWithSeq(db, 0)).as("and every attempt's rollback took its work back").isEqualTo(SIZE);
    }
  }

  /**
   * And the verdict does not survive the transaction it was reached in: a block that crosses a batch boundary and
   * SUCCEEDS leaves the next transaction on the same client free to retry as it always could. Without the reset in
   * {@code begin()} the latch set here would silently disable retries for the rest of the connection's life.
   */
  @Test
  void aLaterTransactionOnTheSameClientStillRetries() {
    try (final RemoteDatabase db = remoteDatabase()) {
      populate(db);

      db.transaction(() -> db.command("sql", "UPDATE " + ROWS + " SET seq = seq + 1 BATCH 10").close());
      assertThat(countRowsWithSeq(db, 1)).isEqualTo(SIZE);

      final AtomicInteger attempts = new AtomicInteger();
      assertThatThrownBy(() -> db.transaction(() -> {
        attempts.incrementAndGet();
        throw new ConcurrentModificationException("simulated conflict in a brand new transaction");
      }, true, ATTEMPTS, null, null)).isInstanceOf(NeedRetryException.class);

      assertThat(attempts.get()).as("the new transaction published nothing, so its attempts are intact")
          .isEqualTo(ATTEMPTS);
    }
  }

  /**
   * The server half of the fix, driven on the wire rather than inferred from the client's behaviour: the
   * {@code /command} response of a statement with a BATCH boundary, run inside a session, carries the header -
   * and the {@code /commit} that ends the same session does NOT, although committing is exactly what moves the
   * counter the header is derived from (see {@code DatabaseAbstractHandler.reportsSessionPartialCommit()}).
   */
  @Test
  void theCommandResponseCarriesTheHeaderAndTheCommitResponseDoesNot() throws Exception {
    try (final RemoteDatabase db = remoteDatabase()) {
      populate(db);
    }

    final String sessionId = post("begin/" + DATABASE_NAME, null, null)
        .getHeaderField(RemoteDatabase.ARCADEDB_SESSION_ID);
    assertThat(sessionId).as("the /begin response names the session").isNotNull();

    final HttpURLConnection batched = post("command/" + DATABASE_NAME,
        "{\"language\":\"sql\",\"command\":\"UPDATE " + ROWS + " SET seq = seq + 1 BATCH 10\"}", sessionId.trim());
    assertThat(batched.getResponseCode()).isEqualTo(200);
    assertThat(batched.getHeaderField(RemoteDatabase.ARCADEDB_SESSION_PARTIAL_COMMIT))
        .as("the batch boundary published 20 of the 25 rows under the caller's transaction").isEqualTo("true");

    final HttpURLConnection committed = post("commit/" + DATABASE_NAME, null, sessionId.trim());
    assertThat(committed.getResponseCode()).isEqualTo(204);
    assertThat(committed.getHeaderField(RemoteDatabase.ARCADEDB_SESSION_PARTIAL_COMMIT))
        .as("committing in full is not a PARTIAL commit, however much it moves the counter").isNull();
  }

  /** A plain command OUTSIDE any session can never carry the header: there is no caller transaction to report on. */
  @Test
  void aSessionLessCommandNeverCarriesTheHeader() throws Exception {
    try (final RemoteDatabase db = remoteDatabase()) {
      populate(db);
    }

    final HttpURLConnection batched = post("command/" + DATABASE_NAME,
        "{\"language\":\"sql\",\"command\":\"UPDATE " + ROWS + " SET seq = seq + 1 BATCH 10\"}", null);
    assertThat(batched.getResponseCode()).isEqualTo(200);
    assertThat(batched.getHeaderField(RemoteDatabase.ARCADEDB_SESSION_PARTIAL_COMMIT)).isNull();
  }

  private HttpURLConnection post(final String path, final String body, final String sessionId) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) URI.create(
        "http://127.0.0.1:" + getServerHttpPort() + "/api/v1/" + path).toURL().openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization", "Basic " + Base64.getEncoder()
        .encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)));
    if (sessionId != null)
      connection.setRequestProperty(RemoteDatabase.ARCADEDB_SESSION_ID, sessionId);
    if (body != null) {
      connection.setRequestProperty("Content-Type", "application/json");
      connection.setDoOutput(true);
      connection.getOutputStream().write(body.getBytes(StandardCharsets.UTF_8));
    }
    connection.connect();
    // Force the response to be read so the headers are populated whatever the status.
    connection.getResponseCode();
    return connection;
  }

  private void populate(final RemoteDatabase db) {
    // The database is created fresh for every test, so no IF NOT EXISTS guard is needed here.
    db.command("sql", "CREATE DOCUMENT TYPE " + ROWS).close();
    db.command("sql", "CREATE PROPERTY " + ROWS + ".seq INTEGER").close();
    db.transaction(() -> {
      for (int i = 0; i < SIZE; i++)
        db.command("sql", "INSERT INTO " + ROWS + " SET seq = 0").close();
    });
  }

  private long countRowsWithSeq(final RemoteDatabase db, final int seq) {
    try (final ResultSet rs = db.query("sql", "SELECT count(*) AS c FROM " + ROWS + " WHERE seq = ?", seq)) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }

  private RemoteDatabase remoteDatabase() {
    return new RemoteDatabase("127.0.0.1", getServerHttpPort(), DATABASE_NAME, "root",
        BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
  }

  @BeforeEach
  public void beginTest() {
    super.beginTest();
    final RemoteServer server = new RemoteServer("127.0.0.1", getServerHttpPort(), "root",
        BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
    if (server.exists(DATABASE_NAME))
      server.drop(DATABASE_NAME);
    server.create(DATABASE_NAME);
  }

  @AfterEach
  public void endTest() {
    final RemoteServer server = new RemoteServer("127.0.0.1", getServerHttpPort(), "root",
        BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
    if (server.exists(DATABASE_NAME))
      server.drop(DATABASE_NAME);
    super.endTest();
  }
}
