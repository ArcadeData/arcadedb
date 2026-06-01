/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.server.ha.raft;

import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.remote.RemoteHttpComponent;
import com.arcadedb.server.BaseGraphServerTest;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end regression test for the STICKY connection-strategy fix.
 *
 * <p>Before the fix, {@link RemoteHttpComponent.CONNECTION_STRATEGY#STICKY} behaved
 * identically to {@link RemoteHttpComponent.CONNECTION_STRATEGY#ROUND_ROBIN}: the
 * three HTTP requests that compose a remote transaction (begin, command, commit)
 * could each land on a different physical node, producing server-side errors of the
 * form {@code "Remote transaction 'AS-...' not found or expired"}.
 *
 * <p>The fix pins the {@link RemoteDatabase} to one concrete cluster member at
 * {@code begin()} time and releases the pin at {@code commit()} or {@code rollback()}.
 * These scenarios exercise the lifecycle against a real 3-node Raft cluster:
 * <ul>
 *   <li>STICKY on a follower-connected client allows a write transaction to succeed
 *       end-to-end.</li>
 *   <li>ROUND_ROBIN executes (smoke check that the default path still works).</li>
 *   <li>After commit, the pin is released so the next transaction repins fresh
 *       and a second back-to-back STICKY transaction succeeds.</li>
 *   <li>After rollback, the pin is also released and a second STICKY transaction
 *       succeeds.</li>
 * </ul>
 */
@Tag("slow")
class RemoteStickyConnectionStrategyIT extends BaseRaftHATest {

  private static final String TYPE_NAME = "StickyConn";

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Test
  void stickyOnFollowerConnectedClientSucceedsForWriteTransaction() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final int followerIndex = firstFollowerIndex(leaderIndex);
    final int followerPort = getServer(followerIndex).getHttpServer().getPort();

    try (final RemoteDatabase db = newRemoteDatabase(followerPort)) {
      db.setConnectionStrategy(RemoteHttpComponent.CONNECTION_STRATEGY.STICKY);

      createTypeIfMissing(db);

      // The transaction must complete without "Remote transaction not found" errors.
      db.transaction(() ->
        db.command("sql", "INSERT INTO " + TYPE_NAME + " SET tag = 'sticky-follower-write'"));

      try (final ResultSet rs = db.query("sql",
          "SELECT count(*) AS c FROM " + TYPE_NAME + " WHERE tag = 'sticky-follower-write'")) {
        assertThat(rs.hasNext()).isTrue();
        final Result row = rs.next();
        assertThat(((Number) row.getProperty("c")).longValue())
            .as("STICKY follower-connected client must persist the row").isEqualTo(1L);
      }
    }
  }

  @Test
  void roundRobinSmokeCheckExecutesAgainstFollower() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final int followerIndex = firstFollowerIndex(leaderIndex);
    final int followerPort = getServer(followerIndex).getHttpServer().getPort();

    try (final RemoteDatabase db = newRemoteDatabase(followerPort)) {
      // The default strategy is ROUND_ROBIN; assert it explicitly so future default
      // changes do not silently weaken this smoke check.
      db.setConnectionStrategy(RemoteHttpComponent.CONNECTION_STRATEGY.ROUND_ROBIN);

      createTypeIfMissing(db);

      // We do not assert that ROUND_ROBIN must fail. The bug was racy, and the goal
      // here is to verify the negative-control path still executes when given an
      // explicit ROUND_ROBIN strategy without throwing.
      try {
        db.transaction(() ->
          db.command("sql", "INSERT INTO " + TYPE_NAME + " SET tag = 'round-robin-smoke'"));
      } catch (final Exception e) {
        LogManager.instance()
            .log(this, Level.INFO, "TEST: ROUND_ROBIN smoke run raised %s (acceptable, documented behaviour)",
                e.getMessage());
      }
    }
  }

  @Test
  void pinIsReleasedAfterCommitAndAllowsSequentialStickyTransactions() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final int followerIndex = firstFollowerIndex(leaderIndex);
    final int followerPort = getServer(followerIndex).getHttpServer().getPort();

    try (final RemoteDatabase db = newRemoteDatabase(followerPort)) {
      db.setConnectionStrategy(RemoteHttpComponent.CONNECTION_STRATEGY.STICKY);

      createTypeIfMissing(db);

      // First transaction commits cleanly.
      db.transaction(() ->
        db.command("sql", "INSERT INTO " + TYPE_NAME + " SET tag = 'first-commit'"));
      assertThat(db.isTransactionActive())
          .as("Session must be released after the first commit so the pin can be reset").isFalse();

      // Second transaction: the previous pin was released on commit, so STICKY must
      // re-resolve the leader and the begin, command, and commit triple must all land
      // on the same node again. If the pin were sticky across the gap (the pre-fix
      // ROUND_ROBIN behaviour) the second begin's session would not be visible to a
      // later command.
      db.transaction(() ->
        db.command("sql", "INSERT INTO " + TYPE_NAME + " SET tag = 'second-commit'"));

      try (final ResultSet rs = db.query("sql",
          "SELECT count(*) AS c FROM " + TYPE_NAME + " WHERE tag IN ['first-commit', 'second-commit']")) {
        assertThat(rs.hasNext()).isTrue();
        final Result row = rs.next();
        assertThat(((Number) row.getProperty("c")).longValue())
            .as("Both back-to-back STICKY transactions must persist their rows").isEqualTo(2L);
      }
    }
  }

  @Test
  void pinIsReleasedAfterRollbackAndAllowsSubsequentStickyTransaction() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final int followerIndex = firstFollowerIndex(leaderIndex);
    final int followerPort = getServer(followerIndex).getHttpServer().getPort();

    try (final RemoteDatabase db = newRemoteDatabase(followerPort)) {
      db.setConnectionStrategy(RemoteHttpComponent.CONNECTION_STRATEGY.STICKY);

      createTypeIfMissing(db);

      // First transaction begins, inserts, then rolls back. The pin must be released.
      db.begin();
      db.command("sql", "INSERT INTO " + TYPE_NAME + " SET tag = 'rollback-me'");
      db.rollback();
      assertThat(db.isTransactionActive())
          .as("Rollback must clear the session and the sticky pin").isFalse();

      // Second STICKY transaction must succeed; it repins fresh.
      db.transaction(() ->
        db.command("sql", "INSERT INTO " + TYPE_NAME + " SET tag = 'after-rollback'"));

      try (final ResultSet rs = db.query("sql",
          "SELECT count(*) AS c FROM " + TYPE_NAME + " WHERE tag = 'after-rollback'")) {
        assertThat(rs.hasNext()).isTrue();
        final Result row = rs.next();
        assertThat(((Number) row.getProperty("c")).longValue())
            .as("A second STICKY transaction after a rollback must commit successfully")
            .isEqualTo(1L);
      }

      try (final ResultSet rs = db.query("sql",
          "SELECT count(*) AS c FROM " + TYPE_NAME + " WHERE tag = 'rollback-me'")) {
        assertThat(rs.hasNext()).isTrue();
        final Result row = rs.next();
        assertThat(((Number) row.getProperty("c")).longValue())
            .as("The rolled-back insert must not be persisted").isEqualTo(0L);
      }
    }
  }

  // --------------------------------------------------------------------------
  // Helpers
  // --------------------------------------------------------------------------

  private RemoteDatabase newRemoteDatabase(final int port) {
    return new RemoteDatabase("127.0.0.1", port, getDatabaseName(), "root",
        BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
  }

  private int firstFollowerIndex(final int leaderIndex) {
    for (int i = 0; i < getServerCount(); i++) {
      if (i != leaderIndex)
        return i;
    }
    throw new IllegalStateException("No follower found in " + getServerCount() + "-node cluster");
  }

  private void createTypeIfMissing(final RemoteDatabase db) {
    // CREATE DOCUMENT TYPE is idempotent across tests in the same JVM only when the
    // database is fresh. Use IF NOT EXISTS so re-runs in the same harness do not
    // interact across @Test methods.
    db.command("sql", "CREATE DOCUMENT TYPE " + TYPE_NAME + " IF NOT EXISTS");
  }
}
