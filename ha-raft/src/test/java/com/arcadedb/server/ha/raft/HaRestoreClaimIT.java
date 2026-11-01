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
package com.arcadedb.server.ha.raft;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.exception.DatabaseNotAvailableException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.server.ArcadeDBServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The claim an {@code overwrite} restore takes on its target database in an HA cluster, end to end on ONE three-node cluster
 * shared by every test ({@link SharedClusterRaftHATest}). Merged from {@code Issue10881RestoreWaitsForReplicatingCommitIT} and
 * {@code Issue10974RestoreHoldRefusesReplicaCommitIT}, which ran on the same fixture: three servers, the stock configuration and
 * the stock populated database.
 * <ul>
 * <li><b>#10881</b>: the claim refuses writes and waits for the commits in flight (#10689), but an HA leader commit leaves the
 * database lock after phase 1 and goes on to quorum and publication with no lock held. The wait took the exclusive lock only, so
 * it returned under a commit already replicating, which the swap then destroyed on every node. The commit is held between phase
 * 1 and its dispatch to Raft with {@link RaftReplicatedDatabase#TEST_PRE_DISPATCH_HOOK}, and the claim is taken on the leader the
 * way {@code ServerControlPlane.reserveRestoreTarget} takes it.</li>
 * <li><b>#10974</b>: the claim on the leader holds the database for the writes of the other nodes until it is released: a commit
 * made on a replica meanwhile is refused instead of acknowledged and then destroyed by the swap, and goes through again once the
 * claim is released.</li>
 * </ul>
 * Why these share a cluster: every claim is taken on the fixture database, and {@link #releaseClaimAndHook()} releases it on
 * every running node after EVERY test, pass or fail, and clears the dispatch hook, exactly as the sources' {@code @AfterEach}
 * did; the release also ends the write refusal and the HA hold ({@code ArcadeDBServer.releaseDatabaseNameReservedForRestore}),
 * so the next test finds the database writable. Each test creates and counts its own type named through
 * {@link #uniqueName(String)}, and the in-flight assertions are taken once the previous test's commits have all completed (its
 * commit and claim futures are awaited, its executor shut down). No test stops, restarts or isolates a node, moves leadership or
 * changes a setting.
 * <p>
 * Siblings, each with the fixture its test needs: {@link HaRestoreClaimReplicatedDropIT} (#8035, no fixture database and a long
 * backup wait) and {@link HaRestoreClaimTwoNodeIT} (#8363, two servers).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class HaRestoreClaimIT extends SharedClusterRaftHATest {

  private static final String TYPE_NAME_10881 = "Issue10881";
  private static final String TYPE_NAME_10974 = "Issue10974";

  @Override
  protected int getServerCount() {
    return 3;
  }

  @AfterEach
  void releaseClaimAndHook() {
    RaftReplicatedDatabase.TEST_PRE_DISPATCH_HOOK = null;
    for (int i = 0; i < getServerCount(); i++)
      try {
        if (getServer(i) != null && getServer(i).isStarted())
          getServer(i).releaseDatabaseNameReservedForRestore(getDatabaseName());
      } catch (final Exception e) {
        LogManager.instance().log(this, Level.WARNING, "TEST: could not release the restore claim on server %d", e, i);
      }
  }

  // ------------------------------------------------------------------ the claim waits for a replicating commit (#10881)

  /** Issue #10881, from {@code Issue10881RestoreWaitsForReplicatingCommitIT}. */
  @Test
  void theRestoreClaimWaitsForALeaderCommitAlreadyReplicating() throws Exception {
    final String typeName = uniqueName(TYPE_NAME_10881);
    final int leaderIndex = requireLeaderIndex();
    final ArcadeDBServer leader = getServer(leaderIndex);
    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());
    leaderDb.transaction(() -> leaderDb.getSchema().createDocumentType(typeName));
    waitForAllServers();

    final CountDownLatch replicating = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    final AtomicInteger fired = new AtomicInteger();
    RaftReplicatedDatabase.TEST_PRE_DISPATCH_HOOK = databaseName -> {
      if (!getDatabaseName().equals(databaseName) || fired.getAndIncrement() > 0)
        return;
      replicating.countDown();
      try {
        release.await(60, TimeUnit.SECONDS);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    };

    final ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      // Past phase 1, and so past the write refusal's check, and on its way to quorum with no lock held
      final Future<?> commit = executor.submit(() -> leaderDb.transaction(() -> leaderDb.newDocument(typeName).set("id", 1).save()));
      assertThat(replicating.await(60, TimeUnit.SECONDS)).as("the commit reached the replication step").isTrue();
      assertThat(localOf(leaderDb).getCommitsInFlight()).as("the replicating commit is registered as in flight").isEqualTo(1);

      // The restore claims the target: writes are refused from now on, and the commits in flight are awaited
      leader.reserveDatabaseNameForRestore(getDatabaseName());
      final Future<?> claim = executor.submit(() -> leader.awaitCommitsInFlightOn(getDatabaseName()));

      // Expected to time out: before the fix the claim returned here, and the swap destroyed the commit acknowledged below
      assertThatThrownBy(() -> claim.get(1, TimeUnit.SECONDS)).as("the claim must wait for the replicating commit")
          .isInstanceOf(TimeoutException.class);

      // A commit that starts now does not pass the refusal
      assertThatThrownBy(() -> leaderDb.transaction(() -> leaderDb.newDocument(typeName).set("id", 2).save()))
          .isInstanceOf(DatabaseNotAvailableException.class);

      release.countDown();
      commit.get(60, TimeUnit.SECONDS);
      claim.get(60, TimeUnit.SECONDS);
      assertThat(localOf(leaderDb).getCommitsInFlight()).isZero();
    } finally {
      release.countDown();
      executor.shutdownNow();
      // On the shared cluster the claim thread must be done before releaseClaimAndHook() runs: one still inside
      // awaitCommitsInFlightOn could take the HA hold AFTER the release and leave the fixture database held for the next test
      executor.awaitTermination(60, TimeUnit.SECONDS);
    }

    assertThat(countOf(leaderDb, typeName)).as("the acknowledged commit is on the leader, only the one").isEqualTo(1L);
  }

  /** Issue #10881, from {@code Issue10881RestoreWaitsForReplicatingCommitIT}. */
  @Test
  void aSchemaSessionIsInFlightUntilItsEntryIsShipped() throws Exception {
    final String typeName = uniqueName(TYPE_NAME_10881 + "Ddl");
    final int leaderIndex = requireLeaderIndex();
    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());
    final LocalDatabase local = localOf(leaderDb);
    assertThat(local.getCommitsInFlight()).isZero();

    final AtomicInteger inFlightInsideTheSession = new AtomicInteger(-1);
    ((DatabaseInternal) leaderDb).getWrappedDatabaseInstance().recordFileChanges(() -> {
      inFlightInsideTheSession.set(local.getCommitsInFlight());
      leaderDb.getSchema().createDocumentType(typeName);
      return null;
    });

    assertThat(inFlightInsideTheSession.get()).as("the schema session registers itself before its callback runs").isEqualTo(1);
    assertThat(local.getCommitsInFlight()).as("and ends once its entry is shipped").isZero();
    waitForAllServers();
    for (int i = 0; i < getServerCount(); i++)
      assertThat(getServerDatabase(i, getDatabaseName()).getSchema().existsType(typeName)).isTrue();
  }

  // ------------------------------------------------------------------ the hold refuses a replica's commit (#10974)

  /** Issue #10974, from {@code Issue10974RestoreHoldRefusesReplicaCommitIT}. */
  @Test
  void aReplicaCommitIsRefusedWhileTheLeaderHoldsTheDatabaseForARestore() {
    final String typeName = uniqueName(TYPE_NAME_10974);
    final int leaderIndex = requireLeaderIndex();
    final int followerIndex = leaderIndex == 0 ? 1 : 0;
    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());
    leaderDb.transaction(() -> leaderDb.getSchema().createDocumentType(typeName));
    waitForAllServers();

    final Database followerDb = getServerDatabase(followerIndex, getDatabaseName());
    followerDb.transaction(() -> followerDb.newDocument(typeName).set("id", 0).save());
    waitForAllServers();

    getServer(leaderIndex).reserveDatabaseNameForRestore(getDatabaseName());
    getServer(leaderIndex).awaitCommitsInFlightOn(getDatabaseName());
    final RaftHAServer leaderRaft = ((RaftHAPlugin) getServer(leaderIndex).getHA()).getRaftHAServer();
    assertThat(leaderRaft.getStateMachine().isHeldForRestore(getDatabaseName())).isTrue();

    assertThatThrownBy(() -> followerDb.transaction(() -> followerDb.newDocument(typeName).set("id", 1).save(), false, 1))
        .isInstanceOf(NeedRetryException.class);

    getServer(leaderIndex).releaseDatabaseNameReservedForRestore(getDatabaseName());
    assertThat(leaderRaft.getStateMachine().isHeldForRestore(getDatabaseName())).isFalse();

    followerDb.transaction(() -> followerDb.newDocument(typeName).set("id", 2).save());
    waitForAllServers();
    for (int i = 0; i < getServerCount(); i++)
      assertThat(countOf(getServerDatabase(i, getDatabaseName()), typeName)).as("server " + i + " holds the commits made outside the claim")
          .isEqualTo(2L);
  }

  private static LocalDatabase localOf(final Database db) {
    return (LocalDatabase) ((DatabaseInternal) db).getEmbedded();
  }

  private static long countOf(final Database db, final String typeName) {
    try (final ResultSet rs = db.query("sql", "SELECT count(@rid) AS cnt FROM " + typeName)) {
      return rs.next().<Number>getProperty("cnt").longValue();
    }
  }
}
