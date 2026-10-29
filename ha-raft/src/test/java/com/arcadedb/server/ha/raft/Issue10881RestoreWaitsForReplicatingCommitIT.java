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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10881: an {@code overwrite} restore refuses writes on its target and waits for the commits in flight (#10689), but an
 * HA leader commit leaves the database lock after phase 1 and goes on to quorum, publication and acknowledgement with no lock
 * held. The wait took the exclusive lock only, so it returned under a commit already replicating, which was then acknowledged
 * and destroyed by the swap on every node.
 * <p>
 * The commit is held between phase 1 and its dispatch to Raft with {@link RaftReplicatedDatabase#TEST_PRE_DISPATCH_HOOK}, and
 * the restore's claim is taken on the leader the way {@code ServerControlPlane.reserveRestoreTarget} takes it.
 */
class Issue10881RestoreWaitsForReplicatingCommitIT extends BaseRaftHATest {

  private static final String TYPE_NAME = "Issue10881";

  @Override
  protected int getServerCount() {
    return 3;
  }

  @AfterEach
  void clearHook() {
    RaftReplicatedDatabase.TEST_PRE_DISPATCH_HOOK = null;
    for (int i = 0; i < getServerCount(); i++)
      if (getServer(i) != null && getServer(i).isStarted())
        getServer(i).releaseDatabaseNameReservedForRestore(getDatabaseName());
  }

  @Test
  void theRestoreClaimWaitsForALeaderCommitAlreadyReplicating() throws Exception {
    final int leaderIndex = requireLeaderIndex();
    final ArcadeDBServer leader = getServer(leaderIndex);
    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());
    leaderDb.transaction(() -> leaderDb.getSchema().createDocumentType(TYPE_NAME));
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
      final Future<?> commit = executor.submit(() -> leaderDb.transaction(() -> leaderDb.newDocument(TYPE_NAME).set("id", 1).save()));
      assertThat(replicating.await(60, TimeUnit.SECONDS)).as("the commit reached the replication step").isTrue();
      assertThat(localOf(leaderDb).getCommitsInFlight()).as("the replicating commit is registered as in flight").isEqualTo(1);

      // The restore claims the target: writes are refused from now on, and the commits in flight are awaited
      leader.reserveDatabaseNameForRestore(getDatabaseName());
      final Future<?> claim = executor.submit(() -> leader.awaitCommitsInFlightOn(getDatabaseName()));

      // Expected to time out: before the fix the claim returned here, and the swap destroyed the commit acknowledged below
      assertThatThrownBy(() -> claim.get(1, TimeUnit.SECONDS)).as("the claim must wait for the replicating commit")
          .isInstanceOf(TimeoutException.class);

      // A commit that starts now does not pass the refusal
      assertThatThrownBy(() -> leaderDb.transaction(() -> leaderDb.newDocument(TYPE_NAME).set("id", 2).save()))
          .isInstanceOf(DatabaseNotAvailableException.class);

      release.countDown();
      commit.get(60, TimeUnit.SECONDS);
      claim.get(60, TimeUnit.SECONDS);
      assertThat(localOf(leaderDb).getCommitsInFlight()).isZero();
    } finally {
      release.countDown();
      executor.shutdownNow();
    }

    assertThat(countOf(leaderDb)).as("the acknowledged commit is on the leader, only the one").isEqualTo(1L);
  }

  @Test
  void aSchemaSessionIsInFlightUntilItsEntryIsShipped() throws Exception {
    final int leaderIndex = requireLeaderIndex();
    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());
    final LocalDatabase local = localOf(leaderDb);
    assertThat(local.getCommitsInFlight()).isZero();

    final AtomicInteger inFlightInsideTheSession = new AtomicInteger(-1);
    ((DatabaseInternal) leaderDb).getWrappedDatabaseInstance().recordFileChanges(() -> {
      inFlightInsideTheSession.set(local.getCommitsInFlight());
      leaderDb.getSchema().createDocumentType(TYPE_NAME + "Ddl");
      return null;
    });

    assertThat(inFlightInsideTheSession.get()).as("the schema session registers itself before its callback runs").isEqualTo(1);
    assertThat(local.getCommitsInFlight()).as("and ends once its entry is shipped").isZero();
    waitForAllServers();
    for (int i = 0; i < getServerCount(); i++)
      assertThat(getServerDatabase(i, getDatabaseName()).getSchema().existsType(TYPE_NAME + "Ddl")).isTrue();
  }

  private static LocalDatabase localOf(final Database db) {
    return (LocalDatabase) ((DatabaseInternal) db).getEmbedded();
  }

  private static long countOf(final Database db) {
    try (final ResultSet rs = db.query("sql", "SELECT count(@rid) AS cnt FROM " + TYPE_NAME)) {
      return rs.next().<Number>getProperty("cnt").longValue();
    }
  }
}
