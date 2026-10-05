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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #8454: an off-apply-thread snapshot install trusted the leader's copy to carry every entry
 * the follower had already applied.
 * <p>
 * The #7958 install lock holds the follower's apply thread off the database from before the download to the swap,
 * so nothing the follower applies AFTER asking for the leader's copy is lost. What it applied BEFORE asking is only
 * safe if the leader's copy has it too, and the leader publishes an entry's pages on its own apply thread, which can
 * trail the follower's. A copy served in that window lacked the entry, the follower's applied index was already past
 * it, and the swap dropped it silently.
 * <p>
 * The test opens the window deterministically: it holds the LEADER's apply thread off the database (through the
 * leader's own install lock), commits a record the two followers apply, and runs the operator resync on one of them.
 * Before the fix the follower installed the leader's copy without the record and never got it back; with it, the
 * follower refuses the copy as behind what it applied, the leader's apply thread is released, and the retry installs
 * a copy that has it.
 */
class Issue8454SnapshotSourceBehindFollowerIT extends BaseRaftHATest {

  @Override
  protected int getServerCount() {
    // 3 nodes so the entry commits on the two followers while the leader's apply thread is held back.
    return 3;
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_SNAPSHOT_INSTALL_RETRIES, 5);
    config.setValue(GlobalConfiguration.HA_SNAPSHOT_INSTALL_RETRY_BASE_MS, 200L);
  }

  @AfterEach
  void clearSeam() {
    SnapshotInstaller.sourceBehindForTesting = null;
    SnapshotInstaller.swapProgressForTesting = null;
  }

  @Test
  @Timeout(240)
  void operatorResyncRefusesALeaderCopyBehindWhatTheFollowerApplied() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final int followerIndex = (leaderIndex + 1) % getServerCount();
    final String dbName = getDatabaseName();
    final String type = "Issue8454Record";

    final Database leaderDb = getServerDatabase(leaderIndex, dbName);
    leaderDb.transaction(() -> {
      leaderDb.getSchema().createVertexType(type);
      for (int i = 0; i < 10; i++)
        leaderDb.newVertex(type).set("id", i).save();
    });
    assertClusterConsistency();

    // Hold the leader's apply thread off the database: the install apply gate is exactly the lock its apply thread takes
    // before applying an entry for it. The gate alone, not runUnderInstallGate: that also registers the database as being
    // replaced, and since #8022 a leader refuses every transaction on such a database, so the entry below would never
    // reach the followers.
    final ArcadeStateMachine leaderMachine = getRaftPlugin(leaderIndex).getRaftHAServer().getStateMachine();
    final CountDownLatch leaderHeld = new CountDownLatch(1);
    final CountDownLatch releaseLeader = new CountDownLatch(1);
    final Thread leaderGateHolder = new Thread(() -> {
      final ArcadeStateMachine.InstallApplyGate gate = leaderMachine.installApplyGate(dbName);
      gate.lock();
      try {
        leaderHeld.countDown();
        releaseLeader.await(180, TimeUnit.SECONDS);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      } finally {
        gate.unlock();
      }
    }, "issue8454-leader-gate");
    leaderGateHolder.start();

    final AtomicBoolean refusedBehindCopy = new AtomicBoolean();
    SnapshotInstaller.sourceBehindForTesting = name -> {
      if (dbName.equals(name) && refusedBehindCopy.compareAndSet(false, true))
        // The follower has just refused the copy behind it: let the leader apply the entry, so the retry finds it.
        releaseLeader.countDown();
    };
    final AtomicReference<String> swapOutcome = new AtomicReference<>();
    SnapshotInstaller.swapProgressForTesting = point -> {
      if ("INSTALLED".equals(point) || "ROLLING_BACK".equals(point))
        swapOutcome.compareAndSet(null, point);
    };

    final AtomicReference<Throwable> writeFailure = new AtomicReference<>();
    Thread writer = null;
    final AtomicReference<Throwable> installFailure = new AtomicReference<>();
    Thread installer = null;
    try {
      assertThat(leaderHeld.await(60, TimeUnit.SECONDS)).as("the test must hold the leader's install lock").isTrue();

      // Committed by the two followers while the leader's apply thread cannot apply it. The leader-side commit waits
      // for its own apply thread to publish the pages, so it runs on its own thread.
      writer = new Thread(() -> {
        try {
          leaderDb.transaction(() -> leaderDb.newVertex(type).set("id", 10).save());
        } catch (final Throwable t) {
          writeFailure.set(t);
        }
      }, "issue8454-writer");
      writer.start();

      assertThat(awaitCountOn(followerIndex, type, 11)).as("the follower must apply the entry the leader has not")
          .isEqualTo(11);
      assertThat(refusedBehindCopy.get()).isFalse();

      final ArcadeStateMachine followerMachine = getRaftPlugin(followerIndex).getRaftHAServer().getStateMachine();
      installer = new Thread(() -> {
        try {
          followerMachine.resyncDatabaseFromLeader(dbName);
        } catch (final Throwable t) {
          installFailure.set(t);
        }
      }, "issue8454-install");
      installer.start();
      installer.join(150_000);
    } finally {
      releaseLeader.countDown();
      leaderGateHolder.join(60_000);
      if (installer != null)
        installer.join(60_000);
      if (writer != null)
        writer.join(60_000);
    }

    assertThat(installer.isAlive()).as("the install must terminate").isFalse();
    assertThat(installFailure.get()).as("the install must succeed").isNull();
    assertThat(swapOutcome.get()).as("the leader's copy must have been swapped in").isEqualTo("INSTALLED");
    assertThat(writeFailure.get()).as("the write must commit").isNull();

    // Before the fix the follower swapped in a copy without the 11th record, its applied index was already past it,
    // and nothing ever applied it again.
    assertThat(awaitCountOn(followerIndex, type, 11)).as("the follower must keep the entry it applied before the install")
        .isEqualTo(11);
    assertThat(refusedBehindCopy.get()).as("the follower must refuse the copy served behind the entry it applied")
        .isTrue();
    assertClusterConsistency();

    // Replication keeps working on the installed copy.
    leaderDb.transaction(() -> leaderDb.newVertex(type).set("id", 11).save());
    assertThat(awaitCountOn(followerIndex, type, 12)).isEqualTo(12);
  }
}
