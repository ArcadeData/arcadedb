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

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #7958: a snapshot install that runs OFF the Raft apply thread lost every entry this node
 * applied between the leader serving the snapshot and the install swapping it in.
 * <p>
 * The off-thread installs (the full resync {@code triggerSnapshotDownload} that {@code retryBootstrapInstall} drives,
 * the targeted resync, the operator resync) download the leader's copy with the live database still open, and the
 * apply thread kept applying committed entries to that live copy meanwhile. The swap then replaced it with the
 * leader's copy as of the moment it was served, and the entries applied in between were gone: the applied index
 * had already moved past them, so nothing re-applied them, and nothing logged anything. A type created in that
 * window existed on every other node and never on this one - the #7259 {@code BoltFollowerWrite} divergence.
 * <p>
 * Each test pauses the install after the snapshot is staged, commits a new type and records on the leader, waits for
 * the follower's apply thread to reach them (before the fix it applied them to the copy about to be discarded; with it,
 * it waits on the install lock), releases the install, checks that the swap really happened, and then asserts the
 * follower has them.
 */
class RaftSnapshotInstallConcurrentApplyIT extends BaseRaftHATest {

  /** A hang detector for the wait on the follower's apply thread, not a latency bound. */
  private static final long APPLY_WAIT_SECONDS = 60;

  @Override
  protected int getServerCount() {
    // 3 nodes so the leader still has a majority while the follower's apply thread is held back.
    return 3;
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_SNAPSHOT_INSTALL_RETRIES, 1);
    config.setValue(GlobalConfiguration.HA_SNAPSHOT_INSTALL_RETRY_BASE_MS, 100L);
  }

  @AfterEach
  void clearSeam() {
    SnapshotInstaller.snapshotStagedForTesting = null;
    SnapshotInstaller.swapProgressForTesting = null;
    ArcadeStateMachine.applyWaitsForInstallForTesting = null;
  }

  @Test
  @Timeout(180)
  void operatorResyncKeepsEntriesCommittedWhileTheSnapshotDownloads() throws Exception {
    runScenario("OperatorResync", follower -> getRaftPlugin(follower).getRaftHAServer().getStateMachine()
        .resyncDatabaseFromLeader(getDatabaseName()));
  }

  @Test
  @Timeout(180)
  void fullResyncKeepsEntriesCommittedWhileTheSnapshotDownloads() throws Exception {
    // The path of the #7259 report: retryBootstrapInstall -> triggerSnapshotDownload on the lifecycle executor.
    runScenario("FullResync", follower -> getRaftPlugin(follower).getRaftHAServer().getStateMachine()
        .triggerSnapshotDownload());
  }

  private interface Install {
    void run(int followerIndex) throws Exception;
  }

  private void runScenario(final String prefix, final Install install) throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final int followerIndex = (leaderIndex + 1) % getServerCount();
    final String dbName = getDatabaseName();
    final String baseType = prefix + "Base";
    final String lateType = prefix + "WrittenDuringInstall";

    final Database leaderDb = getServerDatabase(leaderIndex, dbName);
    leaderDb.transaction(() -> {
      leaderDb.getSchema().createVertexType(baseType);
      for (int i = 0; i < 10; i++)
        leaderDb.newVertex(baseType).set("index", i).save();
    });
    assertClusterConsistency();

    final CountDownLatch staged = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    final AtomicBoolean fired = new AtomicBoolean();
    final AtomicBoolean installerHeldGate = new AtomicBoolean();
    final ArcadeStateMachine followerMachine = getRaftPlugin(followerIndex).getRaftHAServer().getStateMachine();
    SnapshotInstaller.snapshotStagedForTesting = name -> {
      if (!dbName.equals(name) || !fired.compareAndSet(false, true))
        return;
      // What makes the install's own pre-install Raft log purge stand down instead of waiting on the apply thread.
      installerHeldGate.set(followerMachine.isHoldingInstallApplyGate());
      staged.countDown();
      try {
        release.await(120, TimeUnit.SECONDS);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    };

    // The apply thread reaching the held install lock: proof the late entries arrived while the install was running,
    // not after it had already released (CodeRabbit on PR #8456).
    final CountDownLatch applyWaited = new CountDownLatch(1);
    ArcadeStateMachine.applyWaitsForInstallForTesting = name -> {
      if (dbName.equals(name))
        applyWaited.countDown();
    };
    // triggerSnapshotDownload logs a failed install rather than throwing it, so success is read off the swap itself:
    // a copy that was never replaced would receive the late entries normally and pass every data assertion below.
    final Set<String> swapPhases = ConcurrentHashMap.newKeySet();
    SnapshotInstaller.swapProgressForTesting = point -> {
      if (point.indexOf(':') < 0)
        swapPhases.add(point);
    };

    final AtomicReference<Throwable> installFailure = new AtomicReference<>();
    final Thread installer = new Thread(() -> {
      try {
        install.run(followerIndex);
      } catch (final Throwable t) {
        installFailure.set(t);
      }
    }, "issue7958-install");
    installer.start();

    try {
      assertThat(staged.await(60, TimeUnit.SECONDS)).as("the follower's install must reach the staged snapshot").isTrue();

      // The leader has served its snapshot; this write is not in it.
      leaderDb.transaction(() -> {
        leaderDb.getSchema().createVertexType(lateType);
        for (int i = 0; i < 5; i++)
          leaderDb.newVertex(lateType).set("index", i).save();
      });

      // Before the fix the follower's apply thread applied these entries here, to the copy the swap was about to
      // discard. With it, the apply thread reaches the install lock and waits.
      assertThat(applyWaited.await(APPLY_WAIT_SECONDS, TimeUnit.SECONDS))
          .as("the follower must reach the late entries while the install still holds the database's install lock")
          .isTrue();
    } finally {
      release.countDown();
      installer.join(120_000);
    }

    assertThat(installer.isAlive()).as("the install must terminate").isFalse();
    assertThat(installFailure.get()).as("the install must succeed").isNull();
    assertThat(installerHeldGate.get()).as("the install must hold the database's install lock across the download").isTrue();
    assertThat(followerMachine.isHoldingInstallApplyGate()).as("a thread running no install holds no install lock").isFalse();
    assertThat(swapPhases).as("the staged snapshot must have been swapped in, not rolled back")
        .contains("INSTALLED").doesNotContain("ROLLING_BACK", "RESTORING");

    assertClusterConsistency();

    final Database followerDb = getServerDatabase(followerIndex, dbName);
    assertThat(followerDb.getSchema().existsType(lateType))
        .as("the type the leader created while the follower was installing its snapshot must exist on the follower")
        .isTrue();
    assertThat(followerDb.countType(lateType, true)).isEqualTo(5);
    assertThat(followerDb.countType(baseType, true)).isEqualTo(10);

    // Replication keeps working on the installed copy.
    leaderDb.transaction(() -> leaderDb.newVertex(lateType).set("index", 5).save());
    assertClusterConsistency();
    assertThat(getServerDatabase(followerIndex, dbName).countType(lateType, true)).isEqualTo(6);
  }
}
