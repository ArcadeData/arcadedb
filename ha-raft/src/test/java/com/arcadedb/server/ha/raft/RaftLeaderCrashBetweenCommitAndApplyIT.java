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
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.log.LogManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Fault-injection test for the narrow crash window between Raft replication and phase-2 apply.
 * After Raft commits the entry (followers have it), the leader crashes before {@code commit2ndPhase()}
 * runs locally. This test verifies that:
 * <ol>
 *   <li>Surviving followers retain the injected record (Raft durability holds)</li>
 *   <li>A new leader is elected from the surviving nodes</li>
 *   <li>The restarted old leader recovers via Raft log replay (origin-skip does not fire
 *       during replay because {@code isLeader()} returns false)</li>
 *   <li>All 3 nodes converge to identical state (no double-apply corruption)</li>
 * </ol>
 *
 * <p>The fault is injected via {@link RaftReplicatedDatabase#TEST_POST_REPLICATION_HOOK},
 * which fires after Raft replication succeeds but before phase-2 runs.
 */
@Tag("slow")
class RaftLeaderCrashBetweenCommitAndApplyIT extends BaseRaftHATest {

  private static final String VERTEX_TYPE = "CrashBetween";

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_QUORUM, "majority");
  }

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Override
  protected boolean persistentRaftStorage() {
    return true;
  }

  @AfterEach
  void clearPostReplicationHook() {
    RaftReplicatedDatabase.TEST_POST_REPLICATION_HOOK = null;
  }

  @Test
  void oldLeaderRecoversMissingWriteAfterCrashBetweenReplicationAndPhase2() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);

    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());

    // Phase 1: baseline writes with no fault injection.
    leaderDb.transaction(() -> {
      if (!leaderDb.getSchema().existsType(VERTEX_TYPE))
        leaderDb.getSchema().createVertexType(VERTEX_TYPE);
    });
    leaderDb.transaction(() -> {
      for (int i = 0; i < 100; i++) {
        final MutableVertex v = leaderDb.newVertex(VERTEX_TYPE);
        v.set("phase", "baseline");
        v.set("name", "baseline-" + i);
        v.save();
      }
    });
    assertClusterConsistency();

    // Phase 2: arm the fault-injection hook. On the next successful Raft commit it:
    //  (a) kicks server.stop() onto a separate thread - stopping the leader on the
    //      same thread that is mid-commit would deadlock the Ratis gRPC channel
    //  (b) throws a RuntimeException so commit2ndPhase() is never invoked
    // The hook is single-shot: later commits (if any) no-op before returning.
    final AtomicBoolean hookFired = new AtomicBoolean(false);
    final CountDownLatch leaderStopped = new CountDownLatch(1);
    RaftReplicatedDatabase.TEST_POST_REPLICATION_HOOK = dbName -> {
      if (!hookFired.compareAndSet(false, true))
        return;
      LogManager.instance().log(RaftLeaderCrashBetweenCommitAndApplyIT.class, Level.INFO,
          "TEST: fault-injection hook firing for db=%s, stopping leader %d asynchronously",
          dbName, leaderIndex);
      final Thread stopper = new Thread(() -> {
        try {
          getServer(leaderIndex).stop();
        } catch (final Throwable t) {
          LogManager.instance().log(RaftLeaderCrashBetweenCommitAndApplyIT.class, Level.WARNING,
              "TEST: async leader stop failed: %s", t.getMessage());
        } finally {
          leaderStopped.countDown();
        }
      }, "TEST-fault-injection-stop");
      stopper.setDaemon(true);
      stopper.start();
      throw new RuntimeException(
          "TEST fault injection: simulated leader crash between Raft commit and commit2ndPhase");
    };

    // Phase 3: attempt the write that will trigger the fault. Use an explicit begin/commit
    // with no retry so the fault injection fires exactly once.
    try {
      leaderDb.begin();
      final MutableVertex v = leaderDb.newVertex(VERTEX_TYPE);
      v.set("phase", "injected");
      v.set("name", "injected-0");
      v.save();
      leaderDb.commit();
    } catch (final Exception expected) {
      LogManager.instance().log(this, Level.INFO,
          "TEST: leader commit failed as expected: %s", expected.getMessage());
    }

    assertThat(hookFired.get()).as("Fault-injection hook must have fired").isTrue();
    assertThat(leaderStopped.await(30, TimeUnit.SECONDS))
        .as("Async leader stop must complete within 30s").isTrue();

    // Phase 4: a new leader must be elected from the 2 surviving nodes.
    final int newLeaderIndex = waitForNewLeader(leaderIndex);
    assertThat(newLeaderIndex).as("A new leader must be elected").isGreaterThanOrEqualTo(0);
    assertThat(newLeaderIndex).as("New leader must differ from crashed leader").isNotEqualTo(leaderIndex);
    LogManager.instance().log(this, Level.INFO, "TEST: new leader elected: server %d", newLeaderIndex);

    // Phase 5: the injected record must be visible on the new leader. This proves the
    // entry was Raft-committed and applied by followers BEFORE the leader crashed.
    waitForReplicationIsCompleted(newLeaderIndex);
    final Database newLeaderDb = getServerDatabase(newLeaderIndex, getDatabaseName());
    assertThat(newLeaderDb.countType(VERTEX_TYPE, true))
        .as("Surviving leader must have baseline (100) + injected (1) records")
        .isEqualTo(101L);

    // Phase 6: restart the crashed leader. Its Raft log contains the committed entry
    // but commit2ndPhase() never ran, so the pages are missing. Ratis replay applies
    // the entry via the state machine follower path (origin-skip bypassed because
    // isLeader() is false at replay time).
    // Brief pause to allow the OS to release the gRPC port.
    Thread.sleep(2_000);
    LogManager.instance().log(this, Level.INFO, "TEST: restarting old leader %d", leaderIndex);
    getServer(leaderIndex).start();

    waitForReplicationIsCompleted(leaderIndex);

    // Phase 7: the restarted old leader must have recovered the injected record.
    final Database oldLeaderDb = getServerDatabase(leaderIndex, getDatabaseName());
    assertThat(oldLeaderDb.countType(VERTEX_TYPE, true))
        .as("Restarted old leader must recover the injected record via Raft replay")
        .isEqualTo(101L);

    // Phase 8: full cross-node convergence check.
    assertClusterConsistency();
  }

  private int waitForNewLeader(final int excludeIndex) {
    final long deadline = System.currentTimeMillis() + 30_000;
    while (System.currentTimeMillis() < deadline) {
      for (int i = 0; i < getServerCount(); i++) {
        if (i == excludeIndex)
          continue;
        final RaftHAPlugin plugin = getRaftPlugin(i);
        if (plugin != null && plugin.isLeader())
          return i;
      }
      try {
        Thread.sleep(500);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        return -1;
      }
    }
    return -1;
  }
}
