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
import com.arcadedb.database.RID;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.log.LogManager;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #4740 (Fix 1 - leader self-reconciliation).
 * <p>
 * When the leader's local {@code commit2ndPhase} fails <b>after</b> Raft has already committed the
 * entry (followers applied it), the leader's pages stay one version behind the committed log. Before
 * the fix, the ex-leader stepped down without reconciling its own pages: when it then applied
 * subsequent entries as a follower it hit a {@link com.arcadedb.exception.WALVersionGapException} on
 * every entry touching that page, triggering non-converging snapshot resyncs.
 * <p>
 * The fix replays the same committed WAL bytes on the leader (idempotent via page-version guards)
 * before stepping down, so the ex-leader's page versions match the followers and no WAL gap occurs.
 * <p>
 * This test injects a single phase-2 failure via {@link RaftReplicatedDatabase#TEST_PHASE2_COMMIT_FAULT},
 * then drives more writes to the <b>same page</b> through the new leader and asserts:
 * <ol>
 *   <li>the cluster converges (all nodes hold identical record counts), and</li>
 *   <li>no node ever raised a WAL version gap ({@link ArcadeStateMachine#TEST_WAL_GAP_COUNTER} == 0) -
 *       which would have happened on the un-reconciled ex-leader without the fix.</li>
 * </ol>
 */
@Tag("slow")
class Issue4740Phase2ReconcileIT extends BaseRaftHATest {

  private static final String VERTEX_TYPE = "Phase2Reconcile";

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_QUORUM, "majority");
  }

  @Override
  protected int getServerCount() {
    return 3;
  }

  @AfterEach
  void clearHooks() {
    RaftReplicatedDatabase.TEST_PHASE2_COMMIT_FAULT = null;
    ArcadeStateMachine.TEST_WAL_GAP_COUNTER = null;
  }

  @Test
  void leaderReconcilesOwnPagesAfterPhase2FailureSoNoWalGap() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);

    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());

    // Single, low-bucket vertex type so repeated updates land on the same page (forces the
    // page-version contention that surfaces the WAL gap when reconciliation is missing).
    leaderDb.transaction(() -> {
      if (!leaderDb.getSchema().existsType(VERTEX_TYPE))
        leaderDb.getSchema().createVertexType(VERTEX_TYPE, 1);
    });

    // Baseline: a single vertex that every later write updates (same page).
    final Object[] ridHolder = new Object[1];
    leaderDb.transaction(() -> {
      final MutableVertex v = leaderDb.newVertex(VERTEX_TYPE);
      v.set("counter", 0);
      v.save();
      ridHolder[0] = v.getIdentity();
    });
    assertClusterConsistency();

    // Arm the global WAL-gap counter so any divergence on any node is observable.
    final AtomicInteger walGapCounter = new AtomicInteger(0);
    ArcadeStateMachine.TEST_WAL_GAP_COUNTER = walGapCounter;

    // Arm a single-shot phase-2 fault: the next leader commit replicates through Raft (followers
    // apply it) and then throws before commit2ndPhase, leaving the leader one version behind.
    final AtomicBoolean faultFired = new AtomicBoolean(false);
    RaftReplicatedDatabase.TEST_PHASE2_COMMIT_FAULT = dbName -> {
      if (!faultFired.compareAndSet(false, true))
        return;
      LogManager.instance().log(Issue4740Phase2ReconcileIT.class, Level.INFO,
          "TEST: injecting phase-2 commit fault for db=%s on leader %d", dbName, leaderIndex);
      throw new ConcurrentModificationException("TEST fault injection: simulated phase-2 commit failure (#4740)");
    };

    // Trigger the faulted write. The commit throws to the client (the entry is Raft-committed).
    try {
      leaderDb.begin();
      final MutableVertex v = leaderDb.lookupByRID((RID) ridHolder[0], true).asVertex().modify();
      v.set("counter", 1);
      v.save();
      leaderDb.commit();
      LogManager.instance().log(this, Level.INFO, "TEST: faulted commit unexpectedly succeeded");
    } catch (final Exception expected) {
      LogManager.instance().log(this, Level.INFO,
          "TEST: leader phase-2 commit failed as expected: %s", expected.getMessage());
    }

    assertThat(faultFired.get()).as("Phase-2 fault hook must have fired").isTrue();

    // A new leader must take over (the ex-leader stepped down). Find any leader now.
    final int newLeaderIndex = waitForAnyLeader();
    assertThat(newLeaderIndex).as("A leader must be elected after the phase-2 step-down").isGreaterThanOrEqualTo(0);

    // Drive further updates to the SAME page through the cluster. Without reconciliation the
    // ex-leader (now a follower) would be a version behind and raise a WAL gap on these.
    final Database newLeaderDb = getServerDatabase(newLeaderIndex, getDatabaseName());
    for (int i = 2; i <= 12; i++) {
      final int counter = i;
      newLeaderDb.transaction(() -> {
        final MutableVertex v = newLeaderDb.lookupByRID((RID) ridHolder[0], true).asVertex().modify();
        v.set("counter", counter);
        v.save();
      });
    }

    // Let all nodes catch up.
    for (int i = 0; i < getServerCount(); i++)
      waitForReplicationIsCompleted(i);

    // Core assertion: the leader reconciled its own pages, so no node ever hit a WAL version gap.
    assertThat(walGapCounter.get())
        .as("Leader self-reconciliation must prevent any WAL version gap after a phase-2 failure")
        .isZero();

    // The cluster must converge: every node holds exactly one vertex with the final counter value.
    assertClusterConsistency();
    for (int i = 0; i < getServerCount(); i++) {
      final Database db = getServerDatabase(i, getDatabaseName());
      assertThat(db.countType(VERTEX_TYPE, true))
          .as("Node %d must hold exactly one vertex", i).isEqualTo(1L);
    }
  }

  private int waitForAnyLeader() {
    final long deadline = System.currentTimeMillis() + 30_000;
    while (System.currentTimeMillis() < deadline) {
      for (int i = 0; i < getServerCount(); i++) {
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
