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
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.log.LogManager;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end IT for the issue #4741 recovery mechanism: the follower-side stuck-divergence recovery
 * ({@link RaftHAServer#recoverFromDivergence()}) reformats this follower's Raft storage and rejoins
 * the group, so the leader reconciles it through the snapshot-install path instead of looping on
 * {@code received INCONSISTENCY reply} forever.
 * <p>
 * Reproducing the exact Raft-log term divergence that triggers the loop in the wild is intricate and
 * timing-dependent; this test instead exercises the recovery <em>action</em> directly (the same call
 * the {@link HealthMonitor} makes once the stuck condition has persisted, which is unit-tested
 * deterministically in {@code HealthMonitorTest}). It asserts that:
 * <ol>
 *   <li>a healthy follower never reports {@link RaftHAServer#isFollowerStuckDiverged()} (no false
 *       positives that would reformat a healthy node);</li>
 *   <li>after {@code recoverFromDivergence()} the follower comes back, catches up, and the cluster
 *       remains consistent with no data loss;</li>
 *   <li>writes that happen after the recovery still replicate to the recovered follower.</li>
 * </ol>
 * <p>
 * Scope note: this exercises the recovery against a <em>healthy</em> follower (its local database
 * already matches the leader), so it proves the reformat + rejoin + snapshot-install path preserves
 * data and converges. It does not inject an already-diverged local database write before recovery;
 * correcting a diverged on-disk database relies on the same {@code SnapshotInstaller} replace-on-disk
 * path that {@code installFromLeaderForBootstrap} / {@code resyncDatabaseFromLeader} already cover.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("slow")
class RaftDivergedFollowerRecoveryIT extends BaseRaftHATest {

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Override
  protected boolean persistentRaftStorage() {
    return true;
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_QUORUM, "majority");
  }

  @Test
  void divergedFollowerReformatsAndRejoins() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);

    int replicaIndex = -1;
    for (int i = 0; i < getServerCount(); i++)
      if (i != leaderIndex) {
        replicaIndex = i;
        break;
      }
    assertThat(replicaIndex).as("a replica must exist").isGreaterThanOrEqualTo(0);

    final var leaderDb = getServerDatabase(leaderIndex, getDatabaseName());
    leaderDb.transaction(() -> {
      if (!leaderDb.getSchema().existsType("DivergedRecovery"))
        leaderDb.getSchema().createVertexType("DivergedRecovery");
    });
    leaderDb.transaction(() -> {
      for (int i = 0; i < 50; i++) {
        final MutableVertex v = leaderDb.newVertex("DivergedRecovery");
        v.set("name", "pre-" + i);
        v.save();
      }
    });
    assertClusterConsistency();

    // (1) No false positives: every running follower in a healthy cluster must NOT look stuck-diverged.
    // Also validate, against the REAL Ratis getters (not the hand-fed predicate unit test), that a
    // caught-up follower produces the invariant the detector relies on: applied term == current term and
    // commit == applied. If Ratis ever reported these differently, isStuckDivergedState would silently
    // never fire on a real divergence, so pinning the healthy-state getter semantics here de-risks that.
    for (int i = 0; i < getServerCount(); i++) {
      final RaftHAPlugin plugin = getRaftPlugin(i);
      if (plugin == null)
        continue;
      final RaftHAServer raft = plugin.getRaftHAServer();
      assertThat(raft.isFollowerStuckDiverged())
          .as("healthy server %d must not be reported as stuck-diverged", i)
          .isFalse();
      if (raft.isLeader())
        continue;
      final var appliedTI = raft.getStateMachine().getLastAppliedTermIndex();
      assertThat(appliedTI).as("follower %d must have applied at least one entry", i).isNotNull();
      assertThat(raft.getCurrentTerm())
          .as("healthy follower %d: applied term must equal current term (detector getter semantics)", i)
          .isEqualTo(appliedTI.getTerm());
      assertThat(raft.getCommitIndex())
          .as("healthy follower %d: commit must equal applied", i)
          .isEqualTo(appliedTI.getIndex());
    }

    // (2) Trigger the recovery action on the follower (what the HealthMonitor does once the stuck
    // condition has persisted). The follower reformats its Raft storage and rejoins the group.
    LogManager.instance().log(this, Level.INFO, "TEST: triggering divergence recovery on replica %d", replicaIndex);
    getRaftPlugin(replicaIndex).getRaftHAServer().recoverFromDivergence();

    // The reformatted peer must rejoin and catch up to the leader's last applied index.
    waitForReplicationIsCompleted(replicaIndex);

    // (3) The recovered follower has all the pre-recovery data, and the cluster is consistent.
    assertThat(getServerDatabase(replicaIndex, getDatabaseName()).countType("DivergedRecovery", true))
        .as("recovered follower must hold all pre-recovery records after reformat+rejoin")
        .isEqualTo(50L);

    // (4) Writes after the recovery still replicate to the recovered follower.
    leaderDb.transaction(() -> {
      for (int i = 0; i < 10; i++) {
        final MutableVertex v = leaderDb.newVertex("DivergedRecovery");
        v.set("name", "post-" + i);
        v.save();
      }
    });

    assertClusterConsistency();

    assertThat(getServerDatabase(replicaIndex, getDatabaseName()).countType("DivergedRecovery", true))
        .as("recovered follower must replicate writes that happened after the recovery")
        .isEqualTo(60L);
  }
}
