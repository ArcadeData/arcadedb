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

import com.arcadedb.database.Database;
import org.apache.ratis.protocol.RaftPeerId;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #8346 on a real two-node cluster: a runtime joiner that caught up by snapshot install
 * past its re-admission seed, and whose retained security documents already equal the cluster's, is released by the
 * leader's fingerprint comparison instead of being held for the whole
 * {@code arcadedb.ha.securityConvergenceReadinessTimeout} window.
 *
 * <h2>How the state under test is reached</h2>
 * The follower is armed as a re-added node directly on its {@link RuntimeJoinDetector}, at its current applied
 * index: every security document it holds was installed at or before that index, i.e. by its "previous
 * membership", exactly as on a retained config volume. A non-security write then moves the applied index past the
 * join without installing anything, which is what the snapshot install past the seed leaves behind. The catch-up is
 * then driven through its real snapshot-install trigger, which dials the leader over HTTP.
 * <p>
 * The leader's applied index standing still across the catch-up is the evidence that the release came from the
 * comparison ("matched") and not from a seed the leader replicated back ("seeded"), which would have converged the
 * node the old way.
 * <p>
 * Tagged {@code slow}: it starts a two-node cluster.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
@Tag("slow")
class Issue8346SnapshotCaughtUpJoinerConvergesByMatchIT extends BaseRaftHATest {

  /** A hang detector for the polls below, not a latency bound: nothing here is timed. */
  private static final long   WAIT_BUDGET_MS = 60_000L;
  private static final String VERTEX_TYPE    = "Issue8346";

  @Override
  protected int getServerCount() {
    return 2;
  }

  @Test
  void aJoinerWhoseDocumentsMatchTheLeaderIsReleasedByTheComparison() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("a Raft leader must be elected first").isGreaterThanOrEqualTo(0);
    final int followerIndex = leaderIndex == 0 ? 1 : 0;

    final RaftHAServer leader = getRaftPlugin(leaderIndex).getRaftHAServer();
    final RaftHAServer follower = getRaftPlugin(followerIndex).getRaftHAServer();
    final RuntimeJoinDetector detector = follower.getStateMachine().getRuntimeJoinDetector();

    // Re-added at the follower's current applied index: what it installed so far is its previous membership's.
    final long joinIndex = follower.getLastAppliedIndex();
    assertThat(joinIndex).as("the follower's applied index must be readable").isGreaterThan(0L);
    final RaftPeerId self = RaftPeerId.valueOf(peerIdForIndex(followerIndex));
    final RaftPeerId other = RaftPeerId.valueOf(peerIdForIndex(leaderIndex));
    assertThat(detector.onConfiguration(self, List.of(other, self), List.of(other), joinIndex))
        .as("the follower must arm as a re-added node").isTrue();
    assertThat(follower.securityDocumentsNotInstalledSinceRuntimeJoin())
        .as("nothing installed on the follower since the simulated re-add")
        .containsExactly("users", "groups", "API tokens");

    // Past the join without any security install: the position a snapshot install past the seed leaves it in.
    final Database db = getServerDatabase(leaderIndex, getDatabaseName());
    db.transaction(() -> {
      if (!db.getSchema().existsType(VERTEX_TYPE))
        db.getSchema().createVertexType(VERTEX_TYPE);
    });
    db.transaction(() -> db.newVertex(VERTEX_TYPE).set("id", 1).save());
    waitUntil(() -> follower.getLastAppliedIndex() > joinIndex, "the follower must apply past the join index");

    final long leaderIndexBefore = leader.getLastAppliedIndex();

    follower.getStateMachine().getSecurityCatchUp().afterSnapshotInstall(getServer(followerIndex), follower);

    waitUntil(() -> follower.securityDocumentsNotInstalledSinceRuntimeJoin().isEmpty(),
        "a follower whose documents match the leader's must count as converged once the leader says so");
    assertThat(leader.getLastAppliedIndex())
        .as("released by the fingerprint comparison, not by a seed the leader replicated")
        .isEqualTo(leaderIndexBefore);
  }

  /** Polls a condition to a generous deadline; a hang detector, never a latency bound. */
  private static void waitUntil(final BooleanSupplier condition, final String what) throws InterruptedException {
    final long deadline = System.currentTimeMillis() + WAIT_BUDGET_MS;
    while (System.currentTimeMillis() < deadline) {
      if (condition.getAsBoolean())
        return;
      Thread.sleep(100);
    }
    throw new AssertionError("Timed out waiting: " + what);
  }
}
