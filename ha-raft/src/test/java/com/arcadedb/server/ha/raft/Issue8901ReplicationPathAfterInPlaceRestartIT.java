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
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8901, against real Ratis: the signal that holds the stale-term reformat back is armed by the in-place restart
 * the health monitor performs, and is cleared by the first replicated entry the restarted division takes. A node that
 * was never restarted in place never holds the reformat back (issue #4741).
 */
@Tag("slow")
class Issue8901ReplicationPathAfterInPlaceRestartIT extends BaseRaftHATest {

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
  void anInPlaceRestartHoldsTheReformatUntilAnEntryArrives() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final int replicaIndex = leaderIndex == 0 ? 1 : 0;
    final RaftHAServer follower = getRaftPlugin(replicaIndex).getRaftHAServer();

    assertThat(follower.isReplicationPathUnprovenSinceRestart())
        .as("a division never restarted in place has no old server instance that could hold the leader's stream")
        .isFalse();

    waitForReplicationIsCompleted(replicaIndex);
    follower.restartRatisIfNeeded();

    assertThat(follower.isReplicationPathUnprovenSinceRestart())
        .as("right after the in-place restart no entry has reached the new division yet")
        .isTrue();

    final var leaderDb = getServerDatabase(leaderIndex, getDatabaseName());
    leaderDb.transaction(() -> {
      if (!leaderDb.getSchema().existsType("Issue8901"))
        leaderDb.getSchema().createVertexType("Issue8901");
    });
    leaderDb.transaction(() -> {
      final MutableVertex v = leaderDb.newVertex("Issue8901");
      v.set("name", "after-restart");
      v.save();
    });

    Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(200, TimeUnit.MILLISECONDS)
        .until(() -> !follower.isReplicationPathUnprovenSinceRestart());

    waitForReplicationIsCompleted(replicaIndex);
    assertThat(getServerDatabase(replicaIndex, getDatabaseName()).countType("Issue8901", true)).isEqualTo(1L);
  }
}
