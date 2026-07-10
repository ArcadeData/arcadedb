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
import com.arcadedb.utility.CodeUtils;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that a follower restarted after falling behind logs the unified HA resync bookends
 * ("HA resync started" / "HA resync finished") for either the Raft log catch-up path (common) or the
 * snapshot path (when it fell far enough behind), captured via {@link CapturingTestLogger}.
 * <p>
 * Disabled for the same reason as {@code RaftLeaderFloodSuppressionIT}: capturing live-cluster log
 * output across in-process servers is timing- and logging-config-fragile. The catch-up narrative state
 * machine is covered deterministically by {@code FollowerResyncProgressTrackerTest} and its wiring by
 * {@code HealthMonitorTest}; this end-to-end IT is retained as scaffolding to revisit with a more
 * robust capture mechanism.
 */
@Tag("slow")
@Disabled("""
    Live-cluster log capture is timing/logging-config fragile; catch-up narrative verified via \
    FollowerResyncProgressTrackerTest and HealthMonitorTest. Revisit with robust capture.""")
class RaftFollowerCatchupLoggingIT extends BaseRaftHATest {

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    // The base test disables the health monitor (interval 0); the catch-up narrative is driven by its
    // tick, so enable it and shorten the progress interval for the test window.
    config.setValue(GlobalConfiguration.HA_HEALTH_CHECK_INTERVAL, 500L);
    config.setValue(GlobalConfiguration.HA_RESYNC_PROGRESS_INTERVAL, 1000L);
  }

  @Test
  void restartedFollowerLogsResyncBookends() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);

    int replicaIndex = -1;
    for (int i = 0; i < getServerCount(); i++) {
      if (i != leaderIndex) {
        replicaIndex = i;
        break;
      }
    }
    assertThat(replicaIndex).as("a replica must exist").isGreaterThanOrEqualTo(0);

    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());
    leaderDb.transaction(() -> {
      if (!leaderDb.getSchema().existsType("CatchupTest"))
        leaderDb.getSchema().createVertexType("CatchupTest");
    });
    waitForAllServers();

    final CapturingTestLogger arcade = CapturingTestLogger.install();
    try {
      getServer(replicaIndex).stop();

      // Accumulate a backlog the follower must replay on return.
      for (int i = 0; i < 30; i++) {
        final int idx = i;
        leaderDb.transaction(() -> leaderDb.newVertex("CatchupTest").set("idx", idx).save());
        Thread.sleep(500);
      }

      getServer(replicaIndex).start();
      waitForReplicationIsCompleted(replicaIndex);
      CodeUtils.sleep(3_000);

      // Either a Raft log catch-up (common) or, if it fell far enough behind, a snapshot resync.
      final int catchUp = arcade.countContaining("HA resync started", "mode=catch-up");
      final int snapshot = arcade.countContaining("HA resync started", "mode=snapshot");
      assertThat(catchUp + snapshot).isGreaterThanOrEqualTo(1);

      assertThat(arcade.countContaining("HA resync finished")).isGreaterThanOrEqualTo(1);
    } finally {
      arcade.uninstall();
    }
  }
}
