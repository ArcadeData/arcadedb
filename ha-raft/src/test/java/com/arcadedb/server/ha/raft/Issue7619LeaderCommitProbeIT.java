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
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7619: the follower-side half of the wedged-channel readiness fix has to work over a real transport. The
 * unit test stands the leader call in; this one runs it for real - a Ratis group-info call from a follower to the
 * leader of a live three-node cluster, through the same TLS parameters and peer allowlist every other Raft RPC
 * crosses - and checks that the commit index it brings back is the leader's, and that a caught-up follower is
 * still Ready once the readiness gate consults it.
 * <p>
 * The health monitor is disabled in the HA test harness (see {@link BaseRaftHATest#onServerConfiguration}), so the
 * test calls the monitor's hook, {@link RaftHAServer#refreshLeaderCommitIndex()}, itself.
 */
@Tag("slow")
class Issue7619LeaderCommitProbeIT extends BaseRaftHATest {

  private static final String TYPE    = "Issue7619";
  private static final long   MAX_LAG = 100L;

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.SERVER_READINESS_REQUIRES_HA, true);
  }

  @Test
  void aFollowerLearnsTheLeadersCommitIndexOverALiveCluster() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final int followerIndex = (leaderIndex + 1) % getServerCount();

    final RaftHAServer leader = getRaftPlugin(leaderIndex).getRaftHAServer();
    final RaftHAServer follower = getRaftPlugin(followerIndex).getRaftHAServer();

    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());
    leaderDb.transaction(() -> {
      if (!leaderDb.getSchema().existsType(TYPE))
        leaderDb.getSchema().createVertexType(TYPE);
    });
    for (int i = 0; i < 20; i++) {
      final int idx = i;
      leaderDb.transaction(() -> leaderDb.newVertex(TYPE).set("idx", idx).save());
    }
    assertClusterConsistency();

    final long leaderCommitBefore = leader.getCommitIndex();
    assertThat(leaderCommitBefore).isGreaterThanOrEqualTo(0L);
    assertThat(follower.getLeaderReportedCommitIndex()).as("nothing learned before the first probe").isEqualTo(-1L);

    Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(500, TimeUnit.MILLISECONDS).until(() -> {
      follower.refreshLeaderCommitIndex();
      return follower.getLeaderReportedCommitIndex() >= leaderCommitBefore;
    });

    // The figure is the leader's, not the follower's own: never past what the leader has committed.
    assertThat(follower.getLeaderReportedCommitIndex()).isLessThanOrEqualTo(leader.getCommitIndex());
    assertThat(follower.isReadyForTraffic(MAX_LAG)).as("a caught-up follower stays Ready under the new gate").isTrue();

    // The leader never probes itself.
    leader.refreshLeaderCommitIndex();
    assertThat(leader.getLeaderReportedCommitIndex()).isEqualTo(-1L);

    leaderDb.transaction(() -> leaderDb.command("sql", "DELETE FROM " + TYPE));
  }
}
