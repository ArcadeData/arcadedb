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
import com.arcadedb.serializer.json.JSONObject;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8901, against real Ratis: the signal that holds the stale-term reformat back is armed by the in-place restart
 * the health monitor performs, and is cleared by the first replicated entry the restarted division takes. A node that
 * was never restarted in place never holds the reformat back (issue #4741). The dead path itself - the leader's log
 * stream still bound to the closed instance - is not reproduced here: it is timing-dependent (#8898, #8900), so the
 * hold it triggers is covered by {@code Issue8901DeadReplicationPathNoReformatTest} at the health-monitor level.
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
  void anInPlaceRestartHoldsTheReformatUntilAnEntryArrives() throws Exception {
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
    // Issue #9013: the same state, machine-readable in the follower's own status document.
    assertThat(queryClusterEndpoint(replicaIndex).getBoolean("localReplicationPathUnproven")).isTrue();

    // Issue #8953: an idle cluster sends the restarted follower no entry either, so "unproven" alone must not read as an
    // unreachable leader. The health monitor is off in this fixture, so its two hooks are driven here, for longer than
    // the grace, on the real division: the leader's heartbeat makes it known and its commit index is what we hold.
    final ContextConfiguration config = getServer(replicaIndex).getConfiguration();
    final long grace = 2L * RaftPropertiesBuilder.electionTimeoutMaxFor(
        config.getValueAsInteger(GlobalConfiguration.HA_ELECTION_TIMEOUT_MIN),
        config.getValueAsInteger(GlobalConfiguration.HA_ELECTION_TIMEOUT_MAX));
    Awaitility.await().during(grace + 2_000L, TimeUnit.MILLISECONDS).atMost(grace + 30_000L, TimeUnit.MILLISECONDS)
        .pollInterval(500, TimeUnit.MILLISECONDS).until(() -> {
          follower.trackLeaderReachSinceRestart();
          follower.refreshLeaderCommitIndex();
          return follower.getLeaderUnreachableSinceRestartMs() < 0;
        });
    final JSONObject idle = queryClusterEndpoint(replicaIndex);
    assertThat(idle.getBoolean("localLeaderUnreachableSinceRestart")).isFalse();
    assertThat(idle.getJSONArray("alerts").toString()).doesNotContain("follower-leader-unreachable-since-restart");

    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());
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
    assertThat(queryClusterEndpoint(replicaIndex).getBoolean("localReplicationPathUnproven")).isFalse();
  }

  private JSONObject queryClusterEndpoint(final int serverIndex) throws Exception {
    final URL url = new URL("http://localhost:" + getServerHttpPort(serverIndex) + "/api/v1/cluster");
    final HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("GET");
    conn.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)));
    try {
      assertThat(conn.getResponseCode()).isEqualTo(200);
      return new JSONObject(new String(conn.getInputStream().readAllBytes(), StandardCharsets.UTF_8));
    } finally {
      conn.disconnect();
    }
  }
}
