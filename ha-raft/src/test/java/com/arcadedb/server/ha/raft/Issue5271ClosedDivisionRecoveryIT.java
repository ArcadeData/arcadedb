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
import com.arcadedb.serializer.json.JSONObject;

import org.apache.ratis.util.LifeCycle;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #5271: a node whose Ratis <b>division</b> (the per-group
 * {@code RaftServerImpl}) died was invisible to the health monitor, because
 * {@link RaftHAServer#getRaftLifeCycleState()} only read the {@code RaftServer} <b>proxy</b>
 * lifecycle - which stays {@code RUNNING} while the division underneath is {@code CLOSED}. The node
 * kept rejecting every vote request with {@code ServerNotReadyException ... current state is CLOSED},
 * silently reducing the cluster's fault tolerance to zero, and a subsequent leader restart turned
 * into a permanent full-cluster outage (quorum impossible with a CLOSED voter).
 * <p>
 * This test closes a follower's REAL division (not the simulated
 * {@code forceRaftStateForTesting} hook used by {@link RaftHealthMonitorRecoveryIT}) and asserts:
 * the state is reported as {@code CLOSED} instead of {@code RUNNING} (the blind spot), the cluster
 * status endpoint surfaces it in the {@code raftState} field, and the health monitor auto-recovers
 * the node so it replicates again - it is never left {@code CLOSED} while the process runs.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("slow")
class Issue5271ClosedDivisionRecoveryIT extends BaseRaftHATest {

  private static final long HEALTH_CHECK_INTERVAL_MS = 500L;

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    // Re-enable the HealthMonitor (disabled by the base class) with a short interval so recovery is fast.
    config.setValue(GlobalConfiguration.HA_HEALTH_CHECK_INTERVAL, HEALTH_CHECK_INTERVAL_MS);
  }

  @Test
  void closedDivisionIsDetectedSurfacedAndRecovered() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);
    final int followerIndex = (leaderIndex + 1) % getServerCount();
    final RaftHAServer followerRaft = getRaftPlugin(followerIndex).getRaftHAServer();

    assertThat(followerRaft.getRaftLifeCycleState()).isEqualTo(LifeCycle.State.RUNNING);

    // Kill the follower's REAL division, exactly the state observed in issue #5271: the RaftServer
    // proxy keeps running (and answering gRPC) while the group member underneath is CLOSED and can
    // no longer vote or accept a leader's contact.
    followerRaft.getRaftDivision().close();

    // The blind spot: before the fix this returned RUNNING (the proxy state) and the health monitor
    // never saw anything to recover.
    assertThat(followerRaft.getRaftLifeCycleState())
        .as("a CLOSED division must be reported even while the RaftServer proxy is still RUNNING")
        .isEqualTo(LifeCycle.State.CLOSED);

    // Operators must be able to see a non-RUNNING member on the status endpoint (issue #5271 asked
    // for exactly this: the CLOSED voter was invisible on every status surface). The health monitor
    // may be recovering the node at any moment (CLOSED -> STARTING -> RUNNING), so the assertion is
    // that the field exists and reports a genuine lifecycle value, not which phase it caught.
    final JSONObject status = new JSONObject(httpGet(followerIndex, "/api/v1/cluster"));
    final String raftState = status.getString("raftState", "");
    assertThat(LifeCycle.State.valueOf(raftState))
        .as("cluster status must surface the local Raft lifecycle state (got '%s')", raftState)
        .isNotNull();

    // The health monitor must auto-recover the division: never leave a group member CLOSED while
    // the process continues to run and report itself as part of the cluster.
    Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(500, TimeUnit.MILLISECONDS)
        .untilAsserted(() -> assertThat(followerRaft.getRaftLifeCycleState()).isEqualTo(LifeCycle.State.RUNNING));

    // And the recovered node must be a functional replica again.
    final var leaderDb = getServerDatabase(leaderIndex, getDatabaseName());
    leaderDb.transaction(() -> {
      if (!leaderDb.getSchema().existsType("ClosedRecovery"))
        leaderDb.getSchema().createVertexType("ClosedRecovery");
      leaderDb.newVertex("ClosedRecovery").set("v", 1).save();
    });
    final long leaderApplied = getRaftPlugin(leaderIndex).getRaftHAServer().getLastAppliedIndex();
    Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(500, TimeUnit.MILLISECONDS)
        .untilAsserted(() -> {
          // The recovery rebuilds the Ratis server, so re-read it instead of using the pre-fault instance.
          final RaftHAServer recovered = getRaftPlugin(followerIndex).getRaftHAServer();
          assertThat(recovered.getLastAppliedIndex()).isGreaterThanOrEqualTo(leaderApplied);
        });
  }

  private String httpGet(final int serverIndex, final String path) throws Exception {
    final HttpURLConnection conn = (HttpURLConnection) new URI(
        "http://localhost:" + (2480 + serverIndex) + path).toURL().openConnection();
    conn.setRequestMethod("GET");
    conn.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)));
    try {
      return new String(conn.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
    } finally {
      conn.disconnect();
    }
  }
}
