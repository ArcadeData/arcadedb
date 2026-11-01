/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
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
import com.arcadedb.server.monitor.HAReplicationStatsProvider;
import org.apache.ratis.server.raftlog.RaftLogBase;
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
 * A follower whose Raft machinery died under it, against a real follower division, restarted in place by the
 * {@link HealthMonitor} (re-enabled here with a 500 ms interval; the base class disables it) and replicating again.
 * Merged from {@code Issue5271ClosedDivisionRecoveryIT} and {@code Issue8652DeadUpdaterRecoveryIT}, which ran on the same
 * fixture: three servers, volatile Raft storage, health check every 500 ms. The in-place restarts the test drives with
 * the health monitor off live in {@link HaInPlaceRestartRecoveryIT}; the pause-monitor close on persistent storage in
 * {@code Issue8900ClosedServerInPlaceRestartIT}.
 * <ul>
 * <li><b>#5271</b>: a node whose Ratis DIVISION (the per-group {@code RaftServerImpl}) died was invisible to the health
 * monitor, because {@link RaftHAServer#getRaftLifeCycleState()} only read the {@code RaftServer} proxy lifecycle, which
 * stays {@code RUNNING} while the division underneath is {@code CLOSED}. The node rejected every vote, silently reducing
 * fault tolerance to zero, and a later leader restart became a permanent outage. Closing a follower's REAL division (not
 * the simulated {@code forceRaftStateForTesting} hook of {@link RaftHealthMonitorRecoveryIT}) must be reported as
 * {@code CLOSED}, surfaced in the {@code raftState} field of the cluster status, and auto-recovered.</li>
 * <li><b>#8652</b>: the two shapes of a dead node the lifecycle checks could not condemn: the {@code StateMachineUpdater}
 * thread ends (Ratis closes the division from the dying thread), and the Raft log is closed under a division that
 * reports {@code RUNNING} (every append fails with {@code SegmentedRaftLog: Failed to append}, yet Ratis never calls
 * {@code notifyLogFailed}, so the #7037 mark stayed empty). In both the health monitor must restart the node in place
 * and it must replicate again.</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("slow")
class HaInPlaceRestartRecoveryHealthMonitorIT extends BaseRaftHATest {

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

  /**
   * Issue #5271, from {@code Issue5271ClosedDivisionRecoveryIT}: the state is reported as {@code CLOSED} instead of
   * {@code RUNNING} (the blind spot), the cluster status endpoint surfaces it, and the health monitor auto-recovers the
   * node so it replicates again.
   */
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

    // Issue #9429: the restart is visible on the status endpoint, from the same counters the process keeps - so the
    // chaos harness and an operator read it there instead of grepping the log for its wording.
    // Counted once the restart completes, which can be a moment after the new division reads RUNNING.
    final RaftHAServer restarted = getRaftPlugin(followerIndex).getRaftHAServer();
    Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(200, TimeUnit.MILLISECONDS)
        .until(() -> restarted.getInPlaceRestartStats().recovered() + restarted.getInPlaceRestartStats().reformatted() >= 1);
    final HAReplicationStatsProvider.InPlaceRestartStats counted = restarted.getInPlaceRestartStats();
    final JSONObject published = new JSONObject(httpGet(followerIndex, "/api/v1/cluster")).getJSONObject("localInPlaceRestarts");
    assertThat(published.getInt("recovered", -1)).isEqualTo(counted.recovered());
    assertThat(published.getInt("reformatted", -1)).isEqualTo(counted.reformatted());

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

  /** Issue #8652, from {@code Issue8652DeadUpdaterRecoveryIT}. */
  @Test
  void aFollowerWhoseUpdaterDiedIsRestartedAndReplicatesAgain() throws Exception {
    final int leaderIndex = findLeaderIndex();
    final int followerIndex = (leaderIndex + 1) % getServerCount();
    final RaftHAServer followerRaft = getRaftPlugin(followerIndex).getRaftHAServer();
    writeOnLeader(leaderIndex, "UpdaterDied", 1);
    waitForFollowerToApply(followerIndex, leaderIndex);

    final ArcadeStateMachine before = followerRaft.getStateMachine();
    final Thread updater = before.getApplyThreadForTesting();
    assertThat(updater).as("the follower applied an entry, so its updater was seen").isNotNull();
    assertThat(before.describeDeadApplyThread()).isNull();

    // Ratis reads an interrupt of a RUNNING updater as a failure: it logs "caught a Throwable" and closes the division
    // from that thread, the exact end of the chaos run's zombie.
    updater.interrupt();
    updater.join(10_000L);

    // The state machine says so directly, whatever the division reports about itself.
    assertThat(before.describeDeadApplyThread()).contains("terminated");

    awaitRecoveredAndReplicating(leaderIndex, followerIndex, "UpdaterDied");
    assertThat(followerRaft.getStateMachine()).as("a fresh state machine replaced the one whose updater died")
        .isNotSameAs(before);
  }

  /** Issue #8652, from {@code Issue8652DeadUpdaterRecoveryIT}. */
  @Test
  void aFollowerWhoseRaftLogWasClosedUnderARunningDivisionIsRestartedAndReplicatesAgain() throws Exception {
    final int leaderIndex = findLeaderIndex();
    final int followerIndex = (leaderIndex + 1) % getServerCount();
    final RaftHAServer followerRaft = getRaftPlugin(followerIndex).getRaftHAServer();
    writeOnLeader(leaderIndex, "LogClosed", 1);
    waitForFollowerToApply(followerIndex, leaderIndex);

    assertThat(followerRaft.isRaftLogClosed()).as("healthy log").isFalse();
    assertThat(followerRaft.getRaftLifeCycleState()).isEqualTo(LifeCycle.State.RUNNING);

    final ArcadeStateMachine before = followerRaft.getStateMachine();
    // Closes the log only, leaving the division's own lifecycle untouched.
    ((RaftLogBase) followerRaft.getRaftDivision().getRaftLog()).close();

    // The blind spot: not a writer failure Ratis reported, so the #7037 mark stays empty; the closed log is what shows.
    // The health monitor may already have restarted the node
    Awaitility.await().atMost(10, TimeUnit.SECONDS).pollInterval(50, TimeUnit.MILLISECONDS)
        .until(() -> followerRaft.isRaftLogClosed() || followerRaft.getStateMachine() != before);

    awaitRecoveredAndReplicating(leaderIndex, followerIndex, "LogClosed");
  }

  private String httpGet(final int serverIndex, final String path) throws Exception {
    final HttpURLConnection conn = (HttpURLConnection) new URI(
        getServerHttpUrl(serverIndex, path)).toURL().openConnection();
    conn.setRequestMethod("GET");
    conn.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)));
    try {
      return new String(conn.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
    } finally {
      conn.disconnect();
    }
  }

  private void writeOnLeader(final int leaderIndex, final String type, final int value) {
    final var db = getServerDatabase(leaderIndex, getDatabaseName());
    db.transaction(() -> {
      if (!db.getSchema().existsType(type))
        db.getSchema().createVertexType(type);
      db.newVertex(type).set("v", value).save();
    });
  }

  private void waitForFollowerToApply(final int followerIndex, final int leaderIndex) {
    final long leaderApplied = getRaftPlugin(leaderIndex).getRaftHAServer().getLastAppliedIndex();
    Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(200, TimeUnit.MILLISECONDS)
        .untilAsserted(() -> assertThat(getRaftPlugin(followerIndex).getRaftHAServer().getLastAppliedIndex())
            .isGreaterThanOrEqualTo(leaderApplied));
  }

  private void awaitRecoveredAndReplicating(final int leaderIndex, final int followerIndex, final String type) {
    Awaitility.await().atMost(60, TimeUnit.SECONDS).pollInterval(500, TimeUnit.MILLISECONDS)
        .untilAsserted(() -> {
          final RaftHAServer recovered = getRaftPlugin(followerIndex).getRaftHAServer();
          assertThat(recovered.getRaftLifeCycleState()).isEqualTo(LifeCycle.State.RUNNING);
          assertThat(recovered.isRaftLogClosed()).isFalse();
          assertThat(recovered.getDeadStateMachineUpdater()).isNull();
        });

    writeOnLeader(leaderIndex, type, 2);
    final long leaderApplied = getRaftPlugin(leaderIndex).getRaftHAServer().getLastAppliedIndex();
    Awaitility.await().atMost(60, TimeUnit.SECONDS).pollInterval(500, TimeUnit.MILLISECONDS)
        .untilAsserted(() -> assertThat(getRaftPlugin(followerIndex).getRaftHAServer().getLastAppliedIndex())
            .as("the restarted follower replicates again").isGreaterThanOrEqualTo(leaderApplied));
  }
}
