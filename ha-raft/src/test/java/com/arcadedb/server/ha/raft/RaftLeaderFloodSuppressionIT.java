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
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.utility.CodeUtils;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies, when a follower is briefly offline and the leader advances its commit index, that:
 * <ol>
 *   <li>The raw per-retry Ratis "Follower failed ... keep nextIndex" flood is suppressed (now via the
 *       {@code org.apache.ratis.grpc.server.GrpcLogAppender} level set to {@code SEVERE} in
 *       {@code arcadedb-log.properties}).</li>
 *   <li>The leader instead emits the ArcadeDB-level "unreachable" / "reconnected" narrative from
 *       {@code ClusterMonitor} so operators see a clean, actionable signal.</li>
 * </ol>
 * Disabled: capturing live-cluster log output across in-process servers is timing- and
 * logging-config-fragile (ArcadeDB's {@code DefaultLogger} periodically calls JUL
 * {@code readConfiguration}, resetting per-logger handler/filter state mid-test). The suppression is
 * covered by the {@code arcadedb-log.properties} config and the narrative by {@code ClusterMonitorTest};
 * this end-to-end IT is retained as scaffolding to revisit with a more robust capture mechanism.
 */
@Tag("slow")
@Disabled("""
    Live-cluster log capture is timing/logging-config fragile; suppression verified via \
    arcadedb-log.properties and narrative via ClusterMonitorTest. Revisit with robust capture.""")
class RaftLeaderFloodSuppressionIT extends BaseRaftHATest {

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    // Re-enable the health monitor (base class disables it) and configure fast unreachable detection
    // so the narrative fires within the test window. The flood filter is on by default via
    // HA_RESYNC_PROGRESS_LOGGING=true.
    config.setValue(GlobalConfiguration.HA_HEALTH_CHECK_INTERVAL, 500L);
    config.setValue(GlobalConfiguration.HA_PEER_UNREACHABLE_THRESHOLD, 2000L);
  }

  @Test
  void leaderDoesNotFloodAndNarratesUnreachableReplica() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);

    // Find any non-leader replica index - matches the pattern from RaftIdleReplicaRestartIT.
    int replicaIndex = -1;
    for (int i = 0; i < getServerCount(); i++) {
      if (i != leaderIndex) {
        replicaIndex = i;
        break;
      }
    }
    assertThat(replicaIndex).as("a replica must exist").isGreaterThanOrEqualTo(0);

    // Create the write type and replicate it before taking the replica offline so the type
    // is already present on restart without relying on schema-only snapshot transfer.
    final var leaderDb = getServerDatabase(leaderIndex, getDatabaseName());
    leaderDb.transaction(() -> {
      if (!leaderDb.getSchema().existsType("FloodTest"))
        leaderDb.getSchema().createVertexType("FloodTest");
    });
    waitForAllServers();

    // JUL Handler observes Apache Ratis's own logging (the raw flood). ArcadeDB's narrative is
    // captured separately by CapturingTestLogger because ArcadeDB INFO lines are suppressed by
    // arcadedb-log.properties in tests (pinned to SEVERE), so they never reach a JUL Handler.
    final RatisFloodWatcher ratis = RatisFloodWatcher.attach();
    final CapturingTestLogger arcade = CapturingTestLogger.install();
    try {
      getServer(replicaIndex).stop();
      while (getServer(replicaIndex).getStatus() == ArcadeDBServer.STATUS.SHUTTING_DOWN)
        CodeUtils.sleep(200);

      // Keep the leader advancing its commit index while the replica is down so the appender
      // would normally flood and the ClusterMonitor lag-monitor has something to report.
      for (int i = 0; i < 40; i++) {
        final int idx = i;
        leaderDb.transaction(() -> leaderDb.newVertex("FloodTest").set("idx", idx).save());
        Thread.sleep(500);
      }

      getServer(replicaIndex).start();
      waitForReplicationIsCompleted(replicaIndex);
      // Allow one additional lag-monitor tick (5 s cadence) so the reconnected narrative fires
      // after the first successful RPC to the restarted replica is observed.
      CodeUtils.sleep(7_000);

      // The raw per-retry Ratis flood must be suppressed by the GrpcLogAppender level=SEVERE pin in
      // arcadedb-log.properties. A tiny transient margin covers any line emitted during startup.
      assertThat(ratis.countContaining("Follower failed", "keep nextIndex"))
          .as("Ratis per-retry flood must be suppressed by the GrpcLogAppender SEVERE level pin")
          .isLessThanOrEqualTo(2);

      // The operator-facing narrative replaces the flood.
      assertThat(arcade.countContaining("unreachable"))
          .as("ClusterMonitor must emit at least one unreachable narrative line")
          .isGreaterThanOrEqualTo(1);
      assertThat(arcade.countContaining("reconnected"))
          .as("ClusterMonitor must emit the reconnected narrative line after the replica returns")
          .isGreaterThanOrEqualTo(1);
    } finally {
      arcade.uninstall();
      ratis.detach();
    }
  }

  /** Captures Apache Ratis's java.util.logging records (the raw appender flood). */
  private static final class RatisFloodWatcher extends Handler {
    private final List<String> messages = new ArrayList<>();
    private final Logger       root     = Logger.getLogger("");
    private final Level        prev     = root.getLevel();

    static RatisFloodWatcher attach() {
      final RatisFloodWatcher h = new RatisFloodWatcher();
      h.setLevel(Level.ALL);
      h.root.addHandler(h);
      if (h.root.getLevel() == null || h.root.getLevel().intValue() > Level.WARNING.intValue())
        h.root.setLevel(Level.WARNING);
      return h;
    }

    @Override
    public synchronized void publish(final LogRecord record) {
      if (record != null && record.getMessage() != null)
        messages.add(record.getMessage());
    }

    synchronized int countContaining(final String... needles) {
      int n = 0;
      for (final String m : messages) {
        boolean all = true;
        for (final String needle : needles)
          if (!m.contains(needle)) {
            all = false;
            break;
          }
        if (all)
          n++;
      }
      return n;
    }

    @Override public void flush() {}

    @Override public void close() {}

    void detach() {
      root.removeHandler(this);
      if (prev != null)
        root.setLevel(prev);
    }
  }
}
