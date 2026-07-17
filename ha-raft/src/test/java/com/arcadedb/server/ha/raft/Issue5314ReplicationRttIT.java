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
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #5314: the CLUSTER CONFIGURATION table's {@code LATENCY} column reported the follower's
 * time-since-last-RPC ({@code lastRpcElapsedMs}), which on an idle cluster just tracks the heartbeat
 * cadence (multi-second) and is off by thousands from the true sub-millisecond link RTT. This test
 * exercises the fix end to end on a live 2-node Raft cluster:
 * <ul>
 *   <li>{@link RaftHAServer#getReplicationLatencies()} returns a real, positive, sub-second measured
 *       appendEntries/heartbeat round-trip for the follower - read straight from Ratis' gRPC
 *       log-appender latency timers, requiring no write traffic;</li>
 *   <li>the rendered table uses the honest {@code LAST CONTACT} and new {@code RTT} headers and no
 *       longer advertises a {@code LATENCY} column;</li>
 *   <li>the RTT (a real round trip) is materially smaller than LAST CONTACT (heartbeat-cadence age),
 *       which is the whole point of separating them.</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("slow")
class Issue5314ReplicationRttIT extends BaseRaftHATest {

  private static final String TYPE = "Issue5314";

  @Override
  protected int getServerCount() {
    return 2;
  }

  @Test
  void exposesRealReplicationRttDistinctFromLastContact() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);

    final RaftHAServer leader = getRaftPlugin(leaderIndex).getRaftHAServer();

    // Generate a handful of appendEntries RPCs carrying real entries so the append-latency timer (not
    // only the heartbeat one) has samples; the fix works idle-only too, this just makes the assertion
    // robust regardless of whether a heartbeat happened to land during the window.
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

    final String followerPeerId = peerIdForIndex((leaderIndex + 1) % getServerCount());

    // The Ratis timers populate asynchronously once the first RPCs complete; give them a moment.
    Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(500, TimeUnit.MILLISECONDS).until(() -> {
      final Map<String, RaftHAServer.ReplicationLatency> rtt = leader.getReplicationLatencies();
      final RaftHAServer.ReplicationLatency sample = rtt.get(followerPeerId);
      return sample != null && sample.meanMs() > 0d;
    });

    final Map<String, RaftHAServer.ReplicationLatency> latencies = leader.getReplicationLatencies();
    final RaftHAServer.ReplicationLatency followerRtt = latencies.get(followerPeerId);
    assertThat(followerRtt).as("leader must expose a measured RTT for the follower").isNotNull();
    assertThat(followerRtt.meanMs()).as("mean RTT must be a real positive figure").isGreaterThan(0d);
    // Loopback replication RTT is well under a second; the old column reported multi-second heartbeat age.
    assertThat(followerRtt.meanMs()).as("loopback RTT must be sub-second").isLessThan(1000d);
    assertThat(followerRtt.p99Ms()).as("p99 RTT is a valid percentile").isGreaterThanOrEqualTo(0d);

    // The rendered table must use the honest headers and drop the misleading one.
    final String table = leader.getClusterConfigurationTable();
    assertThat(table).as("table present on leader").isNotNull();
    assertThat(table).contains("RTT");
    assertThat(table).contains("LAST CONTACT");
    assertThat(table).doesNotContain("LATENCY");

    leaderDb.transaction(() -> leaderDb.command("sql", "DELETE FROM " + TYPE));
  }
}
