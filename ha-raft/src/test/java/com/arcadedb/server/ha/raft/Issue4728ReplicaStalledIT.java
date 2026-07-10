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
import com.arcadedb.log.LogManager;

import org.apache.ratis.protocol.RaftPeerId;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #4728: a replica that gets stuck STALLED (its {@code matchIndex} never advancing - e.g. stuck
 * at -1 after a rolling upgrade) used to stay stuck forever, since the leader only logged the stall
 * and the follower cannot self-detect it (its own commit index does not advance). The fix makes the
 * LEADER actively force the stalled replica to resync once the stall persists
 * ({@link GlobalConfiguration#HA_STALLED_REPLICA_RESYNC_DURATION_MS}).
 * <p>
 * The pure decision logic (when the leader fires the resync) is covered deterministically by
 * {@code ClusterMonitorTest}. This integration test exercises the cross-node transport the leader
 * uses to recover the follower: {@link RaftHAServer#requestRemoteResync} hitting the follower's
 * {@code POST /api/v1/cluster/resync/{database}} endpoint authenticated with the inter-node cluster
 * token. It also asserts the endpoint rejects a request that does not present the token.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("slow")
class Issue4728ReplicaStalledIT extends BaseRaftHATest {

  private static final String TYPE  = "Issue4728";
  private static final int    COUNT = 50;

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_QUORUM, "majority");
  }

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Test
  void leaderForcesStalledReplicaToResync() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);

    final int replicaIndex = (leaderIndex + 1) % getServerCount();
    final String replicaPeerId = peerIdForIndex(replicaIndex);

    final var leaderDb = getServerDatabase(leaderIndex, getDatabaseName());
    leaderDb.transaction(() -> {
      if (!leaderDb.getSchema().existsType(TYPE))
        leaderDb.getSchema().createVertexType(TYPE);
    });
    leaderDb.transaction(() -> {
      for (int i = 0; i < COUNT; i++)
        leaderDb.newVertex(TYPE).set("idx", i).save();
    });

    assertClusterConsistency();
    assertThat(getServerDatabase(replicaIndex, getDatabaseName()).countType(TYPE, true))
        .as("replica must hold the data before resync").isEqualTo((long) COUNT);

    // NOTE: this exercises only the transport half of the fix - the leader->follower resync call. The
    // decision half (ClusterMonitor detecting a sustained stall and invoking forceResyncStalledReplica)
    // is covered deterministically in ClusterMonitorTest, so the two halves are tested in isolation
    // rather than driving the full ClusterMonitor -> forceResyncStalledReplica -> requestRemoteResync
    // wiring end-to-end (which would require reproducing the hard-to-trigger stall on a live cluster).
    final RaftHAServer leaderRaft = getRaftPlugin(leaderIndex).getRaftHAServer();
    final String followerHttpAddr = leaderRaft.getPeerHttpAddress(RaftPeerId.valueOf(replicaPeerId));
    assertThat(followerHttpAddr).as("leader must resolve the follower's HTTP address").isNotNull();

    // A request WITHOUT the cluster token (and without operator credentials) must be refused.
    assertThatThrownBy(() -> leaderRaft.requestRemoteResync(followerHttpAddr, getDatabaseName(), "wrong-token", false))
        .as("resync endpoint must reject an invalid cluster token")
        .isInstanceOf(IOException.class);

    // The leader-driven recovery path: force the follower to resync using the real cluster token.
    LogManager.instance().log(this, Level.INFO, "TEST: leader forcing resync of replica %d via cluster token", replicaIndex);
    try {
      leaderRaft.requestRemoteResync(followerHttpAddr, getDatabaseName(), leaderRaft.getClusterToken(), false);
    } catch (final IOException e) {
      throw new AssertionError("leader-driven resync with a valid cluster token must succeed", e);
    }

    // After the forced resync the follower still holds the full dataset and the cluster is consistent.
    assertThat(getServerDatabase(replicaIndex, getDatabaseName()).countType(TYPE, true))
        .as("replica must still hold all data after the leader-driven resync").isEqualTo((long) COUNT);
    assertClusterConsistency();
  }
}
