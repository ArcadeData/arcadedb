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
import com.arcadedb.log.LogManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression coverage for issue #579 (targeted-recovery Step 1).
 * <p>
 * The recovery plan sent to the client assumed that stopping two of three cluster nodes and
 * starting the third ("arcadesplit-0") <em>alone</em> would let it self-promote to leader, because
 * "Ratis will detect it is the only member and promote itself to leader". That assumption does not
 * hold for ArcadeDB's Raft integration: the {@link RaftGroup} a node joins is built from the static,
 * statically-configured {@code arcadedb.ha.serverList} (see {@link RaftHAServer}'s constructor,
 * which calls {@code RaftGroup.valueOf(groupId, peers)} with the full parsed peer list) - it is not
 * shrunk to "peers currently reachable". Ratis elections require a majority of the *configured* Raft
 * group (2 of 3 here), not a majority of *running* processes. A lone node with a 3-peer static
 * configuration can therefore never win an election on its own, no matter how long it waits.
 * <p>
 * This mirrors the already-existing 2-node coverage in {@link RaftLeaderDown2NodesIT} /
 * {@link RaftQuorumLostIT} (same underlying majority-quorum mechanism), but exercises the exact
 * scenario reported in #579: a full cluster restart followed by starting only one of three
 * statically-configured nodes.
 */
class Issue579IsolatedNodeSelfElectionIT extends BaseRaftHATest {

  @Override
  protected boolean persistentRaftStorage() {
    return true;
  }

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
  @Timeout(value = 90, unit = TimeUnit.SECONDS)
  void isolatedNodeDoesNotSelfElectWithStaticThreeNodeServerList() throws Exception {
    // Confirm the cluster is healthy and has a leader before the simulated outage.
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected before the test scenario begins")
        .isGreaterThanOrEqualTo(0);

    final var leaderDb = getServerDatabase(leaderIndex, getDatabaseName());
    leaderDb.transaction(() -> {
      if (!leaderDb.getSchema().existsType("Issue579"))
        leaderDb.getSchema().createVertexType("Issue579");
    });
    leaderDb.transaction(() -> {
      final MutableVertex v = leaderDb.newVertex("Issue579");
      v.set("name", "before-full-restart");
      v.save();
    });

    assertClusterConsistency();

    // Simulate the client's situation: the whole 3-node cluster is stopped (rolling restart that
    // left it leaderless), then only node 0 ("arcadesplit-0") is started back up, alone, as
    // Recovery Step 1 instructed.
    for (int i = 0; i < getServerCount(); i++) {
      if (getServer(i).isStarted()) {
        LogManager.instance().log(this, Level.INFO, "TEST: Stopping server %d (simulating full cluster stop)", i);
        getServer(i).stop();
      }
    }

    Thread.sleep(2_000); // let the OS release the gRPC ports before restarting node 0

    LogManager.instance().log(this, Level.INFO,
        "TEST: Starting server 0 alone (simulating 'start arcadesplit-0 in isolation')");
    getServer(0).start();

    // Give node 0 well beyond ArcadeDB's default election timeout (5s) to self-elect if it ever
    // will - several election rounds' worth of time.
    final long deadline = System.currentTimeMillis() + 20_000;
    boolean becameLeader = false;
    while (System.currentTimeMillis() < deadline) {
      final RaftHAPlugin plugin = getRaftPlugin(0);
      if (plugin != null && plugin.isLeader()) {
        becameLeader = true;
        break;
      }
      Thread.sleep(500);
    }

    LogManager.instance().log(this, Level.INFO, "TEST: server 0 isLeader=%s after isolation restart", becameLeader);

    assertThat(becameLeader)
        .as("A node started alone against a static 3-peer arcadedb.ha.serverList cannot win a Raft "
            + "election: it can only cast 1 of the 3 configured votes, short of the 2 needed for "
            + "majority. This reproduces issue #579 - 'arcadesplit-0 did not self-elect as leader "
            + "when the other two nodes were stopped' - and shows the client's recovery Step 1 "
            + "assumption does not hold for a statically-configured 3-node group.")
        .isFalse();
  }
}
