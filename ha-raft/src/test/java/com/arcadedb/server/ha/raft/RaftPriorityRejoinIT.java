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
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.TransactionCommittedRemotelyException;
import com.arcadedb.log.LogManager;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.utility.CodeUtils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #4081.
 * <p>
 * Reproduces the user's reported scenario: a 4-node cluster where node 0 has the
 * highest leader-election priority and node 3 is a replica. After:
 * <ol>
 *   <li>Killing the priority leader (node 0)</li>
 *   <li>Restarting it (Raft storage is wiped because {@code raftPersistStorage=false})</li>
 *   <li>Letting it become leader again via priority</li>
 *   <li>Stopping the replica (node 3)</li>
 *   <li>Restarting the replica (Raft storage is wiped)</li>
 * </ol>
 * the leader must not get stuck in an INCONSISTENCY/append-entries loop. The cluster must
 * eventually converge: writes succeed and the rejoined replica catches up.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("slow")
class RaftPriorityRejoinIT extends BaseRaftHATest {

  private static final int BASE_RAFT_PORT = 2434;
  private static final int BASE_HTTP_PORT = 2480;

  @Override
  protected int getServerCount() {
    return 4;
  }

  @Override
  protected boolean persistentRaftStorage() {
    // The user's setup uses default (false). Storage is wiped on every restart.
    return false;
  }

  @Override
  protected String getServerAddresses() {
    // Mirror the user's configuration: priorities 10/8/6/0, last node is replica role.
    final int[] priorities = { 10, 8, 6, 0 };
    final StringBuilder sb = new StringBuilder();
    for (int i = 0; i < getServerCount(); i++) {
      if (i > 0)
        sb.append(",");
      sb.append("localhost:").append(BASE_RAFT_PORT + i)
          .append(":").append(BASE_HTTP_PORT + i)
          .append(":").append(priorities[i]);
    }
    return sb.toString();
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_QUORUM, "majority");
    // Mirror the user's adb*.sh: quorumTimeout=1000ms.
    config.setValue(GlobalConfiguration.HA_QUORUM_TIMEOUT, 1_000L);

    // Tag the last server as replica role (mirrors the user's adb4.sh).
    final String serverName = config.getValueAsString(GlobalConfiguration.SERVER_NAME);
    final int idx = Integer.parseInt(serverName.substring(serverName.lastIndexOf('_') + 1));
    if (idx == getServerCount() - 1)
      config.setValue(GlobalConfiguration.HA_SERVER_ROLE, "replica");

    // Keep election timeouts small so the test runs in reasonable time.
    config.setValue(GlobalConfiguration.HA_ELECTION_TIMEOUT_MIN, 1_000);
    config.setValue(GlobalConfiguration.HA_ELECTION_TIMEOUT_MAX, 2_000);
  }

  @Test
  void committedBatchIsNotReplayedAfterLocalApplyFailure() {
    final int leader = waitForAnyLeader(-1);
    assertThat(leader).isGreaterThanOrEqualTo(0);
    final var database = getServerDatabase(leader, getDatabaseName());
    database.transaction(() -> database.getSchema().createVertexType("Issue4081"));
    writeBatch("warmup", 0);
    assertClusterConsistency();

    final AtomicBoolean fired = new AtomicBoolean();
    RaftReplicatedDatabase.TEST_PHASE2_COMMIT_FAULT = name -> {
      if (name.equals(getDatabaseName()) && fired.compareAndSet(false, true))
        throw new ConcurrentModificationException("single-shot phase-2 failure for rejoin workload");
    };
    try {
      writeBatch("committed-remotely", 0);
    } finally {
      RaftReplicatedDatabase.TEST_PHASE2_COMMIT_FAULT = null;
    }
    assertThat(fired).isTrue();
    writeBatch("after-recovery", 0);
    assertClusterConsistency();
    for (int i = 0; i < getServerCount(); i++) {
      final var replica = getServerDatabase(i, getDatabaseName());
      assertThat(replica.countType("Issue4081", true)).isEqualTo(30);
      try (final var records = replica.query("sql", "SELECT idx FROM Issue4081 WHERE phase = 'committed-remotely' ORDER BY idx")) {
        for (int idx = 0; idx < 10; idx++) {
          assertThat(records.hasNext()).isTrue();
          assertThat(((Number) records.next().getProperty("idx")).intValue()).isEqualTo(idx);
        }
        assertThat(records.hasNext()).isFalse();
      }
    }
  }

  @Test
  void leaderRestartThenReplicaRestartConverges() {
    // Step 1: write some data with all nodes online.
    final int firstLeader = waitForAnyLeader(-1);
    assertThat(firstLeader).as("a Raft leader must be elected at startup").isGreaterThanOrEqualTo(0);

    final var leaderDb = getServerDatabase(firstLeader, getDatabaseName());
    leaderDb.transaction(() -> {
      if (!leaderDb.getSchema().existsType("Issue4081"))
        leaderDb.getSchema().createVertexType("Issue4081");
    });

    // Many small transactions so the Raft log has plenty of entries to replicate.
    for (int t = 0; t < 50; t++) {
      final int base = t * 10;
      writeBatch("initial", base);
    }

    assertClusterConsistency();

    // Step 2: kill the priority-leader (server 0).
    LogManager.instance().log(this, Level.INFO, "TEST: stopping priority leader (server 0)");
    getServer(0).stop();
    while (getServer(0).getStatus() == ArcadeDBServer.STATUS.SHUTTING_DOWN)
      CodeUtils.sleep(200);

    // Step 3: wait for a new leader among the remaining nodes.
    final int interimLeader = waitForAnyLeader(0);
    assertThat(interimLeader).as("a new leader must be elected after stopping server 0").isGreaterThanOrEqualTo(0);
    assertThat(interimLeader).isNotEqualTo(0);

    // Write more so the interim leader's log advances beyond the killed leader's snapshot point.
    for (int t = 0; t < 30; t++) {
      final int base = t * 10;
      writeBatch("after-leader-stop", base);
    }

    // Step 4: restart server 0. Its Raft storage was wiped (persistStorage=false) so it joins fresh.
    LogManager.instance().log(this, Level.INFO, "TEST: restarting server 0");
    getServer(0).start();

    // Step 5: with priority 10, server 0 should become the leader again. Wait for that.
    final int reLeader = waitForLeader(0, 60_000);
    assertThat(reLeader).as("server 0 should reclaim leadership via priority").isEqualTo(0);

    // Confirm the cluster is functional with server 0 as leader.
    for (int t = 0; t < 30; t++) {
      final int base = t * 10;
      writeBatch("after-leader-rejoin", base);
    }

    // Step 6: stop the replica (server 3).
    LogManager.instance().log(this, Level.INFO, "TEST: stopping replica (server 3)");
    getServer(3).stop();
    while (getServer(3).getStatus() == ArcadeDBServer.STATUS.SHUTTING_DOWN)
      CodeUtils.sleep(200);

    // Write more so the leader's log advances while the replica is offline.
    // Aim for enough entries to cross the snapshot threshold so the leader's log gets purged.
    for (int t = 0; t < 30; t++) {
      final int base = t * 10;
      writeBatch("while-replica-down", base);
    }

    // Step 7: restart the replica. Storage is wiped so it joins as a fresh peer.
    LogManager.instance().log(this, Level.INFO, "TEST: restarting replica (server 3)");
    getServer(3).start();

    // Step 8: the replica must catch up. The leader must NOT get stuck in an INCONSISTENCY loop.
    waitForReplicationIsCompleted(3);

    // Final write to confirm the cluster is fully operational.
    for (int t = 0; t < 30; t++) {
      final int base = t * 10;
      writeBatch("final", base);
    }

    assertClusterConsistency();

    // Every node must hold all 1700 vertices (50+30+30+30+30 transactions x 10 vertices each).
    final long expected = (50 + 30 + 30 + 30 + 30) * 10L;
    for (int i = 0; i < getServerCount(); i++) {
      final long count = getServerDatabase(i, getDatabaseName()).countType("Issue4081", true);
      assertThat(count).as("server " + i + " should have all " + expected + " records").isEqualTo(expected);
    }
  }

  private void writeBatch(final String phase, final int base) {
    final int leader = waitForAnyLeader(-1);
    assertThat(leader).as("a leader must be available for the next batch").isGreaterThanOrEqualTo(0);
    final var database = getServerDatabase(leader, getDatabaseName());
    try {
      database.transaction(() -> {
        for (int i = 0; i < 10; i++)
          database.newVertex("Issue4081").set("phase", phase).set("idx", base + i).save();
      });
    } catch (final TransactionCommittedRemotelyException committed) {
      // The quorum committed this batch. Retrying inserts duplicates; the next batch resolves a fresh
      // leader/handle after phase-2 recovery. Final replica equality and exact counts still verify every write.
      LogManager.instance().log(this, Level.INFO, "Batch %s/%d committed remotely; continuing without replay", phase, base);
    }
  }

  private int waitForAnyLeader(final int excludeIndex) {
    final long deadline = System.currentTimeMillis() + 30_000;
    while (System.currentTimeMillis() < deadline) {
      for (int i = 0; i < getServerCount(); i++) {
        if (i == excludeIndex)
          continue;
        final RaftHAPlugin plugin = getRaftPlugin(i);
        if (plugin != null && plugin.getRaftHAServer().getLeadershipState().leaderReady())
          return i;
      }
      CodeUtils.sleep(250);
    }
    return -1;
  }

  private int waitForLeader(final int expectedIndex, final long timeoutMs) {
    final long deadline = System.currentTimeMillis() + timeoutMs;
    while (System.currentTimeMillis() < deadline) {
      final RaftHAPlugin plugin = getRaftPlugin(expectedIndex);
      if (plugin != null && plugin.getRaftHAServer().getLeadershipState().leaderReady())
        return expectedIndex;
      CodeUtils.sleep(500);
    }
    return -1;
  }
}
