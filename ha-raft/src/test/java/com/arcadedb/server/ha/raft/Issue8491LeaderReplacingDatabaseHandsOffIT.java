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

import com.arcadedb.database.Database;
import com.arcadedb.serializer.json.JSONArray;
import org.apache.ratis.protocol.RaftPeerId;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #8491 on a real 3-node cluster: a node that is replacing a database with the leader's
 * copy (here an operator resync, {@code POST /api/v1/cluster/resync}) can become the leader, because Raft elects on
 * the log alone and the resync leaves the log complete. As leader it cannot download the copy from itself and refuses
 * every request on the database while the copy is being replaced, so every write fails with a healthy majority.
 * <p>
 * The node must report the condition and hand leadership to a peer that holds the data; the install then completes.
 */
@Tag("slow")
class Issue8491LeaderReplacingDatabaseHandsOffIT extends BaseRaftHATest {

  @Override
  protected int getServerCount() {
    return 3;
  }

  @AfterEach
  void clearHook() {
    SnapshotInstaller.snapshotStagedForTesting = null;
  }

  @Test
  void aLeaderReplacingADatabaseHandsLeadershipToAPeer() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);
    final int replacingIndex = (leaderIndex + 1) % getServerCount();

    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());
    leaderDb.transaction(() -> {
      if (!leaderDb.getSchema().existsType("Issue8491"))
        leaderDb.getSchema().createVertexType("Issue8491");
      for (int i = 0; i < 10; i++)
        leaderDb.newVertex("Issue8491").set("value", i).save();
    });
    waitForReplicationIsCompleted(replacingIndex);

    final RaftHAServer leaderRaft = getRaftPlugin(leaderIndex).getRaftHAServer();
    final RaftHAServer replacingRaft = getRaftPlugin(replacingIndex).getRaftHAServer();
    final ArcadeStateMachine replacingSm = replacingRaft.getStateMachine();

    // Park the resync once its copy is downloaded, before the swap: the install is in flight from here until release.
    final CountDownLatch staged = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    SnapshotInstaller.snapshotStagedForTesting = dbName -> {
      staged.countDown();
      try {
        release.await(60, TimeUnit.SECONDS);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    };

    final AtomicReference<Throwable> resyncFailure = new AtomicReference<>();
    final Thread resync = new Thread(() -> {
      try {
        replacingSm.resyncDatabaseFromLeader(getDatabaseName());
      } catch (final Throwable t) {
        resyncFailure.set(t);
      }
    }, "issue8491-resync");
    resync.start();
    try {
      assertThat(staged.await(60, TimeUnit.SECONDS)).as("the resync must reach its swap").isTrue();
      assertThat(replacingSm.getDatabasesBeingReplaced()).containsExactly(getDatabaseName());

      // Neither node has anything to hand off yet: the leader replaces nothing, the replacing node is a follower.
      assertThat(leaderRaft.getStateMachine().handOffLeadershipWhileReplacingDatabase()).isFalse();
      assertThat(replacingSm.handOffLeadershipWhileReplacingDatabase()).isFalse();
      assertThat(findLeaderIndex()).isEqualTo(leaderIndex);

      // The election of the issue: the replacing node becomes the leader, its log being as complete as anyone's.
      final RaftPeerId replacingPeerId = replacingRaft.getLocalPeerId();
      leaderRaft.transferLeadership(replacingPeerId.toString(), 10_000);
      Awaitility.await("the replacing node is the leader").atMost(30, TimeUnit.SECONDS)
          .pollInterval(50, TimeUnit.MILLISECONDS).until(replacingRaft::isLeader);

      // Every other signal reads healthy; the status document must say what is wrong.
      final JSONArray alerts = ClusterAlerts.scan(getServer(replacingIndex), replacingSm);
      assertThat(alerts.toString()).contains("leader-replacing-database");

      // The health tick's hook (RaftHAServer is the monitor's target) hands leadership to a peer.
      replacingRaft.handOffLeadershipWhileReplacingDatabase();
      Awaitility.await("leadership leaves the replacing node").atMost(30, TimeUnit.SECONDS)
          .pollInterval(50, TimeUnit.MILLISECONDS).until(() -> {
            final RaftPeerId leader = replacingRaft.getLeaderId();
            return leader != null && !leader.equals(replacingPeerId) && !replacingRaft.isLeader();
          });
      assertThat(ClusterAlerts.scan(getServer(replacingIndex), replacingSm).toString())
          .doesNotContain("leader-replacing-database");
    } finally {
      release.countDown();
      resync.join(60_000);
    }

    assertThat(resync.isAlive()).as("the resync must finish").isFalse();
    assertThat(resyncFailure.get()).as("the resync must succeed").isNull();
    assertThat(replacingSm.getDatabasesBeingReplaced()).isEmpty();

    // Writes work again, through the new leader, and reach every node.
    final int newLeaderIndex = findLeaderIndex();
    assertThat(newLeaderIndex).isNotEqualTo(replacingIndex).isGreaterThanOrEqualTo(0);
    final Database newLeaderDb = getServerDatabase(newLeaderIndex, getDatabaseName());
    newLeaderDb.transaction(() -> newLeaderDb.newVertex("Issue8491").set("value", 100).save());
    for (int i = 0; i < getServerCount(); i++)
      waitForReplicationIsCompleted(i);
    assertClusterConsistency();
  }
}
