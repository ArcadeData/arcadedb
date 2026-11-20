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
import com.arcadedb.server.ha.raft.ratis.ApplyBackPressure;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11508: the leader keeps the default apply-backlog budget (a quarter of its heap, which this test's few small
 * entries never approach) while the two other nodes are configured with a one-byte budget, standing for followers with
 * a much smaller heap. The leader must learn each follower's budget from the capability poll and hold entries back at
 * THAT budget; sized from its own heap alone, as it was, it never holds anything back. The cluster must still replicate
 * every write and converge.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("slow")
class Issue11508FollowerApplyBudgetIT extends BaseRaftHATest {

  /** The node that keeps the default budget, and that the test makes the leader. */
  private static final int BIG_HEAP_NODE = 0;

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    final String serverName = config.getValueAsString(GlobalConfiguration.SERVER_NAME);
    if (Integer.parseInt(serverName.substring(serverName.lastIndexOf('_') + 1)) != BIG_HEAP_NODE)
      config.setValue(GlobalConfiguration.HA_APPLY_BACKLOG_MAX, "1");
  }

  @Test
  void theLeaderHoldsAFollowerToTheBudgetThatFollowerAdvertised() throws InterruptedException {
    final long leaderBudget = getRaftPlugin(BIG_HEAP_NODE).getRaftHAServer().getApplyBacklogBudgetBytes();
    assertThat(leaderBudget).as("the node that leads keeps the default budget, a quarter of its heap or the 32MB floor")
        .isGreaterThanOrEqualTo(32L << 20);

    // Only a hold the big-heap node makes at the budget of a follower counts: the two 1-byte nodes hold entries back at
    // their own budget when one of them leads, which says nothing about a leader learning a follower's budget. The log
    // line names the budget applied ("above 1 bytes") and the emitting node's own ("this node's is N bytes")
    final String followerBudgetClause = "above 1 bytes";
    final String leaderBudgetClause = "(this node's is " + leaderBudget + " bytes)";
    final AtomicInteger held = new AtomicInteger();
    final Handler watcher = new Handler() {
      @Override
      public void publish(final LogRecord record) {
        if (record == null || record.getMessage() == null)
          return;
        final String message = record.getMessage();
        if (message.contains("Holding Raft entries back") && message.contains(followerBudgetClause)
            && message.contains(leaderBudgetClause))
          held.incrementAndGet();
      }

      @Override
      public void flush() {
      }

      @Override
      public void close() {
      }
    };
    final Logger root = Logger.getLogger("");
    final Level previousLevel = root.getLevel();
    watcher.setLevel(Level.WARNING);
    root.addHandler(watcher);
    if (previousLevel == null || previousLevel.intValue() > Level.WARNING.intValue())
      root.setLevel(Level.WARNING);
    try {
      makeLeader(BIG_HEAP_NODE);

      final RaftHAServer leader = getRaftPlugin(BIG_HEAP_NODE).getRaftHAServer();

      // Each follower's budget reaches the leader with the capability poll (5 s)
      final ApplyBackPressure backPressure = ApplyBackPressure.of(leader.getLocalPeerId().toString());
      assertThat(backPressure).as("the leader registered its back-pressure").isNotNull();
      for (int i = 0; i < getServerCount(); i++) {
        if (i == BIG_HEAP_NODE)
          continue;
        final String follower = peerIdForIndex(i);
        assertThat(getRaftPlugin(i).getRaftHAServer().getApplyBacklogBudgetBytes()).as("server %d advertises its own budget", i)
            .isEqualTo(1L);
        final long deadline = System.nanoTime() + 30_000_000_000L;
        while (leader.getPeerCapabilityRegistry().freshApplyBacklogBudgetOf(follower) != 1L && System.nanoTime() < deadline)
          Thread.sleep(200);
        assertThat(leader.getPeerCapabilityRegistry().freshApplyBacklogBudgetOf(follower))
            .as("the leader learnt the budget of %s from its capability answer", follower).isEqualTo(1L);
        assertThat(backPressure.effectiveBudgetBytes(follower)).as("%s is held to its own budget, not the leader's", follower)
            .isEqualTo(1L);
      }

      final var leaderDb = getServerDatabase(BIG_HEAP_NODE, getDatabaseName());
      leaderDb.transaction(() -> {
        if (!leaderDb.getSchema().existsType("Held"))
          leaderDb.getSchema().createVertexType("Held");
      });

      // Spread over more than one capability poll period (5 s), so the leader learns the followers' applied index
      // while there are still entries to hold back
      for (int round = 0; round < 8; round++) {
        final int base = round * 20;
        leaderDb.transaction(() -> {
          for (int i = 0; i < 20; i++)
            leaderDb.newVertex("Held").set("idx", base + i).save();
        });
        Thread.sleep(1_500);
      }

      assertClusterConsistency();
      for (int i = 0; i < getServerCount(); i++)
        assertThat(getServerDatabase(i, getDatabaseName()).countType("Held", true)).as("server %d", i).isEqualTo(160L);
      assertThat(held.get()).as("the leader, whose own budget is %d bytes, must have held entries back from a follower that "
          + "advertised a 1-byte budget", leaderBudget).isGreaterThan(0);
    } finally {
      root.removeHandler(watcher);
      // Unconditionally: a root logger with no level of its own must not keep the WARNING this test set
      root.setLevel(previousLevel);
    }
  }

  private void makeLeader(final int serverIndex) throws InterruptedException {
    final long deadline = System.nanoTime() + 60_000_000_000L;
    while (System.nanoTime() < deadline) {
      final int current = findLeaderIndex();
      if (current == serverIndex)
        return;
      if (current >= 0)
        try {
          getRaftPlugin(current).getRaftHAServer().transferLeadership(peerIdForIndex(serverIndex), 20_000L);
        } catch (final RuntimeException e) {
          // The transfer can be refused while the cluster settles: ask again
        }
      Thread.sleep(500);
    }
    assertThat(findLeaderIndex()).as("server %d must lead", serverIndex).isEqualTo(serverIndex);
  }
}
