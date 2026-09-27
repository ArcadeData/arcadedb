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
import com.arcadedb.exception.ConfigurationException;
import org.apache.ratis.protocol.RaftPeerId;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #8487 on a real 3-node cluster: the TARGETED {@code transferLeadership(peerId, timeoutMs)},
 * run while every node keeps writing, failed with "client-... is already CLOSED". The failure was judged by sampling
 * the leader view once, the instant it was caught, and the closed client was only the symptom of whatever leader change
 * came next: the handoff itself, or, when the target lost the election it was sent to win, the election timer that
 * finally ended the leaderless window.
 * <p>
 * Every round must now either return with the target already the leader, or fail with a message naming where
 * leadership actually went; and the whole cluster must then agree on one leader.
 */
@Tag("slow")
class Issue8487TargetedTransferUnderWritesIT extends BaseRaftHATest {

  private static final int ROUNDS = 6;

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Test
  void aTargetedTransferUnderConcurrentWritesReportsTheHandoffItMade() throws Exception {
    final int firstLeader = findLeaderIndex();
    assertThat(firstLeader).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final Database leaderDb = getServerDatabase(firstLeader, getDatabaseName());
    leaderDb.transaction(() -> leaderDb.getSchema().getOrCreateVertexType("Issue8487"));
    for (int i = 0; i < getServerCount(); i++)
      waitForReplicationIsCompleted(i);

    final AtomicBoolean stop = new AtomicBoolean();
    final AtomicLong written = new AtomicLong();
    final List<Thread> writers = new ArrayList<>();
    for (int i = 0; i < getServerCount(); i++) {
      final Database db = getServerDatabase(i, getDatabaseName());
      final int node = i;
      final Thread writer = new Thread(() -> {
        while (!stop.get()) {
          try {
            db.transaction(() -> db.newVertex("Issue8487").set("node", node).save());
            written.incrementAndGet();
          } catch (final Exception e) {
            // Writes are refused or retried while leadership moves: they are the load here, not the assertion.
          }
        }
      }, "issue8487-writer-" + i);
      writer.setDaemon(true);
      writer.start();
      writers.add(writer);
    }

    try {
      for (int round = 0; round < ROUNDS; round++) {
        final long before = written.get();
        Awaitility.await("writes are flowing before round " + round).atMost(30, TimeUnit.SECONDS)
            .pollInterval(20, TimeUnit.MILLISECONDS).until(() -> written.get() > before + 20);

        final int leaderIndex = findLeaderIndex();
        assertThat(leaderIndex).as("round %d: a Raft leader must be elected", round).isGreaterThanOrEqualTo(0);
        final RaftHAServer leader = getRaftPlugin(leaderIndex).getRaftHAServer();
        final int targetIndex = (leaderIndex + 1 + (round % 2)) % getServerCount();
        final RaftPeerId targetId = getRaftPlugin(targetIndex).getRaftHAServer().getLocalPeerId();

        Exception failure = null;
        try {
          leader.transferLeadership(targetId.toString(), 10_000);
        } catch (final Exception e) {
          failure = e;
        }

        final RaftPeerId settledLeader;
        if (failure == null) {
          assertThat(leader.getLeaderId()).as("round %d: the target must be the leader when the transfer returns", round)
              .isEqualTo(targetId);
          settledLeader = targetId;
        } else {
          // Ratis can still make the target lose the election it was sent to win (a commit-index metadata entry the
          // leader appends after sending StartLeaderElection leaves the target one entry short, #8487). That is a real
          // failure, and the call must report it as one: naming where leadership went, not the client that the
          // resulting leader change closed under the RPC.
          assertThat(failure).as("round %d: a failed transfer is a ConfigurationException", round)
              .isInstanceOf(ConfigurationException.class);
          assertThat(failure.getMessage()).as("round %d: the failure names the outcome", round)
              .containsAnyOf("instead of " + targetId, "no leader was elected", "is still the leader");
          settledLeader = null;
        }

        Awaitility.await("round " + round + ": every node names the same leader").atMost(30, TimeUnit.SECONDS)
            .pollInterval(50, TimeUnit.MILLISECONDS).until(() -> {
              final RaftPeerId first = getRaftPlugin(0).getRaftHAServer().getLeaderId();
              if (first == null || (settledLeader != null && !settledLeader.equals(first)))
                return false;
              for (int i = 1; i < getServerCount(); i++)
                if (!first.equals(getRaftPlugin(i).getRaftHAServer().getLeaderId()))
                  return false;
              return true;
            });
      }
    } finally {
      stop.set(true);
      for (final Thread writer : writers)
        writer.join(30_000);
    }
  }
}
