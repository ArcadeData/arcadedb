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
 * Regression test for issue #8533 on a real 3-node cluster: a TARGETED {@code transferLeadership(peerId, timeoutMs)} run
 * while every node keeps writing must hand leadership to the target, every time.
 * <p>
 * Before the fix the target could lose the election it was sent to win. When the target's acknowledgement was the one
 * that completed the majority for the leader's last data entry D, Ratis sent {@code StartLeaderElection} with
 * {@code lastEntry = D} and only then appended the commit-index metadata entry D+1 for that commit. D+1 reached the
 * other follower before the target campaigned, both voters rejected a candidate one entry short, and the cluster stayed
 * leaderless until an election timer fired. #8487 made the call REPORT that outcome; this test requires it not to
 * happen.
 */
@Tag("slow")
class Issue8533TargetedTransferUnderWritesWinsIT extends BaseRaftHATest {

  private static final int ROUNDS = 12;

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Test
  void aTargetedTransferUnderConcurrentWritesHandsLeadershipToTheTarget() throws Exception {
    final int firstLeader = findLeaderIndex();
    assertThat(firstLeader).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final Database leaderDb = getServerDatabase(firstLeader, getDatabaseName());
    leaderDb.transaction(() -> leaderDb.getSchema().getOrCreateVertexType("Issue8533"));
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
            db.transaction(() -> db.newVertex("Issue8533").set("node", node).save());
            written.incrementAndGet();
          } catch (final Exception e) {
            // Writes are refused or retried while leadership moves: they are the load here, not the assertion.
          }
        }
      }, "issue8533-writer-" + i);
      writer.setDaemon(true);
      writer.start();
      writers.add(writer);
    }

    final List<String> failures = new ArrayList<>();
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

        try {
          leader.transferLeadership(targetId.toString(), 10_000);
        } catch (final Exception e) {
          failures.add("round " + round + ": " + e.getMessage());
        }

        // Whatever the outcome, let the cluster agree on one leader before the next round, so one lost election does not
        // cascade into the rounds after it.
        Awaitility.await("round " + round + ": every node names the same leader").atMost(30, TimeUnit.SECONDS)
            .pollInterval(50, TimeUnit.MILLISECONDS).until(() -> {
              final RaftPeerId first = getRaftPlugin(0).getRaftHAServer().getLeaderId();
              if (first == null)
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

    assertThat(failures).as("every targeted transfer under writes must hand leadership to its target").isEmpty();
  }
}
