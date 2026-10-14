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

import org.apache.ratis.protocol.RaftPeerId;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The per-peer throttle behind {@link RaftHAServer#onMisdirectedRaftRequest} (issue #10016): a peer whose name keeps
 * resolving to the wrong node has every heartbeat refused, and that must queue one channel recovery and log one
 * WARNING per interval, not one per refused RPC.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10016MisdirectedThrottleTest {

  private static final RaftPeerId PEER  = RaftPeerId.valueOf("arcade2_2434");
  private static final RaftPeerId OTHER = RaftPeerId.valueOf("arcade3_2434");

  @Test
  void aSlotIsClaimedOncePerInterval() {
    final Map<RaftPeerId, Long> slots = new ConcurrentHashMap<>();

    assertThat(RaftHAServer.claimThrottleSlot(slots, PEER, 1_000, 5_000)).as("a free slot").isTrue();
    assertThat(RaftHAServer.claimThrottleSlot(slots, PEER, 1_000, 5_000)).as("same instant").isFalse();
    assertThat(RaftHAServer.claimThrottleSlot(slots, PEER, 5_999, 5_000)).as("inside the interval").isFalse();
    assertThat(RaftHAServer.claimThrottleSlot(slots, PEER, 6_000, 5_000)).as("the interval has elapsed").isTrue();
    assertThat(slots.get(PEER)).isEqualTo(6_000L);

    assertThat(RaftHAServer.claimThrottleSlot(slots, OTHER, 6_001, 5_000)).as("slots are per peer").isTrue();
  }

  /** A released slot - a recovery the executor rejected - is claimable again at once. */
  @Test
  void aReleasedSlotIsClaimableAgain() {
    final Map<RaftPeerId, Long> slots = new ConcurrentHashMap<>();
    assertThat(RaftHAServer.claimThrottleSlot(slots, PEER, 1_000, 5_000)).isTrue();
    slots.remove(PEER);
    assertThat(RaftHAServer.claimThrottleSlot(slots, PEER, 1_001, 5_000)).isTrue();
  }

  /** Every gRPC handler thread refusing a heartbeat at the same moment: exactly one wins the slot. */
  @Test
  void exactlyOneOfManyConcurrentCallersWins() throws Exception {
    final int threads = 16;
    for (int round = 0; round < 50; round++) {
      final Map<RaftPeerId, Long> slots = new ConcurrentHashMap<>();
      if (round % 2 == 1)
        slots.put(PEER, 0L); // an expired claim: the replace arm, not the putIfAbsent one
      final AtomicInteger winners = new AtomicInteger();
      final CountDownLatch start = new CountDownLatch(1);
      final ExecutorService pool = Executors.newFixedThreadPool(threads);
      try {
        for (int i = 0; i < threads; i++)
          pool.execute(() -> {
            try {
              start.await();
            } catch (final InterruptedException e) {
              Thread.currentThread().interrupt();
              return;
            }
            if (RaftHAServer.claimThrottleSlot(slots, PEER, 10_000, 5_000))
              winners.incrementAndGet();
          });
        start.countDown();
      } finally {
        pool.shutdown();
        assertThat(pool.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
      }
      assertThat(winners.get()).as("round %d", round).isEqualTo(1);
    }
  }
}
