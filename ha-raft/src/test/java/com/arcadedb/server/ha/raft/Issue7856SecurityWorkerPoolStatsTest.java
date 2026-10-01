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

import com.arcadedb.utility.DedicatedThreadPool.PoolStats;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7856: the HA security workers - the leader-side membership seed and the rejoining node's catch-up - must
 * report the load and the coalesced count their executor rows publish, so an operator can see a scale-up storm
 * folding into one seed, or a worker that has stopped draining.
 */
class Issue7856SecurityWorkerPoolStatsTest {

  private static final long TIMEOUT_MS = 10_000L;

  /** A seed held on a latch, so the outstanding state is observed rather than raced. */
  private static final class BlockingSeed implements MembershipSecuritySeeder.SecuritySeed {
    final CountDownLatch started = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);

    @Override
    public List<String> seed(final long retryBudgetMs) {
      started.countDown();
      try {
        release.await(TIMEOUT_MS, TimeUnit.MILLISECONDS);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      return List.of();
    }
  }

  @Test
  void anIdleSeederReportsNoThreadAndItsOneFreeSlot() {
    final MembershipSecuritySeeder seeder = new MembershipSecuritySeeder(() -> true, () -> 1_000L, budget -> List.of());
    try {
      assertThat(seeder.getPoolStats()).isEqualTo(MembershipSecuritySeeder.EMPTY_POOL_STATS);
      assertThat(seeder.getCoalescedSeeds()).isZero();
    } finally {
      seeder.close();
    }
  }

  /**
   * Every way a seed request can end without a run of its own is counted - folded into the outstanding seed,
   * answered by one that just finished, refused because the node is stopping - and none of the runs is.
   */
  @Test
  void everySeedRequestWithoutARunOfItsOwnIsCoalesced() throws Exception {
    final BlockingSeed seed = new BlockingSeed();
    final MembershipSecuritySeeder seeder = new MembershipSecuritySeeder(() -> true, () -> 1_000L, seed);
    try {
      final CompletableFuture<List<String>> first = seeder.scheduleForTest("the first request");
      assertThat(seed.started.await(TIMEOUT_MS, TimeUnit.MILLISECONDS)).isTrue();

      final PoolStats busy = seeder.getPoolStats();
      assertThat(busy.poolSize()).isEqualTo(1);
      assertThat(busy.activeThreads()).as("the seed is running on the owned worker").isEqualTo(1);
      assertThat(seeder.getCoalescedSeeds()).as("a seed that runs is not coalesced").isZero();

      assertThat(seeder.scheduleForTest("a request while one is outstanding")).isSameAs(first);
      assertThat(seeder.getCoalescedSeeds()).as("the fold").isEqualTo(1);

      seed.release.countDown();
      first.get(TIMEOUT_MS, TimeUnit.MILLISECONDS);

      assertThat(seeder.scheduleReusingForTest("an admission a round trip behind")).isSameAs(first);
      assertThat(seeder.getCoalescedSeeds()).as("the reuse of a seed that just finished").isEqualTo(2);
    } finally {
      seeder.close();
    }

    assertThat(seeder.scheduleForTest("a request after the stop")).as("a stopped worker refuses the seed").isNull();
    assertThat(seeder.getCoalescedSeeds()).as("the refusal of a stopped worker").isEqualTo(3);
  }

  /** On the test seam the executor is the caller's, so its load is not this seeder's to report. */
  @Test
  void aSeederOnAHandedInExecutorReportsAnIdleRow() {
    final MembershipSecuritySeeder seeder = new MembershipSecuritySeeder(() -> true, () -> 1_000L, budget -> List.of(),
        Runnable::run);
    seeder.scheduleForTest("a request");
    assertThat(seeder.getPoolStats()).isEqualTo(MembershipSecuritySeeder.EMPTY_POOL_STATS);
  }

  @Test
  void theCatchUpCountsEveryRefusedRequest() {
    try (final SecurityCatchUp catchUp = new SecurityCatchUp()) {
      final PoolStats idle = catchUp.getPoolStats();
      assertThat(idle.poolSize()).isZero();
      assertThat(idle.queueDepth()).isZero();
      assertThat(idle.queueCapacityRemaining()).as("the single queue slot").isEqualTo(1);
      assertThat(catchUp.getCoalescedRequests()).isZero();

      catchUp.onRejected(new SecurityCatchUp.Attempt(catchUp.tryTakeRequest(), () -> {
      }));
      catchUp.onRejected(() -> {
      });

      assertThat(catchUp.getCoalescedRequests()).isEqualTo(2);
    }
  }
}
