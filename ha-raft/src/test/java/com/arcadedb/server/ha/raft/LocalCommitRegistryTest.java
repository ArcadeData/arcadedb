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

import com.arcadedb.database.TransactionContext;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * The handshake between the committing thread and the Raft apply thread over a transaction this node originated
 * (issue #6965): exactly one of them owns the pages, and the registry never leaks an entry.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class LocalCommitRegistryTest {
  private static final String DB = "handshake";

  private final LocalCommitRegistry registry = new LocalCommitRegistry();

  @Test
  void theApplyThreadClaimsARegisteredTransactionExactlyOnce() {
    final LocalCommit commit = registered(7L);
    assertThat(registry.size()).isEqualTo(1);

    assertThat(registry.claim(DB, 7L, wal(7L))).isSameAs(commit);
    assertThat(commit.isClaimed()).isTrue();
    assertThat(registry.size()).as("a claim consumes the registration").isZero();

    assertThat(registry.claim(DB, 7L, wal(7L))).as("a replay of the same entry finds nothing to claim").isNull();
    assertThat(registry.withdraw(commit)).as("the committing thread cannot take back a claimed transaction").isFalse();
  }

  @Test
  void theCommittingThreadWithdrawsBeforeTheClaim() {
    final LocalCommit commit = registered(7L);

    assertThat(registry.withdraw(commit)).isTrue();
    assertThat(registry.size()).isZero();
    assertThat(registry.claim(DB, 7L, wal(7L))).as("a withdrawn transaction is applied from its WAL bytes").isNull();
    assertThat(registry.withdraw(commit)).as("withdrawing twice changes nothing").isFalse();
  }

  @Test
  void registrationsAreKeyedByDatabaseAndTransactionId() {
    registered(7L);
    assertThat(registry.claim("other", 7L, wal(7L))).isNull();
    assertThat(registry.claim(DB, 8L, wal(8L))).isNull();
    assertThat(registry.size()).isEqualTo(1);
    assertThat(registry.oldestRegisteredMs()).isGreaterThanOrEqualTo(0L);
  }

  @Test
  void theOutcomeIsAwaitedByTheCommittingThread() throws InterruptedException {
    final LocalCommit commit = registered(7L);
    assertThat(commit.awaitOutcome(10)).isEqualTo(LocalCommit.Outcome.PENDING);

    commit.published();
    assertThat(commit.awaitOutcome(10)).isEqualTo(LocalCommit.Outcome.PUBLISHED);
    assertThat(commit.failure()).isNull();

    final LocalCommit failed = registered(8L);
    final RuntimeException cause = new RuntimeException("boom");
    failed.failed(cause, true);
    assertThat(failed.awaitOutcome(10)).isEqualTo(LocalCommit.Outcome.FAILED);
    assertThat(failed.failure()).isSameAs(cause);
    assertThat(failed.reconciled()).isTrue();
  }

  @Test
  void onlyOneSideWinsUnderContention() throws InterruptedException {
    final int rounds = 200;
    final AtomicInteger claims = new AtomicInteger();
    final AtomicInteger withdrawals = new AtomicInteger();
    for (int round = 0; round < rounds; round++) {
      final LocalCommit commit = registered(round);
      final CountDownLatch start = new CountDownLatch(1);
      final long walTxId = round;
      final List<Thread> threads = new ArrayList<>();
      threads.add(Thread.ofPlatform().unstarted(() -> {
        await(start);
        if (registry.claim(DB, walTxId, wal(walTxId)) != null)
          claims.incrementAndGet();
      }));
      threads.add(Thread.ofPlatform().unstarted(() -> {
        await(start);
        if (registry.withdraw(commit))
          withdrawals.incrementAndGet();
      }));
      threads.forEach(Thread::start);
      start.countDown();
      for (final Thread thread : threads)
        thread.join();
    }
    assertThat(claims.get() + withdrawals.get()).as("every round has exactly one winner").isEqualTo(rounds);
    assertThat(registry.size()).isZero();
  }

  /** An entry with the right id but other bytes is somebody else's transaction, whatever the id says. */
  @Test
  void theClaimMatchesTheBytesNotJustTheId() {
    final LocalCommit commit = registered(7L);
    assertThat(registry.claim(DB, 7L, wal(99L))).as("another node's entry with a colliding id").isNull();
    assertThat(registry.claim(DB, 7L, wal(7L))).isSameAs(commit);
  }

  /** A second registration under the same id is refused, and the first keeps its slot. */
  @Test
  void aCollidingRegistrationIsRefused() {
    final LocalCommit first = registered(7L);
    assertThat(registry.register(new LocalCommit(DB, 7L, mock(TransactionContext.class), null, wal(7L)))).isFalse();
    assertThat(registry.claim(DB, 7L, wal(7L))).isSameAs(first);
  }

  private LocalCommit registered(final long walTxId) {
    final LocalCommit commit = new LocalCommit(DB, walTxId, mock(TransactionContext.class), null, wal(walTxId));
    assertThat(registry.register(commit)).isTrue();
    return commit;
  }

  /** Distinct bytes per transaction id, standing in for the WAL the transaction shipped. */
  private static byte[] wal(final long walTxId) {
    return java.nio.ByteBuffer.allocate(Long.BYTES).putLong(walTxId).array();
  }

  private static void await(final CountDownLatch latch) {
    try {
      latch.await();
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
