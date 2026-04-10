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
package com.arcadedb.server.ha.ratis;

import com.arcadedb.network.binary.ReplicationQueueFullException;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link RaftGroupCommitter}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class RaftGroupCommitterTest {

  @Test
  void submitAndWaitThrowsReplicationQueueFullWhenQueueIsSaturated() throws Exception {
    // Create a committer with a tiny queue for testing. We don't start() the flusher
    // so nothing drains the queue, letting us fill it up.
    final RaftGroupCommitter committer = new RaftGroupCommitter(null, 10);

    // Access the internal queue via reflection to fill it to capacity
    final Field queueField = RaftGroupCommitter.class.getDeclaredField("queue");
    queueField.setAccessible(true);
    @SuppressWarnings("unchecked")
    final LinkedBlockingQueue<Object> queue = (LinkedBlockingQueue<Object>) queueField.get(committer);

    // Fill the queue to capacity (10,000 entries)
    final Field maxQueueField = RaftGroupCommitter.class.getDeclaredField("MAX_QUEUE_SIZE");
    maxQueueField.setAccessible(true);
    final int maxQueueSize = (int) maxQueueField.get(null);

    // Use dummy objects to fill the queue
    for (int i = 0; i < maxQueueSize; i++)
      queue.put(new Object());

    assertThat(queue.remainingCapacity()).isEqualTo(0);

    // Now submitAndWait should throw ReplicationQueueFullException, not QuorumNotReachedException
    assertThatThrownBy(() -> committer.submitAndWait(new byte[] { 1, 2, 3 }, 1000))
        .isInstanceOf(ReplicationQueueFullException.class)
        .hasMessageContaining("Replication queue is full");
  }

  // -- PendingEntry atomic state tests (phantom commit prevention) --

  @Test
  void dispatchedEntryCannotBeCancelled() {
    // Simulates the flusher dispatching before the caller times out.
    // The caller's cancel attempt must fail so it waits for the Raft result.
    final RaftGroupCommitter.PendingEntry entry = new RaftGroupCommitter.PendingEntry(new byte[] { 1 });

    // Flusher transitions PENDING -> DISPATCHED
    assertThat(entry.state.compareAndSet(RaftGroupCommitter.STATE_PENDING, RaftGroupCommitter.STATE_DISPATCHED)).isTrue();

    // Caller's timeout fires, tries PENDING -> CANCELLED - must fail
    assertThat(entry.state.compareAndSet(RaftGroupCommitter.STATE_PENDING, RaftGroupCommitter.STATE_CANCELLED)).isFalse();
    assertThat(entry.state.get()).isEqualTo(RaftGroupCommitter.STATE_DISPATCHED);
  }

  @Test
  void cancelledEntryCannotBeDispatched() {
    // Simulates the caller timing out before the flusher picks up the entry.
    // The flusher's dispatch attempt must fail so the entry is skipped.
    final RaftGroupCommitter.PendingEntry entry = new RaftGroupCommitter.PendingEntry(new byte[] { 1 });

    // Caller times out, transitions PENDING -> CANCELLED
    assertThat(entry.state.compareAndSet(RaftGroupCommitter.STATE_PENDING, RaftGroupCommitter.STATE_CANCELLED)).isTrue();

    // Flusher tries PENDING -> DISPATCHED - must fail
    assertThat(entry.state.compareAndSet(RaftGroupCommitter.STATE_PENDING, RaftGroupCommitter.STATE_DISPATCHED)).isFalse();
    assertThat(entry.state.get()).isEqualTo(RaftGroupCommitter.STATE_CANCELLED);
  }

  @Test
  void concurrentCancelAndDispatchExactlyOneWins() throws Exception {
    // Stress test: many threads race to cancel or dispatch the same entry.
    // Exactly one must win; no entry should end up both dispatched and cancelled.
    final int iterations = 10_000;
    final AtomicInteger dispatchWins = new AtomicInteger();
    final AtomicInteger cancelWins = new AtomicInteger();

    for (int i = 0; i < iterations; i++) {
      final RaftGroupCommitter.PendingEntry entry = new RaftGroupCommitter.PendingEntry(new byte[] { 1 });
      final CountDownLatch ready = new CountDownLatch(2);
      final CountDownLatch go = new CountDownLatch(1);

      final Thread dispatcher = new Thread(() -> {
        ready.countDown();
        try { go.await(); } catch (final InterruptedException ignored) { }
        if (entry.state.compareAndSet(RaftGroupCommitter.STATE_PENDING, RaftGroupCommitter.STATE_DISPATCHED))
          dispatchWins.incrementAndGet();
      });
      final Thread canceller = new Thread(() -> {
        ready.countDown();
        try { go.await(); } catch (final InterruptedException ignored) { }
        if (entry.state.compareAndSet(RaftGroupCommitter.STATE_PENDING, RaftGroupCommitter.STATE_CANCELLED))
          cancelWins.incrementAndGet();
      });

      dispatcher.start();
      canceller.start();
      ready.await();
      go.countDown();
      dispatcher.join();
      canceller.join();

      // Exactly one must have won
      final int state = entry.state.get();
      assertThat(state).isIn(RaftGroupCommitter.STATE_DISPATCHED, RaftGroupCommitter.STATE_CANCELLED);
    }

    // Both sides should win sometimes (validates the test is actually racing)
    assertThat(dispatchWins.get() + cancelWins.get()).isEqualTo(iterations);
    // With 10k iterations both should win at least once (probabilistically certain)
    assertThat(dispatchWins.get()).isGreaterThan(0);
    assertThat(cancelWins.get()).isGreaterThan(0);
  }

  @Test
  void batchRemoveIfSkipsCancelledEntries() {
    // Simulates what flushBatch does: removeIf with CAS filters out cancelled entries
    // and atomically marks the rest as dispatched.
    final List<RaftGroupCommitter.PendingEntry> batch = new ArrayList<>();
    final RaftGroupCommitter.PendingEntry alive = new RaftGroupCommitter.PendingEntry(new byte[] { 1 });
    final RaftGroupCommitter.PendingEntry cancelled = new RaftGroupCommitter.PendingEntry(new byte[] { 2 });
    cancelled.state.set(RaftGroupCommitter.STATE_CANCELLED);
    batch.add(alive);
    batch.add(cancelled);

    // Same logic as flushBatch
    batch.removeIf(p -> !p.state.compareAndSet(RaftGroupCommitter.STATE_PENDING, RaftGroupCommitter.STATE_DISPATCHED));

    assertThat(batch).hasSize(1);
    assertThat(batch.get(0)).isSameAs(alive);
    assertThat(alive.state.get()).isEqualTo(RaftGroupCommitter.STATE_DISPATCHED);
    // Cancelled entry's state is unchanged
    assertThat(cancelled.state.get()).isEqualTo(RaftGroupCommitter.STATE_CANCELLED);
  }
}
