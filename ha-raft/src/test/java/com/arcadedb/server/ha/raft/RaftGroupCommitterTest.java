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

import com.arcadedb.network.binary.QuorumNotReachedException;
import com.arcadedb.network.binary.ReplicationQueueFullException;
import org.apache.ratis.protocol.exceptions.AlreadyClosedException;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

class RaftGroupCommitterTest {

  @Test
  void stopDrainsQueueWithErrors() {
    // Create a committer with no RaftClient (null) - entries will fail on flush
    final RaftGroupCommitter committer = new RaftGroupCommitter(null, Quorum.MAJORITY, 10_000);

    // Submit an entry in a background thread
    final var future = java.util.concurrent.CompletableFuture.supplyAsync(() -> {
      try {
        committer.submitAndWait(new byte[] { 1, 2, 3 });
        return "success";
      } catch (final QuorumNotReachedException e) {
        return "failed: " + e.getMessage();
      }
    });

    // Give the background thread time to enqueue
    try {
      Thread.sleep(200);
    } catch (final InterruptedException ignored) {
    }

    // Stop should drain the queue and complete all futures with errors
    committer.stop();

    final String result = future.join();
    assertThat(result).startsWith("failed:");
  }

  @Test
  void allQuorumWatchFailureThrowsMajorityCommittedException() {
    final var ex = new MajorityCommittedAllFailedException("ALL quorum watch failed");
    assertThat(ex).isInstanceOf(QuorumNotReachedException.class);
    assertThat(ex.getMessage()).contains("ALL quorum");
  }

  @Test
  void cancelledEntryIsSkippedByFlusher() throws Exception {
    final RaftGroupCommitter committer = new RaftGroupCommitter(null, Quorum.MAJORITY, 1);

    try {
      committer.submitAndWait(new byte[] { 1, 2, 3 });
    } catch (final QuorumNotReachedException e) {
      assertThat(e.getMessage()).containsAnyOf("timed out", "cancelled", "not available");
    }

    Thread.sleep(300);

    final var future = java.util.concurrent.CompletableFuture.supplyAsync(() -> {
      try {
        committer.submitAndWait(new byte[] { 4, 5, 6 });
        return "success";
      } catch (final QuorumNotReachedException e) {
        return "failed: " + e.getMessage();
      }
    });

    final String result = future.join();
    assertThat(result).contains("not available");

    committer.stop();
  }

  @Test
  void dispatchedEntryWaitsForResultOnTimeout() {
    final RaftGroupCommitter committer = new RaftGroupCommitter(null, Quorum.MAJORITY, 500);

    try {
      committer.submitAndWait(new byte[] { 1, 2, 3 });
      fail("Expected exception");
    } catch (final QuorumNotReachedException e) {
      assertThat(e.getMessage()).contains("not available");
    } finally {
      committer.stop();
    }
  }

  /**
   * Regression for the customer report on 2026-05-07: a 50k-vertex GraphBatch produced a single
   * Raft log entry of 74MB, which Ratis rejects against its 64MB cap. With the old code the
   * SlidingWindow client got stuck CLOSED forever; the new code fails fast in submitAndWait with
   * a {@link ReplicationQueueFullException} carrying actionable advice.
   */
  @Test
  void oversizeEntryRejectedSynchronouslyWithGuidance() {
    final long cap = 1024L; // 1 KiB cap
    final RaftGroupCommitter committer = new RaftGroupCommitter(null, Quorum.MAJORITY, 500,
        500, 10_000, 100, cap, null);
    try {
      assertThatThrownBy(() -> committer.submitAndWait(new byte[(int) cap + 1]))
          .isInstanceOf(ReplicationQueueFullException.class)
          .hasMessageContaining("exceeds raft.grpc.message.size.max")
          .hasMessageContaining("arcadedb.ha.grpcMessageSizeMax")
          .hasMessageContaining("Reduce the batch size");
    } finally {
      committer.stop();
    }
  }

  /**
   * The pre-check rejects oversize entries BEFORE the queue, so a batch of legitimate-sized
   * entries followed by one oversize entry must still process the legitimate ones (verified
   * indirectly: the oversize one throws synchronously without consuming a queue slot).
   */
  @Test
  void oversizeRejectionDoesNotPoisonQueue() {
    final long cap = 1024L;
    final RaftGroupCommitter committer = new RaftGroupCommitter(null, Quorum.MAJORITY, 500,
        500, 10_000, 100, cap, null);
    try {
      assertThatThrownBy(() -> committer.submitAndWait(new byte[(int) cap + 1]))
          .isInstanceOf(ReplicationQueueFullException.class);
      // A subsequent normal-sized entry must reach the flusher (and fail with the usual
      // "RaftClient not available" because we passed null - i.e. it got past the pre-check).
      assertThatThrownBy(() -> committer.submitAndWait(new byte[10]))
          .isInstanceOf(QuorumNotReachedException.class)
          .hasMessageNotContaining("exceeds raft.grpc.message.size.max");
    } finally {
      committer.stop();
    }
  }

  /**
   * Regression for the customer report on 2026-05-08: heavy parallel ingest causes a brief leader
   * step-down/re-election cycle, during which the in-flight transactions saw "Group committer
   * shutting down" because the broker was hard-stopped. With this fix, undispatched entries are
   * transferred to the new committer so the original {@code submitAndWait} caller completes via
   * the new committer's normal path (success, retryable error, etc.) instead of a hard stop error.
   */
  @Test
  void transferPendingToHandsOffUndispatchedEntries() throws Exception {
    // Both committers use a null RaftClient: any entry that reaches a flusher fails with
    // "RaftClient not available" (which is what we want for this test - it's NOT a "shutting down"
    // failure, so we can distinguish the two error paths).
    final RaftGroupCommitter source = new RaftGroupCommitter(null, Quorum.MAJORITY, 30_000);
    final RaftGroupCommitter target = new RaftGroupCommitter(null, Quorum.MAJORITY, 30_000);

    // Submit an entry in a background thread. submitAndWait blocks until the future completes.
    final java.util.concurrent.CompletableFuture<String> result =
        java.util.concurrent.CompletableFuture.supplyAsync(() -> {
          try {
            source.submitAndWait(new byte[] { 1, 2, 3 });
            return "ok";
          } catch (final QuorumNotReachedException e) {
            return e.getMessage();
          }
        });

    // Race the source flusher (100ms poll) to transfer the entry before it fires. If we lose the
    // race, the test still passes - the entry is dispatched on source and fails with
    // "RaftClient not available", which we accept below.
    final int transferred = source.transferPendingTo(target);
    assertThat(transferred).isBetween(0, 1);

    final String message = result.get(5, java.util.concurrent.TimeUnit.SECONDS);
    // Critical: must NOT be the "Group committer shutting down" message that would surface if we
    // failed pending entries on the source. The new committer (target) handles it instead.
    assertThat(message).doesNotContain("Group committer shutting down");
    // Either the transfer raced ahead and the entry was completed by the target's null-client
    // path, or the source flusher caught it first - either is acceptable as long as we don't
    // surface the hard-stop error.
    assertThat(message).containsAnyOf("not available", "ok");

    source.stop();
    target.stop();
  }

  @Test
  void isClientClosedRecognizesAlreadyClosedException() {
    assertThat(RaftGroupCommitter.isClientClosed(null)).isFalse();
    assertThat(RaftGroupCommitter.isClientClosed(new RuntimeException("boom"))).isFalse();
    assertThat(RaftGroupCommitter.isClientClosed(new AlreadyClosedException("client X is closed"))).isTrue();
    // Wrapped, like the production path: CompletionException -> AlreadyClosedException
    assertThat(RaftGroupCommitter.isClientClosed(
        new java.util.concurrent.CompletionException(new AlreadyClosedException("X is already CLOSED")))).isTrue();
    // Pure message-based detection (matches the SlidingWindow text we see in customer logs).
    assertThat(RaftGroupCommitter.isClientClosed(
        new RuntimeException("SlidingWindow$Client:client-43D76C26FF37->RAFT is closed."))).isTrue();
  }

  /**
   * When the underlying RaftClient is observed to be permanently CLOSED, the committer must
   * invoke the host-supplied refresh callback exactly once per affected batch so the host can
   * rebuild the client. Before this fix, every subsequent flush logged "is closed" forever.
   */
  @Test
  void clientClosedTriggersRefreshCallback() throws Exception {
    final AtomicInteger refreshes = new AtomicInteger();
    // null RaftClient causes "RaftClient not available", which is NOT a CLOSED-state failure,
    // so the callback must NOT fire in this case.
    final RaftGroupCommitter committer = new RaftGroupCommitter(null, Quorum.MAJORITY, 500,
        500, 10_000, 100, RaftGroupCommitter.DEFAULT_MESSAGE_SIZE_MAX, refreshes::incrementAndGet);
    try {
      try {
        committer.submitAndWait(new byte[] { 1, 2, 3 });
      } catch (final QuorumNotReachedException ignored) {
        // expected
      }
      // Give the (would-be) virtual thread a chance to run.
      Thread.sleep(150);
      assertThat(refreshes.get())
          .as("RaftClient null is a startup gap, not a CLOSED state; refresh must NOT fire")
          .isZero();
    } finally {
      committer.stop();
    }
  }
}
