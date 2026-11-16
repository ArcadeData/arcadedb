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

import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.TransactionCommittedRemotelyException;
import com.arcadedb.network.binary.QuorumNotReachedException;
import com.arcadedb.network.binary.ReplicatedEntryTooLargeException;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.ratis.client.retry.ClientRetryEvent;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.exceptions.AlreadyClosedException;
import org.apache.ratis.protocol.exceptions.LeaderNotReadyException;
import org.apache.ratis.protocol.exceptions.LeaderSteppingDownException;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.apache.ratis.protocol.exceptions.StateMachineException;
import org.apache.ratis.protocol.exceptions.TimeoutIOException;
import org.apache.ratis.retry.RetryPolicies;
import org.apache.ratis.retry.RetryPolicy;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.TimeDuration;
import org.apache.ratis.util.Timestamp;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.arcadedb.server.StaticBaseServerTest.allocateFreePorts;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class RaftGroupCommitterTest {

  @Test
  void stopDrainsQueueWithErrors() {
    // Create a committer with no RaftClient (null) - entries will fail on flush
    final RaftGroupCommitter committer = new RaftGroupCommitter(null, Quorum.MAJORITY, 10_000);

    // Submit an entry in a background thread
    final var future = CompletableFuture.supplyAsync(() -> {
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
    // #8481: the MAJORITY committed the entry, so it is a committed-remotely outcome and never a retryable one
    assertThat(ex).isInstanceOf(TransactionCommittedRemotelyException.class);
    assertThat(ex).isNotInstanceOf(NeedRetryException.class);
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

    final var future = CompletableFuture.supplyAsync(() -> {
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
   * a {@link ReplicatedEntryTooLargeException} carrying actionable advice.
   * <p>
   * Issue #4743 narrowed the cap to the SMALLER of the gRPC frame limit and the Ratis appender byte
   * limit, and made the failure non-retryable - retrying an oversized entry makes every newly elected
   * leader step down in turn. See {@link Issue4743OversizedRaftEntryTest} for those guarantees.
   */
  @Test
  void oversizeEntryRejectedSynchronouslyWithGuidance() {
    final long cap = 1024L; // 1 KiB cap
    final RaftGroupCommitter committer = new RaftGroupCommitter(null, Quorum.MAJORITY, 500,
        500, 10_000, 100, cap, null);
    try {
      assertThatThrownBy(() -> committer.submitAndWait(new byte[(int) cap + 1]))
          .isInstanceOf(ReplicatedEntryTooLargeException.class)
          .hasMessageContaining("maximum replicated Raft entry size")
          .hasMessageContaining("arcadedb.ha.appendBufferSize")
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
          .isInstanceOf(ReplicatedEntryTooLargeException.class);
      // A subsequent normal-sized entry must reach the flusher (and fail with the usual
      // "RaftClient not available" because we passed null - i.e. it got past the pre-check).
      assertThatThrownBy(() -> committer.submitAndWait(new byte[10]))
          .isInstanceOf(QuorumNotReachedException.class)
          .hasMessageNotContaining("maximum replicated Raft entry size");
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
    final CompletableFuture<String> result =
        CompletableFuture.supplyAsync(() -> {
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

    final String message = result.get(5, TimeUnit.SECONDS);
    // Critical: must NOT be the "Group committer shutting down" message that would surface if we
    // failed pending entries on the source. The new committer (target) handles it instead.
    assertThat(message).doesNotContain("Group committer shutting down");
    // Three legitimate outcomes, none of which is the hard-stop error: the transfer raced ahead and
    // the target's null-client path completed it ("not available"/"ok"), the source flusher caught it
    // first ("not available"), or the transfer fully won and the caller reclaimed its own un-dispatched
    // entry ("stopped before dispatch").
    assertThat(message).containsAnyOf("not available", "ok", "stopped before dispatch");

    source.stop();
    target.stop();
  }

  /**
   * A {@code submitAndWait} that races in AFTER {@link RaftGroupCommitter#stop()} (or
   * {@link RaftGroupCommitter#transferPendingTo}) has halted the flusher and drained the queue must
   * NOT be stranded: the entry would otherwise sit in a queue nothing polls, blocking the caller for
   * the whole {@code 2 * quorumTimeout} grace window. This reproduces the leader-refresh race where a
   * caller holds the old (volatile) broker reference and submits just after the refresh stopped it.
   */
  @Test
  void submitAfterStopFailsFastInsteadOfStranding() throws Exception {
    // quorumTimeout 30s -> a stranded caller would block 2*30s = 60s; we require completion far sooner.
    final RaftGroupCommitter committer = new RaftGroupCommitter(null, Quorum.MAJORITY, 30_000);
    committer.stop(); // flusher halted and queue drained BEFORE we submit

    final CompletableFuture<String> result = CompletableFuture.supplyAsync(() -> {
      try {
        committer.submitAndWait(new byte[] { 1, 2, 3 });
        return "ok";
      } catch (final QuorumNotReachedException e) {
        return "failed: " + e.getMessage();
      }
    });

    // Without the fix the future never completes and this get() times out; with the fix the caller
    // fails fast with a retryable error well within the 2*quorumTimeout window.
    final String message = result.get(3, TimeUnit.SECONDS);
    assertThat(message).startsWith("failed:");
    assertThat(message).doesNotContain("Group committer shutting down");
  }

  /**
   * Byte-bounded backpressure (issue #4809 follow-up): because a single entry can be up to
   * messageSizeMax, the entry-count bound alone can let a flood of large transactions exhaust the
   * heap. The byte-reservation gate must accept entries up to the budget and then reject (after the
   * offer timeout) once the budget is exhausted - deterministically verified at the gate, since a
   * null client drains too fast for bytes to accumulate through submitAndWait.
   */
  @Test
  void reserveQueuedBytesEnforcesBudget() {
    // budget = 1024 bytes (clamped up to messageSizeMax=1024), offer timeout = 50ms.
    final RaftGroupCommitter committer = new RaftGroupCommitter(null, Quorum.MAJORITY, 200,
        500, 100_000, 50, 1024L, 1024L, null);
    try {
      assertThat(committer.reserveQueuedBytes(512)).as("first 512B fits").isTrue();
      assertThat(committer.reserveQueuedBytes(512)).as("second 512B fills the 1024B budget").isTrue();
      assertThat(committer.getQueuedBytes()).isEqualTo(1024L);
      // Over budget: must wait the offer timeout and then refuse, leaving the accounting unchanged.
      final long before = System.currentTimeMillis();
      assertThat(committer.reserveQueuedBytes(1)).as("over-budget reservation is refused").isFalse();
      assertThat(System.currentTimeMillis() - before).as("refusal waits at least the offer timeout")
          .isGreaterThanOrEqualTo(45L);
      assertThat(committer.getQueuedBytes()).as("a refused reservation must not change the accounting")
          .isEqualTo(1024L);
    } finally {
      committer.stop();
    }
  }

  /**
   * The reserve (enqueue) and release (dequeue/stop) accounting must balance to exactly zero so the
   * byte budget cannot leak and wedge the leader into permanent false backpressure.
   */
  @Test
  void byteAccountingBalancesToZeroAfterDrain() throws Exception {
    final RaftGroupCommitter committer = new RaftGroupCommitter(null, Quorum.MAJORITY, 200,
        500, 100_000, 50, 1024L, 1024L, null);
    // Each submit reserves then (null client) fails fast; the flusher drains and releases the bytes.
    for (int i = 0; i < 20; i++) {
      try {
        committer.submitAndWait(new byte[256]);
      } catch (final QuorumNotReachedException ignored) {
        // expected: "RaftClient not available"
      }
    }
    committer.stop();
    assertThat(committer.getQueuedBytes()).as("queued-bytes accounting must balance to zero").isZero();
  }

  /**
   * A single maximum-size entry must always be enqueueable: the constructor clamps the byte budget up
   * to messageSizeMax so backpressure can never permanently deadlock a legitimate max-size transaction.
   */
  @Test
  void byteBudgetNeverBelowMessageSizeMax() {
    // Request a tiny 10-byte budget but a 1KB message cap: the effective budget must be raised to 1KB.
    final RaftGroupCommitter committer = new RaftGroupCommitter(null, Quorum.MAJORITY, 200,
        500, 100_000, 50, 1024L, 10L, null);
    try {
      // A 1KB entry equals the message cap; it must pass the size pre-check and the byte reservation
      // (failing only later with "not available" because the client is null), not a byte-budget error.
      assertThatThrownBy(() -> committer.submitAndWait(new byte[1024]))
          .isInstanceOf(QuorumNotReachedException.class)
          .hasMessageNotContaining("byte budget");
    } finally {
      committer.stop();
    }
  }

  /**
   * Regression for issue #4743 (leader churn during GraphBatch bulk load): the entry was dispatched
   * via {@code raftClient.async().send()} and the reply future failed with the Ratis
   * "Stream completed: no reply for async request" {@link AlreadyClosedException}. The request may
   * have reached the Raft log and can still commit after the (same-node) re-election, so the caller
   * must see a {@link ReplicationDispatchedTimeoutException} (indeterminate outcome, phase 2 must be
   * marked for local apply) and NOT a plain {@link QuorumNotReachedException} ("safe to roll back").
   */
  @Test
  void dispatchedEntryAsyncFailureSurfacesIndeterminateOutcome() {
    final RaftClient client = mock(RaftClient.class, RETURNS_DEEP_STUBS);
    when(client.async().send(any(Message.class))).thenReturn(
        CompletableFuture.failedFuture(new CompletionException(
            new AlreadyClosedException("client-X->peer: Stream completed: no reply for async request cid=732"))));

    final RaftGroupCommitter committer = new RaftGroupCommitter(client, Quorum.MAJORITY, 2_000);
    try {
      assertThatThrownBy(() -> committer.submitAndWait(new byte[] { 1, 2, 3 }))
          .isInstanceOf(ReplicationDispatchedTimeoutException.class)
          .hasMessageContaining("dispatched");
    } finally {
      committer.stop();
    }
  }

  /**
   * Regression for issue #4743: during a RaftClient refresh after leader churn, the flusher thread
   * is interrupted while awaiting the quorum result of an already-dispatched entry (the June log's
   * "Group commit entry failed: InterruptedException"). The outcome is indeterminate, so the waiting
   * transaction must observe a {@link ReplicationDispatchedTimeoutException}, not a plain retryable
   * error that would let the leader roll back (and later origin-skip) a write the followers commit.
   */
  @Test
  void flusherInterruptedMidQuorumWaitSurfacesIndeterminateOutcome() throws Exception {
    final RaftClient client = mock(RaftClient.class, RETURNS_DEEP_STUBS);
    final CountDownLatch sent = new CountDownLatch(1);
    when(client.async().send(any(Message.class))).thenAnswer(inv -> {
      sent.countDown();
      return new CompletableFuture<RaftClientReply>(); // never completes: reply lost in the churn
    });

    final RaftGroupCommitter committer = new RaftGroupCommitter(client, Quorum.MAJORITY, 30_000);
    final CompletableFuture<Throwable> outcome = CompletableFuture.supplyAsync(() -> {
      try {
        committer.submitAndWait(new byte[] { 1, 2, 3 });
        return null;
      } catch (final Throwable t) {
        return t;
      }
    });

    assertThat(sent.await(5, TimeUnit.SECONDS)).as("entry must be dispatched before the interrupt").isTrue();
    committer.stop(); // interrupts the flusher mid futures[i].get()

    final Throwable t = outcome.get(5, TimeUnit.SECONDS);
    assertThat(t).isInstanceOf(ReplicationDispatchedTimeoutException.class);
    assertThat(t).hasMessageContaining("dispatched");
  }

  /**
   * A definitive negative {@link RaftClientReply} still does not prove an earlier internal Ratis
   * retry of the same call was not appended by a stepping-down leader, so a failed reply for a
   * dispatched entry must also surface as indeterminate.
   */
  @Test
  void dispatchedEntryFailedReplySurfacesIndeterminateOutcome() {
    final RaftClient client = mock(RaftClient.class, RETURNS_DEEP_STUBS);
    final RaftClientReply reply = mock(RaftClientReply.class);
    when(reply.isSuccess()).thenReturn(false);
    when(client.async().send(any(Message.class))).thenReturn(CompletableFuture.completedFuture(reply));

    final RaftGroupCommitter committer = new RaftGroupCommitter(client, Quorum.MAJORITY, 2_000);
    try {
      assertThatThrownBy(() -> committer.submitAndWait(new byte[] { 1, 2, 3 }))
          .isInstanceOf(ReplicationDispatchedTimeoutException.class)
          .hasMessageContaining("dispatched");
    } finally {
      committer.stop();
    }
  }

  /**
   * A reply carrying a {@code StateMachineException} whose cause is a retryable engine conflict is the leader
   * refusing the entry BEFORE appending it (page-version validation in {@code preAppendTransaction}, issue #6965):
   * a definite outcome, surfaced as that very conflict so the caller's retry loop handles it, never as the
   * indeterminate "dispatched, outcome unknown".
   */
  @Test
  void anEntryRefusedBeforeAppendSurfacesTheRetryableConflict() {
    final RaftClient client = mock(RaftClient.class, RETURNS_DEEP_STUBS);
    final RaftClientReply reply = mock(RaftClientReply.class);
    when(reply.isSuccess()).thenReturn(false);
    final ConcurrentModificationException conflict = new ConcurrentModificationException("Concurrent modification on page 3/0");
    when(reply.getException()).thenReturn(new StateMachineException(conflict.getMessage(), conflict, false));
    when(client.async().send(any(Message.class))).thenReturn(CompletableFuture.completedFuture(reply));

    final RaftGroupCommitter committer = new RaftGroupCommitter(client, Quorum.MAJORITY, 2_000);
    try {
      assertThatThrownBy(() -> committer.submitAndWait(new byte[] { 1, 2, 3 }))
          .isInstanceOf(ConcurrentModificationException.class)
          .isNotInstanceOf(ReplicationDispatchedTimeoutException.class)
          .hasMessageContaining("page 3/0");
    } finally {
      committer.stop();
    }
  }

  /**
   * The Ratis client does not hand a refusal back as a failed reply: {@code RaftClientImpl.handleRaftException}
   * completes the send future exceptionally with the {@code StateMachineException}. The committer must recognise
   * the refusal there too, or a definite, retryable conflict would be reported as an unknown outcome.
   */
  @Test
  void anEntryRefusedBeforeAppendIsRecognisedOnTheExceptionalPath() {
    final RaftClient client = mock(RaftClient.class, RETURNS_DEEP_STUBS);
    final ConcurrentModificationException conflict = new ConcurrentModificationException("Concurrent modification on page 3/0");
    final CompletableFuture<RaftClientReply> refused = new CompletableFuture<>();
    refused.completeExceptionally(new CompletionException(new StateMachineException(conflict.getMessage(), conflict, false)));
    when(client.async().send(any(Message.class))).thenReturn(refused);

    final RaftGroupCommitter committer = new RaftGroupCommitter(client, Quorum.MAJORITY, 2_000);
    try {
      assertThatThrownBy(() -> committer.submitAndWait(new byte[] { 1, 2, 3 }))
          .isInstanceOf(ConcurrentModificationException.class)
          .isNotInstanceOf(ReplicationDispatchedTimeoutException.class)
          .hasMessageContaining("page 3/0");
    } finally {
      committer.stop();
    }
  }

  /**
   * Issue #11951: while a leadership transfer is pending, the leader refuses every write with
   * {@link LeaderSteppingDownException}, which Ratis raises before the entry is appended. The Ratis client hands it back as an
   * exceptional completion of the send future. On the one attempt the client made, nothing of the entry reached any log: a
   * definite, retryable refusal (HTTP 503 / gRPC ABORTED), not the 500 "outcome unknown" it used to be answered with.
   */
  @Test
  void anEntryRefusedByASteppingDownLeaderIsARetryableRefusal() {
    final RaftClient client = mock(RaftClient.class, RETURNS_DEEP_STUBS);
    when(client.async().send(any(Message.class))).thenReturn(CompletableFuture.failedFuture(
        new CompletionException(new LeaderSteppingDownException("s1@group-ABC is stepping down"))));

    final RaftGroupCommitter committer = new RaftGroupCommitter(client, Quorum.MAJORITY, 2_000);
    try {
      assertThatThrownBy(() -> committer.submitAndWait(new byte[] { 1, 2, 3 }))
          .isInstanceOf(NeedRetryException.class)
          .isNotInstanceOf(ReplicationDispatchedTimeoutException.class)
          .hasMessageContaining("stepping down");
    } finally {
      committer.stop();
    }
  }

  /**
   * Issue #11951: the stepping-down refusal describes the ATTEMPT it answers. When the Ratis client sent the entry again
   * after an attempt that may have reached a log - a reply lost past the request timeout, a stream reset, a
   * {@code NotLeaderException} from a deposed leader that had appended it - that copy may commit, and Ratis checks the
   * leader state before its retry cache, so even the leader that holds it refuses the re-send. The outcome stays unknown.
   * The #6965 page conflict keeps its definite verdict on a re-sent copy, as before this change.
   */
  @Test
  void aSteppingDownRefusalOfAReSentEntryStaysAnUnknownOutcome() {
    final ConcurrentModificationException conflict = new ConcurrentModificationException("Concurrent modification on page 3/0");

    assertThatThrownBy(() -> submitRefusedAfterAnAmbiguousAttempt(new LeaderSteppingDownException("s1@group-ABC is stepping down")))
        .isInstanceOf(ReplicationDispatchedTimeoutException.class)
        .hasMessageContaining("re-sent after an attempt that may have reached a log")
        .hasMessageContaining("outcome unknown");

    assertThatThrownBy(() -> submitRefusedAfterAnAmbiguousAttempt(new StateMachineException(conflict.getMessage(), conflict, false)))
        .isInstanceOf(ConcurrentModificationException.class)
        .hasMessageContaining("page 3/0");
  }

  /** Submits one entry whose only attempt is refused with {@code refusal}, after the client recorded an ambiguous one. */
  private static void submitRefusedAfterAnAmbiguousAttempt(final Throwable refusal) {
    final RetryPolicy policy = RaftGroupCommitter.recordingResends(RetryPolicies.retryForeverNoSleep());
    final RaftClient client = mock(RaftClient.class, RETURNS_DEEP_STUBS);
    when(client.async().send(any(Message.class))).thenAnswer(invocation -> {
      // What OrderedAsync does before it sends a call again: it asks the retry policy about the attempt that failed
      final Message message = invocation.getArgument(0);
      policy.handleAttemptFailure(new ClientRetryEvent(1, writeRequest(message), 1, new TimeoutIOException("request #1 timeout 10s"),
          Timestamp.currentTime()));
      return CompletableFuture.failedFuture(new CompletionException(refusal));
    });

    final RaftGroupCommitter committer = new RaftGroupCommitter(client, Quorum.MAJORITY, 2_000);
    try {
      committer.submitAndWait(new byte[] { 1, 2, 3 });
    } finally {
      committer.stop();
    }
  }

  /**
   * The record of a re-send rests on two things the Ratis client does, both pinned here against the shipped Ratis rather than
   * a mock: it consults the retry policy before sending a call again, and the request it hands the policy carries the very
   * {@link Message} instance the committer gave it. A real client dialling a port nobody listens on fails every attempt at the
   * transport, so the entry must come back marked; one the client never sent stays unmarked. The client's dummy first
   * request is switched off so the entry is the head of the sliding window: a failed head fails every request queued
   * behind it without sending them.
   */
  @Test
  void theRatisClientMarksTheEntryItSendsAgain() throws Exception {
    final RaftPeer unreachable = RaftPeer.newBuilder().setId("s1").setAddress("127.0.0.1:" + allocateFreePorts(1)[0]).build();
    final RaftProperties properties = new RaftProperties();
    RaftClientConfigKeys.Rpc.setRequestTimeout(properties, TimeDuration.valueOf(2, TimeUnit.SECONDS));
    RaftClientConfigKeys.Async.Experimental.setSendDummyRequest(properties, false);
    try (final RaftClient client = RaftClient.newBuilder()
        .setRaftGroup(RaftGroup.valueOf(RaftGroupId.randomId(), unreachable))
        .setProperties(properties)
        .setRetryPolicy(RaftGroupCommitter.recordingResends(
            RetryPolicies.retryUpToMaximumCountWithFixedSleep(3, TimeDuration.valueOf(10, TimeUnit.MILLISECONDS))))
        .build()) {
      final RaftGroupCommitter.EntryMessage sent = new RaftGroupCommitter.EntryMessage(ByteString.copyFromUtf8("sent"));
      final RaftGroupCommitter.EntryMessage notSent = new RaftGroupCommitter.EntryMessage(ByteString.copyFromUtf8("not sent"));

      final Throwable failure = catchThrowable(() -> client.async().send(sent).get(30, TimeUnit.SECONDS));
      assertThat(failure).isInstanceOf(ExecutionException.class);
      assertThat(sent.earlierAttemptMayHaveLanded)
          .as("the client asked the retry policy before sending the entry again (the call failed with %s)", failure.getCause())
          .isTrue();
      assertThat(notSent.earlierAttemptMayHaveLanded).isFalse();
    }
  }

  /**
   * Only an attempt whose failure leaves open whether a leader appended it marks the entry. The two refusals a leadership
   * change answers most writes with prove the attempt never reached a log: {@code LeaderNotReadyException} (Ratis raises it
   * only before the append) and a {@code NotLeaderException} that names a suggested leader (the deposed leader's answer to
   * the entries it had appended names none). Without that distinction a write sent to a leader that was not ready yet, or to
   * a follower, and then refused by a leader stepping down would be reported as an unknown outcome.
   */
  @Test
  void onlyAnAttemptThatMayHaveBeenAppendedMarksTheEntry() {
    final RaftGroupMemberId member = RaftGroupMemberId.valueOf(RaftPeerId.valueOf("s1"), RaftGroupId.randomId());
    final RaftPeer s2 = RaftPeer.newBuilder().setId("s2").setAddress("127.0.0.1:1").build();

    assertThat(RaftGroupCommitter.mayHaveBeenAppended(new LeaderNotReadyException(member))).isFalse();
    assertThat(RaftGroupCommitter.mayHaveBeenAppended(new CompletionException(new LeaderNotReadyException(member)))).isFalse();
    assertThat(RaftGroupCommitter.mayHaveBeenAppended(new NotLeaderException(member, s2, List.of(s2))))
        .as("a follower that knows the leader refused it before the append").isFalse();

    assertThat(RaftGroupCommitter.mayHaveBeenAppended(new NotLeaderException(member, null, List.of(s2))))
        .as("what a deposed leader answers the entries it had appended").isTrue();
    assertThat(RaftGroupCommitter.mayHaveBeenAppended(new TimeoutIOException("request #1 timeout 10s"))).isTrue();
    assertThat(RaftGroupCommitter.mayHaveBeenAppended(new AlreadyClosedException("Stream completed: no reply for async request")))
        .isTrue();

    final RetryPolicy policy = RaftGroupCommitter.recordingResends(RetryPolicies.retryForeverNoSleep());
    final RaftGroupCommitter.EntryMessage refusedBeforeAppend = new RaftGroupCommitter.EntryMessage(ByteString.copyFromUtf8("a"));
    policy.handleAttemptFailure(new ClientRetryEvent(1, writeRequest(refusedBeforeAppend), 1, new LeaderNotReadyException(member),
        Timestamp.currentTime()));
    policy.handleAttemptFailure(new ClientRetryEvent(2, writeRequest(refusedBeforeAppend), 1,
        new NotLeaderException(member, s2, List.of(s2)), Timestamp.currentTime()));
    assertThat(refusedBeforeAppend.earlierAttemptMayHaveLanded).isFalse();

    final RaftGroupCommitter.EntryMessage deposed = new RaftGroupCommitter.EntryMessage(ByteString.copyFromUtf8("b"));
    policy.handleAttemptFailure(new ClientRetryEvent(1, writeRequest(deposed), 1, new NotLeaderException(member, null, List.of(s2)),
        Timestamp.currentTime()));
    assertThat(deposed.earlierAttemptMayHaveLanded).isTrue();
  }

  /** The shared client of every node is built with the recording policy, and the wrapper leaves every verdict unchanged. */
  @Test
  void theDataClientRetryPolicyRecordsResendsAndKeepsItsVerdicts() {
    assertThat(RaftHAServer.DATA_CLIENT_RETRY_POLICY).isInstanceOf(RaftGroupCommitter.ResendRecordingRetryPolicy.class);

    final RaftClientRequest request = writeRequest(new RaftGroupCommitter.EntryMessage(ByteString.copyFromUtf8("x")));
    final TimeoutIOException timeout = new TimeoutIOException("request #1 timeout 10s");
    final RetryPolicy.Action first = RaftHAServer.DATA_CLIENT_RETRY_POLICY.handleAttemptFailure(
        new ClientRetryEvent(1, request, 1, timeout, Timestamp.currentTime()));
    assertThat(first.shouldRetry()).isTrue();
    assertThat(first.getSleepTime().toLong(TimeUnit.MILLISECONDS)).isEqualTo(1_000L);
    assertThat(RaftHAServer.DATA_CLIENT_RETRY_POLICY.handleAttemptFailure(
        new ClientRetryEvent(61, request, 61, timeout, Timestamp.currentTime())).shouldRetry()).isFalse();
    assertThat(((RaftGroupCommitter.EntryMessage) request.getMessage()).earlierAttemptMayHaveLanded).isTrue();
  }

  private static RaftClientRequest writeRequest(final Message message) {
    return RaftClientRequest.newBuilder()
        .setClientId(ClientId.randomId())
        .setServerId(RaftPeerId.valueOf("s1"))
        .setGroupId(RaftGroupId.randomId())
        .setCallId(1)
        .setMessage(message)
        .setType(RaftClientRequest.writeRequestType())
        .build();
  }

  /**
   * The synchronous-throw path of {@code send()} means the entry never left this node: that failure
   * stays a plain (safe to roll back) {@link QuorumNotReachedException}, NOT an indeterminate one.
   */
  @Test
  void synchronousDispatchFailureStaysSafelyRetryable() {
    final RaftClient client = mock(RaftClient.class, RETURNS_DEEP_STUBS);
    when(client.async().send(any(Message.class)))
        .thenThrow(new IllegalStateException("SlidingWindow$Client:client-X->RAFT is closed."));

    final RaftGroupCommitter committer = new RaftGroupCommitter(client, Quorum.MAJORITY, 2_000);
    try {
      assertThatThrownBy(() -> committer.submitAndWait(new byte[] { 1, 2, 3 }))
          .isInstanceOf(QuorumNotReachedException.class)
          .isNotInstanceOf(ReplicationDispatchedTimeoutException.class)
          .hasMessageContaining("dispatch failed");
    } finally {
      committer.stop();
    }
  }

  @Test
  void isClientClosedRecognizesAlreadyClosedException() {
    assertThat(RaftGroupCommitter.isClientClosed(null)).isFalse();
    assertThat(RaftGroupCommitter.isClientClosed(new RuntimeException("boom"))).isFalse();
    assertThat(RaftGroupCommitter.isClientClosed(new AlreadyClosedException("client X is closed"))).isTrue();
    // Wrapped, like the production path: CompletionException -> AlreadyClosedException
    assertThat(RaftGroupCommitter.isClientClosed(
        new CompletionException(new AlreadyClosedException("X is already CLOSED")))).isTrue();
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

  /**
   * Issue #5503: a replica must wait for its OWN entry to be applied locally before releasing the commit
   * locks it took in phase 1, so {@code submitAndWait} has to hand the committed Raft log index back to
   * the caller. Waiting on the wrong index (or on the replica's own commit index, which still trails the
   * leader) silently skips the wait and reopens the page-corruption window.
   */
  @Test
  void submitAndWaitReturnsTheCommittedLogIndex() {
    final RaftClient client = mock(RaftClient.class, RETURNS_DEEP_STUBS);
    final RaftClientReply reply = mock(RaftClientReply.class);
    when(reply.isSuccess()).thenReturn(true);
    when(reply.getLogIndex()).thenReturn(4242L);
    when(client.async().send(any(Message.class))).thenReturn(CompletableFuture.completedFuture(reply));

    final RaftGroupCommitter committer = new RaftGroupCommitter(client, Quorum.MAJORITY, 5_000);
    try {
      assertThat(committer.submitAndWait(new byte[] { 1, 2, 3 })).isEqualTo(4242L);
    } finally {
      committer.stop();
    }
  }

  /**
   * The index is only meaningful when the entry actually committed. Every failure path throws, so a
   * caller never receives a stale or fabricated index to wait on.
   */
  @Test
  void submitAndWaitThrowsRatherThanReturningAnIndexWhenReplicationFails() {
    final RaftGroupCommitter committer = new RaftGroupCommitter(null, Quorum.MAJORITY, 500);
    try {
      assertThatThrownBy(() -> committer.submitAndWait(new byte[] { 1, 2, 3 }))
          .isInstanceOf(QuorumNotReachedException.class);
    } finally {
      committer.stop();
    }
  }

  /**
   * Regression for issue #5848: {@code flushBatch} dispatches a whole batch concurrently but then
   * awaits each reply SEQUENTIALLY with the FULL {@code quorumTimeout}, on the single flusher thread.
   * A batch where several entries never reply (a stalled quorum) therefore wedges the flusher for
   * {@code batch.size() x quorumTimeout} instead of the intended {@code quorumTimeout} batch deadline.
   * <p>
   * This test fills one batch with entries whose quorum never replies, then submits one more entry
   * right after. With the bug, the flusher is still sequentially waiting out the stalled batch when
   * that extra entry's OWN submitter-side {@code 2 x quorumTimeout} budget expires, so it gets
   * cancelled while still sitting {@code PENDING} in the queue - it never even gets dispatched. With
   * the fix, the flusher frees up within about one {@code quorumTimeout} of the stalled batch's
   * dispatch (regardless of how many entries stalled), dispatches the extra entry normally, and it
   * completes successfully. This holds regardless of how the racing stalled submissions happen to
   * split across drainTo() calls, because either way the buggy code pays a full quorumTimeout per
   * stalled entry before the flusher can loop back to the queue.
   */
  @Test
  void stalledEntriesInOneBatchDoNotWedgeTheFlusherForTheNextEntry() throws Exception {
    final long quorumTimeout = 250; // ms
    final int stallCount = 5;

    final RaftClientReply okReply = mock(RaftClientReply.class);
    when(okReply.isSuccess()).thenReturn(true);
    when(okReply.getLogIndex()).thenReturn(999L);

    final AtomicInteger sendCount = new AtomicInteger();
    final RaftClient client = mock(RaftClient.class, RETURNS_DEEP_STUBS);
    when(client.async().send(any(Message.class))).thenAnswer(inv -> {
      if (sendCount.incrementAndGet() <= stallCount)
        return new CompletableFuture<RaftClientReply>(); // never completes: simulates a stalled quorum
      return CompletableFuture.completedFuture(okReply);
    });

    final RaftGroupCommitter committer = new RaftGroupCommitter(client, Quorum.MAJORITY, quorumTimeout,
        stallCount + 5, 1_000, 100, RaftGroupCommitter.DEFAULT_MESSAGE_SIZE_MAX, null);
    final ExecutorService stallers = Executors.newFixedThreadPool(stallCount);
    try {
      // Fill (at least) one batch with entries whose quorum will never reply. Fire-and-forget: their
      // own eventual outcome (indeterminate or cancelled) is not the focus of this test.
      for (int i = 0; i < stallCount; i++)
        stallers.submit(() -> {
          try {
            committer.submitAndWait(new byte[] { 1, 2, 3 });
          } catch (final Exception ignored) {
            // expected eventually
          }
        });

      // Give the stalling entries time to enqueue. offer() is near-instant, so this margin is
      // generous; it is well under the quorumTimeout the assertion below relies on.
      Thread.sleep(150);

      final AtomicLong index = new AtomicLong(-1);
      assertThatCode(() -> index.set(committer.submitAndWait(new byte[] { 9, 9, 9 })))
          .as("the flusher must free up within roughly one quorumTimeout - not stallCount x "
              + "quorumTimeout - so this entry must be dispatched instead of cancelled while PENDING")
          .doesNotThrowAnyException();
      assertThat(index.get()).isEqualTo(999L);
    } finally {
      stallers.shutdownNow();
      committer.stop();
    }
  }

  /**
   * Regression for issue #6373, a surviving sibling of #5848: the {@code Quorum.ALL} branch of
   * {@code flushBatch} calls the timeout-less {@code raftClient.io().watch(...)} overload once per
   * committed entry, with no reference at all to the {@code deadlineNanos} budget the {@code get()}
   * loop right above it already respects (fixed for #5848 by #6109). A down/partitioned follower
   * under {@code arcadedb.ha.quorum=all} reaches MAJORITY quickly - so the {@code get()} deadline
   * gives no protection here - and then blocks the single flusher thread on the {@code ALL_COMMITTED}
   * watch for as long as the follower stays unreachable, with no bound whatsoever.
   * <p>
   * With the fix, the watch shares the same per-batch deadline: it is bounded by
   * {@code deadlineNanos - System.nanoTime()}, the same remaining budget the entry's {@code get()}
   * already used, so a stalled ALL confirmation fails fast as {@link MajorityCommittedAllFailedException}
   * instead of blocking indefinitely.
   */
  @Test
  void allQuorumWatchGivesUpAtTheBatchDeadlineInsteadOfBlockingIndefinitely() throws Exception {
    final long quorumTimeout = 200; // ms - short deliberately so the bound is easy to observe

    final RaftClientReply majorityReply = mock(RaftClientReply.class);
    when(majorityReply.isSuccess()).thenReturn(true);
    when(majorityReply.getLogIndex()).thenReturn(555L);

    final RaftClient client = mock(RaftClient.class, RETURNS_DEEP_STUBS);
    // MAJORITY is reached quickly - this is the leg #6109 already bounds, and gives no protection here.
    when(client.async().send(any(Message.class))).thenReturn(CompletableFuture.completedFuture(majorityReply));
    // A down/partitioned follower: ALL_COMMITTED is never confirmed. The fixed production code only
    // calls the async().watch() overload (the timeout-less io().watch() overload this issue was about
    // is no longer invoked at all), so an uncompleted future here is what drives the deadline-bound wait.
    when(client.async().watch(anyLong(), any())).thenReturn(new CompletableFuture<>());

    final RaftGroupCommitter committer = new RaftGroupCommitter(client, Quorum.ALL, quorumTimeout);
    try {
      final CompletableFuture<Throwable> outcome = CompletableFuture.supplyAsync(() -> {
        try {
          committer.submitAndWait(new byte[] { 1, 2, 3 });
          return null;
        } catch (final Throwable t) {
          return t;
        }
      });

      // Generous bound, well above quorumTimeout but far below what an unbounded watch() would take
      // (in production, until the follower comes back; in this test, forever). With the fix the
      // flusher gives up at roughly one quorumTimeout; without it, this get() itself times out.
      final Throwable t = outcome.get(3, TimeUnit.SECONDS);
      assertThat(t).isInstanceOf(MajorityCommittedAllFailedException.class);
      assertThat(t.getMessage()).contains("ALL quorum");
    } finally {
      committer.stop();
    }
  }

  /**
   * Companion happy-path coverage for the {@code Quorum.ALL} branch touched by the fix above: when the
   * async watch reply arrives well within the batch deadline, {@code submitAndWait} must return the
   * dispatched entry's log index rather than treating the shared-deadline bound as a source of failure
   * by itself.
   */
  @Test
  void allQuorumWatchSucceedsWithinBudgetReturnsLogIndex() throws Exception {
    final long quorumTimeout = 2_000; // ms - generous; this test exercises the success path, not the deadline

    final RaftClientReply majorityReply = mock(RaftClientReply.class);
    when(majorityReply.isSuccess()).thenReturn(true);
    when(majorityReply.getLogIndex()).thenReturn(777L);

    final RaftClientReply allReply = mock(RaftClientReply.class);
    when(allReply.isSuccess()).thenReturn(true);

    final RaftClient client = mock(RaftClient.class, RETURNS_DEEP_STUBS);
    when(client.async().send(any(Message.class))).thenReturn(CompletableFuture.completedFuture(majorityReply));
    // The follower confirms ALL_COMMITTED promptly, well within the batch deadline.
    when(client.async().watch(anyLong(), any())).thenReturn(CompletableFuture.completedFuture(allReply));

    final RaftGroupCommitter committer = new RaftGroupCommitter(client, Quorum.ALL, quorumTimeout);
    try {
      final long logIndex = committer.submitAndWait(new byte[] { 1, 2, 3 });
      assertThat(logIndex).isEqualTo(777L);
    } finally {
      committer.stop();
    }
  }
}
