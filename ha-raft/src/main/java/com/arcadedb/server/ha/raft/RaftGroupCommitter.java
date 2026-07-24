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

import com.arcadedb.log.LogManager;
import com.arcadedb.network.binary.QuorumNotReachedException;
import com.arcadedb.network.binary.ReplicationQueueFullException;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.exceptions.AlreadyClosedException;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.logging.Level;

/**
 * Groups multiple Raft entries into batched submissions to amortize gRPC round-trip cost.
 * Each transaction enqueues its entry and blocks; a background flusher collects all pending
 * entries and sends them via pipelined async calls, then notifies all waiting threads.
 */
class RaftGroupCommitter {

  /**
   * Default cap if no explicit value is provided. Matches {@code GlobalConfiguration.HA_GRPC_MESSAGE_SIZE_MAX}.
   */
  static final long DEFAULT_MESSAGE_SIZE_MAX = 128L * 1024 * 1024;

  /** Default total-bytes backpressure budget for pending entries when none is provided. */
  static final long DEFAULT_MAX_QUEUED_BYTES = 256L * 1024 * 1024;

  /** Throttle for the "approaching cap" warning: at most one log line per minute. */
  private static final long WARN_THROTTLE_MS = 60_000;

  /**
   * Test-only fault injection (issue #4790). When non-null and the predicate accepts an entry,
   * {@link #submitAndWait(byte[])} still enqueues that entry (so the flusher dispatches it to Ratis
   * for real and it commits on the followers) but then immediately throws a
   * {@link ReplicationDispatchedTimeoutException} instead of waiting for the quorum result. This
   * deterministically reproduces the "entry dispatched, quorum wait timed out" window without having
   * to stall real followers. Always {@code null} in production.
   */
  static volatile Predicate<byte[]> TEST_FORCE_DISPATCHED_TIMEOUT = null;

  private final    RaftClient                                   raftClient;
  private final    Quorum                                       quorum;
  private final    long                                         quorumTimeout;
  private final    int                                          maxBatchSize;
  private final    int                                          offerTimeoutMs;
  private final    long                                         messageSizeMax;
  private final    long                                         maxQueuedBytes;
  private final    Runnable                                     onClientClosed;
  private final    LinkedBlockingQueue<CancellablePendingEntry> queue;
  private final    Thread                                       flusher;
  private final    AtomicLong                                   lastApproachingCapWarnAt = new AtomicLong();
  // Total bytes of entries currently sitting in the queue (reserved at enqueue, released at dequeue).
  // Backs the byte-bounded backpressure so a flood of large transactions cannot exhaust the heap
  // before the entry-count bound engages.
  private final    AtomicLong                                   queuedBytes              = new AtomicLong();
  private volatile boolean                                      running                  = true;

  RaftGroupCommitter(final RaftClient raftClient, final Quorum quorum, final long quorumTimeout) {
    this(raftClient, quorum, quorumTimeout, 500, 10_000, 100, DEFAULT_MESSAGE_SIZE_MAX, null);
  }

  RaftGroupCommitter(final RaftClient raftClient, final Quorum quorum, final long quorumTimeout,
      final int maxBatchSize) {
    this(raftClient, quorum, quorumTimeout, maxBatchSize, 10_000, 100, DEFAULT_MESSAGE_SIZE_MAX, null);
  }

  RaftGroupCommitter(final RaftClient raftClient, final Quorum quorum, final long quorumTimeout,
      final int maxBatchSize, final int maxQueueSize, final int offerTimeoutMs) {
    this(raftClient, quorum, quorumTimeout, maxBatchSize, maxQueueSize, offerTimeoutMs,
        DEFAULT_MESSAGE_SIZE_MAX, null);
  }

  /**
   * @param messageSizeMax matches the Ratis {@code raft.grpc.message.size.max} cap. Entries above this
   *                       size are rejected synchronously in {@link #submitAndWait(byte[])} with a
   *                       clear, retryable exception, instead of being dispatched and rejected deep
   *                       inside the Ratis {@code SlidingWindow} client (which leaves it CLOSED for
   *                       the lifetime of the {@link RaftClient}).
   * @param onClientClosed invoked once when {@link #flushBatch} observes that the underlying Ratis
   *                       client has entered a permanent {@code CLOSED} state. The caller is expected
   *                       to rebuild the {@link RaftClient} (and this committer) so subsequent batches
   *                       use a fresh client. May be {@code null} for tests.
   */
  RaftGroupCommitter(final RaftClient raftClient, final Quorum quorum, final long quorumTimeout,
      final int maxBatchSize, final int maxQueueSize, final int offerTimeoutMs,
      final long messageSizeMax, final Runnable onClientClosed) {
    this(raftClient, quorum, quorumTimeout, maxBatchSize, maxQueueSize, offerTimeoutMs, messageSizeMax,
        DEFAULT_MAX_QUEUED_BYTES, onClientClosed);
  }

  /**
   * @param messageSizeMax matches the Ratis {@code raft.grpc.message.size.max} cap. Entries above this
   *                       size are rejected synchronously in {@link #submitAndWait(byte[])} with a
   *                       clear, retryable exception, instead of being dispatched and rejected deep
   *                       inside the Ratis {@code SlidingWindow} client (which leaves it CLOSED for
   *                       the lifetime of the {@link RaftClient}).
   * @param maxQueuedBytes total-bytes backpressure budget for pending (not-yet-dispatched) entries.
   *                       Complements {@code maxQueueSize} (an entry-count bound): since a single
   *                       entry can be up to {@code messageSizeMax}, a count-only bound would let a
   *                       flood of large transactions exhaust the heap before backpressure engages.
   *                       Clamped up to {@code messageSizeMax} so one maximum-size entry always fits.
   * @param onClientClosed invoked once when {@link #flushBatch} observes that the underlying Ratis
   *                       client has entered a permanent {@code CLOSED} state. The caller is expected
   *                       to rebuild the {@link RaftClient} (and this committer) so subsequent batches
   *                       use a fresh client. May be {@code null} for tests.
   */
  RaftGroupCommitter(final RaftClient raftClient, final Quorum quorum, final long quorumTimeout,
      final int maxBatchSize, final int maxQueueSize, final int offerTimeoutMs,
      final long messageSizeMax, final long maxQueuedBytes, final Runnable onClientClosed) {
    this.raftClient = raftClient;
    this.quorum = quorum;
    this.quorumTimeout = quorumTimeout;
    this.maxBatchSize = maxBatchSize;
    this.offerTimeoutMs = offerTimeoutMs;
    this.messageSizeMax = messageSizeMax;
    // Never set the byte budget below the single-message cap, or a legitimate maximum-size entry could
    // never be enqueued (permanent backpressure deadlock for that transaction).
    this.maxQueuedBytes = Math.max(maxQueuedBytes, messageSizeMax);
    this.onClientClosed = onClientClosed;
    this.queue = new LinkedBlockingQueue<>(maxQueueSize);
    this.flusher = new Thread(this::flushLoop, "arcadedb-raft-group-committer");
    this.flusher.setDaemon(true);
    this.flusher.start();
  }

  void submitAndWait(final byte[] entry) {
    // Pre-check entry size against the configured gRPC message cap. If the entry is bigger than
    // the cap, dispatching it would cause Ratis to reject the request inside the gRPC client and
    // leave the SlidingWindow CLOSED for the lifetime of this RaftClient (every subsequent flush
    // logs "is closed" and never lands a write). We fail fast with a clear, retryable exception so
    // the caller can split the work into smaller batches.
    final int entrySize = entry.length;
    if (entrySize > messageSizeMax) {
      throw new ReplicationQueueFullException(String.format(
          """
          Replicated entry size %d bytes exceeds raft.grpc.message.size.max=%d bytes. \
          Reduce the batch size (e.g. fewer rows per GraphBatch / SQL transaction) or raise \
          arcadedb.ha.grpcMessageSizeMax. Bigger batches also raise leader heartbeat latency and risk \
          election churn under load.""",
          entrySize, messageSizeMax));
    }

    // One-shot warning when we cross 80% of the cap, throttled to once per minute. The intent is
    // to help operators tune their batch size before they hit the hard cap.
    if (entrySize * 5L > messageSizeMax * 4L) {
      final long now = System.currentTimeMillis();
      final long last = lastApproachingCapWarnAt.get();
      if (now - last >= WARN_THROTTLE_MS && lastApproachingCapWarnAt.compareAndSet(last, now)) {
        LogManager.instance().log(this, Level.WARNING,
            """
            Replicated entry size %d bytes is approaching raft.grpc.message.size.max=%d bytes (>%d%%). \
            Consider reducing the batch size, or raise arcadedb.ha.grpcMessageSizeMax. \
            Large batches also raise leader heartbeat latency and risk election churn under load.""",
            entrySize, messageSizeMax, (int) (entrySize * 100L / messageSizeMax));
      }
    }

    // Byte-bounded backpressure: reserve this entry's bytes against the queued-bytes budget BEFORE
    // enqueuing. Because a single entry can be up to messageSizeMax, the entry-count bound alone can
    // let a flood of large transactions exhaust the heap; this reservation makes the leader throw a
    // retryable ReplicationQueueFullException instead of running out of memory under heavy ingest.
    if (!reserveQueuedBytes(entrySize))
      throw new ReplicationQueueFullException(
          "Replication queue byte budget exhausted (" + queuedBytes.get() + " of " + maxQueuedBytes
              + " bytes pending, entry=" + entrySize + " bytes). Leader is overloaded, retry later");

    final long timeoutMs = 2 * quorumTimeout;
    final CancellablePendingEntry pending = new CancellablePendingEntry(entry);
    try {
      if (!queue.offer(pending, offerTimeoutMs, TimeUnit.MILLISECONDS)) {
        queuedBytes.addAndGet(-entrySize); // release the reservation: the entry never entered the queue
        throw new ReplicationQueueFullException(
            "Replication queue is full (" + queue.remainingCapacity() + " remaining of " + (queue.size()
                + queue.remainingCapacity()) + " max). Server is overloaded, retry later");
      }
    } catch (final InterruptedException e) {
      queuedBytes.addAndGet(-entrySize); // release the reservation on interrupt
      Thread.currentThread().interrupt();
      throw new ReplicationQueueFullException("Interrupted while waiting for replication queue space");
    }

    // Test-only: simulate "entry dispatched, then quorum wait timed out" deterministically. The
    // entry stays on the queue (PENDING), so the flusher still dispatches and commits it for real;
    // we just abandon the wait here with the same exception the production grace-expiry path uses.
    final Predicate<byte[]> forceTimeout = TEST_FORCE_DISPATCHED_TIMEOUT;
    if (forceTimeout != null && forceTimeout.test(entry))
      throw new ReplicationDispatchedTimeoutException(
          "TEST: forced dispatched-timeout simulation (entry was dispatched to Raft)");

    try {
      final Exception error = pending.future.get(timeoutMs, TimeUnit.MILLISECONDS);
      if (error != null)
        throw error instanceof RuntimeException re ? re : dispatchAware(pending, error.getMessage());
    } catch (final TimeoutException e) {
      if (pending.tryCancel())
        throw new QuorumNotReachedException("Group commit timed out after " + timeoutMs + "ms (cancelled before dispatch)");

      try {
        final Exception error = pending.future.get(quorumTimeout, TimeUnit.MILLISECONDS);
        if (error != null)
          throw error instanceof RuntimeException re ? re : dispatchAware(pending, error.getMessage());
      } catch (final TimeoutException e2) {
        // The entry was dispatched to Ratis and may still reach quorum and commit on the followers
        // (and on this leader's state machine with the origin-skip). The outcome is INDETERMINATE,
        // not a clean failure: surface a dedicated exception so the caller reconciles instead of
        // rolling back and silently dropping the write on the leader (issue #4790).
        throw new ReplicationDispatchedTimeoutException(
            "Group commit timed out after " + timeoutMs + "ms + " + quorumTimeout + "ms grace (entry was dispatched to Raft)");
      } catch (final RuntimeException re) {
        throw re;
      } catch (final Exception ex) {
        throw dispatchAware(pending, "Group commit failed during grace wait: " + ex.getMessage());
      }
    } catch (final RuntimeException e) {
      throw e;
    } catch (final Exception e) {
      throw dispatchAware(pending, "Group commit failed: " + e.getMessage());
    }
  }

  /**
   * Builds the right exception for a failed wait depending on whether the entry was already
   * dispatched to Ratis. A {@code DISPATCHED} entry may still commit on the followers (and on this
   * leader's state machine), so its outcome is indeterminate and must be surfaced as a
   * {@link ReplicationDispatchedTimeoutException} so the caller reconciles instead of rolling back
   * and skipping its local apply (issue #4790). A {@code PENDING}/{@code CANCELLED} entry never left
   * this node, so a plain {@link QuorumNotReachedException} (safe to roll back) is correct.
   */
  private static QuorumNotReachedException dispatchAware(final CancellablePendingEntry pending, final String message) {
    if (pending.state.get() == CancellablePendingEntry.DISPATCHED)
      return new ReplicationDispatchedTimeoutException(message + " (entry was dispatched to Raft)");
    return new QuorumNotReachedException(message);
  }

  /**
   * Reserves {@code entrySize} bytes against the queued-bytes budget, waiting up to
   * {@code offerTimeoutMs} for the flusher to drain and free space. Returns {@code false} if the
   * budget stays exhausted for the whole window (the caller then surfaces a retryable backpressure
   * error). The reservation is released by {@link #releaseQueuedBytes} once the entry is dequeued,
   * or directly by the caller if the subsequent {@code queue.offer} fails.
   */
  boolean reserveQueuedBytes(final int entrySize) {
    final long deadline = System.currentTimeMillis() + offerTimeoutMs;
    while (true) {
      final long current = queuedBytes.get();
      if (current + entrySize <= maxQueuedBytes) {
        if (queuedBytes.compareAndSet(current, current + entrySize))
          return true;
        continue; // lost the CAS race; re-read and retry without backing off
      }
      if (System.currentTimeMillis() >= deadline)
        return false;
      try {
        Thread.sleep(Math.min(5L, Math.max(1L, offerTimeoutMs / 10L)));
      } catch (final InterruptedException ie) {
        Thread.currentThread().interrupt();
        return false;
      }
    }
  }

  /** Bytes of entries currently reserved in the queue (for backpressure metrics and tests). */
  long getQueuedBytes() {
    return queuedBytes.get();
  }

  /** Releases the byte reservations of entries that have just left the queue. */
  private void releaseQueuedBytes(final List<CancellablePendingEntry> entries) {
    long total = 0;
    for (final CancellablePendingEntry e : entries)
      total += e.entry.length;
    if (total > 0)
      queuedBytes.addAndGet(-total);
  }

  void stop() {
    running = false;
    flusher.interrupt();
    CancellablePendingEntry pending;
    while ((pending = queue.poll()) != null) {
      queuedBytes.addAndGet(-pending.entry.length); // released: entry left the queue
      pending.future.complete(new QuorumNotReachedException("Group committer shutting down"));
    }
  }

  /**
   * Stops the flusher and transfers undispatched (still {@code PENDING}) entries from this
   * committer's queue into {@code target}'s queue, preserving their original futures so the
   * original {@link #submitAndWait} caller blocks until the entry is replicated through the
   * fresh client. Used during {@code RaftHAServer.refreshRaftClient} so a brief leader hiccup
   * does not surface as "Group committer shutting down" errors to in-flight callers.
   * <p>
   * Entries that have already been dispatched to the old {@link RaftClient} stay attached to
   * that client and are completed with the usual error path; we cannot safely re-send them on
   * the new client because Ratis may have committed them on the old one.
   *
   * @return number of entries transferred
   */
  int transferPendingTo(final RaftGroupCommitter target) {
    running = false;
    flusher.interrupt();
    try {
      flusher.join(1_000);
    } catch (final InterruptedException ie) {
      Thread.currentThread().interrupt();
    }

    int transferred = 0;
    int dropped = 0;
    CancellablePendingEntry pending;
    while ((pending = queue.poll()) != null) {
      // Entry left this committer's queue: release its byte reservation here. Transferred entries are
      // re-reserved on the target below so the byte budget stays accurate across the handoff.
      queuedBytes.addAndGet(-pending.entry.length);
      // Only PENDING entries can be safely re-sent. DISPATCHED ones are already tied to the
      // old RaftClient's SlidingWindow; CANCELLED ones already had their futures completed.
      if (pending.state.get() != CancellablePendingEntry.PENDING) {
        // Already dispatched or cancelled: complete with the standard shutdown error so the
        // caller sees a NeedRetryException and retries against the new broker on its own.
        if (!pending.future.isDone())
          pending.future.complete(new QuorumNotReachedException("Group committer shutting down"));
        dropped++;
        continue;
      }
      // Re-queue on the new committer's queue. offer() may fail if the new queue is already
      // full; in that case we surface a retryable error rather than blocking the refresh path.
      if (!target.queue.offer(pending)) {
        pending.future.complete(new ReplicationQueueFullException(
            "Replication queue full after RaftClient refresh; retry"));
        dropped++;
        continue;
      }
      target.queuedBytes.addAndGet(pending.entry.length); // account the transferred entry on the target
      transferred++;
    }
    if (transferred > 0 || dropped > 0)
      LogManager.instance().log(this, Level.INFO,
          "Transferred %d pending entries to fresh committer (%d dropped)", transferred, dropped);
    return transferred;
  }

  private void flushLoop() {
    final List<CancellablePendingEntry> batch = new ArrayList<>(maxBatchSize);

    while (running) {
      try {
        final CancellablePendingEntry first = queue.poll(100, TimeUnit.MILLISECONDS);
        if (first == null)
          continue;

        batch.clear();
        batch.add(first);
        queue.drainTo(batch, maxBatchSize - 1);

        // Entries have left the queue: release their byte reservations now (they are about to be
        // dispatched and held by Ratis, no longer pending in our backpressure budget).
        releaseQueuedBytes(batch);

        flushBatch(batch);

      } catch (final InterruptedException e) {
        if (!running)
          break;
        // Interrupted while still running (e.g. the flag restored by flushBatch's interrupt
        // handling made poll() throw): swallow it and keep flushing. The interrupt protocol for
        // this thread is stop()/transferPendingTo(), which set running=false BEFORE interrupting;
        // re-asserting the flag here would make the next poll() throw immediately and busy-spin.
      } catch (final Exception e) {
        LogManager.instance().log(this, Level.WARNING, "Error in group commit flusher: %s", e.getMessage());
        for (final CancellablePendingEntry p : batch)
          p.future.complete(e);
        batch.clear();
      }
    }
  }

  @SuppressWarnings("unchecked")
  private void flushBatch(final List<CancellablePendingEntry> batch) {
    if (raftClient == null) {
      final Exception err = new QuorumNotReachedException("RaftClient not available");
      for (final CancellablePendingEntry p : batch)
        p.future.complete(err);
      return;
    }

    boolean clientClosedDetected = false;

    final CompletableFuture<RaftClientReply>[] futures = new CompletableFuture[batch.size()];
    for (int i = 0; i < batch.size(); i++) {
      final CancellablePendingEntry p = batch.get(i);
      if (!p.tryDispatch()) {
        p.future.complete(new QuorumNotReachedException("cancelled before dispatch"));
        futures[i] = null;
        continue;
      }
      final Message msg = Message.valueOf(ByteString.copyFrom(p.entry));
      try {
        futures[i] = raftClient.async().send(msg);
      } catch (final Throwable t) {
        // Synchronous failure (e.g. SlidingWindow already CLOSED). Surface to the caller and
        // remember to ask the host to refresh the client.
        if (isClientClosed(t))
          clientClosedDetected = true;
        p.future.complete(new QuorumNotReachedException(
            "Group commit dispatch failed: " + (t.getMessage() != null ? t.getMessage() : t.getClass().getSimpleName())));
        futures[i] = null;
      }
    }

    // From here every remaining entry has been DISPATCHED: the request entered the Ratis client and
    // may have reached the leader's Raft log even when the wait below fails (lost reply, interrupt,
    // timeout, negative reply after internal Ratis retries). Such an entry can still commit on the
    // followers AND on this node's state machine, so its failure must surface as a
    // ReplicationDispatchedTimeoutException (indeterminate outcome, issue #4790): the originator then
    // marks the transaction for local apply instead of rolling back and origin-skipping a write the
    // rest of the cluster keeps - the silent leader divergence behind issue #4743's bulk-load churn.
    for (int i = 0; i < batch.size(); i++) {
      if (futures[i] == null)
        continue;

      try {
        final RaftClientReply reply = futures[i].get(quorumTimeout, TimeUnit.MILLISECONDS);
        if (!reply.isSuccess()) {
          final String err = reply.getException() != null ? reply.getException().getMessage() : "replication failed";
          if (isClientClosed(reply.getException()))
            clientClosedDetected = true;
          batch.get(i).future.complete(new ReplicationDispatchedTimeoutException(
              "Raft replication failed: " + err + " (entry was dispatched to Raft; outcome unknown)"));
          continue;
        }

        if (quorum == Quorum.ALL) {
          try {
            final RaftClientReply watchReply = raftClient.io().watch(
                reply.getLogIndex(), RaftProtos.ReplicationLevel.ALL_COMMITTED);
            if (!watchReply.isSuccess()) {
              batch.get(i).future.complete(new MajorityCommittedAllFailedException(
                  "ALL quorum not reached after MAJORITY commit at logIndex=" + reply.getLogIndex()));
              continue;
            }
          } catch (final Exception e) {
            if (isClientClosed(e))
              clientClosedDetected = true;
            batch.get(i).future.complete(new MajorityCommittedAllFailedException(
                "ALL quorum watch failed after MAJORITY commit: " + e.getMessage(), e));
            continue;
          }
        }

        batch.get(i).future.complete(null); // success - after ALL check
      } catch (final InterruptedException ie) {
        // The flusher was interrupted (client refresh after leader churn, or shutdown) while
        // awaiting quorum results. This entry and every remaining one were already dispatched, so
        // complete them all as indeterminate and stop waiting: lingering here for quorumTimeout per
        // entry would stall the refresh path that interrupted us.
        Thread.currentThread().interrupt();
        for (int j = i; j < batch.size(); j++)
          if (futures[j] != null && !batch.get(j).future.isDone())
            batch.get(j).future.complete(new ReplicationDispatchedTimeoutException(
                "Group commit interrupted while awaiting quorum result (entry was dispatched to Raft; outcome unknown)"));
        break;
      } catch (final Exception e) {
        if (isClientClosed(e))
          clientClosedDetected = true;
        final String detail = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
        batch.get(i).future.complete(new ReplicationDispatchedTimeoutException(
            "Group commit entry failed: " + detail + " (entry was dispatched to Raft; outcome unknown)"));
      }
    }

    HALog.log(this, HALog.DETAILED, "Group commit flushed %d entries in one batch", batch.size());

    // The Ratis SlidingWindow client never recovers from AlreadyClosedException; any future send on
    // it fails the same way. Hand off to the host so it can rebuild the RaftClient (see
    // RaftHAServer.refreshRaftClient). We invoke off the flush loop to avoid deadlock with the
    // synchronized refresh path.
    if (clientClosedDetected && onClientClosed != null) {
      LogManager.instance().log(this, Level.WARNING,
          "Detected permanently CLOSED Raft client during group commit; requesting client refresh");
      final Runnable cb = onClientClosed;
      Thread.ofVirtual().name("arcadedb-raft-client-refresh").start(() -> {
        try {
          cb.run();
        } catch (final Throwable t) {
          LogManager.instance().log(this, Level.WARNING, "RaftClient refresh callback failed: %s", t.getMessage());
        }
      });
    }
  }

  /**
   * Recognizes the Ratis "client/SlidingWindow is CLOSED" failure mode by walking the cause chain.
   * Once it triggers, the only way to make progress is to rebuild the {@link RaftClient}.
   * Package-private for unit testing.
   */
  static boolean isClientClosed(final Throwable t) {
    if (t == null)
      return false;
    Throwable cur = t;
    for (int depth = 0; depth < 8 && cur != null; depth++) {
      if (cur instanceof AlreadyClosedException)
        return true;
      final String msg = cur.getMessage();
      if (msg != null && (msg.contains("is closed") || msg.contains("is already CLOSED")))
        return true;
      cur = cur.getCause();
    }
    return false;
  }

  private static class CancellablePendingEntry {
    static final int PENDING    = 0;
    static final int DISPATCHED = 1;
    static final int CANCELLED  = 2;

    final byte[]                       entry;
    final CompletableFuture<Exception> future = new CompletableFuture<>();
    final AtomicInteger                state  = new AtomicInteger(PENDING);

    CancellablePendingEntry(final byte[] entry) {
      this.entry = entry;
    }

    boolean tryDispatch() {
      return state.compareAndSet(PENDING, DISPATCHED);
    }

    boolean tryCancel() {
      return state.compareAndSet(PENDING, CANCELLED);
    }

  }
}
