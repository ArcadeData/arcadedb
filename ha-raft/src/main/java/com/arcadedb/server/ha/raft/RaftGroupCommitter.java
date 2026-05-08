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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
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

  /** Throttle for the "approaching cap" warning: at most one log line per minute. */
  private static final long WARN_THROTTLE_MS = 60_000;

  private final    RaftClient                                   raftClient;
  private final    Quorum                                       quorum;
  private final    long                                         quorumTimeout;
  private final    int                                          maxBatchSize;
  private final    int                                          offerTimeoutMs;
  private final    long                                         messageSizeMax;
  private final    Runnable                                     onClientClosed;
  private final    LinkedBlockingQueue<CancellablePendingEntry> queue;
  private final    Thread                                       flusher;
  private final    AtomicLong                                   lastApproachingCapWarnAt = new AtomicLong();
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
    this.raftClient = raftClient;
    this.quorum = quorum;
    this.quorumTimeout = quorumTimeout;
    this.maxBatchSize = maxBatchSize;
    this.offerTimeoutMs = offerTimeoutMs;
    this.messageSizeMax = messageSizeMax;
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
          "Replicated entry size %d bytes exceeds raft.grpc.message.size.max=%d bytes. "
              + "Reduce the batch size (e.g. fewer rows per GraphBatch / SQL transaction) or raise "
              + "arcadedb.ha.grpcMessageSizeMax. Bigger batches also raise leader heartbeat latency and risk "
              + "election churn under load.",
          entrySize, messageSizeMax));
    }

    // One-shot warning when we cross 80% of the cap, throttled to once per minute. The intent is
    // to help operators tune their batch size before they hit the hard cap.
    if (entrySize * 5L > messageSizeMax * 4L) {
      final long now = System.currentTimeMillis();
      final long last = lastApproachingCapWarnAt.get();
      if (now - last >= WARN_THROTTLE_MS && lastApproachingCapWarnAt.compareAndSet(last, now)) {
        LogManager.instance().log(this, Level.WARNING,
            "Replicated entry size %d bytes is approaching raft.grpc.message.size.max=%d bytes (>%d%%). "
                + "Consider reducing the batch size, or raise arcadedb.ha.grpcMessageSizeMax. "
                + "Large batches also raise leader heartbeat latency and risk election churn under load.",
            entrySize, messageSizeMax, (int) (entrySize * 100L / messageSizeMax));
      }
    }

    final long timeoutMs = 2 * quorumTimeout;
    final CancellablePendingEntry pending = new CancellablePendingEntry(entry);
    try {
      if (!queue.offer(pending, offerTimeoutMs, TimeUnit.MILLISECONDS))
        throw new ReplicationQueueFullException(
            "Replication queue is full (" + queue.remainingCapacity() + " remaining of " + (queue.size()
                + queue.remainingCapacity()) + " max). Server is overloaded, retry later");
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ReplicationQueueFullException("Interrupted while waiting for replication queue space");
    }

    try {
      final Exception error = pending.future.get(timeoutMs, TimeUnit.MILLISECONDS);
      if (error != null)
        throw error instanceof RuntimeException re ? re : new QuorumNotReachedException(error.getMessage());
    } catch (final java.util.concurrent.TimeoutException e) {
      if (pending.tryCancel())
        throw new QuorumNotReachedException("Group commit timed out after " + timeoutMs + "ms (cancelled before dispatch)");

      try {
        final Exception error = pending.future.get(quorumTimeout, TimeUnit.MILLISECONDS);
        if (error != null)
          throw error instanceof RuntimeException re ? re : new QuorumNotReachedException(error.getMessage());
      } catch (final java.util.concurrent.TimeoutException e2) {
        throw new QuorumNotReachedException(
            "Group commit timed out after " + timeoutMs + "ms + " + quorumTimeout + "ms grace (entry was dispatched to Raft)");
      } catch (final RuntimeException re) {
        throw re;
      } catch (final Exception ex) {
        throw new QuorumNotReachedException("Group commit failed during grace wait: " + ex.getMessage());
      }
    } catch (final RuntimeException e) {
      throw e;
    } catch (final Exception e) {
      throw new QuorumNotReachedException("Group commit failed: " + e.getMessage());
    }
  }

  void stop() {
    running = false;
    flusher.interrupt();
    CancellablePendingEntry pending;
    while ((pending = queue.poll()) != null)
      pending.future.complete(new QuorumNotReachedException("Group committer shutting down"));
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

        flushBatch(batch);

      } catch (final InterruptedException e) {
        if (!running)
          break;
        Thread.currentThread().interrupt();
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

    for (int i = 0; i < batch.size(); i++) {
      if (futures[i] == null)
        continue;

      try {
        final RaftClientReply reply = futures[i].get(quorumTimeout, TimeUnit.MILLISECONDS);
        if (!reply.isSuccess()) {
          final String err = reply.getException() != null ? reply.getException().getMessage() : "replication failed";
          if (isClientClosed(reply.getException()))
            clientClosedDetected = true;
          batch.get(i).future.complete(new QuorumNotReachedException("Raft replication failed: " + err));
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
      } catch (final Exception e) {
        if (isClientClosed(e))
          clientClosedDetected = true;
        final String detail = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
        batch.get(i).future.complete(new QuorumNotReachedException("Group commit entry failed: " + detail));
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
