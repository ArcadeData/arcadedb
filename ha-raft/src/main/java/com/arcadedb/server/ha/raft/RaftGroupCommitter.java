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
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

/**
 * Groups multiple Raft entries into batched submissions to amortize gRPC round-trip cost.
 * Each transaction enqueues its entry and blocks; a background flusher collects all pending
 * entries and sends them via pipelined async calls, then notifies all waiting threads.
 */
class RaftGroupCommitter {

  private final    RaftClient                                   raftClient;
  private final    Quorum                                       quorum;
  private final    long                                         quorumTimeout;
  private final    int                                          maxBatchSize;
  private final    int                                          offerTimeoutMs;
  private final    LinkedBlockingQueue<CancellablePendingEntry> queue;
  private final    Thread                                       flusher;
  private volatile boolean                                      running = true;

  RaftGroupCommitter(final RaftClient raftClient, final Quorum quorum, final long quorumTimeout) {
    this(raftClient, quorum, quorumTimeout, 500, 10_000, 100);
  }

  RaftGroupCommitter(final RaftClient raftClient, final Quorum quorum, final long quorumTimeout,
      final int maxBatchSize) {
    this(raftClient, quorum, quorumTimeout, maxBatchSize, 10_000, 100);
  }

  RaftGroupCommitter(final RaftClient raftClient, final Quorum quorum, final long quorumTimeout,
      final int maxBatchSize, final int maxQueueSize, final int offerTimeoutMs) {
    this.raftClient = raftClient;
    this.quorum = quorum;
    this.quorumTimeout = quorumTimeout;
    this.maxBatchSize = maxBatchSize;
    this.offerTimeoutMs = offerTimeoutMs;
    this.queue = new LinkedBlockingQueue<>(maxQueueSize);
    this.flusher = new Thread(this::flushLoop, "arcadedb-raft-group-committer");
    this.flusher.setDaemon(true);
    this.flusher.start();
  }

  void submitAndWait(final byte[] entry) {
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

    final CompletableFuture<RaftClientReply>[] futures = new CompletableFuture[batch.size()];
    for (int i = 0; i < batch.size(); i++) {
      final CancellablePendingEntry p = batch.get(i);
      if (!p.tryDispatch()) {
        p.future.complete(new QuorumNotReachedException("cancelled before dispatch"));
        futures[i] = null;
        continue;
      }
      final Message msg = Message.valueOf(ByteString.copyFrom(p.entry));
      futures[i] = raftClient.async().send(msg);
    }

    for (int i = 0; i < batch.size(); i++) {
      if (futures[i] == null)
        continue;

      try {
        final RaftClientReply reply = futures[i].get(quorumTimeout, TimeUnit.MILLISECONDS);
        if (!reply.isSuccess()) {
          final String err = reply.getException() != null ? reply.getException().getMessage() : "replication failed";
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
            batch.get(i).future.complete(new MajorityCommittedAllFailedException(
                "ALL quorum watch failed after MAJORITY commit: " + e.getMessage(), e));
            continue;
          }
        }

        batch.get(i).future.complete(null); // success - after ALL check
      } catch (final Exception e) {
        final String detail = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
        batch.get(i).future.complete(new QuorumNotReachedException("Group commit entry failed: " + detail));
      }
    }

    HALog.log(this, HALog.DETAILED, "Group commit flushed %d entries in one batch", batch.size());
  }

  private static class CancellablePendingEntry {
    static final int PENDING    = 0;
    static final int DISPATCHED = 1;
    static final int CANCELLED  = 2;

    final byte[]                       entry;
    final CompletableFuture<Exception> future = new CompletableFuture<>();
    private final AtomicInteger        state  = new AtomicInteger(PENDING);

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
