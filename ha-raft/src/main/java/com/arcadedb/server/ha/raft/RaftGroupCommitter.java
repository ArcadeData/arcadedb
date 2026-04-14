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

import com.arcadedb.log.LogManager;
import com.arcadedb.network.binary.QuorumNotReachedException;
import com.arcadedb.network.binary.ReplicationQueueFullException;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.proto.RaftProtos;
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
 * <p>
 * Instead of each transaction blocking individually on a Raft round-trip (~15ms), transactions
 * enqueue their entries and wait. A background flusher collects all pending entries and sends
 * them via pipelined async calls, then notifies all waiting threads at once.
 * <p>
 * This achieves "group commit" - a single gRPC round-trip commits multiple transactions,
 * dramatically improving throughput under concurrent load.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class RaftGroupCommitter {

  private final int                                    maxBatchSize;
  private final RaftHAServer                           haServer;
  private final LinkedBlockingQueue<PendingEntry>      queue;
  private final Thread                                 flusher;
  private volatile boolean                             running    = true;

  public RaftGroupCommitter(final RaftHAServer haServer, final int maxBatchSize, final int maxQueueSize) {
    this.haServer = haServer;
    this.maxBatchSize = maxBatchSize;
    this.queue = new LinkedBlockingQueue<>(maxQueueSize);
    this.flusher = new Thread(this::flushLoop, "arcadedb-raft-group-committer");
    this.flusher.setDaemon(true);
  }

  /** Starts the background flusher thread. Call after server startup is complete. */
  public void start() {
    this.flusher.start();
  }

  /**
   * Enqueues a Raft entry and blocks until it is committed by the cluster.
   * Multiple concurrent callers will have their entries batched into fewer Raft round-trips.
   * <p>
   * The overall deadline is {@code timeoutMs + quorumTimeout}. The first timeout covers queue
   * waiting and batch dispatch; the second covers the Raft round-trip if the entry was dispatched
   * just before the first timeout expired. In practice, all current callers pass
   * {@code timeoutMs == quorumTimeout}, so the effective upper bound is {@code 2 * quorumTimeout}.
   *
   * @param timeoutMs how long to wait for the entry to be dispatched and committed
   */
  public void submitAndWait(final byte[] entry, final long timeoutMs) {
    final PendingEntry pending = new PendingEntry(entry);
    if (!queue.offer(pending))
      throw new ReplicationQueueFullException(
          "Replication queue is full (" + queue.remainingCapacity() + " remaining of " + (queue.size() + queue.remainingCapacity()) + " max). Server is overloaded, retry later");

    // Two-phase deadline: timeoutMs for queue+dispatch, then quorumTimeout for Raft round-trip
    // if the entry was dispatched just before the first timeout fired.
    final long deadline = System.currentTimeMillis() + timeoutMs + haServer.getQuorumTimeout();
    try {
      final Exception error = pending.future.get(timeoutMs, TimeUnit.MILLISECONDS);
      if (error != null)
        throw error instanceof RuntimeException re ? re : new QuorumNotReachedException(error.getMessage());
    } catch (final java.util.concurrent.TimeoutException e) {
      // Atomically try to cancel. If CAS fails, the flusher already moved the entry to
      // DISPATCHED, so cancellation is impossible and we must wait for the Raft result.
      if (!pending.state.compareAndSet(STATE_PENDING, STATE_CANCELLED)) {
        // Entry was already sent to Raft. We MUST wait for the result to prevent phantom
        // commits (replicated on followers but commit2ndPhase never called on the leader).
        // Use remaining time from the overall deadline (2x quorumTimeout) instead of a fresh timeout.
        final long remaining = Math.max(1, deadline - System.currentTimeMillis());
        HALog.log(this, HALog.BASIC,
            "Group commit entry already dispatched to Raft, waiting %dms for result (initial timeout %dms expired)",
            remaining, timeoutMs);
        try {
          final Exception error = pending.future.get(remaining, TimeUnit.MILLISECONDS);
          if (error != null)
            throw error instanceof RuntimeException re ? re : new QuorumNotReachedException(error.getMessage());
          return; // Raft succeeded after extended wait
        } catch (final InterruptedException e2) {
          Thread.currentThread().interrupt();
          throw new QuorumNotReachedException("Group commit interrupted after extended wait (dispatched to Raft but no reply)");
        } catch (final java.util.concurrent.ExecutionException | java.util.concurrent.TimeoutException e2) {
          throw new QuorumNotReachedException("Group commit timed out after extended wait (dispatched to Raft but no reply)");
        }
      }
      // Successfully cancelled before dispatch
      HALog.log(this, HALog.BASIC, "Group commit entry cancelled after timeout (%dms), not yet dispatched to Raft", timeoutMs);
      throw new QuorumNotReachedException("Group commit timed out after " + timeoutMs + "ms");
    } catch (final RuntimeException e) {
      throw e;
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new QuorumNotReachedException("Group commit interrupted", e);
    } catch (final Exception e) {
      throw new QuorumNotReachedException("Group commit failed: " + e, e);
    }
  }

  public void stop() {
    running = false;
    flusher.interrupt();
    // Wait for the flusher to finish any in-flight batch so dispatched entries are completed
    // before stopService() closes the Raft client/server.
    try {
      flusher.join(haServer.getQuorumTimeout());
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    // Fail any entries still in the queue (not yet dispatched)
    PendingEntry pending;
    while ((pending = queue.poll()) != null)
      pending.future.complete(new QuorumNotReachedException("Group committer shutting down"));
  }

  private void flushLoop() {
    final List<PendingEntry> batch = new ArrayList<>(maxBatchSize);

    while (running) {
      try {
        // Block until the first entry arrives. Using take() instead of poll(100ms)
        // eliminates up to 100ms latency for OLTP workloads with infrequent writes.
        // The flusher wakes instantly when submitAndWait() enqueues an entry.
        // The loop's running check is handled by interrupt() in stop().
        final PendingEntry first = queue.take();

        batch.clear();
        batch.add(first);

        // Drain all entries already in the queue (non-blocking).
        // Under concurrent load, multiple entries accumulate while we process.
        // Under single-thread load, the queue is empty and we flush immediately (zero overhead).
        queue.drainTo(batch, maxBatchSize - 1);

        // Send all entries via pipelined async calls
        flushBatch(batch);

      } catch (final InterruptedException e) {
        if (!running)
          break;
        Thread.currentThread().interrupt();
      } catch (final Exception e) {
        LogManager.instance().log(this, Level.WARNING, "Error in group commit flusher", e);
        // Fail all entries in the current batch
        for (final PendingEntry p : batch)
          p.future.complete(e);
        batch.clear();
      }
    }
  }

  private void flushBatch(final List<PendingEntry> batch) {
    final RaftClient client = haServer.getRaftClient();
    if (client == null) {
      final Exception err = new QuorumNotReachedException("RaftClient not available");
      for (final PendingEntry p : batch)
        p.future.complete(err);
      return;
    }

    // Atomically transition each entry from PENDING to DISPATCHED. Entries that were already
    // CANCELLED by a timed-out caller will fail the CAS and are removed. This single CAS
    // replaces the old two-step (removeIf cancelled + set dispatched) that had a race window.
    batch.removeIf(p -> !p.state.compareAndSet(STATE_PENDING, STATE_DISPATCHED));
    if (batch.isEmpty())
      return;

    // Send all entries asynchronously (pipelined)
    final List<CompletableFuture<RaftClientReply>> futures = new ArrayList<>(batch.size());
    for (int i = 0; i < batch.size(); i++) {
      final Message msg = Message.valueOf(ByteString.copyFrom(batch.get(i).entry));
      futures.add(client.async().send(msg));
    }

    // Collect send results. For ALL quorum, issue watch futures in parallel during this pass.
    final boolean allQuorum = haServer.getQuorum() == Quorum.ALL;
    final List<CompletableFuture<RaftClientReply>> watchFutures = allQuorum ? new ArrayList<>(batch.size()) : null;

    for (int i = 0; i < batch.size(); i++) {
      try {
        final RaftClientReply reply = futures.get(i).get(haServer.getQuorumTimeout(), TimeUnit.MILLISECONDS);
        if (!reply.isSuccess()) {
          final String err = reply.getException() != null ? reply.getException().getMessage() : "replication failed";
          batch.get(i).future.complete(new QuorumNotReachedException("Raft replication failed: " + err));
          if (allQuorum)
            watchFutures.add(null);
          continue;
        }

        if (allQuorum)
          watchFutures.add(client.async().watch(reply.getLogIndex(), RaftProtos.ReplicationLevel.ALL_COMMITTED));
        else
          batch.get(i).future.complete(null); // success for MAJORITY

      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        // Fail this and all remaining entries, then return so flushLoop() can see the interrupt
        for (int j = i; j < batch.size(); j++)
          batch.get(j).future.complete(new QuorumNotReachedException("Group commit interrupted during batch flush"));
        return;
      } catch (final Exception e) {
        batch.get(i).future.complete(new QuorumNotReachedException("Group commit entry failed: " + e, e));
        if (allQuorum)
          watchFutures.add(null);
      }
    }

    // For ALL quorum: all watch RPCs are already in flight, now collect results
    if (allQuorum) {
      for (int i = 0; i < batch.size(); i++) {
        if (batch.get(i).future.isDone())
          continue; // already failed in send phase

        final CompletableFuture<RaftClientReply> watchFuture = watchFutures.get(i);
        if (watchFuture == null)
          continue; // send failed, already completed with error

        try {
          final RaftClientReply watchReply = watchFuture.get(haServer.getQuorumTimeout(), TimeUnit.MILLISECONDS);
          // MAJORITY already committed (applyTransaction fired on the leader with origin-skip).
          // Use MajorityCommittedAllFailedException so ReplicatedDatabase.commit() knows to
          // call commit2ndPhase() rather than roll back - otherwise the leader's database
          // permanently misses this transaction while lastAppliedIndex already reflects it.
          batch.get(i).future.complete(watchReply.isSuccess() ? null : new MajorityCommittedAllFailedException("ALL quorum not reached"));
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          for (int j = i; j < batch.size(); j++)
            if (!batch.get(j).future.isDone())
              batch.get(j).future.complete(new MajorityCommittedAllFailedException("ALL quorum watch interrupted"));
          return;
        } catch (final Exception e) {
          batch.get(i).future.complete(new MajorityCommittedAllFailedException("ALL quorum watch failed: " + e, e));
        }
      }
    }

    HALog.log(this, HALog.DETAILED, "Group commit flushed %d entries in one batch", batch.size());
  }

  // Atomic state constants for PendingEntry. A single AtomicInteger prevents the race
  // between cancel and dispatch that existed with two separate AtomicBooleans: the flusher's
  // removeIf(cancelled) and set(dispatched=true) were not atomic, so a timeout firing between
  // them could cancel an entry that had already passed the cancel check.
  static final int STATE_PENDING    = 0;
  static final int STATE_DISPATCHED = 1;
  static final int STATE_CANCELLED  = 2;

  static class PendingEntry {
    final byte[]                       entry;
    final CompletableFuture<Exception> future = new CompletableFuture<>();
    final AtomicInteger                state  = new AtomicInteger(STATE_PENDING);

    PendingEntry(final byte[] entry) {
      this.entry = entry;
    }
  }
}
