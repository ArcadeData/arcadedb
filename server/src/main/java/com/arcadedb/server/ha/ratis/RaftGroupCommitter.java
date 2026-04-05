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

import com.arcadedb.log.LogManager;
import com.arcadedb.network.binary.QuorumNotReachedException;
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

  private static final int MAX_BATCH_SIZE     = 500;

  private final RaftHAServer                           haServer;
  private final LinkedBlockingQueue<PendingEntry>      queue      = new LinkedBlockingQueue<>();
  private final Thread                                 flusher;
  private volatile boolean                             running    = true;

  public RaftGroupCommitter(final RaftHAServer haServer) {
    this.haServer = haServer;
    this.flusher = new Thread(this::flushLoop, "arcadedb-raft-group-committer");
    this.flusher.setDaemon(true);
    this.flusher.start();
  }

  /**
   * Enqueues a Raft entry and blocks until it is committed by the cluster.
   * Multiple concurrent callers will have their entries batched into fewer Raft round-trips.
   */
  public void submitAndWait(final byte[] entry, final long timeoutMs) {
    final PendingEntry pending = new PendingEntry(entry);
    queue.add(pending);

    try {
      final Exception error = pending.future.get(timeoutMs, TimeUnit.MILLISECONDS);
      if (error != null)
        throw error instanceof RuntimeException re ? re : new QuorumNotReachedException(error.getMessage());
    } catch (final java.util.concurrent.TimeoutException e) {
      throw new QuorumNotReachedException("Group commit timed out after " + timeoutMs + "ms");
    } catch (final RuntimeException e) {
      throw e;
    } catch (final Exception e) {
      throw new QuorumNotReachedException("Group commit failed: " + e.getMessage());
    }
  }

  public void stop() {
    running = false;
    flusher.interrupt();
    // Fail any remaining entries
    PendingEntry pending;
    while ((pending = queue.poll()) != null)
      pending.future.complete(new QuorumNotReachedException("Group committer shutting down"));
  }

  private void flushLoop() {
    final List<PendingEntry> batch = new ArrayList<>(MAX_BATCH_SIZE);

    while (running) {
      try {
        // Wait for the first entry (blocks until one arrives)
        final PendingEntry first = queue.poll(100, TimeUnit.MILLISECONDS);
        if (first == null)
          continue;

        batch.clear();
        batch.add(first);

        // Drain all entries already in the queue (non-blocking).
        // Under concurrent load, multiple entries accumulate while we process.
        // Under single-thread load, the queue is empty and we flush immediately (zero overhead).
        queue.drainTo(batch, MAX_BATCH_SIZE - 1);

        // Send all entries via pipelined async calls
        flushBatch(batch);

      } catch (final InterruptedException e) {
        if (!running)
          break;
        Thread.currentThread().interrupt();
      } catch (final Exception e) {
        LogManager.instance().log(this, Level.WARNING, "Error in group commit flusher: %s", e.getMessage());
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

    // Send all entries asynchronously (pipelined)
    final CompletableFuture<RaftClientReply>[] futures = new CompletableFuture[batch.size()];
    for (int i = 0; i < batch.size(); i++) {
      final Message msg = Message.valueOf(ByteString.copyFrom(batch.get(i).entry));
      futures[i] = client.async().send(msg);
    }

    // Wait for all replies
    for (int i = 0; i < batch.size(); i++) {
      try {
        final RaftClientReply reply = futures[i].get(haServer.getQuorumTimeout(), TimeUnit.MILLISECONDS);
        if (reply.isSuccess())
          batch.get(i).future.complete(null); // success
        else {
          final String err = reply.getException() != null ? reply.getException().getMessage() : "replication failed";
          batch.get(i).future.complete(new QuorumNotReachedException("Raft replication failed: " + err));
        }

        // Handle ALL quorum if needed
        if (reply.isSuccess() && haServer.getQuorum() == RaftHAServer.Quorum.ALL) {
          try {
            final RaftClientReply watchReply = client.io().watch(reply.getLogIndex(), RaftProtos.ReplicationLevel.ALL_COMMITTED);
            if (!watchReply.isSuccess())
              batch.get(i).future.complete(new QuorumNotReachedException("ALL quorum not reached"));
          } catch (final Exception e) {
            batch.get(i).future.complete(new QuorumNotReachedException("ALL quorum watch failed: " + e.getMessage()));
          }
        }

      } catch (final Exception e) {
        batch.get(i).future.complete(new QuorumNotReachedException("Group commit entry failed: " + e.getMessage()));
      }
    }

    HALog.log(this, HALog.DETAILED, "Group commit flushed %d entries in one batch", batch.size());
  }

  private static class PendingEntry {
    final byte[]                       entry;
    final CompletableFuture<Exception> future = new CompletableFuture<>();

    PendingEntry(final byte[] entry) {
      this.entry = entry;
    }
  }
}
