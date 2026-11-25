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
package com.arcadedb.database;

import com.arcadedb.exception.NeedRetryException;

import java.util.List;

/**
 * A definite refusal of commits this node made locally and never published, and which it is about to discard (issue #13437).
 * <p>
 * The shape is a replicated schema session: its transactions commit on the proposer first and ship afterwards, so a block
 * running inside it has moved its transaction's commit count when the refusal arrives, and a retry loop reads that as "part of the
 * block is durable" ({@link TransactionContext#isPartiallyCommitted}) and raises a non-retryable failure instead
 * ({@link TransactionContext#notReplayable}). When nothing of the session reached any other node, that answer is wrong: the node
 * is resynced from the cluster, the resync drops those commits, and a replay of the whole block elsewhere applies nothing twice.
 * <p>
 * The exception carries the {@link Watermark} of the session: the commit count of every transaction stacked on the session's
 * thread when it opened. A block is replayable only when the commits it made all came after that watermark, so a block that
 * published something before the session opened (a {@code BATCH} boundary, a write forwarded to the leader) stays
 * non-replayable.
 * <p>
 * Never retried on this node ({@link #isRetryableOnThisNode()}): the node is quarantined until its resync. The caller retries
 * on the node that leads the cluster next.
 *
 * @author robfrank (ro.franchini@gmail.com)
 */
public class RefusedBeforePublishingException extends NeedRetryException {
  // Transient: an exception that crossed a serialization boundary is no longer on the thread whose transactions it describes,
  // and a null watermark answers "not replayable", the cautious side
  private final transient Watermark watermark;

  /**
   * @param refusal   the definite refusal of the first entry that would have carried the commits
   * @param watermark the commit counts of the session's thread when the session opened
   */
  public RefusedBeforePublishingException(final NeedRetryException refusal, final Watermark watermark) {
    super(refusal.getMessage(), refusal);
    setRetryAfterMs(refusal.getRetryAfterMs());
    this.watermark = watermark;
  }

  /**
   * Whether a block that started with {@code txAtStart} open, at commit count {@code commitCountAtStart}, made all of its
   * commits after the watermark, so none of them was published and a replay applies nothing twice.
   */
  public boolean publishedNothingSince(final TransactionContext txAtStart, final long commitCountAtStart) {
    return watermark != null && watermark.isAtOrBefore(txAtStart, commitCountAtStart);
  }

  /** The node is quarantined until its resync, so the block is replayed on the next leader, never here. */
  @Override
  public boolean isRetryableOnThisNode() {
    return false;
  }

  /**
   * The commit count of every transaction stacked on a thread, for one database, at one moment. Immutable, so the exceptions that
   * share it cannot disagree.
   */
  public static final class Watermark {
    private final Thread               thread;
    private final TransactionContext[] contexts;
    private final long[]               commitCounts;

    private Watermark(final Thread thread, final TransactionContext[] contexts, final long[] commitCounts) {
      this.thread = thread;
      this.contexts = contexts;
      this.commitCounts = commitCounts;
    }

    /** The watermark of the current thread on the database at {@code databasePath}. */
    public static Watermark ofCurrentThread(final String databasePath) {
      final DatabaseContext.DatabaseContextTL context = DatabaseContext.INSTANCE.getContextIfExists(databasePath);
      final List<TransactionContext> stacked = context != null ? context.transactions : List.of();
      final int size = stacked.size();
      final TransactionContext[] contexts = new TransactionContext[size];
      final long[] commitCounts = new long[size];
      for (int i = 0; i < size; ++i) {
        contexts[i] = stacked.get(i);
        commitCounts[i] = contexts[i].getCommitCount();
      }
      return new Watermark(Thread.currentThread(), contexts, commitCounts);
    }

    /**
     * Whether {@code commitCountAtStart} of {@code txAtStart} is at or after this watermark. A context that was not stacked when
     * the watermark was taken was created after it on the same thread, so every commit counted on it came after. Asked on another
     * thread, the answer is no: that thread's commits have nothing to do with the ones the watermark describes.
     */
    boolean isAtOrBefore(final TransactionContext txAtStart, final long commitCountAtStart) {
      if (Thread.currentThread() != thread)
        return false;
      for (int i = 0; i < contexts.length; ++i)
        if (contexts[i] == txAtStart)
          return commitCounts[i] <= commitCountAtStart;
      return true;
    }
  }
}
