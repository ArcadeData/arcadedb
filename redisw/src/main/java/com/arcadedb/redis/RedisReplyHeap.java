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
package com.arcadedb.redis;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.exception.HeapLimitExceededException;
import com.arcadedb.exception.QueryHeapBudgetExceededException;
import com.arcadedb.query.sql.executor.QueryHeapBudget;
import com.arcadedb.query.sql.executor.QueryHeapTracker;

/**
 * The heap the reply of one command takes while it is built and sent, accounted to the budget the queries of every protocol share
 * ({@link GlobalConfiguration#QUERY_MAX_HEAP_RAM}).
 * <p>
 * The reply of HMGET has one entry per key of the request, so what it takes follows the request. Charging each entry as it is added
 * refuses a reply that does not fit, with the error the other protocols answer (a retryable one when other queries hold the budget,
 * a plain one when this reply alone is larger than all of it), before the memory of the whole reply is taken.
 * <p>
 * What is added is the size of an entry as it is held once; the reply is held in several copies while it goes out (the values read,
 * the text built from them, the builder that grows it and the bytes it is encoded to), so {@link #REPLY_COPIES} times that is charged.
 * The charges reach the tracker in batches of {@link #FORWARD_BYTES}, and a reply that stays below one never creates it: a small
 * reply costs two additions and a comparison.
 * <p>
 * Not thread-safe: one command is served by one thread. Whoever builds a reply calls {@link #close()} when it is gone.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
public final class RedisReplyHeap {
  /** The copies of a reply held at once at its peak, see the class comment. */
  public static final int  REPLY_COPIES = 4;
  /** The charge a reply collects before it reaches the tracker. */
  static final        long FORWARD_BYTES = 64 * 1024;

  private final String           operation;
  private       QueryHeapTracker tracker;
  // what the current reply added and has not charged yet, and what it was charged
  private       long             pending;
  private       long             forwarded;

  /**
   * @param operation what holds the reply, as the refusal names it
   */
  public RedisReplyHeap(final String operation) {
    this.operation = operation;
  }

  /**
   * Accounts an entry of {@code bytes} added to the reply.
   *
   * @throws QueryHeapBudgetExceededException when the queries running now hold too much of the budget for the reply to grow
   * @throws HeapLimitExceededException       when the reply alone needs more than the whole budget
   */
  public void add(final long bytes) {
    pending += bytes * REPLY_COPIES;
    if (pending >= FORWARD_BYTES)
      forward();
  }

  /** Charges what the last entries added, for a reply that went past the first batch: the tail of a small one is not worth a tracker. */
  public void flush() {
    if (forwarded > 0 && pending > 0)
      forward();
  }

  /** Gives back to the budget everything this reply was charged, ready for the next reply. Idempotent. */
  public void close() {
    pending = 0L;
    if (forwarded > 0) {
      forwarded = 0L;
      tracker.close();
    }
  }

  private void forward() {
    if (!QueryHeapBudget.isEnabled()) {
      pending = 0L;
      return;
    }
    if (tracker == null)
      tracker = new QueryHeapTracker();
    final long bytes = pending;
    pending = 0L;
    // counted before the charge: a refusal leaves what the reply was charged before it held, and close() must give that back
    forwarded += bytes;
    tracker.charge(bytes, operation);
  }
}
