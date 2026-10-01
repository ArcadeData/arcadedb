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
package com.arcadedb.query.sql.executor;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.QueryHeapBudgetExceededException;
import com.arcadedb.utility.FileUtils;

import java.lang.ref.Cleaner;

/**
 * The heap the buffers of one query hold, and what it reserved for them from the {@link QueryHeapBudget} shared by
 * every query in the JVM (issue #8591). One tracker serves the whole query, sub-queries and parallel-scan workers
 * included: the root {@link BasicCommandContext} owns it and every context derived from that one hands out the same.
 * <p>
 * The operations of the query charge it through their {@link OperationHeapLimit}, and release what they charged when
 * their buffer goes: on close, when it is exhausted, or when the operation fails. The tracker reserves from the budget
 * in chunks - at least {@link #MIN_CHUNK_BYTES}, and a sixteenth of what it holds already, so a query that buffers
 * gigabytes touches the shared counter a few dozen times - and never for the first {@link #UNRESERVED_BYTES}, so a
 * query that buffers little never contends on it. It gives everything back the moment its buffers fit that allowance
 * again, which is where a query ends up once its operations are released.
 * <p>
 * An operation that is never released - a cursor the caller abandoned without closing it - cannot shrink the budget for
 * good: whatever the tracker still holds goes back to the budget when the garbage collector finds the query
 * unreachable, which is also the moment its buffers stop taking heap.
 * <p>
 * Thread-safe: the workers of a parallel scan charge the tracker of the query they work for. Charges arrive in batches
 * (see {@link OperationHeapLimit}), so the monitor is taken once per several kilobytes buffered, not per row.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class QueryHeapTracker {
  /** What a query may buffer before it reserves anything from the budget. */
  public static final long UNRESERVED_BYTES = 256 * 1024;
  /** The smallest reservation a tracker takes from the budget. */
  public static final long MIN_CHUNK_BYTES  = 1024 * 1024;

  private static final Cleaner CLEANER = Cleaner.create(runnable -> {
    final Thread thread = new Thread(runnable, "ArcadeDB-QueryHeapBudget-Cleaner");
    thread.setDaemon(true);
    return thread;
  });

  private final Reservation       reservation = new Reservation();
  private       long              used;
  private       Cleaner.Cleanable cleanable;

  /**
   * What the tracker holds reserved from the budget, kept apart from the tracker so the {@link Cleaner} can give it back
   * once the tracker is unreachable: an action that referenced the tracker would keep it reachable forever.
   */
  private static final class Reservation implements Runnable {
    private volatile long bytes;

    @Override
    public void run() {
      final long reserved = bytes;
      bytes = 0L;
      if (reserved > 0)
        QueryHeapBudget.release(reserved);
    }
  }

  /**
   * Accounts {@code bytes} more held by the query, reserving from the budget what the query's buffers need past the
   * unreserved allowance.
   *
   * @param operation the operation that asks, as the error names it
   *
   * @throws QueryHeapBudgetExceededException when the queries running now hold too much of the budget for this one to
   *                                          get what it needs: nothing is charged then
   * @throws CommandExecutionException        when the query alone needs more than the whole budget
   */
  public synchronized void charge(final long bytes, final String operation) {
    if (bytes <= 0)
      return;
    used += bytes;
    final long missing = used - UNRESERVED_BYTES - reservation.bytes;
    if (missing > 0)
      reserve(missing, bytes, operation);
  }

  /** Accounts {@code bytes} the query no longer holds, and gives back to the budget what it does not need anymore. */
  public synchronized void release(final long bytes) {
    if (bytes <= 0)
      return;
    used = Math.max(0L, used - bytes);

    final long reserved = reservation.bytes;
    if (reserved == 0)
      return;
    final long needed = Math.max(0L, used - UNRESERVED_BYTES);
    // ALL OF IT GOES BACK ONCE THE BUFFERS FIT THE ALLOWANCE AGAIN, WHICH IS WHERE A FINISHED QUERY ENDS UP: IT MUST NOT
    // SIT ON A CHUNK UNTIL THE GARBAGE COLLECTOR FINDS IT. WHILE IT STILL HOLDS MORE, UP TO TWO CHUNKS OF SURPLUS STAY,
    // SO A BUFFER THAT SHRINKS AND GROWS AGAIN (A TOP-N SORT) DOES NOT PAY A ROUND TRIP TO THE SHARED COUNTER EACH TIME
    final long giveBack = needed == 0 ? reserved : reserved - needed - MIN_CHUNK_BYTES;
    if (needed == 0 || giveBack >= MIN_CHUNK_BYTES) {
      reservation.bytes = reserved - giveBack;
      QueryHeapBudget.release(giveBack);
    }
  }

  /**
   * Gives everything back to the budget: the query holds no buffer anymore. The engine does not need to call it - every
   * operation releases its own share when its buffer goes, and the {@link Cleaner} takes back what an abandoned query
   * held - so it is for an embedder or a test that owns a tracker directly.
   */
  public synchronized void close() {
    used = 0L;
    final long reserved = reservation.bytes;
    if (reserved > 0) {
      reservation.bytes = 0L;
      QueryHeapBudget.release(reserved);
    }
  }

  /** The estimated bytes the buffers of the query hold. */
  public synchronized long getUsedBytes() {
    return used;
  }

  /** The bytes the query holds reserved from the budget. */
  public long getReservedBytes() {
    return reservation.bytes;
  }

  private void reserve(final long missing, final long bytes, final String operation) {
    final long limit = QueryHeapBudget.getLimitBytes();
    final long reserved = reservation.bytes;
    final long chunk = Math.max(missing, Math.max(MIN_CHUNK_BYTES, reserved >> 4));
    long granted = chunk;
    if (!QueryHeapBudget.tryReserve(chunk, limit)) {
      // THE CHUNK IS ONLY A WAY TO TOUCH THE SHARED COUNTER LESS OFTEN: WHAT THE BUFFERS REALLY NEED MAY STILL FIT
      granted = missing;
      if (missing == chunk || !QueryHeapBudget.tryReserve(missing, limit)) {
        used -= bytes;
        QueryHeapBudget.refused();
        throw refusal(missing, reserved, limit, operation);
      }
    }
    reservation.bytes = reserved + granted;
    if (cleanable == null)
      cleanable = CLEANER.register(this, reservation);
  }

  private CommandExecutionException refusal(final long missing, final long reserved, final long limit, final String operation) {
    final String setting = GlobalConfiguration.QUERY_MAX_HEAP_RAM.getKey();
    if (reserved + missing > limit)
      // NOTHING THE OTHER QUERIES RELEASE CAN MAKE ROOM FOR THIS ONE: RETRYING IT IS POINTLESS
      return new CommandExecutionException(
          "Query heap budget exceeded: the in-heap " + operation + " needs " + FileUtils.getSizeAsString(missing)
              + " more, and its query would hold more than the whole budget of " + FileUtils.getSizeAsString(limit)
              + " the buffers of all the running queries share. Reduce what the query buffers in heap (a LIMIT, a more "
              + "selective filter, an index on the ORDER BY) or set " + setting + " to increase the budget");

    return new QueryHeapBudgetExceededException(
        "Query heap budget exceeded: the in-heap " + operation + " needs " + FileUtils.getSizeAsString(missing)
            + " more, but the queries running now already hold " + FileUtils.getSizeAsString(QueryHeapBudget.getReservedBytes())
            + " of the " + FileUtils.getSizeAsString(limit) + " budget their buffers share. Retry once they complete, or set "
            + setting + " to increase the budget");
  }
}
