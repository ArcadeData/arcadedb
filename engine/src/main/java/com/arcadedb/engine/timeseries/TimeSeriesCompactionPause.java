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
package com.arcadedb.engine.timeseries;

import com.arcadedb.database.Database;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalTimeSeriesType;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

/**
 * A held pause of TimeSeries compaction across every shard of every TimeSeries type of one database. While it is
 * open, no compaction can COMPLETE: it holds each shard's compaction read lock, which is the lock
 * {@code TimeSeriesShard.compactInternal} needs the write half of in its Phase 0, Phase 4a and Phase 4c.
 * Appends are unaffected - they take the read lock too.
 *
 * <h2>Why a copy of a database needs this (issue #7280)</h2>
 *
 * A sealed store is replaced as a whole file, atomically, entirely outside the point-in-time window a
 * {@code PageSnapshot} gives the page files and outside the flush suspension the fallback path gives them. So a
 * caller that fixes a page image at {@code t0} and then reads {@code .ts.sealed} at {@code T >= t0} can pair a
 * post-swap sealed image with a page image whose mutable bucket has not been cleared yet: the same samples
 * twice, restored.
 * <p>
 * The pause does not have to exclude every compaction, only a WHOLE one. A compaction that had already
 * committed its Phase 0 when the pause was taken has left {@code compactionInProgress} and its block-count
 * watermark in the page image, and the restored database's own open-time recovery
 * ({@code TimeSeriesShard}'s constructor) truncates the sealed store back to that watermark - so that
 * interleaving repairs itself. What has to be excluded is a compaction that both starts and finishes inside the
 * window, because nothing in the page image records that it happened.
 *
 * <h2>Lock ordering</h2>
 *
 * Shards are taken in {@link TimeSeriesShardOrder}, the single total order every multi-shard acquisition in this
 * package shares. That is what makes this pause and {@link TimeSeriesSealedInstallLock} - which takes the WRITE
 * half of a subset of the same locks, concurrently, by design - unable to close a cycle. Read its javadoc before
 * changing either walk: the order used to be each acquirer's own walk of the schema, which is not stable enough
 * to be held to (claude-review on PR #7474).
 * <p>
 * Take it BEFORE any flush suspension or point-in-time window, never inside one: a compaction sitting in Phase
 * 4c holds the write lock while it commits, and a commit that cannot proceed because the caller has already
 * suspended flushing would never release it. Taken first, it waits for that compaction to finish and only then
 * closes the door.
 * <p>
 * The read locks are released by the thread that took them, so acquire and {@link #close()} on the same thread.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class TimeSeriesCompactionPause implements AutoCloseable {
  private final List<Lock> held;
  private       boolean    released;

  private TimeSeriesCompactionPause(final List<Lock> held) {
    this.held = held;
  }

  /**
   * Pauses compaction on every shard of every TimeSeries type registered in {@code database}. Returns
   * immediately, holding nothing, on a database with no TimeSeries type.
   *
   * @param database  the database whose schema is walked for TimeSeries types
   * @param timeoutMs total budget for acquiring every shard's lock, not a per-shard one
   *
   * @return the held pause, to be closed by the caller
   *
   * @throws TimeoutException if the budget runs out, with every lock already taken released first. The caller
   *                          must treat this as a failure rather than proceeding unpaused: an archive written
   *                          without the pause can restore with duplicated samples and report success.
   */
  public static TimeSeriesCompactionPause acquire(final Database database, final long timeoutMs) {
    final List<Lock> acquired = new ArrayList<>();
    final long deadline = System.currentTimeMillis() + timeoutMs;
    try {
      // The shared order, NOT this thread's own walk of the schema: {@link TimeSeriesShardOrder} explains why a
      // walk of getTypes() is not a total order two concurrent acquirers can be held to.
      for (final TimeSeriesShardOrder.ShardSlot slot : TimeSeriesShardOrder.of(database)) {
        final Lock lock = slot.shard().getCompactionLock().readLock();
        final long remaining = deadline - System.currentTimeMillis();
        if (remaining <= 0 || !lock.tryLock(remaining, TimeUnit.MILLISECONDS))
          throw new TimeoutException(
              "Timeout of %dms expired while pausing the compaction of TimeSeries type '%s' shard %d".formatted(
                  timeoutMs, slot.typeName(), slot.shardIndex()));
        acquired.add(lock);
      }
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      release(acquired);
      throw new TimeoutException("Interrupted while pausing TimeSeries compaction", e);
    } catch (final RuntimeException | Error e) {
      release(acquired);
      throw e;
    }
    return new TimeSeriesCompactionPause(acquired);
  }

  /** How many shards this pause is holding. Zero when the database has no TimeSeries type. */
  public int getPausedShards() {
    return held.size();
  }

  /**
   * Releases every shard. Idempotent, so a caller that releases the pause early - as a backup does, the moment
   * the sealed stores have been read - can still close it from a try-with-resources.
   */
  @Override
  public void close() {
    if (released)
      return;
    released = true;
    release(held);
  }

  private static void release(final List<Lock> locks) {
    for (int i = locks.size() - 1; i >= 0; i--)
      locks.get(i).unlock();
    locks.clear();
  }
}
