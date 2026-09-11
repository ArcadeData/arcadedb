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
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

/**
 * The write-side counterpart of {@link TimeSeriesCompactionPause}: held by an HA follower while it replaces a
 * shard's sealed store with the leader's blob and applies the WAL that clears the matching mutable bucket
 * (issue #7337).
 *
 * <h2>Why the follower needs a lock a local compaction already takes</h2>
 *
 * On a follower the sealed store is not rewritten by local compaction at all - it is REPLACED, whole, by
 * {@code TimeSeriesSealedStore.installSealedFile}, driven from {@code ArcadeStateMachine.applySealedBlobs}. That
 * path takes the store's own {@code directoryLock}, which serialises it against readers of that one store and
 * against nothing else. In particular it does not take the shard's {@code compactionLock}, so
 * {@link TimeSeriesCompactionPause} - whose whole mechanism is to hold every shard's compaction READ lock - did
 * not exclude it, and a backup or snapshot taken on that follower could pair:
 * <ul>
 *   <li>a page image captured BEFORE the entry was applied, in which the samples are still in the mutable
 *       bucket, with</li>
 *   <li>a sealed image read AFTER it, in which the same samples are sealed.</li>
 * </ul>
 * Restored, that archive holds every one of those samples twice, and nothing reports an error. The leader ships
 * the sealed bytes and the clear WAL in ONE Raft entry, so they are atomic with respect to each other - but not
 * with respect to a copy of the database being taken on the node applying them, which is the gap this closes.
 * <p>
 * {@code compactionInProgress} is no answer here either: the follower never sets it for a shipped blob, so the
 * open-time watermark recovery that repairs a torn LOCAL compaction has nothing to key on.
 *
 * <h2>Lock ordering</h2>
 *
 * Shards are locked in the SAME order {@link TimeSeriesCompactionPause} takes them - schema type order, then
 * ascending shard index - and this lock only ever takes a subset of them. Two ordered acquisitions over the same
 * sequence cannot close a cycle, so a backup walking every shard and an apply walking the entry's shards can
 * block each other but never deadlock. The pause additionally acquires under a deadline and releases everything
 * it holds on expiry, which is a second, independent way out.
 * <p>
 * Like the pause, the locks are released by the thread that took them, so acquire and {@link #close()} on the
 * same thread.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class TimeSeriesSealedInstallLock implements AutoCloseable {

  /** One shard of one TimeSeries type, as a Raft entry names it. */
  public record ShardRef(String typeName, int shardIndex) {
  }

  private final List<Lock> held;
  private       boolean    released;

  private TimeSeriesSealedInstallLock(final List<Lock> held) {
    this.held = held;
  }

  /**
   * Takes the compaction WRITE lock of every shard named in {@code shards} that actually exists in
   * {@code database}, in the order {@link TimeSeriesCompactionPause} would take them.
   * <p>
   * A reference that resolves to nothing - an unknown type, a non-TimeSeries type, a type whose engine never
   * started (issue #6356), a shard index the engine does not have - is skipped rather than refused. The apply
   * path has its own diagnostics for each of those and must keep making them; this class only locks what is
   * there, and a shard that does not exist cannot be torn.
   * <p>
   * <b>The engine-unavailable case is a KNOWN GAP, not a safe skip</b> (CodeRabbit on PR #7474). When a type's
   * engine never loaded, {@code ArcadeStateMachine.repairEngineWithSealedFile} installs the blob and re-runs
   * {@code initEngine()} over it - with no lock, because the lock lives on {@code TimeSeriesShard} and there is
   * no shard until the engine loads. {@link TimeSeriesCompactionPause} skips those types for the same reason and
   * has since #7280. So a copy of the database taken across a repair can pair the same two images this class
   * exists to keep apart. Closing it means moving the per-shard lock somewhere that outlives the engine, which
   * is a change to who owns the engine's locking rather than a fix to this class, and is tracked separately.
   *
   * @param database  the database whose schema resolves the references
   * @param shards    the shards this entry installs sealed bytes for; empty or {@code null} holds nothing
   * @param timeoutMs total budget for acquiring every lock, not a per-shard one
   *
   * @throws TimeoutException if the budget runs out, with every lock already taken released first. The caller
   *                          must treat this as a failure of the whole apply rather than proceeding unlocked:
   *                          an install that runs outside this lock can be torn against a concurrent copy
   */
  public static TimeSeriesSealedInstallLock acquire(final Database database, final Collection<ShardRef> shards,
      final long timeoutMs) {
    if (shards == null || shards.isEmpty())
      return new TimeSeriesSealedInstallLock(new ArrayList<>());

    final List<Lock> acquired = new ArrayList<>(shards.size());
    final long deadline = System.currentTimeMillis() + timeoutMs;
    try {
      // Driven by the SCHEMA's type order rather than by the entry's, so this walk and the pause's walk visit the
      // shards in the same sequence. Ordering the entry's own list instead would be ordering it by something the
      // pause knows nothing about, which is not an ordering at all for the purpose of avoiding a cycle.
      for (final DocumentType type : database.getSchema().getTypes()) {
        if (!(type instanceof final LocalTimeSeriesType tsType))
          continue;

        final TimeSeriesEngine engine = tsType.getEngine();
        if (engine == null)
          continue;

        for (int i = 0; i < engine.getShardCount(); i++) {
          if (!contains(shards, tsType.getName(), i))
            continue;

          final Lock lock = engine.getShard(i).getCompactionLock().writeLock();
          final long remaining = deadline - System.currentTimeMillis();
          if (remaining <= 0 || !lock.tryLock(remaining, TimeUnit.MILLISECONDS))
            throw new TimeoutException(
                "Timeout of %dms expired while locking TimeSeries type '%s' shard %d for a sealed-store install".formatted(
                    timeoutMs, tsType.getName(), i));
          acquired.add(lock);
        }
      }
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      release(acquired);
      throw new TimeoutException("Interrupted while locking TimeSeries shards for a sealed-store install", e);
    } catch (final RuntimeException | Error e) {
      release(acquired);
      throw e;
    }
    return new TimeSeriesSealedInstallLock(acquired);
  }

  /**
   * A linear scan and not a set: an entry names one or two shards in practice, and building a hash set of that
   * size costs more than the scan it would replace.
   */
  private static boolean contains(final Collection<ShardRef> shards, final String typeName, final int shardIndex) {
    for (final ShardRef ref : shards)
      if (ref.shardIndex() == shardIndex && ref.typeName().equals(typeName))
        return true;
    return false;
  }

  /** How many shards this lock is holding. Zero when the entry named none that resolved. */
  public int getLockedShards() {
    return held.size();
  }

  /** Releases every shard. Idempotent, so a caller can close it from a try-with-resources after releasing early. */
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
