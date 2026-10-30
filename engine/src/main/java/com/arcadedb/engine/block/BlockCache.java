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
package com.arcadedb.engine.block;

import com.arcadedb.GlobalConfiguration;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;

/**
 * JVM-wide cache of decoded column blocks, keyed by {@code (fileId, blockId, column)}.
 * <p>
 * The design follows {@code TimeSeriesDecodedColumnCache}: the byte budget is the cache's one safety property, so
 * the charge, the map and the eviction move together under one monitor. This cache is shared by every query in the
 * JVM instead of one shard, so it is split into lock-striped, access-ordered {@link LinkedHashMap}s, each with an
 * exact budget of {@code maxBytes / stripes}. Per-stripe budgets keep {@link #usedBytes()} {@code <= maxBytes} true
 * at every instant without any cross-stripe locking. An entry larger than one stripe's share is returned uncached.
 * <p>
 * The loader and the sizer run OUTSIDE the stripe monitor, so no I/O or decode ever blocks other queries. Two
 * queries missing on the same key at once may both load; the first to come back admits its value, the second finds
 * it held, drops its own and returns the held one, so the entry is held and charged once.
 * <p>
 * <b>Invalidation.</b> {@link #invalidateFile(long)} and {@link #clear()} advance a global epoch; a load that started
 * before an invalidation and finishes after it is returned to its caller but never admitted, so an in-flight load
 * cannot re-introduce a block of a file that was just rewritten. A lowered budget is enforced by whichever call
 * touches a stripe next (hit or miss), and a budget of 0 empties the stripes it touches and serves no hits. Every
 * entry is charged its sizer result plus {@link #ENTRY_OVERHEAD}; a negative sizer result is refused.
 * <p>
 * <b>Cached values are shared, not copied,</b> so every consumer must treat them as read-only.
 */
public final class BlockCache {
  public static final int DEFAULT_STRIPES = 16;

  /**
   * Fixed bookkeeping charged on top of the sizer result for every entry (map node, key, holder): it makes zero-size
   * values evictable and keeps the budget honest about the cost of many tiny entries.
   */
  static final long ENTRY_OVERHEAD = 96;

  private record Key(long fileId, long blockId, int column) {
  }

  private record Held(Object value, long bytes) {
  }

  private static final class Stripe {
    final Map<Key, Held> entries = new LinkedHashMap<>(16, 0.75f, true);
    volatile long used;
  }

  private static final class Holder {
    static final BlockCache INSTANCE = new BlockCache(GlobalConfiguration.BLOCK_CACHE_MAX_BYTES::getValueAsLong, DEFAULT_STRIPES);
  }

  private final LongSupplier maxBytes;
  private final Stripe[]     stripes;
  private final int          mask;
  private final AtomicLong   fileIds = new AtomicLong();
  private final AtomicLong   epoch   = new AtomicLong();
  private final LongAdder    hits    = new LongAdder();
  private final LongAdder    misses  = new LongAdder();

  /**
   * @param maxBytes read on every call, so a change of the setting applies at the next touch of a stripe (hit or miss)
   * @param stripes  the number of stripes, a power of two
   */
  BlockCache(final LongSupplier maxBytes, final int stripes) {
    if (stripes <= 0 || (stripes & (stripes - 1)) != 0)
      throw new IllegalArgumentException("stripes must be a power of two: " + stripes);
    this.maxBytes = maxBytes;
    this.stripes = new Stripe[stripes];
    for (int i = 0; i < stripes; i++)
      this.stripes[i] = new Stripe();
    this.mask = stripes - 1;
  }

  public static BlockCache instance() {
    return Holder.INSTANCE;
  }

  /**
   * Returns the block held under the key, loading and admitting it on a miss. A {@code null} from the loader is
   * returned and not cached; a loader exception propagates and leaves nothing behind.
   */
  @SuppressWarnings("unchecked")
  public <T> T get(final long fileId, final long blockId, final int column, final Supplier<T> loader, final ToLongFunction<T> sizer) {
    final Key key = new Key(fileId, blockId, column);
    final Stripe stripe = stripes[spread(key.hashCode()) & mask];

    // Read once per call, so every stripe this call touches is trimmed against the same budget.
    final long budget = maxBytes.getAsLong();
    final long share = budget > 0 ? budget / stripes.length : 0;

    synchronized (stripe) {
      // A lowered budget is enforced on every touch, not only on admissions; budget 0 empties the stripe and serves no hit.
      if (stripe.used > share)
        trim(stripe, stripe.used, share);
      if (budget > 0) {
        final Held cached = stripe.entries.get(key);
        if (cached != null) {
          hits.increment();
          return (T) cached.value();
        }
      }
    }
    misses.increment();

    final long epochBefore = epoch.get();
    // No monitor is held while loading or sizing.
    final T loaded = loader.get();
    if (loaded == null || budget <= 0)
      return loaded;

    final long size = sizer.applyAsLong(loaded);
    if (size < 0)
      throw new IllegalArgumentException("Negative block size: " + size);
    // Compared before adding the overhead, so a size near Long.MAX_VALUE cannot overflow into a negative charge.
    if (size > share - ENTRY_OVERHEAD)
      return loaded;
    final long charge = size + ENTRY_OVERHEAD;

    synchronized (stripe) {
      // The file was invalidated while this load was in flight: the value may describe bytes that no longer exist.
      if (epoch.get() != epochBefore)
        return loaded;

      final Held existing = stripe.entries.get(key);
      if (existing != null)
        // Another thread admitted the key while this one was loading: hand back its value, charged once.
        return (T) existing.value();

      stripe.entries.put(key, new Held(loaded, charge));
      // The over-budget total is never published: usedBytes() readers only ever see a total that fits the share.
      trim(stripe, stripe.used + charge, share);
    }
    return loaded;
  }

  /** Evicts from the eldest end until the stripe fits the share. Caller holds the stripe monitor. */
  private static void trim(final Stripe stripe, final long total, final long share) {
    long used = total;
    final Iterator<Held> eldest = stripe.entries.values().iterator();
    while (used > share && eldest.hasNext()) {
      used -= eldest.next().bytes();
      eldest.remove();
    }
    stripe.used = used;
  }

  /** Drops every block of the file, e.g. when the file is rewritten or closed. */
  public void invalidateFile(final long fileId) {
    // Bumped BEFORE the sweep: a load in flight compares the epoch under the stripe monitor at admission, so it is
    // either swept here (admitted earlier) or refused (admitted later).
    epoch.incrementAndGet();
    for (final Stripe stripe : stripes) {
      synchronized (stripe) {
        long used = stripe.used;
        final Iterator<Map.Entry<Key, Held>> it = stripe.entries.entrySet().iterator();
        while (it.hasNext()) {
          final Map.Entry<Key, Held> entry = it.next();
          if (entry.getKey().fileId() == fileId) {
            used -= entry.getValue().bytes();
            it.remove();
          }
        }
        stripe.used = used;
      }
    }
  }

  public void clear() {
    epoch.incrementAndGet();
    for (final Stripe stripe : stripes) {
      synchronized (stripe) {
        stripe.entries.clear();
        stripe.used = 0;
      }
    }
  }

  /** The sum of the stripes' exact totals. */
  public long usedBytes() {
    long total = 0;
    for (final Stripe stripe : stripes)
      total += stripe.used;
    return total;
  }

  /** A process-unique, positive id for a file. Ids are never persisted. */
  public long newFileId() {
    return fileIds.incrementAndGet();
  }

  public long hits() {
    return hits.sum();
  }

  public long misses() {
    return misses.sum();
  }

  /** Whether the key is held, without counting a hit or a miss and without touching the access order. */
  public boolean containsForTesting(final long fileId, final long blockId, final int column) {
    final Key key = new Key(fileId, blockId, column);
    final Stripe stripe = stripes[spread(key.hashCode()) & mask];
    synchronized (stripe) {
      return stripe.entries.containsKey(key);
    }
  }

  /** The number of entries held for the file, walked entry by entry. */
  public long entriesForTesting(final long fileId) {
    long total = 0;
    for (final Stripe stripe : stripes) {
      synchronized (stripe) {
        for (final Key key : stripe.entries.keySet())
          if (key.fileId() == fileId)
            total++;
      }
    }
    return total;
  }

  /** The sum of the held charges walked entry by entry, to check the running totals in tests. */
  long recountBytes() {
    long total = 0;
    for (final Stripe stripe : stripes) {
      synchronized (stripe) {
        for (final Held held : stripe.entries.values())
          total += held.bytes();
      }
    }
    return total;
  }

  private static int spread(final int hash) {
    final int h = hash * 0x9E3779B9;
    return h ^ (h >>> 16);
  }
}
