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
package com.arcadedb.utility;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.lang.management.MemoryUsage;
import java.util.Locale;

/**
 * Readings of the JVM heap for a caller about to size a large allocation by what the heap can still give it.
 * <p>
 * {@code Runtime.freeMemory()} is the wrong reading for that: everything allocated since the last collection counts as
 * used, live or not, so just before a collection it says the heap is full when most of it is about to be reclaimed.
 * {@link MemoryPoolMXBean#getCollectionUsage()} is the garbage-free one: each pool's occupancy right after the JVM last
 * collected it, which approximates the live set. For a generational collector whose old-generation collections are
 * rare it reflects the last one, so it errs toward less headroom than there is, never more.
 * <p>
 * What it counts as live includes the page read cache, which is on the heap and entirely evictable: a caller that
 * credits it as free must reclaim it before it allocates ({@code PageManager.reclaimReadCacheRAM}).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class HeapUsage {
  private HeapUsage() {
  }

  /** The heap ceiling, i.e. what {@code -Xmx} allows, or {@link Long#MAX_VALUE} when the heap is unbounded. */
  public static long maxHeapBytes() {
    return Runtime.getRuntime().maxMemory();
  }

  /**
   * Live (post-collection) heap occupancy, summed over every heap pool that reports it.
   *
   * @return the bytes the JVM still held right after it last collected each pool, or {@code -1} when no pool reports a
   * collection usage - a JVM that has not collected yet, or a GC that does not publish the figure. A caller that gets
   * {@code -1} must not assume zero pressure.
   */
  public static long liveHeapBytes() {
    long live = -1L;
    for (final MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans()) {
      if (pool == null || pool.getType() != MemoryType.HEAP)
        continue;
      final MemoryUsage collectionUsage;
      try {
        collectionUsage = pool.getCollectionUsage();
      } catch (final RuntimeException e) {
        // An implementation that refuses the query is indistinguishable from one that does not publish it.
        continue;
      }
      if (collectionUsage == null)
        continue;
      if (live < 0)
        live = 0L;
      live += Math.max(0L, collectionUsage.getUsed());
    }
    return live;
  }

  /**
   * Heap a new allocation can realistically expect to get: the ceiling minus what is live after the last collection, or
   * {@code -1} when the live occupancy is unknown or the heap is unbounded.
   */
  public static long availableHeapBytes() {
    return availableFrom(liveHeapBytes());
  }

  /**
   * {@link #liveHeapBytes()} that never reports less than is live, for a caller that admits work by the reading and has no
   * other way to tell how recent it is. The post-collection figure of an old-generation pool is refreshed only by a cycle
   * that collects that generation - a young collection of G1 promotes into it without touching it - so what was promoted
   * since is missing from it. Such a pool counts its current occupancy when that is larger: everything promoted, with the
   * garbage it holds until its next cycle, which errs toward less headroom, never more. A pool of a collector with a single
   * generation, or a young one, keeps its post-collection figure.
   *
   * @return the bytes held, or {@code -1} when no pool reports a collection usage
   */
  public static long conservativeLiveHeapBytes() {
    long live = -1L;
    for (final MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans()) {
      if (pool == null || pool.getType() != MemoryType.HEAP)
        continue;
      final MemoryUsage collectionUsage;
      final MemoryUsage usage;
      try {
        collectionUsage = pool.getCollectionUsage();
        usage = pool.getUsage();
      } catch (final RuntimeException e) {
        continue;
      }
      if (collectionUsage == null)
        continue;
      long used = Math.max(0L, collectionUsage.getUsed());
      if (usage != null && isOldGenerationPool(pool.getName()))
        used = Math.max(used, usage.getUsed());
      if (live < 0)
        live = 0L;
      live += used;
    }
    return live;
  }

  /** {@link #availableHeapBytes()} from {@link #conservativeLiveHeapBytes()}: never more headroom than there is. */
  public static long conservativeAvailableHeapBytes() {
    return availableFrom(conservativeLiveHeapBytes());
  }

  /**
   * Whether a heap pool is the old generation of a generational collector (G1 Old Gen, PS Old Gen, Tenured Gen, ZGC Old
   * Generation), whose post-collection figure only that generation's own cycles refresh.
   */
  public static boolean isOldGenerationPool(final String poolName) {
    if (poolName == null)
      return false;
    final String name = poolName.toLowerCase(Locale.ROOT);
    return name.contains("old") || name.contains("tenured");
  }

  private static long availableFrom(final long live) {
    final long max = maxHeapBytes();
    if (live < 0 || max == Long.MAX_VALUE)
      return -1L;
    return Math.max(0L, max - live);
  }
}
