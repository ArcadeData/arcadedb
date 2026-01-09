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

import com.arcadedb.log.LogManager;

import java.util.*;
import java.util.logging.Level;

/**
 * A frequency-based cache that keeps the most frequently accessed elements.
 * When the cache reaches maxSize, it performs a batch eviction removing 25% of entries,
 * keeping the 75% most frequently accessed elements. maxSize less than 1 means unlimited size.
 * <p>
 * This design avoids frequent single-entry evictions and is optimized for scenarios where
 * batch cleanup is more efficient than incremental eviction.
 * <p>
 * Note: This implementation is NOT thread-safe. Wrap with Collections.synchronizedMap()
 * or use external synchronization for concurrent access.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class MostUsedCache<K, V> extends AbstractMap<K, V> {
  private static final float RETENTION_RATIO = 0.75f; // Keep 75% most used

  private final int maxSize;
  private final int targetSizeAfterEviction;
  private final HashMap<K, CacheEntry<V>> cache;

  public MostUsedCache(final int maxSize) {
    this.maxSize = maxSize;
    this.targetSizeAfterEviction = Math.max(1, (int) (maxSize * RETENTION_RATIO));
    this.cache = maxSize > 0 ? new HashMap<>((int) (maxSize / 0.75f) + 1) : new HashMap<>();
  }

  @Override
  public V get(final Object key) {
    final CacheEntry<V> entry = cache.get(key);
    if (entry != null) {
      entry.accessCount++;
      return entry.value;
    }
    return null;
  }

  @Override
  public V put(final K key, final V value) {
    final CacheEntry<V> entry = cache.get(key);

    if (entry != null) {
      // Update existing entry
      final V oldValue = entry.value;
      entry.value = value;
      entry.accessCount++;
      return oldValue;
    } else {
      // Add new entry
      if (maxSize > 0 && cache.size() >= maxSize)
        evictEntries();

      cache.put(key, new CacheEntry<>(value));
      return null;
    }
  }

  @Override
  public V remove(final Object key) {
    final CacheEntry<V> entry = cache.remove(key);
    return entry != null ? entry.value : null;
  }

  @Override
  public void clear() {
    cache.clear();
  }

  @Override
  public int size() {
    return cache.size();
  }

  @Override
  public boolean containsKey(final Object key) {
    return cache.containsKey(key);
  }

  @Override
  public Set<Entry<K, V>> entrySet() {
    // Return a view of entries (without access counts)
    final Set<Entry<K, V>> entries = new HashSet<>(cache.size());
    for (final Entry<K, CacheEntry<V>> entry : cache.entrySet()) {
      entries.add(new SimpleEntry<>(entry.getKey(), entry.getValue().value));
    }
    return entries;
  }

  /**
   * Performs batch eviction, removing the least frequently accessed entries.
   * Keeps the top 75% most frequently accessed entries.
   */
  private void evictEntries() {
    if (cache.isEmpty())
      return;

    // Create list of entries sorted by access count (descending)
    final List<Map.Entry<K, CacheEntry<V>>> entries = new ArrayList<>(cache.entrySet());

    LogManager.instance().log(this, Level.FINE, "Cache reached max size of %d. Performing eviction to reduce to %d entries",
            maxSize, targetSizeAfterEviction);

    // Sort by access count in descending order (most accessed first)
    entries.sort((e1, e2) -> Long.compare(e2.getValue().accessCount, e1.getValue().accessCount));

    // Clear the cache
    cache.clear();

    // Keep only the top entries (most frequently accessed)
    final int keepCount = Math.min(targetSizeAfterEviction, entries.size());
    for (int i = 0; i < keepCount; i++) {
      final Map.Entry<K, CacheEntry<V>> entry = entries.get(i);
      // Reset access count to prevent overflow and give newer entries a chance
      entry.getValue().accessCount = entry.getValue().accessCount / 2;
      cache.put(entry.getKey(), entry.getValue());
    }
  }

  /**
   * Returns the access count for a given key (useful for testing/debugging).
   */
  public long getAccessCount(final K key) {
    final CacheEntry<V> entry = cache.get(key);
    return entry != null ? entry.accessCount : 0;
  }

  /**
   * Returns statistics about the cache.
   */
  public CacheStats getStats() {
    if (cache.isEmpty())
      return new CacheStats(0, 0, 0, 0);

    long totalAccess = 0;
    long minAccess = Long.MAX_VALUE;
    long maxAccess = 0;

    for (final CacheEntry<V> entry : cache.values()) {
      totalAccess += entry.accessCount;
      minAccess = Math.min(minAccess, entry.accessCount);
      maxAccess = Math.max(maxAccess, entry.accessCount);
    }

    final long avgAccess = totalAccess / cache.size();
    return new CacheStats(cache.size(), avgAccess, minAccess, maxAccess);
  }

  private static class CacheEntry<V> {
    V value;
    long accessCount;

    CacheEntry(final V value) {
      this.value = value;
      this.accessCount = 1;
    }
  }

  public record CacheStats(int size, long avgAccessCount, long minAccessCount, long maxAccessCount) {

    @Override
    public String toString() {
      return String.format("CacheStats{size=%d, avg=%d, min=%d, max=%d}",
              size, avgAccessCount, minAccessCount, maxAccessCount);
    }
  }
}
