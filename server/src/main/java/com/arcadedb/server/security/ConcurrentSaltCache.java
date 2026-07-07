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
package com.arcadedb.server.security;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Lock-free bounded cache used by {@link ServerSecurity} to memoize expensive PBKDF2 password
 * hashing results. Storage is a {@link ConcurrentHashMap} so reads ({@link #get}) never take a
 * global monitor: authentication requests no longer serialize on a single lock. A
 * {@link ConcurrentLinkedQueue} tracks insertion order for approximate-FIFO eviction once the
 * configured capacity is exceeded.
 * <p>
 * Eviction is best-effort: under heavy contention the size may transiently exceed the configured
 * maximum by a small margin, or a handful of extra entries may be evicted. This is acceptable
 * because a miss only triggers a recomputation - stored values are always correct.
 * <p>
 * Retention is insertion-order (approximate FIFO), <em>not</em> access-order LRU: a frequently used
 * entry inserted early can be evicted before a colder entry inserted later once the working set
 * exceeds the capacity. For the salt cache this is a non-issue in practice because the capacity
 * typically exceeds the number of distinct credentials; the only cost of an eviction is one extra
 * PBKDF2 recomputation.
 */
public class ConcurrentSaltCache {
  private final ConcurrentHashMap<String, String> map;
  private final ConcurrentLinkedQueue<String>     insertionOrder = new ConcurrentLinkedQueue<>();
  private final int                               maxSize;

  public ConcurrentSaltCache(final int maxSize) {
    if (maxSize <= 0)
      throw new IllegalArgumentException("Cache size must be greater than 0");
    this.maxSize = maxSize;

    // Guard against integer overflow for very large cache sizes: (int) (maxSize / 0.75f) + 1 can
    // wrap to a negative value, which ConcurrentHashMap rejects. Cap at the map's max capacity.
    int initialCapacity = (int) (maxSize / 0.75f) + 1;
    if (initialCapacity < 0)
      initialCapacity = 1 << 30;
    this.map = new ConcurrentHashMap<>(Math.max(16, initialCapacity));
  }

  public String get(final String key) {
    return map.get(key);
  }

  public void put(final String key, final String value) {
    // only track order and evict when the key is genuinely new; ConcurrentHashMap.put is atomic
    // per key, so exactly one racing insert of the same new key sees a null previous value
    if (map.put(key, value) == null) {
      insertionOrder.add(key);

      while (map.size() > maxSize) {
        final String eldest = insertionOrder.poll();
        if (eldest == null)
          break;
        map.remove(eldest);
      }
    }
  }

  public int size() {
    return map.size();
  }

  public boolean isEmpty() {
    return map.isEmpty();
  }

  /**
   * Returns a snapshot copy of the current cache keys. Intended for tests that assert on the cache
   * contents (e.g. that plaintext passwords are never retained as keys).
   */
  Set<String> keys() {
    return new HashSet<>(map.keySet());
  }
}
