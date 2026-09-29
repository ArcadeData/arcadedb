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

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Scan-resistant LRU cache (segmented LRU). A new entry enters a probationary segment and is promoted to the protected segment
 * only when it is hit again, so a burst of one-off keys (for example thousands of queries that embed their values in the text)
 * evicts other one-off keys instead of the entries the application keeps coming back to. The protected segment holds up to
 * {@value #PROTECTED_PERCENT}% of the capacity; a promotion that overflows it demotes its least recently used entry back to
 * probation. An entry needs a second hit to be protected, so one hit twice within a burst of more than the probation window (about
 * 20% of the capacity) of other new keys is still evicted. A capacity of 0 caches nothing. Null values are not cached (a null answer means a miss). Not thread safe: wrap access in a synchronized block.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class SegmentedLRUCache<K, V> {
  private static final int PROTECTED_PERCENT = 80;

  private final int                 capacity;
  private final int                 protectedCapacity;
  private final LinkedHashMap<K, V> probation;
  private final LinkedHashMap<K, V> protectedSegment;

  public SegmentedLRUCache(final int capacity) {
    this.capacity = Math.max(0, capacity);
    // a single slot cannot be split into two segments: it stays a plain probation slot
    this.protectedCapacity = this.capacity < 2 ? 0 : Math.max(1, this.capacity * PROTECTED_PERCENT / 100);
    this.probation = new LinkedHashMap<>(Math.max(16, (int) ((this.capacity - protectedCapacity) / 0.75) + 1), 0.75f, true);
    this.protectedSegment = new LinkedHashMap<>(Math.max(16, (int) (protectedCapacity / 0.75) + 1), 0.75f, true);
  }

  /** Returns the value for the key, or null. A hit refreshes the entry and promotes it out of probation. */
  public V get(final K key) {
    final V value = protectedSegment.get(key);
    if (value != null)
      return value;

    if (protectedCapacity == 0)
      return probation.get(key);

    final V probationary = probation.remove(key);
    if (probationary == null)
      return null;

    protectedSegment.put(key, probationary);
    if (protectedSegment.size() > protectedCapacity) {
      final Iterator<Map.Entry<K, V>> eldest = protectedSegment.entrySet().iterator();
      final Map.Entry<K, V> demoted = eldest.next();
      probation.put(demoted.getKey(), demoted.getValue());
      eldest.remove();
    }
    return probationary;
  }

  public void put(final K key, final V value) {
    if (capacity == 0)
      return;

    if (protectedSegment.containsKey(key))
      protectedSegment.put(key, value);
    else
      probation.put(key, value);

    while (probation.size() + protectedSegment.size() > capacity) {
      final Iterator<K> eldest = (probation.isEmpty() ? protectedSegment : probation).keySet().iterator();
      eldest.next();
      eldest.remove();
    }
  }

  public boolean containsKey(final K key) {
    return protectedSegment.containsKey(key) || probation.containsKey(key);
  }

  public V remove(final K key) {
    final V value = protectedSegment.remove(key);
    return value != null ? value : probation.remove(key);
  }

  public int size() {
    return probation.size() + protectedSegment.size();
  }

  public void clear() {
    probation.clear();
    protectedSegment.clear();
  }
}
