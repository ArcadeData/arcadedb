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

/**
 * Zero-boxing open-addressing hash map: primitive long keys → primitive long values.
 * <p>
 * Uses linear probing with a power-of-2 table size. No Long auto-boxing,
 * no Map.Entry objects, no long[] value wrappers — just two flat arrays.
 * <p>
 * Designed for GAV fused aggregation where keys are packed nodeId pairs
 * and values are counts. Typical usage: 50K-500K entries with ~740K lookups.
 * <p>
 * Load factor: resizes at 75% occupancy. Tombstones not needed (no deletes).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class LongLongHashMap {
  private static final long EMPTY = Long.MIN_VALUE; // sentinel for empty slots
  private static final float LOAD_FACTOR = 0.75f;

  private long[] keys;
  private long[] values;
  private int capacity;
  private int mask;
  private int size;
  private int threshold;

  public LongLongHashMap() {
    this(1024);
  }

  public LongLongHashMap(final int initialCapacity) {
    capacity = nextPowerOfTwo(Math.max(16, initialCapacity));
    mask = capacity - 1;
    keys = new long[capacity];
    values = new long[capacity];
    threshold = (int) (capacity * LOAD_FACTOR);
    java.util.Arrays.fill(keys, EMPTY);
  }

  /**
   * Increments the value for the given key by 1. If the key doesn't exist, inserts it with value 1.
   * This is the hot-path method for fused count(*) aggregation.
   */
  public void increment(final long key) {
    int idx = hash(key) & mask;
    while (true) {
      final long k = keys[idx];
      if (k == key) {
        values[idx]++;
        return;
      }
      if (k == EMPTY) {
        keys[idx] = key;
        values[idx] = 1;
        if (++size >= threshold)
          resize();
        return;
      }
      idx = (idx + 1) & mask;
    }
  }

  /**
   * Adds the given delta to the value for the key. Used for merging thread-local maps.
   */
  public void add(final long key, final long delta) {
    int idx = hash(key) & mask;
    while (true) {
      final long k = keys[idx];
      if (k == key) {
        values[idx] += delta;
        return;
      }
      if (k == EMPTY) {
        keys[idx] = key;
        values[idx] = delta;
        if (++size >= threshold)
          resize();
        return;
      }
      idx = (idx + 1) & mask;
    }
  }

  /**
   * Returns the value for the given key, or the default value if the key is not present.
   */
  public long get(final long key, final long defaultValue) {
    int idx = hash(key) & mask;
    while (true) {
      final long k = keys[idx];
      if (k == key)
        return values[idx];
      if (k == EMPTY)
        return defaultValue;
      idx = (idx + 1) & mask;
    }
  }

  public int size() {
    return size;
  }

  /**
   * Iterates over all entries. The consumer receives (key, value) pairs.
   * No object allocation during iteration.
   */
  public void forEach(final EntryConsumer consumer) {
    for (int i = 0; i < capacity; i++)
      if (keys[i] != EMPTY)
        consumer.accept(keys[i], values[i]);
  }

  /**
   * Merges all entries from another map into this one, summing values for duplicate keys.
   */
  public void mergeFrom(final LongLongHashMap other) {
    other.forEach(this::add);
  }

  @FunctionalInterface
  public interface EntryConsumer {
    void accept(long key, long value);
  }

  private static int hash(final long key) {
    // Murmur3-style finalizer for good bit distribution
    long h = key;
    h ^= h >>> 33;
    h *= 0xff51afd7ed558ccdL;
    h ^= h >>> 33;
    h *= 0xc4ceb9fe1a85ec53L;
    h ^= h >>> 33;
    return (int) h;
  }

  private void resize() {
    final int newCapacity = capacity << 1;
    final int newMask = newCapacity - 1;
    final long[] newKeys = new long[newCapacity];
    final long[] newValues = new long[newCapacity];
    java.util.Arrays.fill(newKeys, EMPTY);

    for (int i = 0; i < capacity; i++) {
      final long k = keys[i];
      if (k != EMPTY) {
        int idx = hash(k) & newMask;
        while (newKeys[idx] != EMPTY)
          idx = (idx + 1) & newMask;
        newKeys[idx] = k;
        newValues[idx] = values[i];
      }
    }

    keys = newKeys;
    values = newValues;
    capacity = newCapacity;
    mask = newMask;
    threshold = (int) (newCapacity * LOAD_FACTOR);
  }

  private static int nextPowerOfTwo(final int v) {
    int n = v - 1;
    n |= n >>> 1;
    n |= n >>> 2;
    n |= n >>> 4;
    n |= n >>> 8;
    n |= n >>> 16;
    return n + 1;
  }
}
