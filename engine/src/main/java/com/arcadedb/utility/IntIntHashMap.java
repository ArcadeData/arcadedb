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

import java.util.Arrays;

/**
 * Zero-boxing open-addressing hash map: primitive int keys → primitive int values.
 * <p>
 * Uses linear probing with a power-of-2 table size. No Integer auto-boxing,
 * no Map.Entry objects — just two flat int[] arrays.
 * <p>
 * Designed for graph algorithms and GAV overlay operations where node IDs (int)
 * map to counts, community labels, or other integer values.
 * <p>
 * Load factor: resizes at 75% occupancy. Tombstones not needed (no deletes).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class IntIntHashMap {
  private static final int   EMPTY       = Integer.MIN_VALUE;
  private static final float LOAD_FACTOR = 0.75f;

  private int[] keys;
  private int[] values;
  private int   capacity;
  private int   mask;
  private int   size;
  private int   threshold;

  public IntIntHashMap() {
    this(64);
  }

  public IntIntHashMap(final int initialCapacity) {
    capacity = nextPowerOfTwo(Math.max(16, initialCapacity));
    mask = capacity - 1;
    keys = new int[capacity];
    values = new int[capacity];
    threshold = (int) (capacity * LOAD_FACTOR);
    Arrays.fill(keys, EMPTY);
  }

  /**
   * Returns the value for the given key, or the defaultValue if not present.
   */
  public int get(final int key, final int defaultValue) {
    int idx = hash(key) & mask;
    while (true) {
      final int k = keys[idx];
      if (k == key)
        return values[idx];
      if (k == EMPTY)
        return defaultValue;
      idx = (idx + 1) & mask;
    }
  }

  /**
   * Sets the value for the given key. Returns the previous value, or defaultValue if not present.
   */
  public int put(final int key, final int value) {
    int idx = hash(key) & mask;
    while (true) {
      final int k = keys[idx];
      if (k == key) {
        final int old = values[idx];
        values[idx] = value;
        return old;
      }
      if (k == EMPTY) {
        keys[idx] = key;
        values[idx] = value;
        if (++size >= threshold)
          resize();
        return Integer.MIN_VALUE;
      }
      idx = (idx + 1) & mask;
    }
  }

  /**
   * Increments the value for the given key by 1. If not present, inserts with value 1.
   */
  public void increment(final int key) {
    int idx = hash(key) & mask;
    while (true) {
      final int k = keys[idx];
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
   * Adds delta to the value for the given key. If not present, inserts with delta as value.
   */
  public void add(final int key, final int delta) {
    int idx = hash(key) & mask;
    while (true) {
      final int k = keys[idx];
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
   * Returns true if the key is present.
   */
  public boolean containsKey(final int key) {
    int idx = hash(key) & mask;
    while (true) {
      final int k = keys[idx];
      if (k == key)
        return true;
      if (k == EMPTY)
        return false;
      idx = (idx + 1) & mask;
    }
  }

  public int size() {
    return size;
  }

  /**
   * Iterates over all entries. No object allocation during iteration.
   */
  public void forEach(final EntryConsumer consumer) {
    for (int i = 0; i < capacity; i++)
      if (keys[i] != EMPTY)
        consumer.accept(keys[i], values[i]);
  }

  /**
   * Returns the key that has the maximum value, or defaultKey if empty.
   */
  public int keyWithMaxValue(final int defaultKey) {
    int maxKey = defaultKey;
    int maxVal = Integer.MIN_VALUE;
    for (int i = 0; i < capacity; i++)
      if (keys[i] != EMPTY && values[i] > maxVal) {
        maxVal = values[i];
        maxKey = keys[i];
      }
    return maxKey;
  }

  /**
   * Resets all entries, keeping the allocated arrays. Zero allocation.
   */
  public void clear() {
    Arrays.fill(keys, EMPTY);
    size = 0;
  }

  @FunctionalInterface
  public interface EntryConsumer {
    void accept(int key, int value);
  }

  private static int hash(final int key) {
    // Murmur3-style 32-bit finalizer
    int h = key;
    h ^= h >>> 16;
    h *= 0x85ebca6b;
    h ^= h >>> 13;
    h *= 0xc2b2ae35;
    h ^= h >>> 16;
    return h;
  }

  private void resize() {
    final int newCapacity = capacity << 1;
    final int newMask = newCapacity - 1;
    final int[] newKeys = new int[newCapacity];
    final int[] newValues = new int[newCapacity];
    Arrays.fill(newKeys, EMPTY);

    for (int i = 0; i < capacity; i++) {
      final int k = keys[i];
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
