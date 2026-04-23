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
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
package com.arcadedb.utility;

import java.util.Arrays;

/**
 * Zero-boxing open-addressing hash map for {@code long} keys to object values.
 * <p>
 * Uses parallel {@code long[] keys} + {@code Object[] values} (linear probing),
 * eliminating Long boxing on every {@code put}/{@code get} and HashMap.Node allocation.
 * <p>
 * Memory per entry: ~16 bytes (long key + reference value) at 0.75 load factor,
 * vs {@code HashMap<Long, V>} at ~72-90 bytes (Node + Long + table slot) -
 * roughly <b>4-5x savings</b>.
 * <p>
 * The reserved sentinel {@link Long#MIN_VALUE} marks empty slots and therefore
 * cannot be used as a key. Designed for vertex-key / position maps where this
 * limitation is irrelevant.
 * <p>
 * Not thread-safe. Mirrors the pattern of {@link LongHashSet}.
 */
public final class LongObjectHashMap<V> {
  private static final long  EMPTY_KEY   = Long.MIN_VALUE;
  private static final float LOAD_FACTOR = 0.75f;

  private long[]   keys;
  private Object[] values;
  private int      capacity;
  private int      mask;
  private int      size;
  private int      threshold;

  public LongObjectHashMap() {
    this(16);
  }

  public LongObjectHashMap(final int initialCapacity) {
    capacity = nextPowerOfTwo(Math.max(16, initialCapacity));
    mask = capacity - 1;
    keys = new long[capacity];
    values = new Object[capacity];
    threshold = (int) (capacity * LOAD_FACTOR);
    Arrays.fill(keys, EMPTY_KEY);
  }

  /**
   * Associates {@code value} with {@code key}. Returns the previous value, or {@code null}
   * if the key was absent.
   *
   * @throws IllegalArgumentException if {@code key == Long.MIN_VALUE}
   *                                  (reserved as the empty-slot sentinel).
   */
  @SuppressWarnings("unchecked")
  public V put(final long key, final V value) {
    if (key == EMPTY_KEY)
      throw new IllegalArgumentException("Long.MIN_VALUE is reserved as the empty-slot sentinel");

    int idx = hash(key) & mask;
    while (true) {
      final long slot = keys[idx];
      if (slot == EMPTY_KEY) {
        keys[idx] = key;
        values[idx] = value;
        if (++size >= threshold)
          resize();
        return null;
      }
      if (slot == key) {
        final V prev = (V) values[idx];
        values[idx] = value;
        return prev;
      }
      idx = (idx + 1) & mask;
    }
  }

  /**
   * Returns the value for {@code key}, or {@code null} if absent.
   */
  @SuppressWarnings("unchecked")
  public V get(final long key) {
    if (key == EMPTY_KEY)
      return null;

    int idx = hash(key) & mask;
    while (true) {
      final long slot = keys[idx];
      if (slot == EMPTY_KEY)
        return null;
      if (slot == key)
        return (V) values[idx];
      idx = (idx + 1) & mask;
    }
  }

  public boolean containsKey(final long key) {
    if (key == EMPTY_KEY)
      return false;

    int idx = hash(key) & mask;
    while (true) {
      final long slot = keys[idx];
      if (slot == EMPTY_KEY)
        return false;
      if (slot == key)
        return true;
      idx = (idx + 1) & mask;
    }
  }

  public int size() {
    return size;
  }

  public boolean isEmpty() {
    return size == 0;
  }

  public void clear() {
    Arrays.fill(keys, EMPTY_KEY);
    Arrays.fill(values, null);
    size = 0;
  }

  /**
   * Iterates all entries. No object allocation during iteration.
   */
  @SuppressWarnings("unchecked")
  public void forEach(final EntryConsumer<V> consumer) {
    for (int i = 0; i < capacity; i++) {
      final long k = keys[i];
      if (k != EMPTY_KEY)
        consumer.accept(k, (V) values[i]);
    }
  }

  /**
   * Returns a snapshot {@code long[]} of all keys. Order is unspecified.
   */
  public long[] keysArray() {
    final long[] out = new long[size];
    int o = 0;
    for (int i = 0; i < capacity; i++) {
      final long k = keys[i];
      if (k != EMPTY_KEY)
        out[o++] = k;
    }
    return out;
  }

  @FunctionalInterface
  public interface EntryConsumer<V> {
    void accept(long key, V value);
  }

  private static int hash(final long value) {
    // MurmurHash3 finalizer on long
    long h = value;
    h ^= h >>> 33;
    h *= 0xff51afd7ed558ccdL;
    h ^= h >>> 33;
    h *= 0xc4ceb9fe1a85ec53L;
    h ^= h >>> 33;
    return (int) h;
  }

  @SuppressWarnings("unchecked")
  private void resize() {
    final int newCapacity = capacity << 1;
    final int newMask = newCapacity - 1;
    final long[] newKeys = new long[newCapacity];
    final Object[] newValues = new Object[newCapacity];
    Arrays.fill(newKeys, EMPTY_KEY);

    for (int i = 0; i < capacity; i++) {
      final long k = keys[i];
      if (k != EMPTY_KEY) {
        int idx = hash(k) & newMask;
        while (newKeys[idx] != EMPTY_KEY)
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
