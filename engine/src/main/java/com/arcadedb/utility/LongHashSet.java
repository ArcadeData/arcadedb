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
 * Zero-boxing open-addressing hash set for primitive {@code long} values.
 * <p>
 * Uses a single {@code long[]} table (linear probing), eliminating Long boxing,
 * HashMap.Node allocation, and pointer indirection.
 * <p>
 * Memory: ~10.7 bytes per entry vs {@code HashSet<Long>} at ~72-80 bytes - roughly
 * <b>7x savings</b>. Lookups skip autoboxing and avoid {@code equals()} indirection,
 * making them several times faster on hot paths.
 * <p>
 * The reserved sentinel {@link Long#MIN_VALUE} marks empty slots and therefore
 * cannot be stored. Designed for vertex-key / position sets where this limitation
 * is irrelevant.
 */
public final class LongHashSet {
  private static final long  EMPTY_SLOT  = Long.MIN_VALUE;
  private static final float LOAD_FACTOR = 0.75f;

  private long[] table;
  private int    capacity;
  private int    mask;
  private int    size;
  private int    threshold;

  public LongHashSet() {
    this(16);
  }

  public LongHashSet(final int initialCapacity) {
    capacity = nextPowerOfTwo(Math.max(16, initialCapacity));
    mask = capacity - 1;
    table = new long[capacity];
    threshold = (int) (capacity * LOAD_FACTOR);
    Arrays.fill(table, EMPTY_SLOT);
  }

  /**
   * Adds {@code value}. Returns true if the set was modified (value was new).
   *
   * @throws IllegalArgumentException if {@code value == Long.MIN_VALUE}
   *                                  (reserved as the empty-slot sentinel).
   */
  public boolean add(final long value) {
    if (value == EMPTY_SLOT)
      throw new IllegalArgumentException("Long.MIN_VALUE is reserved as the empty-slot sentinel");

    int idx = hash(value) & mask;
    while (true) {
      final long slot = table[idx];
      if (slot == EMPTY_SLOT) {
        table[idx] = value;
        if (++size >= threshold)
          resize();
        return true;
      }
      if (slot == value)
        return false;
      idx = (idx + 1) & mask;
    }
  }

  public boolean contains(final long value) {
    if (value == EMPTY_SLOT)
      return false;

    int idx = hash(value) & mask;
    while (true) {
      final long slot = table[idx];
      if (slot == EMPTY_SLOT)
        return false;
      if (slot == value)
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
    Arrays.fill(table, EMPTY_SLOT);
    size = 0;
  }

  /**
   * Iterates all values without allocation.
   */
  public void forEach(final LongConsumer consumer) {
    for (int i = 0; i < capacity; i++) {
      final long v = table[i];
      if (v != EMPTY_SLOT)
        consumer.accept(v);
    }
  }

  @FunctionalInterface
  public interface LongConsumer {
    void accept(long value);
  }

  /**
   * Returns a snapshot {@code long[]} of all values. Order is unspecified.
   */
  public long[] toArray() {
    final long[] out = new long[size];
    int o = 0;
    for (int i = 0; i < capacity; i++) {
      final long v = table[i];
      if (v != EMPTY_SLOT)
        out[o++] = v;
    }
    return out;
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

  private void resize() {
    final int newCapacity = capacity << 1;
    final int newMask = newCapacity - 1;
    final long[] newTable = new long[newCapacity];
    Arrays.fill(newTable, EMPTY_SLOT);

    for (int i = 0; i < capacity; i++) {
      final long v = table[i];
      if (v != EMPTY_SLOT) {
        int idx = hash(v) & newMask;
        while (newTable[idx] != EMPTY_SLOT)
          idx = (idx + 1) & newMask;
        newTable[idx] = v;
      }
    }

    table = newTable;
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
