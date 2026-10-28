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
package com.arcadedb.query.facet;

import com.arcadedb.utility.IntIntHashMap;

/**
 * A counter backed by an {@link IntIntHashMap}. A query counts at most {@code Integer.MAX_VALUE} per ordinal (positions are per
 * bucket), but {@link #add} saturates there instead of wrapping.
 */
public final class HashFacetCounter implements FacetCounter {
  private static final int MIN_CAPACITY = 16;

  private final IntIntHashMap map;
  private final int          initialCapacity;

  /** @param expected the distinct ordinals the counter is expected to hold: it never grows within that bound */
  public HashFacetCounter(final int expected) {
    initialCapacity = presize(expected);
    map = new IntIntHashMap(initialCapacity);
  }

  @Override
  public void increment(final int ordinal) {
    map.increment(ordinal);
  }

  @Override
  public void add(final int ordinal, final long n) {
    final int current = map.get(ordinal, 0);
    map.put(ordinal, n >= Integer.MAX_VALUE - current ? Integer.MAX_VALUE : (int) (current + n));
  }

  @Override
  public long get(final int ordinal) {
    return map.get(ordinal, 0);
  }

  @Override
  public void forEachNonZero(final IntLongConsumer consumer) {
    map.forEach((ordinal, count) -> {
      if (count != 0)
        consumer.accept(ordinal, count);
    });
  }

  @Override
  public long estimatedBytes() {
    return bytesAt(map.size());
  }

  /** The distinct ordinals counted. */
  public int size() {
    return map.size();
  }

  /** The heap the counter holds once it counts {@code entries} distinct ordinals, to reserve before it grows there. */
  public long bytesAt(final int entries) {
    return hashBytes(initialCapacity, entries, Integer.BYTES * 2L);
  }

  /** The heap a counter expecting this many distinct ordinals holds, to reserve before allocating it. */
  public static long bytesFor(final int expected) {
    return hashBytes(presize(expected), 0, Integer.BYTES * 2L);
  }

  /** The slots to ask the map for so that {@code expected} entries stay under its 0.75 load factor: no rehash at that size. */
  static int presize(final int expected) {
    return (int) Math.min(1 << 30, (long) (expected / 0.75) + 1);
  }

  /**
   * The two parallel arrays of an open-addressing map with load factor 0.75: the capacity doubles until {@code size} fits. This is the
   * steady state: while a resize copies, the old arrays are held too (1.5 times this for one doubling), a transient the budget does
   * not charge.
   */
  static long hashBytes(final int initialCapacity, final int size, final long slotBytes) {
    long capacity = Integer.highestOneBit(Math.max(MIN_CAPACITY, initialCapacity) - 1) << 1;
    while (size >= capacity * 3 / 4)
      capacity <<= 1;
    return 32L + capacity * slotBytes;
  }
}
