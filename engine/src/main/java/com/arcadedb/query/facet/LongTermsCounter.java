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

import com.arcadedb.utility.LongLongHashMap;

/**
 * Counts per raw 64-bit term (a LONG value, or the bits of a DOUBLE) for facets whose terms are not dictionary ordinals. The map
 * reserves {@link Long#MIN_VALUE} (the bits of {@code -0.0d}) as its empty marker, so that key is counted aside.
 */
public final class LongTermsCounter {
  private final LongLongHashMap map;
  private final int             initialCapacity;
  private       long            minValueCount;

  /** @param expected the distinct terms the counter is expected to hold: it never grows within that bound */
  public LongTermsCounter(final int expected) {
    initialCapacity = HashFacetCounter.presize(expected);
    map = new LongLongHashMap(initialCapacity);
  }

  public void increment(final long key) {
    if (key == Long.MIN_VALUE)
      minValueCount++;
    else
      map.increment(key);
  }

  public void add(final long key, final long n) {
    if (key == Long.MIN_VALUE)
      minValueCount += n;
    else
      map.add(key, n);
  }

  public long get(final long key) {
    return key == Long.MIN_VALUE ? minValueCount : map.get(key, 0L);
  }

  /** Visits every term with a non-zero count, in no particular order. */
  public void forEachNonZero(final LongLongHashMap.EntryConsumer consumer) {
    map.forEach((key, count) -> {
      if (count != 0)
        consumer.accept(key, count);
    });
    if (minValueCount != 0)
      consumer.accept(Long.MIN_VALUE, minValueCount);
  }

  public long estimatedBytes() {
    return bytesAt(map.size());
  }

  /** The distinct terms counted. */
  public int size() {
    return map.size() + (minValueCount != 0 ? 1 : 0);
  }

  /** The heap the counter holds once it counts {@code entries} distinct terms, to reserve before it grows there. */
  public long bytesAt(final int entries) {
    return HashFacetCounter.hashBytes(initialCapacity, entries, Long.BYTES * 2L);
  }

  /** The heap a counter expecting this many distinct terms holds, to reserve before allocating it. */
  public static long bytesFor(final int expected) {
    return HashFacetCounter.hashBytes(HashFacetCounter.presize(expected), 0, Long.BYTES * 2L);
  }
}
