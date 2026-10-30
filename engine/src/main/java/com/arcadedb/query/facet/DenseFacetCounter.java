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

/** A counter backed by an {@code int[]} indexed by ordinal. Counts saturate at {@link Integer#MAX_VALUE}. */
public final class DenseFacetCounter implements FacetCounter {
  private final int[] counts;

  public DenseFacetCounter(final int cardinality) {
    counts = new int[cardinality];
  }

  @Override
  public void increment(final int ordinal) {
    if (counts[ordinal] != Integer.MAX_VALUE)
      counts[ordinal]++;
  }

  @Override
  public void add(final int ordinal, final long n) {
    final int current = counts[ordinal];
    counts[ordinal] = n >= Integer.MAX_VALUE - current ? Integer.MAX_VALUE : (int) (current + n);
  }

  @Override
  public long get(final int ordinal) {
    return counts[ordinal];
  }

  @Override
  public void forEachNonZero(final IntLongConsumer consumer) {
    for (int ordinal = 0; ordinal < counts.length; ordinal++)
      if (counts[ordinal] != 0)
        consumer.accept(ordinal, counts[ordinal]);
  }

  @Override
  public long estimatedBytes() {
    return bytesFor(counts.length);
  }

  /** The heap a counter of this cardinality holds, to reserve before allocating it. */
  public static long bytesFor(final int cardinality) {
    return 16L + 4L * cardinality;
  }
}
