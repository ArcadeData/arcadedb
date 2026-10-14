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
package com.arcadedb.function.sql.time;

import com.arcadedb.serializer.BinaryComparator;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * Ordering shared by the {@code ts.*} window functions that buffer their rows as {@code Object[]} tuples. Each caller names
 * the slot it orders by, so the key a function's answer is defined over is stated at the call site instead of inherited
 * from a sibling (issue #9956: {@code ts.rank} once ordered by the timestamp because its sort block was pasted from
 * {@code ts.lag}).
 */
final class WindowRows {
  /**
   * Natural order, except that two numbers of different classes (an Integer and a Double, or an Integer and a Long, in a
   * schemaless property) compare by value instead of failing the cast. Anything else that is not mutually comparable still
   * throws, so rows over genuinely unrelated keys are refused rather than ordered by a class-name tiebreak: a Number against
   * a non-Number, or two unrelated classes, fails the {@code Comparable} cast with the ClassCastException the callers
   * translate into a typed argument error.
   */
  @SuppressWarnings("unchecked")
  private static final Comparator<Object> KEY_NULLS_LAST = Comparator.nullsLast(
      (a, b) -> a instanceof Number && b instanceof Number ? BinaryComparator.compareTo(a, b) : ((Comparable<Object>) a).compareTo(b));

  private WindowRows() {
  }

  /**
   * Sorts the rows by {@code slot} (see {@link #KEY_NULLS_LAST}). A null key sorts last and, the sort being stable, rows without a key
   * keep their arrival order, which is the only order they have (issues #6388, #6389). Keys that cannot be compared with
   * each other are a typed argument error (#6390).
   */
  static void sortBy(final String function, final List<Object[]> rows, final int slot, final String argument) {
    try {
      rows.sort((a, b) -> KEY_NULLS_LAST.compare(a[slot], b[slot]));
    } catch (final ClassCastException e) {
      throw notComparable(function, argument, e);
    }
  }

  /**
   * Returns, aligned with {@code rows}, the standard SQL {@code RANK()} of each row's {@code slot} value: one plus the
   * number of rows whose value is strictly smaller. Equal values share a rank and the next distinct value skips the ranks
   * they took ("share and skip"). Null values rank after every non-null value. O(n log n): one sort of the keys, then a
   * lower-bound binary search per row.
   */
  static int[] rankBy(final String function, final List<Object[]> rows, final int slot, final String argument) {
    final int size = rows.size();
    final Object[] sorted = new Object[size];
    for (int i = 0; i < size; i++)
      sorted[i] = rows.get(i)[slot];

    final int[] ranks = new int[size];
    try {
      Arrays.sort(sorted, KEY_NULLS_LAST);
      for (int i = 0; i < size; i++)
        ranks[i] = lowerBound(sorted, rows.get(i)[slot]) + 1;
    } catch (final ClassCastException e) {
      throw notComparable(function, argument, e);
    }
    return ranks;
  }

  /** Index of the first element of {@code sorted} that is not smaller than {@code key}. */
  private static int lowerBound(final Object[] sorted, final Object key) {
    int low = 0;
    int high = sorted.length;
    while (low < high) {
      final int mid = (low + high) >>> 1;
      if (KEY_NULLS_LAST.compare(sorted[mid], key) < 0)
        low = mid + 1;
      else
        high = mid;
    }
    return low;
  }

  private static IllegalArgumentException notComparable(final String function, final String argument, final RuntimeException cause) {
    return new IllegalArgumentException(function + "() cannot order the rows: the " + argument + " values are not mutually comparable",
        cause);
  }
}
