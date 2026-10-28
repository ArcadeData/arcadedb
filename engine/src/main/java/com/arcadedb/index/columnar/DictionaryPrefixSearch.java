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
package com.arcadedb.index.columnar;

import java.util.Arrays;

/**
 * Value-ordered access to a {@link GlobalDictionary} through its rank view: the ordinals whose value starts with a prefix, and the gap
 * rank a cursor value falls in.
 * <p>
 * The rank view ({@link GlobalDictionary#sortedOrdinals()}) covers the ordinals that existed when it was last built; the newer ones (the
 * unranked tail, bounded by the growth since that rebuild, plan 3) are not in it. A search therefore binary-searches the view and scans
 * the tail, then merges both in value order. Unranked ordinals of one gap share one rank ({@link GlobalDictionary#rank}), so a search
 * resumes after a VALUE, never after a rank. Value order is {@link DictionaryValueCodec#compareValues}: strings by
 * {@link String#compareTo}, before every other class, so a prefix range is one contiguous run of the view. The match is case-sensitive
 * and literal: a prefix is never a pattern.
 * <p>
 * Thread safe: the view is read once per call and the dictionary publishes appends safely.
 */
public final class DictionaryPrefixSearch {
  private DictionaryPrefixSearch() {
  }

  /**
   * The ordinals whose value is a string starting with {@code prefix}, in value order, after {@code afterExclusiveOrNull} when given.
   *
   * @param prefix               the literal prefix; empty matches every string value
   * @param afterExclusiveOrNull the last value of the previous batch (a dictionary value or not), null to start at the prefix range
   * @param max                  the most ordinals returned; 0 or less returns none
   * @param out                  receives the ordinals; its length bounds {@code max} too
   *
   * @return the number of ordinals written to {@code out}
   */
  public static int search(final GlobalDictionary dictionary, final String prefix, final String afterExclusiveOrNull, final int max,
      final int[] out) {
    final int limit = Math.min(max, out.length);
    if (limit <= 0)
      return 0;
    // the view and the ordinals it covers, read once: a rebuild publishes a new array
    final int[] sorted = dictionary.sortedOrdinals();
    final boolean afterBounds = afterExclusiveOrNull != null && afterExclusiveOrNull.compareTo(prefix) >= 0;
    // first view position inside the range: after the cursor when it lies at or above the prefix, else at the prefix
    int position = afterBounds ? firstGreater(dictionary, sorted, afterExclusiveOrNull) : firstAtLeast(dictionary, sorted, prefix);

    final int[] tail = tailInRange(dictionary, sorted.length, prefix, afterBounds ? afterExclusiveOrNull : null);
    int t = 0;
    int n = 0;
    while (n < limit) {
      final int fromView = position < sorted.length && startsWith(dictionary.valueOf(sorted[position]), prefix) ? sorted[position] : -1;
      final int fromTail = t < tail.length ? tail[t] : -1;
      if (fromView < 0 && fromTail < 0)
        break;
      if (fromTail < 0 || fromView >= 0 && compare(dictionary, fromView, fromTail) < 0) {
        out[n++] = fromView;
        position++;
      } else {
        out[n++] = fromTail;
        t++;
      }
    }
    return n;
  }

  /**
   * The gap rank {@code 2p} of the first view entry strictly greater than {@code value} (position {@code p}, rank {@code 2p + 1}): every
   * ordinal ranked above it sorts after the value, every ordinal ranked below it before, and an unranked ordinal of exactly that rank lies
   * in the same gap as the value, so only its value tells. The value need not be in the dictionary. 0 when the dictionary has no rank
   * view (every ordinal then ranks 0, and the value decides).
   */
  public static int rankAfter(final GlobalDictionary dictionary, final Object value) {
    final int[] sorted = dictionary.sortedOrdinals();
    if (sorted.length == 0)
      return 0;
    // a value of a class the dictionary cannot hold sorts after every value it holds (FacetValueComparator)
    if (value == null || !DictionaryValueCodec.isSupported(value))
      return 2 * sorted.length;
    return 2 * firstGreater(dictionary, sorted, value);
  }

  /** The first position of the view whose value is strictly greater than {@code value}, {@code sorted.length} when none is. */
  private static int firstGreater(final GlobalDictionary dictionary, final int[] sorted, final Object value) {
    int low = 0;
    int high = sorted.length;
    while (low < high) {
      final int mid = (low + high) >>> 1;
      if (DictionaryValueCodec.compareValues(dictionary.valueOf(sorted[mid]), value) <= 0)
        low = mid + 1;
      else
        high = mid;
    }
    return low;
  }

  /** The first position of the view whose value is at least {@code value}, {@code sorted.length} when none is. */
  private static int firstAtLeast(final GlobalDictionary dictionary, final int[] sorted, final Object value) {
    int low = 0;
    int high = sorted.length;
    while (low < high) {
      final int mid = (low + high) >>> 1;
      if (DictionaryValueCodec.compareValues(dictionary.valueOf(sorted[mid]), value) < 0)
        low = mid + 1;
      else
        high = mid;
    }
    return low;
  }

  /**
   * The ordinals newer than the view (from {@code viewSize} to the size now) whose value starts with the prefix and is greater than
   * {@code afterOrNull}, sorted by value. Bounded by the growth since the last rank-view rebuild.
   */
  private static int[] tailInRange(final GlobalDictionary dictionary, final int viewSize, final String prefix, final String afterOrNull) {
    final int size = dictionary.size();
    int[] matches = new int[Math.min(16, Math.max(0, size - viewSize))];
    int n = 0;
    for (int ordinal = viewSize; ordinal < size; ordinal++) {
      final Object value = dictionary.valueOf(ordinal);
      if (!startsWith(value, prefix) || afterOrNull != null && ((String) value).compareTo(afterOrNull) <= 0)
        continue;
      if (n == matches.length)
        matches = Arrays.copyOf(matches, Math.max(16, n * 2));
      matches[n++] = ordinal;
    }
    final int[] tail = n == matches.length ? matches : Arrays.copyOf(matches, n);
    sortByValue(dictionary, tail, new int[n], 0, n);
    return tail;
  }

  private static boolean startsWith(final Object value, final String prefix) {
    return value instanceof String string && string.startsWith(prefix);
  }

  /** Two distinct ordinals' values: the dictionary holds each value once, so they never tie. */
  private static int compare(final GlobalDictionary dictionary, final int a, final int b) {
    return DictionaryValueCodec.compareValues(dictionary.valueOf(a), dictionary.valueOf(b));
  }

  /** A stable merge sort of {@code ordinals[from, to)} by value, with {@code temp} as scratch. */
  private static void sortByValue(final GlobalDictionary dictionary, final int[] ordinals, final int[] temp, final int from,
      final int to) {
    if (to - from < 2)
      return;
    final int mid = (from + to) >>> 1;
    sortByValue(dictionary, ordinals, temp, from, mid);
    sortByValue(dictionary, ordinals, temp, mid, to);
    if (compare(dictionary, ordinals[mid - 1], ordinals[mid]) <= 0)
      return;
    System.arraycopy(ordinals, from, temp, from, to - from);
    int i = from;
    int j = mid;
    for (int k = from; k < to; k++)
      ordinals[k] = j >= to || i < mid && compare(dictionary, temp[i], temp[j]) <= 0 ? temp[i++] : temp[j++];
  }
}
