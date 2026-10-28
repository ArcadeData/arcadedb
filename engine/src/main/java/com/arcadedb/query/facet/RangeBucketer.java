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

import java.util.Arrays;

/**
 * Counts values into half-open, possibly overlapping ranges {@code [from, to)} through elementary intervals: the sorted distinct
 * bounds cut the number line in {@code b + 1} intervals, each wholly inside or outside every range, so a value costs one binary
 * search and a range count is a difference of two prefix sums. A segment whose {@code [min, max]} lies in one interval can be
 * counted by its cardinality with {@link #addInterval} without decoding it.
 * <p>
 * Two exact modes: the {@code long} constructor for LONG and DATETIME columns (no rounding above 2^53, e.g. nanosecond timestamps),
 * driven by {@link #interval(long)}, {@link #add(long)} and {@link #singleIntervalOf(long, long)}; the {@code double} constructor
 * for DOUBLE columns (a NaN bound is unbounded), driven by the double overloads. Calling the other mode's methods throws.
 * <p>
 * Not thread safe: one instance per facet per query thread.
 */
public final class RangeBucketer {
  private final double[]  doubleBounds;
  private final long[]    longBounds;
  private final int[]     firstInterval;
  private final int[]     endInterval;
  private final boolean[] covered;
  private final long[]    counters;
  private       long[]    prefix;
  private       long      nanCount;
  private       boolean   dirty = true;

  /** @param from inclusive lower bound of each range, NaN for none; @param to exclusive upper bound of each range, NaN for none */
  public RangeBucketer(final double[] from, final double[] to) {
    if (from.length != to.length)
      throw new IllegalArgumentException("from and to must have the same length");
    final double[] all = new double[from.length * 2];
    int n = 0;
    for (int i = 0; i < from.length; i++) {
      if (!Double.isNaN(from[i]))
        all[n++] = from[i];
      if (!Double.isNaN(to[i]))
        all[n++] = to[i];
    }
    Arrays.sort(all, 0, n);
    int distinct = 0;
    for (int i = 0; i < n; i++)
      if (distinct == 0 || Double.compare(all[distinct - 1], all[i]) != 0)
        all[distinct++] = all[i];
    doubleBounds = Arrays.copyOf(all, distinct);
    longBounds = null;

    final int[] fromIndex = new int[from.length];
    final int[] toIndex = new int[from.length];
    for (int r = 0; r < from.length; r++) {
      fromIndex[r] = Double.isNaN(from[r]) ? -1 : Arrays.binarySearch(doubleBounds, from[r]);
      toIndex[r] = Double.isNaN(to[r]) ? -1 : Arrays.binarySearch(doubleBounds, to[r]);
    }
    final int intervals = distinct + 1;
    counters = new long[intervals];
    covered = new boolean[intervals];
    firstInterval = new int[from.length];
    endInterval = new int[from.length];
    layout(fromIndex, toIndex, intervals);
  }

  /**
   * Exact long ranges.
   *
   * @param fromUnbounded true where the range has no lower bound (the matching {@code from} entry is then ignored)
   * @param toUnbounded   true where the range has no upper bound (the matching {@code to} entry is then ignored)
   */
  public RangeBucketer(final long[] from, final long[] to, final boolean[] fromUnbounded, final boolean[] toUnbounded) {
    if (from.length != to.length || from.length != fromUnbounded.length || from.length != toUnbounded.length)
      throw new IllegalArgumentException("from, to, fromUnbounded and toUnbounded must have the same length");
    final long[] all = new long[from.length * 2];
    int n = 0;
    for (int i = 0; i < from.length; i++) {
      if (!fromUnbounded[i])
        all[n++] = from[i];
      if (!toUnbounded[i])
        all[n++] = to[i];
    }
    Arrays.sort(all, 0, n);
    int distinct = 0;
    for (int i = 0; i < n; i++)
      if (distinct == 0 || all[distinct - 1] != all[i])
        all[distinct++] = all[i];
    longBounds = Arrays.copyOf(all, distinct);
    doubleBounds = null;

    final int[] fromIndex = new int[from.length];
    final int[] toIndex = new int[from.length];
    for (int r = 0; r < from.length; r++) {
      fromIndex[r] = fromUnbounded[r] ? -1 : Arrays.binarySearch(longBounds, from[r]);
      toIndex[r] = toUnbounded[r] ? -1 : Arrays.binarySearch(longBounds, to[r]);
    }
    final int intervals = distinct + 1;
    counters = new long[intervals];
    covered = new boolean[intervals];
    firstInterval = new int[from.length];
    endInterval = new int[from.length];
    layout(fromIndex, toIndex, intervals);
  }

  /** The elementary interval of the value: the number of bounds not above it. NaN has no interval, see {@link #add(double)}. */
  public int interval(final double v) {
    requireDouble();
    int lo = 0;
    int hi = doubleBounds.length;
    while (lo < hi) {
      final int mid = (lo + hi) >>> 1;
      if (doubleBounds[mid] <= v)
        lo = mid + 1;
      else
        hi = mid;
    }
    return lo;
  }

  /** The elementary interval of the value in a long bucketer: the number of bounds not above it. */
  public int interval(final long v) {
    requireLong();
    int lo = 0;
    int hi = longBounds.length;
    while (lo < hi) {
      final int mid = (lo + hi) >>> 1;
      if (longBounds[mid] <= v)
        lo = mid + 1;
      else
        hi = mid;
    }
    return lo;
  }

  /** Counts one value. NaN belongs to no range, so it counts as a value in no range. */
  public void add(final double v) {
    if (Double.isNaN(v))
      nanCount++;
    else
      counters[interval(v)]++;
    dirty = true;
  }

  /** Counts one value of a long bucketer. */
  public void add(final long v) {
    counters[interval(v)]++;
    dirty = true;
  }

  /** Counts {@code n} values known to lie in the interval (see {@link #singleIntervalOf}). */
  public void addInterval(final int interval, final long n) {
    counters[interval] += n;
    dirty = true;
  }

  /** The interval holding both {@code min} and {@code max}, or -1 when {@code [min, max]} spans more than one (or is not ordered). */
  public int singleIntervalOf(final double min, final double max) {
    if (Double.isNaN(min) || Double.isNaN(max) || min > max)
      return -1;
    final int interval = interval(min);
    return interval(max) == interval ? interval : -1;
  }

  /** The interval holding both {@code min} and {@code max} of a long bucketer, or -1 when {@code [min, max]} spans more than one. */
  public int singleIntervalOf(final long min, final long max) {
    if (min > max)
      return -1;
    final int interval = interval(min);
    return interval(max) == interval ? interval : -1;
  }

  /** The number of values counted in the range. */
  public long rangeCount(final int range) {
    refresh();
    final int first = firstInterval[range];
    final int end = endInterval[range];
    return end > first ? prefix[end] - prefix[first] : 0;
  }

  /** The number of values counted that lie in no range (NaN included). */
  public long defaultCount() {
    long n = nanCount;
    for (int i = 0; i < counters.length; i++)
      if (!covered[i])
        n += counters[i];
    return n;
  }

  /** @param fromIndex index of the range's lower bound among the bounds, -1 when unbounded; @param toIndex the same for the upper bound */
  private void layout(final int[] fromIndex, final int[] toIndex, final int intervals) {
    for (int r = 0; r < fromIndex.length; r++) {
      firstInterval[r] = fromIndex[r] < 0 ? 0 : fromIndex[r] + 1;
      endInterval[r] = toIndex[r] < 0 ? intervals : toIndex[r] + 1;
      for (int i = firstInterval[r]; i < endInterval[r]; i++)
        covered[i] = true;
    }
  }

  private void refresh() {
    if (!dirty)
      return;
    if (prefix == null)
      prefix = new long[counters.length + 1];
    for (int i = 0; i < counters.length; i++)
      prefix[i + 1] = prefix[i] + counters[i];
    dirty = false;
  }

  private void requireDouble() {
    if (doubleBounds == null)
      throw new IllegalStateException("This bucketer was built with long bounds: use the long methods");
  }

  private void requireLong() {
    if (longBounds == null)
      throw new IllegalStateException("This bucketer was built with double bounds: use the double methods");
  }
}
