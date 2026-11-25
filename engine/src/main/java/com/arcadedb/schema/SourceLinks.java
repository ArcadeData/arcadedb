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
package com.arcadedb.schema;

import com.arcadedb.database.RID;

import java.util.Arrays;

/**
 * The rows of a per-record materialized view paired with the source record each one links to, read in ONE scan of the backing
 * type and held as four parallel primitive arrays sorted by source (issue #11551). The full pass then pairs every query row
 * with its view row by a binary search over these arrays, where it used to descend the {@code _sourceRID} UNIQUE index once per
 * row, and the pairs no query row claimed are the orphans, where it used to scan the backing type a second time to find them.
 * <p>
 * 24 bytes per row plus one bit for the match mark, against a boxed {@code RID} pair and a map entry per row for an object map.
 * <p>
 * Not thread-safe.
 */
final class SourceLinks {
  private int[]  sourceBuckets;
  private long[] sourcePositions;
  private int[]  rowBuckets;
  private long[] rowPositions;
  private long[] matched;
  private int    size;

  SourceLinks(final int initialCapacity) {
    final int capacity = Math.max(initialCapacity, 1);
    sourceBuckets = new int[capacity];
    sourcePositions = new long[capacity];
    rowBuckets = new int[capacity];
    rowPositions = new long[capacity];
  }

  void add(final RID source, final RID row) {
    if (size == sourceBuckets.length) {
      final int grown = size + (size >> 1) + 1;
      sourceBuckets = Arrays.copyOf(sourceBuckets, grown);
      sourcePositions = Arrays.copyOf(sourcePositions, grown);
      rowBuckets = Arrays.copyOf(rowBuckets, grown);
      rowPositions = Arrays.copyOf(rowPositions, grown);
    }
    sourceBuckets[size] = source.getBucketId();
    sourcePositions[size] = source.getPosition();
    rowBuckets[size] = row.getBucketId();
    rowPositions[size] = row.getPosition();
    ++size;
  }

  int size() {
    return size;
  }

  /**
   * Sorts the pairs by source, ties (two rows linking one source, which the UNIQUE index forbids) by row, and allocates the match
   * marks. Call once, after the last {@link #add}.
   */
  void sort() {
    if (size > 1)
      sort(0, size - 1);
    matched = new long[(size + 63) >>> 6];
  }

  /**
   * The first pair of {@code source} not matched yet, marked matched, or the one already matched when every pair of it is (a
   * second query row of the same source writes over the same row, as the index lookup did); {@code -1} when no row links it.
   * Valid only after {@link #sort()}.
   */
  int match(final RID source) {
    final int bucketId = source.getBucketId();
    final long position = source.getPosition();
    // leftmost pair of the source
    int low = 0;
    int high = size;
    while (low < high) {
      final int mid = (low + high) >>> 1;
      if (compare(sourceBuckets[mid], sourcePositions[mid], bucketId, position) < 0)
        low = mid + 1;
      else
        high = mid;
    }
    if (low == size || sourceBuckets[low] != bucketId || sourcePositions[low] != position)
      return -1;

    for (int i = low; i < size && sourceBuckets[i] == bucketId && sourcePositions[i] == position; i++)
      if (!isMatched(i)) {
        matched[i >>> 6] |= 1L << i;
        return i;
      }
    return low;
  }

  boolean isMatched(final int index) {
    return (matched[index >>> 6] & (1L << index)) != 0;
  }

  RID row(final int index) {
    return new RID(rowBuckets[index], rowPositions[index]);
  }

  static int compare(final int bucketA, final long positionA, final int bucketB, final long positionB) {
    if (bucketA != bucketB)
      return Integer.compare(bucketA, bucketB);
    return Long.compare(positionA, positionB);
  }

  private int compareAt(final int a, final int b) {
    final int bySource = compare(sourceBuckets[a], sourcePositions[a], sourceBuckets[b], sourcePositions[b]);
    return bySource != 0 ? bySource : compare(rowBuckets[a], rowPositions[a], rowBuckets[b], rowPositions[b]);
  }

  /**
   * The primitive quicksort of {@link RecordIdentities}, over four arrays: median of three, Hoare partition on a pivot index kept
   * in place by swapping it to {@code high}, recursion on the smaller side, insertion sort for short ranges.
   */
  private void sort(int low, int high) {
    while (high - low > 16) {
      final int mid = (low + high) >>> 1;
      if (compareAt(mid, low) < 0)
        swap(mid, low);
      if (compareAt(high, low) < 0)
        swap(high, low);
      if (compareAt(high, mid) < 0)
        swap(high, mid);
      // the median is now at mid and is <= high: park it at high - 1 and partition (low, high - 1)
      swap(mid, high - 1);
      final int pivot = high - 1;
      int i = low;
      int j = high - 1;
      while (true) {
        while (compareAt(++i, pivot) < 0) {
        }
        while (compareAt(--j, pivot) > 0) {
        }
        if (i >= j)
          break;
        swap(i, j);
      }
      swap(i, pivot);
      // [low, i - 1] <= pivot, [i + 1, high] >= pivot
      if (i - low < high - i) {
        sort(low, i - 1);
        low = i + 1;
      } else {
        sort(i + 1, high);
        high = i - 1;
      }
    }
    for (int i = low + 1; i <= high; i++)
      for (int j = i; j > low && compareAt(j - 1, j) > 0; j--)
        swap(j - 1, j);
  }

  private void swap(final int a, final int b) {
    final int sourceBucket = sourceBuckets[a];
    sourceBuckets[a] = sourceBuckets[b];
    sourceBuckets[b] = sourceBucket;
    final long sourcePosition = sourcePositions[a];
    sourcePositions[a] = sourcePositions[b];
    sourcePositions[b] = sourcePosition;
    final int rowBucket = rowBuckets[a];
    rowBuckets[a] = rowBuckets[b];
    rowBuckets[b] = rowBucket;
    final long rowPosition = rowPositions[a];
    rowPositions[a] = rowPositions[b];
    rowPositions[b] = rowPosition;
  }
}
