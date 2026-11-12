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
 * Record identities held as the two primitives a {@link RID} is made of rather than as a {@code List<RID>}.
 * <p>
 * The materialized view refreshes allocate these on every pass, proportional to the view's size (a full pass) or to the records a
 * commit changed (a per-record pass), and a boxed RID per entry costs several times what the pair of primitives does - a
 * reference plus an object header plus the same two fields, against 12 bytes. Identities are rebuilt one at a time by
 * {@link #get(int)} as the caller consumes them, so at most one is ever live, and each is handed straight to
 * {@code lookupByRID} rather than to {@code RID.asDocument()}: the latter resolves the database from the thread's
 * {@link com.arcadedb.database.DatabaseContext}, which a refresh has no reason to depend on.
 * <p>
 * Not thread-safe.
 */
final class RecordIdentities {
  private int[]  bucketIds;
  private long[] positions;
  private int    size;

  RecordIdentities(final int initialCapacity) {
    final int capacity = Math.max(initialCapacity, 1);
    this.bucketIds = new int[capacity];
    this.positions = new long[capacity];
  }

  void add(final RID rid) {
    add(rid.getBucketId(), rid.getPosition());
  }

  void add(final int bucketId, final long position) {
    if (size == bucketIds.length)
      grow(size + 1);
    bucketIds[size] = bucketId;
    positions[size] = position;
    ++size;
  }

  void addAll(final RecordIdentities other) {
    if (other.size == 0)
      return;
    if (size + other.size > bucketIds.length)
      grow(size + other.size);
    System.arraycopy(other.bucketIds, 0, bucketIds, size, other.size);
    System.arraycopy(other.positions, 0, positions, size, other.size);
    size += other.size;
  }

  int size() {
    return size;
  }

  RID get(final int index) {
    return new RID(bucketIds[index], positions[index]);
  }

  void clear() {
    size = 0;
  }

  /**
   * Sorts by (bucket id, position) and drops duplicates, after which {@link #contains} answers by binary search. The JDK sorts
   * cannot move two parallel arrays together without boxing the pairs, so they are sorted in place with a primitive quicksort that
   * finishes short ranges with an insertion sort.
   */
  void sortAndDeduplicate() {
    if (size < 2)
      return;
    sort(0, size - 1);
    int last = 0;
    for (int i = 1; i < size; i++)
      if (bucketIds[i] != bucketIds[last] || positions[i] != positions[last]) {
        ++last;
        bucketIds[last] = bucketIds[i];
        positions[last] = positions[i];
      }
    size = last + 1;
  }

  /** Binary search; valid only after {@link #sortAndDeduplicate()} and before any further {@code add}. */
  boolean contains(final int bucketId, final long position) {
    int low = 0;
    int high = size - 1;
    while (low <= high) {
      final int mid = (low + high) >>> 1;
      final int cmp = compare(bucketIds[mid], positions[mid], bucketId, position);
      if (cmp < 0)
        low = mid + 1;
      else if (cmp > 0)
        high = mid - 1;
      else
        return true;
    }
    return false;
  }

  boolean contains(final RID rid) {
    return contains(rid.getBucketId(), rid.getPosition());
  }

  private void grow(final int minCapacity) {
    final int grown = Math.max(minCapacity, size + (size >> 1) + 1);
    bucketIds = Arrays.copyOf(bucketIds, grown);
    positions = Arrays.copyOf(positions, grown);
  }

  private static int compare(final int bucketA, final long positionA, final int bucketB, final long positionB) {
    if (bucketA != bucketB)
      return Integer.compare(bucketA, bucketB);
    return Long.compare(positionA, positionB);
  }

  private void sort(int low, int high) {
    while (high - low > 16) {
      // median of three as pivot, then Hoare partition
      final int mid = (low + high) >>> 1;
      if (compareAt(mid, low) < 0)
        swap(mid, low);
      if (compareAt(high, low) < 0)
        swap(high, low);
      if (compareAt(high, mid) < 0)
        swap(high, mid);
      final int pivotBucket = bucketIds[mid];
      final long pivotPosition = positions[mid];

      int i = low;
      int j = high;
      while (i <= j) {
        while (compare(bucketIds[i], positions[i], pivotBucket, pivotPosition) < 0)
          ++i;
        while (compare(bucketIds[j], positions[j], pivotBucket, pivotPosition) > 0)
          --j;
        if (i <= j) {
          swap(i, j);
          ++i;
          --j;
        }
      }
      // recurse on the smaller side, loop on the larger: the stack stays O(log n)
      if (j - low < high - i) {
        sort(low, j);
        low = i;
      } else {
        sort(i, high);
        high = j;
      }
    }
    for (int i = low + 1; i <= high; i++) {
      final int bucket = bucketIds[i];
      final long position = positions[i];
      int j = i - 1;
      while (j >= low && compare(bucketIds[j], positions[j], bucket, position) > 0) {
        bucketIds[j + 1] = bucketIds[j];
        positions[j + 1] = positions[j];
        --j;
      }
      bucketIds[j + 1] = bucket;
      positions[j + 1] = position;
    }
  }

  private int compareAt(final int a, final int b) {
    return compare(bucketIds[a], positions[a], bucketIds[b], positions[b]);
  }

  private void swap(final int a, final int b) {
    final int bucket = bucketIds[a];
    bucketIds[a] = bucketIds[b];
    bucketIds[b] = bucket;
    final long position = positions[a];
    positions[a] = positions[b];
    positions[b] = position;
  }
}
