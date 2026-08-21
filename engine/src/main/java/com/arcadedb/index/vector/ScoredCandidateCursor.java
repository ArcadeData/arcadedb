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
package com.arcadedb.index.vector;

import java.util.NoSuchElementException;

/**
 * Ascending-distance cursor over a fixed set of already-scored candidates, used to merge the delta buffer into the
 * grouped vector search's candidate stream (issue #6501).
 * <p>
 * <b>Why a cursor and not a top-k heap.</b> {@code findNeighborsFromVector}'s delta scan keeps only the k best
 * candidates, which is exact because its only consumer takes the k nearest rows and nothing else. The grouped search
 * cannot bound the set that way: {@link GroupAdmissionState} rejects a row whose group is already full, so an
 * arbitrary number of near candidates can be skipped before a far one is admitted. With {@code limit=2},
 * {@code groupSize=1} and a thousand delta rows in one group, the row that fills the second group is the
 * thousand-and-first by rank - a top-{@code limit * groupSize} heap would have thrown it away. So the whole set stays
 * addressable, and the cap decides how far into it the query actually walks.
 * <p>
 * <b>Why a heap and not a sort.</b> Ordering the whole set costs {@code O(n log n)} up front, of which a query that
 * fills its groups from the first handful of candidates uses almost nothing. Heapifying is {@code O(n)} and each
 * {@link #poll()} is {@code O(log n)}, so the common case - a few candidates drained out of a large delta buffer -
 * pays for what it takes, and the pathological case is no worse than the sort would have been.
 * <p>
 * The two parallel arrays are heapsorted in place and owned by the cursor from construction on: the caller must not
 * read or write them afterwards. Primitive arrays rather than a {@code PriorityQueue} of pairs because the delta
 * buffer routinely runs to tens of thousands of entries, and boxing every one of them per query is exactly the
 * garbage the delta path was already tuned to avoid (issue #5391).
 * <p>
 * Lifetime is one query. Not thread-safe.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class ScoredCandidateCursor {
  private final float[] distance;
  private final int[]   payload;
  private       int     size;

  /**
   * Takes ownership of both arrays and arranges the first {@code size} entries as a min-heap on {@code distance}.
   * Entries past {@code size} are ignored, so the caller can over-allocate and compact as it scores.
   *
   * @param distance distance of each candidate; reordered in place
   * @param payload  caller-defined id of each candidate, moved in lockstep with its distance
   * @param size     number of live entries at the head of both arrays
   */
  ScoredCandidateCursor(final float[] distance, final int[] payload, final int size) {
    if (size < 0 || size > distance.length || size > payload.length)
      throw new IllegalArgumentException(
          "size " + size + " is not addressable in arrays of " + distance.length + "/" + payload.length);
    this.distance = distance;
    this.payload = payload;
    this.size = size;
    for (int i = (size >>> 1) - 1; i >= 0; i--)
      siftDown(i);
  }

  boolean isEmpty() {
    return size == 0;
  }

  int size() {
    return size;
  }

  /**
   * Distance of the nearest candidate not yet polled. O(1).
   */
  float peekDistance() {
    if (size == 0)
      throw new NoSuchElementException("Scored candidate cursor is exhausted");
    return distance[0];
  }

  /**
   * Removes the nearest candidate and returns its payload. O(log n).
   */
  int poll() {
    if (size == 0)
      throw new NoSuchElementException("Scored candidate cursor is exhausted");
    final int head = payload[0];
    if (--size > 0) {
      distance[0] = distance[size];
      payload[0] = payload[size];
      siftDown(0);
    }
    return head;
  }

  /**
   * Standard binary-heap sift-down, hoisting the displaced entry rather than swapping at every level: one write per
   * level instead of three.
   */
  private void siftDown(int slot) {
    final float movedDistance = distance[slot];
    final int movedPayload = payload[slot];
    final int firstLeaf = size >>> 1;
    while (slot < firstLeaf) {
      int child = (slot << 1) + 1;
      final int right = child + 1;
      if (right < size && distance[right] < distance[child])
        child = right;
      if (distance[child] >= movedDistance)
        break;
      distance[slot] = distance[child];
      payload[slot] = payload[child];
      slot = child;
    }
    distance[slot] = movedDistance;
    payload[slot] = movedPayload;
  }
}
