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
package com.arcadedb.index.sparsevector;

import com.arcadedb.database.RID;

import java.util.Arrays;
import java.util.List;

/**
 * Bounded min-heap of {@code (RID, score)} pairs, ordered by score, backed by two parallel arrays.
 * The heap of the current best {@code k} results is the accumulator of every sparse-vector top-K
 * query, so it sits directly on the query hot path.
 * <p>
 * <b>Why not {@link java.util.PriorityQueue}.</b> The previous spelling was
 * {@code new PriorityQueue<>(k, Comparator.comparing(RidScore::score))}, which allocates on two
 * axes - though not equally, and issue #5473 named the cheaper one:
 * <ul>
 *   <li><i>Boxed comparison keys.</i> {@code Comparator.comparing} is typed
 *       {@code <T, U extends Comparable<? super U>>}, so the {@code float} returned by
 *       {@code RidScore::score} is autoboxed into a {@code java.lang.Float} for <i>both</i> operands
 *       of <i>every</i> comparison, and a heap runs O(log k) comparisons per insertion. Real at the
 *       bytecode level, but the boxes never escape the comparator, so C2 scalar-replaces them: with
 *       escape analysis on (i.e. always, in production) the measured cost of this is <b>zero</b>,
 *       and swapping in {@code Comparator.comparingDouble} measures as no change at all. Only with
 *       {@code -XX:-DoEscapeAnalysis} does it show up, at ~350 bytes per replacement.</li>
 *   <li><i>The heap element itself.</i> A {@code PriorityQueue} stores object references, so every
 *       accepted candidate costs one {@link RidScore}, and that one <b>does</b> escape - into the
 *       queue's backing array - so no JIT pass can remove it. This is the allocation that actually
 *       survives compilation: a measured 24 bytes per accepted candidate.</li>
 * </ul>
 * Storing the score in a {@code float[]} beside the RID reference removes both axes at once, the
 * surviving one included: comparisons read two primitives out of arrays that stay in L1 for any
 * realistic {@code k}, and the only {@link RidScore} objects ever created are the ones handed to
 * the caller by {@link #drainInto(List)}. Measured against the {@code PriorityQueue} it replaces,
 * on a fully-accepting stream: 24 -> 0 bytes per accepted candidate, and 24 -> 18 ns/op at
 * {@code k = 64}, 55 -> 34 ns/op at {@code k = 1000}.
 * <p>
 * <b>Ordering contract.</b> Comparisons use {@link Float#compare}, so the total order (including
 * the placement of {@code NaN} above every number, and {@code -0.0f} below {@code 0.0f}) is
 * identical to what {@code Comparator.comparing(RidScore::score)} produced. Admission of a new
 * candidate against a full heap uses the primitive {@code >} test, matching the pruning-threshold
 * comparison used by the callers.
 * <p>
 * <b>Capacity.</b> {@code capacity} is the hard bound on the number of retained entries; the
 * backing arrays start small and grow towards it on demand, so a query asking for a large
 * {@code k} (or a large {@code groupSize}) that ends up matching few documents does not pay for the
 * full-size arrays.
 * <p>
 * Not thread-safe: a heap instance belongs to a single query traversal.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class RidScoreMinHeap {
  private static final int INITIAL_CAPACITY = 16;

  private final int     capacity;
  private       RID[]   rids;
  private       float[] scores;
  private       int     size;

  RidScoreMinHeap(final int capacity) {
    if (capacity <= 0)
      throw new IllegalArgumentException("capacity must be positive; got " + capacity);
    this.capacity = capacity;
    final int initial = Math.min(capacity, INITIAL_CAPACITY);
    this.rids = new RID[initial];
    this.scores = new float[initial];
  }

  int size() {
    return size;
  }

  boolean isEmpty() {
    return size == 0;
  }

  boolean isFull() {
    return size == capacity;
  }

  /**
   * The lowest score currently retained, i.e. the score a new candidate has to beat once the heap is
   * full. Only meaningful when the heap is not empty.
   */
  float minScore() {
    return scores[0];
  }

  /**
   * Offers a candidate. While the heap is below capacity the candidate is always retained; once
   * full it replaces the current minimum only if it is strictly greater than it.
   *
   * @return {@code true} if the heap's contents changed.
   */
  boolean offer(final RID rid, final float score) {
    if (size < capacity) {
      if (size == rids.length)
        grow();
      rids[size] = rid;
      scores[size] = score;
      siftUp(size++);
      return true;
    }
    if (score > scores[0]) {
      rids[0] = rid;
      scores[0] = score;
      siftDown(0);
      return true;
    }
    return false;
  }

  /** Appends the retained pairs to {@code out} in unspecified order. */
  void drainInto(final List<RidScore> out) {
    for (int i = 0; i < size; i++)
      out.add(new RidScore(rids[i], scores[i]));
  }

  private void siftUp(int i) {
    final RID rid = rids[i];
    final float score = scores[i];
    while (i > 0) {
      final int parent = (i - 1) >>> 1;
      if (Float.compare(score, scores[parent]) >= 0)
        break;
      rids[i] = rids[parent];
      scores[i] = scores[parent];
      i = parent;
    }
    rids[i] = rid;
    scores[i] = score;
  }

  private void siftDown(int i) {
    final RID rid = rids[i];
    final float score = scores[i];
    final int half = size >>> 1;
    while (i < half) {
      int child = (i << 1) + 1;
      final int right = child + 1;
      if (right < size && Float.compare(scores[right], scores[child]) < 0)
        child = right;
      if (Float.compare(score, scores[child]) <= 0)
        break;
      rids[i] = rids[child];
      scores[i] = scores[child];
      i = child;
    }
    rids[i] = rid;
    scores[i] = score;
  }

  private void grow() {
    final int newLength = Math.min(capacity, rids.length + (rids.length >> 1) + 1);
    rids = Arrays.copyOf(rids, newLength);
    scores = Arrays.copyOf(scores, newLength);
  }
}
