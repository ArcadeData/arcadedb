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

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The essential-term heap discipline of {@link BmwScorer}, pinned directly against hand-built heaps
 * rather than through a query.
 * <p>
 * Issue #5467 replaced "pop the whole aligned run, move the cursors, push the survivors back" with
 * "find the run without touching the heap, move the cursors, repair each moved slot in place". That
 * is a genuinely subtle exchange, and it rests on three claims that a top-K comparison against
 * {@link BruteForceScorer} can only confirm indirectly - and only for whatever heap shapes the
 * corpus generator happens to produce:
 * <ol>
 *   <li>the slots holding the heap minimum form a connected subtree rooted at slot 0, so a
 *       breadth-first walk that stops at the first non-matching child finds <i>all</i> of them;</li>
 *   <li>the smallest key outside that run is always held by a non-run child of a run slot;</li>
 *   <li>repairing the moved slots deepest-first with a Floyd sift restores the heap property, given
 *       every moved key only ever grew.</li>
 * </ol>
 * Each is checked here against an independent brute-force answer over a random heap, so a broken
 * invariant fails with a small counterexample instead of as a top-K mismatch on a 25k-document
 * corpus. The randomised cases carry the coverage; the fixed ones nail down the shapes a random
 * generator produces too rarely to be relied on (a one-element heap, a heap that is entirely one
 * run, a run that stops at one child but not the other).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class BmwScorerHeapRepairTest {

  @Test
  void theRunIsExactlyTheSlotsHoldingTheMinimumAndTheFrontierIsTheNextKeyAfterIt() {
    final Random rnd = new Random(5467L);
    for (int iteration = 0; iteration < 3_000; iteration++) {
      final int n = 1 + rnd.nextInt(64);
      // A small key space on purpose: ties are the whole point, and a wide one would make the run a
      // single slot almost every time.
      final int[] keyBucketIds = new int[n];
      final long[] keyPositions = new long[n];
      for (int i = 0; i < n; i++) {
        keyBucketIds[i] = rnd.nextInt(2);
        keyPositions[i] = rnd.nextInt(6);
      }
      final int[] heap = heapOfAllTerms(keyBucketIds, keyPositions, n);

      final int[] aligned = new int[n];
      final int[] alignedSlots = new int[n];
      final int alignedCount = BmwScorer.collectAlignedRun(keyBucketIds, keyPositions, heap, n, keyBucketIds[heap[0]],
          keyPositions[heap[0]], aligned, alignedSlots);

      assertRunMatchesBruteForce(keyBucketIds, keyPositions, heap, n, aligned, alignedSlots, alignedCount);

      final long[] frontier = new long[2];
      BmwScorer.nextEssentialKey(keyBucketIds, keyPositions, heap, n, alignedSlots, alignedCount, keyBucketIds[heap[0]],
          keyPositions[heap[0]], frontier);
      assertFrontierMatchesBruteForce(keyBucketIds, keyPositions, heap, n, alignedSlots, alignedCount, frontier);
    }
  }

  @Test
  void repairingTheMovedSlotsDeepestFirstRestoresTheHeap() {
    final Random rnd = new Random(4085L);
    for (int iteration = 0; iteration < 3_000; iteration++) {
      final int n = 1 + rnd.nextInt(64);
      final int[] keyBucketIds = new int[n];
      final long[] keyPositions = new long[n];
      for (int i = 0; i < n; i++) {
        keyBucketIds[i] = rnd.nextInt(2);
        keyPositions[i] = rnd.nextInt(6);
      }
      final int[] heap = heapOfAllTerms(keyBucketIds, keyPositions, n);
      final int[] before = heap.clone();

      final int[] aligned = new int[n];
      final int[] alignedSlots = new int[n];
      final int alignedCount = BmwScorer.collectAlignedRun(keyBucketIds, keyPositions, heap, n, keyBucketIds[heap[0]],
          keyPositions[heap[0]], aligned, alignedSlots);

      // Every cursor in the run advances, so its key only ever grows. That is the precondition the
      // in-place repair relies on, and the traversal guarantees it: a cursor either moves forward or
      // is dropped from the heap entirely.
      for (int j = 0; j < alignedCount; j++) {
        final int term = aligned[j];
        keyPositions[term] += 1 + rnd.nextInt(8);
        if (rnd.nextInt(4) == 0)
          keyBucketIds[term] += 1;
      }

      for (int j = alignedCount - 1; j >= 0; j--)
        BmwScorer.siftDownFromFloyd(keyBucketIds, keyPositions, heap, n, alignedSlots[j]);

      assertIsMinHeap(keyBucketIds, keyPositions, heap, n);
      assertThat(sorted(heap, n))
          .as("the repair must permute the heap, never lose or duplicate a term")
          .isEqualTo(sorted(before, n));
    }
  }

  /**
   * A repaired heap must keep answering "what is the smallest key" correctly for the rest of the
   * traversal, not merely satisfy the parent-child ordering once. This drains one, repeatedly, and
   * checks the minima come out non-decreasing.
   */
  @Test
  void aRepairedHeapKeepsHandingBackTheMinimumInOrder() {
    final Random rnd = new Random(5518L);
    for (int iteration = 0; iteration < 400; iteration++) {
      final int n = 1 + rnd.nextInt(48);
      final int[] keyBucketIds = new int[n];
      final long[] keyPositions = new long[n];
      for (int i = 0; i < n; i++) {
        keyBucketIds[i] = 0;
        keyPositions[i] = rnd.nextInt(10);
      }
      final int[] heap = heapOfAllTerms(keyBucketIds, keyPositions, n);
      final int[] aligned = new int[n];
      final int[] alignedSlots = new int[n];

      int lastBucketId = -1;
      long lastPosition = -1L;
      for (int step = 0; step < 200; step++) {
        final int candidateBucketId = keyBucketIds[heap[0]];
        final long candidatePosition = keyPositions[heap[0]];
        assertThat(SparseSegmentBuilder.compareRid(candidateBucketId, candidatePosition, lastBucketId, lastPosition))
            .as("candidates must come out in non-decreasing order")
            .isGreaterThanOrEqualTo(0);
        assertThat(candidatePosition).isEqualTo(bruteForceMinPosition(keyPositions, heap, n));
        lastBucketId = candidateBucketId;
        lastPosition = candidatePosition;

        final int alignedCount = BmwScorer.collectAlignedRun(keyBucketIds, keyPositions, heap, n, candidateBucketId,
            candidatePosition, aligned, alignedSlots);
        for (int j = 0; j < alignedCount; j++)
          keyPositions[aligned[j]] += 1 + rnd.nextInt(3);
        for (int j = alignedCount - 1; j >= 0; j--)
          BmwScorer.siftDownFromFloyd(keyBucketIds, keyPositions, heap, n, alignedSlots[j]);
        assertIsMinHeap(keyBucketIds, keyPositions, heap, n);
      }
    }
  }

  /** Shapes a random generator produces too rarely to lean on. */
  @Test
  void theFixedShapesBehave() {
    // One element: the run is the root and there is no frontier.
    assertShape(new long[] { 7 }, 1, 1, -1L);
    // Two elements, both on the minimum: the whole heap is one run.
    assertShape(new long[] { 3, 3 }, 2, 2, -1L);
    // Two elements, one on the minimum.
    assertShape(new long[] { 3, 9 }, 2, 1, 9L);
    // Three elements, the run stops at one child but not the other.
    assertShape(new long[] { 4, 4, 8 }, 3, 2, 8L);
    // A run that reaches the third level, so the walk has to descend past a matching child.
    assertShape(new long[] { 1, 1, 1, 1, 5, 6, 7 }, 7, 4, 5L);
  }

  // ---------- helpers ----------

  /**
   * Build a valid min-heap over term indices {@code [0, n)}. Deliberately a plain reference sift-down
   * written here rather than {@link BmwScorer}'s own heapify: the input to the code under test should
   * not be produced by the code under test.
   */
  private static int[] heapOfAllTerms(final int[] keyBucketIds, final long[] keyPositions, final int n) {
    final int[] heap = new int[n];
    for (int i = 0; i < n; i++)
      heap[i] = i;
    for (int i = (n >> 1) - 1; i >= 0; i--)
      referenceSiftDown(keyBucketIds, keyPositions, heap, n, i);
    assertIsMinHeap(keyBucketIds, keyPositions, heap, n);
    return heap;
  }

  private static void referenceSiftDown(final int[] keyBucketIds, final long[] keyPositions, final int[] heap, final int size,
      final int from) {
    int i = from;
    while (true) {
      final int left = (i << 1) + 1;
      if (left >= size)
        return;
      int smallest = left;
      final int right = left + 1;
      if (right < size && compare(keyBucketIds, keyPositions, heap[right], heap[left]) < 0)
        smallest = right;
      if (compare(keyBucketIds, keyPositions, heap[i], heap[smallest]) <= 0)
        return;
      final int tmp = heap[i];
      heap[i] = heap[smallest];
      heap[smallest] = tmp;
      i = smallest;
    }
  }

  private static int compare(final int[] keyBucketIds, final long[] keyPositions, final int a, final int b) {
    return SparseSegmentBuilder.compareRid(keyBucketIds[a], keyPositions[a], keyBucketIds[b], keyPositions[b]);
  }

  private static void assertIsMinHeap(final int[] keyBucketIds, final long[] keyPositions, final int[] heap, final int size) {
    for (int i = 1; i < size; i++) {
      final int parent = (i - 1) >>> 1;
      assertThat(compare(keyBucketIds, keyPositions, heap[parent], heap[i]))
          .as("slot %d must not hold a larger key than slot %d", parent, i)
          .isLessThanOrEqualTo(0);
    }
  }

  private static void assertRunMatchesBruteForce(final int[] keyBucketIds, final long[] keyPositions, final int[] heap,
      final int size, final int[] aligned, final int[] alignedSlots, final int alignedCount) {
    final int minBucketId = keyBucketIds[heap[0]];
    final long minPosition = keyPositions[heap[0]];
    final List<Integer> expected = new ArrayList<>();
    for (int slot = 0; slot < size; slot++)
      if (keyBucketIds[heap[slot]] == minBucketId && keyPositions[heap[slot]] == minPosition)
        expected.add(slot);

    final List<Integer> got = new ArrayList<>();
    for (int j = 0; j < alignedCount; j++) {
      got.add(alignedSlots[j]);
      assertThat(aligned[j]).as("aligned[] must be the term held by alignedSlots[]").isEqualTo(heap[alignedSlots[j]]);
    }
    assertThat(got).as("the run must be every slot holding the minimum, and nothing else").isEqualTo(expected);
    // Ascending slot order is what makes the caller's reverse walk a deepest-first repair.
    for (int j = 1; j < alignedCount; j++)
      assertThat(alignedSlots[j]).isGreaterThan(alignedSlots[j - 1]);
  }

  private static void assertFrontierMatchesBruteForce(final int[] keyBucketIds, final long[] keyPositions, final int[] heap,
      final int size, final int[] alignedSlots, final int alignedCount, final long[] frontier) {
    final boolean[] inRun = new boolean[size];
    for (int j = 0; j < alignedCount; j++)
      inRun[alignedSlots[j]] = true;

    int expectedBucketId = -1;
    long expectedPosition = -1L;
    for (int slot = 0; slot < size; slot++) {
      if (inRun[slot])
        continue;
      final int term = heap[slot];
      if (expectedBucketId < 0
          || SparseSegmentBuilder.compareRid(keyBucketIds[term], keyPositions[term], expectedBucketId, expectedPosition) < 0) {
        expectedBucketId = keyBucketIds[term];
        expectedPosition = keyPositions[term];
      }
    }
    assertThat((int) frontier[0]).as("frontier bucket id").isEqualTo(expectedBucketId);
    assertThat(frontier[1]).as("frontier position").isEqualTo(expectedPosition);
  }

  private static long bruteForceMinPosition(final long[] keyPositions, final int[] heap, final int size) {
    long min = Long.MAX_VALUE;
    for (int slot = 0; slot < size; slot++)
      min = Math.min(min, keyPositions[heap[slot]]);
    return min;
  }

  private static int[] sorted(final int[] heap, final int size) {
    final int[] copy = Arrays.copyOf(heap, size);
    Arrays.sort(copy);
    return copy;
  }

  private void assertShape(final long[] positions, final int n, final int expectedRunSize, final long expectedFrontier) {
    final int[] keyBucketIds = new int[n];
    final long[] keyPositions = Arrays.copyOf(positions, n);
    final int[] heap = heapOfAllTerms(keyBucketIds, keyPositions, n);

    final int[] aligned = new int[n];
    final int[] alignedSlots = new int[n];
    final int alignedCount = BmwScorer.collectAlignedRun(keyBucketIds, keyPositions, heap, n, keyBucketIds[heap[0]],
        keyPositions[heap[0]], aligned, alignedSlots);
    assertThat(alignedCount).as("run size for %s", Arrays.toString(positions)).isEqualTo(expectedRunSize);
    assertRunMatchesBruteForce(keyBucketIds, keyPositions, heap, n, aligned, alignedSlots, alignedCount);

    final long[] frontier = new long[2];
    BmwScorer.nextEssentialKey(keyBucketIds, keyPositions, heap, n, alignedSlots, alignedCount, keyBucketIds[heap[0]],
        keyPositions[heap[0]], frontier);
    assertThat(frontier[1]).as("frontier for %s", Arrays.toString(positions)).isEqualTo(expectedFrontier);

    for (int j = 0; j < alignedCount; j++)
      keyPositions[aligned[j]] += 100;
    for (int j = alignedCount - 1; j >= 0; j--)
      BmwScorer.siftDownFromFloyd(keyBucketIds, keyPositions, heap, n, alignedSlots[j]);
    assertIsMinHeap(keyBucketIds, keyPositions, heap, n);
  }
}
