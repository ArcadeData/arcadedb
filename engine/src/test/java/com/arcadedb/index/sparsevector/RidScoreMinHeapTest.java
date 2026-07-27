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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Behavioural parity and allocation regression coverage for {@link RidScoreMinHeap}, the top-K
 * accumulator on the sparse-vector query hot path (issue #5473).
 * <p>
 * The reference implementation in every parity test is literally the code this replaced -
 * {@code new PriorityQueue<RidScore>(k, Comparator.comparing(RidScore::score))} driven with the
 * same admission rule - so any divergence in ordering, in tie handling, or in the treatment of
 * {@code NaN} / {@code -0.0f} fails here.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class RidScoreMinHeapTest {

  /** The exact comparator the replaced code used, boxing included. */
  private static final Comparator<RidScore> BOXED_BY_SCORE = Comparator.comparing(RidScore::score);

  @Test
  void matchesBoxedPriorityQueueOnRandomStreams() {
    for (int seed = 0; seed < 200; seed++) {
      final Random rnd = new Random(seed);
      final int capacity = 1 + rnd.nextInt(24);
      final int candidates = rnd.nextInt(400);

      final RidScoreMinHeap heap = new RidScoreMinHeap(capacity);
      final PriorityQueue<RidScore> reference = new PriorityQueue<>(capacity, BOXED_BY_SCORE);

      for (int i = 0; i < candidates; i++) {
        final RID rid = new RID(1, i);
        // A deliberately coarse score domain so ties (and equal-score replacements) are frequent.
        final float score = rnd.nextInt(20) / 4.0f;

        heap.offer(rid, score);

        if (reference.size() < capacity)
          reference.add(new RidScore(rid, score));
        else if (score > reference.peek().score()) {
          reference.poll();
          reference.add(new RidScore(rid, score));
        }

        assertThat(heap.size()).as("seed %d, step %d", seed, i).isEqualTo(reference.size());
        if (!heap.isEmpty())
          assertThat(heap.minScore()).as("seed %d, step %d: threshold", seed, i)
              .isEqualTo(reference.peek().score());
      }

      assertThat(scoresDescending(heap)).as("seed %d", seed).isEqualTo(scoresDescending(reference));
    }
  }

  /**
   * {@code Float.compare} - not the primitive {@code <} - is what {@code Comparator.comparing} used
   * on the boxed keys, so the heap has to reproduce its total order: {@code NaN} above every number
   * and {@code -0.0f} strictly below {@code 0.0f}.
   */
  @Test
  void reproducesFloatCompareTotalOrderForNanAndSignedZero() {
    final float[] domain = { Float.NaN, Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY, 0.0f, -0.0f, 1.0f, -1.0f,
        Float.MIN_VALUE, Float.MAX_VALUE };

    for (int capacity = 1; capacity <= domain.length; capacity++) {
      final RidScoreMinHeap heap = new RidScoreMinHeap(capacity);
      final PriorityQueue<RidScore> reference = new PriorityQueue<>(capacity, BOXED_BY_SCORE);

      for (int i = 0; i < domain.length; i++) {
        final RID rid = new RID(1, i);
        final float score = domain[i];

        heap.offer(rid, score);

        if (reference.size() < capacity)
          reference.add(new RidScore(rid, score));
        else if (score > reference.peek().score()) {
          reference.poll();
          reference.add(new RidScore(rid, score));
        }
      }

      assertThat(ridsOf(heap)).as("capacity %d", capacity).containsExactlyInAnyOrderElementsOf(ridsOf(reference));
      // Float.compare, not ==, so a NaN threshold on both sides still counts as agreement.
      assertThat(Float.compare(heap.minScore(), reference.peek().score())).as("capacity %d: threshold", capacity)
          .isZero();
    }
  }

  @Test
  void keepsTheBestKAndNeverExceedsCapacity() {
    final int capacity = 10;
    final RidScoreMinHeap heap = new RidScoreMinHeap(capacity);
    // 1000 candidates whose scores zig-zag, so the stream is neither ascending nor descending.
    final List<Float> all = new ArrayList<>(1000);
    for (int i = 0; i < 1000; i++) {
      final float score = i % 2 == 0 ? i : 1000 - i;
      heap.offer(new RID(1, i), score);
      all.add(score);
    }
    all.sort((a, b) -> Float.compare(b, a));

    assertThat(heap.size()).isEqualTo(capacity);
    assertThat(heap.isFull()).isTrue();
    final List<Float> got = scoresDescending(heap);
    assertThat(got).isEqualTo(all.subList(0, capacity));
    assertThat(got.get(capacity - 1)).isEqualTo(heap.minScore());
  }

  @Test
  void capacityOneRetainsTheMaximum() {
    final RidScoreMinHeap heap = new RidScoreMinHeap(1);
    heap.offer(new RID(1, 0), 1.0f);
    assertThat(heap.isFull()).isTrue();
    assertThat(heap.offer(new RID(1, 1), 0.5f)).isFalse();
    assertThat(heap.offer(new RID(1, 2), 1.0f)).as("strictly-greater admission").isFalse();
    assertThat(heap.offer(new RID(1, 3), 2.0f)).isTrue();

    final List<RidScore> out = new ArrayList<>();
    heap.drainInto(out);
    assertThat(out).hasSize(1);
    assertThat(out.getFirst().score()).isEqualTo(2.0f);
    assertThat(out.getFirst().rid()).isEqualTo(new RID(1, 3));
  }

  /** The backing arrays start small and grow on demand, so a large unfilled K costs nothing. */
  @Test
  void growsLazilyTowardsCapacity() {
    final RidScoreMinHeap heap = new RidScoreMinHeap(100_000);
    for (int i = 0; i < 1000; i++)
      assertThat(heap.offer(new RID(1, i), i)).isTrue();

    assertThat(heap.size()).isEqualTo(1000);
    assertThat(heap.isFull()).isFalse();
    assertThat(heap.minScore()).isEqualTo(0.0f);
    assertThat(scoresDescending(heap)).hasSize(1000).first().isEqualTo(999.0f);
  }

  @Test
  void rejectsNonPositiveCapacity() {
    assertThat(catchIllegalArgument(() -> new RidScoreMinHeap(0))).isNotNull();
    assertThat(catchIllegalArgument(() -> new RidScoreMinHeap(-1))).isNotNull();
  }

  /**
   * The point of the class: a steady-state heap replacement must not allocate at all.
   * <p>
   * The replaced {@code PriorityQueue} allocated one {@link RidScore} per accepted candidate,
   * escaping into the queue's backing array - 24 bytes that no JIT pass can remove. (The boxed
   * comparison keys issue #5473 pointed at are the <i>other</i> axis, and this measurement is also
   * what shows they cost nothing once C2 scalar-replaces them: with escape analysis on, the boxed
   * and the {@code comparingDouble} variants allocate identically.)
   * <p>
   * Both variants are driven with the identical, fully-accepting workload and measured with
   * {@code ThreadMXBean.getThreadAllocatedBytes}. Only the minimum of several rounds is kept, so a
   * GC or a deoptimisation landing inside one round cannot turn the assertion red.
   */
  @Test
  @Tag("performance")
  void offerDoesNotAllocateInSteadyState() {
    final com.sun.management.ThreadMXBean threads = threadAllocationBean();
    assumeTrue(threads != null, "JVM does not expose per-thread allocation counters");

    final int capacity = 64;
    final int operations = 200_000;
    final RID rid = new RID(1, 42);  // reused: RID allocation is not what is under measurement.

    // Warm up both variants past the interpreter and into a stable compiled state.
    for (int i = 0; i < 5; i++) {
      runPrimitiveHeap(capacity, operations, rid);
      runBoxedQueue(capacity, operations, rid);
    }

    long primitiveBytes = Long.MAX_VALUE;
    long boxedBytes = Long.MAX_VALUE;
    for (int round = 0; round < 5; round++) {
      primitiveBytes = Math.min(primitiveBytes, measure(threads, () -> runPrimitiveHeap(capacity, operations, rid)));
      boxedBytes = Math.min(boxedBytes, measure(threads, () -> runBoxedQueue(capacity, operations, rid)));
    }

    final double primitivePerOp = primitiveBytes / (double) operations;
    final double boxedPerOp = boxedBytes / (double) operations;

    // Steady state is the heap already at capacity, so the only allocation left is the initial
    // lazy growth of the two backing arrays - a rounding error against 200k operations.
    assertThat(primitivePerOp).as("bytes allocated per offer(): primitive %.2f vs boxed %.2f", primitivePerOp, boxedPerOp)
        .isLessThan(1.0);
    assertThat(boxedPerOp).as("the replaced PriorityQueue path allocated per accepted candidate").isGreaterThan(8.0);
  }

  // ---------- helpers ----------

  /** Every candidate is strictly greater than the current minimum, so every offer replaces. */
  private static int runPrimitiveHeap(final int capacity, final int operations, final RID rid) {
    final RidScoreMinHeap heap = new RidScoreMinHeap(capacity);
    for (int i = 0; i < operations; i++)
      heap.offer(rid, i);
    return heap.size();
  }

  /** The replaced implementation, verbatim. */
  private static int runBoxedQueue(final int capacity, final int operations, final RID rid) {
    final PriorityQueue<RidScore> heap = new PriorityQueue<>(capacity, BOXED_BY_SCORE);
    for (int i = 0; i < operations; i++) {
      if (heap.size() < capacity)
        heap.add(new RidScore(rid, i));
      else if (i > heap.peek().score()) {
        heap.poll();
        heap.add(new RidScore(rid, i));
      }
    }
    return heap.size();
  }

  private static long measure(final com.sun.management.ThreadMXBean threads, final Runnable body) {
    final long id = Thread.currentThread().threadId();
    final long before = threads.getThreadAllocatedBytes(id);
    body.run();
    return threads.getThreadAllocatedBytes(id) - before;
  }

  private static com.sun.management.ThreadMXBean threadAllocationBean() {
    if (!(ManagementFactory.getThreadMXBean() instanceof final com.sun.management.ThreadMXBean bean))
      return null;
    if (!bean.isThreadAllocatedMemorySupported())
      return null;
    bean.setThreadAllocatedMemoryEnabled(true);
    return bean.isThreadAllocatedMemoryEnabled() ? bean : null;
  }

  private static List<Float> scoresDescending(final RidScoreMinHeap heap) {
    final List<RidScore> out = new ArrayList<>(heap.size());
    heap.drainInto(out);
    return sortedScores(out);
  }

  private static List<Float> scoresDescending(final PriorityQueue<RidScore> queue) {
    return sortedScores(new ArrayList<>(queue));
  }

  private static List<Float> sortedScores(final List<RidScore> rows) {
    rows.sort((a, b) -> Float.compare(b.score(), a.score()));
    final List<Float> out = new ArrayList<>(rows.size());
    for (final RidScore r : rows)
      out.add(r.score());
    return out;
  }

  private static List<RID> ridsOf(final RidScoreMinHeap heap) {
    final List<RidScore> rows = new ArrayList<>(heap.size());
    heap.drainInto(rows);
    return rows.stream().map(RidScore::rid).toList();
  }

  private static List<RID> ridsOf(final PriorityQueue<RidScore> queue) {
    return queue.stream().map(RidScore::rid).toList();
  }

  private static IllegalArgumentException catchIllegalArgument(final Runnable body) {
    try {
      body.run();
      return null;
    } catch (final IllegalArgumentException e) {
      return e;
    }
  }
}
