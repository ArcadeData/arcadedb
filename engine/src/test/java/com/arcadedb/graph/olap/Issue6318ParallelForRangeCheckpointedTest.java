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
package com.arcadedb.graph.olap;

import com.arcadedb.utility.StallAwareStopwatch;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.function.BiConsumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for issue #6318 - {@code GraphAlgorithms.localClusteringCoefficient}'s CSR path had no
 * checkpoint of its own to give it the abortability the OLTP path already had, because its two parallel phases
 * had no per-iteration boundary the way {@code pageRank}/{@code labelPropagation} do to hang one on.
 * {@code parallelForRangeCheckpointed} is the fix: it splits a range into batches and calls the checkpoint
 * between them, on the calling thread, the same contract {@link WorkCheckpoint#check()} documents.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6318ParallelForRangeCheckpointedTest {

  @Test
  void everyIndexInTheRangeIsVisitedExactlyOnce() {
    for (final int n : new int[] { 0, 1, 100, 8192, 20_000, 100_000 }) {
      final AtomicIntegerArray hits = new AtomicIntegerArray(Math.max(n, 1));
      GraphAlgorithms.parallelForRangeCheckpointed(n, WorkCheckpoint.NONE, (start, end) -> {
        for (int i = start; i < end; i++)
          hits.incrementAndGet(i);
      });
      for (int i = 0; i < n; i++)
        assertThat(hits.get(i)).as("index " + i + " for n=" + n).isEqualTo(1);
    }
  }

  @Test
  void aZeroLengthRangeNeverChecksIn() {
    // PR #6714 review round 11: documented in the javadoc as the one exception to "checks in at least once",
    // but not previously pinned by a test - the batch loop's condition (start < n) is false immediately for
    // n == 0, so the loop body (and therefore checkpoint.check()) never runs at all.
    final AtomicInteger checkpointCalls = new AtomicInteger();
    GraphAlgorithms.parallelForRangeCheckpointed(0, checkpointCalls::incrementAndGet, (s, e) -> {
    });
    assertThat(checkpointCalls.get())
        .as("n == 0 has no batch to run, so the checkpoint is never consulted - harmless only because every "
            + "current caller already returns before reaching a checkpointed loop on an empty graph")
        .isZero();
  }

  @Test
  void aSmallRangeChecksInExactlyOnce() {
    final AtomicInteger checkpointCalls = new AtomicInteger();
    GraphAlgorithms.parallelForRangeCheckpointed(10, checkpointCalls::incrementAndGet, (s, e) -> {
    });
    assertThat(checkpointCalls.get())
        .as("a range too small to parallelise must still check in once, like a plain parallelForRange call would")
        .isEqualTo(1);
  }

  @Test
  void aLargeRangeChecksInMoreThanOnce() {
    final AtomicInteger checkpointCalls = new AtomicInteger();
    GraphAlgorithms.parallelForRangeCheckpointed(200_000, checkpointCalls::incrementAndGet, (s, e) -> {
    });
    assertThat(checkpointCalls.get())
        .as("a large range must be split into more than one batch, or the CSR LCC path this backs is exactly as "
            + "unabortable mid-pass as it was before issue #6318")
        .isGreaterThan(1);
  }

  @Test
  void aThrowingCheckpointAbortsBeforeLaterBatchesRun() {
    // Fires on the 3rd call: the first batch's work must have already run (checkpoint.check() happens before a
    // batch's work in the loop, so batch 1 and 2 run, batch 3 is where the throw happens before its own work).
    final AtomicInteger calls = new AtomicInteger();
    final int n = 200_000;
    final AtomicInteger processed = new AtomicInteger();

    final WorkCheckpoint checkpoint = () -> {
      if (calls.incrementAndGet() == 3)
        throw new IllegalStateException("aborted for the test");
    };

    assertThatThrownBy(() -> GraphAlgorithms.parallelForRangeCheckpointed(n, checkpoint, (s, e) -> processed.addAndGet(e - s)))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("aborted for the test");

    assertThat(calls.get()).as("must not keep checking in after the throw").isEqualTo(3);
    assertThat(processed.get())
        .as("batches after the one that threw must never run their work")
        .isLessThan(n);
  }

  /**
   * Code review counterweight for issue #6318 (PR #6714): splitting a range into up to {@code CHECKPOINT_BATCHES}
   * batches means up to that many separate {@link GraphAlgorithms#parallelForRange} dispatches - each with its
   * own thread-pool submission round-trip - where {@code pageRank}/{@code labelPropagation}'s hot parallel phases
   * used to issue exactly one. This measures that the added dispatch overhead stays small relative to a
   * pageRank-shaped amount of per-node work (a handful of flops over a small fixed-size neighbour array, not a
   * bare store - a trivial per-node body would be dispatch-dominated and overstate the overhead in a way real
   * kernels never see), rather than asserting it from the shape of the code alone.
   * <p>
   * {@link StallAwareStopwatch#effectiveMs()} discounts JVM-wide stalls from each of the two measurements before
   * they are compared, so the ratio is not a raw wall-clock reading (#6260) - a shared stop-the-world pause moves
   * both sides together and the ratio is unaffected either way.
   */
  @Test
  @Tag("benchmark")
  void checkpointedBatchingOverheadIsSmallRelativeToPageRankShapedWork() {
    final int n = 2_000_000;
    final int neighborsPerNode = 4;
    final double[] contrib = new double[n];
    final double[] next = new double[n];
    for (int i = 0; i < n; i++)
      contrib[i] = 1.0 / (i + 1);

    // A stand-in for pageRank's pull phase: each node sums a small fixed number of neighbours' contributions,
    // the same order of magnitude of work per node as the real kernel does.
    final BiConsumer<Integer, Integer> pageRankShapedWork = (start, end) -> {
      for (int u = start; u < end; u++) {
        double sum = 0.0;
        for (int k = 0; k < neighborsPerNode; k++)
          sum += contrib[(u + k * 7919) % n];
        next[u] = 0.15 + 0.85 * sum;
      }
    };

    // Warm up the JIT for both call shapes before any measurement is taken.
    for (int i = 0; i < 3; i++) {
      GraphAlgorithms.parallelForRange(n, pageRankShapedWork);
      GraphAlgorithms.parallelForRangeCheckpointed(n, WorkCheckpoint.NONE, pageRankShapedWork);
    }

    long plainMs = Long.MAX_VALUE;
    long checkpointedMs = Long.MAX_VALUE;
    for (int round = 0; round < 3; round++) {
      final StallAwareStopwatch plainWatch = StallAwareStopwatch.start();
      GraphAlgorithms.parallelForRange(n, pageRankShapedWork);
      plainMs = Math.min(plainMs, plainWatch.effectiveMs());

      final StallAwareStopwatch checkpointedWatch = StallAwareStopwatch.start();
      GraphAlgorithms.parallelForRangeCheckpointed(n, WorkCheckpoint.NONE, pageRankShapedWork);
      checkpointedMs = Math.min(checkpointedMs, checkpointedWatch.effectiveMs());
    }

    // Generous on purpose - the point separated is "a handful of extra thread-pool round-trips" from "something
    // pathological", not a tight throughput budget. 50ms floors the comparison so two sub-millisecond, noise-
    // dominated readings cannot fail this by chance.
    assertThat(checkpointedMs)
        .as("up to CHECKPOINT_BATCHES=16 dispatches instead of 1 must not multiply wall-clock time on "
            + "pageRank-shaped work (best-of-3: unbatched=" + plainMs + "ms, batched=" + checkpointedMs + "ms)")
        .isLessThan(Math.max(50L, plainMs * 3));
  }
}
