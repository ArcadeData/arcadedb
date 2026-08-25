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

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;

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
}
