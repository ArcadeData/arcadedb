/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.database.async;

import com.arcadedb.TestHelper;
import com.arcadedb.engine.WALFile;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression for the customer-reported NPE on {@code this.executorThreads} during a heavy parallel
 * 50k-vertex GraphBatch insert workload (logs from 2026-05-07):
 * <pre>
 *   SEVER [PostBatchHandler] Cannot assign field "shutdown" because "this.executorThreads[i]" is null
 *   SEVER [PostBatchHandler] Cannot store to object array because "this.executorThreads" is null
 * </pre>
 * The cause was a race between two callers of {@code createThreads}/{@code shutdownThreads} (concurrent
 * {@code setTransactionUseWAL(true)} during GraphBatch close, back when that setter still tore down and
 * respawned the pool). Each call read {@code executorThreads}, the other thread nulled it, and the first
 * thread NPE'd on the dangling reference. This test exercises concurrent flag flips directly.
 * <p>
 * #6509 turned {@code setTransactionUseWAL()}/{@code setTransactionSync()} into plain volatile writes
 * that no longer call {@code createThreads()} at all, so this exact race is no longer reachable through
 * them - the assertions below stay valid (concurrent flips still must not throw) as a standing guard
 * against the mechanism being reintroduced.
 */
class DatabaseAsyncExecutorLifecycleRaceTest extends TestHelper {

  @Test
  void concurrentLifecycleChangesDoNotNPE() throws Exception {
    final DatabaseAsyncExecutor async = database.async();
    final int threads = 8;
    final int iterations = 100;

    final AtomicReference<Throwable> firstError = new AtomicReference<>();
    final CompletableFuture<?>[] workers = new CompletableFuture<?>[threads];
    for (int t = 0; t < threads; t++) {
      final int seed = t;
      workers[t] = CompletableFuture.runAsync(() -> {
        try {
          for (int i = 0; i < iterations; i++) {
            // #6509: plain volatile writes now, no createThreads()/shutdownThreads() cycle. Kept as a
            // concurrent-flip regression guard for the mechanism this test was written against.
            async.setTransactionUseWAL((seed + i) % 2 == 0);
            async.setTransactionSync((seed + i) % 2 == 0
                ? WALFile.FlushType.NO
                : WALFile.FlushType.YES_NOMETADATA);
            // Touch readers too so a half-published array would be observed as NPE here.
            async.getThreadCount();
          }
        } catch (final Throwable e) {
          firstError.compareAndSet(null, e);
        }
      });
    }
    CompletableFuture.allOf(workers).join();

    if (firstError.get() != null)
      throw new AssertionError("Lifecycle race produced an exception", firstError.get());

    assertThat(async.getThreadCount()).isGreaterThan(0);
  }
}
