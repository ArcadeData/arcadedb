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
 * The cause was a race between two callers of {@code createThreads}/{@code shutdownThreads} (e.g.
 * concurrent {@code setTransactionUseWAL(true)} during GraphBatch close). Each call read
 * {@code executorThreads}, the other thread nulled it, and the first thread NPE'd on the dangling
 * reference. This test exercises that race directly.
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
            // Every flip of WAL or sync triggers an internal createThreads() -> shutdownThreads()
            // cycle on the shared async executor. Multiple threads racing on this is the exact
            // workload the customer hit (one GraphBatch per ingest worker thread, all on the same
            // database).
            async.setTransactionUseWAL((seed + i) % 2 == 0);
            async.setTransactionSync(((seed + i) % 2 == 0)
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
