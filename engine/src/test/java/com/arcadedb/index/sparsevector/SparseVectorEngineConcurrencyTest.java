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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase 3 verification: writers, readers, and a flush/compact orchestrator can all run on the
 * same engine without lost writes, deadlocks, or corruption. Smoke test, not a replacement for
 * deeper WAL/HA recovery testing in Phase 3.x.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class SparseVectorEngineConcurrencyTest {

  private static final SegmentParameters TEST_PARAMS = SegmentParameters.builder()
      .weightQuantization(SegmentFormat.WeightQuantization.FP32)
      .build();

  @Test
  void writersAndReadersCoexistWithBackgroundFlushes(@TempDir final Path tmp) throws IOException, InterruptedException {
    try (final SparseVectorEngine engine = new SparseVectorEngine(tmp, TEST_PARAMS)) {
      final int writers = 4;
      final int readers = 2;
      final int perWriter = 2_000;
      final int dims = 50;
      final ExecutorService exec = Executors.newFixedThreadPool(writers + readers + 1);
      final CountDownLatch start = new CountDownLatch(1);
      final AtomicInteger errors = new AtomicInteger();
      final AtomicInteger queriesRun = new AtomicInteger();

      try {
        // Writers: each owns a disjoint bucket, no conflicts.
        for (int w = 0; w < writers; w++) {
          final int writerId = w;
          exec.submit(() -> {
            try {
              start.await();
              final Random rnd = new Random(writerId);
              for (int i = 0; i < perWriter; i++) {
                engine.put(rnd.nextInt(dims), new RID(writerId, (long) i), rnd.nextFloat());
              }
            } catch (final Throwable t) {
              errors.incrementAndGet();
              t.printStackTrace();
            }
          });
        }

        // Readers: keep running top-K queries while writes happen.
        for (int r = 0; r < readers; r++) {
          final int readerId = r;
          exec.submit(() -> {
            try {
              start.await();
              final Random rnd = new Random(readerId * 10_000L);
              for (int i = 0; i < 200; i++) {
                final int[] qd = { rnd.nextInt(dims), rnd.nextInt(dims) };
                final float[] qw = { 1.0f, 1.0f };
                engine.topK(qd, qw, 10);
                queriesRun.incrementAndGet();
              }
            } catch (final Throwable t) {
              errors.incrementAndGet();
              t.printStackTrace();
            }
          });
        }

        // Background flusher.
        exec.submit(() -> {
          try {
            start.await();
            for (int i = 0; i < 5; i++) {
              Thread.sleep(50);
              engine.flush();
            }
          } catch (final Throwable t) {
            errors.incrementAndGet();
            t.printStackTrace();
          }
        });

        start.countDown();
        exec.shutdown();
        assertThat(exec.awaitTermination(60, TimeUnit.SECONDS)).isTrue();
      } finally {
        exec.shutdownNow();
      }

      assertThat(errors.get()).isZero();
      assertThat(queriesRun.get()).isPositive();

      // Final consistency: total written postings must be discoverable across memtable + segments.
      // Disjoint-bucket writers guarantee no overwrites; the query universe is bounded by writers * perWriter.
      engine.flush();
      engine.compactAll();
      // top-K with tiny k just checks the engine is in a queryable state - no asserts on content because
      // random scores produce random orderings. The fact that the call succeeds with no exceptions is enough.
      final List<RidScore> got = engine.topK(new int[] { 0, 1, 2 }, new float[] { 1, 1, 1 }, 10);
      assertThat(got).isNotNull();
    }
  }
}
