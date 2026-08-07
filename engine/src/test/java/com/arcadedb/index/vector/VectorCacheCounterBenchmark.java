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

import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Measures what the hit/miss counters of {@link VectorCache} cost on the lookup path, which is the path every
 * distance evaluation of a graph build and of a beam search goes through.
 * <p>
 * The counters used to be two {@link LongAdder}s. A DEEP-10M profile contributed on issue #5577 put 26.4% of the
 * whole graph build inside {@code LongAdder.add}, every sample of it reached through {@code VectorCache.get} - a
 * locked compare-and-swap per lookup is the same order of magnitude as the SIMD distance the lookup exists to feed,
 * and unlike the distance it gets worse as the build pool widens. This benchmark reproduces that comparison in
 * isolation so the claim can be re-measured on any box.
 * <p>
 * Excluded from CI: it reports numbers rather than asserting behaviour. Run it with {@code -Dgroups=benchmark}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("benchmark")
class VectorCacheCounterBenchmark {
  private static final VectorTypeSupport VTS = VectorizationProvider.getInstance().getVectorTypeSupport();

  private static final int LOOKUPS_PER_THREAD = 4_000_000;
  private static final int WARMUP_LOOKUPS     = 500_000;

  /** The counter pair as it used to be: two process-wide LongAdders, one increment per lookup. */
  private static final class LongAdderCounters {
    private final LongAdder hits   = new LongAdder();
    private final LongAdder misses = new LongAdder();

    void count(final boolean hit) {
      if (hit)
        hits.increment();
      else
        misses.increment();
    }

    long total() {
      return hits.sum() + misses.sum();
    }
  }

  @Test
  void stripedCountersBeatLongAddersOnTheLookupPath() throws Exception {
    final int[] threadCounts = { 1, 2, 4, Math.max(4, Runtime.getRuntime().availableProcessors()) };

    System.out.printf("%nVectorCache counter cost, %,d lookups per thread%n", LOOKUPS_PER_THREAD);
    System.out.printf("%-10s %14s %14s %10s%n", "threads", "LongAdder ns/op", "striped ns/op", "speedup");

    for (final int threads : threadCounts) {
      final double adderNs = measureLongAdders(threads);
      final double stripedNs = measureVectorCache(threads);
      System.out.printf("%-10d %14.2f %14.2f %9.2fx%n", threads, adderNs, stripedNs, adderNs / stripedNs);
    }
  }

  private static double measureLongAdders(final int threads) throws Exception {
    final LongAdderCounters counters = new LongAdderCounters();
    final long elapsedNs = run(threads, lookups -> {
      for (int i = 0; i < lookups; i++)
        counters.count((i & 1) == 0);
    });
    assertThat(counters.total()).isPositive();
    return (double) elapsedNs / ((long) threads * LOOKUPS_PER_THREAD);
  }

  private static double measureVectorCache(final int threads) throws Exception {
    // One resident entry and one absent id, so every lookup takes the same two array reads and differs only in
    // which counter it touches. The cache itself is not what is being measured.
    final VectorCache cache = new VectorCache(64);
    cache.put(1, VTS.createFloatVector(new float[] { 1f, 1f, 1f, 1f }));

    final long elapsedNs = run(threads, lookups -> {
      for (int i = 0; i < lookups; i++)
        cache.get((i & 1) == 0 ? 1 : 99);
    });
    assertThat(cache.getHits() + cache.getMisses()).isPositive();
    return (double) elapsedNs / ((long) threads * LOOKUPS_PER_THREAD);
  }

  private interface Workload {
    void run(int lookups);
  }

  private static long run(final int threads, final Workload workload) throws Exception {
    workload.run(WARMUP_LOOKUPS);

    final CountDownLatch start = new CountDownLatch(1);
    final CountDownLatch done = new CountDownLatch(threads);

    for (int t = 0; t < threads; t++) {
      final Thread thread = new Thread(() -> {
        try {
          start.await();
          workload.run(LOOKUPS_PER_THREAD);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        } finally {
          done.countDown();
        }
      });
      thread.setDaemon(true);
      thread.start();
    }

    final long begin = System.nanoTime();
    start.countDown();
    assertThat(done.await(5, TimeUnit.MINUTES)).isTrue();
    return System.nanoTime() - begin;
  }
}
