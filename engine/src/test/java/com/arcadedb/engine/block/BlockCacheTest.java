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
package com.arcadedb.engine.block;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BlockCacheTest {

  @Test
  void hitReturnsTheCachedInstanceWithoutLoading() {
    final BlockCache cache = new BlockCache(() -> 1000L, 1);
    final AtomicInteger loads = new AtomicInteger();
    final long[] first = cache.get(1, 1, 0, () -> {
      loads.incrementAndGet();
      return new long[4];
    }, v -> 100L);
    final long[] second = cache.get(1, 1, 0, () -> {
      loads.incrementAndGet();
      return new long[4];
    }, v -> 100L);
    assertThat(second).isSameAs(first);
    assertThat(loads.get()).isEqualTo(1);
    assertThat(cache.hits()).isEqualTo(1);
    assertThat(cache.misses()).isEqualTo(1);
  }

  @Test
  void evictsLeastRecentlyUsedUnderBudget() {
    final BlockCache cache = new BlockCache(() -> 282L, 1);
    final String[] keys = { "a", "b", "c" };
    final int[] loadsPer = new int[3];
    for (final int i : new int[] { 0, 1, 0, 2 })
      cache.get(1, i, 0, () -> {
        loadsPer[i]++;
        return keys[i];
      }, v -> 40L);
    assertThat(cache.usedBytes()).isEqualTo(2 * (40 + BlockCache.ENTRY_OVERHEAD));
    cache.get(1, 0, 0, () -> {
      loadsPer[0]++;
      return "a";
    }, v -> 40L);
    assertThat(loadsPer[0]).isEqualTo(1);
    cache.get(1, 1, 0, () -> {
      loadsPer[1]++;
      return "b";
    }, v -> 40L);
    assertThat(loadsPer[1]).isEqualTo(2);
  }

  @Test
  void entryLargerThanAStripeIsReturnedButNotHeld() {
    final BlockCache cache = new BlockCache(() -> 400L, 4);
    final AtomicInteger loads = new AtomicInteger();
    for (int i = 0; i < 2; i++) {
      final String value = cache.get(1, 1, 0, () -> {
        loads.incrementAndGet();
        return "big";
      }, v -> 101L);
      assertThat(value).isEqualTo("big");
    }
    assertThat(cache.usedBytes()).isZero();
    assertThat(loads.get()).isEqualTo(2);
  }

  @Test
  void zeroBudgetDisablesCaching() {
    final BlockCache cache = new BlockCache(() -> 0L, 4);
    final AtomicInteger loads = new AtomicInteger();
    for (int i = 0; i < 3; i++)
      cache.get(1, 1, 0, () -> {
        loads.incrementAndGet();
        return "v";
      }, v -> 1L);
    assertThat(loads.get()).isEqualTo(3);
    assertThat(cache.usedBytes()).isZero();
  }

  @Test
  void budgetShrinkAppliesAtTheNextTouchOfAStripe() {
    final long[] budget = { 10 * (100 + BlockCache.ENTRY_OVERHEAD) };
    final BlockCache cache = new BlockCache(() -> budget[0], 1);
    for (int i = 0; i < 10; i++) {
      final int block = i;
      cache.get(1, block, 0, () -> "v" + block, v -> 100L);
    }
    assertThat(cache.usedBytes()).isEqualTo(10 * (100 + BlockCache.ENTRY_OVERHEAD));
    budget[0] = 600L;
    cache.get(1, 99, 0, () -> "x", v -> 100L);
    assertThat(cache.usedBytes()).isLessThanOrEqualTo(600);
    assertThat(cache.usedBytes()).isEqualTo(cache.recountBytes());
  }

  @Test
  void invalidateFileDropsOnlyThatFile() {
    final BlockCache cache = new BlockCache(() -> 100_000L, 4);
    final AtomicInteger loads = new AtomicInteger();
    for (long file = 1; file <= 2; file++)
      for (int block = 0; block < 5; block++)
        cache.get(file, block, 0, () -> {
          loads.incrementAndGet();
          return "v";
        }, v -> 10L);
    assertThat(loads.get()).isEqualTo(10);
    cache.invalidateFile(1);
    for (int block = 0; block < 5; block++)
      cache.get(2, block, 0, () -> {
        loads.incrementAndGet();
        return "v";
      }, v -> 10L);
    assertThat(loads.get()).isEqualTo(10);
    for (int block = 0; block < 5; block++)
      cache.get(1, block, 0, () -> {
        loads.incrementAndGet();
        return "v";
      }, v -> 10L);
    assertThat(loads.get()).isEqualTo(15);
    assertThat(cache.usedBytes()).isEqualTo(10 * (10 + BlockCache.ENTRY_OVERHEAD));
    assertThat(cache.usedBytes()).isEqualTo(cache.recountBytes());
  }

  @Test
  void nullFromTheLoaderIsNotCached() {
    final BlockCache cache = new BlockCache(() -> 1000L, 2);
    final AtomicInteger loads = new AtomicInteger();
    for (int i = 0; i < 2; i++) {
      final String value = cache.get(1, 1, 0, () -> {
        loads.incrementAndGet();
        return null;
      }, v -> 10L);
      assertThat(value).isNull();
    }
    assertThat(loads.get()).isEqualTo(2);
    assertThat(cache.usedBytes()).isZero();
  }

  @Test
  void loaderExceptionLeavesNothingBehind() {
    final BlockCache cache = new BlockCache(() -> 1000L, 2);
    assertThatThrownBy(() -> cache.get(1, 1, 0, () -> {
      throw new IllegalStateException("boom");
    }, v -> 10L)).isInstanceOf(IllegalStateException.class);
    assertThat(cache.usedBytes()).isZero();
    assertThat(cache.get(1, 1, 0, () -> "ok", v -> 10L)).isEqualTo("ok");
    assertThat(cache.usedBytes()).isEqualTo(10 + BlockCache.ENTRY_OVERHEAD);
  }

  @Test
  void concurrentFirstLoadsOfOneKeyChargeOnce() throws Exception {
    final BlockCache cache = new BlockCache(() -> 1_000_000L, 16);
    final int threads = 8;
    final long charge = 8016 + BlockCache.ENTRY_OVERHEAD;
    final CountDownLatch start = new CountDownLatch(1);
    // Every loader waits here, so all threads are inside the loader before any of them admits.
    final CountDownLatch allLoading = new CountDownLatch(threads);
    final long[][] results = new long[threads][];
    final Thread[] workers = new Thread[threads];
    final AtomicInteger failures = new AtomicInteger();
    for (int t = 0; t < threads; t++) {
      final int index = t;
      workers[t] = new Thread(() -> {
        try {
          start.await();
          results[index] = cache.get(7, 3, 1, () -> {
            allLoading.countDown();
            try {
              if (!allLoading.await(60, TimeUnit.SECONDS))
                throw new IllegalStateException("not all threads reached the loader");
            } catch (final InterruptedException e) {
              throw new IllegalStateException(e);
            }
            return new long[1000];
          }, v -> 8016L);
        } catch (final Throwable e) {
          failures.incrementAndGet();
        }
      });
      workers[t].start();
    }
    start.countDown();
    for (final Thread w : workers) {
      w.join(60_000);
      assertThat(w.isAlive()).isFalse();
    }
    assertThat(failures.get()).isZero();
    for (final long[] r : results)
      assertThat(r).isNotNull().hasSize(1000);
    assertThat(cache.misses()).isEqualTo(threads);
    assertThat(cache.hits()).isZero();
    assertThat(cache.usedBytes()).isEqualTo(charge);
    assertThat(cache.recountBytes()).isEqualTo(charge);
    final long[] held = cache.get(7, 3, 1, () -> new long[1000], v -> 8016L);
    assertThat(cache.get(7, 3, 1, () -> new long[1000], v -> 8016L)).isSameAs(held);
    int sameAsHeld = 0;
    for (final long[] r : results)
      if (r == held)
        sameAsHeld++;
    // Every racer that lost the admission got the winner's instance.
    assertThat(sameAsHeld).isPositive();
  }

  @Test
  void loweredBudgetIsEnforcedOnStripesThatOnlySeeHits() {
    final long[] budget = { 16L * 10_000 };
    final BlockCache cache = new BlockCache(() -> budget[0], 16);
    final List<long[]> keys = new ArrayList<>();
    for (int block = 0; block < 400; block++) {
      cache.get(1, block, 0, () -> "v", v -> 100L);
      keys.add(new long[] { block });
    }
    assertThat(cache.usedBytes()).isGreaterThan(2000);
    budget[0] = 16L * 250;
    for (final long[] k : keys)
      cache.get(1, k[0], 0, () -> "v", v -> 100L);
    assertThat(cache.usedBytes()).isLessThanOrEqualTo(16L * 250);
    assertThat(cache.usedBytes()).isEqualTo(cache.recountBytes());
  }

  @Test
  void zeroBudgetEmptiesTouchedStripesAndServesNoHits() {
    final long[] budget = { 1_000_000L };
    final BlockCache cache = new BlockCache(() -> budget[0], 16);
    for (int block = 0; block < 200; block++)
      cache.get(1, block, 0, () -> "v", v -> 10L);
    assertThat(cache.usedBytes()).isPositive();
    final long hitsBefore = cache.hits();
    budget[0] = 0;
    for (int block = 0; block < 200; block++)
      assertThat(cache.get(1, block, 0, () -> "fresh", v -> 10L)).isEqualTo("fresh");
    assertThat(cache.usedBytes()).isZero();
    assertThat(cache.hits()).isEqualTo(hitsBefore);
  }

  @Test
  void loadInFlightDuringInvalidateFileIsNotAdmitted() throws Exception {
    final BlockCache cache = new BlockCache(() -> 100_000L, 4);
    final CountDownLatch loading = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    final String[] result = new String[1];
    final Thread loader = new Thread(() -> result[0] = cache.get(1, 1, 0, () -> {
      loading.countDown();
      try {
        release.await(60, TimeUnit.SECONDS);
      } catch (final InterruptedException e) {
        throw new IllegalStateException(e);
      }
      return "stale";
    }, v -> 10L));
    loader.start();
    assertThat(loading.await(60, TimeUnit.SECONDS)).isTrue();
    cache.invalidateFile(1);
    release.countDown();
    loader.join(60_000);
    assertThat(loader.isAlive()).isFalse();
    assertThat(result[0]).isEqualTo("stale");
    assertThat(cache.usedBytes()).isZero();
    assertThat(cache.get(1, 1, 0, () -> "fresh", v -> 10L)).isEqualTo("fresh");
  }

  @Test
  void negativeSizeIsRefusedWithoutStateChange() {
    final BlockCache cache = new BlockCache(() -> 100_000L, 4);
    assertThatThrownBy(() -> cache.get(1, 1, 0, () -> "v", v -> -1L)).isInstanceOf(IllegalArgumentException.class);
    assertThat(cache.usedBytes()).isZero();
    assertThat(cache.recountBytes()).isZero();
  }

  @Test
  void zeroSizeEntriesAreChargedOverheadAndEvictable() {
    final BlockCache cache = new BlockCache(() -> 3 * BlockCache.ENTRY_OVERHEAD, 1);
    for (int block = 0; block < 10; block++)
      cache.get(1, block, 0, () -> "v", v -> 0L);
    assertThat(cache.usedBytes()).isEqualTo(3 * BlockCache.ENTRY_OVERHEAD);
    assertThat(cache.recountBytes()).isEqualTo(3 * BlockCache.ENTRY_OVERHEAD);
  }

  @Test
  void concurrentAccessStaysWithinBudgetAndExact() throws Exception {
    final long budget = 1024L * 1024;
    final BlockCache cache = new BlockCache(() -> budget, 16);
    final int threads = 8;
    final ExecutorService pool = Executors.newFixedThreadPool(threads);
    final CountDownLatch start = new CountDownLatch(1);
    final AtomicLong maxSeen = new AtomicLong();
    final List<Future<?>> futures = new ArrayList<>();
    try {
      for (int t = 0; t < threads; t++) {
        final int index = t;
        futures.add(pool.submit(() -> {
          final Random random = new Random(index);
          start.await();
          for (int op = 0; op < 20_000; op++) {
            final long file = 1 + random.nextInt(3);
            if (random.nextInt(500) == 0) {
              cache.invalidateFile(file);
              continue;
            }
            final long block = random.nextInt(500);
            final int column = random.nextInt(4);
            final int size = 1 + random.nextInt(64 * 1024);
            final byte[] value = cache.get(file, block, column, () -> new byte[1], v -> size);
            assertThat(value).isNotNull();
            final long used = cache.usedBytes();
            maxSeen.accumulateAndGet(used, Math::max);
          }
          return null;
        }));
      }
      start.countDown();
      for (final Future<?> f : futures)
        f.get(120, TimeUnit.SECONDS);
    } finally {
      pool.shutdown();
      assertThat(pool.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
    }
    assertThat(maxSeen.get()).isLessThanOrEqualTo(budget);
    assertThat(cache.usedBytes()).isLessThanOrEqualTo(budget);
    assertThat(cache.usedBytes()).isEqualTo(cache.recountBytes());
  }

  @Test
  void newFileIdIsUnique() throws Exception {
    final BlockCache cache = new BlockCache(() -> 1000L, 1);
    final int threads = 4;
    final long[][] ids = new long[threads][2500];
    final Thread[] workers = new Thread[threads];
    for (int t = 0; t < threads; t++) {
      final int index = t;
      workers[t] = new Thread(() -> {
        for (int i = 0; i < 2500; i++)
          ids[index][i] = cache.newFileId();
      });
      workers[t].start();
    }
    for (final Thread w : workers) {
      w.join(60_000);
      assertThat(w.isAlive()).isFalse();
    }
    final Set<Long> all = new HashSet<>();
    for (final long[] row : ids)
      for (final long id : row) {
        assertThat(id).isPositive();
        all.add(id);
      }
    assertThat(all).hasSize(10_000);
  }

  @Test
  void sizeNearLongMaxIsReturnedUncachedWithoutOverflowingTheCharge() {
    final BlockCache cache = new BlockCache(() -> 100_000L, 1);
    cache.get(1, 1, 0, () -> "kept", v -> 100L);
    final long before = cache.usedBytes();
    final String loaded = cache.get(1, 2, 0, () -> "huge", v -> Long.MAX_VALUE - 10);
    assertThat(loaded).isEqualTo("huge");
    assertThat(cache.usedBytes()).isEqualTo(before);
    assertThat(cache.usedBytes()).isEqualTo(cache.recountBytes());
  }
}
