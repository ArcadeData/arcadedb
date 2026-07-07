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
package com.arcadedb.server.security;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

class ConcurrentSaltCacheTest {

  @Test
  void shouldStoreAndRetrieveValues() {
    final ConcurrentSaltCache cache = new ConcurrentSaltCache(8);

    assertThat(cache.get("k1")).isNull();

    cache.put("k1", "v1");
    cache.put("k2", "v2");

    assertThat(cache.get("k1")).isEqualTo("v1");
    assertThat(cache.get("k2")).isEqualTo("v2");
    assertThat(cache.size()).isEqualTo(2);
    assertThat(cache.isEmpty()).isFalse();
  }

  @Test
  void shouldOverwriteValueForSameKeyWithoutGrowing() {
    final ConcurrentSaltCache cache = new ConcurrentSaltCache(8);

    cache.put("k1", "v1");
    cache.put("k1", "v2");

    assertThat(cache.get("k1")).isEqualTo("v2");
    assertThat(cache.size()).isEqualTo(1);
  }

  @Test
  void shouldNotOverflowInitialCapacityForVeryLargeMaxSize() {
    // (int) (maxSize / 0.75f) + 1 would wrap negative and crash ConcurrentHashMap without the guard
    final ConcurrentSaltCache cache = new ConcurrentSaltCache(Integer.MAX_VALUE);

    cache.put("k1", "v1");
    assertThat(cache.get("k1")).isEqualTo("v1");
    assertThat(cache.size()).isEqualTo(1);
  }

  @Test
  void shouldBoundSizeByEvictingOldEntries() {
    final int max = 16;
    final ConcurrentSaltCache cache = new ConcurrentSaltCache(max);

    for (int i = 0; i < max * 4; i++)
      cache.put("key-" + i, "value-" + i);

    // approximate-FIFO eviction keeps the map within the configured bound
    assertThat(cache.size()).isLessThanOrEqualTo(max);

    // the most recently inserted key must still be present
    assertThat(cache.get("key-" + (max * 4 - 1))).isEqualTo("value-" + (max * 4 - 1));
  }

  @Test
  void shouldRemainConsistentUnderConcurrentAccess() throws Exception {
    final int max = 128;
    final ConcurrentSaltCache cache = new ConcurrentSaltCache(max);

    final int threads = 16;
    final int iterations = 5_000;
    final ExecutorService pool = Executors.newFixedThreadPool(threads);
    final CountDownLatch start = new CountDownLatch(1);
    final AtomicInteger mismatches = new AtomicInteger();

    for (int t = 0; t < threads; t++) {
      final int base = t;
      pool.submit(() -> {
        try {
          start.await();
          for (int i = 0; i < iterations; i++) {
            final String key = "k-" + base + "-" + (i % max);
            final String value = "v-" + base + "-" + (i % max);
            cache.put(key, value);
            final String read = cache.get(key);
            // a read may miss (evicted) but must never return a stale/wrong value
            if (read != null && !read.equals(value))
              mismatches.incrementAndGet();
          }
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      });
    }

    start.countDown();
    pool.shutdown();
    assertThat(pool.awaitTermination(30, SECONDS)).isTrue();

    assertThat(mismatches.get()).isZero();
    assertThat(cache.size()).isLessThanOrEqualTo(max);
  }
}
