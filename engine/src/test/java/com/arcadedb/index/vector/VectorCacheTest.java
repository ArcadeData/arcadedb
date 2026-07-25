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
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for the bounded, boxing-free vector cache introduced with issue #5412.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class VectorCacheTest {
  private static final VectorTypeSupport VTS = VectorizationProvider.getInstance().getVectorTypeSupport();

  @Test
  void storesAndReturnsVectors() {
    final VectorCache cache = new VectorCache(64);

    assertThat(cache.get(1)).isNull();

    cache.put(1, vector(1));
    cache.put(2, vector(2));

    assertThat(cache.get(1).get(0)).isEqualTo(1f);
    assertThat(cache.get(2).get(0)).isEqualTo(2f);
    assertThat(cache.get(3)).isNull();
  }

  @Test
  void capacityIsRoundedUpToAWholeNumberOfBuckets() {
    assertThat(new VectorCache(1).capacity()).isEqualTo(2);
    assertThat(new VectorCache(100).capacity()).isEqualTo(128);
    assertThat(new VectorCache(1024).capacity()).isEqualTo(1024);
    assertThat(new VectorCache(1025).capacity()).isEqualTo(2048);
  }

  @Test
  void denseMonotonicIdsUpToCapacityStayFullyResident() {
    // Vector ids are dense and monotonic, so the identity mapping must keep every one of them resident
    // when the cache is at least as large as the id range: this is the whole point of not hashing the key.
    final int capacity = 1024;
    final VectorCache cache = new VectorCache(capacity);

    for (int i = 0; i < capacity; i++)
      cache.put(i, vector(i));

    for (int i = 0; i < capacity; i++)
      assertThat(cache.get(i)).as("vector %d", i).isNotNull();

    assertThat(cache.size()).isEqualTo(capacity);
  }

  @Test
  void neverGrowsPastItsCapacity() {
    final VectorCache cache = new VectorCache(256);

    for (int i = 0; i < 100_000; i++)
      cache.put(i, vector(i));

    assertThat(cache.size()).isLessThanOrEqualTo(cache.capacity());
    // The most recent insertions win: eviction adapts to the working set instead of freezing on the first
    // vectors seen, which is what the old "stop inserting when full" policy did.
    assertThat(cache.get(99_999)).isNotNull();
  }

  @Test
  void removeDropsTheEntry() {
    final VectorCache cache = new VectorCache(64);
    cache.put(7, vector(7));
    assertThat(cache.get(7)).isNotNull();

    cache.remove(7);
    assertThat(cache.get(7)).isNull();
  }

  @Test
  void clearDropsEverything() {
    final VectorCache cache = new VectorCache(64);
    for (int i = 0; i < 64; i++)
      cache.put(i, vector(i));

    cache.clear();

    assertThat(cache.size()).isZero();
  }

  @Test
  void ignoresNegativeIdsAndNullVectors() {
    final VectorCache cache = new VectorCache(64);

    cache.put(-1, vector(1));
    cache.put(1, null);

    assertThat(cache.get(-1)).isNull();
    assertThat(cache.get(1)).isNull();
    assertThat(cache.size()).isZero();
  }

  @Test
  void tracksHitsAndMisses() {
    final VectorCache cache = new VectorCache(64);
    cache.put(1, vector(1));

    cache.get(1);
    cache.get(1);
    cache.get(99);

    assertThat(cache.getHits()).isEqualTo(2);
    assertThat(cache.getMisses()).isEqualTo(1);
  }

  @Test
  void neverReturnsAVectorForTheWrongIdUnderConcurrency() throws Exception {
    final int capacity = 512;
    final int threads = 8;
    final int iterations = 20_000;
    final VectorCache cache = new VectorCache(capacity);

    final CountDownLatch start = new CountDownLatch(1);
    final CountDownLatch done = new CountDownLatch(threads);
    final AtomicInteger mismatches = new AtomicInteger();

    for (int t = 0; t < threads; t++) {
      final int seed = t;
      new Thread(() -> {
        try {
          start.await();
          for (int i = 0; i < iterations; i++) {
            final int id = (i * 31 + seed) % (capacity * 4);
            final VectorFloat<?> found = cache.get(id);
            if (found != null && found.get(0) != (float) id)
              mismatches.incrementAndGet();
            else if (found == null)
              cache.put(id, vector(id));
          }
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        } finally {
          done.countDown();
        }
      }).start();
    }

    start.countDown();
    assertThat(done.await(60, TimeUnit.SECONDS)).isTrue();
    assertThat(mismatches.get()).isZero();
    assertThat(cache.size()).isLessThanOrEqualTo(cache.capacity());
  }

  private static VectorFloat<?> vector(final int id) {
    return VTS.createFloatVector(new float[] { id, id, id, id });
  }
}
