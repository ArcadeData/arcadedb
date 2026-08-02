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

import com.arcadedb.database.RID;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #4515, and the concurrency contract of the one backend that survived it.
 * <p>
 * In bounded mode ({@code maxSize > 0}) the index was backed by a
 * {@code Collections.synchronizedMap(new LinkedHashMap<>(.., .., true))} with {@code accessOrder=true}. Every
 * {@code get()} became a structural modification, and the streaming methods iterated {@code keySet()}/
 * {@code values()} without holding the wrapper monitor while calling {@code get()} inside the pipeline, so
 * concurrent read/iterate load threw {@link java.util.ConcurrentModificationException} or silently corrupted the
 * underlying linked list. #4515 fixed that by dropping access-order and snapshotting under the monitor; issue #5559
 * then removed the bounded backend altogether - it evicted, and an evicted location cannot be recovered - so the
 * failure mode is structurally gone rather than merely guarded, and the case that drove it has nothing left to
 * construct. What remains is the {@link java.util.concurrent.ConcurrentHashMap} backend, exercised below.
 */
class VectorLocationIndexConcurrencyTest {

  /**
   * The surviving backend is a ConcurrentHashMap and uses the lazy, non-snapshotting stream path. Verify it stays
   * consistent under concurrent writes/iteration without throwing.
   */
  @Test
  void concurrentIterationDoesNotThrow() throws Exception {
    final VectorLocationIndex index = new VectorLocationIndex();

    final int seed = 512;
    for (int i = 0; i < seed; i++)
      index.addVector(false, i * 24L, new RID(1, i));

    final int threads = 8;
    final int iterationsPerThread = 3_000;
    final ExecutorService pool = Executors.newFixedThreadPool(threads);
    final CountDownLatch start = new CountDownLatch(1);
    final CountDownLatch done = new CountDownLatch(threads);
    final AtomicBoolean failed = new AtomicBoolean(false);
    final ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();

    for (int t = 0; t < threads; t++) {
      final int threadId = t;
      pool.submit(() -> {
        try {
          start.await();
          for (int i = 0; i < iterationsPerThread; i++) {
            switch (i % 4) {
            case 0 -> index.addVector(false, i, new RID(1, seed + threadId * iterationsPerThread + i));
            case 1 -> index.getActiveVectorIds().count();
            case 2 -> index.getAllVectorIds().count();
            default -> index.getActiveCount();
            }
          }
        } catch (final Throwable e) {
          failed.set(true);
          errors.add(e);
        } finally {
          done.countDown();
        }
      });
    }

    start.countDown();
    final boolean finished = done.await(60, TimeUnit.SECONDS);
    pool.shutdownNow();

    assertThat(finished).as("all worker threads completed within timeout").isTrue();
    assertThat(failed.get())
        .as("no exception during concurrent access; first error: %s", errors.isEmpty() ? "none" : errors.peek())
        .isFalse();
  }

  /**
   * The single-int constructor used to mean {@code maxSize}, where {@code -1} was the "unlimited" sentinel; it now
   * means an initial capacity. A caller carried over from the old signature must not silently get a map sized 0 or
   * a negative-capacity failure deep inside the map - issue #5559.
   */
  @Test
  void theOldUnlimitedSentinelIsRefusedRatherThanReadAsACapacity() {
    assertThatThrownBy(() -> new VectorLocationIndex(-1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("eviction limit");

    assertThat(new VectorLocationIndex(1024).size()).as("a genuine capacity hint is accepted").isZero();
  }

  /**
   * getActiveVectorIds must exclude tombstoned entries; getActiveCount must
   * match.
   */
  @Test
  void activeVectorIdsExcludeDeleted() {
    final VectorLocationIndex index = new VectorLocationIndex(64);

    final int id0 = index.addVector(false, 0, new RID(1, 0));
    final int id1 = index.addVector(false, 24, new RID(1, 1));
    final int id2 = index.addVector(false, 48, new RID(1, 2));

    index.markDeleted(id1);

    final long active = index.getActiveVectorIds().count();
    assertThat(active).isEqualTo(2);
    assertThat(index.getActiveCount()).isEqualTo(2);
    assertThat(index.getActiveVectorIds().anyMatch(id -> id == id1)).isFalse();
    assertThat(index.getActiveVectorIds().anyMatch(id -> id == id0)).isTrue();
    assertThat(index.getActiveVectorIds().anyMatch(id -> id == id2)).isTrue();

    // Since issue #5516 a tombstoned id keeps no resident location: it is tracked as one bit in the deleted-id set.
    assertThat(index.getAllVectorIds().count()).isEqualTo(2);
    assertThat(index.getLocation(id1)).isNull();
    assertThat(index.isDeleted(id1)).isTrue();
    assertThat(index.getDeletedCount()).isEqualTo(1);
  }
}
