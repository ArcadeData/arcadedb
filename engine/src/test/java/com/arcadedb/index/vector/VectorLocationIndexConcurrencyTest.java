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

/**
 * Regression test for issue #4515.
 * <p>
 * In bounded mode ({@code maxSize > 0}) the index was backed by a
 * {@code Collections.synchronizedMap(new LinkedHashMap<>(.., .., true))} with
 * {@code accessOrder=true}. Every {@code get()} became a structural
 * modification, and the streaming methods iterated {@code keySet()}/
 * {@code values()} without holding the wrapper monitor while calling
 * {@code get()} inside the pipeline, so concurrent read/iterate load threw
 * {@link java.util.ConcurrentModificationException} or silently corrupted the
 * underlying linked list.
 */
class VectorLocationIndexConcurrencyTest {

  /**
   * Drives concurrent reads, iteration and counting against a bounded index.
   * Before the fix this reliably throws ConcurrentModificationException from
   * the streaming methods (or corrupts the map). After the fix it must run
   * cleanly.
   */
  @Test
  void concurrentIterationAndAccessDoesNotThrow() throws Exception {
    final int maxSize = 256;
    final VectorLocationIndex index = new VectorLocationIndex(maxSize, maxSize);

    // Pre-populate the bounded cache up to (and beyond) its capacity so that
    // eviction is active and the linked list is fully exercised.
    for (int i = 0; i < maxSize; i++)
      index.addVector(false, i * 24L, new RID(1, i));

    final int threads = 8;
    final int iterationsPerThread = 5_000;
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
            final int id = (threadId * 31 + i) % maxSize;
            switch (i % 4) {
            case 0 -> index.getLocation(id);                       // mutating get under accessOrder
            case 1 -> index.getActiveVectorIds().count();          // iterate keySet + get inside filter
            case 2 -> index.getAllVectorIds().count();             // iterate keySet
            default -> index.getActiveCount();                     // iterate values
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
        .as("no exception during concurrent access/iteration; first error: %s",
            errors.isEmpty() ? "none" : errors.peek())
        .isFalse();
  }

  /**
   * Eviction must remain insertion-order (FIFO, oldest-inserted first) after
   * dropping access-order, and the map must never exceed maxSize.
   */
  @Test
  void boundedIndexRespectsMaxSizeAndEvictsOldest() {
    final int maxSize = 4;
    final VectorLocationIndex index = new VectorLocationIndex(maxSize, maxSize);

    final int[] ids = new int[8];
    for (int i = 0; i < ids.length; i++)
      ids[i] = index.addVector(false, i, new RID(1, i));

    assertThat(index.size()).isEqualTo(maxSize);

    // The first 4 inserted entries must have been evicted.
    for (int i = 0; i < 4; i++)
      assertThat(index.getLocation(ids[i])).as("evicted oldest entry %d", i).isNull();

    // The last 4 inserted entries must still be present.
    for (int i = 4; i < 8; i++)
      assertThat(index.getLocation(ids[i])).as("retained recent entry %d", i).isNotNull();
  }

  /**
   * Unlimited mode is backed by a ConcurrentHashMap and uses the lazy,
   * non-snapshotting stream path. Verify it stays consistent under concurrent
   * reads/iteration without throwing.
   */
  @Test
  void unlimitedModeConcurrentIterationDoesNotThrow() throws Exception {
    final VectorLocationIndex index = new VectorLocationIndex(); // unlimited (ConcurrentHashMap)

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
        .as("no exception during unlimited-mode concurrent access; first error: %s",
            errors.isEmpty() ? "none" : errors.peek())
        .isFalse();
  }

  /**
   * getActiveVectorIds must exclude tombstoned entries; getActiveCount must
   * match.
   */
  @Test
  void activeVectorIdsExcludeDeleted() {
    final VectorLocationIndex index = new VectorLocationIndex(64, 64);

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
    assertThat(index.getAllVectorIds().count()).isEqualTo(3);
  }
}
