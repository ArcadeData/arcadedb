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
package com.arcadedb.engine;

import com.arcadedb.database.Binary;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for the {@link BufferBloomFilter} findings grouped in issue #4960. The class is not
 * wired into the LSM read path yet, but the defects had to be fixed before it can be:
 * <ul>
 *   <li>the bit index could equal {@code capacity} (one bit past the region), corrupting the adjacent
 *   byte of the shared buffer or overflowing it;</li>
 *   <li>{@code add()} was a non-atomic read-modify-write on shared bytes, so concurrent adds could drop
 *   a bit - a FALSE NEGATIVE, the one failure mode a bloom filter must never have;</li>
 *   <li>a single probe (k=1) gave a needlessly high false-positive rate; two probes are now derived
 *   from the two halves of the 64-bit Murmur hash.</li>
 * </ul>
 */
class BufferBloomFilterCorrectnessTest {

  @Test
  void noFalseNegativesSingleThread() {
    final int slots = 1 << 16;
    final BufferBloomFilter bf = new BufferBloomFilter(new Binary(slots / 8), slots, 23);

    for (int i = 0; i < 5_000; i++)
      bf.add(i * 31 + 7);

    for (int i = 0; i < 5_000; i++)
      assertThat(bf.mightContain(i * 31 + 7))
          .as("added value %d must always be reported as possibly present", i * 31 + 7)
          .isTrue();
  }

  /**
   * #4960: before the fix {@code add()} performed get-or-put on the shared byte without any
   * synchronization, so two threads landing on the same byte could each read the same original value
   * and one bit-set overwrote the other. The lost bit turned into a false negative. The interleaving is
   * a race so a single run cannot PROVE its absence, but with 8 threads hammering a small region the
   * unfixed code fails practically always; the fixed code is deterministic.
   */
  @Test
  void concurrentAddsDoNotDropBits() throws Exception {
    final int slots = 8192;
    final BufferBloomFilter bf = new BufferBloomFilter(new Binary(slots / 8), slots, 23);

    final int threads = 8;
    final int perThread = 25_000;
    final CountDownLatch start = new CountDownLatch(1);
    final List<Thread> workers = new ArrayList<>(threads);
    for (int t = 0; t < threads; t++) {
      final int base = t * perThread;
      final Thread worker = new Thread(() -> {
        try {
          start.await();
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          return;
        }
        for (int i = 0; i < perThread; i++)
          bf.add(base + i);
      });
      worker.start();
      workers.add(worker);
    }

    start.countDown();
    for (final Thread worker : workers)
      worker.join();

    for (int v = 0; v < threads * perThread; v++)
      assertThat(bf.mightContain(v))
          .as("concurrently added value %d must never be a false negative", v)
          .isTrue();
  }

  /**
   * #5063 review round 2: the constructor must reject a buffer that cannot address the
   * {@code ceil(slots / 8)} bytes the filter spans, otherwise the highest slots read/write past the
   * region at runtime.
   */
  @Test
  void undersizedBufferIsRejectedAtConstruction() {
    final int slots = 1 << 16;
    assertThatThrownBy(() -> new BufferBloomFilter(new Binary(slots / 8 - 1), slots, 23))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Buffer too small");

    // EXACTLY-SIZED BUFFER IS ACCEPTED AND FULLY USABLE ON THE HIGHEST SLOTS TOO
    final BufferBloomFilter bf = new BufferBloomFilter(new Binary(slots / 8), slots, 23);
    for (int i = 0; i < 1_000; i++)
      bf.add(i);
    for (int i = 0; i < 1_000; i++)
      assertThat(bf.mightContain(i)).isTrue();
  }
}
