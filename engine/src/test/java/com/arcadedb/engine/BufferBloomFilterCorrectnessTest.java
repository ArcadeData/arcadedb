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

  @Test
  void slotsMustBeAMultipleOfEightAndProbesAtLeastOne() {
    assertThatThrownBy(() -> new BufferBloomFilter(new Binary(128), 100, 23))
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("multiplier of 8");
    assertThatThrownBy(() -> new BufferBloomFilter(new Binary(128), 64, 23, 0))
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("at least one probe");
  }

  /**
   * The filter shares its buffer with whatever the caller put around it - a page header, the next filter of the
   * same page - so it must never touch a byte outside the {@code ceil(slots / 8)} it was given. This is the
   * failure #4960 was about, and the only way to see it is to watch the bytes beyond the region.
   */
  @Test
  void neverWritesOutsideItsRegion() {
    final int slots = 64;
    final int region = slots / 8;
    final Binary buffer = new Binary(region * 4);
    buffer.size(region * 4);

    final byte guard = (byte) 0xA5;
    for (int i = region; i < region * 4; i++)
      buffer.putByte(i, guard);

    final BufferBloomFilter bf = new BufferBloomFilter(buffer, slots, 23, 8);
    for (int i = 0; i < 100_000; i++)
      bf.add(i);

    for (int i = region; i < region * 4; i++)
      assertThat(buffer.getByte(i)).as("byte %d past the %d-byte region", i, region).isEqualTo(guard);
  }

  /**
   * A filter is worth persisting only if the same bytes answer the same way when read back, so the outcome must
   * depend on nothing but the buffer, the slot count, the seed and the probes.
   */
  @Test
  void theSameBytesAnswerTheSameWay() {
    final int slots = 4_096;
    final Binary buffer = new Binary(slots / 8);
    buffer.size(slots / 8);

    final BufferBloomFilter writer = new BufferBloomFilter(buffer, slots, 23, 4);
    for (int i = 0; i < 500; i++)
      writer.add(i);

    // Same region, rebuilt as a reader would after loading the page.
    final Binary reloaded = new Binary(buffer.toByteArray());
    final BufferBloomFilter reader = new BufferBloomFilter(reloaded, slots, 23, 4);

    for (int i = 0; i < 500; i++)
      assertThat(reader.mightContain(i)).as("value %d", i).isTrue();

    int agreed = 0;
    for (int i = 500; i < 5_000; i++)
      if (writer.mightContain(i) == reader.mightContain(i))
        ++agreed;
    assertThat(agreed).isEqualTo(4_500);

    // A different seed is a different filter: the seed has to reach the hash.
    final BufferBloomFilter otherSeed = new BufferBloomFilter(reloaded, slots, 24, 4);
    int same = 0;
    for (int i = 0; i < 500; i++)
      if (otherSeed.mightContain(i))
        ++same;
    assertThat(same).as("a filter read with the wrong seed cannot report every key as present").isLessThan(500);
  }

  @Test
  void anEmptyFilterHoldsNothing() {
    final int slots = 4_096;
    final BufferBloomFilter bf = new BufferBloomFilter(new Binary(slots / 8), slots, 23, 4);

    for (int i = 0; i < 10_000; i++)
      assertThat(bf.mightContain(i)).as("value %d", i).isFalse();
  }

  /**
   * Hashing an int must not allocate on a read path, and the allocation-free form must produce exactly the hash the
   * 4-byte array produced before - otherwise a filter written by one and read by the other silently loses keys.
   */
  @Test
  void theIntHashMatchesTheByteArrayItReplaces() {
    final int slots = 1 << 14;
    final int seed = 23;
    final BufferBloomFilter viaInt = new BufferBloomFilter(new Binary(slots / 8), slots, seed, 4);
    final BufferBloomFilter viaBytes = new BufferBloomFilter(new Binary(slots / 8), slots, seed, 4);

    for (int i = -2_000; i < 2_000; i++) {
      viaInt.add(i);
      viaBytes.add(new byte[] { (byte) (i >>> 24), (byte) (i >>> 16), (byte) (i >>> 8), (byte) i }, 4);
    }

    for (int i = -2_000; i < 2_000; i++)
      assertThat(viaInt.mightContain(i)).as("value %d", i)
          .isEqualTo(viaBytes.mightContain(new byte[] { (byte) (i >>> 24), (byte) (i >>> 16), (byte) (i >>> 8), (byte) i }, 4));

    // The two filters must be bit-identical, not merely in agreement on the keys they hold.
    for (int i = 2_000; i < 20_000; i++)
      assertThat(viaInt.mightContain(i)).as("absent value %d", i).isEqualTo(
          viaBytes.mightContain(new byte[] { (byte) (i >>> 24), (byte) (i >>> 16), (byte) (i >>> 8), (byte) i }, 4));
  }
}
