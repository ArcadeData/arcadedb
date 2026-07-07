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

/**
 * Bloom filter over a caller-provided {@link Binary} region of {@code slots} bits.
 * <p>
 * #4960 hardening (before wiring into the LSM read path):
 * <ul>
 *   <li>the bit index is always reduced modulo {@code capacity}: the previous conditional reduction let
 *   a hash equal to {@code capacity} address one bit PAST the region, corrupting the adjacent byte of
 *   the shared buffer;</li>
 *   <li>{@link #add} is synchronized: the unsynchronized read-modify-write on shared bytes could drop a
 *   concurrently-set bit, turning into a FALSE NEGATIVE - the one failure a bloom filter must never
 *   have. {@link #mightContain} stays lock-free (a filter is built, then published for reading);</li>
 *   <li>two probes (k=2) are derived from the two halves of the 64-bit Murmur hash instead of a single
 *   32-bit probe, roughly halving the false-positive exponent at the same size.</li>
 * </ul>
 * <p>
 * Invariant: the backing {@code buffer} must be at least {@code ceil(slots / 8)} bytes (validated by
 * the constructor). The {@code floorMod} reduction can address bit {@code slots - 1}, i.e. the top byte
 * of the region, so an undersized buffer would read/write past the region on the highest slots.
 */
public class BufferBloomFilter {
  private final Binary buffer;
  private final int    hashSeed;
  private final int    capacity;

  /**
   * Builds a filter over the first {@code ceil(slots / 8)} bytes of {@code buffer}.
   * <p>
   * Publication requirement: {@link #mightContain} is lock-free, so once the build phase is over the
   * filter instance MUST be handed to readers through a safe-publication edge - a {@code final} or
   * {@code volatile} field, or a happens-before established by a lock or a concurrent collection.
   * Publishing it through a plain field lets a reader observe stale buffer bytes and return a FALSE
   * NEGATIVE, the one failure a bloom filter must never have.
   *
   * @throws IllegalArgumentException if {@code slots} is not a multiple of 8, or if {@code buffer}
   *                                  cannot address the {@code ceil(slots / 8)} bytes the filter spans
   */
  public BufferBloomFilter(final Binary buffer, final int slots, final int hashSeed) {
    if (slots % 8 > 0)
      throw new IllegalArgumentException("Slots must be a multiplier of 8");

    final int requiredBytes = (slots + 7) / 8;
    if (buffer.limit() < requiredBytes)
      throw new IllegalArgumentException(
          "Buffer too small for " + slots + " slots: addressable bytes " + buffer.limit() + ", required " + requiredBytes);

    this.buffer = buffer;
    this.hashSeed = hashSeed;
    this.capacity = slots;
  }

  public synchronized void add(final int value) {
    final long hash = hash64(value);
    setBit(Math.floorMod((int) (hash >>> 32), capacity));
    setBit(Math.floorMod((int) hash, capacity));
  }

  /**
   * Lock-free read: only safe after the filter has been fully built and safely published (no concurrent
   * {@link #add}s). Without a happens-before edge between a concurrent add and this read, a stale byte
   * could be observed and produce a false negative.
   */
  public boolean mightContain(final int value) {
    final long hash = hash64(value);
    return testBit(Math.floorMod((int) (hash >>> 32), capacity)) && testBit(Math.floorMod((int) hash, capacity));
  }

  private long hash64(final int value) {
    final byte[] b = new byte[] { (byte) (value >>> 24), (byte) (value >>> 16), (byte) (value >>> 8), (byte) value };
    return MurmurHash.hash64(b, 4, hashSeed);
  }

  private void setBit(final int bit) {
    final int byte2change = bit / 8;
    final byte v = buffer.getByte(byte2change);
    buffer.putByte(byte2change, (byte) (v | (1 << (bit % 8))));
  }

  private boolean testBit(final int bit) {
    final byte v = buffer.getByte(bit / 8);
    return ((v >> (bit % 8)) & 1) == 1;
  }
}
