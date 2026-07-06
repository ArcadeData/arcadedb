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
 */
public class BufferBloomFilter {
  private final Binary buffer;
  private final int    hashSeed;
  private final int    capacity;

  public BufferBloomFilter(final Binary buffer, final int slots, final int hashSeed) {
    if (slots % 8 > 0)
      throw new IllegalArgumentException("Slots must be a multiplier of 8");
    this.buffer = buffer;
    this.hashSeed = hashSeed;
    this.capacity = slots;
  }

  public synchronized void add(final int value) {
    final long hash = hash64(value);
    setBit(Math.floorMod((int) (hash >>> 32), capacity));
    setBit(Math.floorMod((int) hash, capacity));
  }

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
