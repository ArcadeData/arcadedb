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
 * The filter answers "certainly absent" or "possibly present": a false positive costs a wasted lookup, a false
 * negative would hide data, so every guarantee below exists to make the second impossible.
 * <p>
 * #4960 hardening:
 * <ul>
 *   <li>the bit index is always reduced modulo {@code capacity}: the previous conditional reduction let
 *   a hash equal to {@code capacity} address one bit PAST the region, corrupting the adjacent byte of
 *   the shared buffer;</li>
 *   <li>{@link #add} is synchronized: the unsynchronized read-modify-write on shared bytes could drop a
 *   concurrently-set bit, turning into a FALSE NEGATIVE. {@link #mightContain} stays lock-free (a filter is
 *   built, then published for reading).</li>
 * </ul>
 * <p>
 * Later hardening, before wiring it into a read path:
 * <ul>
 *   <li>the number of probes is configurable instead of fixed at two, because two is far from optimal at the
 *   sizes an index would use: at 10 bits per entry two probes give ~3.3% false positives against ~0.8% for the
 *   optimal seven. Use {@link #slotsFor} and {@link #probesFor} rather than guessing;</li>
 *   <li>hashing an {@code int} no longer allocates a 4-byte array per operation - unacceptable on a lookup path -
 *   while producing exactly the hash the array would have produced;</li>
 *   <li>keys of arbitrary bytes are supported, since the keys an index needs to filter are rarely ints.</li>
 * </ul>
 * <p>
 * Invariant: the backing {@code buffer} must be at least {@code ceil(slots / 8)} bytes (validated by
 * the constructor). The {@code floorMod} reduction can address bit {@code slots - 1}, i.e. the top byte
 * of the region, so an undersized buffer would read/write past the region on the highest slots.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class BufferBloomFilter {
  private static final long MURMUR_M     = 0xc6a4a7935bd1e995L;
  private static final int  MURMUR_R     = 47;
  private static final int  DEFAULT_PROBES = 2;

  private final Binary buffer;
  private final int    hashSeed;
  private final int    capacity;
  private final int    probes;

  /**
   * Builds a filter with the historical two probes. Prefer the constructor that takes the probe count together
   * with {@link #slotsFor} / {@link #probesFor}, which size the filter for a target false-positive rate.
   */
  public BufferBloomFilter(final Binary buffer, final int slots, final int hashSeed) {
    this(buffer, slots, hashSeed, DEFAULT_PROBES);
  }

  /**
   * Builds a filter over the first {@code ceil(slots / 8)} bytes of {@code buffer}.
   * <p>
   * Publication requirement: {@link #mightContain} is lock-free, so once the build phase is over the
   * filter instance MUST be handed to readers through a safe-publication edge - a {@code final} or
   * {@code volatile} field, or a happens-before established by a lock or a concurrent collection.
   * Publishing it through a plain field lets a reader observe stale buffer bytes and return a FALSE
   * NEGATIVE, the one failure a bloom filter must never have.
   *
   * @param probes how many bits each key sets, i.e. the {@code k} of the standard formulas
   *
   * @throws IllegalArgumentException if {@code slots} is not a multiple of 8, if {@code probes} is below 1, or if
   *                                  {@code buffer} cannot address the {@code ceil(slots / 8)} bytes the filter spans
   */
  public BufferBloomFilter(final Binary buffer, final int slots, final int hashSeed, final int probes) {
    if (slots % 8 > 0)
      throw new IllegalArgumentException("Slots must be a multiplier of 8");

    if (probes < 1)
      throw new IllegalArgumentException("A bloom filter needs at least one probe, but " + probes + " was requested");

    final int requiredBytes = (slots + 7) / 8;
    if (buffer.limit() < requiredBytes)
      throw new IllegalArgumentException(
          "Buffer too small for " + slots + " slots: addressable bytes " + buffer.limit() + ", required " + requiredBytes);

    this.buffer = buffer;
    this.hashSeed = hashSeed;
    this.capacity = slots;
    this.probes = probes;
  }

  public synchronized void add(final int value) {
    addHash(hash64(value));
  }

  /**
   * Adds the first {@code length} bytes of {@code key}. The keys an index filters are rarely ints, and copying them
   * into an int would throw away the very entropy the filter needs.
   */
  public synchronized void add(final byte[] key, final int length) {
    addHash(MurmurHash.hash64(key, length, hashSeed));
  }

  /**
   * Lock-free read: only safe after the filter has been fully built and safely published (no concurrent
   * {@link #add}s). Without a happens-before edge between a concurrent add and this read, a stale byte
   * could be observed and produce a false negative.
   */
  public boolean mightContain(final int value) {
    return mightContainHash(hash64(value));
  }

  public boolean mightContain(final byte[] key, final int length) {
    return mightContainHash(MurmurHash.hash64(key, length, hashSeed));
  }

  public int getSlots() {
    return capacity;
  }

  public int getProbes() {
    return probes;
  }

  /**
   * Bits a filter needs to hold {@code expectedEntries} with at most {@code falsePositiveRate} false positives,
   * rounded up to the multiple of 8 the constructor requires: {@code m = -n ln(p) / ln(2)^2}.
   */
  public static int slotsFor(final long expectedEntries, final double falsePositiveRate) {
    if (expectedEntries < 1)
      throw new IllegalArgumentException("Expected entries must be at least 1, but was " + expectedEntries);
    if (falsePositiveRate <= 0 || falsePositiveRate >= 1)
      throw new IllegalArgumentException("The false positive rate must be between 0 and 1, but was " + falsePositiveRate);

    final double bits = -expectedEntries * Math.log(falsePositiveRate) / (Math.log(2) * Math.log(2));
    final long slots = ((long) Math.ceil(bits) + 7) / 8 * 8;
    if (slots > Integer.MAX_VALUE - 7)
      throw new IllegalArgumentException(
          "A filter for " + expectedEntries + " entries at " + falsePositiveRate + " needs " + slots + " bits, too many "
              + "to address");
    return (int) Math.max(8, slots);
  }

  /**
   * Probes that minimise the false-positive rate of a filter of {@code slots} bits holding {@code expectedEntries}:
   * {@code k = (m/n) ln 2}.
   */
  public static int probesFor(final int slots, final long expectedEntries) {
    if (expectedEntries < 1)
      throw new IllegalArgumentException("Expected entries must be at least 1, but was " + expectedEntries);

    return Math.max(1, (int) Math.round((double) slots / expectedEntries * Math.log(2)));
  }

  /**
   * False positives this filter is expected to return once it holds {@code entries}, i.e.
   * {@code (1 - e^(-kn/m))^k}. Diagnostics only: what a filter actually costs is decided by the real key
   * distribution.
   */
  public double expectedFalsePositiveRate(final long entries) {
    return Math.pow(1 - Math.exp(-(double) probes * entries / capacity), probes);
  }

  private void addHash(final long hash) {
    final int first = (int) (hash >>> 32);
    // Forced odd so that on a power-of-two capacity the probes never walk the same short cycle.
    final int step = (int) hash | 1;
    for (int i = 0; i < probes; i++)
      setBit(Math.floorMod(first + (long) i * step, capacity));
  }

  private boolean mightContainHash(final long hash) {
    final int first = (int) (hash >>> 32);
    final int step = (int) hash | 1;
    for (int i = 0; i < probes; i++)
      if (!testBit(Math.floorMod(first + (long) i * step, capacity)))
        return false;
    return true;
  }

  /**
   * {@code MurmurHash.hash64} of the four big-endian bytes of {@code value}, without materialising them: this runs
   * on every lookup, and an array per call is not something a read path can afford. The equivalence with the array
   * form is pinned by a test.
   */
  private long hash64(final int value) {
    long h = (hashSeed & 0xffffffffL) ^ (4 * MURMUR_M);

    h ^= (long) (value & 0xff) << 24;
    h ^= (long) ((value >>> 8) & 0xff) << 16;
    h ^= (long) ((value >>> 16) & 0xff) << 8;
    h ^= (value >>> 24) & 0xff;
    h *= MURMUR_M;

    h ^= h >>> MURMUR_R;
    h *= MURMUR_M;
    h ^= h >>> MURMUR_R;

    return h;
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
