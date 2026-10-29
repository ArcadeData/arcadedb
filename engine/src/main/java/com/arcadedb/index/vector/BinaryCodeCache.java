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
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.index.vector;

import io.github.jbellis.jvector.vector.VectorSimilarityFunction;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Arrays;

/**
 * The one-bit codes of a BINARY index, held on the heap by vector id so a search scores a graph node with a Hamming
 * distance over a few words instead of a float similarity over the reconstructed vector (issue #10968).
 * <p>
 * Scoring the reconstruction cost what an unquantized index costs: a {@code float[dimensions]} per node, cached or read
 * from its page and rebuilt, then a similarity over every component. The code of a 1536-dimension vector is 24 longs,
 * and the Hamming distance between two of them is 24 XORs and popcounts. The Hamming distance is not an approximation
 * of the reconstruction's similarity, it IS that similarity: the reconstruction is the sign of every component at unit
 * norm, so two of them have {@code cosine = 1 - 2 * hamming / dimensions} (see {@link #similarity}).
 * <p>
 * <b>Layout.</b> Direct-mapped by vector id over three flat primitive arrays, the codes, the vector id each slot holds
 * and one version per slot, so the cache adds no object per vector for the collector to trace. Vector ids are dense and
 * monotonic, so with a capacity at least the id range every code stays resident; past it, a later id takes the slot of an
 * earlier one. Like {@link VectorCache}, it is a cache and not a store: a miss is answered by the index pages, and the
 * owner clears it whenever vector ids change meaning and evicts the id of a deleted vector.
 * <p>
 * <b>Concurrency.</b> Lock-free, a seqlock per slot. The version is even while the slot is stable and odd while a writer
 * owns it: a writer claims the slot with a CAS from even to odd, writes the id and the code, and publishes the next even
 * version with release semantics; a writer that loses the CAS simply does not cache. A reader reads the version, the id
 * and the words, and reads the version again after an acquire fence: any write in between moved it, so a torn code is
 * never scored, it is reported as a miss. The version, not the id, is what tells two writes apart: a slot that went from
 * one vector to another and back while a reader was in it holds the same id but not the same version (issue #10968, the
 * first cut keyed the check on the id alone and handed out torn codes under exactly that race).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class BinaryCodeCache {
  private static final VarHandle VERSIONS = MethodHandles.arrayElementVarHandle(int[].class);
  private static final int       EMPTY    = -1;

  private final int    words;
  private final int    mask;
  // Even: the slot is stable. Odd: a writer owns it
  private final int[]  versions;
  // The vector id whose code the slot holds, EMPTY for none
  private final int[]  ids;
  private final long[] codes;

  /**
   * @param dimensions        the width of the vectors
   * @param requestedCapacity the vectors to hold, rounded up to a power of two
   */
  BinaryCodeCache(final int dimensions, final int requestedCapacity) {
    this.words = wordsFor(dimensions);
    final int capacity = tableSizeFor(Math.max(1, requestedCapacity), words);
    this.mask = capacity - 1;
    this.versions = new int[capacity];
    this.ids = new int[capacity];
    Arrays.fill(ids, EMPTY);
    this.codes = new long[capacity * words];
  }

  /** @return the longs one code takes. */
  static int wordsFor(final int dimensions) {
    return (dimensions + 63) >>> 6;
  }

  /** @return the heap one cached code costs: its words, its id and its version. */
  static long bytesPerCode(final int dimensions) {
    return (long) wordsFor(dimensions) * Long.BYTES + 2L * Integer.BYTES;
  }

  int capacity() {
    return ids.length;
  }

  /**
   * @return the Hamming distance between {@code query} and the cached code of {@code vectorId}, or {@code -1} when the
   * code is not resident
   */
  int hammingDistance(final int vectorId, final long[] query) {
    if (vectorId < 0)
      return -1;
    final int slot = vectorId & mask;
    final int version = (int) VERSIONS.getAcquire(versions, slot);
    if ((version & 1) != 0 || ids[slot] != vectorId)
      return -1;

    final int base = slot * words;
    int distance = 0;
    for (int w = 0; w < words; w++)
      distance += Long.bitCount(query[w] ^ codes[base + w]);

    // The id and the words must have been read before the version is read again: a writer that started in between moved it
    VarHandle.acquireFence();
    return (int) VERSIONS.getOpaque(versions, slot) == version ? distance : -1;
  }

  /** Caches the code of {@code vectorId}, {@link #wordsFor} longs of {@code code} from {@code offset}. */
  void put(final int vectorId, final long[] code, final int offset) {
    if (vectorId < 0)
      return;
    final int slot = vectorId & mask;
    final int version = (int) VERSIONS.getVolatile(versions, slot);
    if ((version & 1) != 0 || ids[slot] == vectorId || !VERSIONS.compareAndSet(versions, slot, version, version + 1))
      return;
    ids[slot] = vectorId;
    System.arraycopy(code, offset, codes, slot * words, words);
    VERSIONS.setRelease(versions, slot, version + 2);
  }

  /**
   * Drops the code of {@code vectorId}, if resident: called when the vector is deleted. Unlike {@link #put} it does not give
   * up on a slot a writer owns, it waits for the writer, so a deleted vector cannot keep a code a lost race left behind.
   */
  void remove(final int vectorId) {
    if (vectorId < 0)
      return;
    final int slot = vectorId & mask;
    while (true) {
      final int version = (int) VERSIONS.getVolatile(versions, slot);
      if ((version & 1) != 0) {
        Thread.onSpinWait();
        continue;
      }
      if (ids[slot] != vectorId)
        return;
      if (VERSIONS.compareAndSet(versions, slot, version, version + 1)) {
        ids[slot] = EMPTY;
        VERSIONS.setRelease(versions, slot, version + 2);
        return;
      }
    }
  }

  /**
   * Drops every code. Called under the owner's write lock, when vector ids change meaning: no search, and so no writer, runs
   * meanwhile.
   */
  void clear() {
    Arrays.fill(ids, EMPTY);
    VarHandle.fullFence();
  }

  /** @return the codes resident. O(capacity): for diagnostics and tests only. */
  int size() {
    int count = 0;
    for (final int id : ids)
      if (id != EMPTY)
        ++count;
    return count;
  }

  /**
   * Packs a row as the index pages store it - bit {@code i} of byte {@code i / 8} set when component {@code i} is at or
   * above the vector's median - into the code that scores like its reconstruction. The bits are the same, words are
   * little-endian over the bytes, with one exception: every bit set is how a constant vector is stored, and it
   * reconstructs to the sign of that constant, so a negative constant becomes every bit clear (see
   * {@link QuantizedBuildVectors#dequantizeBinary}).
   *
   * @param packed     the bytes of the row
   * @param offset     where the row starts in {@code packed}
   * @param dimensions the components of the vector, i.e. the bits of the row
   * @param median     the median the row was quantized against
   * @param code       receives {@link #wordsFor} longs at {@code codeOffset}
   */
  static void pack(final byte[] packed, final int offset, final int dimensions, final float median, final long[] code,
      final int codeOffset) {
    final int words = wordsFor(dimensions);
    final int bytes = (dimensions + 7) >>> 3;
    for (int w = 0; w < words; w++) {
      long word = 0L;
      final int first = w << 3;
      final int last = Math.min(bytes, first + 8);
      for (int b = first; b < last; b++)
        word |= (packed[offset + b] & 0xFFL) << ((b - first) << 3);
      code[codeOffset + w] = word;
    }
    // Bits past the last component are padding: cleared, so they never count in a distance
    final int tail = dimensions & 63;
    if (tail != 0)
      code[codeOffset + words - 1] &= (1L << tail) - 1;

    if (median < 0 && allSet(code, codeOffset, dimensions))
      for (int w = 0; w < words; w++)
        code[codeOffset + w] = 0L;
  }

  private static boolean allSet(final long[] code, final int offset, final int dimensions) {
    final int full = dimensions >>> 6;
    for (int w = 0; w < full; w++)
      if (code[offset + w] != -1L)
        return false;
    final int tail = dimensions & 63;
    return tail == 0 || code[offset + full] == (1L << tail) - 1;
  }

  /** @return the Hamming distance between two codes of {@code words} longs. */
  static int hammingDistance(final long[] a, final long[] b, final int words) {
    int distance = 0;
    for (int w = 0; w < words; w++)
      distance += Long.bitCount(a[w] ^ b[w]);
    return distance;
  }

  /**
   * The similarity {@code function} gives two reconstructions whose codes are {@code hamming} bits apart. Both are unit
   * vectors whose components are all {@code ±1/sqrt(dimensions)}, so their cosine (and their dot product) is
   * {@code 1 - 2 * hamming / dimensions} and their squared distance {@code 4 * hamming / dimensions}: the same number
   * {@code function.compare} returns for them, without building either.
   */
  static float similarity(final VectorSimilarityFunction function, final int hamming, final int dimensions) {
    final float cosine = 1.0f - 2.0f * hamming / dimensions;
    if (function == VectorSimilarityFunction.EUCLIDEAN)
      return 1.0f / (1.0f + 2.0f * (1.0f - cosine));
    return (1.0f + cosine) / 2.0f;
  }

  /** The power of two at or above {@code size}, kept so the code array stays addressable by an int. */
  private static int tableSizeFor(final int size, final int words) {
    final int limit = Integer.highestOneBit(Math.max(1, (Integer.MAX_VALUE - 8) / words));
    if (size >= limit)
      return limit;
    return size <= 1 ? 1 : Integer.highestOneBit(size - 1) << 1;
  }
}
