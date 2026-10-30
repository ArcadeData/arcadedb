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

import com.arcadedb.utility.VectorApiSupport;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import io.github.jbellis.jvector.vector.VectorUtilSupport;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * The full-precision vectors every search of an index scores, held on the heap by vector id in flat storage and scored
 * where they lie (issue #11003).
 * <p>
 * A graph walk scores every neighbour of every node it expands, so at 128 dimensions the lookup of the operand costs as much
 * as the distance itself. {@link VectorCache}, which the searches used before, answers it with an object chain - its slot,
 * an entry, the vector object, its array - and a search paid two dependent cache misses per neighbour before the distance
 * could start: measured on 100,000 SIFT-like vectors, a walk scoring through it took about 1.25x the same walk scoring an
 * on-heap array. This cache keeps no object per vector. Its slots are a stamp each in one {@code long[]} and the components
 * of every slot sit in large provider-native vectors, so a lookup is one read of the stamp and the distance then streams
 * the components from where they are, with the offset kernels of jvector's own {@link VectorUtilSupport}: the walk scoring
 * here is faster than the one scoring the on-heap array. And a cache of millions of vectors adds no object for the collector
 * to trace.
 * <p>
 * <b>Layout.</b> Direct-mapped by vector id, like {@link BinaryCodeCache}: vector ids are dense and monotonic, so with a
 * capacity at least the id range every vector stays resident; past it, a later id takes the slot of an earlier one. The
 * components live in chunks of up to {@link #MAX_CHUNK_FLOATS} floats, each allocated on the first write into it, so the
 * heap is claimed as vectors are loaded rather than all at once, and no single allocation grows with the capacity. Like
 * {@link VectorCache} it is a cache and not a store: a miss is answered by the index pages, and the owner clears it whenever
 * vector ids change meaning and evicts the id of a deleted vector.
 * <p>
 * <b>Concurrency.</b> Lock-free, a seqlock per slot as in {@link BinaryCodeCache}, with the version and the id in one stamp
 * ({@code version << 32 | id}) so the probe reads one word. The version is even while the slot is stable and odd while a
 * writer owns it: a writer claims the slot with a CAS from even to odd, writes the components, and publishes the next even
 * version with release semantics; a writer that loses the CAS simply does not cache. A reader reads the stamp, scores the
 * components, and reads the stamp again after an acquire fence: any write in between moved it, so a score computed over a
 * half-written vector is never returned, it is reported as a miss. The version, not the id, is what tells two writes apart
 * (issue #10968).
 * <p>
 * <b>Scores.</b> {@link #similarity(VectorSimilarityFunction, VectorFloat, VectorFloat, int, int)} is the one place a
 * search computes a similarity against stored components, both for a vector found here and for one just read from its page,
 * so every candidate of a walk is scored by the same kernel. With the Panama and native providers that kernel is the
 * algorithm {@link VectorSimilarityFunction#compare} runs; the two can still differ in the last bit while the JIT has
 * compiled one and not yet the other, as two calls of {@code compare} itself can.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class FlatVectorCache {
  /**
   * Largest chunk of components, in floats (4 MB). Bounds every allocation the cache makes, whatever its capacity, and is
   * the granularity the heap is claimed in: an index of a few hundred wide vectors costs one chunk, not the slots its
   * capacity floor provides for (8,192 slots of 1536 dimensions would be 50 MB).
   */
  static final int MAX_CHUNK_FLOATS = 1 << 20;

  private static final VarHandle         STAMPS = MethodHandles.arrayElementVarHandle(long[].class);
  /** Version 0, id -1: the stamp of a slot that holds nothing. */
  private static final long              EMPTY  = 0xFFFFFFFFL;
  private static final VectorTypeSupport VTS    = VectorApiSupport.jvectorProvider().getVectorTypeSupport();
  private static final VectorUtilSupport UTIL   = VectorApiSupport.jvectorProvider().getVectorUtilSupport();

  /** Longs per counter stripe: one 64-byte cache line, as in {@link VectorCache}. */
  private static final int STRIPE_LONGS = 8;
  private static final int HITS         = 0;
  private static final int MISSES       = 1;
  private static final int STRIPES      = Integer.highestOneBit(Math.max(4, Runtime.getRuntime().availableProcessors()) - 1) << 1;
  private static final int STRIPE_MASK  = STRIPES - 1;

  private static final VarHandle COUNTERS = MethodHandles.arrayElementVarHandle(long[].class);

  private final int                                 dimensions;
  private final int                                 mask;
  // version << 32 | id. Version even: the slot is stable. Odd: a writer owns it
  private final long[]                              stamps;
  private final int                                 chunkShift;
  private final int                                 chunkMask;
  private final AtomicReferenceArray<VectorFloat<?>> chunks;
  private final long[]                              counters = new long[STRIPES * STRIPE_LONGS];

  /**
   * @param dimensions        the width of the vectors
   * @param requestedCapacity the vectors to hold, rounded up to a power of two
   */
  FlatVectorCache(final int dimensions, final int requestedCapacity) {
    this(dimensions, requestedCapacity, MAX_CHUNK_FLOATS);
  }

  /** @param maxChunkFloats the largest chunk of components, {@link #MAX_CHUNK_FLOATS} but in tests */
  FlatVectorCache(final int dimensions, final int requestedCapacity, final int maxChunkFloats) {
    if (dimensions <= 0)
      throw new IllegalArgumentException("dimensions must be positive, got " + dimensions);
    this.dimensions = dimensions;
    final int capacity = tableSizeFor(Math.max(1, requestedCapacity));
    this.mask = capacity - 1;
    this.stamps = new long[capacity];
    Arrays.fill(stamps, EMPTY);

    // Slots per chunk: the largest power of two whose components fit a chunk, never more than the capacity. One slot when
    // a single vector is wider than a chunk: its chunk is then exactly one vector.
    final int slotsPerChunk = Math.min(capacity, Math.max(1, Integer.highestOneBit(Math.max(1, maxChunkFloats / dimensions))));
    this.chunkShift = Integer.numberOfTrailingZeros(slotsPerChunk);
    this.chunkMask = slotsPerChunk - 1;
    this.chunks = new AtomicReferenceArray<>(capacity / slotsPerChunk);
  }

  /** @return the heap one cached vector costs: its components and its stamp. */
  static long bytesPerVector(final int dimensions) {
    return (long) dimensions * Float.BYTES + Long.BYTES;
  }

  int capacity() {
    return stamps.length;
  }

  int dimensions() {
    return dimensions;
  }

  /**
   * @return the similarity {@code function} gives {@code query} and the cached vector of {@code vectorId}, or {@code NaN}
   * when the vector is not resident
   */
  float similarity(final int vectorId, final VectorFloat<?> query, final VectorSimilarityFunction function) {
    if (vectorId >= 0) {
      final int slot = vectorId & mask;
      final long stamp = (long) STAMPS.getAcquire(stamps, slot);
      if ((int) stamp == vectorId && (stamp & (1L << 32)) == 0) {
        // Plain read: the writer that published this stamp took the chunk from the array before its release, and the
        // acquire above orders this read after that release, so the chunk is visible
        final VectorFloat<?> chunk = chunks.getPlain(slot >>> chunkShift);
        if (chunk != null) {
          final float score = similarity(function, query, chunk, (slot & chunkMask) * dimensions, dimensions);
          // The components must have been read before the stamp is read again: a writer that started in between moved it
          VarHandle.acquireFence();
          if ((long) STAMPS.getOpaque(stamps, slot) == stamp) {
            count(HITS);
            return score;
          }
        }
      }
    }
    count(MISSES);
    return Float.NaN;
  }

  /**
   * @return a copy of the cached vector of {@code vectorId}, or {@code null} when it is not resident. For the callers that
   * need the vector itself rather than a score; a search scores through {@link #similarity(int, VectorFloat, VectorSimilarityFunction)}.
   */
  VectorFloat<?> get(final int vectorId) {
    if (vectorId >= 0) {
      final int slot = vectorId & mask;
      final long stamp = (long) STAMPS.getAcquire(stamps, slot);
      if ((int) stamp == vectorId && (stamp & (1L << 32)) == 0) {
        final VectorFloat<?> chunk = chunks.getPlain(slot >>> chunkShift);
        if (chunk != null) {
          final VectorFloat<?> copy = VTS.createFloatVector(dimensions);
          copy.copyFrom(chunk, (slot & chunkMask) * dimensions, 0, dimensions);
          VarHandle.acquireFence();
          if ((long) STAMPS.getOpaque(stamps, slot) == stamp) {
            count(HITS);
            return copy;
          }
        }
      }
    }
    count(MISSES);
    return null;
  }

  /**
   * Caches the components of {@code vector} as those of {@code vectorId}. An id already resident is left as it is: a vector
   * id never changes content while it lives. An update tombstones the old id (and evicts it, {@link #remove}) and mints a
   * new one, an aborted replay evicts the ids it minted, and the only path that hands an id to another vector, a
   * renumbering compaction, clears the whole cache when it publishes the new ids ({@link #clear}).
   */
  void put(final int vectorId, final VectorFloat<?> vector) {
    if (vectorId < 0 || vector == null || vector.length() != dimensions)
      return;
    final int slot = vectorId & mask;
    final long stamp = (long) STAMPS.getVolatile(stamps, slot);
    final long version = stamp >>> 32;
    if ((version & 1) != 0 || (int) stamp == vectorId)
      return;
    final VectorFloat<?> chunk = chunkFor(slot);
    final long id = vectorId & 0xFFFFFFFFL;
    if (!STAMPS.compareAndSet(stamps, slot, stamp, ((version + 1) << 32) | id))
      return;
    chunk.copyFrom(vector, 0, (slot & chunkMask) * dimensions, dimensions);
    STAMPS.setRelease(stamps, slot, ((version + 2) << 32) | id);
  }

  /**
   * Drops the vector of {@code vectorId}, if resident: called when the vector is deleted. Unlike {@link #put} it does not
   * give up on a slot a writer owns, it waits for the writer, so a deleted vector cannot keep components a lost race left
   * behind.
   */
  void remove(final int vectorId) {
    if (vectorId < 0)
      return;
    final int slot = vectorId & mask;
    while (true) {
      final long stamp = (long) STAMPS.getVolatile(stamps, slot);
      final long version = stamp >>> 32;
      if ((version & 1) != 0) {
        Thread.onSpinWait();
        continue;
      }
      if ((int) stamp != vectorId)
        return;
      // One version step past the write that held it, with no id: a reader that read the old stamp sees it move
      if (STAMPS.compareAndSet(stamps, slot, stamp, ((version + 2) << 32) | 0xFFFFFFFFL))
        return;
    }
  }

  /**
   * Drops every vector. Called under the owner's write lock, when vector ids change meaning. Every slot moves to a version
   * of its own past the one it had, so a reader racing with the clear can never match the stamp it read before it. The
   * chunks stay allocated: the next searches refill them.
   * <p>
   * One CAS per slot, so O(capacity): tens of milliseconds for ten million slots. It runs only when a graph rebuild
   * publishes its location index, next to a rebuild that took orders of magnitude longer, so a global epoch folded into
   * every stamp, which would make this O(1), is not worth one more comparison on every probe of every search.
   */
  void clear() {
    for (int slot = 0; slot < stamps.length; slot++) {
      while (true) {
        final long stamp = (long) STAMPS.getVolatile(stamps, slot);
        final long version = stamp >>> 32;
        if ((version & 1) != 0) {
          Thread.onSpinWait();
          continue;
        }
        if (STAMPS.compareAndSet(stamps, slot, stamp, ((version + 2) << 32) | 0xFFFFFFFFL))
          break;
      }
    }
  }

  /** @return the vectors resident. O(capacity): for diagnostics and tests only. */
  int size() {
    int count = 0;
    for (int slot = 0; slot < stamps.length; slot++)
      if ((int) (long) STAMPS.getOpaque(stamps, slot) != -1)
        ++count;
    return count;
  }

  /** @return the heap the component chunks allocated so far take. */
  long allocatedBytes() {
    long bytes = 0;
    for (int i = 0; i < chunks.length(); i++) {
      final VectorFloat<?> chunk = chunks.get(i);
      if (chunk != null)
        bytes += (long) chunk.length() * Float.BYTES;
    }
    return bytes;
  }

  /** @return the number of lookups served from the cache. Approximate under concurrency, as {@link VectorCache#getHits()}. */
  long getHits() {
    return sum(HITS);
  }

  /** @return the number of lookups that found nothing. Approximate under concurrency, as {@link VectorCache#getMisses()}. */
  long getMisses() {
    return sum(MISSES);
  }

  void resetStats() {
    for (int i = 0; i < counters.length; i++)
      COUNTERS.setOpaque(counters, i, 0L);
  }

  /**
   * The similarity {@code function} gives {@code query} and the {@code dimensions} components of {@code data} from
   * {@code offset}: the formulas of {@link VectorSimilarityFunction#compare} over jvector's offset kernels, called on the
   * provider's {@link VectorUtilSupport} directly. The {@code VectorUtil} facade would add an assertion that the result is
   * finite, which a score taken over a slot a writer is rewriting may not be - and that score is thrown away by the stamp
   * check anyway.
   */
  static float similarity(final VectorSimilarityFunction function, final VectorFloat<?> query, final VectorFloat<?> data,
      final int offset, final int dimensions) {
    return switch (function) {
      case EUCLIDEAN -> 1 / (1 + UTIL.squareDistance(query, 0, data, offset, dimensions));
      case DOT_PRODUCT -> (1 + UTIL.dotProduct(query, 0, data, offset, dimensions)) / 2;
      case COSINE -> (1 + UTIL.cosine(query, 0, data, offset, dimensions)) / 2;
    };
  }

  /**
   * The chunk that holds {@code slot}'s components, allocated on its first use. Allocated under a lock, so the searches that
   * all miss on a cold chunk at once wait for one allocation instead of each making a chunk that all but one throw away.
   * Taken once per chunk over the life of the cache.
   */
  private VectorFloat<?> chunkFor(final int slot) {
    final int index = slot >>> chunkShift;
    VectorFloat<?> chunk = chunks.get(index);
    if (chunk == null) {
      synchronized (chunks) {
        chunk = chunks.get(index);
        if (chunk == null) {
          chunk = VTS.createFloatVector((chunkMask + 1) * dimensions);
          chunks.set(index, chunk);
        }
      }
    }
    return chunk;
  }

  /** Adds one to the calling thread's stripe of {@code counter}, without atomics: see {@link VectorCache}'s count. */
  private void count(final int counter) {
    final int i = (((int) Thread.currentThread().threadId()) & STRIPE_MASK) * STRIPE_LONGS + counter;
    COUNTERS.setOpaque(counters, i, (long) COUNTERS.getOpaque(counters, i) + 1L);
  }

  private long sum(final int counter) {
    long total = 0;
    for (int i = counter; i < counters.length; i += STRIPE_LONGS)
      total += (long) COUNTERS.getOpaque(counters, i);
    return total;
  }

  /** The power of two at or above {@code size}, capped at 2^30. */
  private static int tableSizeFor(final int size) {
    if (size >= 1 << 30)
      return 1 << 30;
    return size <= 1 ? 1 : Integer.highestOneBit(size - 1) << 1;
  }
}
