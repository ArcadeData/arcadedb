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

import com.arcadedb.log.LogManager;
import com.arcadedb.utility.VectorApiSupport;
import io.github.jbellis.jvector.vector.ArrayVectorFloat;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import io.github.jbellis.jvector.vector.VectorUtilSupport;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.logging.Level;

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
 * capacity is any count, not a power of two: an id below it is its own slot (issue #11071, a capacity rounded down to a
 * power of two below the id range made a third of the ids of a 100,000-vector index evict each other on every search). The
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
 * <b>Scores.</b> {@link #similarity(VectorSimilarityFunction, Query, VectorFloat, int, int)} is the one place a
 * search computes a similarity against components that are not resident, so a vector found here and one just read from its
 * page are scored by the same kernel. A cosine is the dot product over the norms: the norm of the query is worked out once
 * for the search ({@link Query}) and that of a stored vector once, when it is cached, so a resident vector costs one pass
 * for the dot product (issue #13274). With the Panama provider that is the algorithm {@link VectorSimilarityFunction#compare}
 * runs, in another summation order; the two can differ in the last bit, as two calls of {@code compare} itself can while the
 * JIT has compiled one and not yet the other.
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
  /** Largest capacity, in vectors: a slot is two longs of {@link #slots}, whose length must fit an int. */
  static final int MAX_CAPACITY     = 1 << 29;

  private static final VarHandle         STAMPS = MethodHandles.arrayElementVarHandle(long[].class);
  /** Version 0, id -1: the stamp of a slot that holds nothing. */
  private static final long              EMPTY  = 0xFFFFFFFFL;
  private static final VectorTypeSupport VTS    = VectorApiSupport.jvectorProvider().getVectorTypeSupport();
  private static final VectorUtilSupport UTIL   = VectorApiSupport.jvectorProvider().getVectorUtilSupport();
  /**
   * True when a cosine is scored by {@link SimdCosineKernels} from the squared norms the cache keeps, false when it is scored by
   * jvector's own kernel: a JVM without the Vector API, a native image, or a provider whose vectors are not {@code float[]}
   * (the native one), none of which the kernels can read.
   */
  private static final boolean            SIMD_COSINE = simdCosine();

  private static boolean simdCosine() {
    if (!VectorApiSupport.isVectorApiCompiled() || !(VTS.createFloatVector(1) instanceof ArrayVectorFloat))
      return false;
    try {
      return SimdCosineKernels.selfTest();
    } catch (final LinkageError | RuntimeException e) {
      LogManager.instance().log(FlatVectorCache.class, Level.INFO,
          "Cosine search kernels disabled, scoring through jvector's own (reason: %s). Add '--add-modules jdk.incubator.vector' to the JVM flags", e.toString());
      return false;
    }
  }

  /** Longs per counter stripe: one 64-byte cache line, as in {@link VectorCache}. */
  private static final int STRIPE_LONGS = 8;
  private static final int HITS         = 0;
  private static final int MISSES       = 1;
  private static final int STRIPES      = Integer.highestOneBit(Math.max(4, Runtime.getRuntime().availableProcessors()) - 1) << 1;
  private static final int STRIPE_MASK  = STRIPES - 1;

  private static final VarHandle COUNTERS = MethodHandles.arrayElementVarHandle(long[].class);

  private final int                                 dimensions;
  private final int                                 capacity;
  // Two longs per slot, at 2 * slot and 2 * slot + 1, so a lookup that reads the stamp finds the norm in the same cache line.
  // The first is the stamp, version << 32 | id; version even: the slot is stable, odd: a writer owns it. The second is the
  // squared norm of the slot's vector as the bits of a float, kept only when SIMD_COSINE: written by the writer that owns the
  // slot, before it publishes the stamp, and read between two reads of the stamp, like the components
  private final long[]                              slots;
  private final int                                 chunkShift;
  private final int                                 chunkMask;
  private final AtomicReferenceArray<VectorFloat<?>> chunks;
  private final long[]                              counters = new long[STRIPES * STRIPE_LONGS];

  /**
   * @param dimensions        the width of the vectors
   * @param requestedCapacity the vectors to hold, exactly: any count, not only a power of two (issue #11071)
   */
  FlatVectorCache(final int dimensions, final int requestedCapacity) {
    this(dimensions, requestedCapacity, MAX_CHUNK_FLOATS);
  }

  /** @param maxChunkFloats the largest chunk of components, {@link #MAX_CHUNK_FLOATS} but in tests */
  FlatVectorCache(final int dimensions, final int requestedCapacity, final int maxChunkFloats) {
    if (dimensions <= 0)
      throw new IllegalArgumentException("dimensions must be positive, got " + dimensions);
    this.dimensions = dimensions;
    this.capacity = Math.min(MAX_CAPACITY, Math.max(1, requestedCapacity));
    this.slots = new long[2 * capacity];
    for (int slot = 0; slot < capacity; slot++)
      slots[slot << 1] = EMPTY;

    // Slots per chunk: the largest power of two whose components fit a chunk, never more than the capacity needs. One slot
    // when a single vector is wider than a chunk: its chunk is then exactly one vector. The last chunk is cut to the slots
    // the capacity leaves it (chunkFor)
    final int slotsPerChunk = Math.min(powerOfTwoAtOrAbove(capacity),
        Math.max(1, Integer.highestOneBit(Math.max(1, maxChunkFloats / dimensions))));
    this.chunkShift = Integer.numberOfTrailingZeros(slotsPerChunk);
    this.chunkMask = slotsPerChunk - 1;
    this.chunks = new AtomicReferenceArray<>((capacity + slotsPerChunk - 1) / slotsPerChunk);
  }

  /** @return the heap one cached vector costs: its components, its stamp and its squared norm. */
  static long bytesPerVector(final int dimensions) {
    return (long) dimensions * Float.BYTES + 2L * Long.BYTES;
  }

  int capacity() {
    return capacity;
  }

  int dimensions() {
    return dimensions;
  }

  /**
   * @return the similarity {@code function} gives {@code query} and the cached vector of {@code vectorId}, or {@code NaN}
   * when the vector is not resident
   */
  float similarity(final int vectorId, final Query query, final VectorSimilarityFunction function) {
    if (vectorId >= 0) {
      final int slot = slotOf(vectorId);
      final int at = slot << 1;
      final long stamp = (long) STAMPS.getAcquire(slots, at);
      if ((int) stamp == vectorId && (stamp & (1L << 32)) == 0) {
        // Plain read: the writer that published this stamp took the chunk from the array before its release, and the
        // acquire above orders this read after that release, so the chunk is visible
        final VectorFloat<?> chunk = chunks.getPlain(slot >>> chunkShift);
        if (chunk != null) {
          final int offset = (slot & chunkMask) * dimensions;
          final float score;
          if (SIMD_COSINE && function == VectorSimilarityFunction.COSINE && query.array != null
              && chunk instanceof final ArrayVectorFloat resident)
            score = residentCosine(query, resident, offset, Float.intBitsToFloat((int) (long) STAMPS.getOpaque(slots, at + 1)));
          else
            score = jvectorSimilarity(function, query.vector, chunk, offset, dimensions);
          // The components and the norm must have been read before the stamp is read again: a writer that started in
          // between moved it
          VarHandle.acquireFence();
          if ((long) STAMPS.getOpaque(slots, at) == stamp) {
            count(HITS);
            return score;
          }
        }
      }
    }
    count(MISSES);
    return Float.NaN;
  }

  /** The cosine of a resident vector from its kept squared norm; a vector whose squared norm overflows is jvector's to score, as in {@link Query#of}. */
  private float residentCosine(final Query query, final ArrayVectorFloat resident, final int offset, final float dataSquaredNorm) {
    if (Float.isInfinite(dataSquaredNorm))
      return jvectorSimilarity(VectorSimilarityFunction.COSINE, query.vector, resident, offset, dimensions);
    return cosine(query, SimdCosineKernels.dot(query.array, resident.get(), offset, dimensions), dataSquaredNorm);
  }

  /**
   * @return a copy of the cached vector of {@code vectorId}, or {@code null} when it is not resident. For the callers that
   * need the vector itself rather than a score; a search scores through {@link #similarity(int, VectorFloat, VectorSimilarityFunction)}.
   */
  VectorFloat<?> get(final int vectorId) {
    if (vectorId >= 0) {
      final int slot = slotOf(vectorId);
      final long stamp = (long) STAMPS.getAcquire(slots, slot << 1);
      if ((int) stamp == vectorId && (stamp & (1L << 32)) == 0) {
        final VectorFloat<?> chunk = chunks.getPlain(slot >>> chunkShift);
        if (chunk != null) {
          final VectorFloat<?> copy = VTS.createFloatVector(dimensions);
          copy.copyFrom(chunk, (slot & chunkMask) * dimensions, 0, dimensions);
          VarHandle.acquireFence();
          if ((long) STAMPS.getOpaque(slots, slot << 1) == stamp) {
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
    final int slot = slotOf(vectorId);
    final int at = slot << 1;
    final long stamp = (long) STAMPS.getVolatile(slots, at);
    final long version = stamp >>> 32;
    if ((version & 1) != 0 || (int) stamp == vectorId)
      return;
    final VectorFloat<?> chunk = chunkFor(slot);
    final long id = vectorId & 0xFFFFFFFFL;
    if (!STAMPS.compareAndSet(slots, at, stamp, ((version + 1) << 32) | id))
      return;
    final int offset = (slot & chunkMask) * dimensions;
    chunk.copyFrom(vector, 0, offset, dimensions);
    if (SIMD_COSINE && chunk instanceof final ArrayVectorFloat stored)
      // Of the copy, not of the argument: the number a reader pairs with these components is the one they have
      STAMPS.setOpaque(slots, at + 1,
          (long) Float.floatToRawIntBits(SimdCosineKernels.squaredNorm(stored.get(), offset, dimensions)));
    STAMPS.setRelease(slots, at, ((version + 2) << 32) | id);
  }

  /**
   * Drops the vector of {@code vectorId}, if resident: called when the vector is deleted. Unlike {@link #put} it does not
   * give up on a slot a writer owns, it waits for the writer, so a deleted vector cannot keep components a lost race left
   * behind.
   */
  void remove(final int vectorId) {
    if (vectorId < 0)
      return;
    final int at = slotOf(vectorId) << 1;
    while (true) {
      final long stamp = (long) STAMPS.getVolatile(slots, at);
      final long version = stamp >>> 32;
      if ((version & 1) != 0) {
        Thread.onSpinWait();
        continue;
      }
      if ((int) stamp != vectorId)
        return;
      // One version step past the write that held it, with no id: a reader that read the old stamp sees it move
      if (STAMPS.compareAndSet(slots, at, stamp, ((version + 2) << 32) | 0xFFFFFFFFL))
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
    for (int slot = 0; slot < capacity; slot++) {
      final int at = slot << 1;
      while (true) {
        final long stamp = (long) STAMPS.getVolatile(slots, at);
        final long version = stamp >>> 32;
        if ((version & 1) != 0) {
          Thread.onSpinWait();
          continue;
        }
        if (STAMPS.compareAndSet(slots, at, stamp, ((version + 2) << 32) | 0xFFFFFFFFL))
          break;
      }
    }
  }

  /** @return the vectors resident. O(capacity): for diagnostics and tests only. */
  int size() {
    int count = 0;
    for (int slot = 0; slot < capacity; slot++)
      if ((int) (long) STAMPS.getOpaque(slots, slot << 1) != -1)
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
   * {@code offset}, for a vector that was just read from its page: the formulas of {@link VectorSimilarityFunction#compare}.
   * A cosine is scored with the Vector API kernels of {@link SimdCosineKernels} when the cache does, so a vector scores the
   * same here as it does once it is resident; any other score, and a cosine where they are off, goes to jvector's offset
   * kernels, called on the provider's {@link VectorUtilSupport} directly. The {@code VectorUtil} facade would add an assertion
   * that the result is finite, which a score taken over a slot a writer is rewriting may not be - and that score is thrown
   * away by the stamp check anyway.
   */
  static float similarity(final VectorSimilarityFunction function, final Query query, final VectorFloat<?> data,
      final int offset, final int dimensions) {
    if (SIMD_COSINE && function == VectorSimilarityFunction.COSINE && query.array != null
        && data instanceof final ArrayVectorFloat array) {
      final long sums = SimdCosineKernels.dotAndSquaredNorm(query.array, array.get(), offset, dimensions);
      final float dataSquaredNorm = Float.intBitsToFloat((int) sums);
      // An overflowing norm is jvector's to score, as in Query.of
      if (!Float.isInfinite(dataSquaredNorm))
        return cosine(query, Float.intBitsToFloat((int) (sums >>> 32)), dataSquaredNorm);
    }
    return jvectorSimilarity(function, query.vector, data, offset, dimensions);
  }

  /** The formulas of {@link VectorSimilarityFunction#compare} over jvector's offset kernels. */
  private static float jvectorSimilarity(final VectorSimilarityFunction function, final VectorFloat<?> query,
      final VectorFloat<?> data, final int offset, final int dimensions) {
    return switch (function) {
      case EUCLIDEAN -> 1 / (1 + UTIL.squareDistance(query, 0, data, offset, dimensions));
      case DOT_PRODUCT -> (1 + UTIL.dotProduct(query, 0, data, offset, dimensions)) / 2;
      case COSINE -> (1 + UTIL.cosine(query, 0, data, offset, dimensions)) / 2;
    };
  }

  /** jvector's cosine of the sums, {@code dot / sqrt(|query|^2 * |data|^2)}, in the same arithmetic, then scaled as {@code compare} does. */
  private static float cosine(final Query query, final float dot, final float dataSquaredNorm) {
    return (1 + (float) (dot / Math.sqrt((double) (query.squaredNorm * dataSquaredNorm)))) / 2;
  }

  /**
   * A query vector and what every score against it shares, worked out once for the search that scores thousands of vectors
   * with it: the float[] the kernels read and its squared norm, instead of the norm of the query once per candidate.
   */
  static final class Query {
    final VectorFloat<?> vector;
    /** The components when the cosine kernels can score the query, else null. */
    final float[]        array;
    final float          squaredNorm;

    private Query(final VectorFloat<?> vector, final float[] array, final float squaredNorm) {
      this.vector = vector;
      this.array = array;
      this.squaredNorm = squaredNorm;
    }

    /**
     * A query whose squared norm overflows is left to jvector's kernel, which the kernels' four accumulators cannot match there:
     * jvector's one accumulator per lane may cancel terms that four of them each overflow on (+inf and -inf add up to NaN,
     * where jvector's score is a finite one), so a score must not be taken from the sums of a vector that overflows.
     */
    static Query of(final VectorFloat<?> vector) {
      if (SIMD_COSINE && vector instanceof final ArrayVectorFloat array) {
        final float squaredNorm = SimdCosineKernels.squaredNorm(array.get(), 0, array.length());
        if (!Float.isInfinite(squaredNorm))
          return new Query(vector, array.get(), squaredNorm);
      }
      return new Query(vector, null, Float.NaN);
    }
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
          // The last chunk holds only the slots the capacity leaves it
          final int slots = Math.min(chunkMask + 1, capacity - (index << chunkShift));
          chunk = VTS.createFloatVector(slots * dimensions);
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

  /**
   * The slot of {@code vectorId}, which is not negative: the id itself below the capacity, so a capacity that covers the id
   * range keeps every vector in a slot of its own whatever the capacity is, and the remainder past it. The remainder costs a
   * division, but only on an id the capacity does not cover, which is a miss most of the time anyway.
   */
  private int slotOf(final int vectorId) {
    return vectorId < capacity ? vectorId : vectorId % capacity;
  }

  /** The power of two at or above {@code size}, for {@code size} in [1, 2^30]. */
  private static int powerOfTwoAtOrAbove(final int size) {
    return size <= 1 ? 1 : Integer.highestOneBit(size - 1) << 1;
  }
}
