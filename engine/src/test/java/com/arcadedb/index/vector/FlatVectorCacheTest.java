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
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * The flat search cache of issue #11003: vectors stored by id in primitive chunks and scored where they lie.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class FlatVectorCacheTest {
  private static final VectorTypeSupport VTS = VectorApiSupport.jvectorProvider().getVectorTypeSupport();

  /**
   * The same similarity {@code compare} gives, up to the last bit: the Vector API reduces lanes in a different order before
   * and after the JIT compiles a kernel, so two calls of the same kernel can already differ there.
   */
  @Test
  void scoresAResidentVectorWithTheSimilarityCompareGives() {
    final int dimensions = 37; // not a multiple of any SIMD width, so the kernels' tails are exercised too
    final Random random = new Random(11003);
    final FlatVectorCache cache = new FlatVectorCache(dimensions, 64);
    final List<VectorFloat<?>> stored = new ArrayList<>();
    for (int id = 0; id < 64; id++) {
      final VectorFloat<?> vector = randomVector(random, dimensions);
      stored.add(vector);
      cache.put(id, vector);
    }

    for (final VectorSimilarityFunction function : VectorSimilarityFunction.values())
      for (int q = 0; q < 8; q++) {
        final VectorFloat<?> query = randomVector(random, dimensions);
        for (int id = 0; id < stored.size(); id++)
          assertThat(cache.similarity(id, query, function)).as("%s, id %d", function, id)
              .isCloseTo(function.compare(query, stored.get(id)), within(1e-6f));
      }
  }

  @Test
  void aMissIsNaNAndGetReturnsACopy() {
    final FlatVectorCache cache = new FlatVectorCache(4, 16);
    final VectorFloat<?> query = vector(1f, 0f, 0f, 0f);

    assertThat(cache.similarity(3, query, VectorSimilarityFunction.EUCLIDEAN)).isNaN();
    assertThat(cache.similarity(-1, query, VectorSimilarityFunction.EUCLIDEAN)).isNaN();
    assertThat(cache.get(3)).isNull();

    final VectorFloat<?> stored = vector(1f, 2f, 3f, 4f);
    cache.put(3, stored);
    final VectorFloat<?> copy = cache.get(3);
    assertThat(copy).isNotSameAs(stored);
    for (int i = 0; i < 4; i++)
      assertThat(copy.get(i)).isEqualTo(stored.get(i));

    // The copy is the caller's own: changing it does not change what is cached
    copy.set(0, 100f);
    assertThat(cache.get(3).get(0)).isEqualTo(1f);

    assertThat(cache.getHits()).isEqualTo(2);
    assertThat(cache.getMisses()).isEqualTo(3);
    cache.resetStats();
    assertThat(cache.getHits()).isZero();
  }

  @Test
  void aVectorOfAnotherWidthIsNotCached() {
    final FlatVectorCache cache = new FlatVectorCache(4, 16);
    cache.put(1, vector(1f, 2f));
    assertThat(cache.get(1)).isNull();
    assertThat(cache.size()).isZero();
  }

  @Test
  void denseIdsUpToTheCapacityStayResidentAndALaterIdTakesTheSlotOfAnEarlierOne() {
    final FlatVectorCache cache = new FlatVectorCache(2, 100);
    assertThat(cache.capacity()).isEqualTo(128);

    for (int id = 0; id < 128; id++)
      cache.put(id, vector(id, -id));
    assertThat(cache.size()).isEqualTo(128);
    for (int id = 0; id < 128; id++)
      assertThat(cache.get(id).get(0)).isEqualTo(id);

    // Direct-mapped: id 128 + 5 lands on the slot of id 5
    cache.put(133, vector(133f, -133f));
    assertThat(cache.get(5)).isNull();
    assertThat(cache.get(133).get(0)).isEqualTo(133f);
    assertThat(cache.size()).isEqualTo(128);
  }

  @Test
  void removeAndClearDropVectors() {
    final FlatVectorCache cache = new FlatVectorCache(3, 16);
    for (int id = 0; id < 16; id++)
      cache.put(id, vector(id, id, id));

    cache.remove(7);
    cache.remove(40); // not resident: no-op
    assertThat(cache.get(7)).isNull();
    assertThat(cache.size()).isEqualTo(15);

    // A removed id can be cached again
    cache.put(7, vector(70f, 70f, 70f));
    assertThat(cache.get(7).get(0)).isEqualTo(70f);

    cache.clear();
    assertThat(cache.size()).isZero();
    for (int id = 0; id < 16; id++)
      assertThat(cache.get(id)).isNull();

    // The chunks are kept and refilled
    cache.put(2, vector(2f, 2f, 2f));
    assertThat(cache.get(2).get(0)).isEqualTo(2f);
  }

  @Test
  void componentsAreSplitIntoChunksAllocatedOnFirstUse() {
    // 100 floats per chunk at 6 dimensions: 16 slots (the power of two at or below 100 / 6), so 64 slots take four chunks
    final int dimensions = 6;
    final FlatVectorCache cache = new FlatVectorCache(dimensions, 64, 100);
    assertThat(cache.allocatedBytes()).isZero();

    cache.put(0, vector(1f, 2f, 3f, 4f, 5f, 6f));
    final long oneChunk = 16L * dimensions * Float.BYTES;
    assertThat(cache.allocatedBytes()).isEqualTo(oneChunk);

    cache.put(63, vector(-1f, -2f, -3f, -4f, -5f, -6f));
    assertThat(cache.allocatedBytes()).isEqualTo(2 * oneChunk);

    // The last slot of a chunk and the first of the next do not overlap
    cache.put(15, vector(15f, 15f, 15f, 15f, 15f, 15f));
    cache.put(16, vector(16f, 16f, 16f, 16f, 16f, 16f));
    assertThat(cache.allocatedBytes()).isEqualTo(3 * oneChunk);

    assertThat(cache.get(0).get(5)).isEqualTo(6f);
    assertThat(cache.get(63).get(0)).isEqualTo(-1f);
    assertThat(cache.get(15).get(5)).isEqualTo(15f);
    assertThat(cache.get(16).get(0)).isEqualTo(16f);
    final VectorFloat<?> query = vector(0f, 0f, 0f, 0f, 0f, 0f);
    assertThat(cache.similarity(63, query, VectorSimilarityFunction.EUCLIDEAN))
        .isCloseTo(VectorSimilarityFunction.EUCLIDEAN.compare(query, vector(-1f, -2f, -3f, -4f, -5f, -6f)), within(1e-6f));

    // At the default chunk size a small index of wide vectors claims one small chunk, not its whole capacity floor
    final FlatVectorCache small = new FlatVectorCache(1536, 8192);
    small.put(0, VTS.createFloatVector(1536));
    assertThat(small.allocatedBytes()).isLessThanOrEqualTo((long) FlatVectorCache.MAX_CHUNK_FLOATS * Float.BYTES);

    // A vector wider than a chunk gets a chunk of its own
    final FlatVectorCache wide = new FlatVectorCache(8, 4, 5);
    wide.put(2, vector(1f, 1f, 1f, 1f, 1f, 1f, 1f, 2f));
    assertThat(wide.allocatedBytes()).isEqualTo(8L * Float.BYTES);
    assertThat(wide.get(2).get(7)).isEqualTo(2f);
  }

  /**
   * The seqlock: writers keep replacing the vector of one slot with the vector of another id, and back, while readers score
   * the slot. Each vector has every component equal to its id, so a score over a half-written slot would be a value no id
   * gives. Every score a reader is answered with must be the score of the id it asked for.
   */
  @Test
  @Timeout(value = 2, unit = TimeUnit.MINUTES)
  void aReaderIsNeverAnsweredWithAScoreOverAHalfWrittenOrForeignSlot() throws Exception {
    final int dimensions = 256;
    final FlatVectorCache cache = new FlatVectorCache(dimensions, 4);
    final int capacity = cache.capacity();
    // Ids 1 and 1 + capacity share slot 1
    final int a = 1;
    final int b = 1 + capacity;
    final VectorFloat<?> vectorA = constant(dimensions, a);
    final VectorFloat<?> vectorB = constant(dimensions, b);
    final VectorFloat<?> query = constant(dimensions, 0f);
    final float expectedA = VectorSimilarityFunction.EUCLIDEAN.compare(query, vectorA);
    final float expectedB = VectorSimilarityFunction.EUCLIDEAN.compare(query, vectorB);

    final AtomicBoolean stop = new AtomicBoolean();
    final AtomicReference<String> failure = new AtomicReference<>();
    final CountDownLatch started = new CountDownLatch(4);
    final List<Thread> threads = new ArrayList<>();
    for (int w = 0; w < 2; w++) {
      threads.add(new Thread(() -> {
        started.countDown();
        while (!stop.get()) {
          cache.put(a, vectorA);
          cache.remove(a);
          cache.put(b, vectorB);
          cache.remove(b);
        }
      }));
    }
    final long[] hits = new long[2];
    for (int r = 0; r < 2; r++) {
      final int reader = r;
      threads.add(new Thread(() -> {
        started.countDown();
        while (!stop.get() && failure.get() == null) {
          final float scoreA = cache.similarity(a, query, VectorSimilarityFunction.EUCLIDEAN);
          if (!Float.isNaN(scoreA)) {
            ++hits[reader];
            if (scoreA != expectedA)
              failure.compareAndSet(null, "id " + a + " scored " + scoreA + ", expected " + expectedA);
          }
          final float scoreB = cache.similarity(b, query, VectorSimilarityFunction.EUCLIDEAN);
          if (!Float.isNaN(scoreB)) {
            ++hits[reader];
            if (scoreB != expectedB)
              failure.compareAndSet(null, "id " + b + " scored " + scoreB + ", expected " + expectedB);
          }
        }
      }));
    }
    for (final Thread t : threads)
      t.start();
    started.await();
    final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
    while (System.nanoTime() < deadline && failure.get() == null)
      Thread.onSpinWait();
    stop.set(true);
    for (final Thread t : threads)
      t.join();

    assertThat(failure.get()).isNull();
    // Not vacuous: the readers did find the slot holding what they asked for
    assertThat(hits[0] + hits[1]).isPositive();
  }

  private static VectorFloat<?> randomVector(final Random random, final int dimensions) {
    final float[] components = new float[dimensions];
    for (int i = 0; i < dimensions; i++)
      components[i] = (float) random.nextGaussian();
    return VTS.createFloatVector(components);
  }

  private static VectorFloat<?> constant(final int dimensions, final float value) {
    final float[] components = new float[dimensions];
    Arrays.fill(components, value);
    return VTS.createFloatVector(components);
  }

  private static VectorFloat<?> vector(final float... components) {
    return VTS.createFloatVector(components);
  }
}
