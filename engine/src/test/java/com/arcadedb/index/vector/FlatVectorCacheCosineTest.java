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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The cosine of the flat search cache (issue #13274): the norm of the query is worked out once for a search and the norm of a
 * stored vector once, when it is cached, and a score is one pass for the dot product. What it scores must be what jvector's
 * {@code cosine}, which it replaces, scored, up to the rounding of another summation order.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class FlatVectorCacheCosineTest {
  private static final VectorTypeSupport VTS  = VectorApiSupport.jvectorProvider().getVectorTypeSupport();
  private static final VectorUtilSupport UTIL = VectorApiSupport.jvectorProvider().getVectorUtilSupport();

  /** The score the flat cache gave a cosine before the norms were kept: jvector's kernel over the two vectors. */
  private static float reference(final VectorFloat<?> query, final VectorFloat<?> data, final int dimensions) {
    return (1 + UTIL.cosine(query, 0, data, 0, dimensions)) / 2;
  }

  /** Equal up to the rounding of another summation order, and the same kind of number where it is not finite. */
  private static void assertSameScore(final float actual, final float expected, final String what) {
    if (Float.isNaN(expected))
      assertThat(actual).as(what).isNaN();
    else if (Float.isInfinite(expected))
      assertThat(actual).as(what).isEqualTo(expected);
    else
      assertThat(Math.abs(actual - expected)).as("%s: %s vs %s", what, actual, expected)
          .isLessThanOrEqualTo(1e-6f * Math.max(1f, Math.abs(expected)));
  }

  @Test
  void scoresMatchJvectorOnTenThousandRandomPairsOfEveryWidth() {
    // Widths below, at and past the lanes of 128, 256 and 512 bits, the unrolled step of four of them, and a tail after each
    final int[] widths = { 1, 3, 4, 7, 8, 15, 16, 17, 31, 32, 33, 63, 64, 65, 100, 127, 128, 129, 384, 768, 1000, 1024, 1536 };
    final Random random = new Random(13264);
    int pairs = 0;
    int round = 0;
    while (pairs < 10_000) {
      final int dimensions = widths[round++ % widths.length];
      final FlatVectorCache cache = new FlatVectorCache(dimensions, 16);
      final VectorFloat<?>[] stored = new VectorFloat<?>[16];
      for (int id = 0; id < stored.length; id++) {
        stored[id] = randomVector(random, dimensions);
        cache.put(id, stored[id]);
      }
      final VectorFloat<?> queryVector = randomVector(random, dimensions);
      final FlatVectorCache.Query query = FlatVectorCache.Query.of(queryVector);
      for (int id = 0; id < stored.length; id++, pairs++) {
        final float expected = reference(queryVector, stored[id], dimensions);
        assertSameScore(cache.similarity(id, query, VectorSimilarityFunction.COSINE), expected,
            "resident, " + dimensions + " dimensions, id " + id);
        assertSameScore(FlatVectorCache.similarity(VectorSimilarityFunction.COSINE, query, stored[id], 0, dimensions), expected,
            "read from the page, " + dimensions + " dimensions, id " + id);
      }
    }
    assertThat(pairs).isGreaterThanOrEqualTo(10_000);
  }

  @Test
  void aVectorScoresTheSameResidentAndReadFromItsPage() {
    final Random random = new Random(7);
    for (final int dimensions : new int[] { 5, 64, 100, 1024 }) {
      final FlatVectorCache cache = new FlatVectorCache(dimensions, 8);
      for (int id = 0; id < 8; id++) {
        final VectorFloat<?> stored = randomVector(random, dimensions);
        final FlatVectorCache.Query query = FlatVectorCache.Query.of(randomVector(random, dimensions));
        cache.put(id, stored);
        assertSameScore(cache.similarity(id, query, VectorSimilarityFunction.COSINE),
            FlatVectorCache.similarity(VectorSimilarityFunction.COSINE, query, stored, 0, dimensions), dimensions + " dimensions");
      }
    }
  }

  @Test
  void zeroDenormalHugeAndNonFiniteVectorsScoreAsJvectorScoresThem() {
    final int dimensions = 100;
    final float[][] shapes = {
        filled(dimensions, 0f),                      // zero: 0 / 0, NaN
        filled(dimensions, 1e-40f),                  // denormal: the squares underflow to zero
        filled(dimensions, 1e30f),                   // the product of two squared norms overflows, the score is 0.5
        filled(dimensions, 1e19f),                   // the squared norm overflows
        filled(dimensions, 3e-20f),                  // the product of two squared norms underflows
        filled(dimensions, Float.NaN),
        filled(dimensions, Float.POSITIVE_INFINITY),
        filled(dimensions, 1f),
        withComponent(filled(dimensions, 1f), 41, Float.NaN),
        withComponent(filled(dimensions, 1f), 99, Float.NEGATIVE_INFINITY),
        withComponent(filled(dimensions, 1f), 0, Float.MAX_VALUE),
    };
    for (int q = 0; q < shapes.length; q++) {
      final VectorFloat<?> queryVector = VTS.createFloatVector(shapes[q]);
      final FlatVectorCache.Query query = FlatVectorCache.Query.of(queryVector);
      final FlatVectorCache cache = new FlatVectorCache(dimensions, shapes.length);
      for (int d = 0; d < shapes.length; d++)
        cache.put(d, VTS.createFloatVector(shapes[d]));
      for (int d = 0; d < shapes.length; d++) {
        final VectorFloat<?> data = VTS.createFloatVector(shapes[d]);
        final float expected = reference(queryVector, data, dimensions);
        // A score that is NaN is "not resident" to the cache, whichever the reason: the caller then reads the page, which
        // gives NaN again. The resident answer for a NaN reference is therefore NaN, as it was
        assertSameScore(cache.similarity(d, query, VectorSimilarityFunction.COSINE), expected, "resident, query " + q + " data " + d);
        assertSameScore(FlatVectorCache.similarity(VectorSimilarityFunction.COSINE, query, data, 0, dimensions), expected,
            "page, query " + q + " data " + d);
      }
    }
  }

  /**
   * Terms that cancel in one accumulator overflow in four. With 8 blocks of {@code L} (the lanes) components and the signs
   * +,+,-,-,+,+,-,-, jvector's single accumulator per lane stays finite and the dot product is 0 (the score 0.5, the squared
   * norms being infinite), whereas the four accumulators of the kernels reach +inf and -inf and add up to NaN. The lane count of the
   * JVM is one of those tried, the others make an ordinary vector.
   */
  @Test
  void termsThatCancelInOneAccumulatorButOverflowInFourScoreAsJvectorScoresThem() {
    int cancelledQueryNorm = 0;
    int cancelledDataNorm = 0;
    for (final int lanes : new int[] { 2, 4, 8, 16, 32 }) {
      final int dimensions = 8 * lanes;
      final float[] signs = new float[dimensions];
      for (int i = 0; i < dimensions; i++)
        signs[i] = switch ((i / lanes) % 4) {
          case 0, 1 -> 1f;
          default -> -1f;
        };
      // The squared norm of the query overflows too
      if (assertCancellationScoresAsJvector(dimensions, scaledArray(filled(dimensions, 1f), 1e19f), scaledArray(signs, 1e19f),
          "query norm overflows, " + lanes))
        cancelledQueryNorm++;
      // The query is finite and only the stored vector's squared norm overflows: the terms are 1e18 * 1e20
      if (assertCancellationScoresAsJvector(dimensions, scaledArray(filled(dimensions, 1f), 1e18f), scaledArray(signs, 1e20f),
          "data norm overflows, " + lanes))
        cancelledDataNorm++;
    }
    assertThat(cancelledQueryNorm).as("the lane count of the JVM is one of those tried: jvector scores that pair finite").isPositive();
    assertThat(cancelledDataNorm).as("and that one with a finite query").isPositive();
  }

  /** @return whether jvector scored the pair with a finite number: only for the lane count of the JVM does the pair cancel there */
  private static boolean assertCancellationScoresAsJvector(final int dimensions, final float[] query, final float[] data, final String what) {
    final VectorFloat<?> queryVector = VTS.createFloatVector(query);
    final VectorFloat<?> dataVector = VTS.createFloatVector(data);
    final FlatVectorCache.Query prepared = FlatVectorCache.Query.of(queryVector);
    final FlatVectorCache cache = new FlatVectorCache(dimensions, 4);
    cache.put(0, dataVector);
    final float expected = reference(queryVector, dataVector, dimensions);
    assertSameScore(cache.similarity(0, prepared, VectorSimilarityFunction.COSINE), expected, "resident, " + what);
    assertSameScore(FlatVectorCache.similarity(VectorSimilarityFunction.COSINE, prepared, dataVector, 0, dimensions), expected,
        "page, " + what);
    return Float.isFinite(expected);
  }

  private static float[] scaledArray(final float[] components, final float factor) {
    final float[] scaled = new float[components.length];
    for (int i = 0; i < components.length; i++)
      scaled[i] = components[i] * factor;
    return scaled;
  }

  @Test
  void aSlotTakenByAnotherVectorScoresWithTheNormOfTheNewOne() {
    final int dimensions = 37;
    final Random random = new Random(99);
    final FlatVectorCache cache = new FlatVectorCache(dimensions, 4);
    final int id = 1;
    final int sameSlot = 1 + cache.capacity();
    final VectorFloat<?> small = scaled(randomVector(random, dimensions), 0.01f);
    final VectorFloat<?> large = scaled(randomVector(random, dimensions), 100f);
    final VectorFloat<?> queryVector = randomVector(random, dimensions);
    final FlatVectorCache.Query query = FlatVectorCache.Query.of(queryVector);

    cache.put(id, small);
    assertSameScore(cache.similarity(id, query, VectorSimilarityFunction.COSINE), reference(queryVector, small, dimensions), "small");
    // Taken over by an id that shares the slot
    cache.put(sameSlot, large);
    assertThat(cache.similarity(id, query, VectorSimilarityFunction.COSINE)).isNaN();
    assertSameScore(cache.similarity(sameSlot, query, VectorSimilarityFunction.COSINE), reference(queryVector, large, dimensions), "large");
    // Dropped, then cached again with other components
    cache.remove(sameSlot);
    assertThat(cache.similarity(sameSlot, query, VectorSimilarityFunction.COSINE)).isNaN();
    cache.put(sameSlot, small);
    assertSameScore(cache.similarity(sameSlot, query, VectorSimilarityFunction.COSINE), reference(queryVector, small, dimensions), "small again");
    cache.clear();
    cache.put(sameSlot, large);
    assertSameScore(cache.similarity(sameSlot, query, VectorSimilarityFunction.COSINE), reference(queryVector, large, dimensions), "after clear");
  }

  @Test
  void otherSimilaritiesAreScoredAsBeforeWithTheQueryPrepared() {
    final int dimensions = 70;
    final Random random = new Random(5);
    final FlatVectorCache cache = new FlatVectorCache(dimensions, 8);
    final VectorFloat<?> stored = randomVector(random, dimensions);
    cache.put(3, stored);
    final VectorFloat<?> queryVector = randomVector(random, dimensions);
    final FlatVectorCache.Query query = FlatVectorCache.Query.of(queryVector);
    for (final VectorSimilarityFunction function : new VectorSimilarityFunction[] { VectorSimilarityFunction.EUCLIDEAN,
        VectorSimilarityFunction.DOT_PRODUCT }) {
      assertSameScore(cache.similarity(3, query, function), function.compare(queryVector, stored), function.name());
      assertSameScore(FlatVectorCache.similarity(function, query, stored, 0, dimensions), function.compare(queryVector, stored), function.name());
    }
  }

  /**
   * The seqlock now covers a second word: a reader must never pair the components of one vector with the squared norm of
   * another. Every vector is a constant and the query is all ones, so a cosine is 1 for any of them, and the components of
   * one with the norm of another score a / b.
   */
  @Test
  @Timeout(value = 2, unit = TimeUnit.MINUTES)
  void aReaderIsNeverAnsweredWithComponentsPairedWithTheNormOfAnotherVector() throws Exception {
    final int dimensions = 256;
    final FlatVectorCache cache = new FlatVectorCache(dimensions, 4);
    final int a = 1;
    final int b = 1 + cache.capacity();
    final VectorFloat<?> vectorA = filledVector(dimensions, 2f);
    final VectorFloat<?> vectorB = filledVector(dimensions, 40f);
    final FlatVectorCache.Query query = FlatVectorCache.Query.of(filledVector(dimensions, 1f));

    final AtomicBoolean stop = new AtomicBoolean();
    final AtomicReference<String> failure = new AtomicReference<>();
    final long[] hits = new long[2];
    final Thread[] threads = new Thread[4];
    for (int w = 0; w < 2; w++)
      threads[w] = new Thread(() -> {
        while (!stop.get()) {
          cache.put(a, vectorA);
          cache.remove(a);
          cache.put(b, vectorB);
          cache.remove(b);
          // And straight over each other, with no removal between
          cache.put(a, vectorA);
          cache.put(b, vectorB);
        }
      });
    for (int r = 0; r < 2; r++) {
      final int reader = r;
      threads[2 + r] = new Thread(() -> {
        while (!stop.get() && failure.get() == null)
          for (final int id : new int[] { a, b }) {
            final float score = cache.similarity(id, query, VectorSimilarityFunction.COSINE);
            if (!Float.isNaN(score)) {
              ++hits[reader];
              if (Math.abs(score - 1f) > 1e-5f)
                failure.compareAndSet(null, "id " + id + " scored " + score + ", expected 1");
            }
          }
      });
    }
    for (final Thread t : threads)
      t.start();
    final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
    while (System.nanoTime() < deadline && failure.get() == null)
      Thread.onSpinWait();
    stop.set(true);
    for (final Thread t : threads)
      t.join();

    assertThat(failure.get()).isNull();
    assertThat(hits[0] + hits[1]).isPositive();
  }

  private static VectorFloat<?> randomVector(final Random random, final int dimensions) {
    final float[] components = new float[dimensions];
    for (int i = 0; i < dimensions; i++)
      components[i] = (float) random.nextGaussian();
    return VTS.createFloatVector(components);
  }

  private static VectorFloat<?> scaled(final VectorFloat<?> vector, final float factor) {
    final float[] components = new float[vector.length()];
    for (int i = 0; i < components.length; i++)
      components[i] = vector.get(i) * factor;
    return VTS.createFloatVector(components);
  }

  private static VectorFloat<?> filledVector(final int dimensions, final float value) {
    return VTS.createFloatVector(filled(dimensions, value));
  }

  private static float[] filled(final int dimensions, final float value) {
    final float[] components = new float[dimensions];
    Arrays.fill(components, value);
    return components;
  }

  private static float[] withComponent(final float[] components, final int index, final float value) {
    components[index] = value;
    return components;
  }
}
