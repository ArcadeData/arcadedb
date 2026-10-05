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

import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #9233: a graph build whose cache holds the whole corpus answers jvector's vector lookups
 * from a flat by-ordinal array instead of walking ordinal -> vector id -> cache slot -> entry. The answer must be the
 * very vector the cache holds, and a build whose cache is smaller than the corpus must keep going through the cache so
 * the array never pins vectors the cache's budget evicted.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9233BuildVectorsByOrdinalTest {
  private static final VectorTypeSupport VTS = VectorizationProvider.getInstance().getVectorTypeSupport();
  private static final int                DIMS = 4;

  private static VectorFloat<?> vector(final int seed) {
    final float[] data = new float[DIMS];
    for (int i = 0; i < DIMS; i++)
      data[i] = seed + i;
    return VTS.createFloatVector(data);
  }

  @Test
  void buildWithWholeCorpusInCacheServesTheCachedVectors() {
    final int n = 1000;
    final int[] ordinalToVectorId = new int[n];
    final VectorCache cache = new VectorCache(n);
    final VectorFloat<?>[] expected = new VectorFloat<?>[n];
    for (int i = 0; i < n; i++) {
      // Ids are not the ordinals: the mapping has to be honoured
      ordinalToVectorId[i] = n - 1 - i;
      expected[i] = vector(i);
      cache.put(ordinalToVectorId[i], expected[i]);
    }

    final ArcadePageVectorValues values = ArcadePageVectorValues.forGraphBuild(null, DIMS, "v", null, ordinalToVectorId, null,
        cache);

    for (int i = 0; i < n; i++)
      assertThat(values.getVector(i)).as("ordinal %d", i).isSameAs(expected[i]);
  }

  @Test
  void buildWithSmallerCacheStillGoesThroughTheCache() {
    final int n = 1000;
    final int[] ordinalToVectorId = new int[n];
    for (int i = 0; i < n; i++)
      ordinalToVectorId[i] = i;
    final VectorCache cache = new VectorCache(64);
    final VectorFloat<?> first = vector(1);
    cache.put(0, first);

    final ArcadePageVectorValues values = ArcadePageVectorValues.forGraphBuild(null, DIMS, "v", null, ordinalToVectorId, null,
        cache);
    assertThat(values.getVector(0)).isSameAs(first);

    // The cache is the only owner: once it drops the vector, the build reader does not keep serving it
    cache.remove(0);
    assertThat(values.getVector(0)).isNotSameAs(first);
  }

  @Test
  void ordinalOutOfRangeYieldsTheSentinel() {
    final VectorCache cache = new VectorCache(8);
    final int[] ordinalToVectorId = { 0, 1 };
    cache.put(0, vector(0));
    cache.put(1, vector(1));
    final ArcadePageVectorValues values = ArcadePageVectorValues.forGraphBuild(null, DIMS, "v", null, ordinalToVectorId, null,
        cache);

    assertThat(values.isDeletedSentinel(values.getVector(2))).isTrue();
    assertThat(values.isDeletedSentinel(values.getVector(-1))).isTrue();
  }

  @Test
  void capacityExactlyEqualToCorpusStillServesEveryVector() {
    final int n = 1024;
    final int[] ordinalToVectorId = new int[n];
    final VectorCache cache = new VectorCache(n);
    assertThat(cache.capacity()).isEqualTo(n);
    final VectorFloat<?>[] expected = new VectorFloat<?>[n];
    for (int i = 0; i < n; i++) {
      ordinalToVectorId[i] = i;
      expected[i] = vector(i);
      cache.put(i, expected[i]);
    }

    final ArcadePageVectorValues values = ArcadePageVectorValues.forGraphBuild(null, DIMS, "v", null, ordinalToVectorId, null,
        cache);
    for (int i = 0; i < n; i++)
      assertThat(values.getVector(i)).as("ordinal %d", i).isSameAs(expected[i]);
  }
}
