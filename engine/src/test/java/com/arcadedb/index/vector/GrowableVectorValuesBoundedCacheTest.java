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

import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #3144: the live incremental builder's {@link GrowableVectorValues} cache used to be
 * unbounded, holding a second on-heap copy of the entire vector set during bulk ingest. This
 * verifies the cache honors its cap while the logical {@code size()} still tracks every ordinal.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class GrowableVectorValuesBoundedCacheTest {
  private static final VectorTypeSupport vts = VectorizationProvider.getInstance().getVectorTypeSupport();
  private static final int DIMENSIONS = 16;

  @Test
  void simpleModeIsUnbounded() {
    // Simple mode has no disk fallback, so it must cache everything to avoid data loss.
    final GrowableVectorValues values = new GrowableVectorValues(DIMENSIONS);
    final int n = 5_000;
    final Random rng = new Random(1);
    for (int i = 0; i < n; i++)
      values.addVector(i, randomVector(rng));

    assertThat(values.size()).isEqualTo(n);
    assertThat(values.vectorCount()).isEqualTo(n);
    // Every ordinal is retrievable from the cache.
    for (int i = 0; i < n; i++)
      assertThat(values.getVector(i)).isNotNull();
  }

  @Test
  void boundedCacheStopsGrowingButStillCountsAllOrdinals() {
    final int cap = 100;
    final int n = 1_000;
    // Bounded cache, no disk fallback wired: this exercises the cap mechanics in isolation.
    final GrowableVectorValues values = new GrowableVectorValues(DIMENSIONS, 16, null, null, null, null, cap);

    final Random rng = new Random(2);
    for (int i = 0; i < n; i++)
      values.addVector(i, randomVector(rng));

    // Logical size reflects every added ordinal even though only `cap` are cached.
    assertThat(values.size()).isEqualTo(n);
    assertThat(values.vectorCount()).isLessThanOrEqualTo(cap);
    // The first `cap` vectors were cached before the cap was hit.
    assertThat(values.getVector(0)).isNotNull();
    // Ordinals past the cap were not cached and (with no disk fallback) resolve to null.
    assertThat(values.getVector(n - 1)).isNull();
  }

  @Test
  void nonPositiveCapMeansUnbounded() {
    // maxCacheSize <= 0 is the backward-compatible "unlimited" escape hatch.
    final GrowableVectorValues values = new GrowableVectorValues(DIMENSIONS, 16, null, null, null, null, -1);
    final int n = 2_000;
    final Random rng = new Random(3);
    for (int i = 0; i < n; i++)
      values.addVector(i, randomVector(rng));

    assertThat(values.size()).isEqualTo(n);
    assertThat(values.vectorCount()).isEqualTo(n);
  }

  @Test
  void addVectorToleratesNullWithoutCaching() {
    // ensureLiveBuilder touches the max ordinal with a possibly-null vector to fix size(); it must
    // advance the count without inserting a null into the map (ConcurrentHashMap forbids nulls).
    final GrowableVectorValues values = new GrowableVectorValues(DIMENSIONS, 16, null, null, null, null, 100);
    values.addVector(42, null);
    assertThat(values.size()).isEqualTo(43);
    assertThat(values.vectorCount()).isEqualTo(0);
  }

  private static VectorFloat<?> randomVector(final Random rng) {
    final float[] v = new float[DIMENSIONS];
    for (int i = 0; i < DIMENSIONS; i++)
      v[i] = rng.nextFloat();
    return vts.createFloatVector(v);
  }
}
