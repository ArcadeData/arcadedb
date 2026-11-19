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

import com.arcadedb.TestHelper;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression for #12544, the reverse of #12511: a search cache sized while a graph build runs on another vector index of the
 * database used to plan on the heap the build's cache had just taken, since that cache is local to the build, preloaded at
 * once and missing from the available-heap reading until a collection of the old generation. The build's cache is now
 * published while it lives and charged to the search caches, to the other builds and to the delta payload budget.
 *
 * @author robfrank (ro.franchini@gmail.com)
 */
class Issue12544BuildCacheChargedToSearchCachesTest extends TestHelper {
  private static final int DIMENSIONS = 32;
  private static final int VECTORS    = 500;
  /** The floor of the first cache, and the smallest cache worth publishing after it (LSMVectorIndex.MIN_SEARCH_CACHE_SIZE). */
  private static final int FLOOR      = 8_192;

  @Test
  void aSearchCacheSizedDuringABuildIsChargedTheBuildCache() {
    final LSMVectorIndex[] buckets = createBucketIndexes("Doc", null);
    final LSMVectorIndex building = buckets[0];
    final LSMVectorIndex searched = buckets[1];
    fill(building, VECTORS);

    final long bytesPerEntry = FlatVectorCache.bytesPerVector(DIMENSIONS);
    final long buildPlan = VECTORS * VectorHeapBudget.bytesPerCachedVector(DIMENSIONS);
    // 1% of this reading is the build's cache plus a useful search cache
    final long available = (buildPlan + (FLOOR + 30) * bytesPerEntry) * 100;
    final long tight = buildPlan * 100;

    assertThat(searched.graphBuildCachesPlannedBytes(true)).as("no build runs yet").isZero();
    final long withNoBuild = searched.affordableSearchCacheEntries(1, bytesPerEntry, available, false);
    assertThat(withNoBuild).as("with no build running, the whole percent is the first cache's")
        .isEqualTo(available / 100 / bytesPerEntry);
    assertThat(searched.affordableSearchCacheEntries(1, bytesPerEntry, tight, false))
        .as("with no build running, the first cache keeps at least the floor").isGreaterThanOrEqualTo(FLOOR);

    final AtomicBoolean observed = new AtomicBoolean();
    final AtomicLong plannedDuringBuild = new AtomicLong();
    final AtomicLong entriesDuringBuild = new AtomicLong();
    final AtomicLong entriesWhenTight = new AtomicLong();
    final AtomicLong searchedHeapForABuild = new AtomicLong();
    final AtomicLong buildingHeapForItsBuild = new AtomicLong();
    final AtomicLong buildingHeapForDeltas = new AtomicLong();
    building.buildVectorGraphNow((phase, processed, total, accesses) -> {
      if (observed.getAndSet(true))
        return;
      plannedDuringBuild.set(searched.graphBuildCachesPlannedBytes(true));
      entriesDuringBuild.set(searched.affordableSearchCacheEntries(1, bytesPerEntry, available, false));
      entriesWhenTight.set(searched.affordableSearchCacheEntries(1, bytesPerEntry, tight, false));
      searchedHeapForABuild.set(searched.availableHeapNetOfSearchCaches(available));
      buildingHeapForItsBuild.set(building.availableHeapNetOfSearchCaches(available));
      buildingHeapForDeltas.set(building.availableHeapNetOfVectorCaches(available));
    });

    assertThat(observed.get()).as("precondition: the build reported progress while its cache was alive").isTrue();
    assertThat(plannedDuringBuild.get()).as("the build publishes what its cache plans on: one float vector per vector walked")
        .isEqualTo(buildPlan);
    assertThat(entriesDuringBuild.get()).as("a search cache sized during the build is charged the build's cache")
        .isEqualTo(FLOOR + 30).isLessThan(withNoBuild);
    assertThat(entriesWhenTight.get()).as("a budget the build's cache takes whole leaves no search cache, not the floor").isZero();
    assertThat(searchedHeapForABuild.get()).as("a build sized on another index is charged the running build's cache")
        .isEqualTo(available - buildPlan);
    assertThat(buildingHeapForItsBuild.get()).as("a build is never charged its own cache").isEqualTo(available);
    assertThat(buildingHeapForDeltas.get()).as("the delta payloads are charged the build's cache, the own index's included")
        .isEqualTo(available - buildPlan);

    assertThat(searched.graphBuildCachesPlannedBytes(true)).as("the build's cache is no longer charged once the build ended")
        .isZero();
    assertThat(searched.affordableSearchCacheEntries(1, bytesPerEntry, available, false))
        .as("and the search cache gets back the budget it had before the build").isEqualTo(withNoBuild);
  }

  @Test
  void aFailedBuildStopsChargingItsCache() {
    final LSMVectorIndex[] buckets = createBucketIndexes("Doc", null);
    final LSMVectorIndex building = buckets[0];
    fill(building, VECTORS);

    final AtomicLong plannedDuringBuild = new AtomicLong();
    assertThatThrownBy(() -> building.buildVectorGraphNow((phase, processed, total, accesses) -> {
      plannedDuringBuild.set(buckets[1].graphBuildCachesPlannedBytes(true));
      throw new IllegalStateException("build aborted by the test");
    })).hasStackTraceContaining("build aborted by the test");

    assertThat(plannedDuringBuild.get()).as("precondition: the cache was charged while the build lived").isPositive();
    assertThat(buckets[1].graphBuildCachesPlannedBytes(true)).as("a failed build no longer charges its cache").isZero();
  }

  @Test
  void aBuildKeepingQuantizedRowsIsChargedTheRowsAndTheSmallFloatCache() {
    final LSMVectorIndex index = createBucketIndexes("Int8", "INT8")[0];
    final long perFloat = VectorHeapBudget.bytesPerCachedVector(DIMENSIONS);

    assertThat(index.graphBuildCachePlannedBytes(100, 0, 1_000)).as("a float cache is charged its capacity")
        .isEqualTo(100 * perFloat);
    assertThat(index.graphBuildCachePlannedBytes(5_000, 0, 1_000)).as("a float cache holds no more than the corpus")
        .isEqualTo(1_000 * perFloat);
    assertThat(index.graphBuildCachePlannedBytes(100, 400, 1_000)).as("quantized rows, and the small float cache beside them")
        .isEqualTo(400 * QuantizedBuildVectors.bytesPerVector(VectorQuantizationType.INT8, DIMENSIONS) + 64 * perFloat);
  }

  private LSMVectorIndex[] createBucketIndexes(final String typeName, final String quantization) {
    database.transaction(() -> {
      database.getSchema().buildDocumentType().withName(typeName).withTotalBuckets(2).create()
          .createProperty("embedding", Type.ARRAY_OF_FLOATS);
      database.command("sql", "CREATE INDEX ON " + typeName + " (embedding) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
          + ", \"similarity\": \"COSINE\"" + (quantization != null ? ", \"quantization\": \"" + quantization + "\"" : "")
          + " }");
    });
    final IndexInternal[] indexes = ((TypeIndex) database.getSchema().getIndexByName(typeName + "[embedding]"))
        .getIndexesOnBuckets();
    assertThat(indexes).hasSize(2);
    return new LSMVectorIndex[] { (LSMVectorIndex) indexes[0], (LSMVectorIndex) indexes[1] };
  }

  /** Writes {@code count} vectors into the bucket of {@code index} only, so the other index of the type stays empty. */
  private void fill(final LSMVectorIndex index, final int count) {
    final String bucketName = database.getSchema().getBucketById(index.getAssociatedBucketId()).getName();
    final String typeName = database.getSchema().getTypeByBucketId(index.getAssociatedBucketId()).getName();
    final Random random = new Random(12544);
    database.transaction(() -> {
      for (int i = 0; i < count; i++) {
        final float[] vector = new float[DIMENSIONS];
        for (int d = 0; d < DIMENSIONS; d++)
          vector[d] = random.nextFloat() + 0.01f;
        database.newDocument(typeName).set("embedding", vector).save(bucketName);
      }
    });
  }
}
