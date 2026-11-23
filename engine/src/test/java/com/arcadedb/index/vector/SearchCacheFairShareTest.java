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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The automatically sized search caches of a database share one budget (issue #12391). Their plans used to be the range of the
 * vector ids of each index rounded up to a power of two, taken one index after the other in the order they were first searched,
 * so with two types of 16 bucket indexes the first type's caches planned on the budget and every index of the second type was
 * left with no cache (issue #13260). A plan is still that range, and when the plans together are past the budget each
 * is cut to the level of a water-filling of the budget over all of them.
 */
class SearchCacheFairShareTest extends TestHelper {
  private static final int DIMENSIONS = 32;
  /** The floor of the first cache, and the smallest cache worth publishing past the share an index is owed (LSMVectorIndex.MIN_SEARCH_CACHE_SIZE). */
  private static final int FLOOR      = 8_192;
  /** The smallest plan of an index (LSMVectorIndex.MIN_SEARCH_CACHE_SLOTS). */
  private static final int SLOTS      = 1_024;

  /**
   * Updates leave the id of the vector they replace behind in the range of ids. The cache is direct-mapped by id, so while the
   * budget affords it the plan covers the range and not only the live vectors: a plan on 1,500 live vectors left the ids past it
   * sharing the slots of the ones below, and a third of the lookups of a search missed.
   */
  @Test
  void aPlanCoversTheIdsAnUpdateLeavesBehindWhileTheBudgetAffordsThem() {
    final LSMVectorIndex index = createBucketIndexes("Doc", 1)[0];
    insert("Doc", 1_500);
    for (int round = 0; round < 3; round++)
      replaceEveryEmbedding("Doc", round + 1);
    assertThat(index.countEntries()).as("precondition: the index holds 1,500 live vectors, 4 times as many ids").isEqualTo(1_500);

    final long available = availableFor(100L * FLOOR * FlatVectorCache.bytesPerVector(DIMENSIONS));
    final FlatVectorCache cache = index.getSearchVectorCache(() -> available);
    assertThat(cache).isNotNull();
    assertThat(cache.capacity()).as("6,000 ids, rounded up to a power of two").isEqualTo(8_192);
  }

  /**
   * The plans of 8 indexes of 1,100 vectors are 8 x 2,048 entries, the budget affords 10,000: every index keeps a cache, as large
   * as the budget cut evenly (1,250 entries) leaves it, and they add up to the budget.
   */
  @Test
  void everyIndexIsCachedWhenTheirPlansTogetherAreOverTheBudget() {
    final LSMVectorIndex[] indexes = createBucketIndexes("Doc", 8);
    insert("Doc", 8 * 1_100);
    final long budget = 10_000L * FlatVectorCache.bytesPerVector(DIMENSIONS);
    final long available = availableFor(budget);

    long planned = 0L;
    for (final LSMVectorIndex index : indexes) {
      final FlatVectorCache cache = index.getSearchVectorCache(() -> available);
      assertThat(cache).as("a cache for %s", index.getName()).isNotNull();
      assertThat(cache.capacity()).as("the plan of %s covers its live vectors", index.getName()).isGreaterThanOrEqualTo(1_100);
      planned += cache.capacity() * FlatVectorCache.bytesPerVector(DIMENSIONS);
    }
    assertThat(planned).as("the plans of the type are no more than the budget").isLessThanOrEqualTo(budget);
  }

  /**
   * An index that wants less than the level keeps all it wants, however late it is sized, and the one that wants more is cut to
   * what is left of the budget: 100 vectors and 9,000 vectors, 10,000 entries.
   */
  @Test
  void aSmallIndexKeepsItsWholePlanWhenALargeOneIsCut() {
    final LSMVectorIndex small = createBucketIndexes("Small", 1)[0];
    final LSMVectorIndex large = createBucketIndexes("Large", 1)[0];
    insert("Small", 100);
    insert("Large", 9_000);
    final long available = availableFor(10_000L * FlatVectorCache.bytesPerVector(DIMENSIONS));

    final FlatVectorCache largeCache = large.getSearchVectorCache(() -> available);
    assertThat(largeCache).isNotNull();
    assertThat(largeCache.capacity()).as("9,000 vectors want 16,384 entries: cut to what leaves the small index its 1,024")
        .isLessThanOrEqualTo(10_000 - SLOTS);

    final FlatVectorCache smallCache = small.getSearchVectorCache(() -> available);
    assertThat(smallCache).as("sized after the large one").isNotNull();
    assertThat(smallCache.capacity()).isEqualTo(SLOTS);
    assertThat(largeCache.capacity() + smallCache.capacity()).isLessThanOrEqualTo(10_000);
  }

  /**
   * A database that has just been opened: the locations of the indexes are not loaded, the first search of an index does not load
   * its siblings' to count their vectors, and the budget is divided among them as if each wanted all of it.
   */
  @Test
  void aReopenedDatabaseDividesTheBudgetBeforeTheIndexesAreLoaded() {
    createBucketIndexes("Doc", 4);
    insert("Doc", 4 * 2_100);
    reopenDatabase();
    final LSMVectorIndex[] indexes = bucketIndexes("Doc", 4);
    for (final LSMVectorIndex index : indexes)
      assertThat(index.areLocationsMaterializedForTest()).as("precondition: %s is not loaded", index.getName()).isFalse();
    final long budget = 12_000L * FlatVectorCache.bytesPerVector(DIMENSIONS);
    final long available = availableFor(budget);

    long planned = 0L;
    for (final LSMVectorIndex index : indexes) {
      final FlatVectorCache cache = index.getSearchVectorCache(() -> available);
      assertThat(cache).as("a cache for %s", index.getName()).isNotNull();
      assertThat(cache.capacity()).as("the share of %s covers its 2,100 vectors", index.getName()).isGreaterThanOrEqualTo(2_100);
      planned += cache.capacity() * FlatVectorCache.bytesPerVector(DIMENSIONS);
    }
    assertThat(planned).isLessThanOrEqualTo(budget);
  }

  /**
   * The heap reading falls as the caches already planned fill, and the others' plans are charged whole against it: the index sized
   * next finds less than its share left. It keeps the smallest plan the indexes still to be sized are owed out of what is left
   * after the fall its own fill causes (a percent of what it plans), so that the last of them is cached too.
   */
  @Test
  void aBudgetThatFellLeavesTheLastIndexesTheirSmallestPlan() {
    final LSMVectorIndex[] indexes = createBucketIndexes("Doc", 4);
    insert("Doc", 4 * 2_100);
    final long bytesPerEntry = FlatVectorCache.bytesPerVector(DIMENSIONS);
    final long plenty = availableFor(9_000L * bytesPerEntry);

    final FlatVectorCache first = indexes[0].getSearchVectorCache(() -> plenty);
    final FlatVectorCache second = indexes[1].getSearchVectorCache(() -> plenty);
    assertThat(first).isNotNull();
    assertThat(second).isNotNull();
    final long fell = availableFor((first.capacity() + second.capacity() + 2_200L) * bytesPerEntry);

    final FlatVectorCache third = indexes[2].getSearchVectorCache(() -> fell);
    assertThat(third).as("what is left, past the smallest plan of the last index, is a cache").isNotNull();
    assertThat(third.capacity()).as("not more than what leaves the last index its smallest plan").isBetween(SLOTS, 2_200 - SLOTS);
    final FlatVectorCache fourth = indexes[3].getSearchVectorCache(() -> fell);
    assertThat(fourth).as("the last index has at least its smallest plan").isNotNull();
    assertThat(fourth.capacity()).isGreaterThanOrEqualTo(SLOTS);
  }

  /**
   * The smallest plan an index without a cache is owed is counted in bytes, whatever the index that is sized first costs per entry:
   * 64 dimensions (264 bytes an entry) first, 768 dimensions (3,080 bytes) with no cache yet, a budget of 5,500,000 bytes.
   */
  @Test
  void theFirstCacheLeavesAnIndexOfOtherDimensionsItsSmallestPlan() {
    final LSMVectorIndex narrow = createBucketIndexes("Narrow", 1, 64)[0];
    createBucketIndexes("Wide", 1, 768);
    final long narrowBytes = FlatVectorCache.bytesPerVector(64);
    final long wideBytes = FlatVectorCache.bytesPerVector(768);
    final int percent = Math.min(((DatabaseInternal) database).getConfiguration()
        .getValueAsInteger(GlobalConfiguration.VECTOR_INDEX_SEARCH_CACHE_MAX_HEAP_PERCENT), 90);
    final long available = availableFor(5_500_000L);
    final long budget = available / 100 * percent;

    final long entries = narrow.affordableSearchCacheEntries(percent, narrowBytes, available, false);
    assertThat(entries).as("what is left past the 1,024 entries of the wide index")
        .isEqualTo((budget - SLOTS * wideBytes) / narrowBytes);
  }

  /** The cache an index was cut to is the one it keeps: asking again with the same reading does not rebuild it. */
  @Test
  void aCutCacheIsNotRebuiltOnEverySearch() {
    final LSMVectorIndex[] indexes = createBucketIndexes("Doc", 4);
    insert("Doc", 4 * 2_100);
    final long available = availableFor(12_000L * FlatVectorCache.bytesPerVector(DIMENSIONS));

    final FlatVectorCache first = indexes[0].getSearchVectorCache(() -> available);
    assertThat(first).isNotNull();
    for (int i = 0; i < 5; i++)
      assertThat(indexes[0].getSearchVectorCache(() -> available)).isSameAs(first);
  }

  @Test
  void theLevelOfTheBudgetIsTheWaterFilling() {
    assertThat(LSMVectorIndex.fairShareBytes(new long[] { 100, 200, 300 }, 3, 1_000)).as("everything fits").isEqualTo(Long.MAX_VALUE);
    assertThat(LSMVectorIndex.fairShareBytes(new long[] { 100, 1_000, 1_000 }, 3, 1_000)).as("the small one keeps its 100")
        .isEqualTo(450);
    assertThat(LSMVectorIndex.fairShareBytes(new long[] { 500, 500, 500, 500 }, 4, 1_000)).isEqualTo(250);
    assertThat(LSMVectorIndex.fairShareBytes(new long[] { Long.MAX_VALUE, 100, Long.MAX_VALUE }, 3, 1_000))
        .as("a cache of an index not loaded wants the whole budget").isEqualTo(450);
    assertThat(LSMVectorIndex.fairShareBytes(new long[] { 5, 5 }, 2, 0)).as("no budget").isZero();
  }

  private LSMVectorIndex[] createBucketIndexes(final String typeName, final int buckets) {
    return createBucketIndexes(typeName, buckets, DIMENSIONS);
  }

  private LSMVectorIndex[] createBucketIndexes(final String typeName, final int buckets, final int dimensions) {
    database.transaction(() -> {
      database.getSchema().buildDocumentType().withName(typeName).withTotalBuckets(buckets).create()
          .createProperty("embedding", Type.ARRAY_OF_FLOATS);
      database.command("sql", "CREATE INDEX ON " + typeName + " (embedding) LSM_VECTOR METADATA { \"dimensions\": " + dimensions
          + ", \"similarity\": \"COSINE\" }");
    });
    return bucketIndexes(typeName, buckets);
  }

  private LSMVectorIndex[] bucketIndexes(final String typeName, final int buckets) {
    final IndexInternal[] indexes = ((TypeIndex) database.getSchema().getIndexByName(typeName + "[embedding]")).getIndexesOnBuckets();
    assertThat(indexes).hasSize(buckets);
    final LSMVectorIndex[] result = new LSMVectorIndex[buckets];
    for (int i = 0; i < buckets; i++)
      result[i] = (LSMVectorIndex) indexes[i];
    return result;
  }

  private void insert(final String typeName, final int count) {
    database.transaction(() -> {
      for (int i = 0; i < count; i++)
        database.newDocument(typeName).set("embedding", vector(i, 0)).save();
    });
  }

  /** Gives every document of the type a new embedding: the old vector id is tombstoned and a new one is minted. */
  private void replaceEveryEmbedding(final String typeName, final int round) {
    final List<MutableDocument> documents = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT FROM " + typeName)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        documents.add(row.getElement().get().asDocument().modify());
      }
    }
    database.transaction(() -> {
      int i = 0;
      for (final MutableDocument document : documents)
        document.set("embedding", vector(i++, round)).save();
    });
  }

  private static float[] vector(final int seed, final int round) {
    final float[] vector = new float[DIMENSIONS];
    vector[seed % DIMENSIONS] = 1f + seed;
    vector[(seed + round + 1) % DIMENSIONS] += 1f;
    return vector;
  }

  /** The smallest heap reading whose share of the percent the database is configured with affords {@code budgetBytes}. */
  private long availableFor(final long budgetBytes) {
    final int percent = Math.min(((DatabaseInternal) database).getConfiguration()
        .getValueAsInteger(GlobalConfiguration.VECTOR_INDEX_SEARCH_CACHE_MAX_HEAP_PERCENT), 90);
    assertThat(percent).as("precondition: the search cache is sized from a share of the heap").isPositive();
    return (budgetBytes / percent + 1) * 100;
  }
}
