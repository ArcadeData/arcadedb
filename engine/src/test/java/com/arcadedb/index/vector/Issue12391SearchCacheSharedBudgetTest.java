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
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.Type;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression for #12391: the automatically sized search cache of every vector index planned on the whole percent of the heap
 * free when it grew, and at least 8,192 entries, so 48 bucket caches of a 3 GB heap planned on 3.4 GB and filled 2.1 GB of it.
 * The percent is now the database's: what the other indexes' caches plan on is charged first.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue12391SearchCacheSharedBudgetTest extends TestHelper {
  private static final int DIMENSIONS = 32;
  /** The room the budget grants the first cache, and the smallest remainder worth handing to a later one (LSMVectorIndex.MIN_SEARCH_CACHE_SIZE). */
  private static final int FLOOR      = 8_192;
  /** What an index holding no vector plans on, and the smallest search cache (LSMVectorIndex.MIN_SEARCH_CACHE_SLOTS). */
  private static final int SLOTS      = 1_024;

  @Test
  void theSearchCachesOfTheDatabaseShareOnePercentOfTheHeap() {
    final LSMVectorIndex[] buckets = createBucketIndexes(database, "Doc", 2, null);
    final LSMVectorIndex index = buckets[0];
    final LSMVectorIndex sibling = buckets[1];
    final long bytesPerEntry = FlatVectorCache.bytesPerVector(DIMENSIONS);

    assertThat(index.affordableSearchCacheEntries(1, bytesPerEntry, 0L, false))
        .as("the database's first cache keeps the floor, even with nothing free").isEqualTo(FLOOR);

    final FlatVectorCache siblingCache = sibling.getSearchVectorCache();
    assertThat(siblingCache).as("precondition: the other bucket's index has a search cache").isNotNull();
    final long siblingPlans = siblingCache.capacity() * bytesPerEntry;

    // 1% of what is free is what the sibling plans on, plus a useful cache
    final long available = (siblingPlans + (FLOOR + 30) * bytesPerEntry) * 100;
    assertThat(index.affordableSearchCacheEntries(1, bytesPerEntry, available, false))
        .as("what the other index's cache plans on is charged to the same budget").isEqualTo(FLOOR + 30);

    assertThat(index.affordableSearchCacheEntries(1, bytesPerEntry, (siblingPlans + 30 * bytesPerEntry) * 100, false))
        .as("a remainder under the smallest useful cache is no cache: it would only churn its slots").isZero();

    assertThat(index.affordableSearchCacheEntries(1, bytesPerEntry, siblingPlans * 100, false))
        .as("a budget the other caches have taken leaves this one none, not the floor").isZero();

    // The heap reading is the one of the last collection: a sibling that fills its cache after it is in no figure the budget
    // can trust, so what it holds must not be credited back as free heap (code review on PR #12392)
    final VectorTypeSupport vts = VectorizationProvider.getInstance().getVectorTypeSupport();
    final float[] vector = new float[DIMENSIONS];
    for (int id = 0; id < 4_000; id++) {
      vector[id % DIMENSIONS] = id;
      siblingCache.put(id, vts.createFloatVector(vector.clone()));
    }
    assertThat(siblingCache.allocatedBytes()).as("precondition: the sibling's cache holds vectors now").isPositive();
    assertThat(index.affordableSearchCacheEntries(1, bytesPerEntry, available, false))
        .as("filling the sibling's cache under the same heap reading does not grow this one's share").isEqualTo(FLOOR + 30);
  }

  /**
   * A BINARY index holds two search caches, the vectors and the one-bit codes: sizing one of them charges the other, and
   * only the one being sized is left out.
   */
  @Test
  void aBinaryIndexChargesItsOtherCacheToTheSameBudget() {
    final LSMVectorIndex index = createBucketIndexes(database, "Bin", 1, "BINARY")[0];
    final FlatVectorCache vectors = index.getSearchVectorCache();
    assertThat(vectors).as("precondition: the index has a vector cache").isNotNull();
    final long vectorsPlan = vectors.capacity() * FlatVectorCache.bytesPerVector(DIMENSIONS);
    final long bytesPerCode = BinaryCodeCache.bytesPerCode(DIMENSIONS);

    final long available = (vectorsPlan + (FLOOR + 30) * bytesPerCode) * 100;
    assertThat(index.affordableSearchCacheEntries(1, bytesPerCode, available, true))
        .as("the codes are charged the vector cache of the same index").isEqualTo(FLOOR + 30);
    assertThat(index.affordableSearchCacheEntries(1, FlatVectorCache.bytesPerVector(DIMENSIONS), 0L, false))
        .as("the vector cache being sized is not charged to itself: alone in the database, it keeps the floor")
        .isEqualTo(FLOOR);
  }

  /**
   * The behaviour the issue reports, through the getter every search calls: the bucket indexes of one type each used to be
   * handed the floor and the whole percent of what was free, however many of them there were.
   */
  @Test
  void theBucketIndexesOfATypeAreHandedNoMoreThanOneBudgetBetweenThem() {
    final LSMVectorIndex[] buckets = createBucketIndexes(database, "Doc", 4, null);
    final long bytesPerEntry = FlatVectorCache.bytesPerVector(DIMENSIONS);
    // two caches of an empty index (the smallest ones) and a remainder one entry short of the smallest useful cache
    final long budget = (2L * SLOTS + FLOOR - 1) * bytesPerEntry;
    final long available = availableFor(budget);

    final FlatVectorCache first = buckets[0].getSearchVectorCache(() -> available);
    final FlatVectorCache second = buckets[1].getSearchVectorCache(() -> available);
    assertThat(first).as("the first cache is handed out").isNotNull();
    assertThat(first.capacity()).as("an empty index plans on the smallest cache, not on the floor").isEqualTo(SLOTS);
    assertThat(second).as("the remainder is a useful cache").isNotNull();
    assertThat(second.capacity()).isEqualTo(SLOTS);

    assertThat(buckets[2].getSearchVectorCache(() -> available))
        .as("what is left is under the smallest useful cache: none, not the floor and not a sliver").isNull();
    assertThat(buckets[3].getSearchVectorCache(() -> available)).as("and nothing for the one after").isNull();

    long planned = 0L;
    for (final LSMVectorIndex bucket : buckets)
      planned += plannedBytes(bucket);
    assertThat(planned).as("the caches of the type plan on no more than the budget").isLessThanOrEqualTo(budget);
  }

  /**
   * The behaviour of issue #13242: the 16 bucket indexes of a type that hold a few hundred vectors each plan on those vectors, not
   * on the 8,192-slot floor each, so a budget of three floor caches reaches every one of them. With the floor charged to each of them
   * the last indexes were left with no cache and read every vector of their searches from the graph file.
   */
  @Test
  void everyBucketIndexOfATypeThatHoldsLittleGetsACache() {
    final int buckets = 16;
    final LSMVectorIndex[] indexes = createBucketIndexes(database, "Doc", buckets, null);
    final long bytesPerEntry = FlatVectorCache.bytesPerVector(DIMENSIONS);
    final long budget = 3L * FLOOR * bytesPerEntry;
    final long available = availableFor(budget);

    long planned = 0L;
    for (final LSMVectorIndex index : indexes) {
      final FlatVectorCache cache = index.getSearchVectorCache(() -> available);
      assertThat(cache).as("a cache for %s", index.getName()).isNotNull();
      assertThat(cache.capacity()).isEqualTo(SLOTS);
      planned += cache.capacity() * bytesPerEntry;
    }
    assertThat(planned).as("the caches of the type plan on no more than the budget").isLessThanOrEqualTo(budget);
  }

  /** An index plans on the range of the vector ids it holds, rounded up to a power of two, once that range is past the smallest cache. */
  @Test
  void anIndexPlansOnTheRangeOfItsOwnVectorIds() {
    assertThat(capacityAfterInserting(1_500)).as("1,500 vector ids").isEqualTo(2_048);
  }

  /** The rounding rule at the edge of the smallest cache: exactly 1,024 ids fit it, one more id needs the next power of two. */
  @Test
  void theCapacityRoundsUpToThePowerOfTwoAtTheEdgeOfTheSmallestCache() {
    assertThat(capacityAfterInserting(SLOTS)).as("exactly %d vector ids", SLOTS).isEqualTo(SLOTS);
  }

  @Test
  void oneIdPastTheSmallestCacheDoublesIt() {
    assertThat(capacityAfterInserting(SLOTS + 1)).as("%d vector ids", SLOTS + 1).isEqualTo(2 * SLOTS);
  }

  /** The capacity the only index of a type is given for the vector ids {@code ids} it holds, with room for far more than that. */
  private int capacityAfterInserting(final int ids) {
    final LSMVectorIndex index = createBucketIndexes(database, "Doc", 1, null)[0];
    final float[] vector = new float[DIMENSIONS];
    database.transaction(() -> {
      for (int i = 0; i < ids; i++) {
        vector[i % DIMENSIONS] = i + 1;
        database.newDocument("Doc").set("embedding", vector.clone()).save();
      }
    });
    final long available = availableFor(100L * FLOOR * FlatVectorCache.bytesPerVector(DIMENSIONS));

    final FlatVectorCache cache = index.getSearchVectorCache(() -> available);
    assertThat(cache).isNotNull();
    return cache.capacity();
  }

  /**
   * Two bucket indexes growing at once each read a budget the other was about to take. 16 threads, room for three caches.
   */
  @Test
  void bucketIndexesGrowingAtOnceDoNotTakeTheSameRoom() throws Exception {
    final int threads = 16;
    final LSMVectorIndex[] buckets = createBucketIndexes(database, "Doc", threads, null);
    // room for three caches of an empty index and a remainder one entry short of the smallest useful cache after them
    final long budget = (3L * SLOTS + FLOOR - 1) * FlatVectorCache.bytesPerVector(DIMENSIONS);
    final long available = availableFor(budget);

    final ExecutorService pool = Executors.newFixedThreadPool(threads);
    try {
      final CountDownLatch ready = new CountDownLatch(threads);
      final CountDownLatch go = new CountDownLatch(1);
      final List<Future<FlatVectorCache>> grown = new ArrayList<>();
      for (final LSMVectorIndex bucket : buckets)
        grown.add(pool.submit(() -> {
          ready.countDown();
          go.await();
          return bucket.getSearchVectorCache(() -> available);
        }));
      assertThat(ready.await(30, TimeUnit.SECONDS)).isTrue();
      go.countDown();

      int withCache = 0;
      for (final Future<FlatVectorCache> future : grown)
        if (future.get(60, TimeUnit.SECONDS) != null)
          withCache++;
      assertThat(withCache).as("room for three whole caches, and no more than that handed out").isEqualTo(3);

      long planned = 0L;
      for (final LSMVectorIndex bucket : buckets)
        planned += plannedBytes(bucket);
      assertThat(planned).isLessThanOrEqualTo(budget);
    } finally {
      pool.shutdownNow();
    }
  }

  /**
   * An index left with no room is asked again on every search: that must not cost a heap reading and a walk over the schema
   * each time.
   */
  @Test
  void anIndexWithNoRoomDoesNotPayForTheBudgetOnEverySearch() {
    final LSMVectorIndex[] buckets = createBucketIndexes(database, "Doc", 2, null);
    final long available = availableFor(FLOOR * FlatVectorCache.bytesPerVector(DIMENSIONS));
    assertThat(buckets[0].getSearchVectorCache(() -> available)).as("precondition: the first index takes the budget").isNotNull();

    final AtomicInteger readings = new AtomicInteger();
    final int searches = 1_000;
    for (int i = 0; i < searches; i++)
      assertThat(buckets[1].getSearchVectorCache(() -> {
        readings.incrementAndGet();
        return available;
      })).as("the budget is taken").isNull();

    // A decline is remembered for about a second, and a stop-the-world pause longer than that between two searches costs one
    // more reading: the bound is a tripwire between "once in a while" and "on every search", not a latency
    assertThat(readings.get()).as("heap readings in %d searches", searches).isPositive().isLessThan(searches / 10);
  }

  /**
   * The share is the database's: a second database in the same JVM is not charged for the caches of the first.
   */
  @Test
  void twoDatabasesDoNotChargeEachOther() {
    final LSMVectorIndex[] mine = createBucketIndexes(database, "Doc", 2, null);
    assertThat(mine[0].getSearchVectorCache(() -> 0L)).as("the first cache of this database is the floor").isNotNull();
    assertThat(mine[1].getSearchVectorCache(() -> 0L)).as("nothing is left for the second").isNull();

    final Database other = createDatabase("target/databases/Issue12391SearchCacheSharedBudgetTestOther");
    try {
      final LSMVectorIndex first = createBucketIndexes(other, "Doc", 1, null)[0];
      final FlatVectorCache cache = first.getSearchVectorCache(() -> 0L);
      assertThat(cache).as("the first cache of the other database is the floor too, whatever this one holds").isNotNull();
      assertThat(cache.capacity()).as("an empty index plans on the smallest cache").isEqualTo(SLOTS);
    } finally {
      other.drop();
    }
  }

  private static LSMVectorIndex[] createBucketIndexes(final Database db, final String typeName, final int buckets,
      final String quantization) {
    db.transaction(() -> {
      db.getSchema().buildDocumentType().withName(typeName).withTotalBuckets(buckets).create()
          .createProperty("embedding", Type.ARRAY_OF_FLOATS);
      db.command("sql", "CREATE INDEX ON " + typeName + " (embedding) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
          + ", \"similarity\": \"COSINE\"" + (quantization != null ? ", \"quantization\": \"" + quantization + "\"" : "")
          + " }");
    });
    final IndexInternal[] indexes = ((TypeIndex) db.getSchema().getIndexByName(typeName + "[embedding]")).getIndexesOnBuckets();
    assertThat(indexes).hasSize(buckets);
    final LSMVectorIndex[] result = new LSMVectorIndex[buckets];
    for (int i = 0; i < buckets; i++)
      result[i] = (LSMVectorIndex) indexes[i];
    return result;
  }

  /** The smallest heap reading whose share of the percent the database is configured with affords {@code budgetBytes}. */
  private long availableFor(final long budgetBytes) {
    final int percent = Math.min(((DatabaseInternal) database).getConfiguration()
        .getValueAsInteger(GlobalConfiguration.VECTOR_INDEX_SEARCH_CACHE_MAX_HEAP_PERCENT), 90);
    assertThat(percent).as("precondition: the search cache is sized from a share of the heap").isPositive();
    return (budgetBytes / percent + 1) * 100;
  }

  private static long plannedBytes(final LSMVectorIndex index) {
    final FlatVectorCache cache = index.getSearchVectorCache(() -> 0L);
    return cache != null ? cache.capacity() * FlatVectorCache.bytesPerVector(DIMENSIONS) : 0L;
  }
}
