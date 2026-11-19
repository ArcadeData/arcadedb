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
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.RID;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.Pair;
import com.arcadedb.utility.VectorApiSupport;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;
import static org.assertj.core.data.Offset.offset;

/**
 * Caches used by the vector search. Each nested class keeps the fixture of the regression test it was folded from.
 */
class VectorSearchCacheTest {
  /**
   * Regression for #11003: flat search cache.
   * <p>
   * Issue #11003: a search scores the graph nodes it visits on the vectors of the index-scoped {@link FlatVectorCache}, where
   * they lie, instead of materializing each one through a chain of objects. The answer must not depend on whether a vector
   * was resident: a search over a cold cache (every node read from its page and scored by the same kernel) and the same
   * search over the warm cache (every node scored in place) return the same rows with the same distances, and those
   * distances are the ones the similarity function gives the stored vectors - up to the last bit, which the Vector API's
   * lane reduction can move between a kernel the JIT has compiled and one it has not.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class FlatSearchCache {
    private static final String            DB_PATH    = "target/databases/Issue11003FlatSearchCacheTest";
    private static final int               DIMENSIONS = 24;
    private static final int               VECTORS    = 600;
    private static final int               K          = 10;
    private static final int               EF_SEARCH  = 64;
    private static final VectorTypeSupport VTS        = VectorApiSupport.jvectorProvider().getVectorTypeSupport();

    @BeforeEach
    @AfterEach
    void cleanup() {
      FileUtils.deleteRecursively(new File(DB_PATH));
    }

    @ParameterizedTest
    @CsvSource({ "EUCLIDEAN, false", "COSINE, false", "DOT_PRODUCT, false", "EUCLIDEAN, true", "COSINE, true" })
    void aWarmSearchScoresInPlaceAndAnswersExactlyAsTheColdOne(final VectorSimilarityFunction function,
        final boolean storeVectorsInGraph) {
      final Random random = new Random(11003);
      final Map<RID, float[]> stored = new HashMap<>();

      try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
        final Database db = factory.create();
        try {
          db.transaction(() -> {
            final var type = db.getSchema().createDocumentType("Doc");
            type.createProperty("vector", Type.ARRAY_OF_FLOATS);
            db.command("sql", "CREATE INDEX ON Doc (vector) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
                + ", \"similarity\": \"" + function + "\", \"quantization\": \"NONE\", \"storeVectorsInGraph\": " + storeVectorsInGraph
                + " }");
          });
          db.transaction(() -> {
            for (int i = 0; i < VECTORS; i++) {
              final float[] vector = unitVector(random);
              stored.put(db.newDocument("Doc").set("vector", vector).save().getIdentity(), vector);
            }
          });

          final LSMVectorIndex index = (LSMVectorIndex) db.getSchema().getType("Doc").getPolymorphicIndexByProperties("vector")
              .getIndexesOnBuckets()[0];
          // Builds the graph over every row, so the walks below are the only thing that reads vectors
          assertThat(index.findNeighborsFromVector(unitVector(random), K, EF_SEARCH)).hasSize(K);
          assertThat(index.getStats().get("graphNodeCount")).isEqualTo((long) VECTORS);

          for (int q = 0; q < 5; q++) {
            final float[] query = unitVector(random);

            // Cold: nothing resident, so every node the walk visits is read from its page, cached and scored
            final FlatVectorCache cache = index.getSearchVectorCache();
            cache.clear();
            final List<Pair<RID, Float>> cold = index.findNeighborsFromVector(query, K, EF_SEARCH);
            assertThat(cold).hasSize(K);
            assertThat(cache.size()).as("the cold walk fills the cache").isPositive();

            // Warm: the same walk, every node now scored where its vector lies
            cache.resetStats();
            final List<Pair<RID, Float>> warm = index.findNeighborsFromVector(query, K, EF_SEARCH);
            assertThat(cache.getMisses()).as("the warm walk materializes no vector").isZero();
            assertThat(cache.getHits()).as("the warm walk is served by the cache").isPositive();

            assertThat(warm).hasSameSizeAs(cold);
            for (int i = 0; i < cold.size(); i++) {
              assertThat(warm.get(i).getFirst()).as("row %d", i).isEqualTo(cold.get(i).getFirst());
              assertThat(warm.get(i).getSecond()).as("distance of row %d", i).isCloseTo(cold.get(i).getSecond(), within(1e-6f));
              final float expected = LSMVectorIndex.scoreToDistance(function,
                  function.compare(VTS.createFloatVector(query), VTS.createFloatVector(stored.get(cold.get(i).getFirst()))));
              assertThat(cold.get(i).getSecond()).as("distance of row %d is the similarity of the stored vector", i)
                  .isCloseTo(expected, within(1e-6f));
            }
          }
        } finally {
          db.drop();
        }
      }
    }

    @ParameterizedTest
    @CsvSource({ "false", "true" })
    void aDeletedVectorLeavesTheCacheAndTheAnswers(final boolean storeVectorsInGraph) {
      final Random random = new Random(110031);
      try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
        final Database db = factory.create();
        try {
          db.transaction(() -> {
            final var type = db.getSchema().createDocumentType("Doc");
            type.createProperty("vector", Type.ARRAY_OF_FLOATS);
            db.command("sql", "CREATE INDEX ON Doc (vector) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
                + ", \"similarity\": \"EUCLIDEAN\", \"storeVectorsInGraph\": " + storeVectorsInGraph + " }");
          });
          final float[][] vectors = new float[VECTORS][];
          final RID[] rids = new RID[VECTORS];
          db.transaction(() -> {
            for (int i = 0; i < VECTORS; i++) {
              vectors[i] = unitVector(random);
              rids[i] = db.newDocument("Doc").set("vector", vectors[i]).save().getIdentity();
            }
          });

          final LSMVectorIndex index = (LSMVectorIndex) db.getSchema().getType("Doc").getPolymorphicIndexByProperties("vector")
              .getIndexesOnBuckets()[0];

          // Querying with a stored vector finds that row first, and leaves its vector resident
          final List<Pair<RID, Float>> before = index.findNeighborsFromVector(vectors[42], K, EF_SEARCH);
          assertThat(before.getFirst().getFirst()).isEqualTo(rids[42]);
          final FlatVectorCache cache = index.getSearchVectorCache();
          final int residentBefore = cache.size();
          assertThat(residentBefore).isPositive();

          db.transaction(() -> db.deleteRecord(db.lookupByRID(rids[42], true)));
          assertThat(cache.size()).as("the deleted vector is evicted").isEqualTo(residentBefore - 1);

          final List<Pair<RID, Float>> after = index.findNeighborsFromVector(vectors[42], K, EF_SEARCH);
          assertThat(after).hasSize(K);
          assertThat(after).extracting(Pair::getFirst).doesNotContain(rids[42]);
        } finally {
          db.drop();
        }
      }
    }

    private static float[] unitVector(final Random random) {
      final float[] vector = new float[DIMENSIONS];
      double norm = 0;
      for (int i = 0; i < DIMENSIONS; i++) {
        vector[i] = (float) random.nextGaussian();
        norm += vector[i] * vector[i];
      }
      final float scale = (float) (1 / Math.sqrt(norm));
      for (int i = 0; i < DIMENSIONS; i++)
        vector[i] *= scale;
      return vector;
    }
  }

  /**
   * Regression for #11071: search cache capacity.
   * <p>
   * Issue #11071: the search caches were sized to a power of two, and when the round-up of the id range did not fit the heap
   * budget it was rounded DOWN, so a 100,000-vector index of 1536 dimensions under a budget of about 120,000 entries got 65,536
   * slots and the ids past them evicted the ids they shared a slot with on every search. The caches now take any capacity
   * and the rule cuts a round-up that does not fit to the budget itself.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class SearchCacheCapacity {
    private static final VectorTypeSupport VTS = VectorApiSupport.jvectorProvider().getVectorTypeSupport();

    @Test
    void aBudgetThatCoversTheIdRangeButNotItsRoundUpGivesEveryIdASlot() {
      // The reporter's numbers: 100,000 ids, 6,152 bytes an entry, 25% of about 3 GB available
      final long affordable = 750_000_000L / FlatVectorCache.bytesPerVector(1536);
      assertThat(affordable).isBetween(100_000L, 131_071L);

      final int capacity = LSMVectorIndex.searchCacheCapacity(100_000, affordable, 0);
      assertThat(capacity).isGreaterThanOrEqualTo(100_000);
      assertThat((long) capacity).isLessThanOrEqualTo(affordable);
    }

    @Test
    void aBudgetThatCoversTheRoundUpTakesItForGrowth() {
      assertThat(LSMVectorIndex.searchCacheCapacity(100_000, 1_000_000, 0)).isEqualTo(131_072);
      assertThat(LSMVectorIndex.searchCacheCapacity(131_072, 1_000_000, 0)).isEqualTo(131_072);
      // The cache that covered 65,536 ids doubles when the range passes it
      assertThat(LSMVectorIndex.searchCacheCapacity(70_000, 1_000_000, 65_536)).isEqualTo(131_072);
    }

    @Test
    void aBudgetSmallerThanTheIdRangeIsUsedWholeNotHalved() {
      // Was 65,536: the power of two at or below the budget
      assertThat(LSMVectorIndex.searchCacheCapacity(200_000, 122_000, 0)).isEqualTo(122_000);
    }

    @Test
    void theCapacityNeverShrinksAndDoesNotGrowByAFewSlotsAtATime() {
      // A heap reading that moved by a few hundred entries does not start a cut cache over
      assertThat(LSMVectorIndex.searchCacheCapacity(200_000, 122_500, 122_000)).isEqualTo(122_000);
      // ... unless the growth makes the cache cover every id
      assertThat(LSMVectorIndex.searchCacheCapacity(122_300, 122_500, 122_000)).isEqualTo(122_500);
      // ... or is worth it: an eighth or more
      assertThat(LSMVectorIndex.searchCacheCapacity(300_000, 140_000, 122_000)).isEqualTo(140_000);
      // A budget that shrank keeps the cache as it is
      assertThat(LSMVectorIndex.searchCacheCapacity(300_000, 50_000, 122_000)).isEqualTo(122_000);
      // A cache that covers the range is answered as it is
      assertThat(LSMVectorIndex.searchCacheCapacity(100_000, 50_000, 122_000)).isEqualTo(122_000);
    }

    /**
     * A capacity past what the cache can hold would never be reached, and the cache rebuilt on every search for it: the wanted
     * capacity is capped at the cache's own maximum, where it stays.
     */
    @Test
    void theCapacityStaysAtTheMostTheCacheCanHold() {
      final int max = BinaryCodeCache.maxCapacity(1536);
      final long idRange = 4L * max;

      final int first = LSMVectorIndex.searchCacheCapacity(idRange, max, 0);
      assertThat(first).isEqualTo(max);
      assertThat(LSMVectorIndex.searchCacheCapacity(idRange, max, first)).isEqualTo(first);
    }

    @Test
    void aFlatCacheOfAnyCapacityKeepsEveryIdBelowItInASlotOfItsOwn() {
      final int capacity = 100;
      // 4 dimensions in chunks of 64 floats: 16 slots a chunk, the last chunk cut to the 4 slots the capacity leaves it
      final FlatVectorCache cache = new FlatVectorCache(4, capacity, 64);
      assertThat(cache.capacity()).isEqualTo(capacity);

      for (int id = 0; id < capacity; id++)
        cache.put(id, vector(id, 1f, 2f, 3f));
      assertThat(cache.size()).isEqualTo(capacity);
      for (int id = 0; id < capacity; id++)
        assertThat(cache.get(id).get(0)).isEqualTo(id);
      assertThat(cache.allocatedBytes()).isEqualTo((long) capacity * 4 * Float.BYTES);

      final VectorFloat<?> query = vector(0f, 0f, 0f, 0f);
      assertThat(cache.similarity(99, query, VectorSimilarityFunction.EUCLIDEAN))
          .isEqualTo(VectorSimilarityFunction.EUCLIDEAN.compare(query, vector(99f, 1f, 2f, 3f)));

      // Past the capacity an id takes the slot of the remainder: 105 that of 5
      cache.put(105, vector(105f, 0f, 0f, 0f));
      assertThat(cache.get(5)).isNull();
      assertThat(cache.get(105).get(0)).isEqualTo(105f);
      assertThat(cache.get(6).get(0)).isEqualTo(6f);
      assertThat(cache.size()).isEqualTo(capacity);
      cache.remove(105);
      assertThat(cache.size()).isEqualTo(capacity - 1);
    }

    @Test
    void aBinaryCodeCacheOfAnyCapacityKeepsEveryIdBelowItInASlotOfItsOwn() {
      final int capacity = 100;
      final BinaryCodeCache cache = new BinaryCodeCache(64, capacity);
      assertThat(cache.capacity()).isEqualTo(capacity);

      final long[] code = new long[1];
      for (int id = 0; id < capacity; id++) {
        code[0] = id;
        cache.put(id, code, 0);
      }
      assertThat(cache.size()).isEqualTo(capacity);
      final long[] zero = new long[1];
      for (int id = 0; id < capacity; id++)
        assertThat(cache.hammingDistance(id, zero)).isEqualTo(Long.bitCount(id));

      code[0] = -1L;
      cache.put(105, code, 0);
      assertThat(cache.hammingDistance(5, zero)).isEqualTo(-1);
      assertThat(cache.hammingDistance(105, zero)).isEqualTo(64);
      assertThat(cache.size()).isEqualTo(capacity);
    }

    private static VectorFloat<?> vector(final float... components) {
      return VTS.createFloatVector(components);
    }
  }

  /**
   * Regression for #8957: COMPACT INDEX clears the search vector cache.
   * <p>
   * Issue #8957: COMPACT INDEX renumbers every vector id, but the search vector cache is keyed by vector id and was left
   * populated, so a cached id answered with the vector of the record that held it before the compaction.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class CompactionSearchCache extends TestHelper {
    private static final int DIMENSIONS = 16;

    private final Map<RID, float[]> vectors = new LinkedHashMap<>();

    @Test
    void searchAfterCompactionScoresTheRightVectors() {
      database.command("sql", "CREATE DOCUMENT TYPE Doc BUCKETS 1");
      database.command("sql", "CREATE PROPERTY Doc.vector ARRAY_OF_FLOATS");
      database.command("sql",
          "CREATE INDEX ON Doc (vector) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS + ", \"similarity\": \"COSINE\" }");

      final Random random = new Random(42);
      database.transaction(() -> {
        for (int i = 0; i < 2000; i++) {
          final float[] v = randomVector(random);
          vectors.put(database.newDocument("Doc").set("vector", v).save().getIdentity(), v);
        }
      });
      database.transaction(() -> {
        final List<RID> rids = new ArrayList<>(vectors.keySet());
        for (int i = 0; i < 500; i++) {
          final float[] v = randomVector(random);
          database.lookupByRID(rids.get(i), true).asDocument().modify().set("vector", v).save();
          vectors.put(rids.get(i), v);
        }
      });

      assertThatEveryQueryFindsItsOwnRecord();
      database.command("sql", "COMPACT INDEX `Doc[vector]`").close();
      assertThatEveryQueryFindsItsOwnRecord();
    }

    private void assertThatEveryQueryFindsItsOwnRecord() {
      final LSMVectorIndex index = (LSMVectorIndex) ((TypeIndex) database.getSchema().getIndexByName("Doc[vector]")).getIndexesOnBuckets()[0];
      final List<RID> rids = new ArrayList<>(vectors.keySet());
      for (int q = 0; q < 100; q++) {
        final RID own = rids.get(q * 20);
        final float[] query = vectors.get(own);
        final List<Pair<RID, Float>> hits = index.findNeighborsFromVector(query, 5, 100);
        assertThat(hits.get(0).getFirst()).isEqualTo(own);
        for (final Pair<RID, Float> hit : hits)
          assertThat((double) hit.getSecond()).isCloseTo(cosineDistance(query, vectors.get(hit.getFirst())),
              offset(1e-4));
      }
    }

    private static float[] randomVector(final Random random) {
      final float[] v = new float[DIMENSIONS];
      for (int d = 0; d < DIMENSIONS; d++)
        v[d] = random.nextFloat() * 2 - 1;
      return v;
    }

    private static double cosineDistance(final float[] a, final float[] b) {
      double dot = 0, na = 0, nb = 0;
      for (int i = 0; i < a.length; i++) {
        dot += a[i] * b[i];
        na += a[i] * a[i];
        nb += b[i] * b[i];
      }
      return 1 - dot / Math.sqrt(na * nb);
    }
  }
}
