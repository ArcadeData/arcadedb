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
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.RID;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #5412: the search-time vector cache used to be created fresh for every
 * single query (and capped at 1024 entries), so the vectors visited during beam search were re-read
 * from the documents on every query, even when the very same query was repeated.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class LSMVectorIndexSearchCacheTest {
  private static final String DB_PATH    = "target/test-databases/LSMVectorIndexSearchCacheTest";
  private static final int    DIMENSIONS = 64;
  private static final int    NUM_VECTORS = 3_000;
  private static final int    K          = 10;
  private static final int    EF_SEARCH  = 100;

  @BeforeEach
  @AfterEach
  void cleanup() {
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  @Test
  void repeatedQueriesReuseTheVectorCacheAcrossSearches() {
    final Random rnd = new Random(31);
    final float[][] vectors = new float[NUM_VECTORS][DIMENSIONS];
    for (int i = 0; i < NUM_VECTORS; i++)
      for (int d = 0; d < DIMENSIONS; d++)
        vectors[i][d] = rnd.nextFloat();

    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      try (final Database db = factory.create()) {
        db.transaction(() -> {
          final var type = db.getSchema().createDocumentType("Doc");
          type.createProperty("id", Type.INTEGER);
          type.createProperty("vector", Type.ARRAY_OF_FLOATS);
          db.command("sql", "CREATE INDEX ON Doc (vector) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
              + ", \"similarity\": \"COSINE\" }");
        });

        db.begin();
        for (int i = 0; i < NUM_VECTORS; i++) {
          db.newDocument("Doc").set("id", i).set("vector", vectors[i]).save();
          if (i % 1000 == 999) {
            db.commit();
            db.begin();
          }
        }
        db.commit();
      }

      // Reopen so the graph is loaded/rebuilt from disk and searches go through ArcadePageVectorValues
      try (final Database db = factory.open()) {
        final LSMVectorIndex index = (LSMVectorIndex) db.getSchema().getType("Doc")
            .getPolymorphicIndexByProperties("vector").getIndexesOnBuckets()[0];

        final float[] query = vectors[7];

        // First query triggers the graph build and warms the cache
        final List<Pair<RID, Float>> first = index.findNeighborsFromVector(query, K, EF_SEARCH);
        assertThat(first).isNotEmpty();

        // Run the same query once more to measure the steady-state per-query document reads
        final long baseline = docFetches(index);
        index.findNeighborsFromVector(query, K, EF_SEARCH);
        final long secondQueryFetches = docFetches(index) - baseline;

        // ...and now repeat it many times: with an index-scoped cache the vectors visited by the beam
        // search are already resident, so the extra queries must not go back to the documents.
        final int repeats = 20;
        final long beforeRepeats = docFetches(index);
        for (int i = 0; i < repeats; i++) {
          final List<Pair<RID, Float>> res = index.findNeighborsFromVector(query, K, EF_SEARCH);
          assertThat(res).hasSameSizeAs(first);
        }
        final long repeatFetches = docFetches(index) - beforeRepeats;

        // Before the fix repeatFetches was ~repeats * secondQueryFetches (no cross-query cache at all).
        assertThat(repeatFetches).isLessThan(Math.max(1, secondQueryFetches));
      }
    }
  }

  @Test
  void cacheCanBeDisabledAndDeletedVectorsAreEvicted() {
    final Random rnd = new Random(11);
    final int count = 500;
    final float[][] vectors = new float[count][DIMENSIONS];
    for (int i = 0; i < count; i++)
      for (int d = 0; d < DIMENSIONS; d++)
        vectors[i][d] = rnd.nextFloat();

    GlobalConfiguration.VECTOR_INDEX_SEARCH_CACHE_SIZE.setValue(-1);
    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      try (final Database db = factory.create()) {
        db.transaction(() -> {
          final var type = db.getSchema().createDocumentType("Doc");
          type.createProperty("id", Type.INTEGER);
          type.createProperty("vector", Type.ARRAY_OF_FLOATS);
          db.command("sql", "CREATE INDEX ON Doc (vector) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
              + ", \"similarity\": \"COSINE\" }");
        });

        db.transaction(() -> {
          for (int i = 0; i < count; i++)
            db.newDocument("Doc").set("id", i).set("vector", vectors[i]).save();
        });
      }

      try (final Database db = factory.open()) {
        final LSMVectorIndex index = (LSMVectorIndex) db.getSchema().getType("Doc")
            .getPolymorphicIndexByProperties("vector").getIndexesOnBuckets()[0];

        // -1 disables the cache entirely: no cache is ever allocated
        index.findNeighborsFromVector(vectors[0], K, EF_SEARCH);
        assertThat(index.getSearchVectorCache()).isNull();
        assertThat(index.getStats().get("searchVectorCacheCapacity")).isZero();

        // Back to automatic sizing: the cache appears and answers subsequent queries
        GlobalConfiguration.VECTOR_INDEX_SEARCH_CACHE_SIZE.setValue(0);
        index.findNeighborsFromVector(vectors[0], K, EF_SEARCH);
        final VectorCache cache = index.getSearchVectorCache();
        assertThat(cache).isNotNull();
        assertThat(cache.size()).isPositive();

        // Deleting the records must not leave their vectors pinned in the cache
        db.transaction(() -> db.command("sql", "DELETE FROM Doc"));
        assertThat(cache.size()).isZero();

        // ...and searching an emptied index still works
        assertThat(index.findNeighborsFromVector(vectors[400], K, EF_SEARCH)).isEmpty();
      }
    } finally {
      GlobalConfiguration.VECTOR_INDEX_SEARCH_CACHE_SIZE.setValue(
          GlobalConfiguration.VECTOR_INDEX_SEARCH_CACHE_SIZE.getDefValue());
    }
  }

  private static long docFetches(final LSMVectorIndex index) {
    final Map<String, Long> stats = index.getStats();
    return stats.getOrDefault("vectorFetchFromDocuments", 0L);
  }
}
