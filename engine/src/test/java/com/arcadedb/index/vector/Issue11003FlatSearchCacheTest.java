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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.RID;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.Pair;
import com.arcadedb.utility.VectorApiSupport;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Issue #11003: a search scores the graph nodes it visits on the vectors of the index-scoped {@link FlatVectorCache}, where
 * they lie, instead of materializing each one through a chain of objects. The answer must not depend on whether a vector
 * was resident: a search over a cold cache (every node read from its page and scored by the same kernel) and the same
 * search over the warm cache (every node scored in place) return the same rows with the same distances, and those
 * distances are the ones the similarity function gives the stored vectors - up to the last bit, which the Vector API's
 * lane reduction can move between a kernel the JIT has compiled and one it has not.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11003FlatSearchCacheTest {
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
