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
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.Pair;
import com.arcadedb.database.RID;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests the in-memory vectorIndex recovery path (tertiary defense) for issue #3722.
 * Verifies that the graph rebuild uses the in-memory vectorIndex as a fallback when
 * page parsing returns fewer vectors than what's known in memory. This catches scenarios
 * where page corruption, stale page counts, or format issues cause the page parser to
 * miss vectors that were successfully added via put().
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue3722VectorInMemoryRecoveryTest extends TestHelper {

  private static final int EMBEDDING_DIM = 32;

  @Test
  void graphRebuildShouldIncludeAllVectorsFromInMemoryIndex() {
    // Disable automatic rebuilds
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, 100_000);
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, -1);

    // Create schema
    database.transaction(() -> {
      database.getSchema().createVertexType("Embedding");
      database.getSchema().getType("Embedding").createProperty("vector", Type.ARRAY_OF_FLOATS);

      database.command("sql", """
          CREATE INDEX ON Embedding (vector) LSM_VECTOR
          METADATA {
              "dimensions": %d,
              "similarity": "EUCLIDEAN"
          }""".formatted(EMBEDDING_DIM));
    });

    final Random random = new Random(42);

    // Insert a batch of vectors
    final int count = 500;
    database.transaction(() -> {
      for (int i = 0; i < count; i++)
        database.command("sql", "INSERT INTO Embedding SET vector = ?", (Object) generateRandomVector(random));
    });

    // Force initial graph build
    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("Embedding[vector]");
    final LSMVectorIndex lsmIndex = (LSMVectorIndex) typeIndex.getIndexesOnBuckets()[0];
    lsmIndex.buildVectorGraphNow();

    // Verify graph has the expected number of nodes
    final Map<String, Long> stats = lsmIndex.getStats();
    assertThat(stats.get("graphNodeCount"))
        .as("Graph should have approximately %d nodes", count)
        .isGreaterThanOrEqualTo((long) count - 5);

    // Search should return all requested neighbors
    final float[] queryVector = generateRandomVector(random);
    final int k = 200;
    final List<Result> results = executeVectorSearch(queryVector, k);
    assertThat(results.size())
        .as("Search for %d neighbors from %d vectors should return %d results", k, count, k)
        .isEqualTo(k);

    // Force another rebuild - should still work (no regression)
    lsmIndex.buildVectorGraphNow();

    final List<Result> results2 = executeVectorSearch(queryVector, k);
    assertThat(results2.size())
        .as("After second rebuild, search should still return %d results", k)
        .isEqualTo(k);
  }

  @Test
  void graphRebuildAfterMultipleBatchesShouldIncludeAllVectors() {
    // Disable automatic rebuilds
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, 100_000);
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, -1);

    // Create schema
    database.transaction(() -> {
      database.getSchema().createVertexType("Embedding");
      database.getSchema().getType("Embedding").createProperty("vector", Type.ARRAY_OF_FLOATS);

      database.command("sql", """
          CREATE INDEX ON Embedding (vector) LSM_VECTOR
          METADATA {
              "dimensions": %d,
              "similarity": "EUCLIDEAN"
          }""".formatted(EMBEDDING_DIM));
    });

    final Random random = new Random(42);
    int totalInserted = 0;

    // Insert multiple batches with rebuilds between each
    for (int batch = 0; batch < 5; batch++) {
      final int batchSize = 100 + batch * 50; // Increasing batch sizes
      database.transaction(() -> {
        for (int i = 0; i < batchSize; i++)
          database.command("sql", "INSERT INTO Embedding SET vector = ?", (Object) generateRandomVector(random));
      });
      totalInserted += batchSize;

      // Rebuild after each batch
      final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("Embedding[vector]");
      final LSMVectorIndex lsmIndex = (LSMVectorIndex) typeIndex.getIndexesOnBuckets()[0];
      lsmIndex.buildVectorGraphNow();

      // Verify graph node count
      final Map<String, Long> stats = lsmIndex.getStats();
      assertThat(stats.get("graphNodeCount"))
          .as("After batch %d (%d total vectors), graph should have approximately %d nodes",
              batch, totalInserted, totalInserted)
          .isGreaterThanOrEqualTo((long) totalInserted - 10);

      // Verify search
      final float[] queryVector = generateRandomVector(new Random(99));
      final int k = Math.min(totalInserted - 10, 300);
      final List<Result> results = executeVectorSearch(queryVector, k);
      assertThat(results.size())
          .as("After batch %d (%d total), search for %d should return %d", batch, totalInserted, k, k)
          .isEqualTo(k);
    }
  }

  @Test
  void javaApiSearchShouldReturnCorrectCountAfterRebuild() {
    // Disable automatic rebuilds
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, 100_000);
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, -1);

    // Create schema
    database.transaction(() -> {
      database.getSchema().createVertexType("Embedding");
      database.getSchema().getType("Embedding").createProperty("vector", Type.ARRAY_OF_FLOATS);

      database.command("sql", """
          CREATE INDEX ON Embedding (vector) LSM_VECTOR
          METADATA {
              "dimensions": %d,
              "similarity": "EUCLIDEAN"
          }""".formatted(EMBEDDING_DIM));
    });

    final Random random = new Random(42);

    // Insert initial batch
    final int initialCount = 100;
    database.transaction(() -> {
      for (int i = 0; i < initialCount; i++)
        database.command("sql", "INSERT INTO Embedding SET vector = ?", (Object) generateRandomVector(random));
    });

    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("Embedding[vector]");
    final LSMVectorIndex lsmIndex = (LSMVectorIndex) typeIndex.getIndexesOnBuckets()[0];
    lsmIndex.buildVectorGraphNow();

    // Insert large additional batch (similar to user's scenario of adding many vectors)
    final int additionalCount = 1000;
    database.transaction(() -> {
      for (int i = 0; i < additionalCount; i++)
        database.command("sql", "INSERT INTO Embedding SET vector = ?", (Object) generateRandomVector(random));
    });

    // Rebuild
    lsmIndex.buildVectorGraphNow();

    final int totalCount = initialCount + additionalCount;
    final float[] queryVector = generateRandomVector(new Random(99));

    // Test via Java API with high k (user's scenario: 500 from 4k+ vectors)
    final int highK = 500;
    final List<Pair<RID, Float>> javaResults = lsmIndex.findNeighborsFromVector(queryVector, highK);
    assertThat(javaResults.size())
        .as("Java API: search for %d from %d total should return %d", highK, totalCount, highK)
        .isEqualTo(highK);

    // Also test via SQL
    final List<Result> sqlResults = executeVectorSearch(queryVector, highK);
    assertThat(sqlResults.size())
        .as("SQL: search for %d from %d total should return %d", highK, totalCount, highK)
        .isEqualTo(highK);
  }

  private List<Result> executeVectorSearch(final float[] queryVector, final int k) {
    final StringBuilder vectorStr = new StringBuilder("[");
    for (int i = 0; i < queryVector.length; i++) {
      if (i > 0)
        vectorStr.append(",");
      vectorStr.append(queryVector[i]);
    }
    vectorStr.append("]");

    final String sql = String.format(
        "SELECT distance, record FROM (SELECT expand(vectorNeighbors('Embedding[vector]', %s, %d)))",
        vectorStr, k);

    final List<Result> results = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", sql)) {
      while (rs.hasNext())
        results.add(rs.next());
    }
    return results;
  }

  private static float[] generateRandomVector(final Random random) {
    final float[] vector = new float[EMBEDDING_DIM];
    for (int i = 0; i < EMBEDDING_DIM; i++)
      vector[i] = random.nextFloat() * 2 - 1;
    return vector;
  }
}
