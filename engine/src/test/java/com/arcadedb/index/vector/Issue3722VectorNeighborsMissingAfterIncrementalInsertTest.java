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
 * Reproduces issue #3722: vectorNeighbors returns fewer results than requested after
 * adding new vectors incrementally. The live builder adds vectors to the HNSW graph,
 * but after the inactivity timeout triggers a full graph rebuild, search returns
 * significantly fewer results than expected.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue3722VectorNeighborsMissingAfterIncrementalInsertTest extends TestHelper {

  private static final int EMBEDDING_DIM = 32;

  @Test
  void vectorNeighborsShouldReturnCorrectCountAfterIncrementalInsertAndRebuild() throws Exception {
    // Set high threshold so automatic rebuild doesn't trigger during inserts, low timeout for quick inactivity rebuild
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, 100_000);
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, 2_000);

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

    // Step 1: Insert initial batch of vectors
    final int initialCount = 100;
    database.transaction(() -> {
      for (int i = 0; i < initialCount; i++)
        database.command("sql", "INSERT INTO Embedding SET vector = ?", (Object) generateRandomVector(random));
    });

    // Force initial graph build
    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("Embedding[vector]");
    final LSMVectorIndex lsmIndex = (LSMVectorIndex) typeIndex.getIndexesOnBuckets()[0];
    lsmIndex.buildVectorGraphNow();

    // Step 2: Verify initial search works
    final int k = 80;
    final float[] queryVector = generateRandomVector(random);
    final List<Result> initialResults = executeVectorSearch(queryVector, k);
    assertThat(initialResults.size())
        .as("Initial search should return %d results from %d vectors", k, initialCount)
        .isEqualTo(k);

    // Step 3: Insert more vectors incrementally (triggers live builder)
    final int additionalCount = 200;
    database.transaction(() -> {
      for (int i = 0; i < additionalCount; i++)
        database.command("sql", "INSERT INTO Embedding SET vector = ?", (Object) generateRandomVector(random));
    });

    final int totalCount = initialCount + additionalCount;

    // Step 4: Wait for inactivity timeout to trigger graph rebuild
    Thread.sleep(6_000);

    // Verify the graph was rebuilt (delta buffer should be empty)
    assertThat(lsmIndex.getStats().get("deltaVectorsCount"))
        .as("Delta buffer should be empty after inactivity rebuild")
        .isEqualTo(0L);

    // Step 5: Search again - should return k results from totalCount vectors
    final List<Result> afterRebuildResults = executeVectorSearch(queryVector, k);
    assertThat(afterRebuildResults.size())
        .as("After incremental insert + rebuild, search should return %d results from %d total vectors", k, totalCount)
        .isEqualTo(k);

    // Step 6: Also test requesting more than initial count but less than total
    final int largerK = 150;
    final List<Result> largerResults = executeVectorSearch(queryVector, largerK);
    assertThat(largerResults.size())
        .as("Search for %d neighbors from %d total vectors should return %d results", largerK, totalCount, largerK)
        .isEqualTo(largerK);
  }

  @Test
  void vectorNeighborsShouldReturnCorrectCountAfterManualRebuild() {
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

    // Force initial graph build
    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("Embedding[vector]");
    final LSMVectorIndex lsmIndex = (LSMVectorIndex) typeIndex.getIndexesOnBuckets()[0];
    lsmIndex.buildVectorGraphNow();

    final float[] queryVector = generateRandomVector(random);

    // Verify initial search
    final int k = 80;
    assertThat(executeVectorSearch(queryVector, k).size()).isEqualTo(k);

    // Insert more vectors incrementally
    final int additionalCount = 300;
    database.transaction(() -> {
      for (int i = 0; i < additionalCount; i++)
        database.command("sql", "INSERT INTO Embedding SET vector = ?", (Object) generateRandomVector(random));
    });

    // Force manual rebuild
    lsmIndex.buildVectorGraphNow();

    // Search after rebuild - should return correct count
    final int totalCount = initialCount + additionalCount;
    final List<Result> results = executeVectorSearch(queryVector, k);
    assertThat(results.size())
        .as("After manual rebuild, search should return %d results from %d total vectors", k, totalCount)
        .isEqualTo(k);

    // Test with larger k
    final int largerK = 250;
    final List<Result> largerResults = executeVectorSearch(queryVector, largerK);
    assertThat(largerResults.size())
        .as("Search for %d neighbors from %d total vectors should return %d", largerK, totalCount, largerK)
        .isEqualTo(largerK);
  }

  @Test
  void vectorNeighborsShouldReturnCorrectCountAfterMultipleIncrementalBatches() {
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

    // Insert initial batch
    final int initialCount = 50;
    database.transaction(() -> {
      for (int i = 0; i < initialCount; i++)
        database.command("sql", "INSERT INTO Embedding SET vector = ?", (Object) generateRandomVector(random));
    });
    totalInserted += initialCount;

    // Force initial graph build
    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("Embedding[vector]");
    final LSMVectorIndex lsmIndex = (LSMVectorIndex) typeIndex.getIndexesOnBuckets()[0];
    lsmIndex.buildVectorGraphNow();

    final float[] queryVector = generateRandomVector(random);

    // Add multiple batches, rebuilding after each
    for (int batch = 0; batch < 5; batch++) {
      final int batchSize = 100;
      database.transaction(() -> {
        for (int i = 0; i < batchSize; i++)
          database.command("sql", "INSERT INTO Embedding SET vector = ?", (Object) generateRandomVector(random));
      });
      totalInserted += batchSize;

      // Force rebuild after each batch
      lsmIndex.buildVectorGraphNow();

      // Verify search returns correct count
      final int k = Math.min(totalInserted, 200);
      final List<Result> results = executeVectorSearch(queryVector, k);
      assertThat(results.size())
          .as("After batch %d (%d total vectors), search for %d neighbors should return %d results",
              batch, totalInserted, k, k)
          .isEqualTo(k);
    }
  }

  @Test
  void searchViaJavaAPIShouldReturnCorrectCountAfterRebuild() {
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
    final int initialCount = 200;
    database.transaction(() -> {
      for (int i = 0; i < initialCount; i++)
        database.command("sql", "INSERT INTO Embedding SET vector = ?", (Object) generateRandomVector(random));
    });

    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("Embedding[vector]");
    final LSMVectorIndex lsmIndex = (LSMVectorIndex) typeIndex.getIndexesOnBuckets()[0];
    lsmIndex.buildVectorGraphNow();

    final float[] queryVector = generateRandomVector(random);

    // Test via Java API directly
    final int k = 150;
    List<Pair<RID, Float>> results = lsmIndex.findNeighborsFromVector(queryVector, k);
    assertThat(results.size())
        .as("Java API: Initial search should return %d results", k)
        .isEqualTo(k);

    // Insert more vectors (4x the initial count, similar to user's scenario)
    final int additionalCount = 800;
    database.transaction(() -> {
      for (int i = 0; i < additionalCount; i++)
        database.command("sql", "INSERT INTO Embedding SET vector = ?", (Object) generateRandomVector(random));
    });

    // Force rebuild
    lsmIndex.buildVectorGraphNow();

    final int totalCount = initialCount + additionalCount;
    final Map<String, Long> stats = lsmIndex.getStats();
    assertThat(stats.get("graphNodeCount"))
        .as("Graph should have %d nodes after rebuild", totalCount)
        .isGreaterThanOrEqualTo((long) totalCount - 10); // Allow small margin for deleted/invalid

    // Search via Java API - verify we get k results
    results = lsmIndex.findNeighborsFromVector(queryVector, k);
    assertThat(results.size())
        .as("Java API: After rebuild (%d total vectors), search for %d should return %d", totalCount, k, k)
        .isEqualTo(k);

    // Search with high k (close to user's 500 out of ~2000)
    final int highK = 500;
    results = lsmIndex.findNeighborsFromVector(queryVector, highK);
    assertThat(results.size())
        .as("Java API: Search for %d from %d total vectors", highK, totalCount)
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
