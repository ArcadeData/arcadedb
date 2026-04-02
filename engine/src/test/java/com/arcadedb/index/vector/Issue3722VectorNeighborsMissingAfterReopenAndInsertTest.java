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
 * Reproduces issue #3722: after closing/reopening a database with vector indexes,
 * inserting many new vectors, and triggering a graph rebuild, the search returns
 * significantly fewer results than expected. This simulates the scenario where
 * a user has an existing database (potentially from a backup), adds new vectors,
 * and the graph rebuild fails to include all vectors.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue3722VectorNeighborsMissingAfterReopenAndInsertTest extends TestHelper {

  private static final int EMBEDDING_DIM = 32;

  @Test
  void vectorSearchShouldReturnCorrectCountAfterReopenAndIncrementalInsert() {
    // Disable automatic rebuilds during inserts
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, 100_000);
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, -1);

    // Create schema and insert initial batch
    database.transaction(() -> {
      database.getSchema().createVertexType("ChunkEmbedding");
      database.getSchema().getType("ChunkEmbedding").createProperty("vector", Type.ARRAY_OF_FLOATS);

      database.command("sql", """
          CREATE INDEX ON ChunkEmbedding (vector) LSM_VECTOR
          METADATA {
              "dimensions": %d,
              "similarity": "EUCLIDEAN"
          }""".formatted(EMBEDDING_DIM));
    });

    final Random random = new Random(42);
    final int initialCount = 50;

    database.transaction(() -> {
      for (int i = 0; i < initialCount; i++)
        database.command("sql", "INSERT INTO ChunkEmbedding SET vector = ?", (Object) generateRandomVector(random));
    });

    // Force initial graph build
    TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("ChunkEmbedding[vector]");
    LSMVectorIndex lsmIndex = (LSMVectorIndex) typeIndex.getIndexesOnBuckets()[0];
    lsmIndex.buildVectorGraphNow();

    // Verify initial search works
    final float[] queryVector = generateRandomVector(random);
    assertThat(executeVectorSearch(queryVector, 30).size()).isEqualTo(30);

    // Close and reopen database (simulates backup restore or server restart)
    reopenDatabase();

    // Re-configure after reopen
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, 100_000);
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, -1);

    // Insert many more vectors (much more than initial count, similar to user's 4k+ scenario)
    final int additionalCount = 500;
    database.transaction(() -> {
      for (int i = 0; i < additionalCount; i++)
        database.command("sql", "INSERT INTO ChunkEmbedding SET vector = ?", (Object) generateRandomVector(random));
    });

    final int totalCount = initialCount + additionalCount;

    // Force manual rebuild
    typeIndex = (TypeIndex) database.getSchema().getIndexByName("ChunkEmbedding[vector]");
    lsmIndex = (LSMVectorIndex) typeIndex.getIndexesOnBuckets()[0];
    lsmIndex.buildVectorGraphNow();

    // Check graph node count
    final Map<String, Long> stats = lsmIndex.getStats();
    assertThat(stats.get("graphNodeCount"))
        .as("Graph should have approximately %d nodes after rebuild", totalCount)
        .isGreaterThanOrEqualTo((long) totalCount - 10);

    // Search should return the requested number of results
    final int k = 100;
    final List<Result> results = executeVectorSearch(queryVector, k);
    assertThat(results.size())
        .as("Search for %d neighbors from %d total vectors should return %d results", k, totalCount, k)
        .isEqualTo(k);
  }

  @Test
  void vectorSearchShouldReturnCorrectCountAfterMultipleReopenCycles() {
    // Disable automatic rebuilds
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, 100_000);
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, -1);

    // Create schema
    database.transaction(() -> {
      database.getSchema().createVertexType("ChunkEmbedding");
      database.getSchema().getType("ChunkEmbedding").createProperty("vector", Type.ARRAY_OF_FLOATS);

      database.command("sql", """
          CREATE INDEX ON ChunkEmbedding (vector) LSM_VECTOR
          METADATA {
              "dimensions": %d,
              "similarity": "EUCLIDEAN"
          }""".formatted(EMBEDDING_DIM));
    });

    final Random random = new Random(42);
    final float[] queryVector = generateRandomVector(new Random(99));
    int totalInserted = 0;

    // Cycle 1: Insert initial batch
    final int batch1 = 50;
    database.transaction(() -> {
      for (int i = 0; i < batch1; i++)
        database.command("sql", "INSERT INTO ChunkEmbedding SET vector = ?", (Object) generateRandomVector(random));
    });
    totalInserted += batch1;

    // Build graph
    TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("ChunkEmbedding[vector]");
    LSMVectorIndex lsmIndex = (LSMVectorIndex) typeIndex.getIndexesOnBuckets()[0];
    lsmIndex.buildVectorGraphNow();

    // Cycle 2: Close, reopen, insert more
    reopenDatabase();
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, 100_000);
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, -1);

    final int batch2 = 200;
    database.transaction(() -> {
      for (int i = 0; i < batch2; i++)
        database.command("sql", "INSERT INTO ChunkEmbedding SET vector = ?", (Object) generateRandomVector(random));
    });
    totalInserted += batch2;

    typeIndex = (TypeIndex) database.getSchema().getIndexByName("ChunkEmbedding[vector]");
    lsmIndex = (LSMVectorIndex) typeIndex.getIndexesOnBuckets()[0];
    lsmIndex.buildVectorGraphNow();

    // Verify
    int k = Math.min(totalInserted - 10, 200);
    List<Result> results = executeVectorSearch(queryVector, k);
    assertThat(results.size())
        .as("After cycle 2 (%d total vectors), search for %d should return %d", totalInserted, k, k)
        .isEqualTo(k);

    // Cycle 3: Close, reopen, insert even more
    reopenDatabase();
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, 100_000);
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, -1);

    final int batch3 = 300;
    database.transaction(() -> {
      for (int i = 0; i < batch3; i++)
        database.command("sql", "INSERT INTO ChunkEmbedding SET vector = ?", (Object) generateRandomVector(random));
    });
    totalInserted += batch3;

    typeIndex = (TypeIndex) database.getSchema().getIndexByName("ChunkEmbedding[vector]");
    lsmIndex = (LSMVectorIndex) typeIndex.getIndexesOnBuckets()[0];
    lsmIndex.buildVectorGraphNow();

    // Verify all vectors are searchable
    k = Math.min(totalInserted - 10, 400);
    results = executeVectorSearch(queryVector, k);
    assertThat(results.size())
        .as("After cycle 3 (%d total vectors), search for %d should return %d", totalInserted, k, k)
        .isEqualTo(k);
  }

  @Test
  void vectorSearchShouldReturnCorrectCountWithInactivityRebuildAfterReopen() throws InterruptedException {
    // Disable automatic rebuilds during initial setup
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, 100_000);
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, -1);

    // Create schema and insert initial batch
    database.transaction(() -> {
      database.getSchema().createVertexType("ChunkEmbedding");
      database.getSchema().getType("ChunkEmbedding").createProperty("vector", Type.ARRAY_OF_FLOATS);

      database.command("sql", """
          CREATE INDEX ON ChunkEmbedding (vector) LSM_VECTOR
          METADATA {
              "dimensions": %d,
              "similarity": "EUCLIDEAN"
          }""".formatted(EMBEDDING_DIM));
    });

    final Random random = new Random(42);
    final int initialCount = 50;

    database.transaction(() -> {
      for (int i = 0; i < initialCount; i++)
        database.command("sql", "INSERT INTO ChunkEmbedding SET vector = ?", (Object) generateRandomVector(random));
    });

    // Build initial graph
    TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("ChunkEmbedding[vector]");
    LSMVectorIndex lsmIndex = (LSMVectorIndex) typeIndex.getIndexesOnBuckets()[0];
    lsmIndex.buildVectorGraphNow();

    // Close and reopen
    reopenDatabase();

    // Enable inactivity rebuild with short timeout
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, 100_000);
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, 2_000);

    // Insert many more vectors
    final int additionalCount = 500;
    database.transaction(() -> {
      for (int i = 0; i < additionalCount; i++)
        database.command("sql", "INSERT INTO ChunkEmbedding SET vector = ?", (Object) generateRandomVector(random));
    });

    // Wait for inactivity rebuild
    Thread.sleep(6_000);

    typeIndex = (TypeIndex) database.getSchema().getIndexByName("ChunkEmbedding[vector]");
    lsmIndex = (LSMVectorIndex) typeIndex.getIndexesOnBuckets()[0];

    // Verify delta buffer is empty (rebuild happened)
    assertThat(lsmIndex.getStats().get("deltaVectorsCount"))
        .as("Delta buffer should be empty after inactivity rebuild")
        .isEqualTo(0L);

    final int totalCount = initialCount + additionalCount;
    final float[] queryVector = generateRandomVector(new Random(99));
    final int k = 100;
    final List<Result> results = executeVectorSearch(queryVector, k);
    assertThat(results.size())
        .as("After reopen + incremental insert + inactivity rebuild, should return %d results from %d total", k, totalCount)
        .isEqualTo(k);
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
        "SELECT distance, record FROM (SELECT expand(vectorNeighbors('ChunkEmbedding[vector]', %s, %d)))",
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
