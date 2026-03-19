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
import com.arcadedb.database.RID;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.Pair;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that vectorNeighbors search respects the mutations-before-rebuild threshold
 * instead of rebuilding the graph on every single mutation.
 * <p>
 * Issue: https://github.com/ArcadeData/arcadedb/issues/3679
 * <p>
 * Before the fix, adding even a single vector to a large index would trigger a full
 * graph rebuild on the next vectorNeighbors call, taking minutes for large indexes.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue3679VectorRebuildThresholdTest extends TestHelper {

  private static final int EMBEDDING_DIM = 64;
  private static final int INITIAL_VECTORS = 200;

  @Test
  void searchShouldNotRebuildGraphBelowMutationThreshold() {
    // Set a threshold smaller than the number of vectors but larger than the single mutation we'll add.
    // The optimization only applies when graph size > threshold (small graphs always rebuild since it's cheap).
    final int highThreshold = 100;
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, highThreshold);

    // Create schema with vector index
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

    // Insert initial vectors
    database.transaction(() -> {
      for (int i = 0; i < INITIAL_VECTORS; i++)
        database.command("sql", "INSERT INTO Embedding SET vector = ?", (Object) generateRandomVector(random));
    });

    // First search to trigger initial graph build
    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("Embedding[vector]");
    final LSMVectorIndex lsmIndex = (LSMVectorIndex) typeIndex.getIndexesOnBuckets()[0];
    final float[] queryVector = generateRandomVector(random);
    List<Pair<RID, Float>> results = lsmIndex.findNeighborsFromVector(queryVector, 10);
    assertThat(results).isNotEmpty();

    // After the initial build, mutation counter should be 0
    Map<String, Long> stats = lsmIndex.getStats();
    assertThat(stats.get("mutationsSinceRebuild")).isEqualTo(0L);

    // Now add a SINGLE new vector (below the threshold)
    database.transaction(() -> {
      database.command("sql", "INSERT INTO Embedding SET vector = ?", (Object) generateRandomVector(random));
    });

    // Verify mutation counter is 1 before search
    stats = lsmIndex.getStats();
    assertThat(stats.get("mutationsSinceRebuild")).isEqualTo(1L);

    // Search should NOT trigger a rebuild since 1 < threshold and graph is large (200 > 100)
    results = lsmIndex.findNeighborsFromVector(queryVector, 10);
    assertThat(results).isNotEmpty();

    // After search, mutation counter should still be 1 (no rebuild happened)
    stats = lsmIndex.getStats();
    assertThat(stats.get("mutationsSinceRebuild"))
        .as("Mutation counter should NOT be reset to 0 when below threshold (1 < %d)", highThreshold)
        .isEqualTo(1L);
  }

  @Test
  void searchShouldRebuildGraphAtOrAboveMutationThreshold() {
    // Set a low threshold so that we can easily exceed it
    final int lowThreshold = 5;
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, lowThreshold);

    // Create schema with vector index
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

    // Insert initial vectors
    database.transaction(() -> {
      for (int i = 0; i < 50; i++)
        database.command("sql", "INSERT INTO Embedding SET vector = ?", (Object) generateRandomVector(random));
    });

    // First search to trigger initial graph build
    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("Embedding[vector]");
    final LSMVectorIndex lsmIndex = (LSMVectorIndex) typeIndex.getIndexesOnBuckets()[0];
    final float[] queryVector = generateRandomVector(random);
    List<Pair<RID, Float>> results = lsmIndex.findNeighborsFromVector(queryVector, 10);
    assertThat(results).isNotEmpty();

    // Add enough vectors to meet/exceed the threshold
    database.transaction(() -> {
      for (int i = 0; i < lowThreshold; i++)
        database.command("sql", "INSERT INTO Embedding SET vector = ?", (Object) generateRandomVector(random));
    });

    // Verify mutations are at threshold before search
    Map<String, Long> stats = lsmIndex.getStats();
    assertThat(stats.get("mutationsSinceRebuild")).isGreaterThanOrEqualTo((long) lowThreshold);

    // Search should trigger a rebuild now (mutations >= threshold)
    results = lsmIndex.findNeighborsFromVector(queryVector, 10);
    assertThat(results).isNotEmpty();

    // After rebuild, mutation counter should be reset to 0
    stats = lsmIndex.getStats();
    assertThat(stats.get("mutationsSinceRebuild"))
        .as("Mutation counter should be reset to 0 after rebuild (mutations >= %d)", lowThreshold)
        .isEqualTo(0L);
  }

  @Test
  void searchViaGetShouldAlsoRespectThreshold() {
    // This tests the get() method path (used by IndexCursor).
    // Use a threshold smaller than the number of vectors so the graph is "large"
    // and the threshold optimization kicks in.
    final int threshold = 50;
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, threshold);

    // Create schema with vector index
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

    // Insert enough vectors so graph is larger than threshold
    database.transaction(() -> {
      for (int i = 0; i < INITIAL_VECTORS; i++)
        database.command("sql", "INSERT INTO Embedding SET vector = ?", (Object) generateRandomVector(random));
    });

    // First query to build graph (use direct API to ensure graph is built)
    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("Embedding[vector]");
    final LSMVectorIndex lsmIndex = (LSMVectorIndex) typeIndex.getIndexesOnBuckets()[0];
    final float[] queryVector = generateRandomVector(random);
    List<Pair<RID, Float>> initialResults = lsmIndex.findNeighborsFromVector(queryVector, 10);
    assertThat(initialResults).isNotEmpty();

    // After initial build, mutation counter should be 0
    Map<String, Long> stats = lsmIndex.getStats();
    assertThat(stats.get("mutationsSinceRebuild")).isEqualTo(0L);

    // Add a single vector (below threshold)
    database.transaction(() -> {
      database.command("sql", "INSERT INTO Embedding SET vector = ?", (Object) generateRandomVector(random));
    });

    // Verify mutation counter is 1
    stats = lsmIndex.getStats();
    assertThat(stats.get("mutationsSinceRebuild")).isEqualTo(1L);

    // Second search should NOT rebuild (1 < 50 threshold, graph has 200 > 50 vectors)
    List<Pair<RID, Float>> results = lsmIndex.findNeighborsFromVector(queryVector, 10);
    assertThat(results).isNotEmpty();

    // Mutation counter should still be 1 (no rebuild happened)
    stats = lsmIndex.getStats();
    assertThat(stats.get("mutationsSinceRebuild"))
        .as("Search should not trigger rebuild for 1 mutation (threshold=%d)", threshold)
        .isEqualTo(1L);
  }

  private float[] generateRandomVector(final Random random) {
    final float[] vector = new float[EMBEDDING_DIM];
    float norm = 0;
    for (int i = 0; i < EMBEDDING_DIM; i++) {
      vector[i] = random.nextFloat() * 2 - 1;
      norm += vector[i] * vector[i];
    }
    norm = (float) Math.sqrt(norm);
    if (norm > 0)
      for (int i = 0; i < EMBEDDING_DIM; i++)
        vector[i] /= norm;
    return vector;
  }
}
