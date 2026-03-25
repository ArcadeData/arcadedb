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
 * Tests that async graph rebuild is re-triggered when embeddings are added during an ongoing build.
 * <p>
 * Issue: https://github.com/ArcadeData/arcadedb/issues/3683
 * <p>
 * Before the fix, adding vectors during an async rebuild would cause the mutation counter to be
 * unconditionally reset to 0 and the graph state set to IMMUTABLE, preventing the next search
 * from triggering a new rebuild for the vectors added during the build.
 */
class Issue3683AsyncRebuildRetriggerTest extends TestHelper {

  private static final int EMBEDDING_DIM = 32;
  private static final int LARGE_INDEX_VECTORS = 1100;

  @Test
  void asyncRebuildShouldBeRetriggeredForMutationsDuringBuild() throws InterruptedException {
    final int threshold = 5;
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

    // Insert enough vectors for a "large" graph (>= 1000 to use async path)
    database.transaction(() -> {
      for (int i = 0; i < LARGE_INDEX_VECTORS; i++)
        database.command("sql", "INSERT INTO Embedding SET vector = ?", (Object) generateRandomVector(random));
    });

    // First search to trigger initial synchronous graph build
    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("Embedding[vector]");
    final LSMVectorIndex lsmIndex = (LSMVectorIndex) typeIndex.getIndexesOnBuckets()[0];
    final float[] queryVector = generateRandomVector(random);
    List<Pair<RID, Float>> results = lsmIndex.findNeighborsFromVector(queryVector, 10);
    assertThat(results).isNotEmpty();

    // After initial build, mutation counter should be 0
    Map<String, Long> stats = lsmIndex.getStats();
    assertThat(stats.get("mutationsSinceRebuild")).isEqualTo(0L);

    // Add enough vectors to exceed the threshold and trigger async rebuild
    database.transaction(() -> {
      for (int i = 0; i < threshold; i++)
        database.command("sql", "INSERT INTO Embedding SET vector = ?", (Object) generateRandomVector(random));
    });

    // Trigger async rebuild via search
    results = lsmIndex.findNeighborsFromVector(queryVector, 10);
    assertThat(results).isNotEmpty();

    // Give async rebuild a moment to start
    Thread.sleep(200);

    // Add more vectors DURING the async rebuild (above threshold count)
    final int vectorsDuringBuild = threshold + 5;
    database.transaction(() -> {
      for (int i = 0; i < vectorsDuringBuild; i++)
        database.command("sql", "INSERT INTO Embedding SET vector = ?", (Object) generateRandomVector(random));
    });

    // Wait for async rebuild to complete
    Thread.sleep(10000);

    // After async rebuild completes, the mutations added DURING the build should be preserved
    stats = lsmIndex.getStats();
    final long mutationsAfterRebuild = stats.get("mutationsSinceRebuild");
    assertThat(mutationsAfterRebuild)
        .as("Mutations added during async rebuild should be preserved in the counter")
        .isGreaterThan(0L);

    // Graph state: MUTABLE (2) if delta vectors exist, or IMMUTABLE (1) if live builder
    // handled them directly via addGraphNode()
    assertThat(stats.get("graphState"))
        .as("Graph state should be MUTABLE (2) or IMMUTABLE (1) after rebuild with concurrent mutations")
        .isIn(1L, 2L);

    // Trigger another search - should start a new async rebuild since mutations >= threshold
    if (mutationsAfterRebuild >= threshold) {
      results = lsmIndex.findNeighborsFromVector(queryVector, 10);
      assertThat(results).isNotEmpty();

      // Wait for second async rebuild to complete
      Thread.sleep(10000);

      // After second rebuild, counter should be low (may not be exactly 0 with incremental inserts)
      stats = lsmIndex.getStats();
      assertThat(stats.get("mutationsSinceRebuild"))
          .as("After second rebuild with no concurrent inserts, counter should be low")
          .isLessThanOrEqualTo(20L);
      assertThat(stats.get("graphState"))
          .as("Graph state should be IMMUTABLE (1) after clean rebuild")
          .isEqualTo(1L); // GraphState.IMMUTABLE ordinal
    }
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
