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
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that buffered vectors below the rebuild threshold are flushed after an inactivity timeout.
 * When vectors are inserted but the mutation count doesn't reach the rebuild threshold, the delta
 * buffer should be flushed and the graph rebuilt after a configurable period of inactivity.
 * <p>
 * Issue: https://github.com/ArcadeData/arcadedb/issues/3737
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue3737VectorInactivityRebuildTest extends TestHelper {

  private static final int EMBEDDING_DIM = 32;

  @Test
  void deltaBufferShouldFlushAfterInactivityTimeout() throws Exception {
    // High threshold so we never reach it, low timeout so the timer fires quickly
    final int highThreshold = 10_000;
    final int timeoutMs = 2_000; // 2 seconds

    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, highThreshold);
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, timeoutMs);

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

    // Insert a small number of vectors (well below the threshold)
    final int vectorCount = 50;
    database.transaction(() -> {
      for (int i = 0; i < vectorCount; i++)
        database.command("sql", "INSERT INTO Embedding SET vector = ?", (Object) generateRandomVector(random));
    });

    // Get the index and verify mutations are pending
    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("Embedding[vector]");
    final LSMVectorIndex lsmIndex = (LSMVectorIndex) typeIndex.getIndexesOnBuckets()[0];

    Map<String, Long> stats = lsmIndex.getStats();
    assertThat(stats.get("mutationsSinceRebuild"))
        .as("Mutations should be pending (not yet at threshold)")
        .isGreaterThan(0L);
    assertThat(stats.get("deltaVectorsCount"))
        .as("Delta buffer should have entries before timeout fires")
        .isGreaterThan(0L);

    // Wait for the inactivity timeout to fire plus some margin
    Thread.sleep(timeoutMs + 3_000);

    // After the timeout, the graph should have been rebuilt and delta buffer flushed
    stats = lsmIndex.getStats();
    assertThat(stats.get("mutationsSinceRebuild"))
        .as("Mutation counter should be reset after inactivity rebuild")
        .isEqualTo(0L);
    assertThat(stats.get("deltaVectorsCount"))
        .as("Delta buffer should be empty after inactivity rebuild")
        .isEqualTo(0L);
  }

  @Test
  void timerShouldResetOnNewMutations() throws Exception {
    // High threshold, short timeout
    final int highThreshold = 10_000;
    final int timeoutMs = 3_000; // 3 seconds

    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, highThreshold);
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, timeoutMs);

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

    // Insert first batch
    database.transaction(() -> {
      for (int i = 0; i < 10; i++)
        database.command("sql", "INSERT INTO Embedding SET vector = ?", (Object) generateRandomVector(random));
    });

    // Wait half the timeout, then insert more (this should reset the timer)
    Thread.sleep(timeoutMs / 2);

    database.transaction(() -> {
      for (int i = 0; i < 10; i++)
        database.command("sql", "INSERT INTO Embedding SET vector = ?", (Object) generateRandomVector(random));
    });

    // Check that mutations are still pending (timer was reset, hasn't fired yet)
    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("Embedding[vector]");
    final LSMVectorIndex lsmIndex = (LSMVectorIndex) typeIndex.getIndexesOnBuckets()[0];

    Map<String, Long> stats = lsmIndex.getStats();
    assertThat(stats.get("mutationsSinceRebuild"))
        .as("Mutations should still be pending (timer was reset)")
        .isGreaterThan(0L);

    // Now wait for the full timeout after the last mutation
    Thread.sleep(timeoutMs + 3_000);

    // After the timeout, all mutations should have been flushed
    stats = lsmIndex.getStats();
    assertThat(stats.get("mutationsSinceRebuild"))
        .as("Mutation counter should be reset after inactivity rebuild")
        .isEqualTo(0L);
  }

  @Test
  void noTimerWhenTimeoutIsZero() throws Exception {
    // Disable the inactivity timeout
    final int highThreshold = 10_000;

    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, highThreshold);
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, 0);

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

    // Insert vectors below threshold
    database.transaction(() -> {
      for (int i = 0; i < 20; i++)
        database.command("sql", "INSERT INTO Embedding SET vector = ?", (Object) generateRandomVector(random));
    });

    // Wait a bit - no timer should fire
    Thread.sleep(3_000);

    // Mutations should still be pending (no timeout configured)
    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("Embedding[vector]");
    final LSMVectorIndex lsmIndex = (LSMVectorIndex) typeIndex.getIndexesOnBuckets()[0];

    final Map<String, Long> stats = lsmIndex.getStats();
    assertThat(stats.get("mutationsSinceRebuild"))
        .as("Mutations should still be pending when timeout is disabled")
        .isGreaterThan(0L);
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
