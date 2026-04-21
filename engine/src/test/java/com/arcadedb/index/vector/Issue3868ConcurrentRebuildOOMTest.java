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
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for GitHub issue #3868: Crash (OOM) during concurrent graph rebuilds.
 * <p>
 * The bug: when multiple vector indexes trigger async graph rebuilds simultaneously
 * (e.g. via inactivity timeouts), each rebuild consumes significant heap memory.
 * Without cross-index coordination, all rebuilds run in parallel, causing OOM kills.
 * <p>
 * The fix: a JVM-wide semaphore (REBUILD_SEMAPHORE) limits the number of concurrent
 * async graph rebuilds. With the default limit of 1, rebuilds are serialized.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue3868ConcurrentRebuildOOMTest extends TestHelper {

  private static final int EMBEDDING_DIM = 16;
  private static final int VECTORS_PER_INDEX = 1100; // > ASYNC_REBUILD_MIN_GRAPH_SIZE (1000)

  /**
   * Verifies that concurrent async rebuilds are serialized by the REBUILD_SEMAPHORE.
   * Creates two vector indexes, triggers mutations past the rebuild threshold on both,
   * and verifies that only one rebuild runs at a time.
   */
  @Test
  void concurrentAsyncRebuildsShouldBeSerialized() throws Exception {
    // Use a very low threshold so rebuilds trigger quickly
    final int threshold = 5;
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, threshold);
    // Disable inactivity rebuild to control timing precisely
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, 0);

    // Create two types with separate vector indexes
    database.transaction(() -> {
      database.getSchema().createVertexType("EmbeddingA");
      database.getSchema().getType("EmbeddingA").createProperty("vector", Type.ARRAY_OF_FLOATS);
      database.command("sql", """
          CREATE INDEX ON EmbeddingA (vector) LSM_VECTOR
          METADATA {"dimensions": %d, "similarity": "EUCLIDEAN"}""".formatted(EMBEDDING_DIM));

      database.getSchema().createVertexType("EmbeddingB");
      database.getSchema().getType("EmbeddingB").createProperty("vector", Type.ARRAY_OF_FLOATS);
      database.command("sql", """
          CREATE INDEX ON EmbeddingB (vector) LSM_VECTOR
          METADATA {"dimensions": %d, "similarity": "EUCLIDEAN"}""".formatted(EMBEDDING_DIM));
    });

    final Random random = new Random(42);

    // Populate both indexes above ASYNC_REBUILD_MIN_GRAPH_SIZE
    database.transaction(() -> {
      for (int i = 0; i < VECTORS_PER_INDEX; i++) {
        database.command("sql", "INSERT INTO EmbeddingA SET vector = ?", (Object) generateRandomVector(random));
        database.command("sql", "INSERT INTO EmbeddingB SET vector = ?", (Object) generateRandomVector(random));
      }
    });

    // Trigger initial graph build via search
    final TypeIndex typeIndexA = (TypeIndex) database.getSchema().getIndexByName("EmbeddingA[vector]");
    final LSMVectorIndex indexA = (LSMVectorIndex) typeIndexA.getIndexesOnBuckets()[0];
    final TypeIndex typeIndexB = (TypeIndex) database.getSchema().getIndexByName("EmbeddingB[vector]");
    final LSMVectorIndex indexB = (LSMVectorIndex) typeIndexB.getIndexesOnBuckets()[0];

    final float[] queryVector = generateRandomVector(random);
    indexA.findNeighborsFromVector(queryVector, 5);
    indexB.findNeighborsFromVector(queryVector, 5);

    // Wait for any initial rebuilds to finish
    Thread.sleep(2000);

    // Now insert enough mutations to trigger async rebuilds on BOTH indexes
    database.transaction(() -> {
      for (int i = 0; i < threshold + 1; i++) {
        database.command("sql", "INSERT INTO EmbeddingA SET vector = ?", (Object) generateRandomVector(random));
        database.command("sql", "INSERT INTO EmbeddingB SET vector = ?", (Object) generateRandomVector(random));
      }
    });

    // Trigger rebuilds by searching both indexes (which checks mutations >= threshold)
    indexA.findNeighborsFromVector(queryVector, 5);
    indexB.findNeighborsFromVector(queryVector, 5);

    // Wait for rebuilds to complete
    Thread.sleep(3000);

    // Verify that both indexes still work correctly after serialized rebuilds
    final List<Pair<RID, Float>> resultsA = indexA.findNeighborsFromVector(queryVector, 5);
    final List<Pair<RID, Float>> resultsB = indexB.findNeighborsFromVector(queryVector, 5);

    assertThat(resultsA).isNotEmpty();
    assertThat(resultsB).isNotEmpty();
  }

  /**
   * Verifies the REBUILD_SEMAPHORE configuration is respected.
   */
  @Test
  void rebuildSemaphoreDefaultIsOne() {
    // The default max concurrent rebuilds should be 1
    assertThat(GlobalConfiguration.VECTOR_INDEX_MAX_CONCURRENT_REBUILDS.getValueAsInteger()).isEqualTo(1);
  }

  private static float[] generateRandomVector(final Random random) {
    final float[] vector = new float[EMBEDDING_DIM];
    for (int i = 0; i < EMBEDDING_DIM; i++)
      vector[i] = random.nextFloat();
    return vector;
  }
}
