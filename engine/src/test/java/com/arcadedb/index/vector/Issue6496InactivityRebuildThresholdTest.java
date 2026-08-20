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

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #6496: the inactivity rebuild timer used to arm and fire on {@code pending > 0} alone, with no regard for
 * the mutation-driven rebuild threshold that {@link LSMVectorIndex#rebuildGraphBeforeSearch()} already honours. A
 * single insert into a large, settled {@code LSM_VECTOR} index therefore cost a full O(N) graph rebuild on the
 * next quiet period - repeatedly, for every trickle insert - even though the very same single mutation against
 * the same threshold would not have triggered anything on the search path.
 * <p>
 * The fix ({@code inactivityRebuildIsWorthIt()}) must apply that gate only where a rebuild is actually expensive:
 * once the graph has grown past {@code ASYNC_REBUILD_MIN_GRAPH_SIZE} (1000 vectors). Below that size a rebuild is
 * cheap and {@code rebuildGraphBeforeSearch()} itself performs it unconditionally, so gating the timer there too
 * would only delay a rebuild that was never a performance problem, leaving pending vectors sitting in the
 * linear-scan delta buffer for no benefit - this is covered separately by
 * {@code LSMVectorIndexRebuildTest.deltaBufferShouldFlushAfterInactivityTimeout} and friends, which pin the
 * small-graph "any pending mutation flushes promptly" behaviour and must keep passing unmodified.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("vector")
class Issue6496InactivityRebuildThresholdTest extends TestHelper {

  private static final int EMBEDDING_DIM = 32;
  // Must be >= LSMVectorIndex.ASYNC_REBUILD_MIN_GRAPH_SIZE (1000) so the timer treats this as a "large" graph.
  private static final int LARGE_INDEX_VECTORS = 1100;

  private static final Duration REBUILD_SETTLE_TIMEOUT =
      Duration.ofMillis(GlobalConfiguration.VECTOR_INDEX_REBUILD_PERMIT_TIMEOUT_MS.getValueAsLong() + 60_000L);

  /**
   * The scenario measured in issue #6496: a settled, large index takes a single insert, then goes quiet. The
   * inactivity timer must NOT rebuild the whole graph for one mutation against a threshold of 100 - it must wait
   * until enough mutations (the threshold-derived floor) have accumulated.
   */
  @Test
  void singleInsertIntoLargeSettledIndexMustNotTriggerFullRebuildOnInactivity() throws Exception {
    final int threshold = 100;
    final int timeoutMs = 500;
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, threshold);
    // Pin the absolute threshold: disable graph-size scaling so the floor is deterministic (threshold/10 = 10).
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_REBUILD_GRAPH_RATIO, 0f);
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, timeoutMs);

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

    // Settle the index: insert enough vectors to cross ASYNC_REBUILD_MIN_GRAPH_SIZE and build the graph.
    database.transaction(() -> {
      for (int i = 0; i < LARGE_INDEX_VECTORS; i++)
        database.command("sql", "INSERT INTO Embedding SET vector = ?", (Object) generateRandomVector(random));
    });

    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("Embedding[vector]");
    final LSMVectorIndex lsmIndex = (LSMVectorIndex) typeIndex.getIndexesOnBuckets()[0];
    final float[] queryVector = generateRandomVector(random);
    final List<Pair<RID, Float>> initialResults = lsmIndex.findNeighborsFromVector(queryVector, 10);
    assertThat(initialResults).isNotEmpty();

    Awaitility.await("initial synchronous build settles the mutation counter")
        .atMost(REBUILD_SETTLE_TIMEOUT)
        .pollInterval(Duration.ofMillis(50))
        .untilAsserted(() -> assertThat(lsmIndex.getStats().get("mutationsSinceRebuild")).isEqualTo(0L));

    // The bug: one single insert into the now-settled, large graph.
    database.transaction(() ->
        database.command("sql", "INSERT INTO Embedding SET vector = ?", (Object) generateRandomVector(random)));

    assertThat(lsmIndex.getStats().get("mutationsSinceRebuild"))
        .as("one pending mutation, well below the threshold-derived floor of %d", threshold / 10)
        .isEqualTo(1L);

    final long snapshotGenBefore = lsmIndex.getStats().get("rebuildSnapshotGeneration");

    // Wait comfortably past the inactivity timeout: the timer must fire and decline to rebuild.
    Thread.sleep(timeoutMs * 4L);

    assertThat(lsmIndex.getStats().get("mutationsSinceRebuild"))
        .as("a single pending mutation against a threshold of %d must not trigger a full graph rebuild "
            + "on inactivity (issue #6496)", threshold)
        .isEqualTo(1L);
    assertThat(lsmIndex.getStats().get("rebuildSnapshotGeneration"))
        .as("no rebuild - of any kind - should have run")
        .isEqualTo(snapshotGenBefore);
  }

  /**
   * Once enough mutations accumulate to reach the threshold-derived floor, the inactivity timer must still do its
   * job and eventually flush the delta buffer - the fix must not defer rebuilds forever, only avoid the wasteful
   * single-mutation case.
   */
  @Test
  void pendingMutationsAtOrAboveTheFloorAreEventuallyRebuiltOnInactivity() throws Exception {
    final int threshold = 100;
    final int floor = threshold / 10; // 10, per inactivityRebuildIsWorthIt()'s Math.max(threshold / 10, 1)
    final int timeoutMs = 500;
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, threshold);
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_REBUILD_GRAPH_RATIO, 0f);
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, timeoutMs);

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

    database.transaction(() -> {
      for (int i = 0; i < LARGE_INDEX_VECTORS; i++)
        database.command("sql", "INSERT INTO Embedding SET vector = ?", (Object) generateRandomVector(random));
    });

    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("Embedding[vector]");
    final LSMVectorIndex lsmIndex = (LSMVectorIndex) typeIndex.getIndexesOnBuckets()[0];
    final float[] queryVector = generateRandomVector(random);
    assertThat(lsmIndex.findNeighborsFromVector(queryVector, 10)).isNotEmpty();

    Awaitility.await("initial synchronous build settles the mutation counter")
        .atMost(REBUILD_SETTLE_TIMEOUT)
        .pollInterval(Duration.ofMillis(50))
        .untilAsserted(() -> assertThat(lsmIndex.getStats().get("mutationsSinceRebuild")).isEqualTo(0L));

    // Insert exactly the floor's worth of pending mutations, then go quiet.
    database.transaction(() -> {
      for (int i = 0; i < floor; i++)
        database.command("sql", "INSERT INTO Embedding SET vector = ?", (Object) generateRandomVector(random));
    });

    assertThat(lsmIndex.getStats().get("mutationsSinceRebuild")).isEqualTo((long) floor);

    Awaitility.await("the inactivity timer rebuilds once pending mutations reach the floor")
        .atMost(REBUILD_SETTLE_TIMEOUT)
        .pollInterval(Duration.ofMillis(50))
        .untilAsserted(() -> assertThat(lsmIndex.getStats().get("mutationsSinceRebuild")).isEqualTo(0L));
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
