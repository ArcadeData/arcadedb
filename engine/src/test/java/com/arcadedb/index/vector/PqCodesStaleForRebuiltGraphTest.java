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
import io.github.jbellis.jvector.quantization.PQVectors;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Regression test for the graph/PQ ordinal mismatch that made {@code findNeighborsFromVectorApproximate()} throw
 * {@code IndexException: Error performing PQ approximate search}, wrapping
 * {@code IndexOutOfBoundsException: Ordinal 1280 out of bounds for vector count 1100} raised inside JVector's
 * {@code PQVectors.getChunk}.
 * <p>
 * {@code graphIndex} is published under the write lock a search's read lock excludes, so it cannot move while a
 * search runs. {@code pqVectors} is not: it is republished later and outside that lock (see
 * {@code buildAndPersistPQ}). A rebuild that has already installed its new, larger graph but has not yet installed
 * the codes for it therefore leaves a window in which the two disagree about how many ordinals exist - and the
 * beam walking that graph asks the old, smaller {@code PQVectors} for an ordinal it was never sized for. It throws
 * from {@code GraphSearcher.initializeInternal}, while scoring an entry point, which is before any {@code Bits}
 * filter is consulted - so no filter can close this.
 * <p>
 * In the wild this reproduces only as a race (it was originally seen roughly one run in five under a concurrent
 * async rebuild, on unmodified main). Waiting for that race would make this test a coin flip, so it installs the
 * end state the race produces - a graph larger than the codes cover - directly, and asserts the search degrades to
 * the exact path instead of throwing. The exact path needs no codes at all, so it stays correct for the whole
 * window, and the fast path resumes by itself once the codes are republished.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("vector")
class PqCodesStaleForRebuiltGraphTest extends TestHelper {

  private static final int EMBEDDING_DIM = 32;
  /** Must clear LSMVectorIndex.ASYNC_REBUILD_MIN_GRAPH_SIZE (1000) so the graph takes the rebuild path at all. */
  private static final int BASE_VECTORS  = 1100;
  private static final int EXTRA_VECTORS = 500;

  private static final Duration SETTLE_TIMEOUT = Duration.ofMinutes(2);

  @Test
  void approximateSearchDegradesToExactWhenPqCodesDoNotCoverTheGraph() throws Exception {
    final Random random = new Random(6797);

    database.transaction(() -> {
      database.getSchema().createVertexType("Embedding");
      database.getSchema().getType("Embedding").createProperty("vector", Type.ARRAY_OF_FLOATS);
      database.command("sql", """
          CREATE INDEX ON Embedding (vector) LSM_VECTOR
          METADATA {
              "dimensions": %d,
              "similarity": "EUCLIDEAN",
              "quantization": "PRODUCT",
              "pqClusters": 32
          }""".formatted(EMBEDDING_DIM));
    });

    insertVectors(random, BASE_VECTORS);

    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("Embedding[vector]");
    final LSMVectorIndex index = (LSMVectorIndex) typeIndex.getIndexesOnBuckets()[0];

    // The first search on an empty graph builds it synchronously whatever the thresholds say.
    assertThat(index.findNeighborsFromVector(randomUnitVector(random), 10)).isNotEmpty();
    awaitSettled(index);

    // Before anything is made stale: the settled pair agrees, and the approximate path answers through it. This
    // is what stops the guard from being a way to switch the PQ path off - if it fired in the steady state the
    // index would quietly serve every query from the exact path, and no other assertion here would notice.
    //
    // graphNodeCount is getIdUpperBound(), which is deliberately the same quantity the guard compares against the
    // code count - not size(). Asserting one while the guard checked the other would leave any future divergence
    // between the two (a graph carrying a hole in its id space) invisible to this test.
    assertThat(index.getStats().get("graphNodeCount"))
        .as("a settled index's codes cover exactly the graph they were built from")
        .isLessThanOrEqualTo((long) index.getPQVectorCount());
    assertThat(index.findNeighborsFromVectorApproximate(randomUnitVector(random), 10))
        .as("the approximate path must work normally when the pair agrees").isNotEmpty();

    // The codes as they stand for THIS graph - the snapshot a rebuild is about to make stale.
    final PQVectors staleCodes = pqVectors(index);
    assertThat(staleCodes).as("the fixture must actually have PQ codes to go stale").isNotNull();
    final int staleCount = staleCodes.count();

    // Grow the graph: enough mutations to clear the default ratio-derived threshold, then a search to trigger the
    // rebuild and a wait for it to land.
    insertVectors(random, EXTRA_VECTORS);
    Awaitility.await("the rebuild that grows the graph past the stale codes")
        .atMost(SETTLE_TIMEOUT)
        .pollInterval(Duration.ofMillis(50))
        .untilAsserted(() -> {
          index.findNeighborsFromVector(randomUnitVector(random), 10);
          assertThat(index.getStats().get("deltaVectorsCount")).isZero();
          assertThat(index.getStats().get("graphNodeCount")).isGreaterThan(staleCount);
        });

    // Nothing may rebuild from here on, or the injected staleness would be repaired before the search reads it.
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_REBUILD_GRAPH_RATIO, 100f);
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MAX_PENDING_MUTATIONS, 0);
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, 0);

    // Install the state the race produces: the new, larger graph paired with the codes of the old, smaller one.
    setPqVectors(index, staleCodes);

    assertThat(index.isPQSearchAvailable())
        .as("the approximate path must still be selected, or this would silently test the exact path instead")
        .isTrue();
    assertThat(index.getStats().get("graphNodeCount"))
        .as("the point of the fixture is a graph holding ordinals the codes were never sized for")
        .isGreaterThan((long) index.getPQVectorCount());

    // The refused attempt searches nothing and hands the query to findNeighborsFromVector, which counts and times
    // itself. So it must not be counted here as well: one caller-facing search would be charged as two, and the
    // two clocks do not measure the same span - this method's spans the exact search on top of its own guard
    // overhead - so the summed latency would outgrow the operation count getAvgSearchLatencyMs divides it by.
    final long searchOpsBefore = index.getStats().get("searchOperations");

    assertThatCode(() -> {
      final var results = index.findNeighborsFromVectorApproximate(randomUnitVector(random), 10);
      assertThat(results)
          .as("degrading to the exact path must still answer the query, not return an empty result")
          .isNotEmpty();
    })
        .as("walking a graph through codes that do not cover it threw IndexOutOfBoundsException from deep inside "
            + "JVector; the search must refuse the mismatched pair and answer exactly instead")
        .doesNotThrowAnyException();

    assertThat(index.getStats().get("searchOperations") - searchOpsBefore)
        .as("a refused PQ attempt is not a search of its own - only the exact search it delegates to may be counted")
        .isEqualTo(1L);
  }

  private static PQVectors pqVectors(final LSMVectorIndex index) throws Exception {
    final Field field = LSMVectorIndex.class.getDeclaredField("pqVectors");
    field.setAccessible(true);
    return (PQVectors) field.get(index);
  }

  private static void setPqVectors(final LSMVectorIndex index, final PQVectors codes) throws Exception {
    final Field field = LSMVectorIndex.class.getDeclaredField("pqVectors");
    field.setAccessible(true);
    field.set(index, codes);
  }

  private void awaitSettled(final LSMVectorIndex index) {
    Awaitility.await("the initial synchronous build settles the buffer")
        .atMost(SETTLE_TIMEOUT)
        .pollInterval(Duration.ofMillis(50))
        .untilAsserted(() -> assertThat(index.getStats().get("deltaVectorsCount")).isZero());
  }

  private void insertVectors(final Random random, final int count) {
    database.transaction(() -> {
      for (int i = 0; i < count; i++)
        database.command("sql", "INSERT INTO Embedding SET vector = ?", (Object) randomUnitVector(random));
    });
  }

  private float[] randomUnitVector(final Random random) {
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
