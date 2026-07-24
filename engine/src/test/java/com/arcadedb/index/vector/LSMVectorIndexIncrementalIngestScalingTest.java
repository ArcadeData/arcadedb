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
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #5391: an LSM_VECTOR index became unusable around 200K vectors - GC thrashing, multi-second query
 * stalls and commit lock timeouts. Three independent defects on the incremental-ingestion path caused it:
 * <ul>
 *   <li>a graph rebuild held the index write lock for its whole O(index size) validation phase, freezing
 *       every insert and every search on the index until the rebuild finished;</li>
 *   <li>the delta buffer of not-yet-indexed vectors was re-converted in full on every query and every
 *       candidate was appended to an unbounded result list before sorting;</li>
 *   <li>the graph never caught up under sustained ingestion, because a finished rebuild waited for the next
 *       search to notice that mutations were still pending.</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class LSMVectorIndexIncrementalIngestScalingTest extends TestHelper {

  private static final int DIM = 32;

  // A rebuild used to hold the index write lock across its whole validation phase (one record read per indexed
  // vector), so concurrent inserts blocked for the entire rebuild. Validation now runs unlocked.
  @Test
  void insertsShouldNotBlockWhileAGraphRebuildValidatesVectors() throws Exception {
    // Enough vectors that validation reports progress at least once mid-phase (every 1000 validated vectors)
    final int vectors = 2_500;
    final long validationHoldMs = 3_000;

    createIndex();
    final LSMVectorIndex idx = index();
    final Random random = new Random(42);

    database.transaction(() -> {
      for (int i = 0; i < vectors; i++)
        database.command("sql", "INSERT INTO Paper SET embedding = ?", (Object) randomVector(random));
    });
    idx.buildVectorGraphNow();

    final CountDownLatch insertDone = new CountDownLatch(1);
    final AtomicReference<Exception> insertFailure = new AtomicReference<>();
    final AtomicBoolean concurrentInsertStarted = new AtomicBoolean();
    final AtomicBoolean insertCommittedDuringValidation = new AtomicBoolean();

    // Force a second rebuild and stall its validation phase. While it is stalled, an unrelated insert must
    // still commit: before the fix it blocked on the index write lock until the whole rebuild completed.
    idx.buildVectorGraphNow((phase, processedNodes, totalNodes, vectorAccesses) -> {
      if (!"validating".equals(phase) || processedNodes <= 0 || processedNodes >= totalNodes)
        return;
      if (!concurrentInsertStarted.compareAndSet(false, true))
        return;

      final Thread writer = new Thread(() -> {
        try {
          database.transaction(() ->
              database.command("sql", "INSERT INTO Paper SET embedding = ?", (Object) randomVector(new Random(7))));
        } catch (final Exception e) {
          insertFailure.set(e);
        } finally {
          insertDone.countDown();
        }
      }, "issue-5391-concurrent-writer");
      writer.setDaemon(true);
      writer.start();

      // Stall validation while waiting for that insert. With the write lock held across validation the insert
      // cannot commit until this callback returns, so the wait times out.
      try {
        insertCommittedDuringValidation.set(insertDone.await(validationHoldMs, TimeUnit.MILLISECONDS));
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    });

    assertThat(concurrentInsertStarted.get())
        .as("the rebuild should have reported at least one mid-validation progress event")
        .isTrue();
    assertThat(insertFailure.get()).isNull();
    assertThat(insertCommittedDuringValidation.get())
        .as("an insert must commit while a graph rebuild is validating vectors, not wait for the rebuild to end")
        .isTrue();
  }

  // The delta buffer holds every vector ingested since the last rebuild and is scanned in full on each query.
  // The scan must still return the exact top-k, now through a bounded heap instead of an unbounded sort.
  @Test
  void deltaScanShouldReturnTheExactTopKWithoutOverflowingTheResultList() {
    // Never rebuild: every vector stays in the delta buffer, so the search result comes purely from the scan
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, 1_000_000);
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, 0);
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_REBUILD_GRAPH_RATIO, 0f);

    createIndex();
    final LSMVectorIndex idx = index();
    final Random random = new Random(7);

    final int vectors = 2_000;
    final int k = 10;
    final List<float[]> ingested = new ArrayList<>(vectors);
    database.transaction(() -> {
      for (int i = 0; i < vectors; i++) {
        final float[] v = randomVector(random);
        ingested.add(v);
        database.command("sql", "INSERT INTO Paper SET embedding = ?", (Object) v);
      }
    });

    assertThat(idx.getStats().get("deltaVectorsCount"))
        .as("all vectors should still be pending in the delta buffer")
        .isEqualTo((long) vectors);

    final float[] query = randomVector(random);
    final List<Pair<RID, Float>> results = idx.findNeighborsFromVector(query, k);

    assertThat(results).as("delta scan must return exactly k results, not the whole buffer").hasSize(k);

    // Distances must be ascending and must match the k best cosine distances computed by brute force
    final List<Float> expected = ingested.stream()
        .map(v -> 1f - cosine(query, v))
        .sorted()
        .limit(k)
        .toList();
    for (int i = 0; i < k; i++)
      assertThat(results.get(i).getSecond())
          .as("delta scan result %d must be the %d-th nearest vector", i, i)
          .isCloseTo(expected.get(i), org.assertj.core.data.Offset.offset(1e-4f));
  }

  // A rebuild only absorbs the vectors persisted when it started. Anything ingested while it ran used to sit in
  // the delta buffer until the next search happened to notice, leaving the graph frozen under sustained
  // ingestion. A finished rebuild now chains into the next one on its own.
  @Test
  void graphShouldKeepAbsorbingPendingVectorsWithoutFurtherSearches() {
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, 100);
    // Disable the inactivity timer so the chained rebuild is the only thing that can drain the buffer
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, 0);
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_REBUILD_GRAPH_RATIO, 0f);

    createIndex();
    final LSMVectorIndex idx = index();
    final Random random = new Random(11);

    // Build an initial graph above ASYNC_REBUILD_MIN_GRAPH_SIZE so rebuilds take the async path
    database.transaction(() -> {
      for (int i = 0; i < 1_200; i++)
        database.command("sql", "INSERT INTO Paper SET embedding = ?", (Object) randomVector(random));
    });
    idx.buildVectorGraphNow();
    assertThat(idx.getStats().get("deltaVectorsCount")).isZero();

    // Ingest more, then trigger exactly one search: it starts an async rebuild for the vectors persisted so far
    database.transaction(() -> {
      for (int i = 0; i < 800; i++)
        database.command("sql", "INSERT INTO Paper SET embedding = ?", (Object) randomVector(random));
    });
    idx.findNeighborsFromVector(randomVector(random), 5);

    // Wait until that rebuild is actually running before ingesting again, so the next batch is guaranteed to
    // land after its snapshot and therefore to still be pending when it finishes. Without this the rebuild can
    // win the race, drain everything, and the chain would have nothing left to pick up.
    Awaitility.await("the async rebuild is running")
        .atMost(Duration.ofSeconds(30))
        .pollInterval(Duration.ofMillis(10))
        .until(() -> idx.getStats().get("asyncRebuildInProgress") == 1L);

    // Keep ingesting so vectors land after that rebuild's snapshot, then stop touching the index entirely
    database.transaction(() -> {
      for (int i = 0; i < 800; i++)
        database.command("sql", "INSERT INTO Paper SET embedding = ?", (Object) randomVector(random));
    });

    Awaitility.await("the graph absorbs every pending vector without any further search")
        .atMost(Duration.ofSeconds(60))
        .pollInterval(Duration.ofMillis(200))
        .untilAsserted(() -> {
          final Map<String, Long> stats = idx.getStats();
          assertThat(stats.get("deltaVectorsCount")).isZero();
          assertThat(stats.get("graphNodeCount")).isEqualTo(2_800L);
        });
  }

  // Rebuilding the whole graph every fixed number of mutations costs O(index size) per handful of new vectors.
  // The trigger threshold now scales with the graph, so a large index is not rebuilt for a trickle of inserts.
  @Test
  void rebuildThresholdShouldScaleWithGraphSize() {
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, 100);
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, 0);
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_REBUILD_GRAPH_RATIO, 0.2f);

    createIndex();
    final LSMVectorIndex idx = index();
    final Random random = new Random(13);

    final int graphSize = 2_000;
    database.transaction(() -> {
      for (int i = 0; i < graphSize; i++)
        database.command("sql", "INSERT INTO Paper SET embedding = ?", (Object) randomVector(random));
    });
    idx.buildVectorGraphNow();

    final long rebuildsBefore = idx.getStats().get("graphRebuildCount");

    // 150 mutations: above the absolute threshold (100) but below 20% of a 2000-vector graph (400)
    database.transaction(() -> {
      for (int i = 0; i < 150; i++)
        database.command("sql", "INSERT INTO Paper SET embedding = ?", (Object) randomVector(random));
    });
    idx.findNeighborsFromVector(randomVector(random), 5);

    assertThat(idx.getStats().get("graphRebuildCount"))
        .as("a 2000-vector graph must not be rebuilt to absorb 150 vectors")
        .isEqualTo(rebuildsBefore);
    assertThat(idx.getStats().get("mutationsSinceRebuild")).isEqualTo(150L);

    // Past 20% of the graph the rebuild does trigger
    database.transaction(() -> {
      for (int i = 0; i < 300; i++)
        database.command("sql", "INSERT INTO Paper SET embedding = ?", (Object) randomVector(random));
    });
    idx.findNeighborsFromVector(randomVector(random), 5);

    Awaitility.await("crossing the scaled threshold triggers a rebuild")
        .atMost(Duration.ofSeconds(60))
        .pollInterval(Duration.ofMillis(100))
        .until(() -> idx.getStats().get("graphRebuildCount") > rebuildsBefore);
  }

  private void createIndex() {
    database.transaction(() -> {
      database.getSchema().createVertexType("Paper");
      database.getSchema().getType("Paper").createProperty("embedding", Type.ARRAY_OF_FLOATS);
      database.command("sql", """
          CREATE INDEX ON Paper (embedding) LSM_VECTOR
          METADATA {"dimensions": %d, "similarity": "COSINE"}""".formatted(DIM));
    });
  }

  private LSMVectorIndex index() {
    return (LSMVectorIndex) ((TypeIndex) database.getSchema().getIndexByName("Paper[embedding]")).getIndexesOnBuckets()[0];
  }

  private static float cosine(final float[] a, final float[] b) {
    float dot = 0, na = 0, nb = 0;
    for (int i = 0; i < a.length; i++) {
      dot += a[i] * b[i];
      na += a[i] * a[i];
      nb += b[i] * b[i];
    }
    return dot / (float) (Math.sqrt(na) * Math.sqrt(nb));
  }

  private static float[] randomVector(final Random random) {
    final float[] v = new float[DIM];
    float norm = 0;
    for (int i = 0; i < DIM; i++) {
      v[i] = random.nextFloat() * 2 - 1;
      norm += v[i] * v[i];
    }
    norm = (float) Math.sqrt(norm);
    if (norm > 0)
      for (int i = 0; i < DIM; i++)
        v[i] /= norm;
    return v;
  }
}
