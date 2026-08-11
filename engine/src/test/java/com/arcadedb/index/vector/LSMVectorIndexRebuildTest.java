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
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LSMVectorIndexMetadata;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.Pair;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * LSM vector index rebuild semantics: threshold-triggered, inactivity-triggered, async retrigger, metadata preservation, and concurrent rebuild serialization.
 * <p>
 * Tagged {@code vector} so the whole class runs in the {@code vector-unit-tests} CI lane. What earns the tag is not the average cost but the spread:
 * measured on one commit, this class took 19 s in a green unit-test run and 626 s in a run that then hit the job's 60-minute timeout. Almost none of
 * either figure is compute. {@link com.arcadedb.index.vector.LSMVectorIndex}'s {@code REBUILD_SEMAPHORE} has one permit for the entire JVM, so every
 * vector class sharing a Surefire fork queues behind every other one - including a rebuild left over from an already-finished class - and the waits
 * below then have to be sized for that worst case. A lane of its own keeps the convoy off the other ~1300 engine test classes. It does not fix the
 * convoy, which is still here; making the permit count configurable, or scoping it per database, would.
 * <p>
 * That queueing theory explains a genuine class of slowdown, but is not what the recurring CI flakes in this class turn out to be: a run where
 * {@code REBUILD_SETTLE_TIMEOUT} was raised to sit above {@code VECTOR_INDEX_REBUILD_PERMIT_TIMEOUT_MS}'s own 600s production timeout still timed
 * out at the new, higher ceiling with no "Timed out after ... waiting for a vector index rebuild permit" warning anywhere in the log - the one line
 * {@code startAsyncGraphRebuild()} would have logged had it actually been waiting on a held permit. So the rebuild that timed out was not queued
 * behind another one; it held the permit itself and simply took far longer than usual to run, which points at CPU-starved CI compute rather than
 * semaphore contention. Configuring more permits would not have helped this specific symptom - it would only let more CPU-starved rebuilds compete
 * for the same scarce cores at once.
 */
@Tag("vector")
class LSMVectorIndexRebuildTest extends TestHelper {

  private static final int EMBEDDING_DIM = 32;
  // Must be >= ASYNC_REBUILD_MIN_GRAPH_SIZE (1000) so the async path is used
  private static final int LARGE_INDEX_VECTORS = 1100;

  /**
   * Ceiling for every "the background rebuild has settled" wait in this class (issue #5765).
   * <p>
   * Deliberately an absolute duration rather than a multiple of the inactivity timeout: what varies between a fast
   * laptop and a loaded CI runner with several engine suites in flight is how long the REBUILD itself takes, and that
   * has nothing to do with how long the timer waits before starting it. Scaling the bound off {@code timeoutMs} made
   * the wait shortest exactly where the work is slowest. Generous on purpose - it costs nothing when the rebuild
   * lands on time, and these are liveness assertions: a rebuild that never happens still fails, just later.
   * <p>
   * Raised from 120s to 300s (PR #5960's CI run): {@code asyncRebuildShouldBeRetriggeredForMutationsDuringBuild}
   * and {@code deltaBufferShouldFlushAfterInactivityTimeout} both hit the 120s ceiling on a loaded runner in the
   * same job where an unrelated vector test ({@code PQSearchDebugTest}) took 403s for work that normally
   * completes in seconds - concrete evidence that 120s is not generous enough on this CI environment's worst
   * case. {@code REBUILD_SEMAPHORE} (JVM-wide, one permit) is shared by every vector index in the whole Surefire
   * fork, so a slow rebuild left over from an unrelated, already-finished test class can still be holding the
   * permit when this class's own tests start.
   * <p>
   * Raised again, from a flat 300s to {@code VECTOR_INDEX_REBUILD_PERMIT_TIMEOUT_MS}'s effective value (600s
   * unless something in this JVM overrode it) plus a 60s margin (issue #6032): a same-day fix (commit
   * {@code e1aa64203}) bounded {@code startAsyncGraphRebuild()}'s semaphore acquire at 600s with a diagnostic
   * WARNING on timeout, specifically so a stuck rebuild would be diagnosable in a future recurrence. But this
   * class's own 300s ceiling - half the production timeout - meant the Awaitility assertion always gave up and
   * failed 300s *before* the production wait could even reach its own timeout and log anything, so the fix's
   * diagnostic benefit could never fire for exactly the tests it was meant to help diagnose (confirmed 3 times:
   * PR #5960, #5980, #6019 - no permit-timeout WARNING ever appeared in any of those failing runs). Deriving the
   * bound from the production config's own live value, instead of a second hardcoded constant, means the two can
   * never silently drift apart again the way the flat 300s did - and stays honest about what the production wait
   * will actually do even in the (currently hypothetical) case where something else in this JVM changes the
   * setting before this field initializes.
   * <p>
   * Invariant this relies on: nothing may change {@code VECTOR_INDEX_REBUILD_PERMIT_TIMEOUT_MS} before this static
   * field is initialized (i.e. before this class's first test runs). The setting is {@code SCOPE.JVM}, so this is
   * not only about this class's own tests - any other test class in the same Surefire fork that changes it and
   * does not reset it before this class loads could also shrink this ceiling. Not currently violated by anything
   * in this class (or observed from another class in this codebase), but a future test added anywhere that sets
   * this value in a static initializer, or a {@code @BeforeAll} that runs before this class loads, would silently
   * change it too.
   */
  private static final Duration REBUILD_SETTLE_TIMEOUT =
      Duration.ofMillis(GlobalConfiguration.VECTOR_INDEX_REBUILD_PERMIT_TIMEOUT_MS.getValueAsLong() + 60_000L);

  // Issue #3147: REBUILD INDEX preserves vector metadata (dimensions, similarity, maxConnections, beamWidth, idPropertyName) instead of recreating with dimensions=0.
  @Test
  void rebuildIndexPreservesVectorMetadata() {
    final int dimensions = 128;
    final int maxConnections = 32;
    final int beamWidth = 200;

    // Create type
    final DocumentType type = database.getSchema().createDocumentType("Embedding");
    type.createProperty("name", String.class);
    type.createProperty("vector", float[].class);

    // Create vector index with custom metadata
    database.command("sql",
        "CREATE INDEX ON Embedding (vector) LSM_VECTOR METADATA " +
            "{dimensions: " + dimensions + ", similarity: 'DOT_PRODUCT', " +
            "maxConnections: " + maxConnections + ", beamWidth: " + beamWidth + ", " +
            "idPropertyName: 'name'}");

    // Add test data
    database.begin();
    for (int i = 0; i < 20; i++) {
      final float[] vector = new float[dimensions];
      for (int j = 0; j < dimensions; j++) {
        vector[j] = (float) Math.random();
      }
      database.newDocument("Embedding")
          .set("name", "embedding" + i)
          .set("vector", vector)
          .save();
    }
    database.commit();

    // Verify index exists with correct metadata before rebuild
    Index index = database.getSchema().getIndexByName("Embedding[vector]");
    assertThat(index).as("Index should exist").isNotNull();
    assertThat(index.getType().toString()).isEqualTo("LSM_VECTOR");

    LSMVectorIndex vectorIndex = (LSMVectorIndex) Arrays.stream(database.getSchema().getIndexes())
        .filter(i -> i instanceof LSMVectorIndex)
        .findFirst()
        .orElseThrow();

    final LSMVectorIndexMetadata metadataBefore = vectorIndex.getMetadata();
    assertThat(metadataBefore.dimensions).isEqualTo(dimensions);
    assertThat(metadataBefore.similarityFunction).isEqualTo(VectorSimilarityFunction.DOT_PRODUCT);
    assertThat(metadataBefore.maxConnections).isEqualTo(maxConnections);
    assertThat(metadataBefore.beamWidth).isEqualTo(beamWidth);

    // Execute REBUILD INDEX
    database.command("sql", "REBUILD INDEX `Embedding[vector]`");

    // Verify index still exists with same metadata after rebuild
    final Index rebuiltTypeIndex = database.getSchema().getIndexByName("Embedding[vector]");
    assertThat(rebuiltTypeIndex).as("Index should exist after rebuild").isNotNull();
    assertThat(rebuiltTypeIndex.getType().toString()).isEqualTo("LSM_VECTOR");

    // Get the underlying bucket index to check metadata
    final LSMVectorIndex rebuiltVectorIndex = (LSMVectorIndex) Arrays.stream(database.getSchema().getIndexes())
        .filter(i -> i instanceof LSMVectorIndex)
        .findFirst()
        .orElseThrow();

    final LSMVectorIndexMetadata metadataAfter = rebuiltVectorIndex.getMetadata();

    assertThat(metadataAfter.dimensions)
        .as("Dimensions should be preserved after rebuild")
        .isEqualTo(dimensions);
    assertThat(metadataAfter.similarityFunction)
        .as("Similarity function should be preserved after rebuild")
        .isEqualTo(VectorSimilarityFunction.DOT_PRODUCT);
    assertThat(metadataAfter.maxConnections)
        .as("Max connections should be preserved after rebuild")
        .isEqualTo(maxConnections);
    assertThat(metadataAfter.beamWidth)
        .as("Beam width should be preserved after rebuild")
        .isEqualTo(beamWidth);
    assertThat(metadataAfter.idPropertyName)
        .as("ID property name should be preserved after rebuild")
        .isEqualTo("name");

    // Verify index is functional after rebuild
    assertThat(rebuiltTypeIndex.countEntries())
        .as("Index should have all entries after rebuild")
        .isEqualTo(20);

    // Verify vector search still works
    final float[] queryVector = new float[dimensions];
    for (int i = 0; i < dimensions; i++) {
      queryVector[i] = (float) Math.random();
    }

    final IndexCursor cursor = rebuiltVectorIndex.get(new Object[] { queryVector }, 5);
    int resultCount = 0;
    while (cursor.hasNext()) {
      cursor.next();
      resultCount++;
    }
    assertThat(resultCount).as("Vector search should return results after rebuild").isGreaterThan(0);
  }

  // Issue #3147: REBUILD INDEX preserves quantization settings (INT8) along with similarity function.
  @Test
  void rebuildIndexPreservesQuantizationSettings() {
    // Create type
    final DocumentType type = database.getSchema().createDocumentType("QuantizedEmbedding");
    type.createProperty("name", String.class);
    type.createProperty("vector", float[].class);

    // Create vector index with INT8 quantization
    database.command("sql",
        """
        CREATE INDEX ON QuantizedEmbedding (vector) LSM_VECTOR METADATA \
        {dimensions: 64, similarity: 'EUCLIDEAN', quantization: 'INT8', \
        maxConnections: 24, beamWidth: 150}\
        """);

    // Add test data
    database.begin();
    for (int i = 0; i < 10; i++) {
      final float[] vector = new float[64];
      for (int j = 0; j < 64; j++) {
        vector[j] = (float) Math.random();
      }
      database.newDocument("QuantizedEmbedding")
          .set("name", "qembed" + i)
          .set("vector", vector)
          .save();
    }
    database.commit();

    // Get metadata before rebuild
    final LSMVectorIndex vectorIndex = (LSMVectorIndex) Arrays.stream(database.getSchema().getIndexes())
        .filter(i -> i instanceof LSMVectorIndex)
        .findFirst()
        .orElseThrow();

    final LSMVectorIndexMetadata metadataBefore = vectorIndex.getMetadata();
    assertThat(metadataBefore.quantizationType).isEqualTo(VectorQuantizationType.INT8);

    // Execute REBUILD INDEX
    database.command("sql", "REBUILD INDEX `QuantizedEmbedding[vector]`");

    // Verify quantization is preserved after rebuild
    final LSMVectorIndex rebuiltVectorIndex = (LSMVectorIndex) Arrays.stream(database.getSchema().getIndexes())
        .filter(i -> i instanceof LSMVectorIndex)
        .findFirst()
        .orElseThrow();

    final LSMVectorIndexMetadata metadataAfter = rebuiltVectorIndex.getMetadata();
    assertThat(metadataAfter.quantizationType)
        .as("Quantization type should be preserved after rebuild")
        .isEqualTo(VectorQuantizationType.INT8);
    assertThat(metadataAfter.similarityFunction)
        .as("Similarity function should be preserved after rebuild")
        .isEqualTo(VectorSimilarityFunction.EUCLIDEAN);
  }

  // Issue #3147: REBUILD INDEX * (rebuild-all) preserves vector metadata across every vector index.
  @Test
  void rebuildAllIndexesPreservesVectorMetadata() {
    // Create type
    final DocumentType type = database.getSchema().createDocumentType("VectorDoc");
    type.createProperty("name", String.class);
    type.createProperty("vector", float[].class);

    // Create vector index
    database.command("sql",
        """
        CREATE INDEX ON VectorDoc (vector) LSM_VECTOR METADATA \
        {dimensions: 32, similarity: 'COSINE', maxConnections: 20, beamWidth: 80}\
        """);

    // Add test data
    database.begin();
    for (int i = 0; i < 5; i++) {
      final float[] vector = new float[32];
      for (int j = 0; j < 32; j++) {
        vector[j] = (float) Math.random();
      }
      database.newDocument("VectorDoc")
          .set("name", "doc" + i)
          .set("vector", vector)
          .save();
    }
    database.commit();

    // Get metadata before rebuild
    final LSMVectorIndex vectorIndex = (LSMVectorIndex) Arrays.stream(database.getSchema().getIndexes())
        .filter(i -> i instanceof LSMVectorIndex)
        .findFirst()
        .orElseThrow();

    assertThat(vectorIndex.getMetadata().dimensions).isEqualTo(32);

    // Execute REBUILD INDEX *
    database.command("sql", "REBUILD INDEX *");

    // Verify metadata is preserved after rebuild all
    final LSMVectorIndex rebuiltVectorIndex = (LSMVectorIndex) Arrays.stream(database.getSchema().getIndexes())
        .filter(i -> i instanceof LSMVectorIndex)
        .findFirst()
        .orElseThrow();

    final LSMVectorIndexMetadata metadataAfter = rebuiltVectorIndex.getMetadata();
    assertThat(metadataAfter.dimensions)
        .as("Dimensions should be preserved after REBUILD INDEX *")
        .isEqualTo(32);
    assertThat(metadataAfter.maxConnections)
        .as("Max connections should be preserved after REBUILD INDEX *")
        .isEqualTo(20);
    assertThat(metadataAfter.beamWidth)
        .as("Beam width should be preserved after REBUILD INDEX *")
        .isEqualTo(80);
  }

  // Issue #3679: vectorNeighbors search must not trigger any rebuild when mutations are below the configured threshold.
  @Test
  void searchShouldNotRebuildGraphBelowMutationThreshold() {
    // Threshold of 100: adding 1 vector should NOT trigger any rebuild on a large graph
    final int threshold = 100;
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

    // Insert enough vectors for a "large" graph
    database.transaction(() -> {
      for (int i = 0; i < LARGE_INDEX_VECTORS; i++)
        database.command("sql", "INSERT INTO Embedding SET vector = ?", (Object) generateRandomVector(random));
    });

    // First search to trigger initial synchronous graph build (graphIndex was null)
    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("Embedding[vector]");
    final LSMVectorIndex lsmIndex = (LSMVectorIndex) typeIndex.getIndexesOnBuckets()[0];
    final float[] queryVector = generateRandomVector(random);
    List<Pair<RID, Float>> results = lsmIndex.findNeighborsFromVector(queryVector, 10);
    assertThat(results).isNotEmpty();

    // After the initial build, mutation counter should be 0
    Map<String, Long> stats = lsmIndex.getStats();
    assertThat(stats.get("mutationsSinceRebuild")).isEqualTo(0L);

    // Now add a SINGLE new vector (below the threshold)
    database.transaction(() ->
      database.command("sql", "INSERT INTO Embedding SET vector = ?", (Object) generateRandomVector(random)));

    // Verify mutation counter is 1 before search
    stats = lsmIndex.getStats();
    assertThat(stats.get("mutationsSinceRebuild")).isEqualTo(1L);

    // Search should return immediately without triggering any rebuild (sync or async)
    results = lsmIndex.findNeighborsFromVector(queryVector, 10);
    assertThat(results).isNotEmpty();

    // Mutation counter should still be 1 - no rebuild was triggered
    stats = lsmIndex.getStats();
    assertThat(stats.get("mutationsSinceRebuild"))
        .as("Mutation counter should NOT be reset when below threshold (1 < %d)", threshold)
        .isEqualTo(1L);
  }

  // Issue #3679: vectorNeighbors search must trigger an async (non-blocking) rebuild once mutations reach the threshold on a large graph.
  @Test
  void searchShouldTriggerAsyncRebuildAtThreshold() throws Exception {
    // Low threshold of 5 so we can easily trigger async rebuild
    final int lowThreshold = 5;
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, lowThreshold);
    // This test pins the absolute trigger threshold, so disable the graph-size scaling added for issue #5391
    // (which would otherwise raise the effective threshold to 20% of the 1100-vector graph).
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_REBUILD_GRAPH_RATIO, 0f);

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

    // Insert enough vectors for a "large" graph
    database.transaction(() -> {
      for (int i = 0; i < LARGE_INDEX_VECTORS; i++)
        database.command("sql", "INSERT INTO Embedding SET vector = ?", (Object) generateRandomVector(random));
    });

    // First search to trigger initial synchronous build
    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("Embedding[vector]");
    final LSMVectorIndex lsmIndex = (LSMVectorIndex) typeIndex.getIndexesOnBuckets()[0];
    final float[] queryVector = generateRandomVector(random);
    List<Pair<RID, Float>> results = lsmIndex.findNeighborsFromVector(queryVector, 10);
    assertThat(results).isNotEmpty();

    // Add enough vectors to exceed the threshold
    database.transaction(() -> {
      for (int i = 0; i < lowThreshold; i++)
        database.command("sql", "INSERT INTO Embedding SET vector = ?", (Object) generateRandomVector(random));
    });

    // Verify mutations are at threshold before search
    Map<String, Long> stats = lsmIndex.getStats();
    assertThat(stats.get("mutationsSinceRebuild")).isGreaterThanOrEqualTo((long) lowThreshold);

    // Search should return immediately (async rebuild starts in background)
    final long startTime = System.nanoTime();
    results = lsmIndex.findNeighborsFromVector(queryVector, 10);
    final long elapsedMs = (System.nanoTime() - startTime) / 1_000_000;
    assertThat(results).isNotEmpty();

    // Search should have returned very fast (not blocked by rebuild)
    assertThat(elapsedMs).as("Search should not block on async rebuild").isLessThan(5000);

    // Wait for the async rebuild to complete. Polled rather than slept: the rebuild runs on a background thread, so
    // any fixed wait is a bet on how loaded the machine is (issue #5765).
    // After async rebuild, mutation counter should be reset or low.
    // With incremental inserts via live builder, counter may reflect inserts that
    // went directly to graph (not via delta/rebuild path).
    Awaitility.await("the async rebuild drains the mutation counter")
        .atMost(REBUILD_SETTLE_TIMEOUT)
        .pollInterval(Duration.ofMillis(100))
        .untilAsserted(() -> assertThat(lsmIndex.getStats().get("mutationsSinceRebuild"))
            .as("Mutation counter should be reset or low after async rebuild completes")
            .isLessThanOrEqualTo((long) lowThreshold));
  }

  // Issue #3679: the IndexCursor get() path also honours the rebuild threshold (no rebuild while below threshold).
  @Test
  void searchViaGetShouldAlsoRespectThreshold() {
    // Test the get() method path (used by IndexCursor)
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

    // Insert enough vectors for a "large" graph
    database.transaction(() -> {
      for (int i = 0; i < LARGE_INDEX_VECTORS; i++)
        database.command("sql", "INSERT INTO Embedding SET vector = ?", (Object) generateRandomVector(random));
    });

    // First query to build graph
    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("Embedding[vector]");
    final LSMVectorIndex lsmIndex = (LSMVectorIndex) typeIndex.getIndexesOnBuckets()[0];
    final float[] queryVector = generateRandomVector(random);
    final List<Pair<RID, Float>> initialResults = lsmIndex.findNeighborsFromVector(queryVector, 10);
    assertThat(initialResults).isNotEmpty();

    // After initial build, mutation counter should be 0
    Map<String, Long> stats = lsmIndex.getStats();
    assertThat(stats.get("mutationsSinceRebuild")).isEqualTo(0L);

    // Add a single vector (below threshold)
    database.transaction(() ->
      database.command("sql", "INSERT INTO Embedding SET vector = ?", (Object) generateRandomVector(random)));

    // Verify mutation counter is 1
    stats = lsmIndex.getStats();
    assertThat(stats.get("mutationsSinceRebuild")).isEqualTo(1L);

    // Second search should NOT trigger rebuild or async rebuild (1 < 50)
    final List<Pair<RID, Float>> results = lsmIndex.findNeighborsFromVector(queryVector, 10);
    assertThat(results).isNotEmpty();

    // Mutation counter should still be 1 (no rebuild happened)
    stats = lsmIndex.getStats();
    assertThat(stats.get("mutationsSinceRebuild"))
        .as("Search should not trigger rebuild for 1 mutation (threshold=%d)", threshold)
        .isEqualTo(1L);
  }

  // Issue #3683: mutations added DURING an async rebuild must be preserved (counter not unconditionally reset) so a follow-up rebuild can incorporate them.
  @Test
  void asyncRebuildShouldBeRetriggeredForMutationsDuringBuild() throws Exception {
    final int threshold = 5;
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, threshold);
    // This test pins the absolute trigger threshold, so disable the graph-size scaling added for issue #5391.
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_REBUILD_GRAPH_RATIO, 0f);
    // Disable the inactivity timer so only the threshold-triggered async rebuild under test can flush the
    // counter; otherwise the timer could fire mid-test and reset the counter, masking the behavior asserted.
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

    // Note the current build-snapshot generation, then trigger the async rebuild via a search.
    final long snapshotGenBefore = lsmIndex.getStats().get("rebuildSnapshotGeneration");
    results = lsmIndex.findNeighborsFromVector(queryVector, 10);
    assertThat(results).isNotEmpty();

    // Wait (bounded) until the async rebuild has snapshotted its start counter. Vectors inserted after this
    // point are recorded past the snapshot, so the rebuild must NOT fold them into its own subtraction
    // (issue #3683). This replaces a fixed 200ms "let it start" sleep whose injection window was missed under
    // CI load; the bound keeps a rebuild delayed behind the single-permit REBUILD_SEMAPHORE from hanging the test.
    Awaitility.await("async rebuild snapshotted its start counter")
        .atMost(REBUILD_SETTLE_TIMEOUT)
        .pollInterval(Duration.ofMillis(20))
        .until(() -> lsmIndex.getStats().get("rebuildSnapshotGeneration") > snapshotGenBefore);

    // Add more vectors DURING the async rebuild, guaranteed to land after the snapshot taken above.
    final int vectorsDuringBuild = threshold + 5;
    database.transaction(() -> {
      for (int i = 0; i < vectorsDuringBuild; i++)
        database.command("sql", "INSERT INTO Embedding SET vector = ?", (Object) generateRandomVector(random));
    });

    // The vectors inserted during the build must not be swallowed by that build's own bookkeeping: they stay
    // pending until a follow-up rebuild picks them up, and none of them may be lost. The follow-up is driven
    // here by a search: this test disables the inactivity timer, and whether the chained rebuild added in issue
    // #5391 catches this batch depends on whether it landed before or after the first rebuild's completion
    // check - that chain is covered on its own by
    // LSMVectorIndexIncrementalIngestScalingTest.graphShouldKeepAbsorbingPendingVectorsWithoutFurtherSearches.
    final long totalInserted = LARGE_INDEX_VECTORS + threshold + vectorsDuringBuild;
    Awaitility.await("every vector inserted during the previous rebuild reaches the graph")
        .atMost(REBUILD_SETTLE_TIMEOUT)
        .pollInterval(Duration.ofMillis(200))
        .untilAsserted(() -> {
          if (lsmIndex.getStats().get("mutationsSinceRebuild") > 0)
            lsmIndex.findNeighborsFromVector(queryVector, 10);

          final Map<String, Long> s = lsmIndex.getStats();
          assertThat(s.get("mutationsSinceRebuild"))
              .as("Once rebuilds settle, no mutation should be left pending")
              .isEqualTo(0L);
          assertThat(s.get("graphState"))
              .as("Graph state should be IMMUTABLE (1) once rebuilds settle")
              .isEqualTo(1L); // GraphState.IMMUTABLE ordinal
          assertThat(s.get("graphNodeCount"))
              .as("Every vector inserted during the previous rebuild must end up in the graph")
              .isEqualTo(totalInserted);
        });

    results = lsmIndex.findNeighborsFromVector(queryVector, 10);
    assertThat(results).isNotEmpty();
  }

  /**
   * Regression test: {@code REBUILD_SEMAPHORE} is JVM-wide with a single permit by default, and an async rebuild
   * used to wait on it with a plain, unbounded {@code acquire()}. If some other rebuild anywhere in the process
   * never returns its permit (its background thread outliving that index's {@code close()} because JVector's
   * {@code GraphIndexBuilder} does not always respond to interruption - see
   * {@code LSMVectorIndex#releaseBackgroundResources()}), every other vector index's rebuild would then block
   * forever, process-wide, until restart. {@code VECTOR_INDEX_REBUILD_PERMIT_TIMEOUT_MS} bounds that wait: this
   * index's rebuild must give up the cycle rather than hang once the permit isn't available in time.
   */
  @Test
  void asyncRebuildGivesUpAfterPermitTimeoutInsteadOfBlockingForever() throws Exception {
    final int threshold = 5;
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, threshold);
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_REBUILD_GRAPH_RATIO, 0f);
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, 0);
    // Shrink the permit-wait ceiling from its 10-minute production default so the test doesn't have to wait for it.
    GlobalConfiguration.VECTOR_INDEX_REBUILD_PERMIT_TIMEOUT_MS.setValue(300);

    try {
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

      final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("Embedding[vector]");
      final LSMVectorIndex lsmIndex = (LSMVectorIndex) typeIndex.getIndexesOnBuckets()[0];
      final float[] queryVector = generateRandomVector(random);
      // Initial synchronous build
      assertThat(lsmIndex.findNeighborsFromVector(queryVector, 10)).isNotEmpty();

      final Field semaphoreField = LSMVectorIndex.class.getDeclaredField("REBUILD_SEMAPHORE");
      semaphoreField.setAccessible(true);
      final Semaphore semaphore = (Semaphore) semaphoreField.get(null);
      final String expectedIndexName = indexName(lsmIndex);

      // Simulate another vector index elsewhere in the JVM already holding the sole rebuild permit and never
      // giving it back - the scenario VECTOR_INDEX_REBUILD_PERMIT_TIMEOUT_MS exists to bound.
      assertThat(semaphore.tryAcquire())
          .as("test setup requires the sole JVM-wide rebuild permit to be free at the start")
          .isTrue();
      try {
        final CapturingHandler handler = new CapturingHandler();
        handler.setLevel(Level.ALL);
        final Logger logger = Logger.getLogger(LSMVectorIndex.class.getName());
        logger.addHandler(handler);
        final Level prevLevel = logger.getLevel();
        logger.setLevel(Level.ALL);
        try {
          // Exceed the mutation threshold, then search to trigger the async rebuild attempt.
          database.transaction(() -> {
            for (int i = 0; i < threshold; i++)
              database.command("sql", "INSERT INTO Embedding SET vector = ?", (Object) generateRandomVector(random));
          });
          assertThat(lsmIndex.findNeighborsFromVector(queryVector, 10)).isNotEmpty();

          Awaitility.await("async rebuild gives up after the permit-wait timeout instead of hanging")
              .atMost(Duration.ofSeconds(10))
              .pollInterval(Duration.ofMillis(20))
              .untilAsserted(() -> {
                assertThat(lsmIndex.getStats().get("asyncRebuildInProgress"))
                    .as("A rebuild that could not get a permit must not be left flagged in-progress forever")
                    .isEqualTo(0L);
                assertThat(handler.snapshot())
                    .as("The timeout must be logged so a starved rebuild elsewhere is diagnosable")
                    .anyMatch(m -> m.contains("Timed out") && m.contains(expectedIndexName));
              });
        } finally {
          logger.removeHandler(handler);
          logger.setLevel(prevLevel);
        }
      } finally {
        semaphore.release();
      }
    } finally {
      GlobalConfiguration.VECTOR_INDEX_REBUILD_PERMIT_TIMEOUT_MS.reset();
    }
  }

  /**
   * Regression test: {@code close()}/{@code releaseBackgroundResources()} gives an in-progress async rebuild
   * thread only a best-effort 5s to stop (interrupt + {@code join(5000)}) before moving on unconditionally. If
   * JVector's {@code GraphIndexBuilder} does not observe that interrupt (see the class-level note on
   * {@code getOrCreateGraphBuildPool()}), the thread outlives close() silently and keeps holding the JVM-wide
   * {@code REBUILD_SEMAPHORE} permit - the exact scenario {@code asyncRebuildGivesUpAfterPermitTimeoutInsteadOf
   * BlockingForever} bounds from the other side. This must be logged, not silent, so the two symptoms (a rebuild
   * thread outliving close() here, a later rebuild elsewhere timing out waiting for the permit) are connectable.
   */
  @Test
  void releaseBackgroundResourcesLogsWhenRebuildThreadOutlivesCloseTimeout() throws Exception {
    database.transaction(() -> {
      database.getSchema().createVertexType("StuckRebuildEmbedding");
      database.getSchema().getType("StuckRebuildEmbedding").createProperty("vector", Type.ARRAY_OF_FLOATS);
      database.command("sql", """
          CREATE INDEX ON StuckRebuildEmbedding (vector) LSM_VECTOR
          METADATA {
              "dimensions": %d,
              "similarity": "EUCLIDEAN"
          }""".formatted(EMBEDDING_DIM));
    });

    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("StuckRebuildEmbedding[vector]");
    final LSMVectorIndex lsmIndex = (LSMVectorIndex) typeIndex.getIndexesOnBuckets()[0];

    // A thread that deliberately swallows interruption, standing in for JVector's GraphIndexBuilder not
    // observing Thread.interrupt() inside a tight compute loop.
    final CountDownLatch stop = new CountDownLatch(1);
    final Thread stuckThread = new Thread(() -> {
      while (stop.getCount() > 0) {
        try {
          stop.await(50, TimeUnit.MILLISECONDS);
        } catch (final InterruptedException ignored) {
          // Deliberately swallow: this is standing in for JVector code that does not check interruption.
        }
      }
    }, "test-stuck-rebuild-thread");
    stuckThread.setDaemon(true);
    stuckThread.start();

    final Field threadField = LSMVectorIndex.class.getDeclaredField("asyncRebuildThread");
    threadField.setAccessible(true);
    threadField.set(lsmIndex, stuckThread);
    final Field inProgressField = LSMVectorIndex.class.getDeclaredField("asyncRebuildInProgress");
    inProgressField.setAccessible(true);
    inProgressField.setBoolean(lsmIndex, true);

    final String expectedIndexName = indexName(lsmIndex);

    final CapturingHandler handler = new CapturingHandler();
    handler.setLevel(Level.ALL);
    final Logger logger = Logger.getLogger(LSMVectorIndex.class.getName());
    logger.addHandler(handler);
    final Level prevLevel = logger.getLevel();
    logger.setLevel(Level.ALL);
    try {
      lsmIndex.releaseBackgroundResources();

      assertThat(handler.snapshot())
          .as("close() must surface that the background rebuild thread outlived the shutdown timeout, since it "
              + "may still hold the JVM-wide REBUILD_SEMAPHORE permit")
          .anyMatch(m -> m.contains("did not terminate") && m.contains(expectedIndexName));
    } finally {
      logger.removeHandler(handler);
      logger.setLevel(prevLevel);
      stop.countDown();
      stuckThread.interrupt();
      stuckThread.join(2000);
    }
  }

  private static String indexName(final LSMVectorIndex index) throws Exception {
    final Field field = LSMVectorIndex.class.getDeclaredField("indexName");
    field.setAccessible(true);
    return (String) field.get(index);
  }

  private static final class CapturingHandler extends Handler {
    private final List<String> records = new CopyOnWriteArrayList<>();

    @Override
    public void publish(final LogRecord record) {
      if (record == null || record.getLevel().intValue() < Level.WARNING.intValue())
        return;
      String msg = record.getMessage();
      if (msg != null && record.getParameters() != null && record.getParameters().length > 0) {
        try {
          msg = msg.formatted(record.getParameters());
        } catch (final Exception ignored) {
        }
      }
      if (msg != null)
        records.add(msg);
    }

    @Override
    public void flush() {
    }

    @Override
    public void close() throws SecurityException {
    }

    List<String> snapshot() {
      return new ArrayList<>(records);
    }
  }

  // Issue #3737: buffered vectors below the rebuild threshold are flushed and the graph rebuilt after the inactivity timeout fires.
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

    // Wait for the inactivity timeout to fire AND for the rebuild it starts to finish. The fixed
    // `timeoutMs + 3s` sleep this replaces bounded only the first of the two (issue #5765).
    Awaitility.await("the inactivity rebuild flushes both counters")
        .atMost(REBUILD_SETTLE_TIMEOUT)
        .pollInterval(Duration.ofMillis(100))
        .untilAsserted(() -> {
          final Map<String, Long> settled = lsmIndex.getStats();
          assertThat(settled.get("mutationsSinceRebuild"))
              .as("Mutation counter should be reset after inactivity rebuild")
              .isEqualTo(0L);
          assertThat(settled.get("deltaVectorsCount"))
              .as("Delta buffer should be empty after inactivity rebuild")
              .isEqualTo(0L);
        });
  }

  // Issue #3737: each new mutation resets the inactivity timer so the rebuild only fires after a sustained quiet period.
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

    // After the last mutation the inactivity timer (re)starts once more; when it fires it rebuilds the graph
    // and resets the counter to 0. Poll instead of sleeping a fixed interval: the timer fire plus the rebuild
    // can take noticeably longer than the raw timeout on a loaded CI runner.
    Awaitility.await("inactivity rebuild flushes the mutation counter after the quiet period")
        .atMost(REBUILD_SETTLE_TIMEOUT)
        .pollInterval(Duration.ofMillis(50))
        .untilAsserted(() -> assertThat(lsmIndex.getStats().get("mutationsSinceRebuild"))
            .as("Mutation counter should be reset after inactivity rebuild")
            .isEqualTo(0L));
  }

  // Issue #3737: setting the inactivity timeout to 0 disables the timer (mutations stay pending indefinitely).
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

  // Issue #3868: concurrent async rebuilds across multiple vector indexes are serialized via REBUILD_SEMAPHORE to avoid OOM kills.
  @Test
  void concurrentAsyncRebuildsShouldBeSerialized() throws Exception {
    // Use a very low threshold so rebuilds trigger quickly
    final int threshold = 5;
    final int oomEmbeddingDim = 16;
    final int vectorsPerIndex = 1100; // > ASYNC_REBUILD_MIN_GRAPH_SIZE (1000)
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, threshold);
    // This test pins the absolute trigger threshold, so disable the graph-size scaling added for issue #5391.
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_REBUILD_GRAPH_RATIO, 0f);
    // Disable inactivity rebuild to control timing precisely
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, 0);

    // Create two types with separate vector indexes
    database.transaction(() -> {
      database.getSchema().createVertexType("EmbeddingA");
      database.getSchema().getType("EmbeddingA").createProperty("vector", Type.ARRAY_OF_FLOATS);
      database.command("sql", """
          CREATE INDEX ON EmbeddingA (vector) LSM_VECTOR
          METADATA {"dimensions": %d, "similarity": "EUCLIDEAN"}""".formatted(oomEmbeddingDim));

      database.getSchema().createVertexType("EmbeddingB");
      database.getSchema().getType("EmbeddingB").createProperty("vector", Type.ARRAY_OF_FLOATS);
      database.command("sql", """
          CREATE INDEX ON EmbeddingB (vector) LSM_VECTOR
          METADATA {"dimensions": %d, "similarity": "EUCLIDEAN"}""".formatted(oomEmbeddingDim));
    });

    final Random random = new Random(42);

    // Populate both indexes above ASYNC_REBUILD_MIN_GRAPH_SIZE
    database.transaction(() -> {
      for (int i = 0; i < vectorsPerIndex; i++) {
        database.command("sql", "INSERT INTO EmbeddingA SET vector = ?", (Object) generateUnnormalizedVector(random, oomEmbeddingDim));
        database.command("sql", "INSERT INTO EmbeddingB SET vector = ?", (Object) generateUnnormalizedVector(random, oomEmbeddingDim));
      }
    });

    // Trigger initial graph build via search
    final TypeIndex typeIndexA = (TypeIndex) database.getSchema().getIndexByName("EmbeddingA[vector]");
    final LSMVectorIndex indexA = (LSMVectorIndex) typeIndexA.getIndexesOnBuckets()[0];
    final TypeIndex typeIndexB = (TypeIndex) database.getSchema().getIndexByName("EmbeddingB[vector]");
    final LSMVectorIndex indexB = (LSMVectorIndex) typeIndexB.getIndexesOnBuckets()[0];

    final float[] queryVector = generateUnnormalizedVector(random, oomEmbeddingDim);
    indexA.findNeighborsFromVector(queryVector, 5);
    indexB.findNeighborsFromVector(queryVector, 5);

    // Wait for any initial rebuilds to finish
    Thread.sleep(2000);

    // Now insert enough mutations to trigger async rebuilds on BOTH indexes
    database.transaction(() -> {
      for (int i = 0; i < threshold + 1; i++) {
        database.command("sql", "INSERT INTO EmbeddingA SET vector = ?", (Object) generateUnnormalizedVector(random, oomEmbeddingDim));
        database.command("sql", "INSERT INTO EmbeddingB SET vector = ?", (Object) generateUnnormalizedVector(random, oomEmbeddingDim));
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

  // Issue #3868: the JVM-wide max-concurrent-rebuilds default is 1 (full serialization out of the box).
  @Test
  void rebuildSemaphoreDefaultIsOne() {
    // The default max concurrent rebuilds should be 1
    assertThat(GlobalConfiguration.VECTOR_INDEX_MAX_CONCURRENT_REBUILDS.getValueAsInteger()).isEqualTo(1);
  }

  // When two small-graph vector indexes share the single-permit rebuild semaphore, the loser of
  // tryAcquire() must re-arm its inactivity timer so it eventually rebuilds once the winner
  // releases the permit. Without the re-arm the loser is stuck with pending mutations
  // indefinitely (no further writes = nothing else to re-arm the timer).
  @Test
  @Tag("slow")
  void skippedInactivityRebuildShouldRetryUntilServed() {
    final int timeoutMs = 300;
    final int highThreshold = 10_000; // never reached via mutations

    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, highThreshold);
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, timeoutMs);

    // Two small-graph indexes on the same database (< ASYNC_REBUILD_MIN_GRAPH_SIZE = 1000 vectors each)
    database.transaction(() -> {
      database.getSchema().createVertexType("SmallA");
      database.getSchema().getType("SmallA").createProperty("vector", Type.ARRAY_OF_FLOATS);
      database.command("sql", """
          CREATE INDEX ON SmallA (vector) LSM_VECTOR
          METADATA {"dimensions": %d, "similarity": "EUCLIDEAN"}""".formatted(EMBEDDING_DIM));

      database.getSchema().createVertexType("SmallB");
      database.getSchema().getType("SmallB").createProperty("vector", Type.ARRAY_OF_FLOATS);
      database.command("sql", """
          CREATE INDEX ON SmallB (vector) LSM_VECTOR
          METADATA {"dimensions": %d, "similarity": "EUCLIDEAN"}""".formatted(EMBEDDING_DIM));
    });

    final Random random = new Random(42);

    // Insert into both indexes in one batch so their inactivity timers start at the same time
    database.transaction(() -> {
      for (int i = 0; i < 50; i++) {
        database.command("sql", "INSERT INTO SmallA SET vector = ?", (Object) generateRandomVector(random));
        database.command("sql", "INSERT INTO SmallB SET vector = ?", (Object) generateRandomVector(random));
      }
    });

    final TypeIndex typeIndexA = (TypeIndex) database.getSchema().getIndexByName("SmallA[vector]");
    final LSMVectorIndex indexA = (LSMVectorIndex) typeIndexA.getIndexesOnBuckets()[0];
    final TypeIndex typeIndexB = (TypeIndex) database.getSchema().getIndexByName("SmallB[vector]");
    final LSMVectorIndex indexB = (LSMVectorIndex) typeIndexB.getIndexesOnBuckets()[0];

    assertThat(indexA.getStats().get("mutationsSinceRebuild")).isGreaterThan(0L);
    assertThat(indexB.getStats().get("mutationsSinceRebuild")).isGreaterThan(0L);

    // The wait covers the first timer cycle, the retry cycle for the skipped index and two synchronous rebuilds.
    Awaitility.await("both small-graph indexes drain pending mutations after at least one skip cycle")
        .atMost(REBUILD_SETTLE_TIMEOUT)
        .pollInterval(Duration.ofMillis(50))
        .untilAsserted(() -> {
          assertThat(indexA.getStats().get("mutationsSinceRebuild")).isEqualTo(0L);
          assertThat(indexB.getStats().get("mutationsSinceRebuild")).isEqualTo(0L);
        });
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

  private static float[] generateUnnormalizedVector(final Random random, final int dim) {
    final float[] vector = new float[dim];
    for (int i = 0; i < dim; i++)
      vector[i] = random.nextFloat();
    return vector;
  }
}
