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

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * LSM vector index rebuild semantics: threshold-triggered, inactivity-triggered, async retrigger, metadata preservation, and concurrent rebuild serialization.
 */
class LSMVectorIndexRebuildTest extends TestHelper {

  private static final int EMBEDDING_DIM = 32;
  // Must be >= ASYNC_REBUILD_MIN_GRAPH_SIZE (1000) so the async path is used
  private static final int LARGE_INDEX_VECTORS = 1100;

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

    // Wait for the async rebuild to complete
    Thread.sleep(5000);

    // After async rebuild, mutation counter should be reset or low.
    // With incremental inserts via live builder, counter may reflect inserts that
    // went directly to graph (not via delta/rebuild path).
    stats = lsmIndex.getStats();
    assertThat(stats.get("mutationsSinceRebuild"))
        .as("Mutation counter should be reset or low after async rebuild completes")
        .isLessThanOrEqualTo((long) lowThreshold);
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
        .atMost(Duration.ofSeconds(30))
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
        .atMost(Duration.ofSeconds(60))
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
        .atMost(Duration.ofMillis(timeoutMs * 20L))
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

    // 25x = first timer cycle + retry cycle for the skipped index + two synchronous rebuilds
    // (well under 1s each at 50 vectors) + slack for GC pauses on loaded CI runners.
    Awaitility.await("both small-graph indexes drain pending mutations after at least one skip cycle")
        .atMost(Duration.ofMillis(timeoutMs * 25L))
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
