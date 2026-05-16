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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.engine.MutablePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.log.LogManager;
import com.arcadedb.log.Logger;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import com.arcadedb.utility.Pair;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * LSM vector index recovery, reopen, restore, and tombstone parsing regression tests.
 */
class LSMVectorIndexRecoveryTest extends TestHelper {

  private static final int EMBEDDING_DIM = 32;

  // Issue #3715: vectorNeighbors must not NPE when the HNSW graph still references ordinals of deleted vectors.
  @Test
  void vectorSearchAfterDeleteShouldNotThrowNPE() {
    final int dimensions = 64;
    final int totalVectors = 1500;
    final int vectorsToDelete = 500;

    // Set very high mutation threshold so the graph is NOT rebuilt after deletions.
    // This forces the search to use the stale graph with edges to deleted ordinals.
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, 100_000);

    // Phase 1: Create schema with vector index
    database.transaction(() -> {
      final VertexType type = database.getSchema().createVertexType("VectorDoc");
      type.createProperty("name", Type.STRING);
      type.createProperty("embedding", Type.ARRAY_OF_FLOATS);

      database.getSchema().buildTypeIndex("VectorDoc", new String[] { "embedding" })
          .withLSMVectorType()
          .withDimensions(dimensions)
          .withSimilarity("COSINE")
          .withMaxConnections(16)
          .withBeamWidth(100)
          .create();
    });

    // Phase 2: Insert enough vectors to exceed ASYNC_REBUILD_MIN_GRAPH_SIZE (1000)
    final List<RID> insertedRIDs = new ArrayList<>();
    database.transaction(() -> {
      for (int i = 0; i < totalVectors; i++) {
        final var vertex = database.newVertex("VectorDoc");
        vertex.set("name", "doc" + i);
        final float[] vector = new float[dimensions];
        for (int j = 0; j < dimensions; j++)
          vector[j] = (float) Math.random();
        vertex.set("embedding", vector);
        vertex.save();
        insertedRIDs.add(vertex.getIdentity());
      }
    });

    // Phase 3: Force graph build by doing a search first
    database.transaction(() -> {
      final float[] queryVector = new float[dimensions];
      Arrays.fill(queryVector, 0.5f);
      final ResultSet rs = database.query("sql",
          "SELECT vectorNeighbors('VectorDoc[embedding]', ?, 10) AS neighbors",
          queryVector);
      assertThat(rs.hasNext()).isTrue();
      rs.close();
    });

    // Phase 4: Delete vectors (below the mutation threshold so NO rebuild is triggered)
    database.transaction(() -> {
      for (int i = 0; i < vectorsToDelete; i++)
        insertedRIDs.get(i).asDocument().delete();
    });

    // Phase 5: Search multiple times with different query vectors.
    database.transaction(() -> {
      for (int s = 0; s < 20; s++) {
        final float[] queryVector = new float[dimensions];
        for (int j = 0; j < dimensions; j++)
          queryVector[j] = (float) Math.random();
        final ResultSet rs = database.query("sql",
            "SELECT vectorNeighbors('VectorDoc[embedding]', ?, 10) AS neighbors",
            queryVector);
        assertThat(rs.hasNext()).isTrue();
        final Result result = rs.next();
        final List<?> neighbors = result.getProperty("neighbors");
        assertThat(neighbors).isNotNull();
        assertThat(neighbors.size()).isGreaterThan(0);
        assertThat(neighbors.size()).isLessThanOrEqualTo(10);
        rs.close();
      }
    });
  }

  // Issue #3715: vectorNeighbors must not NPE when stale-graph deletions persist across a database reopen.
  @Test
  void vectorSearchAfterDeleteWithReopenShouldNotThrowNPE() {
    final int dimensions = 64;
    final int totalVectors = 1500;
    final int vectorsToDelete = 500;

    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, 100_000);

    database.transaction(() -> {
      final VertexType type = database.getSchema().createVertexType("VectorDoc2");
      type.createProperty("name", Type.STRING);
      type.createProperty("embedding", Type.ARRAY_OF_FLOATS);

      database.getSchema().buildTypeIndex("VectorDoc2", new String[] { "embedding" })
          .withLSMVectorType()
          .withDimensions(dimensions)
          .withSimilarity("COSINE")
          .withMaxConnections(16)
          .withBeamWidth(100)
          .create();
    });

    final List<RID> insertedRIDs = new ArrayList<>();
    database.transaction(() -> {
      for (int i = 0; i < totalVectors; i++) {
        final var vertex = database.newVertex("VectorDoc2");
        vertex.set("name", "doc" + i);
        final float[] vector = new float[dimensions];
        for (int j = 0; j < dimensions; j++)
          vector[j] = (float) Math.random();
        vertex.set("embedding", vector);
        vertex.save();
        insertedRIDs.add(vertex.getIdentity());
      }
    });

    // Force graph build
    database.transaction(() -> {
      final float[] queryVector = new float[dimensions];
      Arrays.fill(queryVector, 0.5f);
      final ResultSet rs = database.query("sql",
          "SELECT vectorNeighbors('VectorDoc2[embedding]', ?, 10) AS neighbors",
          queryVector);
      assertThat(rs.hasNext()).isTrue();
      rs.close();
    });

    // Delete vectors
    database.transaction(() -> {
      for (int i = 0; i < vectorsToDelete; i++)
        insertedRIDs.get(i).asDocument().delete();
    });

    reopenDatabase();

    database.transaction(() -> {
      final float[] queryVector = new float[dimensions];
      Arrays.fill(queryVector, 0.5f);
      final ResultSet rs = database.query("sql",
          "SELECT vectorNeighbors('VectorDoc2[embedding]', ?, 10) AS neighbors",
          queryVector);
      assertThat(rs.hasNext()).isTrue();
      final Result result = rs.next();
      final List<?> neighbors = result.getProperty("neighbors");
      assertThat(neighbors).isNotNull();
      assertThat(neighbors.size()).isGreaterThan(0);
      rs.close();
    });
  }

  // Issue #3722: page parser must handle old-format tombstones written without the quantization type byte.
  @Test
  void parserShouldHandleOldFormatTombstoneWithoutQuantTypeByte() {
    final int dimensions = 64;
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, 100_000);
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, 0);

    // Create schema with vector index
    database.transaction(() -> {
      final VertexType type = database.getSchema().createVertexType("Vec");
      type.createProperty("name", Type.STRING);
      type.createProperty("vector", Type.ARRAY_OF_FLOATS);

      database.getSchema().buildTypeIndex("Vec", new String[] { "vector" })
          .withLSMVectorType()
          .withDimensions(dimensions)
          .withSimilarity("COSINE")
          .withMaxConnections(16)
          .withBeamWidth(100)
          .create();
    });

    // Insert 66 initial vectors, then delete one, then insert 480 more
    final RID[] rids = new RID[546];
    database.transaction(() -> {
      for (int i = 0; i < 66; i++) {
        final MutableVertex v = database.newVertex("Vec");
        v.set("name", "v" + i);
        v.set("vector", createDeterministicVector(i, dimensions));
        v.save();
        rids[i] = v.getIdentity();
      }
    });

    // Force graph build
    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("Vec[vector]");
    final LSMVectorIndex index = (LSMVectorIndex) typeIndex.getSubIndexes().iterator().next();
    index.buildVectorGraphNow();

    // Delete one vector to create a tombstone
    database.transaction(() -> rids[30].asVertex().delete());

    // Insert 480 more vectors
    database.transaction(() -> {
      for (int i = 66; i < 546; i++) {
        final MutableVertex v = database.newVertex("Vec");
        v.set("name", "v" + i);
        v.set("vector", createDeterministicVector(i, dimensions));
        v.save();
        rids[i] = v.getIdentity();
      }
    });

    final int fileId = index.getFileId();
    final int pageSize = index.getPageSize();
    final DatabaseInternal db = (DatabaseInternal) database;

    database.transaction(() -> {
      try {
        final MutablePage page = db.getTransaction()
            .getPageToModify(new PageId(db, fileId, 0), pageSize, false);

        final int numEntries = page.readInt(LSMVectorIndex.OFFSET_NUM_ENTRIES);
        final int offsetFreeContent = page.readInt(LSMVectorIndex.OFFSET_FREE_CONTENT);

        int offset = LSMVectorIndex.HEADER_BASE_SIZE;
        int tombstoneQuantTypeOffset = -1;
        for (int i = 0; i < numEntries; i++) {
          final long[] vid = page.readNumberAndSize(offset);
          offset += (int) vid[1];
          final long[] bid = page.readNumberAndSize(offset);
          offset += (int) bid[1];
          final long[] pos = page.readNumberAndSize(offset);
          offset += (int) pos[1];
          final boolean deleted = page.readByte(offset) == 1;
          offset += 1;

          if (deleted) {
            tombstoneQuantTypeOffset = offset;
          }

          final byte quantType = page.readByte(offset);
          offset += 1;
          if (quantType == 1) { // INT8
            final int vecLen = page.readInt(offset);
            offset += 4 + vecLen + 8;
          } else if (quantType == 2) { // BINARY
            final int origLen = page.readInt(offset);
            offset += 4 + ((origLen + 7) / 8) + 4;
          }
        }

        assertThat(tombstoneQuantTypeOffset).as("Should have found a tombstone entry").isGreaterThan(0);

        final int shiftStart = tombstoneQuantTypeOffset + 1;
        final int shiftEnd = offsetFreeContent;
        final int shiftLength = shiftEnd - shiftStart;

        if (shiftLength > 0) {
          final byte[] dataAfter = new byte[shiftLength];
          for (int i = 0; i < shiftLength; i++)
            dataAfter[i] = page.readByte(shiftStart + i);

          for (int i = 0; i < shiftLength; i++)
            page.writeByte(tombstoneQuantTypeOffset + i, dataAfter[i]);

          page.writeInt(LSMVectorIndex.OFFSET_FREE_CONTENT, offsetFreeContent - 1);
        }
      } catch (final Exception e) {
        throw new RuntimeException("Failed to patch page", e);
      }
    });

    reopenDatabase();
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, 100_000);
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, 0);

    database.transaction(() -> {
      final TypeIndex ti = (TypeIndex) database.getSchema().getIndexByName("Vec[vector]");
      final LSMVectorIndex idx = (LSMVectorIndex) ti.getSubIndexes().iterator().next();

      final List<LSMVectorIndexPageParser.VectorEntry> entries =
          LSMVectorIndexPageParser.parseAllEntries(
              (DatabaseInternal) database, idx.getFileId(), idx.getTotalPages(), idx.getPageSize(), false);

      int regularCount = 0;
      int tombstoneCount = 0;
      for (final LSMVectorIndexPageParser.VectorEntry entry : entries) {
        if (entry.deleted)
          tombstoneCount++;
        else
          regularCount++;
      }

      assertThat(tombstoneCount)
          .as("Should have 1 tombstone entry (old format without quantType byte)")
          .isEqualTo(1);

      assertThat(regularCount)
          .as("""
              After fix: most regular entries should be parsed despite old-format tombstone. \
              Before fix, only ~65 entries would be found due to page corruption.""")
          .isGreaterThan(500);

      final float[] queryVector = new float[dimensions];
      Arrays.fill(queryVector, 0.5f);
      final ResultSet rs = database.query("sql",
          "SELECT vectorNeighbors('Vec[vector]', ?, ?) AS neighbors",
          queryVector, 500);

      assertThat(rs.hasNext()).isTrue();
      final Result result = rs.next();
      final List<?> neighbors = result.getProperty("neighbors");
      assertThat(neighbors).isNotNull();

      assertThat(neighbors.size())
          .as("""
              After fix: should find close to 500 results. \
              Before fix: would only find ~65 (entries before the old-format tombstone).""")
          .isGreaterThan(400);

      rs.close();
    });
  }

  // Issue #3722: fallback document scan during vector graph rebuild must be scoped to the index's associated bucket.
  @Test
  void fallbackScanShouldBeScopedToAssociatedBucket() {
    final int dimensions = 32;
    final int vectorsPerBucket = 50;

    database.transaction(() -> {
      final VertexType type = database.getSchema().createVertexType("MultiBucketVector");
      type.createProperty("name", Type.STRING);
      type.createProperty("vector", Type.ARRAY_OF_FLOATS);

      final Bucket extraBucket = database.getSchema().createBucket("MultiBucketVector_extra");
      type.addBucket(extraBucket);

      database.getSchema().buildTypeIndex("MultiBucketVector", new String[] { "vector" })
          .withLSMVectorType()
          .withDimensions(dimensions)
          .withSimilarity("COSINE")
          .withMaxConnections(16)
          .withBeamWidth(100)
          .create();
    });

    database.transaction(() -> {
      final List<Bucket> buckets = database.getSchema().getType("MultiBucketVector").getBuckets(false);
      assertThat(buckets.size()).isGreaterThanOrEqualTo(2);

      for (int i = 0; i < vectorsPerBucket * 2; i++) {
        final var vertex = database.newVertex("MultiBucketVector");
        vertex.set("name", "vec" + i);
        vertex.set("vector", createDeterministicVector(i, dimensions));
        vertex.save();
      }
    });

    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("MultiBucketVector[vector]");
    assertThat(typeIndex.getSubIndexes().size()).isGreaterThanOrEqualTo(2);

    for (final var subIndex : typeIndex.getSubIndexes()) {
      final LSMVectorIndex vectorIndex = (LSMVectorIndex) subIndex;
      vectorIndex.buildVectorGraphNow();

      assertThat(vectorIndex.getAssociatedBucketId()).isNotEqualTo(-1);

      final Map<String, Long> stats = vectorIndex.getStats();
      final long totalVectors = stats.get("totalVectors");

      final Bucket associatedBucket = database.getSchema().getBucketById(vectorIndex.getAssociatedBucketId());
      final long bucketDocCount = database.countBucket(associatedBucket.getName());
      assertThat(totalVectors)
          .as("Sub-index for bucket '%s' should have vectors only from that bucket", associatedBucket.getName())
          .isLessThanOrEqualTo(bucketDocCount);
    }

    reopenDatabase();

    database.transaction(() -> {
      final float[] queryVector = new float[dimensions];
      Arrays.fill(queryVector, 0.5f);
      final ResultSet rs = database.query("sql",
          "SELECT vectorNeighbors('MultiBucketVector[vector]', ?, ?) AS neighbors",
          queryVector, 10);

      assertThat(rs.hasNext()).isTrue();
      final Result result = rs.next();
      final List<?> neighbors = result.getProperty("neighbors");
      assertThat(neighbors).isNotNull();
      assertThat(neighbors.size()).isGreaterThan(0);
      rs.close();
    });
  }

  // Issue #3722: graph rebuild must include all vectors known to the in-memory vectorIndex even when page parsing returns fewer.
  @Test
  void graphRebuildShouldIncludeAllVectorsFromInMemoryIndex() {
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, 100_000);
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, -1);

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

    final int count = 500;
    database.transaction(() -> {
      for (int i = 0; i < count; i++)
        database.command("sql", "INSERT INTO Embedding SET vector = ?", (Object) generateRandomVector(random));
    });

    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("Embedding[vector]");
    final LSMVectorIndex lsmIndex = (LSMVectorIndex) typeIndex.getIndexesOnBuckets()[0];
    lsmIndex.buildVectorGraphNow();

    final Map<String, Long> stats = lsmIndex.getStats();
    assertThat(stats.get("graphNodeCount"))
        .as("Graph should have approximately %d nodes", count)
        .isGreaterThanOrEqualTo((long) count - 5);

    final float[] queryVector = generateRandomVector(random);
    final int k = 200;
    final List<Result> results = executeVectorSearch("Embedding", queryVector, k);
    assertThat(results.size())
        .as("Search for %d neighbors from %d vectors should return %d results", k, count, k)
        .isEqualTo(k);

    lsmIndex.buildVectorGraphNow();

    final List<Result> results2 = executeVectorSearch("Embedding", queryVector, k);
    assertThat(results2.size())
        .as("After second rebuild, search should still return %d results", k)
        .isEqualTo(k);
  }

  // Issue #3722: graph rebuild after multiple incremental batches must include all vectors.
  @Test
  void graphRebuildAfterMultipleBatchesShouldIncludeAllVectors() {
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, 100_000);
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, -1);

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
    int totalInserted = 0;

    for (int batch = 0; batch < 5; batch++) {
      final int batchSize = 100 + batch * 50;
      database.transaction(() -> {
        for (int i = 0; i < batchSize; i++)
          database.command("sql", "INSERT INTO Embedding SET vector = ?", (Object) generateRandomVector(random));
      });
      totalInserted += batchSize;

      final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("Embedding[vector]");
      final LSMVectorIndex lsmIndex = (LSMVectorIndex) typeIndex.getIndexesOnBuckets()[0];
      lsmIndex.buildVectorGraphNow();

      final Map<String, Long> stats = lsmIndex.getStats();
      assertThat(stats.get("graphNodeCount"))
          .as("After batch %d (%d total vectors), graph should have approximately %d nodes",
              batch, totalInserted, totalInserted)
          .isGreaterThanOrEqualTo((long) totalInserted - 10);

      final float[] queryVector = generateRandomVector(new Random(99));
      final int k = Math.min(totalInserted - 10, 300);
      final List<Result> results = executeVectorSearch("Embedding", queryVector, k);
      assertThat(results.size())
          .as("After batch %d (%d total), search for %d should return %d", batch, totalInserted, k, k)
          .isEqualTo(k);
    }
  }

  // Issue #3722: Java-API findNeighborsFromVector must return correct count after rebuild over a large dataset.
  @Test
  void javaApiSearchShouldReturnCorrectCountAfterRebuild() {
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, 100_000);
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, -1);

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

    final int initialCount = 100;
    database.transaction(() -> {
      for (int i = 0; i < initialCount; i++)
        database.command("sql", "INSERT INTO Embedding SET vector = ?", (Object) generateRandomVector(random));
    });

    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("Embedding[vector]");
    final LSMVectorIndex lsmIndex = (LSMVectorIndex) typeIndex.getIndexesOnBuckets()[0];
    lsmIndex.buildVectorGraphNow();

    final int additionalCount = 1000;
    database.transaction(() -> {
      for (int i = 0; i < additionalCount; i++)
        database.command("sql", "INSERT INTO Embedding SET vector = ?", (Object) generateRandomVector(random));
    });

    lsmIndex.buildVectorGraphNow();

    final int totalCount = initialCount + additionalCount;
    final float[] queryVector = generateRandomVector(new Random(99));

    final int highK = 500;
    final List<Pair<RID, Float>> javaResults = lsmIndex.findNeighborsFromVector(queryVector, highK);
    assertThat(javaResults.size())
        .as("Java API: search for %d from %d total should return %d", highK, totalCount, highK)
        .isEqualTo(highK);

    final List<Result> sqlResults = executeVectorSearch("Embedding", queryVector, highK);
    assertThat(sqlResults.size())
        .as("SQL: search for %d from %d total should return %d", highK, totalCount, highK)
        .isEqualTo(highK);
  }

  // Issue #3722: vectorNeighbors must return correct count after incremental insert followed by inactivity-triggered rebuild.
  @Test
  void vectorNeighborsShouldReturnCorrectCountAfterIncrementalInsertAndRebuild() throws Exception {
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, 100_000);
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, 2_000);

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

    final int initialCount = 100;
    database.transaction(() -> {
      for (int i = 0; i < initialCount; i++)
        database.command("sql", "INSERT INTO Embedding SET vector = ?", (Object) generateRandomVector(random));
    });

    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("Embedding[vector]");
    final LSMVectorIndex lsmIndex = (LSMVectorIndex) typeIndex.getIndexesOnBuckets()[0];
    lsmIndex.buildVectorGraphNow();

    final int k = 80;
    final float[] queryVector = generateRandomVector(random);
    final List<Result> initialResults = executeVectorSearch("Embedding", queryVector, k);
    assertThat(initialResults.size())
        .as("Initial search should return %d results from %d vectors", k, initialCount)
        .isEqualTo(k);

    final int additionalCount = 200;
    database.transaction(() -> {
      for (int i = 0; i < additionalCount; i++)
        database.command("sql", "INSERT INTO Embedding SET vector = ?", (Object) generateRandomVector(random));
    });

    final int totalCount = initialCount + additionalCount;

    Thread.sleep(6_000);

    assertThat(lsmIndex.getStats().get("deltaVectorsCount"))
        .as("Delta buffer should be empty after inactivity rebuild")
        .isEqualTo(0L);

    final List<Result> afterRebuildResults = executeVectorSearch("Embedding", queryVector, k);
    assertThat(afterRebuildResults.size())
        .as("After incremental insert + rebuild, search should return %d results from %d total vectors", k, totalCount)
        .isEqualTo(k);

    final int largerK = 150;
    final List<Result> largerResults = executeVectorSearch("Embedding", queryVector, largerK);
    assertThat(largerResults.size())
        .as("Search for %d neighbors from %d total vectors should return %d results", largerK, totalCount, largerK)
        .isEqualTo(largerK);
  }

  // Issue #3722: vectorNeighbors must return correct count after manual rebuild following incremental inserts.
  @Test
  void vectorNeighborsShouldReturnCorrectCountAfterManualRebuild() {
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, 100_000);
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, -1);

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

    final int initialCount = 100;
    database.transaction(() -> {
      for (int i = 0; i < initialCount; i++)
        database.command("sql", "INSERT INTO Embedding SET vector = ?", (Object) generateRandomVector(random));
    });

    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("Embedding[vector]");
    final LSMVectorIndex lsmIndex = (LSMVectorIndex) typeIndex.getIndexesOnBuckets()[0];
    lsmIndex.buildVectorGraphNow();

    final float[] queryVector = generateRandomVector(random);

    final int k = 80;
    assertThat(executeVectorSearch("Embedding", queryVector, k).size()).isEqualTo(k);

    final int additionalCount = 300;
    database.transaction(() -> {
      for (int i = 0; i < additionalCount; i++)
        database.command("sql", "INSERT INTO Embedding SET vector = ?", (Object) generateRandomVector(random));
    });

    lsmIndex.buildVectorGraphNow();

    final int totalCount = initialCount + additionalCount;
    final List<Result> results = executeVectorSearch("Embedding", queryVector, k);
    assertThat(results.size())
        .as("After manual rebuild, search should return %d results from %d total vectors", k, totalCount)
        .isEqualTo(k);

    final int largerK = 250;
    final List<Result> largerResults = executeVectorSearch("Embedding", queryVector, largerK);
    assertThat(largerResults.size())
        .as("Search for %d neighbors from %d total vectors should return %d", largerK, totalCount, largerK)
        .isEqualTo(largerK);
  }

  // Issue #3722: vectorNeighbors must return correct count after multiple incremental batches with rebuilds.
  @Test
  void vectorNeighborsShouldReturnCorrectCountAfterMultipleIncrementalBatches() {
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, 100_000);
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, -1);

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
    int totalInserted = 0;

    final int initialCount = 50;
    database.transaction(() -> {
      for (int i = 0; i < initialCount; i++)
        database.command("sql", "INSERT INTO Embedding SET vector = ?", (Object) generateRandomVector(random));
    });
    totalInserted += initialCount;

    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("Embedding[vector]");
    final LSMVectorIndex lsmIndex = (LSMVectorIndex) typeIndex.getIndexesOnBuckets()[0];
    lsmIndex.buildVectorGraphNow();

    final float[] queryVector = generateRandomVector(random);

    for (int batch = 0; batch < 5; batch++) {
      final int batchSize = 100;
      database.transaction(() -> {
        for (int i = 0; i < batchSize; i++)
          database.command("sql", "INSERT INTO Embedding SET vector = ?", (Object) generateRandomVector(random));
      });
      totalInserted += batchSize;

      lsmIndex.buildVectorGraphNow();

      final int k = Math.min(totalInserted, 200);
      final List<Result> results = executeVectorSearch("Embedding", queryVector, k);
      assertThat(results.size())
          .as("After batch %d (%d total vectors), search for %d neighbors should return %d results",
              batch, totalInserted, k, k)
          .isEqualTo(k);
    }
  }

  // Issue #3722: Java-API search must return correct count after rebuild following large incremental insert.
  @Test
  void searchViaJavaAPIShouldReturnCorrectCountAfterRebuild() {
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, 100_000);
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, -1);

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

    final int initialCount = 200;
    database.transaction(() -> {
      for (int i = 0; i < initialCount; i++)
        database.command("sql", "INSERT INTO Embedding SET vector = ?", (Object) generateRandomVector(random));
    });

    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("Embedding[vector]");
    final LSMVectorIndex lsmIndex = (LSMVectorIndex) typeIndex.getIndexesOnBuckets()[0];
    lsmIndex.buildVectorGraphNow();

    final float[] queryVector = generateRandomVector(random);

    final int k = 150;
    List<Pair<RID, Float>> results = lsmIndex.findNeighborsFromVector(queryVector, k);
    assertThat(results.size())
        .as("Java API: Initial search should return %d results", k)
        .isEqualTo(k);

    final int additionalCount = 800;
    database.transaction(() -> {
      for (int i = 0; i < additionalCount; i++)
        database.command("sql", "INSERT INTO Embedding SET vector = ?", (Object) generateRandomVector(random));
    });

    lsmIndex.buildVectorGraphNow();

    final int totalCount = initialCount + additionalCount;
    final Map<String, Long> stats = lsmIndex.getStats();
    assertThat(stats.get("graphNodeCount"))
        .as("Graph should have %d nodes after rebuild", totalCount)
        .isGreaterThanOrEqualTo((long) totalCount - 10);

    results = lsmIndex.findNeighborsFromVector(queryVector, k);
    assertThat(results.size())
        .as("Java API: After rebuild (%d total vectors), search for %d should return %d", totalCount, k, k)
        .isEqualTo(k);

    final int highK = 500;
    results = lsmIndex.findNeighborsFromVector(queryVector, highK);
    assertThat(results.size())
        .as("Java API: Search for %d from %d total vectors", highK, totalCount)
        .isEqualTo(highK);
  }

  // Issue #3722: vectorNeighbors must return correct count after database reopen followed by incremental insert and rebuild.
  @Test
  void vectorSearchShouldReturnCorrectCountAfterReopenAndIncrementalInsert() {
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, 100_000);
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, -1);

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

    TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("ChunkEmbedding[vector]");
    LSMVectorIndex lsmIndex = (LSMVectorIndex) typeIndex.getIndexesOnBuckets()[0];
    lsmIndex.buildVectorGraphNow();

    final float[] queryVector = generateRandomVector(random);
    assertThat(executeVectorSearch("ChunkEmbedding", queryVector, 30).size()).isEqualTo(30);

    reopenDatabase();

    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, 100_000);
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, -1);

    final int additionalCount = 500;
    database.transaction(() -> {
      for (int i = 0; i < additionalCount; i++)
        database.command("sql", "INSERT INTO ChunkEmbedding SET vector = ?", (Object) generateRandomVector(random));
    });

    final int totalCount = initialCount + additionalCount;

    typeIndex = (TypeIndex) database.getSchema().getIndexByName("ChunkEmbedding[vector]");
    lsmIndex = (LSMVectorIndex) typeIndex.getIndexesOnBuckets()[0];
    lsmIndex.buildVectorGraphNow();

    final Map<String, Long> stats = lsmIndex.getStats();
    assertThat(stats.get("graphNodeCount"))
        .as("Graph should have approximately %d nodes after rebuild", totalCount)
        .isGreaterThanOrEqualTo((long) totalCount - 10);

    final int k = 100;
    final List<Result> results = executeVectorSearch("ChunkEmbedding", queryVector, k);
    assertThat(results.size())
        .as("Search for %d neighbors from %d total vectors should return %d results", k, totalCount, k)
        .isEqualTo(k);
  }

  // Issue #3722: vectorNeighbors must return correct count across multiple reopen-and-insert cycles.
  @Test
  void vectorSearchShouldReturnCorrectCountAfterMultipleReopenCycles() {
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, 100_000);
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, -1);

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

    final int batch1 = 50;
    database.transaction(() -> {
      for (int i = 0; i < batch1; i++)
        database.command("sql", "INSERT INTO ChunkEmbedding SET vector = ?", (Object) generateRandomVector(random));
    });
    totalInserted += batch1;

    TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("ChunkEmbedding[vector]");
    LSMVectorIndex lsmIndex = (LSMVectorIndex) typeIndex.getIndexesOnBuckets()[0];
    lsmIndex.buildVectorGraphNow();

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

    int k = Math.min(totalInserted - 10, 200);
    List<Result> results = executeVectorSearch("ChunkEmbedding", queryVector, k);
    assertThat(results.size())
        .as("After cycle 2 (%d total vectors), search for %d should return %d", totalInserted, k, k)
        .isEqualTo(k);

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

    k = Math.min(totalInserted - 10, 400);
    results = executeVectorSearch("ChunkEmbedding", queryVector, k);
    assertThat(results.size())
        .as("After cycle 3 (%d total vectors), search for %d should return %d", totalInserted, k, k)
        .isEqualTo(k);
  }

  // Issue #3722: vectorNeighbors must return correct count when inactivity rebuild fires after reopen and incremental insert.
  @Test
  void vectorSearchShouldReturnCorrectCountWithInactivityRebuildAfterReopen() throws Exception {
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, 100_000);
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, -1);

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

    TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("ChunkEmbedding[vector]");
    LSMVectorIndex lsmIndex = (LSMVectorIndex) typeIndex.getIndexesOnBuckets()[0];
    lsmIndex.buildVectorGraphNow();

    reopenDatabase();

    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, 100_000);
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, 2_000);

    final int additionalCount = 500;
    database.transaction(() -> {
      for (int i = 0; i < additionalCount; i++)
        database.command("sql", "INSERT INTO ChunkEmbedding SET vector = ?", (Object) generateRandomVector(random));
    });

    Thread.sleep(6_000);

    typeIndex = (TypeIndex) database.getSchema().getIndexByName("ChunkEmbedding[vector]");
    lsmIndex = (LSMVectorIndex) typeIndex.getIndexesOnBuckets()[0];

    assertThat(lsmIndex.getStats().get("deltaVectorsCount"))
        .as("Delta buffer should be empty after inactivity rebuild")
        .isEqualTo(0L);

    final int totalCount = initialCount + additionalCount;
    final float[] queryVector = generateRandomVector(new Random(99));
    final int k = 100;
    final List<Result> results = executeVectorSearch("ChunkEmbedding", queryVector, k);
    assertThat(results.size())
        .as("After reopen + incremental insert + inactivity rebuild, should return %d results from %d total", k, totalCount)
        .isEqualTo(k);
  }

  // Issue #3722: vectorNeighbors must include all vectors after reopen when graph was built with only the initial batch.
  @Test
  void vectorNeighborsShouldReturnAllVectorsAfterReopen() {
    final int dimensions = 64;
    final int initialVectors = 100;
    final int additionalVectors = 400;
    final int totalVectors = initialVectors + additionalVectors;

    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, 100_000);

    database.transaction(() -> {
      final VertexType type = database.getSchema().createVertexType("ImageEmbedding");
      type.createProperty("name", Type.STRING);
      type.createProperty("vector", Type.ARRAY_OF_FLOATS);

      database.getSchema().buildTypeIndex("ImageEmbedding", new String[] { "vector" })
          .withLSMVectorType()
          .withDimensions(dimensions)
          .withSimilarity("COSINE")
          .withMaxConnections(16)
          .withBeamWidth(100)
          .create();
    });

    database.transaction(() -> {
      for (int i = 0; i < initialVectors; i++) {
        final var vertex = database.newVertex("ImageEmbedding");
        vertex.set("name", "img" + i);
        vertex.set("vector", createDeterministicVector(i, dimensions));
        vertex.save();
      }
    });

    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("ImageEmbedding[vector]");
    final LSMVectorIndex index = (LSMVectorIndex) typeIndex.getSubIndexes().iterator().next();
    index.buildVectorGraphNow();

    final Map<String, Long> stats = index.getStats();
    assertThat(stats.get("graphNodeCount"))
        .as("Graph should be built with initial %d vectors", initialVectors)
        .isGreaterThanOrEqualTo(initialVectors);

    database.transaction(() -> {
      for (int i = initialVectors; i < totalVectors; i++) {
        final var vertex = database.newVertex("ImageEmbedding");
        vertex.set("name", "img" + i);
        vertex.set("vector", createDeterministicVector(i, dimensions));
        vertex.save();
      }
    });

    reopenDatabase();
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, 100_000);

    database.transaction(() -> {
      final TypeIndex ti = (TypeIndex) database.getSchema().getIndexByName("ImageEmbedding[vector]");
      final LSMVectorIndex idx = (LSMVectorIndex) ti.getSubIndexes().iterator().next();
      final Map<String, Long> s = idx.getStats();
      final long vectorCount = s.get("totalVectors");
      final long deltaCount = s.get("deltaVectorsCount");

      assertThat(vectorCount)
          .as("All %d vectors should be loaded from pages after reopen", totalVectors)
          .isEqualTo(totalVectors);

      assertThat(deltaCount).as("deltaVectors should be empty after reopen").isEqualTo(0);

      final float[] queryVector = new float[dimensions];
      Arrays.fill(queryVector, 0.5f);
      final int k = totalVectors;
      final ResultSet rs = database.query("sql",
          "SELECT vectorNeighbors('ImageEmbedding[vector]', ?, ?) AS neighbors",
          queryVector, k);

      assertThat(rs.hasNext()).isTrue();
      final Result result = rs.next();
      final List<?> neighbors = result.getProperty("neighbors");
      assertThat(neighbors).isNotNull();

      assertThat(neighbors.size())
          .as("""
              vectorNeighbors should return vectors from both initial and additional batches, \
              not just the %d vectors in the stale persisted graph""", initialVectors)
          .isGreaterThan(initialVectors + additionalVectors / 2);

      rs.close();
    });
  }

  // Issue #3722: vectorNeighbors must return ~all vectors after reopen with auto-rebuild during incremental insertion (user restore scenario).
  @Test
  void vectorNeighborsShouldReturnAllVectorsAfterReopenWithAutoRebuild() {
    final int dimensions = 128;
    final int totalVectors = 546;
    final int queryLimit = 500;

    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, 50);
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, 0);

    database.transaction(() -> {
      final VertexType type = database.getSchema().createVertexType("ImageEmbedding");
      type.createProperty("name", Type.STRING);
      type.createProperty("vector", Type.ARRAY_OF_FLOATS);

      database.getSchema().buildTypeIndex("ImageEmbedding", new String[] { "vector" })
          .withLSMVectorType()
          .withDimensions(dimensions)
          .withSimilarity("COSINE")
          .withMaxConnections(16)
          .withBeamWidth(100)
          .create();
    });

    for (int i = 0; i < totalVectors; i++) {
      final int idx = i;
      database.transaction(() -> {
        final MutableVertex vertex = database.newVertex("ImageEmbedding");
        vertex.set("name", "img" + idx);
        vertex.set("vector", createDeterministicVector(idx, dimensions));
        vertex.save();
      });
    }

    database.transaction(() -> {
      final long count = database.countType("ImageEmbedding", true);
      assertThat(count).as("Should have %d ImageEmbedding records", totalVectors).isEqualTo(totalVectors);
    });

    reopenDatabase();

    database.transaction(() -> {
      final TypeIndex ti = (TypeIndex) database.getSchema().getIndexByName("ImageEmbedding[vector]");
      final LSMVectorIndex idx = (LSMVectorIndex) ti.getSubIndexes().iterator().next();
      final Map<String, Long> stats = idx.getStats();

      assertThat(stats.get("totalVectors"))
          .as("vectorIndex should have all %d vectors loaded from pages", totalVectors)
          .isEqualTo(totalVectors);

      final float[] queryVector = new float[dimensions];
      Arrays.fill(queryVector, 0.5f);
      final ResultSet rs = database.query("sql",
          "SELECT vectorNeighbors('ImageEmbedding[vector]', ?, ?) AS neighbors",
          queryVector, queryLimit);

      assertThat(rs.hasNext()).isTrue();
      final Result result = rs.next();
      final List<?> neighbors = result.getProperty("neighbors");
      assertThat(neighbors).isNotNull();

      assertThat(neighbors.size())
          .as("vectorNeighbors should return close to %d results out of %d total vectors", queryLimit, totalVectors)
          .isGreaterThan(queryLimit * 9 / 10);

      rs.close();
    });
  }

  // Issue #3722: vectorNeighbors must return ~all vectors after reopen even when graph was persisted with only an early subset (stale-graph rebuild path).
  @Test
  void vectorNeighborsShouldReturnAllVectorsAfterStaleGraphReopen() {
    final int dimensions = 128;
    final int totalVectors = 546;
    final int queryLimit = 500;

    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, 100_000);
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, 0);

    database.transaction(() -> {
      final VertexType type = database.getSchema().createVertexType("ImageEmbedding");
      type.createProperty("name", Type.STRING);
      type.createProperty("vector", Type.ARRAY_OF_FLOATS);

      database.getSchema().buildTypeIndex("ImageEmbedding", new String[] { "vector" })
          .withLSMVectorType()
          .withDimensions(dimensions)
          .withSimilarity("COSINE")
          .withMaxConnections(16)
          .withBeamWidth(100)
          .create();
    });

    database.transaction(() -> {
      for (int i = 0; i < 66; i++) {
        final MutableVertex vertex = database.newVertex("ImageEmbedding");
        vertex.set("name", "img" + i);
        vertex.set("vector", createDeterministicVector(i, dimensions));
        vertex.save();
      }
    });

    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("ImageEmbedding[vector]");
    final LSMVectorIndex index = (LSMVectorIndex) typeIndex.getSubIndexes().iterator().next();
    index.buildVectorGraphNow();

    assertThat(index.getStats().get("graphNodeCount")).isGreaterThanOrEqualTo(66L);

    for (int i = 66; i < totalVectors; i++) {
      final int idx = i;
      database.transaction(() -> {
        final MutableVertex vertex = database.newVertex("ImageEmbedding");
        vertex.set("name", "img" + idx);
        vertex.set("vector", createDeterministicVector(idx, dimensions));
        vertex.save();
      });
    }

    reopenDatabase();
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, 100_000);
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, 0);

    database.transaction(() -> {
      final TypeIndex ti = (TypeIndex) database.getSchema().getIndexByName("ImageEmbedding[vector]");
      final LSMVectorIndex idx = (LSMVectorIndex) ti.getSubIndexes().iterator().next();

      assertThat(idx.getStats().get("totalVectors"))
          .as("All %d vectors should be loaded from pages", totalVectors)
          .isEqualTo(totalVectors);

      final float[] queryVector = new float[dimensions];
      Arrays.fill(queryVector, 0.5f);
      final ResultSet rs = database.query("sql",
          "SELECT vectorNeighbors('ImageEmbedding[vector]', ?, ?) AS neighbors",
          queryVector, queryLimit);

      assertThat(rs.hasNext()).isTrue();
      final Result result = rs.next();
      final List<?> neighbors = result.getProperty("neighbors");
      assertThat(neighbors).isNotNull();

      assertThat(neighbors.size())
          .as("vectorNeighbors must return more than the 66 vectors in the stale persisted graph")
          .isGreaterThan(queryLimit * 9 / 10);

      rs.close();
    });
  }

  // Issue #3722: vectorNeighbors must keep returning ~all vectors across repeated reopen cycles.
  @Test
  void vectorNeighborsShouldSurviveMultipleReopens() {
    final int dimensions = 128;
    final int totalVectors = 546;
    final int queryLimit = 500;

    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, 100_000);
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, 0);

    database.transaction(() -> {
      final VertexType type = database.getSchema().createVertexType("ImageEmbedding");
      type.createProperty("name", Type.STRING);
      type.createProperty("vector", Type.ARRAY_OF_FLOATS);

      database.getSchema().buildTypeIndex("ImageEmbedding", new String[] { "vector" })
          .withLSMVectorType()
          .withDimensions(dimensions)
          .withSimilarity("COSINE")
          .withMaxConnections(16)
          .withBeamWidth(100)
          .create();
    });

    database.transaction(() -> {
      for (int i = 0; i < totalVectors; i++) {
        final MutableVertex vertex = database.newVertex("ImageEmbedding");
        vertex.set("name", "img" + i);
        vertex.set("vector", createDeterministicVector(i, dimensions));
        vertex.save();
      }
    });

    database.transaction(() -> {
      final float[] queryVector = new float[dimensions];
      Arrays.fill(queryVector, 0.5f);
      final ResultSet rs = database.query("sql",
          "SELECT vectorNeighbors('ImageEmbedding[vector]', ?, ?) AS neighbors",
          queryVector, queryLimit);
      assertThat(rs.hasNext()).isTrue();
      final List<?> neighbors = rs.next().getProperty("neighbors");
      assertThat(neighbors.size()).isGreaterThan(queryLimit * 9 / 10);
      rs.close();
    });

    for (int reopen = 0; reopen < 3; reopen++) {
      reopenDatabase();
      database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, 100_000);
      database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, 0);

      final int reopenNum = reopen;
      database.transaction(() -> {
        final float[] queryVector = new float[dimensions];
        Arrays.fill(queryVector, 0.5f);
        final ResultSet rs = database.query("sql",
            "SELECT vectorNeighbors('ImageEmbedding[vector]', ?, ?) AS neighbors",
            queryVector, queryLimit);

        assertThat(rs.hasNext()).isTrue();
        final List<?> neighbors = rs.next().getProperty("neighbors");
        assertThat(neighbors.size())
            .as("Reopen #%d: should still return close to %d results", reopenNum + 1, queryLimit)
            .isGreaterThan(queryLimit * 9 / 10);
        rs.close();
      });
    }
  }

  // Issue #3722: vectorNeighbors must return all active vectors after delete (tombstone) and reopen without page-parsing corruption.
  @Test
  void vectorNeighborsShouldReturnAllVectorsAfterDeleteAndReopen() {
    final int dimensions = 64;
    final int initialVectors = 66;
    final int additionalVectors = 480;
    final int totalVectors = initialVectors + additionalVectors;

    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, 100_000);
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, 0);

    database.transaction(() -> {
      final VertexType type = database.getSchema().createVertexType("ImageEmbedding");
      type.createProperty("name", Type.STRING);
      type.createProperty("vector", Type.ARRAY_OF_FLOATS);

      database.getSchema().buildTypeIndex("ImageEmbedding", new String[] { "vector" })
          .withLSMVectorType()
          .withDimensions(dimensions)
          .withSimilarity("COSINE")
          .withMaxConnections(16)
          .withBeamWidth(100)
          .create();
    });

    final RID[] rids = new RID[totalVectors];
    database.transaction(() -> {
      for (int i = 0; i < initialVectors; i++) {
        final MutableVertex vertex = database.newVertex("ImageEmbedding");
        vertex.set("name", "img" + i);
        vertex.set("vector", createDeterministicVector(i, dimensions));
        vertex.save();
        rids[i] = vertex.getIdentity();
      }
    });

    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("ImageEmbedding[vector]");
    final LSMVectorIndex index = (LSMVectorIndex) typeIndex.getSubIndexes().iterator().next();
    index.buildVectorGraphNow();

    database.transaction(() -> {
      final var vertex = rids[0].asVertex();
      vertex.delete();
    });

    database.transaction(() -> {
      for (int i = initialVectors; i < totalVectors; i++) {
        final MutableVertex vertex = database.newVertex("ImageEmbedding");
        vertex.set("name", "img" + i);
        vertex.set("vector", createDeterministicVector(i, dimensions));
        vertex.save();
        rids[i] = vertex.getIdentity();
      }
    });

    reopenDatabase();
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, 100_000);
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, 0);

    database.transaction(() -> {
      final TypeIndex ti = (TypeIndex) database.getSchema().getIndexByName("ImageEmbedding[vector]");
      final LSMVectorIndex idx = (LSMVectorIndex) ti.getSubIndexes().iterator().next();
      final Map<String, Long> stats = idx.getStats();
      final long activeVectors = stats.get("activeVectors");

      assertThat(activeVectors)
          .as("All %d active vectors should be loaded from pages after reopen (initial %d - 1 deleted + %d additional)",
              totalVectors - 1, initialVectors, additionalVectors)
          .isEqualTo(totalVectors - 1);

      final float[] queryVector = new float[dimensions];
      Arrays.fill(queryVector, 0.5f);
      final int k = totalVectors;
      final ResultSet rs = database.query("sql",
          "SELECT vectorNeighbors('ImageEmbedding[vector]', ?, ?) AS neighbors",
          queryVector, k);

      assertThat(rs.hasNext()).isTrue();
      final Result result = rs.next();
      final List<?> neighbors = result.getProperty("neighbors");
      assertThat(neighbors).isNotNull();

      assertThat(neighbors.size())
          .as("vectorNeighbors should return vectors from BOTH batches, not just the %d before the tombstone",
              initialVectors - 1)
          .isGreaterThan(totalVectors / 2);

      rs.close();
    });
  }

  // Issue #3722: vectorNeighbors must return ~all vectors after update (delete + re-insert tombstones) and reopen.
  @Test
  void vectorNeighborsShouldReturnAllVectorsAfterUpdateAndReopen() {
    final int dimensions = 64;
    final int initialVectors = 66;
    final int additionalVectors = 480;
    final int totalVectors = initialVectors + additionalVectors;

    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, 100_000);
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, 0);

    database.transaction(() -> {
      final VertexType type = database.getSchema().createVertexType("ImageEmbedding");
      type.createProperty("name", Type.STRING);
      type.createProperty("vector", Type.ARRAY_OF_FLOATS);

      database.getSchema().buildTypeIndex("ImageEmbedding", new String[] { "vector" })
          .withLSMVectorType()
          .withDimensions(dimensions)
          .withSimilarity("COSINE")
          .withMaxConnections(16)
          .withBeamWidth(100)
          .create();
    });

    final RID[] rids = new RID[totalVectors];
    database.transaction(() -> {
      for (int i = 0; i < totalVectors; i++) {
        final MutableVertex vertex = database.newVertex("ImageEmbedding");
        vertex.set("name", "img" + i);
        vertex.set("vector", createDeterministicVector(i, dimensions));
        vertex.save();
        rids[i] = vertex.getIdentity();
      }
    });

    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("ImageEmbedding[vector]");
    final LSMVectorIndex index = (LSMVectorIndex) typeIndex.getSubIndexes().iterator().next();
    index.buildVectorGraphNow();

    database.transaction(() -> {
      for (int i = 0; i < 5; i++) {
        final var vertex = rids[i].asVertex().modify();
        vertex.set("vector", createDeterministicVector(totalVectors + i, dimensions));
        vertex.save();
      }
    });

    reopenDatabase();
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, 100_000);
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, 0);

    database.transaction(() -> {
      final float[] queryVector = new float[dimensions];
      Arrays.fill(queryVector, 0.5f);
      final int k = totalVectors;
      final ResultSet rs = database.query("sql",
          "SELECT vectorNeighbors('ImageEmbedding[vector]', ?, ?) AS neighbors",
          queryVector, k);

      assertThat(rs.hasNext()).isTrue();
      final Result result = rs.next();
      final List<?> neighbors = result.getProperty("neighbors");
      assertThat(neighbors).isNotNull();

      assertThat(neighbors.size())
          .as("vectorNeighbors should return all %d vectors after update and reopen", totalVectors)
          .isGreaterThan(totalVectors / 2);

      rs.close();
    });
  }

  // Issue #3722: deletion tombstones must not corrupt page parsing of subsequent entries on the same page.
  @Test
  void tombstoneShouldNotCorruptPageParsing() {
    final int dimensions = 64;
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, 100_000);
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, 0);

    database.transaction(() -> {
      final VertexType type = database.getSchema().createVertexType("Vec");
      type.createProperty("name", Type.STRING);
      type.createProperty("vector", Type.ARRAY_OF_FLOATS);

      database.getSchema().buildTypeIndex("Vec", new String[] { "vector" })
          .withLSMVectorType()
          .withDimensions(dimensions)
          .withSimilarity("COSINE")
          .withMaxConnections(16)
          .withBeamWidth(100)
          .create();
    });

    final RID[] rids = new RID[10];
    database.transaction(() -> {
      for (int i = 0; i < 10; i++) {
        final MutableVertex vertex = database.newVertex("Vec");
        vertex.set("name", "v" + i);
        vertex.set("vector", createDeterministicVector(i, dimensions));
        vertex.save();
        rids[i] = vertex.getIdentity();
      }
    });

    database.transaction(() -> rids[5].asVertex().delete());

    database.transaction(() -> {
      for (int i = 10; i < 20; i++) {
        final MutableVertex vertex = database.newVertex("Vec");
        vertex.set("name", "v" + i);
        vertex.set("vector", createDeterministicVector(i, dimensions));
        vertex.save();
      }
    });

    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("Vec[vector]");
    final LSMVectorIndex idx = (LSMVectorIndex) typeIndex.getSubIndexes().iterator().next();

    final List<LSMVectorIndexPageParser.VectorEntry> allEntries =
        LSMVectorIndexPageParser.parseAllEntries(
            (DatabaseInternal) database,
            idx.getFileId(),
            idx.getTotalPages(),
            idx.getPageSize(),
            false);

    int regularCount = 0;
    int tombstoneCount = 0;
    for (final LSMVectorIndexPageParser.VectorEntry entry : allEntries) {
      if (entry.deleted)
        tombstoneCount++;
      else
        regularCount++;
    }

    assertThat(tombstoneCount)
        .as("Should have 1 tombstone entry")
        .isEqualTo(1);

    assertThat(regularCount)
        .as("Should have 20 regular entries (10 initial + 10 additional)")
        .isEqualTo(20);

    assertThat(allEntries.size())
        .as("Total entries should be 21 (20 regular + 1 tombstone)")
        .isEqualTo(21);

    final Set<Integer> foundRegularIds = new HashSet<>();
    final Set<Integer> foundTombstoneIds = new HashSet<>();
    for (final LSMVectorIndexPageParser.VectorEntry entry : allEntries) {
      if (entry.deleted)
        foundTombstoneIds.add(entry.vectorId);
      else
        foundRegularIds.add(entry.vectorId);
    }

    assertThat(foundTombstoneIds)
        .as("Exactly one tombstone should exist (found: %s)", foundTombstoneIds)
        .hasSize(1);

    final int tombstoneVid = foundTombstoneIds.iterator().next();
    assertThat(tombstoneVid)
        .as("Tombstone vectorId should be in range 0-9")
        .isBetween(0, 9);

    assertThat(foundRegularIds)
        .as("""
            Should have 20 regular vectorIds from both batches (0-9 minus tombstone, plus 10-19).
            Found regular IDs: %s""", foundRegularIds)
        .hasSize(20);

    for (final int vid : foundRegularIds) {
      assertThat(vid)
          .as("Regular vectorId %d should be in valid range [0, 19]", vid)
          .isBetween(0, 19);
    }
  }

  // Issue #4228: discoverAndLoadCompactedSubIndex must skip null slots in FileManager.getFiles() instead of NPE-ing on reopen.
  @Test
  void discoveryShouldNotNpeOnNullFileSlotAfterReopen() throws Exception {
    final int dimensions = 16;
    final int vectorCount = 32;

    // 1) Filler type whose bucket grabs an early fileId. Dropping it later produces a null slot in
    //    FileManager that survives the reopen because the bucket file is physically deleted.
    database.transaction(() -> {
      final DocumentType filler = database.getSchema().createDocumentType("Filler");
      filler.createProperty("name", Type.STRING);
    });

    // 2) Vector index + a handful of vectors.
    database.transaction(() -> {
      final DocumentType embedding = database.getSchema().createDocumentType("Embedding");
      embedding.createProperty("name", Type.STRING);
      embedding.createProperty("vector", Type.ARRAY_OF_FLOATS);

      database.command("sql", """
          CREATE INDEX ON Embedding (vector) LSM_VECTOR METADATA {
            "dimensions": %d,
            "similarity": "COSINE",
            "maxConnections": 16,
            "beamWidth": 100,
            "idPropertyName": "name"
          }""".formatted(dimensions));
    });

    database.transaction(() -> {
      for (int i = 0; i < vectorCount; i++) {
        final float[] vector = new float[dimensions];
        for (int j = 0; j < dimensions; j++)
          vector[j] = i * 0.01f + j * 0.001f;
        database.newDocument("Embedding").set("name", "v" + i).set("vector", vector).save();
      }
    });

    // 3) Drop the filler type
    database.getSchema().dropType("Filler");

    // 4) Sanity
    assertThat(hasNullFileSlot((DatabaseInternal) database))
        .as("After dropping Filler, FileManager.getFiles() must contain a null slot")
        .isTrue();

    // 5) Capture warnings/severe logs during the reopen.
    final List<String> captured = new CopyOnWriteArrayList<>();
    final Logger originalLogger = readField(LogManager.instance(), "logger");
    LogManager.instance().setLogger(new CapturingLogger(captured, originalLogger));

    try {
      reopenDatabase();

      assertThat(hasNullFileSlot((DatabaseInternal) database))
          .as("After reopen FileManager.getFiles() must still expose the null slot - otherwise the "
              + "test is not actually exercising the buggy code path")
          .isTrue();

      final List<String> matching = captured.stream()
          .filter(m -> m != null && m.contains("Error discovering compacted sub-index"))
          .toList();

      assertThat(matching)
          .as("The discovery loop must not throw NPE on null slots in FileManager.getFiles(). "
              + "Captured logs at WARNING+ during reopen: %s", captured)
          .isEmpty();
    } finally {
      LogManager.instance().setLogger(originalLogger);
    }
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private List<Result> executeVectorSearch(final String typeName, final float[] queryVector, final int k) {
    final StringBuilder vectorStr = new StringBuilder("[");
    for (int i = 0; i < queryVector.length; i++) {
      if (i > 0)
        vectorStr.append(",");
      vectorStr.append(queryVector[i]);
    }
    vectorStr.append("]");

    final String sql = String.format(
        "SELECT distance, record FROM (SELECT expand(vectorNeighbors('%s[vector]', %s, %d)))",
        typeName, vectorStr, k);

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

  /**
   * Creates a deterministic vector based on the index, ensuring all vectors are unique
   * and non-zero (required for COSINE similarity).
   */
  private static float[] createDeterministicVector(final int index, final int dimensions) {
    final float[] vector = new float[dimensions];
    for (int j = 0; j < dimensions; j++)
      vector[j] = (float) Math.sin(index * 0.1 + j * 0.3) * 0.5f + 0.5f;
    return vector;
  }

  private static boolean hasNullFileSlot(final DatabaseInternal database) {
    for (final ComponentFile f : database.getFileManager().getFiles()) {
      if (f == null)
        return true;
    }
    return false;
  }

  @SuppressWarnings("unchecked")
  private static <T> T readField(final Object target, final String name) throws Exception {
    final Field f = target.getClass().getDeclaredField(name);
    f.setAccessible(true);
    return (T) f.get(target);
  }

  /**
   * Captures messages at WARNING and above. Forwards every record to the production logger so test
   * output still shows what fired.
   */
  private static final class CapturingLogger implements Logger {
    private final List<String> captured;
    private final Logger       delegate;

    CapturingLogger(final List<String> captured, final Logger delegate) {
      this.captured = captured;
      this.delegate = delegate;
    }

    private void capture(final Level level, final String message, final Object... args) {
      if (message == null || level.intValue() < Level.WARNING.intValue())
        return;
      String formatted = message;
      if (args != null && args.length > 0) {
        try {
          formatted = message.formatted(args);
        } catch (final Exception ignored) {
          // Fall back to the raw template; good enough for substring matching.
        }
      }
      captured.add(formatted);
    }

    @Override
    public void log(final Object requester, final Level level, final String message, final Throwable exception,
        final String context, final Object arg1, final Object arg2, final Object arg3, final Object arg4, final Object arg5,
        final Object arg6, final Object arg7, final Object arg8, final Object arg9, final Object arg10, final Object arg11,
        final Object arg12, final Object arg13, final Object arg14, final Object arg15, final Object arg16, final Object arg17) {
      capture(level, message, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13, arg14, arg15,
          arg16, arg17);
      delegate.log(requester, level, message, exception, context, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10,
          arg11, arg12, arg13, arg14, arg15, arg16, arg17);
    }

    @Override
    public void log(final Object requester, final Level level, final String message, final Throwable exception,
        final String context, final Object... args) {
      capture(level, message, args);
      delegate.log(requester, level, message, exception, context, args);
    }

    @Override
    public void flush() {
      delegate.flush();
    }
  }
}
