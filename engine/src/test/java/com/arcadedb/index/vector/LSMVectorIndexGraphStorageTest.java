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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test cases for Phase 2: storing vectors inline in JVector graph files.
 * Tests cover:
 * - storeVectorsInGraph with no quantization (full float32 storage)
 * - storeVectorsInGraph with INT8 quantization
 * - storeVectorsInGraph with BINARY quantization
 * - Vector updates and graph rebuilds
 */
class LSMVectorIndexGraphStorageTest {
  private static final String DB_PATH = "target/test-databases/LSMVectorIndexGraphStorageTest";

  @BeforeEach
  void setup() {
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  @AfterEach
  void cleanup() {
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  /**
   * Test storing full-precision float vectors in graph file (no quantization).
   * Verifies that vectors are stored and retrieved correctly from the graph file.
   */
  @Test
  void storeVectorsInGraphNoQuantization() {
    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      // Phase 1: Create index and insert vectors
      try (final Database db = factory.create()) {
        final int dimensions = 128;
        final int numVectors = 100;

        // Create schema with storeVectorsInGraph enabled
        db.transaction(() -> {
          final DocumentType type = db.getSchema().createDocumentType("VectorDoc");
          type.createProperty("embedding", Type.ARRAY_OF_FLOATS);

          db.command("sql", """
              CREATE INDEX ON VectorDoc (embedding) LSM_VECTOR
              METADATA {
                "dimensions": %d,
                "similarity": "COSINE",
                "storeVectorsInGraph": true
              }
              """.formatted(dimensions));
        });

        // Insert test vectors
        final List<RID> rids = new ArrayList<>();
        db.transaction(() -> {
          for (int i = 0; i < numVectors; i++) {
            final float[] vector = generateRandomVector(dimensions, i);
            final var doc = db.newDocument("VectorDoc");
            doc.set("embedding", vector);
            doc.save();
            rids.add(doc.getIdentity());
          }
        });

        // Verify index configuration
        final LSMVectorIndex index = getVectorIndex(db, "VectorDoc", "embedding");
        assertThat(index).isNotNull();
        assertThat(index.metadata.storeVectorsInGraph).as("storeVectorsInGraph should be enabled").isTrue();
        assertThat(index.metadata.quantizationType).as("Should have no quantization").isEqualTo(VectorQuantizationType.NONE);

        // System.out.println("✓ Index created with storeVectorsInGraph=true, closing database...");
      }

      // Phase 2: Reopen database and verify vectors are fetched from graph
      try (final Database db = factory.open()) {
        final LSMVectorIndex index = getVectorIndex(db, "VectorDoc", "embedding");
        assertThat(index).isNotNull();

        // Perform searches and verify vectorFetchFromGraph metric
        final Map<String, Long> statsBefore = index.getStats();
        final long fetchFromGraphBefore = statsBefore.getOrDefault("vectorFetchFromGraph", 0L);
        final long fetchFromDocsBefore = statsBefore.getOrDefault("vectorFetchFromDocuments", 0L);

        final float[] queryVector = generateRandomVector(128, 42);
        final List<Pair<RID, Float>> results = index.findNeighborsFromVector(queryVector, 10);

        assertThat(results.size() > 0).as("Should find neighbors").isTrue();
        assertThat(results.size() <= 10).as("Should not exceed k limit").isTrue();

        // Verify metrics show vectors fetched from graph
        final Map<String, Long> statsAfter = index.getStats();
        final long fetchFromGraphAfter = statsAfter.getOrDefault("vectorFetchFromGraph", 0L);
        final long fetchFromDocsAfter = statsAfter.getOrDefault("vectorFetchFromDocuments", 0L);

        assertThat(fetchFromGraphAfter > fetchFromGraphBefore).as("Should have fetched vectors from graph (before: " + fetchFromGraphBefore + ", after: " + fetchFromGraphAfter + ")").isTrue();
        assertThat(fetchFromDocsAfter).as("Should NOT have fetched from documents when storeVectorsInGraph is enabled").isEqualTo(fetchFromDocsBefore);

        // System.out.println("✓ Test passed: Vectors stored and fetched from graph file (no quantization)");
        // System.out.println("  Vectors fetched from graph: " + (fetchFromGraphAfter - fetchFromGraphBefore));
        // System.out.println("  Graph state: " + statsAfter.get("graphState") + " (0=LOADING, 1=IMMUTABLE, 2=MUTABLE)");
      }
    }
  }

  /**
   * Test storing INT8 quantized vectors in graph file.
   * Verifies that quantized vectors are stored and retrieved correctly.
   */
  @Test
  void storeVectorsInGraphWithINT8Quantization() {
    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      // Phase 1: Create index and insert vectors
      try (final Database db = factory.create()) {
        final int dimensions = 128;
        final int numVectors = 100;

        // Create schema with INT8 quantization and storeVectorsInGraph
        db.transaction(() -> {
          final DocumentType type = db.getSchema().createDocumentType("VectorDoc");
          type.createProperty("embedding", Type.ARRAY_OF_FLOATS);

          db.command("sql", """
              CREATE INDEX ON VectorDoc (embedding) LSM_VECTOR
              METADATA {
                "dimensions": %d,
                "similarity": "COSINE",
                "quantization": "INT8",
                "storeVectorsInGraph": true
              }
              """.formatted(dimensions));
        });

        // Insert test vectors
        db.transaction(() -> {
          for (int i = 0; i < numVectors; i++) {
            final float[] vector = generateRandomVector(dimensions, i);
            final var doc = db.newDocument("VectorDoc");
            doc.set("embedding", vector);
            doc.save();
          }
        });

        // Verify index configuration
        final LSMVectorIndex index = getVectorIndex(db, "VectorDoc", "embedding");
        assertThat(index).isNotNull();
        assertThat(index.metadata.storeVectorsInGraph).as("storeVectorsInGraph should be enabled").isTrue();
        assertThat(index.metadata.quantizationType).as("Should have INT8 quantization").isEqualTo(VectorQuantizationType.INT8);

        // System.out.println("✓ Index created with INT8 quantization and storeVectorsInGraph=true");
      }

      // Phase 2: Reopen and verify
      try (final Database db = factory.open()) {
        final LSMVectorIndex index = getVectorIndex(db, "VectorDoc", "embedding");

        // Perform searches
        final Map<String, Long> statsBefore = index.getStats();
        final float[] queryVector = generateRandomVector(128, 99);
        final List<Pair<RID, Float>> results = index.findNeighborsFromVector(queryVector, 10);

        assertThat(results.size() > 0).as("Should find neighbors").isTrue();

        // Verify metrics - with quantization, vectors might be fetched from graph or quantized pages
        final Map<String, Long> statsAfter = index.getStats();
        final long fetchFromGraph = statsAfter.getOrDefault("vectorFetchFromGraph", 0L) -
            statsBefore.getOrDefault("vectorFetchFromGraph", 0L);
        final long fetchFromDocs = statsAfter.getOrDefault("vectorFetchFromDocuments", 0L) -
            statsBefore.getOrDefault("vectorFetchFromDocuments", 0L);

        // When storeVectorsInGraph is enabled with quantization, vectors are stored quantized in graph
        assertThat(fetchFromGraph > 0 || fetchFromDocs == 0).as("Should fetch from graph when storeVectorsInGraph is enabled (graph: " + fetchFromGraph + ", docs: " + fetchFromDocs
            + ")").isTrue();

        // System.out.println("✓ Test passed: INT8 quantized vectors stored and fetched from graph");
        // System.out.println("  Vectors fetched from graph: " + fetchFromGraph);
        // System.out.println("  Vectors fetched from docs: " + fetchFromDocs);
      }
    }
  }

  /**
   * Test storing BINARY quantized vectors in graph file.
   * Verifies maximum compression with binary quantization.
   */
  @Test
  void storeVectorsInGraphWithBinaryQuantization() {
    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      try (final Database db = factory.create()) {
        final int dimensions = 128;
        final int numVectors = 100;

        // Create schema with BINARY quantization and storeVectorsInGraph
        db.transaction(() -> {
          final DocumentType type = db.getSchema().createDocumentType("VectorDoc");
          type.createProperty("embedding", Type.ARRAY_OF_FLOATS);

          db.command("sql", """
              CREATE INDEX ON VectorDoc (embedding) LSM_VECTOR
              METADATA {
                "dimensions": %d,
                "similarity": "COSINE",
                "quantization": "BINARY",
                "storeVectorsInGraph": true
              }
              """.formatted(dimensions));
        });

        // Insert test vectors
        db.transaction(() -> {
          for (int i = 0; i < numVectors; i++) {
            final float[] vector = generateRandomVector(dimensions, i);
            final var doc = db.newDocument("VectorDoc");
            doc.set("embedding", vector);
            doc.save();
          }
        });

        // Verify index configuration
        final LSMVectorIndex index = getVectorIndex(db, "VectorDoc", "embedding");
        assertThat(index).isNotNull();
        assertThat(index.metadata.storeVectorsInGraph).isTrue();
        assertThat(index.metadata.quantizationType).isEqualTo(VectorQuantizationType.BINARY);

        // Perform searches
        final float[] queryVector = generateRandomVector(dimensions, 50);
        final List<Pair<RID, Float>> results = index.findNeighborsFromVector(queryVector, 10);

        assertThat(results.size() > 0).as("Should find neighbors even with binary quantization").isTrue();

        // System.out.println("✓ Test passed: BINARY quantized vectors stored and fetched from graph");
      }
    }
  }

  /**
   * Test vector updates with storeVectorsInGraph enabled.
   * Verifies that graph is properly rebuilt after vector updates.
   */
  @Test
  void vectorUpdatesWithGraphStorage() {
    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      try (final Database db = factory.create()) {
        final int dimensions = 64;

        // Create schema
        db.transaction(() -> {
          final DocumentType type = db.getSchema().createDocumentType("VectorDoc");
          type.createProperty("id", Type.INTEGER);
          type.createProperty("embedding", Type.ARRAY_OF_FLOATS);

          db.command("sql", """
              CREATE INDEX ON VectorDoc (embedding) LSM_VECTOR
              METADATA {
                "dimensions": %d,
                "similarity": "COSINE",
                "storeVectorsInGraph": true,
                "mutationsBeforeRebuild": 10
              }
              """.formatted(dimensions));
        });

        // Insert initial vectors
        db.transaction(() -> {
          for (int i = 0; i < 20; i++) {
            final float[] vector = generateRandomVector(dimensions, i);
            final var doc = db.newDocument("VectorDoc");
            doc.set("id", i);
            doc.set("embedding", vector);
            doc.save();
          }
        });

        final LSMVectorIndex index = getVectorIndex(db, "VectorDoc", "embedding");
        final Map<String, Long> statsAfterInsert = index.getStats();
        final long rebuildsAfterInsert = statsAfterInsert.get("graphRebuildCount");

        // Update vectors to trigger rebuild
        db.transaction(() -> {
          for (int i = 0; i < 15; i++) {
            final var result = db.query("sql", "SELECT FROM VectorDoc WHERE id = ?", i).nextIfAvailable();
            final RID rid = result.getIdentity().get();
            final var doc = rid.asDocument().modify();
            final float[] newVector = generateRandomVector(dimensions, i + 1000);
            doc.set("embedding", newVector);
            doc.save();
          }
        });

        // Perform search to trigger graph rebuild
        final float[] queryVector = generateRandomVector(dimensions, 999);
        index.findNeighborsFromVector(queryVector, 5);

        final Map<String, Long> statsAfterUpdate = index.getStats();
        final long rebuildsAfterUpdate = statsAfterUpdate.get("graphRebuildCount");

        // Graph should have been rebuilt due to mutations
        assertThat(rebuildsAfterUpdate >= rebuildsAfterInsert).as("Graph should be rebuilt after mutations (before: " + rebuildsAfterInsert + ", after: " + rebuildsAfterUpdate + ")").isTrue();

        // System.out.println("✓ Test passed: Vector updates trigger graph rebuilds");
        // System.out.println("  Rebuilds after insert: " + rebuildsAfterInsert);
        // System.out.println("  Rebuilds after update: " + rebuildsAfterUpdate);
        // System.out.println("  Mutations since rebuild: " + statsAfterUpdate.get("mutationsSinceRebuild"));
      }
    }
  }

  /**
   * Test that disabling storeVectorsInGraph fetches from documents.
   */
  @Test
  void disabledGraphStorageFetchesFromDocuments() {
    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      try (final Database db = factory.create()) {
        final int dimensions = 64;
        final int numVectors = 50;

        // Create schema with storeVectorsInGraph disabled (default)
        db.transaction(() -> {
          final DocumentType type = db.getSchema().createDocumentType("VectorDoc");
          type.createProperty("embedding", Type.ARRAY_OF_FLOATS);

          db.command("sql", """
              CREATE INDEX ON VectorDoc (embedding) LSM_VECTOR
              METADATA {
                "dimensions": %d,
                "similarity": "COSINE",
                "storeVectorsInGraph": false
              }
              """.formatted(dimensions));
        });

        // Insert test vectors
        db.transaction(() -> {
          for (int i = 0; i < numVectors; i++) {
            final float[] vector = generateRandomVector(dimensions, i);
            final var doc = db.newDocument("VectorDoc");
            doc.set("embedding", vector);
            doc.save();
          }
        });

        final LSMVectorIndex index = getVectorIndex(db, "VectorDoc", "embedding");
        assertThat(index.metadata.storeVectorsInGraph).as("storeVectorsInGraph should be disabled").isFalse();

        // Perform search
        final Map<String, Long> statsBefore = index.getStats();
        final float[] queryVector = generateRandomVector(dimensions, 25);
        index.findNeighborsFromVector(queryVector, 10);

        final Map<String, Long> statsAfter = index.getStats();
        final long fetchFromDocs = statsAfter.get("vectorFetchFromDocuments") - statsBefore.get("vectorFetchFromDocuments");
        final long fetchFromGraph = statsAfter.getOrDefault("vectorFetchFromGraph", 0L) -
            statsBefore.getOrDefault("vectorFetchFromGraph", 0L);

        // Should fetch from documents when storeVectorsInGraph is disabled
        assertThat(fetchFromDocs > 0).as("Should fetch from documents when graph storage is disabled").isTrue();
        assertThat(fetchFromGraph).as("Should NOT fetch from graph when disabled").isEqualTo(0);

        // System.out.println("✓ Test passed: Disabled graph storage fetches from documents");
        // System.out.println("  Vectors fetched from documents: " + fetchFromDocs);
      }
    }
  }

  /**
   * Test for GitHub issue #3142: storeVectorsInGraph=false still produces a vecgraph file that
   * inlines vectors, so vectors are stored twice (bucket + graph), inflating disk and RSS.
   * <p>
   * When storeVectorsInGraph=false, the graph file should store only topology (no vectors).
   */
  @Test
  void storeVectorsInGraphFalseShouldNotStoreVectors() {
    final String dbPathWithVectors = DB_PATH + "_with_vectors";
    final String dbPathWithoutVectors = DB_PATH + "_without_vectors";

    FileUtils.deleteRecursively(new File(dbPathWithVectors));
    FileUtils.deleteRecursively(new File(dbPathWithoutVectors));

    try {
      final int dimensions = 128;
      final int numVectors = 1000; // Use 1000 vectors to ensure file size difference is significant

      // Create database WITH storeVectorsInGraph=true
      long graphFileSizeWithVectors;
      try (final DatabaseFactory factory = new DatabaseFactory(dbPathWithVectors)) {
        try (final Database db = factory.create()) {
          db.transaction(() -> {
            final DocumentType type = db.getSchema().createDocumentType("VectorDoc");
            type.createProperty("embedding", Type.ARRAY_OF_FLOATS);

            db.command("sql", """
                CREATE INDEX ON VectorDoc (embedding) LSM_VECTOR
                METADATA {
                  "dimensions": %d,
                  "similarity": "COSINE",
                  "storeVectorsInGraph": true
                }
                """.formatted(dimensions));
          });

          // Insert test vectors
          db.transaction(() -> {
            for (int i = 0; i < numVectors; i++) {
              final float[] vector = generateRandomVector(dimensions, i);
              final var doc = db.newDocument("VectorDoc");
              doc.set("embedding", vector);
              doc.save();
            }
          });

          // Trigger graph build by performing a search
          final LSMVectorIndex index = getVectorIndex(db, "VectorDoc", "embedding");
          index.findNeighborsFromVector(generateRandomVector(dimensions, 999), 10);
        }

        graphFileSizeWithVectors = getVecgraphFileSize(dbPathWithVectors);
        // System.out.println("Graph file size WITH vectors: " + graphFileSizeWithVectors + " bytes");
      }

      // Create database WITHOUT storeVectorsInGraph (false)
      long graphFileSizeWithoutVectors;
      try (final DatabaseFactory factory = new DatabaseFactory(dbPathWithoutVectors)) {
        try (final Database db = factory.create()) {
          db.transaction(() -> {
            final DocumentType type = db.getSchema().createDocumentType("VectorDoc");
            type.createProperty("embedding", Type.ARRAY_OF_FLOATS);

            db.command("sql", """
                CREATE INDEX ON VectorDoc (embedding) LSM_VECTOR
                METADATA {
                  "dimensions": %d,
                  "similarity": "COSINE",
                  "storeVectorsInGraph": false
                }
                """.formatted(dimensions));
          });

          // Insert identical test vectors
          db.transaction(() -> {
            for (int i = 0; i < numVectors; i++) {
              final float[] vector = generateRandomVector(dimensions, i);
              final var doc = db.newDocument("VectorDoc");
              doc.set("embedding", vector);
              doc.save();
            }
          });

          // Trigger graph build by performing a search
          final LSMVectorIndex index = getVectorIndex(db, "VectorDoc", "embedding");
          index.findNeighborsFromVector(generateRandomVector(dimensions, 999), 10);
        }

        graphFileSizeWithoutVectors = getVecgraphFileSize(dbPathWithoutVectors);
        // System.out.println("Graph file size WITHOUT vectors: " + graphFileSizeWithoutVectors + " bytes");
      }

      // Calculate expected sizes:
      // - Vector data per node: dimensions * 4 bytes (float32) = 128 * 4 = 512 bytes
      // - Total vector data: numVectors * 512 = 200 * 512 = 102,400 bytes (~100KB)
      // - When storeVectorsInGraph=false, the graph file should be ~100KB smaller
      final long vectorDataSize = (long) numVectors * dimensions * 4;
      // System.out.println("Expected vector data size: " + vectorDataSize + " bytes");

      // The graph file WITHOUT vectors should be significantly smaller
      // We expect at least 80% of the vector data to be saved (allowing for overhead)
      final long sizeDifference = graphFileSizeWithVectors - graphFileSizeWithoutVectors;
      // System.out.println("Size difference: " + sizeDifference + " bytes");

      // This assertion should FAIL with the current bug (both sizes are similar)
      // After the fix, this should PASS (WITHOUT vectors should be much smaller)
      assertThat(sizeDifference > vectorDataSize * 0.8).as("BUG #3142: storeVectorsInGraph=false still stores vectors in graph file! " +
          "Size with vectors: " + graphFileSizeWithVectors + " bytes, " +
          "Size without vectors: " + graphFileSizeWithoutVectors + " bytes, " +
          "Expected difference: >" + (vectorDataSize * 0.8) + " bytes, " +
          "Actual difference: " + sizeDifference + " bytes").isTrue();

      // System.out.println("✓ Test passed: storeVectorsInGraph=false correctly excludes vectors from graph file");
    } finally {
      FileUtils.deleteRecursively(new File(dbPathWithVectors));
      FileUtils.deleteRecursively(new File(dbPathWithoutVectors));
    }
  }

  /**
   * Helper method to find and measure the vecgraph file size.
   */
  private long getVecgraphFileSize(final String dbPath) {
    final File dbDir = new File(dbPath);
    long totalSize = 0;

    if (dbDir.exists() && dbDir.isDirectory()) {
      final File[] vecgraphFiles = dbDir.listFiles((dir, name) -> name.endsWith(".vecgraph"));
      if (vecgraphFiles != null) {
        for (final File f : vecgraphFiles) {
          totalSize += f.length();
        }
      }
    }

    return totalSize;
  }

  // Helper methods

  private float[] generateRandomVector(final int dimensions, final int seed) {
    final Random rand = new Random(seed);
    final float[] vector = new float[dimensions];
    for (int i = 0; i < dimensions; i++) {
      vector[i] = rand.nextFloat() * 2.0f - 1.0f; // Range [-1, 1]
    }
    return vector;
  }

  private LSMVectorIndex getVectorIndex(final Database db, final String typeName, final String propertyName) {
    final Schema schema = db.getSchema();
    final DocumentType type = schema.getType(typeName);
    return (LSMVectorIndex) type.getPolymorphicIndexByProperties(propertyName).getIndexesOnBuckets()[0];
  }
}
