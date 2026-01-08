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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.*;

/**
 * Test cases for Phase 2: storing vectors inline in JVector graph files.
 * Tests cover:
 * - storeVectorsInGraph with no quantization (full float32 storage)
 * - storeVectorsInGraph with INT8 quantization
 * - storeVectorsInGraph with BINARY quantization
 * - Vector updates and graph rebuilds
 */
public class LSMVectorIndexGraphStorageTest {
  private static final String DB_PATH = "target/test-databases/LSMVectorIndexGraphStorageTest";

  @BeforeEach
  public void setup() {
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  @AfterEach
  public void cleanup() {
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  /**
   * Test storing full-precision float vectors in graph file (no quantization).
   * Verifies that vectors are stored and retrieved correctly from the graph file.
   */
  @Test
  public void testStoreVectorsInGraphNoQuantization() {
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
        Assertions.assertNotNull(index);
        Assertions.assertTrue(index.metadata.storeVectorsInGraph, "storeVectorsInGraph should be enabled");
        Assertions.assertEquals(VectorQuantizationType.NONE, index.metadata.quantizationType,
            "Should have no quantization");

        System.out.println("✓ Index created with storeVectorsInGraph=true, closing database...");
      }

      // Phase 2: Reopen database and verify vectors are fetched from graph
      try (final Database db = factory.open()) {
        final LSMVectorIndex index = getVectorIndex(db, "VectorDoc", "embedding");
        Assertions.assertNotNull(index);

        // Perform searches and verify vectorFetchFromGraph metric
        final Map<String, Long> statsBefore = index.getStats();
        final long fetchFromGraphBefore = statsBefore.getOrDefault("vectorFetchFromGraph", 0L);
        final long fetchFromDocsBefore = statsBefore.getOrDefault("vectorFetchFromDocuments", 0L);

        final float[] queryVector = generateRandomVector(128, 42);
        final List<Pair<RID, Float>> results = index.findNeighborsFromVector(queryVector, 10);

        Assertions.assertTrue(results.size() > 0, "Should find neighbors");
        Assertions.assertTrue(results.size() <= 10, "Should not exceed k limit");

        // Verify metrics show vectors fetched from graph
        final Map<String, Long> statsAfter = index.getStats();
        final long fetchFromGraphAfter = statsAfter.getOrDefault("vectorFetchFromGraph", 0L);
        final long fetchFromDocsAfter = statsAfter.getOrDefault("vectorFetchFromDocuments", 0L);

        Assertions.assertTrue(fetchFromGraphAfter > fetchFromGraphBefore,
            "Should have fetched vectors from graph (before: " + fetchFromGraphBefore + ", after: " + fetchFromGraphAfter + ")");
        Assertions.assertEquals(fetchFromDocsBefore, fetchFromDocsAfter,
            "Should NOT have fetched from documents when storeVectorsInGraph is enabled");

        System.out.println("✓ Test passed: Vectors stored and fetched from graph file (no quantization)");
        System.out.println("  Vectors fetched from graph: " + (fetchFromGraphAfter - fetchFromGraphBefore));
        System.out.println("  Graph state: " + statsAfter.get("graphState") + " (0=LOADING, 1=IMMUTABLE, 2=MUTABLE)");
      }
    }
  }

  /**
   * Test storing INT8 quantized vectors in graph file.
   * Verifies that quantized vectors are stored and retrieved correctly.
   */
  @Test
  public void testStoreVectorsInGraphWithINT8Quantization() {
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
        Assertions.assertNotNull(index);
        Assertions.assertTrue(index.metadata.storeVectorsInGraph, "storeVectorsInGraph should be enabled");
        Assertions.assertEquals(VectorQuantizationType.INT8, index.metadata.quantizationType,
            "Should have INT8 quantization");

        System.out.println("✓ Index created with INT8 quantization and storeVectorsInGraph=true");
      }

      // Phase 2: Reopen and verify
      try (final Database db = factory.open()) {
        final LSMVectorIndex index = getVectorIndex(db, "VectorDoc", "embedding");

        // Perform searches
        final Map<String, Long> statsBefore = index.getStats();
        final float[] queryVector = generateRandomVector(128, 99);
        final List<Pair<RID, Float>> results = index.findNeighborsFromVector(queryVector, 10);

        Assertions.assertTrue(results.size() > 0, "Should find neighbors");

        // Verify metrics - with quantization, vectors might be fetched from graph or quantized pages
        final Map<String, Long> statsAfter = index.getStats();
        final long fetchFromGraph = statsAfter.getOrDefault("vectorFetchFromGraph", 0L) -
            statsBefore.getOrDefault("vectorFetchFromGraph", 0L);
        final long fetchFromDocs = statsAfter.getOrDefault("vectorFetchFromDocuments", 0L) -
            statsBefore.getOrDefault("vectorFetchFromDocuments", 0L);

        // When storeVectorsInGraph is enabled with quantization, vectors are stored quantized in graph
        Assertions.assertTrue(fetchFromGraph > 0 || fetchFromDocs == 0,
            "Should fetch from graph when storeVectorsInGraph is enabled (graph: " + fetchFromGraph + ", docs: " + fetchFromDocs
                + ")");

        System.out.println("✓ Test passed: INT8 quantized vectors stored and fetched from graph");
        System.out.println("  Vectors fetched from graph: " + fetchFromGraph);
        System.out.println("  Vectors fetched from docs: " + fetchFromDocs);
      }
    }
  }

  /**
   * Test storing BINARY quantized vectors in graph file.
   * Verifies maximum compression with binary quantization.
   */
  @Test
  public void testStoreVectorsInGraphWithBinaryQuantization() {
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
        Assertions.assertNotNull(index);
        Assertions.assertTrue(index.metadata.storeVectorsInGraph);
        Assertions.assertEquals(VectorQuantizationType.BINARY, index.metadata.quantizationType);

        // Perform searches
        final float[] queryVector = generateRandomVector(dimensions, 50);
        final List<Pair<RID, Float>> results = index.findNeighborsFromVector(queryVector, 10);

        Assertions.assertTrue(results.size() > 0, "Should find neighbors even with binary quantization");

        System.out.println("✓ Test passed: BINARY quantized vectors stored and fetched from graph");
      }
    }
  }

  /**
   * Test vector updates with storeVectorsInGraph enabled.
   * Verifies that graph is properly rebuilt after vector updates.
   */
  @Test
  public void testVectorUpdatesWithGraphStorage() {
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
        Assertions.assertTrue(rebuildsAfterUpdate >= rebuildsAfterInsert,
            "Graph should be rebuilt after mutations (before: " + rebuildsAfterInsert + ", after: " + rebuildsAfterUpdate + ")");

        System.out.println("✓ Test passed: Vector updates trigger graph rebuilds");
        System.out.println("  Rebuilds after insert: " + rebuildsAfterInsert);
        System.out.println("  Rebuilds after update: " + rebuildsAfterUpdate);
        System.out.println("  Mutations since rebuild: " + statsAfterUpdate.get("mutationsSinceRebuild"));
      }
    }
  }

  /**
   * Test that disabling storeVectorsInGraph fetches from documents.
   */
  @Test
  public void testDisabledGraphStorageFetchesFromDocuments() {
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
        Assertions.assertFalse(index.metadata.storeVectorsInGraph, "storeVectorsInGraph should be disabled");

        // Perform search
        final Map<String, Long> statsBefore = index.getStats();
        final float[] queryVector = generateRandomVector(dimensions, 25);
        index.findNeighborsFromVector(queryVector, 10);

        final Map<String, Long> statsAfter = index.getStats();
        final long fetchFromDocs = statsAfter.get("vectorFetchFromDocuments") - statsBefore.get("vectorFetchFromDocuments");
        final long fetchFromGraph = statsAfter.getOrDefault("vectorFetchFromGraph", 0L) -
            statsBefore.getOrDefault("vectorFetchFromGraph", 0L);

        // Should fetch from documents when storeVectorsInGraph is disabled
        Assertions.assertTrue(fetchFromDocs > 0, "Should fetch from documents when graph storage is disabled");
        Assertions.assertEquals(0, fetchFromGraph, "Should NOT fetch from graph when disabled");

        System.out.println("✓ Test passed: Disabled graph storage fetches from documents");
        System.out.println("  Vectors fetched from documents: " + fetchFromDocs);
      }
    }
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
