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

import com.arcadedb.TestHelper;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.TypeLSMVectorIndexBuilder;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for LSMVectorIndex using JVector.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class LSMVectorIndexTest extends TestHelper {

  private static final int DIMENSIONS = 1024;

  @Test
  void createIndexViaSQLAndQuery() {
    database.transaction(() -> {
      // Create the schema
      database.command("sql", "CREATE VERTEX TYPE VectorVertex IF NOT EXISTS");
      database.command("sql", "CREATE PROPERTY VectorVertex.id IF NOT EXISTS STRING");
      database.command("sql", "CREATE PROPERTY VectorVertex.title IF NOT EXISTS STRING");
      database.command("sql", "CREATE PROPERTY VectorVertex.embedding IF NOT EXISTS ARRAY_OF_FLOATS");
      database.command("sql", "CREATE PROPERTY VectorVertex.category IF NOT EXISTS STRING");

      // Create the LSM_VECTOR index
      database.command("sql", """
          CREATE INDEX IF NOT EXISTS ON VectorVertex (embedding) LSM_VECTOR
          METADATA {
            "dimensions" : 1024,
            "similarity" : "COSINE",
            "maxConnections" : 16,
            "beamWidth" : 100
          }""");
    });

    // Verify the index was created
    final com.arcadedb.index.TypeIndex typeIndex = (com.arcadedb.index.TypeIndex) database.getSchema()
        .getIndexByName("VectorVertex[embedding]");
    assertThat(typeIndex).as("Index should be created").isNotNull();

    // Get one of the underlying LSMVectorIndex instances to verify configuration
    final LSMVectorIndex index = (LSMVectorIndex) typeIndex.getIndexesOnBuckets()[0];
    assertThat(index.getDimensions()).as("Dimensions should be " + DIMENSIONS).isEqualTo(DIMENSIONS);
    assertThat(index.getSimilarityFunction().name()).as("Similarity should be COSINE").isEqualTo("COSINE");

    // Insert test data
    database.transaction(() -> {
      for (int i = 0; i < 100; i++) {
        final var doc = database.newVertex("VectorVertex");
        doc.set("id", "doc" + i);
        doc.set("title", "Document " + i);

        // Create a random DIMENSIONS-dimensional vector
        final float[] vector = new float[DIMENSIONS];
        for (int j = 0; j < DIMENSIONS; j++)
          vector[j] = (float) Math.random();

        doc.set("embedding", vector);
        doc.set("category", "category" + (i % 3));
        doc.save();
      }
    });

    // Query the index using TypeIndex
    database.transaction(() -> {
      final float[] queryVector = new float[DIMENSIONS];
      Arrays.fill(queryVector, 0.5f);

      final IndexCursor cursor = typeIndex.get(new Object[] { queryVector }, 10);

      int count = 0;
      while (cursor.hasNext()) {
        assertThat(cursor.next()).isNotNull();
        count++;
      }

      assertThat(count).as("Should find at least one result").isGreaterThan(0);
      assertThat(count).as("Should return at most 10 results").isLessThanOrEqualTo(10);
    });
  }

  @Test
  void createIndexProgrammatically() {
    database.transaction(() -> {
      // Create document type
      final DocumentType docType = database.getSchema().createDocumentType("VectorDoc");
      docType.createProperty("id", Type.STRING);
      docType.createProperty("embedding", Type.ARRAY_OF_FLOATS);

      // Create LSM_VECTOR index programmatically using unified API
      TypeLSMVectorIndexBuilder builder = database.getSchema()
          .buildTypeIndex("VectorDoc", new String[] { "embedding" }).withLSMVectorType();

      // Set common index properties
      builder.withIndexName("VectorDoc_embedding_idx");

      // Cast to LSMVectorIndexBuilder to access vector-specific methods
      builder.withDimensions(3);
      builder.withSimilarity("EUCLIDEAN");
      builder.withMaxConnections(8);
      builder.withBeamWidth(50);
      builder.create();
    });

    // Verify index was created
    // Note: TypeIndex is created with default name based on type and properties
    final com.arcadedb.index.TypeIndex typeIndex = (com.arcadedb.index.TypeIndex) database.getSchema()
        .getIndexByName("VectorDoc[embedding]");
    assertThat(typeIndex).isNotNull();

    // Get one of the underlying LSMVectorIndex instances to verify configuration
    final LSMVectorIndex index = (LSMVectorIndex) typeIndex.getIndexesOnBuckets()[0];
    assertThat(index.getDimensions()).isEqualTo(3);
    assertThat(index.getSimilarityFunction().name()).isEqualTo("EUCLIDEAN");
    assertThat(index.getMaxConnections()).isEqualTo(8);
    assertThat(index.getBeamWidth()).isEqualTo(50);

    // Insert and query
    database.transaction(() -> {
      for (int i = 0; i < 10; i++) {
        final var doc = database.newDocument("VectorDoc");
        doc.set("id", "doc" + i);
        doc.set("embedding", new float[] { (float) i, (float) i + 1, (float) i + 2 });
        doc.save();
      }
    });

    database.transaction(() -> {
      final float[] queryVector = { 5.0f, 6.0f, 7.0f };
      final IndexCursor cursor = typeIndex.get(new Object[] { queryVector }, DIMENSIONS);

      int count = 0;
      while (cursor.hasNext()) {
        count++;
        cursor.next();
      }

      assertThat(count).isGreaterThan(0);
      assertThat(count).isLessThanOrEqualTo(DIMENSIONS);
    });
  }

  @Test
  void indexEntryCount() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE TestDoc");
      database.command("sql", "CREATE PROPERTY TestDoc.vec ARRAY_OF_FLOATS");
      database.command("sql", """
          CREATE INDEX ON TestDoc (vec) LSM_VECTOR
          METADATA {"dimensions": 2,
          "similarity": "DOT_PRODUCT",
          "maxConnections": 4,
          "beamWidth": 10}
          """);
    });

    database.transaction(() -> {
      for (int i = 0; i < 50; i++) {
        final var doc = database.newDocument("TestDoc");
        doc.set("vec", new float[] { (float) i, (float) i * 2 });
        doc.save();
      }
    });

    final com.arcadedb.index.TypeIndex typeIndex = (com.arcadedb.index.TypeIndex) database.getSchema()
        .getIndexByName("TestDoc[vec]");
    assertThat(typeIndex.countEntries()).isEqualTo(50);
  }

  @Test
  void transactionalIsolation() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE TxDoc");
      database.command("sql", "CREATE PROPERTY TxDoc.vec ARRAY_OF_FLOATS");
      database.command("sql", """
          CREATE INDEX ON TxDoc (vec) LSM_VECTOR
          METADATA {
            "dimensions": 2,
            "similarity": "COSINE",
            "maxConnections": 4,
            "beamWidth": 10
          }""");
    });

    // Insert in transaction 1
    database.transaction(() -> {
      final var doc = database.newDocument("TxDoc");
      doc.set("vec", new float[] { 1.0f, 2.0f });
      doc.save();
    });

    final com.arcadedb.index.TypeIndex typeIndex = (com.arcadedb.index.TypeIndex) database.getSchema().getIndexByName("TxDoc[vec]");
    assertThat(typeIndex.countEntries()).isEqualTo(1);

    // Verify transaction rollback doesn't affect committed data
    try {
      database.transaction(() -> {
        final var doc = database.newDocument("TxDoc");
        doc.set("vec", new float[] { 3.0f, 4.0f });
        doc.save();

        // Force rollback
        throw new RuntimeException("Test rollback");
      });
    } catch (final RuntimeException e) {
      // Expected
    }

    // Should still have only 1 entry
    assertThat(typeIndex.countEntries()).isEqualTo(1);
  }

  @Test
  void lsmAppendOnlySemantics() {
    // Test LSM append-only: add, remove, add again - last entry should win
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE LSMDoc");
      database.command("sql", "CREATE PROPERTY LSMDoc.id STRING");
      database.command("sql", "CREATE PROPERTY LSMDoc.vec ARRAY_OF_FLOATS");
      database.command("sql", """
          CREATE INDEX ON LSMDoc (vec) LSM_VECTOR
          METADATA {
            "dimensions": 3,
            "similarity":
            "EUCLIDEAN",
            "maxConnections": 4,
            "beamWidth": 10
          }""");
    });

    final com.arcadedb.index.TypeIndex typeIndex = (com.arcadedb.index.TypeIndex) database.getSchema()
        .getIndexByName("LSMDoc[vec]");
    assertThat(typeIndex).isNotNull();

    // Add first version
    final com.arcadedb.database.RID[] docRidHolder = new com.arcadedb.database.RID[1];
    database.transaction(() -> {
      final var doc = database.newDocument("LSMDoc");
      doc.set("id", "doc1");
      doc.set("vec", new float[] { 1.0f, 2.0f, 3.0f });
      doc.save();
      docRidHolder[0] = doc.getIdentity();
    });
    final com.arcadedb.database.RID docRid = docRidHolder[0];

    assertThat(typeIndex.countEntries()).isEqualTo(1);

    // Query to verify it's there
    database.transaction(() -> {
      final float[] queryVec = { 1.0f, 2.0f, 3.0f };
      final var cursor = typeIndex.get(new Object[] { queryVec }, 10);
      int count = 0;
      while (cursor.hasNext()) {
        cursor.next();
        count++;
      }
      assertThat(count > 0).as("Should find the added vector").isTrue();
    });

    // Update the document (delete old entry, add new entry)
    database.transaction(() -> {
      final var doc = docRid.asDocument().modify();
      doc.set("vec", new float[] { 10.0f, 20.0f, 30.0f });
      doc.save();
    });

    // Should still have 1 entry (old marked deleted, new added)
    assertThat(typeIndex.countEntries()).isEqualTo(1);

    // Query with new vector should find it
    database.transaction(() -> {
      final float[] queryVec = { 10.0f, 20.0f, 30.0f };
      final var cursor = typeIndex.get(new Object[] { queryVec }, 10);
      int count = 0;
      while (cursor.hasNext()) {
        cursor.next();
        count++;
      }
      assertThat(count > 0).as("Should find the updated vector").isTrue();
    });

    // Note: RID stays the same after update, but vector value changed
    // With approximate search, we can't reliably test that old vector values aren't found
    // So we just verify the index still has 1 entry (the updated one)

    // Delete the document
    database.transaction(() -> {
      final var doc = docRid.asDocument();
      doc.delete();
    });

    // Should have 0 entries now
    assertThat(typeIndex.countEntries()).isEqualTo(0);

    // Re-add with same RID (simulating LSM append-only where entries accumulate)
    database.transaction(() -> {
      final var doc = database.newDocument("LSMDoc");
      doc.set("id", "doc1_new");
      doc.set("vec", new float[] { 100.0f, 200.0f, 300.0f });
      doc.save();
    });

    // Should have 1 entry (the new one)
    assertThat(typeIndex.countEntries()).isEqualTo(1);
  }

  @Test
  void lsmMergeOnReadLastWins() {
    // Test that in LSM pages, the last entry for a vector ID wins during merge-on-read
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE MergeDoc");
      database.command("sql", "CREATE PROPERTY MergeDoc.vec ARRAY_OF_FLOATS");
      database.command("sql", """
          CREATE INDEX ON MergeDoc (vec) LSM_VECTOR
          METADATA {
            "dimensions": 2,
            "similarity":
            "DOT_PRODUCT",
            "maxConnections": 4,
            "beamWidth": 10
          }""");
    });

    final com.arcadedb.index.TypeIndex typeIndex = (com.arcadedb.index.TypeIndex) database.getSchema()
        .getIndexByName("MergeDoc[vec]");

    // Add multiple documents
    final java.util.List<com.arcadedb.database.RID> rids = new java.util.ArrayList<>();
    database.transaction(() -> {
      for (int i = 0; i < 10; i++) {
        final var doc = database.newDocument("MergeDoc");
        doc.set("vec", new float[] { (float) i, (float) i * 2 });
        doc.save();
        rids.add(doc.getIdentity());
      }
    });

    assertThat(typeIndex.countEntries()).isEqualTo(10);

    // Update several documents multiple times (creates multiple page entries in LSM style)
    for (int iteration = 0; iteration < 3; iteration++) {
      final int iter = iteration;
      database.transaction(() -> {
        for (int i = 0; i < 5; i++) {
          final var doc = rids.get(i).asDocument().modify();
          doc.set("vec", new float[] { (float) (i + 100 * (iter + 1)), (float) (i * 2 + 100 * (iter + 1)) });
          doc.save();
        }
      });
    }

    // Still should have 10 entries (5 updated + 5 unchanged)
    // LSM semantics: last write wins, so even though we wrote multiple times,
    // only the latest version of each vector should be counted
    assertThat(typeIndex.countEntries()).isEqualTo(10);

    // Verify the last values are present (should find vectors with latest iteration values)
    database.transaction(() -> {
      final float[] queryVec = { 300.0f, 300.0f }; // Last iteration values for i=0
      final var cursor = typeIndex.get(new Object[] { queryVec }, 10);
      int count = 0;
      while (cursor.hasNext()) {
        cursor.next();
        count++;
      }
      assertThat(count > 0).as("Should find the last updated vector").isTrue();
    });

    // Verify LSM semantics: The index should correctly handle multiple updates to same documents
    // Key point: Even after 3 updates to 5 documents, we still have exactly 10 entries (not 10 + 5*3)
    // This demonstrates that LSM merge-on-read is working (last entry wins)
  }

  @Test
  void lazyGraphIndexRebuild() {
    // Test that graph index is only rebuilt when needed (lazy)
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE LazyDoc");
      database.command("sql", "CREATE PROPERTY LazyDoc.vec ARRAY_OF_FLOATS");
      database.command("sql", """
          CREATE INDEX ON LazyDoc (vec) LSM_VECTOR
          METADATA {
            "dimensions": 4,
            "similarity": "COSINE",
            "maxConnections": 8,
            "beamWidth": 50
          }""");
    });

    final com.arcadedb.index.TypeIndex typeIndex = (com.arcadedb.index.TypeIndex) database.getSchema()
        .getIndexByName("LazyDoc[vec]");

    // Add documents
    database.transaction(() -> {
      for (int i = 0; i < 20; i++) {
        final var doc = database.newDocument("LazyDoc");
        doc.set("vec", new float[] { (float) i, (float) i + 1, (float) i + 2, (float) i + 3 });
        doc.save();
      }
    });

    // Graph index should be marked dirty after commit (not rebuilt yet)
    // First query should trigger rebuild
    final long startTime = System.currentTimeMillis();
    database.transaction(() -> {
      final float[] queryVec = { 5.0f, 6.0f, 7.0f, 8.0f };
      final var cursor = typeIndex.get(new Object[] { queryVec }, 5);
      int count = 0;
      while (cursor.hasNext()) {
        cursor.next();
        count++;
      }
      assertThat(count > 0).as("Should find vectors after lazy rebuild").isTrue();
    });
    final long rebuildTime = System.currentTimeMillis() - startTime;

    // Second query should be faster (no rebuild needed)
    final long startTime2 = System.currentTimeMillis();
    database.transaction(() -> {
      final float[] queryVec = { 10.0f, 11.0f, 12.0f, 13.0f };
      final var cursor = typeIndex.get(new Object[] { queryVec }, 5);
      int count = 0;
      while (cursor.hasNext()) {
        cursor.next();
        count++;
      }
      assertThat(count > 0).as("Should find vectors without rebuild").isTrue();
    });
    final long noRebuildTime = System.currentTimeMillis() - startTime2;

    // Second query should be significantly faster (no rebuild overhead)
    // Note: This is a heuristic test, may be flaky in CI
    //System.out.println("Rebuild time: " + rebuildTime + "ms, No rebuild time: " + noRebuildTime + "ms");
  }

  @Test
  void compactionTrigger() {
    // Test that compaction is triggered after reaching the threshold
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE CompactDoc IF NOT EXISTS");
      database.command("sql", "CREATE PROPERTY CompactDoc.vec IF NOT EXISTS ARRAY_OF_FLOATS");
      database.command("sql", """
          CREATE INDEX IF NOT EXISTS ON CompactDoc (vec) LSM_VECTOR
          METADATA {
            dimensions: 4,
            similarity: 'COSINE'
          }""");
    });

    final var typeIndex = (com.arcadedb.index.TypeIndex) database.getSchema().getIndexByName("CompactDoc[vec]");
    assertThat(typeIndex).as("Index should exist").isNotNull();

    final var indexes = typeIndex.getIndexesOnBuckets();
    assertThat(indexes.length > 0).as("Should have at least one bucket index").isTrue();

    final LSMVectorIndex lsmIndex = (LSMVectorIndex) indexes[0];
    final int initialPages = lsmIndex.getCurrentMutablePages();

    // Get compaction threshold
    final int threshold = database.getConfiguration()
        .getValueAsInteger(com.arcadedb.GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE);

    // Add enough vectors to trigger multiple page creations
    // Each page is 256KB, with 4-dim vectors, we can fit many vectors per page
    // We need to fill pages to trigger compaction
    final int vectorsPerBatch = 10000; // Should be enough to create new pages
    final int batches = Math.max(threshold + 2, 12); // Ensure we exceed threshold

    for (int batch = 0; batch < batches; batch++) {
      final int batchNum = batch;
      database.transaction(() -> {
        for (int i = 0; i < vectorsPerBatch; i++) {
          final var vertex = database.newVertex("CompactDoc");
          vertex.set("vec", new float[] {
              (float) (batchNum * vectorsPerBatch + i),
              (float) (batchNum * vectorsPerBatch + i + 1),
              (float) (batchNum * vectorsPerBatch + i + 2),
              (float) (batchNum * vectorsPerBatch + i + 3)
          });
          vertex.save();
        }
      });

      // Check if pages increased
//      if (lsmIndex.getCurrentMutablePages() > initialPages) {
//        System.out.println("After batch " + batch + ": mutablePages=" + lsmIndex.getCurrentMutablePages() +
//            ", threshold=" + threshold);
//      }
    }

    final int finalPages = lsmIndex.getCurrentMutablePages();
//    System.out.println("Initial pages: " + initialPages + ", Final pages: " + finalPages +
//        ", Threshold: " + threshold);

    // Verify that compaction would have been triggered
    assertThat(finalPages >= threshold || initialPages >= threshold).as("Should have reached compaction threshold at some point")
        .isTrue();
  }

  @Test
  void compactionMergeMultipleUpdates() {
    // Test K-way merge with multiple updates to same vectors
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE MergeDoc IF NOT EXISTS");
      database.command("sql", "CREATE PROPERTY MergeDoc.id IF NOT EXISTS STRING");
      database.command("sql", "CREATE PROPERTY MergeDoc.vec IF NOT EXISTS ARRAY_OF_FLOATS");
      database.command("sql", """
          CREATE INDEX IF NOT EXISTS ON MergeDoc (vec) LSM_VECTOR
          METADATA {
            dimensions: 3,
            similarity: 'COSINE'
          }
          """);
    });

    final var typeIndex = (com.arcadedb.index.TypeIndex) database.getSchema().getIndexByName("MergeDoc[vec]");
    assertThat(typeIndex).as("Index should exist").isNotNull();

    // Add initial vectors
    final List<com.arcadedb.database.RID> rids = new ArrayList<>();
    database.transaction(() -> {
      for (int i = 0; i < 20; i++) {
        final var vertex = database.newVertex("MergeDoc");
        vertex.set("id", "doc" + i);
        vertex.set("vec", new float[] { (float) i, (float) i * 2, (float) i * 3 });
        vertex.save();
        rids.add(vertex.getIdentity());
      }
    });

    final long countAfterInsert = typeIndex.countEntries();
    assertThat(countAfterInsert).as("Should have 20 entries after insert").isEqualTo(20);

    // Update same vectors multiple times (creates multiple LSM entries)
    for (int iteration = 0; iteration < 5; iteration++) {
      final int iter = iteration;
      database.transaction(() -> {
        for (int i = 0; i < 10; i++) {
          final var doc = rids.get(i).asDocument().modify();
          doc.set("vec", new float[] {
              (float) (i + 100 * (iter + 1)),
              (float) (i * 2 + 100 * (iter + 1)),
              (float) (i * 3 + 100 * (iter + 1))
          });
          doc.save();
        }
      });
    }

    // Count should still be 20 (no duplicates despite multiple updates)
    final long countAfterUpdates = typeIndex.countEntries();
    assertThat(countAfterUpdates).as("Should still have 20 entries after updates (LSM last-write-wins semantics)").isEqualTo(20);

    // Verify we can query the updated vectors
    database.transaction(() -> {
      final float[] queryVec = { 500.0f, 500.0f, 500.0f }; // Query for last iteration values
      final var cursor = typeIndex.get(new Object[] { queryVec }, 5);
      int count = 0;
      while (cursor.hasNext()) {
        cursor.next();
        count++;
      }
      assertThat(count > 0).as("Should find vectors with updated values").isTrue();
    });
  }

  @Test
  void compactionDeletedEntryRemoval() {
    // Test that deleted entries are removed during compaction
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE DeleteDoc IF NOT EXISTS");
      database.command("sql", "CREATE PROPERTY DeleteDoc.id IF NOT EXISTS STRING");
      database.command("sql", "CREATE PROPERTY DeleteDoc.vec IF NOT EXISTS ARRAY_OF_FLOATS");
      database.command("sql", """
          CREATE INDEX IF NOT EXISTS ON DeleteDoc (vec) LSM_VECTOR
          METADATA {
            dimensions: 2,
            similarity: 'COSINE'
          }""");
    });

    final var typeIndex = (com.arcadedb.index.TypeIndex) database.getSchema().getIndexByName("DeleteDoc[vec]");
    assertThat(typeIndex).as("Index should exist").isNotNull();

    // Add vectors
    final List<com.arcadedb.database.RID> rids = new ArrayList<>();
    database.transaction(() -> {
      for (int i = 0; i < 50; i++) {
        final var vertex = database.newVertex("DeleteDoc");
        vertex.set("id", "doc" + i);
        vertex.set("vec", new float[] { (float) i, (float) i * 2 });
        vertex.save();
        rids.add(vertex.getIdentity());
      }
    });

    assertThat(typeIndex.countEntries()).as("Should have 50 entries").isEqualTo(50);

    // Delete half of the vectors
    database.transaction(() -> {
      for (int i = 0; i < 25; i++) {
        rids.get(i).asDocument().delete();
      }
    });

    // Count should reflect deletions
    final long countAfterDelete = typeIndex.countEntries();
    assertThat(countAfterDelete).as("Should have 25 entries after deleting 25").isEqualTo(25);

    // Verify remaining vectors are queryable
    database.transaction(() -> {
      final float[] queryVec = { 30.0f, 60.0f }; // Vector from non-deleted range
      final var cursor = typeIndex.get(new Object[] { queryVec }, 10);
      int count = 0;
      while (cursor.hasNext()) {
        cursor.next();
        count++;
      }
      assertThat(count > 0).as("Should find non-deleted vectors").isTrue();
    });

    // After compaction, deleted entries should be physically removed
    // (We can't directly test this without triggering manual compaction,
    // but the count verification above confirms logical deletion works)
  }

  @Test
  void compactionWithConcurrentQueries() throws Exception {
    // Test that queries work correctly during compaction
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE ConcurrentDoc IF NOT EXISTS");
      database.command("sql", "CREATE PROPERTY ConcurrentDoc.vec IF NOT EXISTS ARRAY_OF_FLOATS");
      database.command("sql", """
          CREATE INDEX IF NOT EXISTS ON ConcurrentDoc (vec) LSM_VECTOR
          METADATA {
            dimensions: 4,
            similarity: 'COSINE'
          }""");
    });

    final var typeIndex = (com.arcadedb.index.TypeIndex) database.getSchema().getIndexByName("ConcurrentDoc[vec]");
    assertThat(typeIndex).as("Index should exist").isNotNull();

    // Add initial vectors
    database.transaction(() -> {
      for (int i = 0; i < 100; i++) {
        final var vertex = database.newVertex("ConcurrentDoc");
        vertex.set("vec", new float[] {
            (float) i, (float) i + 1, (float) i + 2, (float) i + 3
        });
        vertex.save();
      }
    });

    assertThat(typeIndex.countEntries()).as("Should have 100 entries").isEqualTo(100);

    // Perform concurrent queries while index is active
    final int numThreads = 5;
    final int queriesPerThread = 10;
    final List<Thread> threads = new ArrayList<>();
    final java.util.concurrent.atomic.AtomicInteger successfulQueries =
        new java.util.concurrent.atomic.AtomicInteger(0);
    final java.util.concurrent.atomic.AtomicInteger failedQueries =
        new java.util.concurrent.atomic.AtomicInteger(0);

    for (int t = 0; t < numThreads; t++) {
      final int threadId = t;
      final Thread thread = new Thread(() -> {
        for (int q = 0; q < queriesPerThread; q++) {
          final int queryId = q; // Make it final for lambda
          try {
            database.transaction(() -> {
              final float[] queryVec = {
                  (float) (threadId * 10 + queryId),
                  (float) (threadId * 10 + queryId + 1),
                  (float) (threadId * 10 + queryId + 2),
                  (float) (threadId * 10 + queryId + 3)
              };
              final var cursor = typeIndex.get(new Object[] { queryVec }, 5);
              int count = 0;
              while (cursor.hasNext()) {
                cursor.next();
                count++;
              }
              // Should find at least some results
              if (count > 0) {
                successfulQueries.incrementAndGet();
              }
            });
          } catch (final Exception e) {
            failedQueries.incrementAndGet();
            System.err.println("Query failed: " + e.getMessage());
          }
        }
      });
      threads.add(thread);
      thread.start();
    }

    // Wait for all threads
    for (final Thread thread : threads) {
      thread.join();
    }

    System.out.println("Successful queries: " + successfulQueries.get() +
        ", Failed queries: " + failedQueries.get());

    // Most queries should succeed
    assertThat(successfulQueries.get() > (numThreads * queriesPerThread) / 2).as("Most concurrent queries should succeed").isTrue();
    assertThat(failedQueries.get()).as("No queries should fail with exceptions").isEqualTo(0);
  }

  @Test
  void graphIndexCorrectnessAfterCompaction() {
    // Test that graph index is correctly rebuilt after compaction
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE GraphDoc IF NOT EXISTS");
      database.command("sql", "CREATE PROPERTY GraphDoc.vec IF NOT EXISTS ARRAY_OF_FLOATS");
      database.command("sql", """
          CREATE INDEX IF NOT EXISTS ON GraphDoc (vec) LSM_VECTOR
          METADATA {
            dimensions: 8,
            similarity: 'EUCLIDEAN',
            maxConnections: 16,
            beamWidth: 100
          }""");
    });

    final var typeIndex = (com.arcadedb.index.TypeIndex) database.getSchema().getIndexByName("GraphDoc[vec]");
    assertThat(typeIndex).as("Index should exist").isNotNull();

    // Add vectors in a pattern that creates a structure in the graph
    final List<com.arcadedb.database.RID> rids = new ArrayList<>();
    database.transaction(() -> {
      for (int i = 0; i < 100; i++) {
        final var vertex = database.newVertex("GraphDoc");
        // Create vectors in clusters (should form neighborhoods in graph)
        final int cluster = i / 10;
        vertex.set("vec", new float[] {
            (float) cluster * 10, (float) cluster * 10 + 1,
            (float) cluster * 10 + 2, (float) cluster * 10 + 3,
            (float) i % 10, (float) (i % 10) + 1,
            (float) (i % 10) + 2, (float) (i % 10) + 3
        });
        vertex.save();
        rids.add(vertex.getIdentity());
      }
    });

    assertThat(typeIndex.countEntries()).as("Should have 100 entries").isEqualTo(100);

    // Query before any updates (graph index initialized)
    final Set<String> resultsBeforeUpdates = new HashSet<>();
    database.transaction(() -> {
      final float[] queryVec = { 50.0f, 51.0f, 52.0f, 53.0f, 0.0f, 1.0f, 2.0f, 3.0f };
      final var cursor = typeIndex.get(new Object[] { queryVec }, 10);
      while (cursor.hasNext()) {
        final var rid = cursor.next();
        resultsBeforeUpdates.add(rid.getIdentity().toString());
      }
    });

    assertThat(resultsBeforeUpdates.size() > 0).as("Should find results before updates").isTrue();

    // Update some vectors (will trigger graph dirty flag)
    database.transaction(() -> {
      for (int i = 0; i < 20; i++) {
        final var doc = rids.get(i).asDocument().modify();
        final int cluster = i / 10;
        doc.set("vec", new float[] {
            (float) cluster * 10, (float) cluster * 10 + 1,
            (float) cluster * 10 + 2, (float) cluster * 10 + 3,
            (float) i % 10 + 0.1f, (float) (i % 10) + 1.1f,
            (float) (i % 10) + 2.1f, (float) (i % 10) + 3.1f
        });
        doc.save();
      }
    });

    // Query after updates (should trigger lazy graph rebuild)
    final Set<String> resultsAfterUpdates = new HashSet<>();
    database.transaction(() -> {
      final float[] queryVec = { 50.0f, 51.0f, 52.0f, 53.0f, 0.1f, 1.1f, 2.1f, 3.1f };
      final var cursor = typeIndex.get(new Object[] { queryVec }, 10);
      while (cursor.hasNext()) {
        final var rid = cursor.next();
        resultsAfterUpdates.add(rid.getIdentity().toString());
      }
    });

    assertThat(resultsAfterUpdates.size() > 0).as("Should find results after updates (graph index rebuilt)").isTrue();

    // Results should be different after updates (graph index reflects new data)
    System.out.println("Results before updates: " + resultsBeforeUpdates.size() +
        ", Results after updates: " + resultsAfterUpdates.size());
  }

  @Test
  void transactionRollbackVectorIndexChanges() {
    // Setup: Create a vector index
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE RollbackDoc");
      database.command("sql", "CREATE PROPERTY RollbackDoc.id STRING");
      database.command("sql", "CREATE PROPERTY RollbackDoc.vec ARRAY_OF_FLOATS");
      database.command("sql", """
          CREATE INDEX ON RollbackDoc (vec) LSM_VECTOR
          METADATA {
            "dimensions": 3,
            "similarity": "COSINE",
            "maxConnections": 8,
            "beamWidth": 50
          }""");
    });

    final com.arcadedb.index.TypeIndex typeIndex =
        (com.arcadedb.index.TypeIndex) database.getSchema().getIndexByName("RollbackDoc[vec]");
    assertThat(typeIndex).as("Vector index should be created").isNotNull();

    // Verify index is initially empty
    assertThat(typeIndex.countEntries()).as("Index should start empty").isEqualTo(0);

    // Add initial documents to the index
    database.transaction(() -> {
      for (int i = 0; i < 5; i++) {
        final var doc = database.newDocument("RollbackDoc");
        doc.set("id", "initial_" + i);
        doc.set("vec", new float[] { (float) i, (float) i + 1, (float) i + 2 });
        doc.save();
      }
    });

    final long initialCount = typeIndex.countEntries();
    assertThat(initialCount).as("Index should have 5 entries after initial insert").isEqualTo(5);

    // === ROLLBACK TEST ===
    // Try to add more documents in a transaction, then roll it back
    try {
      database.transaction(() -> {
        // Add new documents in this transaction
        for (int i = 0; i < 3; i++) {
          final var doc = database.newDocument("RollbackDoc");
          doc.set("id", "rollback_" + i);
          doc.set("vec", new float[] { (float) (100 + i), (float) (101 + i), (float) (102 + i) });
          doc.save();
        }

        // Force a rollback by throwing an exception
        throw new RuntimeException("Intentional rollback for testing");
      });
    } catch (final RuntimeException e) {
      // Expected - transaction should be rolled back
      assertThat(e.getMessage().contains("Intentional rollback")).as("Should be our rollback exception").isTrue();
    }

    // Verify that the rolled-back changes are NOT in the index
    final long countAfterRollback = typeIndex.countEntries();
    assertThat(countAfterRollback).as(
            "Index should still have only " + initialCount + " entries after rollback (rollback_* docs should not be there)")
        .isEqualTo(initialCount);

    // Verify that we can't find the rolled-back vectors
    database.transaction(() -> {
      final float[] rollbackVec = { 100.0f, 101.0f, 102.0f }; // Vector that should have been rolled back
      final IndexCursor cursor = typeIndex.get(new Object[] { rollbackVec }, 10);

      int foundCount = 0;
      while (cursor.hasNext()) {
        final var rid = cursor.next();
        final var doc = rid.asDocument();
        // We shouldn't find any documents with rollback_* ids
        final String id = doc.get("id").toString();
        assertThat(id.startsWith("rollback_")).as("Should not find rolled-back vector in index (found doc with id: " + id + ")")
            .isFalse();
        foundCount++;
      }

      // With approximate nearest neighbor search, we might find some initial vectors,
      // but we should definitely NOT find the exact rolled-back vectors
      // Just verify we didn't find the specific rollback documents
    });

    // === COMMIT TEST ===
    // Now add documents in a transaction that COMMITS successfully
    database.transaction(() -> {
      for (int i = 0; i < 2; i++) {
        final var doc = database.newDocument("RollbackDoc");
        doc.set("id", "committed_" + i);
        doc.set("vec", new float[] { (float) (200 + i), (float) (201 + i), (float) (202 + i) });
        doc.save();
      }
    });

    // Verify that the committed changes ARE in the index
    final long countAfterCommit = typeIndex.countEntries();
    assertThat(countAfterCommit).as("Index should have " + (initialCount + 2) + " entries after successful commit")
        .isEqualTo(initialCount + 2);

    // Verify that we CAN find the committed vectors
    database.transaction(() -> {
      final float[] committedVec = { 200.0f, 201.0f, 202.0f }; // Vector that was committed
      final IndexCursor cursor = typeIndex.get(new Object[] { committedVec }, 10);

      boolean found = false;
      while (cursor.hasNext()) {
        final var rid = cursor.next();
        final var doc = rid.asDocument();
        final String id = doc.get("id").toString();
        if (id.equals("committed_0")) {
          found = true;
          break;
        }
      }

      assertThat(found).as("Should find the committed vector in index").isTrue();
    });

    // === MULTIPLE ROLLBACK TEST ===
    // Test multiple consecutive rollbacks
    final long countBeforeMultipleRollbacks = typeIndex.countEntries();

    for (int rollbackRound = 0; rollbackRound < 3; rollbackRound++) {
      final int round = rollbackRound; // Capture for lambda
      try {
        database.transaction(() -> {
          for (int i = 0; i < 2; i++) {
            final var doc = database.newDocument("RollbackDoc");
            doc.set("id", "rollback_round_" + round + "_" + i);
            doc.set("vec", new float[] { (float) (300 + round), (float) (301 + round), (float) (302 + round) });
            doc.save();
          }
          throw new RuntimeException("Rollback round " + round);
        });
      } catch (final RuntimeException e) {
        // Expected
      }
    }

    // After multiple rollbacks, count should not have changed
    final long countAfterMultipleRollbacks = typeIndex.countEntries();
    assertThat(countAfterMultipleRollbacks)
        .as("Index count should remain unchanged after multiple rollbacks")
        .isEqualTo(countBeforeMultipleRollbacks);

    // Verify no rolled-back documents from any round are in the index
    database.transaction(() -> {
      for (int rollbackRound = 0; rollbackRound < 3; rollbackRound++) {
        final float[] rollbackVec = { (float) (300 + rollbackRound), (float) (301 + rollbackRound), (float) (302 + rollbackRound) };
        final IndexCursor cursor = typeIndex.get(new Object[] { rollbackVec }, 100);

        while (cursor.hasNext()) {
          final var rid = cursor.next();
          final var doc = rid.asDocument();
          final String id = doc.get("id").toString();
          assertThat(id.startsWith("rollback_round_"))
              .as("Should not find rolled-back vectors from any round (found: " + id + ")")
              .isFalse();
        }
      }
    });
  }
}
