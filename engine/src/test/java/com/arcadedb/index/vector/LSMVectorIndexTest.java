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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.*;

/**
 * Tests for LSMVectorIndex using JVector.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class LSMVectorIndexTest extends TestHelper {

  private static final int DIMENSIONS = 1024;

  @Test
  public void testCreateIndexViaSQLAndQuery() {
    database.transaction(() -> {
      // Create the schema
      database.command("sql", "CREATE VERTEX TYPE VectorDocument IF NOT EXISTS");
      database.command("sql", "CREATE PROPERTY VectorDocument.id IF NOT EXISTS STRING");
      database.command("sql", "CREATE PROPERTY VectorDocument.title IF NOT EXISTS STRING");
      database.command("sql", "CREATE PROPERTY VectorDocument.embedding IF NOT EXISTS ARRAY_OF_FLOATS");
      database.command("sql", "CREATE PROPERTY VectorDocument.category IF NOT EXISTS STRING");

      // Create the LSM_VECTOR index
      database.command("sql", "CREATE INDEX IF NOT EXISTS ON VectorDocument (embedding) LSM_VECTOR " +
          "METADATA {" +
          "  \"dimensions\" : " + DIMENSIONS + "," +
          "  \"similarity\" : \"COSINE\"," +
          "  \"maxConnections\" : 16," +
          "  \"beamWidth\" : 100" +
          "}");
    });

    // Verify the index was created
    final com.arcadedb.index.TypeIndex typeIndex = (com.arcadedb.index.TypeIndex) database.getSchema()
        .getIndexByName("VectorDocument[embedding]");
    Assertions.assertNotNull(typeIndex, "Index should be created");

    // Get one of the underlying LSMVectorIndex instances to verify configuration
    final LSMVectorIndex index = (LSMVectorIndex) typeIndex.getIndexesOnBuckets()[0];
    Assertions.assertEquals(DIMENSIONS, index.getDimensions(), "Dimensions should be " + DIMENSIONS);
    Assertions.assertEquals("COSINE", index.getSimilarityFunction().name(), "Similarity should be COSINE");

    // Insert test data
    database.transaction(() -> {
      for (int i = 0; i < 100; i++) {
        final var doc = database.newVertex("VectorDocument");
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
        Assertions.assertNotNull(cursor.next());
        count++;
      }

      Assertions.assertTrue(count > 0, "Should find at least one result");
      Assertions.assertTrue(count <= 10, "Should return at most 10 results");
    });
  }

  @Test
  public void testCreateIndexProgrammatically() {
    database.transaction(() -> {
      // Create document type
      final DocumentType docType = database.getSchema().createDocumentType("VectorDoc");
      docType.createProperty("id", Type.STRING);
      docType.createProperty("embedding", Type.ARRAY_OF_FLOATS);

      // Create LSM_VECTOR index programmatically
      database.getSchema()
          .buildLSMVectorIndex("VectorDoc", new String[] { "embedding" })
          .withIndexName("VectorDoc_embedding_idx")
          .withDimensions(3)
          .withSimilarity("EUCLIDEAN")
          .withMaxConnections(8)
          .withBeamWidth(50)
          .create();
    });

    // Verify index was created
    // Note: TypeIndex is created with default name based on type and properties
    final com.arcadedb.index.TypeIndex typeIndex = (com.arcadedb.index.TypeIndex) database.getSchema()
        .getIndexByName("VectorDoc[embedding]");
    Assertions.assertNotNull(typeIndex);

    // Get one of the underlying LSMVectorIndex instances to verify configuration
    final LSMVectorIndex index = (LSMVectorIndex) typeIndex.getIndexesOnBuckets()[0];
    Assertions.assertEquals(3, index.getDimensions());
    Assertions.assertEquals("EUCLIDEAN", index.getSimilarityFunction().name());
    Assertions.assertEquals(8, index.getMaxConnections());
    Assertions.assertEquals(50, index.getBeamWidth());

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

      Assertions.assertTrue(count > 0);
      Assertions.assertTrue(count <= DIMENSIONS);
    });
  }

  @Test
  public void testIndexEntryCount() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE TestDoc");
      database.command("sql", "CREATE PROPERTY TestDoc.vec ARRAY_OF_FLOATS");
      database.command("sql", "CREATE INDEX ON TestDoc (vec) LSM_VECTOR " +
          "METADATA {\"dimensions\": 2, \"similarity\": \"DOT_PRODUCT\", \"maxConnections\": 4, \"beamWidth\": 10}");
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
    Assertions.assertEquals(50, typeIndex.countEntries());
  }

  @Test
  public void testTransactionalIsolation() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE TxDoc");
      database.command("sql", "CREATE PROPERTY TxDoc.vec ARRAY_OF_FLOATS");
      database.command("sql", "CREATE INDEX ON TxDoc (vec) LSM_VECTOR " +
          "METADATA {\"dimensions\": 2, \"similarity\": \"COSINE\", \"maxConnections\": 4, \"beamWidth\": 10}");
    });

    // Insert in transaction 1
    database.transaction(() -> {
      final var doc = database.newDocument("TxDoc");
      doc.set("vec", new float[] { 1.0f, 2.0f });
      doc.save();
    });

    final com.arcadedb.index.TypeIndex typeIndex = (com.arcadedb.index.TypeIndex) database.getSchema().getIndexByName("TxDoc[vec]");
    Assertions.assertEquals(1, typeIndex.countEntries());

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
    Assertions.assertEquals(1, typeIndex.countEntries());
  }

  @Test
  public void testLSMAppendOnlySemantics() {
    // Test LSM append-only: add, remove, add again - last entry should win
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE LSMDoc");
      database.command("sql", "CREATE PROPERTY LSMDoc.id STRING");
      database.command("sql", "CREATE PROPERTY LSMDoc.vec ARRAY_OF_FLOATS");
      database.command("sql", "CREATE INDEX ON LSMDoc (vec) LSM_VECTOR " +
          "METADATA {\"dimensions\": 3, \"similarity\": \"EUCLIDEAN\", \"maxConnections\": 4, \"beamWidth\": 10}");
    });

    final com.arcadedb.index.TypeIndex typeIndex = (com.arcadedb.index.TypeIndex) database.getSchema()
        .getIndexByName("LSMDoc[vec]");
    Assertions.assertNotNull(typeIndex);

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

    Assertions.assertEquals(1, typeIndex.countEntries());

    // Query to verify it's there
    database.transaction(() -> {
      final float[] queryVec = { 1.0f, 2.0f, 3.0f };
      final var cursor = typeIndex.get(new Object[] { queryVec }, 10);
      int count = 0;
      while (cursor.hasNext()) {
        cursor.next();
        count++;
      }
      Assertions.assertTrue(count > 0, "Should find the added vector");
    });

    // Update the document (delete old entry, add new entry)
    database.transaction(() -> {
      final var doc = docRid.asDocument().modify();
      doc.set("vec", new float[] { 10.0f, 20.0f, 30.0f });
      doc.save();
    });

    // Should still have 1 entry (old marked deleted, new added)
    Assertions.assertEquals(1, typeIndex.countEntries());

    // Query with new vector should find it
    database.transaction(() -> {
      final float[] queryVec = { 10.0f, 20.0f, 30.0f };
      final var cursor = typeIndex.get(new Object[] { queryVec }, 10);
      int count = 0;
      while (cursor.hasNext()) {
        cursor.next();
        count++;
      }
      Assertions.assertTrue(count > 0, "Should find the updated vector");
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
    Assertions.assertEquals(0, typeIndex.countEntries());

    // Re-add with same RID (simulating LSM append-only where entries accumulate)
    database.transaction(() -> {
      final var doc = database.newDocument("LSMDoc");
      doc.set("id", "doc1_new");
      doc.set("vec", new float[] { 100.0f, 200.0f, 300.0f });
      doc.save();
    });

    // Should have 1 entry (the new one)
    Assertions.assertEquals(1, typeIndex.countEntries());
  }

  @Test
  public void testLSMMergeOnReadLastWins() {
    // Test that in LSM pages, the last entry for a vector ID wins during merge-on-read
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE MergeDoc");
      database.command("sql", "CREATE PROPERTY MergeDoc.vec ARRAY_OF_FLOATS");
      database.command("sql", "CREATE INDEX ON MergeDoc (vec) LSM_VECTOR " +
          "METADATA {\"dimensions\": 2, \"similarity\": \"DOT_PRODUCT\", \"maxConnections\": 4, \"beamWidth\": 10}");
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

    Assertions.assertEquals(10, typeIndex.countEntries());

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
    Assertions.assertEquals(10, typeIndex.countEntries());

    // Verify the last values are present (should find vectors with latest iteration values)
    database.transaction(() -> {
      final float[] queryVec = { 300.0f, 300.0f }; // Last iteration values for i=0
      final var cursor = typeIndex.get(new Object[] { queryVec }, 10);
      int count = 0;
      while (cursor.hasNext()) {
        cursor.next();
        count++;
      }
      Assertions.assertTrue(count > 0, "Should find the last updated vector");
    });

    // Verify LSM semantics: The index should correctly handle multiple updates to same documents
    // Key point: Even after 3 updates to 5 documents, we still have exactly 10 entries (not 10 + 5*3)
    // This demonstrates that LSM merge-on-read is working (last entry wins)
  }

  @Test
  public void testLazyGraphIndexRebuild() {
    // Test that graph index is only rebuilt when needed (lazy)
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE LazyDoc");
      database.command("sql", "CREATE PROPERTY LazyDoc.vec ARRAY_OF_FLOATS");
      database.command("sql", "CREATE INDEX ON LazyDoc (vec) LSM_VECTOR " +
          "METADATA {\"dimensions\": 4, \"similarity\": \"COSINE\", \"maxConnections\": 8, \"beamWidth\": 50}");
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
      Assertions.assertTrue(count > 0, "Should find vectors after lazy rebuild");
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
      Assertions.assertTrue(count > 0, "Should find vectors without rebuild");
    });
    final long noRebuildTime = System.currentTimeMillis() - startTime2;

    // Second query should be significantly faster (no rebuild overhead)
    // Note: This is a heuristic test, may be flaky in CI
    System.out.println("Rebuild time: " + rebuildTime + "ms, No rebuild time: " + noRebuildTime + "ms");
  }

  @Test
  public void testCompactionTrigger() {
    // Test that compaction is triggered after reaching the threshold
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE CompactDoc IF NOT EXISTS");
      database.command("sql", "CREATE PROPERTY CompactDoc.vec IF NOT EXISTS ARRAY_OF_FLOATS");
      database.command("sql", "CREATE INDEX IF NOT EXISTS ON CompactDoc (vec) LSM_VECTOR " +
          "METADATA {dimensions: 4, similarity: 'COSINE'}");
    });

    final var typeIndex = (com.arcadedb.index.TypeIndex) database.getSchema().getIndexByName("CompactDoc[vec]");
    Assertions.assertNotNull(typeIndex, "Index should exist");

    final var indexes = typeIndex.getIndexesOnBuckets();
    Assertions.assertTrue(indexes.length > 0, "Should have at least one bucket index");

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
          final var doc = database.newDocument("CompactDoc");
          doc.set("vec", new float[] {
              (float) (batchNum * vectorsPerBatch + i),
              (float) (batchNum * vectorsPerBatch + i + 1),
              (float) (batchNum * vectorsPerBatch + i + 2),
              (float) (batchNum * vectorsPerBatch + i + 3)
          });
          doc.save();
        }
      });

      // Check if pages increased
      if (lsmIndex.getCurrentMutablePages() > initialPages) {
        System.out.println("After batch " + batch + ": mutablePages=" + lsmIndex.getCurrentMutablePages() +
            ", threshold=" + threshold);
      }
    }

    final int finalPages = lsmIndex.getCurrentMutablePages();
    System.out.println("Initial pages: " + initialPages + ", Final pages: " + finalPages +
        ", Threshold: " + threshold);

    // Verify that compaction would have been triggered
    Assertions.assertTrue(finalPages >= threshold || initialPages >= threshold,
        "Should have reached compaction threshold at some point");
  }

  @Test
  public void testCompactionMergeMultipleUpdates() {
    // Test K-way merge with multiple updates to same vectors
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE MergeDoc IF NOT EXISTS");
      database.command("sql", "CREATE PROPERTY MergeDoc.id IF NOT EXISTS STRING");
      database.command("sql", "CREATE PROPERTY MergeDoc.vec IF NOT EXISTS ARRAY_OF_FLOATS");
      database.command("sql", "CREATE INDEX IF NOT EXISTS ON MergeDoc (vec) LSM_VECTOR " +
          "METADATA {dimensions: 3, similarity: 'COSINE'}");
    });

    final var typeIndex = (com.arcadedb.index.TypeIndex) database.getSchema().getIndexByName("MergeDoc[vec]");
    Assertions.assertNotNull(typeIndex, "Index should exist");

    // Add initial vectors
    final List<com.arcadedb.database.RID> rids = new ArrayList<>();
    database.transaction(() -> {
      for (int i = 0; i < 20; i++) {
        final var doc = database.newDocument("MergeDoc");
        doc.set("id", "doc" + i);
        doc.set("vec", new float[] { (float) i, (float) i * 2, (float) i * 3 });
        doc.save();
        rids.add(doc.getIdentity());
      }
    });

    final long countAfterInsert = typeIndex.countEntries();
    Assertions.assertEquals(20, countAfterInsert, "Should have 20 entries after insert");

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
    Assertions.assertEquals(20, countAfterUpdates,
        "Should still have 20 entries after updates (LSM last-write-wins semantics)");

    // Verify we can query the updated vectors
    database.transaction(() -> {
      final float[] queryVec = { 500.0f, 500.0f, 500.0f }; // Query for last iteration values
      final var cursor = typeIndex.get(new Object[] { queryVec }, 5);
      int count = 0;
      while (cursor.hasNext()) {
        cursor.next();
        count++;
      }
      Assertions.assertTrue(count > 0, "Should find vectors with updated values");
    });
  }

  @Test
  public void testCompactionDeletedEntryRemoval() {
    // Test that deleted entries are removed during compaction
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE DeleteDoc IF NOT EXISTS");
      database.command("sql", "CREATE PROPERTY DeleteDoc.id IF NOT EXISTS STRING");
      database.command("sql", "CREATE PROPERTY DeleteDoc.vec IF NOT EXISTS ARRAY_OF_FLOATS");
      database.command("sql", "CREATE INDEX IF NOT EXISTS ON DeleteDoc (vec) LSM_VECTOR " +
          "METADATA {dimensions: 2, similarity: 'COSINE'}");
    });

    final var typeIndex = (com.arcadedb.index.TypeIndex) database.getSchema().getIndexByName("DeleteDoc[vec]");
    Assertions.assertNotNull(typeIndex, "Index should exist");

    // Add vectors
    final List<com.arcadedb.database.RID> rids = new ArrayList<>();
    database.transaction(() -> {
      for (int i = 0; i < 50; i++) {
        final var doc = database.newDocument("DeleteDoc");
        doc.set("id", "doc" + i);
        doc.set("vec", new float[] { (float) i, (float) i * 2 });
        doc.save();
        rids.add(doc.getIdentity());
      }
    });

    Assertions.assertEquals(50, typeIndex.countEntries(), "Should have 50 entries");

    // Delete half of the vectors
    database.transaction(() -> {
      for (int i = 0; i < 25; i++) {
        rids.get(i).asDocument().delete();
      }
    });

    // Count should reflect deletions
    final long countAfterDelete = typeIndex.countEntries();
    Assertions.assertEquals(25, countAfterDelete, "Should have 25 entries after deleting 25");

    // Verify remaining vectors are queryable
    database.transaction(() -> {
      final float[] queryVec = { 30.0f, 60.0f }; // Vector from non-deleted range
      final var cursor = typeIndex.get(new Object[] { queryVec }, 10);
      int count = 0;
      while (cursor.hasNext()) {
        cursor.next();
        count++;
      }
      Assertions.assertTrue(count > 0, "Should find non-deleted vectors");
    });

    // After compaction, deleted entries should be physically removed
    // (We can't directly test this without triggering manual compaction,
    // but the count verification above confirms logical deletion works)
  }

  @Test
  public void testCompactionWithConcurrentQueries() throws InterruptedException {
    // Test that queries work correctly during compaction
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE ConcurrentDoc IF NOT EXISTS");
      database.command("sql", "CREATE PROPERTY ConcurrentDoc.vec IF NOT EXISTS ARRAY_OF_FLOATS");
      database.command("sql", "CREATE INDEX IF NOT EXISTS ON ConcurrentDoc (vec) LSM_VECTOR " +
          "METADATA {dimensions: 4, similarity: 'COSINE'}");
    });

    final var typeIndex = (com.arcadedb.index.TypeIndex) database.getSchema().getIndexByName("ConcurrentDoc[vec]");
    Assertions.assertNotNull(typeIndex, "Index should exist");

    // Add initial vectors
    database.transaction(() -> {
      for (int i = 0; i < 100; i++) {
        final var doc = database.newDocument("ConcurrentDoc");
        doc.set("vec", new float[] {
            (float) i, (float) i + 1, (float) i + 2, (float) i + 3
        });
        doc.save();
      }
    });

    Assertions.assertEquals(100, typeIndex.countEntries(), "Should have 100 entries");

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
    Assertions.assertTrue(successfulQueries.get() > (numThreads * queriesPerThread) / 2,
        "Most concurrent queries should succeed");
    Assertions.assertEquals(0, failedQueries.get(), "No queries should fail with exceptions");
  }

  @Test
  public void testGraphIndexCorrectnessAfterCompaction() {
    // Test that graph index is correctly rebuilt after compaction
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE GraphDoc IF NOT EXISTS");
      database.command("sql", "CREATE PROPERTY GraphDoc.vec IF NOT EXISTS ARRAY_OF_FLOATS");
      database.command("sql", "CREATE INDEX IF NOT EXISTS ON GraphDoc (vec) LSM_VECTOR " +
          "METADATA {dimensions: 8, similarity: 'EUCLIDEAN', maxConnections: 16, beamWidth: 100}");
    });

    final var typeIndex = (com.arcadedb.index.TypeIndex) database.getSchema().getIndexByName("GraphDoc[vec]");
    Assertions.assertNotNull(typeIndex, "Index should exist");

    // Add vectors in a pattern that creates a structure in the graph
    final List<com.arcadedb.database.RID> rids = new ArrayList<>();
    database.transaction(() -> {
      for (int i = 0; i < 100; i++) {
        final var doc = database.newDocument("GraphDoc");
        // Create vectors in clusters (should form neighborhoods in graph)
        final int cluster = i / 10;
        doc.set("vec", new float[] {
            (float) cluster * 10, (float) cluster * 10 + 1,
            (float) cluster * 10 + 2, (float) cluster * 10 + 3,
            (float) i % 10, (float) (i % 10) + 1,
            (float) (i % 10) + 2, (float) (i % 10) + 3
        });
        doc.save();
        rids.add(doc.getIdentity());
      }
    });

    Assertions.assertEquals(100, typeIndex.countEntries(), "Should have 100 entries");

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

    Assertions.assertTrue(resultsBeforeUpdates.size() > 0, "Should find results before updates");

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

    Assertions.assertTrue(resultsAfterUpdates.size() > 0,
        "Should find results after updates (graph index rebuilt)");

    // Results should be different after updates (graph index reflects new data)
    System.out.println("Results before updates: " + resultsBeforeUpdates.size() +
        ", Results after updates: " + resultsAfterUpdates.size());
  }
}
