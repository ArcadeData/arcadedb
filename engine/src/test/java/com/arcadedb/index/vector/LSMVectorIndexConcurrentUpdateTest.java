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
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.RID;
import com.arcadedb.index.Index;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import com.arcadedb.utility.Pair;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test case to reproduce LSM Vector Index corruption with concurrent insert + update operations.
 *
 * Issue: https://github.com/ArcadeData/arcadedb/issues/3135
 *
 * Problem: When embeddings are inserted with zero vectors and then updated concurrently,
 * the LSM vector index can become corrupted after database reopen, resulting in:
 * 1. Corrupted ordinal-to-vector-id mappings
 * 2. NullPointerException during vector search
 *
 * Scenario:
 * 1. Create vertices with zero-filled embeddings + LSM vector index
 * 2. Concurrently insert new vertices with zero vectors
 * 3. Concurrently update existing vertices' embeddings
 * 4. Close and reopen database
 * 5. Perform vector search - should not fail with NPE
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class LSMVectorIndexConcurrentUpdateTest extends TestHelper {

  private static final int EMBEDDING_DIM = 3072;  // Multi-page record size
  private static final int INITIAL_RECORDS = 1000;
  private static final int CONCURRENT_THREADS = 100;
  private static final int INSERTS_PER_THREAD = 10;
  private static final int UPDATES_PER_THREAD = 20;

  @Test
  void testConcurrentInsertAndUpdateWithLSMVectorIndex() throws Exception {
    final Random random = new Random(42);
    final AtomicInteger recordCounter = new AtomicInteger(0);
    final AtomicInteger errorCounter = new AtomicInteger(0);

    database.transaction(() -> {
      // Create schema with inheritance (like in production use case)
      VertexType baseV = database.getSchema().createVertexType("BaseV");
      baseV.createProperty("id", Type.STRING);
      baseV.createProperty("embedding", Type.ARRAY_OF_FLOATS);

      VertexType recordV = database.getSchema().createVertexType("RecordV");
      recordV.addSuperType(baseV);  // Inheritance is key!

      // Unique index on id
      baseV.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "id");

      // LSM Vector Index on PARENT type (like production)
      database.command("sql", """
          CREATE INDEX IF NOT EXISTS ON BaseV (embedding) LSM_VECTOR
          METADATA {
              "dimensions": %d,
              "similarity": "COSINE"
          }""".formatted(EMBEDDING_DIM));
    });

    // Phase 1: Create initial records with zero embeddings
    //System.out.println("Phase 1: Creating " + INITIAL_RECORDS + " initial records with zero embeddings...");
    final float[] zeroEmbedding = new float[EMBEDDING_DIM];
    for (int i = 0; i < INITIAL_RECORDS; i++) {
      final String id = "record_" + recordCounter.getAndIncrement();
      database.transaction(() -> {
        database.command("sql", "INSERT INTO RecordV SET id=?, embedding=?", id, zeroEmbedding);
      });
    }
    //System.out.println("Created " + INITIAL_RECORDS + " initial records");

    // Phase 2: Concurrent inserts and updates
    //System.out.println("Phase 2: Starting " + CONCURRENT_THREADS + " concurrent threads for insert + update operations...");
    ExecutorService executor = Executors.newFixedThreadPool(CONCURRENT_THREADS);
    CountDownLatch latch = new CountDownLatch(CONCURRENT_THREADS * 2); // For both insert and update tasks

    // Collect all record IDs for updates
    final List<String> allRecordIds = new ArrayList<>();
    for (int i = 0; i < INITIAL_RECORDS; i++) {
      allRecordIds.add("record_" + i);
    }

    // Start insert threads
    for (int t = 0; t < CONCURRENT_THREADS; t++) {
      executor.submit(() -> {
        try {
          for (int i = 0; i < INSERTS_PER_THREAD; i++) {
            final String id = "record_" + recordCounter.getAndIncrement();
            database.transaction(() -> {
              database.command("sql", "INSERT INTO RecordV SET id=?, embedding=?", id, zeroEmbedding);
            });
            synchronized (allRecordIds) {
              allRecordIds.add(id);
            }
          }
        } catch (Exception e) {
          errorCounter.incrementAndGet();
          //System.err.println("Insert error: " + e.getMessage());
        } finally {
          latch.countDown();
        }
      });
    }

    // Start update threads
    for (int t = 0; t < CONCURRENT_THREADS; t++) {
      executor.submit(() -> {
        try {
          for (int i = 0; i < UPDATES_PER_THREAD; i++) {
            // Pick a random record to update
            String id;
            synchronized (allRecordIds) {
              if (allRecordIds.isEmpty()) continue;
              id = allRecordIds.get(random.nextInt(allRecordIds.size()));
            }
            final String recordId = id;

            database.transaction(() -> {
              // Read current record
              try (ResultSet rs = database.query("sql", "SELECT embedding FROM RecordV WHERE id=?", recordId)) {
                if (!rs.hasNext()) return;
                rs.next().getProperty("embedding");
              }

              // Update with new embedding (multi-page write)
              float[] newEmbedding = generateRandomEmbedding(random, EMBEDDING_DIM);
              database.command("sql", "UPDATE RecordV SET embedding=? WHERE id=?", newEmbedding, recordId);
            });
          }
        } catch (Exception e) {
          errorCounter.incrementAndGet();
          //System.err.println("Update error: " + e.getMessage());
        } finally {
          latch.countDown();
        }
      });
    }

    latch.await(5, TimeUnit.MINUTES);
    executor.shutdown();
    executor.awaitTermination(1, TimeUnit.MINUTES);

    int totalRecords = recordCounter.get();
    //System.out.println("Phase 2 complete: " + totalRecords + " total records, " + errorCounter.get() + " errors");

    // Phase 3: Verify all records can be read
    //System.out.println("Phase 3: Verifying all records can be read...");
    int corruptedRecords = 0;
    for (String id : allRecordIds) {
      try {
        database.transaction(() -> {
          try (ResultSet rs = database.query("sql", "SELECT embedding FROM RecordV WHERE id=?", id)) {
            if (rs.hasNext()) {
              rs.next().getProperty("embedding"); // Force read
            }
          }
        }, true);
      } catch (Exception e) {
        if (e.getMessage().contains("Invalid pointer") || e.getMessage().contains("was deleted")) {
          corruptedRecords++;
          System.err.println("Corruption detected on " + id + ": " + e.getMessage());
        }
      }
    }

    //System.out.println("Phase 3 complete: " + corruptedRecords + " corrupted records before reopen");
    assertThat(corruptedRecords).as("No records should be corrupted before reopen").isEqualTo(0);

    // Phase 4: Close and reopen database to test persistence
    //System.out.println("Phase 4: Closing and reopening database...");
    String dbPath = database.getDatabasePath();
    database.close();

    DatabaseFactory factory = new DatabaseFactory(dbPath);
    database = factory.open();
    //System.out.println("Database reopened");

    // Phase 5: Check index state after reopen
    //System.out.println("Phase 5: Checking LSM_VECTOR index state after reopen...");
    TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("BaseV[embedding]");
    assertThat(typeIndex).as("Index should exist after reopen").isNotNull();

    long indexEntries = typeIndex.countEntries();
    //System.out.println("Index entry count after reopen: " + indexEntries);

    // Count actual records
    final AtomicInteger actualRecords = new AtomicInteger(0);
    database.transaction(() -> {
      try (ResultSet rs = database.query("sql", "SELECT count(*) as count FROM RecordV")) {
        if (rs.hasNext()) {
          long count = rs.next().getProperty("count");
          actualRecords.set((int) count);
        }
      }
    }, true);
    //System.out.println("Actual record count after reopen: " + actualRecords.get());

    // Phase 6: Verify all records can still be read after reopen
    //System.out.println("Phase 6: Verifying all records can be read after reopen...");
    int corruptedAfterReopen = 0;
    for (String id : allRecordIds) {
      try {
        database.transaction(() -> {
          try (ResultSet rs = database.query("sql", "SELECT embedding FROM RecordV WHERE id=?", id)) {
            if (rs.hasNext()) {
              rs.next().getProperty("embedding"); // Force read
            }
          }
        }, true);
      } catch (Exception e) {
        if (e.getMessage().contains("Invalid pointer") || e.getMessage().contains("was deleted")) {
          corruptedAfterReopen++;
          System.err.println("Corruption detected after reopen on " + id + ": " + e.getMessage());
        }
      }
    }

    //System.out.println("Phase 6 complete: " + corruptedAfterReopen + " corrupted records after reopen");
    assertThat(corruptedAfterReopen).as("No records should be corrupted after reopen").isEqualTo(0);

    // Phase 7: Attempt vector search after reopen
    //System.out.println("Phase 7: Attempting vector search after reopen...");
    float[] queryVector = generateRandomEmbedding(new Random(99), EMBEDDING_DIM);

    try {
      // With inheritance, there may be multiple bucket indexes (BaseV and RecordV)
      // Search in all of them and combine results
      List<Pair<RID, Float>> allNeighbors = new ArrayList<>();
      for (Index bucketIndex : typeIndex.getIndexesOnBuckets()) {
        LSMVectorIndex lsmIndex = (LSMVectorIndex) bucketIndex;
        //System.out.println("Searching in bucket index: " + lsmIndex.getName() + " (fileId=" + lsmIndex.getFileId() +
        //                   ", pages=" + lsmIndex.getTotalPages() + ")");
        List<Pair<RID, Float>> neighbors = lsmIndex.findNeighborsFromVector(queryVector, 10);
        //System.out.println("Found " + neighbors.size() + " neighbors in this bucket");
        allNeighbors.addAll(neighbors);
      }
      //System.out.println("SUCCESS: Found " + allNeighbors.size() + " total neighbors after reopen");
      assertThat(allNeighbors).as("Should find neighbors after reopen").isNotEmpty();
    } catch (NullPointerException e) {
      System.err.println("NPE during vector search after reopen: " + e.getMessage());
      e.printStackTrace();
      throw new AssertionError("Vector search failed with NPE after reopen - ordinal mapping corrupted!", e);
    } catch (Exception e) {
      System.err.println("Error during vector search after reopen: " + e.getMessage());
      e.printStackTrace();
      throw new AssertionError("Vector search failed after reopen: " + e.getMessage(), e);
    }

    //System.out.println("\n=== TEST PASSED ===");
    //System.out.println("Total records: " + actualRecords.get());
    //System.out.println("Index entries: " + indexEntries);
    //System.out.println("Concurrent operations completed successfully with LSM vector index");
  }

  private float[] generateRandomEmbedding(Random random, int dimensions) {
    float[] embedding = new float[dimensions];
    float norm = 0;
    for (int i = 0; i < dimensions; i++) {
      embedding[i] = random.nextFloat() * 2 - 1; // Range [-1, 1]
      norm += embedding[i] * embedding[i];
    }
    // Normalize
    norm = (float) Math.sqrt(norm);
    if (norm > 0) {
      for (int i = 0; i < dimensions; i++) {
        embedding[i] /= norm;
      }
    }
    return embedding;
  }
}
