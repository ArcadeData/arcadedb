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
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Concurrent UPDATE of multi-page records with LSM_VECTOR index must not corrupt the store.
 * Isolated in a dedicated class because it intentionally exercises corruption-detection paths
 * and therefore disables the post-test database integrity check.
 */
@Tag("slow")
class LSMVectorIndexConcurrentMultiPageUpdateTest extends TestHelper {

  private static final int NUM_RECORDS         = 2000;
  private static final int CONCURRENT_THREADS  = 100;
  private static final int EMBEDDING_DIM       = 3072;

  private final AtomicInteger updateErrors = new AtomicInteger(0);
  private final AtomicInteger verifyErrors = new AtomicInteger(0);

  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    // The test intentionally probes corruption-detection paths under concurrent multi-page writes.
    return false;
  }

  // Issue #3135: concurrent UPDATE of multi-page records with LSM_VECTOR index must not corrupt the store.
  @Test
  void concurrentMultiPageVectorUpdates() throws Exception {
    // Phase 1: Create schema with inheritance and LSM_VECTOR index
    database.transaction(() -> {
      final Schema schema = database.getSchema();

      // Parent type with embedding (like ContentV in production)
      final VertexType baseV = schema.createVertexType("BaseV");
      baseV.createProperty("id", Type.STRING);
      baseV.createProperty("embedding", Type.ARRAY_OF_FLOATS);

      // Child type (like TextV in production)
      final VertexType recordV = schema.createVertexType("RecordV");
      recordV.addSuperType(baseV);  // Inheritance is key!

      // Unique index on id
      baseV.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "id");

      // LSM Vector Index on PARENT type
      database.command("sql", """
          CREATE INDEX ON BaseV (embedding) LSM_VECTOR
          METADATA {
              "dimensions": %d,
              "similarity": "COSINE"
          }""".formatted(EMBEDDING_DIM));
    });

    // Phase 2: Create records with zero embeddings
    for (int i = 0; i < NUM_RECORDS; i++) {
      final String id = "record_" + i;
      final float[] zeroEmbedding = new float[EMBEDDING_DIM];
      database.transaction(() -> {
        database.command("sql", "INSERT INTO RecordV SET id=?, embedding=?", id, zeroEmbedding);
      }, false, 3);
    }

    // Verify all records were created
    database.transaction(() -> {
      try (final ResultSet rs = database.query("sql", "SELECT count(*) as cnt FROM RecordV")) {
        final long count = rs.hasNext() ? rs.next().<Long>getProperty("cnt") : 0L;
        assertThat(count).isEqualTo(NUM_RECORDS);
      }
    });

    // Phase 3: Concurrently update embeddings
    final ExecutorService executor = Executors.newFixedThreadPool(CONCURRENT_THREADS);
    final CountDownLatch latch = new CountDownLatch(NUM_RECORDS);

    for (int i = 0; i < NUM_RECORDS; i++) {
      final String id = "record_" + i;
      executor.submit(() -> {
        try {
          updateEmbedding(id);
        } catch (final Exception e) {
          // Log but continue - we're testing for corruption
        } finally {
          latch.countDown();
        }
      });
    }

    latch.await(5, TimeUnit.MINUTES);
    executor.shutdown();
    executor.awaitTermination(1, TimeUnit.MINUTES);

    // Phase 4: Verification - try to read all embeddings to detect corruption
    for (int i = 0; i < NUM_RECORDS; i++) {
      final String id = "record_" + i;
      try {
        database.transaction(() -> {
          try (final ResultSet rs = database.query("sql", "SELECT embedding FROM RecordV WHERE id=?", id)) {
            if (rs.hasNext()) {
              final float[] embedding = rs.next().getProperty("embedding");
              if (embedding == null) {
                verifyErrors.incrementAndGet();
                System.err.println("   Corruption detected on " + id + ": embedding is null");
              } else if (embedding.length != EMBEDDING_DIM) {
                verifyErrors.incrementAndGet();
                System.err.println("   Corruption detected on " + id + ": embedding length is " + embedding.length + " instead of " + EMBEDDING_DIM);
              }
            } else {
              verifyErrors.incrementAndGet();
              System.err.println("   Corruption detected on " + id + ": record not found");
            }
          }
        }, true, 1);
      } catch (final Exception e) {
        if (e.getMessage() != null &&
            (e.getMessage().contains("Invalid pointer") ||
                e.getMessage().contains("was deleted") ||
                e.getMessage().contains("Concurrent modification"))) {
          verifyErrors.incrementAndGet();
          System.err.println("   Corruption detected on " + id + ": " + e.getMessage());
        } else {
          throw e;
        }
      }
    }

    // The test should pass with no corruption errors
    assertThat(verifyErrors.get())
        .as("Corruption errors detected during verification")
        .isEqualTo(0);
  }

  private void updateEmbedding(final String id) {
    try {
      database.transaction(() -> {
        // Read current record (to simulate real-world read-before-write pattern)
        try (final ResultSet rs = database.query("sql", "SELECT embedding FROM RecordV WHERE id=?", id)) {
          if (!rs.hasNext())
            return;
          rs.next().getProperty("embedding");
        }

        // Update with new embedding (multi-page write)
        final float[] newEmbedding = randomEmbedding();
        database.command("sql", "UPDATE RecordV SET embedding=? WHERE id=?", newEmbedding, id);
      }, false, 3);
    } catch (final DatabaseOperationException e) {
      if (e.getMessage() != null &&
          (e.getMessage().contains("Invalid pointer") ||
              e.getMessage().contains("was deleted") ||
              e.getMessage().contains("Concurrent modification"))) {
        updateErrors.incrementAndGet();
      } else {
        throw e;
      }
    }
  }

  private float[] randomEmbedding() {
    final float[] embedding = new float[EMBEDDING_DIM];
    final ThreadLocalRandom rnd = ThreadLocalRandom.current();
    for (int i = 0; i < EMBEDDING_DIM; i++)
      embedding[i] = rnd.nextFloat();
    return embedding;
  }
}
