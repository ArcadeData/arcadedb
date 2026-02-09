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
package com.arcadedb;

import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import org.junit.jupiter.api.Test;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test: read-only queries on multi-page records must not fail with
 * ConcurrentModificationException when concurrent writes are happening.
 * <p>
 * Before the fix, a query like {@code MATCH ()-[r]->() RETURN COUNT(r)} could throw:
 * "Multi-page record #X:Y was modified during read (page ... version changed from N to M).
 * Please retry the operation" because loadMultiPageRecord did not retry internally.
 * <p>
 * The fix adds automatic retry inside loadMultiPageRecord so that read-only queries
 * transparently handle concurrent modifications without propagating errors.
 */
class ConcurrentMultiPageRecordReadTest extends TestHelper {

  private static final int NUM_RECORDS = 50;
  private static final int EMBEDDING_DIM = 3072; // Forces multi-page records
  private static final int WRITER_THREADS = 4;
  private static final int READER_THREADS = 8;
  private static final int OPERATIONS_PER_THREAD = 20;

  private final Random random = new Random(42);
  private final AtomicInteger readErrors = new AtomicInteger(0);

  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    return false;
  }

  @Test
  void readOnlyQueryShouldNotFailDuringConcurrentWrites() throws Exception {
    // Phase 1: Create schema
    database.transaction(() -> {
      final VertexType vertexType = database.getSchema().createVertexType("LargeRecord");
      vertexType.createProperty("id", Type.INTEGER);
      vertexType.createProperty("data", Type.ARRAY_OF_FLOATS);
      vertexType.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "id");
    });

    // Phase 2: Create multi-page records
    for (int i = 0; i < NUM_RECORDS; i++) {
      final int id = i;
      final float[] largeData = randomEmbedding();
      database.transaction(
              () -> database.command("sql", "INSERT INTO LargeRecord SET id=?, data=?", id, largeData), false, 3);
    }

    // Phase 3: Concurrent readers and writers
    final ExecutorService executor = Executors.newFixedThreadPool(WRITER_THREADS + READER_THREADS);
    final CountDownLatch startLatch = new CountDownLatch(1);
    final CountDownLatch doneLatch = new CountDownLatch(WRITER_THREADS + READER_THREADS);

    // Submit writer threads that update multi-page records
    for (int t = 0; t < WRITER_THREADS; t++) {
      executor.submit(() -> {
        try {
          startLatch.await();
          for (int op = 0; op < OPERATIONS_PER_THREAD; op++) {
            final int id = random.nextInt(NUM_RECORDS);
            final float[] newData = randomEmbedding();
            try {
              database.transaction(
                      () -> database.command("sql", "UPDATE LargeRecord SET data=? WHERE id=?", newData, id), false,
                      3);
            } catch (final Exception e) {
              // Expected under heavy contention - writers may fail, that's ok
            }
          }
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        } finally {
          doneLatch.countDown();
        }
      });
    }

    // Submit reader threads that perform read-only queries on multi-page records
    for (int t = 0; t < READER_THREADS; t++) {
      executor.submit(() -> {
        try {
          startLatch.await();
          for (int op = 0; op < OPERATIONS_PER_THREAD; op++) {
            final int id = random.nextInt(NUM_RECORDS);
            try {
              // This is the query pattern that was failing before the fix
              try (final ResultSet rs = database.query("sql",
                      "SELECT id, data FROM LargeRecord WHERE id = ?", id)) {
                if (rs.hasNext()) {
                  final float[] data = rs.next().getProperty("data");
                  assertThat(data).isNotNull();
                  assertThat(data.length).isEqualTo(EMBEDDING_DIM);
                }
              }
            } catch (final Exception e) {
              if (e.getMessage() != null && e.getMessage().contains("was modified during read"))
                readErrors.incrementAndGet();
            }
          }
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        } finally {
          doneLatch.countDown();
        }
      });
    }

    // Start all threads simultaneously
    startLatch.countDown();
    doneLatch.await(5, TimeUnit.MINUTES);
    executor.shutdown();
    executor.awaitTermination(1, TimeUnit.MINUTES);

    // The key assertion: read-only queries should not fail with ConcurrentModificationException
    // thanks to the internal retry in loadMultiPageRecord
    assertThat(readErrors.get())
            .as("Read-only queries should not fail with ConcurrentModificationException")
            .isEqualTo(0);
  }

  private float[] randomEmbedding() {
    final float[] embedding = new float[EMBEDDING_DIM];
    for (int i = 0; i < EMBEDDING_DIM; i++)
      embedding[i] = random.nextFloat();
    return embedding;
  }
}
