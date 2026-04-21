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
package com.arcadedb.index;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.schema.Schema;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for #3714: "Variable length quantity is too long (must be <= 63)"
 * error during batch UPSERTs after many records. The issue manifests as page data
 * corruption during concurrent batch UPSERT operations with indexed fields.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue3714BatchUpsertVLQTest {
  private static final String DB_PATH = "target/databases/Issue3714BatchUpsertVLQTest";

  private Database database;

  @BeforeEach
  void setUp() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    database = new DatabaseFactory(DB_PATH).create();
    database.transaction(() -> {
      final var type = database.getSchema().buildDocumentType().withName("Metadata").withTotalBuckets(4).create();
      type.createProperty("recordId", String.class);
      type.createProperty("data", String.class);
      database.getSchema().buildTypeIndex("Metadata", new String[] { "recordId" })
          .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(true).withPageSize(64 * 1024).create();
    });
  }

  @AfterEach
  void tearDown() {
    if (database != null && database.isOpen())
      database.drop();
  }

  /**
   * Reproduce #3714: concurrent batch UPSERTs with many records trigger
   * "Variable length quantity is too long" or "arraycopy: length is negative"
   * during transaction commit. Each thread uses its own key namespace to minimize
   * ConcurrentModificationException retries.
   */
  @Test
  void concurrentBatchUpsertsDoNotCorruptIndex() throws Exception {
    // Use 2 threads to reduce contention while still testing concurrency
    final int threads = 2;
    final int batchesPerThread = 500;
    final int recordsPerBatch = 20;
    final AtomicReference<Throwable> error = new AtomicReference<>();
    final AtomicInteger successfulBatches = new AtomicInteger();
    final CountDownLatch startLatch = new CountDownLatch(1);
    final CountDownLatch doneLatch = new CountDownLatch(threads);

    for (int t = 0; t < threads; t++) {
      final int threadId = t;
      new Thread(() -> {
        try {
          startLatch.await();
          for (int batch = 0; batch < batchesPerThread; batch++) {
            final StringBuilder sql = new StringBuilder("BEGIN;");
            for (int r = 0; r < recordsPerBatch; r++) {
              final String id = "t" + threadId + "_b" + batch + "_r" + r;
              final String data = "{\"recordId\":\"" + id + "\",\"value\":\"data_" + id + "\",\"batch\":" + batch + "}";
              sql.append("UPDATE Metadata CONTENT ").append(data)
                  .append(" UPSERT WHERE recordId = '").append(id).append("';");
            }
            sql.append("COMMIT;");

            boolean success = false;
            for (int retry = 0; retry < 200 && !success; retry++) {
              try {
                database.command("sqlscript", sql.toString());
                success = true;
                successfulBatches.incrementAndGet();
              } catch (final Exception e) {
                final String msg = fullErrorMessage(e);
                if (msg.contains("ConcurrentModification") || msg.contains("Please retry"))
                  continue;
                // Unexpected error — likely the VLQ or arraycopy bug
                error.compareAndSet(null, e);
                return;
              }
            }
            // Skip if retries exhausted due to contention — not the bug we're testing for
          }
        } catch (final Throwable e) {
          error.compareAndSet(null, e);
        } finally {
          doneLatch.countDown();
        }
      }, "UpsertThread-" + t).start();
    }

    startLatch.countDown();
    assertThat(doneLatch.await(180, TimeUnit.SECONDS)).as("Threads should finish within timeout").isTrue();

    if (error.get() != null)
      error.get().printStackTrace();

    assertThat(error.get()).as("Batch UPSERTs should not throw: %s",
        error.get() != null ? error.get().getMessage() : "").isNull();

    // Verify data integrity: at least the successful batches should be counted
    database.transaction(() -> {
      final long count = database.countType("Metadata", false);
      assertThat(count).isEqualTo((long) successfulBatches.get() * recordsPerBatch);
    });
  }

  /**
   * Test single-thread batch UPSERTs that re-update the same keys repeatedly.
   * This stresses the index with many modifications to the same page.
   */
  @Test
  void repeatedBatchUpsertsOnSameKeys() {
    final int totalKeys = 1000;
    final int iterations = 100;

    // First insert all keys
    database.transaction(() -> {
      for (int i = 0; i < totalKeys; i++)
        database.command("sql", "INSERT INTO Metadata SET recordId = 'key_" + i + "', data = 'initial'");
    });

    // Repeatedly update in batches
    for (int iter = 0; iter < iterations; iter++) {
      final StringBuilder sql = new StringBuilder("BEGIN;");
      for (int i = 0; i < 20; i++) {
        final int keyIdx = (iter * 20 + i) % totalKeys;
        sql.append("UPDATE Metadata CONTENT {\"recordId\":\"key_").append(keyIdx)
            .append("\",\"data\":\"updated_iter").append(iter).append("_").append(i)
            .append("\"} UPSERT WHERE recordId = 'key_").append(keyIdx).append("';");
      }
      sql.append("COMMIT;");
      database.command("sqlscript", sql.toString());
    }

    // Verify integrity
    database.transaction(() -> {
      assertThat(database.countType("Metadata", false)).isEqualTo(totalKeys);
    });
  }

  private static String fullErrorMessage(final Throwable e) {
    final StringBuilder sb = new StringBuilder();
    Throwable current = e;
    while (current != null) {
      if (current.getMessage() != null)
        sb.append(current.getMessage()).append(" ");
      current = current.getCause();
    }
    return sb.toString();
  }
}
