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
package com.arcadedb.schema;

import com.arcadedb.TestHelper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * Issue #12375. A schema script that creates a type and indexes it runs as ONE schema session ({@link Schema#bulkChange(Runnable)}), which
 * holds the database write lock for the whole script. The index build inside it waits for the async executor to commit its
 * open batches, and a worker's commit needs the read lock that this thread's write lock excludes: once the database has an
 * async executor (an LSM compaction is enough to create one) the script never returned, and every write behind it queued on
 * the lock.
 */
class Issue12375BulkSchemaScriptAsyncDeadlockTest extends TestHelper {

  private static final String SCRIPT = """
      CREATE DOCUMENT TYPE Fresh;
      CREATE PROPERTY Fresh.k STRING;
      CREATE INDEX ON Fresh (k) NOTUNIQUE;
      """;

  @Test
  @Timeout(value = 5, unit = TimeUnit.MINUTES)
  void aSchemaScriptThatIndexesANewTypeCompletesWhileAsyncWorkersHoldOpenBatches() throws Exception {
    database.getSchema().createDocumentType("Pending");
    // A write left in a worker's open batch: the batch is committed only by the next marker or by the commitEvery boundary
    database.async().createRecord(database.newDocument("Pending").set("k", "queued"), null);

    final ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      final Future<?> script = executor.submit(() -> database.command("sqlscript", SCRIPT));
      try {
        // A hang detector, not a latency bound: the script takes milliseconds, and the deadlock never ends
        script.get(60, TimeUnit.SECONDS);
      } catch (final TimeoutException e) {
        // The interrupt makes waitCompletion() return, which releases the write lock and lets the teardown close the database
        script.cancel(true);
        fail("the schema script deadlocked: it held the database write lock while waiting for async workers that need its read lock");
      }
    } finally {
      executor.shutdownNow();
    }

    assertThat(database.getSchema().getType("Fresh").getIndexesByProperties("k")).hasSize(1);
    database.async().waitCompletion();
    assertThat(database.countType("Pending", false)).isEqualTo(1);
  }
}
