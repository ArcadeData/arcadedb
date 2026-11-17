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
import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12375. A schema script that creates a type and indexes it runs as ONE schema session ({@link Schema#bulkChange(Runnable)}), which
 * holds the database write lock for the whole script. The index build inside it waits for the async executor to commit its
 * open batches, and a worker's commit needs the read lock that this thread's write lock excludes: once the database has an
 * async executor (an LSM compaction is enough to create one) the script never returned, and every write behind it queued on
 * the lock.
 * <p>
 * A super type linked by a batched {@code ALTER TYPE} met the same lock from the other side: it asks for a quiescence of its own, inside
 * the frame, and the park task of each worker commits first. That one gave up after the quiesce timeout (60 s) and failed the script.
 */
class Issue12375BulkSchemaScriptAsyncDeadlockTest extends TestHelper {

  /** The tripwire between a script that takes milliseconds and one that waits on its own lock (for ever, or for the 60 s quiesce timeout). */
  private static final long HANG_BOUND_MS = 30_000;

  @Test
  @Timeout(value = 5, unit = TimeUnit.MINUTES)
  void aSchemaScriptThatIndexesANewTypeCompletesWhileAsyncWorkersHoldOpenBatches() throws Exception {
    holdAnOpenAsyncBatch();

    runScript("""
        CREATE DOCUMENT TYPE Fresh;
        CREATE PROPERTY Fresh.k STRING;
        CREATE INDEX ON Fresh (k) NOTUNIQUE;
        """);

    assertThat(database.getSchema().getType("Fresh").getIndexesByProperties("k")).hasSize(1);
    assertTheHeldBatchWasCommitted();
  }

  @Test
  @Timeout(value = 5, unit = TimeUnit.MINUTES)
  void aSchemaScriptThatLinksAnIndexedSuperTypeCompletesWhileAsyncWorkersHoldOpenBatches() throws Exception {
    database.command("sqlscript", """
        CREATE DOCUMENT TYPE Parent;
        CREATE PROPERTY Parent.k STRING;
        CREATE INDEX ON Parent (k) NOTUNIQUE;
        """);
    holdAnOpenAsyncBatch();

    runScript("""
        CREATE DOCUMENT TYPE Child;
        ALTER TYPE Child SUPERTYPE +Parent;
        """);

    final DocumentType child = database.getSchema().getType("Child");
    assertThat(child.instanceOf("Parent")).isTrue();
    assertThat(child.getPolymorphicIndexByProperties("k")).isNotNull();
    assertTheHeldBatchWasCommitted();
  }

  /** Leaves a write in a worker's open batch: the batch is committed only by the next marker or by the commitEvery boundary. */
  private void holdAnOpenAsyncBatch() {
    database.getSchema().createDocumentType("Pending");
    database.async().createRecord(database.newDocument("Pending").set("k", "queued"), null);
  }

  private void assertTheHeldBatchWasCommitted() {
    database.async().waitCompletion();
    assertThat(database.countType("Pending", false)).isEqualTo(1);
  }

  /**
   * Runs the script on another thread, so that a script stuck on its own lock can be interrupted: the interrupt ends the wait, which
   * releases the write lock and lets the teardown close the database.
   */
  private void runScript(final String script) throws InterruptedException, ExecutionException {
    final ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
      final Future<?> running = executor.submit(() -> database.command("sqlscript", script));

      // Short waits against the stall-discounted clock rather than one wall-clock get(): a JVM pause is not the script hanging
      boolean completed = false;
      while (!completed && stopwatch.effectiveMs() < HANG_BOUND_MS)
        try {
          running.get(100, TimeUnit.MILLISECONDS);
          completed = true;
        } catch (final TimeoutException e) {
          // STILL RUNNING
        }

      if (!completed)
        running.cancel(true);

      stopwatch.assertGaveUpWithin(HANG_BOUND_MS,
          "a schema script that asks the async workers to commit before it takes the database write lock from one that asks while holding it");
      assertThat(completed).as("the schema script completed").isTrue();
    } finally {
      executor.shutdownNow();
    }
  }
}
