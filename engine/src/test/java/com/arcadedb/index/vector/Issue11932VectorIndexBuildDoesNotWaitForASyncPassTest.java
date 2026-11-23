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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.FileManager;
import com.arcadedb.engine.PaginatedComponentFile;
import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Creating a vector index makes the built index file durable, and waits for nothing else (issue #11932). The build
 * forces its own file through {@code FileManager.syncFile}, which used to take the lock of {@code syncFiles()}: when
 * the WAL housekeeping was forcing every file the database had written - several hundred after a schema step - a
 * {@code CREATE INDEX ... LSM_VECTOR} on an empty type waited for the whole pass, 7 to 10 seconds.
 */
class Issue11932VectorIndexBuildDoesNotWaitForASyncPassTest extends TestHelper {

  @Test
  void creatingAVectorIndexDoesNotWaitForASyncPassOverOtherFiles() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    database.command("sql", "CREATE DOCUMENT TYPE W BUCKETS 1");
    database.command("sql", "CREATE VERTEX TYPE E BUCKETS 1");
    database.command("sql", "CREATE PROPERTY E.emb ARRAY_OF_FLOATS");

    // The flush thread writes one page at a time, in order: a page of the other bucket still queued when this test holds
    // its channel lock would park that thread on the lock and keep the pages of the index being built queued behind it
    assertThat(db.getPageManager().waitAllPagesOfDatabaseAreFlushed(db)).as("pages of the schema steps flushed").isTrue();

    final PaginatedComponentFile otherBucket = (PaginatedComponentFile) db.getFileManager()
        .getFile(db.getSchema().getType("W").getBuckets(false).getFirst().getFileId());
    final ReentrantReadWriteLock channelLock = channelLock(otherBucket);
    final ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      final Future<Boolean> pass;
      channelLock.writeLock().lock();
      try {
        // A pass over every file the database wrote (the WAL housekeeping's, or this one) gets stuck forcing the other
        // type's bucket, holding the syncFiles() lock. Marked under the channel lock, so whichever pass claims it cannot
        // finish it.
        otherBucket.markUnsynced();
        pass = executor.submit(db.getFileManager()::syncFiles);
        final StallAwareStopwatch queued = StallAwareStopwatch.start();
        while (channelLock.getQueueLength() < 1) {
          queued.assertGaveUpWithin(30_000L, "a pass that reaches the other bucket from one that never does");
          Thread.sleep(1);
        }

        final AtomicReference<Thread> ddlThread = new AtomicReference<>();
        final Future<?> create = executor.submit(() -> {
          ddlThread.set(Thread.currentThread());
          database.command("sql", "CREATE INDEX ON E (emb) LSM_VECTOR METADATA {dimensions: 16, similarity: 'COSINE'}");
        });

        final ReentrantLock syncLock = syncLock(db.getFileManager());
        final StallAwareStopwatch created = StallAwareStopwatch.start();
        while (!create.isDone()) {
          assertThat(ddlThread.get() != null && syncLock.hasQueuedThread(ddlThread.get()))
              .as("CREATE INDEX ... LSM_VECTOR on an empty type waits for a sync pass over other files (issue #11932)")
              .isFalse();
          created.assertGaveUpWithin(60_000L, "a vector index creation that completes from one that never does");
          Thread.sleep(1);
        }
        create.get();
        assertThat(database.getSchema().getType("E").getAllIndexes(false)).hasSize(1);
        assertThat(pass.isDone()).as("the pass is still stuck on the other bucket").isFalse();
      } finally {
        channelLock.writeLock().unlock();
      }
      assertThat(pass.get(30, TimeUnit.SECONDS)).isTrue();
    } finally {
      executor.shutdownNow();
    }
  }

  private static ReentrantReadWriteLock channelLock(final PaginatedComponentFile file) throws Exception {
    final Field field = PaginatedComponentFile.class.getDeclaredField("channelLock");
    field.setAccessible(true);
    return (ReentrantReadWriteLock) field.get(file);
  }

  private static ReentrantLock syncLock(final FileManager fileManager) throws Exception {
    final Field field = FileManager.class.getDeclaredField("syncLock");
    field.setAccessible(true);
    return (ReentrantLock) field.get(fileManager);
  }
}
