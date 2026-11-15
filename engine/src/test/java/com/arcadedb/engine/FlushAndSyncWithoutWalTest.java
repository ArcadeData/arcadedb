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
package com.arcadedb.engine;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link PageManager#flushAndSyncWithoutWAL} makes ONE file durable: the vector graph build calls it on the graph file it
 * committed without the WAL, while holding the build lock concurrent searches of the index wait on. It used to force every
 * file of the database written since its last fsync, so each small-graph rebuild a search triggered paid one fsync per
 * file that writers of other types had touched: with a second indexed type taking inserts, searches of a 128-dimension
 * index with 800 vectors went from 2.4 ms to 8.4 s at the median.
 */
class FlushAndSyncWithoutWalTest extends TestHelper {

  @Test
  void forcesTheFileItIsAskedForAndNoOther() {
    database.getSchema().createDocumentType("Asked", 1);
    database.getSchema().createDocumentType("Other", 1);
    database.transaction(() -> {
      for (int i = 0; i < 100; i++) {
        database.newDocument("Asked").set("i", i).save();
        database.newDocument("Other").set("i", i).save();
      }
    });

    final DatabaseInternal db = (DatabaseInternal) database;
    final PageManager pageManager = db.getPageManager();
    assertThat(pageManager.waitPagesPendingNowOfDatabaseAreFlushed(db, 30_000)).isTrue();

    final LocalBucket askedBucket = (LocalBucket) db.getSchema().getType("Asked").getBuckets(false).getFirst();
    final int askedFileId = askedBucket.getFileId();
    final int otherFileId = db.getSchema().getType("Other").getBuckets(false).getFirst().getFileId();
    final PaginatedComponentFile asked = (PaginatedComponentFile) db.getFileManager().getFile(askedFileId);
    final PaginatedComponentFile other = (PaginatedComponentFile) db.getFileManager().getFile(otherFileId);
    assertThat(asked.isModifiedSinceLastSync()).isTrue();
    assertThat(other.isModifiedSinceLastSync()).isTrue();

    final int[] notOnDisk = pageManager.flushAndSyncWithoutWAL(db, askedFileId, askedBucket.getTotalPages(), 30_000);

    assertThat(notOnDisk).isEmpty();
    assertThat(asked.isModifiedSinceLastSync()).as("the file asked for is forced").isFalse();
    assertThat(other.isModifiedSinceLastSync()).as("a file nobody asked for is left to the next full sync").isTrue();
  }

  /**
   * A full sync in progress (a WAL rotation fsyncing every written file holds the database-wide sync lock throughout) does
   * not hold up a sync of one file: during an application ingest the graph build waited about 1.1 s per call for it, with
   * the searches of its index queued behind the build.
   */
  @Test
  void aOneFileSyncDoesNotWaitForAFullSyncInProgress() throws Exception {
    database.getSchema().createDocumentType("Asked", 1);
    database.transaction(() -> database.newDocument("Asked").set("i", 1).save());
    final DatabaseInternal db = (DatabaseInternal) database;
    assertThat(db.getPageManager().waitPagesPendingNowOfDatabaseAreFlushed(db, 30_000)).isTrue();
    final int askedFileId = db.getSchema().getType("Asked").getBuckets(false).getFirst().getFileId();
    final PaginatedComponentFile asked = (PaginatedComponentFile) db.getFileManager().getFile(askedFileId);
    assertThat(asked.isModifiedSinceLastSync()).isTrue();

    final Field syncLockField = FileManager.class.getDeclaredField("syncLock");
    syncLockField.setAccessible(true);
    final ReentrantLock syncLock = (ReentrantLock) syncLockField.get(db.getFileManager());
    final CountDownLatch held = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    final Thread fullSync = new Thread(() -> {
      syncLock.lock();
      try {
        held.countDown();
        release.await(30, TimeUnit.SECONDS);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      } finally {
        syncLock.unlock();
      }
    });
    fullSync.start();
    final ExecutorService caller = Executors.newSingleThreadExecutor();
    try {
      assertThat(held.await(10, TimeUnit.SECONDS)).isTrue();
      final Future<Boolean> synced = caller.submit(() -> db.getFileManager().syncFile(askedFileId));

      assertThat(synced.get(10, TimeUnit.SECONDS)).as("the one-file sync completes while the full sync holds its lock").isTrue();
      assertThat(asked.isModifiedSinceLastSync()).isFalse();
    } finally {
      release.countDown();
      fullSync.join(10_000);
      caller.shutdownNow();
    }
  }

  /**
   * A file a full sync forced with its metadata is not durable until that sync forces its directory too, after its loop
   * over the files. A one-file sync arriving in between finds the file clean, and forces the directory itself.
   */
  @Test
  void aOneFileSyncForcesTheDirectoryAFullSyncStillOwes() throws IOException {
    database.getSchema().createDocumentType("Created", 1);
    final DatabaseInternal db = (DatabaseInternal) database;
    final int fileId = db.getSchema().getType("Created").getBuckets(false).getFirst().getFileId();
    final PaginatedComponentFile created = (PaginatedComponentFile) db.getFileManager().getFile(fileId);
    created.markUnsynced();

    // What syncFiles() does first for this file, before the directories
    assertThat(created.directoryForceOwed()).isZero();
    assertThat(created.forceIfModified()).isEqualTo(PaginatedComponentFile.SYNC_METADATA);
    assertThat(created.directoryForceOwed()).as("forced with its metadata, the directory still owed").isPositive();

    assertThat(db.getFileManager().syncFile(fileId)).isTrue();
    assertThat(created.directoryForceOwed()).as("the one-file sync forced the directory").isZero();
  }
}
