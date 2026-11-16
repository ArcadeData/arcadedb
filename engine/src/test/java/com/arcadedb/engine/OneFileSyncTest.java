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
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link FileManager#syncFile} and {@link PageManager#flushAndSyncWithoutWAL}, the one-file sync a vector graph build runs
 * while it holds the build lock the searches of its index wait on (issue #11805): it does not queue behind a full sync, it
 * still forces the directory a full sync owes, and its wait for the file's pages does not spin.
 */
class OneFileSyncTest extends TestHelper {

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
   * A copy of a page the disk refused stays in the pipeline but is not counted as pending, so the drain wait returns at
   * once for it: with only such copies left, the wait for the file's pages must still sleep between rechecks instead of
   * spinning on the CPU while the caller holds the graph-build lock.
   */
  @Test
  void waitingOnlyForARefusedPageDoesNotSpin() throws Exception {
    database.getSchema().createDocumentType("Refused", 1);
    database.transaction(() -> database.newDocument("Refused").set("i", 1).save());
    final DatabaseInternal db = (DatabaseInternal) database;
    assertThat(db.getPageManager().waitPagesPendingNowOfDatabaseAreFlushed(db, 30_000)).isTrue();
    final int fileId = db.getSchema().getType("Refused").getBuckets(false).getFirst().getFileId();
    final int pageSize = ((PaginatedComponentFile) db.getFileManager().getFile(fileId)).getPageSize();

    final Field threadField = PageManager.class.getDeclaredField("flushThread");
    threadField.setAccessible(true);
    final PageManagerFlushThread thread = (PageManagerFlushThread) threadField.get(db.getPageManager());
    final Field retryField = PageManagerFlushThread.class.getDeclaredField("lastFailedFlushRetryMs");
    retryField.setAccessible(true);
    final Field failedField = PageManagerFlushThread.class.getDeclaredField("failedFlushPages");
    failedField.setAccessible(true);
    final Map<?, ?> failed = (Map<?, ?>) failedField.get(thread);

    final PageId pageId = new PageId(db, fileId, 0);
    // No retry while the test runs: the copy stays refused
    retryField.setLong(thread, Long.MAX_VALUE);
    thread.registerFailedFlush(new MutablePage(pageId, pageSize));
    try {
      final ThreadMXBean threads = ManagementFactory.getThreadMXBean();
      final long cpuBefore = threads.getCurrentThreadCpuTime();
      final int stillPending = thread.waitPagesOfFileAreFlushed(db, fileId, new int[] { 0 }, 1, 1_000);
      final long cpuMillis = (threads.getCurrentThreadCpuTime() - cpuBefore) / 1_000_000;

      assertThat(stillPending).isEqualTo(1);
      assertThat(cpuMillis).as("CPU spent waiting 1 s for a refused page").isLessThan(300);
    } finally {
      failed.remove(pageId);
      retryField.setLong(thread, 0L);
    }
  }
}
