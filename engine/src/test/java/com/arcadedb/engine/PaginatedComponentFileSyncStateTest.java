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

import com.arcadedb.database.BasicDatabase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * What a data file owes the disk (issue #8626). A file is forced only when something happened to it since its last
 * successful fsync: a page write needs the data synced, a creation or a rename also needs the metadata. A file that
 * was only read owes nothing, which is what makes a clean close of a read-only session cost no fsync at all.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class PaginatedComponentFileSyncStateTest {
  private static final int PAGE_SIZE = 1024;
  private static final int FILE_ID   = 1;

  @TempDir
  Path tempDir;

  private final BasicDatabase                db    = Mockito.mock(BasicDatabase.class);
  private final List<PaginatedComponentFile> files = new ArrayList<>();

  @AfterEach
  void tearDown() {
    for (final PaginatedComponentFile f : files)
      f.close();
  }

  @Test
  void createdFileOwesAMetadataSyncOnce() throws IOException {
    final PaginatedComponentFile file = open(filePath("created"));

    assertThat(file.isModifiedSinceLastSync()).isTrue();
    assertThat(file.forceIfModified()).isEqualTo(PaginatedComponentFile.SYNC_METADATA);

    assertThat(file.isModifiedSinceLastSync()).isFalse();
    assertThat(file.forceIfModified()).isEqualTo(PaginatedComponentFile.SYNC_CLEAN);
  }

  @Test
  void existingFileOpensClean() throws IOException {
    final String path = filePath("existing");
    final PaginatedComponentFile creator = open(path);
    writePage(creator, 0);
    creator.force(true);
    creator.close();

    final PaginatedComponentFile reopened = open(path);
    assertThat(reopened.isModifiedSinceLastSync()).isFalse();

    final MutablePage page = new MutablePage(new PageId(db, FILE_ID, 0), PAGE_SIZE);
    reopened.read(new CachedPage(page, false));
    assertThat(reopened.isModifiedSinceLastSync()).as("a read owes the disk nothing").isFalse();
    assertThat(reopened.forceIfModified()).isEqualTo(PaginatedComponentFile.SYNC_CLEAN);
  }

  @Test
  void pageWriteOwesADataSync() throws IOException {
    final PaginatedComponentFile file = open(filePath("written"));
    file.forceIfModified();

    writePage(file, 0);
    assertThat(file.isModifiedSinceLastSync()).isTrue();
    assertThat(file.forceIfModified()).isEqualTo(PaginatedComponentFile.SYNC_DATA);
    assertThat(file.forceIfModified()).isEqualTo(PaginatedComponentFile.SYNC_CLEAN);
  }

  @Test
  void pageWriteDoesNotDowngradeAPendingMetadataSync() throws IOException {
    final PaginatedComponentFile file = open(filePath("createdThenWritten"));

    writePage(file, 0);
    writePage(file, 1);
    assertThat(file.forceIfModified()).isEqualTo(PaginatedComponentFile.SYNC_METADATA);
  }

  @Test
  void unconditionalForceSettlesThePendingState() throws IOException {
    final PaginatedComponentFile file = open(filePath("forced"));
    writePage(file, 0);

    file.force(false);
    assertThat(file.isModifiedSinceLastSync()).isFalse();
  }

  @Test
  void renameOwesAMetadataSync() throws IOException {
    final PaginatedComponentFile file = open(filePath("renamed"));
    writePage(file, 0);
    file.forceIfModified();

    file.renameComponent("renamedAgain");
    assertThat(file.forceIfModified()).isEqualTo(PaginatedComponentFile.SYNC_METADATA);
  }

  @Test
  void markUnsyncedForcesTheMetadataOfAnExistingFile() throws IOException {
    final String path = filePath("marked");
    open(path).close();

    final PaginatedComponentFile reopened = open(path);
    reopened.markUnsynced();
    assertThat(reopened.forceIfModified()).isEqualTo(PaginatedComponentFile.SYNC_METADATA);
  }

  @Test
  void failedSyncKeepsTheFileOwingTheSync() throws Exception {
    final PaginatedComponentFile file = open(filePath("failing"));
    file.forceIfModified();
    writePage(file, 0);

    // Close the channel underneath and delete the OS file: the #4930 reopen guard then surfaces the failure from the
    // force instead of re-creating the file, which is how the #4934 test breaks an fsync too.
    breakFile(file);

    assertThatThrownBy(file::forceIfModified).isInstanceOf(IOException.class);
    assertThat(file.isModifiedSinceLastSync())
        .as("a failed fsync must leave the file owing it, or the next sync pass would skip the unsynced pages").isTrue();
  }

  @Test
  void writeLandingAfterAClaimIsLeftForTheNextSync() throws Exception {
    final PaginatedComponentFile file = open(filePath("claimThenWrite"));
    final ReentrantReadWriteLock channelLock = channelLock(file);
    final ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      final Future<Integer> sync;
      final Future<Void> write;
      channelLock.writeLock().lock();
      try {
        // The sync claims the pending state and then queues behind the channel lock, before it can force anything.
        sync = executor.submit(file::forceIfModified);
        awaitQueued(channelLock, 1);
        assertThat(file.isModifiedSinceLastSync()).as("the sync claims the state before it forces").isFalse();

        write = executor.submit(() -> {
          writePage(file, 0);
          return null;
        });
        awaitQueued(channelLock, 2);
      } finally {
        channelLock.writeLock().unlock();
      }

      assertThat(sync.get(30, TimeUnit.SECONDS)).isEqualTo(PaginatedComponentFile.SYNC_METADATA);
      write.get(30, TimeUnit.SECONDS);

      // Whichever of the two got the channel first, the page landed after the claim: that sync cannot vouch for it, so
      // the file must still owe the next one.
      assertThat(file.isModifiedSinceLastSync()).as("a write after the claim must not be lost").isTrue();
      assertThat(file.forceIfModified()).isEqualTo(PaginatedComponentFile.SYNC_DATA);
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  void concurrentSyncWaitsForTheOneInFlightAndSeesItsFailure() throws Exception {
    final FileManager fileManager = new FileManager(tempDir.toString(), ComponentFile.MODE.READ_WRITE, Set.of("arc"));
    final ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      final PaginatedComponentFile file = (PaginatedComponentFile) fileManager.getOrCreateFile(FILE_ID, filePath("shared"));
      writePage(file, 0);
      final ReentrantReadWriteLock channelLock = channelLock(file);

      final Future<Boolean> first;
      final Future<Boolean> second;
      final Thread[] secondThread = new Thread[1];
      channelLock.writeLock().lock();
      try {
        // The first sync (the WAL rotation timer, say) claims the file and queues behind the channel lock.
        first = executor.submit(fileManager::syncFiles);
        awaitQueued(channelLock, 1);

        // A second sync (a clean close) must not find the claimed file clean and report success: it has to wait.
        second = executor.submit(() -> {
          secondThread[0] = Thread.currentThread();
          return fileManager.syncFiles();
        });
        awaitBlocked(secondThread, second);

        // Now make the in-flight fsync fail, as #4934 does: channel closed underneath and the OS file gone.
        breakFile(file);
      } finally {
        channelLock.writeLock().unlock();
      }

      assertThat(first.get(30, TimeUnit.SECONDS)).isFalse();
      assertThat(second.get(30, TimeUnit.SECONDS))
          .as("a sync that ran while another one's fsync failed must not report the file durable").isFalse();
      assertThat(file.isModifiedSinceLastSync()).isTrue();
    } finally {
      executor.shutdownNow();
      fileManager.close();
    }
  }

  @Test
  void concurrentWritesAndSyncsLoseNeitherPagesNorPendingState() throws Exception {
    final int writers = 4;
    final int pagesPerWriter = 250;
    final PaginatedComponentFile file = open(filePath("stress"));

    final ExecutorService executor = Executors.newFixedThreadPool(writers + 1);
    try {
      final AtomicBoolean writing = new AtomicBoolean(true);
      final CountDownLatch syncerRunning = new CountDownLatch(1);
      final Future<Integer> syncer = executor.submit(() -> {
        int forced = 0;
        // DO-WHILE, AND THE WRITERS WAIT FOR THE FIRST PASS: THE SYNCER RUNS AT LEAST ONCE AND IS ALREADY LOOPING WHEN THE
        // FIRST PAGE IS WRITTEN, HOWEVER THE THREADS ARE SCHEDULED
        do {
          if (file.forceIfModified() != PaginatedComponentFile.SYNC_CLEAN)
            ++forced;
          syncerRunning.countDown();
        } while (writing.get());
        return forced;
      });

      final List<Future<Void>> writes = new ArrayList<>();
      for (int w = 0; w < writers; w++) {
        final int writer = w;
        writes.add(executor.submit(() -> {
          assertThat(syncerRunning.await(30, TimeUnit.SECONDS)).isTrue();
          for (int i = 0; i < pagesPerWriter; i++) {
            final int pageNumber = writer * pagesPerWriter + i;
            file.write(new MutablePage(new PageId(db, FILE_ID, pageNumber), PAGE_SIZE, pageContent(pageNumber), 1,
                PAGE_SIZE));
          }
          return null;
        }));
      }
      for (final Future<Void> f : writes)
        f.get(60, TimeUnit.SECONDS);
      writing.set(false);
      // THE FIRST PASS ALWAYS FORCES: A CREATED FILE OWES ITS METADATA
      assertThat(syncer.get(60, TimeUnit.SECONDS)).as("the syncer must have run").isPositive();

      // Every write returned before this sync was invoked, so this one - or an earlier one that claimed after the
      // write - covers it. Either way the file is settled afterwards and a further sync has nothing left to force.
      file.forceIfModified();
      assertThat(file.isModifiedSinceLastSync()).isFalse();
      assertThat(file.forceIfModified()).isEqualTo(PaginatedComponentFile.SYNC_CLEAN);

      final ByteBuffer read = ByteBuffer.allocate(PAGE_SIZE);
      for (int pageNumber = 0; pageNumber < writers * pagesPerWriter; pageNumber++) {
        file.readPage(pageNumber, read);
        assertThat(read.array()).as("page %d", pageNumber).isEqualTo(pageContent(pageNumber));
      }
    } finally {
      executor.shutdownNow();
    }
  }

  private PaginatedComponentFile open(final String path) throws IOException {
    final PaginatedComponentFile file = new PaginatedComponentFile(path, ComponentFile.MODE.READ_WRITE);
    files.add(file);
    return file;
  }

  private String filePath(final String name) {
    return tempDir.resolve(name + "." + FILE_ID + "." + PAGE_SIZE + ".v0.arc").toString();
  }

  private static byte[] pageContent(final int pageNumber) {
    final byte[] content = new byte[PAGE_SIZE];
    Arrays.fill(content, (byte) pageNumber);
    content[0] = (byte) (pageNumber >>> 8);
    return content;
  }

  private static ReentrantReadWriteLock channelLock(final PaginatedComponentFile file) throws Exception {
    final Field field = PaginatedComponentFile.class.getDeclaredField("channelLock");
    field.setAccessible(true);
    return (ReentrantReadWriteLock) field.get(file);
  }

  private static void awaitQueued(final ReentrantReadWriteLock lock, final int threads) throws InterruptedException {
    final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
    while (lock.getQueueLength() < threads) {
      assertThat(System.nanoTime()).as("threads never queued on the channel lock").isLessThan(deadline);
      Thread.sleep(1);
    }
  }

  private static void awaitBlocked(final Thread[] thread, final Future<?> future) throws InterruptedException {
    final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
    // BLOCKED ON A MONITOR, OR WAITING ON A java.util.concurrent LOCK: EITHER WAY PARKED ON THE SYNC LOCK
    while (thread[0] == null || (thread[0].getState() != Thread.State.BLOCKED && thread[0].getState() != Thread.State.WAITING)) {
      assertThat(future.isDone()).as("the second sync returned while the first one's fsync was still in flight").isFalse();
      assertThat(System.nanoTime()).as("the second sync never blocked").isLessThan(deadline);
      Thread.sleep(1);
    }
  }

  private static void breakFile(final PaginatedComponentFile file) throws Exception {
    final Field channelField = PaginatedComponentFile.class.getDeclaredField("channel");
    channelField.setAccessible(true);
    ((FileChannel) channelField.get(file)).close();
    assertThat(new File(file.getFilePath()).delete()).isTrue();
  }

  private void writePage(final PaginatedComponentFile file, final int pageNumber) throws IOException {
    file.write(new MutablePage(new PageId(db, FILE_ID, pageNumber), PAGE_SIZE, new byte[PAGE_SIZE], 1, PAGE_SIZE));
  }
}
