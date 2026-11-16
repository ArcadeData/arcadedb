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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.BasicDatabase;
import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
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
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

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

  @BeforeEach
  void fileSyncOn() {
    // the failures come from a real fsync, which the unit-test lanes turn off (arcadedb.fileSync)
    GlobalConfiguration.FILE_SYNC.setValue(true);
  }

  @AfterEach
  void tearDown() {
    for (final PaginatedComponentFile f : files)
      f.close();
    GlobalConfiguration.FILE_SYNC.reset();
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
      final AtomicReference<Thread> secondThread = new AtomicReference<>();
      channelLock.writeLock().lock();
      try {
        // The first sync (the WAL rotation timer, say) claims the file and queues behind the channel lock.
        first = executor.submit(fileManager::syncFiles);
        awaitQueued(channelLock, 1);

        // A second sync (a clean close) must not find the claimed file clean and report success: it has to wait.
        second = executor.submit(() -> {
          secondThread.set(Thread.currentThread());
          return fileManager.syncFiles();
        });
        awaitQueuedOnSyncLock(fileManager, secondThread, second);

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
  void syncOfOneFileDoesNotWaitForAPassStuckOnAnotherFile() throws Exception {
    final FileManager fileManager = new FileManager(tempDir.toString(), ComponentFile.MODE.READ_WRITE, Set.of("arc"));
    final ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      final PaginatedComponentFile stuck = (PaginatedComponentFile) fileManager.getOrCreateFile(FILE_ID, filePath("stuck"));
      final PaginatedComponentFile mine = (PaginatedComponentFile) fileManager.getOrCreateFile(FILE_ID + 1,
          tempDir.resolve("mine." + (FILE_ID + 1) + "." + PAGE_SIZE + ".v0.arc").toString());
      writePage(stuck, 0);
      final ReentrantReadWriteLock channelLock = channelLock(stuck);

      final Future<Boolean> pass;
      channelLock.writeLock().lock();
      try {
        // A pass over every file (the WAL housekeeping's) gets stuck on one of them, holding the syncFiles() lock.
        pass = executor.submit(fileManager::syncFiles);
        awaitQueued(channelLock, 1);

        // A writer that owes the durability of ANOTHER file only must not wait for that pass (issue #11932).
        final AtomicReference<Thread> syncThread = new AtomicReference<>();
        final Future<Boolean> sync = executor.submit(() -> {
          syncThread.set(Thread.currentThread());
          return fileManager.syncFile(FILE_ID + 1);
        });
        final ReentrantLock syncLock = syncLock(fileManager);
        final StallAwareStopwatch wait = StallAwareStopwatch.start();
        while (!sync.isDone()) {
          assertThat(syncThread.get() != null && syncLock.hasQueuedThread(syncThread.get()))
              .as("the sync of one file queued behind a pass over the other files (issue #11932)").isFalse();
          wait.assertGaveUpWithin(30_000L, "a sync of one file that returns from one that never does");
          Thread.sleep(1);
        }
        assertThat(sync.get()).isTrue();
        assertThat(mine.isModifiedSinceLastSync()).isFalse();
        assertThat(pass.isDone()).as("the pass is still stuck on the other file").isFalse();
      } finally {
        channelLock.writeLock().unlock();
      }
      assertThat(pass.get(30, TimeUnit.SECONDS)).isTrue();
    } finally {
      executor.shutdownNow();
      fileManager.close();
    }
  }

  @Test
  void aPassForcesTheDirectoryASyncOfOneFileStillOwes() throws Exception {
    final FileManager fileManager = new FileManager(tempDir.toString(), ComponentFile.MODE.READ_WRITE, Set.of("arc"));
    try {
      final PaginatedComponentFile file = (PaginatedComponentFile) fileManager.getOrCreateFile(FILE_ID, filePath("created"));

      // A syncFile() of the created file has forced it, metadata included, and not yet reached its directory force
      assertThat(file.forceIfModified()).isEqualTo(PaginatedComponentFile.SYNC_METADATA);
      assertThat(file.directorySyncOwed()).as("the created file owes its directory entry").isPositive();

      // A pass finds the file itself clean, and cannot report it durable without its directory entry (issue #11932)
      assertThat(fileManager.syncFiles()).isTrue();
      assertThat(file.directorySyncOwed()).as("the pass forced the directory the file still owed").isZero();
    } finally {
      fileManager.close();
    }
  }

  @Test
  void aDirectoryForceAcknowledgesOnlyTheMetadataItCovered() throws Exception {
    final PaginatedComponentFile file = open(filePath("renamedMeanwhile"));
    assertThat(file.forceIfModified()).isEqualTo(PaginatedComponentFile.SYNC_METADATA);
    // A directory force reads what it covers before it runs...
    final long covered = file.directorySyncOwed();
    assertThat(covered).isPositive();

    // ...and meanwhile the file is renamed and forced again by another caller, whose directory force is still to come
    file.markUnsynced();
    assertThat(file.forceIfModified()).isEqualTo(PaginatedComponentFile.SYNC_METADATA);

    file.directorySynced(covered);
    assertThat(file.directorySyncOwed())
        .as("the earlier directory force must not acknowledge the metadata forced after it read the generation (issue #11932)")
        .isGreaterThan(covered);
  }

  /**
   * The same interleaving through {@link FileManager#syncFile(int)} (issue #11932): the file is renamed and forced by another
   * caller after the sync read the generation it owes and before it forced the directory. That force covers only the
   * generation it read, so the rename stays owed, and the next sync of the file forces the directory again for it.
   */
  @Test
  void aRenameForcedWhileASyncOfTheFileForcesItsDirectoryStaysOwedToTheNextSync() throws Exception {
    final FileManager fileManager = new FileManager(tempDir.toString(), ComponentFile.MODE.READ_WRITE, Set.of("arc"));
    try {
      final PaginatedComponentFile file = (PaginatedComponentFile) fileManager.getOrCreateFile(FILE_ID, filePath("renamedInWindow"));
      final List<Long> owedAtEachDirectoryForce = new ArrayList<>();
      final AtomicBoolean renamed = new AtomicBoolean();
      fileManager.beforeDirectoryForceForTesting = () -> {
        owedAtEachDirectoryForce.add(file.directorySyncOwed());
        if (renamed.compareAndSet(false, true))
          try {
            file.renameComponent("renamedInWindowAgain");
            assertThat(file.forceIfModified()).as("the rename forced by another caller").isEqualTo(PaginatedComponentFile.SYNC_METADATA);
          } catch (final IOException e) {
            throw new RuntimeException(e);
          }
      };

      assertThat(fileManager.syncFile(FILE_ID)).isTrue();
      assertThat(owedAtEachDirectoryForce).as("directory forces of the first sync").hasSize(1);
      final long coveredByTheFirst = owedAtEachDirectoryForce.getFirst();
      assertThat(coveredByTheFirst).isPositive();
      assertThat(file.directorySyncOwed()).as("the rename forced after the first sync read its generation is still owed")
          .isGreaterThan(coveredByTheFirst);

      // The file itself is clean now: only the directory entry of the rename is owed, and the next sync forces it
      assertThat(fileManager.syncFile(FILE_ID)).isTrue();
      assertThat(owedAtEachDirectoryForce).as("the next sync forced the directory").hasSize(2);
      assertThat(file.directorySyncOwed()).as("the directory force of the next sync covered the rename").isZero();

      assertThat(fileManager.syncFile(FILE_ID)).isTrue();
      assertThat(owedAtEachDirectoryForce).as("a sync that owes nothing forces no directory").hasSize(2);
    } finally {
      fileManager.beforeDirectoryForceForTesting = null;
      fileManager.close();
    }
  }

  @Test
  void aDirectoryForceThatFailedLeavesItsGenerationOwed() throws Exception {
    final FileManager fileManager = new FileManager(tempDir.toString(), ComponentFile.MODE.READ_WRITE, Set.of("arc"));
    final File directory = tempDir.toFile();
    try {
      final PaginatedComponentFile file = (PaginatedComponentFile) fileManager.getOrCreateFile(FILE_ID, filePath("created"));

      // A directory that cannot be opened for reading cannot be forced: FileUtils.forceDirectory() answers false
      assumeTrue(directory.setReadable(false), "the file system has no read permission to take away");
      assumeFalse(Files.isReadable(tempDir), "the process reads the directory whatever its permissions (root)");

      assertThat(fileManager.syncFile(FILE_ID)).as("a directory that cannot be forced does not fail the sync").isTrue();
      assertThat(file.directorySyncOwed())
          .as("the created file still owes its directory entry: the force did not happen (issue #11932)").isPositive();
      assertThat(fileManager.syncFiles()).isTrue();
      assertThat(file.directorySyncOwed()).as("nor did the pass force it").isPositive();

      assertThat(directory.setReadable(true)).isTrue();
      assertThat(fileManager.syncFile(FILE_ID)).isTrue();
      assertThat(file.directorySyncOwed()).as("the next sync that can force the directory pays what is owed").isZero();
    } finally {
      directory.setReadable(true);
      fileManager.close();
    }
  }

  @Test
  void aSkippedDirectoryForceOwesNothing() throws Exception {
    final FileManager fileManager = new FileManager(tempDir.toString(), ComponentFile.MODE.READ_WRITE, Set.of("arc"));
    try {
      final PaginatedComponentFile file = (PaginatedComponentFile) fileManager.getOrCreateFile(FILE_ID, filePath("unsynced"));
      assertThat(file.forceIfModified()).isEqualTo(PaginatedComponentFile.SYNC_METADATA);
      assertThat(file.directorySyncOwed()).isPositive();

      // With fsync off FileUtils.forceDirectory() returns false without trying: not a failure, nothing to retry
      GlobalConfiguration.FILE_SYNC.setValue(false);
      assertThat(fileManager.syncFile(FILE_ID)).isTrue();
      assertThat(file.directorySyncOwed()).as("a skipped directory force must not stay owed on every later pass").isZero();
    } finally {
      fileManager.close();
    }
  }

  @Test
  void syncOfOneFileWaitsForThePassForcingItAndSeesItsFailure() throws Exception {
    final FileManager fileManager = new FileManager(tempDir.toString(), ComponentFile.MODE.READ_WRITE, Set.of("arc"));
    final ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      final PaginatedComponentFile file = (PaginatedComponentFile) fileManager.getOrCreateFile(FILE_ID, filePath("shared"));
      writePage(file, 0);
      final ReentrantReadWriteLock channelLock = channelLock(file);

      final Future<Boolean> pass;
      final Future<Boolean> sync;
      final AtomicReference<Thread> syncThread = new AtomicReference<>();
      channelLock.writeLock().lock();
      try {
        // The pass claims the file and queues behind the channel lock, its fsync not done yet.
        pass = executor.submit(fileManager::syncFiles);
        awaitQueued(channelLock, 1);

        // The sync of that same file must not find it clean and report it durable: it waits for the pass's fsync.
        sync = executor.submit(() -> {
          syncThread.set(Thread.currentThread());
          return fileManager.syncFile(FILE_ID);
        });
        final ReentrantLock forceLock = forceLock(file);
        final StallAwareStopwatch wait = StallAwareStopwatch.start();
        while (syncThread.get() == null || !forceLock.hasQueuedThread(syncThread.get())) {
          assertThat(sync.isDone()).as("the sync returned while the pass's fsync of the file was still in flight").isFalse();
          wait.assertGaveUpWithin(30_000L, "a sync that queues on the file's force lock from one that never does");
          Thread.sleep(1);
        }

        // Now make the in-flight fsync fail, as #4934 does: channel closed underneath and the OS file gone.
        breakFile(file);
      } finally {
        channelLock.writeLock().unlock();
      }

      assertThat(pass.get(30, TimeUnit.SECONDS)).isFalse();
      assertThat(sync.get(30, TimeUnit.SECONDS))
          .as("a sync that waited for a failed fsync of its file must not report the file durable").isFalse();
      assertThat(file.isModifiedSinceLastSync()).isTrue();
    } finally {
      executor.shutdownNow();
      fileManager.close();
    }
  }

  @Test
  void concurrentWritesAndSyncsLoseNeitherPagesNorPendingState() throws Exception {
    final int writers = 4;
    final int syncers = 2;
    final int pagesPerWriter = 250;
    // THROUGH FileManager.syncFiles(), WITH TWO SYNCERS: THE CLAIM/RAISE PROTOCOL AND THE SYNC LOCK ARE BOTH EXERCISED
    final FileManager fileManager = new FileManager(tempDir.toString(), ComponentFile.MODE.READ_WRITE, Set.of("arc"));
    final ExecutorService executor = Executors.newFixedThreadPool(writers + syncers);
    final AtomicBoolean writing = new AtomicBoolean(true);
    try {
      final PaginatedComponentFile file = (PaginatedComponentFile) fileManager.getOrCreateFile(FILE_ID, filePath("stress"));

      final CountDownLatch syncersRunning = new CountDownLatch(syncers);
      final List<Future<Boolean>> syncs = new ArrayList<>();
      for (int s = 0; s < syncers; s++)
        syncs.add(executor.submit(() -> {
          boolean allSynced = true;
          // DO-WHILE, AND THE WRITERS WAIT FOR EVERY SYNCER'S FIRST PASS: THE SYNCERS ARE ALREADY LOOPING WHEN THE FIRST
          // PAGE IS WRITTEN, HOWEVER THE THREADS ARE SCHEDULED
          do {
            allSynced &= fileManager.syncFiles();
            syncersRunning.countDown();
          } while (writing.get());
          return allSynced;
        }));

      final List<Future<Void>> writes = new ArrayList<>();
      for (int w = 0; w < writers; w++) {
        final int writer = w;
        writes.add(executor.submit(() -> {
          assertThat(syncersRunning.await(30, TimeUnit.SECONDS)).isTrue();
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
      for (final Future<Boolean> f : syncs)
        assertThat(f.get(60, TimeUnit.SECONDS)).isTrue();
      // THE FIRST PASS ALWAYS FORCES: A CREATED FILE OWES ITS METADATA
      assertThat(fileManager.getStats().syncedFiles).as("the syncers must have run").isPositive();

      // Every write returned before this sync was invoked, so this one - or an earlier one that claimed after the
      // write - covers it. Either way the file is settled afterwards and a further sync has nothing left to force.
      assertThat(fileManager.syncFiles()).isTrue();
      assertThat(file.isModifiedSinceLastSync()).isFalse();
      assertThat(file.forceIfModified()).isEqualTo(PaginatedComponentFile.SYNC_CLEAN);

      final ByteBuffer read = ByteBuffer.allocate(PAGE_SIZE);
      for (int pageNumber = 0; pageNumber < writers * pagesPerWriter; pageNumber++) {
        file.readPage(pageNumber, read);
        assertThat(read.array()).as("page %d", pageNumber).isEqualTo(pageContent(pageNumber));
      }
    } finally {
      // IN A FINALLY: A FAILED WRITER MUST NOT LEAVE THE SYNCERS SPINNING, THEIR LOOP DOES NO INTERRUPTIBLE WORK
      writing.set(false);
      executor.shutdownNow();
      fileManager.close();
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

  /**
   * Waits until {@code thread} is queued on the {@code syncFiles()} lock itself, asked of the lock rather than inferred
   * from the thread state, so the wait cannot be satisfied by the thread parking anywhere else.
   */
  private static void awaitQueuedOnSyncLock(final FileManager fileManager, final AtomicReference<Thread> thread,
      final Future<?> future) throws Exception {
    final ReentrantLock syncLock = syncLock(fileManager);

    final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
    while (thread.get() == null || !syncLock.hasQueuedThread(thread.get())) {
      assertThat(future.isDone()).as("the second sync returned while the first one's fsync was still in flight").isFalse();
      assertThat(System.nanoTime()).as("the second sync never queued on the sync lock").isLessThan(deadline);
      Thread.sleep(1);
    }
  }

  private static ReentrantLock syncLock(final FileManager fileManager) throws Exception {
    final Field field = FileManager.class.getDeclaredField("syncLock");
    field.setAccessible(true);
    return (ReentrantLock) field.get(fileManager);
  }

  private static ReentrantLock forceLock(final PaginatedComponentFile file) throws Exception {
    final Field field = PaginatedComponentFile.class.getDeclaredField("forceLock");
    field.setAccessible(true);
    return (ReentrantLock) field.get(file);
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
