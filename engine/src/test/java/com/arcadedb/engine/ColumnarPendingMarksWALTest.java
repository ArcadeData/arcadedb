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
import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.TransactionContext;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Task 13, ordering fix (D): the first columnar mark of a commit runs before its pages are published, so an apply pass may drain it
 * from the previous page images. Until the second mark ran, the WAL file of the commit is its only record, and the housekeeping must
 * not drop it even once every page of the file reached the disk.
 */
class ColumnarPendingMarksWALTest extends TestHelper {

  @Override
  protected void endTest() {
    TransactionContext.afterPublishBeforeSecondMarkForTesting = null;
  }

  @Test
  void aCommitBetweenItsMarksKeepsItsWALFileUntilTheSecondMark() throws Exception {
    database.command("sql", "CREATE DOCUMENT TYPE Asset BUCKETS 1");
    database.command("sql", "CREATE PROPERTY Asset.brand STRING");
    database.command("sql", "CREATE INDEX ON Asset (brand) COLUMNAR");
    final TransactionManager transactionManager = ((DatabaseInternal) database).getTransactionManager();
    transactionManager.cancelHousekeepingForTesting();

    final CountDownLatch paused = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    TransactionContext.afterPublishBeforeSecondMarkForTesting = () -> {
      paused.countDown();
      try {
        release.await();
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    };

    final ExecutorService committer = Executors.newSingleThreadExecutor();
    try {
      final Future<?> commit = committer.submit(() -> database.transaction(() -> database.newDocument("Asset").set("brand", "a").save()));
      // a hang detector, not a latency bound
      assertThat(paused.await(30, TimeUnit.SECONDS)).isTrue();
      TransactionContext.afterPublishBeforeSecondMarkForTesting = null;

      // retire every WAL file, as a rotation does, and find the one the paused commit wrote to
      WALFile target = null;
      for (int slot = 0; slot < TransactionManager.walFilePoolSize(database.getConfiguration()); slot++) {
        final WALFile file = transactionManager.replaceActiveWALFileForTesting(slot, null);
        if (file == null)
          continue;
        file.setActive(false);
        transactionManager.addInactiveWALFileForTesting(file);
        if (file.getPendingColumnarMarks() > 0)
          target = file;
      }
      assertThat(target).isNotNull();
      final File targetFile = new File(target.getFilePath());

      // the published pages reach the disk: only the pending second mark protects the file now
      for (int i = 0; i < 30_000 && target.getPendingPagesToFlush() > 0; i++)
        Thread.sleep(1);
      assertThat(target.getPendingPagesToFlush()).isZero();

      assertThat(transactionManager.cleanWALFilesForTesting(true, false, false)).isFalse();
      assertThat(targetFile).exists();

      release.countDown();
      commit.get(30, TimeUnit.SECONDS);
      assertThat(target.getPendingColumnarMarks()).isZero();

      assertThat(transactionManager.cleanWALFilesForTesting(true, false, false)).isTrue();
      assertThat(targetFile).doesNotExist();
    } finally {
      release.countDown();
      committer.shutdownNow();
    }
  }

  /**
   * The same guarantee under the group commit (#10509), which queues the WAL record and writes it to a file only at the next flush
   * pass, after the pages are published: at the first mark no WAL file holds the record, so the file the pass writes it to must still
   * not be dropped before the second mark.
   */
  @Test
  void aGroupCommitBetweenItsMarksKeepsEveryWALFileUntilTheSecondMark() throws Exception {
    database.getConfiguration().setValue(GlobalConfiguration.TX_WAL_FLUSH, 1);
    database.getConfiguration().setValue(GlobalConfiguration.TX_WAL_GROUP_COMMIT, true);
    database.command("sql", "CREATE DOCUMENT TYPE Asset BUCKETS 1");
    database.command("sql", "CREATE PROPERTY Asset.brand STRING");
    database.command("sql", "CREATE INDEX ON Asset (brand) COLUMNAR");
    final TransactionManager transactionManager = ((DatabaseInternal) database).getTransactionManager();
    transactionManager.cancelHousekeepingForTesting();

    final CountDownLatch paused = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    TransactionContext.afterPublishBeforeSecondMarkForTesting = () -> {
      paused.countDown();
      try {
        release.await();
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    };

    final ExecutorService committer = Executors.newSingleThreadExecutor();
    try {
      final Future<?> commit = committer.submit(() -> database.transaction(() -> database.newDocument("Asset").set("brand", "a").save()));
      // a hang detector, not a latency bound
      assertThat(paused.await(30, TimeUnit.SECONDS)).isTrue();
      TransactionContext.afterPublishBeforeSecondMarkForTesting = null;

      // the record is still queued: another commit's flush pass writes and flushes it, here this thread's
      final long ticket = transactionManager.getPendingWALSyncTicket();
      assertThat(ticket).as("the paused commit's WAL record is queued by the group commit").isPositive();
      transactionManager.awaitWALDurable(ticket);

      // retire every WAL file, as a rotation does, and find the one the record landed in
      WALFile target = null;
      for (int slot = 0; slot < TransactionManager.walFilePoolSize(database.getConfiguration()); slot++) {
        final WALFile file = transactionManager.replaceActiveWALFileForTesting(slot, null);
        if (file == null)
          continue;
        file.setActive(false);
        transactionManager.addInactiveWALFileForTesting(file);
        if (file.getPendingPagesToFlush() > 0 || target == null)
          target = file;
      }
      assertThat(target).isNotNull();
      assertThat(target.getPendingColumnarMarks()).as("no WAL file held the record at the first mark").isZero();
      final File targetFile = new File(target.getFilePath());

      // the published pages reach the disk: only the pending second mark protects the file now
      for (int i = 0; i < 30_000 && target.getPendingPagesToFlush() > 0; i++)
        Thread.sleep(1);
      assertThat(target.getPendingPagesToFlush()).isZero();

      assertThat(transactionManager.cleanWALFilesForTesting(true, false, false)).isFalse();
      assertThat(targetFile).exists();

      release.countDown();
      commit.get(30, TimeUnit.SECONDS);

      assertThat(transactionManager.cleanWALFilesForTesting(true, false, false)).isTrue();
      assertThat(targetFile).doesNotExist();
    } finally {
      release.countDown();
      committer.shutdownNow();
    }
  }

  /**
   * {@link TransactionManager#forceWALFiles()} runs before a projection publishes its manifest, so that the projection is never more
   * durable than the commits it holds. A group commit's record queued after its pages were published is in no WAL file yet: the call
   * writes and flushes it.
   */
  @Test
  void forcingTheWALFilesMakesAQueuedGroupCommitDurable() throws Exception {
    database.getConfiguration().setValue(GlobalConfiguration.TX_WAL_FLUSH, 1);
    database.getConfiguration().setValue(GlobalConfiguration.TX_WAL_GROUP_COMMIT, true);
    database.command("sql", "CREATE DOCUMENT TYPE Asset BUCKETS 1");
    database.command("sql", "CREATE PROPERTY Asset.brand STRING");
    database.command("sql", "CREATE INDEX ON Asset (brand) COLUMNAR");
    final TransactionManager transactionManager = ((DatabaseInternal) database).getTransactionManager();

    final CountDownLatch paused = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    TransactionContext.afterPublishBeforeSecondMarkForTesting = () -> {
      paused.countDown();
      try {
        release.await();
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    };

    final ExecutorService committer = Executors.newSingleThreadExecutor();
    try {
      final Future<?> commit = committer.submit(() -> database.transaction(() -> database.newDocument("Asset").set("brand", "a").save()));
      // a hang detector, not a latency bound
      assertThat(paused.await(30, TimeUnit.SECONDS)).isTrue();
      TransactionContext.afterPublishBeforeSecondMarkForTesting = null;

      assertThat(transactionManager.getPendingWALSyncTicket()).as("the paused commit's WAL record is queued").isPositive();
      transactionManager.forceWALFiles();
      assertThat(transactionManager.getPendingWALSyncTicket()).as("forceWALFiles left a published commit out of the WAL").isZero();

      release.countDown();
      commit.get(30, TimeUnit.SECONDS);
    } finally {
      release.countDown();
      committer.shutdownNow();
    }
  }


  private TransactionManager tm() {
    return ((DatabaseInternal) database).getTransactionManager();
  }

  private WALFile retire(final String name) throws IOException {
    final WALFile file = new WALFile(new File(database.getDatabasePath(), name).getAbsolutePath());
    tm().addInactiveWALFileForTesting(file);
    return file;
  }

  // #11352
  @Test
  void aCommitStartedBeforeTheRetirementHoldsTheFileUntilItEnds() throws Exception {
    final long epoch = tm().queuedColumnarMarksStarted();
    final File wal = new File(database.getDatabasePath(), "issue11352-a.wal");
    retire("issue11352-a.wal");

    assertThat(tm().cleanWALFilesForTesting(true, false, false)).as("the queued commit may still land in the file").isFalse();
    assertThat(wal.exists()).isTrue();

    tm().queuedColumnarMarksEnded(epoch);

    assertThat(tm().cleanWALFilesForTesting(true, false, false)).isTrue();
    assertThat(wal.exists()).isFalse();
  }

  // #11352
  @Test
  void aCommitStartedAfterTheRetirementDoesNotHoldTheFile() throws Exception {
    final File wal = new File(database.getDatabasePath(), "issue11352-b.wal");
    retire("issue11352-b.wal");

    final long epoch = tm().queuedColumnarMarksStarted();
    try {
      assertThat(tm().cleanWALFilesForTesting(true, false, false))
          .as("a record queued after the file left the active pool cannot land in it: always-overlapping commits must not pin it").isTrue();
      assertThat(wal.exists()).isFalse();
    } finally {
      tm().queuedColumnarMarksEnded(epoch);
    }
  }

  // #11352
  @Test
  void overlappingCommitsNeverPinAFileRetiredBeforeTheNewestOfThem() throws Exception {
    // c1 in flight, F1 retired, c2 starts, c1 ends: the count never reaches zero (the old gate held F1 forever), yet F1 is free
    final long c1 = tm().queuedColumnarMarksStarted();
    final File wal1 = new File(database.getDatabasePath(), "issue11352-c1.wal");
    retire("issue11352-c1.wal");
    final long c2 = tm().queuedColumnarMarksStarted();
    tm().queuedColumnarMarksEnded(c1);

    assertThat(tm().cleanWALFilesForTesting(true, false, false)).isTrue();
    assertThat(wal1.exists()).isFalse();

    // the same for the next file, with c3 overlapping c2
    final File wal2 = new File(database.getDatabasePath(), "issue11352-c2.wal");
    retire("issue11352-c2.wal");
    assertThat(tm().cleanWALFilesForTesting(true, false, false)).as("c2 started before F2 was retired").isFalse();
    final long c3 = tm().queuedColumnarMarksStarted();
    tm().queuedColumnarMarksEnded(c2);
    assertThat(tm().cleanWALFilesForTesting(true, false, false)).isTrue();
    assertThat(wal2.exists()).isFalse();
    tm().queuedColumnarMarksEnded(c3);
  }

  // #11352
  @Test
  void theInactivePoolDrainsUnderOverlappingConcurrentCommitters() throws Exception {
    final int committers = 4;
    final int rounds = 200;
    final ExecutorService pool = Executors.newFixedThreadPool(committers);
    final CountDownLatch go = new CountDownLatch(1);
    try {
      final List<Future<?>> futures = new ArrayList<>();
      for (int t = 0; t < committers; t++)
        futures.add(pool.submit(() -> {
          go.await();
          // each committer keeps one commit open while it starts the next: at every instant at least one is in flight
          long previous = tm().queuedColumnarMarksStarted();
          for (int i = 0; i < rounds; i++) {
            final long next = tm().queuedColumnarMarksStarted();
            tm().queuedColumnarMarksEnded(previous);
            previous = next;
          }
          tm().queuedColumnarMarksEnded(previous);
          return null;
        }));

      go.countDown();
      // rotations happen while the committers run
      for (int i = 0; i < 20; i++)
        retire("issue11352-d" + i + ".wal");
      for (final Future<?> f : futures)
        f.get(60, TimeUnit.SECONDS);
    } finally {
      pool.shutdownNow();
    }

    assertThat(tm().cleanWALFilesForTesting(true, false, false)).as("every committer ended: the whole inactive pool drains").isTrue();
    for (int i = 0; i < 20; i++)
      assertThat(new File(database.getDatabasePath(), "issue11352-d" + i + ".wal").exists()).isFalse();
  }
}
