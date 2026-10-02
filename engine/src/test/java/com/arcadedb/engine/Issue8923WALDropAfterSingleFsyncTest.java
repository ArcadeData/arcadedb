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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Timer;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Regression test for issue #8923: the runtime WAL rotation pass ({@code cleanWALFiles(true, false, true)}, the call
 * {@code runWALHousekeeping()} makes) fsyncs the data files once, lazily, right before the FIRST inactive WAL it
 * drops, and only then reads the pending-pages counter of the files after it. The flush thread decrements that
 * counter without taking any lock the pass holds, right after a {@code write()} that only reaches the OS page cache,
 * so a later file whose last page was written after the single fsync read zero pending pages and was deleted while
 * that page was still only in the OS cache - a power loss then had nothing left to replay it from.
 * <p>
 * The flush thread's write is placed exactly in that window here: file B's last pending page is acknowledged from
 * inside file A's {@code drop()}, which runs after the pass's fsync and before B's pending check. The invariant: a
 * WAL file is dropped only when its pending counter read zero BEFORE the fsync of the pass that drops it.
 */
class Issue8923WALDropAfterSingleFsyncTest extends TestHelper {

  /** A retired WAL whose pending-pages counter the test controls, standing in for the flush thread's decrement. */
  private static final class PendingWALFile extends WALFile {
    private final AtomicInteger pending;

    PendingWALFile(final String path, final int pending) throws FileNotFoundException {
      super(path);
      this.pending = new AtomicInteger(pending);
    }

    @Override
    public int getPendingPagesToFlush() {
      return pending.get();
    }

    @Override
    public void notifyPageFlushed() {
      pending.decrementAndGet();
    }
  }

  /** A retired WAL whose drop runs a callback first: the window between the pass's fsync and the next file's check. */
  private static final class DropHookWALFile extends WALFile {
    private final Runnable onDrop;

    DropHookWALFile(final String path, final Runnable onDrop) throws FileNotFoundException {
      super(path);
      this.onDrop = onDrop;
    }

    @Override
    public synchronized void drop() throws IOException {
      onDrop.run();
      super.drop();
    }
  }

  @Override
  protected void beginTest() {
    // The once-a-second housekeeping timer runs the same pass on the same pool: stop it so it cannot drop the files
    // seeded below between the test's own passes.
    try {
      final Field taskField = TransactionManager.class.getDeclaredField("task");
      taskField.setAccessible(true);
      final Timer task = (Timer) taskField.get(((DatabaseInternal) database).getTransactionManager());
      if (task != null)
        task.cancel();
    } catch (final ReflectiveOperationException e) {
      throw new IllegalStateException(e);
    }
  }

  @Test
  void aFileWhoseLastPageIsFlushedAfterThePassFsyncIsNotDroppedInThatPass() throws Exception {
    final TransactionManager tm = ((DatabaseInternal) database).getTransactionManager();
    final File dir = new File(database.getDatabasePath());

    final File fileB = new File(dir, "issue8923-b.wal");
    final PendingWALFile walB = new PendingWALFile(fileB.getAbsolutePath(), 1);

    final File fileA = new File(dir, "issue8923-a.wal");
    // A's drop runs after the pass's single fsync: B's last page reaches the OS cache right there.
    final DropHookWALFile walA = new DropHookWALFile(fileA.getAbsolutePath(), walB::notifyPageFlushed);

    tm.addInactiveWALFileForTesting(walA);
    tm.addInactiveWALFileForTesting(walB);

    final boolean emptied = tm.cleanWALFilesForTesting(true, false, true);

    assertThat(walB.getPendingPagesToFlush()).as("B's last page was acknowledged during A's drop").isZero();
    assertThat(fileA.exists()).as("A had no pending pages before the fsync, so it is dropped").isFalse();
    assertThat(fileB.exists())
        .as("B's last page reached the OS cache only AFTER the pass's fsync: dropping B now loses it on a power loss")
        .isTrue();
    assertThat(emptied).as("B must stay in the inactive pool for the next pass").isFalse();

    // The next pass fsyncs again before dropping, and that fsync covers B's page: now B can go.
    assertThat(tm.cleanWALFilesForTesting(true, false, true)).isTrue();
    assertThat(fileB.exists()).isFalse();
  }

  @Test
  void aFileWithPendingPagesIsKeptAndTheOthersAreDropped() throws Exception {
    final TransactionManager tm = ((DatabaseInternal) database).getTransactionManager();
    final File dir = new File(database.getDatabasePath());

    final File fileA = new File(dir, "issue8923-a.wal");
    final File fileB = new File(dir, "issue8923-b.wal");
    final File fileC = new File(dir, "issue8923-c.wal");
    final PendingWALFile walA = new PendingWALFile(fileA.getAbsolutePath(), 0);
    final PendingWALFile walB = new PendingWALFile(fileB.getAbsolutePath(), 2);
    final PendingWALFile walC = new PendingWALFile(fileC.getAbsolutePath(), 0);

    tm.addInactiveWALFileForTesting(walA);
    tm.addInactiveWALFileForTesting(walB);
    tm.addInactiveWALFileForTesting(walC);

    assertThat(tm.cleanWALFilesForTesting(true, false, true)).isFalse();
    assertThat(fileA.exists()).isFalse();
    assertThat(fileB.exists()).isTrue();
    assertThat(fileC.exists()).isFalse();

    walB.notifyPageFlushed();
    walB.notifyPageFlushed();
    assertThat(tm.cleanWALFilesForTesting(true, false, true)).isTrue();
    assertThat(fileB.exists()).isFalse();
  }

  @Test
  void forceDropsEveryFileRegardlessOfPendingPages() throws Exception {
    final TransactionManager tm = ((DatabaseInternal) database).getTransactionManager();
    final File dir = new File(database.getDatabasePath());

    final File fileA = new File(dir, "issue8923-a.wal");
    final File fileB = new File(dir, "issue8923-b.wal");
    tm.addInactiveWALFileForTesting(new PendingWALFile(fileA.getAbsolutePath(), 3));
    tm.addInactiveWALFileForTesting(new PendingWALFile(fileB.getAbsolutePath(), 0));

    assertThat(tm.cleanWALFilesForTesting(true, true, true)).isTrue();
    assertThat(fileA.exists()).isFalse();
    assertThat(fileB.exists()).isFalse();
  }

  @Test
  void onePassFsyncsOnceAndBeforeTheFirstDrop() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    final TransactionManager tm = db.getTransactionManager();
    final File dir = new File(database.getDatabasePath());

    final FileManager original = db.getFileManager();
    final FileManager spyFileManager = spy(original);
    swapFileManager(db, spyFileManager);
    try {
      final AtomicInteger syncsSeenAtFirstDrop = new AtomicInteger(-1);
      final Runnable recordSyncs = () -> syncsSeenAtFirstDrop.compareAndSet(-1, syncCalls(spyFileManager));

      final File[] files = new File[3];
      for (int i = 0; i < files.length; i++) {
        files[i] = new File(dir, "issue8923-" + i + ".wal");
        tm.addInactiveWALFileForTesting(new DropHookWALFile(files[i].getAbsolutePath(), recordSyncs));
      }

      assertThat(tm.cleanWALFilesForTesting(true, false, true)).isTrue();

      assertThat(syncsSeenAtFirstDrop.get()).as("the data files are fsync'd before the first WAL is dropped").isEqualTo(1);
      verify(spyFileManager, times(1)).syncFiles();
      for (final File f : files)
        assertThat(f.exists()).isFalse();
    } finally {
      swapFileManager(db, original);
    }
  }

  @Test
  void aFailedFsyncDropsNothing() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    final TransactionManager tm = db.getTransactionManager();
    final File dir = new File(database.getDatabasePath());

    final FileManager original = db.getFileManager();
    final FileManager spyFileManager = spy(original);
    doReturn(false).when(spyFileManager).syncFiles();
    swapFileManager(db, spyFileManager);
    try {
      final File fileA = new File(dir, "issue8923-a.wal");
      final File fileB = new File(dir, "issue8923-b.wal");
      tm.addInactiveWALFileForTesting(new PendingWALFile(fileA.getAbsolutePath(), 0));
      tm.addInactiveWALFileForTesting(new PendingWALFile(fileB.getAbsolutePath(), 0));

      assertThat(tm.cleanWALFilesForTesting(true, false, true)).as("a failed fsync aborts the pass (#4934)").isFalse();
      assertThat(fileA.exists()).isTrue();
      assertThat(fileB.exists()).isTrue();
    } finally {
      swapFileManager(db, original);
    }

    // With a working fsync the retained files go on the next pass.
    assertThat(tm.cleanWALFilesForTesting(true, false, true)).isTrue();
  }

  private static int syncCalls(final FileManager spyFileManager) {
    return (int) mockingDetails(spyFileManager).getInvocations().stream()
        .filter(i -> i.getMethod().getName().equals("syncFiles")).count();
  }

  private static void swapFileManager(final DatabaseInternal db, final FileManager fileManager) throws Exception {
    final Field f = db.getClass().getDeclaredField("fileManager");
    f.setAccessible(true);
    f.set(db, fileManager);
  }
}
