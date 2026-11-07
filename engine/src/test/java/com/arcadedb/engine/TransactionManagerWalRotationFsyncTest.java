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
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Tests for the data-file fsync that guards WAL rotation, WAL drop and clean close in the TransactionManager.
 * Each nested class keeps the regression tests of one originally separate issue-numbered class.
 */
class TransactionManagerWalRotationFsyncTest {

  /**
   * Regression test for issue #4509: runtime WAL rotation drops the previous WAL file once its
   * {@code pendingPagesToFlush} reaches 0, but {@code WALFile.notifyPageFlushed()} is decremented after a
   * {@code write()} that only reaches the OS page cache (no fsync). A power loss between the WAL drop and the OS
   * write-back would lose committed transactions.
   *
   * <p>The fix fsyncs the data files ({@link FileManager#syncFiles()}) before the rotated WAL file is physically
   * dropped. These tests verify that the rotation cleanup path performs the fsync exactly when a WAL file is about
   * to be dropped, and not when there is nothing to drop.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class WALRotationFsync extends TestHelper {

    @Override
    protected void beginTest() {
      // these tests are about fsync, which the unit-test lanes turn off (arcadedb.fileSync)
      GlobalConfiguration.FILE_SYNC.setValue(true);
      database.getSchema().createDocumentType("RotV");
    }

    @Test
    void rotationFsyncsDataFilesBeforeDroppingWAL() throws Exception {
      final DatabaseInternal db = (DatabaseInternal) database;

      // Stop the background WAL maintenance timer so the cleanup path is driven deterministically by the test.
      cancelTransactionManagerTimer(db);

      // Replace the FileManager with a spy that still performs the real fsync but records the invocation.
      final FileManager spyFileManager = spy(db.getFileManager());
      swapFileManager(db, spyFileManager);

      // Produce a transaction, then make sure its pages have been written by the flush thread so that the WAL
      // that recorded them reports pendingPagesToFlush == 0 (the condition that makes it eligible to be dropped).
      db.transaction(() -> db.newDocument("RotV").set("id", 1).set("name", "rotation").save());
      PageManager.INSTANCE.waitAllPagesOfDatabaseAreFlushed(db);

      // Simulate a rotation: move the active WAL files (which recorded the transaction) into the inactive pool.
      final TransactionManager txManager = db.getTransactionManager();
      final int moved = moveActiveWALFilesToInactive(txManager);
      assertThat(moved).as("at least one active WAL file must hold the committed transaction").isGreaterThan(0);

      // Drive the runtime rotation cleanup (dropFiles=true, syncDataOnDrop=true).
      invokeCleanWALFiles(txManager, true, false, true);

      // The data files must have been fsync'd before the WAL files were dropped...
      verify(spyFileManager, atLeastOnce()).syncFiles();
      // ...and the inactive WAL files must now be gone.
      assertThat(getInactiveWALFilePool(txManager)).as("rotated WAL files must be dropped").isEmpty();
    }

    @Test
    void cleanupWithoutDropDoesNotFsync() throws Exception {
      final DatabaseInternal db = (DatabaseInternal) database;

      cancelTransactionManagerTimer(db);

      final FileManager spyFileManager = spy(db.getFileManager());
      swapFileManager(db, spyFileManager);

      final TransactionManager txManager = db.getTransactionManager();
      // Nothing in the inactive pool: the per-second cleanup must not fsync the data files when there is nothing
      // to drop, otherwise it would pay an fsync on every timer tick.
      assertThat(getInactiveWALFilePool(txManager)).isEmpty();

      invokeCleanWALFiles(txManager, true, false, true);

      verify(spyFileManager, never()).syncFiles();
    }

    // --- reflection helpers ---

    private static void cancelTransactionManagerTimer(final DatabaseInternal db) throws Exception {
      db.getTransactionManager().cancelHousekeepingForTesting();
    }

    private static void swapFileManager(final DatabaseInternal db, final FileManager fileManager) throws Exception {
      final Field f = db.getClass().getDeclaredField("fileManager");
      f.setAccessible(true);
      f.set(db, fileManager);
      assertThat(db.getFileManager()).isSameAs(fileManager);
    }

    @SuppressWarnings("unchecked")
    private static List<WALFile> getInactiveWALFilePool(final TransactionManager txManager) throws Exception {
      final Field f = TransactionManager.class.getDeclaredField("inactiveWALFilePool");
      f.setAccessible(true);
      return (List<WALFile>) f.get(txManager);
    }

    private static int moveActiveWALFilesToInactive(final TransactionManager txManager) throws Exception {
      final Field activeField = TransactionManager.class.getDeclaredField("activeWALFilePool");
      activeField.setAccessible(true);
      final WALFile[] active = (WALFile[]) activeField.get(txManager);
      final List<WALFile> inactive = getInactiveWALFilePool(txManager);

      int moved = 0;
      for (int i = 0; i < active.length; i++) {
        final WALFile file = active[i];
        if (file != null) {
          active[i] = null;
          file.setActive(false);
          inactive.add(file);
          moved++;
        }
      }
      return moved;
    }

    private static void invokeCleanWALFiles(final TransactionManager txManager, final boolean dropFiles, final boolean force,
        final boolean syncDataOnDrop) throws Exception {
      final Method m = TransactionManager.class.getDeclaredMethod("cleanWALFiles", boolean.class, boolean.class, boolean.class);
      m.setAccessible(true);
      m.invoke(txManager, dropFiles, force, syncDataOnDrop);
    }
  }

  /**
   * Issue #8626: a clean close used to fsync every data file of the database, whether or not the session wrote to it -
   * about half a millisecond per bucket or index file on an NVMe drive, so a read-only open + close of a database with
   * 100 indexed types paid 100 ms of fsyncs that had nothing to persist. The close now forces only the files written
   * (or created, or renamed) since their last successful fsync, which is exactly the set the WAL it is about to delete
   * still protects (#4332, #4934).
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class CleanCloseFsync extends TestHelper {
    private static final int TYPES            = 10;
    private static final int RECORDS_PER_TYPE = 100;

    @Override
    protected void beginTest() {
      // these tests are about fsync, which the unit-test lanes turn off (arcadedb.fileSync)
      GlobalConfiguration.FILE_SYNC.setValue(true);
      for (int t = 0; t < TYPES; t++) {
        final DocumentType type = database.getSchema().createDocumentType("T" + t);
        type.createProperty("k", Type.LONG);
        database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "T" + t, "k");
      }
      database.transaction(() -> {
        for (int t = 0; t < TYPES; t++)
          for (int i = 0; i < RECORDS_PER_TYPE; i++)
            database.newDocument("T" + t).set("k", (long) i).save();
      });
    }

    @Test
    void cleanCloseOfAReadOnlySessionSyncsNoFile() {
      // The close of the write session above is what makes every file durable; from here on nothing is written.
      reopenDatabase();

      for (int t = 0; t < TYPES; t++) {
        assertThat(database.countType("T" + t, false)).isEqualTo(RECORDS_PER_TYPE);
        try (final ResultSet rs = database.query("sql", "select from T" + t + " where k = ?", 42L)) {
          assertThat(rs.stream().count()).isEqualTo(1);
        }
      }

      final FileManager fileManager = ((DatabaseInternal) database).getFileManager();
      assertThat(paginatedFiles(fileManager)).hasSizeGreaterThanOrEqualTo(2 * TYPES);
      assertThat(unsyncedFiles(fileManager)).as("reading a file owes the disk nothing").isEmpty();

      final long syncedBefore = fileManager.getStats().syncedFiles;
      database.close();

      assertThat(fileManager.getStats().syncedFiles - syncedBefore)
          .as("a clean close with nothing written since the open must not fsync any data file").isZero();
      assertThat(new File(getDatabasePath()).listFiles((d, n) -> n.endsWith(".wal"))).isEmpty();

      reopenDatabase();
      assertThat(database.countType("T0", false)).isEqualTo(RECORDS_PER_TYPE);
    }

    @Test
    void cleanCloseSyncsExactlyTheFilesTheSessionWrote() {
      reopenDatabase();

      final RID[] written = new RID[1];
      database.transaction(() -> {
        final MutableDocument doc = database.newDocument("T0").set("k", 1_000L);
        doc.save();
        written[0] = doc.getIdentity();
      });

      final DatabaseInternal db = (DatabaseInternal) database;
      final FileManager fileManager = db.getFileManager();
      PageManager.INSTANCE.waitAllPagesOfDatabaseAreFlushed(db);

      final List<PaginatedComponentFile> unsynced = unsyncedFiles(fileManager);
      final Set<Integer> unsyncedIds = new HashSet<>();
      for (final PaginatedComponentFile f : unsynced)
        unsyncedIds.add(f.getFileId());

      assertThat(unsyncedIds).as("the bucket the record landed in must owe an fsync").contains(written[0].getBucketId());
      assertThat(unsynced.size()).as("only the files the transaction touched").isLessThan(paginatedFiles(fileManager).size());

      final long syncedBefore = fileManager.getStats().syncedFiles;
      database.close();

      assertThat(fileManager.getStats().syncedFiles - syncedBefore).isEqualTo(unsynced.size());
      assertThat(new File(getDatabasePath()).listFiles((d, n) -> n.endsWith(".wal"))).isEmpty();

      reopenDatabase();
      assertThat(database.countType("T0", false)).isEqualTo(RECORDS_PER_TYPE + 1);
      try (final ResultSet rs = database.query("sql", "select from T0 where k = ?", 1_000L)) {
        assertThat(rs.stream().count()).isEqualTo(1);
      }
    }

    @Test
    void recoverySyncsEveryFileBeforeDroppingTheReplayedWal() {
      reopenDatabase();

      database.transaction(() -> {
        for (int i = 0; i < RECORDS_PER_TYPE; i++)
          database.newDocument("T1").set("k", (long) (RECORDS_PER_TYPE + i)).save();
      });

      // A crash: the WAL of the transaction above is all the next open has to go on.
      ((DatabaseInternal) database).kill();
      database.close();

      database = factory.open();

      // After an unclean shutdown nothing is known about what reached the disk, so recovery treats every file as
      // unsynced and forces all of them before it deletes the WAL it replayed: a power loss right after the open must
      // not find the replayed pages only in the OS page cache with their WAL already gone.
      final FileManager fileManager = ((DatabaseInternal) database).getFileManager();
      assertThat(unsyncedFiles(fileManager)).as("recovery must leave no file owing an fsync").isEmpty();
      assertThat(fileManager.getStats().syncedFiles).isGreaterThanOrEqualTo(paginatedFiles(fileManager).size());

      assertThat(database.countType("T1", false)).isEqualTo(2L * RECORDS_PER_TYPE);
    }

    private static List<PaginatedComponentFile> paginatedFiles(final FileManager fileManager) {
      final List<PaginatedComponentFile> result = new ArrayList<>();
      for (final ComponentFile f : fileManager.getFiles())
        if (f instanceof PaginatedComponentFile pcf && pcf.getFileId() > -1 && pcf.isOpen())
          result.add(pcf);
      return result;
    }

    private static List<PaginatedComponentFile> unsyncedFiles(final FileManager fileManager) {
      final List<PaginatedComponentFile> result = new ArrayList<>();
      for (final PaginatedComponentFile f : paginatedFiles(fileManager))
        if (f.isModifiedSinceLastSync())
          result.add(f);
      return result;
    }
  }

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
  @Nested
  class WALDropAfterSingleFsync extends TestHelper {

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
      // these tests are about fsync, which the unit-test lanes turn off (arcadedb.fileSync)
      GlobalConfiguration.FILE_SYNC.setValue(true);
      // The once-a-second housekeeping timer runs the same pass on the same pool: stop it so it cannot drop the files
      // seeded below between the test's own passes.
      try {
        ((DatabaseInternal) database).getTransactionManager().cancelHousekeepingForTesting();
      } catch (final RuntimeException e) {
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

  /**
   * Regression test for issue #8356: {@code TransactionManager.cleanWALFiles()} manually iterated
   * {@code inactiveWALFilePool} (a {@code Collections.synchronizedList}) with a raw iterator instead of
   * synchronizing on the list, as the list's own contract requires for manual iteration. In production this
   * pool is mutated from two callers that are never coordinated with each other: the once-a-second WAL
   * housekeeping timer ({@code checkWALFiles()} retiring a file into the pool, then this same method removing
   * it) and {@code close()} (reachable from either of two independent JVM shutdown hooks). A {@code close()}
   * landing mid-tick raced the timer's own {@code cleanWALFiles()} pass and threw
   * {@code ConcurrentModificationException} out of {@code TransactionManager.close()}, aborting WAL cleanup
   * and leaving the lock-file release right after it to fail too ({@code ClosedChannelException}).
   * <p>
   * Reproduced here at the {@code TransactionManager} level: many threads run {@code cleanWALFiles()}
   * concurrently against one shared, populated pool - the same shape as two callers landing on the same tick
   * - without needing the real once-a-second timer or a 64MB WAL file to trigger rotation.
   */
  @Nested
  class TransactionManagerCleanWALFilesRace extends TestHelper {

    private static final int FILE_COUNT   = 200;
    private static final int THREAD_COUNT = 8;

    @Test
    void concurrentCleanWALFilesDoesNotThrowConcurrentModificationException() throws Exception {
      final TransactionManager tm = ((DatabaseInternal) database).getTransactionManager();

      final File dir = new File(database.getDatabasePath());
      for (int i = 0; i < FILE_COUNT; i++)
        tm.addInactiveWALFileForTesting(new WALFile(new File(dir, "issue8356-" + i + ".wal").getAbsolutePath()));

      final ExecutorService pool = Executors.newFixedThreadPool(THREAD_COUNT);
      final CountDownLatch ready = new CountDownLatch(THREAD_COUNT);
      final CountDownLatch go = new CountDownLatch(1);
      final List<Throwable> failures = new CopyOnWriteArrayList<>();

      for (int i = 0; i < THREAD_COUNT; i++) {
        pool.submit(() -> {
          try {
            ready.countDown();
            go.await();
            tm.cleanWALFilesForTesting(true, true, false);
          } catch (final Throwable t) {
            failures.add(t);
          }
        });
      }

      assertThat(ready.await(10, TimeUnit.SECONDS)).as("every worker must reach the starting gate").isTrue();
      go.countDown();
      pool.shutdown();
      assertThat(pool.awaitTermination(10, TimeUnit.SECONDS)).as("all workers must finish").isTrue();

      assertThat(failures)
          .as("concurrent cleanWALFiles() passes over the same pool must not throw; got: %s", failures)
          .isEmpty();
    }
  }
}
