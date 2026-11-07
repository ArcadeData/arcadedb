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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

/**
 * Tests for the TransactionManager WAL file pool: sizing, on-demand creation, preserved files and stats.
 * Each nested class keeps the regression tests of one originally separate issue-numbered class.
 */
class TransactionManagerWalFilePoolTest {

  /**
   * Issue #7121: {@code arcadedb.txWalFiles} documents {@code 0 = available cores}, but nothing translated 0. Setting
   * the value the description advertises produced a zero-length WAL file pool, and the first commit then died on
   * {@code threadId() % 0}.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class WalFilePoolSize {
    private static final int CORES = Math.max(Runtime.getRuntime().availableProcessors(), 1);

    @Test
    void zeroResolvesToAvailableCores() {
      final ContextConfiguration configuration = new ContextConfiguration();
      configuration.setValue(GlobalConfiguration.TX_WAL_FILES, 0);
      assertThat(TransactionManager.walFilePoolSize(configuration))
          .as("`0 = available cores` is what the setting documents")
          .isEqualTo(CORES);
    }

    @Test
    void aNegativeValueResolvesTheSameWayInsteadOfThrowing() {
      final ContextConfiguration configuration = new ContextConfiguration();
      configuration.setValue(GlobalConfiguration.TX_WAL_FILES, -3);
      assertThat(TransactionManager.walFilePoolSize(configuration))
          .as("must not reach `new WALFile[-3]`")
          .isEqualTo(CORES);
    }

    @Test
    void anExplicitPositiveValueIsHonoured() {
      final ContextConfiguration configuration = new ContextConfiguration();
      configuration.setValue(GlobalConfiguration.TX_WAL_FILES, 3);
      assertThat(TransactionManager.walFilePoolSize(configuration)).isEqualTo(3);
    }
  }

  /**
   * Regression test for issue #10234: every open database kept a {@code java.util.Timer} thread of its own for the WAL
   * housekeeping, and one open WAL file per core from the moment it opened, so a server holding thousands of databases held
   * thousands of threads and tens of thousands of file descriptors before any request. The housekeeping now runs on one
   * scheduler shared by every database, a WAL file is opened by the first commit that needs it, and the housekeeping
   * releases a WAL file nobody wrote for {@link GlobalConfiguration#TX_WAL_IDLE_TIMEOUT}.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class WALFilesOnDemand extends TestHelper {

    @Test
    void aDatabaseThatIsOnlyReadHoldsNoWALFile() {
      database.getSchema().createDocumentType("Doc").createProperty("name", Type.STRING);
      database.transaction(() -> database.newDocument("Doc").set("name", "a").save());

      reopenDatabase();
      final TransactionManager transactionManager = ((DatabaseInternal) database).getTransactionManager();
      assertThat(transactionManager.getOpenWALFileCountForTesting()).isZero();
      assertThat(walFiles()).isEmpty();

      assertThat(database.query("sql", "SELECT FROM Doc").stream().count()).isEqualTo(1);
      assertThat(transactionManager.getOpenWALFileCountForTesting()).isZero();

      // THE FIRST COMMIT OPENS ONE, ON THE SLOT OF ITS THREAD ONLY
      database.transaction(() -> database.newDocument("Doc").set("name", "b").save());
      assertThat(transactionManager.getOpenWALFileCountForTesting()).isEqualTo(1);
      assertThat(walFiles()).hasSize(1);
    }

    @Test
    void anIdleWALFileIsReleasedAndAFreshOneOpenedByTheNextCommit() {
      database.getSchema().createDocumentType("Doc").createProperty("name", Type.STRING);
      final TransactionManager transactionManager = ((DatabaseInternal) database).getTransactionManager();
      // THE PASSES ARE DRIVEN BY HAND
      transactionManager.cancelHousekeepingForTesting();
      database.getConfiguration().setValue(GlobalConfiguration.TX_WAL_IDLE_TIMEOUT, 1L);

      database.transaction(() -> database.newDocument("Doc").set("name", "first").save());
      assertThat(transactionManager.getOpenWALFileCountForTesting()).isEqualTo(1);
      final List<File> before = walFiles();

      // RELEASED ONLY ONCE ITS PAGES REACHED THE DATA FILES, AND DROPPED BY THE SAME PASS AFTER THE FSYNC
      ((DatabaseInternal) database).getPageManager().waitAllPagesOfDatabaseAreFlushed(database);
      waitForIdleTimeout();
      transactionManager.runWALHousekeeping();
      assertThat(transactionManager.getOpenWALFileCountForTesting()).isZero();
      assertThat(walFiles()).isEmpty();
      assertThat(before.getFirst()).doesNotExist();

      database.transaction(() -> database.newDocument("Doc").set("name", "second").save());
      assertThat(transactionManager.getOpenWALFileCountForTesting()).isEqualTo(1);
      assertThat(walFiles()).hasSize(1).doesNotContainAnyElementsOf(before);

      reopenDatabase();
      assertThat(database.query("sql", "SELECT FROM Doc").stream().count()).isEqualTo(2);
    }

    @Test
    void commitsRacingTheReleaseOfIdleWALFilesLoseNothing() throws Exception {
      database.getSchema().createDocumentType("Doc").createProperty("n", Type.INTEGER);
      final TransactionManager transactionManager = ((DatabaseInternal) database).getTransactionManager();
      transactionManager.cancelHousekeepingForTesting();
      // EVERY FILE IS IDLE AS SOON AS ITS PAGES ARE FLUSHED: THE PASSES BELOW RELEASE FILES WHILE COMMITS PICK THEM
      database.getConfiguration().setValue(GlobalConfiguration.TX_WAL_IDLE_TIMEOUT, 1L);

      final int threads = 4;
      final int perThread = 300;
      final AtomicBoolean writing = new AtomicBoolean(true);
      final AtomicReference<Throwable> failure = new AtomicReference<>();

      final Thread housekeeping = new Thread(() -> {
        while (writing.get())
          try {
            transactionManager.runWALHousekeeping();
          } catch (final Throwable e) {
            failure.compareAndSet(null, e);
          }
      }, "issue10234-housekeeping");

      final List<Thread> writers = new ArrayList<>();
      for (int t = 0; t < threads; t++) {
        final int base = t * perThread;
        writers.add(new Thread(() -> {
          try {
            for (int i = 0; i < perThread; i++) {
              final int n = base + i;
              database.transaction(() -> database.newDocument("Doc").set("n", n).save());
            }
          } catch (final Throwable e) {
            failure.compareAndSet(null, e);
          }
        }, "issue10234-writer-" + t));
      }

      housekeeping.start();
      writers.forEach(Thread::start);
      for (final Thread writer : writers)
        writer.join();
      writing.set(false);
      housekeeping.join();

      assertThat(failure.get()).isNull();
      assertThat(database.countType("Doc", false)).isEqualTo(threads * perThread);

      // AND WHAT THE RACING COMMITS WROTE IS ON DISK AFTER A REOPEN
      reopenDatabase();
      assertThat(database.query("sql", "SELECT count(*) AS c FROM Doc").next().<Long>getProperty("c")).isEqualTo(
          (long) threads * perThread);
    }

    /**
     * The #7479 check moved from the open to the first commit that opens a WAL file: a name another live instance already
     * holds must fail that commit loudly and fence the database, never retry it into another file.
     */
    @Test
    void aWALFileOpenedOnDemandThatAnotherInstanceHoldsFencesTheDatabase() throws Exception {
      database.getSchema().createDocumentType("Doc").createProperty("name", Type.STRING);
      reopenDatabase();
      final LocalDatabase db = (LocalDatabase) database;
      final TransactionManager transactionManager = db.getTransactionManager();
      transactionManager.cancelHousekeepingForTesting();
      assertThat(transactionManager.getOpenWALFileCountForTesting()).isZero();

      final String collidingPath = db.getDatabasePath() + File.separator + "txlog_" + transactionManager.getLogFileCounterForTesting()
          + ".wal";
      // STANDS IN FOR ANOTHER LIVE INSTANCE THAT CLAIMED THE NAME THE COMMIT IS ABOUT TO OPEN: A WALFile LOCKS ITS FILE FOR LIFE
      final WALFile anotherInstanceWAL = new WALFile(collidingPath);
      try {
        assertThatThrownBy(() -> database.transaction(() -> database.newDocument("Doc").set("name", "x").save()))
            .hasMessageContaining("already open by another process or database instance");
        assertThat(db.isFencedForRecovery()).isTrue();
      } finally {
        anotherInstanceWAL.close();
        new File(collidingPath).delete();
      }

      database.close();
    }

    /**
     * The fallback of a commit whose own slot is unusable opens a file for an empty slot, and the #7479 check applies there
     * as it does for the slot of the commit itself: a name another live instance holds fences the database and fails the
     * commit loudly, instead of the commit going on to write to a file it shares.
     */
    @Test
    void aFallbackSlotWhoseWALFileAnotherInstanceHoldsFencesTheDatabase() throws Exception {
      database.close();

      final Object previousWalFiles = GlobalConfiguration.TX_WAL_FILES.getValue();
      GlobalConfiguration.TX_WAL_FILES.setValue(2);
      try {
        database = factory.open();
        database.getSchema().getOrCreateDocumentType("Doc");
        final LocalDatabase db = (LocalDatabase) database;
        final TransactionManager transactionManager = db.getTransactionManager();
        transactionManager.cancelHousekeepingForTesting();

        // THE SLOT OF THIS THREAD HOLDS A FILE THAT REFUSES WRITES, THE OTHER SLOT IS EMPTY
        final int ownSlot = (int) (Thread.currentThread().threadId() % 2);
        final WALFile retired = new WALFile(db.getDatabasePath() + File.separator + "txlog_10234_retired.wal");
        retired.setActive(false);
        final WALFile displaced = transactionManager.replaceActiveWALFileForTesting(ownSlot, retired);

        final String collidingPath =
            db.getDatabasePath() + File.separator + "txlog_" + transactionManager.getLogFileCounterForTesting() + ".wal";
        final WALFile anotherInstanceWAL = new WALFile(collidingPath);
        try {
          assertThatThrownBy(() -> database.transaction(() -> database.newDocument("Doc").set("k", "x").save()))
              .hasMessageContaining("already open by another process or database instance");
          assertThat(db.isFencedForRecovery()).isTrue();
        } finally {
          anotherInstanceWAL.close();
          new File(collidingPath).delete();
          retired.drop();
          if (displaced != null)
            transactionManager.replaceActiveWALFileForTesting(ownSlot, displaced);
        }

        database.close();
      } finally {
        GlobalConfiguration.TX_WAL_FILES.setValue(previousWalFiles);
      }
    }

    /**
     * A crash between the release of an idle WAL file (its slot emptied, the file retired) and its drop after the fsync of the
     * data files leaves the file on disk, and the next open recovers from it as from any other WAL file.
     */
    @Test
    void aCrashBetweenTheReleaseAndTheDropOfAnIdleWALFileLosesNothing() throws Exception {
      database.getSchema().createDocumentType("Doc").createProperty("name", Type.STRING);
      final TransactionManager transactionManager = ((DatabaseInternal) database).getTransactionManager();
      transactionManager.cancelHousekeepingForTesting();
      database.getConfiguration().setValue(GlobalConfiguration.TX_WAL_IDLE_TIMEOUT, 1L);

      database.transaction(() -> {
        for (int i = 0; i < 50; i++)
          database.newDocument("Doc").set("name", "n" + i).save();
      });
      ((DatabaseInternal) database).getPageManager().waitAllPagesOfDatabaseAreFlushed(database);
      waitForIdleTimeout();

      transactionManager.releaseIdleWALFilesForTesting();
      assertThat(transactionManager.getOpenWALFileCountForTesting()).isZero();
      // RETIRED, NOT DROPPED: THE DROP WAITS FOR THE FSYNC OF THE DATA FILES THAT THE NEXT PASS DOES
      assertThat(walFiles()).hasSize(1);

      // A KILL, NOT A CLOSE: THE WAL FILE AND THE LOCK FILE STAY, SO THE NEXT OPEN RUNS THE RECOVERY
      ((LocalDatabase) database).kill();
      database.close();
      database = factory.open();

      assertThat(database.query("sql", "SELECT count(*) AS c FROM Doc").next().<Long>getProperty("c")).isEqualTo(50L);
      // THE RECOVERY LEFT A DATABASE THAT KEEPS WORKING
      database.transaction(() -> database.newDocument("Doc").set("name", "after").save());
      assertThat(database.countType("Doc", false)).isEqualTo(51);
    }

    /**
     * A WAL file that cannot be created (permissions, a full disk) fails the commit at once, before any of its pages is
     * written, where an unusable slot is retried for the 30 seconds of the WAL write timeout; the commit works again once the
     * directory does.
     */
    @Test
    void aWALFileThatCannotBeCreatedFailsTheCommitAtOnce() throws Exception {
      database.getSchema().createDocumentType("Doc").createProperty("name", Type.STRING);
      reopenDatabase();
      final File directory = new File(getDatabasePath());
      assertThat(((DatabaseInternal) database).getTransactionManager().getOpenWALFileCountForTesting()).isZero();

      assertThat(directory.setWritable(false)).isTrue();
      try {
        // A USER WHO CAN WRITE ANYWHERE (ROOT) CANNOT MAKE THE CREATION FAIL THIS WAY
        assumeFalse(Files.isWritable(directory.toPath()));

        final StallAwareStopwatch watch = StallAwareStopwatch.start();
        assertThatThrownBy(() -> database.transaction(() -> database.newDocument("Doc").set("name", "x").save()))
            .hasMessageContaining("Cannot create the WAL file");
        watch.assertGaveUpWithin(20_000,
            "a commit that fails at once versus the 30s write timeout spent retrying a file that cannot be created");
      } finally {
        assertThat(directory.setWritable(true)).isTrue();
      }

      assertThat(database.countType("Doc", false)).isZero();
      database.transaction(() -> database.newDocument("Doc").set("name", "y").save());
      assertThat(database.countType("Doc", false)).isEqualTo(1);
    }

    /** A refusal of the slot of a commit takes the write to a file already open before it opens another one. */
    @Test
    void theFallbackPrefersAFileThatIsAlreadyOpen() throws Exception {
      database.close();

      final Object previousWalFiles = GlobalConfiguration.TX_WAL_FILES.getValue();
      GlobalConfiguration.TX_WAL_FILES.setValue(3);
      try {
        database = factory.open();
        database.getSchema().getOrCreateDocumentType("Doc");
        final LocalDatabase db = (LocalDatabase) database;
        final TransactionManager transactionManager = db.getTransactionManager();
        transactionManager.cancelHousekeepingForTesting();

        // ONE OTHER SLOT HOLDS A FILE, THE THIRD IS EMPTY, AND THE SLOT OF THIS THREAD REFUSES WRITES
        final int ownSlot = (int) (Thread.currentThread().threadId() % 3);
        final int openSlot = (ownSlot + 1) % 3;
        transactionManager.openWALFileForTesting(openSlot);
        final WALFile retired = new WALFile(db.getDatabasePath() + File.separator + "txlog_10234_refuses.wal");
        retired.setActive(false);
        final WALFile displaced = transactionManager.replaceActiveWALFileForTesting(ownSlot, retired);
        final int openBefore = transactionManager.getOpenWALFileCountForTesting();

        try {
          database.transaction(() -> database.newDocument("Doc").set("k", "x").save());
          // THE FILE ALREADY OPEN TOOK IT: NO THIRD FILE WAS OPENED
          assertThat(transactionManager.getOpenWALFileCountForTesting()).isEqualTo(openBefore);
        } finally {
          retired.drop();
          transactionManager.replaceActiveWALFileForTesting(ownSlot, displaced);
        }
      } finally {
        GlobalConfiguration.TX_WAL_FILES.setValue(previousWalFiles);
      }
    }

    @Test
    void anActiveWALFileIsKept() {
      database.getSchema().createDocumentType("Doc").createProperty("name", Type.STRING);
      final TransactionManager transactionManager = ((DatabaseInternal) database).getTransactionManager();
      transactionManager.cancelHousekeepingForTesting();

      // THE DEFAULT TIMEOUT, A MINUTE: A FILE WRITTEN A MOMENT AGO STAYS
      database.transaction(() -> database.newDocument("Doc").set("name", "a").save());
      ((DatabaseInternal) database).getPageManager().waitAllPagesOfDatabaseAreFlushed(database);
      transactionManager.runWALHousekeeping();
      assertThat(transactionManager.getOpenWALFileCountForTesting()).isEqualTo(1);

      // AND A TIMEOUT OF 0 NEVER RELEASES ONE
      database.getConfiguration().setValue(GlobalConfiguration.TX_WAL_IDLE_TIMEOUT, 0L);
      waitForIdleTimeout();
      transactionManager.runWALHousekeeping();
      assertThat(transactionManager.getOpenWALFileCountForTesting()).isEqualTo(1);
    }

    @Test
    void databasesShareTheHousekeepingThreads() {
      final List<Database> databases = new ArrayList<>();
      final List<String> paths = new ArrayList<>();
      try {
        for (int i = 0; i < 6; i++) {
          final String path = getDatabasePath() + "_shared_" + i;
          FileUtils.deleteRecursively(new File(path));
          paths.add(path);
          databases.add(new DatabaseFactory(path).create());
        }

        for (final Database db : databases) {
          assertThat(((DatabaseInternal) db).getTransactionManager().isHousekeepingScheduled()).isTrue();
          // NO TIMER THREAD OF ITS OWN ANY MORE
          assertThat(threadsNamed("ArcadeDB TransactionManager " + db.getName())).isZero();
        }
        assertThat(threadsNamed("ArcadeDB WAL Housekeeping-")).isLessThanOrEqualTo(2);
      } finally {
        for (final Database db : databases) {
          final TransactionManager transactionManager = ((DatabaseInternal) db).getTransactionManager();
          db.drop();
          assertThat(transactionManager.isHousekeepingScheduled()).isFalse();
        }
        for (final String path : paths)
          FileUtils.deleteRecursively(new File(path));
      }
    }

    private static long threadsNamed(final String prefix) {
      return Thread.getAllStackTraces().keySet().stream().filter(t -> t.getName().startsWith(prefix)).count();
    }

    private static void waitForIdleTimeout() {
      // A FEW MILLISECONDS PAST A 1 MS TIMEOUT: A LOWER BOUND THE STALL OF A LOADED JVM ONLY MAKES MORE TRUE
      try {
        Thread.sleep(20);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    private List<File> walFiles() {
      final File[] files = new File(getDatabasePath()).listFiles((dir, name) -> name.endsWith(".wal"));
      return files == null ? List.of() : List.of(files);
    }
  }

  /**
   * Regression tests for the preserved-WAL findings grouped in issue #4958:
   * <ul>
   *   <li>{@code TransactionManager.createWALFilePool} started {@code logFileCounter} from 0 on every open
   *   and {@code WALFile} opens its path in "rw" mode WITHOUT truncation. A WAL file preserved by a previous
   *   open (corrupt-gap detection or a #4928 crash-equivalent close) was therefore silently reused as an
   *   active WAL: new transactions were appended AFTER the old (possibly corrupt) content, and the old
   *   records replayed again on the next recovery.</li>
   *   <li>When recovery aborted on a corrupt-gap, the WAL files were preserved under their original
   *   {@code .wal} names, so beside the reuse above the next recovery re-scanned and re-flagged them
   *   forever. They are now renamed to {@code .wal.corrupt} for manual inspection.</li>
   * </ul>
   */
  @Nested
  class PreservedWAL extends TestHelper {

    // Layout constants mirroring the private statics in WALFile.
    private static final int TX_HEADER_SIZE   = 24; // txId(8) + timestamp(8) + pages(4) + segmentSize(4)
    private static final int PAGE_HEADER_SIZE = 24; // fileId(4) + pageNumber(4) + from(4) + to(4) + version(4) + size(4)
    private static final int TX_FOOTER_SIZE   = 12; // segmentSize(4) + MAGIC_NUMBER(8)

    @Override
    protected void beginTest() {
      database.getSchema().getOrCreateDocumentType("PreservedWALType");
    }

    @Test
    void preservedWalFileIsNeverReusedAsActiveWal() throws Exception {
      final String dbPath = database.getDatabasePath();
      database.close();

      // Simulate a WAL file preserved by a previous open (e.g. after corrupt-gap detection): a stray
      // txlog_0.wal that the new WAL pool must NOT adopt and append to.
      final byte[] preservedContent = new byte[64];
      Arrays.fill(preservedContent, (byte) 0x7F);
      final File preserved = new File(dbPath, "txlog_0.wal");
      Files.write(preserved.toPath(), preservedContent);

      // A single-file pool makes the test deterministic: every commit maps to pool slot 0, the exact
      // slot that (before the fix) reopened the preserved txlog_0.wal and appended after its content.
      final Object previousWalFiles = GlobalConfiguration.TX_WAL_FILES.getValue();
      GlobalConfiguration.TX_WAL_FILES.setValue(1);
      try {
        database = factory.open();

        database.transaction(() -> database.newDocument("PreservedWALType").set("k", 1).save());

        assertThat(preserved.length())
            .as("the preserved WAL file must not be appended to by the new active WAL pool")
            .isEqualTo((long) preservedContent.length);
        assertThat(Files.readAllBytes(preserved.toPath())).isEqualTo(preservedContent);
      } finally {
        GlobalConfiguration.TX_WAL_FILES.setValue(previousWalFiles);
      }
    }

    @Test
    void corruptGapWalFilesAreRenamedForManualInspection() throws Exception {
      final String dbPath = database.getDatabasePath();

      // Craft a WAL with a corrupt first record FOLLOWED by a valid transaction record: exactly the
      // pattern the #4508 gap detector aborts recovery on and preserves the files for.
      final byte[] garbage = new byte[32];
      Arrays.fill(garbage, (byte) 0x7F);

      final byte[] delta = { 1, 2, 3, 4, 5 };
      final int segmentSize = PAGE_HEADER_SIZE + delta.length;
      final ByteBuffer buf = ByteBuffer.allocate(garbage.length + TX_HEADER_SIZE + segmentSize + TX_FOOTER_SIZE);
      buf.put(garbage);
      buf.putLong(42L);              // txId
      buf.putLong(123456L);          // timestamp
      buf.putInt(1);                 // page count
      buf.putInt(segmentSize);
      buf.putInt(3);                 // fileId
      buf.putInt(0);                 // pageNumber
      buf.putInt(0);                 // changesFrom
      buf.putInt(delta.length - 1);  // changesTo
      buf.putInt(1);                 // currentPageVersion
      buf.putInt(1024);              // currentPageSize
      buf.put(delta);
      buf.putInt(segmentSize);
      buf.putLong(WALFile.MAGIC_NUMBER);

      final File corrupt = new File(dbPath, "txlog_777.wal");
      Files.write(corrupt.toPath(), buf.array());

      ((DatabaseInternal) database).getTransactionManager().checkIntegrity();

      assertThat(corrupt)
          .as("a WAL flagged by the corrupt-gap detector must not survive under its active .wal name")
          .doesNotExist();
      assertThat(new File(dbPath, "txlog_777.wal.corrupt"))
          .as("the corrupt WAL must be preserved for manual inspection under the .corrupt suffix")
          .exists();
    }
  }

  /**
   * Regression tests for the low-severity storage/WAL findings grouped in issue #4958 that are observable
   * through public engine behavior:
   * <ul>
   *   <li>{@code TransactionManager.getStats()} used to fold every active WAL file's CUMULATIVE counters
   *   into the long-lived accumulators on each call, so the reported numbers inflated with polling
   *   frequency (each poll re-added everything the active files ever wrote).</li>
   *   <li>{@code TransactionManager.getStats()} used to NPE on a read-only database, where no active WAL
   *   file pool is created.</li>
   *   <li>{@code PageManager.getCachedPage} returned before bumping the cache-miss counter, so
   *   {@code cacheMiss} stayed at ~0 forever and the hit/miss ratio was meaningless.</li>
   *   <li>{@code PageManager.suspendFlushAndExecute} silently SKIPPED the callback when the database was
   *   already suspended by an outer scope (nested backup/snapshot).</li>
   * </ul>
   */
  @Nested
  class StorageStats extends TestHelper {

    @Override
    protected void beginTest() {
      database.getSchema().createDocumentType("StatsType");
    }

    @Test
    void walStatsAreNotInflatedByPolling() {
      database.transaction(() -> database.newDocument("StatsType").set("k", 1).save());

      final TransactionManager txManager = ((DatabaseInternal) database).getTransactionManager();

      final Map<String, Object> first = txManager.getStats();
      final Map<String, Object> second = txManager.getStats();
      final Map<String, Object> third = txManager.getStats();

      // No writes happened between the calls: polling must not change the totals.
      assertThat(second.get("pagesWritten")).as("pagesWritten must not inflate with polling")
          .isEqualTo(first.get("pagesWritten"));
      assertThat(third.get("pagesWritten")).isEqualTo(first.get("pagesWritten"));
      assertThat(second.get("bytesWritten")).as("bytesWritten must not inflate with polling")
          .isEqualTo(first.get("bytesWritten"));
      assertThat(third.get("bytesWritten")).isEqualTo(first.get("bytesWritten"));

      // The counters must still reflect the write that DID happen.
      assertThat((Long) first.get("pagesWritten")).isGreaterThan(0L);
      assertThat((Long) first.get("bytesWritten")).isGreaterThan(0L);
    }

    @Test
    void walStatsOnReadOnlyDatabaseDoesNotThrow() {
      database.transaction(() -> database.newDocument("StatsType").set("k", 2).save());

      reopenDatabaseInReadOnlyMode();
      try {
        final TransactionManager txManager = ((DatabaseInternal) database).getTransactionManager();
        // Read-only databases have no active WAL file pool: getStats() must not NPE.
        final Map<String, Object> stats = txManager.getStats();
        assertThat(stats).containsKeys("logFiles", "pagesWritten", "bytesWritten");
      } finally {
        reopenDatabase();
      }
    }

    @Test
    void pageCacheMissIsCounted() {
      database.transaction(() -> {
        for (int i = 0; i < 100; i++)
          database.newDocument("StatsType").set("k", i).save();
      });

      // Reopening drops this database's pages from the shared read cache, so the scan below must
      // load at least one page from disk = at least one cache miss.
      final long missBefore = ((DatabaseInternal) database).getPageManager().getStats().cacheMiss;

      reopenDatabase();
      database.begin();
      database.iterateType("StatsType", false).forEachRemaining(r -> r.asDocument().get("k"));
      database.commit();

      final long missAfter = ((DatabaseInternal) database).getPageManager().getStats().cacheMiss;
      assertThat(missAfter)
          .as("loading uncached pages from disk must increment the cacheMiss counter")
          .isGreaterThan(missBefore);
    }

    @Test
    void nestedSuspendFlushStillExecutesTheCallback() throws Exception {
      final PageManager pageManager = ((DatabaseInternal) database).getPageManager();
      final AtomicBoolean innerCallbackRan = new AtomicBoolean(false);

      pageManager.suspendFlushAndExecute(database, () ->
          // Nested suspension (e.g. a snapshot triggered during a backup): the inner callback must
          // still run under the outer scope's suspension instead of being silently skipped.
          pageManager.suspendFlushAndExecute(database, () -> innerCallbackRan.set(true)));

      assertThat(innerCallbackRan.get())
          .as("a nested suspendFlushAndExecute must execute its callback, not silently skip it")
          .isTrue();
      assertThat(pageManager.isPageFlushingSuspended(database))
          .as("the outer scope must have restored the flush on exit")
          .isFalse();
    }
  }
}
