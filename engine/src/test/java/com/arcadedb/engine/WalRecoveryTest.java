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
import com.arcadedb.database.Binary;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.exception.WALVersionGapException;
import com.arcadedb.log.LogManager;
import com.arcadedb.log.Logger;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for WAL recovery on open: transaction id counter, torn multi-version pages, full page image bridging and outcome reporting.
 * Each nested class keeps the regression tests of one originally separate issue-numbered class.
 */
class WalRecoveryTest {

  /**
   * Issue #9795: after a WAL replay the transaction counter must be past EVERY transaction id that was ever handed out,
   * because each of them may be durable. Recovery used to set it from the transaction it replayed LAST, which is the
   * highest only when every WAL file holds its transactions in id order - and none does: an id is allocated at the end
   * of {@code commit1stPhase} and the record is appended in phase 2, so two commits that share no file can append in
   * the opposite order to their ids, into one WAL file whenever their thread ids hash to the same slot (always, with
   * {@code arcadedb.txWalFiles=1}). It also threw away the marker persisted by a runtime WAL rotation whenever that
   * marker was higher than what the surviving WAL files held.
   * <p>
   * Both tests pin the claim, not the shape: they build the WAL out of order through the public commit phases, crash
   * without a clean close and assert the recovered counter equals the highest id handed out before the crash.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class RecoveryTransactionIdCounter extends TestHelper {
    private static final String TYPE_A = "Issue9795A";
    private static final String TYPE_B = "Issue9795B";

    private Object previousWalFiles;

    @BeforeEach
    void singleWalFile() {
      // ONE WAL FILE: THE TWO RECORDS ARE CERTAIN TO SHARE IT. WITH THE DEFAULT POOL THE SAME HAPPENS ON A HASH COLLISION
      previousWalFiles = GlobalConfiguration.TX_WAL_FILES.getValue();
      GlobalConfiguration.TX_WAL_FILES.setValue(1);
      database.close();
      database = factory.open();

      database.transaction(() -> {
        database.getSchema().getOrCreateDocumentType(TYPE_A);
        database.getSchema().getOrCreateDocumentType(TYPE_B);
      });
      // THE PROPERTY NAME IS IN THE DICTIONARY BEFORE THE PAIR, SO THE TWO TRANSACTIONS BELOW SHARE NO FILE AND NO LOCK
      database.transaction(() -> {
        database.newDocument(TYPE_A).set("k", 0).save();
        database.newDocument(TYPE_B).set("k", 0).save();
      });
    }

    @AfterEach
    void restoreWalFiles() {
      GlobalConfiguration.TX_WAL_FILES.setValue(previousWalFiles);
    }

    @Test
    void recoveryOfAnOutOfOrderWalSetsTheCounterPastTheHighestReplayedId() throws Exception {
      final long before = lastTransactionId();

      // A ALLOCATES before+1, B ALLOCATES AND APPENDS before+2, THEN A APPENDS: THE FILE HOLDS [before+2, before+1]
      commitInterleaved(() -> database.transaction(() -> database.newDocument(TYPE_B).set("k", 1).save()));

      final long highest = before + 2;
      assertThat(lastTransactionId()).isEqualTo(highest);

      crashAndReopen();

      assertThat(database.countType(TYPE_A, false)).as("A's transaction was replayed").isEqualTo(2);
      assertThat(database.countType(TYPE_B, false)).as("B's transaction was replayed").isEqualTo(2);
      assertThat(lastTransactionId())
          .as("the counter must be past every replayed transaction, not past the one replayed last")
          .isEqualTo(highest);

      database.transaction(() -> database.newDocument(TYPE_A).set("k", 2).save());
      assertThat(lastTransactionId()).as("the next commit must get a fresh id, not one already durable")
          .isEqualTo(highest + 1);
    }

    @Test
    void recoveryNeverMovesTheCounterBelowTheMarkerARotationPersisted() throws Exception {
      final long before = lastTransactionId();

      // B COMMITS before+2 WHILE A HOLDS before+1, THEN A RUNTIME ROTATION DROPS B'S WAL FILE AND PERSISTS THE MARKER AT
      // before+2: A'S RECORD LANDS ALONE IN THE NEW FILE, SO THE REPLAY SEES ONLY before+1
      commitInterleaved(() -> {
        database.transaction(() -> database.newDocument(TYPE_B).set("k", 1).save());
        // THE ROTATION DROPS ONLY A WAL WHOSE PAGES ARE ON DISK: WAIT FOR B'S, WRITTEN BY THE ASYNCHRONOUS FLUSH THREAD. A'S
        // PAGES ARE NOT PUBLISHED BEFORE ITS PHASE 2, SO THIS DOES NOT WAIT FOR THEM
        final DatabaseInternal db = (DatabaseInternal) database;
        assertThat(db.getPageManager().waitAllPagesOfDatabaseAreFlushed(db)).isTrue();
        try {
          assertThat(db.getTransactionManager().rotateAndDropWALForTesting()).as("the rotation must drop B's WAL file").isTrue();
        } catch (final IOException e) {
          throw new UncheckedIOException(e);
        }
      });

      final long highest = before + 2;
      assertThat(lastTransactionId()).isEqualTo(highest);

      crashAndReopen();

      assertThat(database.countType(TYPE_A, false)).isEqualTo(2);
      assertThat(database.countType(TYPE_B, false)).isEqualTo(2);
      assertThat(lastTransactionId())
          .as("the replay must not overwrite a higher persisted marker: B's id is durable even though its WAL is gone")
          .isEqualTo(highest);

      database.transaction(() -> database.newDocument(TYPE_A).set("k", 2).save());
      assertThat(lastTransactionId()).isEqualTo(highest + 1);
    }

    /**
     * Runs phase 1 of an insert into {@link #TYPE_A} on another thread (allocating its id), then {@code betweenPhases}
     * on this thread, then A's phase 2 (appending its WAL record).
     */
    private void commitInterleaved(final Runnable betweenPhases) throws Exception {
      final CountDownLatch phase1Done = new CountDownLatch(1);
      final CountDownLatch resumeA = new CountDownLatch(1);

      final ExecutorService executor = Executors.newSingleThreadExecutor();
      final Future<?> a = executor.submit(() -> {
        database.begin();
        database.newDocument(TYPE_A).set("k", 1).save();
        final TransactionContext tx = ((DatabaseInternal) database).getTransaction();
        final TransactionContext.TransactionPhase1 phase1 = tx.commit1stPhase(true);
        phase1Done.countDown();
        try {
          assertThat(resumeA.await(30, TimeUnit.SECONDS)).isTrue();
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(e);
        }
        tx.commit2ndPhase(phase1);
      });

      try {
        assertThat(phase1Done.await(30, TimeUnit.SECONDS)).isTrue();
        try {
          betweenPhases.run();
        } finally {
          resumeA.countDown();
        }
        a.get(30, TimeUnit.SECONDS);
      } finally {
        // ALSO ON A TIMEOUT OR A FAILED CALLBACK: NEVER LEAVE A'S THREAD PARKED OR ALIVE PAST THE TEST
        resumeA.countDown();
        executor.shutdownNow();
      }
    }

    private long lastTransactionId() {
      return ((DatabaseInternal) database).getTransactionManager().getLastTransactionId();
    }

    private void crashAndReopen() {
      ((DatabaseInternal) database).kill();
      final File[] walFiles = new File(getDatabasePath()).listFiles((dir, name) -> name.endsWith(".wal"));
      assertThat(walFiles).as("the crash must leave the WAL for the next open to replay").isNotEmpty();
      // UNREGISTERS THE KILLED INSTANCE ONLY: THE LOCK FILE AND THE WAL STAY, SO THE OPEN BELOW RUNS RECOVERY
      database.close();
      database = factory.open();
    }
  }

  /**
   * #11151: after a simulated power loss (Jepsen LazyFS) a recovered node served records whose property names resolved to
   * nothing or to another name. The schema dictionary page had several versions pending in the WAL (one per new name, each its
   * own transaction), and the power loss persisted the page's first sector - the one holding the version - while the sectors
   * holding the names appended by the earlier versions stayed stale. Recovery skipped every WAL entry older than the version on
   * disk, so only the last name's range was repaired and the dictionary kept holes where ids that records already carry should
   * have resolved.
   */
  @Nested
  class TornMultiVersionPageRecovery {

    private static final String DB_PATH = "target/databases/WalRecoveryTornMultiVersionPage";
    private static final String[] NAMES = { "acctId", "balance", "val", "key" };

    @BeforeEach
    void fileSyncOn() {
      GlobalConfiguration.FILE_SYNC.setValue(true);
    }

    @AfterEach
    void cleanup() {
      GlobalConfiguration.FILE_SYNC.reset();
      final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
      try {
        if (factory.exists())
          factory.open().drop();
      } catch (final Exception e) {
        // A test may deliberately leave the database unreopenable: remove it on disk below
      }
      factory.close();
      FileUtils.deleteRecursively(new File(DB_PATH));
    }

    private record Crashed(int[] ids, String dictionaryFilePath, long finalVersion, int finalContentSize, int firstNameOffset) {
    }

    /**
     * Adds {@link #NAMES} to the dictionary one commit each, keeps their pages off the disk and kills the database, so that the
     * WAL holds one version of dictionary page 0 per name and the data file still holds the old page.
     */
    private Crashed crashWithPendingNames(final DatabaseFactory factory) throws Exception {
      if (factory.exists())
        factory.open().drop();

      final int[] ids = new int[NAMES.length];
      final DatabaseInternal db = (DatabaseInternal) factory.create();
      db.getSchema().createDocumentType("Doc");
      db.getPageManager().waitAllPagesOfDatabaseAreFlushed(db);

      final Dictionary dictionary = db.getSchema().getDictionary();
      final PaginatedComponentFile file = (PaginatedComponentFile) db.getFileManager().getFile(dictionary.getFileId());

      // KEEP THE NEW NAMES OFF THE DISK: EACH ONE IS ITS OWN COMMIT, HENCE ITS OWN VERSION OF DICTIONARY PAGE 0 IN THE WAL
      assertThat(PageManager.INSTANCE.getFlushThread().setSuspended(db, true))
          .as("flush suspension must engage or the pages could reach disk, invalidating the test").isTrue();

      for (int i = 0; i < NAMES.length; ++i)
        ids[i] = dictionary.getIdByName(NAMES[i], true);

      final ImmutablePage committed = db.getPageManager()
          .getImmutablePage(new PageId(db, dictionary.getFileId(), 0), file.getPageSize(), false, false);
      // WHERE THE ENTRY OF THE FIRST NEW NAME STARTS IN THE FILE: AFTER THE PAGE HEADER, AT THE POSITION THE LOAD READS IT FROM
      int firstNameOffset = -1;
      committed.setBufferPosition(4);
      while (committed.getBufferPosition() < committed.getContentSize()) {
        final int position = committed.getBufferPosition();
        if (committed.readString().equals(NAMES[0]) && firstNameOffset < 0)
          firstNameOffset = BasePage.PAGE_HEADER_SIZE + position;
      }
      assertThat(firstNameOffset).isPositive();

      final Crashed crashed = new Crashed(ids, file.getFilePath(), committed.getVersion(), committed.getContentSize(),
          firstNameOffset);

      ((LocalDatabase) db).kill();
      db.close();
      return crashed;
    }

    /** Forges the header (sector 0) a torn flush persisted alone: the given version and the content size of the last commit. */
    private static void writeHeader(final Crashed crashed, final long version) throws Exception {
      try (final RandomAccessFile raf = new RandomAccessFile(crashed.dictionaryFilePath(), "rw")) {
        raf.seek(0);
        assertThat(raf.readInt()).as("the new names must not have reached the disk").isLessThan((int) crashed.finalVersion() - 1);
        raf.seek(0);
        raf.writeInt((int) version);
        raf.writeInt(crashed.finalContentSize());
      }
    }

    @Test
    void dictionaryTornAcrossSeveralPendingVersionsIsFullyRepairedByRecovery() throws Exception {
      final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
      final Crashed crashed = crashWithPendingNames(factory);

      // SECTOR 0 OF THE LAST VERSION REACHED THE DISK, THE NAMES BEHIND IT DID NOT
      writeHeader(crashed, crashed.finalVersion());

      final Database reopened = factory.open();
      try {
        final Dictionary dictionary = ((DatabaseInternal) reopened).getSchema().getDictionary();
        for (int i = 0; i < NAMES.length; ++i) {
          assertThat(dictionary.getIdByName(NAMES[i], false))
              .as("'%s' must keep the id records already carry (#11151)", NAMES[i]).isEqualTo(crashed.ids()[i]);
          assertThat(dictionary.getNameById(crashed.ids()[i])).isEqualTo(NAMES[i]);
        }
      } finally {
        reopened.close();
        factory.close();
      }
    }

    /**
     * A page whose disk version is beyond the newest version in the WAL had its later versions written and fsynced before their
     * WAL files were dropped. Re-applying an older entry then could overwrite bytes of those durable versions, so the entries
     * must stay skipped: a byte planted where an older entry wrote has to survive the recovery.
     */
    @Test
    void olderEntriesAreNotReappliedWhenTheWalChainToTheDiskVersionHasAGap() throws Exception {
      final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
      final Crashed crashed = crashWithPendingNames(factory);

      // THE DISK CLAIMS TWO VERSIONS BEYOND THE LAST ONE THE WAL KNOWS: NO ENTRY HAS A COMPLETE CHAIN TO IT
      writeHeader(crashed, crashed.finalVersion() + 2);

      // PLANT A SENTINEL WHERE THE ENTRY OF THE FIRST NAME WROTE: ITS LENGTH, THEN THE NAME WITH ITS FIRST LETTER CHANGED
      try (final RandomAccessFile raf = new RandomAccessFile(crashed.dictionaryFilePath(), "rw")) {
        final byte[] sentinel = ("X" + NAMES[0].substring(1)).getBytes(StandardCharsets.UTF_8);
        raf.seek(crashed.firstNameOffset());
        raf.write(sentinel.length);
        raf.write(sentinel);
      }

      final CapturingLogger captured = new CapturingLogger();
      final Logger previousLogger = LogManager.instance().getLogger();
      LogManager.instance().setLogger(captured);
      final Database reopened;
      try {
        reopened = factory.open();
      } finally {
        LogManager.instance().setLogger(previousLogger);
      }
      try {
        final Dictionary dictionary = ((DatabaseInternal) reopened).getSchema().getDictionary();
        assertThat(dictionary.getNameById(crashed.ids()[0]))
            .as("the older entry must stay skipped, the sentinel survives").isEqualTo("X" + NAMES[0].substring(1));
        // #11152: A SKIP THE REPLAY CANNOT TELL FROM A BENIGN ONE IS REPORTED, ONCE FOR THE PAGE
        assertThat(captured.warnings()).as("the skipped entries of the gapped chain are reported once per page").hasSize(1);
        assertThat(captured.warnings().get(0)).contains("cannot repair the page from the WAL");
      } finally {
        reopened.close();
        factory.close();
      }
    }

    /** The repair of a complete chain is not a skip to report: no warning. */
    @Test
    void repairedChainDoesNotWarn() throws Exception {
      final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
      final Crashed crashed = crashWithPendingNames(factory);
      writeHeader(crashed, crashed.finalVersion());

      final CapturingLogger captured = new CapturingLogger();
      final Logger previousLogger = LogManager.instance().getLogger();
      LogManager.instance().setLogger(captured);
      final Database reopened;
      try {
        reopened = factory.open();
      } finally {
        LogManager.instance().setLogger(previousLogger);
      }
      try {
        assertThat(captured.warnings()).isEmpty();
      } finally {
        reopened.close();
        factory.close();
      }
    }

    /**
     * #11152: the repair is not specific to the schema dictionary. The same replay runs for every paginated component, so a
     * bucket page (the records of a type) torn across several pending versions has to come back whole too: each record was
     * its own commit, so each one is a version of the same page.
     */
    @Test
    void bucketPageTornAcrossSeveralPendingVersionsIsFullyRepairedByRecovery() throws Exception {
      final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
      if (factory.exists())
        factory.open().drop();

      final int total = 5;
      final List<RID> rids = new ArrayList<>();
      final DatabaseInternal db = (DatabaseInternal) factory.create();
      db.getSchema().createDocumentType("Doc");
      db.transaction(() -> db.newDocument("Doc").set("n", -1).save());
      db.getPageManager().waitAllPagesOfDatabaseAreFlushed(db);

      final LocalBucket bucket = (LocalBucket) db.getSchema().getType("Doc").getBuckets(false).get(0);
      final PaginatedComponentFile file = (PaginatedComponentFile) db.getFileManager().getFile(bucket.getFileId());

      assertThat(PageManager.INSTANCE.getFlushThread().setSuspended(db, true))
          .as("flush suspension must engage or the pages could reach disk, invalidating the test").isTrue();

      for (int i = 0; i < total; ++i) {
        final int n = i;
        db.transaction(() -> {
          final MutableDocument doc = db.newDocument("Doc").set("n", n);
          doc.save();
          rids.add(doc.getIdentity());
        });
      }

      final ImmutablePage committed = db.getPageManager()
          .getImmutablePage(new PageId(db, bucket.getFileId(), 0), file.getPageSize(), false, false);
      final long finalVersion = committed.getVersion();
      final int finalContentSize = committed.getContentSize();
      final String filePath = file.getFilePath();

      ((LocalDatabase) db).kill();
      db.close();

      // SECTOR 0 OF THE LAST VERSION REACHED THE DISK, THE RECORDS BEHIND IT DID NOT
      try (final RandomAccessFile raf = new RandomAccessFile(filePath, "rw")) {
        raf.seek(0);
        assertThat(raf.readInt()).as("the new records must not have reached the disk").isLessThan((int) finalVersion - 1);
        raf.seek(0);
        raf.writeInt((int) finalVersion);
        raf.writeInt(finalContentSize);
      }

      final Database reopened = factory.open();
      try {
        for (int i = 0; i < total; ++i)
          assertThat(reopened.lookupByRID(rids.get(i), true).asDocument().getInteger("n"))
              .as("record %s must survive a torn bucket page (#11152)", rids.get(i)).isEqualTo(i);
      } finally {
        reopened.close();
        factory.close();
      }
    }

    private record CrashedPage(String filePath, int fileId, long finalVersion, int finalContentSize, RID rid, int lastValue) {
    }

    /**
     * Commits {@code versions} updates of one record, each its own transaction and so its own version of the same bucket page,
     * keeps them off the disk and kills the database: the WAL then holds one entry per version of that page.
     */
    private CrashedPage crashWithManyVersionsOfOnePage(final DatabaseFactory factory, final int versions) throws Exception {
      if (factory.exists())
        factory.open().drop();

      final DatabaseInternal db = (DatabaseInternal) factory.create();
      db.getSchema().createDocumentType("Doc");
      final RID[] rid = new RID[1];
      db.transaction(() -> rid[0] = db.newDocument("Doc").set("n", -1).save().getIdentity());
      db.getPageManager().waitAllPagesOfDatabaseAreFlushed(db);

      final LocalBucket bucket = (LocalBucket) db.getSchema().getType("Doc").getBuckets(false).get(0);
      final PaginatedComponentFile file = (PaginatedComponentFile) db.getFileManager().getFile(bucket.getFileId());

      assertThat(PageManager.INSTANCE.getFlushThread().setSuspended(db, true))
          .as("flush suspension must engage or the pages could reach disk, invalidating the test").isTrue();

      for (int i = 0; i < versions; ++i) {
        final int n = i;
        db.transaction(() -> db.lookupByRID(rid[0], true).asDocument().modify().set("n", n).save());
      }

      // THE ONLY RECORD OF THE BUCKET LIVES IN ITS PAGE 0
      final ImmutablePage committed = db.getPageManager()
          .getImmutablePage(new PageId(db, bucket.getFileId(), 0), file.getPageSize(), false, false);
      final CrashedPage crashed = new CrashedPage(file.getFilePath(), bucket.getFileId(), committed.getVersion(),
          committed.getContentSize(), rid[0], versions - 1);

      ((LocalDatabase) db).kill();
      db.close();
      return crashed;
    }

    private static void writeHeader(final CrashedPage crashed, final long version) throws Exception {
      try (final RandomAccessFile raf = new RandomAccessFile(crashed.filePath(), "rw")) {
        raf.seek(0);
        assertThat(raf.readInt()).as("the updates must not have reached the disk").isLessThan((int) crashed.finalVersion() - 1);
        raf.seek(0);
        raf.writeInt((int) version);
        raf.writeInt(crashed.finalContentSize());
      }
    }

    /**
     * #12225: every WAL entry older than the page on disk walked the WAL versions from itself up to the disk version, so a page with
     * V versions in the WAL cost V^2/2 lookups (a TIMESERIES shard page 0 after 160,000 one-row commits took minutes to replay). The
     * chain of a page is the same for each of its entries: it is looked up once, so the cost stays linear in the versions.
     */
    @Test
    void aCompleteChainOfManyVersionsIsCheckedOncePerPage() throws Exception {
      final int versions = 3_000;
      final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
      final CrashedPage crashed = crashWithManyVersionsOfOnePage(factory, versions);

      // THE DISK HOLDS THE LAST VERSION: EVERY OLDER ENTRY IS A REPAIR CANDIDATE WITH A COMPLETE CHAIN
      writeHeader(crashed, crashed.finalVersion());

      final Database reopened = factory.open();
      try {
        assertThat(reopened.lookupByRID(crashed.rid(), true).asDocument().getInteger("n")).isEqualTo(crashed.lastValue());
        final TransactionManager transactionManager = ((DatabaseInternal) reopened).getTransactionManager();
        assertThat(transactionManager.getRecoveryChainProbesForTesting())
            .as("the chain of the page is looked up once, not once per older entry (#12225)").isLessThanOrEqualTo(3L * versions);
        assertThat(transactionManager.getRecoveryPageWritesForTesting())
            .as("the repairs of the older entries reach the file with the entry ending the chain, not one page write each (#12225)")
            .isLessThanOrEqualTo(10L);
      } finally {
        reopened.close();
        factory.close();
      }
    }

    /** #12225: the same holds when the chain has a gap: the entries stay skipped, one warning, and the lookups stay linear. */
    @Test
    void aGappedChainOfManyVersionsIsCheckedOncePerPage() throws Exception {
      final int versions = 3_000;
      final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
      final CrashedPage crashed = crashWithManyVersionsOfOnePage(factory, versions);

      // TWO VERSIONS BEYOND THE LAST ONE THE WAL KNOWS: NO OLDER ENTRY HAS A COMPLETE CHAIN
      writeHeader(crashed, crashed.finalVersion() + 2);

      final CapturingLogger captured = new CapturingLogger();
      final Logger previousLogger = LogManager.instance().getLogger();
      LogManager.instance().setLogger(captured);
      final Database reopened;
      try {
        reopened = factory.open();
      } finally {
        LogManager.instance().setLogger(previousLogger);
      }
      try {
        assertThat(captured.warnings()).as("the skipped entries of the gapped chain are reported once per page").hasSize(1);
        assertThat(((DatabaseInternal) reopened).getTransactionManager().getRecoveryChainProbesForTesting())
            .as("the chain of the page is looked up once, not once per older entry (#12225)").isLessThanOrEqualTo(3L * versions);
      } finally {
        reopened.close();
        factory.close();
      }
    }

    /**
     * #12225: the repairs of the older entries of a page reach the file with the entry that ends the chain. A replay that stops
     * before that entry (here it is corrupt) still writes them before its WAL is moved aside, so the page keeps every version the
     * WAL could repair, as when each repair wrote the page itself.
     */
    @Test
    void aReplayStoppedBeforeTheEndOfTheChainStillWritesTheRepairs() throws Exception {
      final int versions = 50;
      final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
      final CrashedPage crashed = crashWithManyVersionsOfOnePage(factory, versions);
      writeHeader(crashed, crashed.finalVersion());
      moveNewestWalChangeOutsideThePage(crashed);

      final Database reopened = factory.open();
      try {
        assertThat(((DatabaseInternal) reopened).getTransactionManager().isLastRecoveryIncomplete())
            .as("the replay stops on the corrupt entry").isTrue();
        assertThat(reopened.lookupByRID(crashed.rid(), true).asDocument().getInteger("n"))
            .as("the repairs up to the entry before the corrupt one reach the file (#12225)").isEqualTo(crashed.lastValue() - 1);
      } finally {
        reopened.close();
        factory.close();
      }
    }

    /**
     * #12225: what the repairs hold back is bounded. Over the budget the held images are written, as each repair did before, and
     * the recovery is as whole: here the budget holds no image, so two pages repaired in turn evict each other at every entry.
     */
    @Test
    void repairsOverThePendingBudgetAreWrittenAndStillRepairEveryPage() throws Exception {
      final int versions = 40;
      final String[] types = { "DocA", "DocB" };
      final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
      if (factory.exists())
        factory.open().drop();

      final DatabaseInternal db = (DatabaseInternal) factory.create();
      final RID[] firstRids = new RID[types.length];
      for (final String type : types)
        db.getSchema().createDocumentType(type);
      db.transaction(() -> {
        for (int t = 0; t < types.length; ++t)
          firstRids[t] = db.newDocument(types[t]).set("n", -1).save().getIdentity();
      });
      db.getPageManager().waitAllPagesOfDatabaseAreFlushed(db);

      assertThat(PageManager.INSTANCE.getFlushThread().setSuspended(db, true))
          .as("flush suspension must engage or the pages could reach disk, invalidating the test").isTrue();

      // ONE NEW RECORD PER COMMIT, THE TWO PAGES IN TURN: EACH VERSION WRITES BYTES NO LATER ONE REWRITES, AND THE OLDER ENTRIES OF
      // EACH PAGE ALTERNATE WITH THE OTHER'S IN THE WAL
      final List<RID> inserted = new ArrayList<>();
      for (int i = 0; i < versions; ++i)
        for (final String type : types) {
          final int n = i;
          db.transaction(() -> inserted.add(db.newDocument(type).set("n", n).save().getIdentity()));
        }

      final CrashedPage[] crashed = new CrashedPage[types.length];
      for (int t = 0; t < types.length; ++t) {
        final LocalBucket bucket = (LocalBucket) db.getSchema().getBucketById(firstRids[t].getBucketId());
        final PaginatedComponentFile file = (PaginatedComponentFile) db.getFileManager().getFile(bucket.getFileId());
        final ImmutablePage committed = db.getPageManager()
            .getImmutablePage(new PageId(db, bucket.getFileId(), 0), file.getPageSize(), false, false);
        crashed[t] = new CrashedPage(file.getFilePath(), bucket.getFileId(), committed.getVersion(), committed.getContentSize(),
            firstRids[t], -1);
      }
      assertThat(inserted).as("every record lives in page 0 of its bucket")
          .allMatch(rid -> rid.getPosition() < ((LocalBucket) db.getSchema().getBucketById(rid.getBucketId())).getMaxRecordsInPage());
      ((LocalDatabase) db).kill();
      db.close();

      for (final CrashedPage page : crashed)
        writeHeader(page, page.finalVersion());

      final long previousBudget = TransactionManager.recoveryPendingRepairsMaxBytes;
      TransactionManager.recoveryPendingRepairsMaxBytes = 0;
      final Database reopened;
      try {
        reopened = factory.open();
      } finally {
        TransactionManager.recoveryPendingRepairsMaxBytes = previousBudget;
      }
      try {
        for (int i = 0; i < inserted.size(); ++i)
          assertThat(reopened.lookupByRID(inserted.get(i), true).asDocument().getInteger("n"))
              .as("record %s is repaired although its page was written out over the budget", inserted.get(i))
              .isEqualTo(i / types.length);
        assertThat(((DatabaseInternal) reopened).getTransactionManager().getRecoveryPageWritesForTesting())
            .as("with no budget the held repairs are written at every entry of the other page").isGreaterThanOrEqualTo(versions);
      } finally {
        reopened.close();
        factory.close();
      }
    }

    /**
     * Moves the change of the newest WAL entry, the last version of the page, far outside the page: the WAL still reads it (so the
     * chain is complete), and applying it throws, which stops the replay right there.
     */
    private static void moveNewestWalChangeOutsideThePage(final CrashedPage crashed) throws Exception {
      final int txHeaderSize = 2 * Binary.LONG_SERIALIZED_SIZE + 2 * Binary.INT_SERIALIZED_SIZE;
      final int txFooterSize = Binary.INT_SERIALIZED_SIZE + Binary.LONG_SERIALIZED_SIZE;

      File newestFile = null;
      long newestTxId = -1;
      long newestPosition = -1;
      final File[] walFiles = new File(DB_PATH).listFiles((dir, name) -> name.endsWith(".wal"));
      assertThat(walFiles).isNotEmpty();
      for (final File wal : walFiles)
        try (final RandomAccessFile raf = new RandomAccessFile(wal, "r")) {
          long position = 0;
          while (position + txHeaderSize <= raf.length()) {
            raf.seek(position);
            final long txId = raf.readLong();
            raf.readLong();
            raf.readInt();
            final int segmentSize = raf.readInt();
            if (segmentSize <= 0)
              break;
            if (txId > newestTxId) {
              newestTxId = txId;
              newestFile = wal;
              newestPosition = position;
            }
            position += txHeaderSize + segmentSize + txFooterSize;
          }
        }
      assertThat(newestFile).as("the WAL holds the updates").isNotNull();

      try (final RandomAccessFile raf = new RandomAccessFile(newestFile, "rw")) {
        raf.seek(newestPosition + txHeaderSize);
        assertThat(raf.readInt()).as("the newest entry changes the bucket").isEqualTo(crashed.fileId());
        assertThat(raf.readInt()).as("in its page 0").isZero();
        final int changesFrom = raf.readInt();
        final int changesTo = raf.readInt();
        raf.seek(newestPosition + txHeaderSize + 2L * Binary.INT_SERIALIZED_SIZE);
        raf.writeInt(changesFrom + (1 << 30));
        raf.writeInt(changesTo + (1 << 30));
      }
    }

    private static final class CapturingLogger implements Logger {
      private final List<String> warnings = new CopyOnWriteArrayList<>();

      List<String> warnings() {
        return warnings;
      }

      @Override
      public void log(final Object requester, final Level level, final String message, final Throwable exception, final String context,
          final Object arg1, final Object arg2, final Object arg3, final Object arg4, final Object arg5, final Object arg6,
          final Object arg7, final Object arg8, final Object arg9, final Object arg10, final Object arg11, final Object arg12,
          final Object arg13, final Object arg14, final Object arg15, final Object arg16, final Object arg17) {
        record(level, message);
      }

      @Override
      public void log(final Object requester, final Level level, final String message, final Throwable exception, final String context,
          final Object... args) {
        record(level, message);
      }

      @Override
      public void flush() {
        // NO-OP
      }

      private void record(final Level level, final String message) {
        if (level == Level.WARNING && message != null && message.startsWith("Crash recovery skipped the WAL entry"))
          warnings.add(message);
      }
    }
  }

  /**
   * A WAL entry that ships a page as a FULL image bridges a version gap in an ordinary replay, not only in a
   * {@code forceApply} one (issue #11238): it is what a writer that committed a page without the WAL writes when the
   * page could not reach the disk in time, so recovery must apply it over whatever older version the file holds. A
   * partial delta over a gap is still refused.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class FullPageImageBridgesVersionGap extends TestHelper {
    /** The tests hand-write WAL pages over the bucket, which the end-of-test integrity check would report. */
    @Override
    protected boolean isCheckingDatabaseIntegrity() {
      return false;
    }

    @Override
    protected void beginTest() {
      database.getSchema().createDocumentType("GapType");
      database.transaction(() -> {
        for (int i = 0; i < 10; i++)
          database.newDocument("GapType").set("name", "record-" + i).save();
      });
    }

    @Test
    void aFullPageImageOverAVersionGapIsApplied() throws Exception {
      final DatabaseInternal db = (DatabaseInternal) database;
      final int fileId = db.getSchema().getType("GapType").getBuckets(false).getFirst().getFileId();
      final int pageSize = ((PaginatedComponentFile) db.getFileManager().getFile(fileId)).getPageSize();
      final PageId pageId = new PageId(db, fileId, 0);
      final ImmutablePage before = loadFromDisk(db, pageId, pageSize);
      final int baseVersion = (int) before.getVersion();

      final byte[] image = new byte[pageSize - BasePage.PAGE_HEADER_SIZE];
      image[10] = (byte) 0xAB;
      final WALFile.WALTransaction tx = transactionOf(fileId, baseVersion + 3, BasePage.PAGE_HEADER_SIZE, pageSize - 1, image,
          before.getContentSize());

      assertThat(db.getTransactionManager().applyChanges(tx, Collections.emptyMap(), false)).isTrue();

      final ImmutablePage after = loadFromDisk(db, pageId, pageSize);
      assertThat(after.getVersion()).isEqualTo(baseVersion + 3);
      assertThat(after.getContent().array()[BasePage.PAGE_HEADER_SIZE + 10]).isEqualTo((byte) 0xAB);
    }

    /** Only a gap is bridged: a full image OLDER than the page is superseded and must not roll the page back. */
    @Test
    void aStaleFullPageImageDoesNotOverwriteANewerPage() throws Exception {
      final DatabaseInternal db = (DatabaseInternal) database;
      final int fileId = db.getSchema().getType("GapType").getBuckets(false).getFirst().getFileId();
      final int pageSize = ((PaginatedComponentFile) db.getFileManager().getFile(fileId)).getPageSize();
      final PageId pageId = new PageId(db, fileId, 0);
      final ImmutablePage before = loadFromDisk(db, pageId, pageSize);
      final int baseVersion = (int) before.getVersion();
      assertThat(baseVersion).as("the page needs an older version to be stale against").isGreaterThan(0);
      final byte markerBefore = before.getContent().array()[BasePage.PAGE_HEADER_SIZE + 10];

      final byte[] image = new byte[pageSize - BasePage.PAGE_HEADER_SIZE];
      image[10] = (byte) (markerBefore + 1);
      final WALFile.WALTransaction tx = transactionOf(fileId, baseVersion - 1, BasePage.PAGE_HEADER_SIZE, pageSize - 1, image,
          before.getContentSize());

      assertThat(db.getTransactionManager().applyChanges(tx, Collections.emptyMap(), false)).isFalse();

      final ImmutablePage after = loadFromDisk(db, pageId, pageSize);
      assertThat(after.getVersion()).isEqualTo(baseVersion);
      assertThat(after.getContent().array()[BasePage.PAGE_HEADER_SIZE + 10]).isEqualTo(markerBefore);
    }

    @Test
    void aPartialDeltaOverAVersionGapIsStillRefused() throws Exception {
      final DatabaseInternal db = (DatabaseInternal) database;
      final int fileId = db.getSchema().getType("GapType").getBuckets(false).getFirst().getFileId();
      final int pageSize = ((PaginatedComponentFile) db.getFileManager().getFile(fileId)).getPageSize();
      final PageId pageId = new PageId(db, fileId, 0);
      final ImmutablePage before = loadFromDisk(db, pageId, pageSize);
      final int baseVersion = (int) before.getVersion();

      // the whole page but one byte: still a delta, and the byte it leaves out would keep whatever the file holds
      final byte[] almostAll = new byte[pageSize - BasePage.PAGE_HEADER_SIZE - 1];
      final WALFile.WALTransaction tx = transactionOf(fileId, baseVersion + 3, BasePage.PAGE_HEADER_SIZE, pageSize - 2, almostAll,
          before.getContentSize());

      assertThatThrownBy(() -> db.getTransactionManager().applyChanges(tx, Collections.emptyMap(), false))
          .isInstanceOf(WALVersionGapException.class);
      assertThat(loadFromDisk(db, pageId, pageSize).getVersion()).isEqualTo(baseVersion);
    }

    @Test
    void aPageMarkedFullyModifiedShipsOneFullImage() {
      // an existing page (a new one is modified in full from the start)
      final MutablePage page = new MutablePage(new PageId(database, 1, 0), 65_536, new byte[65_536], 3, 1_000);
      page.updateModifiedRange(100, 103);
      page.updateModifiedRange(9_000, 9_003);
      assertThat(page.getModifiedRangeCount()).isEqualTo(2);

      page.markFullyModified();

      assertThat(page.getModifiedRangeCount()).isEqualTo(1);
      assertThat(page.getModifiedRanges()[0]).isEqualTo(BasePage.PAGE_HEADER_SIZE);
      assertThat(page.getModifiedRanges()[1]).isEqualTo(page.getPhysicalSize() - 1);
    }

    private static ImmutablePage loadFromDisk(final DatabaseInternal db, final PageId pageId, final int pageSize)
        throws Exception {
      db.getPageManager().removePageFromCache(pageId);
      return db.getPageManager().getImmutablePage(pageId, pageSize, false, true);
    }

    private static WALFile.WALTransaction transactionOf(final int fileId, final int version, final int changesFrom,
        final int changesTo, final byte[] content, final int contentSize) {
      final WALFile.WALTransaction tx = new WALFile.WALTransaction();
      tx.txId = 1_000;
      tx.timestamp = System.currentTimeMillis();

      final WALFile.WALPage page = new WALFile.WALPage();
      page.fileId = fileId;
      page.pageNumber = 0;
      page.currentPageVersion = version;
      page.changesFrom = changesFrom;
      page.changesTo = changesTo;
      page.currentPageSize = contentSize;
      page.currentContent = new Binary(content);
      tx.pages = new WALFile.WALPage[] { page };
      return tx;
    }
  }

  /**
   * Regression tests for issue #11250: a crash recovery that did not complete (its WAL has a gap, so it was preserved and
   * nothing further was replayed) used to leave only a log line behind. Callers that can heal the database from another copy,
   * an HA node resyncing from its leader, had no way to ask, and the node went on serving a database whose recovery had
   * stopped half way. {@link TransactionManager#isLastRecoveryIncomplete()} is the answer they read.
   */
  @Nested
  class RecoveryOutcome extends TestHelper {

    private static final int TX_HEADER_SIZE   = 24;
    private static final int PAGE_HEADER_SIZE = 24;
    private static final int TX_FOOTER_SIZE   = 12;

    @Override
    protected void beginTest() {
      database.getSchema().getOrCreateDocumentType("RecoveryOutcomeType");
    }

    @Test
    void aRecoveryAbortedOnAWalGapIsReportedIncomplete() throws Exception {
      final TransactionManager transactionManager = ((DatabaseInternal) database).getTransactionManager();
      assertThat(transactionManager.isLastRecoveryIncomplete()).isFalse();

      // A corrupt first record FOLLOWED by a valid transaction record is the pattern the gap detector aborts recovery on
      final byte[] garbage = new byte[32];
      Arrays.fill(garbage, (byte) 0x7F);
      final byte[] delta = { 1, 2, 3, 4, 5 };
      final int segmentSize = PAGE_HEADER_SIZE + delta.length;
      final ByteBuffer buf = ByteBuffer.allocate(garbage.length + TX_HEADER_SIZE + segmentSize + TX_FOOTER_SIZE);
      buf.put(garbage);
      buf.putLong(42L);
      buf.putLong(123456L);
      buf.putInt(1);
      buf.putInt(segmentSize);
      buf.putInt(3);
      buf.putInt(0);
      buf.putInt(0);
      buf.putInt(delta.length - 1);
      buf.putInt(1);
      buf.putInt(1024);
      buf.put(delta);
      buf.putInt(segmentSize);
      buf.putLong(WALFile.MAGIC_NUMBER);
      Files.write(new File(database.getDatabasePath(), "txlog_777.wal").toPath(), buf.array());

      transactionManager.checkIntegrity();

      assertThat(transactionManager.isLastRecoveryIncomplete())
          .as("recovery stopped on the gap and preserved the WAL: the database is not known to be whole")
          .isTrue();

      // The WAL is now .corrupt, so a second pass finds nothing to replay: it must not erase what the first one skipped
      transactionManager.checkIntegrity();
      assertThat(transactionManager.isLastRecoveryIncomplete()).isTrue();
    }

    @Test
    void aRecoveryThatReplayedEverythingIsNotReportedIncomplete() {
      final TransactionManager transactionManager = ((DatabaseInternal) database).getTransactionManager();

      transactionManager.checkIntegrity();

      assertThat(transactionManager.isLastRecoveryIncomplete()).isFalse();
    }

    /**
     * Issue #11280: a replay that THROWS (a torn page whose bytes no longer decode) escaped recovery, so the open failed and every
     * restart repeated it, because the WAL stayed in place against the same torn pages. It is now the same outcome as a WAL gap:
     * the WAL is preserved aside, the database opens, and the recovery is reported incomplete for the HA node to quarantine it.
     */
    @Test
    void aRecoveryWhoseReplayThrowsIsReportedIncompleteInsteadOfFailingTheOpen() throws Exception {
      final TransactionManager transactionManager = ((DatabaseInternal) database).getTransactionManager();
      database.transaction(() -> database.newDocument("RecoveryOutcomeType").set("a", 1).save());

      final LocalBucket bucket = (LocalBucket) database.getSchema().getType("RecoveryOutcomeType").getBuckets(false).get(0);
      final int pageVersion = (int) ((DatabaseInternal) database).getPageManager()
          .getImmutablePage(new PageId(database, bucket.getFileId(), 0), bucket.getPageSize(), false, false).getVersion();

      // A well-formed transaction one version ahead of the page, whose change lies far outside the page: applying it throws
      final byte[] delta = { 1, 2, 3, 4, 5 };
      final int changesFrom = Integer.MAX_VALUE / 2;
      final int segmentSize = PAGE_HEADER_SIZE + delta.length;
      final ByteBuffer buf = ByteBuffer.allocate(TX_HEADER_SIZE + segmentSize + TX_FOOTER_SIZE);
      buf.putLong(42L);
      buf.putLong(123456L);
      buf.putInt(1);
      buf.putInt(segmentSize);
      buf.putInt(bucket.getFileId());
      buf.putInt(0);
      buf.putInt(changesFrom);
      buf.putInt(changesFrom + delta.length - 1);
      buf.putInt(pageVersion + 1);
      buf.putInt(bucket.getPageSize());
      buf.put(delta);
      buf.putInt(segmentSize);
      buf.putLong(WALFile.MAGIC_NUMBER);
      final File wal = new File(database.getDatabasePath(), "txlog_778.wal");
      Files.write(wal.toPath(), buf.array());

      transactionManager.checkIntegrity();

      assertThat(transactionManager.isLastRecoveryIncomplete()).isTrue();
      assertThat(wal).as("the WAL must not stay in place to be replayed and thrown on again at every open").doesNotExist();
      assertThat(new File(wal.getPath() + ".corrupt")).as("preserved aside for inspection").exists();
    }
  }
}
