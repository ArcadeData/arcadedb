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
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.Record;
import com.arcadedb.exception.PageSnapshotException;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.schema.Schema;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.CRC32;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the page-level copy-on-write snapshot primitive of issue #6075, phase 2b of the backup roadmap.
 * <p>
 * The property under test throughout is that a window opened at t0 keeps serving the t0 image no matter what the
 * live database does afterwards. Every test that asserts "the snapshot did not change" also asserts that the LIVE
 * files DID change in the meantime, so it cannot pass vacuously by simply not writing anything.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class PageSnapshotTest extends TestHelper {

  private static final String TYPE       = "Doc";
  private static final String SPILL_TYPE  = "Spill";
  private static final String SPARSE_TYPE = "Sparse";

  @Override
  protected void beginTest() {
    final Schema schema = database.getSchema();
    final DocumentType type = schema.createDocumentType(TYPE);
    type.createProperty("id", Integer.class);
    type.createProperty("payload", String.class);

    database.transaction(() -> {
      for (int i = 0; i < 2_000; i++)
        newDocument(i, "initial");
    });
  }

  /**
   * The headline property: with the window open, rewriting every record leaves the snapshot untouched. The live
   * files are asserted to have changed, and the shadow to have captured pages, so a snapshot that silently returned
   * the LIVE bytes would fail here rather than pass by accident.
   */
  @Test
  void snapshotKeepsServingTheT0ImageWhileTheDatabaseIsRewritten() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    final PageManager pageManager = db.getPageManager();

    try (final PageSnapshot snapshot = pageManager.openSnapshot(db)) {
      assertThat(snapshot.getStatus()).isEqualTo(PageSnapshot.STATUS.ACTIVE);
      assertThat(snapshot.getFiles()).isNotEmpty();

      // THE SNAPSHOT MUST START OUT BYTE-IDENTICAL TO THE FILES ON DISK: NOTHING IS WRITING RIGHT NOW
      final Map<Integer, Long> t0Checksums = new HashMap<>();
      for (final PageSnapshot.SnapshotFile file : snapshot.getFiles()) {
        final long viaSnapshot = snapshot.calculateChecksum(file.fileId());
        assertThat(viaSnapshot).as("file %s at t0", file.fileName()).isEqualTo(file.file().calculateChecksum());
        t0Checksums.put(file.fileId(), viaSnapshot);
      }

      // REWRITE EVERY RECORD SEVERAL TIMES AND PUSH IT ALL TO DISK
      for (int round = 0; round < 3; round++) {
        final int currentRound = round;
        database.transaction(() -> {
          database.iterateType(TYPE, false).forEachRemaining(record -> {
            final MutableDocument doc = record.asDocument().modify();
            doc.set("payload", "rewritten-" + currentRound + "-" + "x".repeat(200));
            doc.save();
          });
        });
        pageManager.waitAllPagesOfDatabaseAreFlushed(db);
      }

      assertThat(snapshot.getShadowedPages())
          .as("the rewrite must have forced pre-image captures, otherwise this test proves nothing").isPositive();

      boolean anyLiveFileChanged = false;
      for (final PageSnapshot.SnapshotFile file : snapshot.getFiles()) {
        assertThat(snapshot.calculateChecksum(file.fileId())).as("snapshot of %s after the rewrite", file.fileName())
            .isEqualTo(t0Checksums.get(file.fileId()));
        if (file.file().calculateChecksum() != t0Checksums.get(file.fileId()))
          anyLiveFileChanged = true;
      }
      assertThat(anyLiveFileChanged).as("the live database must have changed under the snapshot").isTrue();
    }

    assertThat(pageManager.isSnapshotWindowOpen(db)).isFalse();
  }

  /**
   * A snapshot restored into a fresh directory has to be a database that opens, passes the integrity check and
   * holds exactly the records that existed at t0 - not a byte more, not a byte less.
   */
  @Test
  void restoredSnapshotOpensAndHoldsExactlyTheRecordsOfT0() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    final PageManager pageManager = db.getPageManager();

    final File restoreDir = new File("target/databases/" + getClass().getSimpleName() + "-restored");
    FileUtils.deleteRecursively(restoreDir);

    final long countAtT0;
    try (final PageSnapshot snapshot = pageManager.openSnapshot(db)) {
      countAtT0 = database.countType(TYPE, false);

      // ADD RECORDS AFTER t0: THEY MUST NOT SHOW UP IN THE RESTORE
      database.transaction(() -> {
        for (int i = 0; i < 500; i++)
          newDocument(100_000 + i, "after-t0");
      });
      pageManager.waitAllPagesOfDatabaseAreFlushed(db);

      restoreTo(restoreDir, snapshot);
    }

    try (final DatabaseFactory restoredFactory = new DatabaseFactory(restoreDir.getAbsolutePath())) {
      final Database restored = restoredFactory.open();
      try {
        assertThat(restored.countType(TYPE, false)).isEqualTo(countAtT0);
        assertThat(restored.command("sql", "check database").nextIfAvailable().<Long>getProperty("totalErrors")).isZero();
      } finally {
        restored.drop();
      }
    }

    assertThat(database.countType(TYPE, false)).isEqualTo(countAtT0 + 500);
  }

  /**
   * The t0 barrier under sustained concurrent write load, which is the test that justifies the whole design: the
   * window is opened while writers are mid-flight, and the image it serves must be a genuine transaction boundary -
   * a restored copy opens, passes the integrity check, and holds a record count between what was committed before
   * the barrier started and what was committed by the time it returned. A torn snapshot fails the integrity check;
   * a snapshot that kept moving fails the upper bound.
   */
  @Test
  void barrierProducesAConsistentPointInTimeUnderConcurrentWriters() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    final PageManager pageManager = db.getPageManager();

    final File restoreDir = new File("target/databases/" + getClass().getSimpleName() + "-restored-concurrent");
    FileUtils.deleteRecursively(restoreDir);

    final AtomicBoolean running = new AtomicBoolean(true);
    final AtomicInteger committed = new AtomicInteger();
    final AtomicReference<Exception> writerFailure = new AtomicReference<>();
    final CountDownLatch warmedUp = new CountDownLatch(1);

    final List<Thread> writers = new ArrayList<>();
    for (int w = 0; w < 4; w++) {
      final int writerId = w;
      final Thread writer = new Thread(() -> {
        int seq = 0;
        while (running.get()) {
          try {
            final int id = 1_000_000 * (writerId + 1) + seq++;
            database.transaction(() -> newDocument(id, "concurrent"));
            committed.incrementAndGet();
            warmedUp.countDown();
          } catch (final Exception e) {
            writerFailure.compareAndSet(null, e);
            return;
          }
        }
      }, "snapshot-writer-" + w);
      writer.setDaemon(true);
      writers.add(writer);
      writer.start();
    }

    try {
      assertThat(warmedUp.await(30, TimeUnit.SECONDS)).isTrue();

      final long committedBeforeBarrier = database.countType(TYPE, false);
      final PageSnapshot snapshot = pageManager.openSnapshot(db);
      final long committedAfterBarrier = database.countType(TYPE, false);
      try {
        // KEEP THE WRITERS HAMMERING WHILE THE SNAPSHOT IS BEING READ: EVERY PAGE THEY TOUCH GOES THROUGH THE
        // COPY-ON-WRITE HOOK AND MUST BE SERVED FROM THE SHADOW HERE
        Thread.sleep(500);
        restoreTo(restoreDir, snapshot);
        assertThat(snapshot.getStatus()).isEqualTo(PageSnapshot.STATUS.ACTIVE);
      } finally {
        snapshot.close();
      }

      running.set(false);
      for (final Thread writer : writers)
        writer.join(30_000);
      assertThat(writerFailure.get()).isNull();

      try (final DatabaseFactory restoredFactory = new DatabaseFactory(restoreDir.getAbsolutePath())) {
        final Database restored = restoredFactory.open();
        try {
          assertThat(restored.command("sql", "check database").nextIfAvailable().<Long>getProperty("totalErrors")).isZero();
          assertThat(restored.countType(TYPE, false)).isBetween(committedBeforeBarrier, committedAfterBarrier);
        } finally {
          restored.drop();
        }
      }
    } finally {
      running.set(false);
      for (final Thread writer : writers)
        writer.join(30_000);
    }
  }

  /**
   * Challenge C3: two windows opened at different instants each keep their OWN point in time. The shared pre-image
   * read is only correct because a page's content when it is first written after t1 is also its content at t1.
   */
  @Test
  void overlappingWindowsEachServeTheirOwnPointInTime() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    final PageManager pageManager = db.getPageManager();

    try (final PageSnapshot first = pageManager.openSnapshot(db)) {
      final Map<Integer, Long> firstChecksums = checksums(first);

      database.transaction(() -> {
        for (int i = 0; i < 500; i++)
          newDocument(200_000 + i, "between-windows");
      });
      pageManager.waitAllPagesOfDatabaseAreFlushed(db);

      try (final PageSnapshot second = pageManager.openSnapshot(db)) {
        final Map<Integer, Long> secondChecksums = checksums(second);
        assertThat(secondChecksums).as("the second window must see the records written after the first one")
            .isNotEqualTo(firstChecksums);

        database.transaction(() -> {
          for (int i = 0; i < 500; i++)
            newDocument(300_000 + i, "after-both-windows");
        });
        pageManager.waitAllPagesOfDatabaseAreFlushed(db);

        assertThat(checksums(first)).isEqualTo(firstChecksums);
        assertThat(checksums(second)).isEqualTo(secondChecksums);
      }

      // THE FIRST WINDOW OUTLIVES THE SECOND AND IS STILL ANCHORED WHERE IT WAS
      database.transaction(() -> newDocument(400_000, "after-the-second-window-closed"));
      pageManager.waitAllPagesOfDatabaseAreFlushed(db);
      assertThat(checksums(first)).isEqualTo(firstChecksums);
    }
  }

  /**
   * Challenge C4: on breaching the cap the window fails loudly instead of silently serving a mix of t0 and live
   * pages. Silent unbounded growth, and silent truncation, are both unacceptable; a consumer catching this falls
   * back to the suspend-and-freeze path.
   */
  @Test
  void shadowCapBreachInvalidatesTheWindowInsteadOfTearingIt() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    final PageManager pageManager = db.getPageManager();

    // ENOUGH RECORDS TO SPREAD OVER MANY PAGES, SO REWRITING THEM DIRTIES FAR MORE THAN THE CAP BELOW ALLOWS
    database.transaction(() -> {
      for (int i = 0; i < 20_000; i++)
        newDocument(500_000 + i, "z".repeat(500));
    });
    pageManager.waitAllPagesOfDatabaseAreFlushed(db);

    // ONE MEGABYTE OF SHADOW: A HANDFUL OF PAGES, WHICH THE REWRITE BELOW BLOWS THROUGH IMMEDIATELY
    GlobalConfiguration.PAGE_SNAPSHOT_MAX_RAM.setValue(1);
    GlobalConfiguration.PAGE_SNAPSHOT_MAX_SIZE.setValue(1);

    try (final PageSnapshot snapshot = pageManager.openSnapshot(db)) {
      assertThat(snapshot.getStatus()).isEqualTo(PageSnapshot.STATUS.ACTIVE);

      for (int round = 0; round < 5 && snapshot.getStatus() == PageSnapshot.STATUS.ACTIVE; round++) {
        final int currentRound = round;
        database.transaction(() -> {
          database.iterateType(TYPE, false).forEachRemaining(record -> {
            final MutableDocument doc = record.asDocument().modify();
            doc.set("payload", "overflow-" + currentRound + "-" + "y".repeat(400));
            doc.save();
          });
        });
        pageManager.waitAllPagesOfDatabaseAreFlushed(db);
      }

      assertThat(snapshot.getStatus()).isEqualTo(PageSnapshot.STATUS.OVERFLOWED);
      assertThatThrownBy(() -> snapshot.calculateChecksum(snapshot.getFiles().getFirst().fileId()))
          .isInstanceOf(PageSnapshotException.class).hasMessageContaining("OVERFLOWED");
    }

    // THE LIVE DATABASE IS UNHARMED: A SNAPSHOT MUST NEVER BE ABLE TO BREAK IT
    assertThat(database.countType(TYPE, false)).isEqualTo(22_000);
  }

  /**
   * Challenge C2: a file dropped while a window is open survives on disk until the window closes, so index
   * compaction (which drops files and does NOT take the database write lock) can keep running during a backup
   * instead of being postponed for its whole duration.
   */
  @Test
  void aFileDroppedDuringAWindowIsDeletedOnlyWhenTheWindowCloses() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    final PageManager pageManager = db.getPageManager();

    database.getSchema().createDocumentType("Disposable");
    database.transaction(() -> {
      for (int i = 0; i < 100; i++)
        database.newDocument("Disposable").set("id", i).save();
    });
    pageManager.waitAllPagesOfDatabaseAreFlushed(db);

    final List<File> droppedFiles = new ArrayList<>();
    for (final ComponentFile file : db.getFileManager().getFiles())
      if (file != null && file.getComponentName().startsWith("Disposable"))
        droppedFiles.add(file.getOSFile());
    assertThat(droppedFiles).isNotEmpty();

    try (final PageSnapshot snapshot = pageManager.openSnapshot(db)) {
      for (final File file : droppedFiles)
        assertThat(snapshot.getFile(fileIdOf(db, file))).isNotNull();

      database.getSchema().dropType("Disposable");

      assertThat(database.getSchema().existsType("Disposable")).isFalse();
      for (final File file : droppedFiles)
        assertThat(file).as("deletion of %s must be deferred while the window is open", file.getName()).exists();

      // AND THE WINDOW STILL READS EVERY FILE IT CAPTURED, THE DROPPED ONES INCLUDED
      for (final PageSnapshot.SnapshotFile file : snapshot.getFiles())
        assertThat(snapshot.calculateChecksum(file.fileId())).isNotNegative();
    }

    for (final File file : droppedFiles)
      assertThat(file).as("deletion of %s must happen when the window closes", file.getName()).doesNotExist();
  }

  /**
   * The window between listing the files a snapshot covers and publishing the window has to be closed, or a file
   * dropped inside it is physically deleted even though the snapshot has already claimed it - and the reader then
   * holds a closed channel. Index compaction drops files without taking any database lock, so this is a live race,
   * not a theoretical one. The drops here run against a snapshot barrier over and over to exercise the interleaving.
   */
  @Test
  void aFileDroppedWhileAWindowIsOpeningIsNeverDeletedUnderIt() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    final PageManager pageManager = db.getPageManager();

    final int rounds = 30;
    for (int i = 0; i < rounds; i++) {
      final String typeName = "Racy" + i;
      database.getSchema().createDocumentType(typeName);
      final int round = i;
      database.transaction(() -> {
        for (int r = 0; r < 50; r++)
          database.newDocument(typeName).set("id", round * 1000 + r).save();
      });
    }
    pageManager.waitAllPagesOfDatabaseAreFlushed(db);

    final AtomicReference<Exception> dropperFailure = new AtomicReference<>();
    for (int i = 0; i < rounds; i++) {
      final String typeName = "Racy" + i;
      final Thread dropper = new Thread(() -> {
        try {
          database.getSchema().dropType(typeName);
        } catch (final Exception e) {
          dropperFailure.compareAndSet(null, e);
        }
      }, "snapshot-race-dropper-" + i);
      dropper.setDaemon(true);
      dropper.start();

      try (final PageSnapshot snapshot = pageManager.openSnapshot(db)) {
        // EVERY FILE THE WINDOW CLAIMED MUST STILL BE READABLE, WHETHER OR NOT THE DROP LANDED INSIDE THE BARRIER
        for (final PageSnapshot.SnapshotFile file : snapshot.getFiles())
          assertThat(snapshot.calculateChecksum(file.fileId())).as("file %s of round %d", file.fileName(), 0)
              .isNotNegative();
        assertThat(snapshot.getStatus()).isEqualTo(PageSnapshot.STATUS.ACTIVE);
      }

      dropper.join(30_000);
      assertThat(dropperFailure.get()).isNull();
    }

    assertThat(database.countType(TYPE, false)).isEqualTo(2_000);
  }

  /**
   * A SPARSE set of shadowed pages inside one bulk-read run: the case the reader gets wrong if it only restores the
   * pages it found missing on the first probe.
   * <p>
   * The reader resolves a run of {@code READ_RUN_PAGES} pages by probing the shadow, bulk-reading the span of pages
   * that were NOT in it, then re-probing. A page that was already shadowed but sits INSIDE that span - because its
   * neighbours were not shadowed - has its pre-image overwritten by the bulk read, and is only restored if the
   * re-probe is unconditional. Pages are shadowed in write order, which has nothing to do with run boundaries, so
   * this is the ordinary case rather than an exotic one, and getting it wrong is silent: the archive simply holds
   * post-t0 content for those pages.
   * <p>
   * Updating one record every few hundred lands the dirty pages scattered across the file, which is exactly the
   * pattern needed. The assertion is the whole-file checksum, so any page served from after t0 fails it.
   */
  @Test
  void aSparseSetOfShadowedPagesInsideOneReadRunStillServesT0() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    final PageManager pageManager = db.getPageManager();

    database.getSchema().buildDocumentType().withName(SPARSE_TYPE).withTotalBuckets(1).withPageSize(16_384).create();
    database.transaction(() -> {
      for (int i = 0; i < 20_000; i++)
        database.newDocument(SPARSE_TYPE).set("id", i).set("payload", "p".repeat(120)).save();
    });
    pageManager.waitAllPagesOfDatabaseAreFlushed(db);

    try (final PageSnapshot snapshot = pageManager.openSnapshot(db)) {
      final Map<Integer, Long> t0Checksums = checksums(snapshot);
      final int filesAtT0 = snapshot.getFiles().size();

      // EVERY 500th RECORD: THE DIRTIED PAGES ARE SPREAD OUT, SO A BULK-READ RUN TYPICALLY SPANS SHADOWED PAGES
      // WITH UNSHADOWED NEIGHBOURS ON BOTH SIDES
      database.transaction(() -> {
        final Iterator<Record> records = database.iterateType(SPARSE_TYPE, false);
        for (int i = 0; records.hasNext(); i++) {
          final Record record = records.next();
          if (i % 500 == 0)
            record.asDocument().modify().set("payload", "M".repeat(120)).save();
        }
      });
      pageManager.waitAllPagesOfDatabaseAreFlushed(db);

      final int shadowed = snapshot.getShadowedPages();
      assertThat(shadowed).as("the scattered updates must shadow some pages, but far from all of them").isPositive();
      assertThat(shadowed).as("a fully shadowed file would not exercise the mixed run this test is about")
          .isLessThan(filesAtT0 * 100);

      assertThat(checksums(snapshot)).as("a page shadowed before the read must not be served from the live file")
          .isEqualTo(t0Checksums);
    }
  }

  /**
   * The shadow is RAM first with disk spill (challenge C5), and the spill is the half a short backup never reaches -
   * so it needs its own test or it ships untested. A 1 MB RAM budget against a dataset laid out over hundreds of
   * small pages forces the pre-images past the budget and onto disk, which exercises the spill write, the spill
   * read, the primitive index rehash (its load factor trips past 512 entries), and the scratch-file cleanup on
   * close - while the t0 image the window serves must be exactly as intact as it is for a RAM-only shadow.
   */
  @Test
  void aShadowThatOutgrowsItsRamBudgetSpillsToDiskAndStillServesT0() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    final PageManager pageManager = db.getPageManager();

    // SMALL PAGES SO A FEW MB OF DATA BECOMES THE HUNDREDS OF DISTINCT PAGES THE REHASH AND THE SPILL BOTH NEED
    database.getSchema().buildDocumentType().withName(SPILL_TYPE).withTotalBuckets(1).withPageSize(16_384).create();
    database.transaction(() -> {
      for (int i = 0; i < 60_000; i++)
        database.newDocument(SPILL_TYPE).set("id", i).set("payload", "s".repeat(120)).save();
    });
    pageManager.waitAllPagesOfDatabaseAreFlushed(db);

    GlobalConfiguration.PAGE_SNAPSHOT_MAX_RAM.setValue(1);
    GlobalConfiguration.PAGE_SNAPSHOT_MAX_SIZE.setValue(256);

    final File[] shadowsDuringWindow;
    try (final PageSnapshot snapshot = pageManager.openSnapshot(db)) {
      final Map<Integer, Long> t0Checksums = checksums(snapshot);

      database.transaction(() -> database.iterateType(SPILL_TYPE, false).forEachRemaining(
          record -> record.asDocument().modify().set("payload", "t".repeat(120)).save()));
      pageManager.waitAllPagesOfDatabaseAreFlushed(db);

      assertThat(snapshot.getStatus()).isEqualTo(PageSnapshot.STATUS.ACTIVE);
      assertThat(snapshot.getShadowedPages()).as("the rewrite must shadow more pages than the index starts sized for")
          .isGreaterThan(512);
      assertThat(snapshot.getShadowSpilledBytes()).as("the shadow must have outgrown its 1 MB RAM budget").isPositive();

      shadowsDuringWindow = shadowFiles();
      assertThat(shadowsDuringWindow).as("the spill file must exist while the window is open").isNotEmpty();

      // THE POINT OF THE TEST: A SPILLED PRE-IMAGE READS BACK EXACTLY LIKE A RAM ONE
      assertThat(checksums(snapshot)).isEqualTo(t0Checksums);
    }

    assertThat(shadowFiles()).as("the scratch file must be deleted when the window closes").isEmpty();
  }

  /**
   * Challenge C8: the shadow is pure scratch, so a crash mid-window leaves an orphan file which the next open must
   * delete. It must also never be mistaken for a data file, which is why its extension is absent from
   * {@code LocalDatabase.SUPPORTED_FILE_EXT}.
   */
  @Test
  void orphanShadowFilesAreDeletedOnOpen() throws Exception {
    final File orphan = new File(database.getDatabasePath(), "snapshot-42." + PageSnapshot.SHADOW_FILE_EXT);
    Files.write(orphan.toPath(), new byte[] { 1, 2, 3 });
    assertThat(orphan).exists();

    reopenDatabase();

    assertThat(orphan).doesNotExist();
    assertThat(database.countType(TYPE, false)).isEqualTo(2_000);
    for (final ComponentFile file : ((DatabaseInternal) database).getFileManager().getFiles())
      if (file != null)
        assertThat(file.getFileExtension()).isNotEqualTo(PageSnapshot.SHADOW_FILE_EXT);
  }

  /**
   * Acceptance criterion of #6075: index compaction RUNS during a window instead of being postponed for its whole
   * duration. The old suspension was the only thing keeping the compactor from dropping a file under a running
   * backup, and {@code LSMTreeIndex.scheduleCompaction} refuses outright while flushing is suspended - so a long
   * backup silently stopped compaction. The snapshot suspends nothing, and the compacted file it drops has its
   * deletion deferred instead.
   */
  @Test
  void indexCompactionRunsWhileAWindowIsOpen() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    final PageManager pageManager = db.getPageManager();

    // NO AUTOMATIC SCHEDULING, SO THE COMPACTION THIS TEST DRIVES IS THE ONLY ONE IN FLIGHT AND THE ASSERTION BELOW
    // IS ABOUT THE GUARD BEING OPEN, NOT ABOUT WINNING A RACE WITH THE BACKGROUND COMPACTOR
    GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE.setValue(0);

    database.getSchema().buildTypeIndex(TYPE, new String[] { "id" }).withType(Schema.INDEX_TYPE.LSM_TREE)
        .withUnique(false).withPageSize(4_096).create();
    database.transaction(() -> {
      for (int i = 0; i < 20_000; i++)
        newDocument(600_000 + i, "for-the-index");
    });
    pageManager.waitAllPagesOfDatabaseAreFlushed(db);

    final IndexInternal index = (IndexInternal) database.getSchema().getType(TYPE).getIndexesByProperties("id").getFirst();

    try (final PageSnapshot snapshot = pageManager.openSnapshot(db)) {
      final Map<Integer, Long> t0Checksums = checksums(snapshot);

      assertThat(pageManager.isPageFlushingSuspended(db)).as("a snapshot window must not suspend page flushing").isFalse();

      // BEFORE #6075 BOTH OF THESE RETURNED false FOR AS LONG AS THE BACKUP HELD ITS FLUSH SUSPENSION
      assertThat(index.scheduleCompaction()).as("compaction must not be postponed by an open window").isTrue();
      assertThat(index.compact()).as("a compaction must complete while the window is open").isTrue();
      pageManager.waitAllPagesOfDatabaseAreFlushed(db);

      // AND THE WINDOW IS STILL READABLE, INCLUDING THE FILES THE COMPACTION DROPPED
      assertThat(checksums(snapshot)).isEqualTo(t0Checksums);
      assertThat(snapshot.getStatus()).isEqualTo(PageSnapshot.STATUS.ACTIVE);
    }

    assertThat(database.countType(TYPE, false)).isEqualTo(22_000);
  }

  /** With no window open nothing is registered, so the write path's single field read stays null. */
  @Test
  void noWindowRegisteredWhenNoneIsOpen() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    assertThat(db.getPageManager().isSnapshotWindowOpen(db)).isFalse();

    try (final PageSnapshot snapshot = db.getPageManager().openSnapshot(db)) {
      assertThat(db.getPageManager().isSnapshotWindowOpen(db)).isTrue();
      assertThat(snapshot.getLastTxId()).isNotNegative();
    }

    assertThat(db.getPageManager().isSnapshotWindowOpen(db)).isFalse();
  }

  // ------------------------------------------------------------------------------------------------------ HELPERS

  private void newDocument(final int id, final String payload) {
    database.newDocument(TYPE).set("id", id).set("payload", payload).save();
  }

  private File[] shadowFiles() {
    final File[] found = new File(database.getDatabasePath()).listFiles(
        (dir, name) -> name.endsWith("." + PageSnapshot.SHADOW_FILE_EXT));
    return found != null ? found : new File[0];
  }

  private Map<Integer, Long> checksums(final PageSnapshot snapshot) throws IOException {
    final Map<Integer, Long> result = new HashMap<>();
    for (final PageSnapshot.SnapshotFile file : snapshot.getFiles())
      result.put(file.fileId(), snapshot.calculateChecksum(file.fileId()));
    return result;
  }

  private int fileIdOf(final DatabaseInternal db, final File osFile) {
    for (final ComponentFile file : db.getFileManager().getFiles())
      if (file != null && file.getOSFile().equals(osFile))
        return file.getFileId();
    throw new IllegalArgumentException("File '" + osFile + "' is not registered");
  }

  /**
   * Materialises the snapshot as a database directory: the two configuration files (which the real backup copies
   * under the database read lock, since the snapshot only covers PAGE files) plus every page file as of t0.
   */
  private void restoreTo(final File targetDir, final PageSnapshot snapshot) throws IOException {
    assertThat(targetDir.mkdirs() || targetDir.isDirectory()).isTrue();

    final File configurationFile = ((LocalDatabase) ((DatabaseInternal) database).getEmbedded()).getConfigurationFile();
    if (configurationFile.exists())
      Files.copy(configurationFile.toPath(), new File(targetDir, configurationFile.getName()).toPath());

    final File schemaFile = ((LocalSchema) database.getSchema()).getConfigurationFile();
    if (schemaFile.exists())
      Files.copy(schemaFile.toPath(), new File(targetDir, schemaFile.getName()).toPath());

    for (final PageSnapshot.SnapshotFile file : snapshot.getFiles()) {
      final CRC32 crc = new CRC32();
      long written = 0;
      try (final InputStream in = snapshot.newInputStream(file.fileId());
          final OutputStream out = new FileOutputStream(new File(targetDir, file.fileName()))) {
        final byte[] buffer = new byte[64 * 1024];
        for (int read = in.read(buffer); read > 0; read = in.read(buffer)) {
          crc.update(buffer, 0, read);
          out.write(buffer, 0, read);
          written += read;
        }
      }
      assertThat(written).as("stream of %s", file.fileName()).isEqualTo(file.size());
      assertThat(crc.getValue()).isEqualTo(snapshot.calculateChecksum(file.fileId()));
    }
  }
}
