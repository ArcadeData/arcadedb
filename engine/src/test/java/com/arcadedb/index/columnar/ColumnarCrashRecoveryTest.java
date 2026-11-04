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
package com.arcadedb.index.columnar;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.RID;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.ColumnarTransactionManagerTestAccess;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.engine.PageManager;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.CallableNoReturn;
import com.arcadedb.utility.IntHashSet;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Engine crash tests of the columnar projection: the database is killed ({@link DatabaseInternal#kill()}, no I/O) with an operation
 * stopped at a named kill point, then reopened. The projection must come back equal to GROUP BY through recovery (WAL replay, the
 * persisted recovery set, crash tails dropped), never through a rebuild, except for a no-WAL write, which only a rebuild can cover.
 */
@Tag("slow")
class ColumnarCrashRecoveryTest extends TestHelper {
  private static final String[] PROPERTIES = { "brand", "tags", "fileSize", "publishDate", "author" };
  private static final long     HANG_MS    = 10_000;

  @Override
  protected void beginTest() {
    // only the test flushes: a background flush by age would reach the armed point first, or flush what a test keeps unflushed
    GlobalConfiguration.COLUMNAR_FLUSH_INTERVAL.setValue(Long.MAX_VALUE);
  }

  @Override
  protected void endTest() {
    ColumnarTestHooks.killPoint = null;
    TransactionContext.afterPublishBeforeSecondMarkForTesting = null;
    TransactionContext.afterFirstColumnarMarkForTesting = null;
  }

  @ParameterizedTest
  @ValueSource(strings = { "flush.segmentWritten", "publish.segmentsSynced", "publish.dictionariesSynced", "publish.manifestTmpWritten",
      "publish.manifestRenamed" })
  void killDuringFlushRecoversWithoutRebuild(final String point) throws Exception {
    final ColumnarTestData data = schemaWithData(21L, 2_000);
    // a second batch with deletes and updates, applied but not flushed: the flush killed below has dead rows to write too
    data.deleteRatio = 0.1;
    data.mutate(database, 500);
    FacetOracle.awaitCaughtUp(database);

    final KillPoint kill = KillPoint.arm(point);
    try {
      final ColumnarIndexManager manager = manager();
      crashAt(kill, () -> manager.flushNow(), true);
    } finally {
      kill.disarm();
    }

    database = factory.open();
    assertRecoveredWithoutRebuild();
  }

  @ParameterizedTest
  @ValueSource(strings = { "merge.segmentWritten", "publish.manifestTmpWritten", "publish.manifestRenamed" })
  void killDuringMergeRecoversWithoutRebuild(final String point) throws Exception {
    final ColumnarTestData data = schemaWithData(22L, 1_000);
    data.deleteRatio = 0.1;
    data.mutate(database, 300);
    FacetOracle.awaitCaughtUp(database);

    // no pass from here on: no pass schedules a merge of its own that would delete the selected segments, and no overlay is left for
    // a background flush to publish before the merge does
    KillPoint kill = null;
    try (final AutoCloseable hold = manager().suspendApply()) {
      manager().flushNow();
      final ColumnarProjection projection = manager().projection(bucketId());
      assertThat(projection.manifest().segments().size()).isGreaterThanOrEqualTo(2);
      final long[] toMerge = MergePolicy.select(projection.manifest().segments(), 0.3f, 1);
      assertThat(toMerge).isNotEmpty();

      kill = KillPoint.arm(point);
      crashAt(kill, () -> ColumnarFlusher.merge(projection, toMerge), true);
    } finally {
      if (kill != null)
        kill.disarm();
    }

    database = factory.open();
    assertRecoveredWithoutRebuild();
  }

  @Test
  void killDuringDictionaryAppendTruncatesTheTail() throws Exception {
    schemaWithData(23L, 500);
    final int bucketId = bucketId();
    final ColumnarManifest.DictionaryEntry before = brandDictionary(manager().projection(bucketId).manifest());
    final Path dictionaryFile = bucketDir(bucketId).resolve(before.file());
    assertThat(Files.size(dictionaryFile)).isEqualTo(before.bytes());

    // a brand no record holds yet: the apply pass appends it to the dictionary and stops between the record header and its payload
    final KillPoint kill = KillPoint.arm("dictionary.appendPartial");
    try {
      database.transaction(() -> database.newDocument(ColumnarTestData.TYPE).set("brand", "brand-never-seen").save());
      // reached on an apply-pool thread, which this test has no handle on: the SimulatedCrash fails that pass, and the pass logs it
      assertThat(kill.awaitReached(HANG_MS)).isTrue();
      ((DatabaseInternal) database).kill();
      kill.releaseAsCrashed();
    } finally {
      kill.disarm();
    }
    database.close();
    // the crash tail: a header without its payload past what the manifest names
    assertThat(Files.size(dictionaryFile)).isGreaterThan(before.bytes());

    // no apply pass on the reopened database until the dictionary was checked, so nothing is appended after the open truncated
    GlobalConfiguration.COLUMNAR_APPLY_INTERVAL.setValue(Long.MAX_VALUE);
    database = factory.open();
    final ColumnarProjection projection = manager().projection(bucketId);
    final ColumnarManifest.DictionaryEntry reopened = brandDictionary(projection.manifest());
    assertThat(reopened).isEqualTo(before);
    assertThat(projection.dictionary("brand").size()).isEqualTo(reopened.entries());
    assertThat(Files.size(dictionaryFile)).isEqualTo(reopened.bytes());
    GlobalConfiguration.COLUMNAR_APPLY_INTERVAL.reset();

    assertRecoveredWithoutRebuild();
    assertThat(projection.dictionary("brand").ordinalOf("brand-never-seen")).isGreaterThanOrEqualTo(reopened.entries());
  }

  @Test
  void killAfterRecoverySetPersistedBeforeWalDrop() throws Exception {
    final ColumnarTestData data = schemaWithData(24L, 1_000);
    // only the test drops WAL files: a housekeeping pass could drop them before the kill, or between the snapshot and the rotation
    ColumnarTransactionManagerTestAccess.cancelHousekeeping(((DatabaseInternal) database).getTransactionManager());
    final File databaseDir = new File(database.getDatabasePath());
    final int bucketId = bucketId();
    final Path recoveryFile = recoveryFile();

    final KillPoint kill = KillPoint.arm(ColumnarIndexManager.RECOVERY_SET_PERSISTED);
    try (final AutoCloseable hold = manager().suspendApply()) {
      // pages committed and not applied: the recovery set the rotation persists names them
      data.mutate(database, 500);
      final Set<String> walFiles = walFiles(databaseDir);
      assertThat(walFiles).isNotEmpty();
      // released before the kill: the rotation stops at the point holding the WAL pool lock, which the kill takes. The crash aborts
      // the rotation before its first drop, and nothing runs between the two
      crashAt(kill, () -> ColumnarTransactionManagerTestAccess.rotateAndDropWAL(((DatabaseInternal) database).getTransactionManager()),
          false);
      // the kill came after the set was written and before any WAL file was dropped
      assertThat(walFiles(databaseDir)).containsAll(walFiles);
      assertThat(RecoverySetFile.read(recoveryFile).get(bucketId)).isNotEmpty();
    } finally {
      kill.disarm();
    }

    database = factory.open();
    assertRecoveredWithoutRebuild();
  }

  @Test
  void killAfterWalDropIsCoveredByThePersistedSet() throws Exception {
    final ColumnarTestData data = schemaWithData(25L, 1_000);
    // only the test drops WAL files: a housekeeping pass could drop them before the kill, or between the snapshot and the rotation
    ColumnarTransactionManagerTestAccess.cancelHousekeeping(((DatabaseInternal) database).getTransactionManager());
    final int bucketId = bucketId();
    final Path recoveryFile = recoveryFile();

    try (final AutoCloseable hold = manager().suspendApply()) {
      data.mutate(database, 500);
      // every committed page on disk before the rotation drops the WAL files that cover them
      assertThat(PageManager.INSTANCE.waitAllPagesOfDatabaseAreFlushed(((DatabaseInternal) database).getEmbedded())).isTrue();
      ColumnarTransactionManagerTestAccess.rotateAndDropWAL(((DatabaseInternal) database).getTransactionManager());
      assertThat(RecoverySetFile.read(recoveryFile).get(bucketId)).isNotEmpty();
      ((DatabaseInternal) database).kill();
    }
    database.close();

    database = factory.open();
    assertRecoveredWithoutRebuild();
    assertThat(manager().getStats().recoveryPagesCount()).isPositive();
  }

  @Test
  void killBetweenCommitPublishAndSecondMark() throws Exception {
    schemaWithData(26L, 1_000);
    // only the test drops WAL files: a housekeeping pass could drop them before the kill, or between the snapshot and the rotation
    ColumnarTransactionManagerTestAccess.cancelHousekeeping(((DatabaseInternal) database).getTransactionManager());
    final int bucketId = bucketId();
    final int maxRecordsInPage = manager().projection(bucketId).maxRecordsInPage();
    final Path recoveryFile = recoveryFile();

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
    final AtomicReference<RID> inserted = new AtomicReference<>();
    final Thread committer = new Thread(() -> {
      try {
        database.transaction(() -> inserted.set(database.newDocument(ColumnarTestData.TYPE).set("brand", "between-marks").save().getIdentity()));
      } catch (final Throwable e) {
        // the commit resumes on a killed database: whatever it throws then is part of the crash
      }
    }, "columnar-crash-committer");

    try (final AutoCloseable hold = manager().suspendApply()) {
      committer.start();
      assertThat(paused.await(HANG_MS, TimeUnit.MILLISECONDS)).isTrue();
      TransactionContext.afterPublishBeforeSecondMarkForTesting = null;

      // the published pages reach the disk, then a forced rotation drops the WAL file the paused commit still counts: only the first
      // mark, persisted in the recovery set, knows the page now
      assertThat(PageManager.INSTANCE.waitAllPagesOfDatabaseAreFlushed(((DatabaseInternal) database).getEmbedded())).isTrue();
      ColumnarTransactionManagerTestAccess.rotateAndDropWAL(((DatabaseInternal) database).getTransactionManager());
      ((DatabaseInternal) database).kill();
    } finally {
      release.countDown();
    }
    committer.join(HANG_MS);
    assertThat(committer.isAlive()).isFalse();
    database.close();

    final RID rid = inserted.get();
    assertThat(rid).isNotNull();
    assertThat(RecoverySetFile.read(recoveryFile).get(bucketId)).contains((int) (rid.getPosition() / maxRecordsInPage));

    database = factory.open();
    assertThat(database.countType(ColumnarTestData.TYPE, true)).isEqualTo(1_001);
    assertRecoveredWithoutRebuild();
    assertThat(manager().getStats().recoveryPagesCount()).isPositive();
  }

  @Test
  void killWithNoWalWritesRebuildsUncleanNoWal() throws Exception {
    final ColumnarTestData data = schemaWithData(27L, 1_000);
    final Path marker = bucketDir(bucketId()).resolve(ColumnarStore.NOWAL_MARKER);

    final TransactionContext transaction = ((DatabaseInternal) database).getTransaction();
    try {
      transaction.setUseWAL(false);
      data.insertAssets(database, 500);
    } finally {
      transaction.setUseWAL(true);
    }
    assertThat(marker).exists();
    assertThat(PageManager.INSTANCE.waitAllPagesOfDatabaseAreFlushed(((DatabaseInternal) database).getEmbedded())).isTrue();
    ((DatabaseInternal) database).kill();
    database.close();

    database = factory.open();
    assertEquivalent();
    assertThat(manager().getStats().rebuilds(ColumnarTestData.TYPE, ColumnarRebuilder.UNCLEAN_NO_WAL)).isEqualTo(1);
    assertThat(manager().getStats().rebuildsTotal()).isEqualTo(1);
    assertThat(marker).doesNotExist();
  }

  @Test
  void crashRecoveryNeverRebuilds() throws Exception {
    final ColumnarTestData data = schemaWithData(28L, 0);
    data.insertAssets(database, 10_000);

    ((DatabaseInternal) database).kill();
    database.close();

    database = factory.open();
    assertThat(database.countType(ColumnarTestData.TYPE, true)).isEqualTo(10_000);
    assertRecoveredWithoutRebuild();
  }

  /**
   * Task 3 of #10662: a kill with a coalesced overlay (fewer rows than flushMinRows, younger than flushMaxAge) recovers to the oracle with
   * no rebuild. 3,000 scattered updates over a bucket of about 4,000 pages change more pages than {@link ColumnarIndexManager#REAPPLY_FLOOR}
   * and a quarter of the bucket, which an unclean open would rebuild ({@code unclean_pages}) were they all left unflushed: the
   * unflushed-page bound flushes the overlay before it gets there. With record-granular marks (Task 15): since Task 15b a
   * page read by record is stamped with the version it read, so a flushed one is not taken for changed.
   */
  @Test
  void crashWithCoalescedOverlayRecoversWithoutRebuild() throws Exception {
    // 16 KB pages, so about 4,000 pages take some 60,000 records only
    GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE.setValue(16_384);
    GlobalConfiguration.COLUMNAR_FLUSH_MIN_ROWS.setValue(4_096);
    GlobalConfiguration.COLUMNAR_FLUSH_MAX_AGE.setValue(Long.MAX_VALUE);
    final ColumnarTestData data = new ColumnarTestData(29L);
    data.createSchema(database);
    final LocalBucket bucket = (LocalBucket) database.getSchema().getBucketById(bucketId());
    final List<RID> rids = new ArrayList<>();
    while (bucket.getTotalPages() < 4_000)
      rids.addAll(data.insertAssets(database, 2_000));
    FacetOracle.awaitCaughtUp(database);
    manager().flushNow();
    // coalescing from here: a finite flushInterval the overlay ignores below flushMinRows (3,000 updates stay below 4,096) and a
    // flushMaxAge it never reaches, so only the unflushed-page bound flushes
    GlobalConfiguration.COLUMNAR_FLUSH_INTERVAL.setValue(1_000L);
    final int pages = bucket.getTotalPages();
    final int maxRecordsInPage = manager().projection(bucketId()).maxRecordsInPage();
    final long flushes = manager().getStats().flushCount(ColumnarTestData.TYPE);

    final Random random = new Random(29L);
    final IntHashSet touched = new IntHashSet();
    for (int batch = 0; batch < 30; batch++) {
      database.transaction(() -> {
        for (int i = 0; i < 100; i++) {
          final RID rid = rids.get(random.nextInt(rids.size()));
          touched.add((int) (rid.getPosition() / maxRecordsInPage));
          database.lookupByRID(rid, true).asDocument().modify().set("brand", "brand-" + random.nextInt(200)).save();
        }
      });
      FacetOracle.awaitCaughtUp(database);
    }
    // the test can fail: left unflushed, these pages are past both bounds of the unclean-open page check
    assertThat(touched.size()).isGreaterThan(ColumnarIndexManager.REAPPLY_FLOOR).isGreaterThan(pages / 4);
    assertThat(manager().getStats().flushCount(ColumnarTestData.TYPE)).as("the unflushed-page bound flushed").isGreaterThan(flushes);

    // the kill comes with a coalesced overlay: 20 more updates once no flush is queued or running and the pages left unflushed stay
    // below the bound with them, so no flush takes these rows before the kill
    final ColumnarProjection projection = manager().projection(bucketId());
    for (int i = 0; i < 10_000 && !(manager().isMaintenanceIdleForTesting()
        && projection.tracker().unflushedCount() < ColumnarIndexManager.REAPPLY_FLOOR / 2 - 20); i++)
      Thread.sleep(1);
    assertThat(manager().isMaintenanceIdleForTesting()).isTrue();
    assertThat(projection.tracker().unflushedCount()).isLessThan(ColumnarIndexManager.REAPPLY_FLOOR / 2 - 20);
    database.transaction(() -> {
      for (int i = 0; i < 20; i++)
        database.lookupByRID(rids.get(random.nextInt(rids.size())), true).asDocument().modify().set("brand", "brand-last-" + i).save();
    });
    FacetOracle.awaitCaughtUp(database);
    assertThat(projection.overlay().rows()).as("rows of the coalesced overlay at the kill").isPositive();

    ((DatabaseInternal) database).kill();
    database.close();

    database = factory.open();
    assertRecoveredWithoutRebuild();
  }

  /**
   * Task 15b of #10662: 3,000 scattered updates, applied record by record (Task 15), all flushed, then a kill. A
   * page an apply pass read by record is stamped with the version it read, so the page check of the unclean open finds none of them
   * changed: no {@code unclean_pages} rebuild (before Task 15b each kept the version of its last whole read, and the check rebuilt).
   */
  @Test
  void crashWithRecordMarkedPagesRecoversWithoutRebuild() throws Exception {
    // 16 KB pages, so about 4,000 pages take some 60,000 records only
    GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE.setValue(16_384);
    final ColumnarTestData data = new ColumnarTestData(30L);
    data.createSchema(database);
    final LocalBucket bucket = (LocalBucket) database.getSchema().getBucketById(bucketId());
    final List<RID> rids = new ArrayList<>();
    while (bucket.getTotalPages() < 4_000)
      rids.addAll(data.insertAssets(database, 2_000));
    FacetOracle.awaitCaughtUp(database);
    manager().flushNow();
    final int pages = bucket.getTotalPages();
    final ColumnarProjection projection = manager().projection(bucketId());
    final int maxRecordsInPage = projection.maxRecordsInPage();
    final long rereadBefore = manager().getStats().recordsRereadCount(ColumnarTestData.TYPE);

    final Random random = new Random(30L);
    final IntHashSet touched = new IntHashSet();
    for (int batch = 0; batch < 30; batch++) {
      database.transaction(() -> {
        for (int i = 0; i < 100; i++) {
          final RID rid = rids.get(random.nextInt(rids.size()));
          touched.add((int) (rid.getPosition() / maxRecordsInPage));
          database.lookupByRID(rid, true).asDocument().modify().set("brand", "brand-" + random.nextInt(200)).save();
        }
      });
      FacetOracle.awaitCaughtUp(database);
    }
    // the test can fail: these pages are past both bounds of the unclean-open page check, which rebuilds if it finds them changed
    assertThat(touched.size()).isGreaterThan(ColumnarIndexManager.REAPPLY_FLOOR).isGreaterThan(pages / 4);
    // and they were applied by record, not whole: a whole read of about 4,000 pages would re-read the whole bucket
    assertThat(manager().getStats().recordsRereadCount(ColumnarTestData.TYPE) - rereadBefore).isLessThan(2 * 3_000);

    manager().flushNow();
    assertThat(projection.overlay().rows()).isZero();
    ((DatabaseInternal) database).kill();
    database.close();

    database = factory.open();
    assertRecoveredWithoutRebuild();
    assertThat(manager().getStats().rebuilds(ColumnarTestData.TYPE, ColumnarRebuilder.UNCLEAN_PAGES)).isZero();
  }

  /**
   * Task 15b: the window a version stamped by a record read leaves open. A commit's first mark is applied before its publication (from
   * the previous images); after the publication a pass reads another record of the page by record and stamps the version it read, and
   * the page is flushed with it; the kill comes before the commit's second mark. The page check would take the page for current, so
   * the stale record must come back through the WAL: the housekeeping never drops the file of a commit between its two marks, and its
   * replay marks the page. Removing that guard makes this test fail.
   */
  @Test
  void aRecordMarkPendingAtTheCrashIsReappliedAfterIt() throws Exception {
    schemaWithData(31L, 1_000);
    // only the test drops WAL files, and as the housekeeping does
    ColumnarTransactionManagerTestAccess.cancelHousekeeping(((DatabaseInternal) database).getTransactionManager());
    final File databaseDir = new File(database.getDatabasePath());
    final int bucketId = bucketId();
    final ColumnarIndexManager manager = manager();
    final ColumnarProjection projection = manager.projection(bucketId);
    final int maxRecordsInPage = projection.maxRecordsInPage();
    final List<RID> pair = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT @rid AS rid FROM " + ColumnarTestData.TYPE + " LIMIT 2")) {
      while (rs.hasNext())
        pair.add(rs.next().<RID>getProperty("rid"));
    }
    assertThat(pair).hasSize(2);
    assertThat(pair.get(0).getPosition() / maxRecordsInPage).as("the premise: one page")
        .isEqualTo(pair.get(1).getPosition() / maxRecordsInPage);
    // the WAL files of the load dropped first: the only files the rotation below may keep are the ones the held commit wrote
    assertThat(PageManager.INSTANCE.waitAllPagesOfDatabaseAreFlushed(((DatabaseInternal) database).getEmbedded())).isTrue();
    ColumnarTransactionManagerTestAccess.rotateAndDropWALAsHousekeeping(((DatabaseInternal) database).getTransactionManager());
    final int page = (int) (pair.get(0).getPosition() / maxRecordsInPage);

    final CountDownLatch firstMarked = new CountDownLatch(1);
    final CountDownLatch releaseFirst = new CountDownLatch(1);
    final CountDownLatch published = new CountDownLatch(1);
    final CountDownLatch releasePublished = new CountDownLatch(1);
    TransactionContext.afterFirstColumnarMarkForTesting = () -> pause(firstMarked, releaseFirst);
    TransactionContext.afterPublishBeforeSecondMarkForTesting = () -> pause(published, releasePublished);
    final Thread committer = new Thread(() -> {
      try {
        database.transaction(() -> {
          database.lookupByRID(pair.get(0), true).asDocument().modify().set("brand", "pending-at-crash").save();
          database.lookupByRID(pair.get(1), true).asDocument().modify().set("brand", "read-by-record").save();
        });
      } catch (final Throwable e) {
        // the commit resumes on a killed database: whatever it throws then is part of the crash
      }
    }, "columnar-crash-committer");

    try (final AutoCloseable hold = manager.suspendApply()) {
      committer.start();
      assertThat(firstMarked.await(HANG_MS, TimeUnit.MILLISECONDS)).isTrue();
      TransactionContext.afterFirstColumnarMarkForTesting = null;
      // the first mark is drained and its records read from the images before the publication: nothing changes
      ColumnarApplier.runPass(manager, projection);
      releaseFirst.countDown();
      assertThat(published.await(HANG_MS, TimeUnit.MILLISECONDS)).isTrue();
      TransactionContext.afterPublishBeforeSecondMarkForTesting = null;

      // after the publication, the second record alone is marked (as another commit would) and read by record: the page is stamped with
      // the version it read while the first record keeps its old value, and the page is flushed with that version
      manager.markRecords(bucketId, new long[] { pair.get(1).getPosition() }, 1);
      ColumnarApplier.runPass(manager, projection);
      // the stamp this test is about: the version read, after the publication, with the first record's row still the old one
      final LocalBucket bucket = (LocalBucket) database.getSchema().getBucketById(bucketId);
      final long committed = bucket.visitCommittedSlots(page, new long[maxRecordsInPage / 64], (position, kind, content, offPages,
          count) -> {
      });
      assertThat(projection.overlay().page(page).pageVersion()).as("stamped with the version read").isEqualTo(committed);
      manager.flushNow();
      assertThat(projection.overlay().rows()).isZero();

      // every page on disk, then the housekeeping: it drops what it may (the unused one), and keeps the WAL file of the commit between
      // its marks, which only the guard keeps: its pages are on disk
      assertThat(PageManager.INSTANCE.waitAllPagesOfDatabaseAreFlushed(((DatabaseInternal) database).getEmbedded())).isTrue();
      final Set<String> walOfTheCommit = walFiles(databaseDir);
      ColumnarTransactionManagerTestAccess.rotateAndDropWALAsHousekeeping(((DatabaseInternal) database).getTransactionManager());
      assertThat(walFiles(databaseDir)).as("the WAL file of the commit between its marks").containsAnyElementsOf(walOfTheCommit);
      ((DatabaseInternal) database).kill();
    } finally {
      releaseFirst.countDown();
      releasePublished.countDown();
    }
    committer.join(HANG_MS);
    assertThat(committer.isAlive()).isFalse();
    database.close();

    database = factory.open();
    assertRecoveredWithoutRebuild();
    assertThat(FacetOracle.projectionCounts(database, ColumnarTestData.TYPE, "brand", null, false).get("pending-at-crash"))
        .as("the record whose second mark the crash lost").isEqualTo(1L);
  }

  private static void pause(final CountDownLatch reached, final CountDownLatch release) {
    reached.countDown();
    try {
      release.await();
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  // ------------------------------------------------------------------------------------------------------------------- helpers

  /**
   * Runs the operation on its own thread until it reaches the armed point, kills the database there, lets the operation end as crashed
   * and closes the killed instance. With {@code killFirst} false the operation is ended as crashed before the kill, for a point reached
   * under a lock the kill takes.
   */
  private void crashAt(final KillPoint kill, final CallableNoReturn operation, final boolean killFirst) throws Exception {
    final AtomicReference<Throwable> ended = new AtomicReference<>();
    final Thread worker = new Thread(() -> {
      try {
        operation.call();
      } catch (final Throwable e) {
        ended.set(e);
      }
    }, "columnar-crash-worker");
    worker.start();
    // a hang detector, not a latency bound
    assertThat(kill.awaitReached(HANG_MS)).isTrue();
    if (killFirst)
      ((DatabaseInternal) database).kill();
    kill.releaseAsCrashed();
    worker.join(HANG_MS);
    assertThat(worker.isAlive()).isFalse();
    if (!killFirst)
      ((DatabaseInternal) database).kill();
    assertThat(ended.get()).isInstanceOf(ColumnarTestHooks.SimulatedCrash.class);
    database.close();
  }

  /** The schema of {@link ColumnarTestData}, {@code count} records applied and flushed. */
  private ColumnarTestData schemaWithData(final long seed, final int count) throws InterruptedException {
    final ColumnarTestData data = new ColumnarTestData(seed);
    data.createSchema(database);
    if (count > 0) {
      data.insertAssets(database, count);
      FacetOracle.awaitCaughtUp(database);
      manager().flushNow();
    }
    return data;
  }

  /** Every column equal to GROUP BY once caught up, and no rebuild since the open: the crash was recovered, not rebuilt. */
  private void assertRecoveredWithoutRebuild() {
    assertEquivalent();
    assertThat(manager().getStats().rebuildsTotal()).isZero();
  }

  private void assertEquivalent() {
    for (final String property : PROPERTIES)
      FacetOracle.assertEquivalent(database, ColumnarTestData.TYPE, property, null);
  }

  private static ColumnarManifest.DictionaryEntry brandDictionary(final ColumnarManifest manifest) {
    int indexFileId = -1;
    for (final ColumnDefinition column : manifest.columns())
      if (column.property().equals("brand"))
        indexFileId = column.indexFileId();
    for (final ColumnarManifest.DictionaryEntry entry : manifest.dictionaries())
      if (entry.indexFileId() == indexFileId)
        return entry;
    throw new AssertionError("The manifest has no dictionary for brand");
  }

  private static Set<String> walFiles(final File databaseDir) {
    final String[] names = databaseDir.list((dir, name) -> name.endsWith(".wal"));
    return names == null ? Set.of() : Set.of(names);
  }

  private ColumnarIndexManager manager() {
    return ((LocalDatabase) ((DatabaseInternal) database).getEmbedded()).getColumnarIndexManager();
  }

  private int bucketId() {
    return database.getSchema().getType(ColumnarTestData.TYPE).getBuckets(false).stream().mapToInt(Bucket::getFileId).findFirst()
        .orElseThrow();
  }

  private Path bucketDir(final int bucketId) {
    return ColumnarStore.bucketDir(Path.of(database.getDatabasePath()), bucketId);
  }

  private Path recoveryFile() {
    return Path.of(database.getDatabasePath()).resolve(ColumnarStore.ROOT_DIR).resolve(RecoverySetFile.FILE_NAME);
  }
}
