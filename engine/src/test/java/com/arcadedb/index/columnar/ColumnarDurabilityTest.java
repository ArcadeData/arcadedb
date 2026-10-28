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
import com.arcadedb.database.TransactionContext;
import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.ColumnarTransactionManagerTestAccess;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.engine.NodeLocalFiles;
import com.arcadedb.engine.PageManager;
import com.arcadedb.engine.TransactionManager;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Plan 3 final review B: a projection that may be stale after a crash or a failure never loads as usable. Every path that could lose
 * the marks of a page either re-applies the page or rebuilds the projection with a label, and the fallback serves until it publishes.
 */
@Tag("slow")
class ColumnarDurabilityTest extends TestHelper {
  private static final String[] PROPERTIES = { "brand", "tags", "fileSize", "publishDate", "author" };
  private static final long     HANG_MS    = 10_000;

  private int heldPermits = -1;

  @Override
  protected void beginTest() {
    // only the test flushes: a background flush by age would publish what a test keeps unflushed
    GlobalConfiguration.COLUMNAR_FLUSH_INTERVAL.setValue(Long.MAX_VALUE);
  }

  @Override
  protected void endTest() {
    ColumnarTestHooks.killPoint = null;
    ColumnarProjection.failTombstoneRenameForTesting = false;
    ColumnarIndexManager.REAPPLY_FLOOR = 1_024;
    ColumnarTransactionManagerTestAccess.onWALForce(null);
    GlobalConfiguration.TX_WAL_FILES.reset();
    releaseRebuildPermits();
    GlobalConfiguration.COLUMNAR_FLUSH_INTERVAL.reset();
  }

  /** B-C1: the open-time replay persists the recovery set before it drops the WAL it replayed. */
  @Test
  void aReplayedWalIsDroppedOnlyOnceTheRecoverySetNamesItsPages() throws Exception {
    final ColumnarTestData data = schemaWithData(31L, 1_000);
    cancelHousekeeping();
    try (final AutoCloseable hold = manager().suspendApply()) {
      // committed after the last WAL drop and never applied: only the WAL knows these pages
      data.mutate(database, 300);
      kill();
    }

    // the replay marks the pages, then drops the WAL; a second crash before any flush or WAL rotation
    database = factory.open();
    assertThat(RecoverySetFile.read(recoveryFile()).get(bucketId())).as("the pages the replay marked are persisted before the WAL is dropped")
        .isNotEmpty();
    kill();

    database = factory.open();
    assertRecoveredWithoutRebuild();
    assertThat(manager().getStats().recoveryPagesCount()).isPositive();
  }

  /** B-I1: a clean close that cannot persist the recovery set is crash-equivalent: the WAL and the lock file are kept. */
  @Test
  void aCloseThatCannotPersistTheRecoverySetKeepsTheWalAndTheLockFile() throws Exception {
    final ColumnarTestData data = schemaWithData(32L, 1_000);
    cancelHousekeeping();
    data.mutate(database, 300);
    FacetOracle.awaitCaughtUp(database);

    // the close flush fails (its manifest tmp name is taken by a directory) and so does the recovery set write (same trick)
    final ColumnarProjection projection = manager().projection(bucketId());
    final Path blockedManifest = projection.bucketDir().resolve(ColumnarManifest.FILE_NAME + "." + projection.nextGeneration()
        + ColumnarStore.TMP_SUFFIX);
    final Path blockedRecovery = recoveryFile().resolveSibling(RecoverySetFile.FILE_NAME + ColumnarStore.TMP_SUFFIX);
    Files.createDirectories(blockedManifest);
    Files.createDirectories(blockedRecovery);
    final File lockFile = new File(database.getDatabasePath(), NodeLocalFiles.DATABASE_LOCK_FILE_NAME);
    database.close();

    assertThat(lockFile).as("the lock file makes the next open recover").exists();
    assertThat(walFiles()).as("the WAL holds the commits the projection did not persist").isNotEmpty();
    Files.delete(blockedManifest);
    Files.delete(blockedRecovery);

    database = factory.open();
    assertRecoveredWithoutRebuild();
  }

  /** B-I2: an {@code unclean_no_wal} rebuild serves the fallback, not the stale manifest, until it publishes. */
  @Test
  void anUncleanNoWalRebuildIsUnusableUntilItPublishes() throws Exception {
    final ColumnarTestData data = schemaWithData(33L, 1_000);
    insertWithoutWAL(data, 300);
    assertThat(PageManager.INSTANCE.waitAllPagesOfDatabaseAreFlushed(embedded())).isTrue();
    kill();

    holdRebuildPermits();
    database = factory.open();
    final ColumnarProjection projection = manager().projection(bucketId());
    assertThat(projection.isUsable()).as("the rebuild is held: the fallback answers").isFalse();
    try (final ColumnarSnapshot snapshot = projection.acquire()) {
      assertThat(snapshot.isUsable()).isFalse();
    }
    releaseRebuildPermits();

    assertEquivalent();
    assertThat(manager().getStats().rebuilds(ColumnarTestData.TYPE, ColumnarRebuilder.UNCLEAN_NO_WAL)).isEqualTo(1);
  }

  /** B-I2: a corrupt recovery set rebuilds every projection, unusable until it publishes. */
  @Test
  void aCorruptRecoverySetRebuildIsUnusableUntilItPublishes() throws Exception {
    schemaWithData(34L, 1_000);
    database.close();
    Files.write(recoveryFile(), "not a recovery set".getBytes(StandardCharsets.UTF_8));

    holdRebuildPermits();
    database = factory.open();
    assertThat(manager().projection(bucketId()).isUsable()).as("the rebuild is held: the fallback answers").isFalse();
    releaseRebuildPermits();

    assertEquivalent();
    assertThat(manager().getStats().rebuilds(ColumnarTestData.TYPE, ColumnarProjection.CORRUPT)).isEqualTo(1);
  }

  /**
   * B-I3: records imported without WAL before the bucket had a projection, still in memory when the create rebuild reads them, are lost
   * by a crash: the rebuilt manifest must not be served as exact after it.
   */
  @Test
  void aNoWalImportBeforeTheIndexIsCoveredByTheNoWalMarker() throws Exception {
    final ColumnarTestData data = new ColumnarTestData(35L);
    data.columnarIndexes = false;
    data.createSchema(database);
    // the bucket file as the disk holds it before the import: restored after the crash, it is the import that never reached the disk
    assertThat(PageManager.INSTANCE.waitAllPagesOfDatabaseAreFlushed(embedded())).isTrue();
    final Path bucketFile = Path.of(embedded().getFileManager().getFile(bucketId()).getFilePath());
    final byte[] beforeImport = Files.readAllBytes(bucketFile);

    insertWithoutWAL(data, 500);
    database.command("sql", "CREATE INDEX ON " + ColumnarTestData.TYPE + " (brand) COLUMNAR");
    // the create rebuild published the imported records
    FacetOracle.awaitCaughtUp(database);
    assertThat(manager().projection(bucketId()).manifest().segments()).isNotEmpty();
    kill();
    Files.write(bucketFile, beforeImport);

    database = factory.open();
    assertThat(database.countType(ColumnarTestData.TYPE, false)).isZero();
    FacetOracle.assertEquivalent(database, ColumnarTestData.TYPE, "brand", null);
    assertThat(manager().getStats().rebuilds(ColumnarTestData.TYPE, ColumnarRebuilder.UNCLEAN_NO_WAL)).isEqualTo(1);
  }

  /** B-I4: a manifest publish forces the WAL first: the projection is never more durable than the commits it holds. */
  @Test
  void aManifestPublishForcesTheWalFirst() throws Exception {
    final ColumnarTestData data = schemaWithData(36L, 500);
    data.mutate(database, 200);
    FacetOracle.awaitCaughtUp(database);
    final ColumnarProjection projection = manager().projection(bucketId());
    final long generation = projection.manifest().generation();
    final TransactionManager transactionManager = embedded().getTransactionManager();

    cancelHousekeeping();
    final AtomicInteger syncs = new AtomicInteger();
    ColumnarTransactionManagerTestAccess.onWALSync(transactionManager, () -> {
      syncs.incrementAndGet();
      throw new IOException("injected WAL sync failure");
    });
    try {
      assertThatThrownBy(() -> manager().flushNow()).as("a WAL that cannot be forced fails the publish").isInstanceOf(
          UncheckedIOException.class);
    } finally {
      ColumnarTransactionManagerTestAccess.onWALSync(transactionManager, null);
    }
    assertThat(syncs.get()).isPositive();
    assertThat(projection.manifest().generation()).isEqualTo(generation);

    // round 2: a failed fsync is remembered (its bytes may be gone whatever a later fsync says) until the file is dropped, which
    // happens only once the data files it protects are synced
    assertThatThrownBy(() -> manager().flushNow()).as("a WAL file whose force failed is not durable").isInstanceOf(
        UncheckedIOException.class);
    assertThat(projection.manifest().generation()).isEqualTo(generation);
    // round 3: the failed file left its slot at once, so the next housekeeping drops it once its pages are on disk
    assertThat(PageManager.INSTANCE.waitAllPagesOfDatabaseAreFlushed(embedded())).isTrue();
    ColumnarTransactionManagerTestAccess.runHousekeeping(transactionManager);

    manager().flushNow();
    assertThat(projection.manifest().generation()).isGreaterThan(generation);
    assertEquivalent();
  }

  /** B-I5: the directory of a dropped projection that outlived its drop is swept at the next writable open. */
  @Test
  void anOrphanProjectionDirectoryIsSweptAtOpen() throws Exception {
    schemaWithData(37L, 500);
    final int bucketId = bucketId();
    final ColumnarProjection projection = manager().projection(bucketId);
    // a facet query holds a snapshot across the drop: the directory deletion waits for its release, which the crash prevents
    final ColumnarSnapshot held = projection.acquire();
    assertThat(held.isUsable()).isTrue();
    database.command("sql", "DROP TYPE " + ColumnarTestData.TYPE + " UNSAFE");
    final Path dir = ColumnarStore.bucketDir(Path.of(database.getDatabasePath()), bucketId);
    // round 2 NI2: the files the snapshot reads moved to a tombstone at once
    assertThat(tombstones(dir.getParent())).hasSize(1);
    kill();

    database = factory.open();
    assertThat(dir).as("no COLUMNAR index projects bucket " + bucketId + " any more").doesNotExist();
    assertThat(tombstones(dir.getParent())).as("a tombstone a crash left is swept too").isEmpty();

    // the same schema again: the bucket and index file ids are free to be handed out again, and the new projection starts empty
    final ColumnarTestData again = schemaWithData(38L, 0);
    again.insertAssets(database, 100);
    assertEquivalent();
  }

  /** B-I6: on an unclean open, a page whose committed version is not the one its segments recorded is re-applied. */
  @Test
  void anUncleanOpenReappliesPagesNewerThanTheirSegments() throws Exception {
    final ColumnarTestData data = schemaWithData(39L, 1_000);
    cancelHousekeeping();
    try (final AutoCloseable hold = manager().suspendApply()) {
      data.mutate(database, 300);
      assertThat(PageManager.INSTANCE.waitAllPagesOfDatabaseAreFlushed(embedded())).isTrue();
      // the WAL goes, the recovery set names the pages
      ColumnarTransactionManagerTestAccess.rotateAndDropWAL(embedded().getTransactionManager());
      kill();
    }
    // a mark lost on a path no other defense covers: no WAL, no recovery set
    Files.delete(recoveryFile());

    database = factory.open();
    assertRecoveredWithoutRebuild();
  }

  /** Batch 1 N3: the per-generation manifest tmp file a crash leaves between its write and its rename is deleted at the next open. */
  @Test
  void aManifestTmpFileLeftByACrashIsDeletedAtOpen() throws Exception {
    schemaWithData(40L, 300);
    final Path dir = manager().projection(bucketId()).bucketDir();
    database.close();
    final Path leftover = dir.resolve(ColumnarManifest.FILE_NAME + ".77" + ColumnarStore.TMP_SUFFIX);
    Files.write(leftover, "{}".getBytes(StandardCharsets.UTF_8));

    database = factory.open();
    assertThat(leftover).doesNotExist();
    assertRecoveredWithoutRebuild();
  }

  /** B-M2: a drifted record counter of 0 on a bucket with records does not publish an empty, usable projection. */
  @Test
  void aDriftedZeroCounterDoesNotPublishAnEmptyProjection() throws Exception {
    final ColumnarTestData data = new ColumnarTestData(41L);
    data.columnarIndexes = false;
    data.createSchema(database);
    data.insertAssets(database, 300);
    final LocalBucket bucket = (LocalBucket) database.getSchema().getBucketById(bucketId());
    bucket.setCachedRecordCount(0);

    database.command("sql", "CREATE INDEX ON " + ColumnarTestData.TYPE + " (brand) COLUMNAR");
    FacetOracle.assertEquivalent(database, ColumnarTestData.TYPE, "brand", null);
    // recounted from the pages, so the integrity check after the test finds no drift
    bucket.setCachedRecordCount(-1);
  }

  /** B-M3: a corrupt recovery set whose REBUILD marker cannot be written keeps the WAL until the marker is written. */
  @Test
  void aRebuildMarkerThatCannotBeWrittenKeepsTheWal() throws Exception {
    schemaWithData(42L, 500);
    database.close();
    Files.write(recoveryFile(), "not a recovery set".getBytes(StandardCharsets.UTF_8));
    final Path marker = recoveryFile().resolveSibling(ColumnarStore.REBUILD_MARKER);
    final Path blockedMarker = marker.resolveSibling(ColumnarStore.REBUILD_MARKER + ColumnarStore.TMP_SUFFIX);
    Files.createDirectories(blockedMarker);

    holdRebuildPermits();
    database = factory.open();
    assertThat(marker).doesNotExist();
    assertThat(manager().persistRecoverySet()).as("the lost pages are recorded nowhere yet: the WAL stays").isFalse();
    Files.delete(blockedMarker);
    assertThat(manager().persistRecoverySet()).isTrue();
    assertThat(marker).as("written by the retry").exists();
    releaseRebuildPermits();

    assertEquivalent();
    assertThat(marker).as("cleared once every rebuild published").doesNotExist();
  }

  /** Round 2 NI1: the WAL force of a publish holds no WAL file monitor, so a commit appending to the forced file does not wait. */
  @Test
  void aWalForceDoesNotBlockACommitOnTheForcedFile() throws Exception {
    // one WAL file: the commit below appends to the very file the publish forces
    GlobalConfiguration.TX_WAL_FILES.setValue(1);
    reopenDatabase();
    final ColumnarTestData data = schemaWithData(43L, 300);
    data.mutate(database, 50);
    FacetOracle.awaitCaughtUp(database);

    final CountDownLatch forcing = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    final AtomicBoolean held = new AtomicBoolean();
    ColumnarTransactionManagerTestAccess.onWALForce(() -> {
      if (held.compareAndSet(false, true)) {
        forcing.countDown();
        try {
          release.await();
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    });
    final ExecutorService threads = Executors.newFixedThreadPool(2);
    try {
      final Future<?> flush = threads.submit(() -> {
        manager().flushNow();
        return null;
      });
      assertThat(forcing.await(HANG_MS, TimeUnit.MILLISECONDS)).isTrue();
      final Future<?> commit = threads.submit(
          () -> database.transaction(() -> database.newDocument(ColumnarTestData.TYPE).set("brand", "during-force").save()));
      // a hang detector, not a latency bound: the commit completes while the force is still held
      commit.get(HANG_MS, TimeUnit.MILLISECONDS);
      release.countDown();
      flush.get(HANG_MS, TimeUnit.MILLISECONDS);
    } finally {
      release.countDown();
      ColumnarTransactionManagerTestAccess.onWALForce(null);
      threads.shutdownNow();
    }
    assertEquivalent();
  }

  /** Round 2 NI1: a WAL file with nothing appended since its last force is not forced again. */
  @Test
  void aWalFileWithNoNewBytesIsNotForcedAgain() throws Exception {
    // one WAL file and two projected buckets: the second publish of one flushNow finds the file forced by the first
    GlobalConfiguration.TX_WAL_FILES.setValue(1);
    reopenDatabase();
    final ColumnarTestData data = new ColumnarTestData(44L);
    data.buckets = 2;
    data.createSchema(database);
    data.insertAssets(database, 300);
    FacetOracle.awaitCaughtUp(database);
    assertThat(database.getSchema().getType(ColumnarTestData.TYPE).getBuckets(false)).hasSize(2);
    final AtomicInteger forces = new AtomicInteger();
    ColumnarTransactionManagerTestAccess.onWALForce(forces::incrementAndGet);
    try {
      manager().flushNow();
      assertThat(forces.get()).as("one fsync for the one dirty WAL file, whatever the number of publishes").isEqualTo(1);
    } finally {
      ColumnarTransactionManagerTestAccess.onWALForce(null);
    }
  }

  /** Round 2 NI2: a projection dropped under a held snapshot never deletes the directory of the one recreated on its bucket. */
  @Test
  void aRecreatedProjectionKeepsItsDirectoryWhenTheDroppedSnapshotIsReleased() throws Exception {
    recreateUnderHeldSnapshot();
  }

  /** Round 2 NI2: when the tombstone rename fails (Windows), the new projection waits for the old directory to be gone. */
  @Test
  void aFailedTombstoneRenameDefersTheNewProjection() throws Exception {
    ColumnarProjection.failTombstoneRenameForTesting = true;
    recreateUnderHeldSnapshot();
  }

  /** Round 2 NI3: the unclean-open page check runs in the background, the projection unusable until it ends. */
  @Test
  void theUncleanOpenPageCheckRunsAfterTheOpen() throws Exception {
    loseMarksAndKill(45L);
    final CountDownLatch reached = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    ColumnarTestHooks.killPoint = name -> {
      if (name.equals(ColumnarIndexManager.PAGE_CHECK)) {
        reached.countDown();
        try {
          release.await();
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    };
    try {
      database = factory.open();
      assertThat(reached.await(HANG_MS, TimeUnit.MILLISECONDS)).as("the check runs once the open returned").isTrue();
      assertThat(manager().projection(bucketId()).isUsable()).as("the fallback answers until the check ends").isFalse();
    } finally {
      release.countDown();
      ColumnarTestHooks.killPoint = null;
    }
    assertRecoveredWithoutRebuild();
  }

  /** Round 2: a check finding more changed pages than the bound rebuilds, labeled, instead of re-applying the bucket. */
  @Test
  void aCheckPastTheReapplyBoundRebuilds() throws Exception {
    loseMarksAndKill(46L);
    ColumnarIndexManager.REAPPLY_FLOOR = 0;
    database = factory.open();
    assertEquivalent();
    assertThat(manager().getStats().rebuilds(ColumnarTestData.TYPE, ColumnarRebuilder.UNCLEAN_PAGES)).isEqualTo(1);
  }

  /** Round 3 R1: the page check reads page headers from the file, no page goes through the page manager's read path. */
  @Test
  void thePageCheckLoadsNoPageThroughThePageManager() throws Exception {
    loseMarksAndKill(47L);
    final CountDownLatch started = new CountDownLatch(1);
    final CountDownLatch go = new CountDownLatch(1);
    final CountDownLatch done = new CountDownLatch(1);
    final CountDownLatch finish = new CountDownLatch(1);
    ColumnarTestHooks.killPoint = name -> {
      if (name.equals(ColumnarIndexManager.PAGE_CHECK))
        hold(started, go);
      else if (name.equals(ColumnarIndexManager.PAGE_CHECK_DONE))
        hold(done, finish);
    };
    final long before;
    final long after;
    try {
      database = factory.open();
      assertThat(started.await(HANG_MS, TimeUnit.MILLISECONDS)).isTrue();
      before = PageManager.INSTANCE.getStats().pagesRead;
      go.countDown();
      assertThat(done.await(HANG_MS, TimeUnit.MILLISECONDS)).isTrue();
      after = PageManager.INSTANCE.getStats().pagesRead;
    } finally {
      go.countDown();
      finish.countDown();
      ColumnarTestHooks.killPoint = null;
    }
    assertThat(after - before).as("pages loaded by the check").isZero();
    assertRecoveredWithoutRebuild();
    assertThat(manager().getStats().recoveryPagesCount()).as("the check still found the changed pages").isPositive();
  }

  /** Round 3 R2: a READ_ONLY open finding a CHECK marker serves the fallback: the check it stands for never ended. */
  @Test
  void aCheckMarkerMakesAReadOnlyOpenUnusable() throws Exception {
    schemaWithData(48L, 300);
    final Path marker = manager().projection(bucketId()).bucketDir().resolve(ColumnarStore.CHECK_MARKER);
    database.close();
    Files.write(marker, new byte[0]);

    reopenDatabaseInReadOnlyMode();
    final ColumnarProjection projection = manager().projection(bucketId());
    assertThat(projection.isUsable()).isFalse();
    assertThat(projection.unusableReason()).isEqualTo(ColumnarProjection.UNCLEAN_CHECK);
    assertThat(marker).as("a READ_ONLY open writes nothing").exists();
    reopenDatabase();
    assertEquivalent();
  }

  /** Round 3 Minor 1: a replayed WAL file kept in the retired pool was never forced by this process: a publish forces it. */
  @Test
  void aReplayedWalKeptForItsRecoverySetIsForcedByAPublish() throws Exception {
    final ColumnarTestData data = schemaWithData(49L, 1_000);
    cancelHousekeeping();
    try (final AutoCloseable hold = manager().suspendApply()) {
      data.mutate(database, 100);
      kill();
    }
    // the replay cannot persist the recovery set: its WAL is retired, not dropped
    Files.write(recoveryFile(), "not a recovery set".getBytes(StandardCharsets.UTF_8));
    final AtomicInteger forces = new AtomicInteger();
    ColumnarTransactionManagerTestAccess.onWALForce(forces::incrementAndGet);
    database = factory.open();
    assertEquivalent();
    assertThat(forces.get()).as("the rebuild's publish forced the replayed file").isPositive();
  }

  /** Round 3 Minor 3: a dropped projection that never created its directory deletes nothing its successor created. */
  @Test
  void aDroppedProjectionWithNoDirectoryDeletesNoneOfItsSuccessor() throws Exception {
    database.command("sql", "CREATE DOCUMENT TYPE Item BUCKETS 1");
    database.command("sql", "CREATE PROPERTY Item.brand STRING");
    insertItems(0, 300);
    final int bucketId = database.getSchema().getType("Item").getBuckets(false).getFirst().getFileId();
    final Path dir = ColumnarStore.bucketDir(Path.of(database.getDatabasePath()), bucketId);

    holdRebuildPermits();
    database.command("sql", "CREATE INDEX ON Item (brand) COLUMNAR");
    // its create rebuild waits for a permit: no directory yet, and a fallback query holds its unusable state
    assertThat(dir).doesNotExist();
    final ColumnarSnapshot held = manager().projection(bucketId).acquire();
    final String indexName = database.getSchema().getType("Item").getAllIndexes(false).stream()
        .filter(i -> i.getType() == Schema.INDEX_TYPE.COLUMNAR).findFirst().orElseThrow().getName();
    database.command("sql", "DROP INDEX `" + indexName + "`");
    database.command("sql", "CREATE INDEX ON Item (brand) COLUMNAR");
    releaseRebuildPermits();
    FacetOracle.awaitCaughtUp(database);
    held.close();

    assertThat(dir.resolve(ColumnarManifest.FILE_NAME)).as("the release deleted nothing of the new projection").exists();
    insertItems(300, 50);
    FacetOracle.awaitCaughtUp(database);
    manager().flushNow();
    FacetOracle.assertEquivalent(database, "Item", "brand", null);
  }

  /** Round 3 Minor 4: a CHECK marker that cannot be written keeps the WAL until it is, so a close cannot forget the check. */
  @Test
  void aCheckMarkerThatCannotBeWrittenKeepsTheWal() throws Exception {
    final int bucketId = loseMarksAndKill(50L);
    final Path blocked = ColumnarStore.bucketDir(Path.of(database.getDatabasePath()), bucketId).resolve(ColumnarStore.CHECK_MARKER
        + ColumnarStore.TMP_SUFFIX);
    Files.createDirectories(blocked);
    final CountDownLatch reached = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    ColumnarTestHooks.killPoint = name -> {
      if (name.equals(ColumnarIndexManager.PAGE_CHECK))
        hold(reached, release);
    };
    try {
      database = factory.open();
      assertThat(reached.await(HANG_MS, TimeUnit.MILLISECONDS)).isTrue();
      final Path marker = blocked.resolveSibling(ColumnarStore.CHECK_MARKER);
      assertThat(marker).doesNotExist();
      assertThat(manager().persistRecoverySet()).as("nothing records the pending check yet").isFalse();
      Files.delete(blocked);
      assertThat(manager().persistRecoverySet()).isTrue();
      assertThat(marker).exists();
    } finally {
      release.countDown();
      ColumnarTestHooks.killPoint = null;
    }
    assertRecoveredWithoutRebuild();
  }

  /** Round 3 Minor 5: a drop stops a running page check at its next page instead of waiting for the whole bucket. */
  @Test
  void aDropStopsARunningPageCheck() throws Exception {
    loseMarksAndKill(51L);
    final AtomicInteger pages = new AtomicInteger();
    final CountDownLatch inCheck = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    final CountDownLatch fencing = new CountDownLatch(1);
    ColumnarTestHooks.killPoint = name -> {
      if (name.equals(ColumnarProjection.PAGE_CHECK_PAGE) && pages.incrementAndGet() == 1)
        hold(inCheck, release);
      else if (name.equals(ColumnarIndexManager.DROP_FENCE))
        fencing.countDown();
    };
    final ExecutorService dropper = Executors.newSingleThreadExecutor();
    try {
      database = factory.open();
      assertThat(inCheck.await(HANG_MS, TimeUnit.MILLISECONDS)).isTrue();
      assertThat(((LocalBucket) embedded().getSchema().getBucketById(bucketId())).getCommittedPageCount()).isGreaterThan(2);
      final Future<?> drop = dropper.submit(() -> database.command("sql", "DROP TYPE " + ColumnarTestData.TYPE + " UNSAFE"));
      assertThat(fencing.await(HANG_MS, TimeUnit.MILLISECONDS)).isTrue();
      release.countDown();
      drop.get(HANG_MS, TimeUnit.MILLISECONDS);
    } finally {
      release.countDown();
      ColumnarTestHooks.killPoint = null;
      dropper.shutdownNow();
    }
    assertThat(pages.get()).as("pages the check read once the drop stopped it").isLessThanOrEqualTo(2);
  }

  private static void hold(final CountDownLatch reached, final CountDownLatch release) {
    reached.countDown();
    try {
      release.await();
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  // ------------------------------------------------------------------------------------------------------------------- helpers

  /** Commits with apply suspended, all pages flushed, the WAL dropped, kill, then the recovery set deleted: marks lost on no known path. */
  private int loseMarksAndKill(final long seed) throws Exception {
    final ColumnarTestData data = schemaWithData(seed, 1_000);
    final int bucketId = bucketId();
    cancelHousekeeping();
    try (final AutoCloseable hold = manager().suspendApply()) {
      data.mutate(database, 300);
      assertThat(PageManager.INSTANCE.waitAllPagesOfDatabaseAreFlushed(embedded())).isTrue();
      ColumnarTransactionManagerTestAccess.rotateAndDropWAL(embedded().getTransactionManager());
      kill();
    }
    Files.delete(recoveryFile());
    return bucketId;
  }

  /** DROP INDEX then CREATE INDEX COLUMNAR on the same bucket while a snapshot of the dropped projection is held, then its release. */
  private void recreateUnderHeldSnapshot() throws Exception {
    database.command("sql", "CREATE DOCUMENT TYPE Item BUCKETS 1");
    database.command("sql", "CREATE PROPERTY Item.brand STRING");
    database.command("sql", "CREATE INDEX ON Item (brand) COLUMNAR");
    insertItems(0, 300);
    FacetOracle.awaitCaughtUp(database);
    manager().flushNow();
    final int bucketId = database.getSchema().getType("Item").getBuckets(false).getFirst().getFileId();
    final String indexName = database.getSchema().getType("Item").getAllIndexes(false).stream()
        .filter(i -> i.getType() == Schema.INDEX_TYPE.COLUMNAR).findFirst().orElseThrow().getName();

    final ColumnarSnapshot held = manager().projection(bucketId).acquire();
    database.command("sql", "DROP INDEX `" + indexName + "`");
    database.command("sql", "CREATE INDEX ON Item (brand) COLUMNAR");
    if (ColumnarProjection.failTombstoneRenameForTesting) {
      final ColumnarProjection waiting = manager().projection(bucketId);
      assertThat(waiting == null || !waiting.isUsable()).as("no projection opens on a directory the dropped one still holds").isTrue();
    } else
      // the new projection published in the directory before the dropped one's last state is released
      FacetOracle.awaitCaughtUp(database);
    held.close();

    FacetOracle.awaitCaughtUp(database);
    final Path dir = ColumnarStore.bucketDir(Path.of(database.getDatabasePath()), bucketId);
    assertThat(dir.resolve(ColumnarManifest.FILE_NAME)).as("the release deleted the dropped projection's files only").exists();
    insertItems(300, 50);
    FacetOracle.awaitCaughtUp(database);
    manager().flushNow();
    FacetOracle.assertEquivalent(database, "Item", "brand", null);
    assertThat(tombstones(dir.getParent())).as("the tombstone is gone").isEmpty();
  }

  private static List<String> tombstones(final Path root) throws IOException {
    try (final Stream<Path> entries = Files.list(root)) {
      return entries.map(e -> e.getFileName().toString()).filter(n -> n.contains(ColumnarStore.TOMBSTONE_INFIX)).toList();
    }
  }

  private void insertItems(final int from, final int count) {
    database.transaction(() -> {
      for (int i = from; i < from + count; i++)
        database.newDocument("Item").set("brand", "b" + (i % 13)).save();
    });
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

  private void insertWithoutWAL(final ColumnarTestData data, final int count) {
    final TransactionContext transaction = embedded().getTransaction();
    try {
      transaction.setUseWAL(false);
      data.insertAssets(database, count);
    } finally {
      transaction.setUseWAL(true);
    }
  }

  private void kill() {
    ((DatabaseInternal) database).kill();
    database.close();
  }

  private void cancelHousekeeping() {
    // only the test drops WAL files
    ColumnarTransactionManagerTestAccess.cancelHousekeeping(embedded().getTransactionManager());
  }

  private void holdRebuildPermits() {
    heldPermits = ColumnarRebuilder.REBUILD_SEMAPHORE.drainPermits();
  }

  private void releaseRebuildPermits() {
    if (heldPermits < 0)
      return;
    ColumnarRebuilder.REBUILD_SEMAPHORE.release(heldPermits);
    heldPermits = -1;
    // a rebuild queued while the permits were held starts at the next apply tick
    ColumnarRebuilder.drainWaiting();
  }

  private void assertRecoveredWithoutRebuild() {
    assertEquivalent();
    assertThat(manager().getStats().rebuildsTotal()).isZero();
  }

  private void assertEquivalent() {
    for (final String property : PROPERTIES)
      FacetOracle.assertEquivalent(database, ColumnarTestData.TYPE, property, null);
  }

  private DatabaseInternal embedded() {
    return ((DatabaseInternal) database).getEmbedded();
  }

  private ColumnarIndexManager manager() {
    return ((LocalDatabase) embedded()).getColumnarIndexManager();
  }

  private int bucketId() {
    return database.getSchema().getType(ColumnarTestData.TYPE).getBuckets(false).stream().mapToInt(Bucket::getFileId).findFirst()
        .orElseThrow();
  }

  private Path recoveryFile() {
    return Path.of(database.getDatabasePath()).resolve(ColumnarStore.ROOT_DIR).resolve(RecoverySetFile.FILE_NAME);
  }

  private String[] walFiles() {
    final String[] names = new File(database.getDatabasePath()).list((dir, name) -> name.endsWith(".wal"));
    return names == null ? new String[0] : names;
  }
}
