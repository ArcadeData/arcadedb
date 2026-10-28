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
import com.arcadedb.engine.PageManager;
import com.arcadedb.utility.CallableNoReturn;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
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
