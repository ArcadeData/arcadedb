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

import com.arcadedb.TestHelper;
import com.arcadedb.database.BucketPageLayoutTestSupport;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.ColumnarTransactionManagerTestAccess;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.engine.TransactionManager;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.vector.LSMVectorIndexTestAccess;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.IntHashSet;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** Background rebuild of a projection (spec 6.5): create, recovery reasons, coalescing, writers during the build, page reuse. */
class ColumnarRebuilderTest extends TestHelper {
  private static final String[] PROPERTIES = { "brand", "color", "size", "model", "region" };
  private static final long     WAIT_MS    = 60_000;

  @Override
  protected void endTest() {
    ColumnarTestHooks.killPoint = null;
  }

  @Test
  void createOnExistingDataBuildsInBackground() throws InterruptedException {
    createAssetType();
    insertAssets(20_000, 0);

    database.command("sql", "CREATE INDEX ON Asset (brand) COLUMNAR");

    awaitCaughtUp();
    assertMatchesGroupBy("brand");
    assertThat(stats().rebuilds("Asset", ColumnarRebuilder.CREATE)).isEqualTo(1);
    assertThat(stats().rebuilds("Asset", ColumnarProjection.MISSING)).isZero();
    assertThat(stats().rebuildsTotal()).isEqualTo(1);
  }

  @Test
  void createRequestsCoalesce() throws InterruptedException {
    createAssetType();
    insertAssets(3_000, 0);
    final int permits = ColumnarRebuilder.REBUILD_SEMAPHORE.drainPermits();
    try {
      for (final String property : PROPERTIES)
        database.command("sql", "CREATE INDEX ON Asset (" + property + ") COLUMNAR");
      // a short wait expected to time out: no permit, so nothing runs
      assertThat(manager().awaitCaughtUp(200)).isFalse();
      assertThat(stats().rebuildsTotal()).isZero();
    } finally {
      ColumnarRebuilder.REBUILD_SEMAPHORE.release(permits);
    }

    awaitCaughtUp();
    assertThat(stats().rebuilds("Asset", ColumnarRebuilder.CREATE)).isEqualTo(1);
    assertThat(stats().rebuildsTotal()).isEqualTo(1);
    for (final String property : PROPERTIES)
      assertMatchesGroupBy(property);
  }

  @Test
  void severalIndexesOnAnEmptyTypeCountNoRebuild() throws InterruptedException {
    createAssetType();
    for (final String property : PROPERTIES)
      database.command("sql", "CREATE INDEX ON Asset (" + property + ") COLUMNAR");
    awaitCaughtUp();
    // every column is published (awaitCaughtUp waits for each index), and building an empty bucket is no rebuild
    assertThat(stats().rebuilds("Asset", ColumnarRebuilder.CREATE)).isZero();
    assertThat(stats().rebuildsTotal()).isZero();

    insertAssets(2_000, 0);
    awaitCaughtUp();
    for (final String property : PROPERTIES)
      assertMatchesGroupBy(property);
    assertThat(stats().rebuildsTotal()).isZero();
  }

  @Test
  void severalIndexesOnAnEmptiedTypeCountNoRebuild() throws InterruptedException {
    // pages but no record: the emptiness the create decision reads through LocalBucket.count(), inside the database context
    createAssetType();
    insertAssets(2_000, 0);
    database.transaction(() -> database.command("sql", "DELETE FROM Asset"));
    assertThat(((LocalBucket) database.getSchema().getType("Asset").getBuckets(false).getFirst()).getCommittedPageCount()).isPositive();
    for (final String property : PROPERTIES)
      database.command("sql", "CREATE INDEX ON Asset (" + property + ") COLUMNAR");
    awaitCaughtUp();
    assertThat(stats().rebuildsTotal()).isZero();

    insertAssets(1_000, 5_000);
    awaitCaughtUp();
    for (final String property : PROPERTIES)
      assertMatchesGroupBy(property);
  }

  @Test
  void aCreateWithEveryColumnPublishedDoesNotRunUnlessItAbsorbedAnotherReason() throws InterruptedException {
    createIndexedAssets(2_000);
    final ColumnarProjection projection = manager().projection(database.getSchema().getType("Asset").getBuckets(false).getFirst()
        .getFileId());
    assertThat(stats().rebuilds("Asset", ColumnarRebuilder.CREATE)).isEqualTo(1);

    // every column is published: a create has nothing to build
    ColumnarRebuilder.schedule(manager(), projection, ColumnarRebuilder.CREATE);
    awaitRebuildEnded(projection);
    assertThat(stats().rebuildsTotal()).isEqualTo(1);

    // a check_fix coalesced into a queued create: the create stands for it, so it runs, and counts as the check_fix
    final int permits = ColumnarRebuilder.REBUILD_SEMAPHORE.drainPermits();
    try {
      ColumnarRebuilder.schedule(manager(), projection, ColumnarRebuilder.CREATE);
      ColumnarRebuilder.schedule(manager(), projection, ColumnarRebuilder.CHECK_FIX);
      assertThat(projection.rebuildRequest.get().absorbed()).isEqualTo(ColumnarRebuilder.CHECK_FIX);
    } finally {
      ColumnarRebuilder.REBUILD_SEMAPHORE.release(permits);
    }
    ColumnarRebuilder.drainWaiting();
    awaitRebuildEnded(projection);
    assertThat(stats().rebuilds("Asset", ColumnarRebuilder.CREATE)).isEqualTo(1);
    assertThat(stats().rebuilds("Asset", ColumnarRebuilder.CHECK_FIX)).isEqualTo(1);
    assertThat(stats().rebuildsTotal()).isEqualTo(2);
    awaitCaughtUp();
    assertMatchesGroupBy("brand");
  }

  @Test
  void anIndexCreatedWhileARebuildFinishesIsBuilt() throws Exception {
    createIndexedAssets(2_000);
    final ColumnarProjection projection = manager().projection(database.getSchema().getType("Asset").getBuckets(false).getFirst()
        .getFileId());
    final CountDownLatch finishing = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    holdFirst(ColumnarRebuilder.FINISHING, finishing, release);
    try {
      // a check_fix runs to its end and holds just before it leaves the running state: past every column check it makes
      ColumnarRebuilder.schedule(manager(), projection, ColumnarRebuilder.CHECK_FIX);
      assertThat(finishing.await(WAIT_MS, TimeUnit.MILLISECONDS)).isTrue();
      // the leader's CREATE INDEX meets the running rebuild: it relies on that rebuild for the new column
      database.command("sql", "CREATE INDEX ON Asset (color) COLUMNAR");
      // and the reconcile it asks for runs in the same window (else a later pass could build the column by chance): ADD_COLUMN finds
      // the rebuild running too
      final Map<Integer, ColumnDefinition[]> schema = new HashMap<>();
      schema.put(projection.bucketId(),
          new ColumnDefinition[] { columnarIndex("brand").columnDefinition(), columnarIndex("color").columnDefinition() });
      manager().reconcile(schema);
    } finally {
      release.countDown();
    }
    awaitCaughtUp();
    assertThat(projection.isPublishedWith(columnarIndex("color").getFileId())).isTrue();
    assertMatchesGroupBy("color");
  }

  @Test
  void aColumnReconciledWhileARebuildFinishesIsBuilt() throws Exception {
    createIndexedAssets(2_000);
    final ColumnarProjection projection = manager().projection(database.getSchema().getType("Asset").getBuckets(false).getFirst()
        .getFileId());
    final int brandFileId = columnarIndex("brand").getFileId();
    final int colorFileId = 999_995;
    final CountDownLatch finishing = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    holdFirst(ColumnarRebuilder.FINISHING, finishing, release);
    // held throughout: a pass would reconcile against the real schema, which has no index 999_995, and drop the column
    try (final AutoCloseable ignored = manager().suspendApply()) {
      try {
        ColumnarRebuilder.schedule(manager(), projection, ColumnarRebuilder.CHECK_FIX);
        assertThat(finishing.await(WAIT_MS, TimeUnit.MILLISECONDS)).isTrue();
        // as on a follower: a schema entry adds the column, the reconcile sees ADD_COLUMN while the rebuild is still running
        final ColumnDefinition color = ColumnarIndex.defineColumn(colorFileId, "color", Type.STRING, false);
        projection.addColumn(color);
        final Map<Integer, ColumnDefinition[]> both = new HashMap<>();
        both.put(projection.bucketId(),
            new ColumnDefinition[] { ColumnarIndex.defineColumn(brandFileId, "brand", Type.STRING, false), color });
        manager().reconcile(both);
      } finally {
        release.countDown();
      }
      for (int i = 0; i < WAIT_MS && !projection.isPublishedWith(colorFileId); i++)
        Thread.sleep(1);
      assertThat(projection.isPublishedWith(colorFileId)).as("the column added during the rebuild's tail is built").isTrue();
    }
  }

  @Test
  void aCreateThatKeepsFailingIsNotRescheduledByItsOwnFinish() throws Exception {
    createIndexedAssets(2_000);
    final ColumnarProjection projection = manager().projection(database.getSchema().getType("Asset").getBuckets(false).getFirst()
        .getFileId());
    final long rebuildsBefore = stats().rebuildsTotal();
    // the clock of the retry backoff stands still: only the finish could schedule another run here
    manager().setClockForTesting(() -> 1_000_000L);
    // every scan fails as a full disk would: the projection keeps its previous, usable state and the new column stays unpublished
    final AtomicInteger scans = new AtomicInteger();
    final CountDownLatch finished = new CountDownLatch(1);
    ColumnarTestHooks.killPoint = name -> {
      if (name.equals(ColumnarRebuilder.SCANNED)) {
        scans.incrementAndGet();
        throw new IllegalStateException("Simulated write failure of a columnar rebuild");
      }
      if (name.equals(ColumnarRebuilder.FINISHED))
        finished.countDown();
    };
    database.command("sql", "CREATE INDEX ON Asset (color) COLUMNAR");
    assertThat(finished.await(WAIT_MS, TimeUnit.MILLISECONDS)).isTrue();

    // a create the finish scheduled would be queued by now: the failed run decided on these very columns, so none is
    assertThat(projection.rebuildRequest.get()).as("the failed create reschedules nothing").isNull();
    assertThat(scans.get()).isEqualTo(1);
    assertThat(stats().rebuildsTotal()).isEqualTo(rebuildsBefore + 1);
    assertThat(projection.isUsable()).isTrue();
    assertThat(projection.isPublishedWith(columnarIndex("color").getFileId())).isFalse();

    // once the failure is gone, a new request builds the column
    ColumnarTestHooks.killPoint = null;
    ColumnarRebuilder.schedule(manager(), projection, ColumnarRebuilder.CREATE);
    awaitCaughtUp();
    assertThat(projection.isPublishedWith(columnarIndex("color").getFileId())).isTrue();
    assertMatchesGroupBy("color");
  }

  @Test
  void aFailedRebuildIsRetriedWithBackoffUntilTheFailureClears() throws Exception {
    // final review (Task 16 M1, Task 23): a failure that clears is built without an outside trigger, one attempt per backoff
    createIndexedAssets(2_000);
    final int bucketId = database.getSchema().getType("Asset").getBuckets(false).getFirst().getFileId();
    final ColumnarProjection projection = manager().projection(bucketId);
    final long[] now = { 1_000_000L };
    manager().setClockForTesting(() -> now[0]);
    final AtomicInteger scans = new AtomicInteger();
    final AtomicBoolean failing = new AtomicBoolean(true);
    ColumnarTestHooks.killPoint = name -> {
      if (name.equals(ColumnarRebuilder.SCANNED)) {
        scans.incrementAndGet();
        if (failing.get())
          throw new IllegalStateException("Simulated write failure of a columnar rebuild");
      }
    };
    database.command("sql", "CREATE INDEX ON Asset (color) COLUMNAR");
    awaitRebuildRuns(projection, scans, 1);
    assertThat(manager().rebuildBackoffMsForTesting(bucketId)).isEqualTo(ColumnarIndexManager.REBUILD_RETRY_BASE_MS);

    // passes run all along: none retries before the backoff elapsed
    manager().scheduleMaintenanceForTesting();
    assertThat(projection.rebuildRequest.get()).isNull();
    assertThat(scans.get()).isEqualTo(1);

    now[0] += ColumnarIndexManager.REBUILD_RETRY_BASE_MS;
    manager().scheduleMaintenanceForTesting();
    awaitRebuildRuns(projection, scans, 2);
    assertThat(manager().rebuildBackoffMsForTesting(bucketId)).as("doubled").isEqualTo(2 * ColumnarIndexManager.REBUILD_RETRY_BASE_MS);

    failing.set(false);
    now[0] += 2 * ColumnarIndexManager.REBUILD_RETRY_BASE_MS;
    manager().scheduleMaintenanceForTesting();
    awaitRebuildRuns(projection, scans, 3);
    ColumnarTestHooks.killPoint = null;
    awaitCaughtUp();
    assertThat(projection.isPublishedWith(columnarIndex("color").getFileId())).isTrue();
    assertThat(manager().rebuildBackoffMsForTesting(bucketId)).as("a published rebuild ends the backoff").isZero();
    assertMatchesGroupBy("color");
  }

  @Test
  void aFailedRebuildOfAnUncheckedProjectionWithoutCheckMarkerMovesTheObligationToTheRebuildMarker() throws Exception {
    // batch 2 N3: an unclean check whose CHECK marker cannot be written, and whose rebuild fails, must not keep the WAL for the session
    createIndexedAssets(500);
    final ColumnarProjection projection = manager().projection(database.getSchema().getType("Asset").getBuckets(false).getFirst()
        .getFileId());
    manager().setClockForTesting(() -> 1_000_000L);
    final Path bucketDir = projection.bucketDir();
    Files.deleteIfExists(bucketDir.resolve(ColumnarStore.CHECK_MARKER));
    projection.markUnusableWithoutRequest(ColumnarProjection.UNCLEAN_CHECK);
    final Path rebuildMarker = Path.of(database.getDatabasePath()).resolve(ColumnarStore.ROOT_DIR).resolve(ColumnarStore.REBUILD_MARKER);
    assertThat(bucketDir.toFile().setWritable(false)).isTrue();
    try {
      final CountDownLatch finished = new CountDownLatch(1);
      ColumnarTestHooks.killPoint = name -> {
        if (name.equals(ColumnarRebuilder.FINISHED))
          finished.countDown();
      };
      ColumnarRebuilder.schedule(manager(), projection, ColumnarRebuilder.SCHEMA);
      assertThat(finished.await(WAIT_MS, TimeUnit.MILLISECONDS)).isTrue();

      assertThat(projection.unusableReason()).isEqualTo(ColumnarProjection.CORRUPT);
      assertThat(rebuildMarker).exists();
      assertThat(manager().persistRecoverySet()).as("the WAL may be dropped again").isTrue();
    } finally {
      ColumnarTestHooks.killPoint = null;
      bucketDir.toFile().setWritable(true);
    }

    // the retry publishes and clears the marker
    manager().setClockForTesting(System::currentTimeMillis);
    awaitCaughtUp();
    assertThat(rebuildMarker).doesNotExist();
    assertMatchesGroupBy("brand");
  }

  @Test
  void buildConcurrentWithWritesConverges() throws Exception {
    createAssetType();
    insertAssets(5_000, 0);
    final List<RID> existing = new ArrayList<>();
    try (final ResultSet result = database.query("sql", "SELECT @rid AS rid FROM Asset")) {
      while (result.hasNext())
        existing.add(result.next().getProperty("rid"));
    }
    final AtomicBoolean stop = new AtomicBoolean();
    final AtomicInteger commits = new AtomicInteger();
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final List<Thread> writers = new ArrayList<>();
    for (int w = 0; w < 4; w++) {
      final int writer = w;
      final Thread thread = new Thread(() -> write(writer, existing, stop, commits, failure), "columnar-writer-" + w);
      writers.add(thread);
      thread.start();
    }
    // the writers commit during the scan; after it the rebuild waits until they committed some more, stops them and lets the apply
    // pass take their pages into the OLD state: only the re-mark of the publish can bring those writes into the rebuilt one
    final CountDownLatch scanned = new CountDownLatch(1);
    final AtomicBoolean appliedBeforePublish = new AtomicBoolean();
    ColumnarTestHooks.killPoint = point -> {
      if (!point.equals(ColumnarRebuilder.SCANNED))
        return;
      scanned.countDown();
      final int target = commits.get() + 200;
      for (int i = 0; i < 30_000 && commits.get() < target && failure.get() == null; i++)
        sleep(1);
      stop.set(true);
      for (final Thread thread : writers)
        try {
          thread.join(WAIT_MS);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      final DirtyPageTracker tracker = manager().projection(bucketIds()[0]).tracker();
      for (int i = 0; i < 30_000 && tracker.oldestPendingEpochStartMs() != 0; i++)
        sleep(1);
      appliedBeforePublish.set(tracker.oldestPendingEpochStartMs() == 0);
    };
    try {
      database.command("sql", "CREATE INDEX ON Asset (brand) COLUMNAR");
      assertThat(scanned.await(WAIT_MS, TimeUnit.MILLISECONDS)).isTrue();
      awaitCaughtUp();
    } finally {
      stop.set(true);
      for (final Thread thread : writers)
        thread.join(WAIT_MS);
      ColumnarTestHooks.killPoint = null;
    }
    assertThat(appliedBeforePublish.get()).isTrue();
    assertThat(commits.get()).isGreaterThanOrEqualTo(200);
    assertThat(failure.get()).isNull();

    awaitCaughtUp();
    assertMatchesGroupBy("brand");
    assertThat(stats().rebuilds("Asset", ColumnarRebuilder.CREATE)).isEqualTo(1);
  }

  @Test
  void missingProjectionIsRebuiltAndCounted() throws InterruptedException {
    createIndexedAssets(3_000);
    final int bucketId = bucketIds()[0];

    database.close();
    FileUtils.deleteRecursively(bucketDir(bucketId).toFile());
    database = factory.open();

    awaitCaughtUp();
    assertThat(stats().rebuilds("Asset", ColumnarProjection.MISSING)).isEqualTo(1);
    assertThat(stats().rebuildsTotal()).isEqualTo(1);
    assertMatchesGroupBy("brand");
  }

  @Test
  void corruptSegmentIsRebuiltAsCorrupt() throws Exception {
    createIndexedAssets(3_000);
    manager().flushNow();
    final int bucketId = bucketIds()[0];
    final Path segment;
    try (final Stream<Path> files = Files.list(bucketDir(bucketId))) {
      segment = files.filter(f -> f.getFileName().toString().endsWith(".acs")).findFirst().orElseThrow();
    }

    database.close();
    // a byte of the footer: its CRC fails
    flipByte(segment, Files.size(segment) - 20);
    database = factory.open();

    awaitCaughtUp();
    assertThat(stats().rebuilds("Asset", ColumnarProjection.CORRUPT)).isEqualTo(1);
    assertThat(stats().rebuilds("Asset", ColumnarProjection.MISSING)).isZero();
    assertMatchesGroupBy("brand");
  }

  @Test
  void corruptManifestIsRebuiltAsCorrupt() throws InterruptedException, IOException {
    createIndexedAssets(3_000);
    final int bucketId = bucketIds()[0];

    database.close();
    Files.writeString(bucketDir(bucketId).resolve(ColumnarManifest.FILE_NAME), "{not json");
    database = factory.open();

    awaitCaughtUp();
    assertThat(stats().rebuilds("Asset", ColumnarProjection.CORRUPT)).isEqualTo(1);
    assertThat(stats().rebuildsTotal()).isEqualTo(1);
    assertMatchesGroupBy("brand");
  }

  @Test
  void corruptDictionaryIsRebuiltAsCorrupt() throws Exception {
    createIndexedAssets(3_000);
    manager().flushNow();
    final int bucketId = bucketIds()[0];
    final ColumnarManifest.DictionaryEntry entry = manager().projection(bucketId).manifest().dictionaries().getFirst();
    assertThat(entry.bytes()).isGreaterThan(8);

    database.close();
    try (final FileChannel channel = FileChannel.open(bucketDir(bucketId).resolve(entry.file()), StandardOpenOption.WRITE)) {
      channel.truncate(entry.bytes() - 3);
    }
    database = factory.open();

    awaitCaughtUp();
    assertThat(stats().rebuilds("Asset", ColumnarProjection.CORRUPT)).isEqualTo(1);
    assertThat(stats().rebuilds("Asset", ColumnarProjection.MISSING)).isZero();
    assertMatchesGroupBy("brand");
  }

  @Test
  void noWalMarkerRebuildsUncleanNoWal() throws InterruptedException, IOException {
    createIndexedAssets(3_000);
    manager().flushNow();
    final int bucketId = bucketIds()[0];
    final Path marker = bucketDir(bucketId).resolve(ColumnarStore.NOWAL_MARKER);

    // R8 with the Task 13 ruling: a NOWAL marker means "unclean" only on an unclean open, so the marker is left by a crash
    Files.write(marker, new byte[0]);
    ((DatabaseInternal) database).kill();
    database.close();
    database = factory.open();

    awaitCaughtUp();
    assertThat(stats().rebuilds("Asset", ColumnarRebuilder.UNCLEAN_NO_WAL)).isEqualTo(1);
    assertThat(stats().rebuildsTotal()).isEqualTo(1);
    assertThat(marker).doesNotExist();
    assertMatchesGroupBy("brand");
  }

  @Test
  void strayNoWalMarkerOnACleanOpenIsNoRebuild() throws InterruptedException, IOException {
    createIndexedAssets(1_000);
    final Path marker = bucketDir(bucketIds()[0]).resolve(ColumnarStore.NOWAL_MARKER);
    final ColumnarIndexManager closing = manager();

    // a no-WAL commit landing after the columnar close keeps the marker; the database close flushes every page anyway
    closing.close();
    closing.markPagesWithoutWAL(bucketIds()[0], new int[] { 0 }, 1);
    database.close();
    assertThat(marker).exists();
    database = factory.open();

    awaitCaughtUp();
    assertThat(stats().rebuildsTotal()).isZero();
    assertThat(marker).doesNotExist();
    assertMatchesGroupBy("brand");
  }

  @Test
  void snapshotInstallMarkerRebuildsWithReason() throws InterruptedException, IOException {
    createIndexedAssets(3_000);
    final Path databasePath = Path.of(database.getDatabasePath());

    database.close();
    ColumnarStore.prepareForSnapshotInstall(databasePath);
    database = factory.open();

    awaitCaughtUp();
    assertThat(stats().rebuilds("Asset", ColumnarRebuilder.SNAPSHOT_INSTALL)).isEqualTo(1);
    assertThat(stats().rebuildsTotal()).isEqualTo(1);
    for (int i = 0; i < WAIT_MS && ColumnarStore.readRebuildReason(databasePath) != null; i++)
      Thread.sleep(1);
    assertThat(ColumnarStore.readRebuildReason(databasePath)).isNull();
    assertMatchesGroupBy("brand");
  }

  @Test
  void rebuildUsesItsOwnSemaphore() throws InterruptedException {
    createAssetType();
    insertAssets(2_000, 0);
    final Semaphore vector = LSMVectorIndexTestAccess.rebuildSemaphore();
    final int held = vector.drainPermits();
    try {
      database.command("sql", "CREATE INDEX ON Asset (brand) COLUMNAR");
      awaitCaughtUp();
    } finally {
      vector.release(held);
    }
    assertThat(stats().rebuilds("Asset", ColumnarRebuilder.CREATE)).isEqualTo(1);
    assertMatchesGroupBy("brand");
  }

  @Test
  void unchangedPagesAreReusedOnCheckFixRebuild() throws Exception {
    createIndexedAssets(6_000);
    manager().flushNow();
    final int bucketId = bucketIds()[0];
    final ColumnarProjection projection = manager().projection(bucketId);
    final int pages = pagesWithRecords(bucketId);
    assertThat(pages).isGreaterThan(1);

    rebuildAndWait(projection, ColumnarRebuilder.CHECK_FIX, 1);
    awaitCaughtUp();
    assertThat(stats().rebuildPagesReusedCount("Asset")).isEqualTo(pages);
    assertMatchesGroupBy("brand");

    // a record of page 0 changed: applied into the overlay, so its page is extracted again and the others reused
    database.transaction(() -> {
      final MutableDocument first = database.lookupByRID(new RID(bucketId, 0), true).asDocument().modify();
      first.set("brand", "changed").save();
    });
    awaitCaughtUp();
    rebuildAndWait(projection, ColumnarRebuilder.CHECK_FIX, 2);
    awaitCaughtUp();
    assertThat(stats().rebuildPagesReusedCount("Asset")).isEqualTo(pages + pages - 1L);
    assertThat(projection.isUsable()).isTrue();
    assertMatchesGroupBy("brand");

    // a change the apply pass did not take yet: the page's version moved past the one its segment recorded, so the rebuild reads it
    try (final AutoCloseable ignored = manager().suspendApply()) {
      database.transaction(() -> database.lookupByRID(new RID(bucketId, 1), true).asDocument().modify().set("brand", "pending").save());
      rebuildAndWait(projection, ColumnarRebuilder.CHECK_FIX, 3);
      assertThat(stats().rebuildPagesReusedCount("Asset")).isEqualTo(3L * pages - 2);
      assertThat(segmentValue(projection, 1, "brand")).isEqualTo("pending");
    }
    awaitCaughtUp();
    assertMatchesGroupBy("brand");
  }

  @Test
  void aPlaceholderRecordIsNeverCopiedFromThePreviousState() throws Exception {
    createAssetType();
    final RID[] owner = new RID[1];
    database.transaction(() -> owner[0] = database.newDocument("Asset").set("brand", "p1").save().getIdentity());
    // a placeholder: the pointer stays on page 0, the content lives on another page, which an update rewrites alone
    BucketPageLayoutTestSupport.sealFirstPage(database, "Asset");
    database.transaction(() -> owner[0].asDocument(true).modify().set("v", "b".repeat(20 * 1024)).save());
    assertThat((Long) BucketPageLayoutTestSupport.bucketStats(database, "Asset").get("totalPlaceholderRecords")).isEqualTo(1L);
    database.command("sql", "CREATE INDEX ON Asset (brand) COLUMNAR");
    awaitCaughtUp();
    manager().flushNow();
    final ColumnarProjection projection = manager().projection(bucketIds()[0]);
    rebuildAndWait(projection, ColumnarRebuilder.CHECK_FIX, 1);
    awaitCaughtUp();
    final long pointerPageVersion = pageVersion(0);

    try (final AutoCloseable ignored = manager().suspendApply()) {
      database.transaction(() -> owner[0].asDocument(true).modify().set("brand", "p2").save());
      // the fixture holds only when the pointer page did not move: then only the owner rule keeps the stale value out
      assertThat(pageVersion(0)).isEqualTo(pointerPageVersion);
      rebuildAndWait(projection, ColumnarRebuilder.CHECK_FIX, 2);
      assertThat(segmentValue(projection, owner[0].getPosition(), "brand")).isEqualTo("p2");
    }
    awaitCaughtUp();
    assertMatchesGroupBy("brand");
  }

  /** The version of the committed page, as the rebuild scan reads it. */
  private long pageVersion(final int page) {
    return ((LocalBucket) database.getSchema().getBucketById(bucketIds()[0])).visitCommittedPage(page, (position, kind, content, pages, count) -> {
    });
  }

  @Test
  void aColumnAddedToAUsableProjectionCountsItsConversionFailuresOnce() throws Exception {
    createAssetType();
    // planted while the property is not in the schema: a DECIMAL a LONG column cannot hold
    database.transaction(() -> database.newDocument("Asset").set("brand", "a").set("fileSize", new BigDecimal("1.5")).save());
    insertAssets(100, 0);
    database.command("sql", "CREATE INDEX ON Asset (brand) COLUMNAR");
    awaitCaughtUp();
    database.getSchema().getType("Asset").createProperty("fileSize", Type.LONG);

    // the projection is usable already: the create rebuild of the new column is the one that reports its failing value
    database.command("sql", "CREATE INDEX ON Asset (fileSize) COLUMNAR");
    awaitCaughtUp();
    assertThat(manager().conversionFailures("Asset", "fileSize")).isEqualTo(1);
    assertThat(manager().conversionFailures("Asset", "brand")).isZero();

    // re-reads of the unchanged records count nothing
    final ColumnarProjection projection = manager().projection(bucketIds()[0]);
    for (int pass = 0; pass < 3; pass++) {
      manager().markPages(projection.bucketId(), new int[] { 0 }, 1);
      awaitCaughtUp();
    }
    assertThat(manager().conversionFailures("Asset", "fileSize")).isEqualTo(1);
  }

  @Test
  void anIndexAddedToAUsableProjectionIsNotUsableBeforeItsColumnPublishes() throws InterruptedException {
    createIndexedAssets(1_000);
    final ColumnarIndex brand = subIndex("Asset[brand]_columnar");
    assertThat(brand.isUsable()).isTrue();

    final int permits = ColumnarRebuilder.REBUILD_SEMAPHORE.drainPermits();
    final ColumnarIndex color;
    try {
      database.command("sql", "CREATE INDEX ON Asset (color) COLUMNAR");
      color = subIndex("Asset[color]_columnar");
      // the projection serves brand, but has no color column until the create rebuild publishes it
      assertThat(manager().projection(bucketIds()[0]).isUsable()).isTrue();
      assertThat(brand.isUsable()).isTrue();
      assertThat(color.isUsable()).isFalse();
      // a short wait expected to time out: the pending column is not caught up
      assertThat(manager().awaitCaughtUp(200)).isFalse();
    } finally {
      ColumnarRebuilder.REBUILD_SEMAPHORE.release(permits);
    }

    awaitCaughtUp();
    assertThat(color.isUsable()).isTrue();
    assertMatchesGroupBy("color");
  }

  private ColumnarIndex subIndex(final String name) {
    return (ColumnarIndex) ((TypeIndex) database.getSchema().getIndexByName(name)).getSubIndexes().getFirst();
  }

  /** The value the segments of the current state hold for the position, null when no live row holds it. */
  private static Object segmentValue(final ColumnarProjection projection, final long position, final String property) {
    final ProjectionState state = projection.retainState();
    try {
      final ColumnDefinition[] columns = projection.columns();
      int k = 0;
      while (!columns[k].property().equals(property))
        k++;
      final RowValues row = new RowValues(columns.length);
      final SegmentReader[] segments = state.segments();
      for (int i = segments.length - 1; i >= 0; i--) {
        if (segments[i].row(position) < 0 || segments[i].deadSet() != null && segments[i].deadSet().get(position))
          continue;
        ColumnarProjection.readSegmentValue(segments[i], position, columns[k], k, row);
        return row.present[k] ? state.dictionary(property).valueOf(row.ordinal[k]) : null;
      }
      return null;
    } finally {
      state.release();
    }
  }

  @Test
  void cleanReopenDoesNotRebuild() throws InterruptedException {
    createIndexedAssets(2_000);
    assertThat(stats().rebuildsTotal()).isEqualTo(1);

    reopenDatabase();

    awaitCaughtUp();
    assertThat(stats().rebuildsTotal()).isZero();
    assertMatchesGroupBy("brand");
  }

  @Test
  void stagedDictionaryIsAGenerationFile() throws InterruptedException, IOException {
    createIndexedAssets(2_000);
    final int bucketId = bucketIds()[0];
    final int indexFileId = manager().projection(bucketId).columns()[0].indexFileId();

    database.close();
    FileUtils.deleteRecursively(bucketDir(bucketId).toFile());
    database = factory.open();
    awaitCaughtUp();
    assertThat(stats().rebuilds("Asset", ColumnarProjection.MISSING)).isEqualTo(1);

    final ColumnarManifest manifest = manager().projection(bucketId).manifest();
    assertThat(manifest.dictionaries()).hasSize(1);
    final ColumnarManifest.DictionaryEntry entry = manifest.dictionaries().getFirst();
    assertThat(entry.indexFileId()).isEqualTo(indexFileId);
    assertThat(entry.file()).isEqualTo(SegmentFormat.dictionaryFileName(indexFileId, entry.generation()));
    assertThat(entry.entries()).isEqualTo(13);
    assertThat(Files.size(bucketDir(bucketId).resolve(entry.file()))).isEqualTo(entry.bytes());
  }

  @Test
  void housekeepingPersistsBeforeDrop() throws Exception {
    createIndexedAssets(1_000);
    manager().flushNow();
    final int bucketId = bucketIds()[0];
    final Path recoveryFile = Path.of(database.getDatabasePath()).resolve(ColumnarStore.ROOT_DIR).resolve(RecoverySetFile.FILE_NAME);
    final TransactionManager transactionManager = ((DatabaseInternal) database).getTransactionManager();
    final Set<Path> walFiles = walFiles();
    assertThat(walFiles).isNotEmpty();

    try (final AutoCloseable ignored = manager().suspendApply()) {
      insertAssets(500, 1_000);
      final Set<Integer> pending = new HashSet<>();
      final IntHashSet collected = new IntHashSet();
      manager().projection(bucketId).tracker().collectRecoverySet(collected);
      for (final int page : collected.toArray())
        pending.add(page);
      assertThat(pending).isNotEmpty();

      // a recovery set that cannot be written aborts the drop: the WAL files stay
      ColumnarTestHooks.killPoint = point -> {
        if (point.equals(ColumnarIndexManager.RECOVERY_SET_PERSISTED))
          throw new IllegalStateException("injected recovery set failure");
      };
      ColumnarTransactionManagerTestAccess.rotateAndDropWAL(transactionManager);
      for (final Path wal : walFiles)
        assertThat(wal).exists();

      // the two-argument pass (no data fsync) persists before it drops: at the persist point every WAL file still exists
      final AtomicReference<Map<Integer, int[]>> persisted = new AtomicReference<>();
      final AtomicBoolean walPresentAtPersist = new AtomicBoolean();
      ColumnarTestHooks.killPoint = point -> {
        if (!point.equals(ColumnarIndexManager.RECOVERY_SET_PERSISTED))
          return;
        try {
          persisted.set(RecoverySetFile.read(recoveryFile));
        } catch (final IOException e) {
          throw new IllegalStateException(e);
        }
        walPresentAtPersist.set(walFiles.stream().allMatch(Files::exists));
      };
      assertThat(ColumnarTransactionManagerTestAccess.cleanWALFiles(transactionManager, true, true)).isTrue();
      ColumnarTestHooks.killPoint = null;

      assertThat(walPresentAtPersist.get()).isTrue();
      assertThat(persisted.get()).containsKey(bucketId);
      final Set<Integer> written = new HashSet<>();
      for (final int page : persisted.get().get(bucketId))
        written.add(page);
      assertThat(written).containsAll(pending);
      for (final Path wal : walFiles)
        assertThat(wal).doesNotExist();
    }
  }

  // ---------------------------------------------------------------------------------------------------------------------- helpers

  private void write(final int writer, final List<RID> existing, final AtomicBoolean stop, final AtomicInteger commits,
      final AtomicReference<Throwable> failure) {
    int i = 0;
    while (!stop.get() && failure.get() == null)
      try {
        final int n = i++;
        database.transaction(() -> {
          if (n % 2 == 0)
            database.newDocument("Asset").set("brand", "w" + (n % 7)).save();
          else {
            final MutableDocument updated = database.lookupByRID(existing.get((writer * 997 + n) % existing.size()), true).asDocument()
                .modify();
            updated.set("brand", "u" + (n % 5)).save();
          }
        });
        commits.incrementAndGet();
      } catch (final NeedRetryException e) {
        // a concurrent writer took the page: the next iteration goes on
      } catch (final Throwable e) {
        failure.compareAndSet(null, e);
      }
  }

  private static void sleep(final long ms) {
    try {
      Thread.sleep(ms);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private void rebuildAndWait(final ColumnarProjection projection, final String reason, final long expectedRuns) throws InterruptedException {
    ColumnarRebuilder.schedule(manager(), projection, reason);
    for (int i = 0; i < WAIT_MS && (stats().rebuilds("Asset", reason) < expectedRuns || projection.rebuildRequest.get() != null); i++)
      Thread.sleep(1);
    assertThat(stats().rebuilds("Asset", reason)).isEqualTo(expectedRuns);
    assertThat(projection.rebuildRequest.get()).isNull();
  }

  private ColumnarIndex columnarIndex(final String property) {
    return (ColumnarIndex) database.getSchema().getType("Asset").getAllIndexes(false).stream()
        .filter(i -> i.getType() == Schema.INDEX_TYPE.COLUMNAR && i.getPropertyNames().contains(property)).findFirst().orElseThrow()
        .getIndexesOnBuckets()[0];
  }

  /** Blocks the first thread reaching {@code point} until {@code release}, counting {@code reached} down when it arrives. */
  private static void holdFirst(final String point, final CountDownLatch reached, final CountDownLatch release) {
    final AtomicBoolean first = new AtomicBoolean(true);
    ColumnarTestHooks.killPoint = name -> {
      if (name.equals(point) && first.compareAndSet(true, false)) {
        reached.countDown();
        try {
          release.await(WAIT_MS, TimeUnit.MILLISECONDS);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    };
  }

  /** Waits until {@code runs} scans happened and no rebuild of the projection is queued or running. */
  private static void awaitRebuildRuns(final ColumnarProjection projection, final AtomicInteger scans, final int runs)
      throws InterruptedException {
    for (int i = 0; i < WAIT_MS && (scans.get() < runs || projection.rebuildRequest.get() != null); i++)
      Thread.sleep(1);
    assertThat(scans.get()).isEqualTo(runs);
    assertThat(projection.rebuildRequest.get()).isNull();
  }

  private static void awaitRebuildEnded(final ColumnarProjection projection) throws InterruptedException {
    for (int i = 0; i < WAIT_MS && projection.rebuildRequest.get() != null; i++)
      Thread.sleep(1);
    assertThat(projection.rebuildRequest.get()).isNull();
  }

  private void createAssetType() {
    database.command("sql", "CREATE DOCUMENT TYPE Asset BUCKETS 1");
    for (final String property : PROPERTIES)
      database.command("sql", "CREATE PROPERTY Asset." + property + " STRING");
  }

  private void createIndexedAssets(final int count) throws InterruptedException {
    createAssetType();
    insertAssets(count, 0);
    database.command("sql", "CREATE INDEX ON Asset (brand) COLUMNAR");
    awaitCaughtUp();
  }

  private void insertAssets(final int count, final int offset) {
    for (int start = 0; start < count; start += 1_000) {
      final int from = start;
      final int to = Math.min(count, start + 1_000);
      database.transaction(() -> {
        for (int i = from; i < to; i++) {
          final int n = i + offset;
          final MutableDocument asset = database.newDocument("Asset").set("brand", "b" + (n % 13)).set("size", "s" + (n % 4))
              .set("model", "m" + (n % 29)).set("region", "r" + (n % 3));
          if (n % 11 != 0)
            asset.set("color", "c" + (n % 6));
          asset.save();
        }
      });
    }
  }

  private void awaitCaughtUp() throws InterruptedException {
    assertThat(manager().awaitCaughtUp(WAIT_MS)).isTrue();
  }

  /** The GROUP BY of the property equals what the projections answer (the null group left out: a missing value is not counted). */
  private void assertMatchesGroupBy(final String property) {
    final Map<Object, Long> expected = new HashMap<>();
    try (final ResultSet result = database.query("sql", "SELECT " + property + " AS v, count(*) AS n FROM Asset GROUP BY " + property)) {
      while (result.hasNext()) {
        final Result row = result.next();
        final Object value = row.getProperty("v");
        if (value != null)
          expected.put(value, ((Number) row.getProperty("n")).longValue());
      }
    }
    assertThat(expected).isNotEmpty();
    assertThat(projectedCounts(property)).isEqualTo(expected);
  }

  private Map<Object, Long> projectedCounts(final String property) {
    final Map<Object, Long> counts = new HashMap<>();
    for (final int bucketId : bucketIds())
      try (final ColumnarSnapshot snapshot = manager().projection(bucketId).acquire()) {
        assertThat(snapshot.isUsable()).isTrue();
        assertThat(snapshot.pendingPositions().isEmpty()).isTrue();
        final GlobalDictionary dictionary = snapshot.dictionary(property);
        for (final ColumnarSegmentView view : snapshot.segments()) {
          final ColumnReader column = view.column(property);
          if (column != null)
            column.forEachOrdinal(view.live(), (position, ordinal) -> counts.merge(dictionary.valueOf(ordinal), 1L, Long::sum));
        }
      }
    return counts;
  }

  private int pagesWithRecords(final int bucketId) {
    final int maxRecordsInPage = ((LocalBucket) database.getSchema().getBucketById(bucketId)).getMaxRecordsInPage();
    final Set<Long> pages = new HashSet<>();
    try (final ResultSet result = database.query("sql", "SELECT @rid AS rid FROM Asset")) {
      while (result.hasNext())
        pages.add(((RID) result.next().getProperty("rid")).getPosition() / maxRecordsInPage);
    }
    return pages.size();
  }

  private Set<Path> walFiles() throws IOException {
    try (final Stream<Path> files = Files.list(Path.of(database.getDatabasePath()))) {
      return new HashSet<>(files.filter(f -> f.getFileName().toString().endsWith(".wal")).toList());
    }
  }

  private static void flipByte(final Path file, final long offset) throws IOException {
    try (final FileChannel channel = FileChannel.open(file, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
      final ByteBuffer one = ByteBuffer.allocate(1);
      channel.read(one, offset);
      one.put(0, (byte) (one.get(0) ^ 0x5A));
      one.position(0);
      channel.write(one, offset);
    }
  }

  private ColumnarIndexManager manager() {
    return ((LocalDatabase) ((DatabaseInternal) database).getEmbedded()).getColumnarIndexManager();
  }

  private ColumnarStats stats() {
    return manager().getStats();
  }

  private int[] bucketIds() {
    return database.getSchema().getType("Asset").getBuckets(false).stream().mapToInt(Bucket::getFileId).toArray();
  }

  private Path bucketDir(final int bucketId) {
    return ColumnarStore.bucketDir(Path.of(database.getDatabasePath()), bucketId);
  }
}
