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
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.engine.block.BlockCorruptionException;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.utility.IntHashSet;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Lifecycle of the per-database {@link ColumnarIndexManager}: projections per bucket, applying, maintenance and close. */
class ColumnarIndexManagerLifecycleTest extends TestHelper {
  private static final String INDEX = "Asset[brand]_columnar";

  @Override
  protected void endTest() {
    ColumnarTestHooks.killPoint = null;
  }

  @Test
  void managerExistsForEveryOpenDatabase() {
    final ColumnarIndexManager created = manager();
    assertThat(created).isNotNull();
    assertThat(created.getDatabase()).isSameAs(((DatabaseInternal) database).getEmbedded());
    // a new database is applying too: no recovery ran, the open still starts the tick
    assertThat(created.isApplyingForTesting()).isTrue();

    reopenDatabase();

    assertThat(manager()).isNotNull().isNotSameAs(created);
    assertThat(created.isApplyingForTesting()).isFalse();
  }

  @Test
  void fallbackScanPermitsComeFromTheSettingAtConstruction() {
    assertThat(manager().fallbackScans().availablePermits()).isEqualTo(2);

    final int original = GlobalConfiguration.COLUMNAR_MAX_FALLBACK_SCANS.getValueAsInteger();
    GlobalConfiguration.COLUMNAR_MAX_FALLBACK_SCANS.setValue(5);
    try {
      reopenDatabase();
      assertThat(manager().fallbackScans().availablePermits()).isEqualTo(5);
    } finally {
      GlobalConfiguration.COLUMNAR_MAX_FALLBACK_SCANS.setValue(original);
    }
  }

  @Test
  void aDatabaseWithoutColumnarIndexesRegistersNoTick() {
    // final review C-M1: nothing columnar, no ticker wake-up and no configuration read
    final ColumnarIndexManager manager = manager();
    assertThat(manager.isApplyingForTesting()).isTrue();
    assertThat(manager.isTickRegisteredForTesting()).isFalse();
    database.getConfiguration().setValue(GlobalConfiguration.COLUMNAR_APPLY_INTERVAL, 12_345L);
    try {
      assertThat(manager.tickIntervalForTesting().getAsLong()).as("the idle interval reads no configuration").isZero();
      database.getConfiguration().setValue(GlobalConfiguration.COLUMNAR_APPLY_INTERVAL, 100L);

      createAsset(1);

      assertThat(manager.isTickRegisteredForTesting()).isTrue();
      database.getConfiguration().setValue(GlobalConfiguration.COLUMNAR_APPLY_INTERVAL, 12_345L);
      assertThat(manager.tickIntervalForTesting().getAsLong()).isEqualTo(12_345L);
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.COLUMNAR_APPLY_INTERVAL, 100L);
    }

    // a reopen registers the tick at once: the database has a projection
    reopenDatabase();
    assertThat(manager().isTickRegisteredForTesting()).isTrue();
  }

  @Test
  void applyPacingStretchesWhilePassesLeaveMarksBehind() {
    final ColumnarProjection projection = suspendedProjection();
    final ColumnarIndexManager manager = manager();
    final long now = System.currentTimeMillis();
    manager.paceAfterPassForTesting(now);
    assertThat(manager.applyPacingForTesting()).as("nothing pending").isEqualTo(1);

    manager.markPages(projection.bucketId(), new int[] { 0 }, 1);
    final int[] factors = new int[4];
    for (int i = 0; i < factors.length; i++) {
      manager.paceAfterPassForTesting(now);
      factors[i] = manager.applyPacingForTesting();
    }
    assertThat(factors).containsExactly(2, 4, ColumnarIndexManager.APPLY_PACING_MAX_FACTOR, ColumnarIndexManager.APPLY_PACING_MAX_FACTOR);
    assertThat(manager.tickIntervalForTesting().getAsLong()).isEqualTo(100L * ColumnarIndexManager.APPLY_PACING_MAX_FACTOR);

    // the oldest mark waits longer than the lag limit: back to the base interval, however sustained the ingest
    manager.paceAfterPassForTesting(now + ColumnarIndexManager.APPLY_PACING_LAG_LIMIT_MS + 60_000L);
    assertThat(manager.applyPacingForTesting()).isEqualTo(1);
    assertThat(manager.tickIntervalForTesting().getAsLong()).isEqualTo(100L);
  }

  @Test
  void applyPacingNeverStretchesPastTheLagLimitOrTheBase() {
    // batch 3 r1 M1: the stretched interval is capped at max(base, 500 ms) whatever the configured interval
    final ColumnarProjection projection = suspendedProjection();
    final ColumnarIndexManager manager = manager();
    final long now = System.currentTimeMillis();
    manager.markPages(projection.bucketId(), new int[] { 0 }, 1);
    for (int i = 0; i < 4; i++)
      manager.paceAfterPassForTesting(now);
    assertThat(manager.applyPacingForTesting()).isEqualTo(ColumnarIndexManager.APPLY_PACING_MAX_FACTOR);
    try {
      database.getConfiguration().setValue(GlobalConfiguration.COLUMNAR_APPLY_INTERVAL, 1_000L);
      assertThat(manager.tickIntervalForTesting().getAsLong()).as("a base above the limit is never stretched").isEqualTo(1_000L);
      database.getConfiguration().setValue(GlobalConfiguration.COLUMNAR_APPLY_INTERVAL, 200L);
      assertThat(manager.tickIntervalForTesting().getAsLong()).isEqualTo(ColumnarIndexManager.APPLY_PACING_LAG_LIMIT_MS);
      database.getConfiguration().setValue(GlobalConfiguration.COLUMNAR_APPLY_INTERVAL, 50L);
      assertThat(manager.tickIntervalForTesting().getAsLong()).isEqualTo(50L * ColumnarIndexManager.APPLY_PACING_MAX_FACTOR);
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.COLUMNAR_APPLY_INTERVAL, 100L);
    }
  }

  @Test
  void aGrownDictionaryGetsItsRankViewRebuiltThrottled() throws InterruptedException {
    // plan 3 rank view: rebuilt by the maintenance once more than max(1024, view / 8) values are newer than it, at most every interval
    final ColumnarProjection projection = suspendedProjection();
    final ColumnarIndexManager manager = manager();
    final long[] now = { 1_000_000L };
    manager.setClockForTesting(() -> now[0]);
    final GlobalDictionary dictionary = projection.dictionary("brand");

    for (int i = 0; i < 1_000; i++)
      dictionary.getOrAdd("v" + i);
    scheduleAndWait(manager);
    assertThat(dictionary.rankViewSize()).as("below the threshold").isZero();

    for (int i = 1_000; i < 1_100; i++)
      dictionary.getOrAdd("v" + i);
    scheduleAndWait(manager);
    assertThat(dictionary.rankViewSize()).isEqualTo(1_100);

    for (int i = 1_100; i < 2_300; i++)
      dictionary.getOrAdd("v" + i);
    scheduleAndWait(manager);
    assertThat(dictionary.rankViewSize()).as("throttled").isEqualTo(1_100);

    now[0] += ColumnarIndexManager.RANK_VIEW_MIN_INTERVAL_MS;
    scheduleAndWait(manager);
    assertThat(dictionary.rankViewSize()).isEqualTo(2_300);
    assertThat(dictionary.rank(dictionary.ordinalOf("v10"))).isLessThan(dictionary.rank(dictionary.ordinalOf("v2")));
  }

  @Test
  void projectionPerBucketOnlyForColumnarBuckets() {
    createAsset(3);
    database.command("sql", "CREATE DOCUMENT TYPE Other BUCKETS 2");
    database.command("sql", "CREATE PROPERTY Other.brand STRING");

    assertProjectedOncePerBucket();
    for (final int bucketId : bucketIds("Other"))
      assertThat(manager().projection(bucketId)).isNull();
    assertThat(manager().projection(-1)).isNull();
    assertThat(manager().projection(1_000_000)).isNull();

    // the schema load registers the indexes again: the projections are opened once it published them
    reopenDatabase();
    assertProjectedOncePerBucket();
    for (final Index sub : typeIndex().getSubIndexes())
      assertThat(((ColumnarIndex) sub).isUsable()).isTrue();
  }

  @Test
  void dropIndexDropsItsProjections() {
    createAsset(2);
    final int[] buckets = bucketIds("Asset");
    final Path dir = bucketDir(buckets[0]);
    assertThat(dir.resolve(ColumnarManifest.FILE_NAME)).exists();

    database.command("sql", "DROP INDEX `" + INDEX + "`");

    for (final int bucketId : buckets)
      assertThat(manager().projection(bucketId)).isNull();
    assertThat(dir).doesNotExist();
    assertThat(manager().extractor("Asset")).isNull();
  }

  @Test
  void dropDatabaseRemovesTheProjection() {
    createAsset(1);
    final Path databaseDir = Path.of(database.getDatabasePath());
    assertThat(bucketDir(bucketIds("Asset")[0]).resolve(ColumnarManifest.FILE_NAME)).exists();
    final ColumnarIndexManager manager = manager();

    database.drop();

    assertThat(databaseDir.resolve(ColumnarStore.ROOT_DIR)).doesNotExist();
    assertThat(manager.isApplyingForTesting()).isFalse();
    database = factory.create();
  }

  @Test
  void conversionFailuresDelegatesToStats() {
    manager().getStats().conversionFailure("Asset", "brand");
    manager().getStats().conversionFailure("Asset", "brand");

    assertThat(manager().conversionFailures("Asset", "brand")).isEqualTo(2);
    assertThat(manager().conversionFailures("Asset", "author")).isZero();
  }

  @Test
  void fallbackScansIsOnePerDatabaseWithTwoPermits() {
    final Semaphore scans = manager().fallbackScans();
    assertThat(manager().fallbackScans()).isSameAs(scans);
    assertThat(scans.availablePermits()).isEqualTo(2);

    try (final DatabaseFactory otherFactory = new DatabaseFactory(getDatabasePath() + "-other")) {
      if (otherFactory.exists())
        otherFactory.open().drop();
      final Database other = otherFactory.create();
      try {
        final Semaphore otherScans = ((LocalDatabase) other).getColumnarIndexManager().fallbackScans();
        assertThat(otherScans).isNotSameAs(scans);
        assertThat(otherScans.availablePermits()).isEqualTo(2);
      } finally {
        other.drop();
      }
    }
  }

  @Test
  void extractorResolvesTheRecordsBucketDictionary() {
    createAsset(2);
    final int[] buckets = bucketIds("Asset");
    final GlobalDictionary first = manager().projection(buckets[0]).dictionary("brand");
    final GlobalDictionary second = manager().projection(buckets[1]).dictionary("brand");
    first.getOrAdd("other");
    final int inFirst = first.getOrAdd("acme");
    final int inSecond = second.getOrAdd("acme");
    assertThat(inFirst).isNotEqualTo(inSecond);

    final Document[] saved = new Document[2];
    database.transaction(() -> {
      saved[0] = database.newDocument("Asset").set("brand", "acme").save(bucketName(buckets[0]));
      saved[1] = database.newDocument("Asset").set("brand", "acme").save(bucketName(buckets[1]));
    });

    final ColumnarValueExtractor extractor = manager().extractor("Asset");
    assertThat(ordinalOf(extractor, saved[0])).isEqualTo(inFirst);
    assertThat(ordinalOf(extractor, saved[1])).isEqualTo(inSecond);
    assertThat(manager().extractor("Asset")).isSameAs(extractor);
    assertThat(manager().extractor("NoSuchType")).isNull();
  }

  @Test
  void extractorRefusesRecordsItCannotResolve() {
    createAsset(1);
    database.command("sql", "CREATE DOCUMENT TYPE Other");
    final Document[] other = new Document[1];
    database.transaction(() -> other[0] = database.newDocument("Other").set("brand", "acme").save());
    final ColumnarValueExtractor extractor = manager().extractor("Asset");

    // a record of a bucket with no projection: refused with the reason, never a NullPointerException
    assertThatThrownBy(() -> ordinalOf(extractor, other[0])).isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("no columnar projection");
    final MutableDocument unsaved = database.newDocument("Asset").set("brand", "acme");
    assertThatThrownBy(() -> ordinalOf(extractor, unsaved)).isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("identity");
  }

  @Test
  void cleanOpenStartsApplying() throws InterruptedException {
    createAsset(1);
    final int bucketId = bucketIds("Asset")[0];
    // a committed page to mark: a page past the committed count is retried (final review A-C1), which is not what this test is about
    database.transaction(() -> database.newDocument("Asset").set("brand", "a").save());

    reopenDatabase();

    final ColumnarIndexManager manager = manager();
    assertThat(manager.isApplyingForTesting()).isTrue();
    final ColumnarProjection projection = manager.projection(bucketId);
    manager.markPages(bucketId, new int[] { 0 }, 1);
    assertThat(projection.tracker().oldestPendingEpochStartMs()).isPositive();

    assertThat(manager.awaitCaughtUp(10_000)).isTrue();
    assertThat(projection.tracker().oldestPendingEpochStartMs()).isZero();
  }

  @Test
  void readOnlyOpenDoesNotApply() throws InterruptedException {
    createAsset(1);
    final int bucketId = bucketIds("Asset")[0];

    reopenDatabaseInReadOnlyMode();

    final ColumnarIndexManager manager = manager();
    assertThat(manager.isApplyingForTesting()).isFalse();
    assertThat(manager.projection(bucketId)).isNotNull();
    manager.markPages(bucketId, new int[] { 1 }, 1);
    // a short wait expected to time out: nothing applies on a READ_ONLY database
    assertThat(manager.awaitCaughtUp(300)).isFalse();
    // the integrity check of the teardown writes
    reopenDatabase();
  }

  @Test
  void registerIsIdempotentPerIndexFileId() throws IOException {
    createAsset(2);
    final int[] buckets = bucketIds("Asset");
    final ColumnarProjection before = manager().projection(buckets[0]);
    final List<IndexInternal> previous = typeIndex().getSubIndexes();

    // a full schema reload registers a new ColumnarIndex per file id and unregisters none of the superseded ones
    ((LocalSchema) ((DatabaseInternal) database).getEmbedded().getSchema()).load(ComponentFile.MODE.READ_WRITE, true);

    assertThat(typeIndex().getSubIndexes().getFirst()).isNotSameAs(previous.getFirst());
    assertThat(manager().projection(buckets[0])).isSameAs(before);
    assertThat(before.columns()).hasSize(1);
    for (final Index sub : typeIndex().getSubIndexes())
      assertThat(((ColumnarIndex) sub).isUsable()).isTrue();
  }

  @Test
  void suspendApplyHoldsPassesUntilClosed() throws Exception {
    createAsset(1);
    final int bucketId = bucketIds("Asset")[0];
    final ColumnarIndexManager manager = manager();
    final ColumnarProjection projection = manager.projection(bucketId);

    final AutoCloseable hold = manager.suspendApply();
    // the commit marks its page (Task 13): the lag is observable through a snapshot while the hold keeps the passes out
    database.transaction(() -> database.newDocument("Asset").set("brand", "held").save());
    try (final ColumnarSnapshot snapshot = projection.acquire()) {
      assertThat(snapshot.oldestPendingEpochStartMs()).isPositive();
    }
    assertThat(projection.tracker().isEmpty()).isFalse();
    // a short wait expected to time out: the hold keeps every pass out
    assertThat(manager.awaitCaughtUp(300)).isFalse();

    hold.close();
    hold.close();
    assertThat(manager.awaitCaughtUp(10_000)).isTrue();
  }

  @Test
  void applyTickTakesNoProjectionLock() throws Exception {
    createAsset(1);
    final int bucketId = bucketIds("Asset")[0];
    final ColumnarIndexManager manager = manager();
    final ColumnarProjection projection = manager.projection(bucketId);
    // a committed page to mark, as in cleanOpenStartsApplying
    database.transaction(() -> database.newDocument("Asset").set("brand", "a").save());
    manager.markPages(bucketId, new int[] { 0 }, 1);

    final ExecutorService executor = Executors.newSingleThreadExecutor();
    projection.applyLock().lock();
    projection.publishLock().lock();
    try {
      // a hang detector, not a latency bound: a tick that took a projection lock would never return while this thread holds them
      executor.submit(manager.tickForTesting()).get(30, TimeUnit.SECONDS);
    } finally {
      projection.publishLock().unlock();
      projection.applyLock().unlock();
      executor.shutdownNow();
    }
    assertThat(manager.awaitCaughtUp(10_000)).isTrue();
  }

  @Test
  void watchedFileBumpsTheGenerationAndRunsWithOrWithoutProjection() throws InterruptedException {
    createAsset(1);
    database.command("sql", "CREATE DOCUMENT TYPE Tag");
    final int tagBucket = bucketIds("Tag")[0];
    final int assetBucket = bucketIds("Asset")[0];
    final ColumnarIndexManager manager = manager();
    final CountDownLatch ran = new CountDownLatch(1);
    final Runnable watcher = ran::countDown;
    assertThat(manager.watchGeneration(watcher)).isZero();

    manager.watchFiles(new int[] { tagBucket, assetBucket }, watcher);
    manager.markPages(tagBucket, new int[] { 0, 1, 2 }, 3);

    // synchronous: bumped before markPages returned, whether or not the watcher ran yet
    assertThat(manager.watchGeneration(watcher)).isEqualTo(1);
    assertThat(ran.await(10, TimeUnit.SECONDS)).isTrue();

    // a projected file goes to its tracker and to its watchers too (plan 5 R9: a Tag type with a COLUMNAR index of its own)
    // drained first, so the pending mark below can only be this one
    assertThat(manager.awaitCaughtUp(10_000)).isTrue();
    try (final AutoCloseable hold = manager.suspendApply()) {
      assertThat(manager.projection(assetBucket).tracker().oldestPendingEpochStartMs()).isZero();
      manager.markPages(assetBucket, new int[] { 0 }, 1);
      assertThat(manager.watchGeneration(watcher)).isEqualTo(2);
      assertThat(manager.projection(assetBucket).tracker().oldestPendingEpochStartMs()).isNotZero();
    } catch (final Exception e) {
      throw new IllegalStateException(e);
    }

    // a new file set keeps the watcher and its counter: the generation never goes back
    manager.replaceWatchedFiles(watcher, new int[] { assetBucket });
    assertThat(manager.watchGeneration(watcher)).isEqualTo(2);
    manager.markPages(tagBucket, new int[] { 0 }, 1);
    assertThat(manager.watchGeneration(watcher)).isEqualTo(2);
    manager.markPages(assetBucket, new int[] { 0 }, 1);
    assertThat(manager.watchGeneration(watcher)).isEqualTo(3);

    manager.unwatchFiles(watcher);
    manager.markPages(tagBucket, new int[] { 0 }, 1);
    assertThat(manager.watchGeneration(watcher)).isZero();
  }

  @Test
  void busyMaintenanceSlotIsNotAFailure() {
    final ColumnarProjection projection = suspendedProjection();
    stage(projection, 0, 1, 2);
    assertThat(projection.tryBeginMaintenance()).isTrue();
    try {
      manager().maintainForTesting(projection.bucketId());
    } finally {
      projection.endMaintenance();
    }
    assertThat(manager().maintenanceBackoffMsForTesting(projection.bucketId())).isZero();
    assertThat(projection.manifest().segments()).isEmpty();

    manager().maintainForTesting(projection.bucketId());
    assertThat(projection.manifest().segments()).hasSize(1);
  }

  @Test
  void failingFlushBacksOffExponentiallyAndRecovers() {
    final ColumnarProjection projection = suspendedProjection();
    stage(projection, 0, 1);
    throwAt(ColumnarFlusher.FLUSH_SEGMENT_WRITTEN, new RuntimeException("injected flush failure"));

    final int bucketId = projection.bucketId();
    manager().maintainForTesting(bucketId);
    final long first = manager().maintenanceBackoffMsForTesting(bucketId);
    manager().maintainForTesting(bucketId);
    final long second = manager().maintenanceBackoffMsForTesting(bucketId);
    manager().maintainForTesting(bucketId);
    final long third = manager().maintenanceBackoffMsForTesting(bucketId);
    assertThat(first).isEqualTo(ColumnarIndexManager.MAINTENANCE_BACKOFF_BASE_MS);
    assertThat(second).isEqualTo(2 * first);
    assertThat(third).isEqualTo(4 * first);

    ColumnarTestHooks.killPoint = null;
    manager().maintainForTesting(bucketId);
    assertThat(manager().maintenanceBackoffMsForTesting(bucketId)).isZero();
    assertThat(projection.manifest().segments()).hasSize(1);
  }

  @Test
  void repeatedlyCorruptMergeRequestsACorruptRebuild() {
    database.getConfiguration().setValue(GlobalConfiguration.COLUMNAR_MERGE_MAX_SEGMENTS, 1);
    final ColumnarProjection projection = suspendedProjection();
    final int bucketId = projection.bucketId();
    final List<String> requested = Collections.synchronizedList(new ArrayList<>());
    projection.setRebuildRequester(requested::add);

    stage(projection, 0, 1);
    manager().maintainForTesting(bucketId);
    assertThat(projection.manifest().segments()).hasSize(1);

    stage(projection, 1, 1);
    throwAt(ColumnarFlusher.MERGE_SEGMENT_WRITTEN, new BlockCorruptionException("injected corrupt merge input"));
    // the flush publishes the second segment, then the merge of both fails
    manager().maintainForTesting(bucketId);
    assertThat(projection.manifest().segments()).hasSize(2);
    manager().maintainForTesting(bucketId);
    assertThat(requested).isEmpty();
    assertThat(projection.isUsable()).isTrue();

    manager().maintainForTesting(bucketId);
    assertThat(requested).containsExactly(ColumnarProjection.CORRUPT);
    assertThat(projection.isUsable()).isFalse();
    assertThat(projection.unusableReason()).isEqualTo(ColumnarProjection.CORRUPT);
  }

  @Test
  void cleanCloseDeletesTheNoWalMarker() throws IOException {
    createAsset(1);
    final Path marker = bucketDir(bucketIds("Asset")[0]).resolve(ColumnarStore.NOWAL_MARKER);
    Files.write(marker, new byte[0]);

    reopenDatabase();

    assertThat(marker).doesNotExist();
  }

  @Test
  void killKeepsTheNoWalMarker() throws IOException {
    createAsset(1);
    final Path marker = bucketDir(bucketIds("Asset")[0]).resolve(ColumnarStore.NOWAL_MARKER);
    Files.write(marker, new byte[0]);
    final ColumnarIndexManager manager = manager();

    ((DatabaseInternal) database).kill();
    assertThat(manager.isApplyingForTesting()).isFalse();
    database.close();

    assertThat(marker).exists();
    database = factory.open();
  }

  @Test
  void aProjectionThatFailsToOpenIsRetriedWhileTheOthersOpenAndServedOnlyRebuilt() throws InterruptedException {
    createAsset(1);
    database.command("sql", "CREATE DOCUMENT TYPE Other");
    database.command("sql", "CREATE PROPERTY Other.brand STRING");
    database.command("sql", "CREATE INDEX ON Other (brand) COLUMNAR");
    final AtomicInteger reached = new AtomicInteger();
    ColumnarTestHooks.killPoint = name -> {
      if (name.equals(ColumnarIndexManager.OPEN_PROJECTION) && reached.getAndIncrement() == 0)
        throw new IllegalStateException("injected open failure");
    };

    // no rebuild runs before the test looked at the reopened projection: it holds every rebuild permit
    final String failedType;
    final int permits = ColumnarRebuilder.REBUILD_SEMAPHORE.drainPermits();
    try {
      // the failure happens inside the schema load, after its publication: the open goes on with the new generation
      reopenDatabase();
      failedType = assertFailedOpenIsRetriedAndServedOnlyRebuilt();
    } finally {
      ColumnarRebuilder.REBUILD_SEMAPHORE.release(permits);
    }
    // the retry requested the rebuild: once a permit is free it runs and the projection is served again
    assertThat(manager().awaitCaughtUp(60_000)).isTrue();
    assertThat(manager().getStats().rebuilds(failedType, ColumnarProjection.MISSING)).isEqualTo(1);
    assertThat(manager().getStats().rebuildsTotal()).isEqualTo(1);
  }

  /** @return the type whose projection failed to open */
  private String assertFailedOpenIsRetriedAndServedOnlyRebuilt() throws InterruptedException {

    assertThat(database.getSchema().existsIndex(INDEX)).isTrue();
    assertThat(database.getSchema().existsIndex("Other[brand]_columnar")).isTrue();
    assertThat(database.query("sql", "SELECT count(*) AS n FROM Other").next().<Long>getProperty("n")).isZero();
    final int asset = bucketIds("Asset")[0];
    final int other = bucketIds("Other")[0];
    final ColumnarIndexManager manager = manager();
    assertThat(manager.projection(asset) == null ^ manager.projection(other) == null).isTrue();
    final String failedType = manager.projection(asset) == null ? "Asset" : "Other";
    final int failedBucket = failedType.equals("Asset") ? asset : other;
    final int openBucket = failedType.equals("Asset") ? other : asset;
    final ColumnarIndex failedIndex = (ColumnarIndex) ((TypeIndex) database.getSchema().getIndexByName(failedType + "[brand]_columnar"))
        .getSubIndexes().getFirst();
    assertThat(failedIndex.isUsable()).isFalse();
    // a commit while the bucket has no projection: its marks have nowhere to go
    database.transaction(() -> database.newDocument(failedType).set("brand", "acme").save());
    // a short wait expected to time out: the retry waits for its backoff
    assertThat(manager.awaitCaughtUp(300)).isFalse();

    for (int i = 0; i < 10_000 && manager.projection(failedBucket) == null; i++)
      Thread.sleep(1);
    final ColumnarProjection reopened = manager.projection(failedBucket);
    assertThat(reopened).isNotNull();
    // the files it loaded predate that commit: never served as they are, only once rebuilt
    assertThat(reopened.isUsable()).isFalse();
    assertThat(reopened.unusableReason()).isEqualTo(ColumnarProjection.MISSING);
    assertThat(failedIndex.isUsable()).isFalse();
    assertThat(manager.projection(openBucket).isUsable()).isTrue();
    return failedType;
  }

  @Test
  void filesThatThrowWhileLoadingOpenCorruptAtOnce() {
    createAsset(1);
    final AtomicInteger reached = new AtomicInteger();
    ColumnarTestHooks.killPoint = name -> {
      if (name.equals(ColumnarProjection.LOAD_FILES) && reached.getAndIncrement() == 0)
        throw new BufferUnderflowException();
    };

    reopenDatabase();

    // not an environment failure: no retry loop, the projection is open and waits for its rebuild
    final ColumnarProjection projection = manager().projection(bucketIds("Asset")[0]);
    assertThat(projection).isNotNull();
    assertThat(projection.isUsable()).isFalse();
    assertThat(projection.unusableReason()).isEqualTo(ColumnarProjection.CORRUPT);
  }

  @Test
  void theTickWaitsForTheBackoffOfAFailedOpen() {
    database.getConfiguration().setValue(GlobalConfiguration.COLUMNAR_FLUSH_INTERVAL, 3_600_000L);
    final ColumnarIndexManager manager = manager();
    final long[] now = { System.currentTimeMillis() };
    manager.setClockForTesting(() -> now[0]);
    ColumnarTestHooks.killPoint = name -> {
      if (name.equals(ColumnarIndexManager.OPEN_PROJECTION))
        throw new IllegalStateException("injected open failure");
    };
    createAsset(1);
    assertThat(manager.projection(bucketIds("Asset")[0])).isNull();

    final long before = manager.passesSubmittedForTesting();
    for (int i = 0; i < 100; i++)
      manager.tickForTesting().run();
    assertThat(manager.passesSubmittedForTesting()).isEqualTo(before);

    now[0] += ColumnarIndexManager.MAINTENANCE_BACKOFF_BASE_MS;
    for (int i = 0; i < 100; i++)
      manager.tickForTesting().run();
    assertThat(manager.passesSubmittedForTesting()).isGreaterThan(before);
  }

  @Test
  void aCloseDuringAMergeWaitsForItToAbandonAndPublishesNothing() throws Exception {
    final Path dir = closeDuringAPausedMerge(false);
    final ColumnarManifest onDisk = ColumnarManifest.load(dir);
    assertThat(onDisk.segments()).hasSize(2);
    // the abandoned merge deleted what it wrote: only the files the manifest names, and the manifest itself
    try (final var files = Files.list(dir)) {
      assertThat(files.map(f -> f.getFileName().toString()).filter(n -> !n.equals(ColumnarManifest.FILE_NAME)))
          .allMatch(onDisk.files()::contains);
    }

    // one writer: the reopened projection serves the published state and flushes on its own
    database = factory.open();
    final ColumnarProjection reopened = manager().projection(bucketIds("Asset")[0]);
    assertThat(reopened.isUsable()).isTrue();
    assertThat(reopened.manifest().generation()).isEqualTo(onDisk.generation());
  }

  @Test
  void aDropDuringAMergeLeavesNoColumnarFile() throws Exception {
    final Path dir = closeDuringAPausedMerge(true);
    assertThat(dir).doesNotExist();
    assertThat(Path.of(getDatabasePath()).resolve(ColumnarStore.ROOT_DIR)).doesNotExist();
    database = factory.create();
  }

  @Test
  void aCloseDoesNotWaitForAJobOnlyQueuedBehindAnotherDatabaseAndStillFlushes() throws Exception {
    final Path dir = closeWithAQueuedJob(false);
    // the close flush ran for this projection: its staged rows are the one segment the reopen serves
    assertThat(ColumnarManifest.load(dir).segments()).hasSize(1);
    database = factory.open();
    assertThat(manager().projection(bucketIds("Asset")[0]).manifest().segments()).hasSize(1);
  }

  @Test
  void aDropDoesNotWaitForAJobOnlyQueuedBehindAnotherDatabase() throws Exception {
    final Path dir = closeWithAQueuedJob(true);
    assertThat(dir).doesNotExist();
    database = factory.create();
  }

  /**
   * Every thread of the JVM-wide maintenance pool is held by "another database's" long job, and this database's flush job is queued
   * behind them; the database is closed (or dropped) on another thread, which must return while the pool stays occupied.
   *
   * @return the bucket directory
   */
  private Path closeWithAQueuedJob(final boolean drop) throws Exception {
    final ColumnarProjection projection = suspendedProjection();
    final ColumnarIndexManager manager = manager();
    final Path dir = projection.bucketDir();
    stage(projection, 0, 1, 2);
    database.getConfiguration().setValue(GlobalConfiguration.COLUMNAR_FLUSH_INTERVAL, 0L);

    final ColumnarMaintenancePool pool = ColumnarMaintenancePool.getInstance();
    final int threads = pool.getMaxParallelism();
    final CountDownLatch occupied = new CountDownLatch(threads);
    final CountDownLatch release = new CountDownLatch(1);
    final ExecutorService closer = Executors.newSingleThreadExecutor();
    try {
      for (int i = 0; i < threads; i++)
        pool.getExecutorService().execute(() -> {
          occupied.countDown();
          try {
            release.await();
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        });
      assertThat(occupied.await(10, TimeUnit.SECONDS)).isTrue();
      manager.scheduleMaintenanceForTesting();
      assertThat(manager.isMaintenanceIdleForTesting()).isFalse();

      // a hang detector, not a latency bound: a close waiting for the queued job would wait for the other database's job
      closer.submit(() -> {
        if (drop)
          database.drop();
        else
          database.close();
      }).get(30, TimeUnit.SECONDS);
    } finally {
      release.countDown();
      closer.shutdownNow();
    }
    // the queued job starts once the pool frees up and exits without work
    for (int i = 0; i < 10_000 && !manager.isMaintenanceIdleForTesting(); i++)
      Thread.sleep(1);
    assertThat(manager.isMaintenanceIdleForTesting()).isTrue();
    return dir;
  }

  /**
   * Two flushed segments, then a scheduled flush and merge whose merge is paused after writing its segment; the database is closed (or
   * dropped) on another thread, and the merge released once the projection stopped publishing.
   *
   * @return the bucket directory
   */
  private Path closeDuringAPausedMerge(final boolean drop) throws Exception {
    database.getConfiguration().setValue(GlobalConfiguration.COLUMNAR_MERGE_MAX_SEGMENTS, 1);
    final ColumnarProjection projection = suspendedProjection();
    final ColumnarIndexManager manager = manager();
    final Path dir = projection.bucketDir();
    stage(projection, 0, 1);
    manager.maintainForTesting(projection.bucketId());
    stage(projection, 1, 1);
    final CountDownLatch paused = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    ColumnarTestHooks.killPoint = name -> {
      if (name.equals(ColumnarFlusher.MERGE_SEGMENT_WRITTEN) && paused.getCount() > 0) {
        paused.countDown();
        try {
          release.await();
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    };
    database.getConfiguration().setValue(GlobalConfiguration.COLUMNAR_FLUSH_INTERVAL, 0L);
    manager.setDrainTimeoutForTesting(100);
    manager.scheduleMaintenanceForTesting();
    assertThat(paused.await(10, TimeUnit.SECONDS)).isTrue();
    final long generationAtPause = ColumnarManifest.load(dir).generation();

    final ExecutorService closer = Executors.newSingleThreadExecutor();
    try {
      final var closed = closer.submit(() -> {
        if (drop)
          database.drop();
        else
          database.close();
      });
      for (int i = 0; i < 10_000 && !projection.isClosedOrDropped(); i++)
        Thread.sleep(1);
      assertThat(projection.isClosedOrDropped()).isTrue();
      // the close waits for the paused job: it cannot have returned yet
      assertThat(closed.isDone()).isFalse();
      release.countDown();
      // a hang detector, not a latency bound
      closed.get(30, TimeUnit.SECONDS);
    } finally {
      release.countDown();
      closer.shutdownNow();
    }
    if (!drop)
      assertThat(ColumnarManifest.load(dir).generation()).isEqualTo(generationAtPause);
    return dir;
  }

  @Test
  void aDatabaseWithoutColumnarIndexesSubmitsNoPass() {
    database.getConfiguration().setValue(GlobalConfiguration.COLUMNAR_FLUSH_INTERVAL, 0L);
    final ColumnarIndexManager manager = manager();
    for (int i = 0; i < 100; i++)
      manager.tickForTesting().run();
    assertThat(manager.passesSubmittedForTesting()).isZero();

    // the same ticks with an index: a pass at every flush interval (0 here)
    createAsset(1);
    for (int i = 0; i < 100; i++)
      manager.tickForTesting().run();
    assertThat(manager.passesSubmittedForTesting()).isPositive();
  }

  @Test
  void scheduledMaintenanceHonorsTheBackoff() throws InterruptedException {
    database.getConfiguration().setValue(GlobalConfiguration.COLUMNAR_FLUSH_INTERVAL, 0L);
    final ColumnarProjection projection = suspendedProjection();
    final ColumnarIndexManager manager = manager();
    final long[] now = { System.currentTimeMillis() + 3_600_000L };
    manager.setClockForTesting(() -> now[0]);
    stage(projection, 0, 1);
    final AtomicInteger attempts = new AtomicInteger();
    ColumnarTestHooks.killPoint = name -> {
      if (name.equals(ColumnarFlusher.FLUSH_SEGMENT_WRITTEN)) {
        attempts.incrementAndGet();
        throw new RuntimeException("injected flush failure");
      }
    };

    scheduleAndWait(manager);
    assertThat(attempts).hasValue(1);
    assertThat(manager.maintenanceBackoffMsForTesting(projection.bucketId())).isEqualTo(ColumnarIndexManager.MAINTENANCE_BACKOFF_BASE_MS);

    for (int i = 0; i < 5; i++)
      scheduleAndWait(manager);
    assertThat(attempts).hasValue(1);

    now[0] += ColumnarIndexManager.MAINTENANCE_BACKOFF_BASE_MS;
    scheduleAndWait(manager);
    assertThat(attempts).hasValue(2);
    assertThat(manager.maintenanceBackoffMsForTesting(projection.bucketId())).isEqualTo(2 * ColumnarIndexManager.MAINTENANCE_BACKOFF_BASE_MS);
  }

  @Test
  void aNoWalMarkAfterTheCloseFlushKeepsTheMarker() throws IOException {
    createAsset(1);
    final int bucketId = bucketIds("Asset")[0];
    final Path marker = bucketDir(bucketId).resolve(ColumnarStore.NOWAL_MARKER);
    Files.write(marker, new byte[0]);
    final ColumnarIndexManager manager = manager();

    // the columnar close flushes, then a no-WAL commit lands before the database flushed its pages
    manager.close();
    manager.markPagesWithoutWAL(bucketId, new int[] { 0 }, 1);
    database.close();

    assertThat(marker).exists();
    database = factory.open();
  }

  @Test
  void aFencedCloseKeepsTheNoWalMarker() throws IOException {
    createAsset(1);
    final Path marker = bucketDir(bucketIds("Asset")[0]).resolve(ColumnarStore.NOWAL_MARKER);
    Files.write(marker, new byte[0]);

    // a fenced database closes crash-equivalent: its pages are not known to be on disk
    ((LocalDatabase) ((DatabaseInternal) database).getEmbedded()).fenceForRecovery("test");
    database.close();

    assertThat(marker).exists();
    database = factory.open();
  }

  @Test
  void cleanCloseFlushesAndLeavesEmptyRecoverySet() throws IOException {
    createAsset(1);
    final int bucketId = bucketIds("Asset")[0];
    database.transaction(() -> {
      for (int i = 0; i < 100; i++)
        database.newDocument("Asset").set("brand", "b" + (i % 5)).save();
    });
    final Path recoveryFile = Path.of(database.getDatabasePath()).resolve(ColumnarStore.ROOT_DIR).resolve(RecoverySetFile.FILE_NAME);

    reopenDatabase();

    // written by the close (no WAL file was dropped before it), naming the projected bucket with no page left
    final Map<Integer, int[]> recovery = RecoverySetFile.read(recoveryFile);
    assertThat(recovery).containsKey(bucketId);
    for (final int[] pages : recovery.values())
      assertThat(pages).isEmpty();
    final ColumnarProjection projection = manager().projection(bucketId);
    assertThat(projection.manifest().segments()).isNotEmpty();
    assertThat(projection.manifest().segments().stream().mapToLong(ColumnarManifest.SegmentEntry::rows).sum()).isEqualTo(100);
    assertThat(projection.isUsable()).isTrue();
    assertThat(manager().getStats().rebuildsTotal()).isZero();
  }

  @Test
  void aCloseKeepsThePagesAMarkAfterTheColumnarCloseLeft() throws IOException {
    createAsset(1);
    final int bucketId = bucketIds("Asset")[0];
    final ColumnarIndexManager manager = manager();

    // a commit between the columnar close and the database write lock: the close persists what the tracker holds, never an empty set
    manager.close();
    manager.markPages(bucketId, new int[] { 4 }, 1);
    database.close();

    final Path recoveryFile = Path.of(database.getDatabasePath()).resolve(ColumnarStore.ROOT_DIR).resolve(RecoverySetFile.FILE_NAME);
    assertThat(RecoverySetFile.read(recoveryFile).get(bucketId)).containsExactly(4);
    database = factory.open();
    // taken back by the open: the page is applied again
    assertThat(manager().getStats().recoveryPagesCount()).isEqualTo(1);
  }

  @Test
  void readOnlyOpenNeverWritesTheProjection() throws Exception {
    createAsset(1);
    final int bucketId = bucketIds("Asset")[0];
    database.transaction(() -> {
      for (int i = 0; i < 100; i++)
        database.newDocument("Asset").set("brand", "b" + (i % 5)).save();
    });
    database.close();
    final Path root = Path.of(database.getDatabasePath()).resolve(ColumnarStore.ROOT_DIR);
    final Map<String, Long> before = filesWithTimes(root);

    database = factory.open(ComponentFile.MODE.READ_ONLY);
    final ColumnarProjection projection = manager().projection(bucketId);
    assertThat(projection.isUsable()).isTrue();
    long rows = 0;
    try (final ColumnarSnapshot snapshot = projection.acquire()) {
      for (final ColumnarSegmentView view : snapshot.segments())
        rows += view.live().cardinality();
    }
    assertThat(rows).isEqualTo(100);
    assertThat(manager().getStats().applyPasses()).isZero();
    database.close();
    assertThat(filesWithTimes(root)).isEqualTo(before);

    // a deleted manifest: unusable, no rebuild, no file created
    Files.delete(ColumnarStore.bucketDir(Path.of(database.getDatabasePath()), bucketId).resolve(ColumnarManifest.FILE_NAME));
    final Map<String, Long> withoutManifest = filesWithTimes(root);
    database = factory.open(ComponentFile.MODE.READ_ONLY);
    assertThat(manager().projection(bucketId).isUsable()).isFalse();
    assertThat(manager().getStats().rebuildsTotal()).isZero();
    database.close();
    assertThat(filesWithTimes(root)).isEqualTo(withoutManifest);

    // the integrity check of the teardown writes; the writable open rebuilds the missing projection
    database = factory.open();
    assertThat(manager().awaitCaughtUp(60_000)).isTrue();
    assertThat(manager().getStats().rebuilds("Asset", ColumnarProjection.MISSING)).isEqualTo(1);
  }

  @Test
  void killDoesNoIo() throws Exception {
    createAsset(1);
    database.transaction(() -> {
      for (int i = 0; i < 100; i++)
        database.newDocument("Asset").set("brand", "b" + (i % 5)).save();
    });
    final ColumnarIndexManager manager = manager();
    assertThat(manager.awaitCaughtUp(10_000)).isTrue();
    final Path root = Path.of(database.getDatabasePath()).resolve(ColumnarStore.ROOT_DIR);
    final KillPoint point = KillPoint.arm(ColumnarFlusher.FLUSH_SEGMENT_WRITTEN);
    final AtomicReference<Throwable> flushEnded = new AtomicReference<>();
    final Thread flusher = new Thread(() -> {
      try {
        manager.flushNow();
      } catch (final Throwable e) {
        flushEnded.set(e);
      }
    });
    try {
      flusher.start();
      assertThat(point.awaitReached(10_000)).isTrue();
      final Map<String, Long> atKill = filesWithTimes(root);

      ((DatabaseInternal) database).kill();
      point.releaseAsCrashed();
      flusher.join(10_000);
      database.close();

      assertThat(flushEnded.get()).isInstanceOf(ColumnarTestHooks.SimulatedCrash.class);
      assertThat(filesWithTimes(root)).isEqualTo(atKill);
    } finally {
      point.disarm();
    }
    database = factory.open();
  }

  /** Every regular file under the directory, by relative name, with its modification time. */
  private static Map<String, Long> filesWithTimes(final Path dir) throws IOException {
    final Map<String, Long> files = new TreeMap<>();
    try (final Stream<Path> walk = Files.walk(dir)) {
      for (final Path file : (Iterable<Path>) walk.filter(Files::isRegularFile)::iterator)
        files.put(dir.relativize(file).toString(), Files.getLastModifiedTime(file).toMillis());
    }
    return files;
  }

  private static void scheduleAndWait(final ColumnarIndexManager manager) throws InterruptedException {
    manager.scheduleMaintenanceForTesting();
    for (int i = 0; i < 10_000 && !manager.isMaintenanceIdleForTesting(); i++)
      Thread.sleep(1);
    assertThat(manager.isMaintenanceIdleForTesting()).isTrue();
  }

  private ColumnarIndexManager manager() {
    return ((LocalDatabase) ((DatabaseInternal) database).getEmbedded()).getColumnarIndexManager();
  }

  private void createAsset(final int buckets) {
    database.command("sql", "CREATE DOCUMENT TYPE Asset BUCKETS " + buckets);
    database.command("sql", "CREATE PROPERTY Asset.brand STRING");
    database.command("sql", "CREATE INDEX ON Asset (brand) COLUMNAR");
  }

  /** The projection of a one-bucket Asset with its apply passes held for the whole test, so only the test moves its trackers. */
  private ColumnarProjection suspendedProjection() {
    createAsset(1);
    manager().suspendApply();
    return manager().projection(bucketIds("Asset")[0]);
  }

  /** Stages {@code slots} of {@code page} with the brand "acme" as an apply pass would, so a flush has rows to write. */
  private static void stage(final ColumnarProjection projection, final int page, final int... slots) {
    final int max = projection.maxRecordsInPage();
    final int ordinal = projection.dictionary("brand").getOrAdd("acme");
    projection.applyLock().lock();
    try {
      final DirtyPageTracker tracker = projection.tracker();
      tracker.mark(new int[] { page }, 1);
      tracker.drainInto(new IntHashSet());
      final RowBuffer rows = new RowBuffer(max, projection.columns());
      final RowValues row = new RowValues(1);
      row.present[0] = true;
      row.ordinal[0] = ordinal;
      for (final int slot : slots)
        rows.put(slot, row);
      projection.overlay().replacePage(OverlayPageBlock.build(page, max, projection.columns(), 1, rows));
      projection.publishOverlay();
      tracker.completed(new int[] { page }, 1);
      projection.pagesApplied(new int[] { page }, 1);
    } finally {
      projection.applyLock().unlock();
    }
  }

  private static void throwAt(final String point, final RuntimeException exception) {
    ColumnarTestHooks.killPoint = name -> {
      if (name.equals(point))
        throw exception;
    };
  }

  private void assertProjectedOncePerBucket() {
    final Set<ColumnarProjection> distinct = Collections.newSetFromMap(new IdentityHashMap<>());
    for (final int bucketId : bucketIds("Asset")) {
      final ColumnarProjection projection = manager().projection(bucketId);
      assertThat(projection).isNotNull();
      assertThat(projection.bucketId()).isEqualTo(bucketId);
      assertThat(projection.isUsable()).isTrue();
      distinct.add(projection);
    }
    assertThat(distinct).hasSize(bucketIds("Asset").length);
  }

  private TypeIndex typeIndex() {
    return (TypeIndex) database.getSchema().getIndexByName(INDEX);
  }

  private int[] bucketIds(final String type) {
    return database.getSchema().getType(type).getBuckets(false).stream().mapToInt(Bucket::getFileId).toArray();
  }

  private String bucketName(final int bucketId) {
    return database.getSchema().getBucketById(bucketId).getName();
  }

  private Path bucketDir(final int bucketId) {
    return ColumnarStore.bucketDir(Path.of(database.getDatabasePath()), bucketId);
  }

  /** The ordinal the extractor reports for the record's brand, -1 when it reports none. */
  private static int ordinalOf(final ColumnarValueExtractor extractor, final Document record) {
    final int[] found = { -1 };
    extractor.extract(record, "brand", new ValueSink() {
      @Override
      public void ordinal(final int o) {
        found[0] = o;
      }

      @Override
      public void ordinals(final int[] sorted, final int count) {
        // a scalar column
      }

      @Override
      public void longValue(final long v) {
        // a scalar ordinal column
      }

      @Override
      public void doubleValue(final double v) {
        // a scalar ordinal column
      }

      @Override
      public void missing() {
        // stays -1
      }

      @Override
      public void unknownValue(final Object normalized) {
        // stays -1
      }
    });
    return found[0];
  }
}
