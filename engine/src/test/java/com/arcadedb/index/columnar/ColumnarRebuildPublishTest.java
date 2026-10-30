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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.RID;
import com.arcadedb.engine.Bucket;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Final review A-I1/I2: the publish of a rebuild is the only publish whose content can go backwards on a page (the rebuilt rows of a page
 * the apply pass updated during the scan). No reader may take the rebuilt state as current while such a page is not pending, and the
 * projection is not caught up while a rebuild is queued or running.
 */
class ColumnarRebuildPublishTest extends TestHelper {
  private static final long WAIT_MS = 30_000;

  private final ExecutorService executor = Executors.newSingleThreadExecutor();

  @Override
  protected void endTest() {
    ColumnarTestHooks.killPoint = null;
    executor.shutdownNow();
  }

  @Test
  void aPageAppliedDuringTheScanIsPendingOnceTheRebuiltStateIsCurrent() throws Exception {
    final RID target = createAssets();
    final ColumnarProjection projection = projection();
    final CountDownLatch scanned = new CountDownLatch(1);
    final CountDownLatch releaseScan = new CountDownLatch(1);
    final AtomicReference<Boolean> pendingAtSwap = new AtomicReference<>();
    ColumnarTestHooks.killPoint = name -> {
      if (name.equals(ColumnarRebuilder.SCANNED))
        hold(scanned, releaseScan);
      else if (name.equals(ColumnarProjection.REBUILD_SWAPPED))
        try (final ColumnarSnapshot snapshot = projection.acquire()) {
          pendingAtSwap.set(snapshot.pendingPositions().get(target.getPosition()));
        }
    };

    ColumnarRebuilder.schedule(manager(), projection, ColumnarRebuilder.CHECK_FIX);
    assertThat(scanned.await(WAIT_MS, TimeUnit.MILLISECONDS)).isTrue();
    updateAndApply(target, projection);
    releaseScan.countDown();
    awaitRebuildEnded(projection);

    assertThat(pendingAtSwap.get()).as("the update applied to the old state during the scan is pending in the rebuilt one").isTrue();
    FacetOracle.assertEquivalent(database, "Asset", "brand", null);
  }

  @Test
  void aReaderStraddlingTheRebuildPublishRetriesOnTheRebuiltState() throws Exception {
    final RID target = createAssets();
    final ColumnarProjection projection = projection();
    final CountDownLatch scanned = new CountDownLatch(1);
    final CountDownLatch releaseScan = new CountDownLatch(1);
    final CountDownLatch readerBetween = new CountDownLatch(1);
    final CountDownLatch releaseReader = new CountDownLatch(1);
    final AtomicReference<Thread> reader = new AtomicReference<>();
    final AtomicBoolean readerHeld = new AtomicBoolean();
    ColumnarTestHooks.killPoint = name -> {
      if (name.equals(ColumnarRebuilder.SCANNED))
        hold(scanned, releaseScan);
      else if (name.equals(ColumnarProjection.ACQUIRE_BETWEEN) && Thread.currentThread() == reader.get() && readerHeld.compareAndSet(false,
          true))
        hold(readerBetween, releaseReader);
    };

    ColumnarRebuilder.schedule(manager(), projection, ColumnarRebuilder.CHECK_FIX);
    assertThat(scanned.await(WAIT_MS, TimeUnit.MILLISECONDS)).isTrue();
    try (final AutoCloseable suspended = manager().suspendApply()) {
      updateAndApply(target, projection);
      // the reader reads the pending positions now, before the rebuild re-marks the page, and the state after the publish
      final Future<Boolean> pending = executor.submit(() -> {
        reader.set(Thread.currentThread());
        try (final ColumnarSnapshot snapshot = projection.acquire()) {
          return snapshot.pendingPositions().get(target.getPosition());
        }
      });
      assertThat(readerBetween.await(WAIT_MS, TimeUnit.MILLISECONDS)).isTrue();
      releaseScan.countDown();
      awaitRebuildEnded(projection);
      releaseReader.countDown();

      assertThat(pending.get(WAIT_MS, TimeUnit.MILLISECONDS)).as("the snapshot does not pair the old pending set with the rebuilt state")
          .isTrue();
    }
    FacetOracle.assertEquivalent(database, "Asset", "brand", null);
  }

  @Test
  void notCaughtUpWhileARebuildRuns() throws Exception {
    createAssets();
    final ColumnarProjection projection = projection();
    final CountDownLatch scanned = new CountDownLatch(1);
    final CountDownLatch releaseScan = new CountDownLatch(1);
    ColumnarTestHooks.killPoint = name -> {
      if (name.equals(ColumnarRebuilder.SCANNED))
        hold(scanned, releaseScan);
    };
    ColumnarRebuilder.schedule(manager(), projection, ColumnarRebuilder.CHECK_FIX);
    try {
      assertThat(scanned.await(WAIT_MS, TimeUnit.MILLISECONDS)).isTrue();
      assertThat(manager().awaitCaughtUp(50)).as("a running rebuild is not caught up").isFalse();
    } finally {
      releaseScan.countDown();
    }
    assertThat(manager().awaitCaughtUp(WAIT_MS)).isTrue();
    assertThat(ColumnarRebuilder.isQueuedOrRunning(projection)).isFalse();
  }

  /** Asset(brand) with a COLUMNAR index and 200 rows in a segment; returns the first record. */
  private RID createAssets() throws Exception {
    database.command("sql", "CREATE DOCUMENT TYPE Asset BUCKETS 1");
    database.command("sql", "CREATE PROPERTY Asset.brand STRING");
    database.command("sql", "CREATE INDEX ON Asset (brand) COLUMNAR");
    final List<RID> rids = new ArrayList<>();
    database.transaction(() -> {
      for (int i = 0; i < 200; i++)
        rids.add(database.newDocument("Asset").set("brand", "b" + (i % 7)).save().getIdentity());
    });
    assertThat(manager().awaitCaughtUp(WAIT_MS)).isTrue();
    manager().flushNow();
    return rids.getFirst();
  }

  /** Commits an update of the record on its already scanned page and lets an apply pass take it into the current (old) state. */
  private void updateAndApply(final RID target, final ColumnarProjection projection) {
    database.transaction(() -> target.asDocument(true).modify().set("brand", "updated").save());
    for (int i = 0; i < 1_000 && projection.tracker().oldestPendingEpochStartMs() != 0; i++)
      ColumnarApplier.runPass(manager(), projection);
    assertThat(projection.tracker().oldestPendingEpochStartMs()).as("the update is applied to the old state").isZero();
  }

  private static void hold(final CountDownLatch reached, final CountDownLatch release) {
    reached.countDown();
    try {
      release.await(WAIT_MS, TimeUnit.MILLISECONDS);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private static void awaitRebuildEnded(final ColumnarProjection projection) {
    final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(WAIT_MS);
    while (ColumnarRebuilder.isQueuedOrRunning(projection) && System.nanoTime() < deadline)
      Thread.yield();
    assertThat(ColumnarRebuilder.isQueuedOrRunning(projection)).as("the rebuild ended").isFalse();
  }

  private ColumnarProjection projection() {
    return manager().projection(database.getSchema().getType("Asset").getBuckets(false).stream().mapToInt(Bucket::getFileId).findFirst()
        .orElseThrow());
  }

  private ColumnarIndexManager manager() {
    return ((LocalDatabase) ((DatabaseInternal) database).getEmbedded()).getColumnarIndexManager();
  }
}
