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
import com.arcadedb.engine.Bucket;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Final review A-I3: a drop fences the background jobs of its projection, as a close does, so a projection recreated on the same bucket
 * never shares a file name, the manifest tmp file included, with a job of the dropped one.
 */
class ColumnarDropFenceTest extends TestHelper {
  private static final long WAIT_MS = 30_000;

  private final ExecutorService flusher = Executors.newSingleThreadExecutor();
  private final ExecutorService dropper = Executors.newSingleThreadExecutor();

  @Override
  protected void endTest() {
    ColumnarTestHooks.killPoint = null;
    flusher.shutdownNow();
    dropper.shutdownNow();
  }

  @Test
  void aRecreatedProjectionKeepsItsFilesWhenTheDroppedOnesFlushEnds() throws Exception {
    database.getConfiguration().setValue(GlobalConfiguration.COLUMNAR_FLUSH_INTERVAL, TimeUnit.HOURS.toMillis(1));
    database.command("sql", "CREATE DOCUMENT TYPE Asset BUCKETS 1");
    database.command("sql", "CREATE PROPERTY Asset.brand STRING");
    database.command("sql", "CREATE INDEX ON Asset (brand) COLUMNAR");
    database.transaction(() -> {
      for (int i = 0; i < 300; i++)
        database.newDocument("Asset").set("brand", "b" + (i % 11)).save();
    });
    assertThat(manager().awaitCaughtUp(WAIT_MS)).isTrue();
    final int bucketId = bucketId();
    final String indexName = database.getSchema().getType("Asset").getAllIndexes(false).stream()
        .filter(i -> i.getType() == Schema.INDEX_TYPE.COLUMNAR).findFirst().orElseThrow().getName();
    final ColumnarProjection dropped = manager().projection(bucketId);

    // the flush of the projection about to be dropped stops once its segment is written, and goes on when released
    final CountDownLatch flushHeld = new CountDownLatch(1);
    final CountDownLatch releaseFlush = new CountDownLatch(1);
    final AtomicBoolean fired = new AtomicBoolean();
    ColumnarTestHooks.killPoint = name -> {
      if (name.equals(ColumnarFlusher.FLUSH_SEGMENT_WRITTEN) && fired.compareAndSet(false, true)) {
        flushHeld.countDown();
        try {
          releaseFlush.await(WAIT_MS, TimeUnit.MILLISECONDS);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    };
    final Future<?> flush = flusher.submit(() -> {
      manager().flushNow();
      return null;
    });
    assertThat(flushHeld.await(WAIT_MS, TimeUnit.MILLISECONDS)).isTrue();

    final AtomicReference<Thread> dropThread = new AtomicReference<>();
    final Future<?> drop = dropper.submit(() -> {
      dropThread.set(Thread.currentThread());
      database.command("sql", "DROP INDEX `" + indexName + "`");
      return null;
    });
    // either the drop returned while the flush is held (no fence), or it waits for the flush: then nothing can be recreated before
    // the flush ended, and the recreate below runs once the drop returned
    final boolean droppedWhileHeld = awaitDoneOrParked(drop, dropThread);
    if (droppedWhileHeld)
      database.command("sql", "CREATE INDEX ON Asset (brand) COLUMNAR");
    releaseFlush.countDown();
    flush.get(WAIT_MS, TimeUnit.MILLISECONDS);
    drop.get(WAIT_MS, TimeUnit.MILLISECONDS);
    if (!droppedWhileHeld)
      database.command("sql", "CREATE INDEX ON Asset (brand) COLUMNAR");
    assertThat(manager().awaitCaughtUp(WAIT_MS)).isTrue();

    final ColumnarProjection recreated = manager().projection(bucketId);
    assertThat(recreated).isNotSameAs(dropped);
    for (final String file : manifestFiles(recreated))
      assertThat(recreated.bucketDir().resolve(file)).as("a file the recreated manifest names").exists();
    try (final Stream<Path> entries = Files.list(recreated.bucketDir())) {
      assertThat(entries.map(p -> p.getFileName().toString()).filter(n -> n.endsWith(ColumnarStore.TMP_SUFFIX)).toList())
          .as("no manifest tmp file is left behind").isEmpty();
    }
    FacetOracle.assertEquivalent(database, "Asset", "brand", null);

    reopenDatabase();
    final ColumnarProjection reopened = manager().projection(bucketId);
    assertThat(reopened.isUsable()).as("the recreated projection loads").isTrue();
    assertThat(manager().getStats().rebuilds("Asset", ColumnarProjection.MISSING)).isZero();
    assertThat(manager().getStats().rebuilds("Asset", ColumnarProjection.CORRUPT)).isZero();
    FacetOracle.assertEquivalent(database, "Asset", "brand", null);
  }

  /** True when the drop returned; false once its thread parks inside it (waiting for the held flush). */
  private static boolean awaitDoneOrParked(final Future<?> drop, final AtomicReference<Thread> dropThread) {
    final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(WAIT_MS);
    while (System.nanoTime() < deadline) {
      if (drop.isDone())
        return true;
      final Thread thread = dropThread.get();
      if (thread != null && thread.getState() == Thread.State.TIMED_WAITING && !drop.isDone())
        return false;
      Thread.yield();
    }
    throw new AssertionError("The drop neither returned nor waited");
  }

  private static List<String> manifestFiles(final ColumnarProjection projection) {
    final ColumnarManifest manifest = projection.manifest();
    final List<String> files = new ArrayList<>();
    for (final ColumnarManifest.SegmentEntry segment : manifest.segments())
      files.add(segment.file());
    for (final ColumnarManifest.DictionaryEntry dictionary : manifest.dictionaries())
      files.add(dictionary.file());
    if (manifest.ownerMapFile() != null)
      files.add(manifest.ownerMapFile());
    return files;
  }

  private int bucketId() {
    return database.getSchema().getType("Asset").getBuckets(false).stream().mapToInt(Bucket::getFileId).findFirst().orElseThrow();
  }

  private ColumnarIndexManager manager() {
    return ((LocalDatabase) ((DatabaseInternal) database).getEmbedded()).getColumnarIndexManager();
  }
}
