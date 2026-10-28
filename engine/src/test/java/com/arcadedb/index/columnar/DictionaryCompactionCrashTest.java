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
import com.arcadedb.engine.Bucket;
import com.arcadedb.utility.CallableNoReturn;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Task 24: a database killed ({@link DatabaseInternal#kill()}, no I/O) with a dictionary compaction stopped at a kill point reopens on
 * exactly one dictionary generation, the one its manifest names (the old one before the rename, the compacted one after it), with the
 * orphan generation and segment files deleted, equal to GROUP BY and with no rebuild.
 */
@Tag("slow")
class DictionaryCompactionCrashTest extends TestHelper {
  private static final String[] PROPERTIES = { "brand", "tags", "fileSize", "publishDate", "author" };
  private static final String   TYPE       = ColumnarTestData.TYPE;
  private static final long     HANG_MS    = 10_000;

  @Override
  protected void beginTest() {
    // only the test flushes: a background flush by age would publish before the compaction
    GlobalConfiguration.COLUMNAR_FLUSH_INTERVAL.setValue(Long.MAX_VALUE);
  }

  @Override
  protected void endTest() {
    ColumnarTestHooks.killPoint = null;
  }

  @ParameterizedTest
  @ValueSource(strings = { DictionaryCompactor.DICTIONARY_WRITTEN, DictionaryCompactor.SEGMENTS_WRITTEN, "publish.manifestTmpWritten",
      "publish.manifestRenamed" })
  void killMidCompactionRecoversWithoutRebuild(final String point) throws Exception {
    new ColumnarTestData(31).createSchema(database);
    final List<RID> deleted = new ArrayList<>();
    database.transaction(() -> {
      for (int i = 0; i < 2_000; i++) {
        final RID rid = database.newDocument(TYPE).set("brand", "brand-" + i).set("tags", List.of("tag-" + i, "common")).save()
            .getIdentity();
        if (i < 1_200)
          deleted.add(rid);
      }
    });
    FacetOracle.awaitCaughtUp(database);
    manager().flushNow();
    database.transaction(() -> {
      for (final RID rid : deleted)
        database.deleteRecord(database.lookupByRID(rid, true));
    });
    FacetOracle.awaitCaughtUp(database);
    manager().flushNow();
    // unflushed rows at the kill point: the compaction remaps them in the overlay, the reopened database recovers them from the WAL
    database.transaction(() -> {
      for (int i = 0; i < 40; i++)
        database.newDocument(TYPE).set("brand", i % 2 == 0 ? "brand-" + (1_200 + i) : "overlay-" + i).set("tags", List.of("tag-o" + i))
            .save();
    });
    FacetOracle.awaitCaughtUp(database);
    assertThat(manager().projection(bucketId()).overlay().rows()).isEqualTo(40);

    final int bucketId = bucketId();
    final ColumnarProjection projection = manager().projection(bucketId);
    final int brandIndex = indexFileId(projection, "brand");
    final int generationBefore = dictionaryEntry(projection.manifest(), brandIndex).generation();
    assertThat(DictionaryCompactor.shouldCompact(projection, "brand", 0.5f)).isTrue();

    final KillPoint kill = KillPoint.arm(point);
    try {
      crashAt(kill, () -> DictionaryCompactor.compact(projection, "brand"));
    } finally {
      kill.disarm();
    }

    database = factory.open();
    final ColumnarProjection reopened = manager().projection(bucketId);
    final ColumnarManifest manifest = reopened.manifest();
    final ColumnarManifest.DictionaryEntry entry = dictionaryEntry(manifest, brandIndex);
    final boolean renamed = point.equals("publish.manifestRenamed");
    assertThat(entry.generation()).isEqualTo(renamed ? generationBefore + 1 : generationBefore);
    // the 20 new brands of the overlay rows: in generation n + 1 by the compaction, or appended to n again by the recovery
    FacetOracle.awaitCaughtUp(database);
    assertThat(reopened.dictionary("brand").size()).isEqualTo(renamed ? 820 : 2_020);
    // one generation of the column on disk, the one the manifest names; no file the manifest does not name
    final String prefix = "dict-" + brandIndex + ".";
    final String[] generations = bucketDir(bucketId).toFile().list((dir, name) -> name.startsWith(prefix));
    assertThat(generations).containsExactly(entry.file());
    final String[] dataFiles = bucketDir(bucketId).toFile().list((dir, name) -> name.startsWith("seg-") || name.startsWith("dict-"));
    assertThat(manifest.files()).contains(dataFiles);
    assertThat(manifest.segments()).hasSize(1);

    for (final String property : PROPERTIES)
      FacetOracle.assertEquivalent(database, TYPE, property, null);
    assertThat(manager().getStats().rebuildsTotal()).isZero();
    // the reopened projection compacts what the crash interrupted
    if (!renamed) {
      assertThat(manager().compactDictionaryNow(bucketId, "brand")).isTrue();
      FacetOracle.assertEquivalent(database, TYPE, "brand", null);
    }
  }

  /** Runs the compaction on its own thread until it reaches the point, kills the database there, ends it as crashed, closes it. */
  private void crashAt(final KillPoint kill, final CallableNoReturn operation) throws Exception {
    final AtomicReference<Throwable> ended = new AtomicReference<>();
    final Thread worker = new Thread(() -> {
      try {
        operation.call();
      } catch (final Throwable e) {
        ended.set(e);
      }
    }, "columnar-crash-compactor");
    worker.start();
    // a hang detector, not a latency bound
    assertThat(kill.awaitReached(HANG_MS)).isTrue();
    ((DatabaseInternal) database).kill();
    kill.releaseAsCrashed();
    worker.join(HANG_MS);
    assertThat(worker.isAlive()).isFalse();
    assertThat(ended.get()).isInstanceOf(ColumnarTestHooks.SimulatedCrash.class);
    database.close();
  }

  private static ColumnarManifest.DictionaryEntry dictionaryEntry(final ColumnarManifest manifest, final int indexFileId) {
    for (final ColumnarManifest.DictionaryEntry entry : manifest.dictionaries())
      if (entry.indexFileId() == indexFileId)
        return entry;
    throw new AssertionError("The manifest has no dictionary of index " + indexFileId);
  }

  private static int indexFileId(final ColumnarProjection projection, final String property) {
    for (final ColumnDefinition column : projection.columns())
      if (column.property().equals(property))
        return column.indexFileId();
    throw new AssertionError("No column " + property);
  }

  private ColumnarIndexManager manager() {
    return ((LocalDatabase) ((DatabaseInternal) database).getEmbedded()).getColumnarIndexManager();
  }

  private int bucketId() {
    return database.getSchema().getType(TYPE).getBuckets(false).stream().mapToInt(Bucket::getFileId).findFirst().orElseThrow();
  }

  private Path bucketDir(final int bucketId) {
    return ColumnarStore.bucketDir(Path.of(database.getDatabasePath()), bucketId);
  }
}
