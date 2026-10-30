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
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Task 24, dictionary compaction (spec 5.4): a dictionary whose ordinals are mostly unreferenced by live rows is rewritten as a new
 * generation holding only the referenced values, the segments and the live overlay re-encoded, in one manifest swap; readers that
 * acquired before keep the old pair until they close.
 * <p>
 * The R30 data: {@code brands} brands with two records each, every record with a tag of its own, then every record of the first
 * {@code deleted} brands deleted: with 2_000 brands and 1_200 deleted, 60% of the brand and tag ordinals are unreferenced.
 */
class DictionaryCompactorTest extends TestHelper {
  private static final String TYPE         = ColumnarTestData.TYPE;
  private static final int    PER_BRAND    = 2;
  private static final long   HANG_MS      = 30_000;
  private static final long   TEN_MINUTES  = 600_000;

  private final List<List<RID>> byBrand = new ArrayList<>();

  @Override
  protected void beginTest() {
    // only the test flushes and merges: a background flush by age would publish what a test keeps in the overlay
    GlobalConfiguration.COLUMNAR_FLUSH_INTERVAL.setValue(Long.MAX_VALUE);
  }

  @Override
  protected void endTest() {
    ColumnarTestHooks.killPoint = null;
  }

  @Test
  void deadRatioCountsOnlyLiveReferences() throws Exception {
    new ColumnarTestData(1).createSchema(database);
    insertBrands(2_000);
    FacetOracle.awaitCaughtUp(database);
    // every ordinal is referenced, in the overlay first, then in a segment
    assertThat(DictionaryCompactor.deadOrdinalRatio(projection(), "brand")).isEqualTo(0.0);
    manager().flushNow();
    assertThat(DictionaryCompactor.deadOrdinalRatio(projection(), "brand")).isEqualTo(0.0);

    deleteBrands(0, 1_200);
    FacetOracle.awaitCaughtUp(database);
    // the deleted rows are dead in the segment before any merge: they reference nothing
    assertThat(DictionaryCompactor.deadOrdinalRatio(projection(), "brand")).isEqualTo(0.6);
    flushAndMergeAll();
    assertThat(DictionaryCompactor.deadOrdinalRatio(projection(), "brand")).isEqualTo(0.6);
    assertThat(DictionaryCompactor.deadOrdinalRatio(projection(), "tags")).isEqualTo(0.6);
    // a LONG column has no dictionary
    assertThat(DictionaryCompactor.deadOrdinalRatio(projection(), "fileSize")).isEqualTo(0.0);
  }

  @Test
  void compactionDropsUnreferencedOrdinals() throws Exception {
    r30();
    final ColumnarProjection projection = projection();
    final int brandIndex = indexFileId("brand");
    final ColumnarManifest.DictionaryEntry before = dictionaryEntry(projection.manifest(), brandIndex);
    assertThat(DictionaryCompactor.shouldCompact(projection, "brand", 0.5f)).isTrue();

    DictionaryCompactor.compact(projection, "brand");

    final GlobalDictionary compacted = projection.dictionary("brand");
    assertThat(compacted.size()).isEqualTo(800);
    // the survivors keep their insert order, densely numbered
    assertThat(compacted.ordinalOf("brand-1200")).isZero();
    assertThat(compacted.ordinalOf("brand-1999")).isEqualTo(799);
    assertThat(compacted.ordinalOf("brand-0")).isEqualTo(-1);
    assertRankViewIsValueOrder(compacted, 800);
    final ColumnarManifest.DictionaryEntry after = dictionaryEntry(projection.manifest(), brandIndex);
    assertThat(after.generation()).isEqualTo(before.generation() + 1);
    assertThat(after.entries()).isEqualTo(800);
    // nobody holds the old generation: its file is gone, the new one is named by the manifest
    assertThat(bucketDir().resolve(before.file())).doesNotExist();
    assertThat(bucketDir().resolve(after.file())).exists();

    final ColumnarStats stats = manager().getStats();
    assertThat(stats.dictionaryOrdinalsReclaimed(TYPE, "brand")).isEqualTo(1_200);
    assertThat(stats.dictionaryCompactions(TYPE, "brand")).isEqualTo(1);
    assertThat(stats.dictionaryCompactions(TYPE, "tags")).isZero();
    assertThat(stats.rebuildsTotal()).isZero();
    // a compaction changes the dictionary generation, not the column definition: the column stays published and usable
    assertThat(projection.isUsable()).isTrue();
    assertThat(projection.isPublishedWith(brandIndex)).isTrue();
    assertThat(DictionaryCompactor.deadOrdinalRatio(projection, "brand")).isEqualTo(0.0);
    FacetOracle.assertEquivalent(database, TYPE, "brand", null);
  }

  @Test
  void equivalenceAfterCompaction() throws Exception {
    r30();
    DictionaryCompactor.compact(projection(), "brand");
    DictionaryCompactor.compact(projection(), "tags");
    assertThat(projection().dictionary("brand").size()).isEqualTo(800);
    assertThat(projection().dictionary("tags").size()).isEqualTo(1_600);
    for (final String property : List.of("brand", "tags"))
      FacetOracle.assertEquivalent(database, TYPE, property, null);

    final ColumnarTestData data = new ColumnarTestData(24);
    data.deleteRatio = 0.1;
    data.mutate(database, 2_000);
    for (final String property : List.of("brand", "tags", "fileSize", "publishDate", "author"))
      FacetOracle.assertEquivalent(database, TYPE, property, null);
    // and once the overlay written with the new generation is a segment too
    manager().flushNow();
    for (final String property : List.of("brand", "tags"))
      FacetOracle.assertEquivalent(database, TYPE, property, null);
  }

  @Test
  void readersKeepTheOldPairUntilReleased() throws Exception {
    r30();
    final ColumnarProjection projection = projection();
    final String oldFile = dictionaryEntry(projection.manifest(), indexFileId("brand")).file();

    final ColumnarSnapshot s1 = projection.acquire();
    try {
      final Map<Object, Long> before = counts(s1, "brand");
      assertThat(before).hasSize(800);
      DictionaryCompactor.compact(projection, "brand");

      try (final ColumnarSnapshot s2 = projection.acquire()) {
        assertThat(s1.dictionary("brand")).isNotSameAs(s2.dictionary("brand"));
        assertThat(s1.dictionary("brand").size()).isEqualTo(2_000);
        assertThat(s2.dictionary("brand").size()).isEqualTo(800);
        assertThat(counts(s1, "brand")).isEqualTo(before);
        assertThat(counts(s2, "brand")).isEqualTo(before);
      }
      assertThat(bucketDir().resolve(oldFile)).exists();
    } finally {
      s1.close();
    }
    assertThat(bucketDir().resolve(oldFile)).doesNotExist();
  }

  @Test
  void overlayBlocksAreCopiedNotRemappedInPlace() throws Exception {
    r30();
    // unflushed rows: surviving brands, a brand whose ordinal is unreferenced in the segments, and new brands
    database.transaction(() -> {
      for (int i = 0; i < 30; i++)
        database.newDocument(TYPE).set("brand", "brand-" + (1_200 + i)).set("tags", List.of("tag-x" + i)).save();
      database.newDocument(TYPE).set("brand", "brand-3").save();
      database.newDocument(TYPE).set("brand", "overlay-new").save();
    });
    FacetOracle.awaitCaughtUp(database);
    final ColumnarProjection projection = projection();

    try (final ColumnarSnapshot s1 = projection.acquire()) {
      final ColumnarSegmentView overlay = lastView(s1);
      final Map<Long, Integer> ordinalsBefore = overlayOrdinals(overlay, "brand");
      assertThat(ordinalsBefore).hasSize(32);
      final Map<Long, Object> valuesBefore = decode(ordinalsBefore, s1.dictionary("brand"));
      assertThat(valuesBefore.values()).contains("brand-3", "overlay-new");

      DictionaryCompactor.compact(projection, "brand");

      // s1's overlay view still holds the same blocks: the ordinals of generation n, decoded with generation n
      assertThat(overlayOrdinals(overlay, "brand")).isEqualTo(ordinalsBefore);
      assertThat(decode(overlayOrdinals(overlay, "brand"), s1.dictionary("brand"))).isEqualTo(valuesBefore);
      try (final ColumnarSnapshot s2 = projection.acquire()) {
        final Map<Long, Integer> ordinalsAfter = overlayOrdinals(lastView(s2), "brand");
        assertThat(ordinalsAfter).isNotEqualTo(ordinalsBefore);
        assertThat(decode(ordinalsAfter, s2.dictionary("brand"))).isEqualTo(valuesBefore);
      }
    }
    FacetOracle.assertEquivalent(database, TYPE, "brand", null);
    FacetOracle.assertEquivalent(database, TYPE, "tags", null);
  }

  @Test
  void queriesDoNotBlockDuringCompaction() throws Exception {
    r30();
    final ColumnarProjection projection = projection();
    final Map<Object, Long> expected = FacetOracle.expectedCounts(database, TYPE, "brand", null);
    final KillPoint kill = KillPoint.arm(DictionaryCompactor.SEGMENTS_WRITTEN);
    final AtomicReference<Throwable> ended = new AtomicReference<>();
    final Thread compactor = new Thread(() -> {
      try {
        DictionaryCompactor.compact(projection, "brand");
      } catch (final Throwable e) {
        ended.set(e);
      }
    }, "columnar-compactor");
    try {
      compactor.start();
      assertThat(kill.awaitReached(HANG_MS)).isTrue();
      // the compactor holds the maintenance slot and is blocked: a snapshot and a full count still complete on this thread
      try (final ColumnarSnapshot snapshot = projection.acquire()) {
        assertThat(snapshot.isUsable()).isTrue();
        assertThat(counts(snapshot, "brand")).isEqualTo(expected);
      }
      assertThat(FacetOracle.projectionCounts(database, TYPE, "brand", null, false)).isEqualTo(expected);
    } finally {
      kill.disarm();
    }
    compactor.join(HANG_MS);
    assertThat(compactor.isAlive()).isFalse();
    assertThat(ended.get()).isInstanceOf(ColumnarTestHooks.SimulatedCrash.class);
    assertThat(projection.dictionary("brand").size()).isEqualTo(2_000);
  }

  @Test
  void ordinalsAddedDuringCompactionSurvive() throws Exception {
    r30();
    final ColumnarProjection projection = projection();
    final Pause pause = Pause.at(DictionaryCompactor.SEGMENTS_WRITTEN);
    final AtomicReference<Throwable> ended = new AtomicReference<>();
    final Thread compactor = new Thread(() -> {
      try {
        DictionaryCompactor.compact(projection, "brand");
      } catch (final Throwable e) {
        ended.set(e);
      }
    }, "columnar-compactor");
    compactor.start();
    try {
      assertThat(pause.awaitReached()).isTrue();
      // applied while the compactor waits: 50 new values appended to generation n, and a value the compactor found unreferenced
      database.transaction(() -> {
        for (int i = 0; i < 50; i++)
          database.newDocument(TYPE).set("brand", "late-" + i).save();
        database.newDocument(TYPE).set("brand", "brand-7").save();
      });
      FacetOracle.awaitCaughtUp(database);
      assertThat(projection.dictionary("brand").size()).isEqualTo(2_050);
    } finally {
      pause.release();
    }
    compactor.join(HANG_MS);
    assertThat(compactor.isAlive()).isFalse();
    assertThat(ended.get()).isNull();

    final GlobalDictionary compacted = projection.dictionary("brand");
    // 800 survivors, the 50 late values, and brand-7 brought back by the overlay row that references it
    assertThat(compacted.size()).isEqualTo(851);
    for (int i = 0; i < 50; i++)
      assertThat(compacted.ordinalOf("late-" + i)).as("late-%s", i).isGreaterThanOrEqualTo(800);
    assertThat(compacted.ordinalOf("brand-7")).isGreaterThanOrEqualTo(0);
    // the rank view holds the 800 values written before the publish; the 51 the publish appended are ordered by compare() alone
    assertRankViewIsValueOrder(compacted, 800);
    assertThat(manager().getStats().dictionaryOrdinalsReclaimed(TYPE, "brand")).isEqualTo(1_199);
    FacetOracle.assertEquivalent(database, TYPE, "brand", null);
    manager().flushNow();
    FacetOracle.assertEquivalent(database, TYPE, "brand", null);
  }

  @Test
  void compactionNeverOverlapsAFlush() throws Exception {
    r30();
    database.transaction(() -> {
      for (int i = 0; i < 20; i++)
        database.newDocument(TYPE).set("brand", "brand-" + (1_500 + i)).save();
    });
    FacetOracle.awaitCaughtUp(database);
    final ColumnarProjection projection = projection();
    final List<String> reached = new CopyOnWriteArrayList<>();
    final Pause pause = Pause.at(ColumnarFlusher.FLUSH_SEGMENT_WRITTEN, reached::add);
    final AtomicReference<Throwable> flushEnded = new AtomicReference<>();
    final AtomicReference<Throwable> compactionEnded = new AtomicReference<>();
    final AtomicReference<Boolean> compacted = new AtomicReference<>();
    final Thread flusher = new Thread(() -> {
      try {
        manager().flushNow();
      } catch (final Throwable e) {
        flushEnded.set(e);
      }
    }, "columnar-flusher");
    final Thread compactor = new Thread(() -> {
      try {
        compacted.set(manager().compactDictionaryNow(bucketId(), "brand"));
      } catch (final Throwable e) {
        compactionEnded.set(e);
      }
    }, "columnar-compactor");
    flusher.start();
    try {
      assertThat(pause.awaitReached()).isTrue();
      // the flush holds the maintenance slot: a compaction is refused at once, it never runs beside the flush
      assertThatThrownBy(() -> DictionaryCompactor.compact(projection, "brand")).isInstanceOf(
          ColumnarFlusher.MaintenanceBusyException.class);
      compactor.start();
    } finally {
      pause.release();
    }
    flusher.join(HANG_MS);
    compactor.join(HANG_MS);
    assertThat(flusher.isAlive()).isFalse();
    assertThat(compactor.isAlive()).isFalse();
    assertThat(flushEnded.get()).isNull();
    assertThat(compactionEnded.get()).isNull();
    assertThat(compacted.get()).isTrue();
    // every point of the compaction comes after the flush published its manifest
    final int flushPublished = reached.indexOf("publish.manifestRenamed");
    final int compactionStarted = reached.indexOf(DictionaryCompactor.DICTIONARY_WRITTEN);
    assertThat(flushPublished).isGreaterThan(reached.indexOf(ColumnarFlusher.FLUSH_SEGMENT_WRITTEN));
    assertThat(compactionStarted).isGreaterThan(flushPublished);
    assertThat(projection.dictionary("brand").size()).isEqualTo(800);
    FacetOracle.assertEquivalent(database, TYPE, "brand", null);
  }

  @Test
  void belowThresholdDoesNothing() throws Exception {
    new ColumnarTestData(1).createSchema(database);
    insertBrands(2_000);
    FacetOracle.awaitCaughtUp(database);
    deleteBrands(0, 600);
    flushAndMergeAll();
    final ColumnarProjection projection = projection();
    final float ratio = database.getConfiguration().getValueAsFloat(GlobalConfiguration.COLUMNAR_DICTIONARY_COMPACT_RATIO);
    assertThat(ratio).isEqualTo(0.5f);
    assertThat(DictionaryCompactor.deadOrdinalRatio(projection, "brand")).isEqualTo(0.3);
    assertThat(DictionaryCompactor.shouldCompact(projection, "brand", ratio)).isFalse();
    // the boundary is inclusive
    assertThat(DictionaryCompactor.shouldCompact(projection, "brand", 0.3f)).isTrue();
  }

  @Test
  void keyDefault() {
    assertThat(GlobalConfiguration.COLUMNAR_DICTIONARY_COMPACT_RATIO.getDefValue()).isEqualTo(0.5f);
    assertThat(GlobalConfiguration.COLUMNAR_DICTIONARY_COMPACT_RATIO.getKey()).isEqualTo("arcadedb.columnar.dictionaryCompactRatio");
  }

  @Test
  void compactDictionaryNowIsSynchronous() throws Exception {
    r30();
    final ColumnarProjection projection = projection();
    final int brandIndex = indexFileId("brand");
    final int generation = dictionaryEntry(projection.manifest(), brandIndex).generation();
    final GlobalDictionary old = projection.dictionary("brand");

    assertThat(manager().compactDictionaryNow(bucketId(), "brand")).isTrue();
    // published when the call returns: the next snapshot decodes with generation n + 1
    assertThat(dictionaryEntry(projection.manifest(), brandIndex).generation()).isEqualTo(generation + 1);
    try (final ColumnarSnapshot snapshot = projection.acquire()) {
      assertThat(snapshot.dictionary("brand")).isNotSameAs(old);
      assertThat(snapshot.dictionary("brand").size()).isEqualTo(800);
    }
    // 800 entries now, below the 1024 floor
    assertThat(manager().compactDictionaryNow(bucketId(), "brand")).isFalse();
    assertThat(manager().getStats().dictionaryCompactions(TYPE, "brand")).isEqualTo(1);
  }

  @Test
  void compactDictionaryNowRefusesSmallDictionaries() throws Exception {
    new ColumnarTestData(1).createSchema(database);
    insertBrands(1_000);
    FacetOracle.awaitCaughtUp(database);
    deleteBrands(0, 900);
    flushAndMergeAll();
    assertThat(DictionaryCompactor.deadOrdinalRatio(projection(), "brand")).isEqualTo(0.9);
    assertThat(manager().compactDictionaryNow(bucketId(), "brand")).isFalse();
    assertThat(projection().dictionary("brand").size()).isEqualTo(1_000);
    assertThat(manager().getStats().dictionaryCompactions(TYPE, "brand")).isZero();
  }

  @Test
  void compactDictionaryNowFindsNothingToReclaim() throws Exception {
    new ColumnarTestData(1).createSchema(database);
    insertBrands(2_000);
    FacetOracle.awaitCaughtUp(database);
    manager().flushNow();
    final GlobalDictionary before = projection().dictionary("brand");
    assertThat(manager().compactDictionaryNow(bucketId(), "brand")).isFalse();
    assertThat(projection().dictionary("brand")).isSameAs(before);
  }

  @Test
  void mergeThatDropsRowsTriggersCompactionAtMostEveryTenMinutes() throws Exception {
    final long[] now = { 1_000_000L };
    manager().setClockForTesting(() -> now[0]);
    new ColumnarTestData(1).createSchema(database);
    insertBrands(2_000);
    FacetOracle.awaitCaughtUp(database);
    manager().flushNow();
    final ColumnarStats stats = manager().getStats();

    // round 1: the merge drops 2_400 rows; brand (0.6) and tags (0.6) are compacted
    deleteBrands(0, 1_200);
    FacetOracle.awaitCaughtUp(database);
    manager().maintainForTesting(bucketId());
    assertThat(stats.mergeCount(TYPE)).isEqualTo(1);
    assertThat(stats.dictionaryCompactions(TYPE, "brand")).isEqualTo(1);
    assertThat(stats.dictionaryCompactions(TYPE, "tags")).isEqualTo(1);
    assertThat(projection().dictionary("tags").size()).isEqualTo(1_600);

    // round 2, same minute: one record of every surviving brand deleted, tags reach 0.5 again, but the property was checked just now
    deleteOnePerBrand(1_200, 2_000, 0);
    FacetOracle.awaitCaughtUp(database);
    manager().maintainForTesting(bucketId());
    assertThat(stats.mergeCount(TYPE)).isEqualTo(2);
    assertThat(DictionaryCompactor.deadOrdinalRatio(projection(), "tags")).isEqualTo(0.5);
    assertThat(stats.dictionaryCompactions(TYPE, "tags")).isEqualTo(1);

    // round 3, ten minutes later: the next merge that drops rows checks again; brand is below the 1024 floor
    now[0] += TEN_MINUTES;
    deleteOnePerBrand(1_200, 1_600, 1);
    FacetOracle.awaitCaughtUp(database);
    manager().maintainForTesting(bucketId());
    assertThat(stats.mergeCount(TYPE)).isEqualTo(3);
    assertThat(stats.dictionaryCompactions(TYPE, "tags")).isEqualTo(2);
    assertThat(stats.dictionaryOrdinalsReclaimed(TYPE, "tags")).isEqualTo(2_400 + 1_200);
    assertThat(stats.dictionaryCompactions(TYPE, "brand")).isEqualTo(1);
    for (final String property : List.of("brand", "tags"))
      FacetOracle.assertEquivalent(database, TYPE, property, null);
  }

  // ------------------------------------------------------------------------------------------------------------------- helpers

  /** The R30 data: 2_000 brands, every record of 1_200 of them deleted, flushed and merged into one segment. */
  private void r30() throws Exception {
    new ColumnarTestData(1).createSchema(database);
    insertBrands(2_000);
    FacetOracle.awaitCaughtUp(database);
    deleteBrands(0, 1_200);
    flushAndMergeAll();
    assertThat(projection().manifest().segments()).hasSize(1);
    assertThat(projection().dictionary("brand").size()).isEqualTo(2_000);
  }

  /** Two records per brand, in two transactions, each record with a tag of its own. */
  private void insertBrands(final int brands) {
    for (int b = 0; b < brands; b++)
      byBrand.add(new ArrayList<>());
    for (int copy = 0; copy < PER_BRAND; copy++) {
      final int c = copy;
      database.transaction(() -> {
        for (int b = 0; b < brands; b++)
          byBrand.get(b).add(database.newDocument(TYPE).set("brand", "brand-" + b).set("tags", List.of("tag-" + (c * brands + b)))
              .set("fileSize", (long) b).save().getIdentity());
      });
    }
  }

  /** Deletes every record of brands {@code from..to-1}. */
  private void deleteBrands(final int from, final int to) {
    database.transaction(() -> {
      for (int b = from; b < to; b++)
        for (final RID rid : byBrand.get(b))
          database.deleteRecord(database.lookupByRID(rid, true));
    });
  }

  /** Deletes the {@code copy}-th record of brands {@code from..to-1}. */
  private void deleteOnePerBrand(final int from, final int to, final int copy) {
    database.transaction(() -> {
      for (int b = from; b < to; b++)
        database.deleteRecord(database.lookupByRID(byBrand.get(b).get(copy), true));
    });
  }

  /** Applies, flushes (dead sets persisted) and merges every segment into one, dropping the dead rows. */
  private void flushAndMergeAll() throws Exception {
    FacetOracle.awaitCaughtUp(database);
    manager().flushNow();
    final List<ColumnarManifest.SegmentEntry> segments = projection().manifest().segments();
    final long[] ids = new long[segments.size()];
    for (int i = 0; i < ids.length; i++)
      ids[i] = segments.get(i).segmentId();
    ColumnarFlusher.merge(projection(), ids);
    FacetOracle.awaitCaughtUp(database);
  }

  /** Facet counts of the property through the snapshot's own dictionary generation, every view, live rows only. */
  private static Map<Object, Long> counts(final ColumnarSnapshot snapshot, final String property) {
    final GlobalDictionary dictionary = snapshot.dictionary(property);
    final Map<Object, Long> counts = new HashMap<>();
    for (final ColumnarSegmentView view : snapshot.segments()) {
      final ColumnReader column = view.column(property);
      if (column != null)
        column.forEachOrdinal(view.live(), (position, ordinal) -> counts.merge(dictionary.valueOf(ordinal), 1L, Long::sum));
    }
    return counts;
  }

  /**
   * The rank contract of the published generation: its rank view holds the first {@code ranked} ordinals in value order, and
   * compare() agrees with the value order for every pair that involves an ordinal outside the view (ranks of newer values may tie,
   * compare() breaks the ties).
   */
  private static void assertRankViewIsValueOrder(final GlobalDictionary dictionary, final int ranked) {
    final int[] sorted = dictionary.sortedOrdinals();
    assertThat(sorted).hasSize(ranked);
    for (int i = 1; i < sorted.length; i++) {
      assertThat(dictionary.rank(sorted[i - 1])).isLessThan(dictionary.rank(sorted[i]));
      assertThat(dictionary.compare(sorted[i - 1], sorted[i])).isNegative();
      assertThat(value(dictionary, sorted[i - 1]).compareTo(value(dictionary, sorted[i]))).isNegative();
    }
    for (int newer = ranked; newer < dictionary.size(); newer++)
      for (int other = 0; other < dictionary.size(); other++)
        assertThat(Integer.signum(dictionary.compare(newer, other))).as("compare(%s, %s)", newer, other)
            .isEqualTo(Integer.signum(value(dictionary, newer).compareTo(value(dictionary, other))));
  }

  private static String value(final GlobalDictionary dictionary, final int ordinal) {
    return (String) dictionary.valueOf(ordinal);
  }

  private static ColumnarSegmentView lastView(final ColumnarSnapshot snapshot) {
    return snapshot.segments().getLast();
  }

  private static Map<Long, Integer> overlayOrdinals(final ColumnarSegmentView overlay, final String property) {
    final Map<Long, Integer> ordinals = new HashMap<>();
    overlay.column(property).forEachOrdinal(overlay.live(), ordinals::put);
    return ordinals;
  }

  private static Map<Long, Object> decode(final Map<Long, Integer> ordinals, final GlobalDictionary dictionary) {
    final Map<Long, Object> values = new HashMap<>();
    ordinals.forEach((position, ordinal) -> values.put(position, dictionary.valueOf(ordinal)));
    return values;
  }

  private static ColumnarManifest.DictionaryEntry dictionaryEntry(final ColumnarManifest manifest, final int indexFileId) {
    for (final ColumnarManifest.DictionaryEntry entry : manifest.dictionaries())
      if (entry.indexFileId() == indexFileId)
        return entry;
    throw new AssertionError("The manifest has no dictionary of index " + indexFileId);
  }

  private int indexFileId(final String property) {
    for (final ColumnDefinition column : projection().columns())
      if (column.property().equals(property))
        return column.indexFileId();
    throw new AssertionError("No column " + property);
  }

  private ColumnarIndexManager manager() {
    return ((LocalDatabase) ((DatabaseInternal) database).getEmbedded()).getColumnarIndexManager();
  }

  private ColumnarProjection projection() {
    return manager().projection(bucketId());
  }

  private int bucketId() {
    return database.getSchema().getType(TYPE).getBuckets(false).stream().mapToInt(Bucket::getFileId).findFirst().orElseThrow();
  }

  private Path bucketDir() {
    return ColumnarStore.bucketDir(Path.of(database.getDatabasePath()), bucketId());
  }

  /**
   * A test hook that blocks the first thread reaching the named point until {@link #release()} and then lets it go on normally (unlike
   * {@link KillPoint}, which crashes it); every point reached is passed to the recorder.
   */
  private static final class Pause implements Consumer<String> {
    private final String           name;
    private final Consumer<String> recorder;
    private final CountDownLatch   reached  = new CountDownLatch(1);
    private final CountDownLatch   released = new CountDownLatch(1);

    private Pause(final String name, final Consumer<String> recorder) {
      this.name = name;
      this.recorder = recorder;
    }

    static Pause at(final String name) {
      return at(name, point -> {
      });
    }

    static Pause at(final String name, final Consumer<String> recorder) {
      final Pause pause = new Pause(name, recorder);
      ColumnarTestHooks.killPoint = pause;
      return pause;
    }

    @Override
    public void accept(final String point) {
      recorder.accept(point);
      if (!name.equals(point) || reached.getCount() == 0)
        return;
      reached.countDown();
      try {
        released.await();
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    boolean awaitReached() throws InterruptedException {
      return reached.await(HANG_MS, TimeUnit.MILLISECONDS);
    }

    /** Lets the blocked thread go on; later reaches pass through. */
    void release() {
      released.countDown();
    }
  }
}
