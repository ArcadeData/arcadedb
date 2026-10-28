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
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.IntHashSet;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ColumnarFlusherTest extends TestHelper {
  private static final ColumnDefinition[] COLUMNS = SegmentRoundTripTest.ALL;
  private static final int                MAX     = SegmentRoundTripTest.MAX_RECORDS_IN_PAGE;
  private static final int                BRAND   = 0;
  private static final int                DELETE  = -1;

  private final ColumnarStats            stats    = new ColumnarStats();
  private final List<ColumnarProjection> opened   = new ArrayList<>();
  private final ExecutorService          executor = Executors.newSingleThreadExecutor();
  private       LocalBucket              bucket;
  private       Path                     bucketDir;

  @Override
  protected void endTest() {
    ColumnarTestHooks.killPoint = null;
    executor.shutdownNow();
    for (final ColumnarProjection projection : opened)
      projection.close();
  }

  private ColumnarProjection open() {
    if (bucket == null) {
      final DocumentType type = database.getSchema().createDocumentType("Asset", 1);
      type.createProperty("brand", Type.STRING);
      bucket = (LocalBucket) type.getBuckets(false).getFirst();
      bucketDir = ColumnarStore.bucketDir(Path.of(database.getDatabasePath()), bucket.getFileId());
    }
    final ColumnarProjection projection = ColumnarProjection.open((DatabaseInternal) database, bucket, COLUMNS, stats);
    opened.add(projection);
    final GlobalDictionary brands = projection.dictionary("brand");
    for (int i = 0; i < 5; i++)
      brands.getOrAdd("b" + i);
    return projection;
  }

  /**
   * One hand-driven apply pass of a page, as Task 15's step 4 does it: {@code slotOrdinal} holds pairs (slot, brand ordinal or DELETE).
   * The page's current overlay block seeds the new one; a changed value answered by a segment is marked dead there, one answered by
   * the frozen overlay is superseded.
   */
  private static void apply(final ColumnarProjection projection, final int page, final int... slotOrdinal) {
    projection.applyLock().lock();
    try {
      final DirtyPageTracker tracker = projection.tracker();
      tracker.mark(new int[] { page }, 1);
      tracker.drainInto(new IntHashSet());

      final TreeMap<Integer, Integer> rows = new TreeMap<>();
      final OverlayPageBlock current = projection.overlay().page(page);
      final RowValues row = new RowValues(COLUMNS.length);
      if (current != null)
        for (int r = 0; r < current.rowCount(); r++) {
          current.readRow(r, row);
          rows.put((int) (current.position(r) % MAX), row.ordinal[BRAND]);
        }
      final PositionBitmap deadNow = new PositionBitmap(MAX);
      for (int i = 0; i < slotOrdinal.length; i += 2) {
        final long position = (long) page * MAX + slotOrdinal[i];
        final long found = projection.liveValueLookup(position, BRAND, row);
        if (found == ColumnarProjection.LOOKUP_FROZEN)
          projection.supersedeFrozen(position);
        else if (found >= 0)
          deadNow.set(position);
        if (slotOrdinal[i + 1] == DELETE)
          rows.remove(slotOrdinal[i]);
        else
          rows.put(slotOrdinal[i], slotOrdinal[i + 1]);
      }
      final RowBuffer buffer = new RowBuffer(MAX, projection.columns());
      final RowValues staged = new RowValues(COLUMNS.length);
      for (final Map.Entry<Integer, Integer> entry : rows.entrySet()) {
        staged.present[BRAND] = true;
        staged.ordinal[BRAND] = entry.getValue();
        buffer.put(entry.getKey(), staged);
      }
      projection.overlay().replacePage(OverlayPageBlock.build(page, MAX, projection.columns(), 1, buffer));
      projection.applyDeadPositions(deadNow);
      projection.publishOverlay();
      tracker.completed(new int[] { page }, 1);
      projection.pagesApplied(new int[] { page }, 1);
    } finally {
      projection.applyLock().unlock();
    }
  }

  /** Brand ordinal counts the snapshot answers over pages 0..63. */
  private static Map<Integer, Integer> counts(final ColumnarSnapshot snapshot) {
    final PositionBitmap all = new PositionBitmap(MAX);
    for (int page = 0; page < 64; page++)
      all.setPage(page);
    final Map<Integer, Integer> counts = new TreeMap<>();
    for (final ColumnarSegmentView view : snapshot.segments()) {
      final ColumnReader reader = view.column("brand");
      if (reader != null)
        reader.forEachOrdinal(all, (position, ordinal) -> counts.merge(ordinal, 1, Integer::sum));
    }
    return counts;
  }

  private static Map<Integer, Integer> counts(final ColumnarProjection projection) {
    try (final ColumnarSnapshot snapshot = projection.acquire()) {
      return counts(snapshot);
    }
  }

  private static Map<Integer, Integer> expected(final int... ordinalCount) {
    final Map<Integer, Integer> result = new TreeMap<>();
    for (int i = 0; i < ordinalCount.length; i += 2)
      result.put(ordinalCount[i], ordinalCount[i + 1]);
    return result;
  }

  private static IntHashSet recoverySet(final ColumnarProjection projection) {
    final IntHashSet pages = new IntHashSet();
    projection.tracker().collectRecoverySet(pages);
    return pages;
  }

  /** Blocks the first thread reaching the point until released; never throws (unlike {@link KillPoint}). */
  private static final class Pause {
    final CountDownLatch reached  = new CountDownLatch(1);
    final CountDownLatch released = new CountDownLatch(1);

    static Pause at(final String point) {
      final Pause pause = new Pause();
      final AtomicBoolean fired = new AtomicBoolean();
      ColumnarTestHooks.killPoint = name -> {
        if (!name.equals(point) || !fired.compareAndSet(false, true))
          return;
        pause.reached.countDown();
        try {
          pause.released.await(30, TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      };
      return pause;
    }

    void awaitReached() throws InterruptedException {
      assertThat(reached.await(30, TimeUnit.SECONDS)).isTrue();
    }

    void release() {
      released.countDown();
    }
  }

  private Future<?> flushInBackground(final ColumnarProjection projection) {
    return executor.submit(() -> {
      ColumnarFlusher.flush(projection, 7);
      return null;
    });
  }

  @Test
  void flushPublishesASegmentAndEmptiesTheOverlay() throws Exception {
    final ColumnarProjection projection = open();
    apply(projection, 0, 1, 1, 2, 2, 3, 3);
    apply(projection, 2, 0, 4);
    final long generation = projection.manifest().generation();

    ColumnarFlusher.flush(projection, 42);

    assertThat(projection.overlay().rows()).isZero();
    assertThat(projection.currentOverlaysForTesting()).hasSize(1);
    final ColumnarManifest manifest = projection.manifest();
    assertThat(manifest.generation()).isEqualTo(generation + 1);
    assertThat(manifest.lastTransactionId()).isEqualTo(42);
    assertThat(manifest.segments()).hasSize(1);
    final ColumnarManifest.SegmentEntry entry = manifest.segments().getFirst();
    assertThat(entry.rows()).isEqualTo(4);
    assertThat(entry.deadVersion()).isZero();
    assertThat(entry.minPage()).isZero();
    assertThat(entry.maxPage()).isEqualTo(2);
    assertThat(bucketDir.resolve(entry.file())).exists();
    assertThat(ColumnarManifest.load(bucketDir).generation()).isEqualTo(manifest.generation());
    try (final ColumnarSnapshot snapshot = projection.acquire()) {
      assertThat(snapshot.segments()).hasSize(2);
      assertThat(snapshot.segments().getFirst()).isInstanceOf(SegmentReader.class);
    }
    assertThat(counts(projection)).isEqualTo(expected(1, 1, 2, 1, 3, 1, 4, 1));
    assertThat(recoverySet(projection).isEmpty()).isTrue();

    // durable: a reopened projection reads the same counts from the segment
    projection.close();
    final ColumnarProjection reopened = open();
    assertThat(counts(reopened)).isEqualTo(expected(1, 1, 2, 1, 3, 1, 4, 1));
    final RowValues out = new RowValues(COLUMNS.length);
    assertThat(reopened.liveValueLookup(2, BRAND, out)).isEqualTo(entry.segmentId());
    assertThat(out.ordinal[BRAND]).isEqualTo(2);
  }

  @Test
  void readersSeeTheSameCountsAcrossAFlush() throws Exception {
    final ColumnarProjection projection = open();
    apply(projection, 0, 1, 1, 2, 1, 3, 3);
    apply(projection, 1, 5, 4);
    final Map<Integer, Integer> oracle = expected(1, 2, 3, 1, 4, 1);
    final ColumnarSnapshot before = projection.acquire();

    final Pause pause = Pause.at(ColumnarFlusher.FLUSH_SEGMENT_WRITTEN);
    final Future<?> flush = flushInBackground(projection);
    pause.awaitReached();
    final ColumnarSnapshot during = projection.acquire();
    // the frozen overlay sits before the (empty) live one
    assertThat(projection.currentOverlaysForTesting()).hasSize(2);
    assertThat(projection.overlay().rows()).isZero();
    pause.release();
    flush.get(30, TimeUnit.SECONDS);

    try (before; during) {
      assertThat(counts(before)).isEqualTo(oracle);
      assertThat(counts(during)).isEqualTo(oracle);
      assertThat(counts(projection)).isEqualTo(oracle);
    }
  }

  @Test
  void flushWaitsForInterval() {
    final ColumnarProjection projection = open();
    final int[] slots = new int[20];
    for (int i = 0; i < 10; i++) {
      slots[2 * i] = i;
      slots[2 * i + 1] = 1;
    }
    apply(projection, 0, slots);
    final long oldest = projection.overlay().oldestRowMs();
    assertThat(oldest).isPositive();

    assertThat(ColumnarFlusher.shouldFlush(projection, oldest + 500, 1000, 65_536)).isFalse();
    assertThat(ColumnarFlusher.shouldFlush(projection, oldest + 999, 1000, 65_536)).isFalse();
    assertThat(ColumnarFlusher.shouldFlush(projection, oldest + 1000, 1000, 65_536)).isTrue();
    // the row bound ignores the age
    assertThat(ColumnarFlusher.shouldFlush(projection, oldest, 1000, 10)).isTrue();
    assertThat(ColumnarFlusher.shouldFlush(projection, oldest, 1000, 11)).isFalse();

    final int[] many = new int[2 * MAX];
    for (int page = 1; page <= 32; page++) {
      for (int slot = 0; slot < MAX; slot++) {
        many[2 * slot] = slot;
        many[2 * slot + 1] = slot % 5;
      }
      apply(projection, page, many);
    }
    assertThat(projection.overlay().rows()).isGreaterThanOrEqualTo(65_536);
    assertThat(ColumnarFlusher.shouldFlush(projection, oldest, 1000, 65_536)).isTrue();
  }

  @Test
  void deletesOnlyWorkloadIsFlushed() throws Exception {
    final ColumnarProjection projection = open();
    apply(projection, 0, 1, 1, 2, 2);
    ColumnarFlusher.flush(projection, 1);
    final long flushedAt = projection.lastFlushMs();
    assertThat(ColumnarFlusher.shouldFlush(projection, flushedAt + 5_000, 1000, 65_536)).isFalse();

    // a delete only: no overlay row, but a dead set and an unflushed page to persist
    apply(projection, 0, 1, DELETE);
    assertThat(projection.overlay().rows()).isZero();
    assertThat(recoverySet(projection).contains(0)).isTrue();
    assertThat(ColumnarFlusher.shouldFlush(projection, flushedAt + 500, 1000, 65_536)).isFalse();
    assertThat(ColumnarFlusher.shouldFlush(projection, flushedAt + 1000, 1000, 65_536)).isTrue();

    ColumnarFlusher.flush(projection, 2);
    final ColumnarManifest.SegmentEntry entry = projection.manifest().segments().getFirst();
    assertThat(projection.manifest().segments()).hasSize(1);
    assertThat(entry.deadVersion()).isEqualTo(1);
    assertThat(entry.deadRows()).isEqualTo(1);
    assertThat(bucketDir.resolve(entry.deadFile())).exists();
    assertThat(recoverySet(projection).isEmpty()).isTrue();

    projection.close();
    assertThat(counts(open())).isEqualTo(expected(2, 1));
  }

  @Test
  void pageReappliedDuringFlushStaysUnflushed() throws Exception {
    final ColumnarProjection projection = open();
    apply(projection, 5, 1, 1, 2, 2);
    apply(projection, 7, 3, 3);

    final Pause pause = Pause.at(ColumnarFlusher.FLUSH_SEGMENT_WRITTEN);
    final Future<?> flush = flushInBackground(projection);
    pause.awaitReached();
    // R14/V-B2: page 5 re-applied after the freeze with a delete only, so no live-overlay block holds it
    apply(projection, 5, 1, DELETE);
    assertThat(projection.overlay().page(5)).isNull();
    // the deleted position is superseded: the frozen overlay no longer answers for it, the other one still does
    final RowValues out = new RowValues(COLUMNS.length);
    assertThat(projection.liveValueLookup(5L * MAX + 1, BRAND, out)).isEqualTo(ColumnarProjection.LOOKUP_ABSENT);
    assertThat(projection.liveValueLookup(5L * MAX + 2, BRAND, out)).isEqualTo(ColumnarProjection.LOOKUP_FROZEN);
    pause.release();
    flush.get(30, TimeUnit.SECONDS);

    final IntHashSet pending = recoverySet(projection);
    assertThat(pending.contains(5)).isTrue();
    assertThat(pending.contains(7)).isFalse();
    assertThat(counts(projection)).isEqualTo(expected(2, 1, 3, 1));
    // the deleted row is the new segment's initial dead set, version 1
    final ColumnarManifest.SegmentEntry entry = projection.manifest().segments().getFirst();
    assertThat(entry.deadVersion()).isEqualTo(1);
    assertThat(entry.deadRows()).isEqualTo(1);
  }

  @Test
  void updateOfAFrozenPositionIsNotDoubleCounted() throws Exception {
    final ColumnarProjection projection = open();
    apply(projection, 0, 1, 1, 2, 1, 3, 2);
    final long position = 2;

    final Pause pause = Pause.at(ColumnarFlusher.FLUSH_SEGMENT_WRITTEN);
    final Future<?> flush = flushInBackground(projection);
    pause.awaitReached();
    final RowValues out = new RowValues(COLUMNS.length);
    assertThat(projection.liveValueLookup(position, BRAND, out)).isEqualTo(ColumnarProjection.LOOKUP_FROZEN);
    assertThat(out.ordinal[BRAND]).isEqualTo(1);

    apply(projection, 0, (int) position, 4);
    final Map<Integer, Integer> oracle = expected(1, 1, 2, 1, 4, 1);
    assertThat(counts(projection)).isEqualTo(oracle);
    assertThat(projection.liveValueLookup(position, BRAND, out)).isEqualTo(ColumnarProjection.LOOKUP_OVERLAY);
    pause.release();
    flush.get(30, TimeUnit.SECONDS);

    assertThat(counts(projection)).isEqualTo(oracle);
    assertThat(projection.liveValueLookup(position, BRAND, out)).isEqualTo(ColumnarProjection.LOOKUP_OVERLAY);
    assertThat(out.ordinal[BRAND]).isEqualTo(4);
    // the next flush moves the new value to a segment, still once
    ColumnarFlusher.flush(projection, 8);
    assertThat(counts(projection)).isEqualTo(oracle);
    projection.close();
    assertThat(counts(open())).isEqualTo(oracle);
  }

  @Test
  void addColumnDuringAFlushRekeysTheFrozenOverlay() throws Exception {
    final ColumnarProjection projection = open();
    apply(projection, 0, 1, 3);

    final Pause pause = Pause.at(ColumnarFlusher.FLUSH_SEGMENT_WRITTEN);
    final Future<?> flush = flushInBackground(projection);
    pause.awaitReached();
    projection.addColumn(new ColumnDefinition(15, "color", Type.STRING, ColumnKind.ORDINAL, false, null));
    final RowValues out = new RowValues(COLUMNS.length + 1);
    assertThat(projection.liveValueLookup(1, COLUMNS.length, out)).isEqualTo(ColumnarProjection.LOOKUP_FROZEN);
    assertThat(out.present[COLUMNS.length]).isFalse();
    assertThat(projection.liveValueLookup(1, BRAND, out)).isEqualTo(ColumnarProjection.LOOKUP_FROZEN);
    assertThat(out.ordinal[BRAND]).isEqualTo(3);
    pause.release();
    flush.get(30, TimeUnit.SECONDS);

    final long segmentId = projection.manifest().segments().getFirst().segmentId();
    assertThat(projection.liveValueLookup(1, BRAND, out)).isEqualTo(segmentId);
    assertThat(out.ordinal[BRAND]).isEqualTo(3);
  }

  @Test
  void flushKeepsTheNoWalMarker() throws Exception {
    final ColumnarProjection projection = open();
    // a no-WAL commit marked page 0 and wrote NOWAL; its pages are applied and flushed
    markWithoutWal(projection, 0);
    apply(projection, 0, 1, 1);
    ColumnarFlusher.flush(projection, 1);
    assertThat(projection.manifest().segments()).hasSize(1);
    // R8: the bucket pages of a no-WAL commit reach the disk asynchronously, so only a clean close may delete the marker
    assertThat(bucketDir.resolve(ColumnarStore.NOWAL_MARKER)).exists();
    synchronized (projection.noWalMarkerLock) {
      assertThat(projection.noWalMarkerEpoch).isNotEqualTo(-1);
    }
  }

  /** What Task 13's {@code markPagesWithoutWAL} does: mark, bump the no-WAL epoch, write NOWAL when no marker is current. */
  private void markWithoutWal(final ColumnarProjection projection, final int page) throws IOException {
    projection.tracker().mark(new int[] { page }, 1);
    final long epoch = projection.bumpNoWalEpoch();
    synchronized (projection.noWalMarkerLock) {
      if (projection.noWalMarkerEpoch == -1) {
        ColumnarStore.writeAtomically(bucketDir.resolve(ColumnarStore.NOWAL_MARKER), new byte[0]);
        projection.noWalMarkerEpoch = epoch;
      }
    }
  }

  /** Names of the segment and dead set files of the bucket directory. */
  private List<String> segmentFiles() throws IOException {
    try (final var files = Files.list(bucketDir)) {
      return files.map(p -> p.getFileName().toString()).filter(n -> n.startsWith("seg-")).sorted().toList();
    }
  }

  /** Throws the exception at the named point, once. */
  private static void throwAt(final String point, final RuntimeException exception) {
    final AtomicBoolean fired = new AtomicBoolean();
    ColumnarTestHooks.killPoint = name -> {
      if (name.equals(point) && fired.compareAndSet(false, true))
        throw exception;
    };
  }

  @Test
  void failureBeforeTheManifestLeavesNoFileAndNoHeldName() throws Exception {
    final ColumnarProjection projection = open();
    apply(projection, 0, 1, 1);
    apply(projection, 1, 2, 2);
    ColumnarFlusher.flush(projection, 1);
    apply(projection, 0, 1, DELETE); // a dead set to write too
    apply(projection, 2, 3, 3);
    final List<String> before = segmentFiles();
    final long generation = projection.manifest().generation();

    throwAt(ColumnarFlusher.DICTIONARIES_SYNCED, new IllegalStateException("not an IOException"));
    assertThatThrownBy(() -> ColumnarFlusher.flush(projection, 2)).isInstanceOf(IllegalStateException.class);
    ColumnarTestHooks.killPoint = null;

    assertThat(segmentFiles()).isEqualTo(before);
    assertThat(projection.heldFilesForTesting()).isEmpty();
    assertThat(projection.manifest().generation()).isEqualTo(generation);
    assertThat(projection.hasFrozenOverlay()).isTrue();
    assertThat(counts(projection)).isEqualTo(expected(2, 1, 3, 1));

    ColumnarFlusher.flush(projection, 3);
    assertThat(projection.manifest().generation()).isEqualTo(generation + 1);
    assertThat(projection.hasFrozenOverlay()).isFalse();
    assertThat(recoverySet(projection).isEmpty()).isTrue();
    projection.close();
    assertThat(counts(open())).isEqualTo(expected(2, 1, 3, 1));
  }

  @Test
  void failureAfterTheRenameAdoptsTheRenamedManifest() throws Exception {
    final ColumnarProjection projection = open();
    apply(projection, 0, 1, 1);
    final long generation = projection.manifest().generation();
    // the directory fsync after the rename fails: the manifest on disk is the new one
    throwAt("publish.manifestRenamed", new UncheckedIOException(new IOException("directory fsync failed")));
    ColumnarFlusher.flush(projection, 1);
    ColumnarTestHooks.killPoint = null;

    final ColumnarManifest onDisk = ColumnarManifest.load(bucketDir);
    assertThat(onDisk.generation()).isEqualTo(generation + 1);
    assertThat(projection.manifest().generation()).isEqualTo(generation + 1);
    for (final String name : onDisk.files())
      assertThat(bucketDir.resolve(name)).exists();
    assertThat(projection.heldFilesForTesting()).isEmpty();

    apply(projection, 1, 2, 2);
    ColumnarFlusher.flush(projection, 2);
    assertThat(projection.manifest().generation()).isEqualTo(generation + 2);
    for (final String name : ColumnarManifest.load(bucketDir).files())
      assertThat(bucketDir.resolve(name)).exists();
    projection.close();
    assertThat(counts(open())).isEqualTo(expected(1, 1, 2, 1));
  }

  @Test
  void failedRenameKeepsItsFilesAndTheNextPublishTakesAHigherGeneration() throws Exception {
    final ColumnarProjection projection = open();
    apply(projection, 0, 1, 1);
    ColumnarFlusher.flush(projection, 1);
    final long generation = projection.manifest().generation();
    final byte[] previous = Files.readAllBytes(bucketDir.resolve(ColumnarManifest.FILE_NAME));
    apply(projection, 0, 1, DELETE);
    apply(projection, 1, 2, 2);
    // the rename is reported failed and the old manifest is what the disk holds: whether the new one will appear is unknown
    ColumnarTestHooks.killPoint = name -> {
      if (name.equals("publish.manifestRenamed")) {
        ColumnarTestHooks.killPoint = null;
        try {
          Files.write(bucketDir.resolve(ColumnarManifest.FILE_NAME), previous);
        } catch (final IOException e) {
          throw new UncheckedIOException(e);
        }
        throw new UncheckedIOException(new IOException("rename failed"));
      }
    };
    assertThatThrownBy(() -> ColumnarFlusher.flush(projection, 2)).isInstanceOf(UncheckedIOException.class);

    // nothing deleted, every name kept held, the in-memory manifest unchanged
    final Set<String> held = projection.heldFilesForTesting();
    assertThat(held).isNotEmpty();
    for (final String name : held)
      assertThat(bucketDir.resolve(name)).exists();
    assertThat(projection.manifest().generation()).isEqualTo(generation);

    ColumnarFlusher.flush(projection, 3);
    final ColumnarManifest after = ColumnarManifest.load(bucketDir);
    assertThat(after.generation()).isEqualTo(generation + 2);
    for (final String name : after.files())
      assertThat(held).doesNotContain(name);
    assertThat(counts(projection)).isEqualTo(expected(2, 1));
    projection.close();
    assertThat(counts(open())).isEqualTo(expected(2, 1));
  }

  @Test
  void failedFlushIsRetriedWithItsFrozenOverlay() throws Exception {
    final ColumnarProjection projection = open();
    apply(projection, 0, 1, 1, 2, 2);
    projection.ownersLock().writeLock().lock();
    try {
      projection.owners().setOwnerPages(1, new int[] { 9 }, 1);
    } finally {
      projection.ownersLock().writeLock().unlock();
    }
    // an orphan under the owner map name of the next generation makes the first attempt fail with FileAlreadyExistsException
    final String ownerMap = SegmentFormat.ownerMapFileName(projection.manifest().generation() + 1);
    Files.write(bucketDir.resolve(ownerMap), new byte[] { 1 });
    assertThatThrownBy(() -> ColumnarFlusher.flush(projection, 1)).isInstanceOf(FileAlreadyExistsException.class);
    // final review A-I3: the attempt did not create that file, so its failure path does not delete it; the retry takes the next generation
    assertThat(bucketDir.resolve(ownerMap)).exists();

    assertThat(projection.hasFrozenOverlay()).isTrue();
    assertThat(segmentFiles()).isEmpty();
    assertThat(projection.heldFilesForTesting()).isEmpty();
    assertThat(counts(projection)).isEqualTo(expected(1, 1, 2, 1));
    assertThat(recoverySet(projection).contains(0)).isTrue();
    // applied after the failed freeze: kept in the live overlay, not lost
    apply(projection, 3, 4, 4);

    ColumnarFlusher.flush(projection, 2);
    assertThat(projection.hasFrozenOverlay()).isFalse();
    assertThat(projection.manifest().segments()).hasSize(1);
    assertThat(projection.manifest().ownerMapFile()).isNotEqualTo(ownerMap);
    assertThat(counts(projection)).isEqualTo(expected(1, 1, 2, 1, 4, 1));
    final IntHashSet pending = recoverySet(projection);
    assertThat(pending.contains(0)).isFalse();
    assertThat(pending.contains(3)).isTrue();
    projection.close();
    final ColumnarProjection reopened = open();
    assertThat(counts(reopened)).isEqualTo(expected(1, 1, 2, 1));
    final long[] owners = new long[1];
    assertThat(reopened.owners().owners(9, owners)).isEqualTo(1);
  }

  @Test
  void noOpAppliesAreFlushedWithoutAPublish() throws Exception {
    final ColumnarProjection projection = open();
    apply(projection, 0, 1, 1);
    ColumnarFlusher.flush(projection, 1);
    final long generation = projection.manifest().generation();
    // a pass that re-read page 0 and changed nothing
    projection.applyLock().lock();
    try {
      projection.tracker().mark(new int[] { 0 }, 1);
      projection.tracker().drainInto(new IntHashSet());
      projection.tracker().completed(new int[] { 0 }, 1);
      projection.pagesApplied(new int[] { 0 }, 1);
    } finally {
      projection.applyLock().unlock();
    }
    assertThat(recoverySet(projection).contains(0)).isTrue();

    ColumnarFlusher.flush(projection, 2);
    assertThat(recoverySet(projection).isEmpty()).isTrue();
    assertThat(projection.manifest().generation()).isEqualTo(generation);
    assertThat(ColumnarManifest.load(bucketDir).generation()).isEqualTo(generation);
    assertThat(stats.flushCount("Asset")).isEqualTo(1);

    // a pass that only changed an owner mapping (a placeholder resolved) must publish the owner map
    projection.applyLock().lock();
    try {
      projection.tracker().mark(new int[] { 0 }, 1);
      projection.tracker().drainInto(new IntHashSet());
      projection.ownersLock().writeLock().lock();
      try {
        projection.owners().setOwnerPages(1, new int[] { 9 }, 1);
      } finally {
        projection.ownersLock().writeLock().unlock();
      }
      projection.tracker().completed(new int[] { 0 }, 1);
      projection.pagesApplied(new int[] { 0 }, 1);
    } finally {
      projection.applyLock().unlock();
    }
    ColumnarFlusher.flush(projection, 3);
    assertThat(projection.manifest().generation()).isEqualTo(generation + 1);
    assertThat(projection.manifest().ownerMapFile()).isNotNull();
  }

  @Test
  void manifestTmpIsWrittenOutsideThePublishLock() throws Exception {
    final ColumnarProjection projection = open();
    apply(projection, 0, 1, 1);
    final Pause pause = Pause.at("publish.manifestTmpWritten");
    final Future<?> flush = flushInBackground(projection);
    pause.awaitReached();
    // a query releasing a snapshot (which takes the publish lock) is not stalled by the manifest's fsync
    assertThat(projection.publishLock().isLocked()).isFalse();
    projection.acquire().close();
    pause.release();
    flush.get(30, TimeUnit.SECONDS);
    assertThat(projection.manifest().segments()).hasSize(1);
  }

  @Test
  void appliedPagesNeedTheApplyLock() {
    final ColumnarProjection projection = open();
    assertThatThrownBy(() -> projection.pagesApplied(new int[] { 0 }, 1)).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void flushReachesThePublishPointsInOrder() throws Exception {
    final ColumnarProjection projection = open();
    apply(projection, 0, 1, 1);
    final List<String> reached = new CopyOnWriteArrayList<>();
    ColumnarTestHooks.killPoint = reached::add;
    ColumnarFlusher.flush(projection, 1);
    assertThat(reached).containsExactly(ColumnarFlusher.FLUSH_SEGMENT_WRITTEN, ColumnarFlusher.SEGMENTS_SYNCED,
        ColumnarFlusher.DICTIONARIES_SYNCED, "publish.manifestTmpWritten", "publish.manifestRenamed");

    reached.clear();
    apply(projection, 1, 1, 1);
    ColumnarFlusher.flush(projection, 2);
    reached.clear();
    ColumnarFlusher.merge(projection, MergePolicy.select(projection.manifest().segments(), 0.3f, 1));
    assertThat(reached).containsExactly(ColumnarFlusher.MERGE_SEGMENT_WRITTEN, ColumnarFlusher.SEGMENTS_SYNCED,
        ColumnarFlusher.DICTIONARIES_SYNCED, "publish.manifestTmpWritten", "publish.manifestRenamed");
    assertThat(projection.manifest().segments()).hasSize(1);
  }

  @Test
  void openFilesFollowThePublishedSegments() throws Exception {
    final ColumnarProjection projection = open();
    apply(projection, 0, 1, 1, 2, 2, 3, 3);
    ColumnarFlusher.flush(projection, 1);
    assertThat(stats.openFiles("Asset")).isEqualTo(1);

    apply(projection, 0, 1, DELETE);
    ColumnarFlusher.flush(projection, 2);
    ColumnarFlusher.merge(projection, MergePolicy.select(projection.manifest().segments(), 0.3f, 1));
    assertThat(stats.openFiles("Asset")).as("the merged-away file is closed once its last reader is released").isEqualTo(1);

    projection.close();
    assertThat(stats.openFiles("Asset")).isZero();
  }

  @Test
  void flushCountsInStats() throws Exception {
    final ColumnarProjection projection = open();
    assertThat(stats.flushCount("Asset")).isZero();
    apply(projection, 0, 1, 1);
    ColumnarFlusher.flush(projection, 1);
    assertThat(stats.flushCount("Asset")).isEqualTo(1);
    assertThat(stats.flushDurationCount("Asset")).isEqualTo(1);
    // nothing to flush: no publish, no count
    ColumnarFlusher.flush(projection, 1);
    assertThat(stats.flushCount("Asset")).isEqualTo(1);
  }

  @Test
  void flushRefusedWhileAnotherMaintenanceJobRuns() throws Exception {
    final ColumnarProjection projection = open();
    apply(projection, 0, 1, 1);
    assertThat(projection.tryBeginMaintenance()).isTrue();
    try {
      assertThatThrownBy(() -> ColumnarFlusher.flush(projection, 1)).isInstanceOf(IllegalStateException.class);
      assertThatThrownBy(() -> ColumnarFlusher.merge(projection, new long[] { 1 })).isInstanceOf(IllegalStateException.class);
    } finally {
      projection.endMaintenance();
    }
    ColumnarFlusher.flush(projection, 1);
    assertThat(projection.manifest().segments()).hasSize(1);
  }

  /** Three flushed segments: page 0 (slots 0..9), page 1 (slots 0..9), page 2 (slots 0..9), ordinal slot % 5. */
  private ColumnarProjection threeSegments() throws IOException {
    final ColumnarProjection projection = open();
    for (int page = 0; page < 3; page++) {
      final int[] slots = new int[20];
      for (int slot = 0; slot < 10; slot++) {
        slots[2 * slot] = slot;
        slots[2 * slot + 1] = slot % 5;
      }
      apply(projection, page, slots);
      ColumnarFlusher.flush(projection, page);
    }
    assertThat(projection.manifest().segments()).hasSize(3);
    return projection;
  }

  @Test
  void mergeDropsDeadRowsAndKeepsLiveValues() throws Exception {
    final ColumnarProjection projection = threeSegments();
    // 40% of the second segment dead: slots 0..3 of page 1 deleted
    apply(projection, 1, 0, DELETE, 1, DELETE, 2, DELETE, 3, DELETE);
    // and a newer value of slot 9 of page 2 lives in the overlay
    apply(projection, 2, 9, 0);
    ColumnarFlusher.flush(projection, 3);
    final Map<Integer, Integer> oracle = counts(projection);
    final List<ColumnarManifest.SegmentEntry> before = projection.manifest().segments();
    assertThat(before.get(1).deadRows()).isEqualTo(4);
    assertThat(MergePolicy.select(before, 0.3f, 10)).containsExactly(before.get(1).segmentId());

    ColumnarFlusher.merge(projection, new long[] { before.get(1).segmentId() });

    final List<ColumnarManifest.SegmentEntry> after = projection.manifest().segments();
    assertThat(after).hasSize(before.size());
    assertThat(after.get(1).rows()).isEqualTo(6);
    assertThat(after.get(1).deadRows()).isZero();
    assertThat(after.get(1).deadVersion()).isZero();
    assertThat(after.get(1).segmentId()).isNotEqualTo(before.get(1).segmentId());
    assertThat(counts(projection)).isEqualTo(oracle);
    assertThat(stats.mergeCount("Asset")).isEqualTo(1);
    assertThat(stats.mergeDurationCount("Asset")).isEqualTo(1);

    // every segment into one: live rows only, ascending, one live value per position
    final long[] all = new long[after.size()];
    for (int i = 0; i < all.length; i++)
      all[i] = after.get(i).segmentId();
    ColumnarFlusher.merge(projection, all);
    assertThat(projection.manifest().segments()).hasSize(1);
    assertThat(projection.manifest().segments().getFirst().rows()).isEqualTo(26);
    assertThat(counts(projection)).isEqualTo(oracle);
    final RowValues out = new RowValues(COLUMNS.length);
    projection.liveValueLookup(2L * MAX + 9, BRAND, out);
    assertThat(out.ordinal[BRAND]).isEqualTo(0);
    projection.close();
    assertThat(counts(open())).isEqualTo(oracle);
  }

  @Test
  void mergeKeepsDeadPositionsAddedWhileItRuns() throws Exception {
    final ColumnarProjection projection = threeSegments();
    final List<ColumnarManifest.SegmentEntry> before = projection.manifest().segments();
    final Pause pause = Pause.at(ColumnarFlusher.MERGE_SEGMENT_WRITTEN);
    final Future<?> merge = executor.submit(() -> {
      ColumnarFlusher.merge(projection, new long[] { before.get(0).segmentId(), before.get(1).segmentId() });
      return null;
    });
    pause.awaitReached();
    // a delete applied after the merge read its inputs
    apply(projection, 1, 4, DELETE);
    pause.release();
    merge.get(30, TimeUnit.SECONDS);

    final ColumnarManifest.SegmentEntry merged = projection.manifest().segments().getFirst();
    assertThat(merged.rows()).isEqualTo(20);
    assertThat(merged.deadRows()).isEqualTo(1);
    assertThat(merged.deadVersion()).isEqualTo(1);
    assertThat(counts(projection)).isEqualTo(expected(0, 6, 1, 6, 2, 6, 3, 6, 4, 5));
  }

  @Test
  void mergeKeepsNewestPageVersion() throws Exception {
    final ColumnarProjection projection = open();
    applyWithVersion(projection, 0, 3, 1);
    ColumnarFlusher.flush(projection, 1);
    applyWithVersion(projection, 0, 5, 9);
    ColumnarFlusher.flush(projection, 2);
    applyWithVersion(projection, 4, 1, 2);
    ColumnarFlusher.flush(projection, 3);
    final List<ColumnarManifest.SegmentEntry> inputs = projection.manifest().segments();
    assertThat(inputs).hasSize(3);

    ColumnarFlusher.merge(projection, new long[] { inputs.get(0).segmentId(), inputs.get(1).segmentId(), inputs.get(2).segmentId() });

    try (final ColumnarSnapshot snapshot = projection.acquire()) {
      final SegmentReader merged = (SegmentReader) snapshot.segments().getFirst();
      assertThat(merged.rowCount()).isEqualTo(3);
      assertThat(merged.pageVersion(0)).isEqualTo(9);
      assertThat(merged.pageVersion(4)).isEqualTo(2);
    }
  }

  /** One row at (page, slot) with brand ordinal 1, whose page block carries the version. */
  private static void applyWithVersion(final ColumnarProjection projection, final int page, final int slot, final long version) {
    projection.applyLock().lock();
    try {
      final RowBuffer buffer = new RowBuffer(MAX, projection.columns());
      final RowValues row = new RowValues(COLUMNS.length);
      final OverlayPageBlock current = projection.overlay().page(page);
      if (current != null)
        for (int r = 0; r < current.rowCount(); r++) {
          current.readRow(r, row);
          buffer.put((int) (current.position(r) % MAX), row);
        }
      row.present[BRAND] = true;
      row.ordinal[BRAND] = 1;
      buffer.put(slot, row);
      projection.overlay().replacePage(OverlayPageBlock.build(page, MAX, projection.columns(), version, buffer));
      projection.publishOverlay();
    } finally {
      projection.applyLock().unlock();
    }
  }

  @Test
  void mergedInputsAreDeletedAfterLastReader() throws Exception {
    final ColumnarProjection projection = threeSegments();
    final List<ColumnarManifest.SegmentEntry> inputs = projection.manifest().segments();
    final Path first = bucketDir.resolve(inputs.get(0).file());
    final Path second = bucketDir.resolve(inputs.get(1).file());

    final ColumnarSnapshot held = projection.acquire();
    ColumnarFlusher.merge(projection, new long[] { inputs.get(0).segmentId(), inputs.get(1).segmentId() });
    assertThat(projection.manifest().segments()).hasSize(2);
    assertThat(first).exists();
    assertThat(second).exists();
    assertThat(counts(held)).isEqualTo(counts(projection));

    held.close();
    assertThat(first).doesNotExist();
    assertThat(second).doesNotExist();
    assertThat(bucketDir.resolve(projection.manifest().segments().getFirst().file())).exists();
  }

  @Test
  void flushOfADroppedProjectionLeavesNoFile() throws Exception {
    final ColumnarProjection projection = open();
    apply(projection, 0, 1, 1);
    final Pause pause = Pause.at(ColumnarFlusher.FLUSH_SEGMENT_WRITTEN);
    final Future<?> flush = flushInBackground(projection);
    pause.awaitReached();
    final ColumnarSnapshot held = projection.acquire();
    projection.drop();
    pause.release();
    flush.get(30, TimeUnit.SECONDS);
    // nothing new was published, and the flush deleted its own segment
    assertThat(ColumnarManifest.load(bucketDir).segments()).isEmpty();
    try (final var files = Files.list(bucketDir)) {
      assertThat(files.map(p -> p.getFileName().toString()).filter(n -> n.startsWith("seg-")).toList()).isEmpty();
    }
    held.close();
    assertThat(bucketDir).doesNotExist();
  }
}
