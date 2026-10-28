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
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.engine.block.BlockCache;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalDocumentType;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.IntHashSet;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ColumnarProjectionTest extends TestHelper {
  private static final ColumnDefinition[] COLUMNS = SegmentRoundTripTest.ALL;
  private static final int                MAX     = SegmentRoundTripTest.MAX_RECORDS_IN_PAGE;
  private static final int                BRAND   = 0;

  private final ColumnarStats            stats  = new ColumnarStats();
  private final List<ColumnarProjection> opened = new ArrayList<>();
  private       LocalBucket              bucket;
  private       Path                     bucketDir;

  @Override
  protected void endTest() {
    ColumnarTestHooks.killPoint = null;
    for (final ColumnarProjection projection : opened)
      projection.close();
    if (database.getMode() == ComponentFile.MODE.READ_ONLY)
      reopenDatabase(); // the integrity check of the fixture needs a writable database
  }

  private void useType(final String name) {
    final DocumentType type = database.getSchema().existsType(name) ?
        database.getSchema().getType(name) :
        database.getSchema().createDocumentType(name, 1);
    bucket = (LocalBucket) type.getBuckets(false).getFirst();
    assertThat(bucket.getMaxRecordsInPage()).isEqualTo(MAX);
    bucketDir = ColumnarStore.bucketDir(Path.of(database.getDatabasePath()), bucket.getFileId());
  }

  private ColumnarProjection open() {
    final ColumnarProjection projection = ColumnarProjection.open((DatabaseInternal) database, bucket, COLUMNS, stats);
    opened.add(projection);
    return projection;
  }

  /** A segment whose rows hold only a brand ordinal, {@code position % 5}. */
  private ColumnarManifest.SegmentEntry writeSegment(final long id, final long... positions) throws IOException {
    ColumnarStore.createBucketDir(Path.of(database.getDatabasePath()), bucket.getFileId());
    final String name = SegmentFormat.segmentFileName(id);
    try (final SegmentWriter writer = new SegmentWriter(bucketDir.resolve(name), id, MAX, COLUMNS, SegmentRoundTripTest.BLOCK_ROWS)) {
      final RowValues row = new RowValues(COLUMNS.length);
      for (final long position : positions) {
        row.present[BRAND] = true;
        row.ordinal[BRAND] = (int) (position % 5);
        writer.addRow(position, 1, row);
      }
      final SegmentReader.Meta meta = writer.finish();
      return new ColumnarManifest.SegmentEntry(id, name, meta.rows(), 0, 0, meta.minPage(), meta.maxPage());
    }
  }

  private ColumnarManifest manifest(final long generation, final List<ColumnarManifest.DictionaryEntry> dictionaries,
      final ColumnarManifest.SegmentEntry... segments) throws IOException {
    final ColumnarManifest manifest = new ColumnarManifest(generation, bucket.getFileId(), bucket.getName(), MAX, COLUMNS,
        List.of(segments), dictionaries, null, null, 0);
    manifest.publish(bucketDir);
    return manifest;
  }

  /** An overlay block of the page holding one brand row per slot, ordinal {@code slot % 5}. */
  private static OverlayPageBlock block(final ColumnarProjection projection, final int page, final int... slots) {
    final RowBuffer rows = new RowBuffer(MAX, projection.columns());
    final RowValues row = new RowValues(projection.columns().length);
    for (final int slot : slots) {
      row.present[BRAND] = true;
      row.ordinal[BRAND] = slot % 5;
      rows.put(slot, row);
    }
    return OverlayPageBlock.build(page, MAX, projection.columns(), 1, rows);
  }

  /** What a flush or merge does: publish a changed segment set holding the apply lock, then the publish lock. */
  private static boolean publishSegments(final ColumnarProjection projection, final ColumnarManifest manifest,
      final SegmentReader... segments) {
    projection.applyLock().lock();
    projection.publishLock().lock();
    try {
      return projection.publish(manifest, segments, projection.currentOverlaysForTesting(), projection.dictionaries(), true);
    } finally {
      projection.publishLock().unlock();
      projection.applyLock().unlock();
    }
  }

  private static boolean liveSomewhere(final ColumnarSnapshot snapshot, final long position) {
    for (final ColumnarSegmentView view : snapshot.segments())
      if (view.live().get(position))
        return true;
    return false;
  }

  @Test
  void snapshotListsSegmentsThenOverlay() throws IOException {
    useType("Asset");
    final String dictionaryName = SegmentFormat.dictionaryFileName(11, 0);
    ColumnarStore.createBucketDir(Path.of(database.getDatabasePath()), bucket.getFileId());
    final GlobalDictionary written = GlobalDictionary.create(bucketDir.resolve(dictionaryName));
    written.getOrAdd("acme");
    final GlobalDictionary.DurableState durable = written.fsync();
    written.close();
    manifest(1, List.of(new ColumnarManifest.DictionaryEntry(11, 0, dictionaryName, durable.entries(), durable.bytes())),
        writeSegment(1, 1, 2, 3), writeSegment(2, MAX + 7));

    final ColumnarProjection projection = open();
    assertThat(projection.isUsable()).isTrue();
    assertThat(projection.unusableReason()).isNull();
    projection.overlay().replacePage(block(projection, 3, 9));
    projection.publishOverlay();

    try (final ColumnarSnapshot snapshot = projection.acquire()) {
      assertThat(snapshot.bucketId()).isEqualTo(bucket.getFileId());
      assertThat(snapshot.isUsable()).isTrue();
      final List<ColumnarSegmentView> views = snapshot.segments();
      assertThat(views).hasSize(3);
      assertThat(((SegmentReader) views.get(0)).segmentId()).isEqualTo(1);
      assertThat(((SegmentReader) views.get(1)).segmentId()).isEqualTo(2);
      assertThat(views.get(2)).isNotInstanceOf(SegmentReader.class);
      assertThat(views.get(2).live().get(3L * MAX + 9)).isTrue();
      assertThat(views.get(0).live().get(2)).isTrue();
      assertThat(snapshot.dictionary("brand")).isSameAs(projection.dictionary("brand"));
      assertThat(snapshot.dictionary("brand").valueOf(0)).isEqualTo("acme");
      assertThat(snapshot.dictionary("size")).isNull();
      assertThat(snapshot.pendingPositions().isEmpty()).isTrue();
      assertThat(snapshot.oldestPendingEpochStartMs()).isZero();
    }
    assertThat(projection.externalTracker()).isNull();
    assertThat(projection.externalOwners()).isNull();
    assertThat(projection.externalFileId()).isEqualTo(-1);
  }

  @Test
  void liveValueLookupIsNewestFirst() throws IOException {
    useType("Asset");
    manifest(1, List.of(), writeSegment(1, 4, 5), writeSegment(2, 6));
    final ColumnarProjection projection = open();
    final RowValues out = new RowValues(COLUMNS.length);

    assertThat(projection.liveValueLookup(5, BRAND, out)).isEqualTo(1);
    assertThat(out.present[BRAND]).isTrue();
    assertThat(out.ordinal[BRAND]).isEqualTo(0);
    assertThat(projection.liveValueLookup(6, BRAND, out)).isEqualTo(2);
    assertThat(out.ordinal[BRAND]).isEqualTo(1);
    // the row exists, the column has no value in it
    assertThat(projection.liveValueLookup(6, 2, out)).isEqualTo(2);
    assertThat(out.present[2]).isFalse();
    assertThat(projection.liveValueLookup(7, BRAND, out)).isEqualTo(ColumnarProjection.LOOKUP_ABSENT);

    // the overlay holds the newest value of position 4 (slot 4 of page 0, ordinal 4 % 5)
    projection.overlay().replacePage(block(projection, 0, 4));
    final PositionBitmap dead = new PositionBitmap(MAX);
    dead.set(4);
    projection.applyDeadPositions(dead);
    assertThat(projection.liveValueLookup(4, BRAND, out)).isEqualTo(ColumnarProjection.LOOKUP_OVERLAY);
    assertThat(out.ordinal[BRAND]).isEqualTo(4);
  }

  @Test
  void snapshotHoldsFilesUntilClosed() throws IOException {
    useType("Asset");
    final ColumnarManifest.SegmentEntry first = writeSegment(1, 1, 2);
    final ColumnarManifest.SegmentEntry second = writeSegment(2, MAX + 1);
    manifest(1, List.of(), first, second);
    final ColumnarProjection projection = open();

    final ColumnarSnapshot snapshot = projection.acquire();
    final SegmentReader kept = (SegmentReader) snapshot.segments().get(1);
    final ColumnarManifest next = manifest(2, List.of(), second);
    assertThat(publishSegments(projection, next, kept)).isTrue();
    assertThat(projection.manifest()).isSameAs(next);
    assertThat(bucketDir.resolve(first.file())).exists();
    assertThat(snapshot.segments()).hasSize(3);
    assertThat(snapshot.segments().getFirst().live().get(2)).isTrue();

    snapshot.close();
    snapshot.close(); // a second close does nothing
    assertThat(bucketDir.resolve(first.file())).doesNotExist();
    assertThat(bucketDir.resolve(second.file())).exists();
    try (final ColumnarSnapshot after = projection.acquire()) {
      assertThat(after.segments()).hasSize(2);
    }
  }

  @Test
  void pendingIsReadBeforeState() throws Exception {
    useType("Asset");
    final ColumnarProjection projection = open();
    assertThat(projection.isUsable()).isTrue();
    final DirtyPageTracker tracker = projection.tracker();
    final int iterations = 10_000;
    final CyclicBarrier enter = new CyclicBarrier(2);
    final CyclicBarrier leave = new CyclicBarrier(2);
    final AtomicInteger page = new AtomicInteger();
    final AtomicInteger applied = new AtomicInteger();
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final boolean[] stop = { false };

    // the apply thread: when the reader stands between its two reads, the page is applied, published and completed
    final Thread applier = new Thread(() -> {
      try {
        while (true) {
          enter.await(10, TimeUnit.SECONDS);
          if (stop[0])
            return;
          final int pageNumber = page.get();
          projection.overlay().replacePage(block(projection, pageNumber, 0));
          projection.publishOverlay();
          tracker.completed(new int[] { pageNumber }, 1);
          applied.incrementAndGet();
          leave.await(10, TimeUnit.SECONDS);
        }
      } catch (final Throwable e) {
        failure.set(e);
      }
    });
    applier.start();
    ColumnarTestHooks.killPoint = name -> {
      if (!name.equals("acquire.pendingRead"))
        return;
      try {
        enter.await(10, TimeUnit.SECONDS);
        leave.await(10, TimeUnit.SECONDS);
      } catch (final Exception e) {
        throw new IllegalStateException(e);
      }
    };

    int vanished = 0;
    for (int i = 0; i < iterations && failure.get() == null; i++) {
      final int pageNumber = i % 64;
      page.set(pageNumber);
      tracker.mark(new int[] { pageNumber }, 1);
      assertThat(tracker.drainInto(new IntHashSet())).isEqualTo(1); // in flight
      try (final ColumnarSnapshot snapshot = projection.acquire()) {
        final long position = (long) pageNumber * MAX;
        if (!snapshot.pendingPositions().get(position) && !liveSomewhere(snapshot, position))
          vanished++;
      }
      // the apply thread waits at the barrier: removing the page again is ordered after its work
      projection.overlay().replacePage(OverlayPageBlock.build(pageNumber, MAX, projection.columns(), 1, null));
      projection.publishOverlay();
    }
    ColumnarTestHooks.killPoint = null;
    stop[0] = true;
    enter.await(10, TimeUnit.SECONDS);
    applier.join(10_000);

    assertThat(failure.get()).isNull();
    assertThat(applied.get()).isEqualTo(iterations);
    assertThat(vanished).isZero();
  }

  @Test
  void corruptSegmentMakesProjectionUnusable() throws IOException {
    useType("Asset");
    final ColumnarManifest.SegmentEntry entry = writeSegment(1, 1, 2, 3);
    manifest(1, List.of(), entry);
    final Path file = bucketDir.resolve(entry.file());
    final byte[] bytes = Files.readAllBytes(file);
    final int footerOffset = ByteBuffer.wrap(bytes, bytes.length - SegmentFormat.TRAILER_BYTES, 4).getInt();
    bytes[footerOffset + 2] ^= 0x40;
    Files.write(file, bytes);

    final ColumnarProjection projection = open();
    assertThat(projection.isUsable()).isFalse();
    assertThat(projection.unusableReason()).isEqualTo("corrupt");
    try (final ColumnarSnapshot snapshot = projection.acquire()) {
      assertThat(snapshot.isUsable()).isFalse();
      assertThat(snapshot.segments()).hasSize(1); // the live overlay only
    }

    final List<String> requested = new ArrayList<>();
    projection.setRebuildRequester(requested::add);
    projection.markUnusable("corrupt");
    assertThat(requested).containsExactly("corrupt");
    // the files of the corrupt state are kept for the rebuild to replace, not deleted by a release
    assertThat(file).exists();
  }

  @Test
  void missingSegmentFileIsMissingNotCorrupt() throws IOException {
    useType("Asset");
    final ColumnarManifest.SegmentEntry entry = writeSegment(1, 1);
    manifest(1, List.of(), entry);
    Files.delete(bucketDir.resolve(entry.file()));

    final ColumnarProjection projection = open();
    assertThat(projection.isUsable()).isFalse();
    assertThat(projection.unusableReason()).isEqualTo("missing");
  }

  @Test
  void markUnusableOnAUsableProjectionStopsServing() throws IOException {
    useType("Asset");
    manifest(1, List.of(), writeSegment(1, 1));
    final ColumnarProjection projection = open();
    final List<String> requested = new ArrayList<>();
    projection.setRebuildRequester(requested::add);
    final ColumnarSnapshot before = projection.acquire();

    projection.markUnusable("corrupt");
    assertThat(requested).containsExactly("corrupt");
    assertThat(projection.isUsable()).isFalse();
    assertThat(projection.unusableReason()).isEqualTo("corrupt");
    assertThat(before.isUsable()).isTrue(); // a snapshot keeps what it acquired
    before.close();
    try (final ColumnarSnapshot after = projection.acquire()) {
      assertThat(after.isUsable()).isFalse();
    }
  }

  @Test
  void deadPositionsAreCopyOnWrite() throws IOException {
    useType("Asset");
    manifest(1, List.of(), writeSegment(1, 1, 2, 3));
    final ColumnarProjection projection = open();
    final RowValues out = new RowValues(COLUMNS.length);
    assertThat(projection.liveValueLookup(2, BRAND, out)).isEqualTo(1);

    try (final ColumnarSnapshot before = projection.acquire()) {
      final PositionBitmap dead = new PositionBitmap(MAX);
      dead.set(2);
      dead.set(5L * MAX); // in no segment: ignored
      projection.applyDeadPositions(dead);
      dead.set(1); // the caller's bitmap is not kept

      assertThat(before.segments().getFirst().live().get(2)).isTrue();
      assertThat(before.segments().getFirst().live().cardinality()).isEqualTo(3);
      try (final ColumnarSnapshot after = projection.acquire()) {
        final PositionBitmap live = after.segments().getFirst().live();
        assertThat(live.get(2)).isFalse();
        assertThat(live.get(1)).isTrue();
        assertThat(live.cardinality()).isEqualTo(2);
      }
    }
    assertThat(projection.liveValueLookup(2, BRAND, out)).isEqualTo(ColumnarProjection.LOOKUP_ABSENT);
    assertThat(projection.liveValueLookup(1, BRAND, out)).isEqualTo(1);
  }

  @Test
  void emptyBucketWithoutManifestIsUsable() throws IOException {
    useType("Asset");
    final ColumnarProjection projection = open();
    assertThat(projection.isUsable()).isTrue();
    assertThat(projection.unusableReason()).isNull();

    final ColumnarManifest published = ColumnarManifest.load(bucketDir);
    assertThat(published).isNotNull();
    assertThat(published.generation()).isEqualTo(1);
    assertThat(published.segments()).isEmpty();
    assertThat(published.bucketFileId()).isEqualTo(bucket.getFileId());
    assertThat(projection.manifest().generation()).isEqualTo(1);
    // the ordinal columns get a durable dictionary generation the apply pass can add to
    assertThat(projection.dictionary("brand")).isNotNull();
    assertThat(projection.dictionary("tags")).isNotNull();
    assertThat(projection.dictionary("size")).isNull();
    assertThat(published.files()).contains(SegmentFormat.dictionaryFileName(11, 0), SegmentFormat.dictionaryFileName(12, 0));
    for (final String name : published.files())
      assertThat(bucketDir.resolve(name)).exists();
    try (final ColumnarSnapshot snapshot = projection.acquire()) {
      assertThat(snapshot.isUsable()).isTrue();
    }
  }

  @Test
  void bucketWithRecordsWithoutManifestIsMissing() {
    useType("Asset");
    database.transaction(() -> database.newDocument("Asset").set("brand", "acme").save());
    final ColumnarProjection projection = open();
    assertThat(projection.isUsable()).isFalse();
    assertThat(projection.unusableReason()).isEqualTo("missing");
    assertThat(projection.manifest()).isNull();
    assertThat(bucketDir.resolve(ColumnarManifest.FILE_NAME)).doesNotExist();
  }

  @Test
  void writableOpenDeletesCrashOrphans() throws IOException {
    useType("Asset");
    manifest(1, List.of(), writeSegment(1, 1));
    final Path orphan = bucketDir.resolve(SegmentFormat.dictionaryFileName(11, 1));
    Files.write(orphan, new byte[] { 1 });
    final Path tmp = bucketDir.resolve(ColumnarManifest.FILE_NAME + ColumnarStore.TMP_SUFFIX);
    Files.write(tmp, new byte[] { 1 });

    final ColumnarProjection projection = open();
    assertThat(projection.isUsable()).isTrue();
    assertThat(orphan).doesNotExist();
    assertThat(tmp).exists();
    assertThat(bucketDir.resolve(SegmentFormat.segmentFileName(1))).exists();
  }

  @Test
  void readOnlyMarkUnusableSchedulesNothing() throws IOException {
    useType("Asset");
    database.transaction(() -> database.newDocument("Asset").set("brand", "acme").save());
    manifest(1, List.of(), writeSegment(1, 0));
    Files.delete(bucketDir.resolve(ColumnarManifest.FILE_NAME));
    final List<String> before = listFiles(bucketDir);

    reopenDatabaseInReadOnlyMode();
    useType("Asset");
    final ColumnarProjection projection = open();
    assertThat(projection.isUsable()).isFalse();
    assertThat(projection.unusableReason()).isEqualTo("missing");

    final List<String> requested = new ArrayList<>();
    projection.setRebuildRequester(requested::add);
    projection.markUnusable("corrupt");
    assertThat(requested).isEmpty();
    assertThat(projection.unusableReason()).isEqualTo("corrupt");
    try (final ColumnarSnapshot snapshot = projection.acquire()) {
      assertThat(snapshot.isUsable()).isFalse();
    }
    projection.close();
    assertThat(listFiles(bucketDir)).isEqualTo(before);
    assertThat(listFiles(bucketDir.getParent())).containsExactly(bucketDir.getFileName().toString());
  }

  @Test
  void readOnlyEmptyBucketIsUsableWithoutWriting() {
    useType("Asset");
    reopenDatabaseInReadOnlyMode();
    useType("Asset");
    final ColumnarProjection projection = open();
    assertThat(projection.isUsable()).isTrue();
    assertThat(projection.unusableReason()).isNull();
    assertThat(bucketDir.getParent()).doesNotExist(); // no columnar/ directory created
  }

  @Test
  void dropDefersDeletionToLastRelease() throws IOException {
    useType("Asset");
    manifest(1, List.of(), writeSegment(1, 1));
    final ColumnarProjection projection = open();
    final ColumnarSnapshot snapshot = projection.acquire();
    final SegmentReader reader = (SegmentReader) snapshot.segments().getFirst();
    reader.column("brand").isPresent(1);
    final long cacheFileId = reader.file().cacheFileId();
    assertThat(BlockCache.instance().entriesForTesting(cacheFileId)).isPositive();

    projection.drop();
    assertThat(bucketDir).exists();
    assertThat(reader.column("brand").ordinal(1)).isEqualTo(1); // still readable
    assertThatThrownBy(projection::acquire).isInstanceOf(IllegalStateException.class);
    snapshot.close();
    assertThat(bucketDir).doesNotExist();
    assertThat(BlockCache.instance().entriesForTesting(cacheFileId)).isZero();

    // nothing retained: deleted at once
    useType("Other");
    final ColumnarProjection other = open();
    assertThat(bucketDir).exists();
    other.drop();
    assertThat(bucketDir).doesNotExist();
  }

  @Test
  void releasedSegmentInvalidatesItsCacheOnce() throws IOException {
    useType("Asset");
    final ColumnarManifest.SegmentEntry first = writeSegment(1, 1, 2);
    final ColumnarManifest.SegmentEntry second = writeSegment(2, MAX + 1);
    manifest(1, List.of(), first, second);
    final ColumnarProjection projection = open();

    final ColumnarSnapshot snapshot = projection.acquire();
    final SegmentReader held = (SegmentReader) snapshot.segments().getFirst();
    held.column("brand").isPresent(1);
    final long cacheFileId = held.file().cacheFileId();
    assertThat(BlockCache.instance().entriesForTesting(cacheFileId)).isPositive();

    final PositionBitmap dead = new PositionBitmap(MAX);
    dead.set(1);
    projection.applyDeadPositions(dead);
    assertThat(BlockCache.instance().entriesForTesting(cacheFileId)).isPositive();

    final SegmentReader kept = (SegmentReader) snapshot.segments().get(1);
    final ColumnarManifest next = manifest(2, List.of(), second);
    publishSegments(projection, next, kept);
    assertThat(BlockCache.instance().entriesForTesting(cacheFileId)).isPositive();
    assertThat(bucketDir.resolve(first.file())).exists();

    snapshot.close();
    assertThat(bucketDir.resolve(first.file())).doesNotExist();
    assertThat(BlockCache.instance().entriesForTesting(cacheFileId)).isZero();
    assertThat(held.file().referencesForTesting()).isZero();
  }

  @Test
  void externalPendingOwnersArePrimaryPositions() {
    final DocumentType type = database.getSchema().createDocumentType("Blob", 1);
    type.createProperty("payload", Type.STRING).setExternal(true);
    database.transaction(() -> database.newDocument("Blob").set("payload", "x".repeat(300)).save());
    useType("Blob");
    final Integer externalId = ((LocalDocumentType) type).getExternalBucketIdFor(bucket.getFileId());
    assertThat(externalId).isNotNull();
    final LocalBucket external = (LocalBucket) database.getSchema().getBucketById(externalId);
    assertThat(external.getMaxRecordsInPage()).isNotEqualTo(MAX);

    final ColumnarProjection projection = open();
    assertThat(projection.externalFileId()).isEqualTo(externalId);
    assertThat(projection.externalTracker()).isNotNull();
    assertThat(projection.maxRecordsInPage()).isEqualTo(MAX);

    final long owner = 3L * MAX + 5;
    projection.externalOwnersLock().writeLock().lock();
    try {
      projection.externalOwners().setOwnerPages(owner, new int[] { 0 }, 1);
    } finally {
      projection.externalOwnersLock().writeLock().unlock();
    }
    projection.externalTracker().mark(new int[] { 0 }, 1);

    try (final ColumnarSnapshot snapshot = projection.acquire()) {
      final PositionBitmap pending = snapshot.pendingPositions();
      assertThat(pending.maxRecordsInPage()).isEqualTo(MAX);
      assertThat(pending.get(owner)).isTrue();
      assertThat(pending.get(0)).isFalse(); // page 0 of the external file is not page 0 of the primary
      assertThat(pending.cardinality()).isEqualTo(1);
      assertThat(snapshot.oldestPendingEpochStartMs()).isPositive();
    }
  }

  @Test
  void addColumnRekeysTheOverlayAndReadsMissing() throws IOException {
    useType("Asset");
    manifest(1, List.of(), writeSegment(1, 1));
    final ColumnarProjection projection = open();
    projection.overlay().replacePage(block(projection, 2, 3));
    projection.publishOverlay();
    final ColumnarManifest manifestBefore = projection.manifest();
    final ColumnarSnapshot before = projection.acquire();

    final ColumnDefinition added = new ColumnDefinition(15, "color", Type.STRING, ColumnKind.ORDINAL, false, null);
    projection.addColumn(added);
    projection.addColumn(added); // known index file id: no-op
    assertThat(projection.columns()).hasSize(COLUMNS.length + 1);
    assertThat(projection.columns()[COLUMNS.length]).isEqualTo(added);
    assertThat(projection.manifest()).isSameAs(manifestBefore);
    assertThat(projection.dictionary("color")).isNull();

    // the overlay blocks now carry the new column, so a block built for the new columns is accepted
    final OverlayPageBlock rekeyed = projection.overlay().page(2);
    assertThat(rekeyed.columnsView()).containsExactly(projection.columns());
    final RowValues out = new RowValues(projection.columns().length);
    final long position = 2L * MAX + 3;
    assertThat(projection.liveValueLookup(position, BRAND, out)).isEqualTo(ColumnarProjection.LOOKUP_OVERLAY);
    assertThat(out.ordinal[BRAND]).isEqualTo(3);
    assertThat(projection.liveValueLookup(position, COLUMNS.length, out)).isEqualTo(ColumnarProjection.LOOKUP_OVERLAY);
    assertThat(out.present[COLUMNS.length]).isFalse();
    assertThat(projection.liveValueLookup(1, COLUMNS.length, out)).isEqualTo(1);
    assertThat(out.present[COLUMNS.length]).isFalse();
    projection.overlay().replacePage(block(projection, 4, 1));
    projection.publishOverlay();

    // the state acquired before still reads its own overlay view
    assertThat(before.segments().get(1).live().get(position)).isTrue();
    before.close();
    try (final ColumnarSnapshot after = projection.acquire()) {
      assertThat(after.segments().get(1).column("color")).isNotNull();
      assertThat(after.segments().get(1).column("color").isPresent(position)).isFalse();
      assertThat(after.segments().get(1).live().get(4L * MAX + 1)).isTrue();
    }
  }

  @Test
  void dateColumnDefinitionsSurviveTheEmptyManifest() throws IOException {
    useType("Asset");
    final ColumnDefinition date = new ColumnDefinition(16, "created", Type.DATETIME, ColumnKind.LONG, false, ChronoUnit.MILLIS);
    final ColumnarProjection projection = ColumnarProjection.open((DatabaseInternal) database, bucket, new ColumnDefinition[] { date },
        stats);
    opened.add(projection);
    assertThat(ColumnarManifest.load(bucketDir).columns()).containsExactly(date);
    assertThat(projection.dictionaries()).isEmpty();
  }

  @Test
  void publishChangingSegmentsNeedsTheApplyLock() throws IOException {
    useType("Asset");
    final ColumnarManifest.SegmentEntry first = writeSegment(1, 1);
    manifest(1, List.of(), first, writeSegment(2, MAX + 1));
    final ColumnarProjection projection = open();
    final ColumnarManifest next = manifest(2, List.of(), first);
    final SegmentReader kept;
    try (final ColumnarSnapshot snapshot = projection.acquire()) {
      kept = (SegmentReader) snapshot.segments().getFirst();
    }
    projection.publishLock().lock();
    try {
      // same segments: an overlay or dictionary publish needs only the publish lock
      final ColumnarSegmentView[] overlays = projection.currentOverlaysForTesting();
      assertThat(projection.publish(projection.manifest(), currentSegments(projection), overlays, projection.dictionaries(), true)).isTrue();
      assertThatThrownBy(() -> projection.publish(next, new SegmentReader[] { kept }, overlays, projection.dictionaries(), true))
          .isInstanceOf(IllegalStateException.class).hasMessageContaining("apply lock");
    } finally {
      projection.publishLock().unlock();
    }
    assertThat(projection.manifest().generation()).isEqualTo(1);
    assertThat(publishSegments(projection, next, kept)).isTrue();
    assertThat(projection.manifest()).isSameAs(next);
  }

  private static SegmentReader[] currentSegments(final ColumnarProjection projection) {
    try (final ColumnarSnapshot snapshot = projection.acquire()) {
      final List<ColumnarSegmentView> views = snapshot.segments();
      final List<SegmentReader> readers = new ArrayList<>();
      for (final ColumnarSegmentView view : views)
        if (view instanceof SegmentReader reader)
          readers.add(reader);
      return readers.toArray(new SegmentReader[0]);
    }
  }

  @Test
  void heldFilesSurviveUnreferencedScans() throws IOException {
    useType("Asset");
    final ColumnarManifest.SegmentEntry first = writeSegment(1, 1);
    final ColumnarManifest.SegmentEntry second = writeSegment(2, MAX + 1);
    final ColumnarManifest.SegmentEntry third = writeSegment(3, 2L * MAX + 1);
    manifest(1, List.of(), first, second, third);
    final ColumnarProjection projection = open();
    final SegmentReader[] all = currentSegments(projection);

    // a flush writing its next segment and an owner map, not named by any manifest yet
    final int reserved = projection.reserveSegmentId();
    assertThat(reserved).isEqualTo(4); // above every id on disk
    final Path segment = bucketDir.resolve(SegmentFormat.segmentFileName(reserved));
    final Path ownerMap = bucketDir.resolve("owners-9.own");
    projection.holdFiles(ownerMap.getFileName().toString());
    Files.write(segment, new byte[] { 1 });
    Files.write(ownerMap, new byte[] { 1 });

    // dropping segment 1 releases a state that names an unreferenced file: the scan runs and deletes segment 1 only
    publishSegments(projection, manifest(2, List.of(), second, third), all[1], all[2]);
    assertThat(bucketDir.resolve(first.file())).doesNotExist();
    assertThat(segment).exists();
    assertThat(ownerMap).exists();

    projection.releaseFiles(segment.getFileName().toString(), ownerMap.getFileName().toString());
    publishSegments(projection, manifest(3, List.of(), third), all[2]);
    assertThat(bucketDir.resolve(second.file())).doesNotExist();
    assertThat(segment).doesNotExist();
    assertThat(ownerMap).doesNotExist();
    assertThat(projection.reserveSegmentId()).isEqualTo(5); // never reused
  }

  @Test
  void reservationsStartAboveOrphansAndTheManifest() throws IOException {
    useType("Asset");
    manifest(1, List.of(new ColumnarManifest.DictionaryEntry(11, 3, writeDictionary(11, 3), 0, GlobalDictionary.HEADER_SIZE)),
        writeSegment(2, 1));
    // a crashed rebuild left a newer generation and a higher segment id: both are deleted at open, never reused
    Files.write(bucketDir.resolve(SegmentFormat.dictionaryFileName(11, 4)), new byte[] { 1 });
    Files.write(bucketDir.resolve(SegmentFormat.segmentFileName(7)), new byte[] { 1 });

    final ColumnarProjection projection = open();
    assertThat(projection.isUsable()).isTrue();
    assertThat(bucketDir.resolve(SegmentFormat.segmentFileName(7))).doesNotExist();
    assertThat(projection.reserveSegmentId()).isEqualTo(8);
    assertThat(projection.reserveDictionaryGeneration(11)).isEqualTo(5);
    assertThat(projection.reserveDictionaryGeneration(11)).isEqualTo(6);
    assertThat(projection.reserveDictionaryGeneration(12)).isZero();
  }

  @Test
  void corruptManifestStillSeedsTheReservations() throws IOException {
    useType("Asset");
    writeDictionary(11, 0);
    writeSegment(0, 1);
    Files.writeString(bucketDir.resolve(ColumnarManifest.FILE_NAME), "{ not json");

    final ColumnarProjection projection = open();
    assertThat(projection.unusableReason()).isEqualTo("corrupt");
    // the rebuild markUnusable("corrupt") requests must find names no file uses yet
    assertThat(projection.reserveSegmentId()).isEqualTo(1);
    assertThat(projection.reserveDictionaryGeneration(11)).isEqualTo(1);
    assertThat(bucketDir.resolve(SegmentFormat.dictionaryFileName(11, 0))).exists();
  }

  private String writeDictionary(final int indexFileId, final int generation) throws IOException {
    ColumnarStore.createBucketDir(Path.of(database.getDatabasePath()), bucket.getFileId());
    final String name = SegmentFormat.dictionaryFileName(indexFileId, generation);
    GlobalDictionary.create(bucketDir.resolve(name)).close();
    return name;
  }

  @Test
  void replacedDictionaryGenerationIsClosedOnLastRelease() throws IOException {
    useType("Asset");
    final ColumnarProjection projection = open(); // empty: generation 0 of brand and tags
    final GlobalDictionary old = projection.dictionary("brand");
    old.getOrAdd("acme");
    final ColumnarSnapshot snapshot = projection.acquire();

    // a compaction publishes generation 1 of brand
    final int generation = projection.reserveDictionaryGeneration(11);
    final String name = SegmentFormat.dictionaryFileName(11, generation);
    final GlobalDictionary compacted = GlobalDictionary.create(bucketDir.resolve(name));
    final GlobalDictionary.DurableState durable = compacted.fsync();
    final ColumnarManifest current = projection.manifest();
    final List<ColumnarManifest.DictionaryEntry> entries = new ArrayList<>();
    for (final ColumnarManifest.DictionaryEntry entry : current.dictionaries())
      entries.add(entry.indexFileId() == 11 ? new ColumnarManifest.DictionaryEntry(11, generation, name, durable.entries(), durable.bytes()) :
          entry);
    final ColumnarManifest next = manifest(2, entries);
    final Map<String, GlobalDictionary> copy = new HashMap<>(projection.dictionaries());
    copy.put("brand", compacted);
    final Map<String, GlobalDictionary> dictionaries = Map.copyOf(copy);

    // a dictionary swap without the apply lock could close a generation the apply pass is encoding with
    projection.publishLock().lock();
    try {
      assertThatThrownBy(() -> projection.publish(next, new SegmentReader[0], projection.currentOverlaysForTesting(), dictionaries, true))
          .isInstanceOf(IllegalStateException.class).hasMessageContaining("apply lock");
    } finally {
      projection.publishLock().unlock();
    }
    assertThat(projection.dictionary("brand")).isSameAs(old);

    projection.applyLock().lock();
    projection.publishLock().lock();
    try {
      projection.publish(next, new SegmentReader[0], projection.currentOverlaysForTesting(), dictionaries, true);
    } finally {
      projection.publishLock().unlock();
      projection.applyLock().unlock();
    }
    projection.releaseFiles(name);
    assertThat(projection.dictionary("brand")).isSameAs(compacted);
    assertThat(snapshot.dictionary("brand")).isSameAs(old);
    assertThat(old.getOrAdd("acme")).isZero(); // still open while the snapshot holds it

    snapshot.close();
    assertThatThrownBy(() -> old.getOrAdd("other")).isInstanceOf(IllegalStateException.class).hasMessageContaining("closed");
    assertThat(bucketDir.resolve(SegmentFormat.dictionaryFileName(11, 0))).doesNotExist();
    assertThat(compacted.getOrAdd("other")).isZero();
    assertThat(projection.dictionary("tags").getOrAdd("t")).isZero(); // the unchanged generation stays open
  }

  @Test
  void segmentGaugeIsTheSumOverTheTypeBuckets() throws IOException {
    final DocumentType type = database.getSchema().createDocumentType("Multi", 2);
    final List<ColumnarProjection> projections = new ArrayList<>();
    for (int b = 0; b < 2; b++) {
      bucket = (LocalBucket) type.getBuckets(false).get(b);
      bucketDir = ColumnarStore.bucketDir(Path.of(database.getDatabasePath()), bucket.getFileId());
      if (b == 0)
        manifest(1, List.of(), writeSegment(1, 1), writeSegment(2, MAX));
      else
        manifest(1, List.of(), writeSegment(1, 1));
      projections.add(open());
    }
    assertThat(stats.segments("Multi")).isEqualTo(3);
    projections.get(1).close();
    assertThat(stats.segments("Multi")).isEqualTo(2);
    projections.get(0).drop();
    assertThat(stats.segments("Multi")).isZero();
  }

  @Test
  void renamedColumnKeepsItsSegmentValues() throws IOException {
    useType("Asset");
    manifest(1, List.of(), writeSegment(1, 3));
    final ColumnDefinition[] renamed = COLUMNS.clone();
    renamed[BRAND] = new ColumnDefinition(COLUMNS[BRAND].indexFileId(), "label", Type.STRING, ColumnKind.ORDINAL, false, null);
    final ColumnarProjection projection = ColumnarProjection.open((DatabaseInternal) database, bucket, renamed, stats);
    opened.add(projection);
    final RowValues out = new RowValues(renamed.length);
    assertThat(projection.liveValueLookup(3, BRAND, out)).isEqualTo(1);
    assertThat(out.present[BRAND]).isTrue();
    assertThat(out.ordinal[BRAND]).isEqualTo(3);
  }

  @Test
  void writeFailureOfTheEmptyPublishFailsTheOpen() throws IOException {
    useType("Asset");
    // the bucket directory exists but refuses new files: the empty dictionaries cannot be created
    ColumnarStore.createBucketDir(Path.of(database.getDatabasePath()), bucket.getFileId());
    Files.setPosixFilePermissions(bucketDir, PosixFilePermissions.fromString("r-xr-xr-x"));
    try {
      assertThatThrownBy(this::open).isInstanceOf(UncheckedIOException.class).hasMessageContaining(bucket.getName());
    } finally {
      Files.setPosixFilePermissions(bucketDir, PosixFilePermissions.fromString("rwxr-xr-x"));
    }
    assertThat(listFiles(bucketDir)).isEmpty();
  }

  private static List<String> listFiles(final Path dir) throws IOException {
    try (final var stream = Files.list(dir)) {
      return stream.map(p -> p.getFileName().toString()).sorted().toList();
    }
  }
}
