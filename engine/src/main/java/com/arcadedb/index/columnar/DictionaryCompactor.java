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

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Dictionary compaction of one bucket's projection (spec 5.4), run on {@link ColumnarMaintenancePool}: a dictionary generation whose
 * ordinals are mostly referenced by no live row is rewritten as generation {@code n + 1} holding only the referenced values, and every
 * segment and the live overlay are re-encoded with it, in one manifest swap.
 * <p>
 * Steps: (1) the referenced ordinals, a bitset over the generation filled by {@link ColumnReader#forEachOrdinal} over the live rows of
 * every segment and overlay view of a retained state; (2) a new generation file ({@link GlobalDictionary#create}, R10, its name
 * reserved by {@link ColumnarProjection#reserveDictionaryGeneration}) holding the surviving values in their old ordinal order, so the
 * ordinals stay insert-ordered and dense, fsynced ({@code compact.dictionaryWritten}); (3) a new segment per segment holding the column,
 * its live rows with their positions, page versions and other columns, the column's ordinals remapped (lists sorted again), written
 * with no lock held while queries and apply passes go on ({@code compact.segmentsWritten}); the dead rows of a segment are dropped, as
 * a merge drops them, since their ordinals may be gone; (4) the publish, holding {@link ColumnarProjection#applyLock()} (the apply
 * pass boundary, R13): the values the apply passes added to generation {@code n} since step 1 are appended to {@code n + 1}, a value
 * the scan found unreferenced and an overlay row references by now is appended again, the live overlay's pages are rebuilt as NEW
 * blocks with the remapped ordinals ({@link ColumnarProjection#remapOverlay}), and the {@link ColumnarFlusher} publish (Task 5 order,
 * its phase-based failure handling and the generation of {@link ColumnarProjection#nextGeneration()}) names the new segments and
 * generation; under the publish lock the overlay content and the writer dictionary switch with the state. A position an apply pass
 * killed in a segment meanwhile starts dead in its replacement.
 * <p>
 * A snapshot acquired before the swap keeps the old segments and generation {@code n} ({@link ColumnarSnapshot#dictionary}) until it
 * closes; the last release closes the old generation and deletes its files (R1, R2). A query never waits on a compaction. A compaction
 * holds the projection's maintenance slot ({@link ColumnarProjection#tryBeginMaintenance()}, V-B6), so it never runs beside a flush,
 * merge, rebuild or reconcile of the projection, and does nothing while a failed flush left a frozen overlay.
 */
final class DictionaryCompactor {
  /** Reached once the new generation is written and fsynced. */
  static final String DICTIONARY_WRITTEN = "compact.dictionaryWritten";
  /** Reached once every re-encoded segment is written, before the publish takes the apply lock. */
  static final String SEGMENTS_WRITTEN   = "compact.segmentsWritten";
  /** Entries below which a dictionary is never compacted. */
  static final int    MIN_ENTRIES        = 1024;

  private DictionaryCompactor() {
  }

  /**
   * {@code 1 - referenced / size} over the dictionary generation the current state is encoded with, referenced being the ordinals of
   * the live rows of every segment and overlay view of that state; 0 for a column with no dictionary or an empty one.
   */
  static double deadOrdinalRatio(final ColumnarProjection p, final String property) {
    final ProjectionState state = p.retainState();
    try {
      final GlobalDictionary dictionary = state.dictionary(property);
      final int size = dictionary == null ? 0 : dictionary.size();
      if (size == 0)
        return 0;
      return (double) (size - cardinality(referenced(state, property, size))) / size;
    } finally {
      state.release();
    }
  }

  /** The dictionary of the property holds at least {@link #MIN_ENTRIES} entries and {@link #deadOrdinalRatio} reaches {@code ratio}. */
  static boolean shouldCompact(final ColumnarProjection p, final String property, final float ratio) {
    final GlobalDictionary dictionary = p.dictionary(property);
    // in float, as the ratio is configured: 600 dead of 2_000 ordinals reaches 0.3f
    return dictionary != null && dictionary.size() >= MIN_ENTRIES && (float) deadOrdinalRatio(p, property) >= ratio;
  }

  /**
   * Compacts the dictionary of the property (see the class comment). Nothing happens when no ordinal is reclaimable, on a READ_ONLY
   * database, a closed or dropped projection, one without a manifest or with a frozen overlay, and for a property the current manifest
   * does not publish as an ordinal column.
   *
   * @return true when a new generation was published
   *
   * @throws ColumnarFlusher.MaintenanceBusyException when another maintenance job of the projection runs
   */
  static boolean compact(final ColumnarProjection p, final String property) throws IOException {
    ColumnarFlusher.beginMaintenance(p);
    try {
      final ColumnDefinition column = publishedOrdinalColumn(p, property);
      if (column == null || !p.canWrite() || p.manifest() == null || p.hasFrozenOverlay())
        return false;
      final ProjectionState state = p.retainState();
      try {
        return compactHoldingState(p, state, column);
      } catch (final ColumnarFlusher.PublishingStoppedException e) {
        // closed, dropped or stopped meanwhile: nothing is published, the files written are deleted
        return false;
      } finally {
        state.release();
      }
    } finally {
      p.endMaintenance();
    }
  }

  private static ColumnDefinition publishedOrdinalColumn(final ColumnarProjection p, final String property) {
    for (final ColumnDefinition column : p.columns())
      if (column.property().equals(property))
        return (column.kind() == ColumnKind.ORDINAL || column.kind() == ColumnKind.ORDINAL_LIST) && p.isPublishedWith(column.indexFileId()) ?
            column : null;
    return null;
  }

  private static boolean compactHoldingState(final ColumnarProjection p, final ProjectionState state, final ColumnDefinition column)
      throws IOException {
    final String property = column.property();
    final GlobalDictionary old = state.dictionary(property);
    if (old == null || old != p.dictionary(property) || !state.isUsable())
      return false;
    final int scanned = old.size();
    final long[] referenced = referenced(state, property, scanned);
    if (cardinality(referenced) == scanned)
      return false;
    final ColumnDefinition[] columns = p.columns();
    int k = columns.length - 1;
    while (columns[k].indexFileId() != column.indexFileId())
      k--;
    final int remappedColumn = k;
    final SegmentReader[] inputs = state.segments();

    final ColumnarFlusher.Attempt attempt = new ColumnarFlusher.Attempt();
    GlobalDictionary fresh = null;
    long reclaimed = 0;
    try {
      final int generation = p.reserveDictionaryGeneration(column.indexFileId());
      final String name = SegmentFormat.dictionaryFileName(column.indexFileId(), generation);
      attempt.held.add(name);
      fresh = GlobalDictionary.create(p.bucketDir().resolve(name));
      attempt.created(p.bucketDir().resolve(name));
      final Remap remap = new Remap(old, fresh, referenced, scanned);
      // sorted here, with no lock held
      fresh.rebuildRankView();
      fresh.fsync();
      ColumnarTestHooks.reach(DICTIONARY_WRITTEN);

      final Path[] files = new Path[inputs.length];
      final long[] rows = new long[inputs.length];
      for (int i = 0; i < inputs.length; i++) {
        if (ColumnarProjection.columnReader(inputs[i], column) == null)
          continue; // a segment written before the column existed: nothing to re-encode
        final int segmentId = p.reserveSegmentId();
        final String segmentName = SegmentFormat.segmentFileName(segmentId);
        attempt.held.add(segmentName);
        files[i] = p.bucketDir().resolve(segmentName);
        rows[i] = ColumnarFlusher.writeMerged(p, new SegmentReader[] { inputs[i] }, columns, files[i], segmentId,
            row -> remap.row(row, remappedColumn, column.kind()), attempt);
      }
      ColumnarTestHooks.reach(SEGMENTS_WRITTEN);

      p.applyLock().lock();
      try {
        final SegmentReader[] segments = p.currentState().segments();
        // the maintenance slot keeps flushes, merges and rebuilds out, so the segment ids are the scanned ones; checked all the same
        if (!sameSegmentIds(segments, inputs))
          return false;
        final SegmentReader[] next = new SegmentReader[segments.length];
        int n = 0;
        for (int i = 0; i < segments.length; i++) {
          if (files[i] == null)
            next[n++] = segments[i];
          else if (rows[i] > 0) {
            final SegmentReader reader = SegmentReader.open(files[i], ColumnarFlusher.addDeadSince(null, segments[i].deadSet(),
                inputs[i].deadSet()));
            attempt.readers.add(reader);
            next[n++] = reader;
          }
          // else: no live row was left, the segment leaves the manifest
        }
        remap.extend();
        // the values appended from here on stay outside the rank view, as the apply pass's appends between two rank rebuilds do:
        // readers break rank ties with compare()
        final OverlayPageBlock[] blocks = p.remapOverlay(column.indexFileId(), remap::overlayOrdinal);
        final Map<String, GlobalDictionary> dictionaries = new HashMap<>(p.dictionaries());
        dictionaries.put(property, fresh);
        ColumnarFlusher.publish(p, Arrays.copyOf(next, n), () -> p.compactionPublished(blocks), Map.copyOf(dictionaries),
            new ColumnarManifest.DictionaryEntry(column.indexFileId(), generation, name, 0, 0), p.manifest().lastTransactionId(), attempt,
            () -> {
            });
        reclaimed = old.size() - fresh.size();
      } finally {
        p.applyLock().unlock();
      }
    } catch (final ColumnarTestHooks.SimulatedCrash e) {
      throw e;
    } catch (final Throwable e) {
      abandon(p, attempt, fresh);
      throw e;
    }
    abandon(p, attempt, fresh);
    if (attempt.published)
      p.stats().dictionaryCompaction(p.typeName(), property, reclaimed);
    return attempt.published;
  }

  /** Settles the attempt; the new generation is closed unless a published state owns it now. */
  private static void abandon(final ColumnarProjection p, final ColumnarFlusher.Attempt attempt, final GlobalDictionary fresh) {
    ColumnarFlusher.settle(p, attempt);
    if (!attempt.published && fresh != null)
      fresh.close();
  }

  private static boolean sameSegmentIds(final SegmentReader[] a, final SegmentReader[] b) {
    if (a.length != b.length)
      return false;
    for (int i = 0; i < a.length; i++)
      if (a[i].segmentId() != b[i].segmentId())
        return false;
    return true;
  }

  /** The ordinals below {@code size} the live rows of the state's views reference, one bit each; no per-ordinal object. */
  private static long[] referenced(final ProjectionState state, final String property, final int size) {
    final long[] bits = new long[(size + 63) >>> 6];
    final OrdinalConsumer mark = (position, ordinal) -> {
      if (ordinal < size)
        bits[ordinal >>> 6] |= 1L << ordinal;
    };
    for (final ColumnarSegmentView view : state.segments())
      mark(view, property, mark);
    for (final ColumnarSegmentView view : state.overlays())
      mark(view, property, mark);
    return bits;
  }

  private static void mark(final ColumnarSegmentView view, final String property, final OrdinalConsumer mark) {
    final ColumnReader column = view.column(property);
    if (column != null && (column.kind() == ColumnKind.ORDINAL || column.kind() == ColumnKind.ORDINAL_LIST))
      column.forEachOrdinal(view.live(), mark);
  }

  private static int cardinality(final long[] bits) {
    int count = 0;
    for (final long word : bits)
      count += Long.bitCount(word);
    return count;
  }

  /** Old ordinal to new ordinal, -1 for a value dropped; grows the new generation as the publish finds more referenced values. */
  private static final class Remap {
    private final GlobalDictionary from;
    private final GlobalDictionary to;
    private int[]                  map;

    /** Appends the referenced values of the first {@code size} ordinals to {@code to}, in ordinal order. */
    private Remap(final GlobalDictionary from, final GlobalDictionary to, final long[] referenced, final int size) {
      this.from = from;
      this.to = to;
      this.map = new int[size];
      for (int o = 0; o < size; o++)
        map[o] = (referenced[o >>> 6] & 1L << o) != 0 ? to.getOrAdd(from.valueOf(o)) : -1;
    }

    /** Under the apply lock: appends every value added to {@code from} since the scan. */
    private void extend() {
      final int size = from.size();
      if (size == map.length)
        return;
      final int scanned = map.length;
      map = Arrays.copyOf(map, size);
      for (int o = scanned; o < size; o++)
        map[o] = to.getOrAdd(from.valueOf(o));
    }

    /**
     * An overlay ordinal, after {@link #extend()}: a value the scan found unreferenced that an overlay row references by now (an
     * update applied since) is appended again.
     */
    private int overlayOrdinal(final int ordinal) {
      int mapped = map[ordinal];
      if (mapped < 0)
        map[ordinal] = mapped = to.getOrAdd(from.valueOf(ordinal));
      return mapped;
    }

    /**
     * Re-encodes column {@code k} of a segment row read by the rewrite: every ordinal of a live row was referenced, so a dropped one
     * stays -1 and the segment writer refuses it.
     */
    private void row(final RowValues row, final int k, final ColumnKind kind) {
      if (!row.present[k])
        return;
      if (kind == ColumnKind.ORDINAL) {
        row.ordinal[k] = map[row.ordinal[k]];
        return;
      }
      final int[] list = row.list[k];
      final int count = row.listCount[k];
      for (int i = 0; i < count; i++)
        list[i] = map[list[i]];
      Arrays.sort(list, 0, count);
    }
  }
}
