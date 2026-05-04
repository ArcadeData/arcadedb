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
package com.arcadedb.index.sparsevector;

import com.arcadedb.database.RID;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

/**
 * N-way merger for sealed sparse segments. Used by background compaction (size-tiered policy)
 * and by manual {@code REBUILD INDEX}. Inputs are immutable; the worker never mutates them.
 * <p>
 * Merge semantics:
 * <ul>
 *   <li>Inputs are passed <i>oldest -&gt; newest</i> in {@code inputsOldestFirst}.</li>
 *   <li>Per-dim, postings are emitted in RID-ascending order. Conflicts at the same RID resolve
 *       to the newest input's value (live or tombstone), which is the same precedence rule the
 *       merged-source query cursor enforces at read time.</li>
 *   <li>Tombstones are emitted when {@code dropAllTombstones == false}; pass {@code true} only
 *       when the compaction input set is known to span the entire universe of segments
 *       (e.g.&nbsp;a {@code REBUILD INDEX} that compacts everything).</li>
 *   <li>The output's {@code parent_segments} list records the input segment IDs so a recovery
 *       step can reason about lineage and skip duplicates.</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class CompactionWorker {

  private CompactionWorker() {
    // utility class
  }

  public static SparseSegmentReader compact(final SparseSegmentReader[] inputsOldestFirst, final Path targetFile,
      final long segmentId, final SegmentParameters params) throws IOException {
    return compact(inputsOldestFirst, targetFile, segmentId, params, false);
  }

  public static SparseSegmentReader compact(final SparseSegmentReader[] inputsOldestFirst, final Path targetFile,
      final long segmentId, final SegmentParameters params, final boolean dropAllTombstones) throws IOException {
    if (inputsOldestFirst == null || inputsOldestFirst.length == 0)
      throw new IllegalArgumentException("at least one input segment is required");

    // Collect union of dims across inputs.
    final TreeSet<Integer> allDims = new TreeSet<>();
    for (final SparseSegmentReader r : inputsOldestFirst)
      for (final int d : r.dims())
        allDims.add(d);

    final long[] parents = new long[inputsOldestFirst.length];
    for (int i = 0; i < inputsOldestFirst.length; i++)
      parents[i] = inputsOldestFirst[i].segmentId();

    final Path tmpFile = targetFile.resolveSibling(targetFile.getFileName().toString() + ".tmp");
    boolean wroteAnything = false;
    try {
      try (final SparseSegmentWriter w = new SparseSegmentWriter(tmpFile, params)) {
        w.setSegmentId(segmentId);
        w.setParentSegments(parents);

        for (final int dim : allDims) {
          final boolean any = mergeDim(w, dim, inputsOldestFirst, dropAllTombstones);
          wroteAnything |= any;
        }

        w.finish();
      }
      if (!wroteAnything) {
        // All input postings were tombstones that we dropped. The empty segment is still valid
        // but useless - delete it and return null so the caller can just remove the inputs.
        Files.deleteIfExists(tmpFile);
        return null;
      }
      Files.move(tmpFile, targetFile, java.nio.file.StandardCopyOption.ATOMIC_MOVE);
    } catch (final RuntimeException | IOException e) {
      Files.deleteIfExists(tmpFile);
      throw e;
    }
    return new SparseSegmentReader(targetFile);
  }

  /**
   * Merge one dim across all input cursors that carry it. Returns {@code true} if at least one
   * posting was emitted to the writer.
   */
  private static boolean mergeDim(final SparseSegmentWriter w, final int dim, final SparseSegmentReader[] inputs,
      final boolean dropAllTombstones) throws IOException {
    final List<DimSource> sources = new ArrayList<>(inputs.length);
    try {
      for (int i = 0; i < inputs.length; i++) {
        final SegmentDimCursor c = inputs[i].openCursor(dim);
        if (c == null)
          continue;
        c.start();
        if (c.isExhausted()) {
          c.close();
          continue;
        }
        sources.add(new DimSource(c, i));
      }
      if (sources.isEmpty())
        return false;

      boolean dimOpened = false;
      while (!sources.isEmpty()) {
        // Find the smallest currentRid across live sources.
        RID minRid = sources.get(0).cursor.currentRid();
        for (int i = 1; i < sources.size(); i++) {
          final RID r = sources.get(i).cursor.currentRid();
          if (SparseSegmentWriter.compareRid(r, minRid) < 0)
            minRid = r;
        }

        // Pick the newest source aligned at minRid.
        DimSource newest = null;
        for (final DimSource s : sources) {
          if (minRid.equals(s.cursor.currentRid())) {
            if (newest == null || s.priority > newest.priority)
              newest = s;
          }
        }

        // Emit.
        final boolean tombstone = newest.cursor.isTombstone();
        if (tombstone && dropAllTombstones) {
          // skip
        } else {
          if (!dimOpened) {
            w.startDim(dim);
            dimOpened = true;
          }
          if (tombstone)
            w.appendTombstone(minRid);
          else
            w.appendPosting(minRid, newest.cursor.currentWeight());
        }

        // Advance every aligned cursor; drop those that exhaust.
        for (final Iterator<DimSource> it = sources.iterator(); it.hasNext(); ) {
          final DimSource s = it.next();
          if (minRid.equals(s.cursor.currentRid())) {
            if (!s.cursor.advance()) {
              s.cursor.close();
              it.remove();
            }
          }
        }
      }

      if (dimOpened) {
        w.endDim();
        return true;
      }
      return false;
    } finally {
      for (final DimSource s : sources)
        s.cursor.close();
    }
  }

  private static final class DimSource {
    final SegmentDimCursor cursor;
    final int              priority; // higher = newer

    DimSource(final SegmentDimCursor cursor, final int priority) {
      this.cursor = cursor;
      this.priority = priority;
    }
  }
}
