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
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * {@link SourceCursor} backed by an in-memory {@link Memtable}. Iterates one dim's postings in
 * RID-ascending order, surfacing live entries and tombstones the same way a sealed segment
 * cursor does so the merging {@link DimCursor} can mix the two sources without special cases.
 * <p>
 * The upper bound is conservative: we do not maintain per-block max metadata in the memtable,
 * so {@link #upperBoundRemaining()} returns the cached dim-wide max (recomputed lazily). This
 * is correct, just less aggressive at pruning than the sealed-segment skip-list lookup.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class MemtableSourceCursor implements SourceCursor {

  private final Memtable memtable;
  private final int      dim;
  // Mutable: replaced by a tail iterator on seekTo so block-skips are O(log n) over the
  // ConcurrentSkipListMap rather than O(n) one-entry-at-a-time advances.
  private Iterator<MemtablePosting> iterator;
  private final float               dimMaxWeight;

  private RID     currentRid;
  private float   currentWeight;
  private boolean currentTombstone;
  private boolean started;
  private boolean exhausted;
  // Buffered next: filled lazily by start() / advance().
  private MemtablePosting buffered;

  public MemtableSourceCursor(final Memtable memtable, final int dim) {
    this.memtable = memtable;
    this.dim = dim;
    this.iterator = memtable.iterateDim(dim);
    // Read the running per-dim max maintained by {@link Memtable#put} - O(1). The previous
    // implementation walked the dim's postings a second time at construction; for a memtable
    // with thousands of postings per dim that doubled the per-cursor cost.
    this.dimMaxWeight = memtable.dimMaxWeight(dim);
  }

  @Override
  public void start() throws IOException {
    if (started)
      return;
    started = true;
    if (!iterator.hasNext()) {
      exhausted = true;
      return;
    }
    buffered = iterator.next();
    materializeBuffered();
  }

  @Override
  public boolean advance() throws IOException {
    if (exhausted)
      return false;
    if (!started) {
      start();
      return !exhausted;
    }
    if (!iterator.hasNext()) {
      exhausted = true;
      currentRid = null;
      buffered = null;
      return false;
    }
    buffered = iterator.next();
    materializeBuffered();
    return true;
  }

  @Override
  public boolean seekTo(final RID target) throws IOException {
    if (exhausted)
      return false;
    started = true;
    // If the current entry is already at/past the target, nothing to do.
    if (currentRid != null && SparseSegmentBuilder.compareRid(currentRid, target) >= 0)
      return true;
    // Replace the iterator with an O(log n) tail iterator anchored at target. Avoids the
    // one-entry-at-a-time advance the previous implementation did, which dominated query latency
    // on large memtables (BMW block-skip can jump tens of thousands of postings at once).
    iterator = memtable.iterateDimFrom(dim, target);
    if (!iterator.hasNext()) {
      exhausted = true;
      currentRid = null;
      buffered = null;
      return false;
    }
    buffered = iterator.next();
    materializeBuffered();
    return !exhausted;
  }

  @Override
  public RID currentRid() {
    return currentRid;
  }

  @Override
  public float currentWeight() {
    return currentWeight;
  }

  @Override
  public boolean isTombstone() {
    return currentTombstone;
  }

  @Override
  public boolean isExhausted() {
    return exhausted;
  }

  /**
   * Returns the dim's running max live weight from the memtable. Used by BMW DAAT as the upper
   * bound on this source's contribution to the running prefix-sum.
   * <p>
   * <b>Looseness under tombstone load.</b> {@link Memtable#dimMaxWeight} is monotonically
   * non-decreasing by design (it bumps on {@code put} and is never lowered on {@code remove})
   * - that is the simplest way to keep BMW correct under concurrent writers without scanning
   * the dim on every read. The flip side: when the dim's previous max-weight posting has been
   * tombstoned but a smaller weight remains, the bound is permanently too high until the
   * memtable is flushed and re-snapshotted, which means BMW will skip less aggressively (the
   * pivot shift fires later than it would with an exact bound). For workloads with high
   * tombstone density on heavy-weight postings, this can cost up to a constant factor of skip
   * efficiency on queries that touch this dim. Correctness is unaffected; an over-estimate is
   * always safe for an upper bound. If profiling shows this matters, the fix is either a
   * background re-scan that lowers the bound when the max-bearing posting is tombstoned, or
   * triggering an earlier flush so the sealed-segment per-block max takes over.
   */
  @Override
  public float upperBoundRemaining() {
    return exhausted ? 0.0f : dimMaxWeight;
  }

  @Override
  public void close() {
    exhausted = true;
    currentRid = null;
    buffered = null;
  }

  public int dim() {
    return dim;
  }

  private void materializeBuffered() {
    if (buffered == null) {
      currentRid = null;
      return;
    }
    currentRid = buffered.rid();
    currentTombstone = buffered.tombstone();
    currentWeight = currentTombstone ? Float.NaN : buffered.weight();
  }
}
