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

  private final int                      dim;
  private final Iterator<MemtablePosting> iterator;
  private final float                    dimMaxWeight;

  private RID     currentRid;
  private float   currentWeight;
  private boolean currentTombstone;
  private boolean started;
  private boolean exhausted;
  // Buffered next: filled lazily by start() / advance().
  private MemtablePosting buffered;

  public MemtableSourceCursor(final Memtable memtable, final int dim) {
    this.dim = dim;
    this.iterator = memtable.iterateDim(dim);
    this.dimMaxWeight = computeDimMax(memtable, dim);
  }

  /** Snapshot the maximum live weight for {@code dim} in {@code memtable}. */
  private static float computeDimMax(final Memtable memtable, final int dim) {
    final Iterator<MemtablePosting> it = memtable.iterateDim(dim);
    float m = 0.0f;
    while (it.hasNext()) {
      final MemtablePosting p = it.next();
      if (!p.tombstone() && p.weight() > m)
        m = p.weight();
    }
    return m;
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
    if (!started)
      start();
    while (currentRid != null && SparseSegmentWriter.compareRid(currentRid, target) < 0) {
      if (!advance())
        return false;
    }
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
