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
import java.util.ArrayList;
import java.util.List;

/**
 * Per-dim cursor that merges postings across multiple {@link SourceCursor sources}, exposing
 * a unified forward-only iteration in ascending RID order.
 * <p>
 * <b>Ordering of sources matters.</b> Sources are passed in <i>oldest to newest</i> order; on
 * conflict (the same RID present in multiple sources), the newest source's weight wins. A
 * tombstone in any source masks the RID across all older sources for the duration of this
 * cursor's lifetime.
 * <p>
 * Iteration discipline mirrors {@link SourceCursor}: call {@link #start()} once, then
 * {@link #advance()} until it returns false, with optional {@link #seekTo(RID)} for BMW
 * block-skip.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class DimCursor implements AutoCloseable {

  private final int             dimId;
  private final SourceCursor[]  sources;     // sorted oldest -> newest
  private final boolean[]       sourceLive;  // false once a source is exhausted
  private RID                   currentRid;
  private float                 currentWeight;
  private boolean               currentTombstone;
  private boolean               started;
  private boolean               exhausted;

  public DimCursor(final int dimId, final List<? extends SourceCursor> sources) {
    if (sources == null || sources.isEmpty())
      throw new IllegalArgumentException("at least one source is required");
    this.dimId = dimId;
    this.sources = sources.toArray(new SourceCursor[0]);
    this.sourceLive = new boolean[this.sources.length];
    for (int i = 0; i < this.sources.length; i++)
      this.sourceLive[i] = true;
  }

  public int dimId() {
    return dimId;
  }

  public RID currentRid() {
    return currentRid;
  }

  public float currentWeight() {
    return currentWeight;
  }

  public boolean isTombstone() {
    return currentTombstone;
  }

  public boolean isExhausted() {
    return exhausted;
  }

  /**
   * BMW upper bound: max possible weight contribution from any remaining live posting in any
   * source for this dim. Computed as the max across sources of their {@code upperBoundRemaining}.
   */
  public float upperBoundRemaining() {
    if (exhausted)
      return 0.0f;
    float m = 0.0f;
    for (int i = 0; i < sources.length; i++) {
      if (!sourceLive[i])
        continue;
      final float ub = sources[i].upperBoundRemaining();
      if (ub > m)
        m = ub;
    }
    return m;
  }

  public void start() throws IOException {
    if (started)
      return;
    started = true;
    for (int i = 0; i < sources.length; i++) {
      sources[i].start();
      if (sources[i].isExhausted())
        sourceLive[i] = false;
    }
    materializeMin();
  }

  /**
   * Advance to the next merged RID. Returns false when all sources are exhausted.
   */
  public boolean advance() throws IOException {
    if (exhausted)
      return false;
    if (!started) {
      start();
      return !exhausted;
    }
    if (currentRid == null)
      return false;

    // Advance all sources currently aligned at currentRid.
    final RID consumed = currentRid;
    for (int i = 0; i < sources.length; i++) {
      if (!sourceLive[i])
        continue;
      if (consumed.equals(sources[i].currentRid())) {
        if (!sources[i].advance())
          sourceLive[i] = false;
      }
    }
    materializeMin();
    return !exhausted;
  }

  /**
   * Forward-seek every source to the first posting whose RID >= target.
   */
  public boolean seekTo(final RID target) throws IOException {
    if (exhausted)
      return false;
    if (!started) {
      start();
    }
    if (currentRid != null && SparseSegmentBuilder.compareRid(currentRid, target) >= 0)
      return true;

    for (int i = 0; i < sources.length; i++) {
      if (!sourceLive[i])
        continue;
      if (!sources[i].seekTo(target))
        sourceLive[i] = false;
    }
    materializeMin();
    return !exhausted;
  }

  @Override
  public void close() {
    for (final SourceCursor c : sources)
      c.close();
    exhausted = true;
    currentRid = null;
  }

  // ---------- internals ----------

  /**
   * Compute {@code currentRid} as the min over live sources and resolve {@code currentWeight} /
   * {@code currentTombstone} as the newest source's value at that RID, in a single pass.
   * <p>
   * Sources are passed oldest-first, so a higher index means newer. We track the min RID and,
   * for any source that ties the running min, prefer the newest. Two pieces of state suffice:
   * {@code minRid} and {@code newestAtMinIdx}. When a strictly smaller RID is seen, both are
   * reset; when an equal RID is seen on a newer source, only the newest-index is updated.
   */
  private void materializeMin() {
    RID minRid = null;
    int newestAtMinIdx = -1;
    for (int i = 0; i < sources.length; i++) {
      if (!sourceLive[i])
        continue;
      final RID rid = sources[i].currentRid();
      if (rid == null) {
        sourceLive[i] = false;
        continue;
      }
      if (minRid == null) {
        minRid = rid;
        newestAtMinIdx = i;
        continue;
      }
      final int cmp = SparseSegmentBuilder.compareRid(rid, minRid);
      if (cmp < 0) {
        minRid = rid;
        newestAtMinIdx = i;
      } else if (cmp == 0) {
        // i > newestAtMinIdx since we scan oldest-first and only revisit the same RID on a newer
        // source; just bumping the index keeps newest-wins semantics without a second sweep.
        newestAtMinIdx = i;
      }
    }
    if (minRid == null) {
      exhausted = true;
      currentRid = null;
      currentTombstone = false;
      currentWeight = 0.0f;
      return;
    }
    final SourceCursor newest = sources[newestAtMinIdx];
    currentRid = minRid;
    currentTombstone = newest.isTombstone();
    currentWeight = currentTombstone ? 0.0f : newest.currentWeight();
  }

}
