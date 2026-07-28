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
  // The only source, when there is exactly one. A settled index that has compacted down to a single
  // segment and holds nothing for this dim in the memtable is the common case on a read-mostly
  // workload, and there the merge is pure overhead: the merged position IS the source's position, so
  // every advance can skip the alignment scan and the min-selection pass (issue #5467).
  private final SourceCursor    single;
  // Merged position, held as components with the {@link RID} object materialised on demand. The
  // scorer compares positions far more often than it reads them (issue #5467).
  private int                   currentBucketId = -1;
  private long                  currentPosition = -1L;
  private RID                   currentRidObj;
  // Index of the newest live source aligned at currentRid; -1 when exhausted. The weight and
  // tombstone are resolved lazily from this source only when a caller actually asks for them, so a
  // Block-Max WAND skip that walks currentRid across block boundaries never forces the underlying
  // segment cursor to decode a payload it will not score (issue #5388).
  private int                   newestSourceIdx = -1;
  private boolean               started;
  private boolean               exhausted;

  // Memoised merged block bounds (issue #5467). The Block-Max skip probes these once per candidate,
  // but their value is constant over the whole block range they bound - a default block holds 128
  // postings - so recomputing per candidate walked every live source and, inside each, that
  // segment's block headers. The memo is valid for any probe in {@code [boundsFrom, boundsEnd]};
  // see {@link #ensureBlockBounds} for why a later cursor state cannot invalidate it.
  private boolean               boundsValid;
  private float                 boundsBlockMax;
  private int                   boundsFromBucketId;
  private long                  boundsFromPosition;
  private int                   boundsEndBucketId;
  private long                  boundsEndPosition;
  // The merged block end handed back to callers, or null when no live source bounds a finite one.
  // Held as an object so a repeated probe inside the range allocates nothing.
  private RID                   boundsEndRid;

  public DimCursor(final int dimId, final List<? extends SourceCursor> sources) {
    if (sources == null || sources.isEmpty())
      throw new IllegalArgumentException("at least one source is required");
    this.dimId = dimId;
    this.sources = sources.toArray(new SourceCursor[0]);
    this.sourceLive = new boolean[this.sources.length];
    for (int i = 0; i < this.sources.length; i++)
      this.sourceLive[i] = true;
    this.single = this.sources.length == 1 ? this.sources[0] : null;
  }

  public int dimId() {
    return dimId;
  }

  public RID currentRid() {
    if (currentRidObj == null && currentBucketId >= 0)
      currentRidObj = new RID(currentBucketId, currentPosition);
    return currentRidObj;
  }

  /** Bucket id of the merged position, or {@code -1} when exhausted. */
  public int currentBucketId() {
    return currentBucketId;
  }

  /** Position component of the merged position, or {@code -1} when exhausted. */
  public long currentPosition() {
    return currentPosition;
  }

  public float currentWeight() {
    if (exhausted || newestSourceIdx < 0)
      return 0.0f;
    final SourceCursor s = sources[newestSourceIdx];
    // Resolving the weight forces a lazily-parked segment cursor to decode its block - the single
    // point where a scored document actually pays for a page read.
    return s.isTombstone() ? 0.0f : s.currentWeight();
  }

  public boolean isTombstone() {
    if (exhausted || newestSourceIdx < 0)
      return false;
    return sources[newestSourceIdx].isTombstone();
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

  /**
   * Merged posting count across every source for this dim: how expensive this term is to traverse.
   * {@link BmwScorer} uses it to rank which terms are worth dropping out of the traversal first, so
   * an approximation is fine (sources that cannot answer in O(1) report 0).
   */
  public long documentFrequency() {
    long total = 0L;
    for (final SourceCursor s : sources)
      total += s.documentFrequency();
    return total;
  }

  /**
   * Merged Block-Max WAND upper bound on this dim's contribution to {@code rid}: the max over
   * live sources of their {@link SourceCursor#blockMaxAt(RID) blockMaxAt}. Taking the max is
   * correct because on a conflict the newest source wins and its weight is bounded by its own
   * block max, while every older source's block max independently bounds its own posting.
   */
  public float blockMaxAt(final RID rid) {
    return blockMaxAt(rid.getBucketId(), rid.getPosition());
  }

  /** Primitive-argument {@link #blockMaxAt(RID)}. */
  public float blockMaxAt(final int bucketId, final long position) {
    if (exhausted)
      return 0.0f;
    ensureBlockBounds(bucketId, position);
    return boundsBlockMax;
  }

  /**
   * Right edge of the range that {@link #blockMaxAt(RID)} bounds: the min over live sources of
   * their {@link SourceCursor#blockEndAt(RID) blockEndAt}, ignoring sources that report no finite
   * boundary. Taking the min keeps the merged block-max valid over {@code [rid, blockEnd]} - the
   * tighter (smaller) block boundary of any source is where at least one source's per-block bound
   * stops holding. Returns {@code null} if no live source bounds a finite boundary (e.g. only an
   * in-memory memtable covers {@code rid}), which tells the scorer it cannot block-skip here.
   */
  public RID blockEndAt(final RID rid) {
    return blockEndAt(rid.getBucketId(), rid.getPosition());
  }

  /** Primitive-argument {@link #blockEndAt(RID)}. */
  public RID blockEndAt(final int bucketId, final long position) {
    if (exhausted)
      return null;
    ensureBlockBounds(bucketId, position);
    return boundsEndRid;
  }

  /**
   * Refresh {@code boundsBlockMax} / {@code boundsEndRid} unless the memo already covers
   * {@code (bucketId, position)}, i.e. unless the probe falls inside {@code [boundsFrom, boundsEnd]}.
   * <p>
   * <b>Why a memo across probes is sound.</b> Let {@code b_S} be the block source {@code S} reports
   * for the probe that filled the memo, and {@code end} the smallest of the sources' block ends.
   * For any later probe {@code r} in {@code [from, end]}: {@code b_S} is by definition the first
   * block of {@code S} whose last RID is &gt;= {@code from}, so its predecessor ends strictly before
   * {@code from} &lt;= {@code r}, and its own last RID is &gt;= {@code end} &gt;= {@code r}. A
   * posting of {@code S} at {@code r} therefore lies in {@code b_S}, and the memoised maximum bounds
   * it. Cursor movement between the two probes cannot break that: a source can only move forward, so
   * either it still sits at or before {@code r} (the argument above applies unchanged) or it has
   * moved past {@code r} - and then it has no posting at {@code r} at all, since neither
   * {@link SourceCursor#advance} nor {@link SourceCursor#seekTo} can step over one. A source going
   * exhausted only removes a term from the maximum, leaving the memo an over-estimate, which is
   * always safe for an upper bound.
   * <p>
   * The lower edge of the window is what makes an out-of-order probe recompute. Nothing in the
   * traversal probes backwards - candidates only advance - but {@link #blockMaxAt} is public API and
   * a bound taken from a block that starts after the probe would not cover it.
   * <p>
   * When no live source reports a finite block end (an in-memory memtable does not), the window
   * collapses to the probe itself: the maximum is still correct, there is simply no range over which
   * to reuse it, and the scorer cannot block-skip without a boundary anyway.
   */
  private void ensureBlockBounds(final int bucketId, final long position) {
    if (boundsValid
        && SparseSegmentBuilder.compareRid(bucketId, position, boundsFromBucketId, boundsFromPosition) >= 0
        && SparseSegmentBuilder.compareRid(bucketId, position, boundsEndBucketId, boundsEndPosition) <= 0)
      return;

    float max = 0.0f;
    RID end = null;
    for (int i = 0; i < sources.length; i++) {
      if (!sourceLive[i])
        continue;
      final SourceCursor s = sources[i];
      final float bm = s.blockMaxAt(bucketId, position);
      if (bm > max)
        max = bm;
      final RID be = s.blockEndAt(bucketId, position);
      if (be != null && (end == null || SparseSegmentBuilder.compareRid(be, end) < 0))
        end = be;
    }

    boundsBlockMax = max;
    boundsEndRid = end;
    boundsFromBucketId = bucketId;
    boundsFromPosition = position;
    boundsEndBucketId = end != null ? end.getBucketId() : bucketId;
    boundsEndPosition = end != null ? end.getPosition() : position;
    boundsValid = true;
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
    if (currentBucketId < 0)
      return false;

    // Single source: it is by construction the one sitting on the merged position, so neither the
    // alignment test nor the min-selection pass has anything to decide (issue #5467).
    if (single != null) {
      if (!single.advance())
        sourceLive[0] = false;
      materializeSingle();
      return !exhausted;
    }

    // Advance all sources currently aligned at the merged position.
    final int consumedBucketId = currentBucketId;
    final long consumedPosition = currentPosition;
    for (int i = 0; i < sources.length; i++) {
      if (!sourceLive[i])
        continue;
      if (sources[i].currentPosition() == consumedPosition && sources[i].currentBucketId() == consumedBucketId) {
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
    return seekTo(target.getBucketId(), target.getPosition());
  }

  /** Primitive-argument {@link #seekTo(RID)}. */
  public boolean seekTo(final int targetBucketId, final long targetPosition) throws IOException {
    if (exhausted)
      return false;
    if (!started) {
      start();
    }
    if (currentBucketId >= 0
        && SparseSegmentBuilder.compareRid(currentBucketId, currentPosition, targetBucketId, targetPosition) >= 0)
      return true;

    for (int i = 0; i < sources.length; i++) {
      if (!sourceLive[i])
        continue;
      if (!sources[i].seekTo(targetBucketId, targetPosition))
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
    currentBucketId = -1;
    currentPosition = -1L;
    currentRidObj = null;
    newestSourceIdx = -1;
    boundsValid = false;
    boundsEndRid = null;
  }

  // ---------- internals ----------

  /**
   * Compute {@code currentRid} as the min over live sources and record which source ({@code
   * newestSourceIdx}) supplies its weight/tombstone, in a single pass. The weight and tombstone
   * are NOT read here - they are resolved lazily by {@link #currentWeight()} / {@link
   * #isTombstone()} so a BMW skip that only ever reads {@code currentRid} never forces the winning
   * segment cursor to decode its block (issue #5388).
   * <p>
   * Sources are passed oldest-first, so a higher index means newer. We track the min RID and,
   * for any source that ties the running min, prefer the newest. Two pieces of state suffice:
   * {@code minRid} and {@code newestAtMinIdx}. When a strictly smaller RID is seen, both are
   * reset; when an equal RID is seen on a newer source, only the newest-index is updated.
   */
  private void materializeMin() {
    if (single != null) {
      materializeSingle();
      return;
    }
    int minBucketId = -1;
    long minPosition = -1L;
    int newestAtMinIdx = -1;
    for (int i = 0; i < sources.length; i++) {
      if (!sourceLive[i])
        continue;
      final int bucketId = sources[i].currentBucketId();
      if (bucketId < 0) {
        sourceLive[i] = false;
        continue;
      }
      final long position = sources[i].currentPosition();
      if (newestAtMinIdx < 0) {
        minBucketId = bucketId;
        minPosition = position;
        newestAtMinIdx = i;
        continue;
      }
      final int cmp = SparseSegmentBuilder.compareRid(bucketId, position, minBucketId, minPosition);
      if (cmp < 0) {
        minBucketId = bucketId;
        minPosition = position;
        newestAtMinIdx = i;
      } else if (cmp == 0) {
        // i > newestAtMinIdx since we scan oldest-first and only revisit the same RID on a newer
        // source; just bumping the index keeps newest-wins semantics without a second sweep.
        newestAtMinIdx = i;
      }
    }
    if (newestAtMinIdx < 0) {
      exhausted = true;
      currentBucketId = -1;
      currentPosition = -1L;
      currentRidObj = null;
      newestSourceIdx = -1;
      return;
    }
    currentBucketId = minBucketId;
    currentPosition = minPosition;
    currentRidObj = null;
    newestSourceIdx = newestAtMinIdx;
  }

  /**
   * {@link #materializeMin()} specialised to a lone source: the merge degenerates to "whatever the
   * source says", so the min-selection scan and its newest-wins tie handling drop out entirely.
   * Behaviour is identical to the general path with {@code sources.length == 1}.
   */
  private void materializeSingle() {
    final int bucketId = sourceLive[0] ? single.currentBucketId() : -1;
    if (bucketId < 0) {
      sourceLive[0] = false;
      exhausted = true;
      currentBucketId = -1;
      currentPosition = -1L;
      currentRidObj = null;
      newestSourceIdx = -1;
      return;
    }
    currentBucketId = bucketId;
    currentPosition = single.currentPosition();
    currentRidObj = null;
    newestSourceIdx = 0;
  }

}
