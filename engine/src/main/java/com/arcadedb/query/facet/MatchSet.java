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
package com.arcadedb.query.facet;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.RID;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.index.columnar.ColumnarIndexManager;
import com.arcadedb.index.columnar.ColumnarProjection;
import com.arcadedb.index.columnar.ColumnarSegmentView;
import com.arcadedb.index.columnar.ColumnarSnapshot;
import com.arcadedb.index.columnar.PositionBitmap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * The records a FACETS query matched, as one {@link PositionBitmap} per bucket, plus the disjunctive bookkeeping of the facet FILTERs
 * (spec 7.5): a facet that carries a FILTER counts the records that pass every filter, plus those that fail only its own.
 * <p>
 * Per bucket three kinds of bitmap are kept: the base (every WHERE match), the pass-all (fail mask 0) and, lazily, one failed-only
 * bitmap per filter index (the records whose only failing filter is that one). A facet's set is then derived on demand, so memory is
 * one bitmap per kind instead of one per facet.
 * <p>
 * Memory is charged to {@link FacetMemoryBudget} in 1 MiB steps, one stripe of {@code maxRecordsInPage / 8} bytes per page a bitmap
 * touches. A page bitset per bitmap makes the charge exactly the stripes allocated, in whatever order the records arrive. Once a step is refused the set is
 * {@link #isOverBudget() over budget}: nothing more is recorded and the facets of the query are unavailable (the rows still flow).
 * <p>
 * Not thread safe. {@link #close()} releases the reservation and the projection snapshots; it is idempotent.
 */
public final class MatchSet implements AutoCloseable {
  private static final long STEP_BYTES        = 1L << 20;
  /** The array header of one stripe. */
  private static final long STRIPE_OVERHEAD   = 16;
  /** One page of a bitmap's index: its page number, its stripe reference and its stripe generation. */
  private static final long INDEX_ENTRY_BYTES = 4 + 8 + 8;

  private final BucketMatches[]                       byBucket;
  private final long                                  size;
  private final boolean                               overBudget;
  private final int                                   waits;
  private final List<FacetMemoryBudget.Reservation> reservations;
  private final ColumnarSnapshot[]                    snapshots;
  private       int[]                                 filterIndexByFacet = new int[0];
  private       boolean                               closed;

  /** The pages a bitmap already holds a stripe for, one bit per page. */
  private static final class PageSet {
    private long[] words = new long[4];

    boolean contains(final int page) {
      final int word = page >>> 6;
      return word < words.length && (words[word] & (1L << page)) != 0;
    }

    void add(final int page) {
      final int word = page >>> 6;
      if (word >= words.length)
        words = Arrays.copyOf(words, Math.max(word + 1, words.length * 2));
      words[word] |= 1L << page;
    }

    /** Adds the page and tells whether it was new. */
    boolean addIfAbsent(final int page) {
      if (contains(page))
        return false;
      add(page);
      return true;
    }
  }

  /** The bitmaps of one bucket. */
  private static final class BucketMatches {
    final int            maxRecordsInPage;
    final PositionBitmap base;
    final PositionBitmap passAll;
    PositionBitmap[]     failedOnly;
    final PageSet        basePages = new PageSet();
    PageSet              passPages;
    PageSet[]            failedPages;

    BucketMatches(final int maxRecordsInPage, final int filterCount) {
      this.maxRecordsInPage = maxRecordsInPage;
      this.base = new PositionBitmap(maxRecordsInPage);
      // without filters every match passes all of them: the pass-all set is the base set, held and charged once
      this.passAll = filterCount == 0 ? base : new PositionBitmap(maxRecordsInPage);
      if (filterCount > 0) {
        failedOnly = new PositionBitmap[filterCount];
        failedPages = new PageSet[filterCount];
        passPages = new PageSet();
      }
    }

    /** A bucket whose pass-all set is its base set (a match set with no WHERE and no FILTER). */
    BucketMatches(final PositionBitmap base) {
      this.maxRecordsInPage = base.maxRecordsInPage();
      this.base = base;
      this.passAll = base;
    }
  }

  private MatchSet(final BucketMatches[] byBucket, final long size, final boolean overBudget, final int waits,
      final List<FacetMemoryBudget.Reservation> reservations, final ColumnarSnapshot[] snapshots) {
    this.byBucket = byBucket;
    this.size = size;
    this.overBudget = overBudget;
    this.waits = waits;
    this.reservations = reservations;
    this.snapshots = snapshots;
  }

  /** Records the matches of a query, one {@link Builder#add} per matched record. */
  public static Builder builder(final DatabaseInternal db, final int filterCount, final long budgetWaitMs) {
    return new Builder(db, filterCount, budgetWaitMs);
  }

  public static final class Builder implements AutoCloseable {
    private final DatabaseInternal                    db;
    private final int                                 filterCount;
    private final long                                budgetWaitMs;
    private final FacetMemoryBudget                   budget = FacetMemoryBudget.instance();
    private final List<FacetMemoryBudget.Reservation> reservations = new ArrayList<>();
    private       BucketMatches[]                     byBucket = new BucketMatches[8];
    private       long                                charged;
    private       long                                reserved;
    private       long                                size;
    private       int                                 waits;
    private final Runnable                            countWait = () -> waits++;
    private       boolean                             overBudget;
    private       boolean                             handedOver;

    private Builder(final DatabaseInternal db, final int filterCount, final long budgetWaitMs) {
      if (filterCount < 0 || filterCount > 31)
        throw new IllegalArgumentException("filterCount must be between 0 and 31, found " + filterCount);
      this.db = db;
      this.filterCount = filterCount;
      this.budgetWaitMs = budgetWaitMs;
    }

    /**
     * Records a matched record.
     *
     * @param failMask bit {@code i} set when the record fails facet filter {@code i}; 0 when it passes all of them
     *
     * @return false, recording nothing, once the memory budget refused a step
     */
    public boolean add(final RID rid, final int failMask) {
      if (overBudget)
        return false;
      final int bucketId = rid.getBucketId();
      final long position = rid.getPosition();
      BucketMatches matches = bucketId < byBucket.length ? byBucket[bucketId] : null;
      if (matches == null)
        matches = open(bucketId);
      else if (matches.base.get(position))
        // a record the rows yield twice (a RID list with repeats, a subquery) is one match, counted once
        return true;

      final int page = (int) (position / matches.maxRecordsInPage);
      final long stripeBytes = matches.maxRecordsInPage / 8;
      final int failedIndex;
      if (failMask == 0)
        failedIndex = -1;
      else if ((failMask & (failMask - 1)) == 0) {
        failedIndex = Integer.numberOfTrailingZeros(failMask);
        if (failedIndex >= filterCount)
          throw new IllegalArgumentException("failMask " + failMask + " names a filter beyond the " + filterCount + " declared");
      } else
        failedIndex = -2;

      long needed = 0;
      if (!matches.basePages.contains(page))
        needed += stripeBytes;
      if (failedIndex == -1 && matches.passPages != null && !matches.passPages.contains(page))
        needed += stripeBytes;
      else if (failedIndex >= 0) {
        final PageSet failedPages = matches.failedPages[failedIndex];
        if (failedPages == null || !failedPages.contains(page))
          needed += stripeBytes;
      }
      if (needed > 0 && !charge(needed))
        return false;

      matches.base.set(position);
      matches.basePages.add(page);
      if (failedIndex == -1) {
        // without filters passAll is the base set, already written
        if (matches.passPages != null) {
          matches.passAll.set(position);
          matches.passPages.add(page);
        }
      } else if (failedIndex >= 0) {
        PositionBitmap failed = matches.failedOnly[failedIndex];
        if (failed == null) {
          failed = matches.failedOnly[failedIndex] = new PositionBitmap(matches.maxRecordsInPage);
          matches.failedPages[failedIndex] = new PageSet();
        }
        failed.set(position);
        matches.failedPages[failedIndex].add(page);
      }
      size++;
      return true;
    }

    /** The match set, which now owns the reservations: closing it (not this builder) releases them. */
    public MatchSet build() {
      final MatchSet matchSet = new MatchSet(byBucket, size, overBudget, waits, reservations, null);
      // only once the match set exists does it own the reservations; a failure above leaves them to close()
      handedOver = true;
      return matchSet;
    }

    /** Releases the reservations unless {@link #build()} handed them to the match set; idempotent. Call it in a finally. */
    @Override
    public void close() {
      if (handedOver)
        return;
      for (final FacetMemoryBudget.Reservation reservation : reservations)
        reservation.close();
      reservations.clear();
    }

    private BucketMatches open(final int bucketId) {
      final int maxRecordsInPage = ((LocalBucket) db.getSchema().getBucketById(bucketId)).getMaxRecordsInPage();
      if (bucketId >= byBucket.length)
        byBucket = Arrays.copyOf(byBucket, Math.max(bucketId + 1, byBucket.length * 2));
      return byBucket[bucketId] = new BucketMatches(maxRecordsInPage, filterCount);
    }

    private boolean charge(final long bytes) {
      charged += bytes;
      while (charged > reserved) {
        final FacetMemoryBudget.Reservation reservation = budget.reserve(STEP_BYTES, budgetWaitMs, countWait);
        if (reservation == null) {
          charged -= bytes;
          overBudget = true;
          return false;
        }
        reservations.add(reservation);
        reserved += STEP_BYTES;
      }
      return true;
    }
  }

  /**
   * The no-WHERE match set: per bucket the OR of every segment's {@code live()} and the snapshot's pending positions that hold a
   * committed record (one slot-table read per pending page, never a record read; a pending position is confirmed or dropped by the
   * patcher). It is not built for buckets the caller's open transaction changed (their records are not what the projection or the
   * committed pages hold). A bucket with no projection, a throwing {@code acquire()} or an
   * unusable snapshot has no entry: its matches are collected from rows instead. The snapshots stay open, reachable through
   * {@link #snapshot(int)}, until {@link #close()}; they are also closed on every failure path here. When the memory budget refuses
   * a step the result is {@link #isOverBudget() over budget} and holds nothing.
   */
  public static MatchSet fromProjection(final DatabaseInternal db, final int[] bucketIds, final long budgetWaitMs) {
    final ColumnarIndexManager manager = ((LocalDatabase) db.getEmbedded()).getColumnarIndexManager();
    final FacetMemoryBudget budget = FacetMemoryBudget.instance();
    final List<FacetMemoryBudget.Reservation> reservations = new ArrayList<>();
    final List<ColumnarSnapshot> acquired = new ArrayList<>();
    BucketMatches[] byBucket = new BucketMatches[8];
    long size = 0;
    long charged = 0;
    long reserved = 0;
    final int[] waits = new int[1];
    final Runnable countWait = () -> waits[0]++;
    boolean overBudget = false;
    try {
      for (final int bucketId : bucketIds) {
        final ColumnarProjection projection = manager.projection(bucketId);
        if (projection == null)
          continue;
        final ColumnarSnapshot snapshot;
        try {
          snapshot = projection.acquire();
        } catch (final RuntimeException e) {
          continue;
        }
        acquired.add(snapshot);
        if (!snapshot.isUsable()) {
          acquired.remove(acquired.size() - 1);
          snapshot.close();
          continue;
        }

        final PositionBitmap pending = snapshot.pendingPositions();
        // charge the union's stripes before allocating it: every pending page counts, whether or not a committed record is on it
        final PageSet pages = new PageSet();
        long stripes = 0;
        for (final ColumnarSegmentView view : snapshot.segments())
          for (final int page : view.live().pages())
            if (pages.addIfAbsent(page))
              stripes++;
        for (final int page : pending.pages())
          if (pages.addIfAbsent(page))
            stripes++;
        charged += stripes * (pending.maxRecordsInPage() / 8);
        while (charged > reserved) {
          final FacetMemoryBudget.Reservation reservation = budget.reserve(STEP_BYTES, budgetWaitMs, countWait);
          if (reservation == null) {
            overBudget = true;
            break;
          }
          reservations.add(reservation);
          reserved += STEP_BYTES;
        }
        if (overBudget)
          break;

        final PositionBitmap bitmap = new PositionBitmap(pending.maxRecordsInPage());
        for (final ColumnarSegmentView view : snapshot.segments())
          bitmap.or(view.live());
        addOccupied((LocalBucket) db.getSchema().getBucketById(bucketId), pending, bitmap);

        if (bucketId >= byBucket.length)
          byBucket = Arrays.copyOf(byBucket, Math.max(bucketId + 1, byBucket.length * 2));
        byBucket[bucketId] = new BucketMatches(bitmap);
        size += bitmap.cardinality();
      }
    } catch (final RuntimeException | Error e) {
      closeAll(acquired, reservations);
      throw e;
    }

    if (overBudget) {
      closeAll(acquired, reservations);
      return new MatchSet(new BucketMatches[0], 0, true, waits[0], new ArrayList<>(), null);
    }
    final ColumnarSnapshot[] snapshots = new ColumnarSnapshot[byBucket.length];
    for (final ColumnarSnapshot snapshot : acquired)
      snapshots[snapshot.bucketId()] = snapshot;
    return new MatchSet(byBucket, size, false, waits[0], reservations, snapshots);
  }

  /**
   * One set holding the buckets of both, which must be disjoint: a {@link #fromProjection} set and the matches recorded from the rows
   * of the buckets it has no entry for. The result owns everything both held (closing it releases their reservations and snapshots);
   * the two sets are left closed and must not be used. Over budget when either is.
   */
  public static MatchSet union(final MatchSet first, final MatchSet second) {
    final BucketMatches[] byBucket = Arrays.copyOf(first.byBucket, Math.max(first.byBucket.length, second.byBucket.length));
    for (int i = 0; i < second.byBucket.length; i++)
      if (second.byBucket[i] != null) {
        if (byBucket[i] != null)
          throw new IllegalArgumentException("Bucket " + i + " is in both match sets");
        byBucket[i] = second.byBucket[i];
      }
    ColumnarSnapshot[] snapshots = first.snapshots;
    if (second.snapshots != null) {
      snapshots = snapshots == null ? second.snapshots.clone() : Arrays.copyOf(snapshots, Math.max(snapshots.length, second.snapshots.length));
      for (int i = 0; i < second.snapshots.length; i++)
        if (second.snapshots[i] != null)
          snapshots[i] = second.snapshots[i];
    }
    final List<FacetMemoryBudget.Reservation> reservations = new ArrayList<>(first.reservations);
    reservations.addAll(second.reservations);
    final MatchSet union = new MatchSet(byBucket, first.size + second.size, first.overBudget || second.overBudget,
        first.waits + second.waits, reservations, snapshots);
    // handed over: closing the parts now would release what the union holds
    first.closed = true;
    second.closed = true;
    return union;
  }

  /**
   * Adds to {@code bitmap} the pending positions that hold a committed record. Pending is page-granular (every slot of a dirty page, plus
   * the owners of the content on it): reading each pending page's slot table once ({@link LocalBucket#forEachCommittedRecordSlot}, no
   * record decoded) drops its empty and content slots, so the size of the match set, the counters sized from it and the patch budget
   * count records that exist rather than slots. Written straight into the charged union: no bitmap of its own.
   */
  private static void addOccupied(final LocalBucket bucket, final PositionBitmap pending, final PositionBitmap bitmap) {
    for (final int page : pending.pages())
      bucket.forEachCommittedRecordSlot(page, position -> {
        if (pending.get(position))
          bitmap.set(position);
      });
  }

  /**
   * Closes every snapshot (nulls skipped) and releases every reservation, each in its own guard: the budget is JVM wide, so a
   * snapshot that fails to close must not keep the permits. The first failure is rethrown after everything is released.
   */
  private static void closeAll(final Iterable<ColumnarSnapshot> snapshots, final List<FacetMemoryBudget.Reservation> reservations) {
    RuntimeException failure = null;
    for (final ColumnarSnapshot snapshot : snapshots) {
      if (snapshot == null)
        continue;
      try {
        snapshot.close();
      } catch (final RuntimeException e) {
        if (failure == null)
          failure = e;
        else
          failure.addSuppressed(e);
      }
    }
    for (final FacetMemoryBudget.Reservation reservation : reservations)
      reservation.close();
    if (failure != null)
      throw failure;
  }

  /**
   * Declares, per facet, the index of the filter it carries (-1 for none). Must be called before {@link #forFacet}.
   */
  public void setFacetFilterIndex(final int[] filterIndexByFacet) {
    this.filterIndexByFacet = filterIndexByFacet.clone();
  }

  /** Every WHERE match of the bucket; owned by this set, read only. Null when nothing was recorded for the bucket. */
  public PositionBitmap forBucket(final int bucketId) {
    final BucketMatches matches = matches(bucketId);
    return matches == null ? null : matches.base;
  }

  /**
   * The records the facet counts: those that pass every filter, plus, for a facet that carries filter {@code i}, those that fail only
   * filter {@code i}. A facet with no filter gets the pass-all set itself (owned by this set, read only); one with a filter gets a
   * new bitmap owned by the caller, which shares the pass-all stripes copy-on-write: it allocates its page index and one stripe per
   * page of the failed-only set (see {@link #derivedBytes}), and a write to it clones the stripes it touches. Null when nothing was
   * recorded for the bucket.
   */
  public PositionBitmap forFacet(final int facetIndex, final int bucketId) {
    final BucketMatches matches = matches(bucketId);
    if (matches == null)
      return null;
    final int filterIndex = filterIndexByFacet[facetIndex];
    if (filterIndex < 0)
      return matches.passAll;
    final PositionBitmap result = matches.passAll.snapshot();
    final PositionBitmap failed = failedOnly(matches, filterIndex);
    if (failed != null)
      result.or(failed);
    return result;
  }

  /**
   * The records the facet counts in the bucket ({@link #forFacet}) as a read-only view over this set's own bitmaps: nothing is copied.
   * Null when nothing was recorded for the bucket.
   */
  public FacetMatches facetMatches(final int facetIndex, final int bucketId) {
    final BucketMatches matches = matches(bucketId);
    if (matches == null)
      return null;
    final int filterIndex = filterIndexByFacet[facetIndex];
    return new FacetMatches(matches.passAll, filterIndex < 0 ? null : failedOnly(matches, filterIndex));
  }

  /**
   * An upper bound of the bytes a bitmap derived from the facet's matches allocates: {@link #forFacet} for a facet with a filter, a
   * {@link PositionBitmap#snapshot} of the pass-all set for one without, then, with a non-empty {@code patch}, {@code andNot(patch)}
   * (which clones the stripes of the patched pages). Zero for a facet without filter and without patch: the pass-all set itself serves.
   */
  public long derivedBytes(final int facetIndex, final int bucketId, final PositionBitmap patch) {
    final BucketMatches matches = matches(bucketId);
    if (matches == null)
      return 0;
    final int filterIndex = filterIndexByFacet[facetIndex];
    final PositionBitmap failed = filterIndex < 0 ? null : failedOnly(matches, filterIndex);
    final boolean patched = patch != null && !patch.isEmpty();
    if (filterIndex < 0 && !patched)
      return 0;
    final long stripeBytes = STRIPE_OVERHEAD + matches.maxRecordsInPage / 8;
    final long failedPages = failed == null ? 0 : failed.pageCount();
    final long patchPages = patched ? patch.pageCount() : 0;
    // the page index (page number, stripe reference, stripe generation) of the pass-all pages and of those the failed-only set adds,
    // then one stripe cloned per failed-only page and per patched page
    return INDEX_ENTRY_BYTES * (matches.passAll.pageCount() + failedPages) + stripeBytes * (failedPages + patchPages);
  }

  /** The index, among the facets that carry one, of the facet's FILTER; -1 for a facet without. */
  public int filterIndexOf(final int facetIndex) {
    return filterIndexByFacet[facetIndex];
  }

  private static PositionBitmap failedOnly(final BucketMatches matches, final int filterIndex) {
    return matches.failedOnly == null ? null : matches.failedOnly[filterIndex];
  }

  /**
   * The records one facet counts in one bucket, read through the match set's own bitmaps: the pass-all set and, for a facet with a
   * filter, the records that fail only that filter. The two are disjoint, so the cardinalities add. Valid while the match set is open.
   */
  public static final class FacetMatches {
    private final PositionBitmap passAll;
    private final PositionBitmap failed;

    private FacetMatches(final PositionBitmap passAll, final PositionBitmap failed) {
      this.passAll = passAll;
      this.failed = failed;
    }

    public boolean get(final long position) {
      return passAll.get(position) || failed != null && failed.get(position);
    }

    public long cardinality() {
      return passAll.cardinality() + (failed == null ? 0 : failed.cardinality());
    }

    public long andCardinality(final PositionBitmap other) {
      return passAll.andCardinality(other) + (failed == null ? 0 : failed.andCardinality(other));
    }
  }

  /** The buckets that have matches, ascending. */
  public int[] bucketIds() {
    int count = 0;
    for (final BucketMatches matches : byBucket)
      if (matches != null)
        count++;
    final int[] ids = new int[count];
    int n = 0;
    for (int i = 0; i < byBucket.length; i++)
      if (byBucket[i] != null)
        ids[n++] = i;
    return ids;
  }

  /** Number of matched records recorded. */
  public long size() {
    return size;
  }

  public boolean isOverBudget() {
    return overBudget;
  }

  /** Number of budget steps that had to wait, granted or finally refused (the caller adds them to {@code query.budget.waits}). */
  public int budgetWaits() {
    return waits;
  }

  /** The snapshot {@link #fromProjection} acquired for the bucket, owned by this set; null for any other bucket or builder-made set. */
  public ColumnarSnapshot snapshot(final int bucketId) {
    return snapshots != null && bucketId >= 0 && bucketId < snapshots.length ? snapshots[bucketId] : null;
  }

  private BucketMatches matches(final int bucketId) {
    return bucketId >= 0 && bucketId < byBucket.length ? byBucket[bucketId] : null;
  }

  @Override
  public void close() {
    if (closed)
      return;
    closed = true;
    closeAll(snapshots == null ? List.of() : Arrays.asList(snapshots), reservations);
  }
}
