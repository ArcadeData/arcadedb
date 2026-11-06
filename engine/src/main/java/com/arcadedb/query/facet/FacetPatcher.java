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
import com.arcadedb.database.Document;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.RID;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.index.columnar.ColumnDefinition;
import com.arcadedb.index.columnar.ColumnarIndexManager;
import com.arcadedb.index.columnar.ColumnarSegmentView;
import com.arcadedb.index.columnar.ColumnarSnapshot;
import com.arcadedb.index.columnar.ColumnarValueExtractor;
import com.arcadedb.index.columnar.GlobalDictionary;
import com.arcadedb.index.columnar.PositionBitmap;
import com.arcadedb.query.sql.executor.WorkGuard;
import com.arcadedb.utility.IntHashSet;

import java.util.List;
import java.util.function.LongConsumer;
import java.util.function.LongPredicate;

/**
 * Corrects the projection with the records it cannot answer for (spec 7.3, D11): the matches on pages the apply thread has not applied
 * yet, the matches no segment view holds live, and the records the caller's own open transaction wrote. Each such record is read once
 * and its value handed to every facet that counts it.
 * <p>
 * A position whose record is gone (deleted since the projection or the match set saw it) is not a match and reaches no collector.
 */
public final class FacetPatcher {
  private FacetPatcher() {
  }

  /**
   * The positions of the bucket's matches the projection cannot answer for: {@code (matches AND pending) OR (matches AND NOT (pending
   * OR live of any segment))}, that is the matches minus those a segment view holds live on a page that is not pending, plus the
   * caller's own writes. A match no view covers and no dirty page explains is read from its record, so a projection bug costs time,
   * never a wrong count. A match no facet counts from the projection ({@code counted} refuses it: it fails the FILTERs of two facets,
   * final review A-M1) is left out: reading it would reach no collector.
   *
   * @param counted the matches a facet counts in the bucket from the projection, null for every match
   * @param own     the matches the caller's open transaction wrote in the bucket ({@link #ownWrites}), null for none
   *
   * @return a new bitmap owned by the caller, empty when the bucket has no matches; it allocates at most
   *     {@link #patchSetBytes} of {@link #patchSetSize}
   */
  public static PositionBitmap patchSet(final MatchSet matches, final ColumnarSnapshot snapshot, final int bucketId,
      final LongPredicate counted, final PositionBitmap own) {
    final PositionBitmap base = matches.forBucket(bucketId);
    final PositionBitmap pending = snapshot.pendingPositions();
    final PositionBitmap patch = new PositionBitmap(pending.maxRecordsInPage());
    if (base == null)
      return patch;
    final LongConsumer add = counted == null ? patch::set : position -> {
      if (counted.test(position))
        patch.set(position);
    };
    // the matches no view holds live, then those on pending pages: only the stripes of the patch set are allocated
    base.forEachAndNotAny(lives(snapshot), add);
    base.forEachAnd(pending, add);
    if (own != null)
      own.forEach(add);
    return patch;
  }

  /**
   * The number of positions of {@link #patchSet}, counted without allocating anything: what the patch budget is tested against before
   * any patch set is made.
   */
  public static long patchSetSize(final MatchSet matches, final ColumnarSnapshot snapshot, final int bucketId,
      final LongPredicate counted, final PositionBitmap own) {
    final PositionBitmap base = matches.forBucket(bucketId);
    if (base == null)
      return 0;
    final PositionBitmap pending = snapshot.pendingPositions();
    final PositionBitmap[] lives = lives(snapshot);
    final long[] size = new long[1];
    // uncovered and not pending, then pending: each position once
    base.forEachAndNotAny(lives, position -> {
      if (!pending.get(position) && (counted == null || counted.test(position)))
        size[0]++;
    });
    if (counted == null)
      size[0] += base.andCardinality(pending);
    else
      base.forEachAnd(pending, position -> {
        if (counted.test(position))
          size[0]++;
      });
    if (own != null)
      own.forEach(position -> {
        if (!pending.get(position) && isLive(lives, position) && (counted == null || counted.test(position)))
          size[0]++;
      });
    return size[0];
  }

  private static boolean isLive(final PositionBitmap[] lives, final long position) {
    for (final PositionBitmap live : lives)
      if (live.get(position))
        return true;
    return false;
  }

  /** An upper bound of the bytes a patch set of at most {@code size} positions in the bucket allocates: a page index and stripes. */
  public static long patchSetBytes(final MatchSet matches, final int bucketId, final long size) {
    final PositionBitmap base = matches.forBucket(bucketId);
    if (base == null || size == 0)
      return 0;
    // per page: the stripe and its array header, the index entry (page, stripe reference, generation) twice for the index's growth
    return Math.min(size, base.pageCount()) * (2 * (4 + 8 + 8) + 16 + base.maxRecordsInPage() / 8);
  }

  private static PositionBitmap[] lives(final ColumnarSnapshot snapshot) {
    final List<ColumnarSegmentView> views = snapshot.segments();
    final PositionBitmap[] lives = new PositionBitmap[views.size()];
    for (int i = 0; i < lives.length; i++)
      lives[i] = views.get(i).live();
    return lives;
  }

  /**
   * The positions, per bucket, of the matches the caller's open transaction created, updated or deleted, which the projection never
   * holds (it applies committed pages only). The match set must be made from rows for those buckets: its WHERE ran in the transaction,
   * so a written record matches exactly when the transaction reads it as matching.
   *
   * @return one bitmap per entry of {@code bucketIds}, null entries for buckets the transaction did not write; null when it wrote none
   *
   * @throws IllegalStateException when the transaction wrote a bucket whose matches were made from the projection, which holds neither
   *                               the transaction's new records nor its view of the others
   */
  public static PositionBitmap[] ownWrites(final DatabaseInternal db, final MatchSet matches, final int[] bucketIds) {
    if (bucketIds.length == 0 || !db.isTransactionActive())
      return null;
    final IntHashSet buckets = new IntHashSet(bucketIds.length);
    for (final int bucketId : bucketIds)
      buckets.add(bucketId);
    final TransactionContext transaction = db.getTransaction();
    // the match set of the projection is taken once the rows are done, from the buckets the transaction did not change then: a write
    // to one of them can only come from inside the facet computation, which writes nothing

    if (!transaction.hasChangesInFiles(buckets))
      return null;
    final List<RID> written = transaction.getWrittenRecordIdsIn(buckets);
    if (written.isEmpty())
      return null;

    final PositionBitmap[] own = new PositionBitmap[bucketIds.length];
    boolean any = false;
    for (final RID rid : written) {
      final int index = indexOf(bucketIds, rid.getBucketId());
      final PositionBitmap base = matches.forBucket(rid.getBucketId());
      if (index < 0 || base == null || rid.getPosition() < 0)
        continue;
      if (matches.snapshot(rid.getBucketId()) != null)
        throw new IllegalStateException("Bucket " + rid.getBucketId() + " was written by the open transaction: its facet matches must be "
            + "collected from the rows, not from the columnar projection");
      if (!base.get(rid.getPosition()))
        // not a match of the WHERE, as the transaction itself reads it
        continue;
      if (own[index] == null)
        own[index] = new PositionBitmap(base.maxRecordsInPage());
      own[index].set(rid.getPosition());
      any = true;
    }
    return any ? own : null;
  }

  /**
   * Reads each record of {@code patch} once and hands its value to the collector of every facet whose own matches
   * ({@link MatchSet#facetMatches}) hold the position, however the match set was made. A position with no record (deleted) is skipped.
   * The collectors were begun on this bucket.
   *
   * @param collectors one per facet, in the order of {@code facets}; null for a facet that does not count this bucket
   * @param columns    one per facet: the column its collector was planned with (see {@link Patching})
   * @param guard      the query's deadline and termination, checked every few records
   *
   * @return the records read
   */
  public static long countPatched(final DatabaseInternal db, final MatchSet matches, final ColumnarSnapshot snapshot,
      final PositionBitmap patch, final List<FacetSpec> facets, final List<FacetCollector> collectors, final ColumnDefinition[] columns,
      final WorkGuard guard) {
    final Patching patching = new Patching(db, matches, snapshot, facets, collectors, columns);
    final int bucketId = snapshot.bucketId();
    final int[] ticks = new int[1];
    patch.forEach(position -> {
      guard.checkPeriodically(++ticks[0]);
      final Document record = recordAt(db, new RID(bucketId, position));
      if (record != null)
        patching.accept(record, position);
    });
    return patching.read();
  }

  /**
   * The patching of one bucket, record by record: {@link #countPatched} drives it over the patch set, and the fallback scan drives it
   * over the records it reads anyway when the bucket is also scanned for other facets, so such a record is read once.
   * <p>
   * A record is read with the column its facet's collector was planned with, the definition the segments it counts were written with,
   * not with the one its type's extractor holds now: a column redefined while the query counts (DATE to DATETIME, say) would otherwise
   * hand the collector values in another encoding (final fix batch 2, r1 M1).
   */
  static final class Patching {
    private final List<FacetCollector> collectors;
    private final ColumnDefinition[]   columns;
    private final MatchSet.FacetMatches[] facetMatches;
    private final GlobalDictionary[]   dictionaries;
    private final ColumnarIndexManager manager;
    private final PatchedValue         value = new PatchedValue();
    private       long                 read;

    /**
     * @param collectors one per facet, in the order of {@code facets}; null for a facet that does not count this bucket
     * @param columns    one per facet: the column its collector was planned with; null for a facet that does not count this bucket
     */
    Patching(final DatabaseInternal db, final MatchSet matches, final ColumnarSnapshot snapshot, final List<FacetSpec> facets,
        final List<FacetCollector> collectors, final ColumnDefinition[] columns) {
      this.collectors = collectors;
      this.columns = columns;
      final int bucketId = snapshot.bucketId();
      final int facetCount = facets.size();
      facetMatches = new MatchSet.FacetMatches[facetCount];
      dictionaries = new GlobalDictionary[facetCount];
      GlobalDictionary empty = null;
      for (int f = 0; f < facetCount; f++) {
        if (collectors.get(f) == null)
          continue;
        // whatever made the match set: a facet counts only its own matches (a FILTER of another facet may exclude a record)
        facetMatches[f] = matches.facetMatches(f, bucketId);
        dictionaries[f] = snapshot.dictionary(facets.get(f).property());
        if (dictionaries[f] == null) {
          // a LONG or DOUBLE column: no ordinal is ever read; any other column then reports every value by value
          if (empty == null)
            empty = GlobalDictionary.createInMemory();
          dictionaries[f] = empty;
        }
      }
      manager = ((LocalDatabase) db.getEmbedded()).getColumnarIndexManager();
    }

    /**
     * Hands the patched record's value to the collector of every facet whose matches hold the position. A record whose type has no
     * extractor any more (every COLUMNAR index of it dropped while the query counts) is still a match: it is counted as missing, never
     * dropped.
     */
    void accept(final Document record, final long position) {
      final ColumnarValueExtractor extractor = manager.extractor(record.getTypeName());
      read++;
      for (int f = 0; f < facetMatches.length; f++) {
        final FacetCollector collector = collectors.get(f);
        if (collector == null || facetMatches[f] == null || !facetMatches[f].get(position))
          continue;
        value.reset();
        if (extractor == null || columns[f] == null)
          value.missing();
        else
          extractor.extract(record, columns[f], dictionaries[f], value);
        collector.collectPatched(position, value);
      }
    }

    /** The patched records read. */
    long read() {
      return read;
    }
  }

  /** The record as the caller's transaction reads it, null when there is none at the position. */
  public static Document recordAt(final DatabaseInternal db, final RID rid) {
    // the empty slots of pending pages are not in the patch set (MatchSet keeps the occupied ones), so an absent record is a delete
    try {
      return db.lookupByRID(rid, true).asDocument();
    } catch (final RecordNotFoundException e) {
      return null;
    }
  }

  private static int indexOf(final int[] bucketIds, final int bucketId) {
    for (int i = 0; i < bucketIds.length; i++)
      if (bucketIds[i] == bucketId)
        return i;
    return -1;
  }
}
