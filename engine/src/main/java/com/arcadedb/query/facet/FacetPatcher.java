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
import com.arcadedb.index.columnar.ColumnarIndexManager;
import com.arcadedb.index.columnar.ColumnarSegmentView;
import com.arcadedb.index.columnar.ColumnarSnapshot;
import com.arcadedb.index.columnar.ColumnarValueExtractor;
import com.arcadedb.index.columnar.GlobalDictionary;
import com.arcadedb.index.columnar.PositionBitmap;
import com.arcadedb.utility.IntHashSet;

import java.util.List;

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
   * OR live of any segment))}, that is the matches minus those a segment view holds live on a page that is not pending. A match no view
   * covers and no dirty page explains is read from its record, so a projection bug costs time, never a wrong count.
   *
   * @return a new bitmap owned by the caller, empty when the bucket has no matches
   */
  public static PositionBitmap patchSet(final MatchSet matches, final ColumnarSnapshot snapshot, final int bucketId) {
    final PositionBitmap base = matches.forBucket(bucketId);
    final PositionBitmap pending = snapshot.pendingPositions();
    if (base == null)
      return new PositionBitmap(pending.maxRecordsInPage());
    final PositionBitmap uncovered = base.copy();
    for (final ColumnarSegmentView view : snapshot.segments())
      uncovered.andNot(view.live());
    final PositionBitmap patch = pending.copy();
    patch.and(base);
    patch.or(uncovered);
    return patch;
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
   * ({@link MatchSet#forFacet}) hold the position, however the match set was made. A position with no record (deleted) is skipped.
   * The collectors were begun on this bucket.
   *
   * @param collectors one per facet, in the order of {@code facets}; null for a facet that does not count this bucket
   *
   * @return the records read
   */
  public static long countPatched(final DatabaseInternal db, final MatchSet matches, final ColumnarSnapshot snapshot,
      final PositionBitmap patch, final List<FacetSpec> facets, final List<FacetCollector> collectors) {
    final int bucketId = snapshot.bucketId();
    final int facetCount = facets.size();
    final PositionBitmap[] facetMatches = new PositionBitmap[facetCount];
    final GlobalDictionary[] dictionaries = new GlobalDictionary[facetCount];
    GlobalDictionary empty = null;
    for (int f = 0; f < facetCount; f++) {
      if (collectors.get(f) == null)
        continue;
      // whatever made the match set: a facet counts only its own matches (a FILTER of another facet may exclude a record)
      facetMatches[f] = matches.forFacet(f, bucketId);
      dictionaries[f] = snapshot.dictionary(facets.get(f).property());
      if (dictionaries[f] == null) {
        // a LONG or DOUBLE column: no ordinal is ever read; any other column then reports every value by value
        if (empty == null)
          empty = GlobalDictionary.createInMemory();
        dictionaries[f] = empty;
      }
    }

    final ColumnarIndexManager manager = ((LocalDatabase) db.getEmbedded()).getColumnarIndexManager();
    final PatchedValue value = new PatchedValue();
    final long[] read = new long[1];
    patch.forEach(position -> {
      final Document record = recordAt(db, new RID(bucketId, position));
      if (record == null)
        return;
      final ColumnarValueExtractor extractor = manager.extractor(record.getTypeName());
      if (extractor == null)
        return;
      read[0]++;
      for (int f = 0; f < facetCount; f++) {
        final FacetCollector collector = collectors.get(f);
        if (collector == null || facetMatches[f] == null || !facetMatches[f].get(position))
          continue;
        value.reset();
        extractor.extract(record, facets.get(f).property(), dictionaries[f], value);
        collector.collectPatched(position, value);
      }
    });
    return read[0];
  }

  /** The record as the caller's transaction reads it, null when there is none at the position. */
  private static Document recordAt(final DatabaseInternal db, final RID rid) {
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
