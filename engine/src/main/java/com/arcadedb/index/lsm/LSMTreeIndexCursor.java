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
package com.arcadedb.index.lsm;

import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.database.TransactionIndexContext;
import com.arcadedb.engine.BasePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.IndexCursorEntry;
import com.arcadedb.index.TempIndexCursor;
import com.arcadedb.serializer.BinaryComparator;
import com.arcadedb.serializer.BinarySerializer;

import java.io.IOException;
import java.util.*;

/**
 * Merging cursor over the mutable pages and the compacted series of one LSM-Tree index.
 * <p>
 * Deleted entries are not physically removed by the index, so the merge has to step over them: a key-wide tombstone
 * {@code (-1,-1)}, a per-RID tombstone {@code (-(bucketId+2), position)}, or a whole key group whose RIDs were all
 * shadowed. That skip work is unbounded - compaction never purges tombstones - and it is what makes the cursor's
 * {@link Iterator} contract non-trivial.
 * <p>
 * <b>#5635 - the contract.</b> {@link #hasNext()} PREFETCHES: it runs the merge until it holds a surviving RID, so it
 * answers on what can actually be emitted rather than on how many underlying cursors are still live. {@link #next()} is
 * then a pure drain that never returns null and throws {@link NoSuchElementException} once exhausted, like every other
 * {@link IndexCursor} implementation. Before this, {@code hasNext()} was optimistic and a scan ending on a run of
 * tombstoned keys handed the caller a null element - {@code IndexCursor extends Iterator<Identifiable>}, so
 * {@code for (final Identifiable r : cursor)} yielded it. Consumers grew private guards for that null
 * ({@code FullTextQueryExecutor} carried four); those are gone now that the contract holds here.
 * <p>
 * Prefetching also settles what {@link #getRecord()} and {@link #getKeys()} describe: the entry {@code next()} LAST
 * RETURNED. The previous implementation peeked at the not-yet-consumed value at {@code currentValueIndex}, which read as
 * "the current entry" only by accident - a caller reading them right after {@code next()} saw the FOLLOWING row.
 */
public class LSMTreeIndexCursor implements IndexCursor {
  private final LSMTreeIndexMutable                    index;
  private final boolean                                ascendingOrder;
  /** Bounds narrowed to the index's declared key types WITHOUT the byte[]-for-String probe encoding (#5932):
   *  used for every comparison against an already-deserialized key, whether read back from a page
   *  ({@code PageIterator.getKeys()}) or from the transaction-overlay - both hold this same plain form. */
  private final Object[]                               typedFromKeys;
  private final Object[]                               typedToKeys;
  private final boolean                                toKeysInclusive;
  private final LSMTreeIndexUnderlyingAbstractCursor[] pageCursors;
  private final int                                    totalCursors;
  private final byte[]                                 binaryKeyTypes;
  private final Object[][]                             cursorKeys;
  private final int[]                                  lastConsumedPageNumber;
  private final int[]                                  lastConsumedPosition;
  private final BinaryComparator                       comparator;
  private       Object[]                               currentKeys;
  private       RID[]                                  currentValues;
  private       int                                    currentValueIndex = 0;
  private       TempIndexCursor                        txCursor;
  private       Object[]                               txCursorKeys;
  /** Dead (tombstone-resolved) keys skipped by this scan; flushed to the main index stats at scan end. */
  private       long                                   deadEntriesSkipped = 0;
  /** Prefetched entry (#5635): produced by {@link #fetchNext()}, drained by {@link #next()}. Never a tombstone. */
  private       RID                                    nextValue;
  private       Object[]                               nextValueKeys;
  /** The entry {@link #next()} last returned: what {@link #getRecord()} and {@link #getKeys()} describe. */
  private       RID                                    lastReturnedValue;
  private       Object[]                               lastReturnedKeys;
  /** #6944: the 3 containers below are per-key-group scratch space, hoisted here and {@code clear()}ed at the
   *  start of every group instead of being reallocated - the cursor is single-threaded and each group's
   *  contents are fully consumed before the next group starts, so nothing outlives a reuse. */
  private final List<Integer>                          minorKeyIndexes    = new ArrayList<>();
  private final HashMap<RID, Boolean>                   ridState           = new HashMap<>();
  private final HashSet<RID>                            mergedRIDs         = new HashSet<>();

  public LSMTreeIndexCursor(final LSMTreeIndexMutable index, final boolean ascendingOrder) throws IOException {
    this(index, ascendingOrder, null, true, null, true);
  }

  public LSMTreeIndexCursor(final LSMTreeIndexMutable index, final boolean ascendingOrder, final Object[] fromKeys,
      final boolean beginKeysInclusive, final Object[] toKeys, final boolean endKeysInclusive) throws IOException {
    this.index = index;
    this.ascendingOrder = ascendingOrder;
    this.binaryKeyTypes = index.getBinaryKeyTypes();

    index.checkForNulls(fromKeys);
    index.checkForNulls(toKeys);

    // Disk-probe encoding (byte[]-for-String): needed ONLY to seed lookupInPage and the compacted series'
    // equivalent below - both purely local to this constructor - never for a comparison against an
    // already-deserialized key, which is what every typedFromKeys/typedToKeys comparison elsewhere is for.
    final Object[] serializedFromKeys = index.convertKeys(fromKeys, binaryKeyTypes);
    this.typedFromKeys = index.convertKeysToDeclaredTypes(fromKeys, binaryKeyTypes);

    final Object[] normalizedToKeys = toKeys != null && toKeys.length == 0 ? null : toKeys;
    final Object[] serializedToKeys = index.convertKeys(normalizedToKeys, binaryKeyTypes);
    this.typedToKeys = index.convertKeysToDeclaredTypes(normalizedToKeys, binaryKeyTypes);
    this.toKeysInclusive = endKeysInclusive;

    final DatabaseInternal database = index.getDatabase();
    final BinarySerializer serializer = database.getSerializer();
    this.comparator = serializer.getComparator();

    final LSMTreeIndexCompacted compacted = index.getSubIndex();

    final List<LSMTreeIndexUnderlyingCompactedSeriesCursor> compactedSeriesIterators;

    if (compacted != null)
      // INCLUDE COMPACTED
      compactedSeriesIterators = compacted.newIterators(ascendingOrder, serializedFromKeys, serializedToKeys);
    else
      compactedSeriesIterators = Collections.emptyList();

    final int totalPages = index.getTotalPages();

    totalCursors = totalPages + compactedSeriesIterators.size();

    // CREATE AN ARRAY OF CURSOR. SINCE WITH LSM THE LATEST PAGE IS THE MOST UPDATED, IN THE ARRAY ARE SET FIRST THE MUTABLE ONES BECAUSE THEY ARE MORE UPDATED.
    // FROM THE LAST TO THE FIRST. THEN THE COMPACTED, FROM THE LAST TO THE FIRST
    pageCursors = new LSMTreeIndexUnderlyingAbstractCursor[totalCursors];
    cursorKeys = new Object[totalCursors][binaryKeyTypes.length];

    // Infinite-loop detection state (see advanceCursor()): the consumed (page, position) of each
    // underlying cursor must advance strictly monotonically in scan direction. Sentinel values sort
    // before any real position in the respective direction so the first consumption always passes.
    lastConsumedPageNumber = new int[totalCursors];
    lastConsumedPosition = new int[totalCursors];
    Arrays.fill(lastConsumedPageNumber, ascendingOrder ? Integer.MIN_VALUE : Integer.MAX_VALUE);
    Arrays.fill(lastConsumedPosition, ascendingOrder ? Integer.MIN_VALUE : Integer.MAX_VALUE);

    for (int i = 0; i < compactedSeriesIterators.size(); ++i) {
      LSMTreeIndexUnderlyingCompactedSeriesCursor pageCursor = compactedSeriesIterators.get(i);
      if (pageCursor != null) {
        if (pageCursor.hasNext()) {
          pageCursor.next();
          cursorKeys[totalPages + i] = pageCursor.getKeys();
        } else {
          pageCursor.close(); // release the file registration of an empty series cursor
          pageCursor = null;
        }
        pageCursors[totalPages + i] = pageCursor;
      }
    }

    int pageCounter = 0;
    for (int pageId = totalPages - 1; pageId > -1; --pageId) {
      final int cursorIdx = pageCounter;

      if (serializedFromKeys != null) {
        // SEEK FOR THE FROM RANGE
        final BasePage currentPage = database.getTransaction()
            .getPage(new PageId(database, index.getFileId(), pageId), index.getPageSize());
        final Binary currentPageBuffer = new Binary(currentPage.slice());
        final int count = index.getCount(currentPage);

        if (count > 0) {
          final LSMTreeIndexMutable.LookupResult lookupResult = index.lookupInPage(currentPage.getPageId().getPageNumber(), count,
              currentPageBuffer, serializedFromKeys, ascendingOrder ? 2 : 3);

          if (!lookupResult.outside) {
            pageCursors[cursorIdx] = index.newPageIterator(pageId, lookupResult.keyIndex, ascendingOrder);
            cursorKeys[cursorIdx] = pageCursors[cursorIdx].getKeys();

            // #5932: cursorKeys[] comes from a full page-entry deserialization (PageIterator.getKeys()), so it is
            // already in the index's declared Java types (e.g. a plain String) - NOT the byte[]-for-String probe
            // encoding serializedFromKeys carries for lookupInPage's own raw-bytes binary search above. Comparing
            // it against that probe hits BinaryComparator's generic Object.toString() fallback for a byte[]
            // operand, comparing a garbage "[B@hash" string instead of the real bound and misjudging the range.
            if (ascendingOrder) {
              if (LSMTreeIndexMutable.compareKeys(comparator, binaryKeyTypes, cursorKeys[cursorIdx], typedFromKeys) < 0) {
                pageCursors[cursorIdx] = null;
                cursorKeys[cursorIdx] = null;
              }
            } else {
              if (LSMTreeIndexMutable.compareKeys(comparator, binaryKeyTypes, cursorKeys[cursorIdx], typedFromKeys) > 0) {
                pageCursors[cursorIdx] = null;
                cursorKeys[cursorIdx] = null;
              }
            }
          }
        }

      } else {
        if (ascendingOrder) {
          pageCursors[cursorIdx] = index.newPageIterator(pageId, -1, true);
        } else {
          final BasePage currentPage = database.getTransaction()
              .getPage(new PageId(database, index.getFileId(), pageId), index.getPageSize());
          pageCursors[cursorIdx] = index.newPageIterator(pageId, index.getCount(currentPage), false);
        }

        if (pageCursors[cursorIdx].hasNext()) {
          pageCursors[cursorIdx].next();
          cursorKeys[cursorIdx] = pageCursors[cursorIdx].getKeys();
        } else
          pageCursors[cursorIdx] = null;
      }
      ++pageCounter;
    }

    // COLLECT THE DELETED ENTRIES ACROSS BUCKETS. IT IS POSSIBLE THAT THE SAME KEY IS DELETED MORE THAN ONCE,
    // SO WE NEED TO CHECK IT ALSO ACROSS BUCKETS/CURSORS
    final Set<TransactionIndexContext.ComparableKey> removedKeys = new HashSet<>();

    // CHECK THE VALIDITY OF CURSORS
    for (int i = 0; i < pageCursors.length; ++i) {

      LSMTreeIndexUnderlyingAbstractCursor pageCursor = pageCursors[i];
      // Termination: each iteration either breaks, invalidates the cursor, or advances it via
      // advanceCursor(), which throws on the only non-terminating state (a stuck cursor).
      // No iteration budget: a long run of removed/tombstoned keys is legal work of any length.
      while (pageCursor != null) {
        final TransactionIndexContext.ComparableKey keys = new TransactionIndexContext.ComparableKey(pageCursor.getKeys());
        if (removedKeys.contains(keys)) {
          if (pageCursor.hasNext()) {
            advanceCursor(i); // keeps the cursorKeys cache in sync with the advanced cursor
            continue;
          }
          pageCursor.close();
          pageCursors[i] = null;
          break;
        }

        if (typedFromKeys != null && !beginKeysInclusive) {
          if (LSMTreeIndexMutable.compareKeys(comparator, binaryKeyTypes, cursorKeys[i], typedFromKeys) == 0) {
            // SKIP THIS
            if (pageCursor.hasNext()) {
              advanceCursor(i);
              // Re-evaluate the new key from scratch (removedKeys, range and tombstone checks): falling
              // through would run them against the OLD key still bound above.
              continue;
            }
            // INVALID
            pageCursor.close();
            pageCursors[i] = null;
            break;
          }
        }

        if (this.typedToKeys != null) {
          //final Object[] cursorKey = index.convertKeys(index.checkForNulls(pageCursor.getKeys()), keyTypes);
          final int compare = LSMTreeIndexMutable.compareKeys(comparator, binaryKeyTypes, pageCursor.getKeys(), this.typedToKeys);

          if ((ascendingOrder && ((endKeysInclusive && compare <= 0) || (!endKeysInclusive && compare < 0))) || //
              (!ascendingOrder && ((endKeysInclusive && compare >= 0) || (!endKeysInclusive && compare > 0))))
            ;
          else {
            // INVALID
            pageCursor.close();
            pageCursors[i] = null;
            pageCursor = null;
          }
        }

        if (pageCursors[i] != null) {
          final RID[] rids = pageCursors[i].getValue();

          // For non-unique indexes a key may have a mix of valid RIDs and per-RID tombstones
          // (encoded as RID(-(bucketId+2), position)). The page still contributes to iteration
          // as long as it holds anything other than pure REMOVED_ENTRY_RID full-key tombstones;
          // per-RID filtering happens in next().
          boolean allFullKeyTomb = rids != null && rids.length > 0;
          if (allFullKeyTomb)
            for (final RID r : rids) {
              if (r.getBucketId() != -1 || r.getPosition() != -1) {
                allFullKeyTomb = false;
                break;
              }
            }

          if (allFullKeyTomb) {
            // #4944: never leave a cursor parked on a full-key tombstone - a page whose only entries left
            // are dead weight would sit in pageCursors forever contributing nothing, and every round of
            // fetchNext() would have to walk through it for no gain. Skip past tombstone-only keys
            // (recording them so older cursors skip their shadowed entries too: a newer re-add of
            // the same key always lives in an earlier-processed cursor, so this stays temporally
            // correct) until a countable key is found or the cursor is exhausted.
            removedKeys.add(keys);
            if (pageCursor.hasNext()) {
              advanceCursor(i); // keeps the cursorKeys cache in sync with the advanced cursor
              continue;
            }
            pageCursor.close();
            pageCursors[i] = null;
            cursorKeys[i] = null;
            break;
          }
        }
        break;
      }
    }

    getClosestEntryInTx(typedFromKeys, beginKeysInclusive);
  }

  /**
   * Human-readable description of the logical index this cursor belongs to, so an operator reading the
   * "infinite loop" error can tell which type/properties to rebuild instead of having only the internal
   * bucket-level sub-index name (e.g. {@code MyType_0_1234567890}). Best-effort: never throws, since it runs
   * while surfacing another error.
   */
  private String describeIndexForRebuild() {
    try {
      final LSMTreeIndex mainIndex = index.getMainIndex();
      if (mainIndex != null)
        return "type=" + mainIndex.getTypeName() + ", properties=" + mainIndex.getPropertyNames();
    } catch (final Exception e) {
      // IGNORE: fall back to the sub-index name already present in the message
    }
    return "type=?, properties=?";
  }

  @Override
  public String dumpStats() {
    final StringBuilder buffer = new StringBuilder(1024);

    buffer.append("%nDUMP OF %s UNDERLYING-CURSORS on index %s".formatted(pageCursors.length, index.getName()));
    for (int i = 0; i < pageCursors.length; ++i) {
      final LSMTreeIndexUnderlyingAbstractCursor cursor = pageCursors[i];

      if (cursor == null)
        buffer.append("%n- Cursor[%d] = null".formatted(i));
      else {
        buffer.append(
            "%n- Cursor[%d] %s=%s index=%s compacted=%s totalKeys=%d ascending=%s keyTypes=%s currentPageId=%s currentPosInPage=%d".formatted(
                i, Arrays.toString(cursorKeys[i]), Arrays.toString(cursor.getValue()), cursor.index,
                cursor instanceof LSMTreeIndexUnderlyingCompactedSeriesCursor, cursor.totalKeys, cursor.ascendingOrder,
                Arrays.toString(cursor.keyTypes), cursor.getCurrentPageId(), cursor.getCurrentPositionInPage()));
      }
    }

    return buffer.toString();
  }

  @Override
  public BinaryComparator getComparator() {
    return comparator;
  }

  @Override
  public byte[] getBinaryKeyTypes() {
    return binaryKeyTypes;
  }

  @Override
  public long estimateSize() {
    return -1L;
  }

  /**
   * Exact (#5635): the merge is run up front, so this answers on a RID the cursor can actually hand out. The work it
   * does is the work {@link #next()} used to do; nothing is done twice, since the prefetched entry is cached until
   * drained.
   */
  @Override
  public boolean hasNext() {
    if (nextValue == null)
      fetchNext();
    return nextValue != null;
  }

  @Override
  public RID next() {
    if (nextValue == null)
      fetchNext();

    if (nextValue == null)
      throw new NoSuchElementException("Index '" + index.getName() + "' cursor is exhausted");

    lastReturnedValue = nextValue;
    lastReturnedKeys = nextValueKeys;
    nextValue = null;
    nextValueKeys = null;
    return lastReturnedValue;
  }

  /**
   * Whether any SOURCE of entries is left: a live underlying cursor, the RIDs of the key group currently loaded, or the
   * in-transaction overlay. This is what {@code hasNext()} used to answer directly - optimistic, because a live source
   * may still hold nothing but tombstones - and it is now only the loop condition of {@link #fetchNext()}.
   * <p>
   * Reads {@link #pageCursors} directly rather than a separately maintained live-count (#5683): a counter kept in
   * lockstep with every site that nulls a slot is a standing invitation for the two to drift apart, and #4944 was
   * exactly that - a counter/array drift bug whose failure mode was silent truncation (the scan reported itself
   * exhausted while live page cursors remained). {@code pageCursors} is sized by the number of index components, not
   * by data, so this walk is short and bounded - it runs once per merge round, not once per row.
   */
  private boolean hasMoreSources() {
    if ((currentValues != null && currentValueIndex < currentValues.length) || txCursor != null)
      return true;
    for (final LSMTreeIndexUnderlyingAbstractCursor pageCursor : pageCursors)
      if (pageCursor != null)
        return true;
    return false;
  }

  /**
   * Advances the merge until it holds a surviving RID in {@link #nextValue}, or leaves it null when the scan is over.
   * <p>
   * Termination: every round either produces a value, consumes one RID from currentValues, or consumes one key by
   * advancing every contributing cursor via advanceCursor(), which throws on the only non-terminating state (a stuck
   * cursor); the tx overlay is a bounded collection. No iteration budget is imposed: a long tombstone run to skip is
   * legal work (compaction never purges tombstones) and its length is unrelated to the number of cursors, so any
   * count-based heuristic here misfires on delete-heavy workloads and fails healthy queries.
   */
  private void fetchNext() {
    while (true) {
      if (currentValues != null && currentValueIndex < currentValues.length) {
        final RID value = currentValues[currentValueIndex++];
        if (value != null && !index.isDeletedEntry(value)) {
          nextValue = value;
          nextValueKeys = currentKeys;
          return;
        }

        continue;
      }

      if (!hasMoreSources()) {
        flushScanStats();
        return;
      }

      currentValues = null;
      currentValueIndex = 0;

      Object[] minorKey = null;
      // #6944: explicit clear() rather than relying on the in-loop clear()s below, which only fire on paths
      // that touch a live pageCursor - a tx-only group (no live pageCursors this round) would otherwise carry
      // over stale indices from the previous group.
      minorKeyIndexes.clear();

      // FIND THE MINOR KEY
      for (int p = 0; p < totalCursors; ++p) {
        if (pageCursors[p] != null) {
          if (minorKey == null) {
            // #5683: clear before adding, exactly like every other transition into a new minor key below.
            // Without it, a live cursor whose cached key is itself null (cursorKeys[p] == null) leaves
            // minorKey == null after this branch, so the NEXT live cursor also lands here - and would be
            // appended to a list that already holds the first (differently-keyed) cursor instead of
            // starting a fresh one, folding two unrelated keys into one group.
            minorKeyIndexes.clear();
            minorKey = cursorKeys[p];
            minorKeyIndexes.add(p);
          } else {
            if (cursorKeys[p] != null) {
              final int compare = LSMTreeIndexMutable.compareKeys(comparator, binaryKeyTypes, cursorKeys[p], minorKey);
              if (compare == 0) {
                minorKeyIndexes.add(p);
              } else if ((ascendingOrder && compare < 0) || (!ascendingOrder && compare > 0)) {
                minorKey = cursorKeys[p];
                minorKeyIndexes.clear();
                minorKeyIndexes.add(p);
              }
            }
          }
        }
      }

      // #5055: the in-transaction overlay batch (txCursor) always covers a SINGLE key (txCursorKeys, all its
      // entries share it). Decide whether that key participates in this round WITHOUT consuming any entry yet,
      // so its RIDs can be MERGED with the disk RIDs when the keys collide (a committed and an uncommitted
      // record sharing the same non-unique key), and so a tx key strictly greater than the disk minor key is
      // left for a later round instead of being consumed and dropped.
      boolean includeTx = false;
      if (txCursor != null && txCursor.hasNext() && txCursorKeys != null) {
        if (minorKey == null) {
          minorKey = txCursorKeys;
          includeTx = true;
        } else {
          final int compare = LSMTreeIndexMutable.compareKeys(comparator, binaryKeyTypes, txCursorKeys, minorKey);
          if (compare == 0)
            // COLLISION: the same key exists on disk AND in the overlay - merge both sets of RIDs.
            includeTx = true;
          else if ((ascendingOrder && compare < 0) || (!ascendingOrder && compare > 0)) {
            // The overlay key sorts before every disk key: emit it alone this round.
            minorKey = txCursorKeys;
            minorKeyIndexes.clear();
            includeTx = true;
          }
        }
      }

      if (minorKey == null) {
        flushScanStats();
        return;
      }

      currentKeys = minorKey;

      // FILTER DELETED ITEMS.
      //
      // For non-unique LSM-Tree indexes a key may have a mixture of valid RIDs and tombstones:
      //   - REMOVED_ENTRY_RID = (-1, -1): a key-wide deletion (remove(keys) without rid).
      //   - getRemovedRID(rid) = ((bucketId+2) * -1, position): a per-RID deletion targeting a
      //     specific record that shared this key.
      //
      // The previous implementation collapsed any negative-bucketId entry into "the whole key
      // is removed", which was correct only for unique indexes; for non-unique indexes a single
      // per-RID tombstone caused every other RID at that key to be suppressed. Worse, the
      // constructor would refuse to iterate any page whose last value happened to be a per-RID
      // tombstone, so the cursor returned nothing as soon as deletions reached every page.
      //
      // We now process each page in temporal order (oldest to newest, i.e. minorKeyIndexes is
      // [newest, ..., oldest], so iterate it in reverse) and within each page in insertion
      // order. We track per-RID validity and only mark the whole key as deleted when we
      // encounter a REMOVED_ENTRY_RID tombstone; a later insert at the same key resurrects it.
      ridState.clear();

      for (int i = minorKeyIndexes.size() - 1; i >= 0; --i) {
        final int minorKeyIndex = minorKeyIndexes.get(i);
        final LSMTreeIndexUnderlyingAbstractCursor currentCursor = pageCursors[minorKeyIndex];
        currentKeys = currentCursor.getKeys();

        final RID[] pageValues = currentCursor.getValue();
        for (final RID rid : pageValues) {
          if (rid.getBucketId() == -1 && rid.getPosition() == -1) {
            // KEY-WIDE TOMBSTONE: invalidate every RID seen so far for this key. Any insert
            // that follows in the temporal stream remains valid.
            ridState.clear();
          } else if (rid.getBucketId() < 0) {
            // PER-RID TOMBSTONE: decode and mark only the original RID as deleted.
            ridState.put(new RID(-rid.getBucketId() - 2, rid.getPosition()), Boolean.TRUE);
          } else {
            // INSERT: most recent operation on this RID wins.
            ridState.put(rid, Boolean.FALSE);
          }
        }
      }

      // Collect the surviving RIDs for this key. #5055: disk RIDs and overlay RIDs are UNIONed - a committed
      // record and an uncommitted one sharing the same non-unique key must both appear during in-tx iteration.
      mergedRIDs.clear();

      // ADVANCE EACH PAGE CURSOR PAST THIS KEY and, if any page contributed, add its per-RID-filtered set.
      if (!minorKeyIndexes.isEmpty()) {
        for (final var entry : ridState.entrySet()) {
          if (Boolean.FALSE.equals(entry.getValue()))
            mergedRIDs.add(entry.getKey());
        }

        for (int i = 0; i < minorKeyIndexes.size(); ++i) {
          final int minorKeyIndex = minorKeyIndexes.get(i);
          final LSMTreeIndexUnderlyingAbstractCursor currentCursor = pageCursors[minorKeyIndex];

          if (currentCursor.hasNext()) {
            advanceCursor(minorKeyIndex);

            if (typedToKeys != null) {
              final int compare = LSMTreeIndexMutable.compareKeys(comparator, binaryKeyTypes, cursorKeys[minorKeyIndex], typedToKeys);

              if ((ascendingOrder && ((toKeysInclusive && compare > 0) || (!toKeysInclusive && compare >= 0))) || (!ascendingOrder
                  && ((toKeysInclusive && compare < 0) || (!toKeysInclusive && compare <= 0)))) {
                currentCursor.close();
                pageCursors[minorKeyIndex] = null;
                cursorKeys[minorKeyIndex] = null;
              }
            }
          } else {
            currentCursor.close();
            pageCursors[minorKeyIndex] = null;
            cursorKeys[minorKeyIndex] = null;
          }
        }
      }

      // #5055: drain ALL overlay RIDs sharing this key (the whole txCursor batch) and merge them in.
      if (includeTx)
        while (txCursor.hasNext())
          mergedRIDs.add((RID) txCursor.next());

      // a consumed key with no surviving RID is pure skip work caused by tombstone build-up:
      // account it so operators can see when a delete-heavy index needs a full compaction
      if (mergedRIDs.isEmpty())
        ++deadEntriesSkipped;

      currentValues = mergedRIDs.isEmpty() ? null : mergedRIDs.toArray(new RID[0]);

      if (txCursor == null || !txCursor.hasNext())
        getClosestEntryInTx(currentKeys != null ? currentKeys : typedFromKeys, false);
    }
  }

  /**
   * Advances an underlying cursor past its current (already consumed) entry, enforcing the
   * termination invariant of the whole scan: the consumed (page, position) of every underlying
   * cursor moves strictly monotonically in scan direction. Every legal state satisfies it: a
   * duplicate-key group merged inside a page settles the position on the far end of the group
   * before it is consumed, and a key whose values continue on the next page of a compacted series
   * advances the page number. A genuinely stuck cursor, the only way this iteration can fail to
   * terminate (e.g. the historical DESC duplicate-group bug where getKeys() re-settled on the same
   * group after next()), re-consumes a position and is detected on its first repetition.
   * <p>
   * Unlike the previous iteration-count heuristic (budgeted on the number of cursors), this check
   * can never misfire on a long tombstone run: since compaction does not purge tombstones,
   * delete-heavy workloads legitimately skip an unbounded number of consecutive dead keys, and
   * doing so used to fail healthy range scans with a false "index may be corrupted" error.
   * <p>
   * Must be called only when the cursor's {@code hasNext()} returned true; also refreshes the
   * {@code cursorKeys} cache for the new position.
   */
  private void advanceCursor(final int cursorIndex) {
    final LSMTreeIndexUnderlyingAbstractCursor cursor = pageCursors[cursorIndex];

    final int pageNumber = cursor.getCurrentPageId().getPageNumber();
    final int position = cursor.getCurrentPositionInPage();

    final int lastPage = lastConsumedPageNumber[cursorIndex];
    final int lastPosition = lastConsumedPosition[cursorIndex];

    final boolean advanced = ascendingOrder ?
        pageNumber > lastPage || (pageNumber == lastPage && position > lastPosition) :
        pageNumber < lastPage || (pageNumber == lastPage && position < lastPosition);

    if (!advanced)
      throw new IllegalStateException(
          "Detected infinite loop while iterating index '" + index.getName() + "' (" + describeIndexForRebuild() + ", DESC=" + (
              !ascendingOrder) + ", pageNumber=" + pageNumber + ", positionInPage=" + position
              + "): the cursor did not advance. The index may be corrupted, please rebuild it with: REBUILD INDEX `"
              + index.getName() + "`");

    lastConsumedPageNumber[cursorIndex] = pageNumber;
    lastConsumedPosition[cursorIndex] = position;

    cursor.next();
    cursorKeys[cursorIndex] = cursor.getKeys();
  }

  private void getClosestEntryInTx(final Object[] keys, final boolean inclusive) {
    txCursor = null;
    txCursorKeys = null;
    if (index.getDatabase().getTransaction().getStatus() == TransactionContext.STATUS.BEGUN) {
      Set<IndexCursorEntry> txChanges = null;

      final TreeMap<TransactionIndexContext.ComparableKey, Map<TransactionIndexContext.IndexKey, TransactionIndexContext.IndexKey>> indexChanges = index.getDatabase()
          .getTransaction().getIndexChanges().getIndexKeys(index.getName());
      if (indexChanges != null) {
        Map.Entry<TransactionIndexContext.ComparableKey, Map<TransactionIndexContext.IndexKey, TransactionIndexContext.IndexKey>> entry;
        // #4947: biased navigation keys, never plain ComparableKeys. A PARTIAL key compares equal to every
        // entry sharing its prefix, so plain ceiling/floor/higher/lower land in the MIDDLE of the prefix run
        // (wherever the tree walk first hits equality) and silently skip the run's other entries. The biased
        // keys sort strictly before (low) or after (high) the whole run, making the navigation exact; for
        // full-length keys they degenerate to the plain behavior.
        if (ascendingOrder) {
          if (keys == null)
            entry = indexChanges.firstEntry();
          else if (inclusive)
            entry = indexChanges.ceilingEntry(TransactionIndexContext.lowNavigationKey(keys));
          else
            entry = indexChanges.higherEntry(TransactionIndexContext.highNavigationKey(keys));
        } else {
          if (keys == null)
            entry = indexChanges.lastEntry();
          else if (inclusive)
            entry = indexChanges.floorEntry(TransactionIndexContext.highNavigationKey(keys));
          else
            entry = indexChanges.lowerEntry(TransactionIndexContext.lowNavigationKey(keys));
        }

        // The first candidate found above can be entirely dead (every pending change on it is a REMOVE - e.g.
        // a record inserted and deleted again within this same transaction): walk to the NEXT candidate in the
        // same direction instead of giving up, exactly like the on-page cursor skips a fully-tombstoned key via
        // advanceCursor()+continue in the constructor above. Stopping at the first dead key used to make the
        // WHOLE in-tx overlay look exhausted beyond it, even though older/newer pending keys past it were still
        // live - e.g. a composite-index prefix scan whose ORDER BY DESC starts from the just-deleted top of the
        // group came back empty instead of falling through to the next surviving row (#6592 follow-up).
        while (entry != null) {
          final Object[] tmpKeys = entry.getKey().values;

          if (typedToKeys != null) {
            // #5055: toKeys bounds the FAR end of the scan, whose direction flips with the order: for an
            // ascending scan it is the upper bound (skip entries ABOVE it), for a descending scan it is the
            // lower bound (skip entries BELOW it). #5932: tmpKeys comes straight from the overlay's own
            // ComparableKey, so the bound compared against it must be typedToKeys (no disk byte[]-for-String
            // encoding), not serializedToKeys.
            final int cmp = LSMTreeIndexMutable.compareKeys(comparator, binaryKeyTypes, tmpKeys, typedToKeys);
            final boolean pastBound = (ascendingOrder && cmp > 0) || (!ascendingOrder && cmp < 0) || (!toKeysInclusive && cmp == 0);
            if (pastBound)
              // EVERY CANDIDATE FURTHER IN THIS DIRECTION IS EVEN FURTHER PAST THE BOUND: STOP, DON'T WALK THE
              // REST OF THE OVERLAY FOR NOTHING
              break;
          }

          final Map<TransactionIndexContext.IndexKey, TransactionIndexContext.IndexKey> values = entry.getValue();
          if (values != null) {
            for (final TransactionIndexContext.IndexKey value : values.values()) {
              if (value == null || value.operation == TransactionIndexContext.IndexKey.IndexKeyOperation.REMOVE)
                // REMOVED: A NON-UNIQUE KEY CAN STILL HOLD OTHER LIVE VALUES, SO SKIP JUST THIS ONE RATHER THAN
                // THE WHOLE KEY
                continue;

              if (txChanges == null) {
                txChanges = new HashSet<>();
                // All entries of this batch share the single overlay key just navigated to; cache it so
                // next() can peek the batch key without consuming an entry (#5055).
                txCursorKeys = tmpKeys;
              }

              txChanges.add(new IndexCursorEntry(tmpKeys, value.rid, 1));
            }
          }

          if (txChanges != null)
            // FOUND A LIVE CANDIDATE
            break;

          // THIS KEY HAD NOTHING LIVE: TRY THE NEXT ONE IN THE SAME DIRECTION
          entry = ascendingOrder ? indexChanges.higherEntry(entry.getKey()) : indexChanges.lowerEntry(entry.getKey());
        }
      }

      if (txChanges != null) {
        // MERGE SETS
        txCursor = new TempIndexCursor(txChanges);
      }
    }

  }

  /** The keys of the entry {@link #next()} last returned, or null before the first one (#5635). */
  @Override
  public Object[] getKeys() {
    return lastReturnedKeys;
  }

  /** The entry {@link #next()} last returned, or null before the first one (#5635). */
  @Override
  public Identifiable getRecord() {
    return lastReturnedValue;
  }

  @Override
  public void close() {
    for (final LSMTreeIndexUnderlyingAbstractCursor it : pageCursors)
      if (it != null)
        it.close();
    Arrays.fill(pageCursors, null);
    // a closed cursor is exhausted: drop the prefetched entry and the merge state so hasNext() cannot resurrect it
    txCursor = null;
    txCursorKeys = null;
    currentValues = null;
    currentValueIndex = 0;
    nextValue = null;
    nextValueKeys = null;
    flushScanStats();
  }

  /** Flush-and-reset so exhaustion followed by an explicit close() cannot double-count. */
  private void flushScanStats() {
    if (deadEntriesSkipped > 0 && index.mainIndex != null) {
      index.mainIndex.addDeadEntriesSkipped(deadEntriesSkipped);
      deadEntriesSkipped = 0;
    }
  }

  @Override
  public Iterator<Identifiable> iterator() {
    return this;
  }
}
