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

import com.arcadedb.database.*;
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
 * Index cursor doesn't remove the deleted entries.
 */
public class LSMTreeIndexCursor implements IndexCursor {
  private final LSMTreeIndexMutable                    index;
  private final boolean                                ascendingOrder;
  private final Object[]                               fromKeys;
  private final Object[]                               toKeys;
  private final Object[]                               serializedToKeys;
  private final boolean                                toKeysInclusive;
  private final LSMTreeIndexUnderlyingAbstractCursor[] pageCursors;
  private       Object[]                               currentKeys;
  private       RID[]                                  currentValues;
  private       int                                    currentValueIndex = 0;
  private final int                                    totalCursors;
  private final byte[]                                 keyTypes;
  private final Object[][]                             cursorKeys;
  private final BinaryComparator                       comparator;
  private       int                                    validIterators;
  private       TempIndexCursor                        txCursor;

  public LSMTreeIndexCursor(final LSMTreeIndexMutable index, final boolean ascendingOrder) throws IOException {
    this(index, ascendingOrder, null, true, null, true);
  }

  public LSMTreeIndexCursor(final LSMTreeIndexMutable index, final boolean ascendingOrder, final Object[] fromKeys, final boolean beginKeysInclusive,
      final Object[] toKeys, final boolean endKeysInclusive) throws IOException {
    this.index = index;
    this.ascendingOrder = ascendingOrder;
    this.keyTypes = index.getKeyTypes();

    index.checkForNulls(fromKeys);
    index.checkForNulls(toKeys);

    final Object[] serializedFromKeys = index.convertKeys(fromKeys, keyTypes);

    this.fromKeys = fromKeys;

    this.toKeys = toKeys != null && toKeys.length == 0 ? null : toKeys;
    this.serializedToKeys = index.convertKeys(this.toKeys, keyTypes);
    this.toKeysInclusive = endKeysInclusive;

    BinarySerializer serializer = index.getDatabase().getSerializer();
    this.comparator = serializer.getComparator();

    final LSMTreeIndexCompacted compacted = index.getSubIndex();

    final List<LSMTreeIndexUnderlyingCompactedSeriesCursor> compactedSeriesIterators;

    if (compacted != null)
      // INCLUDE COMPACTED
      compactedSeriesIterators = compacted.newIterators(ascendingOrder, serializedFromKeys);
    else
      compactedSeriesIterators = Collections.emptyList();

    final int totalPages = index.getTotalPages();

    totalCursors = totalPages + compactedSeriesIterators.size();

    // CREATE AN ARRAY OF CURSOR. SINCE WITH LSM THE LATEST PAGE IS THE MOST UPDATED, IN THE ARRAY ARE SET FIRST THE MUTABLE ONES BECAUSE THEY ARE MORE UPDATED.
    // FROM THE LAST TO THE FIRST. THEN THE COMPACTED, FROM THE LAST TO THE FIRST
    pageCursors = new LSMTreeIndexUnderlyingAbstractCursor[totalCursors];
    cursorKeys = new Object[totalCursors][keyTypes.length];

    validIterators = 0;

    for (int i = 0; i < compactedSeriesIterators.size(); ++i) {
      LSMTreeIndexUnderlyingCompactedSeriesCursor pageCursor = compactedSeriesIterators.get(i);
      if (pageCursor != null) {
        if (pageCursor.hasNext()) {
          pageCursor.next();
          cursorKeys[totalPages + i] = pageCursor.getKeys();
        } else
          pageCursor = null;
        pageCursors[totalPages + i] = pageCursor;
      }
    }

    int pageCounter = 0;
    for (int pageId = totalPages - 1; pageId > -1; --pageId) {
      final int cursorIdx = pageCounter;

      if (serializedFromKeys != null) {
        // SEEK FOR THE FROM RANGE
        final BasePage currentPage = index.getDatabase().getTransaction().getPage(new PageId(index.getFileId(), pageId), index.getPageSize());
        final Binary currentPageBuffer = new Binary(currentPage.slice());
        final int count = index.getCount(currentPage);

        if (count > 0) {
          LSMTreeIndexMutable.LookupResult lookupResult = index.lookupInPage(currentPage.getPageId().getPageNumber(), count, currentPageBuffer,
              serializedFromKeys, ascendingOrder ? 2 : 3);

          if (!lookupResult.outside) {
            pageCursors[cursorIdx] = index.newPageIterator(pageId, lookupResult.keyIndex, ascendingOrder);
            cursorKeys[cursorIdx] = pageCursors[cursorIdx].getKeys();

            if (ascendingOrder) {
              if (LSMTreeIndexMutable.compareKeys(comparator, keyTypes, cursorKeys[cursorIdx], fromKeys) < 0) {
                pageCursors[cursorIdx] = null;
                cursorKeys[cursorIdx] = null;
              }
            } else {
              if (LSMTreeIndexMutable.compareKeys(comparator, keyTypes, cursorKeys[cursorIdx], fromKeys) > 0) {
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
          final BasePage currentPage = index.getDatabase().getTransaction().getPage(new PageId(index.getFileId(), pageId), index.getPageSize());
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

    final Set<RID> removedRIDs = new HashSet<>();
    final Set<RID> validRIDs = new HashSet<>();

    // CHECK THE VALIDITY OF CURSORS
    for (int i = 0; i < pageCursors.length; ++i) {
      final LSMTreeIndexUnderlyingAbstractCursor pageCursor = pageCursors[i];

      if (pageCursor != null) {
        if (fromKeys != null && !beginKeysInclusive) {
          if (LSMTreeIndexMutable.compareKeys(comparator, keyTypes, cursorKeys[i], fromKeys) == 0) {
            // SKIP THIS
            if (pageCursor.hasNext()) {
              pageCursor.next();
              cursorKeys[i] = pageCursor.getKeys();
            } else
              // INVALID
              pageCursors[i] = null;
          }
        }

        if (this.serializedToKeys != null) {
          //final Object[] cursorKey = index.convertKeys(index.checkForNulls(pageCursor.getKeys()), keyTypes);
          final int compare = LSMTreeIndexMutable.compareKeys(comparator, keyTypes, pageCursor.getKeys(), this.toKeys);

          if ((ascendingOrder && ((endKeysInclusive && compare <= 0) || (!endKeysInclusive && compare < 0))) || (!ascendingOrder && (
              (endKeysInclusive && compare >= 0) || (!endKeysInclusive && compare > 0))))
            ;
          else
            // INVALID
            pageCursors[i] = null;
        }

        if (pageCursors[i] != null) {
          final RID[] rids = pageCursors[i].getValue();
          if (rids != null && rids.length > 0) {
            for (RID r : rids) {
              if (r.getBucketId() < 0) {
                final RID originalRID = index.getOriginalRID(r);
                if (!validRIDs.contains(originalRID))
                  removedRIDs.add(originalRID);
                break;
              } else if (removedRIDs.contains(r))
                // HAS BEEN DELETED
                continue;

              validRIDs.add(r);
              validIterators++;
            }
          }
        }
      }
    }

    getClosestEntryInTx(fromKeys, beginKeysInclusive);
  }

  @Override
  public String dumpStats() {
    final StringBuilder buffer = new StringBuilder(1024);

    buffer.append(String.format("%nDUMP OF %s UNDERLYING-CURSORS on index %s", pageCursors.length, index.getName()));
    for (int i = 0; i < pageCursors.length; ++i) {
      final LSMTreeIndexUnderlyingAbstractCursor cursor = pageCursors[i];

      if (cursor == null)
        buffer.append(String.format("%n- Cursor[%d] = null", i));
      else {
        buffer.append(String.format("%n- Cursor[%d] %s=%s index=%s compacted=%s totalKeys=%d ascending=%s keyTypes=%s currentPageId=%s currentPosInPage=%d", i,
            Arrays.toString(cursorKeys[i]), Arrays.toString(cursor.getValue()), cursor.index, cursor instanceof LSMTreeIndexUnderlyingCompactedSeriesCursor,
            cursor.totalKeys, cursor.ascendingOrder, Arrays.toString(cursor.keyTypes), cursor.getCurrentPageId(), cursor.getCurrentPositionInPage()));
      }
    }

    return buffer.toString();
  }

  @Override
  public BinaryComparator getComparator() {
    return comparator;
  }

  @Override
  public byte[] getKeyTypes() {
    return keyTypes;
  }

  @Override
  public long size() {
    return 0;
  }

  @Override
  public boolean hasNext() {
    return validIterators > 0 || (currentValues != null && currentValueIndex < currentValues.length) || txCursor != null;
  }

  @Override
  public RID next() {
    do {
      if (currentValues != null && currentValueIndex < currentValues.length) {
        final RID value = currentValues[currentValueIndex++];
        if (!index.isDeletedEntry(value))
          return value;

        continue;
      }

      currentValueIndex = 0;

      Object[] minorKey = null;
      final List<Integer> minorKeyIndexes = new ArrayList<>();

      // FIND THE MINOR KEY
      for (int p = 0; p < totalCursors; ++p) {
        if (pageCursors[p] != null) {
          if (minorKey == null) {
            minorKey = cursorKeys[p];
            minorKeyIndexes.add(p);
          } else {
            if (cursorKeys[p] != null) {
              final int compare = LSMTreeIndexMutable.compareKeys(comparator, keyTypes, cursorKeys[p], minorKey);
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

      if (txCursor != null && txCursor.hasNext()) {
        currentValues = new RID[] { (RID) txCursor.next() };

        final Object[] txKeys = txCursor.getKeys();
        if (minorKey != null) {
          final int compare = LSMTreeIndexMutable.compareKeys(comparator, keyTypes, txKeys, minorKey);
          if (compare == 0) {
          } else if ((ascendingOrder && compare < 0) || (!ascendingOrder && compare > 0)) {
            minorKey = txKeys;
            minorKeyIndexes.clear();
          }
        } else
          minorKey = txKeys;
      }

      if (minorKey == null)
        throw new NoSuchElementException();

      currentKeys = minorKey;

      // FILTER DELETED ITEMS
      final Set<RID> removedRIDs = new HashSet<>();
      final Set<RID> validRIDs = new HashSet<>();

      boolean removedEntry = false;
      for (int i = 0; i < minorKeyIndexes.size(); ++i) {
        final int minorKeyIndex = minorKeyIndexes.get(i);

        LSMTreeIndexUnderlyingAbstractCursor currentCursor = pageCursors[minorKeyIndex];
        currentKeys = currentCursor.getKeys();

        final RID[] tempCurrentValues = currentCursor.getValue();

        if (i == 0 || currentValues == null)
          currentValues = tempCurrentValues;
        else {
          // MERGE VALUES
          final RID[] newArray = Arrays.copyOf(currentValues, currentValues.length + tempCurrentValues.length);

          for (int k = currentValues.length; k < newArray.length; ++k)
            newArray[k] = tempCurrentValues[k - currentValues.length];
          currentValues = newArray;
        }

        // START FROM THE LAST ENTRY
        for (int k = currentValues.length - 1; k > -1; --k) {
          final RID rid = currentValues[k];

          if (index.REMOVED_ENTRY_RID.equals(rid)) {
            removedEntry = true;
            break;
          }

          if (rid.getBucketId() < 0) {
            // RID DELETED, SKIP THE RID
            final RID originalRID = index.getOriginalRID(rid);
            if (!validRIDs.contains(originalRID))
              removedRIDs.add(originalRID);
            continue;
          }

          if (removedRIDs.contains(rid))
            // HAS BEEN DELETED
            continue;

          validRIDs.add(rid);
        }

        // PREPARE THE NEXT ENTRY
        if (currentCursor.hasNext()) {
          currentCursor.next();
          cursorKeys[minorKeyIndex] = currentCursor.getKeys();

          if (serializedToKeys != null) {
            final int compare = LSMTreeIndexMutable.compareKeys(comparator, keyTypes, cursorKeys[minorKeyIndex], toKeys);

            if ((ascendingOrder && ((toKeysInclusive && compare > 0) || (!toKeysInclusive && compare >= 0))) || (!ascendingOrder && (
                (toKeysInclusive && compare < 0) || (!toKeysInclusive && compare <= 0)))) {
              currentCursor.close();
              pageCursors[minorKeyIndex] = null;
              cursorKeys[minorKeyIndex] = null;
              --validIterators;
            }
          }
        } else {
          currentCursor.close();
          pageCursors[minorKeyIndex] = null;
          cursorKeys[minorKeyIndex] = null;
          --validIterators;
        }

        if (removedEntry) {
          currentValues = null;
          break;
        }

        if (validRIDs.isEmpty())
          currentValues = null;
        else
          validRIDs.toArray(currentValues);
      }

      if (txCursor == null || !txCursor.hasNext())
        getClosestEntryInTx(currentKeys != null ? currentKeys : fromKeys, false);

    } while ((currentValues == null || currentValues.length == 0 || (currentValueIndex < currentValues.length && index.isDeletedEntry(
        currentValues[currentValueIndex]))) && hasNext());

    return currentValues == null || currentValueIndex >= currentValues.length ? null : currentValues[currentValueIndex++];
  }

  private void getClosestEntryInTx(final Object[] keys, final boolean inclusive) {
    txCursor = null;
    if (index.getDatabase().getTransaction().getStatus() == TransactionContext.STATUS.BEGUN) {
      Set<IndexCursorEntry> txChanges = null;

      final TreeMap<TransactionIndexContext.ComparableKey, Map<TransactionIndexContext.IndexKey, TransactionIndexContext.IndexKey>> indexChanges = index.getDatabase()
          .getTransaction().getIndexChanges().getIndexKeys(index.getName());
      if (indexChanges != null) {
        final Map.Entry<TransactionIndexContext.ComparableKey, Map<TransactionIndexContext.IndexKey, TransactionIndexContext.IndexKey>> entry;
        if (ascendingOrder) {
          if (keys == null)
            entry = indexChanges.firstEntry();
          else if (inclusive)
            entry = indexChanges.ceilingEntry(new TransactionIndexContext.ComparableKey(keys));
          else
            entry = indexChanges.higherEntry(new TransactionIndexContext.ComparableKey(keys));
        } else {
          if (keys == null)
            entry = indexChanges.lastEntry();
          else if (inclusive)
            entry = indexChanges.floorEntry(new TransactionIndexContext.ComparableKey(keys));
          else
            entry = indexChanges.lowerEntry(new TransactionIndexContext.ComparableKey(keys));
        }

        final Map<TransactionIndexContext.IndexKey, TransactionIndexContext.IndexKey> values = entry != null ? entry.getValue() : null;
        if (values != null) {
          for (final TransactionIndexContext.IndexKey value : values.values()) {
            if (value != null) {
              if (!value.addOperation)
                // REMOVED
                break;

              final Object[] tmpKeys = entry.getKey().values;

              if (toKeys != null) {
                final int cmp = LSMTreeIndexMutable.compareKeys(comparator, keyTypes, tmpKeys, toKeys);

                if (ascendingOrder) {
                  if (toKeysInclusive && cmp > 0)
                    continue;
                  else if (!toKeysInclusive && cmp >= 0)
                    continue;
                } else {
                  if (toKeysInclusive && cmp < 0)
                    continue;
                  else if (!toKeysInclusive && cmp <= 0)
                    continue;
                }
              }

              if (txChanges == null)
                txChanges = new HashSet<>();

              txChanges.add(new IndexCursorEntry(tmpKeys, value.rid, 1));
            }
          }
        }
      }

      if (txChanges != null) {
        // MERGE SETS
        txCursor = new TempIndexCursor(txChanges);
      }
    }

  }

  @Override
  public Object[] getKeys() {
    return currentKeys;
  }

  @Override
  public Identifiable getRecord() {
    if (currentValues != null && currentValueIndex < currentValues.length) {
      final RID value = currentValues[currentValueIndex];
      if (!index.isDeletedEntry(value))
        return value;
    }
    return null;
  }

  @Override
  public int getScore() {
    return 1;
  }

  @Override
  public void close() {
    for (LSMTreeIndexUnderlyingAbstractCursor it : pageCursors)
      if (it != null)
        it.close();
    Arrays.fill(pageCursors, null);
  }

  @Override
  public Iterator<Identifiable> iterator() {
    return this;
  }
}
