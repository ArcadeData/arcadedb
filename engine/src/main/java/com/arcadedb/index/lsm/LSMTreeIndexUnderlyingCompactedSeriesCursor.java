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

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.engine.BasePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.index.IndexException;

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.List;

/**
 * Walks the entries of one compacted series, a key at a time. A key is one entry here, whatever the number of its values.
 * <p>
 * The writer stores the values of a key in chunks (see {@link LSMTreeIndexCompactedStreamWriter#appendBounded}), and a key with
 * many values has its chunks in consecutive entries that can run over the end of a data page. The page cursor merges the chunks
 * of one page; this cursor merges the ones that continue on the next page (or the previous one, scanning backwards), so the
 * values of a key are always returned together, oldest first. Handed out in parts, the merge of the cursors of an index
 * ({@link LSMTreeIndexCursor}) resolved the tombstones of the newer cursors against the first part only, and the later parts
 * returned entries that had been deleted (issue #10188).
 * <p>
 * The key is read on the first {@link #getKeys()}, {@link #getValue()}, {@link #getCurrentPageId()} or
 * {@link #getCurrentPositionInPage()} after {@link #next()}, and from then on the position names the end of the key, the entry
 * {@link #next()} steps past, whichever of the four was asked first. The values of a key are held in one array, so the memory a
 * key takes is proportional to its number of values, as the merge of the cursors already needs.
 */
public class LSMTreeIndexUnderlyingCompactedSeriesCursor extends LSMTreeIndexUnderlyingAbstractCursor {
  // The last data page of the series to visit in the direction of the scan: the highest page number going up, the lowest going down
  private final int                              lastPageNumber;
  private       LSMTreeIndexUnderlyingPageCursor pageCursor;
  private       boolean                          closed = false;
  // The key at the position of the cursor and all of its values, set by the first getKeys() or getValue() and dropped by next()
  private       Object[]                         groupKeys;
  private       RID[]                            groupValues;

  /**
   * The liveness token handed to the file's cursor registry (#5662), and the weak registration to hand back on
   * {@link #close()}. The token is a plain object rather than {@code this} so the registry never gets a reference to a
   * half-constructed cursor; it dies with this cursor, which is what lets an abandoned scan stop pinning the file.
   */
  private final Object                           liveToken = new Object();
  private final WeakReference<Object>            registration;

  public LSMTreeIndexUnderlyingCompactedSeriesCursor(final LSMTreeIndexCompacted index, final int firstPageNumber,
      final int lastPageNumber,
      final byte[] keyTypes, final boolean ascendingOrder, final int posInPage) {
    super(index, keyTypes, keyTypes.length, ascendingOrder);
    this.lastPageNumber = lastPageNumber;

    // Series pages are loaded LAZILY as the scan advances: register with the file so a full
    // compaction cannot physically drop it while this cursor may still need to read from it.
    this.registration = index.onCursorOpened(liveToken);

    loadNextNonEmptyPage(firstPageNumber, posInPage);
  }

  @Override
  public boolean hasNext() {
    if (pageCursor == null)
      return false;

    if (pageCursor.hasNext())
      return true;

    final int nextPage = pageCursor.pageId.getPageNumber() + (ascendingOrder ? 1 : -1);

    loadNextNonEmptyPage(nextPage, -1);

    return pageCursor.hasNext();
  }

  private void loadNextNonEmptyPage(final int startingPageNumber, int posInPage) {
    // LOAD NEXT PAGE IF NEEDED
    for (int currentPageNumber = startingPageNumber; ascendingOrder ?
        currentPageNumber <= lastPageNumber :
        currentPageNumber >= lastPageNumber; currentPageNumber += ascendingOrder ? 1 : -1) {
      try {
        final DatabaseInternal database = index.getDatabase();
        final BasePage page = database.getTransaction()
            .getPage(new PageId(database, index.getFileId(), currentPageNumber), index.getPageSize());
        final int count = index.getCount(page);

        pageCursor = new LSMTreeIndexUnderlyingPageCursor(index, page, posInPage == -1 ? ascendingOrder ? -1 : count : posInPage,
            index.getHeaderSize(currentPageNumber), keyTypes, count, ascendingOrder);

        if (pageCursor.hasNext())
          break;

        // RESET POSITION IN PAGE AFTER THE 1ST PAGE
        posInPage = -1;

      } catch (final IOException e) {
        throw new IndexException("Error on iterating cursor on compacted index", e);
      }
    }
  }

  @Override
  public void next() {
    pageCursor.next();
    groupKeys = null;
    groupValues = null;
  }

  @Override
  public Object[] getKeys() {
    if (groupKeys == null)
      loadGroup();
    return groupKeys;
  }

  @Override
  public RID[] getValue() {
    if (groupKeys == null)
      loadGroup();
    return groupValues;
  }

  /**
   * Reads the key at the position of the cursor with all its values. When the chunks of the key reach the edge of the data page,
   * in the direction of the scan, the next page is read for the ones that continue there, and the cursor moves to the page the
   * key ends on, so that {@link #next()} steps past all of it. A scan reaches a key at the end of its chunks it starts from (the
   * first one going up, the last one going down, which is where a seek lands too), so that is the only direction to look in.
   */
  private void loadGroup() {
    final Object[] keys = pageCursor.getKeys();
    final RID[] firstValues = pageCursor.getValue();

    List<RID[]> parts = null;
    while (reachesEdgeOfPage(pageCursor)) {
      final LSMTreeIndexUnderlyingPageCursor adjacent = adjacentPageCursor(pageCursor.pageId.getPageNumber() + (ascendingOrder ? 1 : -1));
      if (adjacent == null)
        break;
      // To its first entry going up, to its last one going down: where this scan would arrive
      adjacent.next();
      if (LSMTreeIndexMutable.compareKeys(index.comparator, keyTypes, keys, adjacent.getKeys()) != 0)
        break;

      if (parts == null) {
        parts = new ArrayList<>();
        parts.add(firstValues);
      }
      // Oldest first, as the chunks were written: after what was read going up, before it going down
      parts.add(ascendingOrder ? parts.size() : 0, adjacent.getValue());
      pageCursor = adjacent;
    }

    groupKeys = keys;
    groupValues = parts == null ? firstValues : concatenate(parts);
  }

  private boolean reachesEdgeOfPage(final LSMTreeIndexUnderlyingPageCursor cursor) {
    // The page cursor has merged its own chunks of the key and left its position on the last one going up, on the first going down
    return ascendingOrder ? cursor.currentEntryIndex >= cursor.totalKeys - 1 : cursor.currentEntryIndex <= 0;
  }

  /**
   * The cursor of the first page that holds entries from {@code startingPageNumber} on, in the direction of the scan, positioned
   * before its first entry, or null when the series has no such page.
   */
  private LSMTreeIndexUnderlyingPageCursor adjacentPageCursor(final int startingPageNumber) {
    for (int pageNumber = startingPageNumber; ascendingOrder ? pageNumber <= lastPageNumber : pageNumber >= lastPageNumber;
        pageNumber += ascendingOrder ? 1 : -1) {
      try {
        final DatabaseInternal database = index.getDatabase();
        final BasePage page = database.getTransaction().getPage(new PageId(database, index.getFileId(), pageNumber), index.getPageSize());
        final int count = index.getCount(page);
        if (count > 0)
          return new LSMTreeIndexUnderlyingPageCursor(index, page, ascendingOrder ? -1 : count, index.getHeaderSize(pageNumber), keyTypes,
              count, ascendingOrder);
      } catch (final IOException e) {
        throw new IndexException("Error on iterating cursor on compacted index", e);
      }
    }
    return null;
  }

  private static RID[] concatenate(final List<RID[]> parts) {
    int total = 0;
    for (final RID[] part : parts)
      total += part.length;
    final RID[] all = new RID[total];
    int position = 0;
    for (final RID[] part : parts) {
      System.arraycopy(part, 0, all, position, part.length);
      position += part.length;
    }
    return all;
  }

  @Override
  public PageId getCurrentPageId() {
    settleOnKey();
    return pageCursor.pageId;
  }

  @Override
  public int getCurrentPositionInPage() {
    settleOnKey();
    return pageCursor.currentEntryIndex;
  }

  /**
   * Reads the key the cursor stands on, if it has not been read since {@link #next()}, so that a position asked first is the same
   * as one asked after the key: the end of the key, not the start of its first chunk. A cursor that is not on an entry yet (before
   * the first one, or past the last) has no key to read.
   */
  private void settleOnKey() {
    if (groupKeys == null && pageCursor.currentEntryIndex >= 0 && pageCursor.currentEntryIndex < pageCursor.totalKeys)
      loadGroup();
  }

  @Override
  public void close() {
    // idempotent: exhaustion paths and explicit close() may both fire
    if (!closed) {
      closed = true;
      ((LSMTreeIndexCompacted) index).onCursorClosed(registration);
    }
  }
}
