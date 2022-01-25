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

import static com.arcadedb.database.Binary.BYTE_SERIALIZED_SIZE;
import static com.arcadedb.database.Binary.INT_SERIALIZED_SIZE;

import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.database.TrackableBinary;
import com.arcadedb.engine.BasePage;
import com.arcadedb.engine.MutablePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.engine.PaginatedFile;
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.index.IndexCursorEntry;
import com.arcadedb.log.LogManager;

import java.io.IOException;
import java.util.*;
import java.util.logging.Level;

/**
 * The first page (main page) contains the total pages under the fields "compactedPageNumberOfSeries". This is to avoid concurrent read/write while compaction.
 */
public class LSMTreeIndexCompacted extends LSMTreeIndexAbstract {
  public static final String UNIQUE_INDEX_EXT    = "uctidx";
  public static final String NOTUNIQUE_INDEX_EXT = "nuctidx";

  /**
   * Called at cloning time.
   */
  public LSMTreeIndexCompacted(final LSMTreeIndex mainIndex, final DatabaseInternal database, final String name, final boolean unique, final String filePath,
      final byte[] keyTypes, final int pageSize) throws IOException {
    super(mainIndex, database, name, unique, filePath, unique ? UNIQUE_INDEX_EXT : NOTUNIQUE_INDEX_EXT, keyTypes, pageSize,
        LSMTreeIndexMutable.CURRENT_VERSION);
  }

  /**
   * Called at load time (1st page only).
   */
  protected LSMTreeIndexCompacted(final LSMTreeIndex mainIndex, final DatabaseInternal database, final String name, final boolean unique, final String filePath,
      final int id, final PaginatedFile.MODE mode, final int pageSize, final int version) throws IOException {
    super(mainIndex, database, name, unique, filePath, id, mode, pageSize, version);
  }

  public Set<IndexCursorEntry> get(final Object[] keys, final int limit) {
    checkForNulls(keys);

    final Object[] convertedKeys = convertKeys(keys, binaryKeyTypes);
    if (convertedKeys == null && nullStrategy == NULL_STRATEGY.SKIP)
      return Collections.emptySet();

    try {
      final Set<IndexCursorEntry> set = new HashSet<>();

      final Set<RID> removedRIDs = new HashSet<>();

      // SEARCH IN COMPACTED INDEX
      searchInCompactedIndex(keys, convertedKeys, limit, set, removedRIDs);

      return set;

    } catch (IOException e) {
      throw new DatabaseOperationException("Cannot lookup key '" + Arrays.toString(keys) + "' in index '" + name + "'", e);
    }
  }

  public MutablePage appendDuringCompaction(final Binary keyValueContent, MutablePage currentPage, final TrackableBinary currentPageBuffer,
      final int compactedPageNumberOfSeries, final Object[] keys, final RID[] rids) throws IOException, InterruptedException {
    if (keys == null)
      throw new IllegalArgumentException("Keys parameter is null");

    TrackableBinary pageBuffer = currentPageBuffer;

    if (currentPage == null) {
      // CREATE A NEW PAGE
      currentPage = createNewPage(compactedPageNumberOfSeries);
      pageBuffer = currentPage.getTrackable();
    }

    int count = getCount(currentPage);

    int pageNum = currentPage.getPageId().getPageNumber();

    final Object[] convertedKeys = convertKeys(keys, binaryKeyTypes);

    writeEntry(keyValueContent, convertedKeys, rids);

    int keyValueFreePosition = getValuesFreePosition(currentPage);

    if (keyValueFreePosition - (getHeaderSize(pageNum) + (count * INT_SERIALIZED_SIZE) + INT_SERIALIZED_SIZE) < keyValueContent.size()) {
      // NO SPACE LEFT, CREATE A NEW PAGE AND FLUSH TO THE DATABASE THE CURRENT ONE (NO WAL)
      database.getPageManager().updatePage(currentPage, true);
      database.getPageManager().flushPages(Collections.singletonList(currentPage), true);

      currentPage = createNewPage(compactedPageNumberOfSeries);
      pageBuffer = currentPage.getTrackable();
      pageNum = currentPage.getPageId().getPageNumber();
      count = 0;
      keyValueFreePosition = currentPage.getMaxContentSize();
    }

    keyValueFreePosition -= keyValueContent.size();

    // WRITE KEY/VALUE PAIR CONTENT
    pageBuffer.putByteArray(keyValueFreePosition, keyValueContent.toByteArray());

    final int startPos = getHeaderSize(pageNum) + (count * INT_SERIALIZED_SIZE);
    pageBuffer.putInt(startPos, keyValueFreePosition);

    setCount(currentPage, count + 1);
    setValuesFreePosition(currentPage, keyValueFreePosition);

    return currentPage;
  }

  protected LookupResult compareKey(final Binary currentPageBuffer, final int startIndexArray, final Object[] convertedKeys, int mid, final int count,
      final int purpose) {

    int result = compareKey(currentPageBuffer, startIndexArray, convertedKeys, mid, count);

    if (result > 0)
      return HIGHER;
    else if (result < 0)
      return LOWER;

    if (purpose == 0) {
      // EXISTS
      currentPageBuffer.position(currentPageBuffer.getInt(startIndexArray + (mid * INT_SERIALIZED_SIZE)));
      final int keySerializedSize = getSerializedKeySize(currentPageBuffer, convertedKeys.length);

      return new LookupResult(true, false, mid, new int[] { currentPageBuffer.getInt(startIndexArray + (mid * INT_SERIALIZED_SIZE)) + keySerializedSize });
    } else if (purpose == 1) {
      // RETRIEVE
      currentPageBuffer.position(currentPageBuffer.getInt(startIndexArray + (mid * INT_SERIALIZED_SIZE)));
      final int keySerializedSize = getSerializedKeySize(currentPageBuffer, convertedKeys.length);

      return new LookupResult(true, false, mid, new int[] { currentPageBuffer.getInt(startIndexArray + (mid * INT_SERIALIZED_SIZE)) + keySerializedSize });
    }

    // TODO: SET CORRECT VALUE POSITION FOR PARTIAL KEYS
    return new LookupResult(true, false, mid, new int[] { currentPageBuffer.position() });
  }

  protected MutablePage createNewPage(final int compactedPageNumberOfSeries) {
    // NEW FILE, CREATE HEADER PAGE
    final int txPageCounter = getTotalPages();

    final MutablePage currentPage = new MutablePage(database.getPageManager(), new PageId(getFileId(), txPageCounter), pageSize);

    int pos = 0;
    currentPage.writeInt(pos, currentPage.getMaxContentSize());
    pos += INT_SERIALIZED_SIZE;

    currentPage.writeInt(pos, 0); // ENTRIES COUNT
    pos += INT_SERIALIZED_SIZE;

    currentPage.writeByte(pos, (byte) 0); // IMMUTABLE PAGE
    pos += BYTE_SERIALIZED_SIZE;

    currentPage.writeInt(pos, compactedPageNumberOfSeries); // COMPACTED PAGE NUMBER OF SERIES
    pos += INT_SERIALIZED_SIZE;

    if (txPageCounter == 0) {
      currentPage.writeInt(pos, -1); // SUB-INDEX FILE ID
      pos += INT_SERIALIZED_SIZE;

      currentPage.writeByte(pos++, (byte) binaryKeyTypes.length);
      for (int i = 0; i < binaryKeyTypes.length; ++i)
        currentPage.writeByte(pos++, binaryKeyTypes[i]);
    }

    setPageCount(txPageCounter + 1);

    return currentPage;
  }

  public List<LSMTreeIndexUnderlyingCompactedSeriesCursor> newIterators(final boolean ascendingOrder, final Object[] fromKeys) throws IOException {
    final BasePage mainPage = database.getTransaction().getPage(new PageId(file.getFileId(), 0), pageSize);
    final int mainPageCount = getCompactedPageNumberOfSeries(mainPage);

    final int totalPages = getTotalPages();

    if (mainPageCount == 0) {
      // NO PAGES. THIS SHOULD NEVER HAPPEN
      LogManager.instance().log(this, Level.WARNING, "Compacted index '%s' main page 0 has totalPages=%d", null, getName(), totalPages);
      return Collections.emptyList();
    }

    if (mainPageCount > totalPages) {
      // PAGES > TOTAL PAGES. THIS SHOULD NEVER HAPPEN
      LogManager.instance()
          .log(this, Level.WARNING, "Compacted index '%s' main page 0 has an invalid pageNumber=%d totalPages=%d", null, getName(), mainPageCount, totalPages);
      return Collections.emptyList();
    }

    final Object[] convertedFromKeys = convertKeys(fromKeys, binaryKeyTypes);

    final List<LSMTreeIndexUnderlyingCompactedSeriesCursor> iterators = new ArrayList<>();

    for (int rootPageNumber = mainPageCount - 1; rootPageNumber > 0; ) {
      final BasePage lastPage = database.getTransaction().getPage(new PageId(file.getFileId(), rootPageNumber), pageSize);

      final int rootPageCount = getCompactedPageNumberOfSeries(lastPage);

      if (rootPageCount == 0) {
        // EMPTY ROOT PAGE, GET THE PREVIOUS ONE. THIS SHOULD NEVER HAPPEN
        rootPageNumber--;
        continue;
      }

      rootPageNumber -= rootPageCount;

      final PageId pageId = new PageId(file.getFileId(), rootPageNumber);
      BasePage rootPage = database.getTransaction().getPage(pageId, pageSize);

      if (pageId.getPageNumber() > 0) {
        final int rootPageId = getCompactedPageNumberOfSeries(rootPage);
        if (rootPageId != 0) {
          // COMPACTED PAGE NUMBER IS NOT 0. THIS SHOULD NEVER HAPPEN
          LogManager.instance().log(this, Level.WARNING, "Compacted index '%s' root page %s has an invalid pageNumber=%d", null, getName(), pageId, rootPageId);
          return Collections.emptyList();
        }
      }

      LSMTreeIndexUnderlyingCompactedSeriesCursor iterator = null;

      int startingPageNumber = rootPageNumber + 1 + (ascendingOrder ? 0 : rootPageCount);
      final int lastPageNumber = rootPageNumber + 1 + (ascendingOrder ? rootPageCount : 0);

      if (fromKeys != null) {
        final Binary rootPageBuffer = new Binary(rootPage.slice());
        final LookupResult resultInRootPage = lookupInPage(rootPageNumber, rootPageCount + 1, rootPageBuffer, convertedFromKeys, 1);

        if (!resultInRootPage.outside) {
          // IT'S IN THE PAGE RANGE
          int pageInSeries = resultInRootPage.keyIndex;

          if (resultInRootPage.found) {
            if (pageInSeries >= rootPageCount)
              // LAST ITEM + FOUND = IT'S THE LAST ELEMENT OF THE LAST PAGE
              --pageInSeries;
          } else
            // NOT FOUND: GET THE PREVIOUS PAGE
            --pageInSeries;

          final int firstPageNumber = rootPageNumber + 1 + pageInSeries;

          final BasePage firstPage = database.getTransaction().getPage(new PageId(rootPage.getPageId().getFileId(), firstPageNumber), pageSize);
          final Binary firstPageBuffer = new Binary(firstPage.slice());

          final LookupResult result = lookupInPage(firstPageNumber, getCount(firstPage), firstPageBuffer, convertedFromKeys, ascendingOrder ? 2 : 3);

          int posInPage;

          if (result.outside) {
            // STARTS FROM THE BEGINNING OF THE NEXT PAGE
            startingPageNumber = firstPageNumber + 1;
            posInPage = -1;
          } else {
            startingPageNumber = firstPageNumber;
            posInPage = result.keyIndex;
            if (ascendingOrder)
              --posInPage;
            else
              ++posInPage;
          }

          iterator = new LSMTreeIndexUnderlyingCompactedSeriesCursor(this, startingPageNumber, lastPageNumber, binaryKeyTypes, ascendingOrder, posInPage);
        }

      } else
        iterator = new LSMTreeIndexUnderlyingCompactedSeriesCursor(this, startingPageNumber, lastPageNumber, binaryKeyTypes, ascendingOrder, -1);

      if (iterator != null)
        iterators.add(iterator);

      --rootPageNumber;
    }

    return iterators;
  }

  protected void searchInCompactedIndex(final Object[] originalKeys, final Object[] convertedKeys, final int limit, final Set<IndexCursorEntry> set,
      final Set<RID> removedRIDs) throws IOException {
    // JUMP TO ROOT PAGES BEFORE LOADING THE PAGE WITH THE KEY/VALUES
    final BasePage mainPage = database.getTransaction().getPage(new PageId(file.getFileId(), 0), pageSize);
    final int mainPageCount = getCompactedPageNumberOfSeries(mainPage);

    final int totalPages = getTotalPages();

    if (mainPageCount == 0) {
      // NO PAGES. THIS SHOULD NEVER HAPPEN
      LogManager.instance().log(this, Level.WARNING, "Compacted index '%s' main page 0 has totalPages=%d", null, getName(), totalPages);
      return;
    }

    if (mainPageCount > totalPages) {
      // PAGES > TOTAL PAGES. THIS SHOULD NEVER HAPPEN
      LogManager.instance()
          .log(this, Level.WARNING, "Compacted index '%s' main page 0 has an invalid pageNumber=%d totalPages=%d", null, getName(), mainPageCount, totalPages);
      return;
    }

    for (int pageNumber = mainPageCount - 1; pageNumber > 0; ) {
      final BasePage lastPage = database.getTransaction().getPage(new PageId(file.getFileId(), pageNumber), pageSize);

      final int rootPageCount = getCompactedPageNumberOfSeries(lastPage);

      if (rootPageCount == 0) {
        // EMPTY ROOT PAGE, GET THE PREVIOUS ONE. THIS SHOULD NEVER HAPPEN
        pageNumber--;
        continue;
      }

      pageNumber -= rootPageCount;

      final PageId pageId = new PageId(file.getFileId(), pageNumber);
      BasePage rootPage = database.getTransaction().getPage(pageId, pageSize);

      if (pageId.getPageNumber() > 0) {
        final int rootPageId = getCompactedPageNumberOfSeries(rootPage);
        if (rootPageId != 0) {
          // COMPACTED PAGE NUMBER IS NOT 0. THIS SHOULD NEVER HAPPEN
          LogManager.instance().log(this, Level.WARNING, "Compacted index '%s' root page %s has an invalid pageNumber=%d", null, getName(), pageId, rootPageId);
          return;
        }
      }

      final Binary rootPageBuffer = new Binary(rootPage.slice());
      final LookupResult resultInRootPage = lookupInPage(rootPage.getPageId().getPageNumber(), rootPageCount + 1, rootPageBuffer, convertedKeys, 0);

      if (!resultInRootPage.outside) {
        // IT'S IN PAGE RANGE
        int pageInSeries = resultInRootPage.keyIndex;

        if (resultInRootPage.found) {
          if (pageInSeries >= rootPageCount)
            // LAST ITEM + FOUND = IT'S THE LAST ELEMENT OF THE LAST PAGE
            --pageInSeries;
        } else
          // NOT FOUND: GET THE PREVIOUS PAGE
          --pageInSeries;

        final int pageNum = rootPage.getPageId().getPageNumber() + 1 + pageInSeries;
        final BasePage currentPage = database.getTransaction().getPage(new PageId(file.getFileId(), pageNum), pageSize);
        final Binary currentPageBuffer = new Binary(currentPage.slice());
        final int count = getCount(currentPage);

        if (!lookupInPageAndAddInResultset(currentPage, currentPageBuffer, count, originalKeys, convertedKeys, limit, set, removedRIDs))
          return;
      }

      --pageNumber;
    }
  }

  private int getCompactedPageNumberOfSeries(final BasePage currentPage) {
    return currentPage.readInt(INT_SERIALIZED_SIZE + INT_SERIALIZED_SIZE + BYTE_SERIALIZED_SIZE);
  }

  protected MutablePage setCompactedTotalPages() throws IOException {
    final MutablePage mainPage = database.getPageManager().getPage(new PageId(file.getFileId(), 0), pageSize, false, true).modify();
    mainPage.writeInt(INT_SERIALIZED_SIZE + INT_SERIALIZED_SIZE + BYTE_SERIALIZED_SIZE, getTotalPages());
    return mainPage;
  }
}
