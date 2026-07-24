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
import com.arcadedb.database.RID;
import com.arcadedb.database.TrackableBinary;
import com.arcadedb.database.TransactionIndexContext;
import com.arcadedb.engine.BasePage;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.engine.MutablePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.index.IndexCursorEntry;
import com.arcadedb.index.IndexException;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.RidHashSet;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

import static com.arcadedb.database.Binary.BYTE_SERIALIZED_SIZE;
import static com.arcadedb.database.Binary.INT_SERIALIZED_SIZE;

/**
 * The first page (main page) contains the total pages under the fields "compactedPageNumberOfSeries". This is to avoid concurrent read/write while compaction.
 */
public class LSMTreeIndexCompacted extends LSMTreeIndexAbstract {
  public static final String UNIQUE_INDEX_EXT    = "uctidx";
  public static final String NOTUNIQUE_INDEX_EXT = "nuctidx";

  private static final int MAX_SERIES_CHECKED_ON_LOAD    = 2;
  private static final int MAX_PROBLEMS_REPORTED_ON_LOAD = 3;

  /** The load-time key-order check runs once per loaded instance, not on every schema change that reloads the index. */
  private volatile boolean keyOrderCheckedOnLoad = false;

  /**
   * Number of live {@link LSMTreeIndexUnderlyingCompactedSeriesCursor}s over this file. Series cursors load
   * their pages LAZILY (page by page as the scan advances), so unlike the mutable-page cursors - which grab
   * every page buffer eagerly at construction - they cannot survive their file being dropped. A full
   * compaction that replaces this file must therefore defer the physical drop until this count drains to
   * zero (see LSMTreeIndex's retired-file handling).
   */
  private final AtomicInteger activeCursors = new AtomicInteger();

  /**
   * Called at cloning time.
   */
  public LSMTreeIndexCompacted(final LSMTreeIndex mainIndex, final DatabaseInternal database, final String name,
      final boolean unique, final String filePath,
      final Type[] keyTypes, final byte[] binaryKeyTypes, final int pageSize) throws IOException {
    super(mainIndex, database, name, unique, filePath, unique ? UNIQUE_INDEX_EXT : NOTUNIQUE_INDEX_EXT, keyTypes, binaryKeyTypes,
        pageSize,
        LSMTreeIndexMutable.CURRENT_VERSION);
  }

  /**
   * Called at load time (1st page only).
   */
  protected LSMTreeIndexCompacted(final LSMTreeIndex mainIndex, final DatabaseInternal database, final String name,
      final boolean unique, final String filePath,
      final int id, final ComponentFile.MODE mode, final int pageSize, final int version) throws IOException {
    super(mainIndex, database, name, unique, filePath, id, mode, pageSize, version);
  }

  public Set<IndexCursorEntry> get(final Object[] keys, final int limit) {
    checkForNulls(keys);

    final Object[] convertedKeys = convertKeys(keys, binaryKeyTypes);
    if (convertedKeys == null && nullStrategy == NULL_STRATEGY.SKIP)
      return Collections.emptySet();

    try {
      final Set<IndexCursorEntry> set = new HashSet<>();

      // SEARCH IN COMPACTED INDEX
      searchInCompactedIndex(keys, convertedKeys, limit, set, new HashSet<>(), new RidHashSet());

      return set;

    } catch (final IOException e) {
      throw new DatabaseOperationException("Cannot lookup key '" + Arrays.toString(keys) + "' in index '" + componentName + "'", e);
    }
  }

  public List<MutablePage> appendDuringCompaction(final Binary keyValueContent, MutablePage currentPage,
      final TrackableBinary currentPageBuffer, final AtomicInteger compactedPageNumberOfSeries, final Object[] keys,
      final RID[] rids)
      throws IOException, InterruptedException {
    if (keys == null)
      throw new IllegalArgumentException("Keys parameter is null");

    return appendDuringCompactionConverted(keyValueContent, currentPage, currentPageBuffer, compactedPageNumberOfSeries,
        keys, convertKeysForCompaction(keys), rids);
  }

  List<MutablePage> appendDuringCompactionConverted(final Binary keyValueContent, MutablePage currentPage,
      final TrackableBinary currentPageBuffer, final AtomicInteger compactedPageNumberOfSeries, final Object[] keys,
      final Object[] convertedKeys, final RID[] rids)
      throws IOException, InterruptedException {
    final List<MutablePage> newPages = new ArrayList<>();

    TrackableBinary pageBuffer = currentPageBuffer;

    // True when this key starts on a page already holding the PREVIOUS key's values (a continuation page). If this key then
    // overflows, its first values would remain on that shared page, whose root entry is keyed by the previous key. The root
    // index is positional (one entry per leaf page) and cannot index a single page under two keys, so those values would be
    // unreachable on read. To avoid it we force an overflowing key to start on a fresh page it fully owns.
    final boolean startedOnContinuation = currentPage != null && getCount(currentPage) > 0;

    if (currentPage == null) {
      // CREATE A NEW PAGE
      currentPage = createNewPage(compactedPageNumberOfSeries.getAndIncrement());
      newPages.add(currentPage);
      pageBuffer = currentPage.getTrackable();
    }

    int count = getCount(currentPage);

    int pageNum = currentPage.getPageId().getPageNumber();

    int keyValueFreePosition = getValuesFreePosition(currentPage);
    int freeSpaceInPage = keyValueFreePosition - (getHeaderSize(pageNum) + (count * INT_SERIALIZED_SIZE) + INT_SERIALIZED_SIZE);

    RID[] values = rids;
    boolean firstIteration = true;

    // REPEAT TO WRITE ALL THE RIDS (SPLIT THEM IF THEY DON'T FIT IN THE CURRENT PAGE)
    // Termination invariant: each iteration either breaks (all values written) or
    // strictly shrinks `values` via Arrays.copyOfRange(values, writtenValues, values.length)
    // where writtenValues > 0 (when 0, writeEntryMultipleValues throws/returns 0 for a
    // single value that doesn't fit and we restart on a fresh page). The loop therefore
    // runs at most O(rids.length) times.
    do {
      int writtenValues = writeEntryMultipleValues(keyValueContent, convertedKeys, values, freeSpaceInPage,
          currentPage.getMaxContentSize() - getHeaderSize(pageNum), currentPage.getPageId());

      // Move this key to a fresh page when either: (a) no value fits in the current page, or (b) it is the first iteration, the
      // current page is a continuation page already holding the previous key's values, and this key would only partially fit
      // (split). The positional root index cannot index one leaf page under two keys, so a key must not start on a page it then
      // overflows from. Note no bytes are orphaned: this key's serialized entry currently lives only in the scratch buffer
      // (keyValueContent) and is committed to the page by putByteArray() further below - it has NOT been written to the
      // continuation page, which is therefore flushed cleanly with just the previous keys' entries.
      if (writtenValues == 0 || (firstIteration && startedOnContinuation && writtenValues < values.length)) {
        // CREATE A NEW PAGE AND FLUSH TO THE DATABASE THE CURRENT ONE (NO WAL)
        database.getPageManager().updatePageVersion(currentPage, true);
        database.getPageManager().writePages(List.of(currentPage), true);

        currentPage = createNewPage(compactedPageNumberOfSeries.getAndIncrement());

        newPages.add(currentPage);

        pageBuffer = currentPage.getTrackable();
        pageNum = currentPage.getPageId().getPageNumber();
        count = 0;
        keyValueFreePosition = currentPage.getMaxContentSize();

        // WRITE THE KEY/VALUE CONTENT ON THE NEW PAGE. writeEntryMultipleValues clears keyValueContent at the start of its own
        // loop, so this retry re-serializes the key/values from scratch into a clean buffer - the partial content from the call
        // above is discarded, not appended.
        freeSpaceInPage = keyValueFreePosition - (getHeaderSize(pageNum) + INT_SERIALIZED_SIZE);
        writtenValues = writeEntryMultipleValues(keyValueContent, convertedKeys, values, freeSpaceInPage,
            currentPage.getMaxContentSize() - getHeaderSize(pageNum), currentPage.getPageId());
      }

      firstIteration = false;

      keyValueFreePosition -= keyValueContent.size();

      // WRITE KEY/VALUE PAIR CONTENT
      pageBuffer.putByteArray(keyValueFreePosition, keyValueContent.getContent(), keyValueContent.getContentBeginOffset(),
          keyValueContent.size());

      final int startPos = getHeaderSize(pageNum) + (count * INT_SERIALIZED_SIZE);
      pageBuffer.putInt(startPos, keyValueFreePosition);

      setCount(currentPage, count + 1);
      setValuesFreePosition(currentPage, keyValueFreePosition);

      if (writtenValues < values.length) {
        // NOT ALL THE VALUES HAVE BEEN WRITTEN, SPLIT THEM
        LogManager.instance().log(this, Level.FINE,
            "Splitting key values. Total values=%d, written values=%d in page %s",
            values.length, writtenValues, currentPage.getPageId());

        final int prevLen = values.length;
        values = Arrays.copyOfRange(values, writtenValues, values.length);
        assert values.length < prevLen : "compacted writeNewPages must shrink values each iteration to terminate";

        // NO SPACE LEFT, CREATE A NEW PAGE AND FLUSH TO THE DATABASE THE CURRENT ONE (NO WAL)
        database.getPageManager().updatePageVersion(currentPage, true);
        database.getPageManager().writePages(List.of(currentPage), true);

        currentPage = createNewPage(compactedPageNumberOfSeries.getAndIncrement());

        newPages.add(currentPage);

        pageBuffer = currentPage.getTrackable();
        pageNum = currentPage.getPageId().getPageNumber();
        count = 0;
        keyValueFreePosition = currentPage.getMaxContentSize();
        freeSpaceInPage = keyValueFreePosition - (getHeaderSize(pageNum) + INT_SERIALIZED_SIZE);

      } else
        // ALL THE VALUES HAVE BEEN WRITTEN
        break;

    } while (true);

    return newPages;
  }

  int availableSpaceForEntries(final MutablePage page) {
    final int count = getCount(page);
    return getValuesFreePosition(page)
        - (getHeaderSize(page.getPageId().getPageNumber()) + count * INT_SERIALIZED_SIZE);
  }

  int requiredSpaceForEntry(final MutablePage page, final Object[] keys, final RID[] rids) {
    return requiredSpaceForEntry(new Binary(), page, keys, convertKeysForCompaction(keys), rids);
  }

  int requiredSpaceForEntry(final Binary scratch, final MutablePage page, final Object[] keys,
      final Object[] convertedKeys, final RID[] rids) {
    final int pageNumber = page.getPageId().getPageNumber();
    final int pageUsableSpace = page.getMaxContentSize() - getHeaderSize(pageNumber);
    final int written = writeEntryMultipleValues(scratch, convertedKeys, rids, pageUsableSpace, pageUsableSpace,
        page.getPageId());
    if (written != rids.length)
      throw new IndexException("Key/value group for " + Arrays.toString(keys) + " does not fit in one compacted page");
    return INT_SERIALIZED_SIZE + scratch.size();
  }

  boolean canAppendWholeEntry(final MutablePage page, final Object[] keys, final RID[] rids) {
    return requiredSpaceForEntry(page, keys, rids) <= availableSpaceForEntries(page);
  }

  int valuesFittingInEmptyLeaf(final MutablePage referencePage, final Object[] keys, final RID[] rids) {
    return valuesFittingInEmptyLeaf(new Binary(), referencePage, keys, convertKeysForCompaction(keys), rids);
  }

  int valuesFittingInEmptyLeaf(final Binary scratch, final MutablePage referencePage, final Object[] keys,
      final Object[] convertedKeys, final RID[] rids) {
    final int pageUsableSpace = referencePage.getMaxContentSize() - getHeaderSize(1);
    final int written = writeEntryMultipleValues(scratch, convertedKeys, rids,
        pageUsableSpace - INT_SERIALIZED_SIZE, pageUsableSpace, referencePage.getPageId());
    if (written < 1)
      throw new IndexException(
          "Key/value group for " + Arrays.toString(keys) + " does not fit in an empty compacted leaf page");
    return written;
  }

  int requiredSpaceForSerializedEntry(final Binary serializedEntry) {
    return INT_SERIALIZED_SIZE + serializedEntry.size();
  }

  Object[] convertKeysForCompaction(final Object[] keys) {
    return convertKeys(keys, binaryKeyTypes);
  }

  protected LookupResult compareKey(final Binary currentPageBuffer, final int startIndexArray, final Object[] convertedKeys,
      final int mid, final int count,
      final int purpose) {

    final int result = compareKey(currentPageBuffer, startIndexArray, convertedKeys, mid, count);

    if (result > 0)
      return HIGHER;
    else if (result < 0)
      return LOWER;

    if (purpose == 0) {
      // EXISTS
      currentPageBuffer.position(currentPageBuffer.getInt(startIndexArray + (mid * INT_SERIALIZED_SIZE)));
      final int keySerializedSize = getSerializedKeySize(currentPageBuffer, convertedKeys.length);

      return new LookupResult(true, false, mid,
          new int[] { currentPageBuffer.getInt(startIndexArray + (mid * INT_SERIALIZED_SIZE)) + keySerializedSize });
    } else if (purpose == 1) {
      // RETRIEVE

      currentPageBuffer.position(currentPageBuffer.getInt(startIndexArray + (mid * INT_SERIALIZED_SIZE)));
      final int keySerializedSize = getSerializedKeySize(currentPageBuffer, convertedKeys.length);

      // RETRIEVE ALL THE RESULTS
      final int firstKeyPos = findFirstEntryOfSameKey(currentPageBuffer, convertedKeys, startIndexArray, mid);
      final int lastKeyPos = findLastEntryOfSameKey(count, currentPageBuffer, convertedKeys, startIndexArray, mid);

      final int[] positionsArray = new int[lastKeyPos - firstKeyPos + 1];
      for (int i = firstKeyPos; i <= lastKeyPos; ++i)
        positionsArray[i - firstKeyPos] = currentPageBuffer.getInt(startIndexArray + (i * INT_SERIALIZED_SIZE)) + keySerializedSize;

      return new LookupResult(true, false, lastKeyPos, positionsArray);
    }

    // TODO: SET CORRECT VALUE POSITION FOR PARTIAL KEYS
    return new LookupResult(true, false, mid, new int[] { currentPageBuffer.position() });
  }

  protected MutablePage createNewPage(final int compactedPageNumberOfSeries) {
    // NEW FILE, CREATE HEADER PAGE
    final int txPageCounter = getTotalPages();

    final MutablePage currentPage = new MutablePage(new PageId(database, getFileId(), txPageCounter), pageSize);

    int pos = 0;
    pos += currentPage.writeInt(pos, currentPage.getMaxContentSize());
    pos += currentPage.writeInt(pos, 0); // ENTRIES COUNT
    pos += currentPage.writeByte(pos, (byte) 0); // IMMUTABLE PAGE
    pos += currentPage.writeInt(pos, compactedPageNumberOfSeries); // COMPACTED PAGE NUMBER OF SERIES

    if (txPageCounter == 0) {
      pos += currentPage.writeInt(pos, -1); // SUB-INDEX FILE ID

      currentPage.writeByte(pos++, (byte) binaryKeyTypes.length);
      for (int i = 0; i < binaryKeyTypes.length; ++i)
        currentPage.writeByte(pos++, binaryKeyTypes[i]);
    }

    updatePageCount(txPageCounter + 1);

    return currentPage;
  }

  public List<LSMTreeIndexUnderlyingCompactedSeriesCursor> newIterators(final boolean ascendingOrder, final Object[] fromKeys,
      final Object[] toKeys)
      throws IOException {
    final BasePage mainPage = database.getTransaction().getPage(new PageId(database, file.getFileId(), 0), pageSize);
    final int mainPageCount = getCompactedPageNumberOfSeries(mainPage);

    final int totalPages = getTotalPages();

    if (mainPageCount == 0) {
      // NO PAGES. THIS SHOULD NEVER HAPPEN
      LogManager.instance()
          .log(this, Level.WARNING, "Compacted index '%s' main page 0 has totalPages=%d", null, getName(), totalPages);
      return Collections.emptyList();
    }

    final int effectivePageCount;
    if (mainPageCount > totalPages) {
      // Header page count is ahead of physical pages - this can happen after an unclean shutdown
      // where async-flushed compaction pages did not all reach disk before the header was updated.
      // Serve what is physically present rather than returning empty.
      LogManager.instance()
          .log(this, Level.WARNING,
              "Compacted index '%s' main page 0 has an invalid pageNumber=%d totalPages=%d; serving %d available pages (some entries may be missing due to an unclean shutdown)",
              null, getName(), mainPageCount, totalPages, totalPages);
      effectivePageCount = totalPages;
    } else {
      effectivePageCount = mainPageCount;
    }

    final List<LSMTreeIndexUnderlyingCompactedSeriesCursor> iterators = new ArrayList<>();

    for (int rootPageNumber = effectivePageCount - 1; rootPageNumber > 0; ) {
      final BasePage lastPage = database.getTransaction().getPage(new PageId(database, file.getFileId(), rootPageNumber), pageSize);

      final int rootPageCount = getCompactedPageNumberOfSeries(lastPage);

      if (rootPageCount == 0) {
        // EMPTY ROOT PAGE, GET THE PREVIOUS ONE. THIS SHOULD NEVER HAPPEN
        rootPageNumber--;
        continue;
      }

      rootPageNumber -= rootPageCount;

      final PageId pageId = new PageId(database, file.getFileId(), rootPageNumber);
      final BasePage rootPage = database.getTransaction().getPage(pageId, pageSize);

      if (pageId.getPageNumber() > 0) {
        final int rootPageId = getCompactedPageNumberOfSeries(rootPage);
        if (rootPageId != 0) {
          // COMPACTED PAGE NUMBER IS NOT 0. THIS SHOULD NEVER HAPPEN
          LogManager.instance()
              .log(this, Level.WARNING, "Compacted index '%s' root page %s has an invalid pageNumber=%d", null, getName(), pageId,
                  rootPageId);
          return Collections.emptyList();
        }
      }

      LSMTreeIndexUnderlyingCompactedSeriesCursor iterator = null;

      // Each series stores its root first, followed by rootPageCount data pages. DESC must start on the last data page,
      // not one page beyond it (which may be the next series' root page).
      final int startingPageNumber = rootPageNumber + (ascendingOrder ? 1 : rootPageCount);
      final int lastPageNumber = rootPageNumber + (ascendingOrder ? rootPageCount : 1);

      if (fromKeys != null) {
        final Binary rootPageBuffer = new Binary(rootPage.slice());

        // Use purpose=2 (ascending iterator) instead of 1 (retrieve) for root page lookups when
        // keys are partial (fewer components than the composite index defines). Purpose=1 rejects
        // partial keys with "key is composed of N items, while the index defined M items".
        // Purpose=2 allows partial key comparison which correctly matches by prefix.
        // For full keys, keep purpose=1 to preserve exact boundary behavior for descending ranges and the shared-leaf
        // (multi-page duplicate) positioning, both of which depend on purpose=1's value-run result.
        final int fromPurpose = fromKeys.length < binaryKeyTypes.length ? 2 : 1;
        final LookupResult resultInRootPage = lookupInPage(rootPageNumber, rootPageCount + 1, rootPageBuffer, fromKeys,
            fromPurpose);
        if (!resultInRootPage.outside)
          iterator = searchInCurrentPage(ascendingOrder, fromKeys, rootPageNumber, rootPageCount, rootPage, lastPageNumber,
              resultInRootPage);
        else if (ascendingOrder == (resultInRootPage.keyIndex == 0))
          // fromKeys is OUTSIDE this series' key range and the series sits on the scanned side of fromKeys, so the whole
          // series belongs to the result: emit a full-series cursor. keyIndex==0 means fromKeys is below the series
          // (LOWER); a non-zero keyIndex means it is above (HIGHER). Ascending keeps series above the (lower) fromKeys
          // bound; descending keeps series below the (upper) fromKeys bound. The opposite (far) bound is enforced by
          // LSMTreeIndexCursor's toKeys termination. Without this, a series lying wholly inside [fromKeys, toKeys] -
          // containing neither endpoint - produced no cursor and every interior series was silently dropped (#5214).
          iterator = new LSMTreeIndexUnderlyingCompactedSeriesCursor(this, startingPageNumber, lastPageNumber, binaryKeyTypes,
              ascendingOrder, -1);
      } else
        iterator = new LSMTreeIndexUnderlyingCompactedSeriesCursor(this, startingPageNumber, lastPageNumber, binaryKeyTypes,
            ascendingOrder, -1);

      if (iterator != null)
        iterators.add(iterator);

      --rootPageNumber;
    }

    return iterators;
  }

  private LSMTreeIndexUnderlyingCompactedSeriesCursor searchInCurrentPage(boolean ascendingOrder, Object[] convertedFromKeys,
      int rootPageNumber,
      int rootPageCount, BasePage rootPage, int lastPageNumber, LookupResult resultInRootPage) throws IOException {
    LSMTreeIndexUnderlyingCompactedSeriesCursor iterator = null;
    if (!resultInRootPage.outside) {
      // IT'S IN THE PAGE RANGE
      int pageInSeries = resultInRootPage.keyIndex;

      if (resultInRootPage.found) {
        if (ascendingOrder && !unique) {
          // Start at the first matching leaf plus its possible shared predecessor. Legacy files can contain a key that began
          // on its predecessor and then overflowed; the bounded writer can also place a complete leading RID chunk there
          // before later chunks move to matching leaves. A non-matching predecessor advances to the next page below. Unique
          // indexes are exempt: a unique key holds one value and cannot span leaves.
          final int firstMatchingRootEntry = resultInRootPage.valueBeginPositions != null
              ? resultInRootPage.keyIndex - resultInRootPage.valueBeginPositions.length + 1
              : resultInRootPage.keyIndex;
          pageInSeries = Math.max(0, firstMatchingRootEntry - 1);
        } else if (pageInSeries >= rootPageCount)
          // LAST ITEM + FOUND = IT'S THE LAST ELEMENT OF THE LAST PAGE
          --pageInSeries;
      } else
        // NOT FOUND: GET THE PREVIOUS PAGE
        --pageInSeries;

      // Clamp to valid range: pageInSeries can become -1 when the search key is before all entries
      // in this series (e.g., when using iterator purpose for partial key lookups on composite indexes).
      // In this case, start from the first data page.
      if (pageInSeries < 0)
        pageInSeries = 0;

      final int firstPageNumber = rootPageNumber + 1 + pageInSeries;

      final BasePage firstPage = database.getTransaction()
          .getPage(new PageId(database, rootPage.getPageId().getFileId(), firstPageNumber), pageSize);
      final Binary firstPageBuffer = new Binary(firstPage.slice());

      final LookupResult result = lookupInPage(firstPageNumber, getCount(firstPage), firstPageBuffer, convertedFromKeys,
          ascendingOrder ? 2 : 3);

      int posInPage;

      int startingPageNumber;
      if (result.outside) {
        // STARTS FROM THE BEGINNING OF THE NEXT PAGE
        startingPageNumber = firstPageNumber + 1;
        posInPage = -1;
      } else {
        startingPageNumber = firstPageNumber;
        posInPage = result.keyIndex;
        if (ascendingOrder) {
          // Binary search may land in the middle of a repeated-key run. This matters when a bounded write starts a
          // high-cardinality key on its predecessor leaf: starting at the middle drops the earlier RID chunks on that leaf.
          // Position immediately before the first matching entry so the series cursor emits the complete run.
          if (result.found && !unique)
            posInPage = findFirstEntryOfSameKey(firstPageBuffer, convertedFromKeys, getHeaderSize(firstPageNumber), posInPage);
          --posInPage;
        } else
          ++posInPage;
      }

      iterator = new LSMTreeIndexUnderlyingCompactedSeriesCursor(this, startingPageNumber, lastPageNumber, binaryKeyTypes,
          ascendingOrder, posInPage);
    }
    return iterator;
  }

  protected void searchInCompactedIndex(final Object[] originalKeys, final Object[] convertedKeys, final int limit,
      final Set<IndexCursorEntry> set,
      final Set<TransactionIndexContext.ComparableKey> removedKeys, final RidHashSet deletedRIDs) throws IOException {
    // JUMP TO ROOT PAGES BEFORE LOADING THE PAGE WITH THE KEY/VALUES
    final BasePage mainPage = database.getTransaction().getPage(new PageId(database, file.getFileId(), 0), pageSize);
    final int mainPageCount = getCompactedPageNumberOfSeries(mainPage);

    final int totalPages = getTotalPages();

    if (mainPageCount == 0) {
      // NO PAGES. THIS SHOULD NEVER HAPPEN
      LogManager.instance()
          .log(this, Level.WARNING, "Compacted index '%s' main page 0 has totalPages=%d", null, getName(), totalPages);
      return;
    }

    final int effectivePageCount;
    if (mainPageCount > totalPages) {
      // Header page count is ahead of physical pages - this can happen after an unclean shutdown
      // where async-flushed compaction pages did not all reach disk before the header was updated.
      // Serve what is physically present rather than returning empty.
      LogManager.instance()
          .log(this, Level.WARNING,
              "Compacted index '%s' main page 0 has an invalid pageNumber=%d totalPages=%d; serving %d available pages (some entries may be missing due to an unclean shutdown)",
              null, getName(), mainPageCount, totalPages, totalPages);
      effectivePageCount = totalPages;
    } else {
      effectivePageCount = mainPageCount;
    }

    for (int pageNumber = effectivePageCount - 1; pageNumber > 0; ) {
      final BasePage lastPage = database.getTransaction().getPage(new PageId(database, file.getFileId(), pageNumber), pageSize);

      final int rootPageCount = getCompactedPageNumberOfSeries(lastPage);

      if (rootPageCount == 0) {
        // EMPTY ROOT PAGE, GET THE PREVIOUS ONE. THIS SHOULD NEVER HAPPEN
        pageNumber--;
        continue;
      }

      pageNumber -= rootPageCount;

      final PageId pageId = new PageId(database, file.getFileId(), pageNumber);
      final BasePage rootPage = database.getTransaction().getPage(pageId, pageSize);

      if (pageId.getPageNumber() > 0) {
        final int rootPageId = getCompactedPageNumberOfSeries(rootPage);
        if (rootPageId != 0) {
          // COMPACTED PAGE NUMBER IS NOT 0. THIS SHOULD NEVER HAPPEN
          LogManager.instance()
              .log(this, Level.WARNING, "Compacted index '%s' root page %s has an invalid pageNumber=%d", null, getName(), pageId,
                  rootPageId);
          return;
        }
      }

      final Binary rootPageBuffer = new Binary(rootPage.slice());
      final LookupResult resultInRootPage = lookupInPage(rootPage.getPageId().getPageNumber(), rootPageCount + 1, rootPageBuffer,
          convertedKeys, 1);

      if (!resultInRootPage.outside) {
        // IT'S IN PAGE RANGE
        int pageInSeries = resultInRootPage.keyIndex;
        // Unique indexes are exempt: a unique key holds a single value that never overflows a page, so the shared-leaf
        // layout cannot occur and the extra preceding-leaf read below is pure overhead.
        final int firstMatchingRootEntry = !unique && resultInRootPage.found && resultInRootPage.valueBeginPositions != null
            ? resultInRootPage.keyIndex - resultInRootPage.valueBeginPositions.length + 1
            : -1;

        if (resultInRootPage.found) {
          if (pageInSeries >= rootPageCount)
            // LAST ITEM + FOUND = IT'S THE LAST ELEMENT OF THE LAST PAGE
            --pageInSeries;
        } else
          // NOT FOUND: GET THE PREVIOUS PAGE
          --pageInSeries;

        if (resultInRootPage.valueBeginPositions != null && resultInRootPage.valueBeginPositions.length > 1) {
          // Newest leaf first. A key whose values overflow one leaf is written oldest-chunk-first onto ascending pages, so
          // walking the root entries in order would visit the oldest chunk first. That inverts the deletion semantics of
          // lookupInPageAndAddInResultset, whose deletedRIDs set only suppresses RIDs encountered AFTER the tombstone: a
          // tombstone in an early chunk would then kill the live re-add sitting in a later chunk. Every other level of this
          // reader (mutable pages, series, values within a page) already walks newest to oldest for the same reason.
          final List<RID> pages = readAllValuesFromResult(rootPageBuffer, resultInRootPage);
          for (int p = pages.size() - 1; p > -1; --p) {
            final int pageNum = (int) pages.get(p).getPosition();
            if (pageNum < 1)
              continue;

            final BasePage currentPage = database.getTransaction()
                .getPage(new PageId(database, file.getFileId(), pageNum), pageSize);
            final Binary currentPageBuffer = new Binary(currentPage.slice());
            final int count = getCount(currentPage);

            if (!lookupInPageAndAddInResultset(currentPage, currentPageBuffer, count, originalKeys, convertedKeys, limit, set,
                removedKeys, deletedRIDs))
              return;
          }
        } else {
          final int pageNum = rootPage.getPageId().getPageNumber() + 1 + pageInSeries;
          final BasePage currentPage = database.getTransaction().getPage(new PageId(database, file.getFileId(), pageNum), pageSize);
          final Binary currentPageBuffer = new Binary(currentPage.slice());
          final int count = getCount(currentPage);

          if (!lookupInPageAndAddInResultset(currentPage, currentPageBuffer, count, originalKeys, convertedKeys, limit, set,
              removedKeys, deletedRIDs))
            return;
        }

        // Compacted files written before the shared-leaf writer safeguard can store the first chunk of an overflowing key on
        // the leaf that ends with the preceding key. Later chunks have root entries for the searched key, but that first chunk
        // is reachable only through the immediately preceding leaf. The result set removes any overlap.
        if (firstMatchingRootEntry > 0) {
          final int precedingPageNum = rootPage.getPageId().getPageNumber() + firstMatchingRootEntry;
          final BasePage precedingPage = database.getTransaction()
              .getPage(new PageId(database, file.getFileId(), precedingPageNum), pageSize);
          final Binary precedingPageBuffer = new Binary(precedingPage.slice());
          final int precedingCount = getCount(precedingPage);

          if (!lookupInPageAndAddInResultset(precedingPage, precedingPageBuffer, precedingCount, originalKeys, convertedKeys,
              limit, set, removedKeys, deletedRIDs))
            return;
        }
      }

      --pageNumber;
    }
  }

  private int getCompactedPageNumberOfSeries(final BasePage currentPage) {
    return currentPage.readInt(INT_SERIALIZED_SIZE + INT_SERIALIZED_SIZE + BYTE_SERIALIZED_SIZE);
  }

  /**
   * Runs {@link #checkRootPagesKeyOrder(int, int)} once per loaded instance and reports the outcome as a WARNING, so an
   * index left physically mis-ordered by an older build is reported at startup instead of silently under-returning on
   * every lookup. Never propagates: a failure to run the check must not stop the database from opening.
   */
  void checkKeyOrderOnLoad() {
    if (keyOrderCheckedOnLoad)
      return;
    keyOrderCheckedOnLoad = true;

    try {
      final List<String> problems = checkRootPagesKeyOrder(MAX_SERIES_CHECKED_ON_LOAD, MAX_PROBLEMS_REPORTED_ON_LOAD);
      if (!problems.isEmpty())
        LogManager.instance().log(this, Level.WARNING,
            "Index '%s' is physically ordered differently than the current key comparator, so lookups can return fewer (or foreign) records than a scan: run 'REBUILD INDEX %s'. Details: %s",
            null, getName(), getName(), problems);
    } catch (final Exception e) {
      LogManager.instance()
          .log(this, Level.FINE, "Error on checking the key order of index '%s' at load time (%s)", null, getName(),
              e.getMessage());
    }
  }

  /**
   * Bounded variant of {@link #checkKeyOrder(int)} for the open path: it only reads the root page of the most recent
   * compacted series (one entry per leaf page, so a handful of page reads per index) and checks their order. An index
   * physically written under a different key order - the state an upgrade past #5321 leaves behind for keys with
   * multi-byte UTF-8 characters - carries the very same disorder in its root pages, so this catches it at startup and
   * points at a rebuild, instead of the divergence surfacing much later as a lookup that silently returns fewer rows
   * than a scan.
   *
   * @param maxSeries   maximum number of series to inspect, starting from the most recent one
   * @param maxProblems maximum number of problems to describe
   */
  public List<String> checkRootPagesKeyOrder(final int maxSeries, final int maxProblems) throws IOException {
    final List<String> problems = new ArrayList<>();

    final int totalPages = getTotalPages();
    if (totalPages < 2)
      return problems;

    final BasePage mainPage = database.getPageManager()
        .getImmutablePage(new PageId(database, file.getFileId(), 0), pageSize, false, true);

    final int effectivePageCount = Math.min(getCompactedPageNumberOfSeries(mainPage), totalPages);

    int series = 0;
    for (int pageNumber = effectivePageCount - 1; pageNumber > 0 && series < maxSeries && problems.size() < maxProblems; ) {
      final BasePage lastPage = database.getPageManager()
          .getImmutablePage(new PageId(database, file.getFileId(), pageNumber), pageSize, false, true);

      final int rootPageCount = getCompactedPageNumberOfSeries(lastPage);
      if (rootPageCount == 0 || rootPageCount > pageNumber) {
        // EMPTY OR INVALID SERIES MARKER: LEAVE IT TO THE FULL CHECK, JUST MOVE TO THE PREVIOUS PAGE
        --pageNumber;
        continue;
      }

      pageNumber -= rootPageCount;

      final BasePage rootPage = database.getPageManager()
          .getImmutablePage(new PageId(database, file.getFileId(), pageNumber), pageSize, false, true);

      checkKeyOrderInPage(rootPage, getCount(rootPage), problems, maxProblems);

      ++series;
      --pageNumber;
    }

    return problems;
  }

  void onCursorOpened() {
    activeCursors.incrementAndGet();
  }

  void onCursorClosed() {
    activeCursors.decrementAndGet();
  }

  /** Live series cursors over this file; the file cannot be physically dropped while > 0. */
  public int getActiveCursors() {
    return activeCursors.get();
  }

  /**
   * Number of compacted series currently published (page 0's counter), walking the same root-page chain the
   * readers walk. Grows by at least one per incremental compaction round - more when the round is RAM-bound -
   * and collapses back to one after a full compaction.
   */
  public int getSeriesCount() {
    final int totalPages = getTotalPages();
    if (totalPages < 1)
      return 0;

    try {
      final BasePage mainPage = database.getPageManager()
          .getImmutablePage(new PageId(database, file.getFileId(), 0), pageSize, false, true);
      final int effectivePageCount = Math.min(getCompactedPageNumberOfSeries(mainPage), totalPages);

      int series = 0;
      for (int pageNumber = effectivePageCount - 1; pageNumber > 0; ) {
        final BasePage lastPage = database.getPageManager()
            .getImmutablePage(new PageId(database, file.getFileId(), pageNumber), pageSize, false, true);

        final int rootPageCount = getCompactedPageNumberOfSeries(lastPage);
        if (rootPageCount == 0 || rootPageCount > pageNumber) {
          // EMPTY OR INVALID SERIES MARKER: SKIP THE PAGE (same tolerance as newIterators)
          --pageNumber;
          continue;
        }

        pageNumber -= rootPageCount;
        ++series;
        --pageNumber;
      }
      return series;
    } catch (final IOException e) {
      throw new IndexException("Error on counting the series of compacted index '" + getName() + "'", e);
    }
  }

  /**
   * Readers only see the pages page 0's series counter has published ({@link #newIterators} clamps to it):
   * an in-flight compaction flushes its series pages BEFORE bumping the counter, and a failed round leaves
   * orphans beyond it (#4946). The full key-order walk must apply the same clamp, or it inspects pages no
   * reader would ever touch - half-written or orphaned - and reports a healthy index as corrupt.
   */
  @Override
  protected int getCheckablePages() {
    final int totalPages = getTotalPages();
    if (totalPages < 1)
      return totalPages;

    try {
      final BasePage mainPage = database.getPageManager()
          .getImmutablePage(new PageId(database, file.getFileId(), 0), pageSize, false, true);
      return Math.min(getCompactedPageNumberOfSeries(mainPage), totalPages);
    } catch (final IOException e) {
      // let the walk itself surface the unreadable page 0
      return totalPages;
    }
  }

  /**
   * #4946: rolls the in-RAM page count back to the pre-compaction value after a failed compaction round.
   * The orphaned leaf pages flushed by the failed round stay on disk but become unreachable: page 0's series
   * counter never included them, and with the count rolled back {@link #setCompactedTotalPages} of a LATER
   * successful round no longer publishes them (they are overwritten by that round's own pages instead).
   */
  void rollbackPageCountTo(final int pages) {
    pageCount.set(pages);
  }

  protected MutablePage setCompactedTotalPages() throws IOException {
    final MutablePage mainPage = database.getPageManager()
        .getMutablePage(new PageId(database, file.getFileId(), 0), pageSize, false, true);
    mainPage.writeInt(INT_SERIALIZED_SIZE + INT_SERIALIZED_SIZE + BYTE_SERIALIZED_SIZE, getTotalPages());
    return mainPage;
  }
}
