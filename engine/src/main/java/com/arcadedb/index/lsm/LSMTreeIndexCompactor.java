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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.database.TrackableBinary;
import com.arcadedb.engine.BasePage;
import com.arcadedb.engine.MutablePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.log.LogManager;
import com.arcadedb.serializer.BinaryComparator;
import com.arcadedb.serializer.BinarySerializer;
import com.arcadedb.utility.FileUtils;

import java.io.IOException;
import java.util.*;
import java.util.logging.Level;

public class LSMTreeIndexCompactor {
  public LSMTreeIndexCompactor() {
  }

  public static boolean compact(final LSMTreeIndex mainIndex) throws IOException, InterruptedException {
    final LSMTreeIndexMutable index = mainIndex.getMutableIndex();

    final DatabaseInternal database = index.getDatabase();

    final int totalPages = index.getTotalPages();
    LogManager.instance()
        .log(mainIndex, Level.INFO, "Compacting index '%s' (pages=%d pageSize=%d threadId=%d)...", null, index, totalPages, index.getPageSize(),
            Thread.currentThread().getId());

    if (totalPages < 2)
      return false;

    final long startTime = System.currentTimeMillis();

    LSMTreeIndexCompacted compactedIndex = index.getSubIndex();
    if (compactedIndex == null) {
      // CREATE A NEW INDEX
      compactedIndex = index.createNewForCompaction();
      index.getDatabase().getSchema().getEmbedded().registerFile(compactedIndex);
    }

    final byte[] keyTypes = index.getKeyTypes();

    long indexCompactionRAM = database.getConfiguration().getValueAsLong(GlobalConfiguration.INDEX_COMPACTION_RAM_MB) * 1024 * 1024;

    final long maxUsableRAM = Runtime.getRuntime().maxMemory() * 30 / 100;
    if (indexCompactionRAM > maxUsableRAM) {
      LogManager.instance()
          .log(mainIndex, Level.WARNING, "Configured RAM for compaction (%dMB) is more than 1/3 of the max heap (%s). Forcing to %s", null, indexCompactionRAM,
              FileUtils.getSizeAsString(Runtime.getRuntime().maxMemory()), maxUsableRAM);
      indexCompactionRAM = maxUsableRAM;
    }

    long iterations = 1;
    long totalKeys = 0;
    long totalValues = 0;
    long totalMergedKeys = 0;
    long totalMergedValues = 0;

    final Binary keyValueContent = new Binary();

    int pagesToCompact;
    int compactedPages = 0;

    // FIND LAST IMMUTABLE PAGE TO COMPACT
    int lastImmutablePage = totalPages - 1;
    for (int pageIndex = totalPages - 1; pageIndex > -1; --pageIndex) {
      final BasePage page = database.getPageManager().getPage(new PageId(index.getFileId(), pageIndex), index.getPageSize(), false, true);
      if (!index.isMutable(page)) {
        lastImmutablePage = pageIndex;
        break;
      }
    }

    for (int pageIndex = 0; pageIndex < lastImmutablePage; ) {

      final long totalRAMNeeded = (lastImmutablePage - pageIndex + 1L) * index.getPageSize();

      if (totalRAMNeeded > indexCompactionRAM) {
        pagesToCompact = (int) (indexCompactionRAM / index.getPageSize());
        LogManager.instance().log(mainIndex, Level.INFO, "- Creating partial index with %d pages by using %s (totalRAMNeeded=%s)", null, pagesToCompact,
            FileUtils.getSizeAsString(indexCompactionRAM), FileUtils.getSizeAsString(totalRAMNeeded));
      } else
        pagesToCompact = lastImmutablePage - pageIndex + 1;

      // CREATE ROOT PAGE
      MutablePage rootPage = compactedIndex.createNewPage(0);
      TrackableBinary rootPageBuffer = rootPage.getTrackable();
      Object[] lastPageMaxKey = null;

      int compactedPageNumberInSeries = 1;

      final LSMTreeIndexUnderlyingPageCursor[] iterators = new LSMTreeIndexUnderlyingPageCursor[pagesToCompact];
      for (int i = 0; i < pagesToCompact; ++i)
        iterators[i] = index.newPageIterator(pageIndex + i, -1, true);

      final Object[][] keys = new Object[pagesToCompact][keyTypes.length];

      for (int p = 0; p < pagesToCompact; ++p) {
        if (iterators[p].hasNext()) {
          iterators[p].next();
          keys[p] = iterators[p].getKeys();
        } else {
          iterators[p].close();
          iterators[p] = null;
          keys[p] = null;
        }
      }

      final BinarySerializer serializer = database.getSerializer();
      final BinaryComparator comparator = serializer.getComparator();

      MutablePage lastPage = null;
      TrackableBinary currentPageBuffer = null;

      final Set<RID> rids = new LinkedHashSet<>();

      boolean moreItems = true;
      for (; moreItems; ++iterations) {
        moreItems = false;

        Object[] minorKey = null;
        final List<Integer> minorKeyIndexes = new ArrayList<>();

        // FIND THE MINOR KEY
        for (int p = 0; p < pagesToCompact; ++p) {
          if (minorKey == null) {
            minorKey = keys[p];
            if (minorKey != null) {
              moreItems = true;
              minorKeyIndexes.add(p);
            }
          } else {
            if (keys[p] != null) {
              moreItems = true;
              final int cmp = LSMTreeIndexMutable.compareKeys(comparator, keyTypes, keys[p], minorKey);
              if (cmp == 0) {
                minorKeyIndexes.add(p);
                ++totalMergedKeys;
              } else if (cmp < 0) {
                minorKey = keys[p];
                if (minorKey != null) {
                  minorKeyIndexes.clear();
                  minorKeyIndexes.add(p);
                }
              }
            }
          }
        }

        rids.clear();
        for (int i = 0; i < minorKeyIndexes.size(); ++i) {
          final int idx = minorKeyIndexes.get(i);
          final LSMTreeIndexUnderlyingPageCursor iter = iterators[idx];

          // BROWSE THE SAME ITERATOR TO CHECK IF NEXT VALUES HAVE THE SAME KEY
          while (true) {
            if (iter == null)
              break;

            final Object[] value = iter.getValue();
            if (value != null) {
              // NOT DELETED
              for (int r = 0; r < value.length; ++r) {
                final RID rid = (RID) value[r];
                // ADD ALSO REMOVED RIDS. ONCE THE COMPACTING OF COMPACTED INDEXES (2nd LEVEL) IS DONE, REMOVED ENTRIES CAN BE REMOVED
                rids.add(rid);
              }

              if (!rids.isEmpty())
                totalMergedValues += rids.size();
            }

            // CHECK IF THE NEXT ELEMENT HAS THE SAME KEY
            if (iter.hasNext()) {
              iter.next();
              keys[idx] = iterators[idx].getKeys();

              if (LSMTreeIndexMutable.compareKeys(comparator, keyTypes, keys[idx], minorKey) != 0)
                break;

            } else {
              iterators[idx].close();
              iterators[idx] = null;
              keys[idx] = null;
              break;
            }
          }
        }

        if (!rids.isEmpty()) {
          final RID[] ridsArray = new RID[rids.size()];
          rids.toArray(ridsArray);

          final MutablePage newPage = compactedIndex.appendDuringCompaction(keyValueContent, lastPage, currentPageBuffer, compactedPageNumberInSeries, minorKey,
              ridsArray);

          if (newPage != lastPage) {
            ++compactedPageNumberInSeries;

            if (rootPage != null) {
              // NEW PAGE: STORE THE MIN KEY IN THE ROOT PAGE
              final int newPageNum = newPage.getPageId().getPageNumber();

              final MutablePage newRootPage = compactedIndex.appendDuringCompaction(keyValueContent, rootPage, rootPageBuffer, compactedPageNumberInSeries,
                  minorKey, new RID[] { new RID(database, 0, newPageNum) });

              LogManager.instance().log(mainIndex, Level.FINE, "- Creating a new entry in index '%s' root page %s->%d (entry in page=%d)", null, index,
                  Arrays.toString(minorKey), newPageNum, index.getCount(rootPage) - 1);

              if (newRootPage != rootPage) {
                throw new UnsupportedOperationException("Root index page overflow");
//
//                // TODO: MANAGE A LINKED LIST OF ROOT PAGES INSTEAD
//                ++compactedPageNumberInSeries;
//
//                LogManager.instance().info(mainIndex, "- End of space in root index page for index '%s' (rootEntries=%d)", compactedIndex.getName(),
//                    compactedIndex.getCount(rootPage));
//                database.getPageManager().updatePage(rootPage, true, false);
//                rootPage = null;
//                rootPageBuffer = null;
              }
            }

            currentPageBuffer = newPage.getTrackable();
            lastPage = newPage;
          }

          // UPDATE LAST PAGE'S KEY
          if (minorKey != null)
            lastPageMaxKey = minorKey;

          ++totalKeys;
          totalValues += rids.size();

          if (totalKeys % 1000000 == 0)
            LogManager.instance()
                .log(mainIndex, Level.FINE, "- Keys %d values %d - iterations %d (entriesInRootPage=%d)", null, totalKeys, totalValues, iterations,
                    compactedIndex.getCount(rootPage));
        }
      }

      if (rootPage != null && lastPageMaxKey != null) {
        // WRITE THE MAX KEY
        compactedIndex.appendDuringCompaction(keyValueContent, rootPage, rootPageBuffer, compactedPageNumberInSeries, lastPageMaxKey,
            new RID[] { new RID(database, 0, 0) });
        LogManager.instance()
            .log(mainIndex, Level.FINE, "- Creating last entry in index '%s' root page %s (entriesInRootPage=%d)", null, index, Arrays.toString(lastPageMaxKey),
                compactedIndex.getCount(rootPage));
      }

      final List<MutablePage> modifiedPages = new ArrayList<>(1);

      if (lastPage != null)
        modifiedPages.add(database.getPageManager().updatePage(lastPage, true));
      if (rootPage != null)
        modifiedPages.add(database.getPageManager().updatePage(rootPage, true));

      database.getPageManager().flushPages(modifiedPages, false);

      compactedPages += pagesToCompact;

      LogManager.instance()
          .log(mainIndex, Level.FINE, "- compacted %d pages, remaining %d pages (totalKeys=%d totalValues=%d totalMergedKeys=%d totalMergedValues=%d)", null,
              compactedPages, (lastImmutablePage - compactedPages + 1), totalKeys, totalValues, totalMergedKeys, totalMergedValues);

      pageIndex += pagesToCompact;
    }

    final String oldMutableFileName = index.getName();
    final int oldMutableFileId = index.getFileId();

    final LSMTreeIndexMutable newIndex = mainIndex.splitIndex(lastImmutablePage + 1, compactedIndex);

    LogManager.instance().log(mainIndex, Level.INFO, String.format(
        "Index '%s' compacted in %dms (keys=%d values=%d mutablePages=%d immutablePages=%d iterations=%d oldLevel0File=%s(%d) newLevel0File=%s(%d) newLevel1File=%s(%d) threadId=%d)",
        mainIndex.getName(), (System.currentTimeMillis() - startTime), totalKeys, totalValues, newIndex.getTotalPages(), compactedIndex.getTotalPages(),
        iterations, oldMutableFileName, oldMutableFileId, mainIndex.getMutableIndex().getName(), mainIndex.getMutableIndex().getFileId(),
        compactedIndex.getName(), compactedIndex.getFileId(), Thread.currentThread().getId()));

    return true;
  }
}
