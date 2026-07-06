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
import com.arcadedb.engine.ImmutablePage;
import com.arcadedb.engine.MutablePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.log.LogManager;
import com.arcadedb.serializer.BinaryComparator;
import com.arcadedb.serializer.BinarySerializer;
import com.arcadedb.utility.FileUtils;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

public class LSMTreeIndexCompactor {
  private boolean debug = false;

  public LSMTreeIndexCompactor setDebug(final boolean debug) {
    this.debug = debug;
    return this;
  }

  public boolean compact(final LSMTreeIndex mainIndex) throws IOException, InterruptedException {
    final LSMTreeIndexMutable mutableIndex = mainIndex.getMutableIndex();

    final DatabaseInternal database = mutableIndex.getDatabase();

    final int totalPages = mutableIndex.getTotalPages();
    LogManager.instance()
        .log(mainIndex, Level.FINE, "Compacting index '%s' (pages=%d pageSize=%d threadId=%d)...", null, mutableIndex, totalPages,
            mutableIndex.getPageSize(), Thread.currentThread().threadId());

    if (totalPages < 2)
      return false;

    final long startTime = System.currentTimeMillis();

    LSMTreeIndexCompacted compactedIndex = mutableIndex.getSubIndex();
    boolean createdCompactedIndex = false;
    if (compactedIndex == null) {
      // CREATE A NEW INDEX
      compactedIndex = mutableIndex.createNewForCompaction();
      mutableIndex.getDatabase().getSchema().getEmbedded().registerFile(compactedIndex);
      createdCompactedIndex = true;
      LogManager.instance()
          .log(mainIndex, Level.WARNING, "- Creating sub-index '%s' with fileId=%d (threadId=%d)...", null, compactedIndex,
              compactedIndex.getFileId(), Thread.currentThread().threadId());
    }

    // #4946: make a failed compaction atomic. Leaf pages are flushed eagerly during the merge with no WAL;
    // if compact() throws after some were written (root-page overflow, I/O error, interrupt), the orphans
    // sit on disk with the in-RAM page count bumped - and the NEXT successful round's
    // setCompactedTotalPages() would publish them (stale duplicates re-exposing tombstoned entries, and a
    // malformed partial series walked by positional logic). Roll the page count back on failure so the
    // orphan region is overwritten by the next round; if this call CREATED the compacted file (first
    // compaction), also drop it, or every failed first-compaction would leak a registered TEMP file.
    final int preCompactionPages = compactedIndex.getTotalPages();
    try {
      return compactInternal(mainIndex, mutableIndex, compactedIndex);
    } catch (final Throwable e) {
      // Drain in-flight flushes of the orphan pages FIRST: the flush path's updatePageCount would otherwise
      // race this rollback and re-raise the count right after it was reset.
      mutableIndex.getDatabase().getPageManager().waitAllPagesOfDatabaseAreFlushed(mutableIndex.getDatabase());
      compactedIndex.rollbackPageCountTo(preCompactionPages);
      if (createdCompactedIndex) {
        try {
          mutableIndex.getDatabase().getSchema().getEmbedded().removeFile(compactedIndex.getFileId());
          mutableIndex.getDatabase().getFileManager().dropFile(compactedIndex.getFileId());
        } catch (final Throwable cleanupError) {
          e.addSuppressed(cleanupError);
        }
      }
      throw e;
    }
  }

  private boolean compactInternal(final LSMTreeIndex mainIndex, final LSMTreeIndexMutable mutableIndex,
      final LSMTreeIndexCompacted compactedIndex) throws IOException, InterruptedException {
    final DatabaseInternal database = mutableIndex.getDatabase();
    final int totalPages = mutableIndex.getTotalPages();
    final long startTime = System.currentTimeMillis();

    final byte[] keyTypes = mutableIndex.getBinaryKeyTypes();

    long indexCompactionRAM = database.getConfiguration().getValueAsLong(GlobalConfiguration.INDEX_COMPACTION_RAM_MB) * 1024 * 1024;

    final long maxUsableRAM = Runtime.getRuntime().maxMemory() * 30 / 100;
    if (indexCompactionRAM > maxUsableRAM) {
      LogManager.instance()
          .log(mainIndex, Level.FINE, "Configured RAM for compaction (%dMB) is more than 1/3 of the max heap (%s). Forcing to %s",
              null, indexCompactionRAM, FileUtils.getSizeAsString(Runtime.getRuntime().maxMemory()), maxUsableRAM);
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

    if (debug) {
      System.out.println("BEFORE COMPACTING:");
      LSMTreeIndexDebugger.printIndex(mainIndex);
    }

    // FIND LAST IMMUTABLE PAGE TO COMPACT
    int lastImmutablePage = totalPages - 1;
    for (int pageIndex = totalPages - 1; pageIndex > -1; --pageIndex) {
      final ImmutablePage page = database.getPageManager()
          .getImmutablePage(new PageId(database, mutableIndex.getFileId(), pageIndex), mutableIndex.getPageSize(), false, false);
      if (!mutableIndex.isMutable(page)) {
        lastImmutablePage = pageIndex;
        break;
      }
    }

    LogManager.instance().log(mainIndex, Level.FINE, "- Compacting pages 0-%d (threadId=%d)", null, lastImmutablePage,
        Thread.currentThread().threadId());

    for (int pageIndex = 0; pageIndex <= lastImmutablePage; ) {
      final long totalRAMNeeded = (lastImmutablePage - pageIndex + 1L) * mutableIndex.getPageSize();

      if (totalRAMNeeded > indexCompactionRAM) {
        pagesToCompact = (int) (indexCompactionRAM / mutableIndex.getPageSize());
        if (pagesToCompact < 1)
          pagesToCompact = 1;
        LogManager.instance()
            .log(mainIndex, Level.FINE, "- Creating partial index with %d pages by using %s (totalRAMNeeded=%s, threadId=%d)",
                null, pagesToCompact, FileUtils.getSizeAsString(indexCompactionRAM), FileUtils.getSizeAsString(totalRAMNeeded),
                Thread.currentThread().threadId());
      } else
        pagesToCompact = lastImmutablePage - pageIndex + 1;

      // CREATE ROOT PAGE
      final MutablePage rootPage = compactedIndex.createNewPage(0);
      final TrackableBinary rootPageBuffer = rootPage.getTrackable();
      Object[] lastPageMaxKey = null;

      LogManager.instance()
          .log(mainIndex, Level.FINE, "- This turn compacting %d pages using root page %s v.%d (threadId=%d)", null,
              pagesToCompact, rootPage.getPageId(),
              rootPage.getVersion(), Thread.currentThread().threadId());

      final AtomicInteger compactedPageNumberInSeries = new AtomicInteger(1);

      final LSMTreeIndexUnderlyingPageCursor[] iterators = new LSMTreeIndexUnderlyingPageCursor[pagesToCompact];
      for (int i = 0; i < pagesToCompact; ++i)
        iterators[i] = mutableIndex.newPageIterator(pageIndex + i, -1, true);

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

      final List<MutablePage> allNewPages = new ArrayList<>();

      for (boolean moreItems = true; moreItems; ++iterations) {
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
          // Termination invariant: each iteration either breaks (different key, no more
          // entries, or iter == null) or strictly advances `iter` via iter.next(). The
          // loop is therefore bounded by the number of remaining entries in the page.
          while (true) {
            if (iter == null)
              break;

            final Object[] value = iter.getValue();
            if (value != null) {
              // NOT DELETED
              for (int r = 0; r < value.length; ++r) {
                final RID rid = (RID) value[r];
                // ADD ALSO REMOVED RIDS. ONCE THE COMPACTING OF COMPACTED INDEXES (2nd LEVEL) IS DONE, REMOVED ENTRIES CAN BE REMOVED
                // #4942: pages merge oldest to newest and the reader treats the LAST position as the newest entry,
                // so a duplicate (a re-added RID after its tombstone, or a repeated tombstone) must move to the end
                // or the older occurrence keeps its position and the reader resolves the wrong winner, losing the row.
                if (!rids.add(rid)) {
                  rids.remove(rid);
                  rids.add(rid);
                }
              }

              totalMergedValues += value.length;
            }

            // CHECK IF THE NEXT ELEMENT HAS THE SAME KEY
            if (iter.hasNext()) {
              final int prevPos = iter.getCurrentPositionInPage();
              iter.next();
              assert iter.getCurrentPositionInPage() != prevPos :
                  "compactor iterator must advance position on next() to terminate";
              keys[idx] = iter.getKeys();

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

          final List<MutablePage> newPages = compactedIndex.appendDuringCompaction(keyValueContent, lastPage, currentPageBuffer,
              compactedPageNumberInSeries, minorKey,
              ridsArray);

          if (!newPages.isEmpty()) {
            lastPage = newPages.getLast();
            currentPageBuffer = lastPage.getTrackable();

            for (MutablePage newPage : newPages) {
              // NEW PAGE: STORE THE MIN KEY IN THE ROOT PAGE
              final int newPageNum = newPage.getPageId().getPageNumber();

              final List<MutablePage> newRootPages = compactedIndex.appendDuringCompaction(keyValueContent, rootPage,
                  rootPageBuffer,
                  compactedPageNumberInSeries,
                  minorKey, new RID[] { new RID(0, newPageNum) });

              LogManager.instance()
                  .log(mainIndex, Level.FINE,
                      "- Creating a new entry in index '%s' root page %s->%d (entry in page=%d threadId=%d)", null, mutableIndex,
                      Arrays.toString(minorKey), newPageNum, mutableIndex.getCount(rootPage) - 1,
                      Thread.currentThread().threadId());

              if (!newRootPages.isEmpty()) {
                // TODO: MANAGE A LINKED LIST OF ROOT PAGES INSTEAD
                throw new UnsupportedOperationException("Root index page overflow");
              }

              allNewPages.addAll(newPages);
            }
          }

          // UPDATE LAST PAGE'S KEY
          if (minorKey != null)
            lastPageMaxKey = minorKey;

          ++totalKeys;
          totalValues += rids.size();

          if (totalKeys % 1_000_000 == 0)
            LogManager.instance()
                .log(mainIndex, Level.FINE, "- Keys %d values %d - iterations %d (entriesInRootPage=%d, threadId=%d)", null,
                    totalKeys, totalValues,
                    iterations, compactedIndex.getCount(rootPage), Thread.currentThread().threadId());
        }
      }

      if (rootPage != null && lastPageMaxKey != null) {
        // WRITE THE MAX KEY
        compactedIndex.appendDuringCompaction(keyValueContent, rootPage, rootPageBuffer, compactedPageNumberInSeries,
            lastPageMaxKey,
            new RID[] { new RID(0, 0) });
        LogManager.instance()
            .log(mainIndex, Level.FINE, "- Creating last entry in index '%s' root page %s (entriesInRootPage=%d, threadId=%d)",
                null, mutableIndex,
                Arrays.toString(lastPageMaxKey), compactedIndex.getCount(rootPage), Thread.currentThread().threadId());
      }

      final List<MutablePage> modifiedPages = new ArrayList<>(allNewPages);
      modifiedPages.add(database.getPageManager().updatePageVersion(rootPage, true));

      database.getPageManager().writePages(modifiedPages, false);

      compactedPages += pagesToCompact;

      LogManager.instance().log(mainIndex, Level.FINE,
          "- compacted %d pages, remaining %d pages (totalKeys=%d totalValues=%d totalMergedKeys=%d totalMergedValues=%d, threadId=%d)",
          null, compactedPages,
          lastImmutablePage - compactedPages + 1, totalKeys, totalValues, totalMergedKeys, totalMergedValues,
          Thread.currentThread().threadId());

      pageIndex += pagesToCompact;
    }

    final String oldMutableFileName = mutableIndex.getName();
    final int oldMutableFileId = mutableIndex.getFileId();

    final LSMTreeIndexMutable newIndex = mainIndex.splitIndex(lastImmutablePage + 1, compactedIndex);

    LogManager.instance().log(mainIndex, Level.FINE,
        "Index '%s' compacted in %dms (keys=%d values=%d mutablePages=%d immutablePages=%d iterations=%d oldLevel0File=%s(%d) newLevel0File=%s(%d) newLevel1File=%s(%d) threadId=%d)".formatted(
            mainIndex.getName(), System.currentTimeMillis() - startTime, totalKeys, totalValues, newIndex.getTotalPages(),
            compactedIndex.getTotalPages(),
            iterations, oldMutableFileName, oldMutableFileId, mainIndex.getMutableIndex().getName(),
            mainIndex.getMutableIndex().getFileId(),
            compactedIndex.getName(), compactedIndex.getFileId(), Thread.currentThread().threadId()));

    if (debug) {
      System.out.println("AFTER COMPACTING:");
      LSMTreeIndexDebugger.printIndex(mainIndex);
    }

    return true;
  }
}
