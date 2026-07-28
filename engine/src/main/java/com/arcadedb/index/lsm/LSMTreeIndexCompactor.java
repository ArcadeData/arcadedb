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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.engine.ImmutablePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.serializer.BinaryComparator;
import com.arcadedb.serializer.BinarySerializer;
import com.arcadedb.utility.FileUtils;

import java.io.IOException;
import java.util.*;
import java.util.logging.Level;

public class LSMTreeIndexCompactor {
  private static final ThreadLocal<CompactionTestHook> COMPACTION_TEST_HOOK = new ThreadLocal<>();

  private boolean debug = false;

  static void setCompactionTestHook(final CompactionTestHook hook) {
    if (hook == null)
      COMPACTION_TEST_HOOK.remove();
    else
      COMPACTION_TEST_HOOK.set(hook);
  }

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

    // reclaim compacted files retired by previous full compactions once their readers are gone
    mainIndex.dropRetiredCompactedIndexes();

    LSMTreeIndexCompacted compactedIndex = mutableIndex.getSubIndex();

    if (compactedIndex != null && isFullCompactionDue(database, compactedIndex)) {
      final Boolean fullResult = compactFull(mainIndex, mutableIndex, compactedIndex, startTime);
      if (fullResult != null)
        return fullResult;
      // the full merge could not run in a single pass (RAM bound): fall back to an incremental round
    }

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

  private static boolean isFullCompactionDue(final DatabaseInternal database, final LSMTreeIndexCompacted compactedIndex) {
    final int threshold = database.getConfiguration().getValueAsInteger(GlobalConfiguration.INDEX_COMPACTION_FULL_SERIES);
    return threshold > 0 && compactedIndex.getSeriesCount() >= threshold;
  }

  /**
   * FULL compaction: merges EVERY published series of the compacted sub-index together with the mutable
   * immutable pages into a fresh series set written to a NEW compacted file, resolves each key's tombstone
   * history and drops dead entries entirely. Incremental rounds can never do this - they see only a slice of
   * the history, so a tombstone they dropped could resurrect an entry living in an older series - and
   * therefore keep tombstones forever, letting delete-heavy workloads accumulate unbounded dead-key runs
   * (every range scan skips them) and a monotonically growing series count (every cursor construction walks
   * them).
   * <p>
   * Correctness requires the WHOLE history of a key to be resolved in one pass, so the merge never splits
   * into RAM-bound turns: a tombstone resolved-and-dropped in a later turn's series would resurrect the
   * entry emitted by an earlier turn. When the single pass does not fit the compaction RAM budget the method
   * returns null and the caller falls back to an incremental round. The memory footprint is one page per
   * input cursor: series cursors stream page by page, mutable page cursors hold one page buffer each.
   * <p>
   * On success the old compacted file is retired via {@link LSMTreeIndex#retireCompactedIndex}: series
   * cursors load pages lazily, so the physical drop is deferred until no live cursor can still read it.
   *
   * @return TRUE on success, null when the merge cannot run in a single pass (caller falls back)
   */
  private Boolean compactFull(final LSMTreeIndex mainIndex, final LSMTreeIndexMutable mutableIndex,
      final LSMTreeIndexCompacted oldCompacted, final long startTime) throws IOException, InterruptedException {
    final DatabaseInternal database = mutableIndex.getDatabase();
    final int pageSize = mutableIndex.getPageSize();

    long indexCompactionRAM = database.getConfiguration().getValueAsLong(GlobalConfiguration.INDEX_COMPACTION_RAM_MB) * 1024 * 1024;
    final long maxUsableRAM = Runtime.getRuntime().maxMemory() * 30 / 100;
    if (indexCompactionRAM > maxUsableRAM)
      indexCompactionRAM = maxUsableRAM;

    // FIND LAST IMMUTABLE PAGE TO COMPACT (the pages after it stay in the new mutable file)
    final int totalPages = mutableIndex.getTotalPages();
    int lastImmutablePage = totalPages - 1;
    for (int pageIndex = totalPages - 1; pageIndex > -1; --pageIndex) {
      final ImmutablePage page = database.getPageManager()
          .getImmutablePage(new PageId(database, mutableIndex.getFileId(), pageIndex), pageSize, false, false);
      if (!mutableIndex.isMutable(page)) {
        lastImmutablePage = pageIndex;
        break;
      }
    }

    final int seriesCount = oldCompacted.getSeriesCount();
    final long ramNeeded = (seriesCount + lastImmutablePage + 1L) * pageSize;
    if (ramNeeded > indexCompactionRAM) {
      LogManager.instance().log(mainIndex, Level.INFO,
          "Full compaction of index '%s' needs %s for a single-pass merge (%d series + %d mutable pages) but only %s are configured: falling back to incremental compaction",
          null, mainIndex.getName(), FileUtils.getSizeAsString(ramNeeded), seriesCount, lastImmutablePage + 1,
          FileUtils.getSizeAsString(indexCompactionRAM));
      return null;
    }

    LogManager.instance().log(mainIndex, Level.INFO,
        "Full compaction of index '%s' (series=%d mutablePagesToCompact=%d threadId=%d)...", null, mainIndex.getName(),
        seriesCount, lastImmutablePage + 1, Thread.currentThread().threadId());

    // INPUT CURSORS ORDERED OLDEST -> NEWEST: the compacted series first (newIterators returns them newest
    // first, so reverse), then the mutable immutable pages (page 0 is the oldest). The per-key merge below
    // consumes contributors in array order and moves duplicates to the end, so the LAST occurrence of a RID
    // (or tombstone) in the merged sequence is its most recent operation.
    final List<LSMTreeIndexUnderlyingCompactedSeriesCursor> seriesCursors = oldCompacted.newIterators(true, null, null);
    Collections.reverse(seriesCursors);

    final int totalCursors = seriesCursors.size() + lastImmutablePage + 1;
    final LSMTreeIndexUnderlyingAbstractCursor[] iterators = new LSMTreeIndexUnderlyingAbstractCursor[totalCursors];
    for (int i = 0; i < seriesCursors.size(); ++i)
      iterators[i] = seriesCursors.get(i);
    for (int i = 0; i <= lastImmutablePage; ++i)
      iterators[seriesCursors.size() + i] = mutableIndex.newPageIterator(i, -1, true);

    final byte[] keyTypes = mutableIndex.getBinaryKeyTypes();
    final Object[][] keys = new Object[totalCursors][];

    long totalKeys = 0;
    long totalValues = 0;
    long droppedKeys = 0;
    long droppedValues = 0;

    final LSMTreeIndexCompacted newCompacted = mutableIndex.createNewForCompaction();
    database.getSchema().getEmbedded().registerFile(newCompacted);

    try {
      for (int p = 0; p < totalCursors; ++p) {
        if (iterators[p].hasNext()) {
          iterators[p].next();
          keys[p] = iterators[p].getKeys();
        } else {
          iterators[p].close();
          iterators[p] = null;
          keys[p] = null;
        }
      }

      final BinaryComparator comparator = database.getSerializer().getComparator();
      final LSMTreeIndexCompactedStreamWriter streamWriter = new LSMTreeIndexCompactedStreamWriter(mainIndex, newCompacted);

      streamWriter.startSeries();

      final Set<RID> rids = new LinkedHashSet<>();
      final Set<RID> live = new LinkedHashSet<>();

      for (boolean moreItems = true; moreItems; ) {
        moreItems = false;

        Object[] minorKey = null;
        final List<Integer> minorKeyIndexes = new ArrayList<>();

        for (int p = 0; p < totalCursors; ++p) {
          if (keys[p] == null)
            continue;
          moreItems = true;
          if (minorKey == null) {
            minorKey = keys[p];
            minorKeyIndexes.add(p);
          } else {
            final int cmp = LSMTreeIndexMutable.compareKeys(comparator, keyTypes, keys[p], minorKey);
            if (cmp == 0)
              minorKeyIndexes.add(p);
            else if (cmp < 0) {
              minorKey = keys[p];
              minorKeyIndexes.clear();
              minorKeyIndexes.add(p);
            }
          }
        }

        if (!moreItems)
          break;

        // MERGE THE KEY'S FULL HISTORY, OLDEST TO NEWEST (same move-to-end discipline as the
        // incremental merge, see #4942: the last occurrence of a RID is its newest operation)
        rids.clear();
        for (int i = 0; i < minorKeyIndexes.size(); ++i) {
          final int idx = minorKeyIndexes.get(i);
          LSMTreeIndexUnderlyingAbstractCursor iter = iterators[idx];

          while (iter != null) {
            final RID[] value = iter.getValue();
            if (value != null)
              for (final RID rid : value)
                if (!rids.add(rid)) {
                  rids.remove(rid);
                  rids.add(rid);
                }

            if (iter.hasNext()) {
              iter.next();
              keys[idx] = iter.getKeys();
              if (LSMTreeIndexMutable.compareKeys(comparator, keyTypes, keys[idx], minorKey) != 0)
                break;
            } else {
              iterators[idx].close();
              iterators[idx] = null;
              keys[idx] = null;
              iter = null;
            }
          }
        }

        // RESOLVE THE HISTORY: this is the whole history of the key, so tombstones can finally be
        // APPLIED and dropped instead of carried forward. Same semantics as the read path
        // (LSMTreeIndexCursor.next()): a key-wide tombstone (-1,-1) clears everything before it, a
        // per-RID tombstone (-(bucketId+2), position) kills only its target, an insert (re)adds.
        live.clear();
        for (final RID rid : rids) {
          if (rid.getBucketId() == -1 && rid.getPosition() == -1)
            live.clear();
          else if (rid.getBucketId() < 0)
            live.remove(new RID(-rid.getBucketId() - 2, rid.getPosition()));
          else {
            live.remove(rid);
            live.add(rid);
          }
        }

        if (live.isEmpty()) {
          ++droppedKeys;
          droppedValues += rids.size();
        } else {
          streamWriter.appendBounded(minorKey, live.toArray(new RID[0]),
              LSMTreeIndexCompactedStreamWriter.DEFAULT_MAX_DATA_PAGES_PER_SERIES);
          ++totalKeys;
          totalValues += live.size();
          droppedValues += rids.size() - live.size();
        }
      }

      streamWriter.finishSeries();

      final CompactionTestHook testHook = COMPACTION_TEST_HOOK.get();
      if (testHook != null)
        testHook.afterSeriesWritten(newCompacted);

      mainIndex.splitIndex(lastImmutablePage + 1, newCompacted);

      // Register the compacted-file migration so a transaction that resolved the OLD compacted file id
      // before the swap converges transparently at commit: lockFilesInOrder re-resolves a locked-but-missing
      // file through this mapping instead of failing with a ConcurrentModificationException (the same
      // mechanism splitIndex uses for the mutable file). splitIndex just saved the schema, so the in-memory
      // mapping suffices here.
      ((LocalSchema) database.getSchema()).setMigratedFileId(oldCompacted.getFileId(), newCompacted.getFileId(), false);

    } catch (final Throwable e) {
      for (final LSMTreeIndexUnderlyingAbstractCursor iterator : iterators)
        if (iterator != null)
          iterator.close();

      // mirror the incremental failure handling (#4946): drain in-flight flushes, then drop the
      // aborted new file entirely so nothing of the failed merge survives
      database.getPageManager().waitAllPagesOfDatabaseAreFlushed(database);
      try {
        database.getSchema().getEmbedded().removeFile(newCompacted.getFileId());
        database.getFileManager().dropFile(newCompacted.getFileId());
      } catch (final Throwable cleanupError) {
        e.addSuppressed(cleanupError);
      }
      throw e;
    }

    // the swap is published: no new cursor can reach the old file, but live ones may still read it
    // lazily - defer the physical drop until they are gone
    mainIndex.retireCompactedIndex(oldCompacted);

    LogManager.instance().log(mainIndex, Level.INFO,
        "Index '%s' fully compacted in %dms (keys=%d values=%d droppedKeys=%d droppedValues=%d mergedSeries=%d newFile=%s(%d) threadId=%d)".formatted(
            mainIndex.getName(), System.currentTimeMillis() - startTime, totalKeys, totalValues, droppedKeys, droppedValues,
            seriesCount, newCompacted.getName(), newCompacted.getFileId(), Thread.currentThread().threadId()));

    return true;
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

    final LSMTreeIndexCompactedStreamWriter streamWriter = new LSMTreeIndexCompactedStreamWriter(mainIndex, compactedIndex);

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

      final var rootPage = streamWriter.startSeries();

      LogManager.instance()
          .log(mainIndex, Level.FINE, "- This turn compacting %d pages using root page %s v.%d (threadId=%d)", null,
              pagesToCompact, rootPage.getPageId(),
              rootPage.getVersion(), Thread.currentThread().threadId());

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

      final Set<RID> rids = new LinkedHashSet<>();

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

          streamWriter.appendBounded(minorKey, ridsArray,
              LSMTreeIndexCompactedStreamWriter.DEFAULT_MAX_DATA_PAGES_PER_SERIES);

          ++totalKeys;
          totalValues += rids.size();

          if (totalKeys % 1_000_000 == 0)
            LogManager.instance()
                .log(mainIndex, Level.FINE, "- Keys %d values %d - iterations %d (entriesInRootPage=%d, threadId=%d)", null,
                    totalKeys, totalValues,
                    iterations, streamWriter.getRootEntryCount(), Thread.currentThread().threadId());
        }
      }

      streamWriter.finishSeries();

      final CompactionTestHook testHook = COMPACTION_TEST_HOOK.get();
      if (testHook != null)
        testHook.afterSeriesWritten(compactedIndex);

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

  @FunctionalInterface
  interface CompactionTestHook {
    void afterSeriesWritten(LSMTreeIndexCompacted compactedIndex) throws IOException;
  }
}
