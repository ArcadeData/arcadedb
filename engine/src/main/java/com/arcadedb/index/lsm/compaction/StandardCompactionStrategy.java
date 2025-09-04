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
package com.arcadedb.index.lsm.compaction;

import com.arcadedb.engine.ImmutablePage;
import com.arcadedb.engine.MutablePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.index.lsm.LSMTreeIndex;
import com.arcadedb.index.lsm.LSMTreeIndexCompacted;
import com.arcadedb.index.lsm.LSMTreeIndexMutable;
import com.arcadedb.log.LogManager;
import com.arcadedb.utility.FileUtils;

import java.util.List;
import java.util.logging.Level;

/**
 * Standard compaction strategy for LSM-tree indexes.
 * <p>
 * This is the default compaction strategy that implements the traditional
 * merge-based compaction algorithm. It processes pages in ranges, merging
 * keys from multiple pages into a more compact representation while maintaining
 * sort order and handling memory constraints through partial compaction.
 * </p>
 *
 * @author ArcadeData Team
 * @since 25.9.1
 */
public final class StandardCompactionStrategy implements CompactionStrategy {

  @Override
  public CompactionResult executeCompaction(CompactionContext context) {
    try {
      // Find the last immutable page to determine compaction scope
      final int lastImmutablePage = findLastImmutablePage(context);
      if (lastImmutablePage < 0) {
        LogManager.instance().log(context.getMainIndex(), Level.INFO,
            "No immutable pages found for compaction (threadId=%d)",
            null, context.getThreadId());
        return CompactionResult.noCompactionNeeded();
      }

      LogManager.instance().log(context.getMainIndex(), Level.WARNING,
          "- Compacting pages 0-%d using %s strategy (threadId=%d)",
          null, lastImmutablePage, getStrategyName(), context.getThreadId());

      // Initialize processors
      final IteratorManager iteratorManager = new IteratorManager(context.getMutableIndex());
      final KeyMergeProcessor keyMergeProcessor = new KeyMergeProcessor(
          context.getComparator(), context.getKeyTypes());
      final PageRangeProcessor pageRangeProcessor = new PageRangeProcessor(
          context, iteratorManager, keyMergeProcessor);

      // Process pages in ranges based on memory constraints
      int pageIndex = 0;
      while (pageIndex <= lastImmutablePage) {
        final int remainingPages = lastImmutablePage - pageIndex + 1;
        final int pagesToCompact = calculatePagesToCompact(context, remainingPages, pageIndex);

        LogManager.instance().log(context.getMainIndex(), Level.WARNING,
            "- Processing pages %d-%d (%d pages, remaining=%d, threadId=%d)",
            null, pageIndex, pageIndex + pagesToCompact - 1, pagesToCompact,
            remainingPages, context.getThreadId());

        // Reset context state for this page range to avoid cross-range contamination
        context.setLastPage(null);
        context.setCurrentPageBuffer(null);
        context.setLastPageMaxKey(null);
        context.setCompactedPageNumberInSeries(1);

        // Process this range of pages
        final PageRangeProcessor.ProcessingResult result =
            pageRangeProcessor.processPageRange(pageIndex, pagesToCompact);

        if (!result.success()) {
          LogManager.instance().log(context.getMainIndex(), Level.SEVERE,
              "Failed to process page range %d-%d", null, pageIndex,
              pageIndex + pagesToCompact - 1);
          return CompactionResult.failure(context.getMetrics());
        }

        // Write processed pages
        final List<MutablePage> modifiedPages = pageRangeProcessor.prepareForWrite(result.rootPage());
        context.getDatabase().getPageManager().writePages(modifiedPages, false);

        // Update metrics
        context.getMetrics().addCompactedPages(pagesToCompact);

        LogManager.instance().log(context.getMainIndex(), Level.WARNING,
            "- Completed range %d-%d (compacted=%d, remaining=%d, keys=%d, values=%d, threadId=%d)",
            null, pageIndex, pageIndex + pagesToCompact - 1,
            context.getMetrics().getCompactedPages(),
            lastImmutablePage - (pageIndex + pagesToCompact - 1),
            context.getMetrics().getTotalKeys(), context.getMetrics().getTotalValues(),
            context.getThreadId());

        pageIndex += pagesToCompact;
      }

      // Create successful result
      return createSuccessResult(context, lastImmutablePage);

    } catch (Exception e) {
      LogManager.instance().log(context.getMainIndex(), Level.SEVERE,
          "Compaction failed with exception: %s", e, e.getMessage());
      return CompactionResult.failure(context.getMetrics());
    }
  }

  @Override
  public String getStrategyName() {
    return "Standard";
  }

  @Override
  public boolean supportsPartialCompaction() {
    return true;
  }

  @Override
  public int getMinimumPageCount() {
    return 2;
  }

  @Override
  public long estimateMemoryUsage(int pageCount, int pageSize) {
    // Base memory for iterators and buffers
    long baseMemory = (long) pageCount * pageSize;

    // Additional overhead for merge structures and buffers
    long overhead = pageCount * 1024L; // ~1KB per iterator

    return baseMemory + overhead;
  }

  /**
   * Finds the last immutable page that can be compacted.
   *
   * @param context the compaction context
   *
   * @return the index of the last immutable page, or -1 if none found
   */
  private int findLastImmutablePage(CompactionContext context) {
    final int totalPages = context.getMutableIndex().getTotalPages();

    for (int pageIndex = totalPages - 1; pageIndex >= 0; --pageIndex) {
      try {
        final ImmutablePage page = context.getDatabase().getPageManager()
            .getImmutablePage(
                new PageId(context.getDatabase(), context.getMutableIndex().getFileId(), pageIndex),
                context.getConfig().getPageSize(), false, false);

        if (!isPageMutable(context.getMutableIndex(), page)) {
          return pageIndex;
        }
      } catch (Exception e) {
        LogManager.instance().log(context.getMainIndex(), Level.WARNING,
            "Error checking mutability of page %d: %s", e, pageIndex, e.getMessage());
      }
    }

    return -1;
  }

  /**
   * Calculates how many pages can be compacted in the current iteration.
   *
   * @param context          the compaction context
   * @param remainingPages   total number of pages remaining
   * @param currentPageIndex current starting page index
   *
   * @return number of pages to compact in this iteration
   */
  private int calculatePagesToCompact(CompactionContext context, int remainingPages, int currentPageIndex) {
    final long totalRamNeeded = context.getConfig().calculateTotalRamNeeded(remainingPages);

    if (totalRamNeeded <= context.getConfig().getEffectiveRam()) {
      // Can fit all remaining pages in memory
      return remainingPages;
    }

    // Calculate how many pages fit in available RAM
    final int maxPages = context.getConfig().calculateMaxPagesPerIteration(totalRamNeeded);

    LogManager.instance().log(context.getMainIndex(), Level.WARNING,
        "- Memory constraint: using %d pages of %d remaining (RAM needed=%s, available=%s, threadId=%d)",
        null, maxPages, remainingPages, FileUtils.getSizeAsString(totalRamNeeded),
        FileUtils.getSizeAsString(context.getConfig().getEffectiveRam()), context.getThreadId());

    return Math.min(maxPages, remainingPages);
  }

  /**
   * Helper method to check if page is mutable.
   *
   * @param mutableIndex the mutable index
   * @param page         the page to check
   *
   * @return true if page is mutable
   */
  private boolean isPageMutable(LSMTreeIndexMutable mutableIndex, ImmutablePage page) {
    return mutableIndex.isMutable(page);
  }

  /**
   * Helper method to split index.
   *
   * @param mainIndex        the main index
   * @param startingFromPage the starting page
   * @param compactedIndex   the compacted index
   *
   * @return the new mutable index
   */
  private LSMTreeIndexMutable splitIndex(LSMTreeIndex mainIndex, int startingFromPage, LSMTreeIndexCompacted compactedIndex) {
    return mainIndex.splitIndex(startingFromPage, compactedIndex);
  }

  /**
   * Creates a successful compaction result.
   *
   * @param context           the compaction context
   * @param lastImmutablePage the last immutable page processed
   *
   * @return CompactionResult with success details
   */
  private CompactionResult createSuccessResult(CompactionContext context, int lastImmutablePage) {
    final String oldMutableFileName = context.getMutableIndex().getName();
    final int oldMutableFileId = context.getMutableIndex().getFileId();

    // Split the index to create new mutable and finalize compacted
    final var newMutableIndex = splitIndex(
        context.getMainIndex(), lastImmutablePage + 1, context.getCompactedIndex());

    return CompactionResult.success(
        context.getMetrics(),
        oldMutableFileName,
        oldMutableFileId,
        newMutableIndex.getName(),
        newMutableIndex.getFileId(),
        context.getCompactedIndex().getName(),
        context.getCompactedIndex().getFileId(),
        newMutableIndex.getTotalPages(),
        context.getCompactedIndex().getTotalPages(),
        lastImmutablePage);
  }
}
