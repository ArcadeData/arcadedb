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

import com.arcadedb.database.RID;
import com.arcadedb.database.TrackableBinary;
import com.arcadedb.engine.MutablePage;
import com.arcadedb.log.LogManager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;

/**
 * Processes page ranges during LSM-tree index compaction.
 * <p>
 * This class handles the main compaction loop for a range of pages, coordinating
 * the key merging, iterator management, and page creation. It's responsible for
 * the core compaction algorithm that merges sorted pages into a compact form.
 * </p>
 *
 * @author ArcadeData Team
 * @since 25.9.1
 */
public final class PageRangeProcessor {
  private final CompactionContext context;
  private final IteratorManager   iteratorManager;
  private final KeyMergeProcessor keyMergeProcessor;

  /**
   * Creates a new PageRangeProcessor with the specified context and processors.
   *
   * @param context           the compaction context
   * @param iteratorManager   the iterator manager
   * @param keyMergeProcessor the key merge processor
   */
  public PageRangeProcessor(CompactionContext context,
      IteratorManager iteratorManager,
      KeyMergeProcessor keyMergeProcessor) {
    this.context = context;
    this.iteratorManager = iteratorManager;
    this.keyMergeProcessor = keyMergeProcessor;
  }

  /**
   * Result of processing a page range.
   *
   * @param success        whether the processing completed successfully
   * @param rootPage       the root page created during processing
   * @param rootPageBuffer the root page buffer
   * @param processedPages the number of pages that were processed
   */
  public record ProcessingResult(
      boolean success,
      MutablePage rootPage,
      TrackableBinary rootPageBuffer,
      int processedPages) {

    /**
     * Creates a successful processing result.
     *
     * @param rootPage       the root page
     * @param rootPageBuffer the root page buffer
     * @param processedPages the number of processed pages
     *
     * @return a successful ProcessingResult
     */
    public static ProcessingResult success(MutablePage rootPage,
        TrackableBinary rootPageBuffer,
        int processedPages) {
      return new ProcessingResult(true, rootPage, rootPageBuffer, processedPages);
    }

    /**
     * Creates a failed processing result.
     *
     * @return a failed ProcessingResult
     */
    public static ProcessingResult failure() {
      return new ProcessingResult(false, null, null, 0);
    }
  }

  /**
   * Processes a range of pages for compaction.
   * This is the main entry point for compacting a subset of pages, handling
   * the complete merge operation from iterator creation to page writing.
   *
   * @param startPageIndex the starting page index
   * @param pageCount      the number of pages to process
   *
   * @return ProcessingResult containing the outcome and created pages
   */
  public ProcessingResult processPageRange(int startPageIndex, int pageCount) {
    LogManager.instance().log(context.getMainIndex(), Level.WARNING,
        "- Processing page range %d-%d (%d pages, threadId=%d)",
        null, startPageIndex, startPageIndex + pageCount - 1, pageCount, context.getThreadId());

    // Create root page for this range
    final MutablePage rootPage = context.getCompactedIndex().createNewPage(0);
    final TrackableBinary rootPageBuffer = rootPage.getTrackable();

    LogManager.instance().log(context.getMainIndex(), Level.WARNING,
        "- Created root page %s v.%d for range (threadId=%d)",
        null, rootPage.getPageId(), rootPage.getVersion(), context.getThreadId());

    // Initialize iterators for this page range
    final IteratorManager.IteratorContext iteratorContext;
    try {
      iteratorContext = iteratorManager.createIterators(startPageIndex, pageCount);
    } catch (IOException e) {
      LogManager.instance().log(context.getMainIndex(), Level.SEVERE,
          "Failed to create iterators for page range: " + e.getMessage(), e);
      return ProcessingResult.failure();
    }

    try {
      iteratorManager.validateContext(iteratorContext);

      // Process all keys in this range
      final boolean success = processAllKeys(iteratorContext, rootPage, rootPageBuffer);

      if (!success) {
        return ProcessingResult.failure();
      }

      // Finalize root page with max key
      finalizeRootPage(rootPage, rootPageBuffer);

      return ProcessingResult.success(rootPage, rootPageBuffer, pageCount);

    } catch (Exception e) {
      LogManager.instance().log(context.getMainIndex(), Level.SEVERE,
          "Error processing page range %d-%d: %s", e, startPageIndex,
          startPageIndex + pageCount - 1, e.getMessage());
      return ProcessingResult.failure();
    } finally {
      // Always clean up iterators
      iteratorManager.closeIterators(iteratorContext);
    }
  }

  /**
   * Processes all keys from the iterator context, performing the main merge loop.
   *
   * @param iteratorContext the iterator context with active iterators
   * @param rootPage        the root page for this range
   * @param rootPageBuffer  the root page buffer
   *
   * @return true if processing was successful
   */
  private boolean processAllKeys(IteratorManager.IteratorContext iteratorContext,
      MutablePage rootPage,
      TrackableBinary rootPageBuffer) {
    final var iterators = iteratorContext.iterators();
    final var keys = iteratorContext.keys();

    // Main merge loop
    while (true) {
      context.getMetrics().incrementIterations();

      // Find the minimum key across all iterators
      final var minorKeyResult = keyMergeProcessor.findMinorKey(keys);

      if (!minorKeyResult.hasMoreItems()) {
        // No more items to process
        break;
      }

      if (!minorKeyResult.hasMinorKey()) {
        // Inconsistent state - should not happen
        LogManager.instance().log(context.getMainIndex(), Level.WARNING,
            "Inconsistent state: hasMoreItems=true but no minor key found", (Throwable) null);
        continue;
      }

      // Merge all values for this key
      final var mergeResult = keyMergeProcessor.mergeKey(
          minorKeyResult.minorKey(),
          minorKeyResult.iteratorIndexes(),
          iterators,
          keys,
          context.getMetrics());

      if (!mergeResult.isEmpty()) {
        // Write the merged key-value pair
        final boolean written = writeKeyValuePair(minorKeyResult.minorKey(), mergeResult.rids(),
            rootPage, rootPageBuffer);
        if (!written) {
          LogManager.instance().log(context.getMainIndex(), Level.SEVERE,
              "Failed to write key-value pair during compaction", (Throwable) null);
          return false;
        }

        // Update statistics
        context.getMetrics().incrementTotalKeys();
        context.getMetrics().addTotalValues(mergeResult.rids().length);

        // Update last page max key
        context.setLastPageMaxKey(minorKeyResult.minorKey());

        // Log progress if needed
        if (context.getMetrics().shouldLogProgress()) {
          LogManager.instance().log(context.getMainIndex(), Level.WARNING,
              "- Progress: keys=%d values=%d iterations=%d (rootEntries=%d, threadId=%d)",
              null, context.getMetrics().getTotalKeys(), context.getMetrics().getTotalValues(),
              context.getMetrics().getIterations(),
              context.getCompactedIndex().getCount(rootPage), context.getThreadId());
        }
      }
    }

    return true;
  }

  /**
   * Writes a key-value pair to the compacted index, handling page creation and root updates.
   *
   * @param key            the key to write
   * @param rids           the RIDs associated with the key
   * @param rootPage       the root page for this range
   * @param rootPageBuffer the root page buffer
   *
   * @return true if the write was successful
   */
  private boolean writeKeyValuePair(Object[] key, RID[] rids,
      MutablePage rootPage, TrackableBinary rootPageBuffer) {
    try {
      final MutablePage newPage = context.getCompactedIndex().appendDuringCompaction(
          context.getKeyValueContent(),
          context.getLastPage(),
          context.getCurrentPageBuffer(),
          context.getCompactedPageNumberInSeries(),
          key,
          rids);

      // Check if a new page was created
      if (newPage != context.getLastPage()) {
        // Update context state first (increment series number and update page references)
        context.incrementCompactedPageNumberInSeries();
        context.setCurrentPageBuffer(newPage.getTrackable());
        context.setLastPage(newPage);

        // Update root page with new page entry
        final int newPageNum = newPage.getPageId().getPageNumber();
        final MutablePage newRootPage = context.getCompactedIndex().appendDuringCompaction(
            context.getKeyValueContent(),
            rootPage,
            rootPageBuffer,
            context.getCompactedPageNumberInSeries(),
            key,
            new RID[] { new RID(context.getDatabase(), 0, newPageNum) });

        LogManager.instance().log(context.getMainIndex(), Level.WARNING,
            "- Created new entry in root page %s->%d (entry=%d, threadId=%d)",
            null, Arrays.toString(key), newPageNum,
            context.getCompactedIndex().getCount(rootPage) - 1, context.getThreadId());

        if (newRootPage != rootPage) {
          // Root page overflow - this is not currently supported
          LogManager.instance().log(context.getMainIndex(), Level.SEVERE,
              "Root index page overflow - operation not supported", (Throwable) null);
          return false;
        }
      }

      return true;
    } catch (Exception e) {
      LogManager.instance().log(context.getMainIndex(), Level.SEVERE,
          "Error writing key-value pair: %s", (Throwable) e, e.getMessage());
      return false;
    }
  }

  /**
   * Finalizes the root page by writing the maximum key entry.
   *
   * @param rootPage       the root page to finalize
   * @param rootPageBuffer the root page buffer
   */
  private void finalizeRootPage(MutablePage rootPage, TrackableBinary rootPageBuffer) {
    final Object[] lastPageMaxKey = context.getLastPageMaxKey();
    if (rootPage != null && lastPageMaxKey != null) {
      try {
        // Write the max key entry
        context.getCompactedIndex().appendDuringCompaction(
            context.getKeyValueContent(),
            rootPage,
            rootPageBuffer,
            context.getCompactedPageNumberInSeries(),
            lastPageMaxKey,
            new RID[] { new RID(context.getDatabase(), 0, 0) });

        LogManager.instance().log(context.getMainIndex(), Level.WARNING,
            "- Finalized root page with max key %s (entries=%d, threadId=%d)",
            null, Arrays.toString(lastPageMaxKey),
            context.getCompactedIndex().getCount(rootPage), context.getThreadId());
      } catch (Exception e) {
        LogManager.instance().log(context.getMainIndex(), Level.WARNING,
            "Error finalizing root page: %s", e, e.getMessage());
      }
    }
  }

  /**
   * Updates page version and prepares for writing.
   *
   * @param rootPage the root page to update
   *
   * @return list of pages ready for writing
   */
  public List<MutablePage> prepareForWrite(MutablePage rootPage) {
    final List<MutablePage> modifiedPages = new ArrayList<>();

    try {
      if (context.getLastPage() != null) {
        modifiedPages.add(
            context.getDatabase().getPageManager()
                .updatePageVersion(context.getLastPage(), true));
      }

      if (rootPage != null) {
        modifiedPages.add(
            context.getDatabase().getPageManager()
                .updatePageVersion(rootPage, true));
      }
    } catch (Exception e) {
      LogManager.instance().log(context.getMainIndex(), Level.WARNING,
          "Error preparing pages for write: %s", e, e.getMessage());
    }

    return modifiedPages;
  }
}
