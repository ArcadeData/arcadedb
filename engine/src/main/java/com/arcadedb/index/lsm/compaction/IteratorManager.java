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

import com.arcadedb.index.lsm.LSMTreeIndexMutable;
import com.arcadedb.index.lsm.LSMTreeIndexUnderlyingPageCursor;
import com.arcadedb.log.LogManager;

import java.io.IOException;
import java.util.logging.Level;

/**
 * Manages the lifecycle of page iterators during LSM-tree index compaction.
 * <p>
 * This class is responsible for creating, initializing, and properly closing
 * page iterators used during compaction. It handles the setup of iterator
 * arrays and ensures proper resource cleanup.
 * </p>
 *
 * @author ArcadeData Team
 * @since 25.9.1
 */
public final class IteratorManager {
  private final LSMTreeIndexMutable mutableIndex;

  /**
   * Creates a new IteratorManager for the specified mutable index.
   *
   * @param mutableIndex the mutable index to create iterators for
   */
  public IteratorManager(LSMTreeIndexMutable mutableIndex) {
    this.mutableIndex = mutableIndex;
  }

  /**
   * Context containing initialized iterators and their current keys.
   *
   * @param iterators           array of page iterators
   * @param keys                current keys from each iterator
   * @param activeIteratorCount number of iterators that have data
   */
  public record IteratorContext(
      LSMTreeIndexUnderlyingPageCursor[] iterators,
      Object[][] keys,
      int activeIteratorCount) {

    /**
     * Returns true if any iterators are active.
     *
     * @return true if there are active iterators
     */
    public boolean hasActiveIterators() {
      return activeIteratorCount > 0;
    }

    /**
     * Returns the number of total iterators (active + inactive).
     *
     * @return total iterator count
     */
    public int getTotalIteratorCount() {
      return iterators.length;
    }
  }

  /**
   * Creates and initializes iterators for the specified page range.
   * This method creates one iterator per page and advances each iterator
   * to its first element, storing the initial keys.
   *
   * @param startPageIndex the starting page index (inclusive)
   * @param pageCount      the number of pages to create iterators for
   *
   * @return IteratorContext containing initialized iterators and keys
   */
  public IteratorContext createIterators(int startPageIndex, int pageCount) throws IOException {
    final LSMTreeIndexUnderlyingPageCursor[] iterators = new LSMTreeIndexUnderlyingPageCursor[pageCount];
    final Object[][] keys = new Object[pageCount][];

    // Create iterator for each page
    try {
      for (int i = 0; i < pageCount; ++i) {
        iterators[i] = mutableIndex.newPageIterator(startPageIndex + i, -1, true);
      }
    } catch (IOException e) {
      // Clean up any created iterators
      closeIterators(new IteratorContext(iterators, keys, 0));
      throw e;
    }

    // Initialize iterators and extract first keys
    int activeIteratorCount = 0;
    for (int p = 0; p < pageCount; ++p) {
      if (iterators[p].hasNext()) {
        iterators[p].next();
        keys[p] = iterators[p].getKeys();
        activeIteratorCount++;
      } else {
        // Empty page - close iterator immediately
        iterators[p].close();
        iterators[p] = null;
        keys[p] = null;
      }
    }

    LogManager.instance().log(mutableIndex, Level.FINE,
        "Created %d iterators for pages %d-%d (%d active)",
        null, pageCount, startPageIndex, startPageIndex + pageCount - 1, activeIteratorCount);

    return new IteratorContext(iterators, keys, activeIteratorCount);
  }

  /**
   * Safely closes all iterators in the provided context.
   * This method ensures that all resources are properly released,
   * even if some iterators are already null or closed.
   *
   * @param context the iterator context to close
   */
  public void closeIterators(IteratorContext context) {
    if (context == null) {
      return;
    }

    int closedCount = 0;
    final LSMTreeIndexUnderlyingPageCursor[] iterators = context.iterators();

    for (int i = 0; i < iterators.length; i++) {
      if (iterators[i] != null) {
        try {
          iterators[i].close();
          closedCount++;
        } catch (Exception e) {
          LogManager.instance().log(mutableIndex, Level.WARNING,
              "Error closing iterator %d: %s", e, i, e.getMessage());
        } finally {
          iterators[i] = null;
        }
      }
    }

    LogManager.instance().log(mutableIndex, Level.FINE,
        "Closed %d iterators", null, closedCount);
  }

  /**
   * Counts the number of active (non-null) iterators in the context.
   *
   * @param context the iterator context to examine
   *
   * @return the number of active iterators
   */
  public int countActiveIterators(IteratorContext context) {
    if (context == null) {
      return 0;
    }

    int count = 0;
    for (LSMTreeIndexUnderlyingPageCursor iterator : context.iterators()) {
      if (iterator != null) {
        count++;
      }
    }
    return count;
  }

  /**
   * Validates that the iterator context is in a consistent state.
   * This method performs basic sanity checks on the context structure.
   *
   * @param context the context to validate
   *
   * @throws IllegalStateException if the context is invalid
   */
  public void validateContext(IteratorContext context) {
    if (context == null) {
      throw new IllegalStateException("Iterator context cannot be null");
    }

    if (context.iterators().length != context.keys().length) {
      throw new IllegalStateException(
          "Iterator and keys arrays must have the same length");
    }

    if (context.iterators().length == 0) {
      throw new IllegalStateException(
          "Iterator context cannot be empty");
    }
  }

  /**
   * Returns statistics about the iterator context for monitoring purposes.
   *
   * @param context the iterator context to analyze
   *
   * @return formatted statistics string
   */
  public String getContextStatistics(IteratorContext context) {
    if (context == null) {
      return "IteratorContext: null";
    }

    int activeCount = countActiveIterators(context);
    return String.format(
        "IteratorContext: total=%d active=%d closed=%d",
        context.getTotalIteratorCount(), activeCount,
        context.getTotalIteratorCount() - activeCount);
  }
}
