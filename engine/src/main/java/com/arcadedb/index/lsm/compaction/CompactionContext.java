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

import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.TrackableBinary;
import com.arcadedb.engine.MutablePage;
import com.arcadedb.index.lsm.LSMTreeIndex;
import com.arcadedb.index.lsm.LSMTreeIndexCompacted;
import com.arcadedb.index.lsm.LSMTreeIndexMutable;
import com.arcadedb.serializer.BinaryComparator;
import com.arcadedb.serializer.BinarySerializer;

/**
 * Shared context for LSM-tree index compaction operations.
 * <p>
 * This class encapsulates all the shared state and resources needed during
 * compaction, including database references, serializers, page buffers,
 * and intermediate state tracking. It serves as a central repository for
 * compaction-related data and operations.
 * </p>
 *
 * @author ArcadeData Team
 * @since 25.9.1
 */
public final class CompactionContext {
  private final LSMTreeIndex            mainIndex;
  private final LSMTreeIndexMutable     mutableIndex;
  private final LSMTreeIndexCompacted   compactedIndex;
  private final DatabaseInternal        database;
  private final BinarySerializer        serializer;
  private final BinaryComparator        comparator;
  private final byte[]                  keyTypes;
  private final CompactionConfiguration config;
  private final CompactionMetrics       metrics;
  private final Binary                  keyValueContent;

  // Page tracking
  private MutablePage     lastPage;
  private TrackableBinary currentPageBuffer;
  private Object[]        lastPageMaxKey;
  private int             compactedPageNumberInSeries = 1;

  /**
   * Creates a new CompactionContext with the specified parameters.
   *
   * @param mainIndex      the main LSM-tree index being compacted
   * @param mutableIndex   the mutable portion of the index
   * @param compactedIndex the compacted portion of the index
   * @param config         the compaction configuration
   * @param metrics        the metrics collector
   */
  public CompactionContext(LSMTreeIndex mainIndex,
      LSMTreeIndexMutable mutableIndex,
      LSMTreeIndexCompacted compactedIndex,
      CompactionConfiguration config,
      CompactionMetrics metrics) {
    this.mainIndex = mainIndex;
    this.mutableIndex = mutableIndex;
    this.compactedIndex = compactedIndex;
    this.database = mutableIndex.getDatabase();
    this.serializer = database.getSerializer();
    this.comparator = serializer.getComparator();
    this.keyTypes = mutableIndex.getBinaryKeyTypes();
    this.config = config;
    this.metrics = metrics;
    this.keyValueContent = new Binary();
  }

  /**
   * Returns the main LSM-tree index.
   *
   * @return the main index
   */
  public LSMTreeIndex getMainIndex() {
    return mainIndex;
  }

  /**
   * Returns the mutable index.
   *
   * @return the mutable index
   */
  public LSMTreeIndexMutable getMutableIndex() {
    return mutableIndex;
  }

  /**
   * Returns the compacted index.
   *
   * @return the compacted index
   */
  public LSMTreeIndexCompacted getCompactedIndex() {
    return compactedIndex;
  }

  /**
   * Returns the database instance.
   *
   * @return the database
   */
  public DatabaseInternal getDatabase() {
    return database;
  }

  /**
   * Returns the binary serializer.
   *
   * @return the serializer
   */
  public BinarySerializer getSerializer() {
    return serializer;
  }

  /**
   * Returns the binary comparator.
   *
   * @return the comparator
   */
  public BinaryComparator getComparator() {
    return comparator;
  }

  /**
   * Returns the key types byte array.
   *
   * @return the key types
   */
  public byte[] getKeyTypes() {
    return keyTypes;
  }

  /**
   * Returns the compaction configuration.
   *
   * @return the configuration
   */
  public CompactionConfiguration getConfig() {
    return config;
  }

  /**
   * Returns the compaction metrics.
   *
   * @return the metrics
   */
  public CompactionMetrics getMetrics() {
    return metrics;
  }

  /**
   * Returns the binary buffer for key-value content.
   *
   * @return the key-value content buffer
   */
  public Binary getKeyValueContent() {
    return keyValueContent;
  }

  /**
   * Returns the current last page.
   *
   * @return the last page, or null if none
   */
  public MutablePage getLastPage() {
    return lastPage;
  }

  /**
   * Sets the current last page.
   *
   * @param lastPage the last page to set
   */
  public void setLastPage(MutablePage lastPage) {
    this.lastPage = lastPage;
  }

  /**
   * Returns the current page buffer.
   *
   * @return the current page buffer, or null if none
   */
  public TrackableBinary getCurrentPageBuffer() {
    return currentPageBuffer;
  }

  /**
   * Sets the current page buffer.
   *
   * @param currentPageBuffer the page buffer to set
   */
  public void setCurrentPageBuffer(TrackableBinary currentPageBuffer) {
    this.currentPageBuffer = currentPageBuffer;
  }

  /**
   * Returns the maximum key from the last page.
   *
   * @return the last page maximum key, or null if none
   */
  public Object[] getLastPageMaxKey() {
    return lastPageMaxKey;
  }

  /**
   * Sets the maximum key from the last page.
   *
   * @param lastPageMaxKey the maximum key to set
   */
  public void setLastPageMaxKey(Object[] lastPageMaxKey) {
    this.lastPageMaxKey = lastPageMaxKey;
  }

  /**
   * Returns the current compacted page number in series.
   *
   * @return the compacted page number
   */
  public int getCompactedPageNumberInSeries() {
    return compactedPageNumberInSeries;
  }

  /**
   * Sets the compacted page number in series.
   *
   * @param compactedPageNumberInSeries the page number to set
   */
  public void setCompactedPageNumberInSeries(int compactedPageNumberInSeries) {
    this.compactedPageNumberInSeries = compactedPageNumberInSeries;
  }

  /**
   * Increments the compacted page number in series.
   */
  public void incrementCompactedPageNumberInSeries() {
    this.compactedPageNumberInSeries++;
  }

  /**
   * Updates page state when a new page is created during compaction.
   *
   * @param newPage the new page that was created
   */
  public void updatePageState(MutablePage newPage) {
    if (newPage != lastPage) {
      incrementCompactedPageNumberInSeries();
      currentPageBuffer = newPage.getTrackable();
      lastPage = newPage;
    }
  }

  /**
   * Resets the context state for a new compaction iteration.
   */
  public void resetForNewIteration() {
    lastPage = null;
    currentPageBuffer = null;
    lastPageMaxKey = null;
    compactedPageNumberInSeries = 1;
    keyValueContent.clear();
  }

  /**
   * Returns a summary of the current context state for debugging.
   *
   * @return formatted context state string
   */
  public String getStateSummary() {
    return String.format(
        "CompactionContext{mutablePages=%d, compactedPages=%d, currentSeries=%d, " +
            "hasLastPage=%s, hasBuffer=%s, lastKeySet=%s}",
        mutableIndex.getTotalPages(),
        compactedIndex.getTotalPages(),
        compactedPageNumberInSeries,
        lastPage != null,
        currentPageBuffer != null,
        lastPageMaxKey != null);
  }

  /**
   * Returns the thread ID for logging purposes.
   *
   * @return the current thread ID
   */
  public long getThreadId() {
    return Thread.currentThread().threadId();
  }
}
