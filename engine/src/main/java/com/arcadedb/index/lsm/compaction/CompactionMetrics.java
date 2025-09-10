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

import java.util.concurrent.atomic.AtomicLong;

/**
 * Tracks statistics and metrics for LSM-tree index compaction operations.
 * <p>
 * This class provides thread-safe counters for monitoring compaction performance
 * and progress. All metrics are accumulated across the entire compaction operation.
 * </p>
 *
 * @author ArcadeData Team
 * @since 25.9.1
 */
public final class CompactionMetrics {
  private final AtomicLong iterations        = new AtomicLong(0);
  private final AtomicLong totalKeys         = new AtomicLong(0);
  private final AtomicLong totalValues       = new AtomicLong(0);
  private final AtomicLong totalMergedKeys   = new AtomicLong(0);
  private final AtomicLong totalMergedValues = new AtomicLong(0);
  private final AtomicLong compactedPages    = new AtomicLong(0);
  private final long       startTime;

  /**
   * Creates a new CompactionMetrics instance with the start time set to current system time.
   */
  public CompactionMetrics() {
    this.startTime = System.currentTimeMillis();
  }

  /**
   * Increments the iteration counter by one.
   */
  public void incrementIterations() {
    iterations.incrementAndGet();
  }

  /**
   * Increments the total keys counter by one.
   */
  public void incrementTotalKeys() {
    totalKeys.incrementAndGet();
  }

  /**
   * Adds the specified count to the total values counter.
   *
   * @param count the number of values to add
   */
  public void addTotalValues(long count) {
    totalValues.addAndGet(count);
  }

  /**
   * Increments the total merged keys counter by one.
   */
  public void incrementTotalMergedKeys() {
    totalMergedKeys.incrementAndGet();
  }

  /**
   * Adds the specified count to the total merged values counter.
   *
   * @param count the number of merged values to add
   */
  public void addTotalMergedValues(long count) {
    totalMergedValues.addAndGet(count);
  }

  /**
   * Adds the specified count to the compacted pages counter.
   *
   * @param count the number of pages that were compacted
   */
  public void addCompactedPages(long count) {
    compactedPages.addAndGet(count);
  }

  /**
   * Returns the current number of iterations performed.
   *
   * @return the iteration count
   */
  public long getIterations() {
    return iterations.get();
  }

  /**
   * Returns the total number of keys processed.
   *
   * @return the total keys count
   */
  public long getTotalKeys() {
    return totalKeys.get();
  }

  /**
   * Returns the total number of values processed.
   *
   * @return the total values count
   */
  public long getTotalValues() {
    return totalValues.get();
  }

  /**
   * Returns the total number of merged keys.
   *
   * @return the merged keys count
   */
  public long getTotalMergedKeys() {
    return totalMergedKeys.get();
  }

  /**
   * Returns the total number of merged values.
   *
   * @return the merged values count
   */
  public long getTotalMergedValues() {
    return totalMergedValues.get();
  }

  /**
   * Returns the total number of pages that have been compacted.
   *
   * @return the compacted pages count
   */
  public long getCompactedPages() {
    return compactedPages.get();
  }

  /**
   * Returns the start time of the compaction operation in milliseconds since epoch.
   *
   * @return the start time timestamp
   */
  public long getStartTime() {
    return startTime;
  }

  /**
   * Returns the elapsed time of the compaction operation in milliseconds.
   *
   * @return the elapsed time in milliseconds
   */
  public long getElapsedTime() {
    return System.currentTimeMillis() - startTime;
  }

  /**
   * Checks if progress should be logged based on the key count threshold.
   *
   * @return true if progress should be logged (every 1 million keys)
   */
  public boolean shouldLogProgress() {
    return totalKeys.get() % 1_000_000 == 0 && totalKeys.get() > 0;
  }

  /**
   * Returns a string representation of the current metrics.
   *
   * @return formatted metrics string
   */
  @Override
  public String toString() {
    return "CompactionMetrics{" +
        "iterations=" + iterations.get() +
        ", totalKeys=" + totalKeys.get() +
        ", totalValues=" + totalValues.get() +
        ", totalMergedKeys=" + totalMergedKeys.get() +
        ", totalMergedValues=" + totalMergedValues.get() +
        ", compactedPages=" + compactedPages.get() +
        ", elapsedTime=" + getElapsedTime() + "ms" +
        '}';
  }
}
