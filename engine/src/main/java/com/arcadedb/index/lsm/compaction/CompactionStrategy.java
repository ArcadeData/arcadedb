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

/**
 * Strategy interface for LSM-tree index compaction operations.
 * <p>
 * This interface defines the contract for different compaction strategies,
 * allowing for pluggable algorithms and optimization approaches. Different
 * strategies can optimize for memory usage, speed, or specific workload patterns.
 * </p>
 *
 * @author ArcadeData Team
 * @since 25.9.1
 */
public interface CompactionStrategy {

  /**
   * Executes the compaction strategy.
   *
   * @param context the compaction context containing all necessary state
   *
   * @return CompactionResult indicating success/failure and final statistics
   */
  CompactionResult executeCompaction(CompactionContext context);

  /**
   * Returns the name of this compaction strategy.
   *
   * @return strategy name for logging and identification
   */
  String getStrategyName();

  /**
   * Returns whether this strategy supports partial compaction when memory is limited.
   *
   * @return true if partial compaction is supported
   */
  boolean supportsPartialCompaction();

  /**
   * Returns the recommended minimum number of pages needed for this strategy to be effective.
   *
   * @return minimum page count, or 0 if no minimum
   */
  int getMinimumPageCount();

  /**
   * Estimates the memory requirements for compacting the specified number of pages.
   *
   * @param pageCount number of pages to compact
   * @param pageSize  size of each page in bytes
   *
   * @return estimated memory usage in bytes
   */
  long estimateMemoryUsage(int pageCount, int pageSize);
}
