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

import com.arcadedb.index.lsm.LSMTreeIndex;

import java.io.IOException;

/**
 * Main orchestrator for LSM-tree index compaction operations.
 * <p>
 * This class provides the primary entry point for index compaction, handling
 * strategy selection, configuration setup, and session management. It serves
 * as the main API for the refactored compaction system while maintaining
 * backward compatibility with the existing LSMTreeIndexCompactor interface.
 * </p>
 *
 * @author ArcadeData Team
 * @since 25.9.1
 */
public final class CompactionOrchestrator {
  private boolean            debugEnabled = false;
  private CompactionStrategy strategy;

  /**
   * Creates a new CompactionOrchestrator with default settings.
   */
  public CompactionOrchestrator() {
    this.strategy = new StandardCompactionStrategy();
  }

  /**
   * Sets the debug mode for compaction operations.
   * When enabled, detailed debug output will be generated before and after compaction.
   *
   * @param debug true to enable debug mode
   *
   * @return this orchestrator for method chaining
   */
  public CompactionOrchestrator setDebug(boolean debug) {
    this.debugEnabled = debug;
    return this;
  }

  /**
   * Sets the compaction strategy to use.
   * If not set, the StandardCompactionStrategy will be used by default.
   *
   * @param strategy the compaction strategy
   *
   * @return this orchestrator for method chaining
   */
  public CompactionOrchestrator setStrategy(CompactionStrategy strategy) {
    if (strategy == null) {
      throw new IllegalArgumentException("Compaction strategy cannot be null");
    }
    this.strategy = strategy;
    return this;
  }

  /**
   * Executes compaction on the specified LSM-tree index.
   * This is the main entry point for compaction operations, providing a clean
   * API while maintaining full backward compatibility.
   *
   * @param mainIndex the LSM-tree index to compact
   *
   * @return true if compaction was performed successfully, false if skipped or failed
   *
   * @throws IOException          if I/O errors occur during compaction
   * @throws InterruptedException if the operation is interrupted
   */
  public boolean compact(LSMTreeIndex mainIndex) throws IOException, InterruptedException {
    if (mainIndex == null) {
      throw new IllegalArgumentException("Main index cannot be null");
    }

    // Create configuration based on index and debug settings
    final CompactionConfiguration config = createConfiguration(mainIndex);

    // Create and execute compaction session
    final CompactionSession session = new CompactionSession(mainIndex, config, strategy);
    final CompactionResult result = session.execute();

    return result.success() && result.wasWorkPerformed();
  }

  /**
   * Executes compaction with a custom strategy for this operation only.
   * This method allows using a different strategy without changing the orchestrator's
   * default strategy setting.
   *
   * @param mainIndex      the LSM-tree index to compact
   * @param customStrategy the strategy to use for this compaction
   *
   * @return true if compaction was performed successfully, false if skipped or failed
   *
   * @throws IOException          if I/O errors occur during compaction
   * @throws InterruptedException if the operation is interrupted
   */
  public boolean compact(LSMTreeIndex mainIndex, CompactionStrategy customStrategy)
      throws IOException, InterruptedException {
    if (customStrategy == null) {
      throw new IllegalArgumentException("Custom strategy cannot be null");
    }

    final CompactionConfiguration config = createConfiguration(mainIndex);
    final CompactionSession session = new CompactionSession(mainIndex, config, customStrategy);
    final CompactionResult result = session.execute();

    return result.success() && result.wasWorkPerformed();
  }

  /**
   * Executes compaction and returns the detailed result.
   * This method provides access to comprehensive compaction statistics and results
   * for advanced use cases and monitoring.
   *
   * @param mainIndex the LSM-tree index to compact
   *
   * @return CompactionResult with detailed statistics and outcome
   *
   * @throws IOException          if I/O errors occur during compaction
   * @throws InterruptedException if the operation is interrupted
   */
  public CompactionResult compactWithResult(LSMTreeIndex mainIndex)
      throws IOException, InterruptedException {
    if (mainIndex == null) {
      throw new IllegalArgumentException("Main index cannot be null");
    }

    final CompactionConfiguration config = createConfiguration(mainIndex);
    final CompactionSession session = new CompactionSession(mainIndex, config, strategy);

    return session.execute();
  }

  /**
   * Validates whether compaction is possible for the specified index.
   * This method performs pre-flight checks without actually executing compaction.
   *
   * @param mainIndex the LSM-tree index to validate
   *
   * @return true if compaction can be performed
   */
  public boolean canCompact(LSMTreeIndex mainIndex) {
    if (mainIndex == null) {
      return false;
    }

    try {
      final CompactionConfiguration config = createConfiguration(mainIndex);
      final int totalPages = mainIndex.getMutableIndex().getTotalPages();

      // Check minimum requirements
      if (totalPages < strategy.getMinimumPageCount()) {
        return false;
      }

      // Check memory constraints
      final long estimatedMemory = strategy.estimateMemoryUsage(totalPages, config.getPageSize());
      return estimatedMemory <= config.getEffectiveRam() || strategy.supportsPartialCompaction();

    } catch (Exception e) {
      return false;
    }
  }

  /**
   * Returns information about the current configuration and strategy.
   *
   * @return formatted information string
   */
  public String getConfigurationInfo() {
    return String.format("CompactionOrchestrator{strategy=%s, debugEnabled=%s}",
        strategy.getStrategyName(), debugEnabled);
  }

  /**
   * Returns the currently configured strategy.
   *
   * @return the current compaction strategy
   */
  public CompactionStrategy getStrategy() {
    return strategy;
  }

  /**
   * Returns whether debug mode is enabled.
   *
   * @return true if debug mode is enabled
   */
  public boolean isDebugEnabled() {
    return debugEnabled;
  }

  /**
   * Creates a compaction configuration for the specified index.
   *
   * @param mainIndex the index to create configuration for
   *
   * @return CompactionConfiguration with appropriate settings
   */
  private CompactionConfiguration createConfiguration(LSMTreeIndex mainIndex) {
    final int pageSize = mainIndex.getMutableIndex().getPageSize();
    final var database = mainIndex.getMutableIndex().getDatabase();

    return new CompactionConfiguration(database, pageSize, debugEnabled);
  }
}
