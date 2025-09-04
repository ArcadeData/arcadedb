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

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.index.lsm.LSMTreeIndex;
import com.arcadedb.index.lsm.LSMTreeIndexCompacted;
import com.arcadedb.index.lsm.LSMTreeIndexDebugger;
import com.arcadedb.index.lsm.LSMTreeIndexMutable;
import com.arcadedb.log.LogManager;

import java.io.IOException;
import java.util.logging.Level;

/**
 * Orchestrates a complete LSM-tree index compaction session.
 * <p>
 * This class manages the entire lifecycle of a compaction operation, from
 * initial validation and setup through strategy execution and cleanup.
 * It handles pre-conditions, creates the necessary context and indexes,
 * and ensures proper resource management.
 * </p>
 *
 * @author ArcadeData Team
 * @since 25.9.1
 */
public final class CompactionSession {
  private final LSMTreeIndex            mainIndex;
  private final CompactionConfiguration config;
  private final CompactionStrategy      strategy;

  /**
   * Creates a new compaction session with the specified parameters.
   *
   * @param mainIndex the main index to compact
   * @param config    the compaction configuration
   * @param strategy  the compaction strategy to use
   */
  public CompactionSession(LSMTreeIndex mainIndex,
      CompactionConfiguration config,
      CompactionStrategy strategy) {
    this.mainIndex = mainIndex;
    this.config = config;
    this.strategy = strategy;
  }

  /**
   * Executes the complete compaction session.
   * This method handles all aspects of the compaction from validation to cleanup.
   *
   * @return CompactionResult indicating success/failure and final statistics
   *
   * @throws IOException          if I/O errors occur during compaction
   * @throws InterruptedException if the operation is interrupted
   */
  public CompactionResult execute() throws IOException, InterruptedException {
    final CompactionMetrics metrics = new CompactionMetrics();

    LogManager.instance().log(mainIndex, Level.INFO,
        "Starting compaction session for index '%s' using %s strategy (threadId=%d)",
        null, mainIndex.getName(), strategy.getStrategyName(), Thread.currentThread().threadId());

    try {
      // Pre-compaction validation and debug output
      if (config.isDebugEnabled()) {
        System.out.println("BEFORE COMPACTING:");
        LSMTreeIndexDebugger.printIndex(mainIndex);
      }

      // Validate preconditions
      final ValidationResult validation = validatePreconditions();
      if (!validation.isValid()) {
        LogManager.instance().log(mainIndex, Level.WARNING,
            "Compaction skipped: " + validation.reason(), (Object[]) null);
        return CompactionResult.noCompactionNeeded();
      }

      // Log configuration
      config.logConfiguration(mainIndex, Level.INFO);

      // Prepare compacted index
      final LSMTreeIndexCompacted compactedIndex = prepareCompactedIndex();

      // Create compaction context
      final CompactionContext context = new CompactionContext(
          mainIndex, mainIndex.getMutableIndex(), compactedIndex, config, metrics);

      // Execute the compaction strategy
      final CompactionResult result = strategy.executeCompaction(context);

      // Post-compaction debug output
      if (config.isDebugEnabled()) {
        System.out.println("AFTER COMPACTING:");
        LSMTreeIndexDebugger.printIndex(mainIndex);
      }

      // Log final results
      logCompactionResults(result);

      return result;

    } catch (Exception e) {
      LogManager.instance().log(mainIndex, Level.SEVERE,
          "Compaction session failed: " + e.getMessage(), e);
      return CompactionResult.failure(metrics);
    }
  }

  /**
   * Result of precondition validation.
   */
  private static class ValidationResult {
    private final boolean valid;
    private final String  reason;

    private ValidationResult(boolean valid, String reason) {
      this.valid = valid;
      this.reason = reason;
    }

    public static ValidationResult valid() {
      return new ValidationResult(true, null);
    }

    public static ValidationResult invalid(String reason) {
      return new ValidationResult(false, reason);
    }

    public boolean isValid() {
      return valid;
    }

    public String reason() {
      return reason;
    }
  }

  /**
   * Validates that preconditions for compaction are met.
   *
   * @return ValidationResult indicating whether compaction can proceed
   */
  private ValidationResult validatePreconditions() {
    final LSMTreeIndexMutable mutableIndex = mainIndex.getMutableIndex();
    final DatabaseInternal database = mutableIndex.getDatabase();
    final int totalPages = mutableIndex.getTotalPages();

    LogManager.instance().log(mainIndex, Level.INFO,
        "Validating compaction preconditions for index '%s' (pages=%d pageSize=%d threadId=%d)",
        null, mutableIndex, totalPages, config.getPageSize(), Thread.currentThread().threadId());

    // Check minimum page count
    if (totalPages < strategy.getMinimumPageCount()) {
      return ValidationResult.invalid(
          String.format("Insufficient pages for compaction: %d < %d",
              totalPages, strategy.getMinimumPageCount()));
    }

    // Check if database allows compaction
    if (database.getPageManager().isPageFlushingSuspended(database)) {
      return ValidationResult.invalid("Page flushing is suspended (backup in progress?)");
    }

    // Estimate memory requirements
    final long estimatedMemory = strategy.estimateMemoryUsage(totalPages, config.getPageSize());
    if (estimatedMemory > config.getEffectiveRam() && !strategy.supportsPartialCompaction()) {
      return ValidationResult.invalid(
          String.format("Insufficient memory and strategy does not support partial compaction: %d MB required",
              estimatedMemory / (1024 * 1024)));
    }

    return ValidationResult.valid();
  }

  /**
   * Prepares the compacted index for use during compaction.
   *
   * @return the prepared compacted index
   *
   * @throws IOException if index creation fails
   */
  private LSMTreeIndexCompacted prepareCompactedIndex() throws IOException {
    final LSMTreeIndexMutable mutableIndex = mainIndex.getMutableIndex();
    LSMTreeIndexCompacted compactedIndex = mutableIndex.getSubIndex();

    if (compactedIndex == null) {
      // Create a new compacted index
      compactedIndex = mutableIndex.createNewForCompaction();
      mutableIndex.getDatabase().getSchema().getEmbedded().registerFile(compactedIndex);

      LogManager.instance().log(mainIndex, Level.WARNING,
          "- Created new compacted sub-index '%s' with fileId=%d (threadId=%d)",
          null, compactedIndex, compactedIndex.getFileId(), Thread.currentThread().threadId());
    } else {
      LogManager.instance().log(mainIndex, Level.INFO,
          "- Using existing compacted sub-index '%s' with fileId=%d (threadId=%d)",
          null, compactedIndex, compactedIndex.getFileId(), Thread.currentThread().threadId());
    }

    return compactedIndex;
  }

  /**
   * Logs the final compaction results.
   *
   * @param result the compaction result to log
   */
  private void logCompactionResults(CompactionResult result) {
    if (result.success() && result.wasWorkPerformed()) {
      LogManager.instance().log(mainIndex, Level.WARNING, result.getLogSummary(mainIndex.getName()), (Object[]) null);

      // Log additional performance metrics
      final CompactionMetrics metrics = result.metrics();
      LogManager.instance().log(mainIndex, Level.INFO,
          "Compaction performance: iterations=%d mergedKeys=%d mergedValues=%d compressionRatio=%.1f%% (threadId=%d)",
          null, metrics.getIterations(), metrics.getTotalMergedKeys(),
          metrics.getTotalMergedValues(), result.getCompressionRatio(),
          Thread.currentThread().threadId());
    } else if (!result.success()) {
      LogManager.instance().log(mainIndex, Level.WARNING,
          "Compaction failed for index '%s' after %dms",
          null, mainIndex.getName(), result.getElapsedTime());
    } else {
      LogManager.instance().log(mainIndex, Level.INFO,
          "Compaction skipped for index '%s' (no work needed)",
          null, mainIndex.getName());
    }
  }

  /**
   * Returns the main index being compacted.
   *
   * @return the main index
   */
  public LSMTreeIndex getMainIndex() {
    return mainIndex;
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
   * Returns the compaction strategy.
   *
   * @return the strategy
   */
  public CompactionStrategy getStrategy() {
    return strategy;
  }
}
