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
 * Result of an LSM-tree index compaction operation.
 * <p>
 * This immutable record contains the final state and statistics from a completed
 * compaction operation, including success status, performance metrics, and
 * file information.
 * </p>
 *
 * @param success             whether the compaction completed successfully
 * @param metrics             the final metrics from the compaction operation
 * @param oldMutableFileName  name of the old mutable index file
 * @param oldMutableFileId    ID of the old mutable index file
 * @param newMutableFileName  name of the new mutable index file
 * @param newMutableFileId    ID of the new mutable index file
 * @param compactedFileName   name of the compacted index file
 * @param compactedFileId     ID of the compacted index file
 * @param newMutablePages     number of pages in the new mutable index
 * @param compactedIndexPages number of pages in the compacted index
 * @param lastImmutablePage   index of the last immutable page processed
 *
 * @author ArcadeData Team
 * @since 25.9.1
 */
public record CompactionResult(
    boolean success,
    CompactionMetrics metrics,
    String oldMutableFileName,
    int oldMutableFileId,
    String newMutableFileName,
    int newMutableFileId,
    String compactedFileName,
    int compactedFileId,
    int newMutablePages,
    int compactedIndexPages,
    int lastImmutablePage
) {

  /**
   * Creates a successful compaction result with the provided parameters.
   *
   * @param metrics             the final compaction metrics
   * @param oldMutableFileName  name of the old mutable index file
   * @param oldMutableFileId    ID of the old mutable index file
   * @param newMutableFileName  name of the new mutable index file
   * @param newMutableFileId    ID of the new mutable index file
   * @param compactedFileName   name of the compacted index file
   * @param compactedFileId     ID of the compacted index file
   * @param newMutablePages     number of pages in the new mutable index
   * @param compactedIndexPages number of pages in the compacted index
   * @param lastImmutablePage   index of the last immutable page processed
   *
   * @return a successful CompactionResult
   */
  public static CompactionResult success(
      CompactionMetrics metrics,
      String oldMutableFileName,
      int oldMutableFileId,
      String newMutableFileName,
      int newMutableFileId,
      String compactedFileName,
      int compactedFileId,
      int newMutablePages,
      int compactedIndexPages,
      int lastImmutablePage) {
    return new CompactionResult(
        true, metrics, oldMutableFileName, oldMutableFileId,
        newMutableFileName, newMutableFileId, compactedFileName, compactedFileId,
        newMutablePages, compactedIndexPages, lastImmutablePage);
  }

  /**
   * Creates a failed compaction result with minimal information.
   *
   * @param metrics the metrics collected before failure
   *
   * @return a failed CompactionResult
   */
  public static CompactionResult failure(CompactionMetrics metrics) {
    return new CompactionResult(
        false, metrics, null, -1, null, -1, null, -1, 0, 0, -1);
  }

  /**
   * Creates a result indicating no compaction was needed (insufficient pages).
   *
   * @return a CompactionResult indicating no action was taken
   */
  public static CompactionResult noCompactionNeeded() {
    return new CompactionResult(
        false, new CompactionMetrics(), null, -1, null, -1, null, -1, 0, 0, -1);
  }

  /**
   * Returns the elapsed time for the compaction operation in milliseconds.
   *
   * @return elapsed time in milliseconds
   */
  public long getElapsedTime() {
    return metrics != null ? metrics.getElapsedTime() : 0;
  }

  /**
   * Returns whether any work was actually performed.
   *
   * @return true if compaction work was performed
   */
  public boolean wasWorkPerformed() {
    return success && metrics != null && metrics.getTotalKeys() > 0;
  }

  /**
   * Returns the compression ratio as a percentage (0-100).
   * Calculated as: (1 - merged_keys / total_keys) * 100
   *
   * @return compression ratio percentage, or 0 if no keys were processed
   */
  public double getCompressionRatio() {
    if (metrics == null || metrics.getTotalKeys() == 0) {
      return 0.0;
    }

    double ratio = 1.0 - ((double) metrics.getTotalMergedKeys() / metrics.getTotalKeys());
    return Math.max(0.0, ratio * 100.0);
  }

  /**
   * Returns a formatted summary string for logging purposes.
   *
   * @param indexName the name of the index that was compacted
   *
   * @return formatted summary string
   */
  public String getLogSummary(String indexName) {
    if (!success) {
      return String.format("Index '%s' compaction failed", indexName);
    }

    if (!wasWorkPerformed()) {
      return String.format("Index '%s' compaction skipped (insufficient pages)", indexName);
    }

    return String.format(
        "Index '%s' compacted in %dms (keys=%d values=%d mutablePages=%d immutablePages=%d " +
            "iterations=%d compressionRatio=%.1f%% oldFile=%s(%d) newFile=%s(%d) compactedFile=%s(%d))",
        indexName, getElapsedTime(), metrics.getTotalKeys(), metrics.getTotalValues(),
        newMutablePages, compactedIndexPages, metrics.getIterations(), getCompressionRatio(),
        oldMutableFileName, oldMutableFileId, newMutableFileName, newMutableFileId,
        compactedFileName, compactedFileId);
  }

  @Override
  public String toString() {
    return "CompactionResult{" +
        "success=" + success +
        ", elapsedTime=" + getElapsedTime() + "ms" +
        ", totalKeys=" + (metrics != null ? metrics.getTotalKeys() : 0) +
        ", totalValues=" + (metrics != null ? metrics.getTotalValues() : 0) +
        ", compressionRatio=" + String.format("%.1f%%", getCompressionRatio()) +
        '}';
  }
}
