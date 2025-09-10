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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.log.LogManager;
import com.arcadedb.utility.FileUtils;

import java.util.logging.Level;

/**
 * Configuration settings for LSM-tree index compaction operations.
 * <p>
 * This class encapsulates all configuration parameters needed for compaction,
 * including memory limits, page size, and derived settings. It handles the
 * calculation of optimal RAM usage based on system constraints.
 * </p>
 *
 * @author ArcadeData Team
 * @since 25.9.1
 */
public final class CompactionConfiguration {
  private final DatabaseInternal database;
  private final int              pageSize;
  private final long             configuredRam;
  private final long             effectiveRam;
  private final boolean          debugEnabled;

  private static final double MAX_HEAP_PERCENTAGE = 0.30; // 30% of max heap

  /**
   * Creates a new compaction configuration for the specified database and page size.
   *
   * @param database     the database instance
   * @param pageSize     the page size in bytes
   * @param debugEnabled whether debug mode is enabled
   */
  public CompactionConfiguration(DatabaseInternal database, int pageSize, boolean debugEnabled) {
    this.database = database;
    this.pageSize = pageSize;
    this.debugEnabled = debugEnabled;
    this.configuredRam = calculateConfiguredRam();
    this.effectiveRam = calculateEffectiveRam();
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
   * Returns the page size in bytes.
   *
   * @return the page size
   */
  public int getPageSize() {
    return pageSize;
  }

  /**
   * Returns the originally configured RAM limit for compaction in bytes.
   *
   * @return the configured RAM limit
   */
  public long getConfiguredRam() {
    return configuredRam;
  }

  /**
   * Returns the effective RAM limit to be used for compaction in bytes.
   * This may be lower than the configured RAM if it exceeds system constraints.
   *
   * @return the effective RAM limit
   */
  public long getEffectiveRam() {
    return effectiveRam;
  }

  /**
   * Returns whether debug mode is enabled.
   *
   * @return true if debug is enabled
   */
  public boolean isDebugEnabled() {
    return debugEnabled;
  }

  /**
   * Calculates the maximum number of pages that can be compacted at once
   * given the effective RAM limit.
   *
   * @param totalRamNeeded total RAM needed for all pages
   *
   * @return the maximum number of pages to compact in one iteration
   */
  public int calculateMaxPagesPerIteration(long totalRamNeeded) {
    if (totalRamNeeded <= effectiveRam) {
      return Integer.MAX_VALUE; // No limit needed
    }

    return (int) (effectiveRam / pageSize);
  }

  /**
   * Determines if a partial compaction is needed based on total RAM requirements.
   *
   * @param totalPages total number of pages to compact
   *
   * @return true if partial compaction is needed
   */
  public boolean requiresPartialCompaction(int totalPages) {
    long totalRamNeeded = (long) totalPages * pageSize;
    return totalRamNeeded > effectiveRam;
  }

  /**
   * Calculates total RAM needed for the specified number of pages.
   *
   * @param pageCount number of pages
   *
   * @return total RAM needed in bytes
   */
  public long calculateTotalRamNeeded(int pageCount) {
    return (long) pageCount * pageSize;
  }

  /**
   * Logs configuration information at the specified log level.
   *
   * @param source the logging source
   * @param level  the log level
   */
  public void logConfiguration(Object source, Level level) {
    if (configuredRam != effectiveRam) {
      LogManager.instance().log(source, level,
          "Configured RAM for compaction (%dMB) adjusted to %dMB (max heap constraint)",
          null, configuredRam / (1024 * 1024), effectiveRam / (1024 * 1024));
    }

    LogManager.instance().log(source, level,
        "Compaction configuration: effectiveRAM=%s pageSize=%d debugEnabled=%s",
        null, FileUtils.getSizeAsString(effectiveRam), pageSize, debugEnabled);
  }

  private long calculateConfiguredRam() {
    return database.getConfiguration()
        .getValueAsLong(GlobalConfiguration.INDEX_COMPACTION_RAM_MB) * 1024L * 1024L;
  }

  private long calculateEffectiveRam() {
    long maxUsableRam = (long) (Runtime.getRuntime().maxMemory() * MAX_HEAP_PERCENTAGE);

    if (configuredRam > maxUsableRam) {
      return maxUsableRam;
    }

    return configuredRam;
  }

  @Override
  public String toString() {
    return "CompactionConfiguration{" +
        "pageSize=" + pageSize +
        ", configuredRam=" + FileUtils.getSizeAsString(configuredRam) +
        ", effectiveRam=" + FileUtils.getSizeAsString(effectiveRam) +
        ", debugEnabled=" + debugEnabled +
        '}';
  }
}
