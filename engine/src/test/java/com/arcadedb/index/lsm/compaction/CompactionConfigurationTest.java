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
import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Unit tests for CompactionConfiguration class.
 */
class CompactionConfigurationTest extends TestHelper {

  private       CompactionConfiguration compactionConfig;
  private final int                     testPageSize = 4096;

  @BeforeEach
  void setUp() {
    // Database is provided by TestHelper
  }

  @Test
  void testBasicConstruction() {
    compactionConfig = new CompactionConfiguration((DatabaseInternal) database, testPageSize, false);

    assertThat(compactionConfig.getDatabase()).isEqualTo(database);
    assertThat(compactionConfig.getPageSize()).isEqualTo(testPageSize);
    assertThat(compactionConfig.isDebugEnabled()).isFalse();
    assertThat(compactionConfig.getConfiguredRam()).isGreaterThan(0);
    assertThat(compactionConfig.getEffectiveRam()).isGreaterThan(0);
  }

  @Test
  void testDebugEnabled() {
    compactionConfig = new CompactionConfiguration((DatabaseInternal) database, testPageSize, true);
    assertThat(compactionConfig.isDebugEnabled()).isTrue();
  }

  @Test
  void testConfiguredRamCalculation() {
    // Set configuration to 100MB
    GlobalConfiguration.INDEX_COMPACTION_RAM_MB.setValue(100);

    compactionConfig = new CompactionConfiguration((DatabaseInternal) database, testPageSize, false);

    assertThat(compactionConfig.getConfiguredRam()).isEqualTo(100 * 1024L * 1024L);
  }

  @Test
  void testEffectiveRamLimitedByHeapConstraint() {
    // Set a very high RAM configuration that would exceed 30% of heap
    long veryLargeRam = Runtime.getRuntime().maxMemory(); // Exceeds 30% constraint
    GlobalConfiguration.INDEX_COMPACTION_RAM_MB.setValue(veryLargeRam / (1024 * 1024));

    compactionConfig = new CompactionConfiguration((DatabaseInternal) database, testPageSize, false);

    // Effective RAM should be limited to 30% of max heap
    long maxUsableRam = (long) (Runtime.getRuntime().maxMemory() * 0.30);
    assertThat(compactionConfig.getEffectiveRam()).isEqualTo(maxUsableRam);
    assertThat(compactionConfig.getEffectiveRam()).isLessThan(compactionConfig.getConfiguredRam());
  }

  @Test
  void testEffectiveRamNotLimitedWhenWithinConstraint() {
    // Set a reasonable RAM configuration that's within 30% of heap
    long reasonableRam = 64; // 64MB should be within constraint for most test environments
    GlobalConfiguration.INDEX_COMPACTION_RAM_MB.setValue(reasonableRam);

    compactionConfig = new CompactionConfiguration((DatabaseInternal) database, testPageSize, false);

    assertThat(compactionConfig.getConfiguredRam()).isEqualTo(reasonableRam * 1024L * 1024L);
    assertThat(compactionConfig.getEffectiveRam()).isEqualTo(compactionConfig.getConfiguredRam());
  }

  @Test
  void testCalculateMaxPagesPerIteration() {
    GlobalConfiguration.INDEX_COMPACTION_RAM_MB.setValue(10); // 10MB
    compactionConfig = new CompactionConfiguration((DatabaseInternal) database, testPageSize, false);

    long effectiveRam = compactionConfig.getEffectiveRam();
    long totalRamNeeded = effectiveRam * 2; // More than available

    int maxPages = compactionConfig.calculateMaxPagesPerIteration(totalRamNeeded);

    assertThat(maxPages).isEqualTo((int) (effectiveRam / testPageSize));
  }

  @Test
  void testCalculateMaxPagesPerIterationUnlimited() {
    GlobalConfiguration.INDEX_COMPACTION_RAM_MB.setValue(10); // 10MB
    compactionConfig = new CompactionConfiguration((DatabaseInternal) database, testPageSize, false);

    long effectiveRam = compactionConfig.getEffectiveRam();
    long totalRamNeeded = effectiveRam / 2; // Less than available

    int maxPages = compactionConfig.calculateMaxPagesPerIteration(totalRamNeeded);

    assertThat(maxPages).isEqualTo(Integer.MAX_VALUE);
  }

  @Test
  void testRequiresPartialCompactionTrue() {
    GlobalConfiguration.INDEX_COMPACTION_RAM_MB.setValue(1); // 1MB
    compactionConfig = new CompactionConfiguration((DatabaseInternal) database, testPageSize, false);

    int totalPages = 1000; // 1000 * 4KB = ~4MB, more than 1MB limit

    assertThat(compactionConfig.requiresPartialCompaction(totalPages)).isTrue();
  }

  @Test
  void testRequiresPartialCompactionFalse() {
    GlobalConfiguration.INDEX_COMPACTION_RAM_MB.setValue(100); // 100MB
    compactionConfig = new CompactionConfiguration((DatabaseInternal) database, testPageSize, false);

    int totalPages = 10; // 10 * 4KB = 40KB, much less than 100MB limit

    assertThat(compactionConfig.requiresPartialCompaction(totalPages)).isFalse();
  }

  @Test
  void testCalculateTotalRamNeeded() {
    compactionConfig = new CompactionConfiguration((DatabaseInternal) database, testPageSize, false);

    int pageCount = 100;
    long expectedRam = (long) pageCount * testPageSize;

    assertThat(compactionConfig.calculateTotalRamNeeded(pageCount)).isEqualTo(expectedRam);
  }

  @Test
  void testToString() {
    compactionConfig = new CompactionConfiguration((DatabaseInternal) database, testPageSize, true);

    String result = compactionConfig.toString();

    assertThat(result).contains("CompactionConfiguration");
    assertThat(result).contains("pageSize=" + testPageSize);
    assertThat(result).contains("debugEnabled=true");
    assertThat(result).contains("configuredRam=");
    assertThat(result).contains("effectiveRam=");
  }

  @Test
  void testLogConfiguration() {
    // This test mainly ensures the method doesn't throw exceptions
    // Actual logging output testing would require more complex setup
    compactionConfig = new CompactionConfiguration((DatabaseInternal) database, testPageSize, false);

    assertThatCode(() -> {
      compactionConfig.logConfiguration(this, java.util.logging.Level.INFO);
    }).doesNotThrowAnyException();
  }

  @Test
  void testZeroPageSize() {
    compactionConfig = new CompactionConfiguration((DatabaseInternal) database, 0, false);

    // With zero page size, division by zero might occur in calculateMaxPagesPerIteration
    // But the implementation might handle it gracefully
    assertThatCode(() -> {
      compactionConfig.calculateMaxPagesPerIteration(1000);
    }).doesNotThrowAnyException();
  }

  @Test
  void testNegativePageCount() {
    compactionConfig = new CompactionConfiguration((DatabaseInternal) database, testPageSize, false);

    // With negative page count, the calculation is: -5 * 4096 = -20480
    assertThat(compactionConfig.calculateTotalRamNeeded(-5)).isEqualTo(-20480);

    // requiresPartialCompaction compares negative value with positive effectiveRam, so it's false
    assertThat(compactionConfig.requiresPartialCompaction(-5)).isFalse();
  }
}
