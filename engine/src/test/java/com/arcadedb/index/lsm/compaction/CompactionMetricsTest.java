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

import com.arcadedb.TestHelper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for CompactionMetrics class.
 */
class CompactionMetricsTest extends TestHelper {

  private CompactionMetrics metrics;

  @BeforeEach
  void setUp() {
    metrics = new CompactionMetrics();
  }

  @Test
  void testInitialState() {
    assertThat(metrics.getIterations()).isEqualTo(0);
    assertThat(metrics.getTotalKeys()).isEqualTo(0);
    assertThat(metrics.getTotalValues()).isEqualTo(0);
    assertThat(metrics.getTotalMergedKeys()).isEqualTo(0);
    assertThat(metrics.getTotalMergedValues()).isEqualTo(0);
    assertThat(metrics.getCompactedPages()).isEqualTo(0);
    assertThat(metrics.getStartTime()).isGreaterThan(0);
    assertThat(metrics.getElapsedTime()).isGreaterThanOrEqualTo(0);
  }

  @Test
  void testIncrementIterations() {
    metrics.incrementIterations();
    assertThat(metrics.getIterations()).isEqualTo(1);

    metrics.incrementIterations();
    assertThat(metrics.getIterations()).isEqualTo(2);
  }

  @Test
  void testIncrementTotalKeys() {
    metrics.incrementTotalKeys();
    assertThat(metrics.getTotalKeys()).isEqualTo(1);

    metrics.incrementTotalKeys();
    assertThat(metrics.getTotalKeys()).isEqualTo(2);
  }

  @Test
  void testAddTotalValues() {
    metrics.addTotalValues(5);
    assertThat(metrics.getTotalValues()).isEqualTo(5);

    metrics.addTotalValues(3);
    assertThat(metrics.getTotalValues()).isEqualTo(8);
  }

  @Test
  void testIncrementTotalMergedKeys() {
    metrics.incrementTotalMergedKeys();
    assertThat(metrics.getTotalMergedKeys()).isEqualTo(1);

    metrics.incrementTotalMergedKeys();
    assertThat(metrics.getTotalMergedKeys()).isEqualTo(2);
  }

  @Test
  void testAddTotalMergedValues() {
    metrics.addTotalMergedValues(10);
    assertThat(metrics.getTotalMergedValues()).isEqualTo(10);

    metrics.addTotalMergedValues(20);
    assertThat(metrics.getTotalMergedValues()).isEqualTo(30);
  }

  @Test
  void testAddCompactedPages() {
    metrics.addCompactedPages(3);
    assertThat(metrics.getCompactedPages()).isEqualTo(3);

    metrics.addCompactedPages(2);
    assertThat(metrics.getCompactedPages()).isEqualTo(5);
  }

  @Test
  void testElapsedTime() throws InterruptedException {
    long initialElapsed = metrics.getElapsedTime();
    Thread.sleep(10); // Small delay
    long laterElapsed = metrics.getElapsedTime();

    assertThat(laterElapsed).isGreaterThanOrEqualTo(initialElapsed);
  }

  @Test
  void testShouldLogProgressFalseInitially() {
    assertThat(metrics.shouldLogProgress()).isFalse();
  }

  @Test
  void testShouldLogProgressAtMilestone() {
    // Add keys to reach 1 million milestone
    for (int i = 0; i < 1_000_000; i++) {
      metrics.incrementTotalKeys();
    }

    assertThat(metrics.shouldLogProgress()).isTrue();

    // Add one more - should return false
    metrics.incrementTotalKeys();
    assertThat(metrics.shouldLogProgress()).isFalse();

    // Add to reach 2 million milestone
    for (int i = 0; i < 999_999; i++) {
      metrics.incrementTotalKeys();
    }

    assertThat(metrics.shouldLogProgress()).isTrue();
  }

  @Test
  void testToString() {
    metrics.incrementIterations();
    metrics.incrementTotalKeys();
    metrics.addTotalValues(5);
    metrics.incrementTotalMergedKeys();
    metrics.addTotalMergedValues(2);
    metrics.addCompactedPages(1);

    String result = metrics.toString();

    assertThat(result).contains("iterations=1");
    assertThat(result).contains("totalKeys=1");
    assertThat(result).contains("totalValues=5");
    assertThat(result).contains("totalMergedKeys=1");
    assertThat(result).contains("totalMergedValues=2");
    assertThat(result).contains("compactedPages=1");
    assertThat(result).contains("elapsedTime=");
    assertThat(result).contains("ms");
  }

  @Test
  void testThreadSafety() throws InterruptedException {
    final int threadCount = 10;
    final int incrementsPerThread = 1000;

    Thread[] threads = new Thread[threadCount];

    for (int i = 0; i < threadCount; i++) {
      threads[i] = new Thread(() -> {
        for (int j = 0; j < incrementsPerThread; j++) {
          metrics.incrementIterations();
          metrics.incrementTotalKeys();
          metrics.addTotalValues(1);
          metrics.incrementTotalMergedKeys();
          metrics.addTotalMergedValues(1);
          metrics.addCompactedPages(1);
        }
      });
    }

    // Start all threads
    for (Thread thread : threads) {
      thread.start();
    }

    // Wait for all threads to complete
    for (Thread thread : threads) {
      thread.join();
    }

    // Verify final counts
    int expectedCount = threadCount * incrementsPerThread;
    assertThat(metrics.getIterations()).isEqualTo(expectedCount);
    assertThat(metrics.getTotalKeys()).isEqualTo(expectedCount);
    assertThat(metrics.getTotalValues()).isEqualTo(expectedCount);
    assertThat(metrics.getTotalMergedKeys()).isEqualTo(expectedCount);
    assertThat(metrics.getTotalMergedValues()).isEqualTo(expectedCount);
    assertThat(metrics.getCompactedPages()).isEqualTo(expectedCount);
  }
}
