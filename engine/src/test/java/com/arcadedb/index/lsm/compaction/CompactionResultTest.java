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
import static org.assertj.core.api.Assertions.offset;

/**
 * Unit tests for CompactionResult record.
 */
class CompactionResultTest extends TestHelper {

  private CompactionMetrics metrics;

  @BeforeEach
  void setUp() {
    metrics = new CompactionMetrics();
    // Add some test data
    for (int i = 0; i < 1000; i++) {
      metrics.incrementTotalKeys();
    }
    for (int i = 0; i < 100; i++) {
      metrics.incrementTotalMergedKeys();
    }
    metrics.addTotalValues(2000);
    metrics.addTotalMergedValues(200);
  }

  @Test
  void testSuccessfulResult() {
    CompactionResult result = CompactionResult.success(
        metrics, "oldFile", 1, "newFile", 2, "compactedFile", 3,
        10, 5, 8);

    assertThat(result.success()).isTrue();
    assertThat(result.metrics()).isEqualTo(metrics);
    assertThat(result.oldMutableFileName()).isEqualTo("oldFile");
    assertThat(result.oldMutableFileId()).isEqualTo(1);
    assertThat(result.newMutableFileName()).isEqualTo("newFile");
    assertThat(result.newMutableFileId()).isEqualTo(2);
    assertThat(result.compactedFileName()).isEqualTo("compactedFile");
    assertThat(result.compactedFileId()).isEqualTo(3);
    assertThat(result.newMutablePages()).isEqualTo(10);
    assertThat(result.compactedIndexPages()).isEqualTo(5);
    assertThat(result.lastImmutablePage()).isEqualTo(8);
  }

  @Test
  void testFailureResult() {
    CompactionResult result = CompactionResult.failure(metrics);

    assertThat(result.success()).isFalse();
    assertThat(result.metrics()).isEqualTo(metrics);
    assertThat(result.oldMutableFileName()).isNull();
    assertThat(result.oldMutableFileId()).isEqualTo(-1);
    assertThat(result.newMutableFileName()).isNull();
    assertThat(result.newMutableFileId()).isEqualTo(-1);
    assertThat(result.compactedFileName()).isNull();
    assertThat(result.compactedFileId()).isEqualTo(-1);
    assertThat(result.newMutablePages()).isEqualTo(0);
    assertThat(result.compactedIndexPages()).isEqualTo(0);
    assertThat(result.lastImmutablePage()).isEqualTo(-1);
  }

  @Test
  void testNoCompactionNeededResult() {
    CompactionResult result = CompactionResult.noCompactionNeeded();

    assertThat(result.success()).isFalse();
    assertThat(result.metrics()).isNotNull();
    assertThat(result.oldMutableFileName()).isNull();
    assertThat(result.oldMutableFileId()).isEqualTo(-1);
  }

  @Test
  void testGetElapsedTime() {
    CompactionResult result = CompactionResult.success(
        metrics, "oldFile", 1, "newFile", 2, "compactedFile", 3,
        10, 5, 8);

    assertThat(result.getElapsedTime()).isGreaterThanOrEqualTo(0);
    assertThat(result.getElapsedTime()).isEqualTo(metrics.getElapsedTime());
  }

  @Test
  void testWasWorkPerformedTrue() {
    CompactionResult result = CompactionResult.success(
        metrics, "oldFile", 1, "newFile", 2, "compactedFile", 3,
        10, 5, 8);

    assertThat(result.wasWorkPerformed()).isTrue();
  }

  @Test
  void testWasWorkPerformedFalseForFailure() {
    CompactionResult result = CompactionResult.failure(metrics);
    assertThat(result.wasWorkPerformed()).isFalse();
  }

  @Test
  void testWasWorkPerformedFalseForNoCompactionNeeded() {
    CompactionResult result = CompactionResult.noCompactionNeeded();
    assertThat(result.wasWorkPerformed()).isFalse();
  }

  @Test
  void testWasWorkPerformedFalseForNoKeys() {
    CompactionMetrics emptyMetrics = new CompactionMetrics();
    CompactionResult result = CompactionResult.success(
        emptyMetrics, "oldFile", 1, "newFile", 2, "compactedFile", 3,
        10, 5, 8);

    assertThat(result.wasWorkPerformed()).isFalse();
  }

  @Test
  void testGetCompressionRatio() {
    CompactionResult result = CompactionResult.success(
        metrics, "oldFile", 1, "newFile", 2, "compactedFile", 3,
        10, 5, 8);

    // metrics has 1000 total keys and 100 merged keys
    // compression ratio = (1 - 100/1000) * 100 = 90%
    assertThat(result.getCompressionRatio()).isEqualTo(90.0, offset(0.1));
  }

  @Test
  void testGetCompressionRatioWithNoKeys() {
    CompactionMetrics emptyMetrics = new CompactionMetrics();
    CompactionResult result = CompactionResult.success(
        emptyMetrics, "oldFile", 1, "newFile", 2, "compactedFile", 3,
        10, 5, 8);

    assertThat(result.getCompressionRatio()).isEqualTo(0.0, offset(0.1));
  }

  @Test
  void testGetCompressionRatioWithNullMetrics() {
    CompactionResult result = CompactionResult.success(
        null, "oldFile", 1, "newFile", 2, "compactedFile", 3,
        10, 5, 8);

    assertThat(result.getCompressionRatio()).isEqualTo(0.0, offset(0.1));
  }

  @Test
  void testGetCompressionRatioClampedToZero() {
    // Create metrics where merged keys > total keys (edge case)
    CompactionMetrics edgeCaseMetrics = new CompactionMetrics();
    edgeCaseMetrics.incrementTotalKeys();
    for (int i = 0; i < 5; i++) {
      edgeCaseMetrics.incrementTotalMergedKeys();
    }

    CompactionResult result = CompactionResult.success(
        edgeCaseMetrics, "oldFile", 1, "newFile", 2, "compactedFile", 3,
        10, 5, 8);

    assertThat(result.getCompressionRatio()).isEqualTo(0.0, offset(0.1));
  }

  @Test
  void testGetLogSummaryForFailure() {
    CompactionResult result = CompactionResult.failure(metrics);

    String summary = result.getLogSummary("testIndex");
    assertThat(summary).isEqualTo("Index 'testIndex' compaction failed");
  }

  @Test
  void testGetLogSummaryForNoWork() {
    CompactionMetrics emptyMetrics = new CompactionMetrics();
    CompactionResult result = CompactionResult.success(
        emptyMetrics, "oldFile", 1, "newFile", 2, "compactedFile", 3,
        10, 5, 8);

    String summary = result.getLogSummary("testIndex");
    assertThat(summary).isEqualTo("Index 'testIndex' compaction skipped (insufficient pages)");
  }

  @Test
  void testGetLogSummaryForSuccess() {
    CompactionResult result = CompactionResult.success(
        metrics, "oldFile", 1, "newFile", 2, "compactedFile", 3,
        10, 5, 8);

    String summary = result.getLogSummary("testIndex");

    assertThat(summary).contains("testIndex");
    assertThat(summary).contains("compacted");
    assertThat(summary).contains("keys=1000");
    assertThat(summary).contains("values=2000");
    assertThat(summary).contains("mutablePages=10");
    assertThat(summary).contains("immutablePages=5");
    assertThat(summary).contains("compressionRatio=90.0%");
    assertThat(summary).contains("oldFile=oldFile(1)");
    assertThat(summary).contains("newFile=newFile(2)");
    assertThat(summary).contains("compactedFile=compactedFile(3)");
  }

  @Test
  void testToString() {
    CompactionResult result = CompactionResult.success(
        metrics, "oldFile", 1, "newFile", 2, "compactedFile", 3,
        10, 5, 8);

    String toString = result.toString();

    assertThat(toString).contains("CompactionResult");
    assertThat(toString).contains("success=true");
    assertThat(toString).contains("totalKeys=1000");
    assertThat(toString).contains("totalValues=2000");
    assertThat(toString).contains("compressionRatio=90.0%");
    assertThat(toString).contains("elapsedTime=");
    assertThat(toString).contains("ms");
  }

  @Test
  void testToStringForFailure() {
    CompactionResult result = CompactionResult.failure(metrics);

    String toString = result.toString();

    assertThat(toString).contains("success=false");
    assertThat(toString).contains("totalKeys=1000");
    assertThat(toString).contains("totalValues=2000");
  }
}
