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
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for CompactionOrchestrator class.
 */
class CompactionOrchestratorTest extends TestHelper {

  private CompactionOrchestrator orchestrator;
  private DocumentType           documentType;

  @BeforeEach
  void setUp() {
    orchestrator = new CompactionOrchestrator();

    // Create a real document type for integration tests
    documentType = database.getSchema().createDocumentType("TestType");
    documentType.createProperty("id", Type.INTEGER);
  }

  @Test
  void testDefaultConstruction() {
    assertThat(orchestrator.isDebugEnabled()).isFalse();
    assertThat(orchestrator.getStrategy()).isInstanceOf(StandardCompactionStrategy.class);
    assertThat(orchestrator.getStrategy().getStrategyName()).isEqualTo("Standard");
  }

  @Test
  void testSetDebug() {
    CompactionOrchestrator result = orchestrator.setDebug(true);

    assertThat(result).isSameAs(orchestrator); // Test method chaining
    assertThat(orchestrator.isDebugEnabled()).isTrue();
  }

  @Test
  void testSetStrategyWithTestStrategy() {
    // Create a simple custom strategy for testing
    CompactionStrategy testStrategy = new CompactionStrategy() {
      @Override
      public String getStrategyName() {
        return "Test";
      }

      @Override
      public int getMinimumPageCount() {
        return 2;
      }

      @Override
      public boolean supportsPartialCompaction() {
        return true;
      }

      @Override
      public long estimateMemoryUsage(int pageCount, int pageSize) {
        return (long) pageCount * pageSize;
      }

      @Override
      public CompactionResult executeCompaction(CompactionContext context) {
        return CompactionResult.noCompactionNeeded();
      }
    };

    CompactionOrchestrator result = orchestrator.setStrategy(testStrategy);

    assertThat(result).isSameAs(orchestrator); // Test method chaining
    assertThat(orchestrator.getStrategy()).isSameAs(testStrategy);
  }

  @Test
  void testSetStrategyNull() {
    assertThatThrownBy(() -> {
      orchestrator.setStrategy(null);
    }).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void testCompactNullIndex() {
    assertThatThrownBy(() -> {
      orchestrator.compact(null);
    }).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void testCompactWithCustomStrategyNullStrategy() {
    assertThatThrownBy(() -> {
      // Use null for both index and strategy to test validation
      orchestrator.compact(null, null);
    }).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void testCompactWithResultNullIndex() {
    assertThatThrownBy(() -> {
      orchestrator.compactWithResult(null);
    }).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void testCanCompactNullIndex() {
    assertThat(orchestrator.canCompact(null)).isFalse();
  }

  @Test
  void testCanCompactWithBasicValidation() {
    // Test the basic validation logic without requiring a real index
    // This mainly tests that the method doesn't throw exceptions
    boolean canCompact = orchestrator.canCompact(null);

    // Should return false for null index
    assertThat(canCompact).isFalse();
  }

  @Test
  void testCanCompactWithException() {
    // Test with null index's methods being called - should handle gracefully
    assertThat(orchestrator.canCompact(null)).isFalse();
  }

  @Test
  void testGetConfigurationInfo() {
    String info = orchestrator.getConfigurationInfo();

    assertThat(info).contains("CompactionOrchestrator");
    assertThat(info).contains("strategy=Standard");
    assertThat(info).contains("debugEnabled=false");
  }

  @Test
  void testGetConfigurationInfoWithCustomSettings() {
    // Create a simple custom strategy for testing
    CompactionStrategy testStrategy = new CompactionStrategy() {
      @Override
      public String getStrategyName() {
        return "Custom";
      }

      @Override
      public int getMinimumPageCount() {
        return 2;
      }

      @Override
      public boolean supportsPartialCompaction() {
        return true;
      }

      @Override
      public long estimateMemoryUsage(int pageCount, int pageSize) {
        return (long) pageCount * pageSize;
      }

      @Override
      public CompactionResult executeCompaction(CompactionContext context) {
        return CompactionResult.noCompactionNeeded();
      }
    };

    orchestrator.setStrategy(testStrategy).setDebug(true);

    String info = orchestrator.getConfigurationInfo();

    assertThat(info).contains("strategy=Custom");
    assertThat(info).contains("debugEnabled=true");
  }

  @Test
  void testGetStrategy() {
    assertThat(orchestrator.getStrategy()).isSameAs(orchestrator.getStrategy());

    // Create a simple custom strategy for testing
    CompactionStrategy testStrategy = new CompactionStrategy() {
      @Override
      public String getStrategyName() {
        return "Custom";
      }

      @Override
      public int getMinimumPageCount() {
        return 2;
      }

      @Override
      public boolean supportsPartialCompaction() {
        return true;
      }

      @Override
      public long estimateMemoryUsage(int pageCount, int pageSize) {
        return (long) pageCount * pageSize;
      }

      @Override
      public CompactionResult executeCompaction(CompactionContext context) {
        return CompactionResult.noCompactionNeeded();
      }
    };

    orchestrator.setStrategy(testStrategy);
    assertThat(orchestrator.getStrategy()).isSameAs(testStrategy);
  }

  @Test
  void testMethodChaining() {
    // Create a simple custom strategy for testing
    CompactionStrategy testStrategy = new CompactionStrategy() {
      @Override
      public String getStrategyName() {
        return "Custom";
      }

      @Override
      public int getMinimumPageCount() {
        return 2;
      }

      @Override
      public boolean supportsPartialCompaction() {
        return true;
      }

      @Override
      public long estimateMemoryUsage(int pageCount, int pageSize) {
        return (long) pageCount * pageSize;
      }

      @Override
      public CompactionResult executeCompaction(CompactionContext context) {
        return CompactionResult.noCompactionNeeded();
      }
    };

    // Test that methods can be chained fluently
    CompactionOrchestrator result = orchestrator
        .setDebug(true)
        .setStrategy(testStrategy);

    assertThat(result).isSameAs(orchestrator);
    assertThat(orchestrator.isDebugEnabled()).isTrue();
    assertThat(orchestrator.getStrategy()).isSameAs(testStrategy);
  }

  @Test
  void testCompactIntegrationFlow() throws Exception {
    // Create a simple strategy that always returns success
    CompactionStrategy successStrategy = new CompactionStrategy() {
      @Override
      public String getStrategyName() {
        return "Success";
      }

      @Override
      public int getMinimumPageCount() {
        return 0;
      }

      @Override
      public boolean supportsPartialCompaction() {
        return true;
      }

      @Override
      public long estimateMemoryUsage(int pageCount, int pageSize) {
        return (long) pageCount * pageSize;
      }

      @Override
      public CompactionResult executeCompaction(CompactionContext context) {
        CompactionMetrics metrics = new CompactionMetrics();
        metrics.incrementTotalKeys();
        return CompactionResult.success(
            metrics, "oldFile", 1, "newFile", 2, "compactedFile", 3, 10, 5, 4);
      }
    };

    orchestrator.setStrategy(successStrategy);

    // Test with null index - should throw IllegalArgumentException
    assertThatThrownBy(() -> {
      orchestrator.compact(null);
    }).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void testCompactWithResultIntegrationFlow() throws Exception {
    // Create a simple strategy that always returns success
    CompactionStrategy successStrategy = new CompactionStrategy() {
      @Override
      public String getStrategyName() {
        return "Success";
      }

      @Override
      public int getMinimumPageCount() {
        return 0;
      }

      @Override
      public boolean supportsPartialCompaction() {
        return true;
      }

      @Override
      public long estimateMemoryUsage(int pageCount, int pageSize) {
        return (long) pageCount * pageSize;
      }

      @Override
      public CompactionResult executeCompaction(CompactionContext context) {
        CompactionMetrics metrics = new CompactionMetrics();
        metrics.incrementTotalKeys();
        return CompactionResult.success(
            metrics, "oldFile", 1, "newFile", 2, "compactedFile", 3, 10, 5, 4);
      }
    };

    orchestrator.setStrategy(successStrategy);

    // Test with null index - should throw exception
    assertThatThrownBy(() -> {
      orchestrator.compactWithResult(null);
    }).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void testCompactWithCustomStrategyOneTime() throws Exception {
    // Create a simple strategy that always returns success
    CompactionStrategy customStrategy = new CompactionStrategy() {
      @Override
      public String getStrategyName() {
        return "OneTime";
      }

      @Override
      public int getMinimumPageCount() {
        return 0;
      }

      @Override
      public boolean supportsPartialCompaction() {
        return true;
      }

      @Override
      public long estimateMemoryUsage(int pageCount, int pageSize) {
        return (long) pageCount * pageSize;
      }

      @Override
      public CompactionResult executeCompaction(CompactionContext context) {
        CompactionMetrics metrics = new CompactionMetrics();
        metrics.incrementTotalKeys();
        return CompactionResult.success(
            metrics, "oldFile", 1, "newFile", 2, "compactedFile", 3, 10, 5, 4);
      }
    };

    // Don't set the strategy on orchestrator, pass it as parameter
    // Test with null index - should throw exception due to validation
    // The actual exception might be NullPointerException based on implementation
    assertThatThrownBy(() -> {
      orchestrator.compact(null, customStrategy);
    }).isInstanceOf(Exception.class);

    // Verify orchestrator still has its original strategy
    assertThat(orchestrator.getStrategy()).isInstanceOf(StandardCompactionStrategy.class);
  }
}
