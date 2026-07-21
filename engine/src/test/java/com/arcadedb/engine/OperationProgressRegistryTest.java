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
package com.arcadedb.engine;

import com.arcadedb.serializer.json.JSONObject;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for the running-operations registry backing the progress reporting of long maintenance
 * operations.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class OperationProgressRegistryTest {

  @Test
  void registerUpdateSnapshotUnregister() {
    final OperationProgressRegistry registry = OperationProgressRegistry.instance();
    final int initialSize = registry.size();

    final OperationProgress op = registry.register("testdb-progress", "check database");
    try {
      assertThat(registry.size()).isEqualTo(initialSize + 1);
      assertThat(op.getDatabaseName()).isEqualTo("testdb-progress");
      assertThat(op.getOperation()).isEqualTo("check database");
      assertThat(op.getStartedOn()).isGreaterThan(0L);

      // BEFORE ANY UPDATE: empty step, unknown total.
      assertThat(op.getStepIndex()).isEqualTo(0);
      assertThat(op.getPercentage()).isEqualTo(-1);

      op.onProgress("Checking vertices 'Account'", 2, 7, 421, 1000);
      assertThat(op.getStepName()).isEqualTo("Checking vertices 'Account'");
      assertThat(op.getStepIndex()).isEqualTo(2);
      assertThat(op.getTotalSteps()).isEqualTo(7);
      assertThat(op.getDone()).isEqualTo(421);
      assertThat(op.getTotal()).isEqualTo(1000);
      assertThat(op.getPercentage()).isEqualTo(42);

      // SNAPSHOT BY DATABASE, oldest first.
      final List<OperationProgress> ops = registry.getOperations("testdb-progress");
      assertThat(ops).hasSize(1);
      assertThat(ops.getFirst().getId()).isEqualTo(op.getId());
      assertThat(registry.getOperations("some-other-db")).isEmpty();

      // JSON SHAPE consumed by the HTTP endpoint and the pollers.
      final JSONObject json = op.toJSON();
      assertThat(json.getLong("id", -1)).isEqualTo(op.getId());
      assertThat(json.getString("database", "")).isEqualTo("testdb-progress");
      assertThat(json.getString("operation", "")).isEqualTo("check database");
      assertThat(json.getString("stepName", "")).isEqualTo("Checking vertices 'Account'");
      assertThat(json.getInt("stepIndex", -1)).isEqualTo(2);
      assertThat(json.getInt("totalSteps", -1)).isEqualTo(7);
      assertThat(json.getLong("done", -1)).isEqualTo(421);
      assertThat(json.getLong("total", -1)).isEqualTo(1000);
      assertThat(json.getInt("percentage", -1)).isEqualTo(42);
      assertThat(json.getLong("elapsedMs", -1)).isGreaterThanOrEqualTo(0L);
    } finally {
      registry.unregister(op);
    }
    assertThat(registry.size()).isEqualTo(initialSize);
    assertThat(registry.getOperations("testdb-progress")).isEmpty();
  }

  @Test
  void percentageClampsAndHandlesUnknownTotal() {
    final OperationProgressRegistry registry = OperationProgressRegistry.instance();
    final OperationProgress op = registry.register("testdb-progress2", "check database fix");
    try {
      op.onProgress("rebuild", 1, 1, 50, -1);
      assertThat(op.getPercentage()).isEqualTo(-1); // UNKNOWN TOTAL

      op.onProgress("rebuild", 1, 1, 2000, 1000);
      assertThat(op.getPercentage()).isEqualTo(100); // CLAMPED, NEVER OVER 100
    } finally {
      registry.unregister(op);
    }
  }
}
