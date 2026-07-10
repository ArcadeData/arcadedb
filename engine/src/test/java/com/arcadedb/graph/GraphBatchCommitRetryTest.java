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
package com.arcadedb.graph;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.exception.NeedRetryException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for issue #4724: a transient retryable failure (e.g. a Raft
 * {@code QuorumNotReachedException} raised when a leader re-election interrupts replication during
 * a bulk load) must not abort the whole import. {@link GraphBatch#createVertices} now retries the
 * begin/save/commit unit on a {@link NeedRetryException} instead of propagating it.
 * <p>
 * The retry path is exercised deterministically here via {@link GraphBatch#TEST_BEFORE_VERTEX_COMMIT_HOOK}
 * (no Raft cluster needed); the end-to-end HTTP behaviour on a real cluster is covered by
 * {@code RaftBatchEndpointIT}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class GraphBatchCommitRetryTest extends TestHelper {

  private static final String VERTEX_TYPE = "RetryNode";

  @Override
  protected void beginTest() {
    database.transaction(() -> database.getSchema().createVertexType(VERTEX_TYPE));
  }

  @AfterEach
  void clearHook() {
    GraphBatch.TEST_BEFORE_VERTEX_COMMIT_HOOK = null;
  }

  @Test
  void recoversFromTransientCommitFailure() {
    final int count = 500;
    final AtomicInteger attempts = new AtomicInteger();

    // Fail the first two commit attempts with a retryable error, then let the third succeed.
    GraphBatch.TEST_BEFORE_VERTEX_COMMIT_HOOK = attempt -> {
      attempts.incrementAndGet();
      if (attempt <= 2)
        throw new NeedRetryException("simulated transient leader re-election (attempt " + attempt + ")");
    };

    final RID[] rids;
    try (final GraphBatch batch = GraphBatch.builder(database)
        .withCommitRetries(5)
        .withCommitRetryDelay(0)
        .build()) {
      rids = batch.createVertices(VERTEX_TYPE, count);
    }

    assertThat(attempts.get()).as("hook must be invoked once per attempt (2 failed + 1 success)").isEqualTo(3);
    assertThat(rids).hasSize(count);
    for (final RID rid : rids)
      assertThat(rid).isNotNull();

    // The two rolled-back attempts leave nothing behind on a local database: exactly `count` vertices.
    assertThat(database.countType(VERTEX_TYPE, true)).isEqualTo(count);
  }

  @Test
  void propagatesAfterRetriesExhausted() {
    final AtomicInteger attempts = new AtomicInteger();

    // Always fail with a retryable error.
    GraphBatch.TEST_BEFORE_VERTEX_COMMIT_HOOK = attempt -> {
      attempts.incrementAndGet();
      throw new NeedRetryException("permanent simulated failure (attempt " + attempt + ")");
    };

    try (final GraphBatch batch = GraphBatch.builder(database)
        .withCommitRetries(3)
        .withCommitRetryDelay(0)
        .build()) {
      assertThatThrownBy(() -> batch.createVertices(VERTEX_TYPE, 100))
          .isInstanceOf(NeedRetryException.class);
    }

    // 1 initial attempt + 3 retries.
    assertThat(attempts.get()).isEqualTo(4);
    // Nothing was durably committed.
    assertThat(database.countType(VERTEX_TYPE, true)).isZero();
  }

  @Test
  void retriesDisabledFailFast() {
    final AtomicInteger attempts = new AtomicInteger();

    GraphBatch.TEST_BEFORE_VERTEX_COMMIT_HOOK = attempt -> {
      attempts.incrementAndGet();
      throw new NeedRetryException("failure with retries disabled");
    };

    try (final GraphBatch batch = GraphBatch.builder(database)
        .withCommitRetries(0)
        .build()) {
      assertThatThrownBy(() -> batch.createVertices(VERTEX_TYPE, 10))
          .isInstanceOf(NeedRetryException.class);
    }

    assertThat(attempts.get()).as("with retries disabled, only the initial attempt runs").isEqualTo(1);
  }
}
