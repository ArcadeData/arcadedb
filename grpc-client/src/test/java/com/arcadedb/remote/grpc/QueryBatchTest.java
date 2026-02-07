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
package com.arcadedb.remote.grpc;

import com.arcadedb.database.Record;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for QueryBatch.
 * Tests batch result holder functionality.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class QueryBatchTest {

  @Test
  void shouldCreateBatchWithResults() {
    final List<Result> results = new ArrayList<>();
    results.add(new ResultInternal());
    results.add(new ResultInternal());

    final QueryBatch batch = new QueryBatch(results, 2, 2, false);

    assertThat(batch.results()).hasSize(2);
    assertThat(batch.totalInBatch()).isEqualTo(2);
    assertThat(batch.runningTotal()).isEqualTo(2);
    assertThat(batch.isLastBatch()).isFalse();
  }

  @Test
  void shouldCreateLastBatch() {
    final List<Result> results = new ArrayList<>();
    results.add(new ResultInternal());

    final QueryBatch batch = new QueryBatch(results, 1, 10, true);

    assertThat(batch.results()).hasSize(1);
    assertThat(batch.totalInBatch()).isEqualTo(1);
    assertThat(batch.runningTotal()).isEqualTo(10);
    assertThat(batch.isLastBatch()).isTrue();
  }

  @Test
  void shouldCreateEmptyBatch() {
    final List<Result> results = new ArrayList<>();

    final QueryBatch batch = new QueryBatch(results, 0, 0, true);

    assertThat(batch.results()).isEmpty();
    assertThat(batch.totalInBatch()).isEqualTo(0);
    assertThat(batch.runningTotal()).isEqualTo(0);
    assertThat(batch.isLastBatch()).isTrue();
  }

  @Test
  void shouldTrackRunningTotal() {
    final List<Result> results = new ArrayList<>();
    results.add(new ResultInternal());
    results.add(new ResultInternal());
    results.add(new ResultInternal());

    final QueryBatch batch = new QueryBatch(results, 3, 15, false);

    assertThat(batch.results()).hasSize(3);
    assertThat(batch.totalInBatch()).isEqualTo(3);
    assertThat(batch.runningTotal()).isEqualTo(15);
  }

  @Test
  void shouldHandleLargeBatch() {
    final List<Result> results = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      results.add(new ResultInternal());
    }

    final QueryBatch batch = new QueryBatch(results, 1000, 5000, false);

    assertThat(batch.results()).hasSize(1000);
    assertThat(batch.totalInBatch()).isEqualTo(1000);
    assertThat(batch.runningTotal()).isEqualTo(5000);
    assertThat(batch.isLastBatch()).isFalse();
  }

  @Test
  void shouldIndicateNotLastBatch() {
    final List<Result> results = new ArrayList<>();
    results.add(new ResultInternal());

    final QueryBatch batch = new QueryBatch(results, 1, 1, false);

    assertThat(batch.isLastBatch()).isFalse();
  }

  @Test
  void shouldIndicateLastBatch() {
    final List<Result> results = new ArrayList<>();
    results.add(new ResultInternal());

    final QueryBatch batch = new QueryBatch(results, 1, 100, true);

    assertThat(batch.isLastBatch()).isTrue();
  }

  @Test
  void shouldProvideResultsList() {
    final List<Result> results = new ArrayList<>();
    final ResultInternal result1 = new ResultInternal();
    result1.setProperty("id", 1);
    final ResultInternal result2 = new ResultInternal();
    result2.setProperty("id", 2);
    results.add(result1);
    results.add(result2);

    final QueryBatch batch = new QueryBatch(results, 2, 2, false);

    assertThat(batch.results()).hasSize(2);
    assertThat(batch.results().get(0).<Integer>getProperty("id")).isEqualTo(1);
    assertThat(batch.results().get(1).<Integer>getProperty("id")).isEqualTo(2);
  }

  @Test
  void shouldHandleBatchCountMismatch() {
    // Batch can have totalInBatch different from actual results size
    final List<Result> results = new ArrayList<>();
    results.add(new ResultInternal());
    results.add(new ResultInternal());

    final QueryBatch batch = new QueryBatch(results, 5, 5, false);

    assertThat(batch.results()).hasSize(2);
    assertThat(batch.totalInBatch()).isEqualTo(5);
  }

  @Test
  void shouldHandleZeroRunningTotal() {
    final List<Result> results = new ArrayList<>();
    results.add(new ResultInternal());

    final QueryBatch batch = new QueryBatch(results, 1, 0, false);

    assertThat(batch.runningTotal()).isEqualTo(0);
  }

  @Test
  void shouldHandleNegativeRunningTotal() {
    // Technically shouldn't happen, but test for robustness
    final List<Result> results = new ArrayList<>();

    final QueryBatch batch = new QueryBatch(results, 0, -1, true);

    assertThat(batch.runningTotal()).isEqualTo(-1);
  }

  @Test
  void shouldCreateMultipleBatchesIndependently() {
    final List<Result> results1 = new ArrayList<>();
    results1.add(new ResultInternal());

    final List<Result> results2 = new ArrayList<>();
    results2.add(new ResultInternal());
    results2.add(new ResultInternal());

    final QueryBatch batch1 = new QueryBatch(results1, 1, 1, false);
    final QueryBatch batch2 = new QueryBatch(results2, 2, 3, false);

    assertThat(batch1.results()).hasSize(1);
    assertThat(batch1.runningTotal()).isEqualTo(1);

    assertThat(batch2.results()).hasSize(2);
    assertThat(batch2.runningTotal()).isEqualTo(3);
  }

  @Test
  void shouldReturnSameResultsList() {
    final List<Result> results = new ArrayList<>();
    results.add(new ResultInternal());

    final QueryBatch batch = new QueryBatch(results, 1, 1, false);

    assertThat(batch.results()).isSameAs(results);
  }
}
