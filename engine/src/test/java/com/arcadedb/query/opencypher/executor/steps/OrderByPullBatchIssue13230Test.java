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
package com.arcadedb.query.opencypher.executor.steps;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #13230: the sequential top-K of ORDER BY ... LIMIT pulls its input ten times the LIMIT at a time, at least 1000. The
 * product was computed in int and wrapped for a large LIMIT; saturating it at Integer.MAX_VALUE instead would make LIMIT
 * 2147483647 ask for the whole input in one batch, which the step upstream buffers (issue #8591). The batch is bounded whatever
 * the LIMIT, and an ordinary LIMIT keeps the batch it always had.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class OrderByPullBatchIssue13230Test {

  @Test
  void anOrdinaryLimitKeepsTenTimesTheLimitAtLeast1000() {
    assertThat(OrderByStep.pullBatchSize(1)).isEqualTo(1000);
    assertThat(OrderByStep.pullBatchSize(100)).isEqualTo(1000);
    assertThat(OrderByStep.pullBatchSize(5_000)).isEqualTo(50_000);
    assertThat(OrderByStep.pullBatchSize(10_000)).isEqualTo(100_000);
  }

  @Test
  void aLargeLimitIsBoundedAndNeverWrapsToASmallBatch() {
    assertThat(OrderByStep.MAX_PULL_BATCH).isEqualTo(100_000);
    for (final int limit : new int[] { 10_001, 1_000_000, 1 << 29, Integer.MAX_VALUE / 10, Integer.MAX_VALUE / 10 + 1,
        Integer.MAX_VALUE - 1, Integer.MAX_VALUE })
      assertThat(OrderByStep.pullBatchSize(limit)).as("LIMIT " + limit).isEqualTo(OrderByStep.MAX_PULL_BATCH);
  }
}
