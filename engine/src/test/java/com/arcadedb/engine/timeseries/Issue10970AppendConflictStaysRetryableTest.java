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
package com.arcadedb.engine.timeseries;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.schema.LocalTimeSeriesType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #10970: an append whose commit keeps losing a conflict (for example a Raft refusal because the
 * transaction was prepared before a schema change, which tells the caller to retry) used to leave
 * {@link TimeSeriesShard#appendSamples} wrapped in a plain {@code IOException} once its own retries were spent. A caller keying
 * on {@link NeedRetryException} - the HTTP layer answering 503 - never saw it, so the client did not retry and the points were
 * lost.
 */
class Issue10970AppendConflictStaysRetryableTest extends TestHelper {

  @AfterEach
  void cleanupHook() {
    TimeSeriesShard.TEST_APPEND_COMMIT_FAULT = null;
  }

  @Test
  void exhaustedConflictRetriesSurfaceAsRetryable() {
    database.command("sql", "CREATE TIMESERIES TYPE sensor TIMESTAMP ts FIELDS (v DOUBLE) SHARDS 1");
    final TimeSeriesEngine engine = ((LocalTimeSeriesType) database.getSchema().getType("sensor")).getEngine();

    final AtomicInteger attempts = new AtomicInteger();
    TimeSeriesShard.TEST_APPEND_COMMIT_FAULT = () -> {
      attempts.incrementAndGet();
      throw new ConcurrentModificationException("prepared before the schema change. Please retry the operation");
    };

    assertThatThrownBy(() -> engine.appendSamples(new long[] { 1L, 2L }, new Object[] { 1.0, 2.0 }))
        .isInstanceOf(NeedRetryException.class);
    assertThat(attempts.get()).as("the append retried before giving up").isEqualTo(3);
  }

  @Test
  void conflictClearingWithinTheRetriesLosesNothing() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE sensor TIMESTAMP ts FIELDS (v DOUBLE) SHARDS 1");
    final TimeSeriesEngine engine = ((LocalTimeSeriesType) database.getSchema().getType("sensor")).getEngine();

    final AtomicInteger attempts = new AtomicInteger();
    TimeSeriesShard.TEST_APPEND_COMMIT_FAULT = () -> {
      if (attempts.incrementAndGet() < 3)
        throw new ConcurrentModificationException("transient conflict");
    };

    engine.appendSamples(new long[] { 1L, 2L }, new Object[] { 1.0, 2.0 });

    TimeSeriesShard.TEST_APPEND_COMMIT_FAULT = null;
    final long count = ((Number) database.query("sql", "SELECT count(*) AS cnt FROM sensor").next().getProperty("cnt")).longValue();
    assertThat(count).isEqualTo(2L);
  }
}
