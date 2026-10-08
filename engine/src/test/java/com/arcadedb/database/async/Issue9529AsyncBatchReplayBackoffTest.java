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
package com.arcadedb.database.async;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression for issue #9529: the replay {@code commitBatch()} runs after a {@link ConcurrentModificationException}
 * (issue #7615) used to start immediately, so with several workers appending to the tail page of one bucket the
 * replay raced the very workers that had just caused the conflict, and about 1 run in 100 on two cores ran out of
 * {@link GlobalConfiguration#TX_RETRIES}. Like {@code LocalDatabase#transaction}, the replay now waits the jittered
 * exponential backoff of {@link GlobalConfiguration#TX_RETRY_DELAY} / {@link GlobalConfiguration#TX_RETRY_DELAY_BASE}
 * (issue #5587) before every attempt after the first.
 * <p>
 * The wait is observed through {@link DatabaseAsyncExecutorImpl#TEST_BATCH_RETRY_DELAY_HOOK} rather than timed: the
 * delay is a random draw, and a wall-clock bound would only measure the JVM's mood.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9529AsyncBatchReplayBackoffTest extends TestHelper {

  private static final String TYPE = "Issue9529Item";

  @Override
  protected void beginTest() {
    database.getSchema().createDocumentType(TYPE);
  }

  private void insert(final int total) {
    final AsyncResultsetCallback cb = new AsyncResultsetCallback() {
      @Override
      public void onComplete(final ResultSet rs) {
      }

      @Override
      public void onError(final Exception e) {
      }
    };
    for (int i = 0; i < total; i++)
      database.async().command("sql", "INSERT INTO " + TYPE + " SET seq = :seq", cb, Map.of("seq", i));
  }

  @Test
  void everyReplayWaitsABackoffWithinItsWindowBeforeRunning() throws Exception {
    final int total = 10;
    final int base = 40;
    final int cap = 100;

    database.getConfiguration().setValue(GlobalConfiguration.TX_RETRY_DELAY_BASE, base);
    database.getConfiguration().setValue(GlobalConfiguration.TX_RETRY_DELAY, cap);

    database.async().setParallelLevel(1);
    database.async().setCommitEvery(total);
    final AtomicInteger errors = new AtomicInteger();
    database.async().onError(e -> errors.incrementAndGet());

    final List<Long> delays = new CopyOnWriteArrayList<>();
    final AtomicInteger commitCalls = new AtomicInteger();
    DatabaseAsyncExecutorImpl.TEST_BATCH_RETRY_DELAY_HOOK = delays::add;
    DatabaseAsyncExecutorImpl.TEST_BEFORE_BATCH_COMMIT_HOOK = callNumber -> {
      if (commitCalls.incrementAndGet() <= 2)
        throw new ConcurrentModificationException("simulated conflict " + callNumber);
    };

    try {
      insert(total);
      database.async().waitCompletion();
    } finally {
      DatabaseAsyncExecutorImpl.TEST_BEFORE_BATCH_COMMIT_HOOK = null;
      DatabaseAsyncExecutorImpl.TEST_BATCH_RETRY_DELAY_HOOK = null;
      database.getConfiguration().setValue(GlobalConfiguration.TX_RETRY_DELAY_BASE, GlobalConfiguration.TX_RETRY_DELAY_BASE.getDefValue());
      database.getConfiguration().setValue(GlobalConfiguration.TX_RETRY_DELAY, GlobalConfiguration.TX_RETRY_DELAY.getDefValue());
    }

    assertThat(errors.get()).as("two conflicts are within the default retries").isZero();
    assertThat(delays).as("one backoff per replay, none before the first attempt").hasSize(2);
    // retry 0 draws from [1, base], retry 1 from [1, min(cap, 2 * base)]
    assertThat(delays.get(0)).isBetween(1L, (long) base);
    assertThat(delays.get(1)).isBetween(1L, (long) Math.min(cap, 2 * base));
    database.transaction(() -> assertThat(database.countType(TYPE, true)).isEqualTo(total));
  }

  @Test
  void noBackoffWithoutConflict() throws Exception {
    database.async().setParallelLevel(1);
    database.async().setCommitEvery(5);

    final AtomicInteger delays = new AtomicInteger();
    DatabaseAsyncExecutorImpl.TEST_BATCH_RETRY_DELAY_HOOK = delay -> delays.incrementAndGet();
    try {
      insert(23);
      database.async().waitCompletion();
    } finally {
      DatabaseAsyncExecutorImpl.TEST_BATCH_RETRY_DELAY_HOOK = null;
    }

    assertThat(delays.get()).isZero();
    database.transaction(() -> assertThat(database.countType(TYPE, true)).isEqualTo(23));
  }

  @Test
  void zeroRetryDelayDisablesTheBackoff() throws Exception {
    database.getConfiguration().setValue(GlobalConfiguration.TX_RETRY_DELAY, 0);

    database.async().setParallelLevel(1);
    database.async().setCommitEvery(5);

    final AtomicInteger delays = new AtomicInteger();
    final AtomicInteger commitCalls = new AtomicInteger();
    DatabaseAsyncExecutorImpl.TEST_BATCH_RETRY_DELAY_HOOK = delay -> delays.incrementAndGet();
    DatabaseAsyncExecutorImpl.TEST_BEFORE_BATCH_COMMIT_HOOK = callNumber -> {
      if (commitCalls.incrementAndGet() == 1)
        throw new ConcurrentModificationException("simulated conflict");
    };

    try {
      insert(5);
      database.async().waitCompletion();
    } finally {
      DatabaseAsyncExecutorImpl.TEST_BEFORE_BATCH_COMMIT_HOOK = null;
      DatabaseAsyncExecutorImpl.TEST_BATCH_RETRY_DELAY_HOOK = null;
      database.getConfiguration().setValue(GlobalConfiguration.TX_RETRY_DELAY, GlobalConfiguration.TX_RETRY_DELAY.getDefValue());
    }

    assertThat(delays.get()).isZero();
    database.transaction(() -> assertThat(database.countType(TYPE, true)).isEqualTo(5));
  }

  /**
   * The scenario of the report: four workers sharing the tail page of one bucket. Probabilistic by nature (the
   * failure was ~1% on two cores before the fix), so it is a guard against the replay racing its own cause, not a
   * proof - the hook-driven tests above are the deterministic ones.
   */
  @Test
  void fourWorkersOnOneBucketLoseNoBatch() throws Exception {
    final int total = 9_742;

    database.async().setParallelLevel(4);
    database.async().setCommitEvery(1_000);
    final AtomicInteger errors = new AtomicInteger();
    database.async().onError(e -> errors.incrementAndGet());

    insert(total);
    database.async().waitCompletion();

    assertThat(errors.get()).isZero();
    database.transaction(() -> assertThat(database.countType(TYPE, true)).isEqualTo(total));
  }
}
