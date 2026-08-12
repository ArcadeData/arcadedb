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
package com.arcadedb.database;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.exception.ConcurrentModificationException;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for #5587: before this fix, every {@code database.transaction(...)} retry slept a flat
 * {@code U(1, TX_RETRY_DELAY)} interval no matter how many times the transaction had already lost, so a
 * transaction on attempt 50 had the same odds of winning the next race as one on attempt 1. This proves the
 * per-attempt window now widens (exponential backoff with full jitter,
 * {@code min(TX_RETRY_DELAY, TX_RETRY_DELAY_BASE * 2^attempt)}) instead of staying flat.
 * <p>
 * The {@code error} callback fires once per failed attempt, right before the backoff sleep, so the wall-clock
 * gap between consecutive callback invocations is (up to negligible retry-loop overhead) that attempt's sleep.
 * Comparing the sum of the first few gaps against the sum of the last few isolates the backoff's growth from
 * the retry loop's own cost, the same in-run-comparison approach {@code Issue5693TxRetryDelayScopeTest} uses to
 * avoid depending on an absolute wall-clock budget.
 */
public class Issue5587TxRetryExponentialBackoffTest extends TestHelper {
  /** One more than the number of forced failures, so the last attempt succeeds. */
  private static final int ATTEMPTS = 10;
  /** Small enough that early windows (2, 4, 8, 16 ms) stay tiny... */
  private static final int BASE_MS  = 2;
  /** ...and large enough that the late windows (32, 64, 128, 256 ms) never saturate against it. */
  private static final int CAP_MS   = 1_000;

  @Test
  void laterRetriesBackOffFurtherThanEarlierOnes() {
    final int savedCap = GlobalConfiguration.TX_RETRY_DELAY.getValueAsInteger();
    final int savedBase = GlobalConfiguration.TX_RETRY_DELAY_BASE.getValueAsInteger();
    database.getConfiguration().setValue(GlobalConfiguration.TX_RETRY_DELAY, CAP_MS);
    database.getConfiguration().setValue(GlobalConfiguration.TX_RETRY_DELAY_BASE, BASE_MS);
    try {
      final List<Long> failureTimestampsNanos = new ArrayList<>();
      final AtomicInteger attempt = new AtomicInteger();

      database.transaction(() -> {
        if (attempt.incrementAndGet() < ATTEMPTS)
          throw new ConcurrentModificationException("forced retry");
      }, false, ATTEMPTS, null, e -> failureTimestampsNanos.add(System.nanoTime()));

      assertThat(attempt.get()).isEqualTo(ATTEMPTS);
      // one error callback per failed attempt, i.e. every attempt but the last
      assertThat(failureTimestampsNanos).hasSize(ATTEMPTS - 1);

      final List<Long> gapsMs = new ArrayList<>();
      for (int i = 1; i < failureTimestampsNanos.size(); i++)
        gapsMs.add((failureTimestampsNanos.get(i) - failureTimestampsNanos.get(i - 1)) / 1_000_000);

      final int half = gapsMs.size() / 2;
      final long earlyGapsSumMs = gapsMs.subList(0, half).stream().mapToLong(Long::longValue).sum();
      final long lateGapsSumMs = gapsMs.subList(gapsMs.size() - half, gapsMs.size()).stream().mapToLong(Long::longValue).sum();

      // Expected sums are ~17ms (early, windows 2/4/8/16) vs ~242ms (late, windows 32/64/128/256) - a flat
      // delay (the pre-#5587 behaviour) would make the two statistically indistinguishable instead.
      assertThat(lateGapsSumMs)
          .as("later retries (gaps %s) should back off further than earlier ones (gaps %s)", gapsMs.subList(gapsMs.size() - half, gapsMs.size()),
              gapsMs.subList(0, half))
          .isGreaterThan(earlyGapsSumMs * 2);
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.TX_RETRY_DELAY, savedCap);
      database.getConfiguration().setValue(GlobalConfiguration.TX_RETRY_DELAY_BASE, savedBase);
    }
  }

  @Test
  void aZeroCapDisablesTheDelayEntirelyJustLikeBeforeTheFix() {
    final int savedCap = GlobalConfiguration.TX_RETRY_DELAY.getValueAsInteger();
    database.getConfiguration().setValue(GlobalConfiguration.TX_RETRY_DELAY, 0);
    try {
      final AtomicInteger attempt = new AtomicInteger();
      final long start = System.nanoTime();

      database.transaction(() -> {
        if (attempt.incrementAndGet() < ATTEMPTS)
          throw new ConcurrentModificationException("forced retry");
      }, false, ATTEMPTS);

      final long elapsedMs = (System.nanoTime() - start) / 1_000_000;
      assertThat(attempt.get()).isEqualTo(ATTEMPTS);
      assertThat(elapsedMs).isLessThan(1_000);
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.TX_RETRY_DELAY, savedCap);
    }
  }
}
