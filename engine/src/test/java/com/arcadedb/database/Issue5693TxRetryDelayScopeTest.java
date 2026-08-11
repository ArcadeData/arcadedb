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

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * TX_RETRY_DELAY has database scope, so the backoff between transaction retries must come from the database's
 * own configuration and not from the global static, which ignores any per-database override.
 * <p>
 * The assertions compare an in-run baseline (the same retry loop with no backoff configured) against the same
 * loop with the backoff raised, so nothing depends on an absolute wall-clock budget. When the retry loop reads
 * the global static the two runs are indistinguishable and the gap collapses to zero.
 * <p>
 * Since #5587, the per-attempt window is {@code min(TX_RETRY_DELAY, TX_RETRY_DELAY_BASE * 2^attempt)} instead
 * of a flat {@code TX_RETRY_DELAY}. Both settings are set to the same {@link #RAISED_DELAY_MS} here, which
 * collapses that formula back to a flat window on every attempt ({@code min(x, x * 2^n) == x}) - the scope
 * behaviour under test does not depend on the backoff shape, only on which configuration wins, so pinning the
 * shape keeps this test's timing budget identical to before #5587.
 */
public class Issue5693TxRetryDelayScopeTest extends TestHelper {
  /** One more than the number of forced failures, so the last attempt succeeds. */
  private static final int ATTEMPTS         = 7;
  /** Each of the {@link #ATTEMPTS} - 1 backoffs sleeps a random 1..RAISED_DELAY_MS, so the sum dwarfs the gap. */
  private static final int RAISED_DELAY_MS  = 500;
  /** Well below the (ATTEMPTS - 1) * RAISED_DELAY_MS / 2 expected sum, and unreachable without the backoff. */
  private static final int MIN_GAP_MS       = 250;

  @Test
  void theRetryLoopUsesTheDelayConfiguredOnThisDatabase() {
    final int savedGlobal = GlobalConfiguration.TX_RETRY_DELAY.getValueAsInteger();
    final int savedGlobalBase = GlobalConfiguration.TX_RETRY_DELAY_BASE.getValueAsInteger();
    GlobalConfiguration.TX_RETRY_DELAY.setValue(0);
    try {
      // BASELINE TAKEN IN-RUN: NO BACKOFF ANYWHERE, SO THIS IS THE COST OF THE RETRY LOOP ITSELF
      database.getConfiguration().setValue(GlobalConfiguration.TX_RETRY_DELAY, 0);
      final long baselineMs = timeForcedRetries();

      // ONLY THE DATABASE IS RETUNED: THE GLOBAL STATIC STAYS AT 0
      database.getConfiguration().setValue(GlobalConfiguration.TX_RETRY_DELAY, RAISED_DELAY_MS);
      database.getConfiguration().setValue(GlobalConfiguration.TX_RETRY_DELAY_BASE, RAISED_DELAY_MS);
      final long observedMs = timeForcedRetries();

      assertThat(database.getConfiguration().getValueAsInteger(GlobalConfiguration.TX_RETRY_DELAY)).isEqualTo(
          RAISED_DELAY_MS);
      assertThat(observedMs - baselineMs).isGreaterThan(MIN_GAP_MS);
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.TX_RETRY_DELAY, savedGlobal);
      database.getConfiguration().setValue(GlobalConfiguration.TX_RETRY_DELAY_BASE, savedGlobalBase);
      GlobalConfiguration.TX_RETRY_DELAY.setValue(savedGlobal);
    }
  }

  @Test
  void theGlobalDelayStillAppliesWhenTheDatabaseDoesNotOverrideIt() {
    final int savedGlobal = GlobalConfiguration.TX_RETRY_DELAY.getValueAsInteger();
    final int savedGlobalBase = GlobalConfiguration.TX_RETRY_DELAY_BASE.getValueAsInteger();
    GlobalConfiguration.TX_RETRY_DELAY.setValue(0);
    try {
      final long baselineMs = timeForcedRetries();

      // NOTHING IS SET ON THE DATABASE, SO THE GLOBAL VALUE MUST STILL BE HONOURED
      GlobalConfiguration.TX_RETRY_DELAY.setValue(RAISED_DELAY_MS);
      GlobalConfiguration.TX_RETRY_DELAY_BASE.setValue(RAISED_DELAY_MS);
      final long observedMs = timeForcedRetries();

      assertThat(observedMs - baselineMs).isGreaterThan(MIN_GAP_MS);
    } finally {
      GlobalConfiguration.TX_RETRY_DELAY.setValue(savedGlobal);
      GlobalConfiguration.TX_RETRY_DELAY_BASE.setValue(savedGlobalBase);
    }
  }

  /**
   * Runs a transaction that fails with a retryable error on every attempt but the last, and returns how long
   * the whole retry loop took in milliseconds.
   */
  private long timeForcedRetries() {
    final AtomicInteger attempt = new AtomicInteger();
    final long start = System.nanoTime();
    database.transaction(() -> {
      if (attempt.incrementAndGet() < ATTEMPTS)
        throw new ConcurrentModificationException("forced retry");
    }, false, ATTEMPTS);
    assertThat(attempt.get()).isEqualTo(ATTEMPTS);
    return (System.nanoTime() - start) / 1_000_000;
  }
}
