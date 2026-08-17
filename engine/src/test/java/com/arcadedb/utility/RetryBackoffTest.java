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
package com.arcadedb.utility;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Covers the exponential-backoff-with-full-jitter window computation added for #5587: the window widens with
 * every failed attempt instead of staying flat, and saturates at the configured cap instead of growing
 * unbounded.
 */
class RetryBackoffTest {

  @Test
  void windowDoublesWithEachAttemptUntilItHitsTheCap() {
    final long base = 2;
    final long cap = 1000;

    assertThat(RetryBackoff.windowMs(0, base, cap)).isEqualTo(2);
    assertThat(RetryBackoff.windowMs(1, base, cap)).isEqualTo(4);
    assertThat(RetryBackoff.windowMs(2, base, cap)).isEqualTo(8);
    assertThat(RetryBackoff.windowMs(3, base, cap)).isEqualTo(16);
    assertThat(RetryBackoff.windowMs(4, base, cap)).isEqualTo(32);
  }

  @Test
  void windowSaturatesAtTheCapAndNeverExceedsIt() {
    final long base = 2;
    final long cap = 100;

    // 2 * 2^6 = 128 > 100, so attempt 6 is the first to saturate
    assertThat(RetryBackoff.windowMs(6, base, cap)).isEqualTo(cap);
    assertThat(RetryBackoff.windowMs(7, base, cap)).isEqualTo(cap);
    assertThat(RetryBackoff.windowMs(50, base, cap)).isEqualTo(cap);
  }

  @Test
  void windowNeverExceedsTheCapEvenForAVeryLargeAttemptCount() {
    // A naive base << attempt overflows a long well before attempt reaches Integer.MAX_VALUE; the window
    // must still saturate at the cap instead of wrapping to a negative or otherwise bogus value.
    assertThat(RetryBackoff.windowMs(Integer.MAX_VALUE, 2, 500)).isEqualTo(500);
    assertThat(RetryBackoff.windowMs(200, 2, 500)).isEqualTo(500);
  }

  @Test
  void aNonPositiveCapMeansNoBackoffAtAll() {
    assertThat(RetryBackoff.windowMs(0, 2, 0)).isEqualTo(0);
    assertThat(RetryBackoff.windowMs(5, 2, -1)).isEqualTo(0);
  }

  @Test
  void aNonPositiveBaseFallsBackToOneMillisecondInsteadOfNeverGrowing() {
    assertThat(RetryBackoff.windowMs(0, 0, 1000)).isEqualTo(1);
    assertThat(RetryBackoff.windowMs(3, -5, 1000)).isEqualTo(8);
  }

  @Test
  void aNegativeAttemptIsTreatedAsTheFirstAttempt() {
    assertThat(RetryBackoff.windowMs(-1, 2, 1000)).isEqualTo(RetryBackoff.windowMs(0, 2, 1000));
  }

  @Test
  void sleepReturnsImmediatelyWhenTheCapIsNotPositive() {
    final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
    RetryBackoff.sleep(3, 2, 0);
    stopwatch.assertStayedUnder(50, "a non-positive cap meaning no sleep at all");
  }

  @Test
  void sleepNeverWaitsLongerThanTheWindowForThatAttempt() {
    final long base = 2;
    final long cap = 200;
    final int attempt = 2; // window = min(200, 2*2^2) = 8ms

    final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
    RetryBackoff.sleep(attempt, base, cap);

    // Generous upper bound: the window is 8ms, so anything close to the cap (200ms) would indicate the
    // implementation is not actually scaling the sleep down for an early attempt.
    stopwatch.assertStayedUnder(cap, "this attempt's 8ms window, not the 200ms cap");
  }

  @Test
  void sleepRestoresTheInterruptFlagInsteadOfSwallowingIt() throws InterruptedException {
    final AtomicBoolean interruptedAfterSleep = new AtomicBoolean(false);
    final Thread worker = new Thread(() -> {
      Thread.currentThread().interrupt();
      RetryBackoff.sleep(0, 2, 5_000);
      interruptedAfterSleep.set(Thread.currentThread().isInterrupted());
    });
    worker.start();
    worker.join(5_000);

    assertThat(worker.isAlive()).isFalse();
    assertThat(interruptedAfterSleep.get()).isTrue();
  }
}
