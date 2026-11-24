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

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * The coarse clock is read by the page cache's eviction throttle (#12971): a clock that stops ticking would latch the throttle
 * off for good, and the read cache would grow without bound. Nothing in the JVM owns the ticker thread, but an application
 * server cleaning up threads, or a thread dump tool, can interrupt it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CoarseClockTest {
  private static final String TICKER = "ArcadeDB-CoarseClock";

  @Test
  void theClockAdvances() {
    final long start = CoarseClock.currentTimeMillis();
    await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertThat(CoarseClock.currentTimeMillis()).isGreaterThan(start));
  }

  @Test
  void theClockStaysCloseToTheRealOne() {
    await().atMost(Duration.ofSeconds(30)).untilAsserted(
        () -> assertThat(System.currentTimeMillis() - CoarseClock.currentTimeMillis()).isLessThan(CoarseClock.RESOLUTION_MS * 20));
  }

  @Test
  void aTickerThreadThatIsInterruptedKeepsTicking() {
    final Thread ticker = Thread.getAllStackTraces().keySet().stream().filter(t -> TICKER.equals(t.getName())).findFirst()
        .orElseThrow(() -> new AssertionError("the ticker thread of the coarse clock is not running"));

    ticker.interrupt();

    final long afterInterrupt = CoarseClock.currentTimeMillis();
    await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
      assertThat(ticker.isAlive()).as("an interrupt must not kill the ticker: the clock would freeze for the rest of the JVM").isTrue();
      assertThat(CoarseClock.currentTimeMillis()).as("the clock must keep advancing after the ticker was interrupted")
          .isGreaterThan(afterInterrupt);
    });
  }
}
