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
package com.arcadedb.integration.backup;

import com.arcadedb.exception.QueryTerminatedException;
import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The abort check of {@link IoThrottler} runs on every chunk accounted, with or without a rate cap, and between the slices of
 * a park, so a stop request is seen within a slice instead of at the end of the deadline.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class IoThrottlerAbortCheckTest {

  @Test
  void theCheckRunsOnEveryChunkWithoutARateCap() {
    final AtomicInteger calls = new AtomicInteger();
    final IoThrottler throttler = new IoThrottler(0, calls::incrementAndGet);

    for (int i = 0; i < 5; i++)
      throttler.throttle(64 * 1024);

    assertThat(calls.get()).isEqualTo(5);
  }

  @Test
  void aCheckThatFailsStopsAnUncappedRead() {
    final AtomicInteger calls = new AtomicInteger();
    final IoThrottler throttler = new IoThrottler(0, () -> {
      if (calls.incrementAndGet() == 3)
        throw new QueryTerminatedException("stopped");
    });

    throttler.throttle(1);
    throttler.throttle(1);
    assertThatThrownBy(() -> throttler.throttle(1)).isInstanceOf(QueryTerminatedException.class);
  }

  // A HANG DETECTOR: THE PARK BELOW LASTS ABOUT NINE MINUTES IF NOTHING CUTS IT SHORT
  @Test
  @Timeout(value = 120, unit = TimeUnit.SECONDS)
  void aCheckThatFailsCutsAParkShort() {
    final AtomicInteger calls = new AtomicInteger();
    // 1 MB/S AND 512 MB READ AT ONCE: THE DEADLINE IS 512 SECONDS AWAY
    final IoThrottler throttler = new IoThrottler(1, () -> {
      // THE FIRST CALL IS THE ONE AT THE ENTRY: THE FAILURE HAS TO COME FROM INSIDE THE PARK
      if (calls.incrementAndGet() == 4)
        throw new QueryTerminatedException("stopped");
    });

    final StallAwareStopwatch watch = StallAwareStopwatch.start();
    assertThatThrownBy(() -> throttler.throttle(512L * 1024 * 1024)).isInstanceOf(QueryTerminatedException.class);
    watch.assertGaveUpWithin(30_000, "a park that checks between slices from one that sleeps until its deadline");

    assertThat(calls.get()).isEqualTo(4);
  }

  @Test
  void withoutACheckTheThrottlerBehavesAsBefore() {
    final IoThrottler throttler = new IoThrottler(0);
    throttler.throttle(1L << 30);
    assertThat(throttler.isEnabled()).isFalse();
  }
}
