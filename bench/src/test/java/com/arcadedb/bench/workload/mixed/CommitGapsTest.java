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
package com.arcadedb.bench.workload.mixed;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;

import static org.assertj.core.api.Assertions.assertThat;

class CommitGapsTest {
  @Test
  void theLongestGapIncludesTheEdgesOfTheWindow() {
    final CommitGaps gaps = new CommitGaps(0);
    gaps.committed(100);
    gaps.committed(150);
    gaps.committed(400);
    assertThat(gaps.longestGapNanos(450)).isEqualTo(250);
    assertThat(new CommitGaps(0).longestGapNanos(1_000)).isEqualTo(1_000);
  }

  @Test
  void theTrailingGapCounts() {
    final CommitGaps gaps = new CommitGaps(0);
    gaps.committed(10);
    assertThat(gaps.longestGapNanos(1_000)).isEqualTo(990);
  }

  @Test
  void commitsFromManyThreadsAreAllSeen() throws Exception {
    final int threads = 8;
    final int perThread = 1000;
    final CommitGaps gaps = new CommitGaps(0);
    final CountDownLatch start = new CountDownLatch(1);
    final Thread[] workers = new Thread[threads];
    for (int t = 0; t < threads; t++) {
      final int id = t;
      workers[t] = new Thread(() -> {
        try {
          start.await();
        } catch (final InterruptedException e) {
          return;
        }
        // thread t commits at multiples of t + 1 up to (t + 1) * perThread: past 7000 only the thread stepping by 8 is left
        final long step = id + 1;
        for (int i = 1; i <= perThread; i++)
          gaps.committed(step * i);
      });
      workers[t].start();
    }
    start.countDown();
    for (final Thread worker : workers)
      worker.join();

    // below 7000 every gap is at most 7, from 7000 to 8000 only multiples of 8 remain: the longest gap is exactly 8
    assertThat(gaps.longestGapNanos(8_000)).isEqualTo(8);
    // an end past the last commit makes the trailing gap the longest
    assertThat(gaps.longestGapNanos(8_500)).isEqualTo(500);
  }
}
