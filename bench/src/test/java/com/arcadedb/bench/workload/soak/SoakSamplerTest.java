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
package com.arcadedb.bench.workload.soak;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

class SoakSamplerTest {
  private static final long     SECOND = 1_000_000_000L;
  /** A soak window longer than any test's samples: nothing is cut at its end. */
  private static final Duration DAY    = Duration.ofDays(1);

  /** A source that reads nothing: the tests below inject their samples. */
  private static final SoakSampler.Source NOTHING = new SoakSampler.Source() {
    @Override
    public String scrape() {
      return null;
    }

    @Override
    public int threads() {
      return -1;
    }

    @Override
    public int files() {
      return -1;
    }
  };

  @Test
  void parsesTheHeapLinesOfAPrometheusScrape() {
    final String scrape = """
        # HELP jvm_memory_used_bytes The amount of used memory
        # TYPE jvm_memory_used_bytes gauge
        jvm_memory_used_bytes{area="heap",id="G1 Eden Space"} 1.2E7
        jvm_memory_used_bytes{area="nonheap",id="Metaspace"} 2.6066296E7
        jvm_memory_used_bytes{area="heap",id="G1 Old Gen"} 3000000.0
        jvm_memory_used_bytes{area="nonheap",id="CodeHeap 'non-nmethods'"} 1501696.0
        jvm_memory_used_bytes{area="heap",id="G1 Survivor Space"} 500.0
        jvm_memory_max_bytes{area="heap",id="G1 Old Gen"} 2.147483648E9
        jvm_threads_live_threads 39.0
        """;
    assertThat(SoakSampler.heapUsed(scrape)).isEqualTo(12_000_000L + 3_000_000L + 500L);
    // a scrape without the series is no reading, never a heap of zero
    assertThat(SoakSampler.heapUsed("jvm_threads_live_threads 39.0\n")).isEqualTo(-1L);
  }

  @Test
  void theFloorOfAWindowIsItsMinimum() {
    final SoakSampler sampler = new SoakSampler(NOTHING, Duration.ofSeconds(10), Duration.ofMinutes(5), Duration.ZERO, DAY);
    // 10 minutes, a sample every 10 s: a sawtooth whose bottom is 100 in the first 5 minutes and 200 in the next 5
    for (int i = 0; i < 60; i++) {
      final long base = i < 30 ? 100 : 200;
      sampler.add(i * 10 * SECOND, base + (i % 7) * 10, 40, 100);
    }
    // a failed scrape in the second window is left out, not taken as a floor of -1
    sampler.add(600 * SECOND, -1, 40, 100);
    final double[][] floors = sampler.heapFloors();
    assertThat(floors).hasNumberOfRows(2);
    assertThat(floors[0][0]).isCloseTo(2.5 / 60, within(1e-12));
    assertThat(floors[0][1]).isEqualTo(100.0);
    assertThat(floors[1][0]).isCloseTo(7.5 / 60, within(1e-12));
    assertThat(floors[1][1]).isEqualTo(200.0);
  }

  @Test
  void theWindowsStartWhereTheWarmUpEnds() {
    final SoakSampler sampler = new SoakSampler(NOTHING, Duration.ofSeconds(10), Duration.ofMinutes(5), Duration.ofMinutes(2), DAY);
    // 12 minutes: the first 2 are warm-up, low readings that must not become a floor or a median; then 10 minutes at 200
    for (int i = 0; i < 72; i++)
      sampler.add(i * 10 * SECOND, i < 12 ? 50 : 200, i < 12 ? 5 : 40, i < 12 ? 9 : 100);
    final double[][] floors = sampler.heapFloors();
    assertThat(floors).hasNumberOfRows(2);
    assertThat(floors[0][0]).isCloseTo(4.5 / 60, within(1e-12));
    assertThat(floors[0][1]).isEqualTo(200.0);
    final double[][] threads = sampler.threads();
    assertThat(threads).hasNumberOfRows(10);
    assertThat(threads[0][0]).isCloseTo(2.5 / 60, within(1e-12));
    for (final double[] pair : threads)
      assertThat(pair[1]).isEqualTo(40.0);
    for (final double[] pair : sampler.files())
      assertThat(pair[1]).isEqualTo(100.0);
  }

  @Test
  void aReplayedSamplerSummarizesItsSamples() {
    final SoakSampler sampler = SoakSampler.replay(Duration.ofSeconds(10), Duration.ofMinutes(1), Duration.ZERO, DAY,
        new long[] { 0, 10 * SECOND, 20 * SECOND, 30 * SECOND, 40 * SECOND, 50 * SECOND }, new long[] { 7, 5, 9, 6, 8, 5 },
        new int[] { 1, 1, 1, 1, 1, 1 }, new int[] { 2, 2, 2, 2, 2, 2 });
    assertThat(sampler.samples()).isEqualTo(6);
    assertThat(sampler.heapFloors()).hasNumberOfRows(1);
    assertThat(sampler.heapFloors()[0][1]).isEqualTo(5.0);
  }

  @Test
  void samplesAfterTheWindowEndAreIgnored() {
    final SoakSampler sampler = new SoakSampler(NOTHING, Duration.ofSeconds(10), Duration.ofMinutes(5), Duration.ZERO,
        Duration.ofMinutes(10));
    // 15 minutes of samples for a 10 minute window: the last 5 are the drain, loops stopped, the heap and threads way down
    for (int i = 0; i < 90; i++)
      sampler.add(i * 10 * SECOND, i < 60 ? 100 : 10, i < 60 ? 40 : 20, i < 60 ? 100 : 50);
    final double[][] floors = sampler.heapFloors();
    assertThat(floors).hasNumberOfRows(2);
    assertThat(floors[0][1]).isEqualTo(100.0);
    assertThat(floors[1][1]).isEqualTo(100.0);
    final double[][] threads = sampler.threads();
    assertThat(threads).hasNumberOfRows(10);
    for (final double[] pair : threads)
      assertThat(pair[1]).isEqualTo(40.0);
    for (final double[] pair : sampler.files())
      assertThat(pair[1]).isEqualTo(100.0);
  }

  @Test
  void anInterruptedStopStillWaitsForTheSampleUnderWayAndKeepsTheInterrupt() throws Exception {
    final CountDownLatch inScrape = new CountDownLatch(1);
    final AtomicBoolean finished = new AtomicBoolean();
    final SoakSampler.Source slow = new SoakSampler.Source() {
      @Override
      public String scrape() {
        inScrape.countDown();
        // a scrape that takes 300 ms and does not end early on an interrupt
        final long until = System.nanoTime() + 300_000_000L;
        while (System.nanoTime() < until)
          Thread.onSpinWait();
        return "jvm_memory_used_bytes{area=\"heap\",id=\"young\"} 1000.0\n";
      }

      @Override
      public int threads() {
        return 30;
      }

      @Override
      public int files() {
        finished.set(true);
        return 50;
      }
    };
    final SoakSampler sampler = new SoakSampler(slow, Duration.ofSeconds(10), Duration.ofMillis(20), Duration.ZERO, DAY);
    sampler.start();
    inScrape.await();
    // the runner cancelled the case: the measure thread arrives at close() with its interrupt set
    Thread.currentThread().interrupt();
    sampler.close();
    final boolean kept = Thread.interrupted();
    assertThat(kept).isTrue();
    assertThat(finished).isTrue();
    assertThat(sampler.samples()).isEqualTo(1);
  }

  @Test
  void anIncompleteLastWindowIsLeftOut() {
    final SoakSampler sampler = new SoakSampler(NOTHING, Duration.ofSeconds(10), Duration.ofMinutes(5), Duration.ZERO, DAY);
    // 7 minutes: one complete floor window, the second one only 2 minutes in
    for (int i = 0; i < 42; i++)
      sampler.add(i * 10 * SECOND, 100 + i, 40, 100);
    assertThat(sampler.heapFloors()).hasNumberOfRows(1);
    assertThat(sampler.threads()).hasNumberOfRows(7);
  }

  @Test
  void threadsAndFilesAreMedianPerMinute() {
    final SoakSampler sampler = new SoakSampler(NOTHING, Duration.ofSeconds(10), Duration.ofMinutes(5), Duration.ZERO, DAY);
    // minute 0: threads 10,11,12,13,14,90 (one spike) -> median 12.5; minute 1: 20 throughout, files unreadable in it
    final int[] first = { 10, 11, 12, 13, 14, 90 };
    for (int i = 0; i < 6; i++)
      sampler.add(i * 10 * SECOND, 1, first[i], 7);
    for (int i = 6; i < 12; i++)
      sampler.add(i * 10 * SECOND, 1, 20, -1);
    final double[][] threads = sampler.threads();
    assertThat(threads).hasNumberOfRows(2);
    assertThat(threads[0][1]).isEqualTo(12.5);
    assertThat(threads[1][1]).isEqualTo(20.0);
    assertThat(threads[1][0]).isCloseTo(1.5 / 60, within(1e-12));
    final double[][] files = sampler.files();
    assertThat(files).hasNumberOfRows(1);
    assertThat(files[0][1]).isEqualTo(7.0);
  }

  @Test
  void theSamplingThreadReadsTheSourceUntilStopped() throws Exception {
    final AtomicInteger reads = new AtomicInteger();
    final AtomicBoolean closed = new AtomicBoolean();
    final SoakSampler.Source source = new SoakSampler.Source() {
      @Override
      public String scrape() {
        reads.incrementAndGet();
        return "jvm_memory_used_bytes{area=\"heap\",id=\"young\"} 1000.0\n";
      }

      @Override
      public int threads() {
        return 30;
      }

      @Override
      public int files() {
        return 50;
      }

      @Override
      public void close() {
        closed.set(true);
      }
    };
    final SoakSampler sampler = new SoakSampler(source, Duration.ofMillis(5), Duration.ofMillis(20), Duration.ZERO, DAY);
    sampler.start();
    while (reads.get() < 10)
      Thread.sleep(5);
    sampler.close();
    final int taken = sampler.samples();
    assertThat(taken).isGreaterThanOrEqualTo(10);
    assertThat(closed).isTrue();
    // stopped means stopped: no sample lands after close()
    Thread.sleep(50);
    assertThat(sampler.samples()).isEqualTo(taken);
    assertThat(sampler.heapFloors()[0][1]).isEqualTo(1000.0);
  }
}
