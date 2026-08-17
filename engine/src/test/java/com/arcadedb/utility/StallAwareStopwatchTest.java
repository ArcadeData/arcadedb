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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Reproduces #6260 and covers its fix: the wall-clock bounds in the engine suite went red on the shared JVM's
 * stop-the-world pauses rather than on the behaviour they test. {@code AsyncShutdownDrainTest} measured 27 396 ms
 * for a {@code close()} bounded by a 1 s timeout plus a 2 s join, and {@code Issue6199DrainWakeupTest} measured
 * 24 143 ms for a wait bounded by 200 ms - both while the code behaved correctly.
 * <p>
 * The stall is injected here rather than provoked, because provoking a multi-second stop-the-world pause on
 * demand is exactly as unreliable as the flake being fixed.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class StallAwareStopwatchTest {

  @Test
  void aStalledJvmNoLongerFailsABoundTheCodeRespected() {
    final AtomicLong stallNanos = new AtomicLong();
    final StallAwareStopwatch stopwatch = StallAwareStopwatch.startWith(stallNanos::get);

    // The observed failure: 27,396ms measured for an operation whose honest cost is a few seconds, because the
    // JVM spent ~24s of the window not running.
    busyElapse(120);
    stallNanos.set(TimeUnit.MILLISECONDS.toNanos(24_000));

    assertThat(stopwatch.stallMs()).isEqualTo(24_000);
    assertThat(stopwatch.effectiveMs()).isZero();

    // Pre-fix this same window failed a 15s bound. It is the JVM's mood that changed, not the code's behaviour.
    stopwatch.assertGaveUpWithin(15_000, "a 1s close timeout from a 60s wedged worker");
    stopwatch.assertStayedUnder(1_000, "one shared budget, not ten independent ones");
  }

  @Test
  void aGenuinelySlowOperationStillFails() {
    // No stall to hide behind: the bound has to still be able to go red, or the fix would have deleted the tests
    // it was meant to save.
    final StallAwareStopwatch stopwatch = StallAwareStopwatch.startWith(() -> 0L);
    busyElapse(120);

    assertThatThrownBy(() -> stopwatch.assertStayedUnder(50, "one shared budget, not ten independent ones"))
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining("loosening it deletes the test");
    assertThatThrownBy(() -> stopwatch.assertGaveUpWithin(50, "a deadline from an unbounded match"))
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining("tripwire");
  }

  @Test
  void aStallLargerThanTheWindowDoesNotProduceNegativeTime() {
    final AtomicLong stallNanos = new AtomicLong();
    final StallAwareStopwatch stopwatch = StallAwareStopwatch.startWith(stallNanos::get);

    busyElapse(20);
    // A stall straddling the start of the window is attributed to it in full, so the discount can legitimately
    // exceed the window itself. It must clamp rather than run negative.
    stallNanos.set(TimeUnit.HOURS.toNanos(1));

    assertThat(stopwatch.effectiveMs()).isZero();
    stopwatch.assertStayedUnder(1, "a stall wider than the window cannot make the measurement negative");
  }

  @Test
  void blockingIsNotDiscountedAsAStall() throws Exception {
    // The measured thread sleeping is not the JVM stalling: the sampler keeps ticking, so the second is charged
    // in full. Anything else and every test that waits on a latch would become unbounded.
    final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
    Thread.sleep(1_000);

    assertThat(stopwatch.elapsedMs()).isGreaterThanOrEqualTo(1_000);
    // Deliberately lenient: this test runs on the same shared JVM as everything else, so it must not itself
    // depend on the machine being idle. Discounting 90% of a plain sleep would take real starvation, and that
    // is the one case where discounting it is the right answer anyway.
    assertThat(stopwatch.effectiveMs())
        .as("a sleeping test thread must still be charged for the time it slept")
        .isGreaterThan(100);
  }

  @Test
  void routineParkJitterIsNotChargedAsAStall() {
    assertThat(JvmStallMonitor.stallNanosForGap(JvmStallMonitor.TICK_NANOS)).isZero();
    assertThat(JvmStallMonitor.stallNanosForGap(JvmStallMonitor.TICK_NANOS + JvmStallMonitor.TOLERANCE_NANOS)).isZero();
    assertThat(JvmStallMonitor.stallNanosForGap(0)).isZero();
  }

  @Test
  void aGapWiderThanTheToleranceIsChargedInFull() {
    final long gapNanos = TimeUnit.SECONDS.toNanos(24) + JvmStallMonitor.TICK_NANOS + JvmStallMonitor.TOLERANCE_NANOS;

    assertThat(JvmStallMonitor.stallNanosForGap(gapNanos)).isEqualTo(TimeUnit.SECONDS.toNanos(24));
  }

  @Test
  void theMonitorStartsOnFirstUseAndOnlyEverAccumulates() {
    final long before = JvmStallMonitor.accumulatedStallNanos();
    busyElapse(50);
    final long after = JvmStallMonitor.accumulatedStallNanos();

    assertThat(before).isNotNegative();
    assertThat(after).as("readings must be monotonic, or differencing them would be meaningless").isGreaterThanOrEqualTo(before);
    assertThat(Thread.getAllStackTraces().keySet().stream().anyMatch(t -> JvmStallMonitor.THREAD_NAME.equals(t.getName())))
        .as("reading the counter must have started the sampler")
        .isTrue();
  }

  /**
   * Issue #6270: the sampler must not be named like engine machinery. The leak detectors that hunt for engine
   * background threads scan by these prefixes, and this thread is a test utility - it escapes them today only
   * because they skip daemons first, which one edit (a shutdown path making it non-daemon) would undo.
   */
  @Test
  void theSamplerIsNamedOutsideTheEnginesThreadNamespace() {
    assertThat(JvmStallMonitor.THREAD_NAME)
        .as("a leak detector scanning for engine threads must not collect the stall sampler")
        .doesNotStartWith("ArcadeDB")
        .doesNotStartWith("AsyncExecutor-")
        .doesNotStartWith("arcadedb-");
  }

  /** Burns wall-clock time without parking, so the window is real for both the stopwatch and the sampler. */
  private static void busyElapse(final long millis) {
    final long untilNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(millis);
    while (System.nanoTime() < untilNanos)
      Thread.onSpinWait();
  }
}
