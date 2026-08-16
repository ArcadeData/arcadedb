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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * Test-only accounting of the wall-clock time during which the whole JVM was not making progress: stop-the-world
 * GC pauses, and CPU starvation severe enough that a runnable thread does not get scheduled for tens of
 * milliseconds at a time. Both are routine late in a 12 000-test single-JVM suite run and both inflate every
 * wall-clock measurement taken in that JVM, whatever the code under test is doing (see #6260).
 * <p>
 * A single daemon thread parks for {@link #TICK_NANOS} at a time and accumulates, into a monotonically
 * increasing counter, however much longer than that each park actually took. The difference of two readings of
 * that counter is therefore the stall observed inside the interval between them, and subtracting it from a
 * wall-clock measurement taken over the same interval leaves the time the JVM was actually running.
 * <p>
 * Deliberately NOT subtracted: time the measured thread spends sleeping, parked or blocked on I/O. The sampler
 * keeps ticking then, so a test that blocks for a second still measures a second. Only a stall that also stops
 * the sampler is discounted.
 * <p>
 * The counter is started lazily, on the first {@link StallAwareStopwatch}, so suites that take no timing
 * measurement pay nothing for it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 * @see StallAwareStopwatch
 */
final class JvmStallMonitor {
  /** How long the sampler parks between two readings. 100 wake-ups a second is not measurable against a test suite. */
  static final long TICK_NANOS = TimeUnit.MILLISECONDS.toNanos(10);

  /**
   * Slack allowed on top of the tick before a gap counts as a stall. {@code parkNanos()} routinely overshoots by
   * a few milliseconds on a healthy machine; charging that as a stall would slowly eat away at every bound.
   */
  static final long TOLERANCE_NANOS = TimeUnit.MILLISECONDS.toNanos(10);

  /** Name of the sampler thread, so a test can confirm it is running. */
  static final String THREAD_NAME = "arcadedb-test-stall-monitor";

  private static final AtomicLong ACCUMULATED_STALL_NANOS = new AtomicLong();

  private JvmStallMonitor() {
  }

  /** Holder idiom: the sampler thread starts on the first read of {@link #accumulatedStallNanos()}, not on class load. */
  private static final class Sampler {
    static {
      final Thread sampler = new Thread(Sampler::sample, THREAD_NAME);
      sampler.setDaemon(true);
      // Deliberately left at the default priority: the sampler is a proxy for the measured thread, so it has to
      // compete for a core on the same terms. Raising it would let it keep ticking through exactly the CPU
      // starvation the measured thread is suffering, which is the half of the problem GC pauses do not cover.
      sampler.start();
    }

    static void ensureStarted() {
      // Referencing this class is what runs the initializer above; there is nothing else to do here.
    }

    private static void sample() {
      long previous = System.nanoTime();
      while (true) {
        LockSupport.parkNanos(TICK_NANOS);
        final long now = System.nanoTime();
        final long stallNanos = stallNanosForGap(now - previous);
        previous = now;
        if (stallNanos > 0)
          ACCUMULATED_STALL_NANOS.addAndGet(stallNanos);
      }
    }
  }

  /**
   * Total stall observed since the sampler started. Only differences between two readings are meaningful; the
   * absolute value is not.
   */
  static long accumulatedStallNanos() {
    Sampler.ensureStarted();
    return ACCUMULATED_STALL_NANOS.get();
  }

  /** Charged as a stall only what exceeds the tick plus the tolerance. Split out so the accounting is testable. */
  static long stallNanosForGap(final long gapNanos) {
    final long excessNanos = gapNanos - TICK_NANOS - TOLERANCE_NANOS;
    return excessNanos > 0 ? excessNanos : 0;
  }
}
