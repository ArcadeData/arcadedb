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
import java.util.function.LongSupplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A stopwatch for tests that have to bound an operation in wall-clock time, reporting the elapsed time with the
 * JVM-wide stalls inside the measurement window taken out of it (see {@link JvmStallMonitor}).
 * <p>
 * The problem it solves (#6260): late in a 12 000-test single-JVM run a stop-the-world pause of tens of seconds
 * is not exotic, so any bound with less headroom than that becomes a coin flip on the JVM's mood rather than a
 * statement about the code. Loosening the bound is not always available as a fix - several of these bounds are
 * discriminating between a shared 200 ms deadline and ten independent ones, so they have to stay below 2 s to
 * mean anything. Subtracting the stall keeps the bound tight AND makes it immune to the pause.
 * <p>
 * The two assertion methods differ only in the failure message, and that is the point: the message has to say
 * what the number is for, so that the next person to see it go red knows whether loosening it is a repair or a
 * cover-up.
 * <ul>
 * <li>{@link #assertGaveUpWithin} - the bound separates a bounded operation from an unbounded one. It is a
 * tripwire; anywhere between the real limit and the unbounded behaviour will do, and generous is free.</li>
 * <li>{@link #assertStayedUnder} - the bound IS the assertion, standing in for a complexity claim that has no
 * other practical expression. Loosening it deletes the test.</li>
 * </ul>
 * A short wait that is expected to TIME OUT needs neither: a stall only makes it more true.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class StallAwareStopwatch {
  private final LongSupplier stallNanos;
  private final long         beginNanos;
  private final long         beginStallNanos;

  private StallAwareStopwatch(final LongSupplier stallNanos) {
    this.stallNanos = stallNanos;
    this.beginStallNanos = stallNanos.getAsLong();
    this.beginNanos = System.nanoTime();
  }

  /** Starts measuring now. */
  public static StallAwareStopwatch start() {
    return new StallAwareStopwatch(JvmStallMonitor::accumulatedStallNanos);
  }

  /** Starts measuring against a caller-supplied stall source, so the accounting can be driven deterministically. */
  static StallAwareStopwatch startWith(final LongSupplier stallNanos) {
    return new StallAwareStopwatch(stallNanos);
  }

  /**
   * The raw stall counter, for tests that time a sequence of instants rather than one window and so cannot use a
   * stopwatch: sample it alongside each {@code System.nanoTime()} and difference both to get a stall-free gap.
   * Only differences between two readings are meaningful.
   */
  public static long jvmStallNanos() {
    return JvmStallMonitor.accumulatedStallNanos();
  }

  /** Raw wall-clock time since {@link #start()}, stalls included. */
  public long elapsedMs() {
    return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - beginNanos);
  }

  /** How much of {@link #elapsedMs()} the JVM as a whole spent not running. */
  public long stallMs() {
    return TimeUnit.NANOSECONDS.toMillis(Math.max(0L, stallNanos.getAsLong() - beginStallNanos));
  }

  /** {@link #elapsedMs()} without the stall: the time the operation could actually have been using. */
  public long effectiveMs() {
    return Math.max(0L, elapsedMs() - stallMs());
  }

  /**
   * Asserts the operation gave up rather than running to its unbounded end. The bound is a tripwire between the
   * two, not a latency budget, so it should sit well clear of both sides.
   *
   * @param boundMs         the tripwire
   * @param whatItSeparates what the bound discriminates, e.g. "a 200ms deadline from an unbounded match"
   */
  public StallAwareStopwatch assertGaveUpWithin(final long boundMs, final String whatItSeparates) {
    return check(boundMs, "gave up within %,d ms", whatItSeparates,
        "This bound is a tripwire separating " + whatItSeparates + ", not a latency budget: widening it is safe, "
            + "narrowing it is what would break.");
  }

  /**
   * Asserts the operation stayed under a bound that IS the assertion - the only practical expression of a
   * complexity claim (a regex that must not backtrack exponentially, a deadline shared across rows instead of
   * charged per row).
   *
   * @param boundMs the bound the claim rests on
   * @param claim   what the bound proves, e.g. "one shared 200ms budget, not ten independent per-row ones"
   */
  public StallAwareStopwatch assertStayedUnder(final long boundMs, final String claim) {
    return check(boundMs, "stayed under %,d ms", claim,
        "This bound IS the assertion (" + claim + "): loosening it deletes the test. If it is red on a healthy "
            + "machine, the behaviour regressed.");
  }

  private StallAwareStopwatch check(final long boundMs, final String what, final String subject, final String advice) {
    // The two reads are separate snapshots, and the order matters: sampling the stall AFTER the elapsed time means
    // the stall window can only be wider than the elapsed one, never narrower, so the discount can only come out
    // too generous. Read the other way round it could come out too small and fail a run that was fine.
    final long elapsedMs = elapsedMs();
    final long stallMs = stallMs();
    assertThat(Math.max(0L, elapsedMs - stallMs))
        .as(what + " [%s]%n  measured %,d ms, of which %,d ms the JVM was stalled (stop-the-world pause or CPU "
            + "starvation, discounted).%n  %s", boundMs, subject, elapsedMs, stallMs, advice)
        .isLessThan(boundMs);
    return this;
  }
}
