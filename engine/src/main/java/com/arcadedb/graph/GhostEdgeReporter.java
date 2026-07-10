/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.graph;

import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.log.LogManager;

import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

/**
 * Central, low-overhead reporter for <i>ghost edges</i>: dangling edge-segment pointers whose backing
 * edge record no longer exists (e.g. after incomplete HA replication or a partially rolled-back
 * transaction). Traversal and graph-algorithm code skips such edges rather than failing the whole
 * query; routing every skip through here makes the anomaly observable instead of silently swallowed.
 * <p>
 * Each skip is counted and logged at {@code FINE} (full per-occurrence trace, off by default, so there
 * is no cost on clean graphs because this is only reached when a ghost is actually encountered). In
 * addition a {@code WARNING} is emitted at most once per 60s - mirroring the engine's other throttled
 * saturation logs - so operators notice a data-integrity problem without log flooding on a heavily
 * corrupted graph.
 */
public final class GhostEdgeReporter {
  private static final long       WARNING_WINDOW_NANOS = 60_000_000_000L; // 60s
  private static final AtomicLong totalSkipped         = new AtomicLong();
  // Seeded one full window in the past (NOT Long.MIN_VALUE): the throttle compares with the overflow-safe
  // 'now - last >= WINDOW'. Long.MIN_VALUE is not a real nanoTime reading, so 'now - Long.MIN_VALUE' overflows
  // to a negative value on any JVM where System.nanoTime() is positive, and the first WARNING would never fire.
  // -WARNING_WINDOW_NANOS is within a real reading's range, so the first ghost encountered always warns.
  private static final AtomicLong lastWarningNanos     = new AtomicLong(-WARNING_WINDOW_NANOS);

  private GhostEdgeReporter() {
  }

  /**
   * Records and logs that a ghost edge was skipped during traversal.
   *
   * @param cause the {@link RecordNotFoundException} raised when the dangling edge was dereferenced
   */
  public static void reportSkipped(final RecordNotFoundException cause) {
    final long total = totalSkipped.incrementAndGet();

    LogManager.instance().log(GhostEdgeReporter.class, Level.FINE,
        "Skipped ghost edge during traversal (%s); total skipped since startup=%d", cause.getMessage(), total);

    if (shouldEmitWarning(System.nanoTime()))
      LogManager.instance().log(GhostEdgeReporter.class, Level.WARNING,
          """
          Ghost (dangling) edges encountered and skipped during traversal (total skipped since startup=%d). \
          A dangling edge-segment pointer indicates a data-integrity anomaly, e.g. incomplete HA \
          replication or a partially rolled-back transaction.""", total);
  }

  /**
   * Decides, with overflow-safe elapsed arithmetic, whether a throttled WARNING should fire at {@code now}
   * and, if so, claims the slot atomically (CAS) so concurrent skips emit at most one WARNING per window.
   * <p>
   * Package-private ({@code @VisibleForTesting}) so the throttle can be exercised directly without capturing
   * log output: the previous {@code Long.MIN_VALUE} seed made {@code now - last} overflow and suppressed the
   * WARNING forever, a bug invisible to a test that only asserts on the skip count.
   *
   * @param now a {@link System#nanoTime()} reading
   * @return {@code true} iff this caller won the slot and should log the WARNING
   */
  static boolean shouldEmitWarning(final long now) {
    final long last = lastWarningNanos.get();
    return now - last >= WARNING_WINDOW_NANOS && lastWarningNanos.compareAndSet(last, now);
  }

  /** Total ghost edges skipped since JVM startup (monotonic). */
  public static long getTotalSkipped() {
    return totalSkipped.get();
  }

  /**
   * Resets the static counters so each test starts from a clean slate.
   * <p>
   * Package-private on purpose ({@code @VisibleForTesting}): it is only reachable from
   * {@code com.arcadedb.graph.GhostEdgeReporterTest}, which lives in this same package. If that test is
   * ever moved out of {@code com.arcadedb.graph}, this method must be widened (or the test kept in-package).
   */
  static void resetForTests() {
    totalSkipped.set(0);
    lastWarningNanos.set(-WARNING_WINDOW_NANOS);
  }
}
