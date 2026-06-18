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
  private static final AtomicLong lastWarningNanos     = new AtomicLong(Long.MIN_VALUE);

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
        "Skipped ghost edge during traversal (%s); total skipped since startup=%d",
        cause != null ? cause.getMessage() : null, total);

    final long now = System.nanoTime();
    final long last = lastWarningNanos.get();
    if (now - last >= WARNING_WINDOW_NANOS && lastWarningNanos.compareAndSet(last, now))
      LogManager.instance().log(GhostEdgeReporter.class, Level.WARNING,
          "Ghost (dangling) edges encountered and skipped during traversal (total skipped since startup=%d). "
              + "A dangling edge-segment pointer indicates a data-integrity anomaly, e.g. incomplete HA "
              + "replication or a partially rolled-back transaction.", total);
  }

  /** Total ghost edges skipped since JVM startup (monotonic). */
  public static long getTotalSkipped() {
    return totalSkipped.get();
  }

  // @VisibleForTesting (package-private: only GhostEdgeReporterTest, in this package, calls it)
  static void resetForTests() {
    totalSkipped.set(0);
    lastWarningNanos.set(Long.MIN_VALUE);
  }
}
