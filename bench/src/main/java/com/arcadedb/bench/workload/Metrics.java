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
package com.arcadedb.bench.workload;

import com.arcadedb.bench.model.Metric;
import com.arcadedb.bench.runner.LatencyRecorder;

import com.arcadedb.bench.server.ServerUnderTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.function.LongSupplier;

/**
 * Builders of the single-repetition metrics a workload returns.
 */
public final class Metrics {
  private static final Logger LOGGER = LoggerFactory.getLogger(Metrics.class);

  private static final double NANOS_PER_MS     = 1_000_000.0;
  private static final long   SETTLE_POLL_MS   = 250L;
  private static final long   SETTLE_BOUND_MS  = 30_000L;

  private Metrics() {
  }

  public static Metric of(final String name, final String unit, final Metric.Better better, final double value) {
    return new Metric(name, unit, better, value, new double[] { value });
  }

  public static Metric rate(final String name, final String unit, final double value) {
    return of(name, unit, Metric.Better.HIGHER, value);
  }

  public static Metric bytes(final String name, final double value) {
    return of(name, "bytes", Metric.Better.LOWER, value);
  }

  public static Metric seconds(final String name, final double value) {
    return of(name, "s", Metric.Better.LOWER, value);
  }

  /** The three latency percentiles of the recorder in milliseconds: {@code latency_p50_ms}, {@code _p95_ms}, {@code _p99_ms}. */
  public static List<Metric> latencies(final LatencyRecorder recorder) {
    return List.of(of("latency_p50_ms", "ms", Metric.Better.LOWER, recorder.percentile(50) / NANOS_PER_MS),
        of("latency_p95_ms", "ms", Metric.Better.LOWER, recorder.percentile(95) / NANOS_PER_MS),
        of("latency_p99_ms", "ms", Metric.Better.LOWER, recorder.percentile(99) / NANOS_PER_MS));
  }

  /**
   * The size of a database directory once the asynchronous page flush has finished: polls every 250 ms until two consecutive
   * reads agree, for at most 30 s.
   */
  public static long settledDiskBytes(final ServerUnderTest server, final String database) {
    return settle(() -> server.diskBytes(database), SETTLE_POLL_MS, SETTLE_BOUND_MS);
  }

  /** Polls {@code reader} until two consecutive values are equal; after {@code boundMs} returns the last value and warns. */
  static long settle(final LongSupplier reader, final long pollMs, final long boundMs) {
    final long deadline = System.nanoTime() + boundMs * 1_000_000L;
    long previous = reader.getAsLong();
    while (true) {
      if (System.nanoTime() >= deadline) {
        LOGGER.warn("Disk size did not settle within {} ms, using the last read: {}", boundMs, previous);
        return previous;
      }
      try {
        Thread.sleep(pollMs);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        return previous;
      }
      final long current = reader.getAsLong();
      if (current == previous)
        return current;
      previous = current;
    }
  }
}
