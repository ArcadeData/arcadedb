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
package com.arcadedb.engine.timeseries.simd;

import com.arcadedb.log.LogManager;

import java.util.logging.Level;

/**
 * Singleton provider for {@link TimeSeriesVectorOps}.
 * Tries to load the SIMD implementation at class init time; falls back to scalar.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class TimeSeriesVectorOpsProvider {

  private static final int                 WARM_UP_VALUES     = 65_536;
  private static final int                 WARM_UP_ITERATIONS = 200;
  private static final TimeSeriesVectorOps INSTANCE;

  static {
    TimeSeriesVectorOps ops;
    try {
      ops = new SimdTimeSeriesVectorOps();
      // Quick smoke test — verify the implementation returns correct results
      final double smokeResult = ops.sum(new double[] { 1.0, 2.0 }, 0, 2);
      if (smokeResult != 3.0)
        throw new IllegalStateException("SIMD smoke test failed: expected 3.0 but got " + smokeResult);
      LogManager.instance().log(TimeSeriesVectorOpsProvider.class, Level.INFO, "TimeSeries SIMD vector ops enabled");
    } catch (final Exception | LinkageError t) {
      ops = new ScalarTimeSeriesVectorOps();
      LogManager.instance()
          .log(TimeSeriesVectorOpsProvider.class, Level.INFO, "TimeSeries SIMD not available, using scalar fallback: %s",
              t.getMessage());
    }
    INSTANCE = ops;
    if (ops instanceof SimdTimeSeriesVectorOps)
      startWarmUp(ops);
  }

  private static void startWarmUp(final TimeSeriesVectorOps ops) {
    // The Vector API runs through its own Java lane loops until C2 compiles the calls (hundreds of ms on a couple of
    // cores): doing it here, off the query path, keeps the first aggregate of a process from paying for it (#9171)
    final Thread thread = new Thread(() -> {
      try {
        warmUp(ops, WARM_UP_ITERATIONS);
      } catch (final Throwable t) {
        // BEST EFFORT: THE WARM-UP ONLY SPEEDS UP THE FIRST QUERY
        LogManager.instance().log(TimeSeriesVectorOpsProvider.class, Level.FINE, "TimeSeries SIMD warm-up failed: %s", t.getMessage());
      }
    }, "ArcadeDB-TimeSeriesSimdWarmUp");
    thread.setDaemon(true);
    thread.setPriority(Thread.MIN_PRIORITY);
    thread.start();
  }

  /**
   * Exercises the hot aggregation operations (double and long columns) of {@code ops} on a block-sized array so the JIT compiles them.
   *
   * @return a checksum of the results, only meant to keep the calls from being eliminated
   */
  static double warmUp(final TimeSeriesVectorOps ops, final int iterations) {
    final double[] data = new double[WARM_UP_VALUES];
    for (int i = 0; i < data.length; i++)
      data[i] = i % 17 == 0 ? Double.NaN : i;
    final long[] longs = new long[WARM_UP_VALUES];
    for (int i = 0; i < longs.length; i++)
      longs[i] = i;
    double sink = 0;
    for (int i = 0; i < iterations; i++)
      sink += ops.sumLong(longs, 0, longs.length) + ops.minLong(longs, 0, longs.length) + ops.maxLong(longs, 0, longs.length);
    for (int i = 0; i < iterations; i++)
      sink += ops.sum(data, 0, data.length) + ops.countPresent(data, 0, data.length) + ops.min(data, 0, data.length) + ops.max(data, 0,
          data.length);
    return sink;
  }

  private TimeSeriesVectorOpsProvider() {
  }

  public static TimeSeriesVectorOps getInstance() {
    return INSTANCE;
  }
}
