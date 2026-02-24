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
  }

  private TimeSeriesVectorOpsProvider() {
  }

  public static TimeSeriesVectorOps getInstance() {
    return INSTANCE;
  }
}
