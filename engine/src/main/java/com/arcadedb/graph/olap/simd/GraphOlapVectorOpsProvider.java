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
package com.arcadedb.graph.olap.simd;

import com.arcadedb.log.LogManager;

import java.util.logging.Level;

/**
 * Singleton provider for {@link GraphOlapVectorOps}.
 * Tries to load the SIMD implementation at class init time; falls back to scalar.
 */
public final class GraphOlapVectorOpsProvider {

  private static final GraphOlapVectorOps INSTANCE;
  private static final boolean            SIMD_ENABLED;

  static {
    GraphOlapVectorOps ops;
    boolean simd = false;
    try {
      ops = new SimdGraphOlapVectorOps();
      // Quick smoke test — verify gather returns correct results
      final int[] src = { 10, 20, 30, 40, 50 };
      final int[] idx = { 4, 2, 0 };
      final int[] dst = new int[3];
      ops.gatherInt(src, idx, dst, 0, 3);
      if (dst[0] != 50 || dst[1] != 30 || dst[2] != 10)
        throw new IllegalStateException("SIMD gather smoke test failed");

      // Smoke test aggregation
      final double sumResult = ops.sumDouble(new double[] { 1.0, 2.0, 3.0 }, 0, 3);
      if (sumResult != 6.0)
        throw new IllegalStateException("SIMD sum smoke test failed: expected 6.0 but got " + sumResult);

      simd = true;
      LogManager.instance().log(GraphOlapVectorOpsProvider.class, Level.INFO, "Graph-OLAP SIMD vector ops enabled");
    } catch (final Exception | LinkageError t) {
      ops = new ScalarGraphOlapVectorOps();
      LogManager.instance()
          .log(GraphOlapVectorOpsProvider.class, Level.WARNING,
              "Graph-OLAP SIMD not available, using scalar fallback (reason: %s). "
                  + "For better performance, add '--add-modules jdk.incubator.vector' to JVM flags",
              t.getMessage());
    }
    INSTANCE = ops;
    SIMD_ENABLED = simd;
  }

  private GraphOlapVectorOpsProvider() {
  }

  public static GraphOlapVectorOps getInstance() {
    return INSTANCE;
  }

  /**
   * Returns true if the SIMD implementation is active (not the scalar fallback).
   */
  public static boolean isSimdEnabled() {
    return SIMD_ENABLED;
  }
}
