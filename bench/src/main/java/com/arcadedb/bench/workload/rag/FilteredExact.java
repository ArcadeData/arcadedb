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
package com.arcadedb.bench.workload.rag;

import com.arcadedb.bench.workload.VectorRecall;

/**
 * Exact nearest neighbors restricted to the vectors a caller is allowed to see, the ground truth of a filtered vector search.
 */
public final class FilteredExact {
  private FilteredExact() {
  }

  /**
   * The ids of the {@code min(k, visibleCount)} visible vectors most similar to {@code query} by cosine, best first.
   */
  public static int[] topK(final float[][] base, final double[] norms, final boolean[] visible, final float[] query, final int k) {
    return VectorRecall.exactTopK(base, norms, visible, query, k);
  }

  /**
   * The fraction of {@code exact} that {@code approx} contains: the divisor is {@code exact.length}, which is the visible count
   * when fewer than k vectors are visible.
   */
  public static double recall(final int[] approx, final int[] exact) {
    return VectorRecall.recall(approx, exact);
  }
}
