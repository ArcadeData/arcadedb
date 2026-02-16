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
package com.arcadedb.function.vector;

import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.function.StatelessFunction;
import com.arcadedb.index.vector.VectorUtils;
import com.arcadedb.query.sql.executor.CommandContext;

/**
 * vector_distance(vectorA, vectorB, [metric]) - calculates the distance between two vectors.
 * Supported metrics: EUCLIDEAN (default), COSINE, MANHATTAN.
 * Delegates to {@link VectorUtils} for SIMD-optimized computation.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class VectorDistanceFunction implements StatelessFunction {
  @Override
  public String getName() {
    return "vector_distance";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    if (args.length < 2 || args.length > 3)
      throw new CommandExecutionException("vector_distance() requires 2 or 3 arguments (vectorA, vectorB, [metric])");
    if (args[0] == null || args[1] == null)
      return null;

    final float[] a = VectorUtils.toFloatArray(args[0]);
    final float[] b = VectorUtils.toFloatArray(args[1]);
    VectorUtils.validateSameDimension(a, b);

    final String metric = args.length > 2 && args[2] != null ? args[2].toString().toUpperCase() : "EUCLIDEAN";

    return switch (metric) {
      case "EUCLIDEAN" -> (double) VectorUtils.l2Distance(a, b);
      case "COSINE" -> 1.0 - (double) VectorUtils.cosineSimilarity(a, b);
      case "MANHATTAN" -> (double) VectorUtils.manhattanDistance(a, b);
      default -> throw new CommandExecutionException("vector_distance(): unsupported metric: " + metric
          + ". Supported metrics: EUCLIDEAN, COSINE, MANHATTAN");
    };
  }
}
