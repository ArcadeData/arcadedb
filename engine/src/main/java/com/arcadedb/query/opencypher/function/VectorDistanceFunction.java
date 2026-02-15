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
package com.arcadedb.query.opencypher.function;

import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.function.StatelessFunction;
import com.arcadedb.query.sql.executor.CommandContext;

/**
 * vector_distance(vectorA, vectorB, [metric]) - calculates the distance between two vectors.
 * Supported metrics: EUCLIDEAN (default), COSINE, MANHATTAN.
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

    final float[] a = CypherFunctionHelper.toFloatArray(args[0]);
    final float[] b = CypherFunctionHelper.toFloatArray(args[1]);

    if (a.length != b.length)
      throw new CommandExecutionException("vector_distance(): vectors must have the same dimension, got " + a.length + " and " + b.length);

    final String metric = args.length > 2 && args[2] != null ? args[2].toString().toUpperCase() : "EUCLIDEAN";

    return switch (metric) {
      case "EUCLIDEAN" -> euclideanDistance(a, b);
      case "COSINE" -> cosineDistance(a, b);
      case "MANHATTAN" -> manhattanDistance(a, b);
      default -> throw new CommandExecutionException("vector_distance(): unsupported metric: " + metric
          + ". Supported metrics: EUCLIDEAN, COSINE, MANHATTAN");
    };
  }

  private static double euclideanDistance(final float[] a, final float[] b) {
    double sum = 0;
    for (int i = 0; i < a.length; i++) {
      final double diff = a[i] - b[i];
      sum += diff * diff;
    }
    return Math.sqrt(sum);
  }

  private static double cosineDistance(final float[] a, final float[] b) {
    double dot = 0, normA = 0, normB = 0;
    for (int i = 0; i < a.length; i++) {
      dot += a[i] * b[i];
      normA += (double) a[i] * a[i];
      normB += (double) b[i] * b[i];
    }
    if (normA == 0 || normB == 0)
      return 0.0;
    return 1.0 - (dot / (Math.sqrt(normA) * Math.sqrt(normB)));
  }

  private static double manhattanDistance(final float[] a, final float[] b) {
    double sum = 0;
    for (int i = 0; i < a.length; i++)
      sum += Math.abs(a[i] - b[i]);
    return sum;
  }
}
