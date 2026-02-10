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
 * vector.similarity.euclidean(a, b) - computes euclidean similarity between two vectors.
 * Returns 1 / (1 + euclidean_distance) per Neo4j semantics.
 */
public class VectorSimilarityEuclideanFunction implements StatelessFunction {
  @Override
  public String getName() {
    return "vector.similarity.euclidean";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    if (args.length != 2)
      throw new CommandExecutionException("vector.similarity.euclidean() requires exactly 2 arguments");
    if (args[0] == null || args[1] == null)
      return null;
    final float[] v1 = CypherFunctionHelper.toFloatArray(args[0]);
    final float[] v2 = CypherFunctionHelper.toFloatArray(args[1]);
    if (v1.length != v2.length)
      throw new CommandExecutionException("vector.similarity.euclidean() requires vectors of the same dimension");
    double sumSquares = 0.0;
    for (int i = 0; i < v1.length; i++) {
      final double diff = v1[i] - v2[i];
      sumSquares += diff * diff;
    }
    final double distance = Math.sqrt(sumSquares);
    return 1.0 / (1.0 + distance);
  }
}
