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
 * vector.distance.euclidean(vectorA, vectorB) - calculates the Euclidean distance between two vectors.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class VectorDistanceEuclideanFunction implements StatelessFunction {
  @Override
  public String getName() {
    return "vector.distance.euclidean";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    if (args.length != 2)
      throw new CommandExecutionException("vector.distance.euclidean() requires exactly 2 arguments");
    if (args[0] == null || args[1] == null)
      return null;

    final float[] a = CypherFunctionHelper.toFloatArray(args[0]);
    final float[] b = CypherFunctionHelper.toFloatArray(args[1]);

    if (a.length != b.length)
      throw new CommandExecutionException("vector.distance.euclidean(): vectors must have the same dimension");

    double sum = 0;
    for (int i = 0; i < a.length; i++) {
      final double diff = a[i] - b[i];
      sum += diff * diff;
    }
    return Math.sqrt(sum);
  }
}
