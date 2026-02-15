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
 * vector.norm(vector) - returns the L2 norm (magnitude) of a vector.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class VectorNormFunction implements StatelessFunction {
  @Override
  public String getName() {
    return "vector.norm";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    if (args.length != 1)
      throw new CommandExecutionException("vector.norm() requires exactly one argument");
    if (args[0] == null)
      return null;

    final float[] v = CypherFunctionHelper.toFloatArray(args[0]);
    double sum = 0;
    for (final float f : v)
      sum += (double) f * f;
    return Math.sqrt(sum);
  }
}
