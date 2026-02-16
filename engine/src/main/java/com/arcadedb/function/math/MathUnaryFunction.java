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
package com.arcadedb.function.math;

import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.function.StatelessFunction;
import com.arcadedb.query.sql.executor.CommandContext;

import java.util.function.DoubleUnaryOperator;

/**
 * Generic math unary function (ceil, floor, abs, sqrt, round).
 */
public class MathUnaryFunction implements StatelessFunction {
  private final String name;
  private final DoubleUnaryOperator op;

  public MathUnaryFunction(final String name, final DoubleUnaryOperator op) {
    this.name = name;
    this.op = op;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    if (args.length != 1)
      throw new CommandExecutionException(name + "() requires exactly one argument");
    if (args[0] == null)
      return null;
    if (args[0] instanceof Number) {
      final double result = op.applyAsDouble(((Number) args[0]).doubleValue());
      // Return integer type if the result is a whole number
      if (result == Math.floor(result) && !Double.isInfinite(result))
        return (long) result;
      return result;
    }
    throw new CommandExecutionException(name + "() requires a numeric argument");
  }
}
