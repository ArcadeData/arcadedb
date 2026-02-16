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

import java.util.function.DoubleBinaryOperator;

/**
 * Generic math binary function (e.g. atan2).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class MathBinaryFunction implements StatelessFunction {
  private final String name;
  private final DoubleBinaryOperator op;

  public MathBinaryFunction(final String name, final DoubleBinaryOperator op) {
    this.name = name;
    this.op = op;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    if (args.length != 2)
      throw new CommandExecutionException(name + "() requires exactly two arguments");
    if (args[0] == null || args[1] == null)
      return null;
    if (args[0] instanceof Number && args[1] instanceof Number) {
      final double result = op.applyAsDouble(((Number) args[0]).doubleValue(), ((Number) args[1]).doubleValue());
      if (result == Math.floor(result) && !Double.isInfinite(result))
        return (long) result;
      return result;
    }
    throw new CommandExecutionException(name + "() requires numeric arguments");
  }
}
