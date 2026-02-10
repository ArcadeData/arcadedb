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

import java.util.ArrayList;
import java.util.List;

/**
 * range() function - creates a list of numbers from start to end (optionally with step).
 */
public class RangeFunction implements StatelessFunction {
  @Override
  public String getName() {
    return "range";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    if (args.length < 2 || args.length > 3)
      throw new CommandExecutionException("range() requires 2 or 3 arguments: range(start, end) or range(start, end, step)");
    // Validate that arguments are integers, not floats
    for (int i = 0; i < args.length; i++) {
      if (args[i] instanceof Double || args[i] instanceof Float)
        throw new CommandExecutionException("InvalidArgumentType: range() requires integer arguments, got float for argument " + (i + 1));
      if (args[i] == null)
        throw new CommandExecutionException("InvalidArgumentType: range() does not accept null arguments");
      if (!(args[i] instanceof Number))
        throw new CommandExecutionException("InvalidArgumentType: range() requires integer arguments, got " + args[i].getClass().getSimpleName());
    }
    final long start = ((Number) args[0]).longValue();
    final long end = ((Number) args[1]).longValue();
    final long step = args.length == 3 ? ((Number) args[2]).longValue() : 1L;

    if (step == 0)
      throw new CommandExecutionException("range() step cannot be zero");

    final List<Long> result = new ArrayList<>();
    if (step > 0) {
      for (long i = start; i <= end; i += step)
        result.add(i);
    } else {
      for (long i = start; i >= end; i += step)
        result.add(i);
    }
    return result;
  }
}
