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
package com.arcadedb.function.coll;

import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.function.StatelessFunction;
import com.arcadedb.function.cypher.CypherFunctionHelper;
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
  public int getMinArgs() {
    return 2;
  }

  @Override
  public int getMaxArgs() {
    return 3;
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    checkArity(args);
    // Validate that arguments are integers, not floats. These are client-side type errors, so they carry a
    // CommandSemanticException (HTTP 400) rather than a CommandExecutionException (HTTP 500). See issue #5477.
    for (int i = 0; i < args.length; i++) {
      if (args[i] == null)
        throw new CommandSemanticException("Type mismatch: range() does not accept a null argument");
      if (!(args[i] instanceof Number) || args[i] instanceof Double || args[i] instanceof Float)
        throw CypherFunctionHelper.typeMismatch("range", "an INTEGER", args[i]);
    }
    final long start = ((Number) args[0]).longValue();
    final long end = ((Number) args[1]).longValue();
    final long step = args.length == 3 ? ((Number) args[2]).longValue() : 1L;

    if (step == 0)
      throw new CommandExecutionException("range() step cannot be zero");

    final List<Long> result = new ArrayList<>();
    if (step > 0) {
      for (long i = start; i <= end; ) {
        result.add(i);
        if (i > Long.MAX_VALUE - step)
          break;
        i += step;
      }
    } else {
      for (long i = start; i >= end; ) {
        result.add(i);
        // Long.MIN_VALUE - step wraps to 0L when step = Long.MIN_VALUE (two's-complement), which is still a correct underflow boundary.
        if (i < Long.MIN_VALUE - step)
          break;
        i += step;
      }
    }
    return result;
  }
}
