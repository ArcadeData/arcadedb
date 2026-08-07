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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.function.StatelessFunction;
import com.arcadedb.function.cypher.CypherFunctionHelper;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.utility.LongRangeList;

/**
 * range() function - creates a list of numbers from start to end (optionally with step).
 * <p>
 * The list is lazy ({@link LongRangeList}): it stores only start, step and size, so a range costs a constant
 * amount of heap no matter how long it is and consumers that stream it (UNWIND, IN, size(), indexing) allocate
 * nothing. Ranges bigger than {@link GlobalConfiguration#QUERY_MAX_RANGE_SIZE} are rejected up-front with a
 * client error: they would exhaust the heap as soon as something materialises them (sorting, copying, rendering
 * the result in a response). See advisory GHSA-xmjm-8q85-g778.
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

    final long cardinality = LongRangeList.cardinality(start, end, step);
    final long maxAllowed = maxRangeSize(context);

    // A java.util.List cannot hold more than Integer.MAX_VALUE elements, so that is the hard ceiling even when the
    // configured limit is disabled. Both checks are a client error (HTTP 400): the query asks for something that
    // cannot be served, it is not an internal failure.
    if (cardinality > Integer.MAX_VALUE && (maxAllowed < 0 || Integer.MAX_VALUE < maxAllowed))
      throw new CommandSemanticException(
          "range(" + start + ", " + end + ", " + step + ") would produce " + cardinality + " elements, more than the "
              + Integer.MAX_VALUE + " a list can hold: use a smaller range");

    if (maxAllowed >= 0 && cardinality > maxAllowed)
      throw new CommandSemanticException(
          "range(" + start + ", " + end + ", " + step + ") would produce " + cardinality + " elements, exceeding the maximum of "
              + maxAllowed + " allowed by the setting `" + GlobalConfiguration.QUERY_MAX_RANGE_SIZE.getKey()
              + "`: use a smaller range or raise the limit (a negative value disables it)");

    return new LongRangeList(start, step, (int) cardinality);
  }

  private static long maxRangeSize(final CommandContext context) {
    final Database database = context == null ? null : context.getDatabase();
    return database == null ?
        GlobalConfiguration.QUERY_MAX_RANGE_SIZE.getValueAsLong() :
        database.getConfiguration().getValueAsLong(GlobalConfiguration.QUERY_MAX_RANGE_SIZE);
  }
}
