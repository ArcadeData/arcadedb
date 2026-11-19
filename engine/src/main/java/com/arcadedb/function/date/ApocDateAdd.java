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
package com.arcadedb.function.date;

import com.arcadedb.exception.InvalidArgumentException;
import com.arcadedb.function.cypher.CypherFunctionHelper;
import com.arcadedb.query.sql.executor.CommandContext;

/**
 * apoc.date.add(time, unit, addValue, addUnit) - Add {@code addValue} {@code addUnit} to a time in {@code unit}, as APOC's
 * apoc.date.add does (issue #12106): the amount is converted to {@code unit} first, truncating toward zero and saturating at
 * the long range.
 * <p>
 * Only APOC's four-argument signature. ArcadeDB's three-argument date.add(timestamp, value, unit), on a timestamp in
 * milliseconds and with the amount second, is {@link DateAdd}. The two signatures live under different names, so no call is
 * told apart by its argument count, and an APOC call missing its last argument is an arity error.
 * <p>
 * Deviations from APOC: a null time or amount is null, where APOC fails (its parameters are primitive {@code long}); a sum
 * outside the long range is an error, where APOC wraps around.
 */
public class ApocDateAdd extends AbstractApocDateFunction {
  @Override
  protected String getSimpleName() {
    return "add";
  }

  @Override
  public int getMinArgs() {
    return 4;
  }

  @Override
  public int getMaxArgs() {
    return 4;
  }

  @Override
  public String getDescription() {
    return "Add an amount of time to a time as APOC does: apoc.date.add(time, unit, addValue, addUnit)";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    if (args[0] == null || args[2] == null)
      return null;

    final long time = toMillis(args[0]);
    // Issue #10850: a whole number - longValue() read 1.5 hours as 1
    final long value = CypherFunctionHelper.requireWholeNumberOrTextArgument(args[2], getName(), "addValue");
    try {
      return Math.addExact(time, timeUnit(args[1]).convert(value, timeUnit(args[3])));
    } catch (final ArithmeticException e) {
      throw new InvalidArgumentException(getName() + "(): the sum is out of the long range", e);
    }
  }
}
