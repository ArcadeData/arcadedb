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

import com.arcadedb.function.cypher.CypherFunctionHelper;
import com.arcadedb.query.sql.executor.CommandContext;

/**
 * date.add(time, unit, addValue, addUnit) - Add {@code addValue} {@code addUnit} to a timestamp in {@code unit}, as APOC's
 * apoc.date.add does (issue #12106): the amount is converted to {@code unit} first, truncating toward zero and saturating
 * at the long range. A sum outside the long range is an error, where APOC wraps around.
 * <p>
 * The three-argument form date.add(timestamp, value, unit) adds {@code value} {@code unit} to a timestamp in milliseconds,
 * as ArcadeDB always did. APOC has no three-argument form, so the two cannot be confused.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class DateAdd extends AbstractDateFunction {
  @Override
  protected String getSimpleName() {
    return "add";
  }

  @Override
  public int getMinArgs() {
    return 3;
  }

  @Override
  public int getMaxArgs() {
    return 4;
  }

  @Override
  public String getDescription() {
    return "Add an amount of time to a timestamp: date.add(time, unit, addValue, addUnit), or date.add(timestamp, value, unit) "
        + "on a timestamp in milliseconds";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    final Object amount = args.length == 4 ? args[2] : args[1];
    if (args[0] == null || amount == null)
      return null;

    final long timestamp = toMillis(args[0]);
    // Issue #10850: a whole number - longValue() read 1.5 hours as 1
    final long value = CypherFunctionHelper.requireWholeNumberOrTextArgument(amount, getName(), "value");

    try {
      if (args.length == 4)
        return Math.addExact(timestamp, timeUnit(args[1]).convert(value, timeUnit(args[3])));

      // Use Math.multiplyExact and Math.addExact to prevent integer overflow
      final long addMillis = Math.multiplyExact(value, timeUnit(args[2]).toMillis(1));
      return Math.addExact(timestamp, addMillis);
    } catch (final ArithmeticException e) {
      throw new IllegalArgumentException("Date arithmetic overflow: " + e.getMessage(), e);
    }
  }
}
