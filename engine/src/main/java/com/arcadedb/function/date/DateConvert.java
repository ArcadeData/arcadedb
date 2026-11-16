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
 * date.convert(time, unit, toUnit) - Convert a time from one unit to another, as APOC's apoc.date.convert does (issue
 * #12106): {@link java.util.concurrent.TimeUnit#convert(long, java.util.concurrent.TimeUnit)}, truncating toward zero and
 * saturating at {@link Long#MAX_VALUE} or {@link Long#MIN_VALUE} when the result is out of the long range.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class DateConvert extends AbstractDateFunction {
  @Override
  protected String getSimpleName() {
    return "convert";
  }

  @Override
  public int getMinArgs() {
    return 3;
  }

  @Override
  public int getMaxArgs() {
    return 3;
  }

  @Override
  public String getDescription() {
    return "Convert a time from one unit to another: date.convert(time, unit, toUnit), saturating at the long range";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    if (args[0] == null)
      return null;

    // Issue #10850: a whole number - longValue() read 1.5 seconds as 1
    final long value = CypherFunctionHelper.requireWholeNumberOrTextArgument(args[0], getName(), "value");
    return timeUnit(args[2]).convert(value, timeUnit(args[1]));
  }
}
