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
package com.arcadedb.function.temporal;

import com.arcadedb.function.cypher.CypherFunctionHelper;

import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.function.StatelessFunction;
import com.arcadedb.query.opencypher.temporal.CypherDate;
import com.arcadedb.query.opencypher.temporal.CypherDateTime;
import com.arcadedb.query.opencypher.temporal.CypherLocalDateTime;
import com.arcadedb.query.opencypher.temporal.TemporalUtil;
import com.arcadedb.query.sql.executor.CommandContext;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Map;

/**
 * date.truncate() function - truncates a date to the specified unit.
 */
public class DateTruncateFunction implements StatelessFunction {
  @Override
  public String getName() {
    return "date.truncate";
  }

  @SuppressWarnings("unchecked")
  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    if (args.length < 2)
      throw new CommandExecutionException("date.truncate() requires at least 2 arguments: unit, temporal");
    final String unit = args[0].toString();
    final LocalDate date;
    if (args[1] instanceof CypherDate)
      date = ((CypherDate) args[1]).getValue();
    else if (args[1] instanceof CypherLocalDateTime)
      date = ((CypherLocalDateTime) args[1]).getValue().toLocalDate();
    else if (args[1] instanceof CypherDateTime)
      date = ((CypherDateTime) args[1]).getValue().toLocalDate();
    else if (args[1] instanceof LocalDate)
      date = (LocalDate) args[1];
    else if (args[1] instanceof LocalDateTime)
      date = ((LocalDateTime) args[1]).toLocalDate();
    else
      throw new CommandExecutionException("date.truncate() second argument must be a temporal value with a date");
    LocalDate truncated = TemporalUtil.truncateDate(date, unit);
    // Apply optional map adjustment
    if (args.length >= 3 && args[2] instanceof Map)
      truncated = CypherFunctionHelper.applyDateMap(truncated, (Map<String, Object>) args[2]);
    return new CypherDate(truncated);
  }
}
