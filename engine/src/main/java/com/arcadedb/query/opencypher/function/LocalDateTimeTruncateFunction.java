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
import com.arcadedb.query.opencypher.temporal.CypherDate;
import com.arcadedb.query.opencypher.temporal.CypherDateTime;
import com.arcadedb.query.opencypher.temporal.CypherLocalDateTime;
import com.arcadedb.query.opencypher.temporal.TemporalUtil;
import com.arcadedb.query.sql.executor.CommandContext;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Map;

/**
 * localdatetime.truncate() function - truncates a local datetime to the specified unit.
 */
public class LocalDateTimeTruncateFunction implements StatelessFunction {
  @Override
  public String getName() {
    return "localdatetime.truncate";
  }

  @SuppressWarnings("unchecked")
  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    if (args.length < 2)
      throw new CommandExecutionException("localdatetime.truncate() requires at least 2 arguments");
    final String unit = args[0].toString();
    final LocalDateTime dt;
    if (args[1] instanceof CypherLocalDateTime)
      dt = ((CypherLocalDateTime) args[1]).getValue();
    else if (args[1] instanceof CypherDateTime)
      dt = ((CypherDateTime) args[1]).getValue().toLocalDateTime();
    else if (args[1] instanceof CypherDate)
      dt = ((CypherDate) args[1]).getValue().atStartOfDay();
    else if (args[1] instanceof LocalDateTime)
      dt = (LocalDateTime) args[1];
    else if (args[1] instanceof LocalDate)
      dt = ((LocalDate) args[1]).atStartOfDay();
    else
      throw new CommandExecutionException("localdatetime.truncate() second argument must be a temporal value");
    LocalDateTime truncated = TemporalUtil.truncateLocalDateTime(dt, unit);
    if (args.length >= 3 && args[2] instanceof Map)
      truncated = CypherFunctionHelper.applyDateTimeMap(truncated, (Map<String, Object>) args[2]);
    return new CypherLocalDateTime(truncated);
  }
}
