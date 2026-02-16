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
import com.arcadedb.query.opencypher.temporal.CypherDateTime;
import com.arcadedb.query.opencypher.temporal.CypherLocalDateTime;
import com.arcadedb.query.opencypher.temporal.CypherLocalTime;
import com.arcadedb.query.opencypher.temporal.CypherTime;
import com.arcadedb.query.opencypher.temporal.TemporalUtil;
import com.arcadedb.query.sql.executor.CommandContext;

import java.time.LocalTime;
import java.util.Map;

/**
 * localtime.truncate() function - truncates a local time to the specified unit.
 */
public class LocalTimeTruncateFunction implements StatelessFunction {
  @Override
  public String getName() {
    return "localtime.truncate";
  }

  @SuppressWarnings("unchecked")
  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    if (args.length < 2)
      throw new CommandExecutionException("localtime.truncate() requires at least 2 arguments");
    final String unit = args[0].toString();
    final LocalTime time;
    if (args[1] instanceof CypherLocalTime)
      time = ((CypherLocalTime) args[1]).getValue();
    else if (args[1] instanceof CypherTime)
      time = ((CypherTime) args[1]).getValue().toLocalTime();
    else if (args[1] instanceof CypherLocalDateTime)
      time = ((CypherLocalDateTime) args[1]).getValue().toLocalTime();
    else if (args[1] instanceof CypherDateTime)
      time = ((CypherDateTime) args[1]).getValue().toLocalTime();
    else
      throw new CommandExecutionException("localtime.truncate() second argument must be a temporal value with a time");
    LocalTime truncated = TemporalUtil.truncateLocalTime(time, unit);
    if (args.length >= 3 && args[2] instanceof Map)
      truncated = CypherFunctionHelper.applyTimeMap(truncated, (Map<String, Object>) args[2]);
    return new CypherLocalTime(truncated);
  }
}
