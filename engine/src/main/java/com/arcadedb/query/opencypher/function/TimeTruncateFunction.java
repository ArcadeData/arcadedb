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
import com.arcadedb.query.opencypher.temporal.CypherDateTime;
import com.arcadedb.query.opencypher.temporal.CypherLocalDateTime;
import com.arcadedb.query.opencypher.temporal.CypherLocalTime;
import com.arcadedb.query.opencypher.temporal.CypherTime;
import com.arcadedb.query.opencypher.temporal.TemporalUtil;
import com.arcadedb.query.sql.executor.CommandContext;

import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.util.Map;

/**
 * time.truncate() function - truncates a time to the specified unit.
 */
public class TimeTruncateFunction implements StatelessFunction {
  @Override
  public String getName() {
    return "time.truncate";
  }

  @SuppressWarnings("unchecked")
  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    if (args.length < 2)
      throw new CommandExecutionException("time.truncate() requires at least 2 arguments");
    final String unit = args[0].toString();
    final OffsetTime time;
    if (args[1] instanceof CypherTime)
      time = ((CypherTime) args[1]).getValue();
    else if (args[1] instanceof CypherDateTime)
      time = ((CypherDateTime) args[1]).getValue().toOffsetDateTime().toOffsetTime();
    else if (args[1] instanceof CypherLocalTime)
      time = ((CypherLocalTime) args[1]).getValue().atOffset(ZoneOffset.UTC);
    else if (args[1] instanceof CypherLocalDateTime)
      time = ((CypherLocalDateTime) args[1]).getValue().toLocalTime().atOffset(ZoneOffset.UTC);
    else
      throw new CommandExecutionException("time.truncate() second argument must be a temporal value with a time");
    LocalTime truncated = TemporalUtil.truncateLocalTime(time.toLocalTime(), unit);
    ZoneOffset offset = time.getOffset();
    if (args.length >= 3 && args[2] instanceof Map) {
      final Map<String, Object> adjustMap = (Map<String, Object>) args[2];
      truncated = CypherFunctionHelper.applyTimeMap(truncated, adjustMap);
      if (adjustMap.containsKey("timezone"))
        offset = TemporalUtil.parseOffset(adjustMap.get("timezone").toString());
    }
    return new CypherTime(OffsetTime.of(truncated, offset));
  }
}
