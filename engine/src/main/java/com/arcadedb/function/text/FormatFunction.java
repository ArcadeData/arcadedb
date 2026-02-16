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
package com.arcadedb.function.text;

import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.function.StatelessFunction;
import com.arcadedb.query.opencypher.temporal.CypherDate;
import com.arcadedb.query.opencypher.temporal.CypherDateTime;
import com.arcadedb.query.opencypher.temporal.CypherDuration;
import com.arcadedb.query.opencypher.temporal.CypherLocalDateTime;
import com.arcadedb.query.opencypher.temporal.CypherLocalTime;
import com.arcadedb.query.opencypher.temporal.CypherTime;
import com.arcadedb.query.sql.executor.CommandContext;

import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;

/**
 * format() function - formats a temporal value using an optional pattern.
 */
public class FormatFunction implements StatelessFunction {
  @Override
  public String getName() {
    return "format";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    if (args.length < 1 || args.length > 2)
      throw new CommandExecutionException("format() requires 1 or 2 arguments: format(temporal[, pattern])");
    if (args[0] == null)
      return null;

    // Without pattern -> ISO string
    if (args.length == 1 || args[1] == null)
      return args[0].toString();

    final String pattern = args[1].toString();
    final DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern);

    // CypherDuration is not a TemporalAccessor, handle separately
    if (args[0] instanceof CypherDuration)
      throw new CommandExecutionException("format() with a pattern is not supported for Duration values");

    // Extract the TemporalAccessor from Cypher temporal types
    final TemporalAccessor temporal;
    if (args[0] instanceof CypherDate)
      temporal = ((CypherDate) args[0]).getValue();
    else if (args[0] instanceof CypherLocalTime)
      temporal = ((CypherLocalTime) args[0]).getValue();
    else if (args[0] instanceof CypherTime)
      temporal = ((CypherTime) args[0]).getValue();
    else if (args[0] instanceof CypherLocalDateTime)
      temporal = ((CypherLocalDateTime) args[0]).getValue();
    else if (args[0] instanceof CypherDateTime)
      temporal = ((CypherDateTime) args[0]).getValue();
    else if (args[0] instanceof TemporalAccessor)
      temporal = (TemporalAccessor) args[0];
    else
      throw new CommandExecutionException("format() requires a temporal value as first argument");

    return formatter.format(temporal);
  }
}
