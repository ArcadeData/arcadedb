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
import com.arcadedb.query.opencypher.temporal.CypherTemporalValue;
import com.arcadedb.query.sql.executor.CommandContext;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Map;

/**
 * date() constructor function - creates a CypherDate from a string, map, or temporal value.
 */
@SuppressWarnings("unchecked")
public class DateConstructorFunction implements StatelessFunction {
  @Override
  public String getName() {
    return "date";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    if (args.length == 0)
      return CypherFunctionHelper.getStatementTime(context).get("date");
    if (args[0] == null)
      return null;
    if (args[0] instanceof String)
      return CypherDate.parse((String) args[0]);
    if (args[0] instanceof Map)
      return CypherDate.fromMap((Map<String, Object>) args[0]);
    if (args[0] instanceof CypherDate)
      return args[0];
    if (args[0] instanceof CypherLocalDateTime)
      return new CypherDate(((CypherLocalDateTime) args[0]).getValue().toLocalDate());
    if (args[0] instanceof CypherDateTime)
      return new CypherDate(((CypherDateTime) args[0]).getValue().toLocalDate());
    if (args[0] instanceof LocalDate)
      return new CypherDate((LocalDate) args[0]);
    if (args[0] instanceof LocalDateTime)
      return new CypherDate(((LocalDateTime) args[0]).toLocalDate());
    if (args[0] instanceof CypherTemporalValue) {
      // Generic temporal -> extract date part
      final CypherTemporalValue tv = (CypherTemporalValue) args[0];
      final Object year = tv.getTemporalProperty("year");
      if (year != null)
        return new CypherDate(LocalDate.of(((Number) year).intValue(),
            ((Number) tv.getTemporalProperty("month")).intValue(),
            ((Number) tv.getTemporalProperty("day")).intValue()));
    }
    throw new CommandExecutionException("date() expects a string, map, or temporal argument");
  }
}
