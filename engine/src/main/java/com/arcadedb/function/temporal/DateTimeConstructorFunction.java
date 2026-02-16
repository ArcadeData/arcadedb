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
import com.arcadedb.query.sql.executor.CommandContext;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Map;

/**
 * datetime() constructor function - creates a CypherDateTime from a string, map, or temporal value.
 */
@SuppressWarnings("unchecked")
public class DateTimeConstructorFunction implements StatelessFunction {
  @Override
  public String getName() {
    return "datetime";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    if (args.length == 0)
      return CypherFunctionHelper.getStatementTime(context).get("datetime");
    if (args[0] == null)
      return null;
    if (args[0] instanceof String)
      return CypherDateTime.parse((String) args[0]);
    if (args[0] instanceof Map)
      return CypherDateTime.fromMap((Map<String, Object>) args[0]);
    if (args[0] instanceof CypherDateTime)
      return args[0];
    if (args[0] instanceof CypherLocalDateTime)
      return new CypherDateTime(((CypherLocalDateTime) args[0]).getValue().atZone(ZoneOffset.UTC));
    if (args[0] instanceof CypherDate)
      return new CypherDateTime(((CypherDate) args[0]).getValue().atStartOfDay(ZoneOffset.UTC));
    if (args[0] instanceof LocalDateTime)
      return new CypherDateTime(((LocalDateTime) args[0]).atZone(ZoneOffset.UTC));
    if (args[0] instanceof LocalDate)
      return new CypherDateTime(((LocalDate) args[0]).atStartOfDay(ZoneOffset.UTC));
    throw new CommandExecutionException("datetime() expects a string, map, or temporal argument");
  }
}
