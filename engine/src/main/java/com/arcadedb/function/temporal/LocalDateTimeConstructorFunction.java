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
import com.arcadedb.query.opencypher.temporal.CypherTime;
import com.arcadedb.query.sql.executor.CommandContext;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Map;

/**
 * localdatetime() constructor function - creates a CypherLocalDateTime from a string, map, or temporal value.
 */
@SuppressWarnings("unchecked")
public class LocalDateTimeConstructorFunction implements StatelessFunction {
  @Override
  public String getName() {
    return "localdatetime";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    if (args.length == 0)
      return CypherFunctionHelper.getStatementTime(context).get("localdatetime");
    if (args[0] == null)
      return null;
    if (args[0] instanceof String)
      return CypherLocalDateTime.parse((String) args[0]);
    if (args[0] instanceof Map)
      return CypherLocalDateTime.fromMap((Map<String, Object>) args[0]);
    if (args[0] instanceof CypherLocalDateTime)
      return args[0];
    if (args[0] instanceof CypherDateTime)
      return new CypherLocalDateTime(((CypherDateTime) args[0]).getValue().toLocalDateTime());
    if (args[0] instanceof CypherDate)
      return new CypherLocalDateTime(((CypherDate) args[0]).getValue().atStartOfDay());
    if (args[0] instanceof LocalDateTime)
      return new CypherLocalDateTime((LocalDateTime) args[0]);
    if (args[0] instanceof LocalDate)
      return new CypherLocalDateTime(((LocalDate) args[0]).atStartOfDay());
    if (args[0] instanceof CypherTime)
      return new CypherLocalDateTime(LocalDateTime.of(LocalDate.now(), ((CypherTime) args[0]).getValue().toLocalTime()));
    throw new CommandExecutionException("localdatetime() expects a string, map, or temporal argument");
  }
}
