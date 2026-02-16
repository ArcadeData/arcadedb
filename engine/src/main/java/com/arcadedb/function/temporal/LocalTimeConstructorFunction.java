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
import com.arcadedb.query.sql.executor.CommandContext;

import java.time.LocalDateTime;
import java.util.Map;

/**
 * localtime() constructor function - creates a CypherLocalTime from a string, map, or temporal value.
 */
@SuppressWarnings("unchecked")
public class LocalTimeConstructorFunction implements StatelessFunction {
  @Override
  public String getName() {
    return "localtime";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    if (args.length == 0)
      return CypherFunctionHelper.getStatementTime(context).get("localtime");
    if (args[0] == null)
      return null;
    if (args[0] instanceof String)
      return CypherLocalTime.parse((String) args[0]);
    if (args[0] instanceof Map)
      return CypherLocalTime.fromMap((Map<String, Object>) args[0]);
    if (args[0] instanceof CypherLocalTime)
      return args[0];
    if (args[0] instanceof CypherTime)
      return new CypherLocalTime(((CypherTime) args[0]).getValue().toLocalTime());
    if (args[0] instanceof CypherLocalDateTime)
      return new CypherLocalTime(((CypherLocalDateTime) args[0]).getValue().toLocalTime());
    if (args[0] instanceof CypherDateTime)
      return new CypherLocalTime(((CypherDateTime) args[0]).getValue().toLocalTime());
    if (args[0] instanceof LocalDateTime)
      return new CypherLocalTime(((LocalDateTime) args[0]).toLocalTime());
    throw new CommandExecutionException("localtime() expects a string, map, or temporal argument");
  }
}
