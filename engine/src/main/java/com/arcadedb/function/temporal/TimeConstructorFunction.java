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

import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.function.StatelessFunction;
import com.arcadedb.function.cypher.CypherFunctionHelper;
import com.arcadedb.query.opencypher.temporal.CypherDateTime;
import com.arcadedb.query.opencypher.temporal.CypherLocalDateTime;
import com.arcadedb.query.opencypher.temporal.CypherLocalTime;
import com.arcadedb.query.opencypher.temporal.CypherTime;
import com.arcadedb.query.sql.executor.CommandContext;

import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Map;

/**
 * time() constructor function - creates a CypherTime from a string, map, or temporal value.
 */
@SuppressWarnings("unchecked")
public class TimeConstructorFunction implements StatelessFunction {
  @Override
  public String getName() {
    return "time";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    if (args.length == 0)
      return CypherFunctionHelper.getStatementTime(context).get("time");
    if (args[0] == null)
      return null;
    if (args[0] instanceof String str) {
      try {
        return CypherTime.parse(str);
      } catch (final Exception e) {
        try {
          return new CypherTime(OffsetTime.now(ZoneId.of(str)));
        } catch (final Exception e2) {
          throw new CommandExecutionException("time() cannot parse '" + str + "' as a time or timezone");
        }
      }
    }
    if (args[0] instanceof Map)
      return CypherTime.fromMap((Map<String, Object>) args[0]);
    if (args[0] instanceof CypherTime)
      return args[0];
    if (args[0] instanceof CypherDateTime time)
      return new CypherTime(time.getValue().toOffsetDateTime().toOffsetTime());
    if (args[0] instanceof CypherLocalTime time1)
      return new CypherTime(time1.getValue().atOffset(ZoneOffset.UTC));
    if (args[0] instanceof CypherLocalDateTime time2)
      return new CypherTime(time2.getValue().toLocalTime().atOffset(ZoneOffset.UTC));
    throw new CommandExecutionException("time() expects a string, map, or temporal argument");
  }
}
