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

import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.function.StatelessFunction;
import com.arcadedb.query.opencypher.temporal.CypherDateTime;
import com.arcadedb.query.opencypher.temporal.CypherLocalDateTime;
import com.arcadedb.query.opencypher.temporal.CypherLocalTime;
import com.arcadedb.query.opencypher.temporal.CypherTime;
import com.arcadedb.query.opencypher.temporal.TemporalUtil;
import com.arcadedb.query.sql.executor.CommandContext;

import java.time.DateTimeException;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
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
  public int getMinArgs() {
    return 0;
  }

  @Override
  public int getMaxArgs() {
    return 1;
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    checkArity(args);
    // No argument means "now". checkArity() lets a null array through for a zero-minimum function, so treat it as empty
    // rather than dereferencing it (issue #5602).
    if (args == null || args.length == 0)
      return CypherFunctionHelper.getStatementTime(context).get("time");
    if (args[0] == null)
      return null;
    if (args[0] instanceof String) {
      final String str = (String) args[0];
      try {
        return CypherTime.parse(str);
      } catch (final TemporalUtil.InvalidTemporalTextException e) {
        // The text is a time that contradicts itself (issue #12298): no timezone reading of it applies
        throw new CommandSemanticException("time() " + e.getMessage());
      } catch (final Exception e) {
        try {
          return new CypherTime(CypherFunctionHelper.getStatementTime(context, ZoneId.of(str)).toOffsetDateTime().toOffsetTime());
        } catch (final Exception e2) {
          // Neither a valid time nor a valid timezone: determined entirely by the supplied string, so it is a
          // client error (HTTP 400) rather than a CommandExecutionException (HTTP 500). See issue #5910.
          throw new CommandSemanticException("time() cannot parse '" + str + "' as a time or timezone");
        }
      }
    }
    if (args[0] instanceof Map) {
      final Map<String, Object> map = (Map<String, Object>) args[0];
      // A map holding only a timezone is the current time in that zone (issue #12297): it used to read as a map
      // with no year, which defaulted to January 1 of the current year
      try {
        if (map.size() == 1 && map.containsKey("timezone") && map.get("timezone") != null) {
          final ZonedDateTime now = CypherFunctionHelper.getStatementTime(context, TemporalUtil.parseZone(map.get("timezone").toString()));
          return new CypherTime(now.toOffsetDateTime().toOffsetTime());
        }
        return CypherTime.fromMap(map);
      } catch (final DateTimeException | IllegalArgumentException e) {
        // A field outside its range (month 13, week 54 of any year, February 30, nanosecond 10^9) is the caller's error, not an
        // internal one. TemporalUtil range-checks the sub-second fields with an IllegalArgumentException (#11655)
        throw new CommandSemanticException("time() " + e.getMessage());
      }
    }
    // A native temporal (a parameter a driver sent, a value read off a map or a list) is the Cypher temporal its literal is (issue #13569)
    final Object temporal = TemporalUtil.fromCoreJavaType(args[0]);
    if (temporal instanceof CypherTime)
      return temporal;
    if (temporal instanceof CypherDateTime)
      return new CypherTime(((CypherDateTime) temporal).getValue().toOffsetDateTime().toOffsetTime());
    if (temporal instanceof CypherLocalTime)
      return new CypherTime(((CypherLocalTime) temporal).getValue().atOffset(ZoneOffset.UTC));
    if (temporal instanceof CypherLocalDateTime)
      return new CypherTime(((CypherLocalDateTime) temporal).getValue().toLocalTime().atOffset(ZoneOffset.UTC));
    throw new CommandSemanticException("time() expects a string, map, or temporal argument");
  }
}
