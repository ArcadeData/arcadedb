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

import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.function.StatelessFunction;
import com.arcadedb.function.cypher.CypherFunctionHelper;
import com.arcadedb.query.opencypher.temporal.CypherDateTime;
import com.arcadedb.query.opencypher.temporal.CypherLocalDateTime;
import com.arcadedb.query.opencypher.temporal.CypherLocalTime;
import com.arcadedb.query.opencypher.temporal.CypherTime;
import com.arcadedb.query.opencypher.temporal.TemporalUtil;
import com.arcadedb.query.sql.executor.CommandContext;

import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.DateTimeException;
import java.util.Map;

/**
 * time.truncate() function - truncates a time to the specified unit.
 */
public class TimeTruncateFunction implements StatelessFunction {
  @Override
  public String getName() {
    return "time.truncate";
  }

  @Override
  public int getMinArgs() {
    return 2;
  }

  @Override
  public int getMaxArgs() {
    return 3;
  }

  @SuppressWarnings("unchecked")
  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    checkArity(args);
    // issue #11589: a null unit or a null temporal value is a null answer, as Cypher propagates a null argument, not a
    // NullPointerException or a type error
    if (args[0] == null || args[1] == null)
      return null;
    final String unit = args[0].toString();
    final OffsetTime time;
    // A native temporal (a parameter a driver sent, a value read off a map or a list) is the Cypher temporal its literal is (issue #13569)
    final Object temporal = TemporalUtil.fromCoreJavaType(args[1]);
    if (temporal instanceof CypherTime)
      time = ((CypherTime) temporal).getValue();
    else if (temporal instanceof CypherDateTime)
      time = ((CypherDateTime) temporal).getValue().toOffsetDateTime().toOffsetTime();
    else if (temporal instanceof CypherLocalTime)
      time = ((CypherLocalTime) temporal).getValue().atOffset(ZoneOffset.UTC);
    else if (temporal instanceof CypherLocalDateTime)
      time = ((CypherLocalDateTime) temporal).getValue().toLocalTime().atOffset(ZoneOffset.UTC);
    else
      // Not a time-bearing temporal value: determined entirely by the supplied argument, so it is a client error
      // (HTTP 400) rather than a CommandExecutionException (HTTP 500). See issue #6638.
      throw new CommandSemanticException("time.truncate() second argument must be a temporal value with a time");
    LocalTime truncated = TemporalUtil.truncateLocalTime(time.toLocalTime(), unit);
    ZoneOffset offset = time.getOffset();
    // An explicitly written null adjustment map propagates, like every argument before it; only an omitted one means
    // "no adjustment" (issue #5629). This sits after the unit and the temporal value have been validated, so a bad unit
    // is still reported rather than being masked by the null - the same ordering round() uses.
    if (CypherFunctionHelper.isExplicitNull(args, 2))
      return null;
    if (args.length >= 3 && args[2] instanceof Map) {
      final Map<String, Object> adjustMap = (Map<String, Object>) args[2];
      truncated = CypherFunctionHelper.applyTimeMap(truncated, adjustMap, true);
      final Object timezone = adjustMap.get("timezone");
      // A null timezone names no zone: refused as Neo4j refuses it, as an invalid zone is
      if (timezone == null && adjustMap.containsKey("timezone"))
        throw new CommandSemanticException("time.truncate() timezone cannot be null");
      if (timezone != null)
        try {
          offset = TemporalUtil.parseOffset(timezone.toString());
        } catch (final DateTimeException e) {
          throw new CommandSemanticException("time.truncate() timezone is not a valid offset or zone: " + timezone);
        }
    }
    return new CypherTime(OffsetTime.of(truncated, offset));
  }
}
