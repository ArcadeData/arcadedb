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
import com.arcadedb.query.opencypher.temporal.CypherDate;
import com.arcadedb.query.opencypher.temporal.CypherDateTime;
import com.arcadedb.query.opencypher.temporal.CypherLocalDateTime;
import com.arcadedb.query.opencypher.temporal.TemporalUtil;
import com.arcadedb.query.sql.executor.CommandContext;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.DateTimeException;
import java.util.Map;

/**
 * datetime.truncate() function - truncates a datetime to the specified unit.
 */
public class DateTimeTruncateFunction implements StatelessFunction {
  @Override
  public String getName() {
    return "datetime.truncate";
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
    final ZonedDateTime dt;
    if (args[1] instanceof CypherDateTime)
      dt = ((CypherDateTime) args[1]).getValue();
    else if (args[1] instanceof CypherLocalDateTime)
      dt = ((CypherLocalDateTime) args[1]).getValue().atZone(ZoneOffset.UTC);
    else if (args[1] instanceof CypherDate)
      dt = ((CypherDate) args[1]).getValue().atStartOfDay(ZoneOffset.UTC);
    else if (args[1] instanceof LocalDateTime)
      dt = ((LocalDateTime) args[1]).atZone(ZoneOffset.UTC);
    else if (args[1] instanceof LocalDate)
      dt = ((LocalDate) args[1]).atStartOfDay(ZoneOffset.UTC);
    else
      // Not a recognized temporal value: determined entirely by the supplied argument, so it is a client error
      // (HTTP 400) rather than a CommandExecutionException (HTTP 500). See issue #6638.
      throw new CommandSemanticException("datetime.truncate() second argument must be a temporal value");
    LocalDateTime truncated = TemporalUtil.truncateLocalDateTime(dt.toLocalDateTime(), unit);
    ZoneId zone = dt.getZone();
    // An explicitly written null adjustment map propagates, like every argument before it; only an omitted one means
    // "no adjustment" (issue #5629). This sits after the unit and the temporal value have been validated, so a bad unit
    // is still reported rather than being masked by the null - the same ordering round() uses.
    if (CypherFunctionHelper.isExplicitNull(args, 2))
      return null;
    if (args.length >= 3 && args[2] instanceof Map) {
      final Map<String, Object> adjustMap = (Map<String, Object>) args[2];
      truncated = CypherFunctionHelper.applyDateTimeMap(truncated, adjustMap, true);
      final Object timezone = adjustMap.get("timezone");
      // A null timezone names no zone: refused as Neo4j refuses it, as an invalid zone is
      if (timezone == null && adjustMap.containsKey("timezone"))
        throw new CommandSemanticException("datetime.truncate() timezone cannot be null");
      if (timezone != null)
        try {
          zone = TemporalUtil.parseZone(timezone.toString());
        } catch (final DateTimeException e) {
          throw new CommandSemanticException("datetime.truncate() timezone is not a valid zone: " + timezone);
        }
    }
    return new CypherDateTime(ZonedDateTime.of(truncated, zone));
  }
}
