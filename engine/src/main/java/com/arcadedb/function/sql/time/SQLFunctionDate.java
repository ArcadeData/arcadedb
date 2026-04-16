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
package com.arcadedb.function.sql.time;

import com.arcadedb.database.Identifiable;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.function.sql.FunctionOptions;
import com.arcadedb.function.sql.SQLFunctionAbstract;
import com.arcadedb.utility.DateUtils;

import java.time.*;
import java.time.format.*;
import java.util.Map;
import java.util.Set;

/**
 * Builds a date object from the format passed as a number or a string. If no arguments are passed, then the system date is built (like sysdate() function).
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 * @see SQLFunctionSysdate
 */
public class SQLFunctionDate extends SQLFunctionAbstract {
  public static final String NAME = "date";

  private static final Set<String> OPTIONS = Set.of("format", "timezone");

  /**
   * Get the date at construction to have the same date for all the iteration.
   */
  public SQLFunctionDate() {
    super(NAME);
  }

  public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
      final CommandContext context) {
    final LocalDateTime date;

    if (params.length == 0 || params[0] == null)
      date = LocalDateTime.now();
    else if (params[0] instanceof Number number)
      date = DateUtils.millisToLocalDateTime(number.longValue(), null);
    else if (params[0] instanceof String dateAsString) {
      try {
        // params[1] can be a format string (positional) or a full options map { format, timezone }.
        String format = null;
        String timezone = null;

        if (params.length > 1 && params[1] != null) {
          if (params[1] instanceof Map<?, ?> rawMap) {
            final FunctionOptions opts = new FunctionOptions(NAME, rawMap, OPTIONS);
            format = opts.getString("format", null);
            timezone = opts.getString("timezone", null);
          } else {
            format = params[1].toString();
            if (params.length > 2 && params[2] != null)
              timezone = params[2].toString();
          }
        }

        if (format == null) {
          final String databaseDateFormat = context.getDatabase().getSchema().getDateFormat();
          if (dateAsString.length() == databaseDateFormat.length())
            format = databaseDateFormat;
          else {
            final String databaseDateTimeFormat = context.getDatabase().getSchema().getDateTimeFormat();
            if (dateAsString.length() == databaseDateTimeFormat.length())
              format = databaseDateTimeFormat;
            else
              return null;
          }
        }

        final DateTimeFormatter formatter = DateUtils.getFormatter(format)
            .withZone(timezone != null ? ZoneId.of(timezone) : context.getDatabase().getSchema().getZoneId());

        date = LocalDateTime.parse(dateAsString, formatter);
      } catch (DateTimeParseException e) {
        // DATE FORMAT NOT CORRECT
        return null;
      }
    } else
      return null;

    return DateUtils.getDate(date, context.getDatabase().getSerializer().getDateTimeImplementation());
  }

  public String getSyntax() {
    return "date([<date-as-number-or-string> [, <format> | { format: '...', timezone: '...' } [, <timezone>]]])";
  }
}
