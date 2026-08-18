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

import java.time.DateTimeException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
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

        final DateTimeFormatter formatter = formatterFor(format, timezone, context);

        date = LocalDateTime.parse(dateAsString, formatter);
      } catch (final DateTimeParseException e) {
        // THE VALUE DOES NOT MATCH THE FORMAT: NOT AN ERROR, THE DOCUMENTED ANSWER IS NULL. THE ARGUMENTS THEMSELVES
        // BEING WRONG IS A DIFFERENT STORY AND IS RAISED BY formatterFor() BELOW.
        return null;
      }
    } else
      return null;

    return DateUtils.getDate(date, context.getDatabase().getSerializer().getDateTimeImplementation());
  }

  /**
   * Builds the formatter for the requested pattern and zone. A malformed pattern ({@code IllegalArgumentException}
   * out of {@code appendPattern}) and an unknown zone id ({@code ZoneRulesException}, a {@link DateTimeException} but
   * NOT a {@link DateTimeParseException}) both escaped the caller's catch and surfaced as raw JDK exceptions - an
   * HTTP 500 for what is a mistake in the call (issue #6388). They are raised here as a typed client error instead,
   * deliberately NOT folded into the {@code return null} above: a value that does not match its format is an
   * ordinary miss, a format that cannot exist is a query to fix.
   */
  private static DateTimeFormatter formatterFor(final String format, final String timezone, final CommandContext context) {
    final DateTimeFormatter formatter;
    try {
      formatter = DateUtils.getFormatter(format);
    } catch (final IllegalArgumentException e) {
      throw new IllegalArgumentException(NAME + "() received an invalid date format '" + format + "': " + e.getMessage(), e);
    }

    if (timezone == null)
      return formatter.withZone(context.getDatabase().getSchema().getZoneId());

    try {
      return formatter.withZone(ZoneId.of(timezone));
    } catch (final DateTimeException e) {
      throw new IllegalArgumentException(NAME + "() received an unknown time zone id '" + timezone + "'", e);
    }
  }

  public String getSyntax() {
    return "date([<date-as-number-or-string> [, <format> | { format: '...', timezone: '...' } [, <timezone>]]])";
  }
}
