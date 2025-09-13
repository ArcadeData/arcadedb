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
package com.arcadedb.query.sql.function.time;

import com.arcadedb.database.Identifiable;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.function.SQLFunctionAbstract;
import com.arcadedb.utility.DateUtils;

import java.time.*;
import java.time.format.*;

/**
 * Builds a date object from the format passed as a number or a string. If no arguments are passed, then the system date is built (like sysdate() function).
 *
 * @author Luca Garulli (l.garulli--(at)--gmail.com)
 * @see SQLFunctionSysdate
 */
public class SQLFunctionDate extends SQLFunctionAbstract {
  public static final String NAME = "date";

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
        final String format;

        if (params.length > 1)
          format = (String) params[1];
        else {
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
            .withZone(params.length > 2 ? ZoneId.of(params[2].toString()) : context.getDatabase().getSchema().getZoneId());

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
    return "date([<date-as-number-or-string>] [,<format>] [,<timezone>])";
  }
}
