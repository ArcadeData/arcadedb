/*
 * Copyright 2023 Arcade Data Ltd
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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

  public Object execute(final Object iThis, final Identifiable iCurrentRecord, final Object iCurrentResult, final Object[] iParams,
      final CommandContext iContext) {
    final LocalDateTime date;

    if (iParams.length == 0 || iParams[0] == null)
      date = LocalDateTime.now();
    else if (iParams[0] instanceof Number)
      date = DateUtils.millisToLocalDateTime(((Number) iParams[0]).longValue());
    else if (iParams[0] instanceof String) {
      try {
        final String dateAsString = (String) iParams[0];
        final String format;

        if (iParams.length > 1)
          format = (String) iParams[1];
        else {
          final String databaseDateFormat = iContext.getDatabase().getSchema().getDateFormat();
          if (dateAsString.length() == databaseDateFormat.length())
            format = databaseDateFormat;
          else {
            final String databaseDateTimeFormat = iContext.getDatabase().getSchema().getDateTimeFormat();
            if (dateAsString.length() == databaseDateTimeFormat.length())
              format = databaseDateTimeFormat;
            else
              return null;
          }
        }

        final DateTimeFormatter formatter = DateUtils.getFormatter(format)
            .withZone(iParams.length > 2 ? ZoneId.of(iParams[2].toString()) : iContext.getDatabase().getSchema().getZoneId());

        date = LocalDateTime.parse(dateAsString, formatter);
      } catch (DateTimeParseException e) {
        // DATE FORMAT NOT CORRECT
        return null;
      }
    } else
      return null;

    return DateUtils.getDate(date, iContext.getDatabase().getSerializer().getDateTimeImplementation());
  }

  public String getSyntax() {
    return "date([<date-as-number-or-string>] [,<format>] [,<timezone>])";
  }
}
